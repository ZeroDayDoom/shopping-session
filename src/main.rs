// Shopping Sessions pipeline.
// Implements rules.md exactly: custom CSV dialect, strict timestamps,
// linked-ID union-find, sessionization, 7-day attribution + assists,
// and writes $OUTPUT_DIR/user_daily and $OUTPUT_DIR/campaign_daily as Parquet.
//
// Log to stderr only; stdout must stay quiet.

use std::collections::HashMap;
use std::env;
use std::fs;
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use rayon::prelude::*;

// ---------------------------------------------------------------- hashing

#[inline]
fn mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// 128-bit key from a byte string: two FNV-1a streams with different inits,
/// finalized with splitmix. Inputs are non-adversarial (generated data); only
/// exact byte equality of ids matters to the rules.
#[inline]
fn hash128(b: &[u8], seed: u64) -> [u64; 2] {
    let mut h1 = 0xcbf2_9ce4_8422_2325u64 ^ seed;
    let mut h2 = 0x8422_2325_cbf2_9ce4u64 ^ mix(seed | 1);
    for &c in b {
        h1 = (h1 ^ c as u64).wrapping_mul(0x1_0000_0001_b3);
        h2 = (h2 ^ c as u64).wrapping_mul(0x1_0000_0001_b3);
    }
    [mix(h1), mix(h2 ^ 0x9e37_79b9_7f4a_7c15)]
}

/// Hasher for keys that are already 128-bit hashes.
#[derive(Default)]
struct PreHasher(u64);

impl Hasher for PreHasher {
    #[inline]
    fn write_u64(&mut self, v: u64) {
        self.0 = mix(self.0 ^ v);
    }
    #[inline]
    fn write_u128(&mut self, v: u128) {
        self.write_u64(v as u64);
        self.write_u64((v >> 64) as u64);
    }
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        // Fallback fold for keys whose derived Hash writes raw bytes
        // (e.g. [u64; 2]). Deterministic; quality is fine for 16-byte keys.
        for &b in bytes {
            self.0 = (self.0 ^ b as u64).wrapping_mul(0x1_0000_0001_b3);
        }
    }
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
}

// ---------------------------------------------------------------- CSV

/// Clean one raw CSV field. A quote may only wrap a whole field.
/// Returns None when the field makes the whole line malformed.
#[inline]
fn clean_field(f: &[u8]) -> Option<&[u8]> {
    if !f.is_empty() && f[0] == b'"' {
        if f.len() < 2 || f[f.len() - 1] != b'"' {
            return None;
        }
        let inner = &f[1..f.len() - 1];
        if inner.contains(&b'"') {
            return None;
        }
        Some(inner)
    } else {
        if f.contains(&b'"') {
            return None;
        }
        Some(f)
    }
}

/// Split a line into exactly N fields (comma separated, quote rules above).
/// A line with fewer or more fields is malformed. An empty line is malformed.
#[inline]
fn split_fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    let mut out: [&[u8]; N] = [&[]; N];
    let mut rest = line;
    for i in 0..N {
        let f: &[u8];
        if i + 1 < N {
            let p = rest.iter().position(|&c| c == b',')?;
            f = &rest[..p];
            rest = &rest[p + 1..];
        } else {
            if rest.iter().any(|&c| c == b',') {
                return None;
            }
            f = rest;
        }
        out[i] = clean_field(f)?;
    }
    Some(out)
}

/// Read one CSV file and parse every data line in parallel chunks.
/// Returns per-chunk record vectors and per-chunk byte arenas.
/// The file's first line is always the header and is skipped.
fn parse_file<T, F>(path: &Path, parse_line: F) -> Vec<(Vec<T>, Vec<u8>)>
where
    T: Send,
    F: Fn(&[u8], &mut Vec<T>, &mut Vec<u8>) + Sync,
{
    let data = match fs::read(path) {
        Ok(d) => d,
        Err(_) => return Vec::new(),
    };
    if data.is_empty() {
        return Vec::new();
    }
    // Chunk at \n boundaries near 24 MB. Values never contain \n, so a \n
    // always ends a line.
    const TARGET: usize = 24 << 20;
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    if data.len() <= TARGET {
        ranges.push((0, data.len()));
    } else {
        let mut start = 0usize;
        while start < data.len() {
            let mut end = (start + TARGET).min(data.len());
            if end < data.len() {
                match data[end..].iter().position(|&c| c == b'\n') {
                    Some(p) => end += p + 1,
                    None => end = data.len(),
                }
            }
            ranges.push((start, end));
            start = end;
        }
    }
    ranges
        .par_iter()
        .map(|&(lo, hi)| {
            let mut out = Vec::new();
            let mut arena = Vec::new();
            let mut first = lo == 0; // only the file's first chunk holds the header
            for seg in data[lo..hi].split_inclusive(|&c| c == b'\n') {
                let mut line = seg;
                if line.last() == Some(&b'\n') {
                    line = &line[..line.len() - 1];
                }
                if first {
                    first = false;
                    continue; // header
                }
                if line.is_empty() {
                    continue; // an empty line is malformed
                }
                parse_line(line, &mut out, &mut arena);
            }
            (out, arena)
        })
        .collect()
}

// ---------------------------------------------------------------- timestamps

const INVALID_TS: i64 = i64::MIN;

#[inline]
fn num(b: &[u8]) -> Option<i64> {
    let mut v: i64 = 0;
    for &c in b {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (c - b'0') as i64;
    }
    Some(v)
}

#[inline]
fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

#[inline]
fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap(y) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

/// Days since 1970-01-01 (Howard Hinnant's algorithm). y/m/d must be sane.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12; // March = 0
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Calendar part of a timestamp: bytes 0..19 formatted YYYY-MM-DDTHH:MM:SS.
/// Validates every field strictly.
fn civil_ms(b: &[u8]) -> i64 {
    let (y, mo, d, h, mi, s) = match (
        num(&b[0..4]),
        num(&b[5..7]),
        num(&b[8..10]),
        num(&b[11..13]),
        num(&b[14..16]),
        num(&b[17..19]),
    ) {
        (Some(a), Some(b2), Some(c), Some(e), Some(f), Some(g)) => (a, b2, c, e, f, g),
        _ => return INVALID_TS,
    };
    if mo < 1 || mo > 12 || d < 1 || d > days_in_month(y, mo) {
        return INVALID_TS;
    }
    if h > 23 || mi > 59 || s > 59 {
        return INVALID_TS;
    }
    days_from_civil(y, mo, d) * 86_400_000 + h * 3_600_000 + mi * 60_000 + s * 1_000
}

/// Parse ts / ingest_ts. Three forms only (rules.md):
/// F1 `2026-10-03T23:58:10Z`, F2 `2026-10-03T19:58:10-04:00`,
/// F3 digits-only epoch millis. Everything else is INVALID_TS.
fn parse_ts(b: &[u8]) -> i64 {
    if b.len() == 20
        && b[4] == b'-'
        && b[7] == b'-'
        && b[10] == b'T'
        && b[13] == b':'
        && b[16] == b':'
        && b[19] == b'Z'
    {
        return civil_ms(b);
    }
    if b.len() == 25
        && b[4] == b'-'
        && b[7] == b'-'
        && b[10] == b'T'
        && b[13] == b':'
        && b[16] == b':'
        && (b[19] == b'+' || b[19] == b'-')
        && b[22] == b':'
    {
        let base = civil_ms(b);
        if base == INVALID_TS {
            return INVALID_TS;
        }
        let (oh, om) = match (num(&b[20..22]), num(&b[23..25])) {
            (Some(a), Some(c)) => (a, c),
            _ => return INVALID_TS,
        };
        if oh > 23 || om > 59 {
            return INVALID_TS;
        }
        let off_ms = (oh * 3600 + om * 60) * 1000;
        if b[19] == b'+' {
            base - off_ms
        } else {
            base + off_ms
        }
    } else if !b.is_empty() && b.iter().all(|&c| c.is_ascii_digit()) {
        // F3: leading zeros fine; too long for i64 means outside the window.
        let mut v: i64 = 0;
        for &c in b {
            let d = (c - b'0') as i64;
            match v.checked_mul(10).and_then(|x| x.checked_add(d)) {
                Some(x) => v = x,
                None => return i64::MAX, // beyond window_end for any real window
            }
        }
        v
    } else {
        INVALID_TS
    }
}

/// price_cents: ASCII digits only, value 1..=10,000,000 for purchases.
#[inline]
fn parse_price(b: &[u8]) -> (i64, bool) {
    if b.is_empty() || b.len() > 8 {
        return (0, false);
    }
    let mut v: i64 = 0;
    for &c in b {
        if !c.is_ascii_digit() {
            return (0, false);
        }
        v = v * 10 + (c - b'0') as i64;
    }
    (v, v >= 1 && v <= 10_000_000)
}

// ---------------------------------------------------------------- interner

const NSHARDS: usize = 256;

struct Shard {
    map: HashMap<[u64; 2], u32, BuildHasherDefault<PreHasher>>,
    strs: Vec<(u32, Box<[u8]>)>,
}

/// Concurrent string -> u32 interner. Keeps every distinct byte string.
struct Interner {
    shards: Vec<Mutex<Shard>>,
    next: AtomicUsize,
}

impl Interner {
    fn new() -> Self {
        Interner {
            shards: (0..NSHARDS)
                .map(|_| {
                    Mutex::new(Shard {
                        map: HashMap::default(),
                        strs: Vec::new(),
                    })
                })
                .collect(),
            next: AtomicUsize::new(0),
        }
    }

    #[inline]
    fn intern(&self, domain: u64, b: &[u8]) -> u32 {
        let h = hash128(b, domain);
        let si = (h[0] as usize) % NSHARDS;
        let mut sh = self.shards[si].lock().unwrap();
        if let Some(&id) = sh.map.get(&h) {
            return id;
        }
        let id = self.next.fetch_add(1, Ordering::Relaxed) as u32;
        sh.map.insert(h, id);
        sh.strs.push((id, Box::from(b)));
        id
    }

    fn n(&self) -> usize {
        self.next.load(Ordering::Relaxed)
    }

    /// id -> bytes for every interned string.
    fn strings(&self) -> Vec<Box<[u8]>> {
        let n = self.n();
        let mut v: Vec<Option<Box<[u8]>>> = (0..n).map(|_| None).collect();
        for sh in &self.shards {
            let sh = sh.lock().unwrap();
            for (id, s) in &sh.strs {
                v[*id as usize] = Some(s.clone());
            }
        }
        v.into_iter().map(|x| x.expect("interned string")).collect()
    }
}

// ---------------------------------------------------------------- records

#[derive(Clone)]
struct ERec {
    h: [u64; 2], // event_id hash (dedup key)
    ts: i64,     // parsed or INVALID_TS
    ingest: i64,
    user: u32,
    product: u32,
    typ: u8, // 0 invalid, 1 view, 2 cart, 3 purchase
    price_ok: bool,
    price: i64,
}

#[derive(Clone, Copy)]
struct Ev2 {
    key: u64,     // root << 31 | ts_rel  (ts_rel = ts - window_start < 2^31)
    product: u32, // only for purchases
    price: i32,   // > 0 only for purchases
}

#[derive(Clone)]
struct CRec {
    h: [u64; 2], // click_id hash (dedup key)
    ts: i64,
    user: u32,
    camp: u32,
    arena: u32,
    off: u32,
    len: u32,
}

#[derive(Clone, Copy)]
struct CFinal {
    root: u32,
    ts: i64,
    camp: u16,
    arena: u32,
    off: u32,
    len: u32,
}

#[derive(Clone, Copy)]
struct Sess {
    root: u32,
    start: i64,
    day: i32, // days since epoch of the session start
    revenue: i64,
    has_p: bool,
    camp: u16, // credited campaign, set during attribution
}

struct UDRow {
    root: u32,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: i32, // category rank, -1 = null
}

// ---------------------------------------------------------------- helpers

fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut v = Vec::new();
    if let Ok(rd) = fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.is_file() && p.extension().map(|x| x == "csv").unwrap_or(false) {
                v.push(p);
            }
        }
    }
    v.sort();
    v
}

fn json_str_field(s: &str, key: &str) -> Option<String> {
    let pat = format!("\"{}\"", key);
    let i = s.find(&pat)?;
    let rest = &s[i + pat.len()..];
    let c = rest.find(':')?;
    let rest = &rest[c + 1..];
    let q1 = rest.find('"')?;
    let rest2 = &rest[q1 + 1..];
    let q2 = rest2.find('"')?;
    Some(rest2[..q2].to_string())
}

fn find(p: &mut [u32], mut x: u32) -> u32 {
    while p[x as usize] != x {
        p[x as usize] = p[p[x as usize] as usize]; // path halving
        x = p[x as usize];
    }
    x
}

/// Iterate data lines of a small sequential CSV, skipping the header.
fn for_each_data_line(text: &[u8], mut f: impl FnMut(&[u8])) {
    let mut first = true;
    for seg in text.split_inclusive(|&c| c == b'\n') {
        let mut line = seg;
        if line.last() == Some(&b'\n') {
            line = &line[..line.len() - 1];
        }
        if first {
            first = false;
            continue;
        }
        if line.is_empty() {
            continue;
        }
        f(line);
    }
}

// ---------------------------------------------------------------- parquet

mod pq {
    use super::*;
    use parquet::basic::Compression;
    use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;

    const RG: usize = 262_144;

    fn open(file: fs::File, msg: &str) -> SerializedFileWriter<fs::File> {
        let tpe = parse_message_type(msg).expect("schema");
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        SerializedFileWriter::new(file, Arc::new(tpe), Arc::new(props)).expect("parquet writer")
    }

    // Write one column chunk into the current row group.
    macro_rules! col {
        ($rg:expr, $ty:ty, $vals:expr) => {{
            let mut cw = $rg.next_column().unwrap().unwrap();
            cw.typed::<$ty>().write_batch($vals, None, None).unwrap();
            cw.close().unwrap();
        }};
    }
    // Optional column: values plus definition levels (1 = present).
    macro_rules! col_opt {
        ($rg:expr, $ty:ty, $vals:expr, $defs:expr) => {{
            let mut cw = $rg.next_column().unwrap().unwrap();
            cw.typed::<$ty>().write_batch($vals, Some($defs), None).unwrap();
            cw.close().unwrap();
        }};
    }

    pub fn write_user_daily(
        path: &Path,
        rows: &[UDRow],
        names: &[Box<[u8]>],
        name_of_root: &[u32],
        cats: &[Box<[u8]>],
    ) {
        let msg = "
            message user_daily {
                REQUIRED BYTE_ARRAY user_id (UTF8);
                REQUIRED INT32 day (DATE);
                REQUIRED INT64 sessions;
                REQUIRED INT64 events;
                REQUIRED INT64 revenue_cents;
                OPTIONAL BYTE_ARRAY top_category (UTF8);
            }";
        let file = fs::File::create(path).expect("create parquet");
        let mut w = open(file, msg);
        let mut uid: Vec<ByteArray> = Vec::with_capacity(RG);
        let mut day: Vec<i32> = Vec::with_capacity(RG);
        let mut sess: Vec<i64> = Vec::with_capacity(RG);
        let mut evs: Vec<i64> = Vec::with_capacity(RG);
        let mut rev: Vec<i64> = Vec::with_capacity(RG);
        let mut top: Vec<ByteArray> = Vec::with_capacity(RG);
        let mut top_def: Vec<i16> = Vec::with_capacity(RG);
        for chunk in rows.chunks(RG) {
            uid.clear();
            day.clear();
            sess.clear();
            evs.clear();
            rev.clear();
            top.clear();
            top_def.clear();
            for r in chunk {
                uid.push(ByteArray::from(
                    names[name_of_root[r.root as usize] as usize].to_vec(),
                ));
                day.push(r.day);
                sess.push(r.sessions);
                evs.push(r.events);
                rev.push(r.revenue);
                if r.top >= 0 {
                    top_def.push(1);
                    top.push(ByteArray::from(cats[r.top as usize].to_vec()));
                } else {
                    top_def.push(0);
                }
            }
            let mut rg = w.next_row_group().expect("row group");
            col!(rg, ByteArrayType, &uid);
            col!(rg, Int32Type, &day);
            col!(rg, Int64Type, &sess);
            col!(rg, Int64Type, &evs);
            col!(rg, Int64Type, &rev);
            col_opt!(rg, ByteArrayType, &top, &top_def);
            rg.close().unwrap();
        }
        w.close().unwrap();
    }

    pub struct CampRow {
        pub id: String,
        pub channel: String,
        pub day: i32,
        pub sessions: i64,
        pub purchasing: i64,
        pub revenue: i64,
        pub assisted: i64,
    }

    pub fn write_campaign_daily(path: &Path, rows: &[CampRow]) {
        let msg = "
            message campaign_daily {
                REQUIRED BYTE_ARRAY campaign_id (UTF8);
                REQUIRED BYTE_ARRAY channel (UTF8);
                REQUIRED INT32 day (DATE);
                REQUIRED INT64 sessions;
                REQUIRED INT64 purchasing_sessions;
                REQUIRED INT64 revenue_cents;
                REQUIRED INT64 assisted_sessions;
            }";
        let file = fs::File::create(path).expect("create parquet");
        let mut w = open(file, msg);
        let mut cid: Vec<ByteArray> = Vec::with_capacity(RG);
        let mut chan: Vec<ByteArray> = Vec::with_capacity(RG);
        let mut day: Vec<i32> = Vec::with_capacity(RG);
        let mut sess: Vec<i64> = Vec::with_capacity(RG);
        let mut pur: Vec<i64> = Vec::with_capacity(RG);
        let mut rev: Vec<i64> = Vec::with_capacity(RG);
        let mut ass: Vec<i64> = Vec::with_capacity(RG);
        for chunk in rows.chunks(RG) {
            cid.clear();
            chan.clear();
            day.clear();
            sess.clear();
            pur.clear();
            rev.clear();
            ass.clear();
            for r in chunk {
                cid.push(ByteArray::from(r.id.as_bytes().to_vec()));
                chan.push(ByteArray::from(r.channel.as_bytes().to_vec()));
                day.push(r.day);
                sess.push(r.sessions);
                pur.push(r.purchasing);
                rev.push(r.revenue);
                ass.push(r.assisted);
            }
            let mut rg = w.next_row_group().expect("row group");
            col!(rg, ByteArrayType, &cid);
            col!(rg, ByteArrayType, &chan);
            col!(rg, Int32Type, &day);
            col!(rg, Int64Type, &sess);
            col!(rg, Int64Type, &pur);
            col!(rg, Int64Type, &rev);
            col!(rg, Int64Type, &ass);
            rg.close().unwrap();
        }
        w.close().unwrap();
    }
}

// ---------------------------------------------------------------- main

fn main() {
    let t0 = Instant::now();
    let data_dir = PathBuf::from(env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out_dir = PathBuf::from(env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let ncpu: usize = env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(8)
        });
    rayon::ThreadPoolBuilder::new()
        .num_threads(ncpu)
        .build_global()
        .unwrap();
    let log = |s: &str| eprintln!("[{:>10.2?}] {}", t0.elapsed(), s);

    // ---- manifest
    let manifest = fs::read_to_string(data_dir.join("manifest.json")).expect("manifest.json");
    let ws_s = json_str_field(&manifest, "window_start").expect("window_start");
    let we_s = json_str_field(&manifest, "window_end").expect("window_end");
    let ws = parse_ts(ws_s.as_bytes());
    let we = parse_ts(we_s.as_bytes());
    assert!(
        ws != INVALID_TS && we != INVALID_TS && we > ws,
        "bad manifest window"
    );
    log("manifest read");

    // ---- interners. Domain salts keep namespaces separate.
    let users = Interner::new();
    let products = Interner::new();
    let camps = Interner::new();
    let empty_user = users.intern(0, b"");

    // ---- campaigns.csv: campaign_id -> interned id 0..n; channel per id.
    let mut camp_channels: Vec<String> = Vec::new();
    {
        let path = data_dir.join("campaigns.csv");
        if let Ok(text) = fs::read(&path) {
            for_each_data_line(&text, |line| {
                if let Some(f) = split_fields::<2>(line) {
                    let id = camps.intern(2, f[0]);
                    if (id as usize) == camp_channels.len() {
                        camp_channels.push(String::from_utf8_lossy(f[1]).into_owned());
                    }
                }
            });
        }
    }
    let n_camp = camp_channels.len();
    let camp_names = camps.strings();
    let organic_id = n_camp as u16;
    log(&format!("{} campaigns", n_camp));

    // ---- products.csv: product_id -> interned id 0..n; category per id.
    let mut prod_cat: Vec<u32> = Vec::new(); // category index per known product
    let mut cat_names: Vec<Box<[u8]>> = Vec::new();
    {
        let path = data_dir.join("products.csv");
        if let Ok(text) = fs::read(&path) {
            for_each_data_line(&text, |line| {
                if let Some(f) = split_fields::<2>(line) {
                    let id = products.intern(1, f[0]);
                    if (id as usize) == prod_cat.len() {
                        let ci = match cat_names.iter().position(|c| &**c == f[1]) {
                            Some(i) => i as u32,
                            None => {
                                cat_names.push(Box::from(f[1]));
                                (cat_names.len() - 1) as u32
                            }
                        };
                        prod_cat.push(ci);
                    }
                }
            });
        }
    }
    let n_known_products = prod_cat.len();
    // Intern the empty product id AFTER products.csv so known ids stay 0..n.
    let empty_product = products.intern(1, b"");
    log(&format!(
        "{} products, {} categories",
        n_known_products,
        cat_names.len()
    ));

    // Category ranks: byte-wise sorted; "unknown" competes like any category.
    let mut categories: Vec<Box<[u8]>> = cat_names.clone();
    categories.push(Box::from(&b"unknown"[..]));
    categories.sort();
    let unknown_rank = categories
        .iter()
        .position(|c| &**c == b"unknown")
        .expect("unknown rank") as u32;
    let cat_rank: Vec<u32> = cat_names
        .iter()
        .map(|c| categories.iter().position(|x| &**x == &**c).unwrap() as u32)
        .collect();

    // ---- links: parse all files, collect id pairs.
    let mut link_pairs: Vec<(u32, u32)> = Vec::new();
    {
        let files = csv_files(&data_dir.join("links"));
        let chunks: Vec<Vec<(u32, u32)>> = files
            .iter()
            .flat_map(|p| {
                parse_file(p, |line, out, _| {
                    if let Some(f) = split_fields::<2>(line) {
                        if !f[0].is_empty() && !f[1].is_empty() {
                            let a = users.intern(0, f[0]);
                            let b = users.intern(0, f[1]);
                            out.push((a, b));
                        }
                    }
                })
            })
            .map(|(recs, _)| recs)
            .collect();
        for ch in chunks {
            link_pairs.extend(ch);
        }
    }
    log(&format!("{} link pairs", link_pairs.len()));

    // ---- events: parse all files.
    struct Chunk<T> {
        base: usize,
        recs: Vec<T>,
    }
    let event_files = csv_files(&data_dir.join("events"));
    let event_chunks: Vec<Chunk<ERec>> = {
        let mut offset = 0usize;
        let mut out = Vec::new();
        for p in &event_files {
            for (recs, _arena) in parse_file(p, |line, out, _| {
                let Some(f) = split_fields::<7>(line) else {
                    return;
                };
                if f[0].is_empty() {
                    return; // event_id empty -> dropped at parse
                }
                let ts = parse_ts(f[2]);
                let ingest = parse_ts(f[3]);
                if ingest == INVALID_TS {
                    return; // rules promise valid F1; be safe
                }
                let typ = match f[4] {
                    b"view" => 1u8,
                    b"cart" => 2,
                    b"purchase" => 3,
                    _ => 0,
                };
                let (price, price_ok) = parse_price(f[6]);
                let user = users.intern(0, f[1]);
                let product = products.intern(1, f[5]);
                out.push(ERec {
                    h: hash128(f[0], 0xEE),
                    ts,
                    ingest,
                    user,
                    product,
                    typ,
                    price_ok,
                    price,
                });
            }) {
                let base = offset;
                offset += recs.len();
                out.push(Chunk { base, recs });
            }
        }
        out
    };
    let n_events: usize = event_chunks
        .last()
        .map(|c| c.base + c.recs.len())
        .unwrap_or(0);
    log(&format!(
        "{} event rows from {} files",
        n_events,
        event_files.len()
    ));

    // ---- clicks: parse all files (click_id bytes kept in arenas for ties).
    let click_files = csv_files(&data_dir.join("clicks"));
    let mut click_arenas: Vec<Box<[u8]>> = Vec::new();
    let click_chunks: Vec<Chunk<CRec>> = {
        let mut offset = 0usize;
        let mut out = Vec::new();
        for p in &click_files {
            for (mut recs, arena) in parse_file(p, |line, out, arena| {
                let Some(f) = split_fields::<4>(line) else {
                    return;
                };
                if f[0].is_empty() {
                    return;
                }
                let ts = parse_ts(f[3]);
                let user = users.intern(0, f[1]);
                let camp = camps.intern(2, f[2]);
                let off = arena.len() as u32;
                arena.extend_from_slice(f[0]);
                out.push(CRec {
                    h: hash128(f[0], 0xCC),
                    ts,
                    user,
                    camp,
                    arena: 0,
                    off,
                    len: f[0].len() as u32,
                });
            }) {
                let ai = click_arenas.len() as u32;
                for r in recs.iter_mut() {
                    r.arena = ai;
                }
                let base = offset;
                offset += recs.len();
                click_arenas.push(arena.into_boxed_slice());
                out.push(Chunk { base, recs });
            }
        }
        out
    };
    let n_clicks: usize = click_chunks
        .last()
        .map(|c| c.base + c.recs.len())
        .unwrap_or(0);
    log(&format!(
        "{} click rows from {} files",
        n_clicks,
        click_files.len()
    ));

    // ---- union-find over every interned user id.
    let n_users = users.n();
    let mut p: Vec<u32> = (0..n_users as u32).collect();
    for &(a, b) in &link_pairs {
        let ra = find(&mut p, a);
        let rb = find(&mut p, b);
        if ra != rb {
            let (lo, hi) = if ra < rb { (ra, rb) } else { (rb, ra) };
            p[hi as usize] = lo;
        }
    }
    let roots: Vec<u32> = (0..n_users as u32).map(|i| find(&mut p, i)).collect();
    // Person name: byte-wise smallest id in the component.
    let user_strs = users.strings();
    let mut name_of_root: Vec<u32> = vec![u32::MAX; n_users];
    for i in 0..n_users {
        let r = roots[i] as usize;
        if name_of_root[r] == u32::MAX || user_strs[i] < user_strs[name_of_root[r] as usize] {
            name_of_root[r] = i as u32;
        }
    }
    log(&format!("{} user ids linked", n_users));

    // ---- product id -> category rank (known ids first, the rest unknown).
    let mut prod_rank: Vec<u32> = vec![unknown_rank; products.n()];
    for (i, r) in prod_rank.iter_mut().enumerate().take(n_known_products) {
        *r = cat_rank[prod_cat[i] as usize];
    }

    // ---- event dedup: keep the copy with the smallest ingest_ts.
    let dedup: DashMap<[u64; 2], (i64, u32), BuildHasherDefault<PreHasher>> =
        DashMap::with_capacity_and_hasher(n_events + 16, Default::default());
    event_chunks.par_iter().for_each(|c| {
        for (i, r) in c.recs.iter().enumerate() {
            let idx = (c.base + i) as u32;
            match dedup.entry(r.h) {
                Entry::Occupied(mut o) => {
                    let v = o.get_mut();
                    if r.ingest < v.0 {
                        *v = (r.ingest, idx);
                    }
                }
                Entry::Vacant(vac) => {
                    vac.insert((r.ingest, idx));
                }
            }
        }
    });
    log("event dedup map built");

    // ---- validate kept copies, map to persons, build sortable records.
    const DAY_MS: i64 = 86_400_000;
    const LATE_MS: i64 = 86_400_000;
    let mut ev2: Vec<Ev2> = event_chunks
        .par_iter()
        .flat_map_iter(|c| {
            c.recs.iter().enumerate().filter_map(|(i, r)| {
                let (ing, idx) = *dedup.get(&r.h)?;
                if idx as usize != c.base + i {
                    return None; // a smaller-ingest copy won
                }
                let _ = ing;
                if r.user == empty_user
                    || r.ts == INVALID_TS
                    || r.ts < ws
                    || r.ts >= we
                    || r.typ == 0
                    || r.product == empty_product
                    || (r.typ == 3 && !r.price_ok)
                    || r.ingest - r.ts > LATE_MS
                {
                    return None;
                }
                let root = roots[r.user as usize];
                let ts_rel = (r.ts - ws) as u64;
                debug_assert!(ts_rel < (1u64 << 31));
                let key = ((root as u64) << 31) | ts_rel;
                let purchase = r.typ == 3;
                Some(Ev2 {
                    key,
                    product: if purchase { r.product } else { 0 },
                    price: if purchase { r.price as i32 } else { 0 },
                })
            })
        })
        .collect();
    drop(dedup);
    drop(event_chunks);
    log(&format!("{} surviving events", ev2.len()));

    ev2.par_sort_unstable_by_key(|e| e.key);
    log("events sorted");

    // ---- sessionize + user_daily aggregation in one sequential pass.
    struct SessB {
        root: u32,
        start: i64,
        day: i32,
        last_ts: i64,
        count: i64,
        revenue: i64,
        has_p: bool,
    }
    struct Run {
        root: u32,
        day: i32,
        sessions: i64,
        events: i64,
        revenue: i64,
        cats: HashMap<u32, i64>,
        has_p: bool,
    }
    let mut sessions: Vec<Sess> = Vec::new();
    let mut ud_rows: Vec<UDRow> = Vec::new();
    let mut sb: Option<SessB> = None;
    let mut run: Option<Run> = None;

    macro_rules! close_session {
        ($s:expr) => {{
            let r = run.as_mut().unwrap();
            r.sessions += 1;
            r.events += $s.count;
            r.revenue += $s.revenue;
            r.has_p |= $s.has_p;
            sessions.push(Sess {
                root: $s.root,
                start: $s.start,
                day: $s.day,
                revenue: $s.revenue,
                has_p: $s.has_p,
                camp: organic_id,
            });
        }};
    }
    macro_rules! flush_run {
        () => {{
            let r = run.take().unwrap();
            let mut top: i32 = -1;
            if r.has_p {
                let mut best_v: i64 = i64::MIN;
                let mut best_k: u32 = 0;
                for (&k, &v) in &r.cats {
                    if v > best_v || (v == best_v && k < best_k) {
                        best_v = v;
                        best_k = k;
                    }
                }
                top = best_k as i32;
            }
            ud_rows.push(UDRow {
                root: r.root,
                day: r.day,
                sessions: r.sessions,
                events: r.events,
                revenue: r.revenue,
                top,
            });
        }};
    }

    for e in &ev2 {
        let root = (e.key >> 31) as u32;
        let ts = ws + (e.key & 0x7FFF_FFFF) as i64;
        let new_sess = match &sb {
            None => true,
            Some(s) => root != s.root || ts - s.last_ts > 1_800_000,
        };
        if new_sess {
            if let Some(s) = sb.take() {
                close_session!(s);
            }
            let day = (ts / DAY_MS) as i32;
            let need_flush = match &run {
                None => false,
                Some(r) => r.root != root || r.day != day,
            };
            if need_flush {
                flush_run!();
            }
            if run.is_none() {
                run = Some(Run {
                    root,
                    day,
                    sessions: 0,
                    events: 0,
                    revenue: 0,
                    cats: HashMap::new(),
                    has_p: false,
                });
            }
            sb = Some(SessB {
                root,
                start: ts,
                day,
                last_ts: ts,
                count: 0,
                revenue: 0,
                has_p: false,
            });
        }
        let s = sb.as_mut().unwrap();
        s.count += 1;
        s.last_ts = ts;
        if e.price != 0 {
            s.revenue += e.price as i64;
            s.has_p = true;
            let r = run.as_mut().unwrap();
            *r.cats.entry(prod_rank[e.product as usize]).or_insert(0) += e.price as i64;
        }
    }
    if let Some(s) = sb.take() {
        close_session!(s);
    }
    if run.is_some() {
        flush_run!();
    }
    drop(ev2);
    log(&format!(
        "{} sessions, {} user_daily rows",
        sessions.len(),
        ud_rows.len()
    ));

    // ---- clicks: dedup, validate, map to persons, sort per person.
    let cdedup: DashMap<[u64; 2], u32, BuildHasherDefault<PreHasher>> =
        DashMap::with_capacity_and_hasher(n_clicks + 16, Default::default());
    click_chunks.par_iter().for_each(|c| {
        for (i, r) in c.recs.iter().enumerate() {
            let idx = (c.base + i) as u32;
            cdedup.entry(r.h).or_insert(idx); // identical copies: keep any
        }
    });
    let click_win_start = ws - 7 * DAY_MS;
    let mut clicks: Vec<CFinal> = click_chunks
        .par_iter()
        .flat_map_iter(|c| {
            c.recs.iter().enumerate().filter_map(|(i, r)| {
                if *cdedup.get(&r.h)? != (c.base + i) as u32 {
                    return None;
                }
                if r.user == empty_user
                    || r.ts == INVALID_TS
                    || r.ts < click_win_start
                    || r.ts >= we
                    || r.camp as usize >= n_camp
                {
                    return None;
                }
                Some(CFinal {
                    root: roots[r.user as usize],
                    ts: r.ts,
                    camp: r.camp as u16,
                    arena: r.arena,
                    off: r.off,
                    len: r.len,
                })
            })
        })
        .collect();
    drop(cdedup);
    drop(click_chunks);
    log(&format!("{} valid clicks", clicks.len()));

    // Sort by (root asc, ts asc, click_id desc): the last click in the window
    // is then the credited one (latest ts, smallest click_id on ties).
    {
        let arenas = &click_arenas;
        clicks.par_sort_unstable_by(|a, b| {
            let ab = &arenas[a.arena as usize][a.off as usize..(a.off + a.len) as usize];
            let bb = &arenas[b.arena as usize][b.off as usize..(b.off + b.len) as usize];
            a.root
                .cmp(&b.root)
                .then(a.ts.cmp(&b.ts))
                .then_with(|| bb.cmp(ab))
        });
    }
    log("clicks sorted");

    // ---- attribution + campaign aggregation.
    let ws_day = (ws / DAY_MS) as i32;
    let we_day = (we / DAY_MS) as i32;
    let ndays = (we_day - ws_day + 1).max(1) as usize;
    let ncamp1 = n_camp + 1;
    let mut cred = vec![0i64; ncamp1 * ndays];
    let mut purc = vec![0i64; ncamp1 * ndays];
    let mut rev = vec![0i64; ncamp1 * ndays];
    let mut assi = vec![0i64; ncamp1 * ndays];

    // Per-root session ranges (sessions are already grouped by root).
    let mut s_ranges: Vec<(u32, usize, usize)> = Vec::new();
    for (i, s) in sessions.iter().enumerate() {
        match s_ranges.last_mut() {
            Some((r, _, e)) if *r == s.root => *e = i + 1,
            _ => s_ranges.push((s.root, i, i + 1)),
        }
    }
    let mut c_ranges: Vec<(u32, usize, usize)> = Vec::new();
    for (i, c) in clicks.iter().enumerate() {
        match c_ranges.last_mut() {
            Some((r, _, e)) if *r == c.root => *e = i + 1,
            _ => c_ranges.push((c.root, i, i + 1)),
        }
    }

    {
        let mut counts = vec![0u32; ncamp1];
        let mut in_active = vec![false; ncamp1];
        let mut seen_gen = vec![u32::MAX; ncamp1];
        let mut active: Vec<u16> = Vec::new();
        let seven = 7 * DAY_MS;
        let (mut i, mut j) = (0usize, 0usize);
        while i < s_ranges.len() && j < c_ranges.len() {
            let (sr, ss, se) = s_ranges[i];
            let (cr, cs, ce) = c_ranges[j];
            if sr < cr {
                i += 1;
                continue;
            }
            if cr < sr {
                j += 1;
                continue;
            }
            let mut l = cs;
            let mut r_in: isize = cs as isize - 1; // last click index entered, or cs-1
            for si in ss..se {
                let s = &mut sessions[si];
                let lo = s.start - seven;
                while ((r_in + 1) as usize) < ce && clicks[(r_in + 1) as usize].ts <= s.start {
                    r_in += 1;
                    let c = clicks[r_in as usize].camp as usize;
                    if counts[c] == 0 && !in_active[c] {
                        active.push(c as u16);
                        in_active[c] = true;
                    }
                    counts[c] += 1;
                }
                while (l as isize) <= r_in && clicks[l].ts < lo {
                    let c = clicks[l].camp as usize;
                    counts[c] -= 1;
                    if counts[c] == 0 {
                        in_active[c] = false;
                    }
                    l += 1;
                }
                let credited = if r_in >= 0
                    && (l as isize) <= r_in
                    && clicks[r_in as usize].ts >= lo
                {
                    clicks[r_in as usize].camp
                } else {
                    organic_id
                };
                s.camp = credited;
                if active.len() > 32 {
                    active.retain(|&c| counts[c as usize] > 0 && in_active[c as usize]);
                }
                let d = (s.day - ws_day) as usize;
                let tag = si as u32;
                for &c in &active {
                    let cu = c as usize;
                    if seen_gen[cu] == tag {
                        continue; // already counted this campaign for this session
                    }
                    seen_gen[cu] = tag;
                    if counts[cu] > 0 && in_active[cu] {
                        assi[cu * ndays + d] += 1;
                    }
                }
            }
            for k in cs..ce {
                let c = clicks[k].camp as usize;
                counts[c] = 0;
                in_active[c] = false;
            }
            active.clear();
            i += 1;
            j += 1;
        }
    }
    log("attribution done");

    // Credited aggregation over every session.
    for s in &sessions {
        let d = (s.day - ws_day) as usize;
        let base = s.camp as usize * ndays + d;
        cred[base] += 1;
        if s.has_p {
            purc[base] += 1;
            rev[base] += s.revenue;
        }
    }

    // ---- write outputs.
    let ud_dir = out_dir.join("user_daily");
    let cd_dir = out_dir.join("campaign_daily");
    fs::create_dir_all(&ud_dir).unwrap();
    fs::create_dir_all(&cd_dir).unwrap();
    pq::write_user_daily(
        &ud_dir.join("part-00000.parquet"),
        &ud_rows,
        &user_strs,
        &name_of_root,
        &categories,
    );
    log("user_daily written");

    let mut camp_rows: Vec<pq::CampRow> = Vec::new();
    for c in 0..ncamp1 {
        for d in 0..ndays {
            let idx = c * ndays + d;
            if cred[idx] > 0 || assi[idx] > 0 {
                let (id, channel) = if c == n_camp {
                    ("organic".to_string(), "organic".to_string())
                } else {
                    (
                        String::from_utf8_lossy(&camp_names[c]).into_owned(),
                        camp_channels[c].clone(),
                    )
                };
                camp_rows.push(pq::CampRow {
                    id,
                    channel,
                    day: ws_day + d as i32,
                    sessions: cred[idx],
                    purchasing: purc[idx],
                    revenue: rev[idx],
                    assisted: assi[idx],
                });
            }
        }
    }
    pq::write_campaign_daily(&cd_dir.join("part-00000.parquet"), &camp_rows);
    log(&format!(
        "done: {} user rows, {} campaign rows",
        ud_rows.len(),
        camp_rows.len()
    ));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_quoted_empties() {
        let line = b"e2hxzvxo7,ug4xemlz,2027-12-15T22:30:40Z,2027-12-15T22:38:24Z,view,\"\",\"\"";
        let f = split_fields::<7>(line).expect("should parse");
        assert_eq!(f[0], b"e2hxzvxo7".as_slice());
        assert_eq!(f[4], b"view".as_slice());
        assert_eq!(f[5], b"".as_slice());
        assert_eq!(f[6], b"".as_slice());
    }

    #[test]
    fn test_parse_ts_basic() {
        assert_eq!(parse_ts(b"2027-12-15T22:30:40Z"), 1828909840000);
        assert_eq!(parse_ts(b"1828909211000"), 1828909211000);
    }
}
