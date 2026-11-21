// Core pipeline: records, chunk parsing into hash buckets, bucket-local dedup
// with open-addressing maps, sessionization and attribution passes.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use memchr::memchr;
use rayon::prelude::*;

use std::collections::HashMap;

use crate::csv::{find, hash128, parse_price, parse_ts, PreMap, INVALID_TS};

pub const NB: usize = 128; // hash buckets (power of two)
pub const CHUNK: usize = 32 << 20; // chunk target

pub const DAY_MS: i64 = 86_400_000;
pub const LATE_MS: i64 = 86_400_000;
pub const SEVEN: i64 = 7 * DAY_MS;
pub const GAP: i64 = 1_800_000;

pub const PROD_EMPTY: u8 = 0x80;

pub const ORGANIC: &str = "organic";

// ---------------------------------------------------------------- records

#[derive(Clone)]
pub struct ERec {
    pub h: [u64; 2], // event_id hash (dedup key)
    pub ts: i64,     // parsed or INVALID_TS
    pub ingest: i64,
    pub uid: u64,  // global user id (dense)
    pub pidx: u32, // product-hash slot in the chunk's side vec (MAX if none)
    pub price: i32,
    pub flags: u8, // bits 0-2 type (1 view, 2 cart, 3 purchase), bit 7 product empty
}

#[derive(Clone, Copy)]
pub struct Ev2 {
    pub key: u64, // root << 31 | (ts - window_start)  (< 2^31 for <= 24 day windows)
    pub val: u64, // (category rank << 32) | price  (price > 0 only for purchases)
}

#[derive(Clone)]
pub struct CRec {
    pub h: [u64; 2], // click_id hash (dedup key)
    pub ts: i64,
    pub uid: u64, // global user id (dense)
    pub camp: u32, // campaign index or u32::MAX if unknown
    pub seq: u32,  // click chunk seq (arena for the click_id bytes)
    pub off: u32,
    pub len: u32,
}

#[derive(Clone, Copy)]
pub struct CFinal {
    pub root: u32,
    pub ts: i64,
    pub camp: u16,
    pub arena: u32, // chunk seq whose arena holds the click_id bytes
    pub off: u32,
    pub len: u32,
}

#[derive(Clone, Copy)]
pub struct Sess {
    pub root: u32,
    pub start: i64,
    pub day: i32, // days since epoch of the session start
    pub revenue: i64,
    pub has_p: bool,
    pub camp: u16, // credited campaign, set during attribution
}

pub struct UDRow {
    pub root: u32,
    pub day: i32,
    pub sessions: i64,
    pub events: i64,
    pub revenue: i64,
    pub top: i32, // category rank, -1 = null
}

// ---------------------------------------------------------------- users

/// Flat lock-free user table: 64-bit hash -> dense gid, where the gid IS the
/// slot index (stable and race-free: CAS claims the key, the id is the slot).
/// Unused slots are their own components and never appear in rows.
pub struct UserTable {
    keys: Vec<AtomicU64>, // 0 = empty slot
    mask: usize,
}

impl UserTable {
    pub fn new(cap_hint: usize) -> Self {
        let slots = (cap_hint * 2).max(1024).next_power_of_two();
        UserTable {
            keys: (0..slots).map(|_| AtomicU64::new(0)).collect(),
            mask: slots - 1,
        }
    }

    #[inline]
    pub fn get(&self, h: u64) -> Option<u32> {
        let mut i = (h as usize) & self.mask;
        loop {
            let k = self.keys[i].load(Ordering::Relaxed);
            if k == 0 {
                return None;
            }
            if k == h {
                return Some(i as u32);
            }
            i = (i + 1) & self.mask;
        }
    }

    /// Insert h if absent. Returns (gid, is_new); gid == slot index.
    pub fn get_or_insert(&self, h: u64) -> (u32, bool) {
        let mut i = (h as usize) & self.mask;
        loop {
            let s = &self.keys[i];
            let k = s.load(Ordering::Relaxed);
            if k == h {
                return (i as u32, false);
            }
            if k == 0 {
                match s.compare_exchange(0, h, Ordering::Relaxed, Ordering::Relaxed) {
                    Ok(_) => return (i as u32, true),
                    Err(cur) => {
                        if cur == h {
                            return (i as u32, false);
                        }
                        i = (i + 1) & self.mask;
                    }
                }
                continue;
            }
            i = (i + 1) & self.mask;
        }
    }

    pub fn n(&self) -> usize {
        self.keys.len()
    }
}

/// 64-bit user-id hash (collision odds ~1e-7 at millions of users).
#[inline]
pub fn hash64(b: &[u8]) -> u64 {
    crate::csv::mix(crate::csv::hash128(b, 0xA1)[0])
}

// ---------------------------------------------------------------- parse

/// Per-source-chunk parse of an event file chunk. Returns the chunk's side
/// vec of product hashes and its bucket vecs.
pub fn parse_event_chunk(
    data: &[u8],
    lo: usize,
    hi: usize,
    users: &UserTable,
    arena: &mut Vec<u8>,
    name_rec: &mut Vec<(u32, u32, u32)>,
) -> (Vec<[u64; 2]>, Vec<Vec<ERec>>) {
    let est_rows = (hi - lo) / 80 + 64;
    let mut side: Vec<[u64; 2]> = Vec::new();
    let mut buckets: Vec<Vec<ERec>> = Vec::with_capacity(NB);
    let per_bucket = est_rows / NB + 64;
    for _ in 0..NB {
        buckets.push(Vec::with_capacity(per_bucket));
    }
    let mask = (NB - 1) as usize;

    let mut start = 0usize;
    let mut first = lo == 0; // the file's first line is the header
    while start < data.len() {
        let nl = match memchr(b'\n', &data[start..]) {
            Some(p) => start + p,
            None => data.len(),
        };
        let line = &data[start..nl];
        start = nl + 1;
        if first {
            first = false;
            continue;
        }
        if line.is_empty() {
            continue;
        }
        let Some(f) = split7(line) else { continue };
        if f[0].is_empty() {
            continue; // event_id empty -> dropped at parse
        }
        let ts = parse_ts(f[2]);
        let ingest = parse_ts(f[3]);
        if ingest == INVALID_TS {
            continue; // rules promise valid F1; be safe
        }
        let typ: u8 = match f[4] {
            b"view" => 1,
            b"cart" => 2,
            b"purchase" => 3,
            _ => 0,
        };
        let uid_h = hash64(f[1]);
        let (uid, is_new) = users.get_or_insert(uid_h);
        if is_new {
            let off = arena.len() as u32;
            arena.extend_from_slice(f[1]);
            name_rec.push((uid, off, f[1].len() as u32));
        }
        let mut flags = typ;
        let mut pidx = u32::MAX;
        let mut price: i32 = 0;
        if f[5].is_empty() {
            flags |= PROD_EMPTY;
        } else if typ == 3 {
            let (v, ok) = parse_price(f[6]);
            if ok {
                price = v as i32;
            }
            pidx = side.len() as u32;
            side.push(hash128(f[5], 0xB2));
        }
        let h = hash128(f[0], 0xEE);
        let rec = ERec {
            h,
            ts,
            ingest,
            uid: uid as u64,
            pidx,
            price,
            flags,
        };
        buckets[(h[0] as usize) & mask].push(rec);
    }
    (side, buckets)
}

/// Click chunk parse. Same shape as events.
pub fn parse_click_chunk(
    data: &[u8],
    lo: usize,
    hi: usize,
    seq: u32,
    known_campaigns: &PreMap<[u64; 2], u32>,
    users: &UserTable,
    arena: &mut Vec<u8>,
    name_rec: &mut Vec<(u32, u32, u32)>,
) -> (Vec<u8>, Vec<Vec<CRec>>) {
    let est_rows = (hi - lo) / 45 + 64;
    let mut arena: Vec<u8> = Vec::with_capacity((hi - lo) / 4);
    let mut buckets: Vec<Vec<CRec>> = Vec::with_capacity(NB);
    let per_bucket = est_rows / NB + 64;
    for _ in 0..NB {
        buckets.push(Vec::with_capacity(per_bucket));
    }
    let mask = (NB - 1) as usize;

    let mut start = 0usize;
    let mut first = lo == 0;
    while start < data.len() {
        let nl = match memchr(b'\n', &data[start..]) {
            Some(p) => start + p,
            None => data.len(),
        };
        let line = &data[start..nl];
        start = nl + 1;
        if first {
            first = false;
            continue;
        }
        if line.is_empty() {
            continue;
        }
        let Some(f) = split4(line) else { continue };
        if f[0].is_empty() {
            continue;
        }
        let ts = parse_ts(f[3]);
        let uid_h = hash64(f[1]);
        let (uid, is_new) = users.get_or_insert(uid_h);
        if is_new {
            let off = arena.len() as u32;
            arena.extend_from_slice(f[1]);
            name_rec.push((uid, off, f[1].len() as u32));
        }
        let camp = known_campaigns
            .get(&hash128(f[2], 0xC3))
            .copied()
            .unwrap_or(u32::MAX);
        let h = hash128(f[0], 0xDD);
        let off = arena.len() as u32;
        arena.extend_from_slice(f[0]);
        let rec = CRec {
            h,
            ts,
            uid: uid as u64,
            camp,
            seq,
            off,
            len: f[0].len() as u32,
        };
        buckets[(h[0] as usize) & mask].push(rec);
    }
    (arena, buckets)
}

/// Links parse (small; sequential). Returns pairs of user gids.
pub fn parse_links(
    data: &[u8],
    users: &UserTable,
    arena: &mut Vec<u8>,
    name_rec: &mut Vec<(u32, u32, u32)>,
) -> Vec<(u32, u32)> {
    let mut pairs = Vec::new();
    let mut first = true;
    for line in data.split(|&c| c == b'\n') {
        if first {
            first = false;
            continue;
        }
        if line.is_empty() {
            continue;
        }
        let Some(f) = split2(line) else { continue };
        if f[0].is_empty() || f[1].is_empty() {
            continue;
        }
        let ha = hash64(f[0]);
        let hb = hash64(f[1]);
        let (a, new_a) = users.get_or_insert(ha);
        let (b, new_b) = users.get_or_insert(hb);
        if new_a {
            let off = arena.len() as u32;
            arena.extend_from_slice(f[0]);
            name_rec.push((a, off, f[0].len() as u32));
        }
        if new_b {
            let off = arena.len() as u32;
            arena.extend_from_slice(f[1]);
            name_rec.push((b, off, f[1].len() as u32));
        }
        pairs.push((a, b));
    }
    pairs
}

#[inline]
fn split7(line: &[u8]) -> Option<[&[u8]; 7]> {
    crate::csv::split_fields::<7>(line)
}
#[inline]
fn split4(line: &[u8]) -> Option<[&[u8]; 4]> {
    crate::csv::split_fields::<4>(line)
}
#[inline]
fn split2(line: &[u8]) -> Option<[&[u8]; 2]> {
    crate::csv::split_fields::<2>(line)
}

// ------------------------------------------------------------- dedup maps

/// Open-addressing dedup map for event rows: keeps the smallest ingest.
#[derive(Clone, Copy)]
struct ESlot {
    key: [u64; 2],
    ingest: i64,
    wkey: u64,
}

struct EDedup {
    slots: Vec<ESlot>,
    mask: usize,
}

impl EDedup {
    fn new(rows: usize) -> Self {
        let slots = (rows + (rows >> 1)).max(64).next_power_of_two();
        EDedup {
            slots: vec![ESlot { key: [0, 0], ingest: 0, wkey: 0 }; slots],
            mask: slots - 1,
        }
    }
    #[inline]
    fn upsert(&mut self, h: [u64; 2], ingest: i64, wkey: u64) {
        let mut i = (h[0] as usize) & self.mask;
        loop {
            let k = self.slots[i].key;
            if k == [0, 0] {
                self.slots[i] = ESlot { key: h, ingest, wkey };
                return;
            }
            if k == h {
                if ingest < self.slots[i].ingest {
                    self.slots[i].ingest = ingest;
                    self.slots[i].wkey = wkey;
                }
                return;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline]
    fn winner(&self, h: [u64; 2]) -> Option<u64> {
        let mut i = (h[0] as usize) & self.mask;
        loop {
            let k = self.slots[i].key;
            if k == [0, 0] {
                return None;
            }
            if k == h {
                return Some(self.slots[i].wkey);
            }
            i = (i + 1) & self.mask;
        }
    }
}

/// Open-addressing dedup map for click rows: keeps the first insert.
#[derive(Clone, Copy)]
struct CSlot {
    key: [u64; 2],
    wkey: u64,
}

struct CDedup {
    slots: Vec<CSlot>,
    mask: usize,
}

impl CDedup {
    fn new(rows: usize) -> Self {
        let slots = (rows + (rows >> 1)).max(64).next_power_of_two();
        CDedup {
            slots: vec![CSlot { key: [0, 0], wkey: 0 }; slots],
            mask: slots - 1,
        }
    }
    #[inline]
    fn insert_first(&mut self, h: [u64; 2], wkey: u64) {
        let mut i = (h[0] as usize) & self.mask;
        loop {
            let k = self.slots[i].key;
            if k == [0, 0] {
                self.slots[i] = CSlot { key: h, wkey };
                return;
            }
            if k == h {
                return;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline]
    fn winner(&self, h: [u64; 2]) -> Option<u64> {
        let mut i = (h[0] as usize) & self.mask;
        loop {
            let k = self.slots[i].key;
            if k == [0, 0] {
                return None;
            }
            if k == h {
                return Some(self.slots[i].wkey);
            }
            i = (i + 1) & self.mask;
        }
    }
}

// ---------------------------------------------------------------- dedup

/// Bucket-local event dedup + validation. Emits Ev2 records.
#[allow(clippy::too_many_arguments)]
pub fn dedup_events(
    ebuckets: Vec<Vec<(usize, Vec<ERec>)>>,
    root_of_user: &[u32],
    uid_empty: u32,
    sides: &[Vec<[u64; 2]>],
    known_products: &PreMap<[u64; 2], u32>,
    unknown_rank: u32,
    ws: i64,
    we: i64,
) -> Vec<Ev2> {
    ebuckets
        .into_par_iter()
        .map(|chunks| {
            let total: usize = chunks.iter().map(|c| c.1.len()).sum();
            let mut map = EDedup::new(total);
            for (ci, (_seq, chunk)) in chunks.iter().enumerate() {
                for (i, r) in chunk.iter().enumerate() {
                    let wkey = ((ci as u64) << 32) | i as u64;
                    map.upsert(r.h, r.ingest, wkey);
                }
            }
            let mut out: Vec<Ev2> = Vec::with_capacity(total * 9 / 10 + 16);
            for (ci, (seq, chunk)) in chunks.iter().enumerate() {
                for (i, r) in chunk.iter().enumerate() {
                    if map.winner(r.h) != Some(((ci as u64) << 32) | i as u64) {
                        continue; // a smaller-ingest copy won
                    }
                    let typ = r.flags & 0x07;
                    if r.ts == INVALID_TS || r.ts < ws || r.ts >= we {
                        continue;
                    }
                    if typ == 0 || r.flags & PROD_EMPTY != 0 {
                        continue;
                    }
                    if typ == 3 && r.price <= 0 {
                        continue;
                    }
                    if r.ingest - r.ts > LATE_MS {
                        continue;
                    }
                    if r.uid as usize == uid_empty as usize {
                        continue; // empty user
                    }
                    let root = root_of_user[r.uid as usize];
                    let ts_rel = (r.ts - ws) as u64;
                    let prank = if typ == 3 {
                        let ph = sides[*seq][r.pidx as usize];
                        known_products.get(&ph).copied().unwrap_or(unknown_rank)
                    } else {
                        0
                    };
                    out.push(Ev2 {
                        key: ((root as u64) << 31) | ts_rel,
                        val: ((prank as u64) << 32) | (r.price as u32 as u64),
                    });
                }
            }
            out
        })
        .collect::<Vec<Vec<Ev2>>>()
        .into_iter()
        .flatten()
        .collect()
}

/// Bucket-local click dedup (keep any copy) + validation.
pub fn dedup_clicks(
    cbuckets: Vec<Vec<(usize, Vec<CRec>)>>,
    root_of_user: &[u32],
    uid_empty: u32,
    known_max: usize,
    ws: i64,
    we: i64,
) -> Vec<CFinal> {
    cbuckets
        .into_par_iter()
        .map(|chunks| {
            let total: usize = chunks.iter().map(|c| c.1.len()).sum();
            let mut map = CDedup::new(total);
            for (ci, (_seq, chunk)) in chunks.iter().enumerate() {
                for (i, r) in chunk.iter().enumerate() {
                    let wkey = ((ci as u64) << 32) | i as u64;
                    map.insert_first(r.h, wkey);
                }
            }
            let mut out: Vec<CFinal> = Vec::with_capacity(total * 9 / 10 + 16);
            for (ci, (_seq, chunk)) in chunks.iter().enumerate() {
                for (i, r) in chunk.iter().enumerate() {
                    if map.winner(r.h) != Some(((ci as u64) << 32) | i as u64) {
                        continue;
                    }
                    if r.uid as usize == uid_empty as usize
                        || r.ts == INVALID_TS
                        || r.ts < ws - SEVEN
                        || r.ts >= we
                        || r.camp as usize >= known_max
                    {
                        continue;
                    }
                    out.push(CFinal {
                        root: root_of_user[r.uid as usize],
                        ts: r.ts,
                        camp: r.camp as u16,
                        arena: r.seq,
                        off: r.off,
                        len: r.len,
                    });
                }
            }
            out
        })
        .collect::<Vec<Vec<CFinal>>>()
        .into_iter()
        .flatten()
        .collect()
}

// ---------------------------------------------------------------- sessions

/// Sessionize one contiguous run of events whose first element starts a fresh
/// root (group boundaries are root boundaries). Also aggregates user_daily rows.
pub fn sessionize_group(
    ev: &[Ev2],
    ws: i64,
    organic: u16,
    _ndays: usize,
) -> (Vec<Sess>, Vec<UDRow>) {
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
        cats: PreMap<u32, i64>,
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
                camp: organic,
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

    for e in ev {
        let root = (e.key >> 31) as u32;
        let ts = ws + (e.key & 0x7FFF_FFFF) as i64;
        let price = (e.val & 0xFFFF_FFFF) as i32;
        let rank = (e.val >> 32) as u32;
        let new_sess = match &sb {
            None => true,
            Some(s) => root != s.root || ts - s.last_ts > GAP,
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
                    cats: crate::csv::premap(16),
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
        if price != 0 {
            s.revenue += price as i64;
            s.has_p = true;
            let r = run.as_mut().unwrap();
            *r.cats.entry(rank).or_insert(0) += price as i64;
        }
    }
    if let Some(s) = sb.take() {
        close_session!(s);
    }
    if run.is_some() {
        flush_run!();
    }
    (sessions, ud_rows)
}

// ------------------------------------------------------------ attribution

#[derive(Clone)]
pub struct Agg {
    pub cred: Vec<i64>,
    pub purc: Vec<i64>,
    pub rev: Vec<i64>,
    pub assi: Vec<i64>,
}

impl Agg {
    pub fn new(ncamp1: usize, ndays: usize) -> Self {
        let n = ncamp1 * ndays;
        Agg {
            cred: vec![0; n],
            purc: vec![0; n],
            rev: vec![0; n],
            assi: vec![0; n],
        }
    }
    pub fn merge(&mut self, other: &Agg) {
        for k in 0..self.cred.len() {
            self.cred[k] += other.cred[k];
            self.purc[k] += other.purc[k];
            self.rev[k] += other.rev[k];
            self.assi[k] += other.assi[k];
        }
    }
}

/// One group's attribution: two-pointer over the person's clicks, credited
/// campaign per session, assists, and the credited aggregation.
#[allow(clippy::too_many_arguments)]
pub fn attribute_group(
    sessions: &mut [Sess],
    clicks: &[CFinal],
    c_lo: usize,
    c_hi: usize,
    organic: u16,
    ncamp1: usize,
    ndays: usize,
    ws_day: i32,
) -> Agg {
    let mut agg = Agg::new(ncamp1, ndays);
    let mut s_ranges: Vec<(u32, usize, usize)> = Vec::new();
    for (i, s) in sessions.iter().enumerate() {
        match s_ranges.last_mut() {
            Some((r, _, e)) if *r == s.root => *e = i + 1,
            _ => s_ranges.push((s.root, i, i + 1)),
        }
    }
    let mut c_ranges: Vec<(u32, usize, usize)> = Vec::new();
    for (i, c) in clicks[c_lo..c_hi].iter().enumerate() {
        match c_ranges.last_mut() {
            Some((r, _, e)) if *r == c.root => *e = i + 1 + c_lo,
            _ => c_ranges.push((c.root, i + c_lo, i + 1 + c_lo)),
        }
    }

    let mut counts = vec![0u32; ncamp1];
    let mut in_active = vec![false; ncamp1];
    let mut seen_gen = vec![u32::MAX; ncamp1];
    let mut active: Vec<u16> = Vec::new();

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
        let mut r_in: isize = cs as isize - 1;
        for si in ss..se {
            let s = &mut sessions[si];
            let lo = s.start - SEVEN;
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
                organic
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
                    continue;
                }
                seen_gen[cu] = tag;
                if counts[cu] > 0 && in_active[cu] {
                    agg.assi[cu * ndays + d] += 1;
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

    for s in sessions.iter() {
        let d = (s.day - ws_day) as usize;
        let base = s.camp as usize * ndays + d;
        agg.cred[base] += 1;
        if s.has_p {
            agg.purc[base] += 1;
            agg.rev[base] += s.revenue;
        }
    }
    agg
}

/// click_id bytes for tie-break comparisons.
#[inline]
pub fn id_bytes<'a>(arenas: &'a [Vec<u8>], c: &CFinal) -> &'a [u8] {
    &arenas[c.arena as usize][c.off as usize..(c.off + c.len) as usize]
}

pub fn union_pairs(pairs: &[u32], p: &mut [u32]) {
    for ab in pairs.chunks(2) {
        let ra = find(p, ab[0]);
        let rb = find(p, ab[1]);
        if ra != rb {
            let (lo, hi) = if ra < rb { (ra, rb) } else { (rb, ra) };
            p[hi as usize] = lo;
        }
    }
}
