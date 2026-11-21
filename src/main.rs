// Shopping Sessions pipeline (optimized).
//
// Reads $DATA_DIR, writes $OUTPUT_DIR/user_daily and $OUTPUT_DIR/campaign_daily
// as Parquet. Logs to stderr only.

mod csv;
mod pipeline;
mod pq;

use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use memchr::memchr;
use rayon::prelude::*;

use crate::csv::{find, hash128, json_str_field, parse_ts, PreMap, INVALID_TS};
use pipeline::*;

fn csv_files(dir: &PathBuf) -> Vec<PathBuf> {
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

/// Read every csv file in a folder in parallel; build chunk ranges split at \n.
struct Src {
    bufs: Vec<Vec<u8>>,
    chunks: Vec<(u32, usize, usize)>, // (file index, lo, hi)
}

fn build_src(dir: &PathBuf) -> Src {
    let files = csv_files(dir);
    let bufs: Vec<Vec<u8>> = files
        .par_iter()
        .map(|p| fs::read(p).unwrap_or_default())
        .collect();
    let mut chunks: Vec<(u32, usize, usize)> = Vec::new();
    for (fi, buf) in bufs.iter().enumerate() {
        let len = buf.len();
        if len == 0 {
            continue;
        }
        if len <= CHUNK {
            chunks.push((fi as u32, 0, len));
        } else {
            let mut start = 0usize;
            while start < len {
                let mut end = (start + CHUNK).min(len);
                if end < len {
                    match memchr(b'\n', &buf[end..]) {
                        Some(o) => end += o + 1,
                        None => end = len,
                    }
                }
                chunks.push((fi as u32, start, end));
                start = end;
            }
        }
    }
    Src { bufs, chunks }
}

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
    macro_rules! log {
        ($s:expr) => {
            eprintln!("[{:>10.2?}] {}", t0.elapsed(), $s)
        };
    }

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
    let ws_day = (ws / DAY_MS) as i32;
    let we_day = (we / DAY_MS) as i32;
    let ndays = (we_day - ws_day + 1).max(1) as usize;
    log!("manifest");

    // ---- campaigns (campaign_id -> index, in file order)
    let mut camp_channels: Vec<String> = Vec::new();
    let mut camp_names: Vec<Box<[u8]>> = Vec::new();
    let mut known_campaigns: PreMap<[u64; 2], u32> = csv::premap(4096);
    if let Ok(text) = fs::read(data_dir.join("campaigns.csv")) {
        let mut first = true;
        for line in text.split(|&c| c == b'\n') {
            if first {
                first = false;
                continue;
            }
            if line.is_empty() {
                continue;
            }
            if let Some(f) = csv::split_fields::<2>(line) {
                if f[0].is_empty() {
                    continue;
                }
                let h = hash128(f[0], 0xC3);
                if !known_campaigns.contains_key(&h) {
                    known_campaigns.insert(h, camp_channels.len() as u32);
                    camp_channels.push(String::from_utf8_lossy(f[1]).into_owned());
                    camp_names.push(Box::from(f[0]));
                }
            }
        }
    }
    let n_camp = camp_channels.len();
    let organic = n_camp as u16;
    let ncamp1 = n_camp + 1;
    log!("campaigns");

    // ---- products (product_id hash -> category rank; categories byte-sorted)
    let mut cat_names: Vec<Box<[u8]>> = Vec::new();
    let mut prod_cat: Vec<(usize, [u64; 2])> = Vec::new();
    if let Ok(text) = fs::read(data_dir.join("products.csv")) {
        let mut first = true;
        for line in text.split(|&c| c == b'\n') {
            if first {
                first = false;
                continue;
            }
            if line.is_empty() {
                continue;
            }
            if let Some(f) = csv::split_fields::<2>(line) {
                if f[0].is_empty() {
                    continue;
                }
                let ci = match cat_names.iter().position(|c| &**c == f[1]) {
                    Some(i) => i,
                    None => {
                        cat_names.push(Box::from(f[1]));
                        cat_names.len() - 1
                    }
                };
                prod_cat.push((ci, hash128(f[0], 0xB2)));
            }
        }
    }
    let mut categories: Vec<Box<[u8]>> = cat_names.clone();
    categories.push(Box::from(&b"unknown"[..]));
    categories.sort();
    let unknown_rank = categories
        .iter()
        .position(|c| &**c == b"unknown")
        .expect("unknown rank") as u32;
    let mut known_products: PreMap<[u64; 2], u32> = csv::premap(prod_cat.len() + 16);
    for (ci, h) in &prod_cat {
        let rank = categories
            .iter()
            .position(|x| &**x == &cat_names[*ci as usize][..])
            .unwrap() as u32;
        known_products.insert(*h, rank);
    }
    log!("products");

    // ---- global user table (hash -> dense gid), lock-free CAS inserts
    let users = UserTable::new(3_000_000);

    // ---- links (small; sequential)
    let mut link_arena: Vec<u8> = Vec::new();
    let mut link_names: HashMap<u64, (u32, u32, u32)> = HashMap::new();
    let mut link_pairs: Vec<(u32, u32)> = Vec::new();
    for p in csv_files(&data_dir.join("links")) {
        if let Ok(text) = fs::read(&p) {
            link_pairs.extend(parse_links(&text, &users, &mut link_arena, &mut link_names));
        }
    }
    log!("links parsed");

    // ---- event chunks (parallel parse into hash buckets)
    let esrc = build_src(&data_dir.join("events"));
    let parsed: Vec<(Vec<[u64; 2]>, Vec<u8>, HashMap<u64, (u32, u32, u32)>, Vec<Vec<ERec>>)> =
        esrc
            .chunks
            .par_iter()
            .enumerate()
            .map(|(seq, &(fi, lo, hi))| {
                let data: &[u8] = &esrc.bufs[fi as usize];
                let mut arena = Vec::new();
                let mut names = HashMap::new();
                let (side, b) =
                    parse_event_chunk(data, lo, hi, &users, &mut arena, &mut names);
                (side, arena, names, b)
            })
            .collect();
    let mut esides: Vec<Vec<[u64; 2]>> = Vec::with_capacity(parsed.len());
    let mut ebm: Vec<Vec<(usize, Vec<ERec>)>> = vec![Vec::new(); NB];
    let mut chunk_arenas: Vec<Vec<u8>> = Vec::with_capacity(parsed.len() + 1);
    let mut chunk_names: Vec<HashMap<u64, (u32, u32, u32)>> =
        Vec::with_capacity(parsed.len() + 1);
    for (seq, (side, arena, nm, per_bucket)) in parsed.into_iter().enumerate() {
        esides.push(side);
        chunk_arenas.push(arena);
        chunk_names.push(nm);
        for (b, chunk) in per_bucket.into_iter().enumerate() {
            if !chunk.is_empty() {
                ebm[b].push((seq, chunk));
            }
        }
    }
    log!("events parsed");

    // ---- click chunks
    let csrc = build_src(&data_dir.join("clicks"));
    let cparsed: Vec<(Vec<u8>, HashMap<u64, (u32, u32, u32)>, Vec<Vec<CRec>>)> = csrc
        .chunks
        .par_iter()
        .enumerate()
        .map(|(seq, &(fi, lo, hi))| {
            let data: &[u8] = &csrc.bufs[fi as usize];
            let mut arena = Vec::new();
            let mut names = HashMap::new();
            let (a, b) =
                parse_click_chunk(data, lo, hi, seq as u32, &known_campaigns, &users, &mut arena, &mut names);
            (a, names, b)
        })
        .collect();
    let mut carenas: Vec<Vec<u8>> = Vec::with_capacity(cparsed.len());
    let mut cbm: Vec<Vec<(usize, Vec<CRec>)>> = vec![Vec::new(); NB];
    for (seq, (arena, nm, per_bucket)) in cparsed.into_iter().enumerate() {
        carenas.push(arena);
        chunk_names.push(nm);
        for (b, chunk) in per_bucket.into_iter().enumerate() {
            if !chunk.is_empty() {
                cbm[b].push((seq, chunk));
            }
        }
    }
    let n_event_chunks = esides.len();
    let n_click_chunks = carenas.len();
    chunk_arenas.push(link_arena);
    chunk_names.push(link_names);
    let n_link_idx = chunk_names.len() - 1; // link entry index in the lists
    log!("clicks parsed");

    // ---- reconcile user names: gid -> name (first chunk that saw it wins)
    let n_ids = users.n();
    let mut name_slots: Vec<Option<Box<[u8]>>> = (0..n_ids).map(|_| None).collect();
    fn fill_names(
        slots: &mut Vec<Option<Box<[u8]>>>,
        nm: &HashMap<u64, (u32, u32, u32)>,
        arena: &[u8],
    ) {
        for (_h, (gid, off, len)) in nm {
            let gid = *gid as usize;
            if slots[gid].is_none() {
                slots[gid] = Some(Box::from(&arena[*off as usize..(*off + *len) as usize]));
            }
        }
    }
    for (ci, nm) in chunk_names.iter().take(n_event_chunks).enumerate() {
        fill_names(&mut name_slots, nm, &chunk_arenas[ci]);
    }
    for (ci, nm) in chunk_names.iter().skip(n_event_chunks).take(n_click_chunks).enumerate() {
        fill_names(&mut name_slots, nm, &carenas[ci]);
    }
    if let (Some(nm), Some(arena)) = (chunk_names.last(), chunk_arenas.last()) {
        fill_names(&mut name_slots, nm, arena);
    }
    let names: Vec<Box<[u8]>> = name_slots
        .into_iter()
        .map(|x| x.unwrap_or_else(|| Box::from(&b""[..])))
        .collect();
    log!("users reconciled");

    // ---- union-find over links (link pairs are gids)
    let mut p: Vec<u32> = (0..n_ids as u32).collect();
    {
        let mut flat: Vec<u32> = Vec::with_capacity(2 * link_pairs.len() + 16);
        for (a, b) in &link_pairs {
            flat.push(*a);
            flat.push(*b);
        }
        union_pairs(&flat, &mut p);
    }
    let root_of_user: Vec<u32> = (0..n_ids as u32).map(|i| find(&mut p, i)).collect();
    let uid_empty = users
        .get(hash64(b""))
        .unwrap_or(u32::MAX); // MAX = no empty-user row existed
    let mut name_of_root: Vec<u32> = vec![u32::MAX; n_ids];
    for i in 0..n_ids {
        let r = root_of_user[i] as usize;
        if name_of_root[r] == u32::MAX || names[i] < names[name_of_root[r] as usize] {
            name_of_root[r] = i as u32;
        }
    }
    log!("users linked");

    // ---- event dedup + validation -> Ev2
    let mut ev2 = dedup_events(
        ebm,
        &root_of_user,
        uid_empty,
        &esides,
        &known_products,
        unknown_rank,
        ws,
        we,
    );
    drop(esides);
    log!("events deduped");

    ev2.par_sort_unstable_by_key(|e| e.key);
    log!("events sorted");

    // ---- partition events into balanced groups at root boundaries
    let n_groups = (ncpu * 4).min(64).max(1);
    let mut bounds: Vec<usize> = vec![0];
    if !ev2.is_empty() {
        let per = (ev2.len() / n_groups).max(1);
        let mut next = per;
        for i in 1..ev2.len() {
            if (ev2[i].key >> 31) != (ev2[i - 1].key >> 31) && i >= next {
                bounds.push(i);
                next = i + per;
            }
        }
        bounds.push(ev2.len());
    } else {
        bounds.push(0);
    }
    let sess_spans: Vec<(usize, usize)> = bounds.windows(2).map(|w| (w[0], w[1])).collect();
    let (mut sess_parts, ud_parts): (Vec<Vec<Sess>>, Vec<Vec<UDRow>>) = sess_spans
        .par_iter()
        .map(|&(a, b)| {
            if b > a {
                let (s, u) = sessionize_group(&ev2[a..b], ws, organic, ndays);
                (s, u)
            } else {
                (Vec::new(), Vec::new())
            }
        })
        .unzip();
    drop(ev2);
    log!("sessions built");

    // ---- click dedup + validation + sort
    let mut clicks = dedup_clicks(cbm, &root_of_user, uid_empty, n_camp, ws, we);
    log!("clicks deduped");
    {
        let arenas = &carenas;
        clicks.par_sort_unstable_by(|a, b| {
            let ab = id_bytes(arenas, a);
            let bb = id_bytes(arenas, b);
            a.root
                .cmp(&b.root)
                .then(a.ts.cmp(&b.ts))
                .then_with(|| bb.cmp(ab))
        });
    }
    log!("clicks sorted");

    // ---- per-group attribution spans
    let mut c_ranges: Vec<(u32, usize, usize)> = Vec::new();
    for (i, c) in clicks.iter().enumerate() {
        match c_ranges.last_mut() {
            Some((r, _, e)) if *r == c.root => *e = i + 1,
            _ => c_ranges.push((c.root, i, i + 1)),
        }
    }
    let mut attr_spans: Vec<(usize, usize, usize, usize)> = Vec::with_capacity(sess_parts.len());
    for part in sess_parts.iter() {
        if part.is_empty() {
            attr_spans.push((0, 0, 0, 0));
            continue;
        }
        let first_root = part[0].root;
        let last_root = part[part.len() - 1].root;
        let e_lo = c_ranges.partition_point(|r| r.0 < first_root);
        let e_hi = c_ranges.partition_point(|r| r.0 <= last_root);
        let c_lo = if e_lo < c_ranges.len() { c_ranges[e_lo].1 } else { clicks.len() };
        let c_hi = if e_hi > e_lo { c_ranges[e_hi - 1].2 } else { c_lo };
        attr_spans.push((0, part.len(), c_lo, c_hi));
    }

    // ---- attribution + aggregates
    let aggs: Vec<Agg> = sess_parts
        .par_iter_mut()
        .zip(attr_spans.par_iter())
        .map(|(sess_slice, &(_n, _m, c_lo, c_hi))| {
            attribute_group(sess_slice, &clicks, c_lo, c_hi, organic, ncamp1, ndays, ws_day)
        })
        .collect();
    let mut agg = Agg::new(ncamp1, ndays);
    for a in &aggs {
        agg.merge(a);
    }
    drop(aggs);
    log!("attribution done");

    // ---- write outputs
    let ud_dir = out_dir.join("user_daily");
    let cd_dir = out_dir.join("campaign_daily");
    fs::create_dir_all(&ud_dir).unwrap();
    fs::create_dir_all(&cd_dir).unwrap();
    let n_ud_files = ud_parts.len().max(1);
    (0..n_ud_files).into_par_iter().for_each(|i| {
        let rows: &[UDRow] = ud_parts.get(i).map(|v| v.as_slice()).unwrap_or(&[]);
        pq::write_user_daily(
            &ud_dir.join(format!("part-{:05}.parquet", i)),
            rows,
            &names,
            &name_of_root,
            &categories,
        );
    });
    log!("user_daily written");

    let mut camp_rows: Vec<pq::CampRow> = Vec::new();
    for c in 0..ncamp1 {
        for d in 0..ndays {
            let idx = c * ndays + d;
            if agg.cred[idx] > 0 || agg.assi[idx] > 0 {
                let (id, channel) = if c == n_camp {
                    (ORGANIC.to_string(), ORGANIC.to_string())
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
                    sessions: agg.cred[idx],
                    purchasing: agg.purc[idx],
                    revenue: agg.rev[idx],
                    assisted: agg.assi[idx],
                });
            }
        }
    }
    pq::write_campaign_daily(&cd_dir.join("part-00000.parquet"), &camp_rows);
    log!(&format!(
        "done: {} user rows, {} campaign rows",
        ud_parts.iter().map(|p| p.len()).sum::<usize>(),
        camp_rows.len()
    ));
}
