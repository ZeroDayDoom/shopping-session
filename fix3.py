s = open(r'src/main.rs').read()
start = s.index('    if std::env::var(')
end = s.index('    log!("events read");')
new = '''    if std::env::var("SHOP_BENCH").is_ok() && !esrc.chunks.is_empty() {
        let (fi, lo, hi) = *esrc.chunks.iter().max_by_key(|c| c.2 - c.1).unwrap();
        let data: &[u8] = &esrc.bufs[fi as usize];
        let data = &data[lo..hi];
        for mode in 0..=4u32 {
            for _ in 0..2 {
                let t = Instant::now();
                let rows = bench_chunk(data, lo, mode, &interner);
                let el = t.elapsed().as_nanos() as f64;
                eprintln!("BENCH mode={} {}B {} rows: {:.0}ms {:.0} ns/row", mode, data.len(), rows, el / 1e6, el / rows.max(1) as f64);
            }
        }
    }
'''
s = s[:start] + new + s[end:]
open(r'src/main.rs', 'w').write(s)
print('ok')
