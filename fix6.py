s = open(r'src/main.rs').read()
start = s.index('    if std::env::var("SHOP_BENCH")')
end = s.index('    log!("events read");')
new = '''    if std::env::var("SHOP_BENCH").is_ok() && !esrc.chunks.is_empty() {
        let (fi, lo, hi) = *esrc.chunks.iter().max_by_key(|c| c.2 - c.1).unwrap();
        let data: &[u8] = &esrc.bufs[fi as usize];
        let data = &data[lo..hi];
        for _ in 0..3 {
            let mut arena = Vec::new();
            let mut names = HashMap::new();
            let t = Instant::now();
            let (side, b) = parse_event_chunk(data, lo, hi, 0, &users, &mut arena, &mut names);
            let rows: usize = b.iter().map(|v| v.len()).sum();
            eprintln!(
                "BENCH {}B {} rows: {:?} ({:.0} ns/row) side={}",
                data.len(), rows, t.elapsed(),
                t.elapsed().as_nanos() as f64 / rows.max(1) as f64, side.len()
            );
        }
    }
'''
s = s[:start] + new + s[end:]
open(r'src/main.rs', 'w').write(s)
print('ok')
