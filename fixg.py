s = open(r'src/pipeline.rs').read()
s = s.replace('''        let rec = ERec {
            h,
            ts,
            ingest,
            uid,
            pidx,
            price,
            flags,
        };''', '''        let rec = ERec {
            h,
            ts,
            ingest,
            uid: uid as u64,
            pidx,
            price,
            flags,
        };''')
s = s.replace('''        let rec = CRec {
            h,
            ts,
            uid,
            camp,''', '''        let rec = CRec {
            h,
            ts,
            uid: uid as u64,
            camp,''')
open(r'src/pipeline.rs', 'w').write(s)

m = open(r'src/main.rs').read()
m = m.replace('parse_links(&text, &users, &mut link_arena, &mut link_names)', 'parse_links(&text, &users, &mut link_arena, &mut link_name_rec)')
m = m.replace('let mut link_names: PreMap<u64, (u32, u32, u32)> = csv::premap(1024);', 'let mut link_name_rec: Vec<(u32, u32, u32)> = Vec::new();')
m = m.replace('parse_event_chunk(data, lo, hi, &users, &mut arena, &mut names);', 'parse_event_chunk(data, lo, hi, &users, &mut arena, &mut name_rec);')
m = m.replace('parse_click_chunk(data, lo, hi, seq as u32, &known_campaigns, &users, &mut arena, &mut names);', 'parse_click_chunk(data, lo, hi, seq as u32, &known_campaigns, &users, &mut arena, &mut name_rec);')
m = m.replace('(side, arena, names, b)', '(side, arena, name_rec, b)')
m = m.replace('(a, names, b)', '(a, name_rec, b)')
open(r'src/main.rs', 'w').write(m)
print('ok')
