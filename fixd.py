m = open(r'src/main.rs').read()

# event chunks call site
old = '''            let mut arena = Vec::new();
            let mut names = HashMap::new();
            let (side, b) =
                parse_event_chunk(data, lo, hi, &users, &mut arena, &mut names);
            (side, arena, names, b)'''
new = '''            let mut arena = Vec::new();
            let mut name_rec = Vec::new();
            let (side, b) =
                parse_event_chunk(data, lo, hi, &users, &mut arena, &mut name_rec);
            (side, arena, name_rec, b)'''
assert old in m, 'ev call'
m = m.replace(old, new)

# click chunks call site
old = '''            let mut arena = Vec::new();
            let mut names = csv::premap(4096);
            let (a, b) =
                parse_click_chunk(data, lo, hi, seq as u32, &known_campaigns, &users, &mut arena, &mut names);
            (a, names, b)'''
new = '''            let mut arena = Vec::new();
            let mut name_rec = Vec::new();
            let (a, b) =
                parse_click_chunk(data, lo, hi, seq as u32, &known_campaigns, &users, &mut arena, &mut name_rec);
            (a, name_rec, b)'''
assert old in m, 'cl call'
m = m.replace(old, new)

# links call site
old = '''            link_pairs
                .extend(parse_links(&text, &users, &mut link_arena, &mut link_names));'''
new = '''            link_pairs
                .extend(parse_links(&text, &users, &mut link_arena, &mut link_name_rec));'''
assert old in m, 'links call'
m = m.replace(old, new)
m = m.replace(
    'let mut link_names: PreMap<u64, (u32, u32, u32)> = csv::premap(1024);',
    'let mut link_name_rec: Vec<(u32, u32, u32)> = Vec::new();')

# reconciliation: change fill_names to take triples slice
old = '''    fn fill_names(
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
    }'''
new = '''    fn fill_names(
        slots: &mut Vec<Option<Box<[u8]>>>,
        rec: &[(u32, u32, u32)],
        arena: &[u8],
    ) {
        for (gid, off, len) in rec {
            let gid = *gid as usize;
            if slots[gid].is_none() {
                slots[gid] = Some(Box::from(&arena[*off as usize..(*off + *len) as usize]));
            }
        }
    }
    for (ci, rec) in chunk_names.iter().take(n_event_chunks).enumerate() {
        fill_names(&mut name_slots, rec, &chunk_arenas[ci]);
    }
    for (ci, rec) in chunk_names.iter().skip(n_event_chunks).take(n_click_chunks).enumerate() {
        fill_names(&mut name_slots, rec, &carenas[ci]);
    }
    if let (Some(rec), Some(arena)) = (chunk_names.last(), chunk_arenas.last()) {
        fill_names(&mut name_slots, rec, arena);
    }'''
assert old in m, 'reconcile'
m = m.replace(old, new)

# types of chunk_names vec
m = m.replace(
    'let mut chunk_names: Vec<HashMap<u64, (u32, u32, u32)>> =',
    'let mut chunk_names: Vec<Vec<(u32, u32, u32)>> =')

open(r'src/main.rs', 'w').write(m)
print('ok')
