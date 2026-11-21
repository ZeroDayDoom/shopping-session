s = open(r'src/main.rs').read()
start = s.index('    // ---- reconcile user names')
end = s.index('    let names: Vec<Box<[u8]>> = name_slots')
new = '''    // ---- reconcile user names: gid -> name (first chunk that saw it wins)
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
'''
s = s[:start] + new + s[end:]
open(r'src/main.rs', 'w').write(s)
print('ok')
