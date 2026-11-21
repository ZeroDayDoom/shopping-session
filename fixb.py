s = open(r'src/pipeline.rs').read()

# unify: replace both identical names.entry blocks (event + click)
old = '''        let uid_h = hash64(f[1]);
        let uid = users.get_or_insert(uid_h);
        match names.entry(uid_h) {
            std::collections::hash_map::Entry::Vacant(v) => {
                let off = arena.len() as u32;
                arena.extend_from_slice(f[1]);
                v.insert((uid, off, f[1].len() as u32));
            }
            _ => {}
        }'''
new = '''        let uid_h = hash64(f[1]);
        let (uid, is_new) = users.get_or_insert(uid_h);
        if is_new {
            let off = arena.len() as u32;
            arena.extend_from_slice(f[1]);
            name_rec.push((uid, off, f[1].len() as u32));
        }'''
n = s.count(old)
assert n == 2, f'expected 2 occurrences, found {n}'
s = s.replace(old, new)

# signatures
old_sig_ev = '''    users: &UserTable,
    arena: &mut Vec<u8>,
    names: &mut PreMap<u64, (u32, u32, u32)>,
) -> (Vec<[u64; 2]>, Vec<Vec<ERec>>) {'''
new_sig_ev = '''    users: &UserTable,
    arena: &mut Vec<u8>,
    name_rec: &mut Vec<(u32, u32, u32)>,
) -> (Vec<[u64; 2]>, Vec<Vec<ERec>>) {'''
assert old_sig_ev in s
s = s.replace(old_sig_ev, new_sig_ev)

old_sig_cl = '''    known_campaigns: &PreMap<[u64; 2], u32>,
    users: &UserTable,
    arena: &mut Vec<u8>,
    names: &mut PreMap<u64, (u32, u32, u32)>,
) -> (Vec<u8>, Vec<Vec<CRec>>) {'''
new_sig_cl = '''    known_campaigns: &PreMap<[u64; 2], u32>,
    users: &UserTable,
    arena: &mut Vec<u8>,
    name_rec: &mut Vec<(u32, u32, u32)>,
) -> (Vec<u8>, Vec<Vec<CRec>>) {'''
assert old_sig_cl in s
s = s.replace(old_sig_cl, new_sig_cl)

# links: use (gid, is_new) from get_or_insert
old_links = '''        let a = users.get_or_insert(hash64(f[0]));
        let b = users.get_or_insert(hash64(f[1]));
        if let std::collections::hash_map::Entry::Vacant(v) = names.entry(ha) {
            let off = arena.len() as u32;
            arena.extend_from_slice(f[0]);
            v.insert((a, off, f[0].len() as u32));
        }
        if let std::collections::hash_map::Entry::Vacant(v) = names.entry(hb) {
            let off = arena.len() as u32;
            arena.extend_from_slice(f[1]);
            v.insert((b, off, f[1].len() as u32));
        }
        pairs.push((a, b));'''
new_links = '''        let ha = hash64(f[0]);
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
        pairs.push((a, b));'''
assert old_links in s, 'links block not found'
s = s.replace(old_links, new_links)

open(r'src/pipeline.rs', 'w').write(s)
print('ok')
