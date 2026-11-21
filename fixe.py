s = open(r'src/pipeline.rs').read()

# Replace BOTH names.entry blocks (event + click parses are identical here)
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
assert n == 2, f'expected 2, got {n}'
s = s.replace(old, new)
s = s.replace('            uid: uid as u64,', '            uid,')
s = s.replace('            uid: uid as u64,\n            camp,', '            uid: uid as u64,\n            camp,')
open(r'src/pipeline.rs', 'w').write(s)
print('ok')
