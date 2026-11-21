s = open(r'src/pipeline.rs').read()

old = '''        let ha = hash64(f[0]);
        let hb = hash64(f[1]);
        let a = users.get_or_insert(ha);
        let b = users.get_or_insert(hb);
        {
            let is_new_a = !name_rec.iter().any(|r| r.0 == a);
            let is_new_b = !name_rec.iter().any(|r| r.0 == b);
            let _ = (is_new_a, is_new_b);
        }
        if !name_rec.iter().any(|r| r.0 == a) {
            let off = arena.len() as u32;
            arena.extend_from_slice(f[0]);
            name_rec.push((a, off, f[0].len() as u32));
        }
        if !name_rec.iter().any(|r| r.0 == b) {
            let off = arena.len() as u32;
            arena.extend_from_slice(f[1]);
            name_rec.push((b, off, f[1].len() as u32));
        }
        pairs.push((a, b));'''
new = '''        let ha = hash64(f[0]);
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
assert old in s
s = s.replace(old, new)
open(r'src/pipeline.rs', 'w').write(s)
print('ok')
