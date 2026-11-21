s = open(r'src/pipeline.rs').read()

old = '''    /// Insert h if absent. Returns the gid (== slot index).
    pub fn get_or_insert(&self, h: u64) -> u32 {
        let mut i = (h as usize) & self.mask;
        loop {
            let s = &self.keys[i];
            let k = s.load(Ordering::Relaxed);
            if k == h {
                return i as u32;
            }
            if k == 0 {
                match s.compare_exchange(0, h, Ordering::Relaxed, Ordering::Relaxed) {
                    Ok(_) => return i as u32,
                    Err(cur) => {
                        if cur == h {
                            return i as u32;
                        }
                        i = (i + 1) & self.mask;
                    }
                }
                continue;
            }
            i = (i + 1) & self.mask;
        }
    }'''
new = '''    /// Insert h if absent. Returns (gid, is_new); gid == slot index.
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
    }'''
assert old in s
s = s.replace(old, new)
open(r'src/pipeline.rs', 'w').write(s)
print('ok')
