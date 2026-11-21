s = open(r'src/pipeline.rs').read()
# revert UserTable to keys-only with gid = slot index (race-free)
start = s.index('/// gid is assigned on first insert and stored in the slot.')
end = s.index('/// 64-bit user-id hash (collision odds ~1e-7 at millions of users).')
new = '''/// gid == slot index (stable, race-free: CAS claims the key, the id is the
/// slot). Unused slots are their own components and never appear in rows.
pub struct UserTable {
    keys: Vec<AtomicU64>, // 0 = empty slot
    mask: usize,
}

impl UserTable {
    pub fn new(cap_hint: usize) -> Self {
        let slots = (cap_hint * 2).max(1024).next_power_of_two();
        UserTable {
            keys: (0..slots).map(|_| AtomicU64::new(0)).collect(),
            mask: slots - 1,
        }
    }

    #[inline]
    pub fn get(&self, h: u64) -> Option<u32> {
        let mut i = (h as usize) & self.mask;
        loop {
            let k = self.keys[i].load(Ordering::Relaxed);
            if k == 0 {
                return None;
            }
            if k == h {
                return Some(i as u32);
            }
            i = (i + 1) & self.mask;
        }
    }

    /// Insert h if absent. Returns (gid, is_new); gid == slot index.
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
    }

    pub fn n(&self) -> usize {
        self.keys.len()
    }
}

'''
s = s[:start] + new + s[end:]
# keep NB=512 (already applied above)
open(r'src/pipeline.rs', 'w').write(s)
print('ok')
