s = open(r'src/pipeline.rs').read()

# 1. More buckets: dedup maps become L2-resident
s = s.replace('pub const NB: usize = 128; // hash buckets (power of two)',
              'pub const NB: usize = 512; // hash buckets (power of two)')

# 2. UserTable: interleave key+gid in one 16B slot (1 cache line per probe),
#    gid stored explicitly (fixes lost-race gaps differently than slot-index).
old = '''/// Unused slots are their own components and never appear in rows.
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
}'''
new = '''/// gid is assigned on first insert and stored in the slot.
pub struct UserTable {
    slots: Vec<USlot>, // interleaved: one cache line covers 4 slots
    mask: usize,
}

struct USlot {
    key: AtomicU64, // 0 = empty
    gid: std::sync::atomic::AtomicU32,
}

impl UserTable {
    pub fn new(cap_hint: usize) -> Self {
        let slots = (cap_hint * 2).max(1024).next_power_of_two();
        UserTable {
            slots: (0..slots)
                .map(|_| USlot {
                    key: AtomicU64::new(0),
                    gid: std::sync::atomic::AtomicU32::new(0),
                })
                .collect(),
            mask: slots - 1,
        }
    }

    #[inline]
    pub fn get(&self, h: u64) -> Option<u32> {
        let mut i = (h as usize) & self.mask;
        loop {
            let sl = &self.slots[i];
            let k = sl.key.load(Ordering::Relaxed);
            if k == 0 {
                return None;
            }
            if k == h {
                return Some(sl.gid.load(Ordering::Relaxed));
            }
            i = (i + 1) & self.mask;
        }
    }

    /// Insert h if absent. Returns (gid, is_new).
    pub fn get_or_insert(&self, h: u64) -> (u32, bool) {
        let mut i = (h as usize) & self.mask;
        loop {
            let sl = &self.slots[i];
            let k = sl.key.load(Ordering::Relaxed);
            if k == h {
                return (sl.gid.load(Ordering::Relaxed), false);
            }
            if k == 0 {
                match sl.key.compare_exchange(0, h, Ordering::Relaxed, Ordering::Relaxed) {
                    Ok(_) => {
                        let gid = sl.gid.load(Ordering::Relaxed);
                        return (gid, true);
                    }
                    Err(cur) => {
                        if cur == h {
                            return (sl.gid.load(Ordering::Relaxed), false);
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
        self.slots.len()
    }
}'''
assert old in s
s = s.replace(old, new)
open(r'src/pipeline.rs', 'w').write(s)
print('ok')
