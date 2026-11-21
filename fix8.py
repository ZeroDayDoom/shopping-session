s = open(r'src/pipeline.rs').read()

# 1. UserTable interleaved slots
old_ut = '''pub struct UserTable {
    keys: Vec<AtomicU64>,
    gids: Vec<std::sync::atomic::AtomicU32>,
    mask: usize,
    count: AtomicUsize,
}

impl UserTable {
    pub fn new(cap_hint: usize) -> Self {
        let slots = (cap_hint * 2).max(1024).next_power_of_two();
        UserTable {
            keys: (0..slots).map(|_| AtomicU64::new(0)).collect(),
            gids: (0..slots).map(|_| std::sync::atomic::AtomicU32::new(0)).collect(),
            mask: slots - 1,
            count: AtomicUsize::new(0),
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
                return Some(self.gids[i].load(Ordering::Relaxed));
            }
            i = (i + 1) & self.mask;
        }
    }

    /// Insert h if absent. Returns the gid (existing or newly assigned).
    pub fn get_or_insert(&self, h: u64) -> u32 {
        let mut i = (h as usize) & self.mask;
        loop {
            let k = self.keys[i].load(Ordering::Relaxed);
            if k == h {
                return self.gids[i].load(Ordering::Relaxed);
            }
            if k == 0 {
                // claim the slot with the hash
                match self.keys[i].compare_exchange(
                    0,
                    h,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        let gid = self.count.fetch_add(1, Ordering::Relaxed) as u32;
                        self.gids[i].store(gid, Ordering::Relaxed);
                        return gid;
                    }
                    Err(cur) => {
                        if cur == h {
                            return self.gids[i].load(Ordering::Relaxed);
                        }
                        // another key claimed it: keep probing
                        i = (i + 1) & self.mask;
                    }
                }
                continue;
            }
            i = (i + 1) & self.mask;
        }
    }

    pub fn n(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }
}'''
new_ut = '''struct USlot {
    key: AtomicU64,
    gid: std::sync::atomic::AtomicU32,
}

pub struct UserTable {
    slots: Vec<USlot>,
    mask: usize,
    count: AtomicUsize,
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
            count: AtomicUsize::new(0),
        }
    }

    #[inline]
    pub fn get(&self, h: u64) -> Option<u32> {
        let mut i = (h as usize) & self.mask;
        loop {
            let s = &self.slots[i];
            let k = s.key.load(Ordering::Relaxed);
            if k == 0 {
                return None;
            }
            if k == h {
                return Some(s.gid.load(Ordering::Relaxed));
            }
            i = (i + 1) & self.mask;
        }
    }

    /// Insert h if absent. Returns the gid (existing or newly assigned).
    pub fn get_or_insert(&self, h: u64) -> u32 {
        let mut i = (h as usize) & self.mask;
        loop {
            let s = &self.slots[i];
            let k = s.key.load(Ordering::Relaxed);
            if k == h {
                return s.gid.load(Ordering::Relaxed);
            }
            if k == 0 {
                match s.key.compare_exchange(0, h, Ordering::Relaxed, Ordering::Relaxed) {
                    Ok(_) => {
                        let gid = self.count.fetch_add(1, Ordering::Relaxed) as u32;
                        s.gid.store(gid, Ordering::Relaxed);
                        return gid;
                    }
                    Err(cur) => {
                        if cur == h {
                            return s.gid.load(Ordering::Relaxed);
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
        self.count.load(Ordering::Relaxed)
    }
}'''
assert old_ut in s, 'UT not found'
s = s.replace(old_ut, new_ut)
open(r'src/pipeline.rs', 'w').write(s)
print('ok')
