s = open(r'src/pipeline.rs').read()

old_ed = '''struct EDedup {
    keys: Vec<[u64; 2]>,
    ingest: Vec<i64>,
    wkey: Vec<u64>,
    mask: usize,
}

impl EDedup {
    fn new(rows: usize) -> Self {
        let slots = (rows + (rows >> 1)).max(64).next_power_of_two();
        EDedup {
            keys: vec![[0, 0]; slots],
            ingest: vec![0; slots],
            wkey: vec![0; slots],
            mask: slots - 1,
        }
    }
    #[inline]
    fn upsert(&mut self, h: [u64; 2], ingest: i64, wkey: u64) {
        let mut i = (h[0] as usize) & self.mask;
        loop {
            let k = self.keys[i];
            if k == [0, 0] {
                self.keys[i] = h;
                self.ingest[i] = ingest;
                self.wkey[i] = wkey;
                return;
            }
            if k == h {
                if ingest < self.ingest[i] {
                    self.ingest[i] = ingest;
                    self.wkey[i] = wkey;
                }
                return;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline]
    fn winner(&self, h: [u64; 2]) -> Option<u64> {
        let mut i = (h[0] as usize) & self.mask;
        loop {
            let k = self.keys[i];
            if k == [0, 0] {
                return None;
            }
            if k == h {
                return Some(self.wkey[i]);
            }
            i = (i + 1) & self.mask;
        }
    }
}'''
new_ed = '''#[derive(Clone, Copy)]
struct ESlot {
    key: [u64; 2],
    ingest: i64,
    wkey: u64,
}

struct EDedup {
    slots: Vec<ESlot>,
    mask: usize,
}

impl EDedup {
    fn new(rows: usize) -> Self {
        let slots = (rows + (rows >> 1)).max(64).next_power_of_two();
        EDedup {
            slots: vec![ESlot { key: [0, 0], ingest: 0, wkey: 0 }; slots],
            mask: slots - 1,
        }
    }
    #[inline]
    fn upsert(&mut self, h: [u64; 2], ingest: i64, wkey: u64) {
        let mut i = (h[0] as usize) & self.mask;
        loop {
            let k = self.slots[i].key;
            if k == [0, 0] {
                self.slots[i] = ESlot { key: h, ingest, wkey };
                return;
            }
            if k == h {
                if ingest < self.slots[i].ingest {
                    self.slots[i].ingest = ingest;
                    self.slots[i].wkey = wkey;
                }
                return;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline]
    fn winner(&self, h: [u64; 2]) -> Option<u64> {
        let mut i = (h[0] as usize) & self.mask;
        loop {
            let k = self.slots[i].key;
            if k == [0, 0] {
                return None;
            }
            if k == h {
                return Some(self.slots[i].wkey);
            }
            i = (i + 1) & self.mask;
        }
    }
}'''
assert old_ed in s, 'ED not found'
s = s.replace(old_ed, new_ed)

old_cd = '''struct CDedup {
    keys: Vec<[u64; 2]>,
    wkey: Vec<u64>,
    mask: usize,
}

impl CDedup {
    fn new(rows: usize) -> Self {
        let slots = (rows + (rows >> 1)).max(64).next_power_of_two();
        CDedup {
            keys: vec![[0, 0]; slots],
            wkey: vec![0; slots],
            mask: slots - 1,
        }
    }
    #[inline]
    fn insert_first(&mut self, h: [u64; 2], wkey: u64) {
        let mut i = (h[0] as usize) & self.mask;
        loop {
            if self.keys[i] == [0, 0] {
                self.keys[i] = h;
                self.wkey[i] = wkey;
                return;
            }
            if self.keys[i] == h {
                return;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline]
    fn winner(&self, h: [u64; 2]) -> Option<u64> {
        let mut i = (h[0] as usize) & self.mask;
        loop {
            let k = self.keys[i];
            if k == [0, 0] {
                return None;
            }
            if k == h {
                return Some(self.wkey[i]);
            }
            i = (i + 1) & self.mask;
        }
    }
}'''
new_cd = '''#[derive(Clone, Copy)]
struct CSlot {
    key: [u64; 2],
    wkey: u64,
}

struct CDedup {
    slots: Vec<CSlot>,
    mask: usize,
}

impl CDedup {
    fn new(rows: usize) -> Self {
        let slots = (rows + (rows >> 1)).max(64).next_power_of_two();
        CDedup {
            slots: vec![CSlot { key: [0, 0], wkey: 0 }; slots],
            mask: slots - 1,
        }
    }
    #[inline]
    fn insert_first(&mut self, h: [u64; 2], wkey: u64) {
        let mut i = (h[0] as usize) & self.mask;
        loop {
            let k = self.slots[i].key;
            if k == [0, 0] {
                self.slots[i] = CSlot { key: h, wkey };
                return;
            }
            if k == h {
                return;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline]
    fn winner(&self, h: [u64; 2]) -> Option<u64> {
        let mut i = (h[0] as usize) & self.mask;
        loop {
            let k = self.slots[i].key;
            if k == [0, 0] {
                return None;
            }
            if k == h {
                return Some(self.slots[i].wkey);
            }
            i = (i + 1) & self.mask;
        }
    }
}'''
assert old_cd in s, 'CD not found'
s = s.replace(old_cd, new_cd)
open(r'src/pipeline.rs', 'w').write(s)
print('ok')
