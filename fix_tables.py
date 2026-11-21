s = open(r'src/pipeline.rs').read()

# ERec / CRec field rename
s = s.replace('pub ulid: u32, // (chunk seq << 24) | local user id', 'pub uid: u32, // global user id')

# remove Table struct
s = s.replace('''/// Per-chunk interning table for user ids (plus per-chunk side vecs/arenas).
pub struct Table {
    pub users: PreMap<[u64; 2], u32>,
    pub strings: Vec<Box<[u8]>>,
}

impl Table {
    pub fn new(est_rows: usize) -> Self {
        Table {
            users: crate::csv::premap(est_rows / 6 + 16),
            strings: Vec::with_capacity(est_rows / 6 + 16),
        }
    }

    #[inline]
    pub fn intern(&mut self, h: [u64; 2], bytes: &[u8]) -> u32 {
        match self.users.get(&h) {
            Some(&id) => id,
            None => {
                let id = self.strings.len() as u32;
                self.users.insert(h, id);
                self.strings.push(Box::from(bytes));
                id
            }
        }
    }
}

''', '')

# remove merge_user_tables + root_remaps (per-table) functions
import re
start = s.index('/// Merge per-chunk user tables into a global id space.')
end = s.index('/// Bucket-local event dedup + validation.')
s = s[:start] + s[end:]

# dedup_events signature + lookups
s = s.replace('''pub fn dedup_events(
    ebuckets: Vec<Vec<(usize, Vec<ERec>)>>,
    root_remaps: &[Vec<u32>],
    sides: &[Vec<[u64; 2]>],''', '''pub fn dedup_events(
    ebuckets: Vec<Vec<(usize, Vec<ERec>)>>,
    root_of_user: &[u32],
    uid_empty: u32,
    sides: &[Vec<[u64; 2]>],''')
s = s.replace('''                    let root = root_remaps[(r.ulid >> 24) as usize][(r.ulid & 0xFF_FFFF) as usize];
                    if root == u32::MAX {
                        continue; // empty user
                    }''', '''                    if r.uid == uid_empty {
                        continue; // empty user
                    }
                    let root = root_of_user[r.uid as usize];''')

# dedup_clicks signature + lookups
s = s.replace('''pub fn dedup_clicks(
    cbuckets: Vec<Vec<(usize, Vec<CRec>)>>,
    root_remaps: &[Vec<u32>],
    ev_table_base: usize,
    known_max: usize,
    ws: i64,
    we: i64,
) -> Vec<CFinal> {''', '''pub fn dedup_clicks(
    cbuckets: Vec<Vec<(usize, Vec<CRec>)>>,
    root_of_user: &[u32],
    uid_empty: u32,
    known_max: usize,
    ws: i64,
    we: i64,
) -> Vec<CFinal> {''')
s = s.replace('''                    let root =
                        root_remaps[ev_table_base + (r.ulid >> 24) as usize]
                            [(r.ulid & 0xFF_FFFF) as usize];
                    if root == u32::MAX
                        || r.ts == INVALID_TS''', '''                    if r.uid == uid_empty
                        || r.ts == INVALID_TS''')
s = s.replace('''                    out.push(CFinal {
                        root,
                        ts: r.ts,
                        camp: r.camp as u16,
                        arena: (r.ulid >> 24) as u32,
                        off: r.off,
                        len: r.len,
                    });''', '''                    out.push(CFinal {
                        root: root_of_user[r.uid as usize],
                        ts: r.ts,
                        camp: r.camp as u16,
                        arena: (r.arena_seq) as u32,
                        off: r.off,
                        len: r.len,
                    });''')

open(r'src/pipeline.rs', 'w').write(s)
print('done')
