m = open(r'src/main.rs').read()

# event chunk task: local vec name
m = m.replace('''                let mut arena = Vec::new();
                let mut names = csv::premap(4096);
                let (side, b) =
                    parse_event_chunk(data, lo, hi, &users, &mut arena, &mut name_rec);
                (side, arena, name_rec, b)''', '''                let mut arena = Vec::new();
                let mut name_rec = Vec::new();
                let (side, b) =
                    parse_event_chunk(data, lo, hi, &users, &mut arena, &mut name_rec);
                (side, arena, name_rec, b)''')

# click chunk task: local vec name
m = m.replace('''            let mut arena = Vec::new();
            let mut names = csv::premap(4096);
            let (a, b) =
                parse_click_chunk(data, lo, hi, seq as u32, &known_campaigns, &users, &mut arena, &mut name_rec);
            (a, name_rec, b)''', '''            let mut arena = Vec::new();
            let mut name_rec = Vec::new();
            let (a, b) =
                parse_click_chunk(data, lo, hi, seq as u32, &known_campaigns, &users, &mut arena, &mut name_rec);
            (a, name_rec, b)''')

# links local
m = m.replace(
    'let mut link_names: PreMap<u64, (u32, u32, u32)> = csv::premap(1024);',
    'let mut link_name_rec: Vec<(u32, u32, u32)> = Vec::new();')
m = m.replace('    chunk_names.push(link_names);', '    chunk_names.push(link_name_rec);')

# chunk_names / cparsed types
m = m.replace(
    'let mut chunk_names: Vec<PreMap<u64, (u32, u32, u32)>> =',
    'let mut chunk_names: Vec<Vec<(u32, u32, u32)>> =')
m = m.replace(
    'let cparsed: Vec<(Vec<u8>, PreMap<u64, (u32, u32, u32)>, Vec<Vec<CRec>>)> = csrc',
    'let cparsed: Vec<(Vec<u8>, Vec<(u32, u32, u32)>, Vec<Vec<CRec>>)> = csrc')

# fill_names signature
m = m.replace('''    fn fill_names(
        slots: &mut Vec<Option<Box<[u8]>>>,
        nm: &PreMap<u64, (u32, u32, u32)>,
        arena: &[u8],
    ) {
        for (_h, (gid, off, len)) in nm {''', '''    fn fill_names(
        slots: &mut Vec<Option<Box<[u8]>>>,
        rec: &[(u32, u32, u32)],
        arena: &[u8],
    ) {
        for (gid, off, len) in rec {''')

open(r'src/main.rs', 'w').write(m)
print('ok')
