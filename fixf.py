s = open(r'src/pipeline.rs').read()
s = s.replace('''    names: &mut PreMap<u64, (u32, u32, u32)>,
) -> (Vec<[u64; 2]>, Vec<Vec<ERec>>) {''', '''    name_rec: &mut Vec<(u32, u32, u32)>,
) -> (Vec<[u64; 2]>, Vec<Vec<ERec>>) {''')
s = s.replace('''    names: &mut PreMap<u64, (u32, u32, u32)>,
) -> (Vec<u8>, Vec<Vec<CRec>>) {''', '''    name_rec: &mut Vec<(u32, u32, u32)>,
) -> (Vec<u8>, Vec<Vec<CRec>>) {''')
s = s.replace('''            ts,
            uid,
            pidx,''', '''            ts,
            uid: uid as u64,
            pidx,''')
s = s.replace('''            ts,
            uid,
            camp,''', '''            ts,
            uid: uid as u64,
            camp,''')
open(r'src/pipeline.rs', 'w').write(s)
print('ok')
