s = open(r'src/main.rs').read()

old = '''/// Memory-map every csv file in a folder; build chunk ranges split at \\n.
struct Src {
    mmaps: Vec<memmap2::Mmap>,
    chunks: Vec<(u32, usize, usize)>, // (file index, lo, hi)
}

fn build_src(dir: &PathBuf) -> Src {
    let files = csv_files(dir);
    let mut mmaps = Vec::new();
    let mut chunks: Vec<(u32, usize, usize)> = Vec::new();
    for (fi, p) in files.iter().enumerate() {
        let Ok(f) = fs::File::open(p) else { continue };
        let Ok(md) = f.metadata() else { continue };
        let len = md.len() as usize;
        if len == 0 {
            continue;
        }
        let mapped = unsafe { memmap2::Mmap::map(&f) };
        let Ok(map) = mapped else { continue };
        if len <= CHUNK {
            chunks.push((fi as u32, 0, len));
        } else {
            let data: &[u8] = &map;
            let mut start = 0usize;
            while start < len {
                let mut end = (start + CHUNK).min(len);
                if end < len {
                    match memchr(b'\\n', &data[end..]) {
                        Some(o) => end += o + 1,
                        None => end = len,
                    }
                }
                chunks.push((fi as u32, start, end));
                start = end;
            }
        }
        mmaps.push(map);
    }
    Src { mmaps, chunks }
}'''

new = '''/// Read every csv file in a folder in parallel; build chunk ranges split at \\n.
struct Src {
    bufs: Vec<Vec<u8>>,
    chunks: Vec<(u32, usize, usize)>, // (file index, lo, hi)
}

fn build_src(dir: &PathBuf) -> Src {
    let files = csv_files(dir);
    let bufs: Vec<Vec<u8>> = files
        .par_iter()
        .map(|p| fs::read(p).unwrap_or_default())
        .collect();
    let mut chunks: Vec<(u32, usize, usize)> = Vec::new();
    for (fi, buf) in bufs.iter().enumerate() {
        let len = buf.len();
        if len == 0 {
            continue;
        }
        if len <= CHUNK {
            chunks.push((fi as u32, 0, len));
        } else {
            let mut start = 0usize;
            while start < len {
                let mut end = (start + CHUNK).min(len);
                if end < len {
                    match memchr(b'\\n', &buf[end..]) {
                        Some(o) => end += o + 1,
                        None => end = len,
                    }
                }
                chunks.push((fi as u32, start, end));
                start = end;
            }
        }
    }
    Src { bufs, chunks }
}'''

assert old in s, 'old block not found'
s = s.replace(old, new)
s = s.replace('let data: &[u8] = &esrc.mmaps[fi as usize];', 'let data: &[u8] = &esrc.bufs[fi as usize];')
s = s.replace('let data: &[u8] = &csrc.mmaps[fi as usize];', 'let data: &[u8] = &csrc.bufs[fi as usize];')
open(r'src/main.rs', 'w').write(s)
print('done')
