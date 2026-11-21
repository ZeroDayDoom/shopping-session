// Spec-exact CSV splitting (single SIMD pass), strict timestamps, hashing.

#[inline]
pub fn mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// 128-bit key from a byte string: two FNV-1a streams, finalized with splitmix.
/// Inputs are generated (non-adversarial); exact byte identity is what matters.
#[inline]
pub fn hash128(b: &[u8], seed: u64) -> [u64; 2] {
    let mut h1 = 0xcbf2_9ce4_8422_2325u64 ^ seed;
    let mut h2 = 0x8422_2325_cbf2_9ce4u64 ^ mix(seed | 1);
    for &c in b {
        h1 = (h1 ^ c as u64).wrapping_mul(0x1_0000_0001_b3);
        h2 = (h2 ^ c as u64).wrapping_mul(0x1_0000_0001_b3);
    }
    [mix(h1), mix(h2 ^ 0x9e37_79b9_7f4a_7c15)]
}

/// Hasher for keys that are already 128-bit hashes.
#[derive(Default)]
pub struct PreHasher(u64);

impl std::hash::Hasher for PreHasher {
    #[inline]
    fn write_u64(&mut self, v: u64) {
        self.0 = mix(self.0 ^ v);
    }
    #[inline]
    fn write_u128(&mut self, v: u128) {
        self.write_u64(v as u64);
        self.write_u64((v >> 64) as u64);
    }
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0 ^ b as u64).wrapping_mul(0x1_0000_0001_b3);
        }
    }
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
}

pub type PreMap<K, V> = std::collections::HashMap<K, V, std::hash::BuildHasherDefault<PreHasher>>;

/// Pre-hashed map with the given capacity.
pub fn premap<K: std::hash::Hash + Eq, V>(cap: usize) -> PreMap<K, V> {
    std::collections::HashMap::with_capacity_and_hasher(cap, Default::default())
}

/// Split a line into exactly N fields, per the spec dialect: a quote may only
/// wrap a whole field; a quote anywhere else makes the line malformed; a wrong
/// field count or an empty line is malformed. Single memchr2 pass over the line.
#[inline]
pub fn split_fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    use memchr::memchr2;

    let mut out: [&[u8]; N] = [&[]; N];
    let len = line.len();
    if len == 0 {
        return None;
    }
    let mut fi = 0usize;
    let mut content_start = 0usize;
    let mut pos = 0usize;
    let mut quoted = false;
    if line[0] == b'"' {
        quoted = true;
        content_start = 1;
        pos = 1;
    }
    loop {
        match memchr2(b',', b'"', &line[pos..]) {
            None => {
                if fi + 1 != N {
                    return None;
                }
                if quoted {
                    if len < content_start + 1 || line[len - 1] != b'"' {
                        return None;
                    }
                    out[fi] = &line[content_start..len - 1];
                } else {
                    out[fi] = &line[content_start..len];
                }
                return Some(out);
            }
            Some(rel) => {
                let abs = pos + rel;
                if line[abs] == b'"' {
                    if !quoted {
                        if abs != content_start {
                            return None; // quote not at field start
                        }
                        quoted = true;
                        content_start = abs + 1;
                    } else {
                        // closing quote: must be followed by ',' or end of line
                        if abs + 1 != len && line[abs + 1] != b',' {
                            return None;
                        }
                    }
                    pos = abs + 1;
                } else {
                    // comma: close current field
                    if fi >= N {
                        return None;
                    }
                    if quoted {
                        if abs < content_start + 1 || line[abs - 1] != b'"' {
                            return None;
                        }
                        out[fi] = &line[content_start..abs - 1];
                    } else {
                        out[fi] = &line[content_start..abs];
                    }
                    fi += 1;
                    quoted = false;
                    content_start = abs + 1;
                    pos = abs + 1;
                    if content_start < len && line[content_start] == b'"' {
                        quoted = true;
                        content_start += 1;
                        pos = content_start;
                    }
                }
            }
        }
    }
}

pub const INVALID_TS: i64 = i64::MIN;

#[inline]
fn num(b: &[u8]) -> Option<i64> {
    let mut v: i64 = 0;
    for &c in b {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (c - b'0') as i64;
    }
    Some(v)
}

#[inline]
fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

#[inline]
fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap(y) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

/// Days since 1970-01-01 (Howard Hinnant's algorithm). y/m/d must be sane.
#[inline]
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12; // March = 0
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Calendar part of a timestamp: bytes 0..19 formatted YYYY-MM-DDTHH:MM:SS.
#[inline]
fn civil_ms(b: &[u8]) -> i64 {
    let (y, mo, d, h, mi, s) = match (
        num(&b[0..4]),
        num(&b[5..7]),
        num(&b[8..10]),
        num(&b[11..13]),
        num(&b[14..16]),
        num(&b[17..19]),
    ) {
        (Some(a), Some(b2), Some(c), Some(e), Some(f), Some(g)) => (a, b2, c, e, f, g),
        _ => return INVALID_TS,
    };
    if mo < 1 || mo > 12 || d < 1 || d > days_in_month(y, mo) {
        return INVALID_TS;
    }
    if h > 23 || mi > 59 || s > 59 {
        return INVALID_TS;
    }
    days_from_civil(y, mo, d) * 86_400_000 + h * 3_600_000 + mi * 60_000 + s * 1_000
}

/// Parse ts / ingest_ts. Three forms only (rules.md):
/// F1 `2026-10-03T23:58:10Z`, F2 `2026-10-03T19:58:10-04:00`,
/// F3 digits-only epoch millis. Everything else is INVALID_TS.
#[inline]
pub fn parse_ts(b: &[u8]) -> i64 {
    if b.len() == 20
        && b[4] == b'-'
        && b[7] == b'-'
        && b[10] == b'T'
        && b[13] == b':'
        && b[16] == b':'
        && b[19] == b'Z'
    {
        return civil_ms(b);
    }
    if b.len() == 25
        && b[4] == b'-'
        && b[7] == b'-'
        && b[10] == b'T'
        && b[13] == b':'
        && b[16] == b':'
        && (b[19] == b'+' || b[19] == b'-')
        && b[22] == b':'
    {
        let base = civil_ms(b);
        if base == INVALID_TS {
            return INVALID_TS;
        }
        let (oh, om) = match (num(&b[20..22]), num(&b[23..25])) {
            (Some(a), Some(c)) => (a, c),
            _ => return INVALID_TS,
        };
        if oh > 23 || om > 59 {
            return INVALID_TS;
        }
        let off_ms = (oh * 3600 + om * 60) * 1000;
        if b[19] == b'+' {
            base - off_ms
        } else {
            base + off_ms
        }
    } else if !b.is_empty() && b.iter().all(|&c| c.is_ascii_digit()) {
        // F3: leading zeros fine; too long for i64 means outside the window.
        let mut v: i64 = 0;
        for &c in b {
            let d = (c - b'0') as i64;
            match v.checked_mul(10).and_then(|x| x.checked_add(d)) {
                Some(x) => v = x,
                None => return i64::MAX, // beyond window_end for any real window
            }
        }
        v
    } else {
        INVALID_TS
    }
}

/// price_cents: ASCII digits only, value 1..=10,000,000 for purchases.
#[inline]
pub fn parse_price(b: &[u8]) -> (i64, bool) {
    if b.is_empty() || b.len() > 8 {
        return (0, false);
    }
    let mut v: i64 = 0;
    for &c in b {
        if !c.is_ascii_digit() {
            return (0, false);
        }
        v = v * 10 + (c - b'0') as i64;
    }
    (v, v >= 1 && v <= 10_000_000)
}

pub fn find(p: &mut [u32], mut x: u32) -> u32 {
    while p[x as usize] != x {
        p[x as usize] = p[p[x as usize] as usize]; // path halving
        x = p[x as usize];
    }
    x
}

pub fn json_str_field(s: &str, key: &str) -> Option<String> {
    let pat = format!("\"{}\"", key);
    let i = s.find(&pat)?;
    let rest = &s[i + pat.len()..];
    let c = rest.find(':')?;
    let rest = &rest[c + 1..];
    let q1 = rest.find('"')?;
    let rest2 = &rest[q1 + 1..];
    let q2 = rest2.find('"')?;
    Some(rest2[..q2].to_string())
}
