s = open(r'src/csv.rs').read()
start = s.index('/// Split a line into exactly N fields')
end = s.index('pub const INVALID_TS: i64 = i64::MIN;')
new = '''/// Split a line into exactly N fields, per the spec dialect: a quote may only
/// wrap a whole field; a quote anywhere else makes the line malformed; a wrong
/// field count or an empty line is malformed. One memchr2 pass per line drives
/// the whole state machine (all ',' and '"' hits in scan order).
#[inline]
pub fn split_fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    use memchr::memchr2_iter;

    let mut out: [&[u8]; N] = [&[]; N];
    let len = line.len();
    if len == 0 {
        return None;
    }
    let mut fi = 0usize;
    let mut content_start = 0usize;
    let mut quoted = false;
    for abs in memchr2_iter(b',', b'"', line) {
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
        }
    }
    // end of line: close the last field
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
    Some(out)
}

'''
s = s[:start] + new + s[end:]
open(r'src/csv.rs', 'w').write(s)
print('ok')
