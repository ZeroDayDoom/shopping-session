s = open(r'src/pipeline.rs').read()
s = s.replace(
    'use std::sync::atomic::{AtomicU64, Ordering};',
    'use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};')
open(r'src/pipeline.rs', 'w').write(s)

t = open(r'src/main.rs').read()
marker = '    log!("clicks parsed");'
assert marker in t
t = t.replace(marker, '    let n_event_chunks = esides.len();\n' + marker)
open(r'src/main.rs', 'w').write(t)
print('ok')
