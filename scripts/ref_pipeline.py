"""Independent Python reference for the Shopping Sessions rules.

Slow but straightforward; used to cross-check the Rust pipeline on a small
generated dataset. Writes user_daily.csv / campaign_daily.csv to OUT.
"""
import glob
import json
import math
import os
import sys
from datetime import datetime, timezone

DEV = sys.argv[1] if len(sys.argv) > 1 else r'C:\Users\ANKIT REWAR\Downloads\Hackathon\devsmall'
OUT = sys.argv[2] if len(sys.argv) > 2 else r'C:\Users\ANKIT REWAR\Downloads\Hackathon\refout'
SEVEN = 604800000
DAY = 86400000
GAP = 1800000
LATE = 86400000


def fields(line):
    out = []
    rest = line
    while True:
        i = rest.find(b',')
        f = rest if i < 0 else rest[:i]
        rest = b'' if i < 0 else rest[i + 1:]
        if f[:1] == b'"':
            if len(f) < 2 or f[-1:] != b'"' or b'"' in f[1:-1]:
                return None
            out.append(f[1:-1])
        else:
            if b'"' in f:
                return None
            out.append(f)
        if i < 0:
            return out


def dim(y, m):
    if m in (1, 3, 5, 7, 8, 10, 12):
        return 31
    if m in (4, 6, 9, 11):
        return 30
    leap = (y % 4 == 0 and y % 100 != 0) or y % 400 == 0
    return 29 if leap else 28


def days_from_civil(y, m, d):
    yy = y - 1 if m <= 2 else y
    era = (yy if yy >= 0 else yy - 399) // 400
    yoe = yy - era * 400
    mp = (m + 9) % 12
    doy = (153 * mp + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146097 + doe - 719468


def parse_ts(b):
    if len(b) == 20 and b[4:5] == b'-' and b[7:8] == b'-' and b[10:11] == b'T' \
            and b[13:14] == b':' and b[16:17] == b':' and b[19:20] == b'Z':
        try:
            y, mo, d, h, mi, s = (int(b[0:4]), int(b[5:7]), int(b[8:10]),
                                  int(b[11:13]), int(b[14:16]), int(b[17:19]))
        except ValueError:
            return None
        for v in (y, mo, d, h, mi, s):
            pass
        if mo < 1 or mo > 12 or d < 1 or d > dim(y, mo) or h > 23 or mi > 59 or s > 59:
            return None
        return days_from_civil(y, mo, d) * DAY + h * 3600000 + mi * 60000 + s * 1000
    if len(b) == 25 and (b[19:20] in (b'+', b'-')) and b[22:23] == b':' \
            and b[4:5] == b'-' and b[7:8] == b'-' and b[10:11] == b'T' \
            and b[13:14] == b':' and b[16:17] == b':':
        try:
            y, mo, d, h, mi, s = (int(b[0:4]), int(b[5:7]), int(b[8:10]),
                                  int(b[11:13]), int(b[14:16]), int(b[17:19]))
            oh, om = int(b[20:22]), int(b[23:25])
        except ValueError:
            return None
        if mo < 1 or mo > 12 or d < 1 or d > dim(y, mo) or h > 23 or mi > 59 or s > 59:
            return None
        if oh > 23 or om > 59:
            return None
        base = days_from_civil(y, mo, d) * DAY + h * 3600000 + mi * 60000 + s * 1000
        off = (oh * 3600 + om * 60) * 1000
        return base - off if b[19:20] == b'+' else base + off
    if b and all(48 <= c <= 57 for c in b):
        try:
            return int(b)
        except ValueError:
            return 2 ** 62  # too long -> outside window
    return None


manifest = json.load(open(os.path.join(DEV, 'manifest.json')))
WS = parse_ts(manifest['window_start'].encode())
WE = parse_ts(manifest['window_end'].encode())

# campaigns
campaigns = {}
for p in glob.glob(os.path.join(DEV, 'campaigns.csv')):
    for line in open(p, 'rb').read().split(b'\n')[1:]:
        if not line:
            continue
        f = fields(line)
        if f and len(f) == 2 and f[0]:
            campaigns[f[0].decode()] = f[1].decode()

# products
products = {}
for p in glob.glob(os.path.join(DEV, 'products.csv')):
    for line in open(p, 'rb').read().split(b'\n')[1:]:
        if not line:
            continue
        f = fields(line)
        if f and len(f) == 2 and f[0]:
            products[f[0].decode()] = f[1].decode()
categories = sorted(set(products.values()) | {'unknown'})

# links
parent = {}


def find(x):
    while parent.get(x, x) != x:
        parent[x] = parent.get(parent[x], parent[x])
        x = parent[x]
    return x


def union(a, b):
    ra, rb = find(a) if a in parent else a, find(b) if b in parent else b
    if ra == rb:
        return
    lo, hi = sorted([ra, rb], key=lambda s: s.encode())
    parent[hi] = lo
    parent.setdefault(lo, lo)


for p in glob.glob(os.path.join(DEV, 'links', '*.csv')):
    for line in open(p, 'rb').read().split(b'\n')[1:]:
        if not line:
            continue
        f = fields(line)
        if f is not None and len(f) == 2 and f[0] and f[1]:
            union(f[0].decode(), f[1].decode())


def person(u):
    return find(u) if u in parent else u


def name_of(root_members):
    return min(root_members, key=lambda s: s.encode())


# events: parse -> dedup by (event_id -> min ingest) -> validate -> late
events = {}
for p in glob.glob(os.path.join(DEV, 'events', '*.csv')):
    for line in open(p, 'rb').read().split(b'\n')[1:]:
        if not line:
            continue
        f = fields(line)
        if f is None or len(f) != 7:
            continue
        eid = f[0].decode()
        if not eid:
            continue
        ingest = parse_ts(f[3])
        if ingest is None:
            continue
        if eid not in events or ingest < events[eid][1]:
            events[eid] = (f, ingest)

surviving = []
for eid, (f, ingest) in events.items():
    u = f[1].decode()
    ts = parse_ts(f[2])
    typ = f[4].decode()
    prod = f[5].decode()
    price_s = f[6]
    if not u or ts is None or ts < WS or ts >= WE or typ not in ('view', 'cart', 'purchase'):
        continue
    if not prod:
        continue
    if typ == 'purchase':
        ps = price_s.decode()
        if not ps or not ps.isdigit() or len(ps) > 8:
            continue
        price = int(ps)
        if not (1 <= price <= 10000000):
            continue
        if ingest - ts > LATE:
            continue
    else:
        price = 0
        if ingest - ts > LATE:
            continue
    surviving.append((person(u), ts, typ, prod, price))

surviving.sort(key=lambda e: (e[0].encode(), e[1]))

# sessionize
sessions = []  # (person, start, day, [events])
cur = None
for (pers, ts, typ, prod, price) in surviving:
    if cur is None or pers != cur[0] or ts - cur[3] > GAP:
        if cur:
            sessions.append(cur)
        cur = [pers, ts, [(ts, typ, prod, price)], ts]
    else:
        cur[2].append((ts, typ, prod, price))
        cur[3] = ts
if cur:
    sessions.append(cur)

# clicks: parse -> dedup -> validate
clicks = {}
seen = set()
for p in glob.glob(os.path.join(DEV, 'clicks', '*.csv')):
    for line in open(p, 'rb').read().split(b'\n')[1:]:
        if not line:
            continue
        f = fields(line)
        if f is None or len(f) != 4:
            continue
        cid = f[0].decode()
        if not cid or cid in seen:
            continue
        seen.add(cid)
        u = f[1].decode()
        c = f[2].decode()
        ts = parse_ts(f[3])
        if not u or ts is None or ts < WS - SEVEN or ts >= WE or c not in campaigns:
            continue
        clicks.setdefault(person(u), []).append((ts, cid, c))

# attribution + assists + aggregates
user_daily = {}
camp_daily = {}


def camp_row(c, day):
    if c not in camp_daily:
        camp_daily[c] = {}
    return camp_daily[c].setdefault(day, [0, 0, 0, 0])


for (pers, start, evs, _last) in [(s[0], s[1], s[2], None) for s in sessions]:
    day = datetime.fromtimestamp(start / 1000, timezone.utc).date()
    cl = clicks.get(pers, [])
    lo = start - SEVEN
    window = [(ts, cid, c) for (ts, cid, c) in cl if lo <= ts <= start]
    if window:
        best = max(window, key=lambda w: (w[0], tuple(-b for b in w[1].encode())))
        credited = best[2]
    else:
        credited = 'organic'
    row = user_daily.setdefault((pers, day), [0, 0, 0, {}])
    row[0] += 1
    row[1] += len(evs)
    cats = {}
    for (_ts, typ, prod, price) in evs:
        if typ == 'purchase':
            row[2] += price
            cat = products.get(prod, 'unknown')
            cats[cat] = cats.get(cat, 0) + price
    if cats:
        for k, v in cats.items():
            row[3][k] = row[3].get(k, 0) + v
    cr = camp_row(credited, day)
    cr[0] += 1
    purchases = [e[3] for e in evs if e[1] == 'purchase']
    if purchases:
        cr[1] += 1
        cr[2] += sum(purchases)
    if credited != 'organic':
        pass
    assists = set(c for (_ts, _cid, c) in window)
    assists.discard('organic')
    for c in assists:
        camp_row(c, day)[3] += 1

os.makedirs(OUT, exist_ok=True)
with open(os.path.join(OUT, 'user_daily.csv'), 'w', newline='') as fh:
    fh.write('user_id,day,sessions,events,revenue_cents,top_category\n')
    for (pers, day) in sorted(user_daily, key=lambda k: (k[0].encode(), k[1])):
        r = user_daily[(pers, day)]
        cats = r[3]
        if cats:
            top = sorted(cats.items(), key=lambda kv: (-kv[1], kv[0].encode()))[0][0]
        else:
            top = ''
        fh.write(f'{pers},{day},{r[0]},{r[1]},{r[2]},{top}\n')
with open(os.path.join(OUT, 'campaign_daily.csv'), 'w', newline='') as fh:
    fh.write('campaign_id,channel,day,sessions,purchasing_sessions,revenue_cents,assisted_sessions\n')
    rows = []
    for c, days in camp_daily.items():
        for day, (s, pu, rev, ass) in days.items():
            if s == 0 and ass == 0:
                continue
            chan = campaigns.get(c, 'organic')
            rows.append((c.encode(), day, s, pu, rev, ass, chan))
    rows.sort()
    for (cb, day, s, pu, rev, ass, chan) in rows:
        fh.write(f'{cb.decode()},{chan},{day},{s},{pu},{rev},{ass}\n')
print('ref done: user rows', len(user_daily), 'camp rows', len(rows))
