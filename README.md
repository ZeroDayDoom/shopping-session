# Shopping Sessions — Rust pipeline

Implements `rules.md` exactly: dirty CSV cleaning, strict F1/F2/F3 timestamps,
transitive linked-ID union-find, 30-minute sessionization, 7-day click
attribution and assists, and the two daily Parquet tables.

## Layout

- `src/main.rs` — the whole pipeline (single file).
- `Dockerfile` — two-stage rust build; final image is `debian:bookworm-slim` and
  runs `/app/pipeline` as uid 1000 with no network.
- `fixtures/` — the 36 edge-case fixtures with hand-written expected tables.
- `scripts/` — fixture runner, output comparer, docker runner (scorer flags).

## Design notes

- Custom CSV field splitter matching the spec dialect: a quote may only wrap a
  whole field, `\r` is an ordinary character, wrong field counts are malformed.
- Strict timestamp parser: F1 (`...Z`), F2 (`±HH:MM`, hours 00-23, minutes
  00-59, colon required), F3 (ASCII digits only), full calendar validation.
- IDs are interned into u32s with a sharded concurrent interner; links are
  union-find; each person is named by the byte-wise smallest of their IDs.
- Event dedup on a 128-bit hash of `event_id` keeping the smallest `ingest_ts`,
  before validation, exactly as the rules require.
- Events sort by (person, session gap) in one parallel sort keyed
  `person << 31 | (ts - window_start)`, then one sequential sessionization pass
  also produces `user_daily` (per person-day runs, category sums, byte-wise
  tie-break on ranks of byte-sorted category names).
- Clicks sort by (person, ts asc, click_id desc) so the last click in the
  sliding 7-day window is the credited one; assists come from the same window
  with per-person campaign counts, and a generation marker deduplicates the
  active-campaign scan per session.
- Parquet written via the `parquet` crate (snappy): INT64 counts, INT32 DATE,
  UTF8 strings, nullable `top_category`.

## Development without Docker

```sh
cargo build --release
python3 scripts/run_fixtures.py fixtures -- target/release/shopping-sessions
generate --seed 1 --events 1000000 --clicks 250000 --output dev
DATA_DIR=dev OUTPUT_DIR=out ./target/release/shopping-sessions
python3 scripts/compare.py out dev-answer
```

Timing on one machine (8 threads): ~0.4 s for the 1M dev set, ~9-10 s for
20M events; the 60M final set is projected around 30 s, comfortably under
the 3-minute cap.
