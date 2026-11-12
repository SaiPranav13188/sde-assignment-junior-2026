# Submission

## Candidate
- **Name:** B. Sai Pranav
- **Assignment chosen:** A (Data): Interaction Ledger & Reporting Pipeline
- **Time spent (honest estimate):** ~2h wall-clock, heavily AI-assisted with Claude Code (see `AI_USAGE.md`)

## How to run
```bash
# setup (Python 3.11+)
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt                         # adds `cryptography` for PII encryption

# generate data
python assignment-a-data/tools/generate_data.py --seed 42 --scale small --out data

# run the pipeline (from the repo root)
python solution/pipeline.py ingest data/drop_1
python solution/pipeline.py ingest data/drop_2          # or: python solution/pipeline.py ingest data
python solution/pipeline.py report --out reports/

# tests (93 tests, ~10 s)
python -m pytest

# optional: independent ground-truth check against the generator (any seed)
python solution/tools/verify_against_generator.py . 42 small data reports   # -> ALL MATCH
```
State lives in `solution/.state/ledger.db` (override with `--db`). Delete that folder to start
fresh. The PII master key comes from the `AU_PII_KEY` env var. If that is unset, a random key is
created on first run at `<db>.key`.

## Design

**Flow.** `ingest <drop>` validates and loads one drop into SQLite in **a single transaction**.
`report` reads the ledger and the leads, derives every fact from scratch, and writes the five
files atomically (temp file + rename). Code is split into pure rule modules (`normalize.py`,
`derive.py`), which hold all business logic and are unit-tested without a DB, and thin I/O modules
(`ingest.py`, `report.py`, `db.py`).

**Key idea: link at read time, store at write time.** Ingest stores *facts* (lead rows and
events exactly as validated). It does **not** freeze whether an event is an orphan. Event→lead
linking, call assembly, effective dispositions and buckets are derived at report time from the
full ledger. So:
- an event that arrived before its lead counts as soon as the lead exists (the heads-up case),
  and orphan counts always reflect the final state;
- `drop_1` then `drop_2` gives exactly the same result as ingesting everything at once
  (tested, byte-identical);
- a rule change (for example the bucket definition) needs no data migration; just re-run `report`.

**Schema (SQLite)**

| Table | Purpose | Key constraints |
|---|---|---|
| `drops` | each ingest run, in order | `drop_seq` autoincrement = processing order |
| `source_files` | every ingested file, with its SHA-256 | `UNIQUE(kind, sha256)`; partial `UNIQUE(workflow_id, batch_id)` for lead files |
| `leads` | one row per lead identity | `UNIQUE(workflow_id, phone_index)`; `batch_id` is set once at creation |
| `lead_rows` | audit row for **every** non-blank CSV row: `created` / `merged` / `rejected` + reason + warning count. No PII | `PK(file_id, row_number)`; CHECKs tie `outcome`, `reason` and `lead_id` together |
| `events` | **append-only interaction ledger** | `UNIQUE(event_id)`; `BEFORE UPDATE/DELETE` triggers `RAISE(ABORT)` |
| `event_duplicates`, `malformed_lines` | quarantine for lines that never enter the ledger (malformed lines store a hash, not the text, since it may contain PII) | `PK(file_id, line_no)` |

Every number in `data_quality.json` is a `COUNT(*)` over these tables (or a derivation over them),
never a stored counter. That is why re-runs cannot inflate it. Overrides are ordinary new
ledger rows; the original `disposition.set` is never touched (the DB enforces this).

**PII at rest.** Fields marked `pii: true` (name, phone) are never stored in plaintext:
- **Matching** uses a *blind index*: `HMAC-SHA256(k_index, "+91XXXXXXXXXX")`. It is
  deterministic, so an event's phone is matched by an indexed equality lookup. It is keyed
  because a plain SHA-256 of a 10-digit mobile number can be brute-forced in seconds.
- **Recovery** uses Fernet (AES + HMAC, random IV) over a JSON of all PII fields, so ops can
  still decrypt a lead when they really need it (for example to dial).
- Both keys are derived (HMAC-based KDF) from one master secret. A key fingerprint is stored in
  `meta`, and a run with the wrong key **fails loudly** instead of silently linking nothing.
- Event phones are stored only as the blind index (data minimisation). Only `phone_last4` is
  kept in clear, which is exactly what the masked report already exposes.
- Verified by test: grepping the raw `.db` bytes for any name or phone finds nothing.

**Idempotency and atomicity.** Files are identified by content hash; a file already ingested is
skipped, so re-ingesting a drop changes no row (tested by snapshotting every table). A whole drop
commits or rolls back as one unit, so a crash mid-drop leaves the DB untouched and the drop can
simply be re-run.

## Decisions & assumptions

| Decision | Why | Alternative considered |
|---|---|---|
| SQLite, single file | Zero setup, transactional, more than fast enough for the volumes here | Postgres (docker-compose): better for concurrent writers and production, but more setup for reviewers |
| Link events to leads at **report** time, not ingest time | Late-arriving leads must resolve orphans; keeps incremental == one-shot by construction | Re-link stored orphans when new leads arrive: more state and more ways to drift |
| Idempotency by **file content hash** | Survives drops being moved or renamed; a true re-run is a no-op | Path-based: breaks if the folder moves. Trade-off: a byte-identical events file re-delivered in a *new* drop is skipped as a whole rather than counted as N duplicate events |
| A lead file whose `(workflow, batch_id)` was already ingested **with different content** is refused (exit 2, whole drop rolled back) | Re-merging it would double-count rows and corrupt row numbers | Treat as a new batch; replace the old one. Both silently change history |
| Whole drop = one transaction | Crash safety: all or nothing | Per-file transactions: finer grained, but leave half-ingested drops |
| PII: HMAC blind index + Fernet; `phone_last4` in clear | Match without plaintext; last 4 digits are already public in the report | Deterministic encryption of the phone (ties matching to the cipher); hashing without a key (brute-forceable) |
| `connectivity_rate` uses exact decimal arithmetic, rounded **half-up** | "Exact metrics"; avoids binary-float artefacts | `f"{c/t:.4f}"` (round-half-even on a float). Differs only at exact ties such as 1/32: we give `0.0313`, float gives `0.0312` |
| A required field that is present but invalid (non-phone) → reject as `invalid:<field>` | The spec only defines the phone case; with the given schemas this is unreachable (required fields are strings or phone) | Store null + warning (would create leads that break the "required" contract) |
| `invalid_phone` applies even if a phone field were optional | Spec lists it unconditionally; an invalid identity cannot be a lead | Warning only |
| Two columns mapping to the same field → leftmost **non-empty** value wins | Deterministic; doesn't lose data when the first column is blank | Leftmost always; error out |
| Unknown-disposition check only runs on **linked** events (orphan check comes first); codes match exactly (case-sensitive) | Spec's processing order is malformed → duplicate → orphan → unknown; `RENEWD` is a different code | Case-insensitive matching |
| An event whose `occurred_at` is missing or unparseable is counted as **malformed** | Cannot be ordered or dated, so it cannot be used safely | Keep it with a null time. Not reachable with the generator |
| `disposition.override` links via `call_id` **within the same workflow**, to the lead of that call's `call.initiated` (falling back to any other linked event of the call) | Spec: "the lead of that call's other events" | Global `call_id` lookup |
| A call's events count only if they link to the same lead as its `call.initiated`; a repeated `call.initiated` (different `event_id`) keeps the earliest time | Defensive; never happens in generated data | |
| Ties: same `occurred_at` → larger `event_id` wins; `current_disposition` ties on initiated time → larger `call_id`; `lead_report` ties on `external_ref` → creation order | Deterministic output for every input | Arrival order (would depend on file layout) |
| `batch_summary` lists batches that have ≥ 1 lead | It is a lead summary; a batch with only rejected rows shows up in `rejections.csv` | Emit zero rows for it |
| `rejections_by_reason` lists only reasons that occurred, sorted by key | Matches the example's shape | Pre-seed every possible reason with 0 |
| Dates accept 1–2 digit day/month; `Mon` = 3-letter English abbreviation, any case | Spec formats; day-first throughout | Full month names |
| Decimals: prefix `₹`, `Rs`, `Rs.`, `INR` (any case, optional space); commas stripped; no sign or exponent; 2 dp half-up | Spec | Validate Indian vs Western comma grouping (rejects real-world input for no gain) |
| Defaults are applied after merging, stored in `attributes_effective_json`; the pre-default merged values are kept in `attributes_json` | A later duplicate must still overwrite a null that was "defaulted" | Write the default into the merged value (would block later merges) |
| `last_call_at` at second precision | Matches the contract example | Keep fractional seconds |
| Drops must be ingested in chronological order | Spec defines `batch_id` and merge winner by ingest order; we record the order in `drops` | Sort drops by name automatically (done only when you pass a parent folder) |

## Failure modes considered
- **Re-runs:** same drop twice → no-op (content hash). Tested by snapshotting all tables.
- **Crash mid-ingest:** one transaction per drop → rolled back by SQLite; just re-run.
- **Crash mid-report:** each file is written to `*.tmp` and atomically renamed; never half-written.
- **Changed file under an existing batch name:** refused with a clear error; nothing committed.
- **Wrong or missing PII key:** fingerprint mismatch → hard error (otherwise every event would
  silently become an orphan).
- **Dirty leads:** every rule in the spec. Blank rows and BOMs are handled; short or long rows,
  extra or unknown columns ignored.
- **Dirty events:** invalid JSON, non-object JSON, missing `event_id`, bad timestamps,
  duplicates within and across drops, orphans, events before their lead, overrides without
  phone, unknown codes, blank lines.
- **Accidental ledger edits:** DB triggers reject UPDATE/DELETE on `events`.
- **Not handled:** concurrent `ingest` processes on one DB (SQLite serialises writers with
  `BEGIN IMMEDIATE`, but there is no queue or lock file); schema-file changes between runs are not
  versioned; no key rotation tooling (would need re-encryption and re-indexing of leads); an
  unknown workflow folder under `leads/` aborts the drop rather than quarantining it.

## Testing
- `python -m pytest`: **93 tests, all passing.**
  - `test_normalize.py`: every phone format the generator emits plus invalid cases (landline,
    US number, 9 digits, bad prefix, `N/A`); dates (all 4 formats, day-first, leap years,
    impossible dates); decimals (all prefixes, commas, negatives, words); enums; the 3 timestamp
    forms; the IST date rollover at midnight; rate rounding.
  - `test_derive.py`: effective disposition (override beats set, latest by `occurred_at` not
    arrival), every bucket and its precedence, override linking via `call_id`, orphan cases,
    unknown codes, IST dates in `daily_overview`.
  - `test_pipeline.py`: a hand-built 2-drop dataset with renamed and reordered columns, blank
    rows, duplicates across batches, an event arriving before its lead, a cross-drop duplicate,
    malformed lines, an unknown code and an override. **Expected report files were worked out by
    hand and are compared byte-for-byte.** Also tested: orphans resolve after drop 2;
    re-ingest is a no-op; incremental == one-shot (hand-built and real generator data); PII
    absent from the DB bytes but decryptable with the key; ledger append-only; conflicting batch
    refused atomically; wrong key rejected.
- **Ground-truth oracle** (`solution/tools/verify_against_generator.py`): independently computes
  the expected reports from the generator's internal truth (shares no code with the pipeline).
  **All three CSV reports and every data-quality counter match exactly for seeds 42, 7 and 2026
  (small) and seed 42 (large, 133k leads).** The README's own examples (`0.5625`,
  `2026-10-14T17:55:24+05:30`) also appear in our seed-42 output.
- Not tested: concurrent writers; a Postgres backend.

## Results
Seed 42, small: `lead_rows_total` 2057, rejected 108, merged 57, field warnings 122,
`events_total` 8745 (8 malformed, 237 duplicates, 462 orphans, 18 unknown dispositions).

**Timings.** Measured on a slow test machine: a pure-Python 5M-iteration loop takes 3.8 s there
versus about 0.3 s on a typical laptop, and disk writes are about 8× slower than in-memory.

| Dataset | ingest drop_1 | ingest drop_2 | report | total |
|---|---|---|---|---|
| small (2k leads, 8.7k events) | 0.4–3.4 s | 0.4–1.8 s | 0.2–0.8 s | < 6 s |
| large (144k lead rows, 633k events) | 63 s | 62 s | 46 s | **179 s** (re-ingest of both drops: < 1 s, no-op) |

Profiling showed the large-run time is dominated by SQLite B-tree inserts on random keys (HMAC
index, `event_id`) and by Python CPU. I raised the page cache to 256 MB and switched from WAL
to a rollback journal (1.5× faster for bulk single-writer loads). On a normal laptop
(≈10× this machine) this should land well inside the 2-minute target, but **I have not
measured that, so I don't claim it**.

**Surprising in the data:** after ingesting `drop_1` alone, **27%** of its accepted events
(1123 / 4160) are orphans, because batch_003's leads only arrive in drop 2 while many of their
calls fall in drop 1's time range. After drop 2 that falls to 462 / 8500 (~5%): only
rejected-lead and never-uploaded numbers remain. Some calls start
outside 09:00–21:00 IST, because the generator adds 3–70 h between attempts. Overrides can flip
a `renewed` lead to non-success, so `ever_success` must use effective dispositions, not raw
`disposition.set`.

## Stretch goals
- **Truly incremental:** drop 2 never re-reads or re-validates drop 1's files (content-hash
  skip, cached lead index per drop). *Trade-off:* `report` re-derives all facts from the whole
  ledger every time. That is O(total events), but it makes late-arriving leads, orphans and
  overrides correct by construction. At much larger scale I would materialise a `calls`
  table updated incrementally per affected `(workflow, call_id)` / lead, keeping the full
  re-derivation as a nightly consistency check.
- **Performance:** measured and profiled above.
- **Production at 100k leads/day:**
  - Run on Postgres, with one ingest job per drop triggered by object-store arrival (or a daily
    Airflow/Temporal schedule). The per-drop transaction and content-hash idempotency carry over
    unchanged, so retries are always safe.
  - Partition `events` by month, use `COPY` for bulk load, and keep the blind-index key in a
    KMS, never on disk.
  - For backfills, ingest the late drop and re-run `report`. Because reports are derived, a
    backfill can never leave stale aggregates. Version the schema files and record which version
    each file was validated against.
  - Alert on data-quality ratios (orphans, rejections, malformed) crossing thresholds per drop.

## If I had another day
1. Materialise derived `calls` and per-lead facts in SQL views or tables (report aggregation in
   SQL) to cut report time and let analysts query the same truth.
2. Postgres backend behind the same interface, plus a lock so two `ingest` processes can't race.
3. Key rotation command (decrypt with old key, re-encrypt and re-index with new) and moving the
   master key to KMS or env-only.
4. Faster bulk path: parse with multiprocessing and insert with `executemany` into an
   index-less staging table, then build indexes once.
5. A `quarantine` export (orphan and malformed lines per drop, PII-masked) for ops to chase
   upstream.
