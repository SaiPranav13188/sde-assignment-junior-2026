# Assignment A: Interaction Ledger & Reporting Pipeline

Run everything from the **repository root**. See `../SUBMISSION.md` for the design and the decisions behind it.

```bash
pip install -r requirements.txt
python assignment-a-data/tools/generate_data.py --seed 42 --scale small --out data

python solution/pipeline.py ingest data/drop_1
python solution/pipeline.py ingest data/drop_2      # or: ingest data   (all drops, in order)
python solution/pipeline.py report --out reports/

python -m pytest                                    # all tests (config in ../pytest.ini)
```

Global options go **before** the sub-command:

| Option | Default | Meaning |
|---|---|---|
| `--db PATH` | `solution/.state/ledger.db` | SQLite database file |
| `--schemas DIR` | `assignment-a-data/schemas` | workflow schemas and `dispositions.csv` |
| `--key-file PATH` | `<db>.key` | PII master key, used only when `AU_PII_KEY` is not set |

To start from scratch, delete `solution/.state/`.

## Layout

```
au_pipeline/
  normalize.py   pure value rules: headers, phone, date, decimal, enum, timestamps, rate
  schema.py      workflow schemas + disposition catalogue
  crypto.py      PII vault: HMAC blind index + Fernet encryption
  db.py          SQLite schema (append-only ledger enforced by triggers)
  ingest.py      drop -> DB (validation, merging, dedupe, quarantine), one transaction per drop
  derive.py      pure derivation: linking, calls, effective disposition, buckets, report rows
  report.py      DB -> derive -> five report files (written atomically)
  cli.py         command-line interface
tests/           unit tests for every rule + end-to-end tests with hand-computed expected output
```
