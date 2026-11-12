"""Command-line interface.

    python solution/pipeline.py ingest data/drop_1 [data/drop_2 ...]
    python solution/pipeline.py report --out reports/

Global options (before the sub-command): --db, --schemas, --key-file.
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

from .crypto import PiiVault
from .db import check_key_fingerprint, connect
from .ingest import IngestError, IngestStats, ingest_drop, resolve_drops
from .report import write_reports
from .schema import load_dispositions, load_schemas

SOLUTION_DIR = Path(__file__).resolve().parent.parent
REPO_ROOT = SOLUTION_DIR.parent
DEFAULT_DB = SOLUTION_DIR / ".state" / "ledger.db"
DEFAULT_SCHEMAS = REPO_ROOT / "assignment-a-data" / "schemas"


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pipeline", description="Interaction ledger & reporting pipeline")
    p.add_argument("--db", type=Path, default=DEFAULT_DB, help=f"SQLite file (default {DEFAULT_DB})")
    p.add_argument("--schemas", type=Path, default=DEFAULT_SCHEMAS,
                   help="folder with <workflow>.json schemas and dispositions.csv")
    p.add_argument("--key-file", type=Path, default=None,
                   help="PII master key file, used when AU_PII_KEY is unset (default: <db>.key)")
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="command", required=True)
    ing = sub.add_parser("ingest", help="ingest one or more drops (idempotent)")
    ing.add_argument("drops", nargs="+", type=Path,
                     help="drop folders, or a parent folder containing drop_* folders")
    rep = sub.add_parser("report", help="write the five report files")
    rep.add_argument("--out", type=Path, required=True)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(levelname)s %(message)s")
    log = logging.getLogger("pipeline")

    vault = PiiVault.load(args.key_file or args.db.with_name(args.db.name + ".key"))
    conn = connect(args.db)
    try:
        check_key_fingerprint(conn, vault.fingerprint)
        if args.command == "ingest":
            schemas = load_schemas(args.schemas)
            for d in args.drops:
                for drop in resolve_drops(d):
                    t0 = time.perf_counter()
                    stats = IngestStats()
                    ingest_drop(conn, vault, schemas, drop, stats)
                    for note in stats.notes:
                        log.warning(note)
                    log.info("ingested %s: %d new file(s), %d already ingested, %d lead rows, "
                             "%d event lines (%.2fs)", drop, stats.files_ingested,
                             stats.files_skipped, stats.lead_rows, stats.events_lines,
                             time.perf_counter() - t0)
        else:
            t0 = time.perf_counter()
            dq = write_reports(conn, load_dispositions(args.schemas), args.out)
            log.info("reports written to %s (%.2fs): %d lead rows, %d events, %d orphans",
                     args.out, time.perf_counter() - t0, dq["lead_rows_total"],
                     dq["events_total"], dq["orphan_events"])
    except IngestError as e:
        log.error("%s", e)
        return 2
    finally:
        conn.close()
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
