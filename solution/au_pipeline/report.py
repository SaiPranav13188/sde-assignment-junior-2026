"""Read the ledger + leads from the DB, derive facts, and write the five report files."""
from __future__ import annotations

import csv
import json
import os
import sqlite3
from pathlib import Path

from .derive import EventRec, LeadRec, build_reports, derive_calls
from .schema import Disposition

BATCH_COLS = ["workflow_id", "batch_id", "total_leads", "yet_to_reach_out",
              "in_progress_telephony_failed", "in_progress_dialed", "completed"]
DAILY_COLS = ["workflow_id", "date", "total_calls", "connected_calls", "connectivity_rate",
              "success_leads"]
LEAD_COLS = ["workflow_id", "external_ref", "batch_id", "phone_masked", "bucket", "total_calls",
             "connected_calls", "last_call_at", "current_disposition", "ever_success"]
REJECT_COLS = ["workflow_id", "batch_id", "row_number", "reason"]


def load_leads(conn: sqlite3.Connection) -> list[LeadRec]:
    return [LeadRec(*r) for r in conn.execute(
        "SELECT lead_id, workflow_id, phone_index, phone_last4, external_ref, batch_id"
        " FROM leads ORDER BY lead_id")]


def load_events(conn: sqlite3.Connection) -> list[EventRec]:
    return [EventRec(*r) for r in conn.execute(
        "SELECT event_id, event_type, workflow_id, call_id, occurred_at_us, phone_index,"
        " disposition_code FROM events ORDER BY seq")]


def load_rejections(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT f.workflow_id, f.batch_id, r.row_number, r.reason"
        " FROM lead_rows r JOIN source_files f USING (file_id)"
        " WHERE r.outcome = 'rejected'"
        " ORDER BY f.workflow_id, f.batch_id, r.row_number").fetchall()
    return [dict(zip(REJECT_COLS, r)) for r in rows]


def data_quality(conn: sqlite3.Connection, orphans: int, unknown: int) -> dict:
    one = lambda sql: conn.execute(sql).fetchone()[0]  # noqa: E731
    by_reason = dict(conn.execute(
        "SELECT reason, COUNT(*) FROM lead_rows WHERE outcome='rejected'"
        " GROUP BY reason ORDER BY reason"))
    accepted = one("SELECT COUNT(*) FROM events")
    malformed = one("SELECT COUNT(*) FROM malformed_lines")
    duplicates = one("SELECT COUNT(*) FROM event_duplicates")
    return {
        "lead_rows_total": one("SELECT COUNT(*) FROM lead_rows"),
        "lead_rows_rejected": one("SELECT COUNT(*) FROM lead_rows WHERE outcome='rejected'"),
        "rejections_by_reason": by_reason,
        "duplicate_lead_rows_merged": one("SELECT COUNT(*) FROM lead_rows WHERE outcome='merged'"),
        "field_warnings": one("SELECT COALESCE(SUM(field_warnings),0) FROM lead_rows"
                              " WHERE outcome <> 'rejected'"),
        "events_total": accepted + malformed + duplicates,
        "malformed_events": malformed,
        "duplicate_events_dropped": duplicates,
        "orphan_events": orphans,
        "unknown_disposition_events": unknown,
    }


def _write_csv(path: Path, cols: list[str], rows: list[dict]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
    os.replace(tmp, path)  # atomic: readers never see a half-written report


def write_reports(conn: sqlite3.Connection, catalogue: dict[str, dict[str, Disposition]],
                  out_dir: Path) -> dict:
    leads = load_leads(conn)
    derived = derive_calls(leads, load_events(conn), catalogue)
    batch_rows, daily_rows, lead_rows = build_reports(leads, derived, catalogue)
    dq = data_quality(conn, derived.orphan_events, derived.unknown_disposition_events)

    out_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(out_dir / "batch_summary.csv", BATCH_COLS, batch_rows)
    _write_csv(out_dir / "daily_overview.csv", DAILY_COLS, daily_rows)
    _write_csv(out_dir / "lead_report.csv", LEAD_COLS, lead_rows)
    _write_csv(out_dir / "rejections.csv", REJECT_COLS, load_rejections(conn))
    tmp = out_dir / "data_quality.json.tmp"
    tmp.write_text(json.dumps(dq, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, out_dir / "data_quality.json")
    return dq
