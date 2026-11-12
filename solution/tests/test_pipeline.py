"""End-to-end tests through the CLI on a hand-built two-drop dataset whose expected
reports were worked out by hand, plus idempotency / incrementality / PII checks."""
import json
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from au_pipeline.cli import main
from au_pipeline.crypto import PiiVault

REPO = Path(__file__).resolve().parents[2]

BATCH_001 = """customer_name,phone,policy_number,policy_expiry,premium_amount,preferred_language
Asha K,+91 98765 43210,POL1,2026-11-21,"₹27,554",HI
,9876500001,POL2,,,
Bad Phone,12345,POL3,,,
,,,,,
Ravi,98765 00002,,,,
Meena,8765400003,POL5,31/02/2026,N/A,Hindi
Asha Dup,09876543210,POL1,,Rs. 30000,
  ,bad,,,,
"""

# renamed + re-ordered columns, an extra column, and a lead whose events arrived in drop 1
BATCH_003 = """Language,Insured Name,Policy No,Mobile No,Expiry Date,Premium,agent_notes
ta,Chitra,POL7,7000000004,01-Dec-2026,INR 100.5,VIP
,Meena Two,POL5,(+91) 8765400003,,,
"""


def e(eid, etype, call, at, phone=None, **payload):
    d = {"event_id": eid, "event_type": etype, "workflow_id": "motor_renewal", "call_id": call,
         "occurred_at": at}
    if phone is not None:
        d["phone"] = phone
    d["payload"] = payload
    return json.dumps(d)


EVENTS_1 = "\n".join([
    e("e1", "call.initiated", "c1", "2026-10-01 10:00:00", "919876543210"),          # naive = IST
    e("e2", "call.connected", "c1", "2026-10-01T04:30:10Z", "+91 98765 43210"),
    e("e3", "disposition.set", "c1", "2026-10-01T10:05:00+05:30", "9876543210",
      disposition_code="renewed"),
    e("e1", "call.initiated", "c1", "2026-10-01 10:00:00", "919876543210"),          # duplicate
    '{"event_id": "e9", "event_ty',                                                   # malformed
    '{"event_type": "call.initiated"}',                                               # no event_id
    "",                                                                                # blank: not counted
    e("e4", "call.initiated", "c2", "2026-10-01T20:00:00+05:30", "7000000004"),       # lead arrives later
    e("e5", "call.telephony_failed", "c2", "2026-10-01T20:00:03+05:30", "7000000004",
      reason="sip_503"),
    e("e7", "call.initiated", "c3", "2026-10-01T11:00:00+05:30", "7999999999"),       # never uploaded
    e("e8", "disposition.set", "c1", "2026-10-01T10:06:00+05:30", "9876543210",
      disposition_code="RENEWD"),                                                     # unknown code
]) + "\n"

EVENTS_2 = "\n".join([
    e("e1", "call.initiated", "c1", "2026-10-01 10:00:00", "919876543210"),          # dup across drops
    e("e10", "disposition.override", "c1", "2026-10-02T09:00:00+05:30",
      disposition_code="not_interested", overridden_by="supervisor_1", reason="qa_review"),
    e("e11", "call.initiated", "c4", "2026-10-02T04:00:00Z", "8765400003"),
    e("e12", "call.telephony_failed", "c4", "2026-10-02T04:00:05Z", "8765400003", reason="x"),
]) + "\n"


def write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


@pytest.fixture
def drops(tmp_path):
    root = tmp_path / "data"
    write(root / "drop_1" / "leads" / "motor_renewal" / "batch_001.csv", BATCH_001)
    write(root / "drop_1" / "events" / "events_0001.jsonl", EVENTS_1)
    write(root / "drop_2" / "leads" / "motor_renewal" / "batch_003.csv", BATCH_003)
    write(root / "drop_2" / "events" / "events_1001.jsonl", EVENTS_2)
    return root


def run(tmp_path, *args, db="ledger.db"):
    base = ["--db", str(tmp_path / db), "--key-file", str(tmp_path / "pii.key")]
    assert main(base + list(args)) == 0


def read_reports(out: Path) -> dict[str, str]:
    return {p.name: p.read_text(encoding="utf-8") for p in sorted(out.iterdir())}


EXPECTED = {
    "batch_summary.csv": (
        "workflow_id,batch_id,total_leads,yet_to_reach_out,in_progress_telephony_failed,"
        "in_progress_dialed,completed\n"
        "motor_renewal,batch_001,2,0,1,0,1\n"
        "motor_renewal,batch_003,1,0,1,0,0\n"),
    "daily_overview.csv": (
        "workflow_id,date,total_calls,connected_calls,connectivity_rate,success_leads\n"
        "motor_renewal,2026-10-01,2,1,0.5000,0\n"
        "motor_renewal,2026-10-02,1,0,0.0000,0\n"),
    "lead_report.csv": (
        "workflow_id,external_ref,batch_id,phone_masked,bucket,total_calls,connected_calls,"
        "last_call_at,current_disposition,ever_success\n"
        "motor_renewal,POL1,batch_001,+91******3210,completed,1,1,2026-10-01T10:00:00+05:30,"
        "not_interested,false\n"
        "motor_renewal,POL5,batch_001,+91******0003,in_progress_telephony_failed,1,0,"
        "2026-10-02T09:30:00+05:30,,false\n"
        "motor_renewal,POL7,batch_003,+91******0004,in_progress_telephony_failed,1,0,"
        "2026-10-01T20:00:00+05:30,,false\n"),
    "rejections.csv": (
        "workflow_id,batch_id,row_number,reason\n"
        "motor_renewal,batch_001,2,missing_required:customer_name\n"
        "motor_renewal,batch_001,3,invalid_phone\n"
        "motor_renewal,batch_001,4,missing_required:policy_number\n"
        "motor_renewal,batch_001,7,missing_required:customer_name\n"),
}
EXPECTED_DQ = {
    "lead_rows_total": 9,
    "lead_rows_rejected": 4,
    "rejections_by_reason": {"invalid_phone": 1, "missing_required:customer_name": 2,
                             "missing_required:policy_number": 1},
    "duplicate_lead_rows_merged": 2,
    "field_warnings": 3,
    "events_total": 14,
    "malformed_events": 2,
    "duplicate_events_dropped": 2,
    "orphan_events": 1,
    "unknown_disposition_events": 1,
}


def test_full_pipeline_exact_output(tmp_path, drops):
    run(tmp_path, "ingest", str(drops / "drop_1"))
    run(tmp_path, "ingest", str(drops / "drop_2"))
    run(tmp_path, "report", "--out", str(tmp_path / "out"))
    got = read_reports(tmp_path / "out")
    for name, text in EXPECTED.items():
        assert got[name] == text, name
    assert json.loads(got["data_quality.json"]) == EXPECTED_DQ


def test_orphans_reflect_state_so_far_and_resolve_when_lead_arrives(tmp_path, drops):
    run(tmp_path, "ingest", str(drops / "drop_1"))
    run(tmp_path, "report", "--out", str(tmp_path / "after1"))
    dq1 = json.loads((tmp_path / "after1" / "data_quality.json").read_text())
    assert dq1["orphan_events"] == 3          # e4, e5 (lead not yet uploaded) + e7
    lead_report = (tmp_path / "after1" / "lead_report.csv").read_text()
    assert "POL7" not in lead_report

    run(tmp_path, "ingest", str(drops / "drop_2"))
    run(tmp_path, "report", "--out", str(tmp_path / "after2"))
    assert json.loads((tmp_path / "after2" / "data_quality.json").read_text())["orphan_events"] == 1


def test_reingest_changes_nothing_and_one_shot_equals_incremental(tmp_path, drops):
    run(tmp_path, "ingest", str(drops / "drop_1"), db="a.db")
    db = sqlite3.connect(tmp_path / "a.db")
    tables = ["leads", "lead_rows", "events", "event_duplicates", "malformed_lines", "drops",
              "source_files"]
    snapshot = {t: db.execute(f"SELECT * FROM {t}").fetchall() for t in tables}
    run(tmp_path, "ingest", str(drops / "drop_1"), db="a.db")   # re-run: no-op
    assert {t: db.execute(f"SELECT * FROM {t}").fetchall() for t in tables} == snapshot
    db.close()
    run(tmp_path, "ingest", str(drops / "drop_2"), str(drops / "drop_1"), str(drops / "drop_2"),
        db="a.db")
    run(tmp_path, "report", "--out", str(tmp_path / "a"), db="a.db")

    run(tmp_path, "ingest", str(drops), db="b.db")               # everything at once
    run(tmp_path, "report", "--out", str(tmp_path / "b"), db="b.db")

    assert read_reports(tmp_path / "a") == read_reports(tmp_path / "b")
    assert json.loads(read_reports(tmp_path / "a")["data_quality.json"]) == EXPECTED_DQ


def test_pii_is_not_stored_in_plaintext_but_is_recoverable(tmp_path, drops):
    run(tmp_path, "ingest", str(drops))
    raw = (tmp_path / "ledger.db").read_bytes()
    for needle in ["Asha", "Meena", "Chitra", "9876543210", "8765400003", "7000000004",
                   "7999999999"]:
        assert needle.encode() not in raw, needle

    vault = PiiVault((tmp_path / "pii.key").read_text().strip().encode())
    db = sqlite3.connect(tmp_path / "ledger.db")
    rows = {ref: (vault.decrypt_json(ct), json.loads(attrs)) for ref, ct, attrs in db.execute(
        "SELECT external_ref, pii_ciphertext, attributes_effective_json FROM leads")}
    # later duplicate rows overwrite only non-null fields; defaults apply after merging
    assert rows["POL1"][0] == {"customer_name": "Asha Dup", "phone": "+919876543210"}
    assert rows["POL1"][1]["premium_amount"] == "30000.00"
    assert rows["POL1"][1]["preferred_language"] == "hi"
    assert rows["POL1"][1]["policy_expiry"] == "2026-11-21"
    assert rows["POL5"][0]["customer_name"] == "Meena Two"
    assert rows["POL5"][1]["preferred_language"] == "en"           # default after merge
    assert rows["POL5"][1]["premium_amount"] is None                # invalid -> null + warning


def test_ledger_is_append_only(tmp_path, drops):
    run(tmp_path, "ingest", str(drops))
    db = sqlite3.connect(tmp_path / "ledger.db")
    with pytest.raises(sqlite3.DatabaseError, match="append-only"):
        db.execute("UPDATE events SET disposition_code = 'renewed'")
    with pytest.raises(sqlite3.DatabaseError, match="append-only"):
        db.execute("DELETE FROM events")


def test_changed_batch_content_is_refused_atomically(tmp_path, drops):
    run(tmp_path, "ingest", str(drops / "drop_1"))
    bad = tmp_path / "bad_drop"
    write(bad / "leads" / "motor_renewal" / "batch_001.csv", BATCH_001 + "Zed,9000000009,POL9,,,\n")
    write(bad / "events" / "events_9999.jsonl", e("e99", "call.initiated", "c9",
                                                  "2026-10-01T10:00:00Z", "9000000009") + "\n")
    base = ["--db", str(tmp_path / "ledger.db"), "--key-file", str(tmp_path / "pii.key")]
    assert main(base + ["ingest", str(bad)]) == 2
    db = sqlite3.connect(tmp_path / "ledger.db")
    assert db.execute("SELECT COUNT(*) FROM events WHERE event_id='e99'").fetchone()[0] == 0


def test_wrong_pii_key_is_rejected(tmp_path, drops, monkeypatch):
    run(tmp_path, "ingest", str(drops / "drop_1"))
    monkeypatch.setenv("AU_PII_KEY", "a-completely-different-secret-key")
    with pytest.raises(RuntimeError, match="PII key"):
        main(["--db", str(tmp_path / "ledger.db"), "ingest", str(drops / "drop_2")])


@pytest.mark.parametrize("seed", [3])
def test_generated_data_incremental_equals_one_shot(tmp_path, seed):
    """Real generator output: drop-by-drop (with a re-run) == everything at once."""
    data = tmp_path / "gen"
    subprocess.run([sys.executable, str(REPO / "assignment-a-data" / "tools" / "generate_data.py"),
                    "--seed", str(seed), "--scale", "small", "--out", str(data)],
                   check=True, capture_output=True)
    run(tmp_path, "ingest", str(data / "drop_1"), db="inc.db")
    run(tmp_path, "ingest", str(data / "drop_1"), str(data / "drop_2"), db="inc.db")
    run(tmp_path, "report", "--out", str(tmp_path / "inc"), db="inc.db")
    run(tmp_path, "ingest", str(data), db="all.db")
    run(tmp_path, "report", "--out", str(tmp_path / "all"), db="all.db")
    inc, one = read_reports(tmp_path / "inc"), read_reports(tmp_path / "all")
    assert inc == one
    dq = json.loads(inc["data_quality.json"])
    assert dq["events_total"] == (dq["malformed_events"] + dq["duplicate_events_dropped"]
                                  + _accepted_events(tmp_path / "inc.db"))
    assert "+91" not in inc["rejections.csv"]          # rejections carry no PII


def _accepted_events(db_path: Path) -> int:
    db = sqlite3.connect(db_path)
    try:
        return db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    finally:
        db.close()
