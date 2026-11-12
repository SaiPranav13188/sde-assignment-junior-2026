"""Ingest one or more drops into the database.

Ordering (spec): drops in the order ingested, lead files in filename order, rows in file
order; event files in filename order, lines in file order.

Atomicity / idempotency:
* A whole drop is ingested inside a single transaction: a crash mid-drop leaves the DB
  exactly as before, and the drop can simply be re-run.
* Every file is recorded by SHA-256 of its bytes. A file already ingested is skipped, so
  re-ingesting a drop changes nothing (no extra rows, no extra counters).
* A lead file whose ``(workflow, batch_id)`` was already ingested with *different* content
  is refused: silently re-merging it would corrupt row numbers and batch attribution.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

from .crypto import PiiVault
from .normalize import (InvalidValue, normalize_phone, parse_date, parse_decimal, parse_enum,
                        parse_timestamp, to_epoch_us, try_normalize_phone)
from .schema import Field, WorkflowSchema

log = logging.getLogger(__name__)


class IngestError(RuntimeError):
    pass


@dataclass
class IngestStats:
    drops: int = 0
    files_ingested: int = 0
    files_skipped: int = 0
    lead_rows: int = 0
    events_lines: int = 0
    notes: list[str] = field(default_factory=list)


# --------------------------------------------------------------------------- discovery

def resolve_drops(path: Path) -> list[Path]:
    """A drop dir has ``leads/`` and/or ``events/``. A parent dir of drops is expanded
    (sorted by name) so ``ingest data`` == ``ingest data/drop_1 data/drop_2``."""
    if not path.is_dir():
        raise IngestError(f"{path} is not a directory")
    if (path / "leads").is_dir() or (path / "events").is_dir():
        return [path]
    children = sorted(p for p in path.iterdir() if p.is_dir()
                      and ((p / "leads").is_dir() or (p / "events").is_dir()))
    if not children:
        raise IngestError(f"{path} contains no drops (expected leads/ or events/ subfolders)")
    return children


# --------------------------------------------------------------------------- lead rows

def parse_field(f: Field, raw: str):
    """Normalise a non-empty value for its field type (raises InvalidValue)."""
    if f.type == "string":
        return raw
    if f.type == "phone":
        return normalize_phone(raw)
    if f.type == "date":
        return parse_date(raw).isoformat()
    if f.type == "decimal":
        return str(parse_decimal(raw))
    if f.type == "enum":
        return parse_enum(raw, list(f.values))
    raise IngestError(f"unsupported field type {f.type!r} for {f.name}")


@dataclass
class RowResult:
    values: dict[str, object] | None   # None when rejected
    reason: str | None
    warnings: int


def validate_row(schema: WorkflowSchema, raw: dict[str, str]) -> RowResult:
    """Walk fields in schema order; the first failure is the row's single reason."""
    values: dict[str, object] = {}
    warnings = 0
    for f in schema.fields:
        v = (raw.get(f.name) or "").strip()
        if not v:
            if f.required:
                return RowResult(None, f"missing_required:{f.name}", 0)
            values[f.name] = None
            continue
        try:
            values[f.name] = parse_field(f, v)
        except InvalidValue:
            if f.type == "phone":
                return RowResult(None, "invalid_phone", 0)
            if f.required:  # not reachable with the given schemas; see SUBMISSION.md
                return RowResult(None, f"invalid:{f.name}", 0)
            values[f.name] = None
            warnings += 1
    return RowResult(values, None, warnings)


def header_mapping(schema: WorkflowSchema, header: list[str]) -> list[tuple[int, str]]:
    """(column index, field name) for every header that matches a field, left to right.
    Unmatched columns are ignored."""
    return [(i, name) for i, h in enumerate(header)
            if (name := schema.field_for_header(h)) is not None]


def map_row(mapping: list[tuple[int, str]], cells: list[str]) -> dict[str, str]:
    """Header-mapped raw values. If two columns map to one field, the leftmost
    non-empty one wins."""
    out: dict[str, str] = {}
    n = len(cells)
    for i, name in mapping:
        if i < n and not out.get(name, "").strip():
            out[name] = cells[i]
    return out


class LeadStore:
    """Per-workflow view of existing leads used while ingesting (cached per drop)."""

    def __init__(self, conn: sqlite3.Connection, vault: PiiVault, schema: WorkflowSchema):
        self.conn, self.vault, self.schema = conn, vault, schema
        self.by_index: dict[str, int] = dict(conn.execute(
            "SELECT phone_index, lead_id FROM leads WHERE workflow_id = ?", (schema.workflow_id,)))
        self.pii_fields = [f.name for f in schema.fields if f.pii]
        self.plain_fields = [f.name for f in schema.fields if not f.pii]
        self.defaults = {f.name: f.default for f in schema.fields if f.default is not None}

    def _effective(self, attrs: dict) -> dict:
        return {k: (v if v is not None else self.defaults.get(k)) for k, v in attrs.items()}

    def upsert(self, values: dict, batch_id: str, file_id: int) -> tuple[str, int]:
        phone = values[self.schema.phone_field]
        idx = self.vault.blind_index(phone)
        ref_field = self.schema.reference_field
        lead_id = self.by_index.get(idx)
        if lead_id is None:
            attrs = {k: values[k] for k in self.plain_fields}
            pii = {k: values[k] for k in self.pii_fields}
            cur = self.conn.execute(
                "INSERT INTO leads (workflow_id, phone_index, phone_last4, external_ref, batch_id,"
                " attributes_json, attributes_effective_json, pii_ciphertext,"
                " created_file_id, updated_file_id) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (self.schema.workflow_id, idx, phone[-4:],
                 values.get(ref_field) if ref_field else None, batch_id,
                 json.dumps(attrs, sort_keys=True), json.dumps(self._effective(attrs), sort_keys=True),
                 self.vault.encrypt_json(pii), file_id, file_id))
            lead_id = cur.lastrowid
            self.by_index[idx] = lead_id
            return "created", lead_id

        # merged duplicate: overwrite only fields whose new value is non-null (last write wins)
        attrs_json, pii_ct = self.conn.execute(
            "SELECT attributes_json, pii_ciphertext FROM leads WHERE lead_id = ?", (lead_id,)).fetchone()
        attrs, pii = json.loads(attrs_json), self.vault.decrypt_json(pii_ct)
        for k in self.plain_fields:
            if values[k] is not None:
                attrs[k] = values[k]
        for k in self.pii_fields:
            if values[k] is not None:
                pii[k] = values[k]
        self.conn.execute(
            "UPDATE leads SET external_ref = ?, attributes_json = ?, attributes_effective_json = ?,"
            " pii_ciphertext = ?, updated_file_id = ?, merge_count = merge_count + 1"
            " WHERE lead_id = ?",
            (attrs.get(ref_field) if ref_field else None, json.dumps(attrs, sort_keys=True),
             json.dumps(self._effective(attrs), sort_keys=True), self.vault.encrypt_json(pii),
             file_id, lead_id))
        return "merged", lead_id


def _is_blank(cells: list[str]) -> bool:
    return all(not c.strip() for c in cells)


def ingest_lead_file(conn, store: LeadStore, path: Path, file_id: int) -> int:
    text = path.read_bytes().decode("utf-8-sig")
    reader = csv.reader(io.StringIO(text, newline=""))
    header = next(reader, None)
    if header is None:
        return 0
    batch_id = path.stem
    mapping = header_mapping(store.schema, header)
    row_number = 0
    rows = []
    for cells in reader:
        if _is_blank(cells):
            continue  # blank rows are skipped and not counted
        row_number += 1
        result = validate_row(store.schema, map_row(mapping, cells))
        if result.reason is not None:
            rows.append((file_id, row_number, "rejected", result.reason, 0, None))
            continue
        outcome, lead_id = store.upsert(result.values, batch_id, file_id)
        rows.append((file_id, row_number, outcome, None, result.warnings, lead_id))
    conn.executemany(
        "INSERT INTO lead_rows (file_id, row_number, outcome, reason, field_warnings, lead_id)"
        " VALUES (?,?,?,?,?,?)", rows)
    return row_number


# --------------------------------------------------------------------------- events

def classify_event_line(line: str):
    """Returns ('malformed', reason) or ('ok', parsed_event_dict, epoch_us)."""
    try:
        obj = json.loads(line)
    except (json.JSONDecodeError, ValueError):
        return ("malformed", "invalid_json")
    if not isinstance(obj, dict):
        return ("malformed", "not_an_object")
    eid = obj.get("event_id")
    if isinstance(eid, int) and not isinstance(eid, bool):
        eid = str(eid)
    if not isinstance(eid, str) or not eid.strip():
        return ("malformed", "missing_event_id")
    obj["event_id"] = eid.strip()
    try:
        ts = to_epoch_us(parse_timestamp(obj.get("occurred_at")))
    except InvalidValue:
        return ("malformed", "invalid_occurred_at")
    return ("ok", obj, ts)


class EventIngestor:
    def __init__(self, conn: sqlite3.Connection, vault: PiiVault):
        self.conn, self.vault = conn, vault
        self.seen: set[str] = {r[0] for r in conn.execute("SELECT event_id FROM events")}
        self._index_cache: dict[str, str] = {}

    def _phone(self, raw) -> tuple[str, str | None]:
        if raw is None or (isinstance(raw, str) and not raw.strip()):
            return "absent", None
        canonical = try_normalize_phone(raw)
        if canonical is None:
            return "invalid", None
        idx = self._index_cache.get(canonical)
        if idx is None:
            idx = self._index_cache[canonical] = self.vault.blind_index(canonical)
        return "valid", idx

    def ingest_file(self, path: Path, file_id: int) -> int:
        events, dups, bad = [], [], []
        n_lines = 0
        with open(path, encoding="utf-8-sig", newline="") as fh:
            for line_no, line in enumerate(fh, start=1):
                if not line.strip():
                    continue  # events_total counts non-empty lines only
                n_lines += 1
                res = classify_event_line(line)
                if res[0] == "malformed":
                    bad.append((file_id, line_no, res[1],
                                hashlib.sha256(line.encode("utf-8")).hexdigest()))
                    continue
                _, obj, ts = res
                eid = obj["event_id"]
                if eid in self.seen:
                    dups.append((file_id, line_no, eid))
                    continue
                self.seen.add(eid)
                status, idx = self._phone(obj.get("phone"))
                payload = obj.get("payload")
                if not isinstance(payload, dict):
                    payload = {}
                etype = obj.get("event_type")
                code = payload.get("disposition_code") if isinstance(etype, str) \
                    and etype.startswith("disposition.") else None
                events.append((
                    eid, _str_or_none(etype), _str_or_none(obj.get("workflow_id")),
                    _str_or_none(obj.get("call_id")), ts, str(obj.get("occurred_at")),
                    status, idx, _str_or_none(code),
                    json.dumps(payload, sort_keys=True, ensure_ascii=False), file_id, line_no))
        self.conn.executemany(
            "INSERT INTO events (event_id, event_type, workflow_id, call_id, occurred_at_us,"
            " occurred_at_raw, phone_status, phone_index, disposition_code, payload_json,"
            " file_id, line_no) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", events)
        self.conn.executemany(
            "INSERT INTO event_duplicates (file_id, line_no, event_id) VALUES (?,?,?)", dups)
        self.conn.executemany(
            "INSERT INTO malformed_lines (file_id, line_no, reason, line_sha256) VALUES (?,?,?,?)", bad)
        return n_lines


def _str_or_none(v) -> str | None:
    if v is None:
        return None
    return v if isinstance(v, str) else str(v)


# --------------------------------------------------------------------------- drops

def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def ingest_drop(conn: sqlite3.Connection, vault: PiiVault, schemas: dict[str, WorkflowSchema],
                drop: Path, stats: IngestStats) -> None:
    lead_files: list[tuple[str, Path]] = []
    leads_dir = drop / "leads"
    if leads_dir.is_dir():
        for wf_dir in sorted(p for p in leads_dir.iterdir() if p.is_dir()):
            if wf_dir.name not in schemas:
                raise IngestError(f"no schema for workflow {wf_dir.name!r} ({wf_dir})")
            lead_files += [(wf_dir.name, f) for f in sorted(wf_dir.glob("*.csv"), key=lambda p: p.name)]
    events_dir = drop / "events"
    event_files = sorted(events_dir.glob("*.jsonl"), key=lambda p: p.name) if events_dir.is_dir() else []

    conn.execute("BEGIN IMMEDIATE")
    try:
        drop_seq = None
        stores: dict[str, LeadStore] = {}
        event_ingestor: EventIngestor | None = None

        def new_file(kind, wf, batch, path, digest) -> int:
            nonlocal drop_seq
            if drop_seq is None:
                drop_seq = conn.execute("INSERT INTO drops (source_path) VALUES (?)",
                                        (str(drop),)).lastrowid
            return conn.execute(
                "INSERT INTO source_files (drop_seq, kind, workflow_id, batch_id, file_name,"
                " sha256, line_count) VALUES (?,?,?,?,?,?,0)",
                (drop_seq, kind, wf, batch, path.name, digest)).lastrowid

        for wf, path in lead_files:
            digest = _sha256(path)
            prior = conn.execute(
                "SELECT sha256 FROM source_files WHERE kind='leads' AND workflow_id=? AND batch_id=?",
                (wf, path.stem)).fetchone()
            if prior is not None:
                if prior[0] != digest:
                    raise IngestError(
                        f"{wf}/{path.name}: batch {path.stem} was already ingested with different "
                        "content; refusing to re-merge it")
                stats.files_skipped += 1
                continue
            if conn.execute("SELECT 1 FROM source_files WHERE kind='leads' AND sha256=?",
                            (digest,)).fetchone():
                stats.notes.append(f"{wf}/{path.name}: identical content already ingested "
                                   "under another batch name; skipped")
                stats.files_skipped += 1
                continue
            file_id = new_file("leads", wf, path.stem, path, digest)
            store = stores.get(wf) or stores.setdefault(wf, LeadStore(conn, vault, schemas[wf]))
            n = ingest_lead_file(conn, store, path, file_id)
            conn.execute("UPDATE source_files SET line_count=? WHERE file_id=?", (n, file_id))
            stats.files_ingested += 1
            stats.lead_rows += n

        for path in event_files:
            digest = _sha256(path)
            if conn.execute("SELECT 1 FROM source_files WHERE kind='events' AND sha256=?",
                            (digest,)).fetchone():
                stats.files_skipped += 1
                continue
            file_id = new_file("events", None, None, path, digest)
            event_ingestor = event_ingestor or EventIngestor(conn, vault)
            n = event_ingestor.ingest_file(path, file_id)
            conn.execute("UPDATE source_files SET line_count=? WHERE file_id=?", (n, file_id))
            stats.files_ingested += 1
            stats.events_lines += n

        conn.execute("COMMIT")
        if drop_seq is not None:
            stats.drops += 1
    except BaseException:
        conn.execute("ROLLBACK")
        raise
