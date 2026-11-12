"""SQLite storage: schema, connection and small query helpers.

Design (see SUBMISSION.md for the reasoning):

* ``source_files``  – every file ever ingested, keyed by content hash. This is what makes
  re-ingesting a drop a no-op.
* ``leads``         – one row per lead identity ``(workflow_id, phone_index)``. PII lives only
  as a keyed blind index (for matching) and a Fernet ciphertext (for recovery).
* ``lead_rows``     – audit log of every non-blank lead row and what happened to it
  (created / merged / rejected + reason). No PII. Rejections and counters derive from it.
* ``events``        – the **append-only interaction ledger**. Triggers forbid UPDATE/DELETE.
  Overrides are new rows; nothing is ever edited.
* ``event_duplicates`` / ``malformed_lines`` – quarantine for lines that never enter the
  ledger, kept so data-quality counters are derived (not incremented), hence idempotent.

Linking events to leads (and therefore orphan status) is deliberately **not** frozen at
ingest time: it is derived at report time from the ledger + current leads, so an event
that arrived before its lead is counted as soon as the lead exists.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA_VERSION = "1"

DDL = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS drops (
    drop_seq     INTEGER PRIMARY KEY AUTOINCREMENT,   -- ingest order
    source_path  TEXT NOT NULL,
    ingested_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS source_files (
    file_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    drop_seq     INTEGER NOT NULL REFERENCES drops(drop_seq),
    kind         TEXT NOT NULL CHECK (kind IN ('leads', 'events')),
    workflow_id  TEXT,                 -- leads only
    batch_id     TEXT,                 -- leads only: file stem, e.g. batch_001
    file_name    TEXT NOT NULL,
    sha256       TEXT NOT NULL,
    line_count   INTEGER NOT NULL,
    UNIQUE (kind, sha256)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_source_files_batch
    ON source_files (workflow_id, batch_id) WHERE kind = 'leads';

CREATE TABLE IF NOT EXISTS leads (
    lead_id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id              TEXT NOT NULL,
    phone_index              TEXT NOT NULL,   -- HMAC-SHA256 blind index of +91XXXXXXXXXX
    phone_last4              TEXT NOT NULL,   -- exactly what the masked report exposes
    external_ref             TEXT,
    batch_id                 TEXT NOT NULL,   -- first batch that created the lead; never changes
    attributes_json          TEXT NOT NULL,   -- merged non-PII fields, nulls kept (pre-default)
    attributes_effective_json TEXT NOT NULL,  -- same, with schema defaults applied
    pii_ciphertext           BLOB NOT NULL,   -- Fernet(JSON of all pii:true fields)
    created_file_id          INTEGER NOT NULL REFERENCES source_files(file_id),
    updated_file_id          INTEGER NOT NULL REFERENCES source_files(file_id),
    merge_count              INTEGER NOT NULL DEFAULT 0,
    UNIQUE (workflow_id, phone_index)
);

CREATE TABLE IF NOT EXISTS lead_rows (
    file_id         INTEGER NOT NULL REFERENCES source_files(file_id),
    row_number      INTEGER NOT NULL,
    outcome         TEXT NOT NULL CHECK (outcome IN ('created', 'merged', 'rejected')),
    reason          TEXT,
    field_warnings  INTEGER NOT NULL DEFAULT 0,
    lead_id         INTEGER REFERENCES leads(lead_id),
    PRIMARY KEY (file_id, row_number),
    CHECK ((outcome = 'rejected') = (reason IS NOT NULL)),
    CHECK ((outcome = 'rejected') = (lead_id IS NULL))
);

CREATE TABLE IF NOT EXISTS events (
    seq               INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id          TEXT NOT NULL UNIQUE,
    event_type        TEXT,
    workflow_id       TEXT,
    call_id           TEXT,
    occurred_at_us    INTEGER NOT NULL,     -- UTC epoch microseconds
    occurred_at_raw   TEXT NOT NULL,        -- exactly as received
    phone_status      TEXT NOT NULL CHECK (phone_status IN ('valid', 'invalid', 'absent')),
    phone_index       TEXT,                 -- blind index; NULL unless phone_status='valid'
    disposition_code  TEXT,                 -- payload.disposition_code, for disposition.* events
    payload_json      TEXT NOT NULL,
    file_id           INTEGER NOT NULL REFERENCES source_files(file_id),
    line_no           INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_events_phone ON events (workflow_id, phone_index);
CREATE INDEX IF NOT EXISTS ix_events_call ON events (workflow_id, call_id);

CREATE TRIGGER IF NOT EXISTS events_no_update BEFORE UPDATE ON events
BEGIN SELECT RAISE(ABORT, 'interaction ledger is append-only'); END;
CREATE TRIGGER IF NOT EXISTS events_no_delete BEFORE DELETE ON events
BEGIN SELECT RAISE(ABORT, 'interaction ledger is append-only'); END;

CREATE TABLE IF NOT EXISTS event_duplicates (
    file_id   INTEGER NOT NULL REFERENCES source_files(file_id),
    line_no   INTEGER NOT NULL,
    event_id  TEXT NOT NULL,
    PRIMARY KEY (file_id, line_no)
);

CREATE TABLE IF NOT EXISTS malformed_lines (
    file_id      INTEGER NOT NULL REFERENCES source_files(file_id),
    line_no      INTEGER NOT NULL,
    reason       TEXT NOT NULL,
    line_sha256  TEXT NOT NULL,             -- raw text not kept: it may contain PII
    PRIMARY KEY (file_id, line_no)
);
"""


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, isolation_level=None)  # explicit transactions only
    conn.execute("PRAGMA foreign_keys = ON")
    # Rollback journal (not WAL): one big transaction per drop, single writer, no concurrent
    # readers needed; measured ~1.5x faster than WAL for bulk loads. FULL = durable commits.
    conn.execute("PRAGMA journal_mode = DELETE")
    conn.execute("PRAGMA synchronous = FULL")
    # Random-key B-tree inserts (blind index, event_id) thrash the default 2 MB page cache
    # on large drops; 256 MB keeps the hot indexes in memory.
    conn.execute("PRAGMA cache_size = -262144")
    conn.execute("PRAGMA temp_store = MEMORY")
    conn.executescript(DDL)
    row = conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
    if row is None:
        conn.execute("INSERT INTO meta (key, value) VALUES ('schema_version', ?)", (SCHEMA_VERSION,))
    elif row[0] != SCHEMA_VERSION:
        raise RuntimeError(f"database schema version {row[0]} != expected {SCHEMA_VERSION}")
    return conn


def check_key_fingerprint(conn: sqlite3.Connection, fingerprint: str) -> None:
    """Refuse to mix PII keys within one database (would silently break linking)."""
    row = conn.execute("SELECT value FROM meta WHERE key = 'pii_key_fingerprint'").fetchone()
    if row is None:
        conn.execute("INSERT INTO meta (key, value) VALUES ('pii_key_fingerprint', ?)", (fingerprint,))
    elif row[0] != fingerprint:
        raise RuntimeError(
            "PII key does not match the key this database was created with; "
            "set AU_PII_KEY / key file to the original key")
