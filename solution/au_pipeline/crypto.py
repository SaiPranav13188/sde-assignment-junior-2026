"""PII protection at rest.

Two keys are derived from one master secret:

* **blind-index key** -> HMAC-SHA256(canonical phone). Deterministic, so events can be
  matched to leads with an indexed equality lookup, but the phone cannot be recovered
  from the database without the key (a plain unsalted SHA-256 of a 10-digit number would
  be brute-forced in seconds; a keyed HMAC is not).
* **encryption key** -> Fernet (AES-128-CBC + HMAC-SHA256, random IV) for the full PII
  values (name, phone), so they stay recoverable for operations that legitimately need
  them (e.g. actually dialling), but are unreadable at rest.

The master secret comes from ``AU_PII_KEY`` or, for local runs, a key file created on
first use next to the database. A fingerprint of the key is stored in the DB so a run with
the wrong key fails loudly instead of silently producing zero matches.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
from pathlib import Path

from cryptography.fernet import Fernet

ENV_VAR = "AU_PII_KEY"


def _derive(master: bytes, label: bytes) -> bytes:
    return hmac.new(master, label, hashlib.sha256).digest()


class PiiVault:
    def __init__(self, master_secret: bytes):
        if len(master_secret) < 16:
            raise ValueError("PII master secret must be at least 16 bytes")
        self._index_key = _derive(master_secret, b"au/blind-index/v1")
        self._fernet = Fernet(base64.urlsafe_b64encode(_derive(master_secret, b"au/encrypt/v1")))
        self.fingerprint = _derive(master_secret, b"au/fingerprint/v1").hex()[:16]

    @classmethod
    def load(cls, key_file: Path) -> "PiiVault":
        env = os.environ.get(ENV_VAR)
        if env:
            return cls(env.encode("utf-8"))
        if not key_file.exists():
            key_file.parent.mkdir(parents=True, exist_ok=True)
            key_file.write_text(secrets.token_urlsafe(32), encoding="utf-8")
            try:
                os.chmod(key_file, 0o600)
            except OSError:  # pragma: no cover - best effort on Windows
                pass
        return cls(key_file.read_text(encoding="utf-8").strip().encode("utf-8"))

    def blind_index(self, canonical_phone: str) -> str:
        return hmac.new(self._index_key, canonical_phone.encode("utf-8"), hashlib.sha256).hexdigest()

    def encrypt_json(self, obj: dict) -> bytes:
        return self._fernet.encrypt(json.dumps(obj, sort_keys=True, ensure_ascii=False).encode("utf-8"))

    def decrypt_json(self, token: bytes) -> dict:
        return json.loads(self._fernet.decrypt(token).decode("utf-8"))

    def encrypt_text(self, text: str) -> bytes:
        return self._fernet.encrypt(text.encode("utf-8"))
