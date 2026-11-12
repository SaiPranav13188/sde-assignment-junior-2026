"""Pure value-normalisation rules from the spec.

Every function here is side-effect free and is the single place a rule is implemented,
so the unit tests in tests/test_normalize.py pin the behaviour exactly.

Convention: parsers return the normalised value, or raise ``InvalidValue`` when the
(non-empty) input is present but invalid. Callers decide what "invalid" means for a row
(rejection vs field warning).
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation

IST = timezone(timedelta(hours=5, minutes=30), name="IST")


class InvalidValue(ValueError):
    """A present (non-empty) value that fails its type's validation."""


# --------------------------------------------------------------------------- headers

_HEADER_SEP = re.compile(r"[_\s]+")


def normalize_header(h: str) -> str:
    """lower-case, trim, and collapse every run of '_' / whitespace into one space."""
    return _HEADER_SEP.sub(" ", h.strip().lower()).strip()


# --------------------------------------------------------------------------- phone

_NON_DIGIT = re.compile(r"\D")


def normalize_phone(raw: str) -> str:
    """Return the canonical ``+91XXXXXXXXXX`` form of an Indian mobile number.

    Strip every non-digit; 12 digits starting with 91 -> drop 91; 11 digits starting
    with 0 -> drop 0. Valid only if exactly 10 digits starting with 6/7/8/9.
    """
    digits = _NON_DIGIT.sub("", raw or "")
    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith("0"):
        digits = digits[1:]
    if len(digits) != 10 or digits[0] not in "6789":
        raise InvalidValue("invalid_phone")
    return "+91" + digits


def try_normalize_phone(raw: object) -> str | None:
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        return normalize_phone(raw)
    except InvalidValue:
        return None


def mask_phone(canonical_last4: str) -> str:
    return "+91******" + canonical_last4


# --------------------------------------------------------------------------- date

_MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], start=1)}

_ISO_DATE = re.compile(r"^(\d{4})-(\d{1,2})-(\d{1,2})$")
_DMY_SLASH = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")
_DMY_DASH = re.compile(r"^(\d{1,2})-(\d{1,2})-(\d{4})$")
_DMON = re.compile(r"^(\d{1,2})-([A-Za-z]{3})-(\d{4})$")


def parse_date(raw: str) -> date:
    """Accept YYYY-MM-DD, DD/MM/YYYY, DD-MM-YYYY, DD-Mon-YYYY (day-first, strict)."""
    s = raw.strip()
    try:
        if m := _ISO_DATE.match(s):
            return date(int(m[1]), int(m[2]), int(m[3]))
        if m := (_DMY_SLASH.match(s) or _DMY_DASH.match(s)):
            return date(int(m[3]), int(m[2]), int(m[1]))
        if m := _DMON.match(s):
            month = _MONTHS.get(m[2].lower())
            if month is None:
                raise InvalidValue("invalid_date")
            return date(int(m[3]), month, int(m[1]))
    except ValueError as e:  # impossible dates such as 31/02 or month 13
        raise InvalidValue("invalid_date") from e
    raise InvalidValue("invalid_date")


# --------------------------------------------------------------------------- decimal

_CURRENCY_PREFIX = re.compile(r"^(?:₹|rs\.?|inr)\s*", re.IGNORECASE)
_PLAIN_NUMBER = re.compile(r"^\d+(?:\.\d+)?$|^\.\d+$")


def parse_decimal(raw: str) -> Decimal:
    """Optional ₹ / Rs / Rs. / INR prefix, ',' thousands separators, >= 0, 2 dp."""
    s = _CURRENCY_PREFIX.sub("", raw.strip(), count=1).replace(",", "").strip()
    if not _PLAIN_NUMBER.match(s):  # rejects negatives, words, '-', exponents
        raise InvalidValue("invalid_decimal")
    try:
        value = Decimal(s)
    except InvalidOperation as e:  # pragma: no cover - regex already guards this
        raise InvalidValue("invalid_decimal") from e
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


# --------------------------------------------------------------------------- enum

def parse_enum(raw: str, values: list[str]) -> str:
    v = raw.strip().lower()
    if v not in {x.lower() for x in values}:
        raise InvalidValue("invalid_enum")
    return v


# --------------------------------------------------------------------------- timestamps

def parse_timestamp(raw: object) -> datetime:
    """Parse an event timestamp to an aware datetime.

    ``...Z`` = UTC, ``...+05:30`` = offset, naive = **IST** (per spec).
    """
    if not isinstance(raw, str) or not raw.strip():
        raise InvalidValue("invalid_timestamp")
    s = raw.strip()
    if s.endswith(("Z", "z")):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError as e:
        raise InvalidValue("invalid_timestamp") from e
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)
    return dt


def to_epoch_us(dt: datetime) -> int:
    """Exact integer microseconds since the epoch (no float rounding)."""
    delta = dt - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def from_epoch_us(us: int) -> datetime:
    return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=us)


def ist_iso(us: int) -> str:
    """ISO-8601 in IST, second precision, e.g. 2026-10-14T17:55:24+05:30."""
    return from_epoch_us(us).astimezone(IST).strftime("%Y-%m-%dT%H:%M:%S+05:30")


def ist_date(us: int) -> str:
    return from_epoch_us(us).astimezone(IST).strftime("%Y-%m-%d")


# --------------------------------------------------------------------------- metrics

def format_rate(numerator: int, denominator: int) -> str:
    """Exact ratio with 4 decimals, rounded half-up (decimal arithmetic, no float error)."""
    if denominator == 0:
        return "0.0000"
    return str((Decimal(numerator) / Decimal(denominator)).quantize(
        Decimal("0.0001"), rounding=ROUND_HALF_UP))
