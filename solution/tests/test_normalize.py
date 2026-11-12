from datetime import date
from decimal import Decimal

import pytest

from au_pipeline.normalize import (InvalidValue, format_rate, ist_date, ist_iso, mask_phone,
                                   normalize_header, normalize_phone, parse_date, parse_decimal,
                                   parse_enum, parse_timestamp, to_epoch_us)


# ----------------------------------------------------------------------------- headers

@pytest.mark.parametrize("raw,expected", [
    ("Mobile No", "mobile no"),
    ("  Policy__No ", "policy no"),
    ("customer_name", "customer name"),
    ("Insured\t Name", "insured name"),
    ("ORDER_ _ID", "order id"),
])
def test_normalize_header(raw, expected):
    assert normalize_header(raw) == expected


# ----------------------------------------------------------------------------- phone

@pytest.mark.parametrize("raw", [
    "9876543210", "+91 98765 43210", "09876543210", "91-98765-43210", "(+91) 9876543210",
    "+919876543210", " 98765-43210 ", "919876543210",
])
def test_phone_all_generator_formats_canonicalise(raw):
    assert normalize_phone(raw) == "+919876543210"


@pytest.mark.parametrize("raw", [
    "987654321",          # 9 digits
    "5876543210",         # bad leading digit
    "022-2345-6789",      # landline: 11 digits with 0 -> 10 digits starting with 2
    "N/A",                # no digits
    "+1 415 555 0134",    # US number (11 digits, starts with 1)
    "0919876543210",      # 13 digits: only one prefix rule applies
    "98765432101",        # 11 digits not starting with 0
    "",
])
def test_phone_invalid(raw):
    with pytest.raises(InvalidValue):
        normalize_phone(raw)


def test_phone_12_digits_must_start_with_91():
    with pytest.raises(InvalidValue):
        normalize_phone("929876543210")


def test_mask_phone():
    assert mask_phone("3210") == "+91******3210"


# ----------------------------------------------------------------------------- dates

@pytest.mark.parametrize("raw,expected", [
    ("2026-11-21", date(2026, 11, 21)),
    ("21/11/2026", date(2026, 11, 21)),
    ("21-11-2026", date(2026, 11, 21)),
    ("01-Dec-2026", date(2026, 12, 1)),
    ("01-dec-2026", date(2026, 12, 1)),
    ("01-DEC-2026", date(2026, 12, 1)),
    ("05/04/2026", date(2026, 4, 5)),     # day-first, not April->May confusion
    ("29/02/2028", date(2028, 2, 29)),    # leap year
])
def test_dates_valid(raw, expected):
    assert parse_date(raw) == expected


@pytest.mark.parametrize("raw", [
    "31/02/2026", "2026-13-01", "soon", "TBD", "29/02/2026", "01-Foo-2026",
    "2026/11/21", "11/21/2026", "21.11.2026", "",
])
def test_dates_invalid(raw):
    with pytest.raises(InvalidValue):
        parse_date(raw)


# ----------------------------------------------------------------------------- decimals

@pytest.mark.parametrize("raw,expected", [
    ("27554", "27554.00"),
    ("₹27,554", "27554.00"),
    ("₹ 1,27,554.5", "127554.50"),
    ("Rs. 24787", "24787.00"),
    ("rs 100", "100.00"),
    ("Rs.100", "100.00"),
    ("INR 25301.4", "25301.40"),
    ("inr25301.456", "25301.46"),
    ("0", "0.00"),
    ("10.005", "10.01"),   # half-up
])
def test_decimal_valid(raw, expected):
    assert parse_decimal(raw) == Decimal(expected)


@pytest.mark.parametrize("raw", ["N/A", "-", "twelve thousand", "-100", "Rs -5", "1e3", "₹", "USD 5"])
def test_decimal_invalid(raw):
    with pytest.raises(InvalidValue):
        parse_decimal(raw)


# ----------------------------------------------------------------------------- enums

def test_enum_case_insensitive_and_lowercased():
    assert parse_enum("HI", ["en", "hi"]) == "hi"
    assert parse_enum(" hi ", ["en", "hi"]) == "hi"
    with pytest.raises(InvalidValue):
        parse_enum("Hindi", ["en", "hi"])


# ----------------------------------------------------------------------------- timestamps

def test_timestamp_three_forms_are_the_same_instant():
    utc = to_epoch_us(parse_timestamp("2026-10-03T04:31:00Z"))
    offset = to_epoch_us(parse_timestamp("2026-10-03T10:01:00+05:30"))
    naive_is_ist = to_epoch_us(parse_timestamp("2026-10-03 10:01:00"))
    assert utc == offset == naive_is_ist


def test_ist_date_rolls_over_at_ist_midnight():
    # 18:45Z on 3 Oct is 00:15 IST on 4 Oct: reports use the IST date.
    us = to_epoch_us(parse_timestamp("2026-10-03T18:45:00Z"))
    assert ist_date(us) == "2026-10-04"
    assert ist_iso(us) == "2026-10-04T00:15:00+05:30"


@pytest.mark.parametrize("raw", [None, "", "yesterday", 12345])
def test_timestamp_invalid(raw):
    with pytest.raises(InvalidValue):
        parse_timestamp(raw)


# ----------------------------------------------------------------------------- metrics

@pytest.mark.parametrize("n,d,expected", [
    (9, 16, "0.5625"), (1, 3, "0.3333"), (2, 3, "0.6667"), (1, 32, "0.0313"),
    (0, 5, "0.0000"), (5, 5, "1.0000"),
])
def test_format_rate(n, d, expected):
    assert format_rate(n, d) == expected
