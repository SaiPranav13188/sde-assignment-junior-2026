"""Effective disposition, linking, orphans and bucket logic (pure, no database)."""
from au_pipeline.derive import Call, EventRec, LeadRec, bucket_for, build_reports, derive_calls
from au_pipeline.normalize import parse_timestamp, to_epoch_us
from au_pipeline.schema import Disposition

CAT = {"wf": {
    "renewed": Disposition("renewed", True, True),
    "callback_requested": Disposition("callback_requested", False, False),
    "not_interested": Disposition("not_interested", True, False),
}}
D = CAT["wf"]


def ts(s: str) -> int:
    return to_epoch_us(parse_timestamp(s))


def ev(eid, etype, call, at, phone="P1", code=None, wf="wf"):
    return EventRec(eid, etype, wf, call, ts(at), phone, code)


LEAD = LeadRec(1, "wf", "P1", "3210", "REF1", "batch_001")


# ----------------------------------------------------------------------------- effective disposition

def test_effective_disposition_latest_set_when_no_override():
    c = Call("c", 1, 0, sets=[(10, "e1", "callback_requested"), (20, "e2", "renewed")])
    assert c.effective_disposition == "renewed"


def test_override_beats_any_set_even_if_earlier():
    c = Call("c", 1, 0, sets=[(50, "e1", "renewed")], overrides=[(10, "e2", "not_interested")])
    assert c.effective_disposition == "not_interested"


def test_latest_override_by_occurred_at_not_arrival_order():
    c = Call("c", 1, 0, overrides=[(30, "e9", "renewed"), (10, "e5", "not_interested")])
    assert c.effective_disposition == "renewed"


def test_no_disposition():
    assert Call("c", 1, 0).effective_disposition is None


# ----------------------------------------------------------------------------- buckets

def mk(tf=False, sets=None, overrides=None):
    return Call("c", 1, 0, telephony_failed=tf, sets=sets or [], overrides=overrides or [])


def test_bucket_yet_to_reach_out():
    assert bucket_for([], D) == "yet_to_reach_out"


def test_bucket_completed_wins_over_telephony_failed():
    calls = [mk(tf=True), mk(sets=[(1, "e", "not_interested")])]
    assert bucket_for(calls, D) == "completed"


def test_bucket_all_telephony_failed():
    assert bucket_for([mk(tf=True), mk(tf=True)], D) == "in_progress_telephony_failed"


def test_bucket_dialed_when_some_calls_not_failed():
    assert bucket_for([mk(tf=True), mk()], D) == "in_progress_dialed"
    assert bucket_for([mk(sets=[(1, "e", "callback_requested")])], D) == "in_progress_dialed"


def test_override_away_from_terminal_reopens_lead():
    calls = [mk(sets=[(1, "e1", "renewed")], overrides=[(2, "e2", "callback_requested")])]
    assert bucket_for(calls, D) == "in_progress_dialed"


# ----------------------------------------------------------------------------- linking

def test_override_links_through_call_id_and_unknown_codes_are_ignored():
    events = [
        ev("e1", "call.initiated", "c1", "2026-10-01T10:00:00+05:30"),
        ev("e2", "call.connected", "c1", "2026-10-01T10:00:10+05:30"),
        ev("e3", "disposition.set", "c1", "2026-10-01T10:05:00+05:30", code="renewed"),
        ev("e4", "disposition.override", "c1", "2026-10-02T10:00:00+05:30", phone=None,
           code="not_interested"),
        ev("e5", "disposition.override", "c1", "2026-10-03T10:00:00+05:30", phone=None,
           code="RENEWD"),                                     # unknown -> ignored
    ]
    d = derive_calls([LEAD], events, CAT)
    (call,) = d.calls_by_lead[1]
    assert call.connected and call.effective_disposition == "not_interested"
    assert d.orphan_events == 0 and d.unknown_disposition_events == 1


def test_orphans_unknown_phone_invalid_phone_unknown_call_and_unknown_workflow():
    events = [
        ev("e1", "call.initiated", "c1", "2026-10-01T10:00:00Z", phone="NOPE"),
        ev("e2", "call.initiated", "c2", "2026-10-01T10:00:00Z", phone=None),
        ev("e3", "disposition.override", "c404", "2026-10-01T10:00:00Z", phone=None, code="renewed"),
        ev("e4", "call.initiated", "c3", "2026-10-01T10:00:00Z", wf="other"),
        # override of an orphan call is itself an orphan (not an unknown disposition)
        ev("e5", "disposition.override", "c1", "2026-10-01T10:00:00Z", phone=None, code="BAD"),
    ]
    d = derive_calls([LEAD], events, CAT)
    assert d.orphan_events == 5 and d.unknown_disposition_events == 0
    assert d.calls_by_lead == {}


def test_call_without_initiated_is_not_counted_but_its_events_are_linked():
    events = [ev("e1", "call.connected", "c1", "2026-10-01T10:00:00Z"),
              ev("e2", "disposition.set", "c1", "2026-10-01T10:01:00Z", code="renewed")]
    d = derive_calls([LEAD], events, CAT)
    assert d.calls_by_lead == {} and d.orphan_events == 0


def test_reports_use_ist_dates_and_most_recent_call_with_disposition():
    events = [
        # call 1: 23:50 IST on 1 Oct, renewed
        ev("e1", "call.initiated", "c1", "2026-10-01T18:20:00Z"),
        ev("e2", "call.connected", "c1", "2026-10-01T18:20:10Z"),
        ev("e3", "disposition.set", "c1", "2026-10-01T18:25:00Z", code="renewed"),
        # call 2: 00:30 IST on 2 Oct (still 1 Oct in UTC), no disposition
        ev("e4", "call.initiated", "c2", "2026-10-01T19:00:00Z"),
        ev("e5", "call.telephony_failed", "c2", "2026-10-01T19:00:05Z"),
    ]
    d = derive_calls([LEAD], events, CAT)
    batch, daily, leads = build_reports([LEAD], d, CAT)
    assert daily == [
        {"workflow_id": "wf", "date": "2026-10-01", "total_calls": 1, "connected_calls": 1,
         "connectivity_rate": "1.0000", "success_leads": 1},
        {"workflow_id": "wf", "date": "2026-10-02", "total_calls": 1, "connected_calls": 0,
         "connectivity_rate": "0.0000", "success_leads": 0},
    ]
    assert leads[0] == {
        "workflow_id": "wf", "external_ref": "REF1", "batch_id": "batch_001",
        "phone_masked": "+91******3210", "bucket": "completed", "total_calls": 2,
        "connected_calls": 1, "last_call_at": "2026-10-02T00:30:00+05:30",
        "current_disposition": "renewed", "ever_success": "true"}
    assert batch == [{"workflow_id": "wf", "batch_id": "batch_001", "total_leads": 1,
                      "yet_to_reach_out": 0, "in_progress_telephony_failed": 0,
                      "in_progress_dialed": 0, "completed": 1}]
