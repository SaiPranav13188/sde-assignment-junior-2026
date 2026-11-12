"""Derived facts: linking, calls, effective dispositions, buckets and report rows.

Pure functions over plain records. No database access, so every rule here is unit-tested
directly (tests/test_derive.py) and the same code serves incremental and one-shot runs.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field

from .normalize import format_rate, ist_date, ist_iso, mask_phone
from .schema import Disposition

INITIATED = "call.initiated"
TELEPHONY_FAILED = "call.telephony_failed"
CONNECTED = "call.connected"
DISPOSITION_SET = "disposition.set"
DISPOSITION_OVERRIDE = "disposition.override"

BUCKETS = ("yet_to_reach_out", "in_progress_telephony_failed", "in_progress_dialed", "completed")


@dataclass(frozen=True)
class LeadRec:
    lead_id: int
    workflow_id: str
    phone_index: str
    phone_last4: str
    external_ref: str | None
    batch_id: str


@dataclass(frozen=True)
class EventRec:
    event_id: str
    event_type: str | None
    workflow_id: str | None
    call_id: str | None
    occurred_at_us: int
    phone_index: str | None
    disposition_code: str | None


@dataclass
class Call:
    call_id: str
    lead_id: int
    initiated_us: int
    connected: bool = False
    telephony_failed: bool = False
    # (occurred_at_us, event_id, code); event_id breaks exact-timestamp ties deterministically
    sets: list[tuple[int, str, str]] = field(default_factory=list)
    overrides: list[tuple[int, str, str]] = field(default_factory=list)

    @property
    def effective_disposition(self) -> str | None:
        """Latest override by occurred_at; else latest set; else None."""
        if self.overrides:
            return max(self.overrides)[2]
        if self.sets:
            return max(self.sets)[2]
        return None


@dataclass
class Derived:
    calls_by_lead: dict[int, list[Call]]
    orphan_events: int
    unknown_disposition_events: int


# --------------------------------------------------------------------------- linking

def derive_calls(leads: list[LeadRec], events: list[EventRec],
                 catalogue: dict[str, dict[str, Disposition]]) -> Derived:
    lead_by_key = {(l.workflow_id, l.phone_index): l.lead_id for l in leads}

    # 1. link every event that carries a phone
    link: dict[str, int | None] = {}
    call_lead: dict[tuple[str, str], int] = {}       # (wf, call_id) -> lead, from call.initiated
    call_lead_fallback: dict[tuple[str, str], int] = {}
    for e in events:
        if e.event_type == DISPOSITION_OVERRIDE:
            continue
        lid = lead_by_key.get((e.workflow_id, e.phone_index)) if e.phone_index else None
        link[e.event_id] = lid
        if lid is not None and e.call_id is not None:
            key = (e.workflow_id, e.call_id)
            if e.event_type == INITIATED:
                call_lead.setdefault(key, lid)
            else:
                call_lead_fallback.setdefault(key, lid)

    # 2. overrides have no phone: link through the call's other events
    for e in events:
        if e.event_type == DISPOSITION_OVERRIDE:
            key = (e.workflow_id, e.call_id)
            link[e.event_id] = call_lead.get(key, call_lead_fallback.get(key)) \
                if e.call_id is not None else None

    orphans = sum(1 for e in events if link[e.event_id] is None)

    # 3. unknown dispositions (only meaningful for linked events), then build calls
    unknown = 0
    calls: dict[tuple[str, str], Call] = {}
    for e in sorted((e for e in events if e.event_type == INITIATED and link[e.event_id] is not None),
                    key=lambda e: (e.occurred_at_us, e.event_id)):
        key = (e.workflow_id, e.call_id)
        if e.call_id is None or key in calls:
            continue  # a repeated initiated (distinct event_id) keeps the earliest time
        calls[key] = Call(e.call_id, link[e.event_id], e.occurred_at_us)

    for e in events:
        lid = link[e.event_id]
        if lid is None:
            continue
        if e.event_type in (DISPOSITION_SET, DISPOSITION_OVERRIDE):
            if e.disposition_code not in catalogue.get(e.workflow_id, {}):
                unknown += 1
                continue
        call = calls.get((e.workflow_id, e.call_id))
        if call is None or call.lead_id != lid:
            continue  # no counted call to attach to (no linked call.initiated)
        if e.event_type == CONNECTED:
            call.connected = True
        elif e.event_type == TELEPHONY_FAILED:
            call.telephony_failed = True
        elif e.event_type == DISPOSITION_SET:
            call.sets.append((e.occurred_at_us, e.event_id, e.disposition_code))
        elif e.event_type == DISPOSITION_OVERRIDE:
            call.overrides.append((e.occurred_at_us, e.event_id, e.disposition_code))

    by_lead: dict[int, list[Call]] = defaultdict(list)
    for c in calls.values():
        by_lead[c.lead_id].append(c)
    for lst in by_lead.values():
        lst.sort(key=lambda c: (c.initiated_us, c.call_id))
    return Derived(dict(by_lead), orphans, unknown)


# --------------------------------------------------------------------------- per-lead facts

def bucket_for(calls: list[Call], dispositions: dict[str, Disposition]) -> str:
    if not calls:
        return "yet_to_reach_out"
    for c in calls:
        d = c.effective_disposition
        if d is not None and dispositions[d].is_terminal:
            return "completed"
    if all(c.telephony_failed for c in calls):
        return "in_progress_telephony_failed"
    return "in_progress_dialed"


def lead_row(lead: LeadRec, calls: list[Call], dispositions: dict[str, Disposition]) -> dict:
    calls = sorted(calls, key=lambda c: (c.initiated_us, c.call_id))
    with_disp = [c for c in calls if c.effective_disposition is not None]
    return {
        "workflow_id": lead.workflow_id,
        "external_ref": lead.external_ref or "",
        "batch_id": lead.batch_id,
        "phone_masked": mask_phone(lead.phone_last4),
        "bucket": bucket_for(calls, dispositions),
        "total_calls": len(calls),
        "connected_calls": sum(1 for c in calls if c.connected),
        "last_call_at": ist_iso(calls[-1].initiated_us) if calls else "",
        "current_disposition": with_disp[-1].effective_disposition if with_disp else "",
        "ever_success": "true" if any(
            dispositions[c.effective_disposition].is_success for c in with_disp) else "false",
    }


# --------------------------------------------------------------------------- reports

def build_reports(leads: list[LeadRec], derived: Derived,
                  catalogue: dict[str, dict[str, Disposition]]):
    lead_rows, batch, daily = [], {}, {}
    for lead in leads:
        disp = catalogue.get(lead.workflow_id, {})
        calls = derived.calls_by_lead.get(lead.lead_id, [])
        row = lead_row(lead, calls, disp)
        lead_rows.append((lead.lead_id, row))

        b = batch.setdefault((lead.workflow_id, lead.batch_id), dict.fromkeys(BUCKETS, 0))
        b[row["bucket"]] += 1

        for c in calls:
            d = daily.setdefault((lead.workflow_id, ist_date(c.initiated_us)),
                                 {"total": 0, "connected": 0, "success": set()})
            d["total"] += 1
            d["connected"] += c.connected
            eff = c.effective_disposition
            if eff is not None and disp[eff].is_success:
                d["success"].add(lead.lead_id)

    batch_rows = [
        {"workflow_id": wf, "batch_id": bid, "total_leads": sum(b.values()),
         "yet_to_reach_out": b["yet_to_reach_out"],
         "in_progress_telephony_failed": b["in_progress_telephony_failed"],
         "in_progress_dialed": b["in_progress_dialed"], "completed": b["completed"]}
        for (wf, bid), b in sorted(batch.items())
    ]
    daily_rows = [
        {"workflow_id": wf, "date": day, "total_calls": d["total"],
         "connected_calls": d["connected"], "connectivity_rate": format_rate(d["connected"], d["total"]),
         "success_leads": len(d["success"])}
        for (wf, day), d in sorted(daily.items())
    ]
    lead_rows.sort(key=lambda t: (t[1]["workflow_id"], t[1]["external_ref"], t[0]))
    return batch_rows, daily_rows, [r for _, r in lead_rows]
