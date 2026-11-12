"""Independent ground-truth check against the data generator.

Re-runs assignment-a-data/tools/generate_data.py with *recording* wrappers around
Gen.lead_row / Gen.make_calls (they consume no randomness, so the output is byte-identical),
computes the expected reports from what the generator knows to be true (which leads are
valid, which batch each one first appeared in, each call's real outcome), and diffs them
against the pipeline's report folder. It shares no code with the pipeline.

Usage (from the repo root):
    python solution/tools/verify_against_generator.py <repo_root> <seed> <small|large> <gen_out_dir> <reports_dir>

    python solution/tools/verify_against_generator.py . 7 small /tmp/gen7 /dev/null   # generate only
    python solution/pipeline.py --db /tmp/s7.db ingest /tmp/gen7
    python solution/pipeline.py --db /tmp/s7.db report --out /tmp/r7
    python solution/tools/verify_against_generator.py . 7 small /tmp/gen7 /tmp/r7
"""
import csv, importlib.util, json, sys, os
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_HALF_UP

repo, seed, scale, outdir, reports = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4], sys.argv[5]
spec = importlib.util.spec_from_file_location("gd", os.path.join(repo, "assignment-a-data/tools/generate_data.py"))
gd = importlib.util.module_from_spec(spec); spec.loader.exec_module(gd)
IST = gd.IST

ROWS = []   # (wf, lead, row)
CALLS = []  # (wf, lead_phone, valid, is_pure_orphan, events)
orig_row, orig_calls = gd.Gen.lead_row, gd.Gen.make_calls
def lead_row(self, wf, lead, alias):
    row = orig_row(self, wf, lead, alias); ROWS.append((wf, lead, row)); return row
def make_calls(self, wf, phone, valid):
    ev = orig_calls(self, wf, phone, valid); CALLS.append((wf, phone, ev)); return ev
gd.Gen.lead_row, gd.Gen.make_calls = lead_row, make_calls
sys.argv = ["gen", "--seed", str(seed), "--scale", scale, "--out", outdir]
gd.main()

def parse(s):
    if s.endswith("Z"): return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    if "T" in s: return datetime.fromisoformat(s)
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST)

D = gd.DISPOSITIONS
# leads: first row (in batch order, then file order) of non-defect leads
valid_leads = {}  # (wf, phone) -> lead dict
for wf, lead, row in ROWS:
    if lead["defect"] is None:
        valid_leads.setdefault((wf, lead["phone"]), lead)
BAD_DATES = {"31/02/2026", "soon", "2026-13-01", "TBD"}; BAD_LANG = {"Hindi", "english", "tamil"}
BAD_AMT = {"N/A", "-", "twelve thousand"}
warnings = 0; rejected = 0; merged = 0; seen = set()
for wf, lead, row in ROWS:
    if lead["defect"] is not None: rejected += 1; continue
    vals = set(row.values())
    warnings += len(vals & BAD_DATES) + len(vals & BAD_LANG) + len(vals & BAD_AMT)
    k = (wf, lead["phone"])
    if k in seen: merged += 1
    seen.add(k)

# events -> per lead truth
lead_calls = defaultdict(list); orphans = 0; unknown = 0
for wf, phone, evs in CALLS:
    linked = (wf, phone) in valid_leads
    if not linked: orphans += len(evs); continue
    bycall = defaultdict(list)
    for e in evs: bycall[e["call_id"]].append(e)
    for cid, es in bycall.items():
        ini = parse(next(e for e in es if e["event_type"] == "call.initiated")["occurred_at"])
        ov = [e for e in es if e["event_type"] == "disposition.override"]
        st = [e for e in es if e["event_type"] == "disposition.set"]
        for e in st + ov:
            if e["payload"]["disposition_code"] not in D[wf]: unknown += 1
        eff = None
        if ov: eff = ov[-1]["payload"]["disposition_code"]
        elif st and st[-1]["payload"]["disposition_code"] in D[wf]: eff = st[-1]["payload"]["disposition_code"]
        lead_calls[(wf, phone)].append(dict(ini=ini, cid=cid, eff=eff,
            conn=any(e["event_type"] == "call.connected" for e in es),
            tf=any(e["event_type"] == "call.telephony_failed" for e in es)))

exp_leads, batch, daily = [], defaultdict(lambda: defaultdict(int)), defaultdict(lambda: [0, 0, set()])
for (wf, phone), lead in valid_leads.items():
    cs = sorted(lead_calls[(wf, phone)], key=lambda c: c["ini"])
    if not cs: b = "yet_to_reach_out"
    elif any(c["eff"] and D[wf][c["eff"]][0] for c in cs): b = "completed"
    elif all(c["tf"] for c in cs): b = "in_progress_telephony_failed"
    else: b = "in_progress_dialed"
    wd = [c for c in cs if c["eff"]]
    exp_leads.append([wf, lead["ref"], lead["batch"], "+91******" + phone[-4:], b, str(len(cs)),
        str(sum(c["conn"] for c in cs)),
        cs[-1]["ini"].astimezone(IST).strftime("%Y-%m-%dT%H:%M:%S+05:30") if cs else "",
        wd[-1]["eff"] if wd else "", "true" if any(D[wf][c["eff"]][1] for c in wd) else "false"])
    batch[(wf, lead["batch"])][b] += 1
    for c in cs:
        d = daily[(wf, c["ini"].astimezone(IST).strftime("%Y-%m-%d"))]
        d[0] += 1; d[1] += c["conn"]
        if c["eff"] and D[wf][c["eff"]][1]: d[2].add(phone)
exp_leads.sort(key=lambda r: (r[0], r[1]))
exp_batch = [[wf, bid, str(sum(v.values()))] + [str(v[k]) for k in
             ("yet_to_reach_out", "in_progress_telephony_failed", "in_progress_dialed", "completed")]
             for (wf, bid), v in sorted(batch.items())]
exp_daily = [[wf, day, str(t), str(c), str((Decimal(c) / Decimal(t)).quantize(Decimal("0.0001"), ROUND_HALF_UP)), str(len(s))]
             for (wf, day), (t, c, s) in sorted(daily.items())]

def read(name):
    with open(os.path.join(reports, name), newline="", encoding="utf-8") as f: return list(csv.reader(f))[1:]
ok = True
for name, exp in [("lead_report.csv", exp_leads), ("batch_summary.csv", exp_batch), ("daily_overview.csv", exp_daily)]:
    got = read(name)
    if got != exp:
        ok = False; diffs = [(g, e) for g, e in zip(got, exp) if g != e]
        print(f"MISMATCH {name}: got {len(got)} rows, expected {len(exp)}; first diffs:", diffs[:5])
    else: print(f"OK {name} ({len(got)} rows)")
dq = json.load(open(os.path.join(reports, "data_quality.json")))
exp_dq = dict(lead_rows_total=len(ROWS), lead_rows_rejected=rejected, duplicate_lead_rows_merged=merged,
              field_warnings=warnings, orphan_events=orphans, unknown_disposition_events=unknown)
for k, v in exp_dq.items():
    s = "OK" if dq[k] == v else "MISMATCH"; ok &= dq[k] == v
    print(f"{s} {k}: got {dq[k]} expected {v}")
print("ALL MATCH" if ok else "DIFFERENCES FOUND")
