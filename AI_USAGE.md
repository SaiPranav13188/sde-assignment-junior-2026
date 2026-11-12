# AI Usage Disclosure

## Tools used
- Claude Code (Anthropic, Claude Opus model), used agentically in VS Code: it read the repo,
  wrote code, ran commands and ran tests.

## What I used them for
- Reading both assignments and choosing A (deterministic output that can be verified
  end-to-end, versus B's timing- and crash-dependent behaviour).
- Drafting the whole implementation in `solution/au_pipeline/`: normalisation rules, SQLite
  schema, ingest, derivation, report writer and CLI.
- Writing the test suite, including the hand-built two-drop fixture and its expected outputs.
- Writing the ground-truth checker `solution/tools/verify_against_generator.py`, which wraps
  the data generator to compute expected reports independently of the pipeline.
- Profiling and tuning the large-scale run, and drafting `SUBMISSION.md`.

## What I wrote or decided myself
- I set the goal and constraints for the AI agent: solve one assignment carefully and completely,
  using the time I had, and verify the result rather than just producing code.
- I accepted the choice of Assignment A and the design the agent proposed. To be transparent:
  the implementation, the schema design and the ambiguity decisions in `SUBMISSION.md` were
  drafted by the AI, not written by me line by line.
- I will be ready to walk through the design and its decisions in the interview, in particular
  the event→lead linking at report time, the PII blind index, and content-hash idempotency.

## Where the AI was wrong or unhelpful
- **Performance diagnosis.** The first large run took about 5 minutes. The AI's first fix
  (a bigger SQLite page cache, and precomputing the header mapping per file instead of per row)
  gave no measurable gain. Only a separate micro-benchmark showed that the machine itself was
  about 10× slower than a typical laptop for both CPU and disk. Switching from WAL to a
  rollback journal was then chosen from a measured comparison (≈1.5× on bulk inserts), not
  from assumption. The timings in SUBMISSION.md are reported as measured, with that context,
  rather than claiming the 2-minute target.
- **A muddled test assertion.** A first draft of the generated-data test contained a
  meaningless expression (`sum(1 for _ in [0]) * 0 + ...`). It was caught on review and
  rewritten as a plain equality.
- **Unverified claim in the write-up.** A draft of SUBMISSION.md said "about 25%" of events
  were orphans after drop 1. That was a guess, so it was measured (27%, 1123 / 4160) and corrected.

## How I verified AI-generated code
- **93 pytest tests**, including byte-for-byte comparison of all five report files against
  expected output worked out by hand for a small messy dataset.
- **An independent oracle**: expected reports computed from the generator's own ground truth
  (which leads are valid, each call's true outcome). It matched exactly on seeds 42, 7 and 2026
  (small) and 42 (large, 133k leads), so the output does not just agree with itself.
- Checked that the README's own examples (`0.5625`, `2026-10-14T17:55:24+05:30`) appear in the
  seed-42 output.
- Idempotency and incrementality checked by diffing reports from drop-by-drop (with re-runs)
  against one-shot ingestion. PII checked by grepping the raw database bytes for names and phones.
