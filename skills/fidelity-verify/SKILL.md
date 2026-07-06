---
name: fidelity-verify
description: Use AFTER a sluice migrate, sync cold-start, or restore to confirm the target faithfully matches the source. Drives `sluice verify --format json` (count mode default; `--depth=sample` adds sampled row-content hashes) and interprets drift. Read-only — never writes to either database. Trigger when the user asks to verify / check / confirm a migration, or asks "is the migration correct / did it copy everything / did anything get lost".
---

# fidelity-verify

Confirm a completed migrate / sync cold-start / restore is faithful, and return a fidelity report. This is sluice's "never report a migration done without verifying it" rule (`AGENTS.md`), run as a checklist. Entirely read-only — `verify` never modifies the target.

## When to use
A data-moving command just finished and you need evidence it was correct. Always run this after `migrate`, after a `sync start` cold-start completes, and after `restore` — before telling the human the work is done.

## Inputs you need
- Source + target DSNs (prefer env: `SLUICE_SOURCE` / `SLUICE_TARGET`).
- The source/target drivers (`--source-driver` / `--target-driver`).
- Whether the target was **redacted** (changes how to verify — see step 3).

## Steps

1. **Run count verify (the default, cross-engine safe).**
   ```sh
   sluice verify --format json \
     --source-driver <drv> --source "$SLUICE_SOURCE" \
     --target-driver <drv> --target "$SLUICE_TARGET"
   ```
   Count mode compares per-table row counts source-vs-target. **Exit 0** = clean; **exit 1** = at least one table's counts differ (drift found); **exit 2** = the check couldn't run (connect/engine error → hand to `sluice-error-triage`). Scope with `--include-table` / `--exclude-table` (glob-capable, mutually exclusive) when verifying a subset.

2. **Escalate to sample mode for content confidence** (same-engine only). `--depth=sample` adds per-table sampled-row content hashes (~99% confidence of catching a 5%+ corruption at the default `--sample-rows-per-table 100`; raise it for rarer anomalies). Tune determinism with `--sample-seed` (same seed → same sample rows on both sides) and `--strict-hash` (SHA-256 instead of MD5). A **cross-engine** verify refuses `--depth=sample` loudly (exit 2, a plain-text error — NOT a JSON envelope).

   **⚠ Cross-engine verify is count-only — a clean count is NOT a fidelity guarantee.** Count mode compares row *counts*, not row *values*. Because sample mode is unavailable cross-engine (the exact case above), a cross-engine `verify` **cannot detect value-level drift** — a mistranslated type, a lossily-carried value, a wrong DEFAULT all pass a count check with exit 0. This matters because MySQL↔Postgres is sluice's *primary* use case, so its most-used verification path is the weakest. Do **not** report a cross-engine migration "faithful" on a clean count alone: state that counts match and value-level fidelity was *not* machine-checked, and (for critical columns) spot-check values manually or stage a same-engine sample.

3. **Handle a redacted target.** `verify` has no redaction awareness. `--depth=sample` hashes full row content, so on a redacted migration it flags every redacted row as a mismatch *by design* (source `alice@example.com` vs target `5a8e91…`). For a redacted run: use `--depth=count` (row counts are unchanged by redaction), or scope `--depth=sample` to the **non-redacted** tables with `--include-table` (see `docs/cookbook/recipe-redaction-keyset.md`).

4. **Pin the class, not the representative (the Bug-74 discipline).** Confidence comes from exercising every value *family* — native int/float/bool, string-leaf (text/uuid/inet/decimal), temporal (time/timestamp/date), and arrays × {scalar, multi-dim, NULL-element} — not one representative type. A green sample on `text[]` does NOT cover `numeric[]` (identical sluice code, different driver codec). When a migration leaned on a value-codec path, note in the report which families the sample actually touched, and flag families it did not (see `docs/value-types.md`). **Note this only applies same-engine** — sample mode is where families get exercised, and it can't run cross-engine, so on a cross-engine migration this whole family-coverage check is unavailable, precisely where family-codec drift is most likely. Flag that as the standing residual risk of any cross-engine verify.

## What you return — the fidelity report
- **Verdict:** FAITHFUL / DRIFT-FOUND / COULD-NOT-VERIFY (map to exit 0 / 1 / 2). **Qualify a cross-engine exit 0 honestly:** it means "row counts match; values not machine-checked", NOT a blanket FAITHFUL — reserve unqualified FAITHFUL for a same-engine run that reached `--depth=sample` clean.
- **Counts:** tables checked, tables clean, tables with a count mismatch (named).
- **Sample results (if run):** rows sampled per table, any content-hash mismatches named by table; note it was same-engine.
- **Drift detail:** for each mismatched table, source vs target count (or the mismatching sample), and the likely cause to chase (a failed table → `SLUICE-E-BULKCOPY-TABLE-FAILED`, resume with `--resume`).
- **Coverage caveat:** which value families the check exercised; any un-covered family flagged as residual risk.

On exit 1 (drift), do not call the migration done — report the drift and the recovery path. On exit 2, triage the operational error before drawing any fidelity conclusion.

## References (canonical — don't duplicate)
`AGENTS.md` (verify in the standard workflow; JSON output) · `docs/value-types.md` (the value-family contract; Bug-74 lesson) · `docs/cookbook/recipe-redaction-keyset.md` (verifying a redacted target) · `docs/operator/error-codes.md` · `sluice verify --help`.
