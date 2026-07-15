# sluice v0.99.245

**New: `sluice backfill --verify` / `--verify-only` — the explicit, scriptable "safe to run the contract step" signal (ADR-0159 Phase 2), closing the expand→migrate→contract loop.** Drop-in upgrade: two additive flags and one new error code on the backfill command that shipped in v0.99.244; no behavior change to `migrate`, `sync`, `backup`, `restore`, or any other command.

## Added

- **`--verify` — the post-walk completion gate.** Phase 1 gave you the online-safe walk; what it couldn't tell you is whether the table is actually *done* — on a live table, rows inserted **behind the walk's cursor** during the run still match the guard after the walk exits 0. `--verify` closes that gap: after the walk completes (and equally after the completed-spec no-op re-run), sluice runs one whole-table remaining-count on the `--where` guard. Zero remaining prints the explicit safe-to-contract line ("safe to run the contract step — drop/rename the old column") and exits clean; a nonzero count fails with the new coded `SLUICE-E-BACKFILL-INCOMPLETE` — **runtime** class (exit 1, the "ran cleanly, found work" semantics, not a refusal) — with the catch-up remedy in the hint: re-run the backfill to pick up the stragglers (a spec whose stored state is `complete` needs `--restart` to walk again), then verify again. On a quiesced database, a nonzero count after a clean walk means something different — the `--where` guard doesn't actually self-describe doneness (an already-backfilled row still matches) — and the hint says to fix the predicate (e.g. `new_col IS NULL`). A failed verify never marks the migration state failed: the walk itself succeeded and every persisted chunk stands — the gate is only saying the table isn't yet safe for the contract step.

- **`--verify-only` — the standalone, scriptable gate.** The same 0-clean / >0-coded-error exit contract with no walk at all: no UPDATEs, no control-table reads or writes — the shape a deploy script wants between the migrate step and the contract deploy request (`sluice backfill --driver … --dsn … --table t --where 'new_col IS NULL' --verify-only && ship-contract`). Because it never walks, none of the walk's requirements apply: a no-PK table is verifiable, and `--set` is optional (any `--set` given is still parsed and schema-checked, so the unknown-column refusal keeps working).

- **Guard rails on the gate itself.** Both verify modes **require `--where`** — without a self-describing guard the remaining-count is the whole table and the completion signal is meaningless, so sluice refuses rather than emit a number that looks authoritative and isn't. Contradictory combinations (`--verify-only` with `--dry-run` or `--restart`, `--verify` with `--dry-run`) are refused at both the kong layer (xor groups) and the library layer, so programmatic callers of `pipeline.Backfiller` get the same refusals as the CLI. `SLUICE-E-BACKFILL-INCOMPLETE` is documented in `docs/operator/error-codes.md` alongside the three Phase 1 codes.

- **Refusal-ordering polish: the coded unsupported-engine answer now always wins.** The `SLUICE-E-BACKFILL-UNSUPPORTED-ENGINE` check now runs before table resolution, so an engine without the backfill surface (SQLite/D1) gets the coded refusal even when the named table is also missing — previously the schema read ran first and surfaced an uncoded table-not-found, observed in the v0.99.244 regression cycle.

## Compatibility

- **Additive flags on a one-release-old command; no change outside `backfill`.** Two new flags, one new runtime-class error code, and two `pipeline.Backfiller` fields — every other command, the engine surfaces, and the control-table format are untouched. Two behavior notes scoped to `backfill` itself: (1) the unsupported-engine/missing-table refusal ordering above — same refusal, now coded and first; (2) `--set`'s at-least-one requirement moved from kong parse-time to run-time validation (it had to become optional for `--verify-only`), so an invocation missing `--set` still fails up front before any connection or UPDATE, just via sluice's own validation error instead of kong's usage error. Every existing valid invocation shape behaves identically.

## Who needs this

No action required for anyone — this release contains no fixes, so there is nothing to re-verify or re-run. Pick it up if you script expand-contract schema changes: `--verify-only` is the missing exit-code gate between "the backfill ran" and "it is safe to ship the contract step," and `--verify` makes a single backfill invocation self-certifying on a live table where rows can land behind the cursor mid-walk.

---

Install / upgrade: see the [README](https://github.com/sluicesync/sluice#install). Verify downloads against `checksums.txt`.
