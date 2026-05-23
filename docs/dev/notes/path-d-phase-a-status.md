# Path D Phase A — runbook + status

ADR-0036 Phase A status note. Read this with `docs/adr/adr-0036-mid-stream-loss-surface-characterization.md` open.

## What's on this branch

`path-d-phase-a-diagnose` carries:

- **Instrumentation only.** Zero production fixes. The branch is observational.
- **DEBUG-level slog instrumentation** in the v0.24.0 mid-stream live add-table flow:
  - `internal/pipeline/add_table.go::AddTable.Run` — captures `pg_current_wal_lsn()` before and after publication-add (`addtable.diag` phase=`pub-add-window`); captures the snapshot consistent-point LSN on snapshot open (`addtable.diag` phase=`snapshot-open`).
  - `internal/engines/postgres/cdc_reader.go::dispatchWAL` — logs every BEGIN, COMMIT, RelationMessage, and row event with txn-start LSN, txn-commit LSN, WAL record LSN, relation name, and `first_seen_for_rel` flag.
  - `internal/engines/postgres/engine.go::Engine.ReadCurrentWALPosition` — new optional engine surface used by the orchestrator's instrumentation; PG-only; tight DSN-only signature mirroring `ReadSlotPosition`.
- **One Phase A diagnostic test:** `internal/pipeline/add_table_live_pg_diagnose_integration_test.go::TestAddTable_LiveMode_PG_DiagnoseLossSurface`. Runs the same burst-writer scenario as the existing under-load test, but installs a JSON DEBUG slog handler, captures every diag line into an in-memory buffer, performs a source-vs-target set-diff on the loader rows, and emits four `VERDICT_M[1-4]` lines via `t.Logf`. The test does NOT enforce zero-loss assertions — it's purely observational.
- **ADR-0036** — `docs/adr/adr-0036-mid-stream-loss-surface-characterization.md`. Verdict lines are TODO until the run completes; do not edit them speculatively.

The instrumentation is gated entirely behind DEBUG slog (`Level: slog.LevelDebug`); a run with the default INFO slog level will not see any of it. Production logs are unchanged.

## How to run on the Vultr box

Assumes the standing Vultr instance is up at `root@<previous-vultr-IP>` per `C:\vultr-cli\sluice-test-spin-up.md`. The repo is bootstrapped at `/root/code/sluice` via the github deploy-key path (Path B in the spin-up runbook).

```bash
ssh root@<previous-vultr-IP>
cd /root/code/sluice
git fetch origin
git checkout path-d-phase-a-diagnose
git pull origin path-d-phase-a-diagnose

# Single-test run, verbose output so the t.Logf VERDICT lines show up.
# -count=1 so test cache doesn't suppress the run; -race because the
# diagnostic touches goroutine boundaries (the slog buffer + the
# CDC pump goroutine writing into it).
go test -tags=integration -race -count=1 -v -timeout=10m \
  -run '^TestAddTable_LiveMode_PG_DiagnoseLossSurface$' \
  ./internal/pipeline/... 2>&1 | tee /tmp/adr-0036-phase-a-run-$(date +%Y%m%d-%H%M%S).log
```

Expected wall-clock: ~60-90s (one container boot + warmup + the live add + 3s drain pause + verdict rendering).

The log file will contain four `VERDICT_M[1-4]` lines among other test output. Grep them out:

```bash
grep -E '^\s+(diag|VERDICT|DIAG)' /tmp/adr-0036-phase-a-run-*.log
```

Quote those lines verbatim into ADR-0036's per-mechanism sections (replace each `TODO_run_to_populate` block) and write the per-mechanism Decision in ADR-0036's "Decision" section based on the captured verdicts.

## Disambiguation: things the run will need to clarify

The mechanisms are not perfectly orthogonal; a few cases need operator interpretation:

- **M1 verdict + M3 verdict can both look like HOLDS.** A long transaction that started before publication-add IS one mechanism by which pgoutput's catalog snapshot for a row record can be "stale" relative to the current catalog. M1 is the workload-side phrasing; M3 is the pgoutput-internal phrasing. Treat them as distinct only if the captured trace shows row events whose `wal_start ≥ lsn_pubadd_after` AND `txn_start_lsn < lsn_pubadd_after` (M1's dispatch-level signature). If row events whose `wal_start ≥ lsn_pubadd_after` arrive but the relation's first-event LSN is well after `lsn_pubadd_after` even when `txn_start_lsn ≥ lsn_pubadd_after`, that's M3 alone.
- **M4 FAILS-with-zero-missing.** If the diagnostic run shows zero loss (target_delivered == source_committed, missing_count == 0), the test's specific scheduler / cadence didn't reproduce CI's failing conditions. The Vultr box's network and CPU profile differ from GitHub Actions' Ubuntu runner. Re-run with adjusted parameters: drop the loader sleep to 1 ms (10x burst rate), or run multiple iterations to gather distribution. The CI under-load test reproduces CI's specific kernel scheduler dynamics; if the Vultr box can't repro the loss, M4's verdict is INCONCLUSIVE for this characterization run.
- **No first-event-for-relation captured (M3 INCONCLUSIVE).** If the burst loader produced 50+ rows on `events` but the captured trace shows zero `cdc.diag` row events with `relation=events` and `first_seen_for_rel=true`, two interpretations: (a) 100% loss in the captured window — the absence IS the signal — re-run with longer drain pause to confirm; (b) the slog handler missed the lines because the test's deferred slog restoration ran before the streamer's pump exited. Inspect the captured log for any `cdc.diag` lines on `events` regardless of `first_seen_for_rel` to disambiguate.

## What NOT to do during the run

- Do NOT push the branch to origin. The main session pushes when the verdicts are captured.
- Do NOT edit ADR-0036's verdict lines speculatively. The whole point of Phase A is the data, not the fix; pre-filled verdicts make Phase B (the actual fix) start from a wrong frame.
- Do NOT bypass the pre-commit hook. The branch passed the local lint/vet/test gate at commit time; if a follow-up commit on this branch trips the hook, fix the root cause.

## What happens after the run

1. Capture the four `VERDICT_M[1-4]` lines from the run log.
2. Edit `docs/adr/adr-0036-mid-stream-loss-surface-characterization.md` and replace each `TODO_run_to_populate` with the captured verdict lines verbatim.
3. Write the per-mechanism Decision in ADR-0036's "Decision" section based on which mechanisms HELD.
4. Per CLAUDE.md's three-phase debug protocol, only THEN does Phase B (the actual fix) start. Do not bundle Phase B into this branch — keep it on a separate branch so the diff for Phase B reviews cleanly against the Phase A artifact.

If the run came back FAILS-or-INCONCLUSIVE across all four mechanisms, treat that as a real outcome (the v0.24.0 best-effort property is the shipping behavior; document the failure-to-reproduce in ADR-0036's "What we still don't know" and close out). Do not loop into more instrumentation without an explicit operator decision — the speculate-and-patch trap is symmetric with the speculate-and-instrument trap.
