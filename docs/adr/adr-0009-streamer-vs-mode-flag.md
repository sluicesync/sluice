# ADR-0009: Streamer as separate orchestrator (not a Mode flag)

## Status

Accepted. Implemented in `internal/pipeline/streamer.go` alongside the existing `internal/pipeline/migrate.go` (`Migrator`).

## Context

Sluice has two operator-facing modes: one-shot bulk migration (simple mode, served by `pipeline.Migrator`) and continuous sync with snapshot+CDC handoff and warm resume (served by, well, the question this ADR answers).

The natural design choice was: extend `Migrator` with a `Mode` field (`ModeSimple` / `ModeSnapshotPlusCDC`), or add a separate `Streamer` type. The two modes share the schema-apply + bulk-copy phases; the difference is what happens after.

Mode-flag arguments: one orchestrator type, fewer files, the user picks behavior with one bool. Separate-type arguments: lifecycle, required parameters, and failure modes differ enough that conflating them produces a `Run` method that does dramatically different things based on a flag — exactly the kind of "valid only if Mode == X" friction the project tenets call out.

## Decision

Add `pipeline.Streamer` as a separate type. Share the schema-apply + bulk-copy phases via a private `runBulkCopy` helper that both `Migrator` and `Streamer` call. Each type stays parameter-light for its own use case: `Migrator` doesn't carry an unused `ChangeApplier` field; `Streamer` doesn't carry an unused `DryRun` field.

`Streamer.Run` is long-running (returns on ctx cancellation); `Migrator.Run` is one-shot (returns when bulk-copy + constraints are done). Failure semantics differ accordingly.

## Consequences

Two orchestrator types instead of one, ~200 lines each. The shared helper means duplication isn't real — both paths route through the same schema-apply sequence. Adding a future mode (e.g., incremental-only) is a third type that calls `runBulkCopy` (or not) without disturbing either existing one.
