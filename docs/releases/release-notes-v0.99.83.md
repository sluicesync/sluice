# sluice v0.99.83

**CRITICAL fix: `--apply-concurrency > 1` on a Postgres target could silently drop CDC changes (Bug 158).** This closes the known issue flagged in the v0.99.82 release notes. If you enabled concurrent apply on a Postgres target in v0.99.82, upgrade now. MySQL targets and serial Postgres apply (the default) were never affected.

## Fixed

**Postgres concurrent apply silently lost changes when a table tripped the first-boundary phantom-shape skip (Bug 158, CRITICAL silent loss).** On a Postgres target with `--apply-concurrency > 1`, when a table's first post-cold-start schema boundary was classified as a phantom destructive/mutating shape (the ADR-0091 cold-start-seed-vs-CDC-projection false positive — benign and correctly skipped), the concurrent apply path could silently drop every subsequent CDC change on that stream and freeze the resume position at lsn `0/0` while the source replication slot advanced past the lost WAL. There was no error and the stream kept heart-beating, so the target silently stopped tracking the source — the most dangerous failure class.

The post-release regression cycle caught it on the very first test of the v0.99.82 feature, and instrumentation confirmed the exact mechanism: the concurrent orchestrator's barrier **unconditionally** invalidated the per-table metadata caches on *every* SchemaSnapshot, including the first-touch baseline. That marked the table schema-dirty, which forced every subsequent concurrent-lane DML onto Postgres's text-encode path (`pgx.QueryExecModeExec`); a json/jsonb value — which the CDC reader decodes to `[]byte` per sluice's value contract — then failed to encode as text (`SQLSTATE 22P02`), the lane aborted, and the run wedged with the position pinned at `0/0`. Serial apply was never affected because its boundary invalidation is *guarded*: it fires only on a real signature-changing schema boundary, never the first-touch baseline. (This is why a plain Postgres table, or a table without a text-incompatible binary type like json, did not reproduce it — the over-invalidation was always wrong, but only a json/jsonb-bearing table turned it into total loss.)

The fix removes the orchestrator's separate unconditional invalidation entirely and defers to the engine's guarded apply-then-invalidate — the same path serial uses — so the concurrent path now invalidates byte-identically: a real `ALTER` still busts the caches and the lanes re-probe the live catalog; a phantom first-touch boundary does not. Two companion hardenings make the resume position robust: the concurrent barrier now applies **position-free** (the resume position is owned exclusively by the frontier checkpoint — the ADR-0104 relaxation), and a SchemaSnapshot's metadata-anchored token (the pgoutput first-touch `0/0`) is excluded from boundary tracking, so a phantom boundary can never be persisted as the resume position and warm-resume always lands on the surrounding rows' real LSN. MySQL-target concurrency was not silently affected (its text bind tolerated the over-invalidation) but is fixed symmetrically.

Pinned by a Postgres integration test that drives the first-boundary phantom under `--apply-concurrency=4` across the full value-family matrix with json/jsonb carried as `[]byte` (the load-bearing decode shape — a Go `string` would have masked the bug), asserting the serial-vs-W4 byte-identical differential and a non-`0/0` resume position, plus an orchestrator unit test that a SchemaSnapshot token never becomes the resume position. Validated live on the cross-region Vitess→PlanetScale-Postgres link: W=4 now converges byte-identical to the source and warm-resumes with no loss.

## Compatibility

No interface or default-behavior changes. `--apply-concurrency` still defaults to `0` (serial, byte-identical) on both engines. This release is strictly a correctness fix for the opt-in Postgres concurrent path introduced in v0.99.82; upgrading is recommended for anyone who enabled (or intends to enable) `--apply-concurrency > 1` on a Postgres target.

## Who needs this

Anyone running — or planning to run — `sluice sync --apply-concurrency=W` against a **Postgres target**. v0.99.82 shipped that capability with this CRITICAL silent-loss bug; v0.99.83 makes it safe. MySQL-target concurrency and serial Postgres apply were correct in v0.99.82 and are unchanged here.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.83
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.83
```
