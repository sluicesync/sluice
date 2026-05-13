# sluice v0.43.0 — VStream COPY-phase dedup (closes #14)

**Closes GitHub issue #14.** Operator-confirmed in the v0.42.0 retest: a single PlanetScale-MySQL source feeding three target engines (vanilla MySQL, PS-Postgres, PS-MySQL) failed cold-start with duplicate-PK errors on **all three simultaneously**, ruling out any target-side hypothesis. The source-side root cause is Vitess's intentional COPY-mode behavior, documented in the upstream VStream Copy RFC ([vitessio/vitess#6277](https://github.com/vitessio/vitess/issues/6277)):

> "We copy a batch of rows until a particular PK using a consistent snapshot. However once the copy is completed the binlog position would have moved possibly containing updates to the rows already transmitted. Hence we need to perform a 'catchup' where we play the events up to the current position. **We can only send updates to rows that we have already sent to the stream.**"

So Vitess emits TWO kinds of ROW events on the same gRPC stream during COPY:

1. **Forward COPY emissions** — rows ordered by PK ascending, from the consistent-snapshot scan.
2. **Catchup-phase replay** — binlog events for rows already past the COPY scan's lastpk.

Sluice's pre-v0.43.0 VStream snapshot reader buffered both kinds as snapshot rows. The catchup emissions reached the bulk-copy writer as fresh INSERTs; the writer's destination already had the row from the forward emission and rejected the second INSERT with `Error 1062 (23000)` / `SQLSTATE 23505`. v0.43.0 tracks max-PK-seen per `(keyspace, shard, table)` scope and drops behind-the-scan emissions — exactly the dedup pattern Vitess's RFC documents for clients to implement via `LastTablePK`.

## Fixed

- **`internal/engines/mysql/vstream_copy_dedup.go` (new)** — `copyDedupTracker`:
  - PK column identification from FIELD events via the `query.MySqlFlag_PRI_KEY_FLAG` bit (no separate `information_schema` round-trip needed — Vitess emits PK metadata inline).
  - Composite PKs compared lexicographically by column.
  - Type-aware compare for `int64` / `uint64` / `float64` / `string` / `[]byte` / `time.Time` / `bool` (matching the canonical IR-Row types from `decodeVStreamCell`).
  - Tables without a declared PK fall through (no dedup possible; v0.42.x behaviour preserved).
- **`internal/engines/mysql/cdc_vstream_snapshot.go`** — `vstreamSnapshotStream` gained a `dedup *copyDedupTracker` field. `dispatchCopyEvent` FIELD branch calls `dedup.recordFields`; `bufferCopyRow` consults `dedup.shouldKeep` before appending to `rowBuffer`. Dropped rows are replayed via the post-COPY CDC phase (the binlog tail resumes from the snapshot's terminal GTID), where the applier's idempotent semantics (ADR-0010) absorb them.
- **DEBUG-level summary at COPY_COMPLETED** — operators on busy keyspaces get a single log line at the snapshot→CDC boundary: `mysql/vstream: snapshot: COPY-phase dedup summary (GitHub #14) drops_by_scope="<scope>=<N>, ..."`. Empty/no log for streams that saw no re-emissions.

## Migration / Compatibility

- **Drop-in upgrade from v0.42.x.** No CLI changes, no IR changes, no engine-interface changes.
- **PG / vanilla-MySQL targets fed from a Vitess source**: drop-in benefit. The fix is source-side, so target engine doesn't matter.
- **Same-engine MySQL → MySQL on a vanilla MySQL source**: drop-in; this path uses the binlog reader (not VStream), so the dedup is unreachable.
- **Tables without a primary key**: dedup is a no-op (the tracker can't identify what to dedup).
- **CDC apply-phase semantics**: unchanged. The dedup only filters COPY-phase emissions; CDC ROW events are passed through untouched.

## Who needs this release

- **Anyone running `sluice sync start` against a PlanetScale-MySQL or self-hosted Vitess source under concurrent source writes**: **upgrade**. This closes the only remaining open bug from the v0.39.1 → v0.42.0 fix sequence.
- **Operators on quiescent / write-paused source databases**: drop-in; no behaviour change (catchup-phase emissions only fire under concurrent writes).
- **Operators not using VStream**: drop-in; the code path is unreachable.

## Verification surface

13 new unit tests in `internal/engines/mysql/vstream_copy_dedup_test.go`:

- nil-tracker fall-through (every row kept)
- No-PK table (every row kept — dedup is keyed on PK presence)
- Single int PK, monotonic forward (zero drops, summary empty)
- **The GitHub #14 repro shape** — forward emissions up to PK 1100, then a behind-the-scan PK 545 (the v0.42.0 retest's actual repro IDs), drop count matches
- Per-scope independence across multi-shard streams (`-80` and `80-` track independently)
- Composite PK lexicographic compare on `(region, id)` with cross-region forward / behind / equal cases
- Every value-type compare path in `comparePKCell` (int64 / uint64 / float64 / string / []byte / time.Time / bool / nil / type-mismatch fall-open)
- `recordFields` idempotency on FIELD-event re-emit (Vitess can re-emit on DDL or stream restart)
- Summary string format pinning (for operator log grep)

## Issue tracker after v0.43.0

| # | State | Resolution |
|---|---|---|
| 12 | ✅ Closed | v0.40.0 — CDC generated-column filter |
| 13 | ✅ Closed | v0.42.0 — bounded retry on transient applier errors (ADR-0038) |
| 14 | ✅ Closed | v0.43.0 — **VStream COPY-phase dedup** |
| 15 | ✅ Closed | v0.41.0 — pre-CDC anchor write |

Three production-reported bugs closed in 24 hours via four sequential releases. With v0.43.0 shipping, the Phase A diagnostic branch (`phase-a-issue-14`, commit `b995afb`) is retained as a historical artifact but unnecessary for ongoing operations — the dedup fix supersedes its purpose and the per-COPY_COMPLETED summary log provides equivalent (and permanent) operator visibility.
