# sluice v0.99.62

**A PlanetScale/Vitess source reshard mid-stream is now followed automatically — the sync continues across the cut instead of halting (ADR-0094).** When the source keyspace reshards (a shard split, merge, or `MoveTables`), sluice reopens the stream onto the new shard layout and keeps applying, exactly-once across the seam, with no operator intervention and no re-snapshot.

## Added

- **Reshard auto-follow (ADR-0094).** Vitess signals a reshard with a JOURNAL marker and ends the stream at the cut. Before this release sluice surfaced that as a loud terminal error (`shard layout changed … reopen required`) and the sync stopped until restarted. The CDC reader already detected the reshard and could rebuild the stream against the new layout from the journal-stamped per-shard GTIDs (proven exactly-once by the reshard chaos test) — but the orchestrator never called that path, so it was effectively dead. It's now wired: on the reshard signal the Streamer reopens onto the new layout and continues, with **no gap and no overlap** at the seam (the journal GTIDs anchor the cut), **no re-snapshot**, and **no operator action**. The follow is **bounded** — a pathological reshard storm or a misbehaving reader still fails loud rather than spinning — and a pending schema-forwarding error is never masked by a reopen.

## Compatibility / notes

- **Scope: single-stream Vitess/PlanetScale (VStream) sync.** Both the warm-resume reader and the cold-start path (the production default) now follow a reshard.
- **Shape-A is deferred.** A reshard while `--inject-shard-column` (multi-shard consolidation) is engaged is intentionally **not** auto-followed yet — it keeps the prior loud-terminal behavior; the consolidation/reshard interplay is a tracked follow-up.
- **No effect on non-Vitess sources** (vanilla MySQL binlog, Postgres) — reshard is a Vitess-only concept; those readers are a no-op for this path.
- No flag or config change. The prior behavior (loud terminal on reshard) was a stop-the-sync inconvenience, never silent data loss; this turns it into a seamless continuation.

## Validation

- Unit tests (run under `-race`): exactly-once + order across the reopen seam, reopen-error and budget-exhaustion both loud, Shape-A not followed.
- **End-to-end on a real multi-process Vitess cluster:** a cold-started sync driven through a live **1→2 reshard** mid-stream — the Streamer follows the reshard and lands every pre- and post-reshard row exactly once on the target (src count == dst count, no gap, no dup).

## Who needs this

- Anyone running continuous sync from a **sharded PlanetScale/Vitess** keyspace that may reshard while a sync is live.

## Install

```
# Linux/macOS/Windows binaries + checksums on the release page.
docker pull ghcr.io/sluicesync/sluice:v0.99.62
```
