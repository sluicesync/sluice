# sluice v0.99.179

**Fix: a refused Postgres-source `sync start` cold-start no longer leaves its replication slot behind on the source (Bug 177). Pre-fix, the `SLUICE-E-COLDSTART-TARGET-NOT-EMPTY` refusal orphaned the just-created slot — pinning source WAL until manually dropped — and the refusal's own preferred recovery (`--reset-target-data`) then failed on "slot already exists" until an un-hinted manual `sluice slot drop`. Loud failure, zero data risk, latent since v0.10.0.**

## Fixed

**Refused/failed PG-source cold-starts clean up their replication slot (Bug 177; latent in v0.10.0–v0.99.178).** The sync cold-start opens its snapshot — which atomically creates the logical replication slot — before the target-side preflights run, and every refusal or setup-failure exit merely closed the stream, deliberately leaving the slot alive (for a CONSUMED stream that is correct: the slot is the CDC resume anchor). An unconsumed cold start has no anchor, so the leftover slot was pure debris: it retained WAL on the source until someone noticed, and it broke the refusal hint's preferred `--reset-target-data` recovery with `replication slot "sluice_slot" already exists`. The snapshot stream now distinguishes the two exits: `Abandon()` ("this cold start will never consume the stream") releases everything Close does and additionally drops the slot the open created, and every pre-anchor exit on the sync cold-start — target-writer open failures, connection-budget/RLS/shard/populated-target refusals, bulk-copy failure, and a failed CDC-anchor write — abandons instead of closes, on both the single-database and multi-database/multi-schema paths. The boundary is the anchor write: once the CDC position is durably persisted, warm-resume depends on the slot, and teardowns keep today's slot-preserving Close. MySQL-binlog and VStream sources create nothing durable at open and are byte-identical. A failed slot drop is loud — a WARN naming the manual `pg_drop_replication_slot` command plus the surfaced error — and the sync refusal hint now also names the slot-drop step for the one shape the fix cannot reach (a hard-killed process can't run cleanup).

Pinned by unit tests (the Abandon dispatch, the engines-without-slots fallback, error propagation, and the orchestrator refusal path actually invoking Abandon) and real-Postgres integration tests: Abandon drops the slot and a fresh cold-start then succeeds, Close still preserves a consumed stream's slot, and the full Bug-177 shape end-to-end — populated-target refusal → zero slots on the source → `--reset-target-data` recovery succeeding first try.

## Compatibility

**No breaking changes; drop-in.** No flags added or changed. The only behavior change is on PG-source sync cold-start ERROR paths before the CDC anchor exists, where the previously-leaked slot is now dropped; consumed streams (normal operation, warm-resume, CDC) keep the slot exactly as before. Operators who scripted a manual `sluice slot drop` after refused runs can remove that step for v0.99.179+; it remains necessary only after a hard kill (SIGKILL/power loss), which no in-process fix can cover.

## Who needs this — action required

- **No one must act.** The failure was loud with zero data risk; nothing needs re-verification.
- **Anyone running PG-source continuous sync in automation:** refused cold-starts (wrong target, populated target, budget/RLS refusals) no longer accumulate WAL-pinning slots on the source, and the documented `--reset-target-data` recovery now works first try after a refusal.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.179 · **Container:** ghcr.io/sluicesync/sluice:0.99.179
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
