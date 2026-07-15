# ADR-0161: backup-chain concurrent-writer guard (lineage CAS + `SLUICE-E-BACKUP-CHAIN-CONFLICT`)

## Status

Accepted (implemented; roadmap item 64(b)). Numbered 0161 because 64(a) (slot-health notifications) was in flight for 0160 when this chunk started; if 0160 remained free, renumbering is a mechanical rename (the code cites "ADR-0161" in comments).

## Context

Every writer of a backup chain shares ONE read-modify-write object: `lineage.json`, the ADR-0046 lineage catalog. Full-backup finalize, incremental one-shots, `backup stream` rollovers, the rotation FSM's COMMIT, compaction's catalog swap, prune's floor advance, and the reconcile/rebuild repair paths all load it, mutate it in memory, and Put it back whole. Nothing arbitrated between two concurrent writers: two cron'd `backup incremental` runs, a backup racing a `backup compact`/`backup prune`, or an operator double-start would interleave, the last Put won, and the loser's structural update **silently vanished** — a lost catalog append at best, a structurally corrupted chain (mis-parented restore refusals, orphaned segments) at worst. The chunk-level data files are write-once (distinct timestamped paths) and the chain's parent-link walk refuses branches loudly at restore, but the catalog RMW itself had no guard.

Ground truth established before designing (what already existed):

- **`stream_state.json` heartbeat guard** — covers exactly ONE writer pair: stream-vs-stream ("stream is already running", `--force` takeover, freshness window). It does not see one-shot incrementals, compaction, prune, or fulls.
- **Full-into-existing-dir refusal** — `PartialState`/`--force-overwrite` guards re-running a full into an occupied destination, not concurrent catalog writers.
- **Restore-side branch refusal** — the strict per-link chain walk catches an already-branched lineage loudly at restore time; detection, not prevention, and only for the data-chain shape (a lost catalog append is invisible to it).

So the gap was real and specifically the catalog RMW. Prior art: burnside-project's `pg-cdc` CAS-es its manifest writes (S3 `If-Match` ETag; filesystem O_EXCL/rename) with a dual-writer circuit breaker after N conflicts (see the 2026-07-15 addendum in `docs/research/sluice-as-analytics-source.md`).

**The storage-layer constraint that shaped the design:** sluice's cloud backends ride `gocloud.dev/blob` (v0.46.0), which portably exposes only *create-if-absent* (`WriterOptions.IfNotExist` → S3/Azure `If-None-Match: *`, GCS generation-0, fileblob O_EXCL) — **not** conditional *overwrite* (`If-Match` ETag). A direct ETag CAS on `lineage.json` itself is therefore not available without per-provider SDK forks.

## Decision

### CAS via generation-claim markers (create-only primitive → full compare-and-swap)

The catalog write becomes a compare-and-swap on a **chain write-generation** arbitrated by create-only claim markers, `lineage.gen/g-<N>` (zero-padded, lexical == numeric), at the lineage root:

1. **Observe (at load), then read.** `lineage.LoadLineageCatalogForUpdate` lists `lineage.gen/` and records the max claimed generation *before* reading `lineage.json`, stamping the observation on the returned `Catalog` (unexported fields — never serialized). Observe-before-read is load-bearing: the reverse order would let a writer observe PAST a competitor's just-landed update and silently clobber it; observe-first turns that window into a spurious-but-safe conflict.
2. **Claim (at write), then Put.** `lineage.WriteLineageCatalog` on a stamped catalog first creates `g-<observed+1>` via the store's `PutIfAbsent`. Exactly one concurrent writer wins the slot; the loser's create fails with `irbackup.ErrPathExists` and the write **refuses loudly** — coded `SLUICE-E-BACKUP-CHAIN-CONFLICT` (refusal class, registered in `internal/sluicecode` + `docs/operator/error-codes.md`) — having written **no catalog change**. The marker body is forensic JSON (`claimed_at`, `sluice_version`, `host`, `pid`) so the conflict message can point the operator at the other writer.
3. **GC.** After a successful write, markers below a trailing window of 8 are best-effort deleted (bounded litter; see residuals for why a window, not just-the-newest).

Liveness without leases: a writer that claims a slot and crashes before its Put leaves an **orphaned marker, not a stale lock** — the next writer's observation lists markers (not catalog content), sees the orphan as the new base, and claims the slot after it. No TTLs, no clock heuristics, no manual unlock.

### Optional-capability shape

`irbackup.ConditionalPutter` (`PutIfAbsent(ctx, path, r)` failing with `ErrPathExists` when occupied) is a new OPTIONAL `Store` capability, the same type-assert-and-degrade pattern as `irbackup.Appender` (progress sidecar). Stores without it keep today's unguarded last-write-wins behavior — pinned by test so the degrade is deliberate.

Backend support matrix:

| Backend | Primitive | Guarded |
|---|---|---|
| LocalStore (`--output-dir`) | `O_CREATE\|O_EXCL` (+fsync) | yes (pinned: unit + interleaved-writer test) |
| S3 (`s3://`) | gocloud `IfNotExist` → `If-None-Match: *` | yes (pinned against real MinIO, which enforces server-side; AWS S3 has supported conditional PUT since 2024) |
| GCS (`gs://`) | gocloud `IfNotExist` → generation-0 precondition | yes (same code path; per-driver mapping pinned via fileblob + memblob drivers; no GCS emulator in the suite) |
| Azure (`azblob://`) | gocloud `IfNotExist` → `If-None-Match: *` | yes (same caveat as GCS) |
| fileblob (`file://`) | gocloud `IfNotExist` → exclusive create | yes (pinned: unit, real driver) |
| `prefixedStore` (segment sub-dir views) | not forwarded | n/a — `lineage.json` lives at the lineage ROOT; every catalog writer uses the root store. (Mirrors the existing posture: `prefixedStore` doesn't forward `Appender` either.) |

**Runtime degradation (named wart):** an S3-compatible endpoint that predates conditional writes may reject (or ignore) `If-None-Match`. A claim failure that is *not* `ErrPathExists` degrades to an unguarded write with a WARN ("proceeding UNGUARDED for this write") instead of bricking backups against such providers — availability over a hard refusal for a *hardening* feature whose absence is exactly yesterday's shipped behavior. A server that silently *ignores* the header is undetectable per-request; the MinIO integration test exists precisely to prove enforcement on the S3-compatible class we test against. Pinned: `TestChainGuard_DegradeOnNonConflictClaimError`.

### Refuse-on-conflict, no reconcile, no breaker

On conflict the operation refuses immediately. The pg-cdc N-conflicts circuit breaker was rejected (loud-failure tenet: the FIRST conflict is already evidence of a mis-configured scheduler or a racing maintenance job; deferring the signal for N rounds hides it). Benign-reconcile (re-read and linearize when the other writer's change is provably independent) was considered and **deliberately skipped for v1**: the writers mutate overlapping catalog state (open-segment incrementals list, EndPosition, segment list, restore floor), so "provably benign" is nearly never provable — plain refuse-on-conflict with a clean re-run is the honest contract, and the error message promises exactly that.

### Every catalog writer covered

- `backup full` finalize and `backup incremental` — via `UpdateLineageForManifest[BestEffort]`, which now loads-for-update internally. **`…BestEffort` keeps swallowing ordinary catalog hiccups (WARN) but returns the conflict** — a concurrent writer is not a transient store failure, and the next write would not heal it. Full/incremental runs fail loudly on it (their own manifest is already durable; only the catalog append was refused, and the next writer / stream resume re-catalogs it).
- `backup stream` rollovers (`commitRollover`) — conflict fails the stream run. The ctx-cancel **drain-commit** path instead WARN-logs the conflict with its code: the stream is already exiting, the manifest is durable, and the resume-time reconcile re-catalogs it (its own guarded writes surface a persistent dual-writer loudly).
- Rotation FSM COMMIT — loads-for-update at FSM entry; a conflicting COMMIT aborts **stay-open** (provisional segment discarded, prior segment intact — the FSM's existing failure containment).
- Compaction catalog swap, prune floor advance, `ReconcileOpenSegmentCatalog`, `backup verify --rebuild-catalog` — all load-for-update (rebuild observes at entry, before its walk).

### Prune reordered to commit-then-sweep

Pre-existing hazard the guard would have amplified: prune deleted manifests/chunks *before* rewriting the catalog, so any failed catalog write (now including a CAS refusal) stranded a catalog referencing deleted files — and the promised "re-run" remediation would hit missing manifests. Prune now **commits the catalog first, then runs the delete pass** — the exact commit-then-sweep order compaction has always used. A refused (or crashed) prune therefore leaves the chain byte-untouched; a post-commit crash leaves only orphaned, already-uncatalogued files (disk, not correctness). Pinned: `TestPruneChain_ConcurrentWriterConflictLeavesChainUntouched` (refusal → catalog unchanged, all manifests present, re-run succeeds).

## Alternatives considered

- **Lockfiles / lease objects** — rejected: the stale-lock liveness problem on object stores (a crashed holder blocks the chain until a TTL or a human; TTLs reintroduce clock trust). The claim-marker scheme has no held state to go stale.
- **ETag `If-Match` conditional overwrite of `lineage.json` itself** — the conceptually purest CAS, but not portably exposed by gocloud v0.46.0; would require per-provider SDK code for all four backends. The generation-marker scheme achieves the same guarantee over the one primitive every backend shares.
- **Generation counter stored inside `lineage.json`** (no marker listing) — rejected: a claim orphaned by a crash would then permanently conflict with the catalog-recorded generation — a brick requiring manual repair (the stale-lock problem in different clothes). Listing markers as the observation source is what buys lock-free liveness.
- **N-conflicts circuit breaker (pg-cdc)** — rejected, loud-failure tenet (above).

## Residuals

- **Marker-GC slot-reopen window.** Deleting old markers can theoretically re-open a claimed slot: a writer stalled mid-RMW across ≥ `chainGenKeepTrailing` (8) *complete* competing catalog writes could claim a re-opened generation and clobber. The RMW window is seconds; competing writes are minutes-to-hours apart; and today's exposure is an *infinite* unguarded window — accepted, documented on the constant.
- **GCS/Azure legs are code-path-covered, not emulator-pinned.** The gocloud `IfNotExist` mapping is pinned through three real drivers (fileblob, memblob, s3blob-on-MinIO); no GCS/Azurite emulator runs in the suite. The per-driver precondition plumbing is upstream-tested (gocloud drivertest); first real-cloud validation should note it here.
- **`rotation_state.json` / `stream_state.json`** remain owned by the (stream-state-guarded) stream process and are not CAS-protected; they are recovery markers, not the structural record.
- **The drain-commit WARN** (stream exiting on ctx-cancel) is the one conflict site that logs rather than fails — deliberate, documented above.
- **No unit conflict pin for compaction/rotation writers specifically** — they share the identical `LoadLineageCatalogForUpdate` → `WriteLineageCatalog` pair pinned at the lineage layer and via prune; a compact-specific interleave fixture wasn't worth its weight for v1.
