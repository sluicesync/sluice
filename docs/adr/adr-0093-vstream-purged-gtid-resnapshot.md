# ADR-0093: VStream purged-GTID resume → reactive cold-start re-snapshot (with opt-out)

## Status

Accepted; **amended by Bug 146** (Part 4 below). Extends
[ADR-0022](adr-0022-slot-missing-fall-through.md) (slot-missing / invalid-position →
cold-start fall-through) to the PlanetScale/Vitess VStream source, and adds an operator
opt-out. Discovered by cross-referencing PlanetScale's own `planetscale/fivetran-source`
connector PRs (#69, #73) against sluice's VStream reader.

**Bug 146 amendment (the load-bearing part for Vitess 24):** the original reactive
classifier (Part 1) proved insufficient — Vitess 24's vtgate does not surface a reactive
purged error; it idles. Part 4 adds the **proactive `gtid_purged` pre-flight** that
actually closes the gap. Read Part 4 with Part 1.

## Context

When a persisted resume position is older than the source's retained binlogs
(`gtid_purged` has advanced past it), the only recovery is a fresh cold-start
re-snapshot — CDC cannot bridge the gap. sluice already does this **for the
self-hosted MySQL binlog source**: `verifyPositionResumable` / `verifyGTIDSetReachable`
(`internal/engines/mysql/cdc_reader.go:1049,1170`) run a **pre-flight** `gtid_purged ⊆
resume` check at `StreamChanges` time and return `ir.ErrPositionInvalid`; `warmResume`
propagates it synchronously and the streamer's pre-flight fall-through
(`internal/pipeline/streamer_run_phases.go:537,590`) re-enters `coldStart` in the same
run (ADR-0022). Auto-recovery, no operator action.

**The VStream/PlanetScale source has no equivalent.** Two reasons, both confirmed by
code-reading:

1. **No clean pre-flight.** vtgate is a proxy; there is no single `@@gtid_purged` to
   subset-check the way a direct MySQL connection allows. `vstreamCDCReader`
   (`cdc_vstream.go:620,648`) opens the stream directly. So the purged condition can
   only be discovered **reactively** — vtgate rejects the position on the stream and the
   pump's `stream.Recv()` returns the error (`cdc_vstream.go:788-793`).
2. **The reactive path is not routed to cold-start.** The pump stores the error via
   `setErr(classifyReaderError(...))`; it surfaces through `surfaceSourceError`
   (`streamer.go:1146`) → `phaseSettleDispatch` → `runWithRetry`
   (`streamer_retry.go:104-126`). `classifyReaderError`
   (`internal/engines/mysql/reader_errors.go:68`) maps the vtgate error only to
   *retriable* (gRPC `Unavailable/Aborted/Unknown/ResourceExhausted`) or *terminal* —
   **never `ir.ErrPositionInvalid`** — and `runWithRetry` returns a non-retriable error
   terminal. So a purged-position VStream resume **exits**, and on supervisor restart
   `warmResume` re-opens the stream and hits the same purged position again: a restart
   loop, not the clean re-snapshot the binlog path performs.

This is exactly the PlanetScale deployment where it bites — the platform's ~3-day binlog
retention makes a resume position that has fallen behind retention a routine event (the
same mechanism sluice's binlog purged-GTID test header already calls out). The
`fivetran-source` connector handles it (PR #69 resets the cursor to force a fresh
historical sync on `"Cannot replicate because the source purged required binary logs"`;
PR #73 stamps a `BINLOG_EXPIRATION_ERROR` marker). sluice should reach parity with its
own binlog path.

This is a **loud-failure resilience gap, not silent data loss** — today's behavior is a
confusing terminal error / restart loop, never wrong data.

## Decision

Three coordinated parts.

### 1. Classify the vtgate purged-position error as `ir.ErrPositionInvalid`

Add a named matcher `isVStreamPurgedGTIDError` (sibling of `isVStreamSchemaResolutionError`)
in `reader_errors.go`, matched on the discriminating substring **`purged required binary
logs`** — which covers both MySQL 1236's canonical "the master has purged required binary
logs" and Vitess's inclusive "the source purged required binary logs" wording. In
`classifyReaderError`, check it **before** the gRPC-code and applier-classifier branches
(a purged error that happens to carry `codes.Unknown` must NOT be mis-classified
retriable — retrying a purged position spins forever), and wrap `ir.ErrPositionInvalid`
with an actionable message that also carries the original error text for diagnostics.
Kept as a named helper so a test pins the exact wording set (a vtgate wording change then
fails the pin rather than silently reverting to a restart loop).

### 2. Route a reactive `ir.ErrPositionInvalid` to a one-shot cold-start re-snapshot

In the `Run` retry path (`runWithRetry`, and the `attempts == 1` direct `runOnce` path),
when `runOnce` returns an error that `errors.Is(err, ir.ErrPositionInvalid)` and it is not
a bare ctx-cancellation: log a loud WARN naming the position, then **re-run `runOnce`
once** with `RestartFromScratch = true` (the existing forced-cold-start knob — ignores
the persisted position and re-snapshots). On the VStream/PlanetScale path this ADR targets,
the cold-copy is idempotent (UPSERT, Bug 125), so the re-snapshot absorbs the overlap with
no target drop. **Amendment (#244 value-fidelity review):** the same `RestartFromScratch`
knob is also reached for NON-idempotent native-MySQL-binlog sources (whose cold-copy is
plain INSERT). On that path "absorb the overlap" never held — a fresh plain-INSERT onto the
prior copy's leftover rows dup-key-errors (MySQL Error 1062). The cold-start gate
(`coldStartGatePreflight` → `resetTargetTablesForRestart`) therefore drops + recreates the
in-scope target tables before the re-copy when the reader is non-idempotent, so the auto-
resnapshot lands cleanly for native MySQL too; the cdc-state row is preserved (only the
position is discarded). The idempotent VStream path is unchanged. This is **bounded**: at most one re-snapshot per
purged-position detection (a second consecutive `ir.ErrPositionInvalid` after a fresh
cold-start is terminal — it would indicate the source is purging faster than the snapshot
completes, which auto-retry cannot fix and must surface loudly). Mirrors the binlog
pre-flight fall-through's outcome, at the reactive layer the VStream path needs.

### 3. Opt-out flag `--no-auto-resnapshot` (default: auto, parity with binlog)

A boolean CLI flag (kong, mirroring the `--no-auto-tune` opt-out convention), threaded to
`Streamer.AutoResnapshotOnInvalidPosition` (`= !NoAutoResnapshot`, default true). When the
operator sets `--no-auto-resnapshot`, **both** the existing pre-flight fall-through
(ADR-0022 sites) **and** the new reactive recovery are suppressed: `ir.ErrPositionInvalid`
instead surfaces as a **loud, actionable terminal error** naming the recovery commands
(`--restart-from-scratch` / `--reset-target-data`). This gives operators who would rather
not have a surprise full re-snapshot (e.g. a very large table where a re-snapshot is
expensive and they want to decide deliberately) an explicit off switch, while keeping the
resilient, binlog-symmetric behavior as the default. Gating both paths on one flag keeps
the binlog and VStream behavior consistent under the opt-out.

### 4. Proactive `gtid_purged` pre-flight on the VStream open path (Bug 146 amendment)

**Parts 1–3 assumed the reactive error arrives. On Vitess 24 it does not.** A local
multi-process Vitess-24 cluster repro (the `vitess-cluster-validator` harness) proved that
re-opening a VStream from a purged position surfaces **no** `purged required binary logs`
error: `uvstreamer` accepts a position that is *behind* the tablet, the tablet's mysqld
drops the binlog dump with errno 2013 (`CRServerLost`), and vtgate treats that as a clean
stream end — emitting only heartbeats. The stream therefore **idles** into the Phase-1
liveness watchdog timeout, which (per Bug 141) is **retriable** — so it retry-loops on the
same purged position and never cold-starts. Part 1's reactive classifier can never fire on
this version. (PlanetScale's hosted vtgate *may* surface the reactive error — the
fivetran evidence suggests so — but relying on it is not portable.)

The fix is a **proactive pre-flight** mirroring the binlog reader's `verifyGTIDSetReachable`:
in `vstreamCDCReader.StreamChanges`, before opening the gRPC stream, for a decoded
(resumed) CDC-tail position, query `GTID_SUBSET(@@global.gtid_purged, <resume>)` and return
`ir.ErrPositionInvalid` when the resume is unreachable (subset = 0). The orchestrator's
ADR-0022 / Part-2 fall-through then cold-starts (or, under `--no-auto-resnapshot`, surfaces
the loud terminal error). Two findings from the Phase-A probe make it correct:

- **`gtid_purged` is tablet-type-routed by vtgate** (default → primary). The CDC tail
  streams from the configured `vstream_tablet_type` (default **replica**), and a replica
  can purge independently of the primary — so the pre-flight MUST read `gtid_purged` from
  the **same tablet type the stream binds to**, via vtgate's `keyspace@<tablettype>` target
  syntax (a `cfg.Clone()` with the retargeted `DBName`, opened with the existing
  `discoverShards` `openDB` pattern). Reading the default (primary) value would validate a
  tablet the stream never reads — leaving the wedge in place when a replica has purged more.
- The resume GTID carries the Vitess `<flavor>/` prefix (`MySQL56/…`), which `GTID_SUBSET`
  rejects (ER 1772); it is stripped (`stripGTIDFlavor`) before the query.

**Degrade, don't refuse:** if the probe connection or query fails (e.g. vtgate's transient
`no healthy tablet` during warmup), the pre-flight logs and **proceeds** — only a definitive
`GTID_SUBSET = 0` refuses, so a check that can't run never forces a spurious re-snapshot
(mirrors `verifySourceInstanceIdentity`'s degrade philosophy). The reactive classifier
(Part 1) is **retained as defence-in-depth** for any source that does surface the error.
Scope: the pure CDC-tail resume; a mid-COPY resume (TablePKs cursor) runs the ADR-0072
primary-pinned path and is out of scope. The cluster integration test (originally written
to pin the *reactive* path, then the documented gap) now asserts the **proactive** refusal:
`StreamChanges` returns `ir.ErrPositionInvalid` synchronously, non-retriable.

## Consequences

- **VStream resume from a purged position auto-recovers** (re-snapshot) by default, the
  same as the binlog path — no more restart loop on the PlanetScale flavor. On Vitess 24
  this works via the **proactive pre-flight** (Part 4); the reactive classifier (Part 1)
  remains for sources that surface the error.
- **One opt-out** for operators who want a deliberate, loud stop instead of an automatic
  re-snapshot. The default favors uptime/resilience; the flag favors control.
- **No silent data loss either way** — auto-resnapshot re-establishes a consistent full
  copy; the opt-out is a loud actionable error.
- A second `ErrPositionInvalid` immediately after an auto re-snapshot is terminal (loud) —
  the auto-recovery cannot mask a source that purges faster than a snapshot completes.
- Behavior change to the binlog path **only under the new flag** (its default
  auto-recovery is unchanged); the flag merely makes the existing behavior suppressible.

## Testing

- **Unit:** `classifyReaderError` maps the `purged required binary logs` wording (both
  master/source variants) to `ir.ErrPositionInvalid` and NOT to retriable — pin the exact
  wording (sibling of `TestClassifyReaderError_SchemaResolution`). `runWithRetry` re-runs
  once in forced cold-start on a reactive `ir.ErrPositionInvalid` (stub source), and with
  `--no-auto-resnapshot` returns the loud actionable error instead; the bounded one-shot
  (no infinite re-snapshot loop) is pinned.
- **Integration (`-race`, Vitess cluster — via the `vitess-cluster-validator` harness):**
  start a VStream sync, capture a resume position, advance the source's `gtid_purged` past
  it (e.g. `FLUSH BINARY LOGS` + `PURGE BINARY LOGS` on the underlying tablet, or the
  cluster's purge mechanism), restart the sync, and assert it auto re-snapshots and
  re-converges (default), and that `--no-auto-resnapshot` instead fails loudly with the
  recovery message.
- **`-race`-before-tag:** this touches the streamer `Run`/retry loop (concurrency-
  adjacent), so the `-race` integration gate runs before the tag is cut.

## Alternatives considered

- **VStream pre-flight `gtid_purged` check (symmetric with binlog):** rejected — vtgate
  exposes no single authoritative `gtid_purged` to subset-check; the reactive error is the
  only reliable signal Vitess gives, which is why PlanetScale's own connector matches on
  it.
- **Classifier only, no auto-recovery (loud actionable error as the default):** rejected
  as the default — it leaves the VStream path *less* resilient than the binlog path for no
  good reason; offered instead as the `--no-auto-resnapshot` opt-out.
- **Make the reactive error retriable (let the ADR-0038 backoff loop handle it):**
  rejected — retrying the *same* purged position never succeeds; it would convert a
  restart loop into a slower in-process spin. The position is invalid, not transient.
