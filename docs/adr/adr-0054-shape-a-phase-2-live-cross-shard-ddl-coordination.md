# ADR-0054 — Shape A Phase 2: live cross-shard DDL coordination

**Status:** **Accepted (2026-05-22).** All four decision points
signed off by the owner via AskUserQuestion dialogue. Closes
ADR-0048 §4's deferred Phase 2 surface (control-table DDL lease).
Lifts ADR-0048 DP-3's "drained model for v1" restriction by adding
a per-stream `--no-coordinate-live-ddl` opt-out around a default-on
live-coordination path.

## Context

ADR-0048 v1 ships Shape A's discriminator-column injection,
populated-target preflight, and composite-PK rewrite (v0.72.0,
hotfixed in v0.72.1 / v0.72.2). DP-3 resolved cross-shard schema
migration to the **drained model** for v1: operator runs
`sluice sync stop --wait` on every shard, runs one cross-shard
schema migrate, then runs `sluice sync start --resume` on every
shard.

The drained model is correct but operationally heavy on N-shard
fleets: it requires an outage window proportional to the slowest
shard's drain. For PlanetScale Vitess sources (the make-or-break
audience per the [[planetscale-validation-track]] memory) where
shards are independently scaled and may have very different lag,
the slowest-drain pole stops the world.

Phase 2 — what this ADR ships — adds **live cross-shard DDL
coordination**: when source DDL is observed, the first stream to
notice acquires a target-side lease, applies the DDL exactly once,
records the applied schema version + DDL checksum; other shards
observe the recorded state and skip the apply, continuing CDC
against the migrated target without a drain.

The design space was sketched in ADR-0048 §4 ("Cross-shard
schema-migration coordination — a control-table DDL lease"). This
ADR fills in the operational details that §4 named as Phase 2
concerns: lease semantics, DDL idempotence handling, crash recovery
on non-transactional-DDL engines, and the engagement flag.

## Tenet alignment, established up front

- **Loud-fail beats silent corruption.** DDL-checksum mismatch
  across shards (one shard sees ALTER X, another sees ALTER Y at
  the same boundary) is the silent-divergence hazard; the lease's
  checksum-recorded shape (DP-B) refuses loudly when shards have
  diverged, never silently picks a winner.
- **Reuse existing substrate.** The lease lives in a new control
  table next to `sluice_cdc_state`; the additive-migration pattern
  is the same `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` /
  `CREATE TABLE IF NOT EXISTS` shape ADR-0030/0034/0031 and v0.72.x
  already use. No new lock service, no etcd.
- **Heartbeat-based liveness, TTL safety net.** The lease pattern
  directly mirrors Vitess OnlineDDL's coordination — the most
  relevant prior art for sluice's PlanetScale Vitess audience. See
  the §"Prior art" appendix for the Kubernetes / etcd / Consul
  parallels.

## Decision

### 1. Lease state machine and persistence

A new control table `sluice_shard_consolidation_lease` (additive
migration on both engines) tracks one row per consolidated target
table:

```sql
CREATE TABLE IF NOT EXISTS sluice_shard_consolidation_lease (
    -- Scoped to consolidated-table-identity (ADR-0048 §4: NOT
    -- stream-id). All shards' streams converge on the same row
    -- for the same target table.
    target_table_full_name        VARCHAR(512) NOT NULL,

    -- Lease state. Held: expires_at > now(). Expired: expires_at
    -- <= now() AND applied_at IS NULL → takeover-eligible.
    -- Applied: applied_at IS NOT NULL → observers verify checksum
    -- + advance their schema-version pointer (no re-apply).
    lease_holder_stream_id        VARCHAR(64)  NULL,
    lease_expires_at              TIMESTAMP    NULL,

    -- DDL identity. Recorded by the lease-holder after the ALTER
    -- commits. Observers compare their observed DDL's checksum to
    -- this; match → skip; mismatch → loud refusal.
    ddl_text                      TEXT         NULL,
    ddl_checksum                  VARCHAR(64)  NULL,   -- SHA-256 hex of normalized DDL text
    applied_schema_version        BIGINT       NOT NULL DEFAULT 0,
    applied_at                    TIMESTAMP    NULL,

    -- Cleanup discriminator. Lease rows whose applied_schema_version
    -- is <= every stream's current schema-version cursor can be
    -- garbage-collected by a background sweep. Out of scope for v1
    -- Phase 2; tracked as a follow-up.
    created_at                    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (target_table_full_name)
);
```

State transitions:

```
       ┌─────────────────────────────────────────────────────────┐
       │                                                         │
       │           ABSENT (no row)                               │
       │                                                         │
       └───────────┬─────────────────────────────────────────────┘
                   │ Stream observes DDL, attempts INSERT with
                   │ lease_holder_stream_id + lease_expires_at = now + TTL
                   ▼
       ┌─────────────────────────────────────────────────────────┐
       │                                                         │
       │           HELD (expires_at > now)                       │
       │           Lease-holder heartbeats every RetryPeriod to  │
       │           extend expires_at by LeaseDuration            │
       │                                                         │
       └───────────┬─────────────────────────────────────────────┘
                   │ Lease-holder runs ALTER on target, then
                   │ UPDATE: applied_at = now, ddl_checksum = ...,
                   │ applied_schema_version = new_version
                   ▼
       ┌─────────────────────────────────────────────────────────┐
       │                                                         │
       │           APPLIED (applied_at IS NOT NULL)              │
       │           Observers verify checksum → skip + advance    │
       │           cursor. Cleanup-eligible after all streams    │
       │           are past this version.                        │
       │                                                         │
       └─────────────────────────────────────────────────────────┘

                   ◇ Crash path: lease_holder vanishes mid-ALTER
                   │
                   │ Heartbeats stop → expires_at <= now → state
                   │ becomes EXPIRED (expires_at <= now AND
                   │ applied_at IS NULL).
                   │
                   │ Another stream attempts takeover via
                   │ conditional UPDATE (lease_holder_stream_id
                   │ = self, expires_at = now + TTL) WHERE
                   │ lease_expires_at <= now. On success, runs
                   │ probe-and-record:
                   │   1. Probe target schema for the DDL's
                   │      effect (e.g. column exists, index
                   │      exists, ...).
                   │   2a. If observed effect matches the recorded
                   │       ddl_text → just write applied_at +
                   │       applied_schema_version (no re-apply).
                   │   2b. If observed effect indicates the ALTER
                   │       didn't fire → re-apply, then record.
                   │   2c. If observed effect indicates a partial
                   │       state inconsistent with the recorded
                   │       ddl_text → loud refusal with operator-
                   │       actionable recovery hint.
```

### 2. Lease timing (DP-A — hybrid TTL + heartbeat)

The lease uses Kubernetes leader-election semantics directly:

| Parameter | Default | Role |
|---|---|---|
| `LeaseDuration` | 30s | TTL safety net — lease expires this far in the future on every heartbeat-write |
| `RenewDeadline` | 20s | Time a lease-holder has to renew before it considers itself failed (and exits the apply path) |
| `RetryPeriod` | 10s | Heartbeat cadence; lease-holder writes `expires_at = now + LeaseDuration` at this interval |

Operator-tunable via `--shard-coordination-lease-duration=DUR`,
`--shard-coordination-renew-deadline=DUR`,
`--shard-coordination-retry-period=DUR` for unusual ALTER patterns
(e.g. operators running ALTER on tables >100GB might want
LeaseDuration=300s).

**Why hybrid over pure TTL or pure heartbeat:** see §"Prior art"
below for the Vitess OnlineDDL / etcd / Kubernetes precedents +
the failure-mode walk-through that motivated the choice. Briefly:
pure TTL forces operators to guess the worst-case ALTER duration
(too short pre-empts a healthy long-ALTER; too long delays
crash-recovery by 5+ minutes). Pure heartbeat pre-empts on
heartbeat-goroutine stall even when the ALTER is healthy. Hybrid
gets fast crash detection AND immunity to mis-tuned TTL.

### 3. DDL idempotence (DP-B — recorded-version + checksum)

All DDL types are supported (no allow-list, no parser). The flow:

1. Lease-holder observes source DDL boundary (binlog QUERY event
   on MySQL, Relation message + schema delta on PG — both engines
   surface this via the ADR-0049 schema-history machinery already).
2. Normalize the DDL text (strip leading/trailing whitespace,
   collapse runs of internal whitespace, lowercase reserved
   keywords) → compute SHA-256 hex → that's the `ddl_checksum`.
3. Apply the DDL to the consolidated target.
4. UPDATE the lease row with `applied_at`, `applied_schema_version`,
   `ddl_text`, `ddl_checksum`.

Observers (the other N-1 shards' streams) at the same DDL
boundary:

1. Observe the source DDL on their own stream.
2. Compute the checksum of their observed DDL.
3. Poll the lease row (with a backoff cap; default 5s polling for
   up to LeaseDuration × 2 = 60s, configurable).
4. When `applied_at` is set, compare checksums.
5. **Match** → skip the local apply; advance the stream's
   schema-history version pointer to `applied_schema_version`;
   resume CDC against the now-migrated target.
6. **Mismatch** → loud refusal naming both checksums, both DDL
   texts, and the recovery path (drained model: stop all shards,
   reconcile the divergent DDL, sync start --resume).

**Why normalize the DDL text:** byte-exact text comparison is too
strict (different MySQL versions emit slightly different DDL
phrasing for the same logical change; whitespace varies). Logical
checksum after normalization catches genuine divergence without
flagging cosmetic differences. Same shape as ADR-0049's
SchemaSignature.Equal.

### 4. Crash recovery (DP-C — probe-and-record)

The ADR-0048 §4 named hazard: lease-holder crashes after the
ALTER commits but before the version-record UPDATE commits. On
transactional-DDL engines (PG) the ALTER + version-record can run
in a single tx — atomicity closes the gap. On non-transactional
DDL engines (MySQL) the ALTER commits implicitly; the version-
record is a separate UPDATE. A crash between them leaves the
target's catalog migrated but the lease's `applied_at` unset.

**Probe-and-record recovery** handles this case + the hybrid
lease's heartbeat-stall edge case (lease-holder didn't actually
crash but missed heartbeats due to GC pause / connection drop /
etc.; another stream takes over a "live" lease):

1. Takeover-stream acquires the expired lease (conditional UPDATE
   WHERE `lease_expires_at <= now()`).
2. Read the existing row's `ddl_text` (set by the previous
   lease-holder before it began the apply).
3. **Probe the target schema** for the DDL's observable effect:
   - ADD COLUMN: column exists with expected name + type
   - DROP COLUMN: column does NOT exist
   - CREATE INDEX: index exists with expected definition
   - ALTER COLUMN type/nullability: column has expected attributes
   - … (per-DDL-shape probe; see Implementation §1 for the catalog)
4. Three outcomes:
   - **Applied** (probe matches recorded DDL): write `applied_at`
     + `applied_schema_version` (just record; no re-apply). Other
     observers were correctly waiting; they now proceed.
   - **Not applied** (probe shows target unchanged): re-apply the
     DDL, then record. This is the "lease-holder crashed before
     the ALTER even started" case.
   - **Partial / inconsistent** (probe shows something else —
     column exists but wrong type; index exists but wrong
     columns): refuse loudly with operator-actionable recovery
     (operator drains the shards, manually reconciles target
     schema, restarts).

**Why this handles both engines uniformly:** the probe operates
on the post-state of the target. It doesn't care whether the
previous lease-holder's failure was "before ALTER" / "after
ALTER but before record" / "heartbeat stall but ALTER succeeded"
— all three converge on a probe outcome the takeover-stream can
act on. PG's transactional DDL means the "after ALTER but before
record" window is closed at the database level (the ALTER would
have rolled back too); MySQL's window stays open but the probe
detects the half-applied state.

### 5. Engagement (DP-D — opt-out via `--no-coordinate-live-ddl`)

Live coordination is **on by default** when `--inject-shard-column`
is set. Operators who prefer the drained model add
`--no-coordinate-live-ddl` to their `sluice sync start`
invocation. Behaviour-change-by-default mirrors the AIMD opt-out
pattern (ADR-0052 DP-1).

| Form | Behaviour |
|---|---|
| `--inject-shard-column=NAME=VALUE` (default) | **Live coordination ENABLED** — lease-based cross-shard DDL apply |
| `--inject-shard-column=NAME=VALUE --no-coordinate-live-ddl` | **Drained model (v1 behavior)** — operator coordinates via stop/migrate/resume |
| (no `--inject-shard-column`) | Live coordination flag is a no-op; not Shape A |

Compatibility note for the next release: operators on v0.72.x with
hand-coordinated drained workflows for Shape A will see different
behavior on upgrade unless they add `--no-coordinate-live-ddl`.
Documented in the release notes' Compatibility section.

### 6. Operator UX

- **`sluice sync status`** surfaces lease state on Shape A streams
  with a new `consolidation_lease` field in the JSON output
  (`held` / `applying` / `applied` / `none`) and a per-target-table
  detail block. Text output gets a one-line summary (`Shape A: 2
  shards observing applied DDL boundary v3, 1 shard applying DDL
  v4 (lease held by stream-c, expires in 18s)`).
- **DDL-checksum mismatch refusal** produces a recovery message
  with both checksums, a `diff -u` of the normalized DDL text, and
  the drained-model recovery commands.
- **Lease-acquisition logging** (INFO): one log per lease
  acquire/heartbeat-extend/apply-complete; per-batch heartbeat
  logs at DEBUG.

## Decision points (RESOLVED 2026-05-22)

All four resolved via AskUserQuestion dialogue. Recorded inline
for future-reviewer audit:

### DP-A — Lease semantics

**RESOLVED: hybrid TTL + heartbeat-extend (Kubernetes
leader-election shape).** LeaseDuration=30s, RenewDeadline=20s,
RetryPeriod=10s defaults. Operator-tunable via three flags.

Owner choice driven by the Vitess OnlineDDL prior-art alignment +
the failure-mode walk-through that exposed pure-TTL's
operator-tuning burden and pure-heartbeat's stall-pre-emption
hazard. See §"Prior art" below.

### DP-B — DDL idempotence

**RESOLVED: recorded-version + DDL-text-checksum.** All DDL types
supported; checksum on normalized DDL text catches genuine
divergence without flagging cosmetic differences. Allow-list +
SQL-parser approach rejected as scope-creep.

### DP-C — Crash recovery on non-transactional-DDL engines

**RESOLVED: probe-and-record on lease takeover.** Single uniform
recovery shape for PG (transactional DDL) and MySQL
(non-transactional DDL); also recovers the hybrid lease's
heartbeat-stall edge case for free. Two-stage record + refuse-
MySQL alternatives rejected (overkill + audience-mismatch
respectively).

### DP-D — Engagement

**RESOLVED: always-on with `--no-coordinate-live-ddl` opt-out.**
Behaviour-change-by-default consistent with the ADR-0052 AIMD
opt-out pattern. Documented in release notes' Compatibility
section.

### DP-E — DDL apply path: how does the lease-holder determine what to apply?

**Added 2026-05-22 in response to implementation finding.** The
subagent implementing Phase 2a surfaced a real ambiguity between §3
step 3 ("Apply the DDL to the consolidated target") and §4's probe
catalog (ADD/DROP COLUMN, CREATE/DROP INDEX, ALTER COLUMN
type/nullability), reconciling against DP-B's "no allow-list, no
parser" framing. Three readings were on the table:

- **(a) IR-delta + full extended `SchemaDeltaApplier`** —
  derive (pre, post) IR delta, apply via per-shape engine methods,
  add as many shape methods to the interface as the catalog needs.
  Cross-engine via existing translator. Largest LOC commitment.
- **(b) Same-engine textual passthrough** — replay source DDL
  verbatim. Smallest surface but breaks cross-engine, defeating
  the §"Phase 2e" cross-engine integration test value.
- **(c) Recognized-shape catalog via IR-delta classifier** —
  classify the IR delta into a finite catalog of recognized
  shapes; apply via per-shape engine methods (small extension to
  `ir.SchemaDeltaApplier`); probe-and-record uses the same
  classifier on the target schema; unrecognized shapes refuse
  loudly with the drained-model recovery hint.

**RESOLVED 2026-05-22: (c) recognized-shape catalog via IR-delta
classifier.** Owner-confirmed via AskUserQuestion dialogue. DP-B's
"no allow-list, no parser" intent is preserved: the IR-delta
classifier compares two `*ir.Table` structs (sluice's own canonical
schema representation, not SQL text), and the "shapes" are sluice's
own categories of structural changes — not an operator-curated SQL
allowlist. §4's probe catalog and the apply catalog are the SAME
set by design (the same classifier picks the apply path on first
acquire and verifies state on takeover).

**v1 Phase 2 recognized-shape catalog** (apply-side AND probe-side):

1. **ADD COLUMN** — `len(post.Columns) > len(pre.Columns)` AND the
   extra columns appear at the end (or anywhere — engine emits
   `ADD COLUMN` accordingly).
2. **DROP COLUMN** — `len(post.Columns) < len(pre.Columns)` AND a
   column present in pre is absent in post.
3. **CREATE INDEX** — `len(post.Indexes) > len(pre.Indexes)` AND a
   new named index appears in post.
4. **DROP INDEX** — `len(post.Indexes) < len(pre.Indexes)` AND a
   named index present in pre is absent in post.
5. **ALTER COLUMN type / nullability** — a column with the same
   name exists in both pre and post but `Type` or `Nullable`
   differs.

**Unrecognized structural changes refuse loudly** — operator gets a
drained-model recovery hint. The v1 catalog covers the high-frequency
operator workflows (column adds, index creates, type widening); the
deferred shapes (CHECK / EXCLUDE / RENAME / TABLE drops, multi-shape
combo-deltas, generated-column changes, etc.) are tracked as
follow-ups. Loud-failure tenet preserved.

§3 step 3 ("Apply the DDL to the consolidated target") is read as
"apply the IR-delta-derived shape changes to the consolidated
target via `ir.SchemaDeltaApplier`" — not "execute raw DDL text."
The subagent will extend `ir.SchemaDeltaApplier` with the missing
shape methods (`AlterDropColumn`, `CreateIndex`, `DropIndex`,
`AlterColumnType`, `AlterColumnNullability`) as part of Phase 2c;
today only `AlterAddColumn` exists.

## Implementation plan

### Phase 2a — Lease primitive + control table

Additive migration on both engines:
- `internal/engines/{postgres,mysql}/control_table.go` — add
  `EnsureShardConsolidationLeaseTable` to the existing migration
  flow.
- New `internal/pipeline/shard_consolidation_lease.go` —
  `LeaseManager` type with `Acquire(ctx, tableName, ddlText) →
  (Lease, error)`, `Heartbeat(lease) → error`,
  `Apply(lease, version, checksum) → error`,
  `Observe(ctx, tableName) → (LeaseState, error)`.
- Unit tests: state-machine transitions, concurrent-acquire
  contention via `t.Parallel()` + a shared in-memory fake; pin
  the lock-out semantics (only one holder at a time).

### Phase 2b — DDL detection + engagement

- New optional `--no-coordinate-live-ddl` flag on `sluice sync
  start` + the migrate-via-resume path.
- Streamer integration: when Shape A is engaged AND
  coordination-live, route observed DDL boundaries (from the
  ADR-0049 SchemaSnapshot path) through the LeaseManager.
- Unit tests: streamer engagement tests (default-on, opt-out
  works, no-shape-A no-op).

### Phase 2c — Probe-and-record recovery

- New `internal/pipeline/shard_consolidation_probe.go` —
  per-DDL-shape probes (`ProbeAddColumn`, `ProbeDropColumn`,
  `ProbeCreateIndex`, …). Each probe takes the target schema
  reader + the DDL text, returns
  `ProbeOutcome{Applied|NotApplied|Inconsistent}`.
- Takeover path in `LeaseManager.Acquire` calls the probe when
  the lease has `ddl_text` populated but `applied_at` is NULL.
- Unit tests + integration tests: crash-injection matrix (panic
  the lease-holder at each state-machine boundary; verify
  takeover-stream recovers cleanly).

### Phase 2d — Operator UX

- `sluice sync status` JSON output gains `consolidation_lease`
  block; text output gets the one-line summary.
- INFO logs on acquire/heartbeat-extend/apply-complete; DEBUG
  per-heartbeat.
- DDL-checksum mismatch refusal includes the diff + recovery
  commands.
- Unit tests: status output shape; refusal message content.

### Phase 2e — Integration tests + release

- Cross-engine integration test: 3-shard simulated PlanetScale
  Vitess source consolidated into PG target; operator runs ALTER
  on keyspace; assert exactly-once apply on target, all 3 shards
  resume CDC, target row counts correct.
- Same shape against MySQL target.
- Crash-injection matrix: kill the lease-holder process at 3
  state-machine points; assert takeover-stream applies cleanly.
- Release-class push: concurrency-class (touches lease state
  machine + heartbeat goroutine); push-first / CI -race green
  before tag per CLAUDE.md.

Estimated LOC: ~800-1500 total (lease primitive ~300, DDL
probes ~200, streamer wiring ~150, status surface ~100, CLI
~50, tests ~500).

## Out of scope (Phase 3+)

- **Operator-issued DDL through sluice**: a future `sluice schema
  migrate-shape-a` subcommand that drives the entire
  cross-shard DDL on the operator's behalf (lease-acquire +
  ALTER + record, no stream interaction). The current Phase 2
  design assumes streams observe their own DDL via CDC.
- **Cleanup sweep for old lease rows**: background process that
  garbage-collects `sluice_shard_consolidation_lease` rows whose
  `applied_schema_version` is older than every stream's current
  cursor. v1 Phase 2 leaves rows in place (no operational
  impact; rows are small + bounded by # of distinct DDLs).
- **Cross-region lease semantics**: if shards live in different
  regions and target is single-region, the TTL needs to account
  for source→target RTT. Default 30s assumes intra-region; the
  flag override covers cross-region. No automatic detection.
- **Lease-holder preference**: currently first-to-acquire wins.
  Future: operator could nominate a "preferred lease-holder
  shard" via flag. Not in v1.

## Prior art (appendix)

The hybrid TTL + heartbeat-extend pattern this ADR specifies
mirrors several production lease-coordination systems:

| System | Pattern | TTL primitive | Heartbeat primitive |
|---|---|---|---|
| **Vitess OnlineDDL** (closest analogue) | hybrid | stale-detection on `liveness_timestamp` | runner updates `liveness_timestamp` cadence |
| **etcd** | hybrid | lease grant w/ TTL | KeepAlive RPC stream |
| **Consul** | hybrid | session TTL | session renew RPC |
| **Kubernetes leader-election** | hybrid | `LeaseDuration` | `RenewDeadline` + `RetryPeriod` |
| **PG advisory locks** | TCP-bound | n/a (session-scoped) | n/a (the TCP connection itself) |
| **MySQL GET_LOCK** | TCP-bound | optional timeout on acquire | n/a (session-bound) |

The TCP-bound flavors lean on the database connection as the
liveness signal. Clean if you can guarantee one connection lives
for the whole operation; fragile if anything resets it.

The hybrid flavors get fast crash detection (heartbeat cadence)
AND immunity to mis-tuned TTL (heartbeats extend the lease so
long-running ops aren't pre-empted). Sluice's existing CDC
streamer already has heartbeat-shaped machinery (slot keep-alive,
lag measurement, ADR-0020 standby status updates) — the
DDL-coordination lease reuses that pattern at the operational
level.

The Kubernetes default values (`LeaseDuration=15s`,
`RenewDeadline=10s`, `RetryPeriod=2s`) are tuned for control-plane
component leases. Sluice picks slightly more relaxed defaults
(30s/20s/10s) because the failure mode is operator-visible
(stream pauses) rather than control-plane-visible (no immediate
operator impact), so the latency budget can be a bit larger.

## References

- [ADR-0048](adr-0048-multi-source-aggregation-shape-a.md) — v1 design that this ADR's Phase 2 extends
- [ADR-0030](adr-0030-mid-stream-live-add-table.md) — Strategy A drained model that v1 used; this ADR adds the live-coordination alternative
- [ADR-0034](adr-0034-mysql-phase-2-live-add-table.md) — MySQL live add-table coordination; control-table additive-column pattern
- [ADR-0049](adr-0049-cdc-schema-history.md) — schema-history machinery this ADR reuses for DDL boundary detection
- [ADR-0007](adr-0007-position-persistence.md) — position-and-data atomicity that closes the "applied-but-not-recorded" gap on PG (transactional DDL)
- [ADR-0051](adr-0051-pg-cdc-source-identity-pinning.md) — PG CDC source-identity pinning (`IDENTIFY_SYSTEM` sysid + timeline). Closes a silent-loss class on the position-resume path that ADR-0054's lease takeover implicitly depends on (the per-stream persisted position must reference the same source instance the lease was acquired against).
- Vitess OnlineDDL (`vitess/vitess` schema migrations) — the closest direct prior art
- Kubernetes leader-election defaults (the source of the 30/20/10 timing recommendation)

## Related PG-internals research

The PG-internals research runs (`sluice-pg-internals-research-2026-05-22.md` for Ch 12 logical-replication, `sluice-pg-internals-research-chapters-9-10-11-2026-05-22.md` for Ch 9-11) inform several of this ADR's PG-side guarantees:

- **F1** (pgoutput protocol-version audit) is relevant to the boundary-detection path: if the source's pgoutput version changes (e.g. on a PG upgrade across major versions), new message types may carry schema metadata this ADR's recognized-shape classifier doesn't currently handle. Tracked as a backlog item.
- **F3** (`confirmed_flush_lsn` pin test) is an invariant that interacts with the lease's recorded-version: a lease APPLIED state implicitly assumes the position-write happened-before the slot's `confirmed_flush_lsn` advanced past the boundary. This is true today (the apply tx is ADR-0007's "position + data in one tx") but worth pinning.
- **F5** (source-identity pinning — ADR-0051) closes the timeline-change silent-loss class on the resume path. A lease's recorded `applied_schema_version` references a specific source identity; if the source changes underneath sluice (PITR, promotion, wrong-instance pointer), ADR-0051's refusal kicks in before the lease takeover attempts to consume the recorded version against the new identity.
- **F7** (synchronous_commit hardening — ADR-0007 amendment) closes the durability-bypass class. The lease's APPLIED state is durable only if the position-write tx is durable; F7's `SET LOCAL synchronous_commit = on` ensures that holds even when role/db-level defaults set `synchronous_commit = off`.
