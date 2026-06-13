# Diagnosing VStream delays on PlanetScale MySQL sources

A runbook for sluice operators whose `sluice sync` against a
PlanetScale-MySQL (or any Vitess-backed) source is lagging or has
stopped advancing. PlanetScale doesn't expose MySQL's binary log
directly; sluice consumes its row-change feed through Vitess's
VStream gRPC API. That extra layer adds failure modes that don't
exist with vanilla MySQL `binlog`. This doc names them, says what
you can do about each, and flags what's coming with Vitess 24.

If `sluice sync health` (see `docs/dev/design/sync-health-monitoring.md`)
is reporting non-zero `lag_seconds` or `seconds_since_last_event`,
start here. Sluice's VStream client lives in
`internal/engines/mysql/cdc_vstream.go`; DSN flags
(`vstream_shards`, `vstream_auto_discover_shards`, etc.) are in
`docs/managed-services.md`.

## What's happening when your sync is lagging

VStream is a chain of three things on the source side:

```
   MySQL primary
       │ binlog
       ▼
   vttablet (VStreamer; reads binlog, applies throttler)
       │ gRPC
       ▼
   vtgate (aggregates per-shard streams; hands events to clients)
       │ gRPC over TLS
       ▼
   sluice (vstreamCDCReader; decodes events into ir.Change)
```

A "delay" can show up as: events arriving slowly, events not
arriving at all (`seconds_since_last_event` climbing while the
source is clearly active), or the stream terminating cleanly and
refusing to resume. Each manifests differently.

## Causes you can do something about

> **Measured (2026-06-13, default-config Vitess v24.0.1, lag metric / 5s threshold).** We drove realistic load against a primary+replica cluster and watched an external `sluice sync` for stalls. The findings, which should reset intuition:
> - **The #1 real-world trigger is a *co-tenant* VReplication migration on the same keyspace, not your own write rate.** A routine `OnlineDDL ddl-strategy=vitess` ALTER on a ~1 M-row table drove shard lag **0 → 251 s** and held `vstreamer THRESHOLD_EXCEEDED` for the entire ~4-minute copy phase — **totally stalling an unrelated external sluice stream** the whole time. The migration's vreplication copy moves the *shared* shard-lag metric, which gates every other app (including your VStream). Migration size ≈ stall duration.
> - **A second cause is a contended/under-resourced replica** (CPU/IO-starved) — lag climbs, the stream stalls.
> - **Write-heavy primary *alone* does NOT trip the default lag throttler** on a healthy cluster — vttablet caps transaction concurrency before the replica can fall behind (we held ~900 tps + bulk binlog at ~1 s lag). So "my app got busy" is rarely the cause by itself.
> - **vtgate/connection load doesn't move the lag metric** (a custom `threads_running` throttle *can* be configured to gate on concurrency, but it's not the default), and the **idle heartbeat-staleness artifact self-heals** the instant a stream leases on-demand heartbeats.
> - **No data is ever lost** — every cleared throttle saw lag collapse and the target catch up within ~one poll; the stall is pure latency/availability. (Full quantified guide: roadmap item 19(d).)

### 1. Replica replication-lag on the chosen tablet

`vstreamCDCReader` opens against `TabletType_REPLICA` (in
`buildVStreamRequest`) — vtgate routes to a replica vttablet to
keep VStream off the primary's hot path. Whatever lag the replica
has, your VStream lag is at least that. The events you receive
carry the *replica's* timestamp, so `lag_seconds` is really
"client lag relative to whichever tablet vtgate picked."

VStream historically did *not* migrate to a less-lagged tablet on
its own. See [vitessio/vitess#15754
](https://github.com/vitessio/vitess/issues/15754) ("VStream does
not switch tablets when lag exceeds discovery_low_replication_lag",
closed Apr 2024) and [#17833
](https://github.com/vitessio/vitess/issues/17833). The fix landed
in newer Vitess; whether your PlanetScale branch has it depends on
the underlying Vitess version (see "Versions" below).

**What you can do:**

- Check PlanetScale's per-cluster replication-lag graphs.
- Restart the stream: `sluice sync stop --wait` then `sluice sync
  start --resume`. This forces vtgate to re-pick a tablet — if a
  fresher replica exists, you'll land on it.
- If lag is consistently high under normal write load, the source
  is under-provisioned; that's a PlanetScale support conversation,
  not a sluice fix.

### 2. Source-side throttler activation

Vitess runs a [tablet throttler
](https://vitess.io/docs/22.0/reference/features/tablet-throttler/)
on every vttablet. It samples replication lag and other metrics
and returns a check verdict to internal apps. The factory default
lag threshold is **5 seconds** — when the most-lagged replica
exceeds it, the throttler starts saying "no" to its registered
apps: `vreplication`, `online-ddl`, `tablegc`, `vplayer`,
`schema-tracker`. **VStream is not on that list** — in plain
Vitess the throttler doesn't directly suspend VStream. But it
hurts indirectly:

- Throttled VReplication on the source means **replicas fall
  behind**. VStream rides on a replica's binlog, so this becomes
  cause #1.
- A throttled OnlineDDL stretches the schema-change window,
  during which VStream sees DDL events and the field cache
  invalidates (`dispatchDDL` clears `r.fields`).
- PlanetScale may register its own throttler-app variants;
  invisible from outside.

Vitess 24 adds [`QueryThrottlerRequests`, `QueryThrottlerThrottled`,
and latency metrics
](https://vitess.io/blog/2026-04-30-announcing-vitess-24/#querythrottler-observability),
plus topology-watch propagation of config. Self-hosted Vitess 24
operators get much-improved visibility.

**The silent-stall gap (verified).** When the throttler engages
against a tablet whose VStream sluice is consuming, the tablet's
vstreamer *does* signal it in-band (`VEvent.throttled` /
`throttled_reason`) — but **vtgate strips those fields**: it
filters out the tablet's heartbeats and synthesizes its own
throttle-blind ones. So an external gRPC consumer like sluice sees
a mid-stream throttle as **content-free heartbeats + zero ROW
events + no gRPC error**. The `ResourceExhausted` retry classifier
never fires (no error arrives), the mid-stream progress watchdog
stays re-armed by the heartbeats (correct — the stream is alive and
*will* catch up when the throttle clears), and without the WARN
below it would be **silent**: unbounded lag, zero diagnostic. The
clean in-band fix is gated on a vtgate change to propagate
`throttled` onto its synthesized heartbeat (the external-consumer
analog of the vplayer fix in vitessio/vitess#16575/#16577).

**What sluice surfaces (observability, no behavior change).** Since
sluice can't see the in-band flag, it surfaces the *symptom*
loudly:

- **At stream open**, a throttle that denies the stream before the
  first event trips the Phase-1 liveness timeout, whose error now
  names the throttler as a candidate cause alongside the
  primary-only topology wedge ("...or the source tablet throttler
  is denying the stream — check `SHOW VITESS_THROTTLED_APPS` on the
  primary").
- **Mid-stream**, once data has flowed, a spell of heartbeats-only
  for ~30s (the soft idle window) emits a rate-limited WARN —
  *"alive (heartbeats flowing) but NO change events for Ns ... the
  source may be throttled or genuinely idle; check `SHOW
  VITESS_THROTTLED_APPS` on the primary tablet"* — once per quiet
  spell, cleared by the next real change event. This is **purely a
  heads-up**: the stream stays connected and resilient and catches
  up when events resume. The soft window is tunable per-DSN via
  `vstream_idle_warn_timeout` (a Go duration; `0` disables the WARN
  only — the hard liveness/progress guards are unaffected).

`SHOW VITESS_THROTTLED_APPS` (on a primary-routed connection) is
the right out-of-band check, but note it only reflects an
**explicit per-app deny** (it lists `vstreamer`/`rowstreamer`/
`vreplication` with expiry + ratio); it does **not** reflect the
common **threshold/lag-metric** throttle, whose live verdict lives
only on the gRPC `CheckThrottler` control plane.

**What you can do:**

- Self-hosted Vitess: `vtctldclient GetThrottlerStatus <tablet>`
  for current verdict, threshold, and app rules. `CheckThrottler`
  for ad-hoc probes.
- PlanetScale: no operator-facing throttler API. Persistent
  VStream lag with no obvious source pressure → support ticket.
- Smooth the source write rate during initial bulk-copy + cutover.

### 3. Internal Vitess operations: deploys, reshards, failovers

Three classes of internal operation touch your VStream:

**Tablet failover / planned reparent.** vtgate routes to a single
vttablet; replacement (planned reparent, kernel upgrade, node
decom) terminates the stream. Vitess 24 added [automatic tablet
retry for tablet-specific errors
](https://github.com/vitessio/vitess/blob/main/changelog/24.0/24.0.0/summary.md)
in *VReplication workflows* — but VStream clients (sluice
included) still reconnect themselves. Sluice's streamer outer
loop handles plain failovers.

**Reshard.** A keyspace split / merge / move emits a Vitess
`JOURNAL` event. Sluice catches it in `dispatchDDL` and
`journalToShardLayoutErr`. The stream sets `StopOnReshard: true`
so the journal cleanly terminates rather than silently rewriting
shards under the consumer. The reader surfaces a typed
`ShardLayoutChangedError`; the streamer's outer loop calls
`Reopen` with the new shard set.

**Deploy requests** (PlanetScale-specific). PS's "deploy request"
workflow pushes schema changes onto production via Vitess
OnlineDDL. The DDL flows through your VStream (`dispatchDDL`:
parse `TRUNCATE` for the typed event, otherwise invalidate the
field cache and rely on the next `FIELD` event). For non-trivial
gh-ost / managed-schema-migration deploys, the cutover pauses
writes briefly; your stream sees a quiet period followed by
catch-up events.

PlanetScale deploys can run **multi-hour** on big tables. The
table is being copied internally; sluice's stream sees every
row-level INSERT/UPDATE from the copy. `events_applied_total`
climbs rapidly even with no application traffic — that's the
migration's data movement.

**What you can do:**

- Subscribe to PS's status page; time long-running migrations
  outside maintenance.
- Unplanned failover: sluice's outer loop reconnects from the
  persisted CDC position. A single brief
  `seconds_since_last_event` spike is almost always transient.
- Reshard: confirm sluice handles cleanly — typed
  `ShardLayoutChangedError` → log line → `Reopen` retry. If the
  streamer terminates with `mysql/vstream: shard layout changed`
  but doesn't resume, that's a bug; file an issue.
- Deploy requests: patience. Watch `events_applied_total` and
  `lag_seconds` — both should decrease once the deploy's cutover
  finishes.

### 4. Schema-change side effects on the field cache

VStream sends column metadata in `FIELD` events; subsequent `ROW`
events reference columns positionally. Sluice caches in
`r.fields` (see `fieldCacheKey`). On `DDL` the reader clears the
cache.

A cluster running OnlineDDL may emit DDL events back-to-back.
Each invalidates the cache. If a `ROW` arrives before the next
`FIELD`, sluice errors with `mysql/vstream: row event for "X"
without preceding FIELD event` and terminates.

There's also a documented Vitess subtlety: ["VStreams that are
lagging can see a more recent schema than when the older binlog
events occurred"
](https://vitess.io/docs/22.0/reference/vreplication/internal/tracker/).
The Vitess schema-tracker addresses this server-side, but only
when vttablet runs with `--watch_replication_stream` and
`--track_schema_versions`. PS's defaults are opaque from outside.

**What you can do:**

- Rare; recovery is automatic — restart the stream.
- For correlated DDL bursts: follow [`docs/schema-change-runbook.md`
  ](schema-change-runbook.md) — `sync stop --wait` → apply
  schema → `sync start --resume`. Avoid DDL during active sync
  when you can.

### 5. gRPC / network layer

VStream is gRPC over TLS. Connection issues — TLS handshake
failures, intermediate-firewall idle timeouts, edge-gateway
throttling — manifest as the stream ending or pausing without
events.

Sluice sets `HeartbeatInterval: 5`s. Absence of any event for
~10s means either the source is silent *and* heartbeats aren't
reaching you (network broken), or the gRPC peer has gone silent.

**What you can do:**

- Corporate proxies can interfere with long-lived gRPC streams.
- Idle-timeout kills (typical 60-300s on enterprise firewalls):
  the 5s heartbeat keeps the stream chatty, but some middleware
  ignores HTTP/2 PING. Allowlist the PS endpoint or bypass the
  proxy.
- Outer loop reconnects on clean stream-end; check logs for
  repeating `mysql/vstream: recv: ...` errors that indicate a
  churning reconnect cycle.

## Causes you have to wait out

Some delays are properties of the underlying system:

- **Throttler-driven VReplication backlog.** PS-internal
  workflows falling behind back up the throttler → replica lag
  → cause #1. Operator-side fix: none. PS support can inspect.
- **Maintenance windows.** Plan around the published status
  page.
- **Big-table OnlineDDL on the source.** Sluice applies every
  row of the migration's internal copy. No "skip ahead" —
  VStream's contract is exactly-once delivery.
- **vtgate-side congestion under load spikes.** No client
  visibility into vtgate's queuing. Wait or escalate.

## Detection: what to watch

Metrics from `docs/dev/design/sync-health-monitoring.md`:

- **`sluice_lag_seconds`**: source-event timestamp vs. now. Most
  meaningful. Sustained > 60s on a write-active source = worth
  investigating; > 10 minutes = real incident.
- **`sluice_seconds_since_last_event`**: distinguishes "source
  quiet" from "stream broken" only when combined with knowledge
  of source write activity. Heartbeats keep this < 6s.
- **`sluice_streamer_state`**: `streaming` → `stopping` /
  `stopped` without operator-issued `sync stop` is the loud
  signal. Wire alerting on this transition.

PlanetScale-side signals to combine:

- Replica-lag graph on the PS dashboard (cause #1).
- Status-page subscriptions (cause #3).
- Deploy-request log on the PS console.

For the field-cache failure (cause #4), watch sluice's logs for
`row event for "X" without preceding FIELD event` and correlate
with PS deploy timestamps.

For a mid-stream throttle/idle stall (cause #2), watch for the
rate-limited WARN `alive (heartbeats flowing) but NO change events
for Ns` — it fires once per quiet spell when heartbeats keep
arriving but no change events do. Pair it with `sluice_lag_seconds`
climbing while `sluice_seconds_since_last_event` stays low (< 6s,
heartbeats arriving): that combination is the throttle/idle
signature, because vtgate strips the in-band `throttled` flag (see
cause #2). A genuinely idle source produces the same WARN — check
`SHOW VITESS_THROTTLED_APPS` on the primary to tell them apart.

## Targeting a PlanetScale branch (control-table DDL needs an `admin`-role password)

When a **PlanetScale branch is the sluice _target_** (`--target-driver=planetscale`),
the password's role must allow DDL: on a cold-start sluice creates the destination
tables and its control tables (`sluice_cdc_state`, `sluice_cdc_schema_history`,
`sluice_shard_consolidation_lease`). A `reader`/`writer`/`readwriter`-role password
is **denied DDL** on a production branch and the cold-start fails immediately at the
control-table step:

```
pipeline: ensure control table: mysql: ... Error 1105 (HY000): ...
DDL command denied to user '<user>', in groups [planetscale-writer], for table 'sluice_cdc_state' (ACL check error)
```

Mint the **target** password with `--role admin` (`pscale password create <db> <branch> --role admin`).
This applies to the target only — a VStream **source** password needs just read
access. (PlanetScale **production** branches are the case that enforces this; the
ACL group `planetscale-writer` excludes DDL.) If your target branch has **Safe
Migrations** enabled, schema changes go through deploy requests instead — pre-create
the tables and use `--schema-already-applied` (see GitHub #17).

## What's new in Vitess 24 (and when does PlanetScale get it?)

The [Vitess 24 announcement
](https://vitess.io/blog/2026-04-30-announcing-vitess-24/)
introduces **binlog streaming support** — vtgate can now serve
GTID-based binlog events through:

- **MySQL protocol** via `COM_BINLOG_DUMP_GTID` (the same
  command vanilla MySQL replicas use), or
- **gRPC** via a new `BinlogDumpGTID` streaming RPC.

Gated by `--enable-binlog-dump`, access-controlled by
`--binlog-dump-authorized-users`.

**How it differs from VStream:**

| | VStream | Vitess 24 binlog dump |
|--|--|--|
| Aggregation | Cross-shard via vtgate | Single tablet only |
| Failover | vtgate handles tablet selection | Client manages |
| Filter | Per-table rules | Native binlog (filter client-side) |
| Resharding | Surfaces `JOURNAL`, `StopOnReshard` | Not reshard-aware |
| Schema metadata | `FIELD` events | `TABLE_MAP_EVENT` |
| Use case | MoveTables, Reshard, complex consumers | "Point-in-time consumption" |

The Vitess team is explicit: ["binlog streaming is best suited to
point-in-time consumption rather than `MoveTables` or `Reshard`
use cases, where the VStream API remains the right tool."
](https://vitess.io/blog/2026-04-30-announcing-vitess-24/#binlog-streaming-support)

**Could sluice use it?** Maybe, eventually. Benefits: simpler
protocol (sluice already understands MySQL binlog events from
the vanilla flavor in `internal/engines/mysql/cdc_reader.go`),
lower latency (one fewer hop), clearer ops story. Drawbacks: no
cross-shard aggregation (sluice replicates fan-in logic vtgate
provides), no reshard handling, single-tablet failover is the
operator's problem.

For sluice's PlanetScale-MySQL flavor, **VStream remains the
right primitive** for sharded keyspaces and any case where the
platform should handle reshards and tablet selection. A future
ADR could add `CDCBinlogDumpGTID` as a capability for unsharded
sources wanting the simpler path.

**When does PlanetScale get it?** Genuinely uncertain. As of May
2026: most PS-MySQL branches run **Vitess 22**; a move to
**Vitess 23** is expected within months; **Vitess 24** is months
further out from any PS rollout. Even on Vitess 24, the
binlog-dump endpoint defaults to disabled and is access-
controlled — PS would need a product decision to expose it. No
public roadmap commitment. **Treat as future capability, not a
near-term plan.**

PS operators can't inspect the underlying Vitess version from
the dashboard. Best signals: PS changelog/blog, `SHOW VARIABLES
LIKE 'version_comment'` (sometimes), status-page entries during
rollouts.

## What sluice plans to do

Forward-looking; tracks against `docs/dev/roadmap.md` and the
proto-ADR in `docs/dev/design/sync-health-monitoring.md`.

1. **Ship `sluice sync health` and `--metrics-listen`** so the
   `sluice_lag_seconds`, `sluice_seconds_since_last_event`,
   `sluice_streamer_state` triple is observable without parsing
   logs.
2. **VStream-specific health context in `sync status`** — for the
   PlanetScale flavor, surface the active shard's GTID and a
   "healthy / reconnecting / terminated" summary from
   `vstreamCDCReader.Err()` + streamer state.
3. **Document the playbook** — this doc; iterate from real
   incidents.
4. **Track Vitess 24 binlog streaming.** When PS's Vitess uplift
   lands and binlog-dump becomes accessible, an ADR weighs adding
   `CDCBinlogDumpGTID` and sharing decoder code with the vanilla
   MySQL flavor.
5. **Reshard recovery polish** — structured log on each reopen
   plus a `sluice_vstream_reshards_total` counter.

## Reference: source-code map

- `internal/engines/mysql/cdc_vstream.go` — VStream client:
  `openVStreamReader` (DSN/gRPC), `buildVStreamRequest` (REPLICA,
  `MinimizeSkew`, `StopOnReshard`, 5s heartbeat), `pump` /
  `dispatch` / `dispatchRow` / `dispatchDDL`, `Reopen`,
  `ShardLayoutChangedError` / `ErrShardLayoutChanged`.
- `internal/engines/mysql/cdc_vstream_liveness.go` — the two-phase
  progress watchdog: Phase 1 (first serving-proof event), Phase 2
  (mid-stream total-silence guard), and the Phase-2 SOFT idle-WARN
  sub-window (`vstreamIdleWarnMessage`, the throttle/idle heads-up).
- `internal/engines/mysql/cdc_vstream_position.go` — VGtid
  encode/decode for the persisted CDC position.
- `internal/engines/mysql/cdc_vstream_snapshot.go` — VStream COPY
  mode for snapshot+CDC handoff.
- `docs/managed-services.md` — DSN flags, sharded conventions,
  verification matrix.
- `docs/schema-change-runbook.md` — `sync stop --wait` → ALTER →
  `sync start --resume` workflow.

## Reference: external links

- [Vitess 24 announcement
  ](https://vitess.io/blog/2026-04-30-announcing-vitess-24/)
- [Vitess VStream reference (v22)
  ](https://vitess.io/docs/22.0/reference/vreplication/vstream/)
- [Vitess tablet throttler reference
  ](https://vitess.io/docs/22.0/reference/features/tablet-throttler/)
- [Vitess schema tracker (and the "lagging streams see newer
  schema" warning)
  ](https://vitess.io/docs/22.0/reference/vreplication/internal/tracker/)
- [VStream tablet-switching bug and fix
  ](https://github.com/vitessio/vitess/issues/15754)
- [VStream tablet selection feature request
  ](https://github.com/vitessio/vitess/issues/17833)
- [Binlog timestamp watermarking proposal
  ](https://github.com/vitessio/vitess/issues/16477)
- [Vitess 24 release notes
  ](https://github.com/vitessio/vitess/blob/main/changelog/24.0/24.0.0/summary.md)
