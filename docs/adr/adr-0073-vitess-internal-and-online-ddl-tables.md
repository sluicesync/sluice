# ADR-0073: Handling Vitess internal + online-DDL lifecycle tables in VStream

## Status

Accepted (part (c) implemented). Surfaces a reproduced gap from the Bug-125 investigation and an operator-flagged robustness requirement. Builds on the VStream snapshot/CDC path ([ADR-0071](adr-0071-vstream-snapshot-bounded-memory.md), [ADR-0072](adr-0072-resumable-coldstart-copy.md)) and the existing `_vt_*` exclusion (GitHub issue #22 / "Bug 22"), whose coverage this ADR completes for the VStream COPY filter.

The implementation — Phase-A ground truth, the exclusion choke points, the cutover-survival handling, and the vendored-Vitess verdict — is recorded in [Implementation notes (part c)](#implementation-notes-part-c) below.

## Context

PlanetScale *is* Vitess under the hood, and on Vitess **online DDL is the default schema-change mechanism**. An online `ALTER` doesn't mutate the table in place — Vitess builds a **shadow table**, copies rows into it via VReplication, then **atomically cuts over** (renames) and cleans up. The transient artifacts carry reserved names:

- **Internal-operation tables, under a _version-dependent_ naming scheme** ([vitessio/vitess#14582](https://github.com/vitessio/vitess/issues/14582)). The **legacy** format (Vitess 19–20) used per-state GC hints — `_vt_HOLD_*`, `_vt_PURGE_*`, `_vt_EVAC_*`, `_vt_DROP_*` — plus UUID-leading online-DDL / vreplication names (e.g. `…_vrepl`). **Vitess 20+ replaced these with a single unified pattern** `_vt_<op>_<uuid>_<timestamp>_` (fixed-width, `_vt_` prefix + trailing underscore), where `<op>` is a 3-char code: `hld`/`prg`/`evc`/`drp` (GC states), **`vrp`** (online-DDL / vreplication), `gho`/`ghc`/`del` (gh-ost shadow / changelog / deleted). The `_vt_vrp_*` table the Bug-125 probe hit is this new format's online-DDL code. **The format demonstrably changed across Vitess versions** — which is precisely why the decision below anchors to Vitess's own helper rather than a name list sluice maintains.

sluice's VStream reader requests **every** table — the COPY filter rule is `Match:"/.*/"` (`cdc_vstream.go`, `cdc_vstream_snapshot.go`). So these Vitess-internal artifacts are pulled into the stream.

**This is not hypothetical — it was reproduced.** During the Bug-125 hunt, a probe run against a branch with an *in-flight deploy* (an online schema change) found sluice's COPY filter **picked up a `_vt_vrp_*` shadow table** instead of (or alongside) the real table, which then tripped the ADR-0071 scope-name-mismatch loud-refusal (`activeTable` mismatch) and aborted the cold-start with zero rows. A real schema change on a PlanetScale source can therefore break a sync today.

This functionality must be **rock-solid**: every PlanetScale user will run schema changes, so the first online `ALTER` during a sync cannot corrupt, drop, or wedge the stream.

### Two distinct requirements

1. **Exclude Vitess-internal tables** — `_vt_*` workflow tables, `_vt_vrp_*` shadows, and the lifecycle-hint names — from BOTH the COPY filter AND the CDC dispatch path. They are never logical user tables and must never be copied, applied, or counted.
2. **Survive an online-DDL cutover mid-sync** — the shadow build, the atomic rename/swap, and the cleanup must not break the logical table's stream: the logical table keeps its identity across the cutover, the internal events are ignored, and COPY/CDC continue with zero loss.

## Decision

1. **Filter Vitess-internal tables by Vitess's own convention, not a blanket match.** Use Vitess's `schema.IsInternalOperationTableName()` (it recognizes the internal-table patterns known to the Vitess version sluice vendors — both the legacy per-state hints and the v20+ unified `_vt_<op>_<uuid>_<timestamp>_` form; see #14582) — applied (a) when constructing the VStream `Filter` so vtgate doesn't even stream them where possible, and (b) defensively at the dispatch level (`dispatchRow` / the snapshot pump's row buffering) so a leaked internal-table ROW/FIELD event is dropped, not copied or applied. Keeping the matcher anchored to the Vitess helper (rather than a hand-maintained prefix list) means it tracks Vitess's lifecycle-naming evolution; a pin asserts the exclusion set so a Vitess wording change fails a test rather than silently leaking.
2. **Treat the logical table as identity-stable across an online-DDL cutover.** The stream follows the user-named table; the internal rename/swap events for `_vt_*` tables are excluded by (1), so the cutover is transparent to the logical stream. Where the cutover surfaces a FIELD event (schema delta) on the logical table, the existing ADR-0049 schema-history path handles it.

## Consequences

- **A *version-dependent* dependency on Vitess's internal-table naming convention.** #14582 is the proof the names change across Vitess releases (legacy per-state hints in 19–20 → the unified `_vt_<op>_…` form in 20+), so a static prefix list sluice maintains would be wrong on some cluster version. Anchoring to `schema.IsInternalOperationTableName()` outsources this to Vitess — but **the helper only knows the formats of the Vitess version sluice vendors**, so (c) must confirm the vendored version recognizes the formats real PlanetScale / self-hosted clusters emit (and bump the vendored Vitess if not), with a pin that fails on a recognized-set change rather than silently leaking.
- **The exclusion must apply on both paths** (COPY filter + CDC dispatch) — a COPY-only or CDC-only fix is incomplete (the Bug-125 probe hit the COPY path; a steady-state online DDL hits CDC).
- **New test surface (the high-value part, gated on Bug 126's fix that lets the CLI drive vttestserver):** a `vttestserver` harness run with `ENABLE_ONLINE_DDL=true` that (i) seeds a table, (ii) starts a sync, (iii) runs an online `ALTER` mid-stream, and asserts sluice **excludes** the `_vt_*` artifacts and **keeps syncing the logical table byte-clean through the cutover** (zero loss, no spurious copies/applies/refusals). This is correctness-gating — same tier as the silent-loss work.

## Alternatives considered

- **Rely on vtgate's own internal-table exclusion.** Insufficient — the reproduced `_vt_vrp_*` leak shows the blanket `/.*/` request still surfaces them to sluice; sluice must filter.
- **A static `_vt_` prefix list.** Fragile against the lifecycle-hint names and future Vitess conventions; prefer the Vitess helper as the single source of truth.

## Implementation notes (part c)

Implemented following the three-phase debugging protocol against `vitess/vttestserver:mysql80` (booted with `ENABLE_ONLINE_DDL=true`; the integration harness's `startVTTestServerWithShards` now sets it explicitly).

### Vendored-Vitess verdict (Step 0)

sluice vendors `vitess.io/vitess v0.24.1`. Its `schema.IsInternalOperationTableName()` is importable and **covers every format real PlanetScale / vttestserver (v20+) emit — no Vitess bump needed.** Recognized set (pinned in `vstream_internal_table_test.go`):

- **Unified v20+ format** `_vt_<op>_<uuid32hex>_<ts14>_` (`InternalTableNameExpression = ^_vt_([a-zA-Z0-9]{3})_([0-f]{32})_([0-9]{14})_$`), so all op codes match: `hld`/`prg`/`evc`/`drp` (GC states), **`vrp`** (vreplication / online-DDL — the Bug-125 shadow), `gho`/`ghc`/`del` (gh-ost). `_vt_vrp_*` is covered.
- **Legacy gh-ost / vreplication** `_<uuid>_<ts>_(gho|ghc|del|new|vrepl)` via `IsOnlineDDLTableName`.
- **pt-online-schema-change** `_..._old`.

One caveat worth recording: v0.24.1 does **not** match the pre-v19 *uppercase* `_vt_HOLD/PURGE/EVAC/DROP_<uuid>` legacy GC names (only the lowercase unified `_vt_hld_…`). That uppercase form predates the Vitess releases PlanetScale and `vttestserver:mysql80` run, so it isn't a real exposure today; if a very old self-hosted Vitess ever surfaces it, the matcher would need a Vitess bump or a supplement, and the unit pin would flag the gap.

### Phase-A ground truth (observe before fixing)

A temporary instrumented test (deleted in Phase C) tapped the raw VEvent stream during a real `ddl_strategy='vitess'` ALTER, and a second probe created an internal-named (`_vt_vrp_*`) table directly. Findings:

1. **Do `_vt_*` internal tables reach sluice?** **Yes.** A directly-created `_vt_vrp_*` table surfaced **2 rows via `ReadRows` on the COPY path** pre-fix — i.e. its FIELD + ROW events flow through `bufferCopyRow` and get buffered under the `/.*/` filter. This is the exact Bug-125 leak (a buffered shadow that can trip the ADR-0071 scope-mismatch refusal).
2. **What does the cutover emit?** During the online ALTER, vtgate streamed the shadow-table **DDL events** — `CREATE TABLE _vt_vrp_…`, `ALTER TABLE _vt_vrp_… ADD COLUMN …`, `ALTER TABLE _vt_vrp_… AUTO_INCREMENT=…` — as `VEventType_DDL`, with the internal name in the **statement text** and an **empty event `table` field**.
3. **vttestserver limit (the Step-2 knob hunt).** The full online-DDL **scheduler is "not implemented in vtcombo"** (`SHOW VITESS_MIGRATIONS` reports `migration_status=failed message="not implemented in vtcombo"`). vtcombo's internal tablet-manager client stubs out the tmclient RPCs the cutover needs (`LockTables`/`UnlockTables`, …; `go/vt/vtcombo/tablet_map.go`). So vttestserver builds the shadow-table **DDL** but **cannot complete the VReplication copy or the atomic cutover** — there is no knob to make it. The end-to-end *cutover-with-rows + post-cutover-schema* assertion therefore lives in the real-PlanetScale `psverify` suite; vttestserver validates (a) the internal-table COPY/CDC ROW+FIELD exclusion (via a directly-created internal table, which produces the identical wire events) and (b) that the shadow-table DDL events don't wedge the logical stream.

### The exclusion (Phase B)

Single source of truth: `isVitessInternalTable()` in `internal/engines/mysql/vstream_internal_table.go`, delegating to `schema.IsInternalOperationTableName()` (callers strip the `keyspace.` prefix first). The `Match:"/.*/"` filters are unchanged (Vitess RE2 filters can't negative-lookahead-exclude). Internal-table events are dropped at every dispatch/buffer choke point, on **both** the COPY and the CDC paths:

| Path | Site | Action |
| --- | --- | --- |
| COPY ROW | `vstreamSnapshotStream.bufferCopyRow` | skip before buffering (so the ADR-0071 scope-mismatch can't fire on `_vt_*`) |
| COPY FIELD | `dispatchCopyEventLocked` FIELD branch | don't cache fields for internal tables |
| CDC ROW | `vstreamCDCReader.dispatchRow` **and** `vstreamSnapshotStream.dispatchCDCRow` | skip before the FIELD lookup |
| CDC FIELD | `vstreamCDCReader.dispatch` **and** `dispatchCDCEvent` FIELD branches | don't register/schema-snapshot internal tables |

The ROW skips precede the field-cache lookup so the existing "row event without preceding FIELD event" loud floor stays reserved for genuine logical-table bugs (the matching FIELD was already dropped).

### Cutover survival (decision #2)

The VStream DDL handlers (`dispatchDDL` / `dispatchCDCDDL`) invalidate the **whole** field cache on every DDL ("a DDL might have changed the column shape"). A shadow-table DDL (Phase-A item 2) does **not** change any logical table's schema — clearing the logical cache on it would force a spurious "row without FIELD" wedge on the next logical ROW. So a small detector, `isVitessInternalDDL()` (extracts the target table from `CREATE/ALTER/DROP/RENAME TABLE …` and tests it with `isVitessInternalTable`), makes both DDL handlers **skip** internal-table DDLs entirely (no emit, no invalidation). The detector is fail-safe: anything it can't confidently parse falls through to the normal cache-clear (the pre-ADR behaviour). The atomic cutover's rename swaps the shadow onto the logical name, which surfaces as a FIELD re-emit on the **logical** table — not an internal-table DDL — so it still flows through the normal ADR-0049 schema-history path.

### Pins

- **Unit** (`vstream_internal_table_test.go`) — `isVitessInternalTable` over the full recognized set (every unified op code + each legacy family) **and** a battery of non-internal lookalikes that must NOT be excluded (`_vtok`, `vt_foo`, `_vt_foo`, `_vt_users_backup`, …; a false positive there is silent loss — a user table dropped from the migration). Plus `isVitessInternalDDL` over shadow vs logical DDL shapes. Pin-the-class so a Vitess wording change fails a test.
- **Integration** (`integration && vstream`, `cdc_vstream_onlineddl_integration_test.go`):
  - `TestVStream_OnlineDDL_InternalTablesExcluded_ColdStart` — a logical table copies byte-clean (5/5) alongside a 20-row internal `_vt_vrp_*` table; the internal table surfaces **0** rows and **no** scope-mismatch refusal fires (`stream.Rows.Err() == nil`).
  - `TestVStream_OnlineDDL_LogicalStreamSurvivesCutover` — a real `ddl_strategy='vitess'` ALTER mid-CDC-stream; the logical stream is **not wedged** (3/3 post-ALTER inserts delivered, `Err() == nil`, no internal table surfaces as a change).

### Validating the full cutover — test-harness tiering (decision)

Phase-A item 3 established that vttestserver **cannot complete** an online-DDL cutover (the vtcombo scheduler is stubbed), so the end-to-end *cutover-with-rows + post-cutover-schema-on-the-logical-table* assertion — the part where Vitess actually renames the shadow onto the logical name — is **unproven** by the pins above. The question is which vehicle proves it. The decision is a **three-tier harness**, and the full-cutover validation targets the *middle* tier rather than real PlanetScale:

1. **vttestserver** (single container, ~25–40s boot) — the default `Integration (vstream)` gate. Fast enough for every-PR coverage of the COPY/CDC paths: the internal-table exclusion (validated here via a directly-created internal table, which produces wire-identical FIELD+ROW events), resume, schema-evolution, the Bug-125 matrix. The scheduler stub is its only material gap.
2. **Full local Vitess cluster** (a minimal custom `docker compose` on `vitess/lite:v20.0.6` — etcd + vtctld + a primary + a replica vttablet + vtgate) — runs the **real** online-DDL scheduler, so it does the VReplication copy + atomic rename cutover that vtcombo can't. **This is the vehicle for the full cutover-survival validation** (and future resharding / FK-mode / multi-keyspace tests). Chosen over real PlanetScale because it is free, deterministic, automatable, reusable, *and* it validates self-hosted Vitess directly (aligning with the `vitess` flavor + the broader hardening track). Heavier (multi-service, slower boot, more resources) → a **separate, gated/manual** job, not the per-PR default. **BUILT** as track item (b2) — see below.
3. **Real PlanetScale** (`psverify`) — occasional production spot-checks; PlanetScale's vtgate version/config can differ subtly from upstream Vitess, so a final-confidence run belongs here, but it is **not** the primary cutover-validation vehicle (cost, credentials, ephemerality, CI-automation friction).

**Status consequence:** part (c)'s **exclusion** is validated + shipped to `main`; its **full cutover-survival** is now **VALIDATED on tier 2** (see the (b2) notes below).

### (b2) — the full-cluster harness (BUILT + (c) cutover-survival VALIDATED)

The tier-2 harness is `startVitessCluster(t)` (`internal/engines/mysql/cdc_vstream_cluster_integration_test.go`, build tag `integration && vitesscluster`). It mirrors `startVTTestServer`'s ergonomics (returns the vtgate MySQL DSN + gRPC endpoint + keyspace + cleanup) but boots a **real** cluster via `docker compose` (`testdata/vitesscluster/docker-compose.yml`): etcd + vtctld + a PRIMARY vttablet + a REPLICA vttablet + vtgate, all on `vitess/lite:v20.0.6` (matching the vendored `vitess.io/vitess v0.24.1` client). The harness shells to `docker compose` directly rather than adding the testcontainers compose module (zero new dependency).

Phase-A proof (the load-bearing risk — that a real cutover is achievable on this hardware and reachable through sluice's VStream): a real `ddl_strategy='vitess'` ALTER reaches **`migration_status=complete`** (`strategy: vitess`, `ddl_action: alter`, `rows_copied: 5`) — i.e. the cluster genuinely performs the VReplication copy + atomic cutover, NOT the vtcombo stub. Boot+init ~28 s; running footprint ~500 MB RSS across the 5 containers.

The (c) full cutover-survival test, `TestVitessCluster_OnlineDDL_CutoverSurvivesWithZeroLoss`, seeds a 300-row logical table, cold-starts a sluice VStream sync (snapshot COPY → CDC tail), fires a real online ALTER mid-stream, polls `SHOW VITESS_MIGRATIONS` to `complete`, then keeps writing through the new schema — and asserts all four ADR-0073 (c) properties: (i) the logical stream is **not wedged** across the rename swap, (ii) **zero row loss** (final COUNT == seed + post), (iii) the post-cutover schema (the new column) flows through CDC, (iv) no `_vt_*` table surfaces as a copied/applied row and no scope-mismatch refusal fires. A sibling test, `TestVitessCluster_OnlineDDL_ComplexShapesSurviveCutover`, runs the same survival assertions across **richer DDL shapes** that real migrations mix in — a **mid-table column DROP** (a column-position shift, not just a trailing add), an **ENUM column ADD**, and an **ENUM value-set EXTEND** — each completing a real cutover with the schema delta flowing through and zero loss. Both PASS.

**Robustness finding surfaced by (b2) (out of ADR-0073's scope; logged for follow-up):** sluice's VStream CDC tail hardcodes `TabletType_REPLICA` (`cdc_vstream.go` `buildVStreamRequest`; matches PlanetScale's read-from-replica convention). Against a **primary-only** cluster — PlanetScale *development* branches, and minimal self-hosted setups — vtgate has no REPLICA tablet and the stream **wedges**: vtgate logs `failed to find a REPLICA tablet for VStream in <ks>/<shard>`, and (empirically, via a primary-only variant of this harness) sluice's reader presents this as a **silent hang** (no events; `Err()` stays nil within the window) rather than a loud failure — which conflicts with the loud-failure tenet. Conversely, RDONLY-capable enterprise setups are an opportunity (sluice could prefer RDONLY to offload even replicas). A robustness fix would: (1) make the CDC tail's tablet type fall back / be selectable (e.g. `PRIMARY` when no REPLICA exists, optional RDONLY preference), and (2) surface vtgate's "no tablet" condition as a **loud** reader error instead of a silent stall. The `vitesscluster` harness (with a primary-only compose variant) is the ready-made vehicle to pin such a fix.
