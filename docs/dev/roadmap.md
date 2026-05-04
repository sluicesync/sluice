# Roadmap

Living list of work items beyond the current state, with enough context per entry that any one of them could be picked up as a self-contained chunk. Priority order is *suggested*, not strict.

Each entry has the same shape: a one-line summary, a *why* (the user-visible payoff), a *what* (load-bearing technical detail), and any *gotchas / open questions* known going in.

---

## Recently landed

For continuity when a chunk references "the previous work":

- **Simple-mode orchestrator** — three-phase apply, wired into `sluice migrate`.
- **Integration coverage in all four directions**: MySQL→MySQL, PG→PG, MySQL→PG, PG→MySQL. CI Integration job runs them on every PR.
- **MySQL CDC reader** — binlog client (go-mysql-org/go-mysql), GTID and file/pos modes, schema cache invalidated on DDL, Insert/Update/Delete/Truncate events.
- **Postgres CDC reader** — pgoutput plugin via pgx replication-mode connection, RELATION-message-driven schema cache, wal_status checks on resume.
- **MySQL VStream CDC reader** — FlavorPlanetScale, multi-shard with auto-discovery and reshard detection, snapshot+CDC handoff.
- **Snapshot→CDC handoff** — gapless cutover via `START TRANSACTION WITH CONSISTENT SNAPSHOT` (MySQL) and `EXPORT_SNAPSHOT`+`SET TRANSACTION SNAPSHOT` (PG).
- **Position persistence** — per-target `sluice_cdc_state` control table, position commit in the same tx as data writes.
- **Postgres COPY-protocol writer** — `chanCopySource` adapter wrapping pgx `CopyFrom` for ~3-5x faster bulk load on PG targets.
- **Identity sequence sync** — post-bulk `setval(pg_get_serial_sequence(...), MAX(id))` so user inserts don't collide with bulk-copied IDs.
- **`sluice sync start` / `sync status` / `sync start --dry-run`** — operator-facing CLI for streams.
- **`sluice slot list` / `slot drop`** — operator-facing slot management; auto-drop on failed cold-start; `wal_status='unreserved'|'lost'` detection on resume.
- **Postgres slot creation with `FAILOVER true` on PG 17+** — slots survive Patroni / `sync_replication_slots` failover when configured. Warning on PG ≤ 16.
- **Translation policy fixes**: JSON wire encoding for MySQL targets (no `_binary` charset prefix), warm-resume engine alias, PG UPDATE empty-WHERE under REPLICA IDENTITY DEFAULT, TIMESTAMP precision matching on `CURRENT_TIMESTAMP` defaults.
- **Operator docs**: `docs/postgres-source-prep.md` covers required GUCs, slot lifecycle, wal_status recovery, and the failover-survival mechanisms (Patroni `slots:`, PlanetScale "Logical slot name", PG 17 `sync_replication_slots`).

---

## Next up

### 1. Structured logging + bulk-copy progress reporting

**Why.** The `--log-level` flag is parsed but unwired — debug/warn/error all behave the same. Worse, during a long bulk copy (a 50M-row table can take 30+ minutes) the user sees no output between "phase 2: bulk copying rows" and "phase 3: creating indexes". Both gaps make production usage frustrating: you can't crank verbosity to debug a problem, and you can't tell whether a long migration is making progress or hung.

**What.** Two changes that pair naturally:

- **Switch `fmt.Fprintf`-to-stdout calls to `log/slog`.** Set up a slog handler in `main.go` whose level is driven by `Globals.LogLevel`. Replace the `Stdout` field on `pipeline.Migrator` / `pipeline.Streamer` with a `slog.Logger` (or just rely on `slog.Default()`). Engine packages use the same logger via the slog default.
- **Progress callbacks on `RowReader`/`RowWriter`.** Add a `WithProgress(func(rowsRead, rowsWritten int64))` option (or a typed `Progress` field on the structs) that the orchestrator wires to a periodic ticker. The ticker emits `slog.Info("bulk copy progress", "table", t, "rows", n, "rate", r)` every ~2s. No callback = no progress line.

**Design questions.**
- Default log level: `info` is right; debug should expose per-row events at the engine layer.
- Progress format: structured key-value (slog default) is friendlier to log aggregators than freeform sentences.
- Where to count rows: simplest is in the orchestrator (it already drives the channel), but the engine has a more accurate view for COPY-protocol writes that batch internally. Pragmatic call: count at the orchestrator, accept the slight inaccuracy for COPY.

**Gotchas.**
- A handful of CLI commands (`sluice slot list`, `sync status`, `engines`) deliberately format human-readable tables to stdout; those should keep using `fmt.Fprintf` to `os.Stdout` and never go through slog.
- Tests assert exact stdout contents in a few places. Switching the migrator's logging to slog will break those — need to either capture the handler output in tests or swap the assertion shape.
- Don't pull in slog wrappers (zap, zerolog adapters); stdlib `log/slog` is enough.

---

### 2. Better error messages — phase-aware hints

**Why.** Engine errors today come through wrapped with phase prefixes (e.g. `pipeline: bulk copy: postgres: insert into "users": ERROR: relation "users" does not exist`). The phase prefix is good; the inner Postgres error is correct but cryptic to users who haven't memorised PG's surface. A single hint line could turn a head-scratcher into a "oh, the schema-apply phase failed silently" diagnosis in seconds.

**What.** Add a hint layer that runs after the phase wrap. Pattern: each phase has a small map from common error substrings to a one-line operator-facing hint. Wrapped error becomes:

```
pipeline: bulk copy: postgres: insert into "users": ERROR: relation "users" does not exist
hint: target table "users" not found in schema "public" — did the schema-apply phase fail?
```

Keep the original error verbatim; hints are additive. Source: a small registry of `(phase, errorContains, hint)` triples.

**Gotchas.**
- The hint set should be tiny and load-bearing — not a translation layer. Anything beyond the most common 5-10 errors is noise.
- Avoid covering up errors. The original error stays unchanged; the hint is a separate line.

---

### 3. Follow-up ADRs for v0.2.x design decisions

**Why.** ADRs 0001–0010 are landed (foundational decisions: IR-first, sealed interfaces, kong+koanf, three-phase apply, MySQL flavors, pgoutput, position persistence, go-mysql, Streamer-as-separate-orchestrator, idempotent applier semantics). Several non-obvious decisions in v0.2.x deserve their own entries:

- **ADR-0011**: `SlotManager` as an optional engine surface (the optional-interface + type-assertion pattern for engine-specific operator surfaces).
- **ADR-0012**: Bypassing `pglogrepl` to send raw `CREATE_REPLICATION_SLOT` for PG 17+ `FAILOVER true`. Records why we don't wait for upstream library support.
- **ADR-0013**: Applier value-shaping via column-type cache + `CAST(? AS JSON)` (the Bug 6 fix shape — explains why we cache schema in the applier and why WHERE needs explicit JSON casting).
- **ADR-0014**: Phase-aware error-hint registry (substring + phase matching, deliberately tiny, additive via `%w`-wrapping).

**Scope.** Each ADR is short — typically under 200 words. Half a day's work for the batch.

---

### 4. Operational features (post-v1)

Not blocking v1 but worth tracking. The bigger items in this bucket landed in v0.3.0 (`--include-table` / `--exclude-table` filtering, `sluice sync stop`, `sluice migrate --resume`); what's left are smaller knobs:

- **Schema rename mapping.** Source schema `app` → target schema `webapp`. Useful for environments where naming differs. (The mappings YAML config covers some of this; surface it in flags too.)
- **Type override config (CLI form).** YAML hook for the user to say "treat MySQL `bigint(20) unsigned` in column X as Postgres `numeric(20)` regardless of default policy" — already works via the `mappings:` YAML; CLI surface for one-off overrides would be friendlier.
- ~~**VStream composite-PK CDC coverage.**~~ Landed in v0.3.x. `cdc_vstream_composite_pk_integration_test.go` exercises Insert/Update.Before/Update.After/Delete.Before on a composite-PK table through vttestserver; test passes (VStream is unaffected by the Bug-8-class issue, same as MySQL binlog).
- **Migrate `.golangci.yml` to v2 schema.** golangci-lint v2 (released early 2026) reworked the config layout. CI is currently pinned to `v1.64.8` to keep the existing config working; the v2 migration is a straightforward batch of key renames per the [migration guide](https://golangci-lint.run/docs/product/migration-guide/) plus dropping the pin in `.github/workflows/ci.yml`.
- **Per-batch checkpointing for resume.** v0.3.0's resume truncates and re-copies any in-progress table on retry. For multi-hour copies of single huge tables, per-batch progress would let resume pick up mid-table. See ADR-0015 for the trade-off.

---

## Cross-engine bug surface that hasn't been hit yet

Tracked here so they're not forgotten — each will surface once the relevant test exercises it. The "Recently landed" translation-policy fixes covered the most common ones; what's left:

- `BIGINT UNSIGNED` (MySQL) → Postgres — needs explicit policy (warn + map to `BIGINT`, or refuse).
- TIMESTAMP precision differences beyond the `CURRENT_TIMESTAMP` default fix (e.g. `TIMESTAMP(6)` ↔ `TIMESTAMPTZ` round-trips).
- CHARSET/COLLATION translation across dialects.
- Generated columns (MySQL `GENERATED ALWAYS AS (...) STORED` vs PG `GENERATED ... STORED`).
- CHECK constraints (MySQL 8.0+ supports them; translation should be straightforward but untested).
- PG arrays (`int[]`, `text[]`, …) → MySQL — currently errors on schema apply (correct), but the message could be friendlier and point at the type-override hook.

---

## How to use this doc

When starting a new chunk in Claude Code:

1. Pick an item from "Next up". Earlier items have more context inheritance.
2. Open the relevant section in the prompt: *"Read CLAUDE.md and docs/dev/roadmap.md section 1 (Structured logging + bulk-copy progress reporting). Propose a design before writing code."*
3. Iterate on the plan.
4. Implement.
5. Update this file when the chunk lands — move the entry to "Recently landed" and trim it to one line.
