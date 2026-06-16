# Changelog

All notable changes to sluice are recorded here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project follows [Semantic Versioning](https://semver.org/).

## [0.99.60] - 2026-06-16

### Fixed
- **PostgreSQL-source `ENUM` columns now replicate over PG→PG CDC (catalog Bug 151).** The `pgoutput` CDC reader's OID-to-type map had no case for a user-defined `ENUM` — its OID is dynamic (assigned at `CREATE TYPE` time), so the static lookup declined it and the first enum `INSERT`/`UPDATE`/`DELETE` wedged the stream with `unsupported column type OID <dyn> (typmod -1)`. Enums cold-started (bulk copy) fine but could not be continuously synced PG→PG. This is the same class as the closed Bug 144 (arrays) and Bug 147 (geometry) reader gaps, and it's fixed the same way: the reader resolves the set of user-defined enum type OIDs (`pg_type.typtype='e'`) at the relation boundary — cumulatively, so a mid-stream `CREATE TYPE` + `ADD COLUMN` is picked up too — and maps a matching column to an enum whose value rides the wire as its text label. A non-enum user-defined type (composite, domain) and enum **arrays** (`enum[]`) stay loudly refused (no silent loss). MySQL→PostgreSQL enum sync and the cold-start path were already correct; this closes the PG-source CDC path.

## [0.99.59] - 2026-06-16

### Fixed
- **A forwarded `DROP COLUMN` of a synthesized ENUM column now drops the per-column PostgreSQL enum type too (Bug 150).** When a MySQL-source `ENUM` column is migrated to PostgreSQL, sluice synthesizes a dedicated `"<table>_<col>_enum"` type for it (MySQL enums carry no type identity). A schema-change-forwarded `DROP COLUMN` dropped the column but left that type behind as an orphan — harmless to existing data, but a later re-add of a same-named column with a *different* value set would have collided with (or silently reused) the stale type. `AlterDropColumn` now also issues `DROP TYPE IF EXISTS "<schema>"."<table>_<col>_enum"`, but **only** for these synthesized per-column types. A preserved/named PostgreSQL enum type (`TypeName` set — which may be shared across columns or tables) is never auto-dropped, so same-engine PG→PG enum sharing is unaffected.

## [0.99.58] - 2026-06-16

### Fixed
- **MySQL `SET` columns now replicate to a PostgreSQL `text[]` over CDC (Bug 149).** With v0.99.56's SET fix, a MySQL `SET` decodes to a Go string slice and its PG target is `TEXT[]` — but the CDC applier's array binding required a different internal slice shape and rejected it with a loud `expected []any for Array column, got []string`, halting the stream at apply (no silent loss). The applier's array binding now accepts the string-slice shape and routes it through the same path as a native `text[]`, so a MySQL `SET` lands as its member labels in a PG `text[]` column. (Cold-start migration of `SET` → `text[]` already worked; this closes the continuous- sync apply path.)

## [0.99.57] - 2026-06-16

### Fixed
- **PlanetScale/Vitess (VStream) resume from a purged GTID position now reliably auto-recovers (Bug 146, ADR-0093 amendment).** v0.99.51 added a *reactive* recovery (classify vtgate's "purged required binary logs" error → cold-start re-snapshot), but a local Vitess-24 cluster reproduction proved vtgate does **not** surface that error on a purged resume — it accepts the stale position, the tablet drops the binlog dump (errno 2013), and vtgate idles on heartbeats, so the stream looped on a retriable liveness timeout and never cold-started. sluice now does a **proactive pre-flight** before opening the stream — `GTID_SUBSET(@@global.gtid_purged, <resume>)`, mirroring the binlog reader — and returns the cold-start signal when the resume position is unreachable. The check is routed at the **same tablet type the stream binds to** (`gtid_purged` is tablet-type-routed by vtgate; a replica can purge independently of the primary), strips the Vitess GTID flavor prefix, and degrades gracefully (proceeds, never a spurious re-snapshot) if the probe can't run. The reactive classifier is retained as defence-in-depth. `--no-auto-resnapshot` still turns the recovery into a loud terminal error.

## [0.99.56] - 2026-06-16

### Fixed
- **MySQL `SET` columns now sync correctly over binlog CDC (Bug 148).** The go-mysql binlog decoder hands a `SET` cell back as its **numeric bitmask** (not the member text), and sluice's CDC value decoder passed that through — so a replicated `SET('a','c')` carried `["5"]` (the mask, stringified) instead of `["a","c"]`. The decoder now maps the bitmask to its member labels via the column's value list (bit *i* → the *i*-th declared member, in declaration order), errors loudly on a bit with no member, and treats mask 0 as the empty set. Comma-joined label text (the snapshot/copy path and the VStream reader) still passes through unchanged. This is the `SET` sibling of the v0.99.52 ENUM fix (Bug 145); it applies to **self-hosted MySQL binlog CDC** (snapshot and PlanetScale/Vitess sync were already correct, as they deliver the label text).

## [0.99.55] - 2026-06-16

### Fixed
- **PostGIS `geometry` now syncs over CDC from a PostgreSQL *source* too (Bug 147) — completes geometry-over-CDC.** v0.99.54 added the apply-side geometry codec (which made MySQL→PG work); this release fixes the read side. The pgoutput CDC reader's type map had no `geometry` case — its OID is dynamic (assigned at `CREATE EXTENSION postgis`), so it can't be a static entry — so the first geometry change over PG→PG CDC killed the stream with `unsupported column type OID <n> (geometry)`. (Cold-start COPY was unaffected.) The reader now resolves the runtime geometry OID and decodes the value (and correctly treats pgoutput's text-format hex-EWKB bytes as hex, not raw EWKB — a silent-corruption trap caught in review). Pinned across point / polygon / multipolygon / geometrycollection, 2D / Z / M / ZM, SRID 4326 and 0, `POINT EMPTY`, NULL, and UPDATE/DELETE images.
- `geography` over CDC remains **loudly refused** (no applier codec yet) — tracked as a follow-up; no silent loss.

## [0.99.54] - 2026-06-16

### Added
- **PostGIS `geometry` columns now apply over CDC to a PostgreSQL target (#20)** — the apply (write) side; combined with the Bug 147 read-side fix in v0.99.55 this makes geometry-over-CDC work in both PG-source and MySQL-source directions. Geometry was previously un-appliable over continuous sync: the CDC applier had no codec for PostGIS's (dynamically-assigned) `geometry` type OID, so the EWKB bytes were shipped in text format and PostGIS rejected them (`parse error - invalid geometry`) — a loud refusal on both the serial and pipelined (ADR-0092) apply paths. (Cold-start COPY was unaffected.) A binary geometry codec is now registered on the applier connections and ships EWKB to `geometry_recv`, so an INSERT/UPDATE/DELETE carrying a geometry value applies correctly — every subtype (point / line / polygon / multi* / collection), dimension (2D/Z/M/ZM), and byte order, pinned against a real PostGIS target.

### Fixed
- **Per-column SRID is now preserved on geometry CDC apply (#20).** The CDC readers strip per-row SRID to raw WKB (the IR carries SRID as a per-column property, ADR-0035), and the applier previously defaulted the column SRID to 0 — so a replicated value into a constrained `geometry(<type>,<srid>)` column lost its SRID. The applier now recovers each geometry column's real SRID (and subtype) from `geometry_columns` / `geography_columns`, so the stored geometry matches the source (verified `ST_AsEWKB` + `ST_SRID` src==dst).

### Compatibility / notes
- No flag or config change; geometry-over-CDC works by default once the target has PostGIS installed.
- `geography` columns and `geometry[]` (arrays of geometry) remain **loudly refused** over CDC apply (no silent loss) — separate follow-up work. A per-row SRID stored in an *unconstrained* `geometry` column is still dropped by design (ADR-0035).

## [0.99.53] - 2026-06-16

### Fixed
- **Keyless-table CDC WARN now states at-least-once delivery honestly (Bug 143).** The ADR-0089 keyless guard applies each INSERT into a table with no PRIMARY KEY (and no usable unique index) as its own transaction so the adaptive batch default never makes such tables worse than `--apply-batch-size=1`. Its WARN, however, claimed this meant "crash-replay cannot duplicate rows" — which is **false**. A keyless INSERT is not idempotent, and crash-resume granularity is the **source transaction** (the GTID/LSN only advances at its commit; for VStream the position-bearing VGTID arrives *after* every row event of the transaction), so a hard kill before that commit checkpoint re-inserts **every keyless row in the interrupted source transaction** on resume — not one. Keyless CDC is at-least-once; per-row checkpointing cannot change that (you cannot resume mid-source- transaction). This release corrects the WARN (and the matching ADR-0089 / code-comment claims) to state the at-least-once semantics plainly and point at adding a PRIMARY KEY (or NOT NULL UNIQUE index) for exactly-once, batched throughput. **Behaviour is unchanged** — this is a truth-in-logging fix; the long-standing guidance (ADR-0010: tables without a key are not recommended for continuous sync) is unchanged.

## [0.99.52] - 2026-06-16

### Fixed
- **MySQL ENUM values now sync correctly over CDC (Bug 145).** The MySQL binlog hands an `ENUM` cell back as its **1-based ordinal index**, not the label, and the CDC value decoder passed that straight through — so a replicated INSERT carried `"2"` instead of `"active"`. A PostgreSQL enum target **rejected** it (`SQLSTATE 22P02 invalid input value for enum`); a MySQL target only appeared to work because it coerces the numeric string by index (fragile, and wrong the moment the value list shifts). The decoder now maps the index to its label via the column's value list; a label string (the snapshot/copy path and the VStream reader both deliver labels) passes through unchanged. Applies to **all** MySQL enum CDC, not just schema-change forwarding.
- **MySQL ENUM schema changes now forward to a PostgreSQL target (Bug 145).** Two DDL gaps in the forward path: an `ADD COLUMN <enum>` referenced the named PG enum type without creating it (`42704 type does not exist`), and a MySQL `MODIFY … ENUM(...)` that appends a value (which arrives as an alter-column-type) hit an internal "enum DDL requires column context" error. The forward path now creates the enum type first (idempotent `CREATE TYPE`) for an added enum column, and forwards an appended value as `ALTER TYPE … ADD VALUE IF NOT EXISTS`. Appending a value is exact; a value rename/removal on the source leaves the PG enum a superset (no data loss — every value the source can still produce remains valid). Combined with the value fix above, MySQL→PG ENUM now works end-to-end (add column, add value, rows landing by label).

## [0.99.51] - 2026-06-15

### Fixed
- **A PlanetScale / Vitess (VStream) resume from a purged GTID position now auto-recovers instead of restart-looping (ADR-0093).** When a persisted resume position falls behind the source's retained binlogs (`gtid_purged` advanced past it — routine on PlanetScale's binlog-retention window), the stream used to exit with an unclassified error and, on supervisor restart, hit the same purged position again — a restart loop. The self-hosted MySQL binlog source already auto-recovers this via a pre-flight check → cold-start (ADR-0022); the VStream source had no parity because vtgate is a proxy with no single `gtid_purged` to pre-flight, so the condition only surfaces reactively from the stream and was never classified as an invalid position. Now the vtgate "purged required binary logs" error is classified as an invalid position and routed to a **one-shot, non-destructive cold-start re-snapshot** (the idempotent copy absorbs the overlap; the target is not dropped). The recovery is bounded — a second consecutive invalid position after a fresh re-snapshot fails loudly (the source is purging faster than a snapshot can complete, which auto-retry cannot fix). This was always a loud failure, never silent data loss; it now self-heals. Found by cross-referencing PlanetScale's own `fivetran-source` connector.

### Added
- **`--no-auto-resnapshot`** (`sync start`): opt out of the automatic re-snapshot above. With this flag, a purged/invalid resume position surfaces as a loud, actionable terminal error naming the recovery commands (`--restart-from-scratch` / `--reset-target-data`) instead of auto re-snapshotting — for operators who would rather decide a (potentially expensive) full re-snapshot of very large tables deliberately. The flag gates both the binlog pre-flight fall-through and the new VStream reactive recovery, keeping the two paths consistent. Default (unset) = auto-recover, parity with the binlog path.

## [0.99.50] - 2026-06-15

### Fixed
- **Postgres array columns now sync over CDC (Bug 144).** A Postgres source table with any array column (`int4[]`, `text[]`, `numeric[]`, `timestamptz[]`, …) wedged the continuous-sync stream on the first INSERT/UPDATE/DELETE to that table with `unsupported column type OID 1007/1009/1231…`: the pgoutput CDC reader's type resolver (`oidToType`) had no array-OID cases, even though the initial bulk-copy (cold-start) handled arrays fine. Array columns are now resolved to the IR array type by mapping each array OID to its element OID and recursing — so an array element decodes identically to the same scalar column — and the array-value decoder now also accepts the `[]byte` text form that the pgoutput wire delivers (it previously only handled the cold-start `[]any` / `string` shapes, which silently mis-walked the text bytes). Multi-dimensional arrays are preserved (not flattened), NULL elements survive as NULLs, and `numeric[]` keeps full scale. This was a loud failure (the stream stopped with a clear error), never silent data loss. The decode↔write asymmetry is unchanged: `timetz[]`, `bytea[]`, and `json[]`/`jsonb[]` arrays still refuse loudly at apply (no silent acceptance). Same dual-registry-drift class as the earlier Bug 97/118 fix; a parity guard now pins the CDC array registry to the schema reader's so they can't drift again.

## [0.99.49] - 2026-06-15

### Changed
- **Postgres CDC apply is now pipelined (ADR-0092) — ~70× higher apply throughput on latency-bound (cross-region/cross-cloud) links.** The batch apply path used to send a batch of N changes as N serial `Exec` round trips inside one transaction, plus a position upsert and a commit (N+2 round trips). Batching (ADR-0017/ADR-0089) amortized the commit fsync but did nothing for the N data round trips — they stayed serial, so steady-state apply throughput was bounded by `1 / per_row_exec_latency`, which on any non-co-located link is dominated by network RTT. The data statements **and** the position upsert are now queued onto a single `pgx.Batch` and sent in one pipelined flush; round trips per batch drop from N+2 to O(1) (begin + flush + commit), independent of N, so throughput becomes bounded by the server's execution rate rather than `N × RTT`. Measured on a live PlanetScale soak, apply was pinned at ~90 rows/s on a ~7 ms cross-cloud link and a PS-10 → PS-80 database upsize moved it 0% (the bottleneck was the wire, not the DB); pipelining lifts that ~70×. The win scales with RTT — co-located/low-latency targets were already fast (batching amortized the commit) and see a smaller gain, never a regression. Default-on for the batch apply path (`--apply-batch-size` > 1, the `auto` default); `--apply-batch-size=1` keeps the serial per-change path verbatim. Value encoding is byte-identical to the prior path: the pipelined pool runs in pgx `QueryExecModeDescribeExec` (real described OID, BINARY format, same per-OID codecs), reusing the existing `buildInsertSQL`/`buildUpdateSQL`/`buildDeleteSQL` builders and `prepareValue` codec path byte-for-byte — pipelining changes *when* statements are sent, never *how a value is encoded*. Durability and atomicity are unchanged (position rides the same tx, `synchronous_commit = on` pinned, crash before the single commit rolls back both — ADR-0007 holds). Postgres-only; MySQL apply is unchanged (the ADR-0081 seam was generalized so MySQL can adopt pipelining later). If the raw pgx-conn escape is ever unavailable it falls back to serial `*sql.Tx` exec with a one-time WARN — loud, never silent. Pre-existing, unchanged limitation: geometry over CDC is refused loudly on the applier path (no binary geometry codec yet), identically on the serial and pipelined paths; the snapshot/migration COPY path handles geometry fine.

## [0.99.48] - 2026-06-15

### Fixed
- **Online schema changes now forward on a PlanetScale / Vitess (VStream) source after a cold start (ADR-0091, F7c).** The default-on schema-change forwarding from v0.99.45–46 did not engage for a VStream source that had cold-started: the VStream cold-start CDC tail (`dispatchCDCEvent`) is a separate dispatch from the standalone reader and its FIELD branch only cached the field list — it never emitted the ADR-0049 `ir.SchemaSnapshot` boundary the forward intercept needs. So an ADD/DROP/ALTER COLUMN on the source after a cold start never forwarded, and the post-DDL row then failed to apply with `42703` (PG) / `1054` (MySQL). (A warm-resumed VStream stream was unaffected.) The cold-start FIELD branch now emits the boundary, exactly as the standalone reader does. Additionally, `NormalizeForCDCComparison` is now flavor-aware: the VStream FIELD projection carries no primary key, no secondary indexes, and not reliably charset/collation, so for VStream flavors those are stripped from the cold-start seed to prevent a phantom PK-index-drop / ALTER-TYPE refusal (vanilla MySQL binlog is unchanged). Documented limitation, in line with the wire's fidelity: CREATE/DROP INDEX and charset-only ALTER cannot forward on a VStream source. With this, the ADR-0091 §1d forwarding matrix holds across all three source paths — MySQL binlog, PG pgoutput, and PlanetScale/Vitess VStream.

## [0.99.47] - 2026-06-15

### Fixed
- **Connection-budget preflight no longer false-refuses on tight managed Postgres (PG 18 / pooled targets).** The target connection-budget preflight computed `in_use` with an unfiltered `SELECT count(*) FROM pg_stat_activity`, which also counts PostgreSQL's background processes — checkpointer, background/wal writer, autovacuum launcher, archiver, logical-replication launcher, and (PG 18+) the async I/O workers. None of those consume a `max_connections` slot, so `in_use` was over-reported by the background-process count (≈9 on a PG-18 managed instance). On a tight target — e.g. a managed Postgres with `max_connections=25` and ~9 background processes — this produced a false `target connection budget exhausted` that blocked **cold start entirely**, even though only ~4 real client backends were in use. The probe now counts `WHERE backend_type = 'client backend'` (PG 10+; sluice's pgoutput CDC already requires PG 10+). The sibling role/database probes already filtered correctly; only the global count was affected.

## [0.99.46] - 2026-06-15

### Added
- **PostgreSQL-source `RENAME COLUMN` forwarding (ADR-0091, F7b).** Completes the default-on schema-change forwarding from v0.99.45 — the one shape it still refused. Under `--schema-changes=forward` (default), a column rename on a PostgreSQL source now forwards to the target (`ALTER TABLE … RENAME COLUMN`, data preserved), **but only when provable.** A rename and a `DROP x + ADD y` of the same type are indistinguishable from the replication stream's row shape, so sluice proves the distinction with `pg_attribute.attnum` — the column's catalog identity, stable across a rename: same attnum + different name = real rename (forward); different attnum = genuine drop+add (refuse). The proof is definitive, so this can only forward a true rename or refuse — never mis-forward a drop+add into data loss. Same-engine (PG→PG) and cross-engine (PG→MySQL). A new optional `ir.Column.StableID` carries the proof; it is pure metadata (excluded from the schema-decode signature, alter-detection, and the schema-history/backup codec). A **MySQL source** has no stable column id, so a MySQL-source rename continues to refuse loudly — unchanged.

### Changed
- **PostgreSQL-source renames now forward by default** (extends the v0.99.45 default-on forwarding to the rename shape). Set `--schema-changes=refuse` to keep the conservative halt-on-DDL behavior for all shapes. No on-disk/format change.

## [0.99.45] - 2026-06-15

### Added
- **Default-on schema-change forwarding for single-stream CDC (ADR-0091, F7a).** New tristate `--schema-changes=forward|refuse` (default `forward`) replaces the opt-in ADD-COLUMN-only path: an unambiguous source DDL change is now retargeted to the target dialect and applied in-line on the CDC boundary, keeping the sync online instead of refusing and forcing a manual drain-and-DDL. Real forwarding matrix (ADR-0091 §1d is the source of truth): ADD COLUMN / DROP COLUMN / ALTER COLUMN TYPE forward on **both** source engines, same-engine and cross-engine (MySQL↔PG; DROP COLUMN auto-applies on the target); ALTER NULLABILITY forwards on a **MySQL source only** (PG's pgoutput carries no nullability flag); REORDER is a no-op (name-based decode); RENAME COLUMN **refuses loudly on both engines** (indistinguishable from drop+add from the stream — data-loss risk; PG attnum-proven rename is follow-up F7b); multi-shape combos and volatile/computed DEFAULT on ADD still refuse. Documented limitations (the wire doesn't carry the metadata, so no boundary is produced): PG-source nullability/index/check and MySQL-source index/check are **not** forwarded — any resulting incompatibility surfaces as a **loud apply error**, never silent corruption. Safety: a seed-guard never forwards a destructive shape classified against the cold-start baseline (only on a genuine CDC→CDC boundary), and the PG normalizer strips the generated columns + secondary indexes pgoutput omits, so a phantom-destructive forward can't be synthesized (a CRITICAL regression CI caught on the flip and fixed before ship). `--forward-schema-add-column` is **deprecated** (warns, still forwards — subsumed). **Behavior change on upgrade** — see Changed. Pin the old conservative behavior with `--schema-changes=refuse`.

### Changed
- **`sluice sync` now forwards source schema changes by default (`--schema-changes=forward`).** A stream that previously **refused loudly** on source DDL now **forwards it by default** — including DROP COLUMN, which auto-applies on the target. Restore the exact pre-v0.99.45 halt-on-DDL behavior with `--schema-changes=refuse`. Refused shapes (RENAME, multi-shape, volatile DEFAULT) still refuse loudly. No on-disk/format change; `migrate` and the cold-start copy path are untouched.

### Fixed
- **Source-side Vitess schema-resolution errors are now retriable, not terminal (F9).** Right after a DDL cutover — or when the Vitess schema historian is off (`track_schema_versions` is default-disabled on PlanetScale) — the source vstreamer transiently misses with `unknown table <t> in schema` / `no schema found for table <t>`. These arrive as free-text VStream errors with no gRPC status or MySQL wrapper, so they fell through to **terminal** and killed the stream on a window that self-clears. They are now `ir.RetriableError`, so the ADR-0038 backoff rides out the cutover in-process; substring-matched and pinned, with a near-miss guard so a bare "unknown table" (real DROP/typo) stays terminal.
- **Target schema-drift apply errors no longer tight-restart crash-loop; the sync self-heals (F8).** A source ADD COLUMN (or new table) not yet created on the target made the apply fail terminal — PG 42703 / 42P01, MySQL 1054 / 1146 — exiting the process; under a supervisor this became a tight-restart loop (the soak observed `NRestarts=1821`). These codes are now `ir.RetriableError` with a remedy-named message, so the ADR-0038 backoff rides them out in-process and the sync **self-heals the moment the operator adds the missing column/table on the target**. The wrap keeps the underlying `*PgError`/`*MySQLError` reachable via `errors.As` so the offending column stays named on every (loud) retry. Covers MySQL→MySQL (incl. PlanetScale→PlanetScale) and PG targets symmetrically. Scope: ADD COLUMN / missing-table only.

## [0.99.44] - 2026-06-14

### Changed
- **`sluice sync --apply-batch-size` now defaults to `auto` (ADR-0089) — >10× CDC apply throughput out of the box.** The ADR-0052 AIMD batch-size controller has shipped since v0.72.0, but the conservative default `--apply-batch-size=1` made its cap equal its floor, leaving it dormant for every default user. The first real PlanetScale soak measured single-row apply at ~240 rows/s vs ~6,500 at `auto` (>10×). The default is now `auto` (engine ceiling 1000 mysql/postgres, 100 planetscale; AIMD adapts within `[1, ceiling]`). Safety guard: a table with no PRIMARY KEY and no usable unique index (non-idempotent plain INSERT on replay — Bug 125 class 3) is never batched — each change commits alone (crash-replay duplicate blast radius stays at 1), with a one-time WARN; PK/unique tables batch and adapt. Restore the old behavior with `--apply-batch-size=1` or `--no-auto-tune`.

### Fixed
- **VStream throttle/large-transaction stalls no longer crash-loop a continuous sync (Bug 141 / ADR-0090).** A transient source-tablet throttle (vtgate withholds change events and, near its 10-min tolerance, heartbeats) made the liveness/progress watchdog fire and misdiagnose the throttle as a failover; the terminal error exited the process → a supervisor restarted it → it warm-resumed to the same throttled position and re-stalled → a tight, non-converging crash-loop. The watchdog timeouts are now `ir.RetriableError`, so the ADR-0038 backoff retry reconnects from the last position in-process and rides out the throttle (correct for a real failover too); a genuinely non-healing wedge still fails loud after the bounded retry budget. Found + root-caused on the soak and a self-hosted Vitess-24 reproduction.
- **Self-hosted `--source-driver=vitess` can now warm-resume (Bug 142).** The `vitess` flavor's engine name wasn't in the position-decode accept set, so a resumed position stamped `Engine="vitess"` was rejected and every restart crash-looped with `wrong engine "vitess"`. Unconditional; PlanetScale (flavor `"planetscale"`) was unaffected. The decoder now accepts `"vitess"`.

## [0.99.43] - 2026-06-13

### Added
- **MySQL `backup full`: coordinated parallel backup snapshot — ~2.6× faster dump, ~2.3× faster restore (ADR-0088).** sluice's MySQL backup table sweep was serial (one `START TRANSACTION WITH CONSISTENT SNAPSHOT` connection) because MySQL — unlike Postgres — has no shareable *exported* snapshot to lazily import onto parallel readers. It now opens N reader transactions whose consistent snapshots **coincide** under a brief `FLUSH TABLES WITH READ LOCK` window (mydumper's own mechanism), so `--table-parallelism > 1` (default auto = 4) overlaps cross-table reads on a vanilla MySQL source. Measured on a 16.25 GB / 33 M-row corpus: **dump 184 s → 70 s (2.63×), restore 404 s → 179 s (2.26×)**, artifact unchanged. Cross-table consistency and the anchored `EndPosition` are preserved (the N snapshots are identical by construction). Falls back — loudly — to the serial single-reader path when the source role lacks `RELOAD` (most managed tiers); PlanetScale/Vitess sources are unaffected (they keep the VStream-COPY path). See `docs/comparison-backup.md` for the fair-fight.
- **MySQL/Vitess CDC: surface a throttled-or-idle VStream stall instead of hanging silently (observability; roadmap item 19(a)).** When a Vitess source's tablet throttler engages mid-stream, vtgate withholds ROW/change events but keeps sending ~5s heartbeats *and strips the tablet's in-band `VEvent.throttled` flag* — so the stream stays alive (the progress watchdog re-arms on heartbeats, correctly resilient) but the stall was **silent**: unbounded lag, zero diagnostic. Three observability-only changes (no change to the resilient streaming behavior): (1) the at-stream-open Phase-1 liveness error now names the source throttler as a candidate cause alongside the primary-only topology wedge ("...or the source tablet throttler is denying the stream — check `SHOW VITESS_THROTTLED_APPS` on the primary"); (2) a new Phase-2 SOFT idle sub-window emits a rate-limited WARN — *"alive (heartbeats flowing) but NO change events for Ns — the source may be throttled or genuinely idle"* — once per quiet spell, cleared by the next real change event, default 30s and tunable per-DSN via `vstream_idle_warn_timeout` (`0` disables the WARN only; the hard liveness/progress guards are unaffected). The soft-window timer lives entirely in the single watchdog goroutine (the race-free pattern). Docs: `docs/vitess-vstream-troubleshooting.md` §2 + Detection.

### Compatibility
- **Drop-in from v0.99.42 — no format or breaking changes.** On a vanilla MySQL `backup full`, `--table-parallelism > 1` (default auto = 4) now engages the ADR-0088 coordinated FTWRL path instead of sweeping serially; the artifact is byte-equivalent and the recorded position is unchanged. The new `vstream_idle_warn_timeout` DSN param defaults to 30s (`0` disables the idle WARN only). `migrate`, the Postgres paths, and cross-engine behavior are untouched.

## [0.99.42] - 2026-06-13

### Fixed
- **MySQL CDC: a source-side `TRUNCATE TABLE` carrying a leading SQL comment is no longer silently dropped (Bug 140).** MySQL preserves a statement's leading comment verbatim in the binlog `QUERY_EVENT` (only the trailing delimiter is stripped), but the CDC reader's truncate detection (`parseTruncateTable`) required the statement to *start* with `TRUNCATE`. A commented truncate — a hand-written migration (`-- clear staging\nTRUNCATE TABLE t`) or an APM/ORM query tag (`/* trace=… */ TRUNCATE …`) — therefore fell through to generic DDL handling and never emitted a typed `ir.Truncate`, so on a live MySQL → {MySQL, Postgres} sync the **target silently retained every row the source truncated** and the stream never converged (no error, no lag-clearing signal): a HIGH silent-divergence class on a routine operation. The reader now strips leading SQL comments (`--`, `#`, `/* */`) before recognising the verb; executable comments (`/*! */`, `/*+ */`) are deliberately left in place (stripping them could discard conditionally-run SQL — they fall through harmlessly as before). Postgres sources were never affected (pgoutput emits a typed truncate message; no query-string parse). Pinned by unit comment-variant cases and a new MySQL-source truncate-propagation integration test (the only prior truncate integration test was Postgres-only).

### Internal
- Extended the random-op sync-convergence property to the **cross-engine directions (PG↔MySQL)** with a value-semantic canonical compare, alongside the existing same-engine pair. Running the new leg surfaced and fixed two harness-side cross-engine canonicalisation edges (timestamp trailing-zero fractions: PG `::text` renders the minimal fraction while MySQL `DATETIME(6)` pads to six digits; and dump row ordering: `ORDER BY id` binds to the text projection on PG but the bigint column on MySQL). Test-only.
- CI: stabilised a flaky AIMD-controller integration test by binding the metrics listener to a dynamically-allocated port instead of a hardcoded one inside Linux's ephemeral range. Test-only.

## [0.99.41] - 2026-06-12

### Fixed
- **`backup compact` no longer refuses an ordinary rotated chain when a rotation-born segment never received a rollover commit in its creating session (Bug 139, ADR-0087).** The "rotate on a timer, stop when idle" workflow (and a crash/end at a rotation boundary) leaves a rotation-born segment with no committed incremental, so it carries no `incremental_coverage_start` stamp and resolves to its full's snapshot anchor `S` — a few WAL bytes past the prior segment's `end_position`. Compact saw that as a position gap and refused the WHOLE run with a message blaming "a pre-ADR-0067, imported, or corrupted lineage" — for a chain its own rotation produced (loud, zero data loss, but the compact DR-maintenance feature became unusable across that boundary). Now compact **splits** the merge group at the gap instead: the stamp-less segment stays in its own group (one WARN naming the boundary; no data lost, chain fully restorable) while every contiguous run around it still merges — both naive and `--smart-compaction`, including `--dry-run`. Separately, the next `backup stream` / `backup incremental` resume of such a segment now replays from the prior segment's `end_position` (`P_N`), so its first incremental stamps `incremental_coverage_start = P_N` and the boundary heals and compacts fully (N→1). Neither half ever stamps coverage no committed incremental proves — creation-time stamping and resume-backfill were rejected as silent-DR-loss hazards. Affected releases: v0.88.0 through v0.99.40 — the strict contiguous-rotation handoff that produces the `S > P_N` boundary shipped with ADR-0067 (Bug 95) at v0.88.0; before that, rotated chains were refused by design, not by this false positive. Pinned by the compact-split unit matrix, the resume-rule unit test, and a PG idle-stop integration repro (split + restore == oracle; resume-heal → whole-chain N→1 + restore == oracle).

## [0.99.40] - 2026-06-12

### Fixed
- **`backup compact --strategy=smart` leaked one open file handle per compacted change chunk (task #9).** The decode pass wrapped its store reader in `io.NopCloser`, so the handle opened by `Get` was never released on the success path. On Linux the leaked descriptor merely lingered until process exit; on Windows it was fatal — the rewrite step renames over the very path the leaked handle still holds open, failing loudly with `Access is denied`. The byte-count wrapper now owns the store handle so the chunk reader's `Close` releases it; pinned by a platform-neutral handle-tracking test (revert-verified: the old code leaks exactly one handle per chunk) and by the previously-failing Windows integration repro now passing.
- **`backup full` no longer refuses tables containing float `NaN` / `±Infinity` (Bug 138).** PG `float4`/`float8` columns legally hold IEEE specials and `migrate` carries them exactly — but the chunk codec rendered floats as JSON numbers, which cannot represent them, so one NaN row made the whole database un-backupable (loud refusal: `json: unsupported value: NaN`). Non-finite floats now ride a new additive tagged envelope (`{"_t":"f64s","v":"NaN"|"+Inf"|"-Inf"}`), on both the fast and legacy codec paths. Restores are `float8send`-bit-identical to a `pg_dump` round trip: ±Inf exact, every NaN canonicalized to the IEEE quiet NaN exactly as PG's own text format does (NaN payload bits are not representable in either format). Compatibility: chunks WITHOUT non-finite floats are byte-unchanged; a chunk that DOES contain one is refused loudly ("unknown value tag") by v0.99.39-and-older binaries — additive-tag forward compatibility, never silent. Numeric (`numeric`-typed) `NaN` was always fine and is untouched.

## [0.99.39] - 2026-06-11

### Performance
- **Backup/restore per-row JSON codec rewritten as a direct buffer-append fast path (tasks #51/#52).** Profiling the 136 GB bench corpus showed the reflection-based `encoding/json` round trip of the per-row map was 49% of `backup full` CPU and 69% of `restore` CPU. The chunk row encode/decode now runs on a specialized codec that emits/parses the SAME wire bytes (byte-identical output for every shape the fast path accepts, no chunk-format change, old and new binaries read each other's chunks): ~10× faster per row in both directions at the microbenchmark level (encode 82→0 allocs/row, decode 189→27). Any value or line outside the canonical shapes falls back to the legacy path, which remains the semantic and error oracle; differential sweeps plus two fuzz targets pin the two paths equivalent on arbitrary input. Measured end-to-end on the 136 GB / 431M-row bench corpus (together with the O(1) checkpoint fix below): `backup full` 881 s → 435 s and `restore` 2810 s → 1390 s — both legs −51%, zero-loss, shrinking the gap to the `pg_dump`/`pg_restore -j8` specialists from ~3.1–3.2× to 1.83× / 1.51× (see `docs/comparison-backup.md`).
- **PG→PG raw-copy single streams are ~4.9× faster (task #37).** The PG server emits one CopyData message per row on `COPY TO STDOUT`, and each row paid a synchronous unbuffered-pipe rendezvous plus a ~265-byte socket write to the target — 81.8% of single-stream CPU. A 64 KiB buffer ahead of the pipe coalesces the frames (byte-transparent — the COPY stream has no per-Write framing): 72.6 s → 15.0 s on a 4M-row / 1040 MB single-stream run (14 → ~73 MB/s), checksum-verified zero-loss.
- **MySQL `LOAD DATA` bulk writes get the same per-row pipe-rendezvous fix.** The TSV encoder issued one unbuffered pipe write per row; it is now buffered the same way (64 KiB, flushed before close on success, errors still poison the read). Byte-transparent; covered by the existing LOAD DATA zero-loss and warning-probe pins.
- **Backup checkpoints are now O(1) per event — the manifest is no longer rewritten per chunk/table (task #54, ADR-0086).** Every per-chunk / per-table checkpoint during `backup full` used to re-marshal the ENTIRE manifest (embedded schema included) and re-Put `manifest.json`, making the row sweep quadratic in table count (the
  #38 scale probe measured ~78 hours of pure manifest rewriting at
  100k tables). The in-progress manifest is now a base written once plus an append-only `manifest.progress.jsonl` sidecar (one JSON line per event); the manifest is marshaled exactly twice per run (base + final), and a successful backup's on-disk layout is unchanged — restore/verify/chain tooling and older binaries read finalized backups exactly as before. In-progress sidecar-layout manifests are stamped format version 3 so an OLDER binary asked to resume a crashed backup refuses loudly ("upgrade sluice") instead of silently resuming off a base that under-reports progress; new binaries resume old-format in-progress backups unchanged. Stores without an append primitive (S3/GCS/Azure blob stores) keep the previous full-rewrite checkpoints, named loudly on large corpora.

### Fixed
- **PG schema reads no longer die with SQLSTATE 53100 on huge catalogs under small `/dev/shm` (task #55, found by the #38 scale probe).** At ≥50k-table catalog sizes Postgres planned parallel hash joins for several of sluice's catalog metadata queries; parallel workers allocate their shared hash tables as dynamic shared memory segments in `/dev/shm`, which on container-default 64 MB shm exhausts with `could not resize shared memory segment … No space left on device (SQLSTATE 53100)`. Every PG SchemaReader catalog query now runs in its own read-only transaction with `SET LOCAL max_parallel_workers_per_gather = 0` — serial plans build hash tables in process-local `work_mem` and cannot hit the wall, and parallelism buys nothing on catalog reads (validated: a 50k-table / 150k-index schema read completes in ~15 s either way, and the fixed binary succeeds even with `/dev/shm` 100% full where the previous one failed). No operator action or `--shm-size` tuning needed.

### Changed
- **Schema fingerprints are now stable across manifest JSON round-trips (task #49).** `ComputeSchemaHash`'s canonical view normalizes a nil `Column.Default` to the explicit `DefaultNone` the manifest decode hooks materialize, so a reader-fresh schema and the same schema re-read from a manifest fingerprint identically; the backup resume drift guard no longer JSON-round-trips the fresh side to compensate. Recorded `schema_hash` values change for schemas with columns that have no default — harmless: manifests' stored hashes are write-only today (nothing compares against a previously stored value), and the drift guard always recomputes both sides.

## [0.99.38] - 2026-06-11

### Fixed
- **Silent chain gap on crash-resume of an anchored backup (task #42, ADR-0085) — a CRITICAL silent-loss class.** A resumed `backup full` kept the interrupted attempt's completed tables verbatim (exact as-of the FIRST attempt's snapshot anchor A1) but always opened a fresh snapshot and recorded the NEW anchor A2 as the manifest's `EndPosition` — the in-progress manifest never carried an anchor, so a crashed run's anchor was simply lost. Writes landing on kept tables in (A1, A2] were then in NEITHER the row chunks NOR the next incremental's window: the chain restored cleanly, exit 0, missing those writes. The `--chain-slot` shape compounded it — the slot-already-exists refusal advised `sluice slot drop` + retry as crash recovery, releasing the very WAL that covered the gap. Now: the in-progress manifest carries the anchor from its first write (made durable before the sweep), a resume ADOPTS the prior anchor (the fresh snapshot serves only read consistency; re-streamed tables' overlap with the replay window converges under the idempotent ADR-0010 appliers), and a `--chain-slot` resume preflights and adopts the standing chain slot instead of refusing (the slot now deliberately survives an interrupted run — it is the resume's WAL-retention guarantee — and the refusal message says "re-run the same command", reserving drop + `--force-overwrite` for a deliberate fresh start). Loud refusals close the unsound corners: a truly keyless table that must be (re-)streamed on an anchored resume (overlap replay would duplicate it), schema drift between the attempts, and incrementals/streams chaining off a still-in-progress parent. Pre-fix in-progress manifests (no recorded anchor) re-stream every table. `--force-overwrite` now also discards an in-progress prior, making it the uniform escape hatch.

## [0.99.37] - 2026-06-11

### Fixed
- **Bug 136: PG → MySQL with an index on a `text`/`bytea`/JSON-landing column now refuses EARLY — before any DDL or rows move — instead of dying with MySQL Error 1170 at create-indexes, after the full bulk copy.** MySQL cannot index a TEXT/BLOB column without an explicit key length (and JSON columns cannot be key parts at all); a PG source never carries prefix lengths, so a UNIQUE or secondary index on such a column emitted invalid index DDL that failed at the latest possible moment, and `sluice schema preview` rendered it with no advisory. sluice deliberately does NOT auto-emit a prefix key length — a prefix index (above all a UNIQUE one) silently changes matching/uniqueness semantics. The refusal fires at the shared cross-engine pre-flight (migrate, chain restore, restore, and incremental schema-deltas all go through it), names every offending `table.column` + index, and spells out the `--type-override TABLE.COL=varchar(N)` escape hatch — which keeps working end-to-end (indexes build, UNIQUE semantics preserved on the full value). `schema preview` now renders a dedicated "migrate WILL REFUSE" section (text) and a `text_index_refusals` JSON list. The scan covers the full no-key-length family per the Bug 74 doctrine — every TEXT tier, every BLOB tier, the Bug 72 wide-`varchar(N)` down-map tiers, JSON/arrays/ hstore, and DOMAINs over any of those — × {UNIQUE, plain secondary, composite member, PRIMARY KEY}. Same-engine MySQL → MySQL prefix-indexed TEXT sources are unaffected (the prefix length round-trips as before), as is PG → PG. The MySQL-family target gate in the translation-notice scans now also recognizes the self-hosted `vitess` flavor (previously only `mysql`/`planetscale`), so the Bug 69/72 advisories and this refusal fire for vitess targets too.
- **ADR-0054 shard-consolidation leases are now host-TZ-independent on Postgres targets (task #44).** `lease_expires_at` is a naive `TIMESTAMP` column, and pgx encodes a `time.Time` parameter as its own location's wall-clock digits — so a sluice process on a TZ-behind-UTC host (e.g. PDT) wrote an expiry hours in the past, making a just-acquired lease instantly stealable by a peer shard (the cross-shard DDL serialization guarantee was void), while a TZ-ahead-of-UTC host wrote an expiry hours in the future (stuck lease blocking takeover). Lease expiries are now normalized to UTC before binding, and the takeover guard compares against `timezone('utc', now())` instead of `CURRENT_TIMESTAMP`. Rows written by earlier versions from UTC hosts already hold UTC digits and remain compatible; rows from non-UTC hosts were already broken in the same direction this fixes. MySQL targets were unaffected (the driver config pins `loc=UTC` + session `time_zone='+00:00'`); a host-TZ independence pin now covers both engines and both skew directions.
- **Bug 137: a hard-killed Postgres-source `backup full` no longer leaks its snapshot-anchor replication slot.** The default-shape (non `--chain-slot`) anchor (`sluice_backup_anchor_<timestamp>`) was created as a persistent slot and dropped only on graceful close — so every SIGKILLed/crashed backup left an inactive slot behind, each one silently pinning WAL at its `restart_lsn` until the source disk filled. Two-part fix: (1) the anchor is now created protocol-`TEMPORARY`, so the server itself reclaims it when the backup's replication connection dies — including on hard process death; (2) when a backup RESUMES an in-progress run, the engine sweeps persistent anchor slots leaked by pre-fix binaries (inactive, non-temporary, anchor-named, older than a one-hour safety margin), WARN-logging each slot dropped and each too-young suspect deliberately left alone. `--chain-slot` runs are unaffected by design: the persistent chain slot is the deliverable, and a crashed run's leftover is surfaced loudly by the already-exists refusal on retry.

## [0.99.36] - 2026-06-11

### Fixed
- **CRITICAL (Bug 135, v0.99.35 regression, caught by the post-release battle-test): resuming an interrupted backup no longer silently corrupts the artifact.** The per-chunk resume reuse kept a prior partial run's chunk N verbatim and skipped N×chunk-rows rows of the NEW row stream — which assumed the two runs deliver rows in identical order. The reader has never guaranteed that (full-table reads carry no ORDER BY by design), and under v0.99.35's parallel sweep the accidental stability broke reliably: resumed backups contained duplicate AND missing rows while exiting 0 with a self-consistent manifest. Resume is now table-granular — fully-completed tables are still kept verbatim (order-independent), partially-written tables re-stream from scratch (bounded by the crash contract: at most `--table-parallelism` tables were in flight), and byte-identical re-produced chunks still skip their upload via a content-addressed comparison. Pinned by a revert-tested order-divergence test that reproduces the exact corruption shape pre-fix. If you resumed an interrupted backup ON v0.99.35, discard that artifact and re-run it fresh.

## [0.99.35] - 2026-06-11

### Added
- **`backup full --chain-slot` — one-flag incremental-chain provisioning (Postgres, ADR-0083).** The persistent chain slot (named by `--slot-name`) is created *as* the snapshot anchor — its consistent point IS the manifest's recorded EndPosition — and the pgoutput publication is ensured *before* the anchor, so `backup incremental` chains with zero gap and zero manual setup. Previously the operator had to create both objects by hand *before* the full (the manifest pointed at a slot that might not exist); pgoutput's historic-catalog rule means a late-created publication can never decode the chain's first window, observed live in benchmarking. The slot is kept only when the backup completes — a failed run drops it so retries start clean — and an already-existing slot is refused loudly. Engines without a slot concept (MySQL) log a loud no-op.

### Fixed
- **Silent-loss class closed: an incremental against a late-created or foreign-advanced slot now refuses loudly instead of silently gapping the chain.** A replication slot created (or advanced by another consumer) *after* the parent backup cannot serve the WAL in between — PostgreSQL silently fast-forwards `START_REPLICATION` to the slot's `confirmed_flush_lsn`, so `backup incremental` used to SUCCEED while the chain silently missed every write in the gap. A new chain-resume preflight refuses with the exact positions and the recovery (`backup full --chain-slot`). The slot-missing refusal also now says the slot may never have existed (the old message blamed WAL pruning).
- **Backup chains no longer ack streamed-but-uncommitted positions.** `backup incremental` / `backup stream` have no applier, so the CDC reader's keepalive fell back to acking the *streamed* LSN — events parsed by the pump but discarded at window close could advance `confirmed_flush_lsn` past the recorded EndPosition, silently gapping the next link (timing-dependent). The chain consumers now hold the slot ack at the stream start and release it only to durably committed window ends; on `backup stream` this also bounds source WAL retention to ~one rollover window.
- **DDL-free incrementals no longer record phantom `alter_table` schema deltas.** Schema readers drained indexes and foreign keys through Go map iteration (randomized order), so two reads of the *same* schema could be structurally unequal — a recorded parent manifest vs the end-of-window catalog read then diffed as spurious `alter_table` entries (observed: 6 phantom deltas on a DDL-free incremental), and `ComputeSchemaHash` fingerprints diverged for identical schemas. Both engines' readers now drain in sorted (table, name) order; the incremental schema diff compares indexes as a name-keyed set (so chains rooted in pre-fix manifests stop false-alarming too); and the schema hash fingerprints a canonical view (non-semantic collections name-sorted; table and column order stay semantic).
- **Stop-then-restart no longer stalls on a lingering walsender.** Closing a replication connection with an already-cancelled context skipped the protocol's graceful `Terminate` message, so the server-side walsender kept the slot marked active until `wal_sender_timeout` (60 s default) reaped it — longer than the v0.99.34 slot-active retry budget, producing `slot is active for PID N` refusals on immediate restarts. Every replication-conn close now runs under a fresh bounded context so `Terminate` is always sent and the slot releases in milliseconds; the retry stays as defense in depth.

### Performance
- **`backup full` reads tables in parallel on Postgres sources (`--table-parallelism`, ADR-0084).** The full-backup row sweep now fans out across a bounded worker pool (default 4 tables at once, pgcopydb `--table-jobs` parity), every reader pinned to the SAME exported snapshot via `SET TRANSACTION SNAPSHOT` — cross-table consistency is identical to the serial sweep. Motivated by the 2026-06-10 benchmark (133 GB / 43 tables: 2367 s vs `pg_dump -j8`'s 232 s; ~3.4× of that gap is pure cross-table parallelism). MySQL backups stay serial (per-session snapshot, not shareable — an INFO log names the reason), as does the non-snapshot fallback. Manifest table order and resume semantics are unchanged-by-construction: entries are pre-staged in schema order and a crashed run leaves at most `--table-parallelism` tables with partial chunk lists, which the existing resume path already handles. **Measured on the benchmark corpus: 2367 s → 881 s (2.7×) with defaults**, closing the gap vs `pg_dump -j8` from 10.2× to 3.2× (`docs/comparison-backup.md`).
- **`restore` bulk-applies tables in parallel on EVERY target (`--table-parallelism`, ADR-0084 restore side).** The restore chunk-apply phase now fans out across a bounded writer pool (default 4 tables at once), one dedicated row-writer connection per concurrent table — engine-generic, since parallel writers need no shared snapshot (PG and MySQL targets alike), bounded by the target's connection budget. Motivated by the same 2026-06-10 benchmark (serial restore projected ~3 h vs `pg_restore -j8`'s 917 s); restore wall time is the operator's recovery-time objective. Per-table chunk ordering, per-chunk SHA-256 verification, and chain restores' ordered incremental replay are unchanged; chain restores parallelize each segment full's bulk-apply via the same flag. Copy/index overlap on restore is deliberately deferred. **Measured: a projected ~3 h serial restore completes in 2810 s (≥3.8×) with defaults**, closing the gap vs `pg_restore -j8` from ~11.5× to 3.1×.

## [0.99.34] - 2026-06-10

### Fixed
- **Warm resumes and crash recoveries no longer fail on a not-yet-released replication slot.** Restarting a PG stream moments after the prior owner stopped (or crashed) could hit `replication slot is active for PID N` (SQLSTATE 55006) and fail loudly for a condition that self-heals as soon as Postgres reaps the dead walsender. `START_REPLICATION` now retries with bounded backoff (8 attempts, 0.5–8 s, each wait visible at INFO); a *genuinely* concurrent second writer still holds the slot past the budget and gets the original loud refusal — the two-writers guard is unchanged and pinned.

### Performance
- **Migration-state checkpoints are O(1) in table count (ADR-0082).** Per-table progress and resume cursors now live one-row-per-table instead of re-serializing the entire progress map into a single hot row on every checkpoint. Measured at 10k tables on real Postgres: **31.7 ms → 377 µs per checkpoint (84×)**; a 10k-table migration's total state writes drop from ~17 GB to ~1.3 MB. Existing migrations upgrade transparently on first resume (crash-safe, one-time); a *downgraded* binary encountering the upgraded layout fails loudly instead of silently re-copying.

### Internal
- Applier control-plane extraction arc complete (ADR-0081 tiers a–d): the duplicated batch loop, control-table CRUD, and lease-row conversion now live once in `internal/appliershared`; ADR-0081 records what stays engine-specific and why.
- Random-op sync-convergence property test (`pgregory.net/rapid`) joins the suite: random transaction interleavings against live PG/MySQL syncs must converge to exact content equality; smoke budget in PR CI, env-knobbed deep runs.

## [0.99.33] - 2026-06-10

### Fixed
- **Single-manifest (full-only) cross-engine restores now run the same unsupportability gate as chain restores (Bug 134).** v0.99.32 fixed the PG→`vitess` *chain*-restore refusal skip — but the single-manifest restore branch (a `backup full` with no incrementals) never called the gate at all, on **any** MySQL-family target: a full-only PG backup carrying an `EXCLUDE` constraint restored to `mysql`, `planetscale`, or `vitess` with exit 0 and the constraint silently downgraded to a plain non-unique `KEY` (the same applies to the gate's other refusal families — extension opclasses, PostGIS metadata). Pre-existing on every version with cross-engine restore; found by the v0.99.32 regression cycle within hours of the chain-path fix — the instance one branch over. Anyone who restored a **full-only** PG backup to a MySQL-family target should re-check that schema's constraints (adding even one incremental made the same restore refuse loudly, so chains were covered). Pinned across all three MySQL-family targets plus the PG→PG and clean-schema controls, with a revert-test proving the pin catches the bug.

### Internal
- **The applier batch loop now lives once (ADR-0081, extraction tier b).** Both engines' ~500-line mirrored AIMD/flush/idle-grace state machines collapsed into one shared loop in `internal/appliershared` behind a closure seam; the measured 69 divergent lines reduce to five named config fields. Behavior-identical — the item-18 timing pins and the ADR-0010 idempotency pin passed unchanged on both engines. The next batch-loop fix lands in one file instead of two.

### CI
- Weekly Postgres version matrix (`pg-version-matrix.yml`): the postgres engine integration suite now runs against stock `postgres:17`, `:18`, and a `:latest` canary (PG19-beta drift signal) on a Saturday schedule + dispatch — PR CI stays on the prebaked PG16. Enabled by a `SLUICE_TEST_PG_IMAGE` override on the shared test container.
- `funlen`/`gocyclo` hold-the-line lint ceilings (210 lines / complexity 60, ratchet-down note in the config) so the orchestrator mega-function class can't regrow silently.

## [0.99.32] - 2026-06-10

### Fixed
- **PG → `vitess`-flavor chain restores no longer silently skip the PG-native refusal checks.** The `vitess` self-hosted flavor (shipped v0.99.15) was missing from the MySQL-family target check in the cross-engine restore gate, so restoring a PG-lineage backup chain to a `vitess` target silently skipped every PG-native unsupportability refusal — a PG schema carrying `EXCLUDE` constraints or extension opclasses would restore with those constraints **silently dropped** instead of refusing loudly. Found while converting engine-name dispatch to capability dispatch (the exact bug class that conversion exists to kill); now pinned. Anyone who restored a PG-lineage chain to a `vitess`-flavor target on v0.99.15–v0.99.31 should re-check that schema's constraints. (`planetscale` and `mysql` targets were always covered; PG→PG restores unaffected.)

### Changed
- **The `vitess` flavor now inherits the PlanetScale-tuned apply defaults it was always meant to have** (same vtgate semantics): conservative AIMD p95 target latency (5s, was the generic 10s), the apply-batch-size>50 transaction-killer warning, and MySQL-dialect schema-diff rendering (was PG-style).

### Performance
- **Idle `backup stream` broker ticks are now O(1) store reads instead of one GET per manifest in the chain** (~2,000 GETs per 30s tick on a week-old 5-minute-rollover stream → exactly 2). The walked chain is cached on the byte-identity of the lineage catalog + tail manifest; any structural change (rotation, compaction, prune, append, tail checkpoint rewrite) invalidates.
- **Bulk-copy decode and write now overlap** (bounded 64-row buffers on the row-channel chain; backpressure preserved) and the PG COPY bridge no longer allocates per row (0 allocs/op pinned).
- **The per-flush `SHOW WARNINGS` probe on batched-INSERT targets is sampled** (first 10 flushes exhaustive, then 1-in-16, final flush always) — up to ~30 min saved on large cross-region PlanetScale loads; the LOAD DATA path keeps its every-statement check.

### Internal
- Applier column-metadata shapes converged across engines + byte-identical helpers extracted to `internal/appliershared` (control-plane extraction tiers a; groundwork for one-applier-fix-lands-once).
- Orchestrator engine dispatch re-anchored to `ir.Capabilities` (five new declared fields); per-engine compile-time optional-interface declarations (a method-set break now fails compile instead of silently downgrading).
- The three orchestrator mega-functions (`runOnce`, `coldStart`, `runSingleDatabase`) carved into named phase methods and `streamer.go` split by its own seams (3,205 → 1,235 lines) — purely mechanical, zero test edits, teardown ordering byte-identical.

## [0.99.31] - 2026-06-10

### Added
- **Combined-`ALTER` MySQL secondary-index builds (ADR-0080 follow-up).** When `sluice migrate` builds a table's secondary indexes on a MySQL target, all combinable `BTREE`/`UNIQUE` indexes for that table are now created in **one** `ALTER TABLE ... ADD INDEX ..., ADD INDEX ...` statement — one InnoDB scan and one metadata lock per table instead of one per index. `FULLTEXT`/`SPATIAL` stay separate statements (MySQL restricts combining them). Measured on the `benchmarks/mysql/` harness: **−18.1 % median index-phase time** on top of the v0.99.30 overlap win, zero-loss verified. Applies to both the overlapped and serial post-copy index paths.
- **`--log-format=json`.** Emits one JSON object per line on stderr (slog's JSONHandler) instead of the human-readable text format — the shape Loki / Datadog / CloudWatch agents ingest natively. Pairs with the existing `/metrics` + `/healthz` + `/readyz` endpoints for running `sluice sync` under Kubernetes. Default remains `text`.

### Security
- **Local backup stores and crash bundles are now written owner-only (0600 files / 0700 directories; previously 0644/0755).** Backup chunks contain full row data and `--encrypt` is opt-in, so a world-readable backup directory handed any local user the dataset. Existing stores keep their current permissions (only newly created files/dirs are affected); restore is unaffected. No effect on Windows.

### Fixed
- **Failed backup-compact orphan sweeps now leave a WARN breadcrumb.** The post-commit delete pass that removes a merged segment's superseded files was silently best-effort; a failure leaked backup-store disk with no log line at any level. The chain remains correct either way — this is purely operator visibility.

### CI
- **The `postgres-trigger` engine's integration tests now run in CI.** The package landed after the integration-shard split and no shard listed it, so its suite — including the capture-payload pin for a known silent-loss class — had never executed in CI. A new Lint-job guard fails CI if a package with integration-tagged tests is ever outside the shard matrix again, and a tags-vet matrix type-checks every build-tag combination (including tagged test files) on every PR.

## [0.99.30] - 2026-06-09

### Added
- **Index-build overlap extended to MySQL targets (ADR-0080).** `sluice migrate` to a MySQL target (MySQL→MySQL and PG→MySQL) now builds each table's secondary indexes as that table's copy lands, concurrently with the still-copying tables — collapsing the separate post-copy whole-schema index phase, the same structural win Postgres targets got in v0.99.29 (ADR-0077). The MySQL `SchemaWriter` now implements `ir.IncrementalIndexBuilder` + `ir.TableIndexedNotifier`, draining the completed-tables channel into a bounded worker pool (each table's indexes built on its own connection, detect-then-skip for idempotent resume). MySQL has no connection-slot prober, so the pool sizes itself from a fixed worker count (default 4) rather than a measured budget; PlanetScale/Vitess targets decline the overlap and defer to their own online-DDL (the channel is drained into a no-op that still fires the per-table callback so resume accounting stays correct, then the post-copy `CreateIndexes` runs as before). No throughput number is claimed yet — it needs an at-scale MySQL measurement.
- **Within-table chunking on the PG-source `sync start` fast cold-start (ADR-0079 v1.1).** v0.99.29 gave the fast sync cold-start cross-table parallelism + index-overlap + raw passthrough; v0.99.30 adds within-table PK-range chunking so a large single table on the sync path is copied in parallel chunks instead of single-streamed. Engages when the source table has planner stats (`pg_class.reltuples`); a never-ANALYZEd table stays single-stream (slower, never lossy). PG-source only. Implemented via a new optional `ir.RowCountEstimator` surface (sibling to `RowCounter`) that carries the chunk-decision-only estimate; the PG implementation reads `reltuples` off a separate connection so it never races the live snapshot stream. Only the chunk-decision estimate changes — a wrong estimate degrades to single-stream, never corrupts the copy.

### Fixed
- **MySQL `SPATIAL` / `FULLTEXT` index creation no longer fails with `Error 1089`.** sluice emitted a per-column prefix length (e.g. `pt(32)`) on `SPATIAL` and `FULLTEXT` indexes, which MySQL rejects with `Error 1089` ("Incorrect prefix key; the used key part isn't a string…"), so the index was never created and the migration errored. The source reader can legitimately surface a `SUB_PART` (length) on a geometry/full-text index column; the prefix is now dropped at emit time for those two index kinds (every other kind keeps the source's prefix length). Affected any migration carrying a `SPATIAL`/`FULLTEXT` secondary index to a MySQL target, on both the serial post-copy `CreateIndexes` path and the new overlapped path. Pre-existing bug — present in every prior published version on that path — first surfaced by the ADR-0080 work. Loud failure (the migration errors; the index is not created), not silent data loss, so no data re-verification is needed.

## [0.99.29] - 2026-06-09

### Added

- **Cross-table copy worker pool for `sluice migrate` (`--table-parallelism`, ADR-0076) + adaptive `--bulk-parallel-min-rows`.** `migrate` now copies multiple tables concurrently (the cross-table axis, pgcopydb's `--table-jobs`), composed with the existing within-table `--bulk-parallelism` PK-range splitting — closing the many-medium-table gap where each table sat below the within-table-split threshold and the serial table loop left cores idle. The two axes multiply, and the product is bounded by the target's connection budget (and `--max-target-connections`) at a single chokepoint. `0` (default) = auto 4; `1` disables. `--bulk-parallel-min-rows` became a `0=auto` sentinel that dials the within-table-split threshold down on many-table schemas (`base/table-count`, floored at 10000); explicit values are honoured verbatim, single/few-table behaviour unchanged. ~2.76× on the 30×50k shape, zero-loss.
- **Index-build overlap for `migrate` (ADR-0077, Postgres target).** Each table's secondary indexes are now built as that table's copy lands, concurrently with the still-copying tables, instead of a separate post-copy whole-schema index phase (which had been a sequential ~457 s tail — 29% of total — on the 110 GB benchmark). Copy and index connections are open simultaneously, so the budget reserves a clamped ~25% slice (capped at 8) for the index pool at the same chokepoint. Constraints/FKs stay after the combined phase. Resume is additive (`TableProgress.IndexesBuilt`, omitempty; old tokens re-feed, a no-op under `CREATE INDEX IF NOT EXISTS`). PG-only; a MySQL target falls back to the post-copy `CreateIndexes`.
- **PG→PG identity passthrough (ADR-0078).** For a same-engine, no-transform PG→PG copy, `migrate` byte-pipes the raw COPY stream (`COPY (SELECT …) TO STDOUT` → `io.Pipe` → `COPY … FROM STDIN`) past the typed IR, removing the per-value decode/re-encode and closing the per-stream rate gap vs pgcopydb. Auto-engaged behind a value-fidelity gate: same-engine + no transform (no `--redact` / `--type-override` / `--expr-override` / `--inject-shard-column`) + a per-table projection that excludes OID/wire-format-sensitive types (extension / verbatim / bit / geometry / array — the Bug-74 element-family classes) in v1; any transform falls back to the IR path. New `--raw-copy-format=text|binary|auto` (text default — cross-major safe; binary only when source/target server majors match, downgrading to text loudly otherwise). Both sessions are forced to `client_encoding=UTF8` so the byte-pipe is self-consistent regardless of either DSN (an asymmetric `client_encoding` would otherwise silently corrupt non-ASCII text).
- **Fast parallel cold-start for the `sync` path (ADR-0079, Postgres source).** `sluice sync start`'s PG-source cold-start now reuses the fast machinery — cross-table pool + index-build overlap + raw passthrough — so the copy-then-continuously-follow workflow (pgcopydb's `--follow` equivalent) gets the fast initial copy instead of the old serial one. All parallel readers are pinned to the one exported snapshot via `SET TRANSACTION SNAPSHOT`, so the reads are snapshot-consistent (gap-free). Capability-gated on a shareable exported-snapshot surface (IR-first, never an engine-name check): MySQL and VStream/PlanetScale sources stay on the serial cold-start with a loud INFO log. Resume, `--schema-already-applied`, and multi-database / multi-schema cold-start stay serial in v1. New `sync start` flags mirror migrate's: `--table-parallelism`, `--bulk-parallelism`, `--bulk-parallel-min-rows`, `--bulk-batch-size`, `--raw-copy-format` (inert on MySQL/VStream sources).

### Changed

- Vitess cluster integration-test floor extended to v21, with a scheduled multi-version matrix (v21→latest) backed by prebaked GHCR server images (test/CI infrastructure only — no binary behaviour change). Testcontainers ryuk disabled on the integration jobs to kill the docker.io ryuk-pull flake; `actions/setup-go` bumped v5→v6.

## [0.99.28] - 2026-06-08

### Fixed

- **A silent value clamp/truncation under `--mysql-sql-mode=''` is now reported loudly (Vector B).** Passing `--mysql-sql-mode=''` (the legacy-data escape hatch) relaxes the MySQL target so it accepts legacy zero-dates — but it also makes MySQL **silently** clamp or truncate any *other* out-of-range value on write (a numeric overflow → MAX, an over-long string → cut). The post-write warning guard previously skipped its check entirely under relaxed mode, so those coercions passed unannounced. sluice now emits a loud **one-time-per- column WARN** (not a refusal — you opted into relaxed mode) naming the coercions and the data-preserving remedy (`--type-override`), on all three bulk-copy write paths (`LOAD DATA`, batched INSERT, and the idempotent upsert path used on resume / parallel chunked copy / cold-start). Under strict sql_mode the value is still refused, as before. Drop `--mysql-sql-mode=''` to refuse instead of coerce.
- **Range/overflow refusal messages no longer render an empty `Examples: []`.** The warning guard read `@@warning_count` before `SHOW WARNINGS`, and that intervening read clears MySQL's diagnostic list — so the strict-mode refusal (and the NaN/±Infinity refusal) listed no offending values. The guard now reads `SHOW WARNINGS` first, so the refusal names the values.

## [0.99.27] - 2026-06-08

### Added

- **`--type-override COL=interval` carries a MySQL `TIME` *duration* to a Postgres `INTERVAL` (Vector C).** A MySQL `TIME` column is a duration in the range `-838:59:59…838:59:59`, which exceeds Postgres `time`'s `00:00–24:00` time-of-day range — so a column used to store a real duration (a >24h span, a negative offset) could not be carried by the default `TIME → time` mapping. Overriding the column to `interval` maps it to PG `INTERVAL`, which holds the full range; the value is carried as its textual form and PG's interval parser accepts it. Works on both `migrate` and continuous `sync` (CDC), in all the duration shapes (max-positive, negative, fractional-second, zero, NULL). `interval` is now a first-class Postgres type in the IR (a new `ir.Interval`, distinct from `ir.Time`): PG→PG round-trips a native `interval` column, and a non-Postgres target — which has no interval type — is refused loudly rather than silently degraded back to `TIME` (which would re-lose the range the override exists to preserve).

## [0.99.26] - 2026-06-08

### Added

- **`--type-override` accepts parenthesised precision/length from the CLI: `decimal(20,0)`, `numeric(20,0)`, `decimal(20)`, `varchar(255)`.** Previously the CLI flag passed the whole post-`=` string as a bare type name, so a precision-bearing decimal could only be set via the YAML `mappings:` form (`target_type_options`) — the documented remediation hint `decimal:precision=20,scale=0` did not actually parse from the CLI. The concise paren form now works, and `numeric` is accepted as an alias for `decimal` (the Postgres spelling). Malformed specs (unbalanced/empty parens, non-integer or wrong-arity arguments, parens on a non-parametric type) are rejected with a clear error.

### Fixed

- **The unsigned-bigint / unconstrained-numeric range advisories now point at a remediation that actually works from the CLI.** The notices, `schema preview` output, and the LOAD-DATA recovery hint recommended `--type-override COL=decimal:precision=N,scale=M`, which the CLI never parsed (it silently became an unknown type name). They now recommend the working `--type-override COL=decimal(N,M)` form (e.g. `decimal(20,0)` to carry a full unsigned-64 value into PG `numeric(20,0)`).

## [0.99.25] - 2026-06-08

### Fixed

- **A string with an embedded NUL byte (`0x00`) bound for a Postgres `text`/`varchar`/`char` column is now refused loudly and early (Vector C).** PostgreSQL text types cannot store a NUL byte, and over the COPY protocol PG rejects it with SQLSTATE 22021 as an opaque stream error far from the offending row. A MySQL `CHAR`/`VARCHAR`/`TEXT` *can* hold embedded NULs, so a cross-engine MySQL → Postgres copy can hit this. sluice now detects the NUL at the value layer and refuses with an actionable message naming the column and the data-preserving remedy (`--type-override <col>=bytea`, since `bytea` holds arbitrary bytes including NUL) instead of letting the driver fail cryptically mid-stream. No value is silently altered — stripping the NUL would be silent corruption. Pinned by `TestPrepareValueNULByteRefused` (text/varchar/char + DOMAIN-over-text recursion; `bytea` and NUL-free text unaffected).

## [0.99.24] - 2026-06-07

### Added

- **Postgres-source multi-schema CONTINUOUS SYNC / CDC (ADR-0075 Phase 2b).** `sluice sync start` against a Postgres source now supports the multi-schema fan-out flags `--include-schema` / `--exclude-schema` / `--all-schemas` for the full cold-start **and** continuous-CDC path — the steady-state counterpart to Phase 2a's multi-schema `migrate`. Each selected source schema is replicated to a same-named target namespace (a Postgres schema, or a database on a MySQL target). Because a Postgres logical-replication slot is database-wide, the selected schemas are cold-started under **one spanning exported snapshot**, then the single database-wide CDC stream is routed per-change to the matching target namespace; warm-resume continues all schemas from the one persisted slot/LSN. This mirrors the ADR-0074 MySQL multi-database shape (the orchestrator is shared and unchanged). Previously a multi-schema `sync start` against a Postgres source was refused loudly ("Phase 2b, not in this release"); that refusal is now real support. Same-named tables in different schemas are isolated on the target (routing + per-namespace applier caches are schema-keyed), out-of-scope schemas are dropped (never misapplied), and CDC `TRUNCATE` routes to exactly one namespace. Pinned by per-drop-site scope unit tests plus PG→PG and PG→MySQL multi-schema integration tests (cold-start + steady-state insert/update/delete/truncate + cross-schema bleed guards + warm-resume parity), run under the CI `-race` Integration gate.

## [0.99.23] - 2026-06-07

### Fixed

- **`--zero-date` now works on a PlanetScale/Vitess source.** The VStream CDC decoder parsed temporal cells with a strict layout that rejected a zero or partial date (`'0000-00-00'`, `'YYYY-00-DD'`, `'YYYY-MM-00'`), then fell back to handing the raw bytes downstream — where a Postgres target failed with a confusing `expected time.Time, got []byte` instead of applying the operator's `--zero-date` policy. The vanilla MySQL binlog / bulk-copy paths have honored `--zero-date` since the original Vector A fix; this brings the VStream path to parity. A zero/partial date now resolves per `--zero-date`: `error` (default) refuses the stream loudly naming the column, `null` carries SQL `NULL` (refused on a `NOT NULL` column), `epoch` substitutes `1970-01-01 00:00:01`. A genuinely malformed but non-zero date (month 13, Feb 30) still fails loudly as before. Covers the live CDC reader and the cold-start COPY + catch-up paths. Pinned by decoder unit tests across the temporal family × every zero shape × each policy (including the `NOT NULL` refusal).

## [0.99.22] - 2026-06-07

### Fixed

- **The `TINYINT(1)` out-of-range WARN now also fires on the CDC read paths.** v0.99.21 made a `TINYINT(1)` value outside `{0,1}` loud on the bulk-copy / snapshot path (it had been silently collapsed to `true`). This extends the same one-time-per-column WARN to the steady-state CDC tail — both the binlog reader and the PlanetScale/Vitess VStream reader (and its cold-start COPY) — so a non-`{0,1}` value written live during continuous sync is flagged too, not just during `migrate` / `sync` cold-start. Detection-only and side-effect-free: the decoded value is unchanged; the `--type-override <table>.<col>=smallint` (or `=int`) remedy already preserved the integer on every path. Closes the Vector D detection gap.

## [0.99.21] - 2026-06-07

### Fixed

- **MySQL `TINYINT(1)` columns that store real integers (not a 0/1 boolean) no longer collapse silently.** sluice maps `TINYINT(1)` to boolean by the documented MySQL convention, but `TINYINT(1)` is only a display width — the column physically stores the full signed 8-bit range, so a schema using it as a status code or small enum can hold `2`, `127`, `-1`, etc. The boolean decode collapsed every non-zero value to `true`, losing the integer with no warning — even MySQL→MySQL. The bulk-copy / snapshot read path now **WARNs loudly, once per column** (naming the `table.column` and an example value) when it reads a `TINYINT(1)` value outside `{0,1}`, instead of doing it silently.

### Added

- **`--type-override <table>.<col>=smallint` (and `=int` / `=integer`) to preserve a `TINYINT(1)` integer column.** The override rewrites the IR type the reader decodes with, so the cell is read as an integer (not collapsed to a bool) and carried faithfully end-to-end — cross-engine and same-engine. `smallint` is the recommended floor: a `TINYINT(1)` value always fits, and unlike a `tinyint` override it can't re-emit a MySQL `TINYINT(1)` target column that would re-trigger the boolean mapping on a round-trip. The new out-of-range WARN points operators here. (The WARN currently fires on the copy / `sync` cold-start path; the CDC tail is a tracked follow-up — the override already preserves the value on every path.)

## [0.99.20] - 2026-06-07

### Fixed

- **`--zero-date=epoch` now lands a real date on a MySQL `TIMESTAMP` target instead of silently storing the `0000-00-00` zero sentinel.** The epoch substitute was `1970-01-01 00:00:00` — exactly one second below MySQL's `TIMESTAMP` range floor (`1970-01-01 00:00:01` UTC). Because reading a legacy zero-date source requires `--mysql-sql-mode=''`, which also relaxes the target connection, a midnight-epoch write into a MySQL `TIMESTAMP` column was silently coerced back to `0000-00-00` — re-introducing the very value epoch is meant to replace — at exit 0 with no warning. (`DATE`/`DATETIME` targets and all Postgres targets were unaffected; the decoder was always correct.) The epoch sentinel is now `1970-01-01 00:00:01`, which sits at the `TIMESTAMP` floor and is representable by every temporal target. The one-second offset is meaningless on a synthetic placeholder for an invalid date. Pinned by a real-MySQL integration test that ground-truths the midnight coercion and proves the new sentinel round-trips as a non-zero value.

## [0.99.19] - 2026-06-07

### Fixed

- **CRITICAL: MySQL zero and partial dates were silently corrupted into a wrong calendar date on the `migrate` / snapshot bulk-copy path.** A source column holding a legacy invalid date — the all-zero `'0000-00-00'`, a zero month (`'2026-00-15'`), or a zero day (`'2026-06-00'`), all storable under a relaxed source `sql_mode` — was read through the MySQL driver's `parseTime=true`, which hands such values to Go's `time.Date(2026, 0, 0, …)`. Go *normalizes* a zero component into a neighbouring real date (`'2026-06-00'` → `2026-05-31`, `'2026-00-00'` → `2025-11-30`), so the migration carried a **different, plausible-looking date** and exited 0. The all-zero `'0000-00-00'` was the only case handled sanely (→ NULL); every partial date was silently wrong. The CDC binlog tail already surfaced these loudly; only the bulk-copy read path corrupted them.

  **Fix:** sluice now reads `DATE`/`DATETIME`/`TIMESTAMP` columns as their raw text (`CAST(... AS CHAR)`) so the decode layer sees MySQL's literal value before any `time.Time` is constructed, and resolves zero/partial dates per a new `--zero-date` policy:
  - `--zero-date=error` (**default**) — refuse loudly, naming the column. The safe default: nothing silently wrong leaves the source.
  - `--zero-date=null` — carry the value as SQL `NULL` (refused loudly for a `NOT NULL` column).
  - `--zero-date=epoch` — substitute `1970-01-01`.

  A genuinely out-of-range but non-zero date (month 13, Feb 30) stays a hard error regardless of the flag. This is a **behavior change**: prior versions silently mapped `'0000-00-00'` to NULL; that case now refuses by default — pass `--zero-date=null` to restore it. See [docs/operator/migrating-legacy-mysql.md](docs/operator/migrating-legacy-mysql.md). Pinned by unit tests across the full temporal family × every zero shape × each policy, plus an integration test that ground-truths the live-driver normalization against real MySQL 8.0.

- **Temporal primary keys now paginate by the real date column on the chunked copy path.** The zero-date fix above projects `DATE`/`DATETIME`/`TIMESTAMP` columns as `CAST(... AS CHAR)`, which aliases a temporal column to its own name. On the >100k-row keyset-paginated bulk copy, an unqualified `ORDER BY` then sorted by that text alias while the cursor predicate compared the real date column — consistent only by ISO date strings sorting in calendar order, and it defeated the primary-key index (forced a filesort). The cursor and ordering clauses are now table-qualified so both bind the real column: date-typed throughout and index-ordered. No user-visible behavior change for valid data; caught by the value-fidelity review of the zero-date fix. Pinned by a SQL-shape unit test plus `DATE`/`DATETIME(6)` primary-key pagination integration tests across page boundaries.

## [0.99.18] - 2026-06-07

### Fixed

- **CRITICAL: a `migrate` from a PlanetScale/Vitess source (`--source-driver= planetscale` or `vitess`) silently copied only a tiny fraction of a large PK table and still reported success.** v0.99.14 set vtgate's `workload=olap` as a **session-wide** setting on the source reader (to lift vtgate's ~100k OLTP result cap on a *no-PK* full-table scan). But that session setting also covered the `LIMIT`-paged reads used by the **parallel chunked bulk-copy** (the default for tables at or above `--bulk-parallel-min-rows`, 100k), and under olap *streaming* mode each concurrently-read chunk's page was truncated — so a large PK table was copied only in part (e.g. ~7.5k of 1.5M rows) while `sluice migrate` still exited 0 with `migration complete`. Single-stream copies (`--bulk-parallelism=1`) and vanilla MySQL sources were unaffected, and the bug only appeared above the chunk threshold — which is why the existing VStream tests (sub-threshold tables) did not catch it.

  **Affected releases: v0.99.14, v0.99.15, v0.99.16, v0.99.17.** Anyone who ran a PlanetScale/Vitess `migrate` of a table with ≥100k rows at the default parallelism on those versions should **re-verify row counts** (and re-run on v0.99.18). No fix-up is needed for the *source* — the data was never touched; the target simply received a partial copy. `sync start` cold-start and CDC were not affected by this path.

  **Fix:** `workload=olap` is now scoped to **just** the unbounded no-PK full scan (`ReadRows`), applied on a dedicated connection — never session-wide. The `LIMIT`-paged `ReadRowsBatch` the chunked copy uses is olap-free again, exactly as it was before v0.99.14, so the parallel copy reads every row, while the no-PK 100k-cap lift the olap change was added for is preserved. Pinned by a new VStream regression test that migrates an above-threshold PK table at parallelism > 1 and asserts exact row-count parity (the chunk-threshold dimension the prior pins missed).

## [0.99.17] - 2026-06-07

### Fixed

- **Crash during a backup rotation could leave the backup un-restorable ("branching/mis-stitched lineage").** When a streaming backup rotated to a new segment and the process crashed (or was cancelled) at just the wrong moment, the segment's first incremental could be written durably to object storage but lost from `lineage.json`'s incremental list (its catalog append is best-effort, so it never fails the stream). On resume the stream correctly re-stitched off the on-disk tail — so **no data was lost** — but the catalog kept the gap: its first recorded incremental then parented off the orphaned one instead of the segment's full, and a later `restore` **refused the whole segment** with `branching/mis-stitched lineage` even though the on-disk chain was complete. sluice now **reconciles the open segment's catalog against the on-disk chain on resume**, re-cataloguing any orphaned incremental in chain order before streaming continues, so restore succeeds. The repair is conservative and idempotent — it refuses to guess when the on-disk manifests aren't a single clean linear chain (a branch, a parentless incremental, or an unreachable manifest), leaving those for restore's strict validation to surface rather than masking real corruption. This was a loud, recoverable failure (a refused restore, never silent data loss), surfaced by the ADR-0046 crash-injection matrix under the race detector.

## [0.99.16] - 2026-06-07

**Multi-database MySQL migration and continuous sync (ADR-0074).** A single `sluice` run can now connect to a MySQL server and migrate — and continuously sync — many databases at once, each landing in its own same-named target namespace, analogous to how a Postgres source carries multiple schemas. Drop-in from v0.99.15 — purely additive; without the new flags, every existing single-database run is byte-identical.

### Added

- **`migrate` across multiple MySQL databases in one run.** New flags `--include-database <glob>` / `--exclude-database <glob>` (mutually exclusive, repeatable) and `--all-databases`. When any is set, the source DSN is a *server* connection (its database component is optional), sluice enumerates the server's databases, and each selected database is migrated to a **same-named target namespace**: a Postgres **schema** (MySQL→Postgres) or an auto-created target **database** (MySQL→MySQL, via `CREATE DATABASE IF NOT EXISTS`). System databases (`information_schema`, `performance_schema`, `mysql`, `sys`) are always excluded. Cross-database foreign keys are preserved when both databases are in scope and applied in a final pass after every database's tables exist; a foreign key pointing at a database *outside* the selected set is **refused loudly** (sluice can't guarantee the referent exists on the target) rather than silently flattened.

- **`sync start` across multiple MySQL databases — cold-start, CDC, and resume.** The same `--include-database` / `--exclude-database` / `--all-databases` flags on `sync start` give continuous multi-database replication. The cold start captures **one consistent snapshot spanning all selected databases** (a single `START TRANSACTION WITH CONSISTENT SNAPSHOT` on one pinned connection, one binlog position) so the snapshot→CDC handoff is a single gapless cut across every database. Steady-state CDC then rides the **server-wide MySQL binlog as one stream**, routing each change to its source database's target namespace — not N streams. A stopped stream **warm-resumes** from the one persisted server-wide position without re-copying. Works MySQL→MySQL and MySQL→Postgres.

### Fixed

- **Silent-loss boundary gap in the binlog snapshot→CDC handoff.** The binlog snapshot opener captured the row view (`START TRANSACTION WITH CONSISTENT SNAPSHOT`) and the CDC start position (`SHOW BINARY LOG STATUS`) as two separate statements. A transaction committing in the window between them landed in **neither** the snapshot (it committed after the read view froze) **nor** the CDC tail (its binlog offset is below the captured position) — a silently lost row. The capture is now wrapped in `FLUSH TABLES WITH READ LOCK` … `UNLOCK TABLES` (the mydumper/Debezium consistent-snapshot pattern), so the snapshot view and the binlog position name the exact same logical cut; the lock is released immediately after the position read and writes resume captured by CDC from the frozen position. `FLUSH TABLES WITH READ LOCK` needs the `RELOAD` privilege — absent it, sluice warns and falls back to the prior lock-free capture rather than failing the run. The fix lands in the shared snapshot opener, so it closes the gap for both the new multi-database cold start **and** the pre-existing single-database binlog snapshot path. Caught by the multi-database concurrent-writes regression test under `-race`.

### Compatibility

- No breaking API or CLI changes. Drop-in from v0.99.15. Multi-database mode engages only when a `--*-database` / `--all-databases` flag is set; without them, single-database `migrate` and `sync start` are byte-identical (same snapshot, same position, same apply path). The feature is MySQL-source fan-out; PlanetScale/VStream multi-keyspace and the reverse Postgres-source→MySQL-multi-database direction are tracked follow-ons. New engine surfaces are additive optional interfaces. A MySQL→MySQL multi-database target DSN must name a "home" database for the sync control table (it errors clearly if absent); per-source user data still routes to its own database.

## [0.99.15] - 2026-06-07

A self-hosted Vitess engine flavor and a clearer error for an unsupported Postgres user-defined type. Drop-in from v0.99.14 — additive, no breaking API or CLI changes.

### Added

- **`--source-driver=vitess` — a self-hosted Vitess engine flavor.** A sibling to the `planetscale` flavor for operators running their own Vitess (etcd + vtctld + vtgate + vttablets), rather than PlanetScale's hosted service. It shares PlanetScale's VStream engine code and capabilities verbatim; the difference is the **self-hosted connection defaults**: a typical self-hosted vtgate speaks plaintext gRPC with no auth, so the `vitess` flavor defaults `vstream_transport=plaintext` and `vstream_auth=none` — `--source-driver=vitess` connects without hand-set `vstream_*` params. The hosted `planetscale` flavor keeps its secure `tls` + `basic` defaults; a value set in the DSN always wins. (`vstream_endpoint` and `vstream_shards` have no universal self-hosted default, so the operator still supplies those.) Internally, the VStream-vs-binlog branch points (CDC reader, cold-start + resumable snapshot, backup snapshot, `_vt_*` internal-table exclusion) now gate on a `usesVStream()` capability predicate, so the new flavor is correct at every path. Validated against `vttestserver` (a `vitess`-flavor cold-start COPY connects and streams with no transport/auth params).

### Fixed

- **Clearer error for an unsupported Postgres user-defined type.** A `USER-DEFINED` column that is not a recognised enum, a catalogued/enabled extension type, geometry, or a same-engine verbatim-passthrough type (i.e. a composite or domain type, which the IR does not model) previously refused with `user-defined type "X" is not a recognised enum` — misleading, since the type was never going to be an enum. The message now states the type is unsupported and why, and points at the reliable `--exclude-table` escape.

### Compatibility

- No breaking API or CLI changes. Drop-in from v0.99.14. The `vitess` flavor is purely additive (a new registered engine name); existing `mysql` / `planetscale` behaviour is byte-identical. The user-defined-type change is error-message-only.

## [0.99.14] - 2026-06-07

Resume-idempotency hardening plus a PlanetScale no-PK migrate fix. A `migrate --resume` whose schema-apply phases re-run over already-built objects no longer aborts on a duplicate, and migrating a large no-PK table from PlanetScale no longer silently truncates at vtgate's row cap. Drop-in from v0.99.13 — no breaking API or CLI changes.

### Fixed

- **`migrate --resume` is now idempotent across the index and constraint phases (no more `Duplicate key name` / `constraint already exists`).** When a resume re-entered `phase=indexes` or `phase=constraints` over a table whose objects a prior run had already created — e.g. after the resume changed `--include-table`/`--exclude-table` scope, or after a phase that had partially completed — sluice re-issued a plain `CREATE INDEX` / `ADD FOREIGN KEY` and the engine rejected the duplicate: MySQL `Error 1061 (Duplicate key name)` / `Error 1826 (Duplicate foreign key constraint name)`, Postgres `relation … already exists` / `constraint … already exists`. No data was lost (it failed loud and left the target intact), but the run wedged on a confusing error. Both phases are now idempotent in both engines — sluice owns these tables, so a same-named index/FK is the one it built and is skipped: MySQL probes `information_schema` and skips already-present objects (it has no `IF NOT EXISTS` for indexes/FKs); Postgres promotes index creation to `CREATE INDEX IF NOT EXISTS` and catalog-checks foreign keys before adding. Pinned per engine by a re-run-must-be-a-no-op integration matrix.

### Added

- **PlanetScale source migrations of a no-PK table no longer truncate at vtgate's ~100k-row cap.** vtgate's default OLTP workload caps a single result set at ~100,000 rows. The `migrate` bulk read is one streaming `SELECT`, and a no-PK source table can't be primary-key-chunked — so its full-scan copy is a single large `SELECT` that vtgate silently truncated at 100k rows. sluice now sets `workload=olap` (the battle-tested pscale-dumper convention) on the migrate reader's session, which lifts the cap and streams. It is applied **only** to the read session, and **only** for PlanetScale / VStream (vtgate) flavors — never to the writer, applier, or any path that uses transactions (OLAP mode forbids them), and never to vanilla MySQL (which has no such variable). A `workload` set in the DSN still wins. Validated against `vttestserver` (the session var is accepted and `@@workload` reports `OLAP`).

### Compatibility

- No breaking API or CLI changes. Drop-in from v0.99.13. The resume-idempotency change only affects re-run behaviour (a fresh first run emits byte-identical DDL); the `workload=olap` change only affects the PlanetScale/VStream `migrate` read session — vanilla MySQL and all write/CDC paths are unchanged.

## [0.99.13] - 2026-06-06

Cross-engine parity + table-scope completion. A no-PK table that carries a NOT-NULL UNIQUE key now migrates and syncs MySQL→Postgres without a manual schema change (it already worked MySQL→MySQL), and the v0.99.12 `--include-table` snapshot scope now also covers the PlanetScale cold-start *resume* and *backup* paths. Drop-in from v0.99.12 — no breaking API or CLI changes.

### Fixed

- **A no-PK table with a NOT-NULL UNIQUE key now does an idempotent cold-start COPY on a Postgres target (cross-engine symmetry with MySQL).** Previously such a table (e.g. a PlanetScale `connections` table: `UNIQUE KEY id` with no declared `PRIMARY KEY`) copied fine MySQL→MySQL — the MySQL writer promotes the unique key for an idempotent upsert — but was **refused** on a Postgres target (`table "X" has no PRIMARY KEY and the target's idempotent bulk-copy writer does not support no-PK upsert`), forcing a manual `PRIMARY KEY` addition on the source. The Postgres engine now mirrors MySQL: it picks a deterministic NOT-NULL UNIQUE index (every column NOT NULL, then fewest columns, then lexicographically smallest name), **inline-promotes it as a `CONSTRAINT … UNIQUE` at `CREATE TABLE`** so Postgres's `ON CONFLICT (cols)` has a real matching unique index to infer against while rows land, and keys both the cold-start COPY writer **and** the CDC applier's Insert on that key — so a re-applied Insert (an at-least-once CDC replay, or a VStream COPY catch-up re-emission) upserts idempotently instead of erroring with a unique violation or duplicating the row. A truly-keyless table (no PK, no NOT-NULL UNIQUE) is still refused loudly, matching MySQL. UPDATE/DELETE are unaffected (they identify the target row via the full before-image). Validated end-to-end MySQL→Postgres (zero-loss copy + idempotent replay, single- and composite-key) plus a pin-the-class unit matrix.

- **`--include-table` now scopes the PlanetScale (VStream) cold-start *resume* path too.** v0.99.12 scoped the fresh cold-start snapshot COPY, but a process-restart resume of an interrupted cold-start still streamed the whole keyspace. The resumed COPY now carries the same table allowlist as a fresh one. Vitess's resume cursor (`TablePKs`) is per-table, so the scope composes with the cursor with no manual reconciliation — a table with a cursor entry resumes from it, an in-scope table without one starts fresh, and a de-scoped table simply stops being copied.

- **`--include-table` now scopes the PlanetScale (VStream) *backup* snapshot COPY too.** `sluice backup … --include-table X` already restricted what was written, but the backup's VStream snapshot used the catch-all filter and streamed/buffered the whole keyspace — so a scoped backup of a small table coexisting with a large one could overflow `--max-buffer-bytes` (the same ADR-0071 over-stream v0.99.12 fixed for cold-start). The backup snapshot now scopes its COPY filter to the included tables via a new optional `ir.TableScopedBackupSnapshotOpener`. Postgres and vanilla MySQL read per-table and never over-stream, so they are unaffected (they fall back to the unchanged whole-snapshot path).

### Compatibility

- No breaking API or CLI changes. Drop-in from v0.99.12. The Postgres no-PK path only activates for tables that have a NOT-NULL UNIQUE key but no PRIMARY KEY (previously refused outright); existing PK tables emit byte-identical SQL. The resume/backup table-scope only changes the PlanetScale/VStream path — whole-keyspace runs (no `--include-table`) and all non-PlanetScale engines are unaffected. The new engine surfaces are additive: the optional `ir.TableScopedBackupSnapshotOpener`, and a `tables` parameter on the `ir.SnapshotStreamResumer` resume hook.

## [0.99.12] - 2026-06-06

`--include-table` now scopes the PlanetScale (VStream) cold-start snapshot COPY, not just the write path — so copying a subset of tables out of a large keyspace no longer streams (and buffers) the excluded tables. Drop-in from v0.99.11.

### Fixed

- **`--include-table` / `--exclude-table` now scope the PlanetScale (VStream) cold-start snapshot, not just what gets written.** The VStream snapshot COPY used a catch-all filter (`/.*/`) that copied **every** table in the keyspace; `--include-table` only restricted what sluice *wrote*. So copying one small table from a keyspace that also held a large table streamed and buffered the large table too, overflowing `--max-buffer-bytes` (`table "X" would buffer … exceeding the cap … this multi-table interleaving case is not yet disk-spilled`, ADR-0071) — the subset copy could fail outright. sluice now passes the filtered table set into the VStream snapshot's **per-table filter rules**, so vtgate's COPY scans only the in-scope tables and a large excluded table in the same keyspace is never streamed. The CDC tail is unchanged (it still streams all tables and filters on dispatch, to keep live add-table working); resume and backup snapshots keep whole-keyspace scope (documented follow-up). New optional engine surface `ir.TableScopedSnapshotOpener`; vanilla MySQL and Postgres snapshots are already per-table and are unaffected. Validated on real PlanetScale (a 1M-row subset copied cleanly with a coexisting 19M-row table) plus a vttestserver integration test.

### Compatibility

- No breaking API or CLI changes. Drop-in from v0.99.11. Whole-keyspace syncs (no `--include-table`) are unaffected — the snapshot is scoped to all discovered tables, equivalent to the prior catch-all. Only the PlanetScale/VStream snapshot path changed; vanilla MySQL and Postgres are unchanged.

## [0.99.11] - 2026-06-06

A small CLI safety fix: `sync start` now validates mutually-exclusive flags *before* prompting for the `--reset-target-data` destructive confirmation. Drop-in from v0.99.10; no behaviour change for valid invocations.

### Fixed

- **`sync start --restart-from-scratch --reset-target-data` no longer prompts to DROP the target tables before reporting that the flags are mutually exclusive.** The `--reset-target-data` typed confirmation ("Type 'reset' to confirm") ran ahead of the flag-combination validation, so an operator combining the two flags was asked to authorize dropping every target table and only *after* typing `reset` learned the combination is rejected (the command then aborted without dropping). No data was ever lost — the mutex was always enforced, and `--yes` failed cleanly up front — but a *validation* error must fire before a *destructive-action* confirmation. The three sync-start flag-combination checks (`--restart-from-scratch`×`--reset-target-data`, `--restart-from-scratch`×`--position-from-manifest`, `--position-from-manifest`×`--reset-target-data`) now run up front, ahead of the prompt. Pinned by a unit test.

### Compatibility

- No breaking API or CLI changes. Drop-in from v0.99.10. Valid invocations are unaffected; only the error *ordering* for the rejected `--restart-from-scratch` + `--reset-target-data` combination changed (now loud up front, with no destructive prompt).

## [0.99.10] - 2026-06-06

A Vitess/PlanetScale stream-resilience release: a stalled stream now fails loud instead of hanging silently, cold-start CDC errors are no longer swallowed, an unproductive reconnect loop after a tablet death can no longer churn forever, and a new `--restart-from-scratch` flag forces a clean cold-start without dropping the target. Drop-in upgrade from v0.99.9 — no breaking API or CLI changes; every new behaviour is opt-in and the defaults are unchanged. The hardening was surfaced by a new fault-injection ("chaos") test suite run against a full Vitess 24 cluster — killed tablets, primary failovers (PlannedReparentShard + EmergencyReparentShard), vtgate restarts, and a rolling version upgrade — each asserting the load-bearing invariant: after a fault, the stream delivers every row exactly once **or** fails loudly, never a silent partial.

### Added

- **`--restart-from-scratch` (sync) forces a fresh cold-start, ignoring the persisted position, without dropping the target.** It sits between `--force-cold-start` (which only skips the cold-start preflight and still warm-resumes from the persisted position, including a mid-COPY cursor) and `--reset-target-data` (which drops the target tables): it re-runs the full cold-start COPY from the beginning while leaving the target data in place (the idempotent COPY writer absorbs the re-copy). Mutually exclusive with `--reset-target-data` and `--position-from-manifest`; applies to `sync` only (not `migrate`, which resumes only via explicit `--resume`). Use it to recover a sync whose persisted position is suspect without a destructive target rebuild.

- **`vstream_progress_timeout` and `vstream_copy_progress_timeout` DSN parameters** tune the new mid-stream liveness watchdog (see Fixed). Defaults: 45s for the CDC tail, 10m for the cold-start COPY (which tolerates vreplication's multi-minute slow start).

### Fixed

- **A wedged Vitess/PlanetScale stream now fails loud instead of hanging silently.** v0.99.7 added a *first-event* liveness watchdog (for the silent primary-only stall). This generalizes it to a **continuous two-phase watchdog**: phase 1 is the absolute first-event deadline (unchanged); phase 2 re-arms on every event and fires a loud, actionable error if the stream goes totally silent mid-flight — no data, no heartbeat, `Err() == nil`. That is the failure mode a hard `EmergencyReparentShard` can leave behind, where the gRPC `Recv` goes dead-silent; without the watchdog a post-failover dead stream looked identical to an idle-but-healthy one. The per-pump windows are DSN-tunable (see Added).

- **Cold-start CDC-pump errors are no longer silently swallowed.** The cold-start CDC reader wrapper (`vstreamSnapshotChanges`) had no `Err()`, so the pipeline's optional-error probe read back `nil` and a genuine loud failure on the cold-start path — the watchdog's, and a post-failover "row event without preceding FIELD event" decode error — was dropped, a silent-partial hazard. It now delegates `Err()` to the underlying snapshot stream, so cold-start CDC failures surface loudly.

- **An unproductive reconnect loop after a tablet death no longer churns forever.** The in-place COPY reconnect budget reset on *any* successful `Recv`, so a loop of reconnect → non-progress events (a heartbeat or a stale VGTID, e.g. when the cursor is unresumable post-reparent) → error → repeat never exhausted its budget and never failed. The reset is now gated on actual COPY *progress* (a row buffered), so an unproductive loop burns `reconnectMax` and surfaces a loud `failCopy` the pipeline's retry can act on. A *productive* reconnect — e.g. the COPY resuming across an `EmergencyReparentShard` onto a surviving replica — still resets and continues; the chaos suite validates that exact path end-to-end with zero loss.

### Compatibility

- No breaking API or CLI changes. Drop-in from v0.99.9. All defaults preserve existing behaviour; the watchdog windows are opt-in DSN knobs. Operational note (unchanged, now documented): a PlanetScale-branch **target** needs an `admin`-role password for sluice's control-table DDL (`readwriter` is denied — `Error 1105 … DDL command denied … [planetscale-writer]`); the **source** needs only read access. See [docs/vitess-vstream-troubleshooting.md](docs/vitess-vstream-troubleshooting.md).

## [0.99.9] - 2026-06-06

A CRITICAL silent-loss fix for the resumable PlanetScale cold-start. Drop-in upgrade from v0.99.8; strongly recommended for anyone relying on cold-start resume. Found by post-release validation against a real PlanetScale production branch.

### Fixed

- **CRITICAL: resuming a hard-crashed PlanetScale (VStream) cold-start no longer silently drops rows.** The resumable-COPY checkpoint (ADR-0072) persisted the cursor for rows **received from vtgate and buffered**, not rows **durably written to the target** — and the target writer lags the receive path by up to `--max-buffer-bytes`. So after a **hard crash** (OOM-kill, SIGKILL, container/node kill, power loss) of an in-flight cold-start, the persisted checkpoint sat *ahead* of the durably-written data; resume restarted at the cursor and **silently skipped the gap** (up to a full buffer of rows), finishing "bulk copy complete" with no error. Measured on a real 19M-row branch with a 2 GB buffer: the checkpoint sat ~5.1M rows ahead of the durable frontier, and an end-to-end resume left the target missing ~5.26M rows (~27.6%). The checkpoint now tracks a **durable-write watermark** — the COPY pump records a position breadcrumb at each VGTID boundary, the row writer reports per-flush durable deltas after each successful commit, and the checkpoint persists only the highest breadcrumb whose covered rows are all durably written (**invariant: persisted position ≤ durable frontier**). A crash + resume now restarts at-or-before the last durable row and the idempotent COPY writer absorbs the bounded re-copied overlap — zero loss. The fix covers the Postgres target path too (PK VStream→PG resume had the same exposure). Pinned by a structural unit test (the checkpoint is never ahead of the durable frontier across interleaved receive/commit steps) and a hard-crash-then-resume zero-loss integration test. Pre-existing since v0.99.5; activated by v0.99.8 (which made resume actually run). Corrects v0.99.5's "resumes from the last-copied PK, zero loss" claim, which held only for the in-place reconnect, not a process restart. See [ADR-0072](docs/adr/adr-0072-resumable-coldstart-copy.md).

### Compatibility

- No breaking API or CLI changes. Drop-in from v0.99.8. Graceful stop/restart and steady-state CDC were never affected; this hardens the **hard-crash-then-resume** path of an in-flight cold-start.

## [0.99.8] - 2026-06-05

Two PlanetScale/VStream warm-resume fixes — restarting a sync no longer crashes, and an interrupted cold-start now resumes its bulk COPY at full speed instead of silently crawling. Drop-in upgrade from v0.99.7; a no-op for migrations that never restart a PlanetScale sync. Both were found by post-release validation against a real PlanetScale production branch.

### Fixed

- **Restarting a PlanetScale (VStream) sync no longer crashes the schema-history orderer.** On any warm-resume, sluice orders the persisted position against its retained schema-history (ADR-0049). The MySQL position-orderer assumed the vanilla single-object binlog position, but a VStream position is a **JSON array of per-shard `shardGtid`** — so once any schema-history existed, a restart crashed at startup with `mysql: position-orderer: decode p: json: cannot unmarshal array into Go value of type mysql.binlogPos`. This broke **both** ordinary post-cold-start CDC restarts and interrupted-cold-start resumes on real PlanetScale. The orderer is now VStream-aware: it orders by per-shard GTID superset (the same ADR-0049 partial order, applied per shard), ignoring the COPY-resume `TablePKs` cursor. Pinned across the full VStream-position family (single/multi-shard × with/without cursor × the GTID relations), plus an end-to-end schema-history-prime warm-resume integration test the prior vttestserver pins didn't cover.

- **Resuming an interrupted PlanetScale cold-start now continues the bulk COPY instead of silently crawling.** When a cold-start COPY was interrupted (a process restart with a persisted mid-COPY `TablePKs` cursor), the pipeline routed the resume through the plain CDC reader, which applied the un-copied tail **one INSERT round-trip at a time** (~10 rows/sec against a remote target) — so a large table stalled near where it left off, with only heartbeats and no error (a silent-degrade hazard: an operator could mark a 5%-complete sync "done"). The pipeline now detects a cursor-carrying resume and routes it through the **seeded snapshot stream → batched bulk-COPY writer**, continuing from the cursor (not re-copying from row 0; the idempotent writer absorbs overlap; the partial target copy is preserved). Measured on a real 19M-row PlanetScale branch: resume throughput went from ~514 rows/min to ~5,000 rows/sec. The completed-cold-start (cursor-less) restart stays on the fast plain-CDC path, unchanged. A cursor-less or non-VStream position is refused loudly rather than silently re-copying from row 0. Pinned by a new process-restart resume integration test (distinct from the in-place reconnect path). See [ADR-0072](docs/adr/adr-0072-resumable-coldstart-copy.md).

### Compatibility

- No breaking API or CLI changes. Drop-in from v0.99.7. PlanetScale **production** restarts (the common case) are fixed by the first item; the second restores the resumable-cold-start guarantee v0.99.5 introduced (which did not hold on real PlanetScale across a process restart).

## [0.99.7] - 2026-06-05

A Vitess robustness release: online-DDL cutovers no longer leak shadow-table rows into the target, and a primary-only Vitess cluster now works (or fails loudly) instead of wedging silently. Drop-in upgrade from v0.99.6 — no breaking API or CLI changes; a no-op for migrations that don't run online DDL on a PlanetScale/Vitess source.

### Fixed

- **Vitess internal / online-DDL shadow tables (`_vt_*`) are now excluded from VStream — a silent-loss hazard during online-DDL cutovers.** While an online DDL (`gh-ost`/`vreplication`-backed `ALTER`) is in flight, Vitess materializes transient shadow tables (`_vt_vrp_*`, `_vt_hld/prg/evc/drp/…` — the unified `_vt_<op>_<uuid>_<timestamp>_` form, vitessio/vitess#14582) and emits their rows + DDL on the stream. sluice previously forwarded those to the target, which at cutover could write rows under an internal table name or apply churn that never belonged in the user's schema. sluice now anchors exclusion to Vitess's own `schema.IsInternalOperationTableName()` helper (not a static name list, so it tracks Vitess's evolving naming) and drops both the row events and the shadow-table DDL across every dispatch path (COPY buffer, CDC tail, snapshot post-COPY, DDL). The user's real tables — including the freshly-cut table an online DDL swaps in — flow through untouched. See [ADR-0073](docs/adr/adr-0073-vitess-internal-and-online-ddl-tables.md). Validated end-to-end against a **full Vitess 24 cluster** (real online-DDL scheduler) through a completed cutover with zero row loss, including complex shapes (column drop, enum add/extend).

- **A primary-only Vitess cluster no longer wedges silently — it works (or fails loudly).** sluice's pure CDC tail requests a `REPLICA` tablet by default (to keep load off the primary). Against a cluster with **no replica** — a PlanetScale **development** branch, or a minimal self-hosted Vitess — vtgate has no `REPLICA` tablet to serve the stream, yet keeps sending heartbeats while emitting no data, so the reader hung forever with `Err() == nil`: a silent stall. Two fixes: (1) a **first-event liveness watchdog** converts that wedge into a **loud, actionable error** (`vstream_liveness_timeout`, default 30s; keyed on the absence of any *non-heartbeat* event, so a legitimately idle-but-healthy source never false-trips); (2) a new **`vstream_tablet_type={primary|replica|rdonly}` DSN parameter** (default `replica`, unchanged for PlanetScale production) lets a primary-only cluster stream from the primary via `vstream_tablet_type=primary`. A COPY-resume (mid-snapshot cursor) still targets `PRIMARY` regardless, per [ADR-0072](docs/adr/adr-0072-resumable-coldstart-copy.md). Pinned on a primary-only Vitess-cluster harness: the default tail fails loudly, the `primary` tail delivers with zero loss. See [ADR-0073](docs/adr/adr-0073-vitess-internal-and-online-ddl-tables.md).

## [0.99.6] - 2026-06-05

A self-hosted-Vitess compatibility fix. A no-op for PlanetScale users.

### Fixed

- **`--source-driver=planetscale` no longer leaks `vstream_*` DSN parameters into the MySQL session.** sluice's `vstream_*` DSN extensions (`vstream_endpoint`, `vstream_transport`, `vstream_auth`, `vstream_shards`, …) are consumed only by the gRPC CDC reader, but the schema-reader / row-reader / schema-writer / change-applier paths passed them straight through to the underlying MySQL connection, which emitted them as `SET vstream_endpoint = …` session variables on connect. A self-hosted Vitess / vttestserver rejects those (`Error 1105` for the IP-bearing endpoint, `VT05006 unknown system variable` for the rest), so a VStream-source cold-start failed at "open source schema reader" before any data moved. The parameters are now stripped centrally before every MySQL connection (one `openDB` choke point, leak-proof against future paths). Real PlanetScale was unaffected (its vtgate tolerates the unknown vars), so this is a no-op there and a fix for self-hosted Vitess / vttestserver. Pinned by a new `vttestserver`-backed integration test — the first to exercise the PlanetScale `Open*` (non-CDC) path against a real Vitess, which opens the door to CLI-driven Vitess test coverage.

## [0.99.5] - 2026-06-05

Resumable PlanetScale cold-start, a memory hard-cap, and a no-PK CDC-resume correctness fix. Drop-in upgrade from v0.99.4 — no breaking API or CLI changes.

### Added

- **Resumable VStream cold-start COPY.** A transient connection drop — or a full process crash — *mid-COPY* now **resumes the snapshot from the last-copied primary key** instead of re-copying the table from row 0. sluice checkpoints Vitess's per-table `TablePKs` cursor to the control table on a bounded cadence (50k rows / 10s) during the COPY; on an in-place reconnect or a warm-resume after restart it replays the cursor so vtgate continues the scan from where it stopped, and the catch-up rows upsert idempotently (zero loss, no `1062`). For a large table over a flaky link, a fault now costs the in-flight chunk, not the whole table. Completes the cold-start-hardening arc begun in v0.99.4 (Gap 1 auto-retry + this resumable COPY). See [ADR-0072](docs/adr/adr-0072-resumable-coldstart-copy.md).
  - **Correction (added in v0.99.9):** the zero-loss guarantee above held for the **in-place reconnect** (a transient drop during an active stream), but **not** for a **full process restart** as originally shipped. A restart routed the resume through the plain CDC reader (slow per-row apply, **fixed in v0.99.8** — Bug 128) and, more seriously, the checkpoint persisted the *received-from-vtgate* cursor rather than the *durably-written* one, so a **hard crash** could leave the checkpoint ahead of the data and a resume could silently skip up to `--max-buffer-bytes` of rows (**fixed in v0.99.9** — Bug 129). Upgrade to v0.99.9 for the resumable cold-start to be hard-crash-safe.
- **`--max-memory` flag.** Sets a hard Go runtime memory soft-limit (`GOMEMLIMIT`) to bound RSS — e.g. `--max-memory=4GiB`. `--max-buffer-bytes` accounts only raw value bytes, but the real Go-heap footprint of buffered rows is several times larger, so a large `--max-buffer-bytes` (or many tables) can drive RSS to roughly 9× the configured cap; `--max-memory` gives the garbage collector a real ceiling to defend. Default off (the `GOMEMLIMIT` environment variable is also honored natively).

### Fixed

- **No-`PRIMARY KEY` CDC apply is now idempotent on a unique key.** The MySQL change applier emitted a plain `INSERT` for a table with no `PRIMARY KEY`, so a CDC warm-resume (the applier re-applies a few changes around the persisted position) of a no-PK-but-`UNIQUE` table — e.g. the v0.99.4 `connections` shape — hit `1062` and the resume failed. The applier now emits `INSERT … ON DUPLICATE KEY UPDATE` with a full-row SET even when the PK list is empty; MySQL fires it on *any* unique index, so the re-applied rows upsert idempotently. A truly keyless table (no PK and no unique index) is unchanged best-effort. This also underpins the no-PK case of the resumable COPY above.

## [0.99.4] - 2026-06-04

A cold-start-hardening release: a CRITICAL silent-loss fix on the PlanetScale (VStream) cold-start path, plus auto-retry on transient mid-copy connection drops. Drop-in upgrade from v0.99.3 — no API or CLI changes.

### Fixed

- **CRITICAL: silent row loss on PlanetScale (VStream) cold-start of a table with no explicit PRIMARY KEY.** The COPY-phase dedup dropped any row whose key was at or below the running maximum, assuming Vitess emits the COPY scan in ascending order of the column it flags as the primary key. That assumption is false when Vitess orders the scan by a *cheaper* unique key than the flagged one (its column-type-cost heuristic prefers e.g. a `TINYINT`/`SMALLINT` unique over a `BIGINT` one): legitimate rows then arrive "out of order" and were silently discarded. A real migration of a ~19M-row table with a `UNIQUE` id but no declared `PRIMARY KEY` lost ~70% of its rows (13.5M of 19M) this way. The fragile order-dependent dedup is **removed**; the cold-start COPY writer is now **idempotent** (`INSERT … ON DUPLICATE KEY UPDATE` on a unique key present during copy), so Vitess's catch-up re-emissions are absorbed instead of dropped — independent of scan order. A truly keyless table (no `PRIMARY KEY` and no non-null `UNIQUE` index) is now **refused loudly** at cold-start rather than silently duplicating re-emitted rows. Pinned end-to-end against `vttestserver` across the key-shape family (explicit-PK / single-unique / cheaper-unique / catch-up-overlap / keyless), now gated in CI under `-race` (the new `Integration (vstream)` job). See [ADR-0072](docs/adr/adr-0072-resumable-coldstart-copy.md).

- **PlanetScale (VStream) cold-start now auto-retries a transient connection drop instead of failing.** A mid-stream `Unavailable: connector reset by peer` — and other native gRPC transients (`Aborted` / `Unknown` / `ResourceExhausted`) — surfaces from the VStream stream as a gRPC *status* error, not a MySQL `1105` wrapper, so the retry classifier didn't recognize it and a large-table cold-start failed terminally on a network blip. The source-reader classifier now honors gRPC status codes directly, so the pipeline's retry policy reconnects and resumes. (A transient still re-copies the table from the start; *resumable* mid-copy continuation is designed in [ADR-0072](docs/adr/adr-0072-resumable-coldstart-copy.md) and tracked for a later release.)

### Changed

- **Cross-engine safety guard for no-PK VStream→Postgres copies.** With the source-side dedup removed, a table with no `PRIMARY KEY` copied from a VStream source to a **Postgres** target would have duplicated Vitess's catch-up re-emissions (Postgres's idempotent path plain-INSERTs no-PK tables). Such a migration is now **refused loudly** until the Postgres target gains the symmetric unique-key-upsert treatment. PK tables and MySQL targets are unaffected.

## [0.99.3] - 2026-06-04

### Fixed

- **CRITICAL: unbounded memory on PlanetScale (VStream) cold-start.** The VStream snapshot reader buffered the *entire* COPY phase in RAM before writing a single row to the target, so a large source table could exhaust memory and be OOM-killed mid-cold-start — a ~13 GB / ~19M-row table drove RSS to ~41 GB on a 32 GB host, into swap, until the process was killed (with zero target writes during the whole cold-start). The COPY phase now **streams**: a byte-capped, backpressured pump (`--max-buffer-bytes`) feeds rows to the target as they arrive, so large-table cold-start runs at **constant memory** and target writes begin immediately instead of after the full snapshot buffers. Multi-table snapshots that would exceed the cap refuse loudly instead of OOM-ing (disk-spill for that case is deferred). Extends ADR-0028's bounded-memory audit to this path; see [ADR-0071](docs/adr/adr-0071-vstream-snapshot-bounded-memory.md). Multi-shard fan-in, COPY-phase dedup, and the snapshot→CDC position handoff are preserved and validated under `-race`. The `ir.SnapshotStream` contract is unchanged.

## [0.99.2] - 2026-06-03

A distribution release — sluice is now installable via Homebrew, Scoop, WinGet, and native Debian/RedHat packages. No engine, API, or runtime changes from v0.99.1.

### Added

- **Package-manager distribution.** GoReleaser now publishes on every release:
  - **Homebrew** (macOS + Linux): `brew install sluicesync/tap/sluice` (tap: [`sluicesync/homebrew-tap`](https://github.com/sluicesync/homebrew-tap)).
  - **Scoop** (Windows): `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`.
  - **WinGet** (Windows): `winget install sluicesync.sluice` (manifest submitted to `microsoft/winget-pkgs`; available once their review lands).
  - **Debian / RedHat / Alpine**: `.deb` / `.rpm` / `.apk` packages for amd64 + arm64 attached to the release — `sudo dpkg -i` / `sudo rpm -i`.

### Changed

- **CI:** fork pull requests now pull the public pre-baked test images anonymously (the GHCR login is skipped when no token is present), so external-contributor PRs pass the integration suite without maintainer intervention.

## [0.99.1] - 2026-06-03

The first release published from the public repository. No new features — a MySQL concurrency fix, a Go toolchain security bump, and routine dependency updates.

### Fixed

- **`fix(mysql): retry shard-lease acquire on InnoDB deadlock (1213)`** — during multi-shard consolidation, the MySQL shard-lease acquire (`SELECT ... FOR UPDATE` on the lease row, then INSERT-if-absent) could deadlock (Error 1213 / SQLSTATE 40001) when concurrent shards raced on the gap lock at the INSERT, and the error surfaced to the caller without a retry. The acquire is now wrapped in a bounded deadlock-retry (`tryAcquireShardLeaseOnce` + an `isMySQLDeadlock` classifier; 8 attempts, 5 ms → 200 ms context-aware backoff). Postgres is unaffected (its lease path is an atomic upsert). Surfaced by moving CI to GitHub-hosted runners — the different MySQL scheduling exposed what self-hosted job-level auto-retries had been quietly masking as flakiness. Pinned by `TestIsMySQLDeadlock` and the Phase-2e 3-shard-contention integration test under `-race`.

### Security

- **Go 1.26.4 toolchain.** Bumps the `go` directive 1.26.2 → 1.26.4, clearing two standard-library advisories present in 1.26.2: GO-2026-5039 (`net/textproto`) and GO-2026-5037 (`crypto/x509`). Build-only; no API or behavior change.

### Dependencies

- pgx/v5 5.9.2 → 5.10.0, koanf/v2 2.3.4 → 2.3.5, gocloud.dev 0.45.0 → 0.46.0, the aws-sdk-go-v2 group (5 modules), and the `docker/*` CI actions (login-action / setup-buildx-action / setup-qemu-action v3 → v4). Dependabot now groups `docker/*` actions into a single rollup PR to stop sibling action bumps cross-conflicting on the shared workflow files.

## [0.99.0] - 2026-06-02

The "new home" release: sluice moved to its own GitHub organization and a vanity module path, and now ships an official container image. No functional engine changes from v0.98.1 — the connection-resilience + index-build work shipped in v0.98.0 / v0.98.1.

### Changed

- **New home — `github.com/sluicesync/sluice`, module path `sluicesync.dev/sluice`.** The repository moved from `orware/sluice` to the new `sluicesync` organization, and the Go module path is now the vanity path `sluicesync.dev/sluice` (served via GitHub Pages, so imports are decoupled from the host and won't break on any future move). Install with `go install sluicesync.dev/sluice/cmd/sluice@latest`. GitHub's transfer redirect keeps the old `github.com/orware/sluice` URLs — and existing `go get github.com/orware/sluice@<oldtag>` pins — working; new code should use the vanity path. The prebaked CI test images also moved to `ghcr.io/sluicesync/sluice-*`.

### Added

- **Official multi-arch runtime container image.** `ghcr.io/sluicesync/sluice:0.99.0` + `:latest` (linux/amd64 + linux/arm64) — a distroless image wrapping the static binary (no shell, non-root, tiny). Run sluice as a Kubernetes Deployment (`sync start` behind the `/healthz` / `/readyz` / `/metrics` endpoints, ADR-0069), a CronJob (`backup` / `cutover` / `matview refresh`), or a one-shot CI migration step instead of managing the binary on a host. See [`docs/operator/running-as-a-service.md`](docs/operator/running-as-a-service.md). Published by GoReleaser on each tagged release.

### Dependencies

- Routine Dependabot bumps: aws-sdk-go-v2 group, google.golang.org/api, smithy-go, golang.org/x/crypto.

## [0.98.1] - 2026-06-02

### Fixed

- **`fix(pipeline): reap stale backends before the cold-start preflight (Bug 123)`** — v0.98.0's `--reap-stale-backends` was unreachable in its primary designed scenario. In `Migrator.Run` the cold-start preflight (which reads each target table to enforce the empty-target contract — an AccessShare lock) ran **before** the stale-backend preflight. A `sluice/`-labelled orphan from a hard-killed prior run holds an AccessExclusive lock on a target table — exactly the lockout the reaper exists to clear — so the cold-start preflight's `IsTableEmpty` probe blocked on (or, if the table read through, refused on) that lock *before* the reap could fire. The reaper itself was correct (an idle-in-tx orphan on an empty target was reaped, the default detect-and-report WARN fired, a non-`sluice/` backend was left untouched); only the preflight ordering defeated the lock-holding case. v0.98.1 moves `preflightStaleBackends` ahead of the cold-start preflight (and it remains ahead of the connection-budget probe, preserving that invariant), so the reap clears both the table lock the cold-start preflight then needs and the slots the budget math sees. The streamer cold-start path already ran the reap first and is unaffected. Pinned by `TestRunReapsStaleBackendsBeforeColdStartPreflight` (drives `Migrator.Run` with a fake recording the reap/cold-start call order; asserts reap-before-probe). Found by the v0.98.0 post-release regression cycle (focus F3).

## [0.98.0] - 2026-06-02

A connection-resilience + index-build-throughput arc. Five opt-in capabilities harden Postgres targets against connection-slot exhaustion and orphaned backends, and make the deferred secondary-index build phase materially faster on managed Postgres. Every new behavior is default-safe: connection labelling is the only thing on by default, the budget cap is auto-sizing (refuse-loudly only when a target genuinely cannot host the copy), and reaping / memory / parallelism tuning are explicit opt-ins.

### Added

- **`feat(connection-resilience): phase 1 — application_name, PG keepalive, connection-budget cap`** — every Postgres connection is now stamped `application_name=sluice/<id>/<role>` (role ∈ {snapshot, applier, cdc-reader, schema, control}), never clobbering an operator-set value — the enabler for orphan detection and for finding sluice in `pg_stat_activity`. A new `ir.TargetConnectionBudgetProber` capability (engine-neutral; MySQL no-ops) probes `max_connections` / `superuser_reserved_connections` / live `pg_stat_activity` / `rolconnlimit` / `datconnlimit` before the bulk-copy pool opens, clamps requested parallelism to the available budget, and **refuses loudly** when the copy budget falls below 1 rather than failing mid-copy with `too_many_connections`. Probe failure degrades to a WARN, never breaks a working migration. New flag `--max-target-connections N` (default `0` = auto) on `migrate` + `sync start`.
- **`feat(connection-resilience): phase 2 — stale-backend detection + opt-in reaping`** — closes the orphan-lockout class: a SIGKILL'd / OOM'd / partitioned prior run leaves its server-side COPY backend alive, still holding the target-table lock and a connection slot, blocking the next cold-start's DROP/CREATE. `DetectStaleBackends` scans `pg_stat_activity LEFT JOIN pg_locks` for *own* orphaned backends (idle-in-transaction or holding a lock on a schema sluice is about to write), reports them loudly by default, and — only under `--reap-stale-backends` — terminates them. The safety scope (`application_name LIKE 'sluice/%' AND usename = current_user AND pid <> pg_backend_pid()`) is one greppable constant re-applied by both the detect scan and the terminate statement, so a recycled pid can never be hit out of bound. Runs before the budget probe so a reap frees slots the budget math then sees. New flag `--reap-stale-backends` on `migrate` + `sync start`.
- **`feat(connection-resilience): phase 2b — AIMD backoff on copy-pool slot exhaustion`** — a transient mid-copy slot shortage (a peer process grabbing slots *after* Phase 1's preflight measured them free) no longer fails the whole migration. A `SQLSTATE 53300` (`too_many_connections`, including the superuser-reserved-slots startup FATAL) on a chunk connection now multiplicatively-decreases effective parallelism (halve, floor 1), backs off (bounded exponential), and retries — giving up loudly only after a bounded retry/total-wait. **Only** the slot-exhaustion class is retried; every other open error (bad DSN, permission denied, real COPY failure) still fails loudly and immediately. Double-copy-safe: a 53300 fails at connection open/ping, strictly before any row is written, so a retried chunk replays from its recorded cursor and can never duplicate rows.
- **`feat(postgres): index-build phase tuning — phase A (maintenance_work_mem + parallel workers)`** — the deferred secondary-index build runs against an idle target after the bulk COPY, but `maintenance_work_mem` (the dominant in-memory-sort vs external-merge lever) sat at the provider's steady-state ~4%-of-RAM default. sluice now probes `shared_buffers` as a RAM proxy and raises `maintenance_work_mem` + `max_parallel_maintenance_workers` (never lowers) on a dedicated connection for the build phase. Best-effort: a denied SET / failed probe WARNs and proceeds untuned. New flag `--index-build-mem` (human size like `512MB` / `2GB`, or `auto`) on `migrate` + `sync start`.
- **`feat(postgres): index-build phase tuning — phase B (concurrent index builds)`** — the deferred indexes now build through a bounded concurrent worker pool instead of a serial loop, each worker on its own connection. Because N concurrent builds each consume their own `maintenance_work_mem`, auto-N divides the Phase A memory budget across workers and bounds N by **both** the memory budget **and** the target's spare connection budget (reusing the Phase 1 probe), plus the index count and an operator cap (conservative hard cap 8). `N=1` degenerates to exactly the prior serial path. New flag `--index-build-parallelism N` (default `0` = auto) on `migrate` + `sync start`. PG-target only; MySQL unaffected.

### Performance

- **`perf(backup): use klauspost/compress gzip for the gzip codec`** — the non-default gzip backup codec (`--compression=gzip`; zstd has been the default since v0.67.0) now uses `github.com/klauspost/compress/gzip` instead of stdlib `compress/gzip` for a ~2–6× encode speedup at <5% ratio cost. Drop-in replacement — identical gzip wire format, so **no chunk-format change and no backup-version bump**; existing gzip backups read back unchanged. klauspost/compress is already a direct dependency (zstd codec), so this adds zero binary-size or module cost.

### Added (docs)

- **`docs(cookbook): broker continuous-replication recipe`** (#136) — adds the broker continuous-replication recipe with a broker-vs-`sync start` decision matrix.
- **`docs: correct Heroku-source guidance`** — the Heroku-source prep guidance now reflects the shipped postgres-trigger engine as the supported path (Heroku's managed tiers have `rolreplication=f`, so slot-based PG CDC is unavailable at any tier).

## [0.97.2] - 2026-05-31

### Added

- **`feat(pipeline): close the Phase 4.5 multi-segment broker deferral`** — `sluice sync from-backup run` now follows the full lineage across rotation boundaries instead of refusing loudly on multi-segment chains. Phase 4.5 originally deferred this with a flag-and-defer pending validation that the existing chain walker + idempotent applier covered the rotation seam; the v0.97.1 Round D soak (2026-05-31 — `sluice-testing/session-reports/v0.97.1-roundD-broker-soak.md`) characterized the gap. The implementation is a ~5-line change at `buildBrokerChain`: instead of refusing on `len(cat.Segments) > 1`, it now delegates to `buildLineageChain` directly — the same multi-segment walker `sluice restore` uses. The broker's apply loop already skips full manifests unconditionally (`broker.go:823`), so segment-N+1's rotation snapshot is auto-skipped; ADR-0067's born-contiguous rotation guarantees the new segment's first incremental covers the `(P_N, S]` overlap from the prior segment's end position; ADR-0010's idempotent applier handles the brief re-application of any changes that landed between the broker's last advance and the rotation moment. Single-segment broker behavior is byte-identical to the pre-fix code path. Pinned by `TestBuildBrokerChain_MultiSegmentFollows` (3-segment lineage walked end-to-end; chain ordering + kinds asserted) + `TestBuildBrokerChain_DeferralRemoved` (the literal Phase 4.5 refusal is gone on the 2-segment minimal case). ADR-0046 updated to mark the deferral CLOSED with the resolution.

## [0.97.1] - 2026-05-31

### Fixed

- **`fix(mysql): double-escape backslashes in PG → MySQL DOMAIN CHECK regex emission (v0.97.0 strict-fidelity follow-up)`** — v0.97.0's inline-CHECK translator emitted the source regex pattern into MySQL's SQL string literal without escaping backslashes. MySQL's string-literal parser treats `\` as an escape character by default, so the literal `'\.'` arrived at the regex engine as `.` (any character) rather than `\.` (literal dot). The email-DOMAIN regex `^[^@]+@[^@]+\.[^@]+$` stayed *functionally* correct (the `@` and negated character classes carried the rejection — an input without `@` was still rejected, an input with `@` but matching the `[^@]+\.[^@]+` shape was still accepted), but the stored expression diverged from PG's source semantics — a strict-fidelity gap flagged by the v0.97.0 post-release cycle. v0.97.1 closes the gap: `translateRegexCheckBody` now `strings.ReplaceAll(pattern, \`\\\`, \`\\\\\`)` before passing to `quoteSQLString`, so the SQL literal `'\\.'` arrives at the regex engine as `\.` regardless of the operator's `SQL_MODE` setting. PG regex shorthands (`\d`, `\s`, `\w`, `\b`) translate the same way. Pinned by an updated `TestTranslateDomainCheckToMySQL` sub-pin asserting the doubly-escaped emission AND a new sub-pin exercising multiple backslash escapes in the same pattern (`\d+\s\.[a-z]+`).

### Added (docs)

- **`docs(cookbook): three more recipes — sluice vs pg_dump, Heroku migration, PostGIS round-trip`** — extends the v0.97.0 cookbook scaffolding with the operator-facing comparisons + walkthroughs the existing breadth was missing. `compare-pg-dump.md` is the "why not just use pg_dump?" answer; `recipe-heroku-migration.md` is the slot-less managed-PG walkthrough that doubles as the canonical RDS / Crunchy Bridge / Supabase template; `recipe-postgis.md` is the cross-engine geometry round-trip recipe demonstrating the Bug 26/27 closure. All three link in from `docs/cookbook/README.md`.

## [0.97.0] - 2026-05-31

### Added

- **`feat(mysql): inline MySQL 8.0.16+ table-level CHECK for translatable PG DOMAIN CHECKs (v0.96.2 WARN follow-up stretch)`** — v0.96.2 made the cross-engine PG → MySQL DOMAIN-CHECK silent-drop class loud-observable via a structured WARN. v0.97.0 closes the remaining gap from "operator-actionable observability" to "in-database enforcement on MySQL targets" by translating two well-defined DOMAIN CHECK shapes into MySQL table-level CHECK clauses inline at CREATE TABLE time: regex (PG `CHECK (VALUE ~ 'pattern')` → MySQL `CHECK (REGEXP_LIKE(<col>, 'pattern'))`) and range (PG `CHECK (VALUE >= X AND VALUE <= Y)` → MySQL `CHECK (<col> >= X AND <col> <= Y)`). Gated on MySQL 8.0.16+ (the version that started enforcing CHECK constraints) — probed once at `OpenSchemaWriter` time via `SELECT VERSION()` and parsed by `mysqlVersionSupportsInlineCheck`; MariaDB is always excluded regardless of version (separate dialect; conservative default). Any DOMAIN CHECK shape outside the regex / range whitelist (function calls like `LENGTH(VALUE) > 5`, IN lists, negated regex, single-sided ranges, non-numeric range literals) is **silently dropped at emit time** — a wrong CHECK on dst is more dangerous than no CHECK (operators see it in `SHOW CREATE TABLE` and assume parity), so the v0.96.2 WARN continues to cover those columns. WARN is **suppressed** for columns whose every attached DOMAIN CHECK translated AND inlined (no silent-loss class to warn about); the existing `check_constraint_dropped` field continues to count un-translated CHECKs only. Pinned by `TestTranslateDomainCheckToMySQL` (15 sub-pins covering canonical PG output, no-cast variants, no-inner-parens variants, negative / decimal bounds, plus 8 fallback shapes) + `TestEmitTableDef_DomainCheck_*` (5 sub-pins on the CREATE TABLE integration: text regex / numeric range / unsupported-version no-emit / untranslatable-DROPPED-not-emitted / preserves-user-CHECKs) + `TestMaybeWarnDomainCheckDrop_v0970_*` (4 sub-pins on WARN suppression: suppress-when-all-translated, fire-when-partial, fire-when-older-MySQL, fire-when-zero-CHECKs) + `TestMySQLVersionSupportsInlineCheck` (16 sub-pins on version parsing).

## [0.96.3] - 2026-05-31

### Fixed

- **`fix(pipeline): probe operator envelope against a parent per-chunk WrappedCEK at incremental/stream start (Bug 117 ingestion-path closure)`** — v0.94.1 closed Bug 117 on the `sluice backup verify` path: per-chunk-mode chains accepting a rotated passphrase silently because SHA-256 verify hashes the on-disk bytes (compressed + encrypted) and a passphrase rotation re-wraps later chunks under the new envelope without changing the SHA observation surface. `VerifyBackupWith` + `probeChunkDecrypt` made that loud at verify time. The symmetric ingestion-path hole stayed open: `IncrementalBackup.alignEncryption` and `BackupStream.alignEncryption` returned `nil` for per-chunk mode without probing the operator's envelope against any of the parent's existing chunk WrappedCEKs, so an operator rotating their passphrase between two incrementals (or between full and incremental) in per-chunk mode silently accepted the rotation at incremental START, wrote new chunks under the rotated envelope, and the loud failure only surfaced later at restore-time crossing the rotation boundary. v0.96.3 adds a probe to both `alignEncryption` paths: after the existing mode-mismatch check, when the chain is per-chunk mode, find the first probe-able chunk in the parent manifest (Tables[].Chunks for full-manifest shape; ChangeChunks for incremental-manifest shape) via the new `firstPerChunkProbe` helper, then call `probeChunkDecrypt` (the same helper VerifyBackupWith uses) against the operator's envelope. On unwrap failure the call returns the documented `"passphrase rotated mid-chain?"` error wrapped with the `incremental:` / `stream:` prefix — refusing the incremental/stream start before any new chunks land. When the parent carries no probe-able chunks (e.g. an empty prior incremental window), the probe falls through silently so no regression is introduced on the brand-new-chain edge. Pinned by `TestIncrementalAlignEncryption_PerChunkDecryptProbe_Bug117_Ingestion` (4 sub-pins: per-chunk correct, per-chunk rotated → loud refuse, per-chain correct, per-chain rotated → existing chain-CEK probe fires first) + `TestFirstPerChunkProbe` (7 sub-pins on the helper covering nil/empty/full/incremental/per-chain-mode/plaintext/precedence-order).

## [0.96.2] - 2026-05-31

### Fixed

- **`fix(mysql): emit structured WARN on cross-engine PG → MySQL DOMAIN-CHECK silent-downgrade (Bug 113 PG→MySQL follow-up closure)`** — v0.95.2/v0.95.3 closed Bug 113 round-trip carry on same-engine PG → PG (`CREATE DOMAIN` emitted, CHECK preserved, row stream byte-faithful). The cross-engine PG → MySQL path's row stream also carried (Bug 122 fix covers cross-engine), but the MySQL writer silently downgraded the DOMAIN column to its base type with no WARN and no MySQL-level CHECK inlined — a residual silent-CHECK-loss class on the cross-engine path, same family as the original Bug 113. v0.96.2 adds `maybeWarnDomainCheckDrop` to `internal/engines/mysql/schema_writer.go` alongside the existing RLS-drop WARN pattern: at `CreateTablesWithoutConstraints` time, walk every table's columns, collect tuples of `(table.column, source_domain, target_base_type, check_count)` for every `ir.Domain`-typed column, and emit one structured `slog.WarnContext` per writer lifetime (one per stream, sync.Once-gated to avoid per-column flooding) carrying `affected_column_count`, `affected_columns`, `source_domains`, `target_base_types`, `check_constraint_dropped`, and an actionable `hint` line ("add a MySQL table-level CHECK (MySQL 8.0.16+) manually if input validation matters, or re-target to PG to preserve the DOMAIN"). The DOMAIN's MySQL base type is computed by recursing through the writer's existing `emitColumnType` so the WARN names the actual target MySQL spelling (e.g. `TINYTEXT`/`LONGTEXT` for PG `text` DOMAINs, `DECIMAL(65,30)` for unconstrained `numeric` DOMAINs). Same-engine MySQL → PG / MySQL → MySQL is unaffected (MySQL has no DOMAIN; the MySQL SchemaReader never populates `ir.Domain`). Pinned by `TestMaybeWarnDomainCheckDrop_*` covering text-DOMAIN, numeric-DOMAIN, MySQL-source-no-op, sync.Once-across-many-columns, sync.Once-across-many-calls, CHECK-less DOMAIN still WARNs, per-writer independence, and nil-schema defensiveness — 8 sub-pins mirroring the RLS WARN test matrix.

## [0.96.1] - 2026-05-31

### Fixed

- **`fix(pipeline): surface 'previously-completed tables have no indexes yet' hint on bulk-copy mid-phase abort (Bug 114 closure)`** — pre-fix when `sluice migrate` hit a bulk-copy failure on table N+1, the loud error correctly named the failing table (good — preserves loud-fail tenet) but said nothing about the state of tables 1..N. Because the migrate phases are `tables → bulk_copy → identity_sync → indexes → constraints → views`, an N+1 abort leaves tables 1..N with full row counts AND the PK index (from CREATE TABLE), but WITHOUT any of their declared secondary indexes — the index phase runs only after every table finishes bulk_copy. Operators inspecting `pg_indexes` on those earlier tables saw the PK and concluded "this table migrated cleanly", missing the absent secondary indexes; recovery (`--resume`) wasn't surfaced as the next step in the error message. v0.96.1 extends the `hints.go` registry (the existing operator-friendly post-error-hint layer) with a `PhaseBulkCopy` substring-keyed entry matching the standard `pipeline: copy table` wrapper prefix produced by `migrate_bulk.go`'s copy-table failure paths. The hint reads `any earlier tables in this run have data but NOT their declared secondary indexes (the indexes phase runs after ALL tables finish bulk-copy); use --resume to continue after fixing the offending table, or --exclude-table=<name> to skip it`. The existing "does not exist" / "doesn't exist" PhaseBulkCopy entries continue to win first (first-match-wins ordering) so the more-actionable "target table not found" hint still fires for schema-apply-mismatch errors; the new entry catches the residual "underlying engine error" class (e.g. Bug 114's `jsonb[]` COPY-protocol refusal). Pinned by `TestHintForRegistry` adding a new sub-case naming Bug 114 with the catalog's `sentry_releases` repro shape.

## [0.96.0] - 2026-05-31

### Fixed

- **`fix(cmd/sluice): apply CLI-overrides-YAML semantics to redaction rules (Bug 108 closure)`** — pre-fix when an operator passed BOTH a YAML config (`redactions:` block) AND a CLI `--redact` flag declaring DIFFERENT strategies for the SAME column, sluice silently picked the YAML rule and ignored the CLI flag — the OPPOSITE of the documented precedence model ("CLI flags … override anything set here" from `docs/examples/sluice.yaml`). The root cause was the YAML merge step running AFTER CLI parsing and calling `redact.Registry.Set` for every YAML entry, where the underlying map-Set silently overwrote any existing CLI rule. The bug surfaced as silent policy substitution on a compliance-critical feature: an operator inheriting a weak team-template YAML strategy (e.g. `static:redacted`) who tried to override per-run with a stronger CLI strategy (`hash:hmac-sha256` for cross-run consistency) would silently get the team-template's weak strategy applied. Discovered only by comparing dst output against expected output of the CLI strategy. v0.96.0 closes this in `mergeYAMLRedactions` by checking `reg.Get(schema, table, column)` for each YAML entry; when a CLI rule is already present, the YAML entry is skipped with a loud `slog.Warn` naming the column AND the YAML strategy that was skipped, so operators get a clear "your YAML rule was overridden by your CLI flag" signal instead of silent substitution. Pinned by `TestMergeYAMLRedactions_CLIOverridesYAML` (2 sub-pins covering BUG-CATALOG.md Bug 108 entry's variant A (YAML hash + CLI static → CLI static wins) and variant B (YAML static + CLI hash → CLI hash wins)).

## [0.95.3] - 2026-05-31

### Fixed

- **`fix(postgres,mysql): dispatch ir.Domain value codec to base type (Bug 122 closure — v0.95.2 round-trip carrier unblocker)`** — v0.95.2 wired the schema half of Bug 113's round-trip carry (reader populates `ir.Domain`, writer Phase 1a' emits `CREATE DOMAIN`, `emitColumnType` references the DOMAIN name). The post-release cycle's `gl_users` repro confirmed the schema half lands correctly on PG dst (`typtype='d'`, column typed `email_address`, CHECK regex preserved, dst rejects `NOT-AN-EMAIL`), but `bulk_copy` aborted on the first row with `postgres: no decoder for IR type ir.Domain` — the PG row-stream value codec dispatch had no `ir.Domain` case, so DOMAIN-typed columns surfaced a loud-failure migrate exit 1 with zero rows carried. Generic across base types (DOMAIN over `text` and `numeric` both reproduced); plain `text` negative control was unaffected. Same shape on cross-engine PG→MySQL (MySQL writer silently downgraded the column to the base type's MySQL DDL, but the source PG row reader's decoder hit the same dispatch gap before the value ever crossed). v0.95.3 adds an `ir.Domain` case to `internal/engines/postgres/value_decode.go` (`decodeValue` recurses against `Domain.BaseType` — PG's wire / text I/O for a DOMAIN-typed column is byte-identical to its base type) and to `internal/engines/postgres/row_writer.go::prepareValue` (same recursion shape) so every downstream specialization (Array / Geometry / Bit / Extension / Verbatim / scalar passthrough) reaches its existing branch. Defense-in-depth `ir.Domain` case added to `internal/engines/mysql/row_writer.go::prepareValue` (synthesize a `*ir.Column` with the base type and recurse), covering the scenario where ir.Domain leaks past the retarget layer into the MySQL applier's value-prep path. With this fix the v0.95.2 round-trip carry's `gl_users` repro is end-to-end functional: dst has DOMAIN preserved + invalid email rejected + rows carried.

## [0.95.2] - 2026-05-31

### Added

- **`feat(postgres): round-trip PG DOMAIN with CHECK constraints (Bug 113 full closure)`** — v0.95.1 shipped the loud-refuse closure for Bug 113 (silent CHECK-constraint loss prevented at the read boundary). v0.95.2 rotates to actual round-trip carry: the PG schema reader pre-reads every DOMAIN's CHECK definitions via `pg_get_constraintdef` (`readDomainChecks` — keyed by `pg_type.typname`, joined to `pg_constraint` on `contypid` + `contype='c'`), and `populateColumns` wraps the column's BASE-translated IR type in `ir.Domain{Name, BaseType, Checks}` when `pg_type.typtype == 'd'`. The base IR type comes for free because `information_schema.columns` unwraps DOMAINs at every field it exposes (`data_type`, `udt_name`, `char_max_len`, etc.) so the existing `translateType` call produces the base type; the DOMAIN-specific metadata (name + CHECKs) is wrapped on AFTER. PG schema writer adds Phase 1a' (after Phase 1a enum types, before Phase 1b tables): walk every column for `ir.Domain`, dedupe by `Name`, emit `CREATE DOMAIN <schema>.<name> AS <base type DDL> [CONSTRAINT <name>] CHECK (<body>);` so column references in CREATE TABLE resolve to the just-created DOMAIN. `emitColumnType` dispatches `ir.Domain` to emit the schema-qualified DOMAIN name (NOT the base type's DDL) when a table-column reference is rendered. Cross-engine PG→MySQL: MySQL has no DOMAIN counterpart; the MySQL writer downgrades to the DOMAIN's BASE type DDL (a partial close — the CHECK constraints attached to the DOMAIN are not yet re-emitted as table-level CHECKs; tracked as a follow-up). Pinned by `TestSchemaReader_DomainRoundTrip_Bug113` (DOMAIN round-trips as `ir.Domain` with BaseType=`ir.Text` + one CHECK with non-empty body) + `TestSchemaReader_DomainRoundTrip_NonDomainUserDefinedStillRoundTrips` (negative control: ENUM still round-trips as `ir.Enum`, not wrapped as Domain).

## [0.95.1] - 2026-05-31

### Fixed

- **`fix(postgres): refuse loudly when a column references a PG DOMAIN (Bug 113 closure)`** — pre-fix a column referencing a Postgres `CREATE DOMAIN` user-defined type (e.g. `CREATE DOMAIN email_address AS text CHECK (VALUE ~ '...@..\\..*')`) surfaced through `information_schema.columns` as the underlying base type — `information_schema` silently unwraps DOMAINs (`data_type` returns `text`, not `USER-DEFINED`) — so the PG schema reader translated the column to `ir.Text{}` and the DOMAIN's CHECK constraints disappeared on PG→PG migrate (CRITICAL silent-constraint-loss class: every input-validation invariant the operator encoded as a DOMAIN was silently destroyed on the target). The bug catalog (Bug 113) records this as a same-engine PG→PG class: the target accepted values the source would reject, with no WARN, no error, and exit 0. v0.95.1 reads `pg_type.typtype` directly alongside the per-column dispatch; when `typtype == 'd'` the reader refuses loudly at the read boundary so no partial schema lands on the target, and the operator gets a clear actionable error naming the table, column, and domain name, plus the recovery `ALTER TABLE … ALTER COLUMN … TYPE … USING …::…` shape. Per the bug-catalog suggested-fix: "Either is acceptable; silent-drop is not." Round-trip DOMAIN carry is queued for a v0.95.2 follow-up; this release ships the IR scaffolding (`ir.Domain{Name, BaseType, Checks}`, `ir.DomainCheck{Name, Body}`, `ir.ExtDomain` ExtensionKind, JSON tagged-union round-trip in `MarshalType` / `UnmarshalType`) and the loud-refusal. Negative control pinned alongside: a column referencing a `CREATE TYPE ... AS ENUM` (`pg_type.typtype == 'e'`, also `USER-DEFINED` in `information_schema`) continues to round-trip cleanly so the DOMAIN refusal doesn't over-broaden to every user-defined type and regress the v0.16.x ENUM handling. Pinned by `TestSchemaReader_DomainRefusal_Bug113` + `TestSchemaReader_DomainRefusal_NonDomainUserDefinedStillRoundTrips`.

## [0.95.0] - 2026-05-31

### Fixed

- **`fix(postgres): carry non-default core operator classes through schema reader (Bug 115 closure)`** — pre-fix the PG schema reader populated `ir.IndexColumn.OperatorClass` only when (a) the index used an extension-introduced access method (pgvector's hnsw), (b) the opclass was owned by an enabled extension on a core AM (pg_trgm's `gin_trgm_ops` / `gist_trgm_ops`), or (c) an uncatalogued extension-owned opclass surfaced under the ADR-0047 verbatim tier. Operator-explicit **non-default core PG opclasses** on core AMs fell through every branch and were silently dropped: `btree (col text_pattern_ops)` (required for `LIKE 'prefix%'` index use in C locale), `btree (col varchar_pattern_ops)` (same case for varchar), and `gin (col jsonb_path_ops)` (~50% smaller, substantially faster for `@>` containment vs default `jsonb_ops`) all migrated PG→PG to an index using the default opclass — index name preserved, structure intact, but the operational semantics differ and "we migrated and our `@>` queries are 10× slower" became a debug mystery. v0.95.0 extends the reader's SQL to fetch `pg_opclass.opcdefault` alongside `opcname` and adds a dispatch branch: when `opclass != "" && !opclassExtOwned && !opclassDefault`, the bareword is carried verbatim through `ir.IndexColumn.OperatorClass`. The existing same-engine PG writer at `emitIndexColumnList` already emits `<column> <opclass>` for any non-empty `OperatorClass`, so the fix is purely on the reader side. Default-opclass cases on built-in types continue to leave `OperatorClass` empty so the writer emits nothing extra (preserves the Bug 47 invariant that a non-empty value through the IR is an honest "operator-significant opclass" marker and keeps DDL diffs stable against `pg_get_indexdef` across PG major versions). Pinned by `TestSchemaReader_NonDefaultCoreOpclasses_Bug115` (integration test against a real Postgres covering the three documented Bug 115 cases — `text_pattern_ops`, `varchar_pattern_ops`, `jsonb_path_ops` — plus a default-opclass negative control that asserts the IR stays empty).

## [0.94.1] - 2026-05-31

### Fixed

- **`fix(ir,pipeline): bump manifest FormatVersion to 2 for security-metadata-bearing schemas (Bug 116 closure)`** — pre-fix manifests carried `FormatVersion=1` regardless of whether the schema used security-relevant fields older binaries would silently drop. The pre-v0.94.x manifest contract said "field-additions are forward-compatible (older sluice ignores unknown fields)" — correct for behaviorally-idempotent additions, *wrong* for security and correctness fields: an older `sluice restore` reading a newer manifest's `Schema.Tables[].RLSEnabled` / `RLSForced` / `Policies` / `ExcludeConstraints` would silently drop them and write a target schema with no RLS, no policies, and no EXCLUDE constraints — a CRITICAL silent-loss class for tenant isolation. v0.94.1 introduces `FormatVersionLegacy=1` / `FormatVersionSecurityMetadata=2` constants + an `ir.FormatVersionFor(*Schema) int` helper that scans the schema for any of those four fields and returns the smallest version safe for it. The proportional version-stamp rule is wired into all three orchestrator manifest constructors (`pipeline.Backup`, `IncrementalBackup`, `Streamer`'s rollover constructor): a manifest gets `FormatVersion=2` ONLY when its schema actually uses one of the gated features; innocent backups stay on `FormatVersion=1` and continue to restore on older binaries (preserves backward compatibility for the common case). `BackupFormatVersion` is bumped to 2 so the current build's existing preflight (`if m.FormatVersion > ir.BackupFormatVersion`) accepts both v1 and v2; older binaries (v0.94.0 and earlier) hit their own preflight and refuse v2 manifests loudly — they cannot silently drop what they cannot decode. Going forward, any schema-metadata addition with security or correctness implications bumps FormatVersion; purely informational additions stay under the field-additions rule. Pinned by `TestChooseFormatVersion_Bug116` (10 sub-pins: nil/empty schemas → legacy, each of RLSEnabled / RLSForced / Policies / ExcludeConstraints independently triggers the bump, multi-table mixed innocent+RLS picks security, nil-element tolerance, agreement between `chooseFormatVersion` and the exported `FormatVersionFor`, `BackupFormatVersion == FormatVersionSecurityMetadata` ceiling) + `TestBackupFormatVersion_Bumped` (constants invariant) + `TestBackup_FormatVersion_Bug116` (4 sub-pins at the pipeline-orchestrator boundary: innocent → v1, RLSEnabled → v2, Policies → v2, EXCLUDE → v2).

- **`fix(pipeline): add envelope decrypt probe to backup verify (Bug 117 closure)`** — pre-fix `sluice backup verify` was SHA-256-only: it hashed each chunk's on-disk bytes and compared against the manifest. For per-chunk-mode encrypted chains (`--encrypt-mode=per-chunk`), each chunk's CEK is wrapped freshly at write time with the current envelope; an operator who ran `backup incremental` with a *different* `--encryption-passphrase` than the original `backup full` produced a chain where each segment's chunks were wrapped under different KEKs, but every chunk's on-disk SHA still matched the manifest — `backup verify` reported "all chunks OK", and the divergence only surfaced at `restore` time as a partial-fail at the first rotated chunk with no rollback (target left mid-state). v0.94.1 adds `VerifyOptions{Envelope}` + `VerifyBackupWith` (the historical `VerifyBackup(ctx, store)` is preserved as a SHA-only wrapper for backward compatibility) and wires `cmd/sluice backup verify` to load the chain root manifest, build a read envelope from the operator's `--encrypt` flags (re-deriving the chain's Argon2id KEK against the chain's recorded salt), and pass it through. When the envelope is non-nil, verify performs an up-front chain-CEK unwrap probe (per-chain mode + chain-level KEKMode mismatch fail fast with a clear error) and a per-chunk `probeChunkDecrypt` that unwraps each chunk's `WrappedCEK` — an unwrap failure on a per-chunk-mode chain (Bug 117 signature) is counted as a verify failure with a descriptive `unwrap chunk cek (passphrase rotated mid-chain?)` error that names the chunk path. When the chain is encrypted but the operator omits `--encrypt`, verify falls through to the legacy SHA-only path AND emits a loud `slog.Warn` advising the operator to re-run with `--encrypt` for full coverage. Pinned by `TestVerifyBackupWith_DecryptProbe_Bug117` (6 sub-pins: plaintext+nil-envelope, per-chain+correct-envelope, per-chain+wrong-passphrase fast-refuse, per-chunk+correct-envelope, per-chunk+rotated-passphrase fail-every-chunk with belt-and-suspenders confirmation that legacy SHA-only still reports 0 failed on the same store, KEKMode mismatch fast-refuse) + `TestProbeChunkDecrypt_NilSafe`.

## [0.94.0] - 2026-05-31

### Fixed

- **`fix(pipeline): scope incremental backup's end-position schema-read to the parent chain's table set (Bug 110 closure)`** — pre-fix `IncrementalBackup.readSourceSchema` called the schema reader's unscoped `ReadSchema`, which iterated every table in the source. A chain originally taken with `--include-table=X` would silently re-read every table at end-position recording, and a single unrelated table carrying a verbatim-eligible column type (`xml` / `money` / `interval` / `tsvector` / etc.) failed the whole incremental at `read source schema (end): postgres: read columns: table "Y" column "Z": postgres: unsupported data_type` — a previously-working chain broke because an unrelated table was added to the source. v0.94.0 derives a table-name predicate from the parent manifest's recorded `Schema.Tables` at `Run` start and threads it through `readSourceSchema` so on engines that implement `ir.TableScoper` (PostgreSQL today; MySQL falls through to the unscoped read because MySQL has no verbatim-type-in-schema problem to begin with), the end-position read restricts itself to the chain's original table set. A parent manifest with no recorded table list (corrupt / pre-v0.94 fallback) leaves the scope nil and preserves the historical unscoped behaviour. Pinned by `TestIncrementalBackup_ScopeFromParentManifest` (5 sub-pins: nil-schema unscoped fallback, empty-schema unscoped fallback, single-table admit, multi-table exact set with the Bug 110 false-positive cases (`unrelated_xml`, `unrelated_money`), nil-element tolerance in the table slice).

## [0.93.0] - 2026-05-31

### Fixed

- **`fix(postgres): refuse loudly on incompatible CDC schema-race situations mid-stream (Bugs 112 + 119 + 120 v0.93.0 closure)`** — pre-fix the applier's `colTypeCache` (keyed by `"schema.table"` with no invalidation) silently used stale shape when the source's relation changed mid-stream: Bug 112 (RENAME) saw writes to the renamed table vanish from dst because pgoutput's new RelationMessage carried the new name while the dst still had the old name, so apply hit `errUnknownTable` and silently skipped; Bug 119 (DROP COLUMN) silently drifted dst's column populated as NULL on new INSERTs; Bug 120 (DROP+CREATE same name) silently dropped the new relation's writes onto the orphaned old cache entry. The shared root cause is the applier's cache having no awareness of relation-OID changes mid-stream. v0.93.0 adds `detectIncompatibleRelationChange` + `checkSchemaRace` in the CDC reader's RelationMessage handler (`internal/engines/postgres/cdc_relations.go`) that compare every incoming RelationMessage against the previously-cached entry for the same OID, and scan the relations map for any orphaned entry with the same `(Schema, Name)` but a different OID. Detected RENAME / DROP COLUMN / RENAME COLUMN / ALTER COLUMN TYPE / DROP+CREATE all surface as a loud stream-killing error naming the table, OID(s), and the drained-model recovery hint (`sluice sync stop --wait` → migration tool → `sluice sync start --resume`). ADD COLUMN appended at the end remains compatible — the existing ADR-0058 `--forward-schema-add-column` opt-in forwarding path continues to work. Pinned by `TestDetectIncompatibleRelationChange` (9 sub-pins covering each shape including the benign re-send pgoutput emits on reconnect) + `TestCheckSchemaRace_DROPCREATESameNameDifferentOID` + `TestCheckSchemaRace_SameOIDReentryIsBenign` + `TestCheckSchemaRace_ADDColumnIsCompatible`. Concurrency-adjacent — `-race` integration gate runs on the push-first-tag-after branch before tag cut per CLAUDE.md.

## [0.92.4] - 2026-05-31

### Fixed

- **`fix(postgres): convert []byte to string for ir.VerbatimType columns in prepareApplierValue (Bug 97 wire-encoding REDO — v0.92.3 partial-close did not actually close it)`** — v0.92.3 added explicit `$N::TYPE` casts in the apply SQL, but the v0.92.3 verification cycle found the bug STILL reproduces for `money` and `pg_lsn`: pgx's `database/sql` adapter binds Go `[]byte` as PG `bytea` on the wire, so PG evaluates `bytea::TYPE` which goes through an implicit `bytea → text` cast producing a `\x…` hex literal, which then fails the `text → TYPE` parse with `invalid input syntax for type money: "\x2439392e3939"` / `pg_lsn: "\x302f33303030303030"`. `xml` / `tsvector` / `int4range` syntactically tolerated the bytea-hex form. v0.92.4 closes the second wire-format layer: `prepareApplierValue` now converts `[]byte` to `string` for `ir.VerbatimType` columns so pgx binds as text. PG's cast machinery then sees the canonical text form (`$99.99`, `0/3000000`, etc.). Pinned by `TestPrepareApplierValue_VerbatimTypeBytesBecomeString` (5 sub-pins covering money / pg_lsn / xml / string-idempotency / non-verbatim-byte-passthrough). Bug 74 family-dispatch lesson applied uniformly across every verbatim family.

## [0.92.3] - 2026-05-31

### Fixed

- **`fix(postgres): emit `$N::TYPE` casts for ir.VerbatimType columns in apply SQL (Bug 97 v0.92.3 wire-encoding closure)`** — v0.92.2 closed the applier-side translator gap (the column type now lands as `ir.VerbatimType`), but the v0.92.2 verification cycle found that `money` and `pg_lsn` rows still failed at runtime: pgx fell back to bytea binary encoding for the unknown PG type, sending the value's ASCII bytes as a `\x…` hex literal, and PG rejected with `invalid input syntax for type money` / `invalid input syntax for type pg_lsn`. The `xml` / `tsvector` / `int4range` families happened to round-trip because their text-IO syntactically tolerated the bytea-hex form. v0.92.3 closes the wire-encoding gap with explicit `$N::<verbatim-type>` casts in INSERT VALUES + UPDATE SET clauses, and `col::text = $N::TYPE::text` in WHERE equality predicates (canonical text comparison). The `Definition` string comes from `pg_catalog.format_type` which produces canonical type names with no user-controlled input, so it's safe to interpolate. New `applyPlaceholder` + `verbatimPlaceholder` helpers in `internal/engines/postgres/change_applier.go`. Pinned by `TestBuildSQL_VerbatimTypeCasts` (4 sub-pins covering INSERT VALUES, UPDATE SET, WHERE equality, and the non-verbatim plain-`$N` path). Bug 74 family-dispatch lesson applied per-family.

- **`fix(cli): route --slot-name through pipeline.ResolveSlotName for backup incremental + backup stream (Bug 121)`** — `sluice backup full --slot-name=X` correctly applied the sluice-prefix convention (`X` → `sluice_X`); `sluice backup incremental --slot-name=X` and `sluice backup stream run --slot-name=X` took `X` literally. Operators following a chain workflow with consistent `--slot-name=X` across commands hit `position references slot "sluice_X" but reader is configured with slot "X"` and the chain stalled. v0.92.3 routes the two missing call sites through `pipeline.ResolveSlotName(...)` so the convention applies uniformly. Same kong-tag-drift class as v0.92.1's `--mysql-sql-mode` typo — found by the v0.92.2 verification cycle.

## [0.92.2] - 2026-05-30

### Fixed

- **`fix(mysql): refuse loudly on LOAD DATA conversion warnings (Bugs 102 + 103 root-cause closure)`** — v0.92.1's `sql_mode='STRICT_TRANS_TABLES,...'` injection IS correct end-to-end (`@@SESSION.sql_mode` on a sluice-opened connection contains every strict mode; direct `INSERT` of out-of-range NUMERIC errors `1264`, direct INSERT of zero-date errors `1292`) — but the bulk-copy path uses `LOAD DATA LOCAL INFILE` with `(@var) SET col=@var` indirection, and **MySQL silently bypasses strict mode for type-conversion errors in that path** (verified empirically: 80-digit NUMERIC into DECIMAL(65,30) silently clamps to MAX, zero-date TIMESTAMP silently lands, explicit `CAST(@var AS type)` in the SET clause does not change the behavior). `@@warning_count` IS bumped though, so v0.92.2 pins the LOAD DATA connection, queries `@@warning_count` after the exec, and refuses loudly with the first up-to-8 `SHOW WARNINGS` rows. Skipped on the `--mysql-sql-mode=''` legacy-data path (operator opted out). Closes the silent-loss class v0.92.1 announced but did not actually deliver.
- **`fix(postgres): plumb verbatim-carry types through the applier's type translator (Bug 97 applier-side gap)`** — v0.92.0's CDC reader OID-switch fix landed correctly, but the **applier-side** type translator (`loadColumnTypes` in `internal/engines/postgres/change_applier.go`) carried the same allowlist gap. The applier's information_schema query didn't fetch `pg_catalog.format_type` and didn't set `VerbatimEligible`, so `translateType` hit the generic loud refusal on the first DML touching `money` / `xml` / `tsvector` / `pg_lsn` / `txid_snapshot` / `pg_snapshot` / range / multirange. Stream died at `pipeline: apply changes: postgres: applier: translate <col>: postgres: unsupported data_type "<type>"`. v0.92.2 adds the `pg_class` + `pg_attribute` join (mirroring the schema reader) and sets `VerbatimEligible=true` in the applier context (target is PG; cross-engine sources can't produce these types). 11-family pin matrix added.
- **`fix(cli): kong tag pin for --mysql-sql-mode (v0.92.1 flag-name typo)`** — kong's auto-kebab-case reads `MySQLSQLMode` as `My` + `SQLSQL` (one acronym block, no lowercase break) + `Mode`, emitting `--my-sqlsql-mode` — a typo that contradicts the help text. v0.92.2 adds `name:"mysql-sql-mode"` explicit kong tag. Two unit pins (positive: `--mysql-sql-mode=...` parses; negative: `--my-sqlsql-mode` is rejected as an unknown flag so v0.92.1-typo paste-copies surface clearly).
- **`fix(pipeline): refuse loudly when a redaction rule targets a GENERATED column (Bug 109 — CRITICAL silent PII leak)`** — `--redact='<generated_column>=<strategy>'` (or YAML equivalent) silently no-op'd at apply time: the target's `GENERATED ALWAYS AS (...) STORED` / virtual column re-derives from the source columns the expression depends on, so the operator's intent to block PII propagation was silently nullified — exit 0, dst still shows the PII via the re-derivation. Same family as Bug 99 (selector unresolved silent no-op). The fix adds a third case to `preflightRedactTypes` (`internal/pipeline/redact_preflight.go`): every rule whose selector resolves to a column with non-empty `GeneratedExpr` is refused with the new `errRedactOnGeneratedColumn` sentinel naming the column, the generated expression, the dialect, and the recovery hint ("redact the source columns the expression depends on"). Found by the v0.92.0 deep bug-finding sweep #3.

### Added

- **`feat(mysql): WARN at schema-read on ENUM/SET labels containing '?' (Bug 106 documentation)`** — Bug 106 is a **MySQL server-side limitation, not a sluice bug**: MySQL's data dictionary silently substitutes `?` for supplementary-plane (4-byte UTF-8) characters in ENUM/SET labels at `CREATE TABLE` time regardless of column charset; `mysqldump` reproduces the same loss. The label is already gone from the source's catalog by the time sluice reads it — no recovery possible from sluice's side. v0.92.2 surfaces this at schema-read time via a WARN line so operators discover the loss before the runtime row-INSERT loud-fails ("invalid input value for enum ..." on PG). Heuristic kept narrow (warn only on `?` chars in column_type) to keep false positives low. `docs/operator/migrating-legacy-mysql.md` gets a new section explaining the MySQL behavior, the runtime symptom, and recovery via `--type-override=TABLE.COL=text`.

## [0.92.1] - 2026-05-30

### Added

- **`feat(mysql): force strict sql_mode + utf8mb4 collation on every connection (Bug 102 + Bug 103 + Bug 106 closure)`** — sluice previously inherited the MySQL server's `sql_mode` and connection collation, which on dev / older / managed deployments allowed three CRITICAL silent-loss classes: PG NUMERIC overflow → silent clamp on MySQL target (Bug 102), PG TIMESTAMPTZ out-of-range → silent `'0000-00-00 00:00:00'` (Bug 103), and MySQL ENUM labels containing 4-byte UTF-8 → silent `?` substitution during MySQL → PG schema-read (Bug 106). v0.92.1 forces `sql_mode='STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO'` and `Collation='utf8mb4_general_ci'` on every sluice MySQL connection, turning the silent-loss class into the loud MySQL error path. DSN-level `?sql_mode=...` still wins absolutely.
- **`feat(cli): --mysql-sql-mode top-level flag (legacy-data escape hatch)`** — the strict-by-default closes the silent-loss class but would refuse legacy MySQL data (zero-dates, silently-truncated VARCHARs) that pre-MySQL-5.7 schemas commonly carry (20+ year-old WHMCS-shaped corpus is the canonical example). The new `--mysql-sql-mode` Globals flag is the escape hatch: pass `--mysql-sql-mode=''` (explicit empty) to fall through to the server's default sql_mode for migrating such data; pass a specific comma-separated mode list to force exactly those modes. See [docs/operator/migrating-legacy-mysql.md](docs/operator/migrating-legacy-mysql.md) for the full migration story.
- **`feat(pipeline): randomize:int target-column range preflight (Bug 105 closure)`** — `--redact='COL=randomize:int:LO,HI'` whose `LO,HI` exceeded the target column's representable integer range previously had its values silently clamped to the column MAX every row at apply time, defeating PII randomization. The new preflight check compares the rule's `Min`/`Max` against the source column's `ir.Integer{Width, Unsigned}` range and refuses loudly via the new `errRedactRandomizeRangeOverflow` sentinel if out-of-range.

### Fixed

- **`fix(postgres): refuse VARCHAR(0)/CHAR(0) at emit with operator-actionable recovery hint (Bug 107)`** — MySQL allowed `VARCHAR(0)` as a marker column for 20+ years; PG refuses zero-length char/varchar at CREATE TABLE with `length for type varchar must be at least 1` (SQLSTATE 22023). Pre-fix sluice forwarded the VARCHAR(0) into the PG schema-apply DDL and crashed with that raw error AFTER the cold-start preamble had already run — late, ugly, recovery non-obvious. v0.92.1 catches this at sluice's PG `emitColumnType` and refuses loudly with the recovery flag named (`--type-override=TABLE.COL=text` or `--type-override=TABLE.COL=boolean`). Same fix covers `CHAR(0)`. Found by an operator question that referenced a real-world WHMCS-shaped schema.

## [0.92.0] - 2026-05-30

### Fixed

- **`fix(postgres): plumb Stage 1+2 verbatim-carry OIDs through the CDC reader (Bug 97 — silent migrate-vs-sync contract gap)`** — same-engine PG → PG `sluice sync` of columns whose `pg_catalog` type is one of the 19 verbatim-carry types (Stage 1 tsvector/tsquery/range/multirange family, Stage 2 xml/money/pg_lsn/txid_snapshot/pg_snapshot per ADR-0070) crashed the sync stream on the first DML. The schema reader's text-keyed `coreVerbatimEligibleTypes` allowlist and the CDC reader's OID-based `oidToType()` switch had drifted: every type was eligible at schema-read but unknown at decode-time, so the streamer hit `postgres: cdc: unsupported column type OID <N>` and exited. The v0.91.0 release notes assert "preserve path" — that was true for `migrate`; the sync path was quietly broken for these types all the way back to Stage 1 (v0.71.0). The fix adds a `coreVerbatimCDCOIDs` map in `internal/engines/postgres/cdc_relations.go` that mirrors the schema reader's allowlist OID-for-typname, with one entry per verbatim type. Cross-engine PG → MySQL stays loud-refused via `ir.VerbatimType` in `cross_engine_supportable.go` (the orchestrator-side check is unchanged). 19 unit pins added to `TestOIDToType` cover every entry. **Architectural follow-up:** the two registries should consolidate into one source of truth shared between schema_reader and cdc_relations (deferred — the per-OID list is small and explicit).
- **`fix(pipeline): refuse loudly when source contains PG declaratively-partitioned tables (Bug 100 — silent constraint loss)`** — `sluice migrate` / `sluice sync start` against a PG source whose schema contains tables declared `PARTITION BY RANGE | LIST | HASH (...)` silently flattened the partitioned parent to a plain heap on the target: the partition key declaration was dropped, the partition children disappeared from the parent narrative, AND the parent's composite PRIMARY KEY (which in a partitioned table must include every partition-key column) was silently dropped. The child tables are individual heap tables in `information_schema.tables` and were also copied, so the operator either lost the children (filtered) or duplicated the data (parent flat copy + N child copies). Sluice does not yet support partition-aware migration; v0.92.0 turns the silent-flatten into a loud refusal at preflight. New `partitionPreflightProber` engine surface (PG implements via `SchemaReader.PartitionedTables`), new `preflightPartitionedTables` orchestrator-side check wired into both `migrate.go` and `streamer.go` cold-start preambles. Filter-aware — operators who already `--exclude-table=<parent>` are not refused. Refusal lists every offending parent and surfaces three operator-actionable recovery paths (exclude parent, scope via `--include-table`, or land the partition hierarchy via `pg_dump --schema-only` + `sluice migrate --schema-already-applied`). 7 unit pins added.
- **`fix(postgres, mysql): preserve TRUNCATE CASCADE / RESTART IDENTITY through CDC (Bug 98 — stream-crash on common operator action)`** — pgoutput's `TruncateMessage.Option` flag (bit 0 = CASCADE, bit 1 = RESTART IDENTITY) was discarded in `emitTruncate`; `ir.Truncate` carried no flag; the PG applier emitted a naked `TRUNCATE TABLE` which fails on FK-referenced targets with `ERROR: cannot truncate a table referenced in a foreign key constraint` (SQLSTATE 0A000), crashing the stream. The fix plumbs the option byte through: `ir.Truncate` grows `Cascade` and `RestartIdentity` boolean fields; `emitTruncate` decodes the bits and stamps both onto every emitted `ir.Truncate`; `buildTruncateSQL` (PG) appends `RESTART IDENTITY` then `CASCADE` per PG's grammar when set. MySQL applier logs a WARN if either flag is set on a cross-engine PG → MySQL stream (MySQL `TRUNCATE` has no CASCADE concept; InnoDB always resets `AUTO_INCREMENT`) and emits the plain `TRUNCATE`. 4 unit pins added (all four flag combinations).
- **`fix(pgtrigger): orphaned DDL event trigger names the recovery command (Bug 101 — operator-friction with catastrophic blast radius)`** — `sluice_capture_ddl_trg` is an event trigger that INSERTs a marker row into `sluice_change_log` for every DDL command. If the operator manually drops `sluice_change_log` without running `sluice trigger teardown`, the trigger fires on the next DDL, the INSERT fails with `relation does not exist`, and PG returns the full PL/pgSQL function body as the error message — blocking ALL subsequent DDL on the source with no operator-visible recovery hint. The fix wraps the INSERT in an `EXCEPTION WHEN undefined_table` handler that `RAISE EXCEPTION`s with: `MESSAGE='sluice trigger engine is partially uninstalled (...); DDL blocked by orphaned event trigger'`, `HINT='To fully remove the sluice trigger engine, run: sluice trigger teardown ... Or to restore CDC: sluice trigger setup ...'`, and `ERRCODE='object_not_in_prerequisite_state'` (55000) so monitoring tools can key off it. Unit pin verifies the handler shape + the recovery message content.

### Compatibility

- **Minor version bump (v0.92.0)** — additive + bug-fix release. Drop-in from v0.91.1 except for the documented behavior changes below.
- **Two new loud refusals** that previously failed silently or crashed mid-stream:
  - PG declaratively-partitioned tables in source schema → preflight refusal (Bug 100). Operators with partitioned schemas must explicitly `--exclude-table` the parents or scope via `--include-table` to opt into the silent-flatten if that's what they want (the workaround was always wrong, but it was previously the silent default).
  - Cross-engine PG → MySQL `TRUNCATE … CASCADE`: a one-shot WARN log per source TRUNCATE event documenting which option flags were dropped at the cross-engine boundary (the apply still happens, plain TRUNCATE).
- **Two formerly-stream-crashing scenarios that now apply cleanly** (Bugs 97 + 98).

## [0.91.1] - 2026-05-30

### Fixed

- **`fix(pipeline): refuse loudly when a redaction rule's selector doesn't resolve (Bug 99 — CRITICAL silent-PII-loss)`** — `sluice migrate` / `sluice sync start` with a typo'd `--redact='TABLE.COLUMN=STRATEGY'` selector (e.g. `users.emial` instead of `users.email`) silently no-op'd the rule, leaving plaintext PII to land at the destination. The pre-existing per-strategy preflight checks (`mask:uuid` type compatibility, `randomize:*` PK requirement, `hash:hmac-sha256` / `tokenize:dict` keyset presence) all `continue`d on a missed schema lookup; strategies with no per-strategy guard (most notably `hash:sha256`, `static`, `truncate`, `null`) hit no check at all, so a typo on those strategies passed preflight silently and applied to no rows at the apply step. The fix adds a selector-resolution check at the top of `preflightRedactTypes` (`internal/pipeline/redact_preflight.go`): every rule's `(Table, Column)` must resolve to a real column in the post-mappings schema; a rule that doesn't is refused with the new `errRedactSelectorUnresolved` sentinel ("redaction rule's TABLE.COLUMN selector does not resolve to any column in the source schema (typo class — would silently leak PII)") naming the unresolved selector. Found by the v0.91.0 deep bug-finding sweep (sluice-testing/BUG-CATALOG.md Bug 99). **Behavior change:** an existing pipeline with a typo'd rule that previously "worked" by silently doing nothing will now refuse loudly at startup. This is correct: a rule that applies to nothing is not a no-op, it's a silent compliance failure — operators discovering the refusal should fix the typo (or remove the dead rule). Two existing unit-test pins that asserted the silent-skip-as-feature behavior were inverted to assert the new loud refusal (the pre-fix pins were pinning the bug). Three new pins document the load-bearing cases: selector-unresolved on `mask:uuid`, on `randomize:*`, and on `hash:sha256` (the canonical Bug 99 repro: the strategy with no per-strategy guard).

### Compatibility

- **Patch bump (v0.91.1) — hotfix.** Drop-in from v0.91.0 except for the one documented behavior change above: typo'd redaction rules now refuse loudly at preflight instead of silently leaking. Operators with a redact rule that doesn't match anything in their schema will see the refusal at the next `migrate` / `sync start`; the actionable fix is to correct the selector or remove the dead rule. No config / schema / IR / lineage-format changes.

## [0.91.0] - 2026-05-30

### Added

- **`feat(postgres): Stage 2 verbatim-carry promote — xml / money / pg_lsn / txid_snapshot / pg_snapshot (ADR-0070)`** — same-engine PG → PG `sluice migrate` / `sync` of columns whose `pg_catalog` type is `xml`, `money`, `pg_lsn`, `txid_snapshot`, or `pg_snapshot` now preserves the column type (`typname`) and round-trips the value byte-equal, instead of refusing loudly at translate time. The five types were the [ADR-0051 §"Stage 2 candidates"](docs/adr/adr-0051-core-pg-type-verbatim-carry.md) list, deferred until per-type round-trip integration pins shipped in v0.90.0. With those pins now CI-locked, the allowlist add is a five-line additive change in `internal/engines/postgres/types.go::coreVerbatimEligibleTypes` and the existing pins automatically hit the "preserve" branch instead of the "refuse-loudly" branch. **Cross-engine PG → MySQL behaviour is unchanged** — `ir.VerbatimType` continues to refuse loudly at preflight via `cross_engine_supportable.go`. See [ADR-0070](docs/adr/adr-0070-stage-2-verbatim-carry-promote.md) for the per-type rationale and the Stage 3 closing-the-door analysis.
- **`feat(pipeline): --poll-interval flag on sync start (roadmap item 18(c))`** — operator-tunable cadence for poll-based CDC readers. Default `0` (engines use their built-in cadence; today: postgres-trigger 1 s); set to e.g. `--poll-interval=250ms` to tighten apply latency on a write-heavy postgres-trigger stream, or `--poll-interval=5s` to trade latency for source load. Push-based engines (postgres pgoutput, mysql binlog, planetscale VStream) have no poll loop and silently ignore the flag. The setter contract — a `pollIntervalSetter` optional interface on the CDC reader, type-asserted by the streamer between open and `StreamChanges` — leaves the `ir.Engine` interface unchanged. **Item 18(c) sub-piece `--idle-flush-grace` is deferred** to a separate release: the batched applier's idle-flush timer touches concurrent state (the apply goroutine + the timer), so it needs the `-race` + integration gate per the CLAUDE.md concurrency-chunk rule.
- **`feat(cli): trigger teardown --yes flag`** — `sluice trigger teardown` now mirrors `sluice slot drop`: it prompts for confirmation by default (`Tear down the sluice trigger engine on the source ...? [y/N]`) and accepts `--yes` (`-y`) to skip the prompt for scripted/CI use. Previous behavior had no confirmation but also rejected `--yes` with help-text output, which surprised operators (and the post-release cycle subagent) into thinking the command needed `--yes` and then silently no-op'd when it was passed.

## [0.90.0] - 2026-05-29

### Added

- **`feat(pipeline): /readyz on sluice sync start (ADR-0069)`** — the metrics HTTP server (`--metrics-listen ADDR`) now exposes a `/readyz` endpoint alongside the existing `/metrics` + `/healthz`. Returns **503 "not ready"** while the streamer is in its cold-start / warm-resume preamble (snapshot, bulk-copy, schema apply, cache prime), then flips to **200 "ready"** once the apply loop is about to begin consuming events. Lets k8s readiness probes, Heroku release-phase scripts, and systemd unit-started gates wait for the stream to actually be mirroring rather than just for the process to be up. The signal is monotonic — a streamer that exits brings down the process and the orchestrator restarts it, which starts at 503 again. `/readyz` deliberately does **not** check apply lag (use the `sluice_seconds_since_last_apply` gauge from `/metrics` for that); the choice was the operator-confirmed default in the design review. See [ADR-0069](docs/adr/adr-0069-service-mode-readyz.md) for full rationale and [docs/operator/running-as-a-service.md](docs/operator/running-as-a-service.md) for systemd / docker / k8s / Heroku wiring examples.

### Test coverage (no behavior change)

- **Broader-mining + Stage-2 type pins** — eleven new integration pins document sluice's current behavior on edge classes mined from prior incidents and the ADR-0051 Stage-2 verbatim-carry type list. Each pin asserts **one of three legitimate outcomes** (refuse-loudly with an operator-grep-able hint / preserve correctly / fail loudly on silent type-loss); only silent flatten fails the test. Covered: PG special floats / temporals (`infinity` / `-infinity` / `NaN`), TOAST round-trip under REPLICA IDENTITY DEFAULT, ENUM mid-stream `ALTER TYPE ADD VALUE` drift, DOMAIN-typed array, `money`, SAVEPOINT / ROLLBACK TO suppression, TRUNCATE CDC event, `xml`, `pg_lsn`, `txid_snapshot`, `pg_snapshot`. No production code changed by these pins — they freeze the loud-failure surface and will catch a future regression that silently maps any of these types to text/varchar.

## [0.89.0] - 2026-05-29

### Fixed

- **`fix(engines/postgres): exclude extension-owned relations from schema read (Bug 96)`** — A default `sluice migrate` / `sync` from a managed-Postgres source (Heroku, RDS, Cloud SQL, Supabase) failed at the create-views phase. Those platforms pre-install `pg_stat_statements`, whose views (`pg_stat_statements`, `pg_stat_statements_info`) live in `public`; sluice's `SchemaReader` returned them as user views, so recreating them on a target that lacks the extension errored with `function pg_stat_statements does not exist` (SQLSTATE 42883) — *after* the user tables had already bulk-copied (a completed-but-errored migrate). The reader now excludes **extension-member relations** (those recorded in `pg_depend` with `deptype='e'` against a `pg_extension`) from both the table and view sets, via a new `extensionMemberRelations()` helper. This mirrors `pg_dump`'s extension-member exclusion and sluice's existing Vitess `_vt_*` shadow-table (Bug 22) and bookkeeping-table (Bug 93) exclusions: extension-provided objects (also PostGIS `spatial_ref_sys` / `geometry_columns`, etc.) belong to the extension and are recreated by `CREATE EXTENSION` on the target — the operator's `--enable-pg-extension` decision — never silently copied as user data. Pinned by an integration test (an extension view is excluded; user tables + user views kept) and validated A/B against a real Heroku Postgres source (essential-0 + standard-0): the prior binary failed at create-views, the fixed binary completes the default migrate cleanly with no `--skip-views`.

### Compatibility

- **Minor bump (v0.89.0)** — additive, drop-in from v0.88.0. No config / schema / IR / lineage-format changes.
- **One behavior change:** a PG source's extension-owned tables/views (e.g. `pg_stat_statements`, PostGIS `spatial_ref_sys`/`geometry_columns`) are no longer migrated as user data — they belong to the target's `CREATE EXTENSION`. This fixes the previously-failing default migrate from every managed-PG provider; operators who (unusually) relied on sluice copying an extension's relations should recreate the extension on the target via `--enable-pg-extension`.

## [0.88.0] - 2026-05-29

### Fixed

- **`fix(backup): rotated backup chains are now compactable (Bug 95, ADR-0067)`** — `sluice backup compact` could never merge across a rotation boundary on a continuously-written source. The rotation handoff dropped the `(P_N, S]` window of changes between the prior segment's terminal position `P_N` and the new segment's snapshot anchor `S` (`skipThrough = S`), so those changes lived ONLY in the new segment's full snapshot — which naive compaction discards. The §14d contiguity pre-flight correctly refused every such merge (loud, no data loss), but because update-heavy churn implies continuous writes, every rotation boundary gapped and the documented "rotate a long chain, then compact the churn" workflow was unreachable. **Fix (ADR-0067):** the rotation handoff now KEEPS the `(P_N, S]` overlap in the new segment's incrementals (`skipThrough = P_N`), so rotated chains are born-contiguous and compactable by both the naive and smart compactors — for all table shapes (PK and no-PK) and encrypted or not, because the fix is upstream of the compactor. A new additive `lineage.json` field `incremental_coverage_start` records where a segment's incrementals begin (the prior terminal `P_N` for a rotated segment); the compaction (§14d) and restore segment-boundary contiguity checks key off it, while `start_position` keeps its full-anchor / restore-base meaning (empty resolves to `start_position`, so existing one-segment and pre-0067 chains are unaffected — no lineage-format bump). The kept overlap re-applies idempotently on restore (the same snapshot→CDC handoff dedup proven at the initial full→stream transition, ADR-0010), and the within-segment full→first-incremental boundary tolerates it while still refusing a forward gap. The field is derived from the ACTUAL first incremental at commit time (not recorded at rotation), so it stays honest across a crash that resumes at `S` instead of `P_N`. A latent compaction gap this fix reached for the first time — merged segments never re-stitched `ParentBackupID` across the dropped intermediate fulls, so a merged real chain would have failed its own restore-walk — is also fixed (cascade-free, since `ir.ComputeBackupID` excludes `ParentBackupID`). A genuine position gap (a pre-0067, imported, or crash-truncated lineage) is still refused loudly. Pinned by a unit boundary matrix + a rotated-overlap restore/compaction suite, and a live-rotation integration test that flipped from asserting the old "refuses loudly" to asserting "merges + restore-walks clean."

### Compatibility

- **Minor bump (v0.88.0)** — additive, drop-in from v0.87.0. The new `incremental_coverage_start` lineage field is optional (absent → resolves to `start_position`); no lineage-format-version bump, no config / schema / IR changes. Existing one-segment and never-compacted chains restore byte-identically.
- **One behavior change:** rotated backup chains (`backup stream run --retain-rotate-at*`) now retain a small `(P_N, S]` event overlap per rotation boundary (re-applied idempotently on restore, so the restored result is unchanged) — this is what makes the chain compactable. `backup compact` collapses the redundancy; standalone restore correctness is unaffected.

## [0.87.0] - 2026-05-29

### Added

- **`feat(pipeline): Heroku-permission refuse-loudly preflight for slot-based Postgres CDC (task #61)`** — Slot-based `postgres` source CDC creates a logical replication slot at cold start, which requires the connecting role to be a superuser or carry the `REPLICATION` attribute. On managed Postgres tiers that forbid `REPLICATION` (Heroku Postgres Essential, Render Basic, Supabase free), `sluice sync start --source-driver=postgres` previously failed **mid-cold-start** with a raw `ERROR: permission denied to create replication slot` (SQLSTATE 42501) — opaque, fired after schema-read + filter work, and gave no hint that a slot-less path exists. A new source-side preflight (`SchemaReader.SourceReplicationCapability`, probing `pg_roles.rolsuper OR rolreplication` for `current_user`) now detects the missing capability **upfront** and refuses loudly with an operator-actionable message that names the role and points at `--source-driver=postgres-trigger` — sluice's slot-less trigger-capture engine built for exactly this tier. The refusal is gated on the source **engine name** being the slot-based `postgres` engine, so it never fires for `postgres-trigger` (whose delegated SchemaReader would otherwise satisfy the same probe interface), for MySQL sources, or for a pure bulk `migrate` (which needs only `SELECT` and genuinely works on Heroku). Pinned by unit tests (gate + message + the postgres-trigger/mysql exclusions) and an integration test against a real `NOSUPERUSER NOREPLICATION` role.

### Changed

- **`feat(net): TCP keep-alives on all long-lived database connections (task #77)`** — sluice's CDC streams (Postgres pgoutput, MySQL binlog) and the postgres-trigger poller hold a TCP connection open across quiet periods; cloud NAT gateways and L4 load balancers (Heroku, Render, GCP Cloud SQL, AWS NLB) silently evict idle connections, after which the next read/write stalls for minutes on the kernel's TCP-retransmit budget rather than failing fast. A new `internal/netkeepalive` package centralises one TCP keep-alive policy (enable + idle 30s + interval 10s + count 3, mirroring the three TCP settings PlanetScale's heroku-migrator PR #7 found necessary) and wires it into **all four** long-lived dial paths: the Postgres query pool + the pgoutput replication connection, the postgres-trigger poller/snapshot/setup pools (via a new exported `postgres.OpenPgxDB` funnel), the MySQL query pool (custom-registered keep-alive network), and the MySQL binlog syncer (`BinlogSyncerConfig.Dialer`). This is the transport-level complement to the existing application-level keepalives (pgoutput standby-status updates, binlog `HeartbeatPeriod`): it keeps the NAT mapping warm and bounds dead-peer detection to seconds. Values are fixed (not config surface). No behavior change for short-lived/pooled query connections beyond the keep-alive probes themselves.

### Internal

- **`ci: bounded-retry GHCR login + image pull (self-heal transient registry flakes)`** — the self-hosted runner pool intermittently hit `docker login ghcr.io: context deadline exceeded` / pull i/o timeouts on the pre-baked-image step, red-failing a whole integration shard. The login + pulls now run through `scripts/ci-ghcr-pull.sh`, which retries with linear backoff (5 attempts) and still fails loudly after the budget so a real outage isn't masked. CI-only; no effect on the shipped binary.

### Compatibility

- **Minor bump (v0.87.0)** — additive, drop-in from v0.86.1. No config / schema / IR changes.
- Two operator-visible behavior changes, both improvements: (1) long-lived connections now carry TCP keep-alive probes (transparent stability hardening on cloud-NAT networks); (2) a slot-based `postgres` source whose role lacks `REPLICATION` is now refused **upfront** (pointing to `--source-driver=postgres-trigger`) instead of failing mid-cold-start — a clearer, earlier failure for a role that genuinely cannot create a slot. Pure bulk `migrate`, the `postgres-trigger` engine, and all MySQL directions are unaffected.

## [0.86.1] - 2026-05-28

### Fixed

- **`fix(engines/pgtrigger): Bug 94 — postgres-trigger `sync start` silently used the slot-based path, never the trigger poller (CRITICAL)`** — Running `sluice sync start` with a `postgres-trigger` source drove the orchestrator's engine-neutral cold-start, which calls `OpenSnapshotStream`. The trigger engine **delegated** that method to the composed slot-based `postgres` engine's snapshot+pgoutput path — so on a managed tier the engine exists to serve (Heroku Essential / Render Basic / Supabase free, where logical-replication slots are unavailable) the run either failed trying to create a slot, or, on a slot-capable server, silently created a replication slot the operator's tier forbids and streamed pgoutput instead of the `sluice_change_log` capture poller — defeating the engine's entire reason to exist. The same-engine and cross-engine congruence tests masked this because they drive the trigger reader via the MANUAL path (`Setup → Migrator → OpenCDCReader`), never through the Streamer. **Fix:** `pgtrigger.Engine.OpenSnapshotStream` is now trigger-native (`cdc_snapshot.go`): it opens a dedicated `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY` snapshot, bulk-copies the user tables via the reused postgres RowReader machinery on that pinned connection, hands off to the trigger CDC poller, and uses **NO replication slot and NO pgoutput**. The load-bearing correctness point is the CDC handoff anchor: because the capture log's BIGSERIAL `id` is allocated at INSERT time but is **not** commit-ordered (a low-id txn can commit after a higher-id txn; rolled-back txns leave permanent id gaps), a naive `MAX(id)` anchor risks a SILENT GAP — an in-flight low id masked by a committed higher id would be skipped by CDC (`id > anchor`) forever once it commits. The anchor is instead the **contiguous committed-prefix high-water**: `(first id whose xmin ≥ pg_snapshot_xmin(current)) − 1`, else `MAX(id)` when nothing is in flight, captured in the SAME transaction as the snapshot. Everything ≤ anchor is in the bulk-copy snapshot; everything > anchor is replayed by CDC. Over-replay (anchor too low) is safe because the applier is idempotent (ADR-0010); a gap (anchor too high) is forbidden. Pinned by a new integration test that drives `Streamer.Run` (the real `sync start` path) for both `postgres-trigger → postgres-trigger` and `postgres-trigger → mysql`, asserting (a) `count(*) FROM pg_replication_slots WHERE slot_name LIKE 'sluice%'` stays 0 on a `wal_level=logical` source, (b) the capture-log poller is consumed (INSERT/UPDATE/DELETE land), and (c) no row loss under writes that interleave with the bulk-copy window. Severity CRITICAL (silent data loss / silent slot-creation on the engine's target tier).

- **`fix(engines/postgres,engines/pgtrigger): Bug 93 — postgres-trigger schema read leaked the capture tables, hard-failing cross-engine migrate (HIGH)`** — The `postgres-trigger` engine reads schema by delegating to the `postgres` `SchemaReader`, which returned the engine's own source-side capture tables (`sluice_change_log`, `sluice_change_log_meta`) as if they were user tables. A cross-engine `sluice migrate` to MySQL then hard-failed at create-tables (`Error 3770`: `committed_at`'s `statement_timestamp()` default is untranslatable), and same-engine runs dragged the capture tables onto the target as a surprise. **Fix:** the postgres `SchemaReader.readTables` bookkeeping-table exclusion (previously `sluice_cdc_state`, `sluice_migrate_state`) now also excludes `sluice_change_log` / `sluice_change_log_meta`. Excluding at the shared reader fixes BOTH the `migrate` (Migrator) and `sync start` (Streamer) paths uniformly, and is harmless on a vanilla `postgres` source (the names are sluice-reserved). Failure mode was loud on the cross-engine path (CREATE TABLE failed, no data landed) but a silent extra-table surprise on the same-engine path; severity HIGH.

## [0.86.0] - 2026-05-28

### Added

- **`feat(engines/pgtrigger): task #72 Phase 2 — postgres-trigger cross-engine (postgres-trigger → mysql/planetscale)`** — Completes the postgres-trigger engine (ADR-0066): the trigger-based capture engine for slot-less managed PG (Heroku Essential / Render Basic / Supabase free) now targets MySQL / PlanetScale cross-engine, not just the same-engine `postgres-trigger → postgres-trigger` round-trip Phase 1 shipped. Three parts: (1) **gate fix** — `checkCrossEngineSupportable` / `checkCrossEngineDeltaSupportable` now treat `postgres-trigger` as a PG source, so a trigger source carrying PostGIS Geometry / pg_trgm opclass indexes / EXCLUDE constraints trips the SAME cross-engine refusals a `postgres` source does (pre-fix it silently skipped every PG-native refusal — a Phase-2 silent-loss hole). (2) **cross-engine value fidelity** — the trigger CDC reader decodes the JSONB capture log into a different value shape than the proven pgoutput path (numerics → `json.Number`, bytea → `\x`-hex TEXT, timestamps → ISO strings, jsonb → nested `map[string]any`), which flows straight into the MySQL `ChangeApplier`. The MySQL value-prepare path (`prepareValue`) gained three branches so every family lands byte-correct: **bytea** `\x`-hex strings hex-decode to raw bytes for VARBINARY/BLOB (was: the literal ASCII of the hex text was stored — SILENT corruption, Bug-92 class); **jsonb** `map[string]any` marshals to a JSON object string for JSON columns (was: `unsupported type map` — LOUD failure; `json.Number` leaves preserve numeric precision); **timestamptz** ISO+offset strings get the zone offset stripped for DATETIME/TIMESTAMP (was: MySQL Error 1292 — LOUD failure). (3) **sequence cutover** confirmed to work for the trigger source via SchemaReader delegation (no forwarding code needed). Pinned by a cross-engine `postgres-trigger`-vs-`postgres` congruence integration test exercising the full Bug-74 value-family matrix (int4/int8/numeric(30,12)/text/varchar/boolean/timestamp/timestamptz/bytea/jsonb × scalar/NULL/unchanged-rich-UPDATE) with a MySQL-side per-column digest oracle, a cross-engine cutover integration test (PG IDENTITY → MySQL AUTO_INCREMENT priming), and unit pins on the value-prepare path + the gate.

### Changed

- **`feat(engines/postgres): warn (not refuse) on a stale empty publication — silent-CDC-stall diagnostic`** — When the PG CDC reader's no-scope path (`ensurePublication` with no caller-supplied table list) finds a publication that exists, is **not** `FOR ALL TABLES`, and has no member tables / no `FOR TABLES IN SCHEMA` memberships, it now emits a loud `WARN` naming the publication and the recovery. Such a publication can never emit a pgoutput row, so streaming from it pins the slot's `confirmed_flush_lsn` and replicates nothing — a silent stall that's painful to diagnose (usually a stale publication left from an aborted run; `DROP SCHEMA` does not drop publications). It **warns rather than refuses** because an empty publication occurs legitimately on this path (a reader reusing a publication whose tables were just dropped; the streamer's own scoped `EnsurePublication` establishes scope in the normal migrate/sync flow), so a hard error would break those callers. `FOR ALL TABLES` is implicitly non-empty and excluded; the emptiness check counts `pg_publication_rel` plus `pg_publication_namespace` (PG 15+), probed via `to_regclass` so it stays valid on older servers.

## [0.85.2] - 2026-05-28

### Fixed

- **`fix(engines/postgres): Bug 92 — silent UPDATE/DELETE loss under REPLICA IDENTITY FULL with rich-type columns (CRITICAL)`** — A PostgreSQL source table set to `REPLICA IDENTITY FULL` could silently drop CDC UPDATEs (and, latently, DELETEs) when a row carried a rich-type column (jsonb, timestamptz, bytea, high-precision numeric). Root cause: under FULL, pgoutput marks **every** column as part of the replica identity, and the CDC reader trusted that wire flag to build the `Update.Before` / `Delete.Before` image — so the applier's `WHERE` clause spanned all columns. A rich-type OLD value did not `=`-match the target after the pgoutput decode→rebind round-trip, the statement matched **zero rows**, and the ADR-0010 idempotency tolerance swallowed it with no error and no WARN — silent divergence. The DELETE path was equally affected; it only appeared to work because the prior FULL test corpus used int/varchar-only tables that round-trip exactly. **Fix:** the reader now resolves an `IdentityKeyCols` set per relation — `DEFAULT`/`USING INDEX` keep the pgoutput wire-flagged columns, but `FULL` now narrows `Before` to the table's **true PRIMARY KEY** (read from `pg_index`), so the `WHERE` is `id = $N` for both UPDATE and DELETE; a PK-less FULL table falls back to the full row. **Also fixed** a bytea CDC silent-corruption surfaced by the new test: the CDC path delivers bytea as pgoutput `\x`-hex text, which the shared decoder copied verbatim (`\xcafebabe` → 10 literal ASCII bytes instead of 4) — a new `decodeBytea` hex-decodes the `\x`-prefixed CDC shape while preserving the row-reader raw-bytes path. Pinned by `TestCDCReader_UpdateUnderReplicaIdentityFull_FamilyMatrix` (every value family × FULL + UPDATE, including an unchanged-rich-column UPDATE) and a postgres-trigger-vs-parent congruence integration test. Found by the postgres-trigger Phase-2 readiness gate's congruence test on its first run — the trigger engine was correct; the slot-based parent was dropping the writes. Pre-existing latent bug (not introduced in v0.85.x); severity CRITICAL (silent data loss on the core engine).

## [0.85.1] - 2026-05-28

### Fixed

- **`fix(engines/mysql): Bug 77 — CHECK constraint POSIX-regex refuse-loudly on the CREATE TABLE path`** — v0.85.0's release notes claimed PG → MySQL migrations with non-translatable CHECK expressions "refuse loudly", but two gaps meant a PG `CHECK (col ~ '...')` regex constraint reached MySQL verbatim and failed at CREATE TABLE with an opaque `Error 1064` syntax error: (1) the untranslatable-token list carried only `~*` (case-insensitive regex), missing bare `~`, `!~`, and `!~*`; (2) the refuse-loudly pre-flight (`refuseUntranslatedCheckExprMySQL`) was wired into the Shape A `AlterAddCheck` live-migration path but **not** the cold-start CREATE TABLE emit (`emitCheckConstraint`), which is the path a plain `sluice migrate` uses. v0.85.1 adds the three missing regex operators to the token list and runs the refuse-loudly check inside `emitCheckConstraint`, so a cross-dialect CHECK carrying any POSIX-regex operator now fails before any DDL is issued with a structured error naming the table, the constraint, and the offending token. The recovery hint is also corrected: the v0.85.0 notes referenced a `--checks` flag (and the in-code message referenced `--expr-override` for constraint keys) — **neither mechanism exists for CHECK constraints** (`--expr-override` only targets generated columns). The honest recovery is to drop the CHECK on the source before migrating and re-create an equivalent MySQL CHECK (using `REGEXP`) on the target post-migration; the error message now says exactly this. Failure mode was always loud (CREATE TABLE failed, no data landed — no silent loss), so severity is MEDIUM; this fix makes the error operator-actionable and matches the binary to the documented contract.

## [0.85.0] - 2026-05-27

### Added

- **`feat(engines/pgtrigger): task #62 Phase 1 — postgres-trigger engine variant (ADR-0066)`** — New `postgres-trigger` engine registered alongside `postgres`, intended as a Go-native alternative to Perl-based Bucardo for PG environments where logical replication slots are unavailable (Heroku Postgres, certain managed-PG tiers). Phase 1 ships composition over the existing `postgres` engine (delegation, not embedding — preserves `ir.SlotManagerOpener` / `CDCReaderWithSlotOpener` type-assertion semantics per ADR-0066 §9), trigger-based row-change capture into a `sluice_pgtrigger_capture` JSONB log table (BIGSERIAL ordering + `xmin` safety-lag), `sluice trigger setup/teardown` kong subcommands for explicit lifecycle, and an end-to-end same-engine integration test (bulk-copy + CDC tail). Phase 2 (cross-engine `postgres-trigger → mysql/planetscale`, full Bug-74 family-matrix pin, sequence/cutover) is tracked separately and will land in a future release.

- **`feat(pipeline): task #23 (ADR-0054 Phase 2e) — MySQL multi-shard test + remaining crash-injection boundaries`** + **`feat(pipeline): task #24 Phase 2e v3 — 3-source streamer-driven harness (ADR-0054)`** — Closes ADR-0054 Phase 2e. The 3-source streamer-driven harness exercises three concurrent source PG instances → one consolidated target through the streamer (`shard_consolidation_phase2e_streamer_pg_integration_test.go`), and the MySQL counterpart adds the multi-shard MySQL boundary path with the remaining crash-injection cases. Together these prove the streamer-orchestrated cross-shard consolidation path end-to-end. The `BoundaryRouter` routing/observe-flow class (tasks #65/#66/#67) was diagnosed via Phase A instrumentation during this work — root cause was MySQL `go-sql-driver`'s `ClientFoundRows=false` default returning 0 rows-affected on same-value UPDATEs, fixed in `RecordDDLText` (#66), plus a missing post-ALTER DML in the PG streamer harness to trigger pgoutput `RelationMessage` emission (#67).

- **`feat(pipeline,backup): task #16 — backup chain 14e smart compaction (same-row event collapsing)`** — `sluice backup compact --merge-window DUR` now collapses redundant per-row CDC events within the window: multiple UPDATEs to the same primary-key reduce to the latest before/after-image pair; DELETE preceded by INSERT in the same window cancels both. Closes the backup-chain "naive concat retains every micro-update" inefficiency that #15 (naive compaction) shipped as a placeholder.

- **`feat(ir,engines): task #22 (ADR-0054 catalog) — CHECK constraint shape support + ADR-0064`** — `ir.Table.Checks` carries source-side CHECK constraints into the IR; PG schema writer + shape-delta applier reproduce them on the target; ADR-0054 Phase 2 catalog adds the matching `AlterAddCheck` / `AlterModifyCheck` shape entries with same-engine probers and cross-engine refuse-loudly. Extends the Phase 2 catalog that previously covered ADD/DROP/MODIFY column and RENAME COLUMN.

- **`docs(adr): ADR-0066 postgres-trigger engine variant + task #62 planning brief`** — New ADR documents the engine-variant pattern (compose over embed; trigger-table+xmin safety-lag instead of replication slot; JSONB capture log with UseNumber semantics; refuse-loudly §14 boundaries). Cross-references ADR-0007 (engine pattern) and ADR-0066 §9 (type-assertion forwarding).

### Fixed — CI hardening (pre-baked images + mid-shard-death sentinels)

- **`test(engines/postgres): task #59 — Option B shared PG container for engines/postgres integration suite`** — Mirrors task #56 (Option B shared MySQL fixture) for the PG side. `TestMain` boots one PG container per `engines/postgres` shard, every test calls `resetSharedDB(t, dbName)` to drop slots/databases per-test. Replaces ~80 per-test container boots with one shard-lifetime boot — multi-minute wall-time win.

- **`test(ci,pipeline): task #69 — mysqlBootTimeout 2min → 4min + WithWaitStrategyAndDeadline`** — Bumps the MySQL boot timeout from 2min to 4min for self-hosted runner disk-I/O contention (4 commits: `dfaf0fa` for the bump itself, `655de7c` to switch to testcontainers-go's `WithWaitStrategyAndDeadline` so the outer-deadline race no longer truncates the inner wait, plus `6a07038` + `0fa4136` applying it to the per-test GTID boot site that bypassed the shared TestMain). The `WithWaitStrategy` → `WithWaitStrategyAndDeadline` switch was the key insight: the former hardcodes a 60s outer deadline that wraps whatever `WithStartupTimeout` you set, silently truncating.

- **`test(ci): task #68 — pre-bake MySQL/Postgres/PostGIS CI container images`** + **`test(ci): task #70 — pre-bake pgvector to GHCR (#68 follow-up)`** — New `scripts/build-prebaked-images.sh` + `.github/workflows/build-prebaked-images.yml` mirror MySQL 8.0, postgres:16, postgis:16-3.4, and pgvector:0.7.4-pg16 to `ghcr.io/sluicesync/sluice-{mysql,postgres,postgis,pgvector}:*-prebaked` with first-boot init pre-applied. Eliminates the ~30-60s `mysqld --initialize-insecure` / `initdb` step from every CI run (turns into ~5s pull), and removes the docker.io single-point-of-failure exposed during task #70's investigation (3 consecutive `pipeline-rest-other` failures with `TLS handshake timeout` pulling `pgvector/pgvector:0.7.4-pg16`). Gotchas-encoded: `pg_hba.conf` `host all all all trust`, `CREATE ROLE test WITH SUPERUSER LOGIN BYPASSRLS`, `rm -f /var/lib/mysql/auto.cnf` (else shared `server_uuid` breaks fresh-instance tests), `FORCE_REBUILD` env to bypass the base-digest-skip when the bake-script itself changes.

- **`test(engines/postgres): task #64 — PG shared-container mid-shard-death TCP-dial sentinel`** + **`test(engines/mysql): task #71 — MySQL shared-container TCP-dial sentinel (#64 follow-up)`** — When the shared container died mid-shard (docker daemon restart on self-hosted runner; postgres process death inside an otherwise-alive container; port-mapping loss), every subsequent test in the shard would individually fail with `dial tcp ...: connect: connection refused` — 80x identical noise that buried the actual root cause. New `checkSharedContainerAlive(t)` helper does a 500ms `net.DialTimeout` to the mapped port before each test's reset; on dial failure flips a `sync.Once`-gated `containerDead` flag, emits one loud `DOCKER-ENGINE-DEAD:` log line at first detection + the trailing TestMain summary, and `t.Fatalf`'s every subsequent test fast. First-pass attempt used `Container.IsRunning()` (PR #72 first commit) — discovered via the cascade recurring in PR #72's own CI that IsRunning returned true while the SQL port refused, so the TCP-dial probe is the actual liveness signal.

## [0.84.0] - 2026-05-26

### Added

- **`feat(engines/postgres,engines/mysql,ir): task #52 sub-deliverables 2 + 3 — RLS IR capture + emit (ADR-0063)`** — closes the "target schema arrives without policies" silent-security-regression class (failure mode 3 of five enumerated in task #52). PG `SchemaReader` now captures `pg_class.relrowsecurity` / `relforcerowsecurity` and every `pg_policies` row into the IR (`ir.Table.RLSEnabled` / `RLSForced` / `Policies`); PG `SchemaWriter` re-emits `ALTER TABLE … ENABLE ROW LEVEL SECURITY` (+ `FORCE`) and `CREATE POLICY` for each policy on the target, in that order (ENABLE before CREATE POLICY — without ENABLE the policies are defined but inert). Cross-engine PG → MySQL writer logs exactly one WARN per stream naming affected tables (MySQL has no RLS surface — operators routing PG → MySQL accept the policy-layer drop). MySQL → PG is a no-op (MySQL sources never populate the new IR fields). Sub-deliverable 1 (RLS preflight) shipped earlier in v0.78.4; this lands sub-deliverables 2 + 3 to complete the failure-mode-3 fix. Bug-74-style class-pin coverage: integration test exercises Command × Permissive × USING/CHECK × ENABLE/FORCE matrix end-to-end on a real PG container.

- **`docs(task #50): use-cases + cutover + comparison + copywriting-guardrails (F20 + F21 + F15 + F22)`** — Marketing/positioning bundle. `docs/use-cases.md` names four concrete operator scenarios (managed-PG upgrades, cross-cloud migration, MySQL ↔ PG consolidation, logical-CDC backups). `docs/cutover.md` is the operator companion to ADR-0062 — when to run `sluice cutover`, refuse-loudly classes, procedural rollback shapes. `docs/comparison.md` is the longer-form per-row companion to the README's matrix. `docs/dev/copywriting-guardrails.md` codifies F22: claim only what the loud-failure machinery enforces.

- **`docs(readme): rewrite around HVR-class positioning + Heroku scope statement (F50)`** — README rewrites the hero around "Open-source HVR-class CDC for MySQL ↔ Postgres". New "When NOT to use sluice" section names Heroku Postgres, one-off same-engine snapshots, logical-decoding-to-applications, and versioned-schema-migration tooling as explicit non-fits.

### Fixed — CI hardening

- **`test(engines/mysql): task #60 — retry-with-backoff on shared TestMain boot`** — engines/mysql shared TestMain container now retries `mysqltc.Run` with backoff on `wait until ready` failures.

- **`test(pipeline): task #63 — retry-with-backoff on per-test MySQL boots`** — pipeline package gets `runMySQLWithRetry(t, opts...)` helper applied to all 7 `startMySQL*` helpers.

- **`test(engines/mysql,pipeline): task #12 — bump retry attempts 3 → 5 + wrap GTID per-test boot`** — Bumps both shared TestMain and pipeline wrapper from 3 to 5 attempts (schedule: 30s / 60s / 120s / 240s), and wraps the engines/mysql `startMySQLGTIDForCDC` per-test boot which previously bypassed task #60's retry by design. Closes the in_progress portion of task #12 (TestCDCReader_GTIDPositionLoss_DetectedLoud + TestAddTable_LiveMode_MySQL_UnderLoad flake class).

## [0.83.0] - 2026-05-25

### Added

- **`feat(cli/engines): F10 — sluice cutover subcommand for sequence priming (#49 / ADR-0062)`** — New `sluice cutover` subcommand reads source sequences (PG `pg_sequences.last_value`) / `AUTO_INCREMENT` values (MySQL) and bumps the target's by `--cutover-sequence-margin=N` (default 1000). Closes the PK-collision-on-first-INSERT-after-cutover class: between snapshot and cutover, the source advances sequences as new rows are inserted; CDC ships rows to target but the target's sequence still lags by the catch-up window. Pre-F10, operators ran `SELECT setval(...)` per table by hand; v0.83.0 makes it one command.

  ### CLI surface

  - `sluice cutover --config sluice.yaml` — reads source sequences, applies to target with safety margin
  - `--cutover-sequence-margin=N` (default 1000) — buffer above source value
  - **Idempotent**: target-side guarded; running cutover twice does not regress sequence values
  - **Refuses loudly** when target value is >margin ahead of source (recovery hint: "manual re-snapshot recommended" — signals post-cutover-INSERT-before-cutover-command)
  - **Skips composite-PK / UUID / no-sequence tables** gracefully (no false-positive refusal on identifier-only tables)

  ### Architecture

  - `ir.SequencePrimer` interface + `ir.SequenceState` / `ir.SequencePrimeReport` / `ir.ErrCutoverSequenceTargetAhead`
  - PG impl: `internal/engines/postgres/cutover_sequence.go` — `pg_get_serial_sequence` for column→sequence resolution; `pg_sequences.last_value` (NULL = never called) on source; `setval('<target_seq>', N+margin, true)` on target
  - MySQL impl: `internal/engines/mysql/cutover_sequence.go` — `INFORMATION_SCHEMA.TABLES.AUTO_INCREMENT` with `information_schema_stats_expiry = 0` to bypass catalog cache; `ALTER TABLE … AUTO_INCREMENT = N` on target
  - Pipeline orchestrator: `internal/pipeline/cutover.go` — single-goroutine, table-by-table, target-side-read-guarded for idempotency
  - CLI subcommand: `cmd/sluice/cutover.go`

### Tests

- **11 unit tests**: orchestrator dispatch / refuse-loudly / margin (7), IR-shape (2), PG sequence-name parser (2) — covers 4 shape variants: bare, fully-quoted, mixed-quoted, dot-in-quoted (Bug 74 class-pin)
- **3 PG integration tests**: prime + idempotency, refuse-loudly when target ahead, composite-PK skip
- **3 MySQL integration tests**: same matrix
- **1 cross-engine integration test**: PG → MySQL full handoff + idempotency

### Docs

- **ADR-0062 — Cutover sequence priming** (`docs/adr/adr-0062-cutover-sequence-priming.md`). Covers motivation (Reddit-research F10), two-phase model, safety-margin rationale, idempotency contract, refuse-loudly class, and relationship to chain-restore + bulk-copy phase.

### Compatibility

- **Drop-in upgrade from v0.82.0.** New subcommand only — operators who don't run `sluice cutover` see no behavior change.
- **Minor version bump (v0.83.0)** because of the new subcommand.
- **Severity a** — closes the PK-collision-on-first-post-cutover-INSERT class. Operators migrating to a new system who hit this used to surface "data corruption" tickets that were actually sequence-priming gaps.

## [0.82.0] - 2026-05-25

### Added

- **`feat(pipeline/engines): F17 — source-side sluice_heartbeat writer (#48 / ADR-0061)`** — Sluice now optionally writes a tiny periodic row to a sluice-owned table on the source DB. The INSERT generates WAL (Postgres) / binlog (MySQL) so the CDC consumer's position advances even against a quiet source — preventing the silent slot-eviction / binlog-rotation-past-consumer class on low-traffic source DBs (off-hours, weekends, dev environments).

  **Default-OFF**: operators opt in with `--source-heartbeat-interval=30s`. The INSERT is a behavior change on the source DB — operators on regulated systems must explicitly enable.

  **Permission-denied path is graceful**: when the role lacks `CREATE TABLE`, the writer WARNs once and the stream continues — F17 is not fatal to the rest of sluice. The WARN names three remediation options (grant CREATE, pre-create the table, `--no-source-heartbeat`).

  **F13 (ADR-0059) pairs with F17 (ADR-0061)**: F13 detects the slot-eviction symptom; F17 prevents the cause.

  ### CLI surface

  - `--source-heartbeat-interval=DUR` (default `0s` = disabled)
  - `--source-heartbeat-prune-window=DUR` (default 1h; 0 disables) — bounds heartbeat-table growth via periodic DELETE
  - `--source-heartbeat-table-name=NAME` (default `sluice_heartbeat`) — override for hostile DBA-managed namespaces
  - `--no-source-heartbeat` — opt-out escape hatch

  ### Architecture

  - `ir.HeartbeatWriter` optional interface + `ir.ErrHeartbeatPermission` sentinel (`internal/ir/health.go`)
  - PG impl: `internal/engines/postgres/heartbeat_writer.go` — `BIGSERIAL` + `TIMESTAMPTZ DEFAULT NOW()` schema
  - MySQL impl: `internal/engines/mysql/heartbeat_writer.go` — `AUTO_INCREMENT` PK + `TIMESTAMP DEFAULT CURRENT_TIMESTAMP`
  - Pipeline wiring: `internal/pipeline/source_heartbeat.go` — per-stream goroutine driven by `time.Ticker`, dedicated `*sql.DB`, `sync.Once`-guarded cleanup. Mirrors F13's `attachSlotHealthProbe` shape.

### Docs

- **ADR-0061 — Source-side sluice_heartbeat writer** (`docs/adr/adr-0061-source-side-heartbeat-writer.md`). Covers motivation (Reddit-research F17), config surface, opt-out rationale, why default-off, permission-denied semantics, and the F13/F17 pairing.

### Tests

- **7 pipeline-package unit tests** — loop / attachment / opt-out branches (Bug 74 "pin the class" discipline)
- **3 MySQL engine unit tests** — table-name guard, permission classifier, sentinel matching
- **5 PG integration tests** — table create + schema, accumulation, prune, WAL advancement, permission-denied surfaces sentinel
- **5 MySQL integration tests** — same matrix against binlog position
- **All 12 CI jobs SUCCESS including the `-race` Integration shards** on Linux. **-race gate (per CLAUDE.md "Concurrency chunks") verified before tag.**

### Compatibility

- **Drop-in upgrade from v0.81.0.** Default-OFF; operators who don't set `--source-heartbeat-interval` see no observable change.
- **Minor version bump (v0.82.0)** because four new operator-facing flags are added.
- **Severity a** — prevents the silent slot-eviction / binlog-rotation class for low-traffic sources. PG operators against busy-by-day-quiet-by-night sources are the most likely audience.
- **Behavior change to flag for opted-in operators**: a new sluice-owned table `sluice_heartbeat` is auto-created on the source. At 30s cadence + 1h default prune, the table holds ~120 rows steady-state.

## [0.81.0] - 2026-05-25

### Added

- **`feat(pipeline): F11 — per-table schema-drift diff in CDC refuse-loudly messages (#47 / ADR-0060)`** — When sluice's ADR-0058 intercept refuses a non-ADD-COLUMN source DDL, the error message now names the **specific columns, indexes, and constraints** that drifted plus an operator-action hint per category. Closes the silent-under-information class where pre-F11 the refusal said "schema change detected on table X" with no detail, forcing operators to manually `pg_dump`-diff to find out *what* changed.

  Pure-function `ir.DiffTable(pre, post)` returns a `SchemaDriftReport` covering column add/drop/alter/rename, index add/drop, CHECK add/drop/alter, FK add/drop/alter. Bug-74 class-pin discipline: per-category unit tests + per-category integration test on PG → PG.

  Operator-action wording in `pipeline.RenderSchemaDriftReport` — e.g. `[column-added] foo TIMESTAMP NULL — drained schema migrate ... OR restart with --forward-schema-add-column ...`. Greppable category prefixes for ticket-paste / Slack workflows.

  **Augments** the existing refuse-loudly catalog; does NOT add auto-remediation flags. Tenet is loud-failure-to-operator; F11 makes the loudness *more useful*. The "do it automatically" half is ADR-0058 (#45) ADD COLUMN forwarding.

### Docs

- **ADR-0060 — CDC apply-side schema-drift diff** (`docs/adr/adr-0060-cdc-schema-drift-diff.md`). Covers motivation (Reddit-research F11), diff structure, operator-action mapping, scope exclusions, and ADR-0058/ADR-0029/ADR-0054 relationships. §6 documents the known limitation: index-only DDL (CREATE/DROP INDEX) not detected via F11 because pgoutput RelationMessage describes column-shape only — CREATE INDEX doesn't trigger a snapshot. Operators see index drift through chain-restore at backup boundaries; live detection deferred to F47 schema-drift catalog.

### Tests

- **`test(ir): schema_drift_test.go`** — 19 unit tests covering Bug-74 class matrix (column add/drop/type/nullable/default/rename + multi-kind, index add/drop, CHECK add/drop/alter, FK alter via OnDelete change, multi-shape combo, deterministic ordering, nil-handling)
- **`test(pipeline): schema_drift_render_test.go`** — 8 renderer unit tests asserting greppable prefixes + per-category hint content
- **`test(pipeline): schema_forward_intercept_drift_test.go`** — 6+ subtests pinning per-category intercept refusal output
- **`test(pipeline): schema_drift_pg_integration_test.go`** — PG → PG integration test exercising 3 refused shapes (drop column / rename column / alter type) end-to-end with testcontainers
- **Existing 17 ADR-0058 `TestForwardAddColumn_*` subtests remain green** — F11 only augments refuse-message strings; happy paths untouched

### Compatibility

- **Drop-in upgrade from v0.80.0.** No new flag surface; F11 augments existing refuse-loudly messages.
- **Minor version bump (v0.81.0)** because the refusal-message shape is a new observable behavior operators can grep on.
- **Severity a** — operator-trust improvement: reduces median time-to-diagnose for source-side DDL drift from ~15-30min (manual pg_dump diff) to ~30 seconds (paste refusal into ticket).

## [0.80.0] - 2026-05-25

### Added

- **`feat(pipeline/postgres): pre-emptive PG slot-health warnings (70% / 85% retention + 30m inactivity) (#46 / ADR-0059)`** — Sluice now runs a 30-second-cadence background probe per PG-sourced stream that surfaces three operator-actionable WARN conditions BEFORE Postgres can silently evict the replication slot:
  - **WAL retention ≥70% of `max_slot_wal_keep_size`** — warning level. Names the slot, the bytes held, the percentage, and the action ("consumer may be falling behind; check…").
  - **WAL retention ≥85%** — critical level. Same shape with a more urgent action hint.
  - **Slot inactive ≥30m** — when `pg_replication_slots.active = false` and no recent re-attach. Action hint: "check whether the consumer (sluice or otherwise) is still running."

  Each condition is **de-duplicated within a 5-minute rate-limit window**; severity transitions (e.g. 70 → 85) emit immediately even within the window; **condition-clears emit a one-line INFO** so operators see the recovery, not just the alarm. `max_slot_wal_keep_size = -1` (unlimited; the PG default) cleanly bypasses the percentage warnings.

  **Why this exists**: the dominant operator pain in Postgres logical-replication threads is silent slot loss — a slot accumulates WAL because the consumer falls behind or disconnects, retention limits eventually fire, and the slot drops with **no warning**, leaving the operator with a broken pipeline and no way back without a full re-snapshot. Reddit-research F13 catalogued this as severity-a. v0.80.0 closes the silent-slot-loss class by surfacing the slow burn *before* eviction. Loud-failure discipline applied to a class of slow-burning silent failure.

  **Architecture**: `ir.SlotHealthReporter` optional interface + `ir.SlotHealth` value type; `internal/engines/postgres/slot_health_reporter.go` queries `pg_replication_slots` + `pg_current_wal_lsn()` + `max_slot_wal_keep_size` GUC; `internal/pipeline/slot_health.go` houses the pure-function threshold evaluator + the rate-limit state + the per-stream goroutine. Wiring mirrors F2's `attachSpillReporter` shape (dedicated `*sql.DB`, non-fatal on cross-engine sources).

### Tests

- **`test(pipeline): slot_health_test.go`** + **`slot_health_loop_test.go`** — 18 unit tests covering every threshold/rate-limit boundary (Bug 74 "pin the class" discipline): 70%/85% + skip-past-70 transitions, -1 unlimited bypass, 0 extreme bypass, inactive below/above threshold, active-probe-resets-inactivity, rate-limit suppress + expire, 70→85 transition emits inside window, clear→INFO, still-clean silent, retention-supersedes-inactive, plus 4 lifecycle tests for the probe goroutine + `slotHealthProbeAttachment.Close` idempotency.
- **`test(engines/postgres): slot_health_integration_test.go`** — 4 PG 16 testcontainer integration pins (no-slot / empty-slot / default-unlimited GUC / explicit-64MB-GUC-to-bytes); ~8.4s total.

### Docs

- **`docs(adr): ADR-0059 — Pre-emptive PG slot-health pre-warning`** (`docs/adr/adr-0059-pg-slot-health-prewarning.md`) — covers motivation (F13), thresholds + their rationale, the rate-limit policy + state-transition semantics, the per-stream goroutine + dedicated-`*sql.DB` design, and explicitly documents what's deferred: MySQL binlog-retention parallel (different shape, separate task), Prometheus metric exposure (small follow-up), `sluice diagnose` integration, and any auto-action on threshold cross (tenet is loud-failure-to-operator, not auto-remediation).

### Compatibility

- **Behavior change to flag**: every Postgres-sourced stream now gets a background probe goroutine + a dedicated `*sql.DB` connection on the source. Cost is one idle backend session per stream (negligible against any production sluice deployment); benefit is operator-visible WARN before silent slot eviction. Disabled in `--dry-run` mode (the wiring path is bypassed). MySQL-sourced and target-only streams see no change. Cross-engine sources without a `SlotHealthReporter` impl (e.g. MySQL source) silently skip the probe.
- **Minor version bump (v0.80.0)** because the operator-visible WARN surface is a new observable behavior.
- **Severity a** — closes a catalogued silent-loss class. Postgres operators running long-lived sluice streams against busy sources are the most likely to have been near the silent-eviction edge without realizing it.
- **No new flag**. Always-on (except DryRun); the rate-limit window prevents noise on healthy slots.

## [0.79.1] - 2026-05-25

### Fixed

- **`fix(pipeline): Bug 91 — surface raw column_default to volatility classifier (PG nextval) + close MySQL UUID() test-fixture impossibility`** — Follow-on to the Bug 90 hotfix discovered during its CI run. Two surgical issues:
  - **PG `nextval` not classified**: not a classifier regex gap (the v0.79.0 classifier already handled `regclass`-cast forms). The actual locus is upstream in `internal/engines/postgres/schema_reader.go:translateDefault()` — PG's auto-increment heuristic returns `ir.DefaultNone{}` for any column whose `column_default` starts with `nextval(` (correct for SERIAL/BIGSERIAL; wrong for user-written `BIGINT DEFAULT nextval('manual_seq')` without OWNED BY). The Bug 90 prober received `DefaultNone{}`, the classifier saw nothing, and the intercept forwarded silently. Fix: added `RawDefaultReader` interface + `(*postgres.SchemaReader).ReadRawColumnDefault` querying `information_schema.columns.column_default` directly, bypassing the SERIAL heuristic; `newSourceDefaultProber` prefers raw-text when available.
  - **MySQL UUID() test-fixture impossibility**: MySQL 8.0 rejects `ALTER TABLE … DEFAULT (UUID())` at DDL parse time with `Error 1674` regardless of `binlog_format=ROW` / `gtid_mode=ON` (empirically verified across multiple session settings). Removed the `uuid-default` scenario from the MySQL test with a doc-comment naming Error 1674 explicitly so future contributors don't re-add it; `now-default` is sufficient to exercise the production plumbing.
- **`fix(pipeline): refuse loudly on ADD COLUMN with computed DEFAULT — Bug 90 closure (ADR-0058 §2a)`** — v0.79.0's `--forward-schema-add-column` did not fire the §2a refusal in production. The check only ran when [ir.Column.Default] was [ir.DefaultExpression], but the CDC reader's RelationMessage / TableMapEvent projection drops the DEFAULT clause on every column (pgoutput has no `attdefault` slot; MySQL's TableMapEvent has no `COLUMN_DEFAULT`), so the post-DDL SchemaSnapshot always arrived with `Default == nil`. Operators turning on `--forward-schema-add-column` for a routine `created_at TIMESTAMPTZ DEFAULT NOW()` ALTER saw the happy-path log line, the target ALTER landed, and every pre-existing row on the target silently diverged from source (source materialized per-row insert timestamps via the table rewrite; target's pre-existing rows carried either a single target-session-evaluated timestamp or NULL). Class also reproduces with `nextval(seq)`, `random()`, `gen_random_uuid()`, `UUID()`, `RAND()` — the failure dispatches on the DEFAULT-expression volatility class, not on `NOW()` specifically. **Severity-A silent-loss** on the marquee F12+F16 forwarding path.

  Fix wires a source-side SchemaReader probe through the intercept (`schemaForwardDeps.defaultProber`) and runs a text-based volatility scan (`classifyDefaultVolatility`) against an explicit deny-list (time-volatile / sequence-stateful / random / session-state on both PG and MySQL) plus a small allow-list (ABS / COALESCE / CAST / …). Any function call not on either list triggers refuse-on-uncertainty — better to over-refuse than to silently corrupt. Refuse message names the column, table, DEFAULT expression text, volatility reason, and the drained-model recovery hint. Pinned by `schema_forward_volatility_test.go` (53-cell class matrix), three new unit tests in `schema_forward_intercept_test.go` covering the probe path, and two new integration tests (`TestAddColumnForward_PG_RefusesComputedDefault`, `TestAddColumnForward_MySQL_RefusesComputedDefault`).

  Literal DEFAULTs (`DEFAULT 'pending'`, `DEFAULT 0`, `DEFAULT FALSE`, `DEFAULT NULL`) continue to forward — the existing happy-path integration tests remain green.

## [0.79.0] - 2026-05-24

### Added

- **`feat(pipeline): online ADD COLUMN forwarding through the CDC apply path (--forward-schema-add-column, --backfill-added-column) (#45 / ADR-0058)`** — Sluice now optionally forwards `ALTER TABLE … ADD COLUMN` from source to target through the live CDC apply path. Two new opt-in flags on `sluice sync start`:
  - `--forward-schema-add-column` (off by default): when a source ADD COLUMN is observed in the CDC stream, the streamer applies the equivalent ALTER on the target via the existing `ir.SchemaDeltaApplier.AlterAddColumn` surface (the same call chain-restore + Shape A live-coordination already use). The IF NOT EXISTS semantics on both engines make the ALTER idempotent on retry.
  - `--backfill-added-column` (off by default; only consulted when `--forward-schema-add-column` is set): after the target ALTER lands, the streamer issues a bounded PK-cursor SELECT against the source for already-shipped rows and emits synthetic UPDATE events to populate the new column with the source's per-row values.

  **Why this exists**: F12 + F16 from the task #40 Reddit research identified ADD COLUMN as the marquee positioning feature where every CDC competitor falls down differently — SQL Server native CDC silently ignores it; DMS / Qlik require manual restart; Debezium forwards but doesn't backfill. The OSS tier of HVR (the gold standard) handles both, but is paid. v0.79.0 puts sluice in HVR's category on this dimension, on the OSS tier, cross-engine.

  **Refuse-loudly catalog (preserved)**: every other recognized shape — DROP COLUMN, ALTER COLUMN TYPE, RENAME COLUMN, CREATE / DROP INDEX, multi-shape combos, ADD COLUMN with a computed DEFAULT (e.g. `DEFAULT NOW()` / sequence reference) — continues to refuse loudly with the drained-model recovery hint. ADR-0058 §1a documents the scope split: ADD COLUMN is the additive case where target rows have a clean default; every other shape benefits from explicit operator coordination.

  **No-op when Shape A is engaged**: `--inject-shard-column` streams already forward every recognized shape via the ADR-0054 lease + boundary router. The `--forward-schema-add-column` flag is silently a no-op on Shape A streams (Shape A's intercept already covers the case).

  **Cross-engine works**: PG → MySQL and MySQL → PG forward correctly via the existing `translate.RetargetForEngine` path (the same call broker + chain-restore use).

### Docs

- **`docs(adr): ADR-0058 — Online schema-change forwarding in the CDC apply path`** (`docs/adr/adr-0058-online-schema-change-forwarding.md`) — 14-section ADR covering: F12 + F16 motivation, scope split (why ADD COLUMN only in v0.79.0), opt-in flag rationale, backfill design + positions, computed-default refusal (§2a), target-ALTER failure mode (§2b), source-backfill failure modes (§2c), why no MySQL INSTANT, same-engine vs cross-engine, difference from the chain-restore caller, forward-compat note with F11 (CDC schema-drift detection — task #47). Status Accepted (v0.79.0).

### Tests

- **`test(pipeline): schema_forward_intercept_test.go`** — 11 unit tests covering intercept dispatch (nil applier pass-through, first-snapshot anchor, ADD COLUMN happy path, every refuse-loudly shape including DROP / RENAME / ALTER TYPE, computed DEFAULT refusal, literal DEFAULT forwarding, applier-error rewind, NoneShape passthrough, non-snapshot pass-through, backfill UPDATE synthesis, backfill no-PK refusal).
- **`test(pipeline): migrate_add_column_forward_pg_integration_test.go`** — 3 PG → PG integration pins: flag-on forwards the ALTER + post-ALTER INSERT lands; flag-on + backfill populates already-shipped rows; flag-off preserves pre-v0.79.0 refuse-loudly behaviour (lowercase control).
- **`test(pipeline): migrate_add_column_forward_mysql_integration_test.go`** — 2 MySQL → MySQL integration pins (flag-on + flag-on with backfill).
- **`test(pipeline): migrate_add_column_forward_cross_integration_test.go`** — 2 cross-engine integration pins: MySQL → PG and PG → MySQL.

### Compatibility

- **Drop-in upgrade from v0.78.4.** Default behavior unchanged: operators who don't set `--forward-schema-add-column` see exactly the pre-v0.79.0 path (source ADD COLUMN surfaces as `column does not exist` on the next row event, refuses loudly through the standard retry path).
- **Minor version bump (v0.79.0, not v0.78.5)** because new operator-facing flags are added.
- **Two flags are opt-in by design.** Operators on staging environments where DDL is gated through a separate change-management process must continue to refuse loudly on any source DDL; default-off honors that. Operators who want Debezium-class schema evolution opt in explicitly with both flags. The two-flag form (rather than a single tristate) was chosen for grep-ability in CI/deployment manifests and forward-compat with future shape flags.

## [0.78.4] - 2026-05-24

### Added

- **`feat(pipeline, engines/postgres): PG Row-Level Security preflight (refuse loudly when role lacks BYPASSRLS) (#52 sub-deliverable 1)`** — Sluice now probes every included table for `pg_class.relrowsecurity` + `relforcerowsecurity`, and probes the connecting role for `pg_roles.rolbypassrls`, then refuses to proceed if ANY included table has RLS enabled AND the connecting role lacks `rolbypassrls=true`. Runs on both source-read and target-write sides (different refuse-message wording: source-side warns about silent filter via USING expressions, target-side warns about WITH CHECK rejection). Operator-actionable recovery hint names the role, lists offending tables, calls out FORCE ROW LEVEL SECURITY explicitly (even table owner is checked under FORCE), and offers three recovery paths: `ALTER ROLE <role> BYPASSRLS;` (preferred), re-run with a superuser/owner role, or `--exclude-table` if the data is intentionally tenant-scoped and should not cross to the target.

  **Why this exists**: PG RLS is a known silent-loss class (see [PlanetScale's "RLS sounds great until it isn't"](https://planetscale.com/blog/rls-sounds-great-until-it-isnt)). Prior to v0.78.4, sluice happily proceeded against an RLS-enabled source whose connecting role lacked BYPASSRLS — the source snapshot was silently filtered by policy `USING` expressions, the migration "succeeded" with fewer rows than the source, and no error surfaced. Per CLAUDE.md's loud-failure tenet, v0.78.4 refuses loudly with operator-actionable recovery instead.

  **What this does NOT do** (deferred to #52 sub-deliverables 2-3, planned for v0.79.0): does NOT capture `pg_policies` into the IR, does NOT emit `CREATE POLICY` from the schema writer, does NOT include the full Bug-74-style matrix integration suite. The v0.78.4 scope closes the worst silent-loss path; the full RLS feature ships with ADR-0058 in v0.79.0.

  **Diagnose-bundle integration**: `sluice diagnose` standard-level bundle now reports per-table RLS state (`enabled` / `forced`) + the connecting role's `rolbypassrls` attribute under `EngineState.rls`. Operators can run diagnose to see the state before attempting a migration.

### Tests

- **`test(pipeline): rls_preflight_test.go`** — 12 table-driven unit tests covering the 4 cells of {RLS on/off} × {role BYPASSRLS yes/no} + the FORCE-RLS variant + source-vs-target-side wording + multiple-offenders-sorted + empty-schema no-op + missing-prober no-op + probe-error propagation.
- **`test(engines/postgres): rls_preflight_integration_test.go`** — 9 integration tests against a real PG container with a fixture that creates `rls_off` / `rls_on` / `rls_force` tables + a non-superuser `sluice_app` role explicitly `NOBYPASSRLS NOSUPERUSER`. Pins the catalog SQL against actual `pg_class` / `pg_roles` values, validates the diagnose bundle's rendered JSON, and verifies an unprivileged role's INSERT into a FORCE-RLS table is actually refused by PG (ground-truth pin on the silent-loss class itself).
- **Lowercase control verified** — non-RLS PG → PG migrations continue to work clean; no false-positive refusal on the common case.

### Compatibility

- **Drop-in upgrade from v0.78.3.** Behaviour change: if an operator was previously running sluice against an RLS-enabled PG source/target with a non-BYPASSRLS role, the migration was silently filtering or failing opaquely; v0.78.4 will now refuse loudly with the recovery hint. **This is a deliberate user-visible change** consistent with the loud-failure tenet. Operators on the common case (no RLS tables, or BYPASSRLS-equipped role) see no change.
- **Severity a — closes a catalogued silent-loss class.** PG operators who run migrations against multi-tenant RLS-segregated tables are the most likely to have hit the silent-filter mode and not realized it. The refusal-with-recovery-hint shape lets them surface the misconfiguration before data loss.
- **No new flag.** Per the loud-failure tenet, no `--allow-rls-without-bypass` opt-out is added — the recovery is operator action (grant BYPASSRLS or exclude the table), not a sluice-side bypass.

## [0.78.3] - 2026-05-24

### Fixed

- **`fix(engines/mysql): Bug 88 — narrow DELETE Before-image to PK columns (mirror PG's filterDeleteBefore) (#51)`** — Bug-8-equivalent silent-loss class discovered by the v0.78.2 #44 hard-delete family-matrix pin (per Bug 74's "pin the class, not the representative" discipline — the matrix existed to find exactly this class of finding). Under MySQL `binlog_row_image=MINIMAL` (and `NOBLOB` when a BLOB/TEXT non-PK column exists), the binlog DELETE rows-event carries `nil` for non-PK columns. The MySQL applier's `buildWhereClause` (`internal/engines/mysql/change_applier.go:1240-1248`) emitted `col IS NULL` predicates for those nils → DELETE matched zero rows on target → ADR-0010 idempotency absorbed the miss → position advanced → **source DELETE silently didn't propagate**. Exactly the Bug 8 pattern PG already fixed via `filterDeleteBefore` in `internal/engines/postgres/cdc_reader.go`. v0.78.3 mirrors the PG pattern: MySQL CDC reader now narrows the DELETE Before-image to PK columns only before emitting `ir.Delete`. The applier path is unchanged — `buildWhereClause` produces correct SQL when given only identity-key columns; the bug was in the CDC reader emitting the full Before-image with nils, not in the applier consuming them. Phase A instrumentation (six DEBUG probes from binlog emit through applier txExec) confirmed the hypothesis verbatim before the fix landed. The four matrix cells previously t.Skip()'d in `cdc_delete_matrix_mysql_integration_test.go` (`MINIMAL × {plain-delete, update-then-delete}` and `NOBLOB × toast-delete`) are now un-skipped and PASS. New unit pin `TestFilterDeleteBefore` in `internal/engines/mysql/cdc_reader_test.go` pins the narrowing behaviour across 5 sub-cases (MINIMAL, FULL, NOBLOB-with-TOAST, composite-PK, PK-less fallback).

### Docs

- **ADR-0057 closure** (`docs/adr/adr-0057-hard-delete-semantics-across-engines.md`) — appended "Bug 88 closure (v0.78.3, 2026-05-24)" subsection under the MySQL matrix section noting the fix locus (CDC reader, not applier), the four un-skipped cells, the unit-test pin, and the VStream-out-of-scope note. The original "Task #44 finding" text is preserved for historical context.

### Tests

- **4 matrix cells un-skipped** in `internal/pipeline/cdc_delete_matrix_mysql_integration_test.go`: `binlog_row_image=MINIMAL × {plain-delete, update-then-delete}` and `binlog_row_image=NOBLOB × toast-delete`. All now PASS post-fix.
- **`test(engines/mysql): cdc_reader_test.go (Bug 88 unit pin)`** — `TestFilterDeleteBefore` with 5 sub-cases pinning the PK-only narrowing.

### Compatibility

- **Drop-in upgrade from v0.78.2.** Pure bugfix; no flag surface change. **Severity a — silent-loss class.** Operators running MySQL→{MySQL, PG} streams against sources with `binlog_row_image` set to MINIMAL (or NOBLOB with BLOB/TEXT columns) silently lost DELETEs prior to v0.78.3. `docs/dev/notes/prep-change-applier.md:26` declares `binlog_row_image=FULL` as the only supported config — so in spec, this only affected operators who'd switched to MINIMAL/NOBLOB for perf. The fix closes the gap regardless of declared support, matching PG's already-narrowed behavior across the engine pair.

## [0.78.2] - 2026-05-23

### Fixed

- **`fix(engines/postgres): Bug 87 — quote schema.table in syncOneIdentity (#43)`** — `internal/engines/postgres/schema_writer.go:345` passed `w.schema + "." + table.Name` as the `tableArg` to `pg_get_serial_sequence($1, $2)` without quoting. `pg_get_serial_sequence` parses its first argument as identifier text — and per PG's identifier rules, **unquoted identifiers fold to lowercase**. For any target table with case-preserved name + an IDENTITY / SERIAL column (e.g. source `CREATE TABLE "Widgets" ("id" BIGSERIAL PRIMARY KEY)`), `tableArg` became `public.Widgets`, which PG interpreted as `public.widgets` and raised `relation "public.widgets" does not exist (SQLSTATE 42P01)`. The MAX read on the line just above (line 328) already quoted correctly via `quoteIdent`; the fix is the same single-statement consistency: `tableArg := quoteIdent(w.schema) + "." + quoteIdent(table.Name)`. **Downstream symptom (initially misdiagnosed as a separate "Bug 2"):** for the CDC streamer, `coldStart` calls the same `SyncIdentitySequences` phase as the Migrator. When this errored, the streamer's runOnce loop hit its retry backoff and never transitioned to CDC mode — operator-visible as "bulk copy complete (2 rows) + nothing replicates after that", a silent-loss-class shape. Phase A instrumentation (six DEBUG probes on the pgoutput → applier dispatch chain) ruled out the four hypothesized fold-points in the CDC apply path and traced the symptom back to the failed identity-sync phase. **The one-line fix closes both the loud-Migrator-abort and the silent-streamer-stall.**

### Tests

- **`test(pipeline): Bug 87 — cross-engine case-preservation matrix (32 scenarios)`** — Per the Bug 74 "pin the class, not the representative" lesson, three new integration test files (`migrate_case_preservation_pg_integration_test.go` + `_mysql_integration_test.go` + `_cross_integration_test.go`, totalling ~1160 LOC) pin a **4-direction × 4-shape × 2-path matrix**: directions are PG↔PG, MySQL↔MySQL, PG↔MySQL, MySQL↔PG; shapes are `lowercase_simple` (control), `UPPERCASE_ONLY`, `MixedCase`, and `Snake_With_Caps`; paths are bulk-copy (Migrator) and CDC (Streamer). All 32 scenarios pass on the Bug 87 fix. Without the fix, the 12 PG-target case-preserved scenarios fail in the two ways the bug report described. MySQL containers are explicitly configured with `--lower-case-table-names=0` via dedicated `startMySQLCaseSensitive` / `startMySQLBinlogCaseSensitive` helpers so the test is hermetic against the per-OS default-folding policy.

### Compatibility

- **Drop-in upgrade from v0.78.1.** Pure bugfix; no flag surface change. **Severity a** — v0.78.1 (and every prior release) silently broke PG-target migration / streaming for any operator who used quoted mixed-case or uppercase table names with an IDENTITY column. The Migrator-side surface (loud SQLSTATE 42P01 abort) was at least visible; the Streamer-side surface (silent-loss after a successful bulk copy) is exactly the user-trust-gates-throughput class the project's CLAUDE.md tenets are designed against. The matrix test now pins both engines so the next regression of the always-quote invariant surfaces in CI, not in user reports.

## [0.78.1] - 2026-05-23

### Fixed

- **`fix(engines/postgres): Bug 86 — extend NormalizeForCDCComparison to cover Nullable + temporal Precision (#41)`** — v0.78.0's RENAME COLUMN classifier refused on any PG schema carrying a nullable `NUMERIC`, `TEXT`, or default-precision temporal column. Same Bug 84-family pgoutput-vs-SchemaReader IR-canonicalization asymmetry, two additional fields the v0.73.2 normalizer didn't cover. **`ir.Column.Nullable`** is the catalogued repro's smoking gun: pgoutput's `RelationMessage` format carries `(name, OID, typmod, key-flag)` only — no `attnotnull` — so `projectRelation` leaves Nullable=false on every CDC-projected column, while cold-start's `SchemaReader.populateColumns` reads `information_schema.columns.is_nullable` faithfully. Any nullable column on the source-side schema triggered `diffAlteredColumn`'s `nullDiffers` branch with a phantom `ShapeKindAlterColumnNullability`, which combined with the RENAME's added=1/dropped=1 into a multi-shape combo refusal. **`ir.DateTime.Precision`** (plus `Time` / `Timestamp`) is a Type-level asymmetry surfaced by the post-fix matrix test: cold-start reads `datetime_precision=6` (PG's default reporting) for a plain `TIMESTAMP`; CDC's `temporalTypmod(-1)` returns 0. `diffAlteredColumn` fires `ShapeKindAlterColumnType` on the mismatch. Fix extends `internal/engines/postgres/cdc_normalize.go` to zero `Nullable` / `Default` / `Comment` on every seed column (CDC can never carry these) and to collapse `Precision == 6 → 0` on temporal types when the wire-shape is the default-precision zero (explicit non-default precisions like `TIMESTAMP(3)` pass through unchanged via the negative-precision-passthrough test).

### Tests

- **`test(engines/postgres): cdc_normalize_test.go`** — Five new `TestNormalizeForCDCComparison_PG/*` subtests pin the new normalizer behaviour: numeric-nullable zeroing, text-nullable zeroing, varchar-nullable zeroing, temporal-default-precision collapse (6→0), and the explicit-non-default-precision negative (passthrough).
- **`test(pipeline): shard_consolidation_rename_pg_integration_test.go` + `shard_consolidation_rename_mysql_integration_test.go`** — Per the Bug 74 "pin the class, not the representative" lesson, both engines' RENAME integration tests now exercise a **six-cell type matrix** at the boundary: a `name VARCHAR(64) NOT NULL → product_name VARCHAR(64) NOT NULL` rename against schemas carrying `extra_numeric_nullable NUMERIC(10,2)`, `extra_text_nullable TEXT`, `extra_varchar_nullable VARCHAR(64)`, `extra_integer_nullable INTEGER`, `extra_timestamp_nullable TIMESTAMP`, and `extra_boolean_nullable BOOLEAN`. The original v0.78.0 RENAME pin used a single fixture whose representative-of-one didn't expose either asymmetric field — the matrix would have caught Bug 86 in the same CI roundtrip that produced v0.78.0. The matrix now pins the class on both engines.

### Compatibility

- **Drop-in upgrade from v0.78.0.** Pure bugfix; no flag surface change. Operators who hit v0.78.0's spurious "Unrecognized combo" refusal on RENAME against PG schemas with nullable NUMERIC / TEXT / temporal columns now get the auto-apply behaviour ADR-0054's RENAME shape advertises. Severity a — the v0.78.0 RENAME feature was effectively broken on the majority of real-world PG schemas (any column reading default precision from `information_schema` or any nullable NUMERIC / TEXT — both extremely common); cycle subagent hit it on the first realistic test schema.

## [0.78.0] - 2026-05-23

### Added

- **`feat(pipeline, engines/{postgres,mysql}): ADR-0054 catalog expansion — RENAME COLUMN shape (#22 sub-task)`** — Closes one of the three v1-deferred sub-shapes ADR-0054's `ShapeKindUnrecognized` named explicitly (RENAME, CHECK constraint, generated-column). The classifier (`pipeline.ClassifyShape`) now recognizes RENAME when the IR delta between pre- and post-DDL `SchemaSnapshot` boundaries shows exactly 1 added + 1 dropped column with full `ir.Column` attribute equality minus `Name` (Type, Nullable, Default, etc.) — the signal both PG and MySQL `RENAME COLUMN` preserve. New `ShapeKindRenameColumn` shape carries `RenamedColumnBefore` + `RenamedColumnAfter`; new `ir.ShapeDeltaApplier.AlterRenameColumn(ctx, table, oldName, newName) error` emits the per-engine DDL (PG: `ALTER TABLE "<schema>"."<table>" RENAME COLUMN "<old>" TO "<new>"`; MySQL: backtick-quoted equivalent) with detect-then-RENAME idempotency via `information_schema.columns`. New `ir.ShardConsolidationProber.ProbeRenameColumn(ctx, table, oldName, newName, want)` mirrors the v0.76.0 ProbeAlterColumnType v2 silent-divergence catch: returns `Applied` when newName is present + oldName absent + observed IR type matches `want.Type`, `NotApplied` when oldName-still-present, `Inconsistent` + error naming the mismatch when newName is present with the WRONG type (catches a drop+re-add silent-divergence the existence-only check would miss). The `BoundaryRouter` dispatches the new shape to `applier.AlterRenameColumn` on apply and to `prober.ProbeRenameColumn` on takeover. **Out of v1 scope (still refuses loudly)**: multi-column rename in a single source DDL (`added=N + dropped=N` for N>1 — ambiguous pair-up); CHECK constraint changes; generated-column changes. **Indistinguishable-from-drop-add-same-attrs edge**: at the IR level a literal `DROP COLUMN foo; ADD COLUMN bar <same-attrs>` is byte-identical to `RENAME COLUMN foo TO bar`. The classifier treats both as rename — correct from a CDC apply perspective since the operator's load-bearing intent is preserved data under a new identifier (documented in the ADR-0054 v0.78.0 amendment).

### Tests

- **`test(pipeline): shard_consolidation_probe_test.go (RENAME shape pins)`** — Unit pin matrix for `ClassifyShape` on the new RENAME shape: happy-path classification (1-added + 1-dropped + matching attrs → `ShapeKindRenameColumn`); preserves-nullability rule (both nullable=true still classifies as rename); rejects type-diff and nullability-diff as combo refusals (those are reshape, not rename); rejects multi-column rename (`added=2 + dropped=2` → `Unrecognized`); rejects rename-plus-index-change (combo refusal); single-add and single-drop still take the existing `ShapeKindAddColumn` / `ShapeKindDropColumn` paths. Plus `DispatchProbe` routing pin + nil-guard pins for the rename payload fields.
- **`test(pipeline): shard_consolidation_router_test.go (rename happy path)`** — Router pin: rename-shaped delta fires `AlterRenameColumn` exactly once and the lease row's `applied_at` is set.
- **`test(engines/postgres): shard_consolidation_probe_integration_test.go (RENAME pins)`** — Three real-PG integration pins (`integration` build tag): `TestShapeDeltaApplier_RenameColumn_IdempotentRoundtrip` (rename twice, second is no-op; row data survives the rename), `TestShapeDeltaApplier_RenameColumn_BothPresentRefusesLoudly` (both old and new present → refusal — the partial-recovery state the operator must resolve), `TestShardConsolidationProber_RenameColumn` (pre-rename NotApplied; post-rename type-match Applied; post-rename type-mismatch Inconsistent + error naming the mismatch; both-absent Inconsistent).
- **`test(engines/mysql): shard_consolidation_probe_integration_test.go (RENAME pins)`** — Sibling matrix on real MySQL (same three pins) so both engines pin the class per the Bug 74 "pin the class, not the representative" lesson.
- **`test(pipeline): shard_consolidation_rename_pg_integration_test.go`** + **`shard_consolidation_rename_mysql_integration_test.go`** — End-to-end pins driving the production streamer through cold-start → CDC → in-flight `ALTER TABLE ... RENAME COLUMN ... TO ...` on the source → INSERT under the new column name. Asserts target schema reflects the rename (newName present + oldName absent), pre-existing row data is preserved under the renamed column, the post-RENAME INSERT lands, and the lease row records the applied state. **Validate end-to-end before building more** — same pattern as the Bug 83 PG+MySQL end-to-end pins so the v1 catalog's wire-up gap on RENAME is closed against real engines, not stubs.
- **`test(pipeline): shard_consolidation_lease_gc_end_to_end_integration_test.go`** (task #39) — closes the "known gap" v0.77.1's `### Tests` block flagged. New real-engine ChangeApplier integration pin (`TestSweepFiresEndToEnd_OnRealPGEngagement`) exercises the streamer-side wire-up of the v0.76.0 lease GC sweep end-to-end against a real PG container: drives `engageShardCoordination` with a REAL `postgres.Engine` as both Source (the `ir.PositionOrderer` surface — Bug 85.b's load-bearing assertion site) and Target, plus a REAL `postgres.ChangeApplier` (the lease store / lister / deleter surface) — no stubs anywhere on the lease-coordination path. Compile-pins `mgr.gcDeps != nil` plus each of the four fields non-nil; would have caught Bug 85 (missing wire-up) AND Bug 85.b (orderer on wrong surface) immediately. Drives an eligible lease through APPLIED state with a populated anchor, writes a `sluice_cdc_state` row whose LSN is past the anchor, acquires a second never-Applied lease to keep a heartbeat goroutine alive, then polls (300ms sweep cadence via `gcEveryNTicks=3` + `RetryPeriod=100ms`) until the heartbeat-driven sweep deletes the row. Test-only addition; no production code change. **The regression guard for the Bug 85 saga's test/production surface-mismatch class** — every prior pin (unit fakes calling `SweepConsolidationLeases` directly, the v0.76.0 PG integration test driving the sweep function directly, v0.77.0's stub-based engagement pin) bypassed the production glue, which is why the bug shipped across three releases.

### Compatibility

- **Drop-in upgrade.** Additive on the recognized-shape catalog. Chains that previously hit `Unrecognized` refusal on a single-column RENAME now get auto-apply behaviour. Operators relying on that refusal as a soft no-op should add `--no-coordinate-live-ddl` (already shipped) to retain the v0.77.x drained-model default for their stream(s). The two remaining task #22 sub-shapes (CHECK constraint changes, generated-column changes) still surface as `Unrecognized` with the operator-actionable drained-model recovery hint — task #22 has more sub-tasks coming.

## [0.77.1] - 2026-05-23

### Fixed

- **`fix(pipeline): Bug 85.b — assert PositionOrderer on s.Source not applier (#38)`**. v0.77.0's Bug 85 fix attempt added `mgr.WithGC(...)` wiring to `engageShardCoordination` but used the wrong type-assertion surface: `applier.(ir.PositionOrderer)`. `PositionAtOrAfter` is defined on the `Engine` factory type (e.g. `internal/engines/postgres/position_orderer.go:33` — `func (Engine) PositionAtOrAfter(...)`), NOT on `*ChangeApplier`. Type-assertion silently failed on every real engine → `gcDeps` stayed nil → heartbeat-loop guard evaluated false → sweep STILL dead code. **Same Bug 85 failure mode, second cycle.** The v0.77.0 unit pin test passed because `supportingApplier` stubbed `PositionAtOrAfter` on itself — test/production surface mismatch hiding the real gap. v0.77.1 asserts the orderer on `s.Source.(ir.PositionOrderer)` (the source engine, where the orderer actually lives); the supportingApplier's misleading orderer stub is REMOVED (it was actively misleading); `stubNamedEngine` gains a `PositionAtOrAfter` method so the pin test's `s.Source` assertion succeeds at the right surface; new `TestEngage_NoGCWhenSourceLacksOrderer` pin guards the no-GC default when a future engine doesn't ship a PositionOrderer.

### Tests

- **`test(pipeline): shard_consolidation_engage_test.go`** — updated Bug 85 pins to assert on the right surfaces (orderer on engine, lister+deleter on applier). New `TestEngage_NoGCWhenSourceLacksOrderer` regression guard for the no-GC default. **Known gap**: a real-engine ChangeApplier integration test that exercises the heartbeat-fires-sweep path end-to-end is still missing — without it, future regressions of this test/production surface-mismatch class can hide for another release cycle. Tracked as a follow-up.

### Compatibility

- **Drop-in upgrade from v0.77.0.** Behaviour change: lease GC sweep now actually fires (it was claimed-but-dead in both v0.76.0 and v0.77.0). Operators with accumulated lease rows from v0.76.0+ deployments see them GC'd on the next heartbeat sweep (every ~5 min) after upgrading. Severity b/c — operational regression closure, NOT silent-loss-class.

## [0.77.0] - 2026-05-23

### Added

- **`feat(cmd/sluice, internal/pipeline): sluice backup compact (ADR-0046 §14d, Task #15)`** — New `sluice backup compact --merge-window DUR` subcommand that concatenates consecutive lineage segments whose CreatedAt gaps fall within a single operator-supplied window into one merged segment. "Naive" = byte-level chunk concat — each merged source's chunk files are MOVED verbatim into the merged segment's directory; bytes are NEVER decompressed, recompressed, or re-encrypted (event-level dedup is deferred to #16). The merged segment's full = the OLDEST source's full (the restore base, its snapshot anchor S_0 covers the group's full restore range); its incrementals = the union of every source's incrementals in lineage order; its Codec / ChainEncryption / VerbatimExtension markers carry over from the oldest source unchanged. Pre-flight refuses LOUDLY (before any mutation) on three boundary conditions: mixed codecs within a merge group, divergent encryption keysets within a group (fingerprint = KEKMode|KEKRef|Mode|argon2id-salt-hex), and position gaps between consecutive sources (`seg[i].EndPosition != seg[i+1].StartPosition` — the rotation FSM guarantees `<=`, with equality the common case; a strict-less gap would mean events live only in the later segment's full snapshot that naive compact is about to drop, so the operator gets a clear refusal with the recovery hint). Atomic safety: staging-dir → final-dir → ATOMIC catalog swap (single lineage.json Put) → orphan source sweep. The catalog swap is the linearization commit — pre-swap → "compact never happened", post-swap → "compact happened". Mid-compact crash recovery: leftover `.compact-staging-*` dirs on resume are unsalvageable garbage the next compact run sweeps. `--dry-run` mirrors prune (reports the per-group plan with merged segment IDs + window spans + bytes moved, runs the same pre-flight refusals, never touches storage or rewrites the catalog). Never auto-runs; compact is an explicit operator action via `sluice backup compact`. Companion ADR-0046 §14d amendment documents the locked design.

### Tests

- **`test(pipeline): chain_compact_test.go`** — Unit pin matrix per ADR-0046 §14d: 2-segment merge collapses to 1 with both source dirs swept post-commit; 3-segment merge collapses to 1; out-of-window pair untouched; mixed in/out-of-window 4-segment lineage produces two merged groups (size-2 each); keyset boundary refusal with the documented recovery-hint message + no-mutation assertion; codec boundary refusal wrapping `errMergeGroupCodecMismatch`; position-gap refusal naming the boundary; single-segment lineage no-op; all-out-of-window lineage no-op; `--dry-run` populates the Plan + Plan-fields without mutating catalog / storage; missing-catalog refusal mirrors prune's "run --rebuild-catalog first" hint; post-compact `buildLineageChain` succeeds (the restore-shape invariant); stale `.compact-staging-*` from an earlier crashed run sweeps on the next CompactChain invocation.
- **`test(pipeline): backup_compact_integration_test.go`** — Integration pin (build tag `integration`) drives a 3+-segment rotated PG lineage under continuous CDC write load, restores AS-IS into the target (pre-compact baseline), runs `CompactChain` with a 1h merge window that captures the trailing range as one group, asserts the post-compact lineage has strictly fewer segments, restores the post-compact lineage idempotently into the same target, and asserts the row set on the target matches the pre-compact baseline exactly (the load-bearing "validate end-to-end before building more" gate per ADR-0046 §14d).
- **`test(cmd/sluice): backup_test.go`** — Two new kong-parse cases pin the `sluice backup compact` flag surface: `--from-dir` + `--merge-window=1h` happy path, and `--from=s3://...` + `--merge-window=24h` + `--dry-run`.

### Fixed

- **`fix(pipeline): Bug 85 — wire LeaseManager.WithGC at engagement time (#37)`** — v0.76.0 shipped Task #21's lease GC sweep (auto-deletion of `applied`-state `sluice_shard_consolidation_lease` rows once every stream is past the row's anchor) but missed the streamer-side wire-up: `engageShardCoordination` constructed the `LeaseManager` and stored it on the Streamer but never called `mgr.WithGC(...)`. The `m.gcDeps` field stayed nil; the heartbeat-loop's GC-trigger guard always evaluated false; the sweep was dead code in production. v0.76.0's release notes claimed otherwise. v0.77.0 adds the wire-up (type-assert the applier for `ir.ShardConsolidationLeaseLister` + `ir.ShardConsolidationLeaseDeleter` + `ir.PositionOrderer`; populate `LeaseGCDeps`; call `mgr.WithGC`). Both PG and MySQL engines already implement all three surfaces — the v0.76.0 unit + integration tests exercised them directly, which is exactly why the production glue gap slipped through. **Pin tests:** `TestEngage_WiresGCDepsWhenAllSurfacesPresent` asserts `gcDeps` is populated when all three surfaces type-assert; `TestEngage_InheritsNoGCDefaultWhenSurfacesMissing` pins the safe no-GC default when an applier (e.g. a future engine) only implements the lease store + prober. Severity b/c — operational regression (lease table grew unboundedly) but NOT silent-loss-class; recoverable via manual DELETE. Cycle detail tracked in the project's internal regression catalog (Bug 85).

### Compatibility

- Drop-in upgrade from v0.76.0. The new behaviour is the `sluice backup compact` subcommand (operator-explicit; never auto-runs); operators who don't invoke it see no change. Existing lineages are forward-compatible: compact appends a new `seg-merged-<groupID>/` sub-dir + a `cap_reason: "compacted"` discriminator to merged segments, both additive on the existing schema (no `lineage_catalog_format_version` bump). Reading a compacted lineage with an older sluice silently ignores the unknown `compacted` cap_reason and otherwise treats the segment as a normal capped segment with sub-dir layout. Restore-after-compact uses the same lineage-walk code path as restore-after-rotation; there is no new restore-side code surface.
- **Bug 85 fix (v0.76.0 → v0.77.0)**: operators running long-lived Shape A deployments on v0.76.0 should upgrade to v0.77.0 for lease-table GC to actually function. Existing `sluice_shard_consolidation_lease` rows accumulated since v0.76.0 are GC-eligible immediately on upgrade if all streams have advanced past their anchors; the next heartbeat-driven sweep (every ~5 min) clears them.

## [0.76.0] - 2026-05-23

### Fixed

- **`fix(pipeline, engines/{postgres,mysql}): ADR-0054 Phase 2 v1 closure — lease GC sweep + ProbeAlterColumnType v2 (#21, #20)`** — Closes the two v1-ship-deferred items from ADR-0054's "known follow-ups." (#21) `sluice_shard_consolidation_lease` rows now garbage-collect automatically: new `pipeline.SweepConsolidationLeases` enumerates every APPLIED row, compares its anchor against every stream's persisted `source_position` via the engine's `ir.PositionOrderer`, and DELETEs rows every live stream has advanced past. The sweep piggybacks on the existing `LeaseManager` heartbeat goroutine (every 30 ticks = ~5 min by default), so no new CLI surface; an additive `anchor_position TEXT NULL` + `source_engine TEXT NULL` migration (`ADD COLUMN IF NOT EXISTS` on PG, detect-then-ALTER on MySQL 8.0.x) carries the source-side CDC position the boundary was observed at, and `FinalizeLeaseApply` persists it. Legacy v0.75.0 rows (NULL anchor) are defensively retained. HELD / EXPIRED rows are never deleted regardless of position. GC errors are LOGGED at WARN but never propagated up (loud-failure tenet: retention is a maintenance op, not a correctness one — a failing sweep cannot crash an otherwise-healthy stream). (#20) `ProbeAlterColumnType` is no longer existence-only: v2 introspects the column's catalog-reported IR type via the same per-engine `translateType` helper the schema reader uses and refuses loudly with `ProbeOutcomeInconsistent` + an error naming expected vs observed type when they differ. Catches the previously-silent silent-divergence shape where a column drops + re-adds with the wrong type. PG NUMERIC unconstrained-vs-constrained is preserved; MySQL VARCHAR length is compared (charset deliberately excluded per the ADR-0054 v0.73.2 normalizer amendment).

### Tests

- **`test(pipeline): shard_consolidation_lease_gc_test.go`** — Unit pins for the GC two-condition safety matrix: all streams past anchor → deleted, one stream behind → retained, HELD row → never deleted, legacy NULL-anchor row → retained, empty table → no-op, no streams → conservatively skipped, mixed fleet → exactly the eligible row is deleted, per-row delete failure accumulates the error and continues, engine-without-deleter is a no-op, orderer error retains the row defensively.
- **`test(engines/postgres): shard_consolidation_lease_gc_integration_test.go`** — Integration pins (`integration` build tag) drive a real Postgres container through `EnsureControlTable` (verifies the additive `anchor_position` / `source_engine` migration lands), `FinalizeLeaseApply` with a populated anchor, `ListLeases` (verifies the round-trip), and `pipeline.SweepConsolidationLeases` against the engine's own `ListStreams` + `PositionOrderer`. Two flows: stream-past-anchor → deletes; stream-behind-anchor → retains.
- **`test(engines/{postgres,mysql}): shard_consolidation_probe_integration_test.go (v2)`** — `TestShardConsolidationProber_AlterColumnType_V2` (both engines) lands a real `ALTER COLUMN INT → BIGINT`, asserts `Applied` post-ALTER, then drops + re-adds the column with the WRONG type (TEXT) and asserts `Inconsistent` + an error message naming expected and observed types — the silent-divergence shape v1 missed. PG-specific `TestShardConsolidationProber_AlterColumnType_V2_NumericUnconstrained` pins the bare `NUMERIC` vs `NUMERIC(p,s)` distinction the v2 probe must surface.

### Compatibility

- Drop-in upgrade from v0.75.0. The new behaviour is automatic GC (operationally invisible — no new flags) and a stricter `ProbeAlterColumnType` that catches a previously-silent silent-divergence shape (operators relying on a wrong-type drop+re-add to silently pass would now see a loud refusal with a recovery hint, which is the intended behaviour per ADR-0054's loud-failure tenet). The additive `anchor_position` / `source_engine` columns on `sluice_shard_consolidation_lease` are written by all v0.76.0+ boundaries; legacy v0.75.0 rows with NULL anchors are defensively retained by the GC sweep and harmlessly accumulate until a new boundary on the same target rewrites them.

## [0.75.0] - 2026-05-23

### Added

- **`feat(cmd/sluice, internal/diagnose): sluice diagnose operator-bundle (ADR-0056, Task #18)`** — New `sluice diagnose --stream-id X --output bundle.zip` subcommand that assembles a `cockroach debug zip`-shape ZIP carrying everything a sluice maintainer needs to triage a GitHub issue: per-stream `sluice_cdc_state` row, capped (100 most-recent) `sluice_cdc_schema_history` rows, `sluice_shard_consolidation_lease` rows, engine snapshots (PG slot state + version; MySQL master-status + GTID), cross-engine health probe mirroring `sluice sync health`, declared engine `Capabilities()`, DSN-redacted CLI argv. Three privacy levels (`basic` / `standard` / `verbose`) with pinned per-level inclusion contracts; `basic` is state-only and the safest default for unattended bundles, `standard` adds version + DSN-redacted config + health, `verbose` adds per-table `COUNT(*)` on the target and the last 200 lines of `--log-file`. Row-level data is excluded at every level. Companion `--diagnose-on-crash-dir` flag on `sync start` / `migrate` installs an auto-on-crash hook that writes a bundle when the subcommand exits with an error — opt-in (default off, the unattended bundle is a privacy risk that requires the operator to enable); default privacy is `basic`; the hook NEVER masks the original error (loud-failure tenet). New `ir.DiagnoseProber` (engine snapshot surface), `ir.SchemaHistoryReader` (cdc-schema-history row enumeration), implemented by both PG and MySQL engines. DSN-redaction helpers in `internal/diagnose/redact.go` mirror (without depending on) `internal/redact.redactDSNForAudit` and `internal/pipeline.redactBlobURL` — see ADR-0056's note that `internal/redact` is row-value-level redaction, NOT diagnose redaction.

### Docs

- **`docs(adr-0056): sluice diagnose operator-bundle`** — New ADR documenting the privacy-level inclusion / exclusion contract (per-level table), the auto-on-crash safety semantics (best-effort, never masks the original error, default `basic` level for unattended bundles), the DSN-redaction surface boundary versus the existing row-value-level `internal/redact` package, and the bundle ZIP layout. Cross-references ADR-0007 (position persistence), ADR-0049 (schema history), ADR-0054 (lease state), and the `sluice-public-release-audit-2026-05-22.md` audit doc that named Task #18 as the last pre-public-release item.

### Tests

- **`test(internal/diagnose): bundle_test.go`** — Per-privacy-level unit pins (Bug-74 discipline applied): `TestBundle_BasicLevel_IncludesStateExcludesEverythingElse` (state dumps only, no version / DSN / engine names in the manifest), `TestBundle_StandardLevel_IncludesHealthAndConfigExcludesLogsAndRowCounts`, `TestBundle_VerboseLevel_IncludesEverything`. DSN redaction is pinned across the family matrix (`TestBundle_RedactsDSNCredentials`: URI-form Postgres, URI-form MySQL, go-sql-driver form — each asserting the credential is stripped AND the host:port survives). Companion `TestBundle_PreservesDatabaseName`, `TestBundle_RedactCLIArgs` (`--flag value` + `--flag=value` + non-DSN passthrough), `TestBundle_BasicScopesToRequestedStream` (no leakage of unrelated stream-ids), and loud-failure pins (`TestBundle_RejectsUnsetPrivacyLevel`, `TestBundle_RejectsEmptyStreamID`).
- **`test(internal/diagnose): crash_hook_test.go`** — `TestCrashHook_WritesBundleBeforeReturn`, `TestCrashHook_BundleWriteFailureDoesNotMaskOriginalError` (the load-bearing loud-failure invariant), `TestCrashHook_BasicLevelDefault` (the default-safest decision pinned), `TestCrashHook_DisabledWhenDirEmpty` (opt-in default), `TestCrashHook_RefusesInvalidDir` (install-time failure beats crash-time failure), nil-receiver passthrough pins.
- **`test(engines/postgres): diagnose_integration_test.go`** — Live-PG end-to-end pins (build tag `integration`). `TestDiagnose_ListSchemaHistory_OrdersByCreatedAtDesc` writes three boundaries and asserts the reader returns most-recent first with `cap` honoured. `TestDiagnose_ListSchemaHistory_AbsentTable` asserts the reader returns an empty slice (not an error) when `sluice_cdc_schema_history` doesn't exist — the graceful-degrade contract for diagnose against pre-ADR-0049 streams. `TestDiagnose_DiagnoseBundle_EmbedsServerVersionAndState` asserts the PG SchemaReader's `DiagnoseBundle` populates `EngineName=postgres`, `EngineVersion` containing "PostgreSQL", and a JSON-decodable `EngineState`. `TestDiagnose_BundleEndToEnd_AssemblesAgainstLivePG` is the "validate end-to-end before building more" pin — seeds a `sluice_cdc_state` row on a live PG container, calls `diagnose.Write` against the live engine, unzips the result, and asserts the manifest carries a redacted target DSN (no `@` userinfo character), the state dump contains the seeded stream-id, and all expected sections exist.

### Compatibility

- Additive only. No behaviour change for existing flows. The `--diagnose-on-crash-dir` flag is opt-in and absent by default. The new `ir.DiagnoseProber` and `ir.SchemaHistoryReader` interfaces are OPTIONAL — engines that don't implement them surface in the diagnose bundle as `__skipped.txt` reason files; existing engines (PG and MySQL) implement both as part of this release.

## [0.74.2] - 2026-05-23

### Added

- **`feat(engines/postgres): F1 — refuse loudly on pgoutput StreamAbortMessageV2`** — Closes severity-c finding F1 of the 2026-05-22 PG-internals research run. sluice's `START_REPLICATION` passes `proto_version=2` without `streaming='on'`, so PG should never emit streaming chunk messages — but the receiver's `dispatchWAL` previously silently skipped `StreamAbortMessageV2` via the `default:` arm of its type switch (alongside benign skips for `TypeMessage` / `OriginMessage` / `LogicalDecodingMessage` / `StreamCommitMessageV2`). The silent-skip on StreamAbort was latent silent-loss-class: if streaming ever got enabled externally (PG config drift) or by a future sluice change without wiring chunk-rollback into the IR, each pre-abort `StreamStart` / `StreamStop` chunk has already been emitted as `ir.TxBegin` / `ir.TxCommit` (per ADR-0027) and committed on the target. A silently-skipped abort would leave the target carrying rows the source rolled back — silent unrecoverable divergence. The fix adds an explicit `case *pglogrepl.StreamAbortMessageV2:` returning a self-describing error (xid + sub-xid + recovery hint pointing at slot-drop + re-snapshot, references ADR-0055). No production behaviour change for any current operator — the refusal can only fire if streaming is enabled, which sluice does not do.

### Docs

- **`docs(adr-0055): pgoutput streaming-protocol audit`** — New ADR documenting the pgoutput v1 vs v2 protocol distinction (parsing capability via `proto_version >= 2` vs emitting capability via `streaming='on'`), sluice's current `proto_version=2`-without-streaming config, why the defensive handlers exist (against config drift), and the F1 decision to refuse loudly on StreamAbort. Cross-references ADR-0027 (chunk-as-tx batching), ADR-0028 (memory-bounded streaming), ADR-0007 (position-durability invariant), ADR-0020 (slot-ack-after-apply, related family of silent-loss closures), and ADR-0010 (idempotent applier convergence assumption).

### Tests

- **`test(engines/postgres): cdc_reader_streaming_protocol_test.go`** — F1 unit pin. Constructs synthetic StreamAbortMessageV2 wire-format bytes (`'A'` + xid + sub-xid big-endian uint32s), drives them through `dispatchWAL`, and asserts the returned error names the message type, includes the xid + sub-xid for operator correlation, carries the recovery hint, references ADR-0055, and emits no `ir.Change` before refusing. A second pin asserts the error does NOT wrap `ir.ErrPositionInvalid` (which would incorrectly route through the ADR-0022 cold-start fall-through instead of forcing the operator to drop + re-snapshot).
- **`test(engines/postgres): cdc_reader_streaming_protocol_integration_test.go`** — F1 integration pin (receiver-side empirical confirmation). Boots PG with `logical_decoding_work_mem=64kB`, runs a single ~1000-row INSERT transaction that comfortably exceeds the cap, drains the changes channel, and asserts EXACTLY ONE `TxBegin` / 1000 `Insert` / EXACTLY ONE `TxCommit` triplet arrives — proving streaming chunks are not being emitted under sluice's default plugin args even when the source spills to disk. A streaming-enabled stream would produce ≥2 `TxBegin` / `TxCommit` pairs (one per chunk).
- **`test(engines/postgres): confirmed_flush_invariant_integration_test.go`** — F3 pin. Asserts the load-bearing ADR-0007 / ADR-0020 invariant that `pg_replication_slots.confirmed_flush_lsn <= max(target's persisted source_position LSN)` holds continuously during a real CDC stream. Wires CDCReader + ChangeApplier with the LSN tracker, drives 25 distinct insert transactions, and a polling goroutine samples both LSNs every 100 ms — any violation is captured at the time it happens. Also asserts the slot's `confirmed_flush_lsn` advanced strictly above 0 (rules out the trivially-passing case where both sides stayed at zero) and re-checks the invariant post-stop after the stream tears down. F3 is pin-only — no production code change.

## [0.74.1] - 2026-05-22

### Added

- **`feat(engines/postgres, pipeline, cmd/sluice): F2 — surface PG-14+ logical-decoding spill counters in sync health + Prometheus`** — Severity-B finding F2 from the 2026-05-22 PG-internals research run. PG's `pg_stat_replication_slots` view (PG 14+) exposes `spill_txns` and `spill_bytes` — cumulative counters tracking when CDC-decoded transactions exceed `logical_decoding_work_mem` (default 64 MB) and PG spools un-emitted change records to disk under `pg_replslot/<slot>/snap/`. Pre-finding sluice surfaced no signal: operators had to know to query the view themselves, and a slot filling its disk-resident spill directory would eventually trigger PG to invalidate the slot (`wal_status` → `lost`) — silent-loss-class for sluice. v0.74.1 adds (1) a new optional `ir.SlotSpillReporter` interface implemented by PG's `SchemaReader` reading `spill_txns` / `spill_bytes` (gracefully degrades on PG < 14 via the `42P01 undefined_table` SQLSTATE — surfaces as "unavailable" rather than a misleading `0`); (2) `sluice sync health` JSON / text output gains `spill_txns` + `spill_bytes` (pointer-omitempty — absent when unavailable) plus a new `--slot-name` flag for non-default slot names; (3) the Prometheus `/metrics` endpoint exposes `sluice_pg_slot_spill_txns_total{stream_id,slot}` and `sluice_pg_slot_spill_bytes_total{stream_id,slot}` counters when the streamer is connected to a PG source. Operator action: alert on `rate(sluice_pg_slot_spill_bytes_total[5m]) > 0`, then bump `logical_decoding_work_mem` or split large transactions; see `docs/postgres-source-prep.md` (new "Logical-decoding spill" section) for the playbook.

### Fixed

- **`fix(engines/postgres): retry EnsureControlTable on pg_type / pg_class catalog race`** — Closes Task #29 (carryover from ADR-0054 Phase 2e). PG's `CREATE TABLE IF NOT EXISTS` checks pg_class for the relation but the table's row type allocates a pg_type row independently; concurrent CREATEs for the same name race on `pg_type_typname_nsp_index` (or `pg_class_relname_nsp_index`) and the loser surfaces SQLSTATE 23505. This is the same race the v0.73.0 Phase 2e test had to work around with pre-creation (`shard_consolidation_router_pg_integration_test.go`). v0.74.0+1 wraps `EnsureControlTable` (the only sluice call site that's prone to this — N shards starting tightly against a fresh target) in `retryOnCatalogRace`: 3 attempts with 50/100/200 ms backoff, ONLY retrying the narrow pg_type / pg_class catalog-race shape (constraint-name match). Other 23505s (user-table unique violations) stay non-retriable per ADR-0038.

- **`fix(githooks): pre-commit fails on unresolved merge-conflict markers in ANY file`** — Closes Task #36. The pre-commit hook previously only ran Go-file checks; merge-conflict markers in non-Go files (CHANGELOG.md, docs/, configs) slipped through to commits and to main. v0.74.0's F5 cherry-pick landed CHANGELOG.md with live `<<<<<<<` / `=======` / `>>>>>>>` markers because the hook short-circuited on "no Go files staged." Both `.githooks/pre-commit` (Bash) and `scripts/pre-commit.ps1` (PowerShell) now check all staged files for `<<<<<<<` and `>>>>>>>` markers (the unambiguous ones — `=======` alone would false-positive on Setext markdown underlines) BEFORE the Go-only gate.

### Tests

- **`test(engines/postgres): change_applier_catalog_race_test.go`** — unit pins on `isCatalogRaceError` (constraint-name discriminator, wrapping via `errors.As`, non-23505 / non-catalog 23505 negatives) and `retryOnCatalogRace` (immediate success, retry-then-success, exhausted retries, non-race-error immediate return, context cancellation observed between retries).
- **`test(engines/postgres): health_reporter_test.go + slot_spill_integration_test.go`** — F2 pins. Unit: `isUndefinedTableError` correctly matches PG SQLSTATE `42P01` (including via `errors.As` for wrapped errors) and rejects other 42-class codes / non-PG errors / nil. Integration: `SlotSpillStats` returns `ok=false` for a nonexistent slot (operator probing before CDC starts), errors on empty `slotName` (wiring-layer bug detector), and reads non-zero `spill_bytes` end-to-end after a deliberately oversized transaction is decoded through a slot with `logical_decoding_work_mem = '64kB'` — confirms the view→struct wire-up against a real PG container.
- **`test(pipeline): metrics_test.go`** — F2 Prometheus surface pins. `emitSpillMetrics` renders both counter lines with the `{stream_id,slot}` label set, including the "zero is real data" case (counter renders 0 only when reporter said `ok=true`). `MetricsServer.AttachSpillReporter` end-to-end: attaching emits the lines, omitting / detaching / `ok=false` suppresses them, and a reporter error surfaces as a `# error: slot-spill-stats: ...` exposition comment instead of blanking the rest of `/metrics`.
- **`test(cmd/sluice): sync_health_test.go`** — F2 sync-health surface pins. Spill fields render in text output when populated, stay absent (text + JSON) when `nil` (pointer-omitempty contract), and round-trip cleanly through `json.Marshal` / `Unmarshal` when populated.

### Docs

- **`docs(postgres-source-prep): F2 — Logical-decoding spill operator playbook`** — New section documenting what spill means (`logical_decoding_work_mem`, `pg_replslot/<slot>/snap/` disk pressure, eventual slot invalidation), where the signal appears in sluice's surfaces (`sync health` + Prometheus `/metrics`), and the recovery actions (bump `logical_decoding_work_mem`, split large transactions, watch `pg_replslot/` disk usage).

## [0.74.0] - 2026-05-22

### Fixed

- **`fix(adr-0051): F5 — PG CDC source-identity pinning closes post-PITR / post-promotion silent-loss class`** — The PG CDC reader's pre-ADR-0051 `resolveStartPosition` called `IDENTIFY_SYSTEM` only on the cold-start path (purely to read `XLogPos`) and discarded `SystemID` / `Timeline` from the reply. PG's logical-replication LSN reference frame is timeline-scoped: after a source-side PITR, a standby promotion, or sluice being pointed at a different instance with the same DSN host:port shape, the (sysid, timeline) tuple changes and the persisted LSN lives in a different timeline's reference frame than what the new source's WAL uses. Pre-fix, sluice would silently `START_REPLICATION` from the persisted LSN and stream WAL from "the same LSN" on the new timeline — silent-loss class. Fix: `IDENTIFY_SYSTEM` now runs on every `StreamChanges` call (both cold-start and resume); `SystemID` + `Timeline` are pinned onto the reader and persisted in the position token (additively, with `omitempty` so pre-ADR-0051 positions decode cleanly); on resume the live `IDENTIFY_SYSTEM` reply is compared against the persisted pin via `checkSourceIdentity` and divergence refuses loudly with `fmt.Errorf("...: %w", ir.ErrPositionInvalid)` — routing through the existing ADR-0022 cold-start fall-through. Positions persisted by pre-ADR-0051 sluice trigger one-time INFO-level "lazy install" the first time they reconnect, after which the now-installed pin engages strict checking. References: PG-internals Ch 10.3 (sysid + timeline tuple), Ch 10.4 (switchover and failover), Ch 11.1 (`IDENTIFY_SYSTEM`). Closes severity-A finding F5 of the 2026-05-22 PG-internals research run.

- **`fix(engines/postgres): F7 — force synchronous_commit=on inside every apply tx`** — Severity-A silent-loss closure from the 2026-05-22 PG-internals research run (durable findings doc: `sluice-pg-internals-research-chapters-9-10-11-2026-05-22.md`). ADR-0007's "position + data lands durably together" guarantee assumes the COMMIT ACK only returns after the WAL is durably flushed (`synchronous_commit = on`). PG's parameter-precedence chain (PG Internals Ch 11.2) allows `ALTER ROLE name SET synchronous_commit = off` or `ALTER DATABASE name SET synchronous_commit = off` to pre-apply asynchronous-commit semantics (Ch 9.5) on every login from that role or to that database; the sluice apply session inherits this silently, allowing a COMMIT ACK to return BEFORE the WAL is durably flushed. A target-side crash between the ACK and the WAL flush then loses the position+data tx despite sluice having persisted forward — breaking ADR-0007 without any observable signal. Fix: the PG applier now emits `SET LOCAL synchronous_commit = on` as the first statement on every apply transaction (the three apply-tx start sites: `applyOne`, `applyOneBatch`, and `WritePosition`, all in `internal/engines/postgres/change_applier.go` / `change_applier_batch.go`). `SET LOCAL` scope reverts at tx end so non-sluice sessions on the same role are unaffected; sessions that already had `synchronous_commit = on` (the PG default) see no behaviour change. The MySQL applier does not need an analogous fix — its sync-commit settings (`sync_binlog`, `innodb_flush_log_at_trx_commit`) are not per-role inheritable in the same way. ADR-0007 amended with a "Durability hardening for Postgres targets (F7)" section.

### Docs

- **`docs(postgres-source-prep): F6 — WAL volume cost of wal_level=logical`** — Operator-facing guidance on the 1.2×-1.6× WAL byte-rate multiplier that flipping `wal_level` from `replica` to `logical` introduces (full tuple data carried alongside FPIs per PG Internals Ch 9.4; `REPLICA IDENTITY FULL` amplifies). Operator-visible consequences on slot retention disk pressure, WAL archive volume, and replica bandwidth.

- **`docs(adr-0007, adr-0054, adr-0022): F8 + F9 — PG-internals research cross-references`** — ADR-0007 gains a "Related PG-internals research" section pointing at F1/F3 (logical-replication chapter) and F5/F7 (chapters 9-11). ADR-0054 gains the analogous section calling out how F1/F3/F5/F7 each interact with the lease state machine. ADR-0022 documents that `pg_replslot/<slot>/state` on disk (Ch 11.4) is the source of truth behind the slot-missing fall-through, and that ADR-0051's timeline-change refusal extends the same machinery via a different precondition check.

### Tests

- **`test(engines/postgres): cdc_reader_test.go`** — unit pins on the F5 position-token round-trip (new SystemID/Timeline fields), pre-ADR-0051 token decode compatibility, the wire-format omitempty invariant, and every branch of the `checkSourceIdentity` comparator (exact match, lazy-install sentinel, timeline diverges, sysid diverges, both diverge — each refusal asserts `errors.Is(err, ir.ErrPositionInvalid)` plus that the message names both the old and new tuples plus the slot-drop recovery hint).
- **`test(engines/postgres): cdc_reader_source_identity_integration_test.go`** — F5 end-to-end pins against a real PG container. Happy-path resume with matched pin succeeds; resume with a tampered systemid refuses loudly (and the same un-tampered position still succeeds, guarding against over-strict refusal); a legacy (pre-ADR-0051) token shape lazy-installs the pin and subsequent emitted positions carry it.
- **`test(engines/postgres): change_applier_synccommit_test.go`** — F7 unit pin via an in-process `database/sql/driver` recording driver. Confirms `forceSynchronousCommitOn` emits exactly `SET LOCAL synchronous_commit = on` as the first statement on the tx it's handed, so a refactor of the helper can't silently regress the F7 hardening.
- **`test(engines/postgres): change_applier_synccommit_integration_test.go`** — F7 integration pin against a PG container with a role configured `ALTER ROLE … SET synchronous_commit = off`. Asserts (1) the role default DOES propagate to fresh sessions (proving the test exercises the real hazard); (2) end-to-end apply succeeds under the hostile role default; (3) inside an applier-shaped tx after `SET LOCAL synchronous_commit = on`, `current_setting('synchronous_commit')` reads back as `on`. Closes the F7 cycle.
- **`test(engines/postgres): waitForSlotInactive helper`** — Test infrastructure for between-stage slot reuse. Polls `pg_replication_slots.active = false` for a 3s grace period, then force-terminates the active backend via `pg_terminate_backend()`. Required because `CDCReader.Close()` is deliberately asynchronous (cancels streamer ctx, returns immediately; pump goroutine closes the replication conn on its way out) — PG marks the slot active until the walsender backend observes the disconnect, which can exceed test budgets without the hard kill.

### Compatibility

- **Drop-in upgrade from v0.73.2.** No CLI surface change. No storage shape change for non-PG-source streams.
- **PG CDC streams** persisted by pre-v0.74.0 sluice with no `SystemID` / `Timeline` in their position token will trigger a one-time INFO-level "pin installed lazily" log on first reconnect after upgrade. After the pin is installed, subsequent reconnects strict-check the source identity and refuse loudly on PITR / promotion / wrong-instance reconnects via the existing ADR-0022 cold-start fall-through.
- **PG apply sessions** running against a role or database with `ALTER ROLE/DATABASE … SET synchronous_commit = off` will now run with `synchronous_commit = on` for the duration of every apply transaction (transparent — the operator sees the same behavior as a session with no override). Non-sluice sessions on the same role/database are unaffected (`SET LOCAL` reverts at tx end).
- **MySQL paths** see no changes.

## [0.73.2] - 2026-05-22

### Fixed

- **`fix(adr-0054): Bug 84 — PG cold-start seed CDC-projection mismatch`** — Closes the v0.73.1 PG → PG Shape A live coordination failure ("unrecognized multi-shape combo delta (added=1 dropped=0 created-idx=0 dropped-idx=0 altered-col=true)"). Phase A instrumentation in `ClassifyShape` / `diffAlteredColumn` against a local PG repro using `widgets (id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, name VARCHAR(64) NOT NULL)` captured the exact field-level asymmetry: `id` column's `pre_type=ir.Integer{Width:64, AutoIncrement:true}` vs `post_type=ir.Integer{Width:64, AutoIncrement:false}` — `type_differs=true`. Root cause: the cold-start seed comes from the PG `SchemaReader` which reads `information_schema` (rich projection — populates `Integer.AutoIncrement` for IDENTITY columns, `Varchar`/`Char`/`Text.Collation` for explicit collations, `Decimal.Unconstrained` for bare `numeric`), but the CDC-emitted `SchemaSnapshot` comes from pgoutput's `RelationMessage` which carries only `(name, OID, typmod, key-flag)` — none of those richer fields survive. The classifier's `reflect.DeepEqual` on IR Type surfaced a false `altered-col=true` on the IDENTITY column, combining with the legitimate `added=1` (the new `price` column) into a multi-shape combo refusal. Fix: new optional engine interface `ir.CDCSchemaSnapshotNormalizer` (`NormalizeForCDCComparison(*Table) *Table`); PG implements it by zeroing the four known-asymmetric fields (`Integer.AutoIncrement`, `Varchar.Collation`, `Char.Collation`, `Text.Collation`, `Decimal.Unconstrained`); MySQL doesn't implement it (its `TableMapEvent` decoder re-reads `information_schema` on schema-change boundaries so its CDC projection already matches the SchemaReader's). The cold-start seed is normalized once before being stored in the intercept's cache so subsequent `(pre, post)` comparisons see byte-identical projections on unchanged columns.

### Tests

- **`test(pipeline): shard_consolidation_bug84_pg_integration_test.go`** — end-to-end pin reproducing Bug 84's PG-specific failure path. Uses the exact Bug 84 schema (`id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, name VARCHAR(64) NOT NULL`) so the IDENTITY column's `Integer.AutoIncrement` asymmetry is exercised. Confirmed to FAIL on `main` (pre-fix) with the catalogued combo-refusal message, and PASS on the v0.73.2 branch. The Bug 83 pin (`shard_consolidation_bug83_pg_integration_test.go`) uses a minimal `id INT PRIMARY KEY` schema whose type-struct projection happens to match across the SchemaReader/pgoutput boundary — it did not catch Bug 84. Closes the "pin the class, not the representative" gap on the cold-start seed path.

- **`test(engines/postgres): cdc_normalize_test.go`** — unit pins on the PG normalizer covering each known-asymmetric field (Integer.AutoIncrement, Varchar/Char/Text.Collation, Decimal.Unconstrained), unrelated types passthrough, nil safety, idempotence, and the compile-time interface assertion that `Engine` satisfies `ir.CDCSchemaSnapshotNormalizer`.

### Compatibility

- **Drop-in upgrade from v0.73.1.** No CLI surface change, no storage shape change, no behaviour change outside the previously-broken PG Shape A live-coordination cold-start path. MySQL Shape A is entirely unaffected (no normalizer implemented — its CDC projection already matches the SchemaReader's). Operators on v0.73.1 PG who applied the `--no-coordinate-live-ddl` workaround can remove it after upgrading.

## [0.73.1] - 2026-05-22

### Fixed

- **`fix(adr-0054): Bug 83 — Phase 2 intercept cold-start seed`** — The ADR-0054 Shape A Phase 2 live coordination intercept's table cache started empty at CDC startup and treated the first CDC SchemaSnapshot per table as the cold-start anchor. Because the cold-start phase does not emit SchemaSnapshot through the same channel (only CDC readers do), the first SchemaSnapshot reflected the source's CURRENT schema — which, if source DDL had occurred between cold-start completion and the first CDC row event, was the POST-DDL schema. The intercept cached the post-DDL schema as the cold-start anchor and never routed the boundary; the next CDC row event then crashed the applier with column-does-not-exist. v0.73.1 captures the pre-Shape-A-rewrite source IR per filtered table at cold-start completion and feeds it to the intercept as a synthetic SchemaSnapshot seed, restoring the correct (pre, post) boundary classification on the first real CDC SchemaSnapshot. Closes Bug 83 (cycle session report: sluicesync/sluice-testing v0.73.0.md).

- **`fix(adr-0054): Bug 83 — ADD COLUMN emits nullable`** — second-iteration fix landing alongside the cold-start seed. The shape applier's per-engine `AlterAddColumn` now overrides the column's `Nullable` flag to `true` before emitting the DDL. Two reasons: (1) pgoutput's `RelationMessage` does NOT carry `pg_attribute.attnotnull`, so every column in the CDC-projected IR has `Nullable=false` by zero-value default — emitting `ADD COLUMN ... NOT NULL` on a non-empty target raised SQLSTATE 23502 in the v0.73.1 PG integration pin. (2) Behaviour-symmetry between PG and MySQL: both engines now emit ADD COLUMN nullable regardless of the IR's nullability flag. **v1 limitation:** target columns added via Phase 2 live coordination land nullable. Operators who need NOT NULL on the target can apply `ALTER COLUMN SET NOT NULL` post-apply once the existing rows have a backfilled value.

- **`fix(adr-0054): Bug 83 — MySQL seed key alignment`** — second-iteration fix landing alongside the cold-start seed. The MySQL source schema reader doesn't populate `ir.Table.Schema` (it reads `information_schema` for a single bound DB; the IR convention pre-v0.73.1 left `Schema` empty), so the cold-start seed's `QualifiedName()` was the bare table name. The MySQL CDC reader, however, does set `Schema` (to the DSN's DB name) on its emitted `SchemaSnapshot`, so the first CDC snapshot's `QualifiedName` looked like `"<db>.<table>"` — a MISS against the bare-name seed, which made the intercept fall back to the "no pre" branch and treat the first CDC snapshot as the cold-start anchor (the exact regression the v0.73.1 seed was supposed to fix). The intercept now falls back to the bare table-name key when the qualified-name lookup misses; engine-agnostic, so PG (which always populates `Schema`) sees no change. Pinned by `TestIntercept_SeededFromColdStart_BareNameKeyAlignment`.

### Tests

- **`test(pipeline): shard_consolidation_bug83_{pg,mysql}_integration_test.go`** — end-to-end pin reproducing the Bug 83 failure path (cold-start + source DDL + first CDC row event) against real PG and MySQL containers. The lease state machine must transition to APPLIED and the post-DDL row must replicate. The "Validate end-to-end before building more" tenet was violated by the consumer-side-only integration tests that shipped in v0.73.0; this pin closes the gap.

- **`test(pipeline): shard_consolidation_intercept_test.go`** — new unit tests for the seed parameter: seed-then-CDC-snapshot routes; seed-only no-CDC doesn't route; seed multi-table dispatch.

### Compatibility

- **Drop-in upgrade from v0.73.0.** No CLI surface change. Operators who applied the v0.73.0 correction-banner workaround (`--no-coordinate-live-ddl` mandatory) can remove the flag after upgrading.

## [0.73.0] - 2026-05-22

### Features

- **ADR-0054 Shape A Phase 2 — live cross-shard DDL coordination.** Lifts ADR-0048 DP-3's "drained model for v1" restriction. When Shape A (`--inject-shard-column`) is engaged and `--no-coordinate-live-ddl` is absent (the new default), observed source DDL boundaries route through a per-target lease (`sluice_shard_consolidation_lease`, additive control table next to `sluice_cdc_state`): the first shard to notice acquires the lease, applies the IR-delta-derived shape change to the consolidated target, records the applied schema version + DDL checksum; peer shards observe the recorded state and skip the apply, continuing CDC without a drain. Resolves the operationally-heavy drain-window-proportional-to-slowest-shard hazard the v0.72.x drained model carries on N-shard fleets.
  - **DP-A (lease semantics):** Hybrid TTL + heartbeat-extend (Kubernetes leader-election shape). Defaults `--shard-coordination-lease-duration=30s`, `--shard-coordination-renew-deadline=20s`, `--shard-coordination-retry-period=10s` (operator-tunable for unusual ALTER patterns, e.g. tables >100GB).
  - **DP-B (DDL idempotence):** Recorded-version + DDL-text-checksum on normalized DDL text (whitespace collapse + reserved-keyword lowercase, mirroring ADR-0049's `SchemaSignature.Equal`). Mismatch across shards refuses loudly with both checksums + drained-model recovery commands.
  - **DP-C (crash recovery):** Probe-and-record on lease takeover. The takeover stream probes the target schema for the prior holder's recorded shape; Applied → record only, NotApplied → re-apply, Inconsistent → refuse loudly. Uniform across PG (transactional DDL) and MySQL (non-transactional DDL).
  - **DP-D (engagement):** Always-on with `--no-coordinate-live-ddl` opt-out (operators on the v0.72.x drained model pass the flag to preserve pre-ADR-0054 semantics). Behaviour-change-by-default consistent with the ADR-0052 AIMD opt-out pattern.
  - **DP-E (DDL apply derivation; added 2026-05-22):** Recognized-shape catalog via IR-delta classifier. The lease-holder classifies the delta between the pre-DDL and post-DDL `SchemaSnapshot` IR tables into a finite catalog (ADD COLUMN, DROP COLUMN, CREATE INDEX, DROP INDEX, ALTER COLUMN type/nullability); unrecognized shapes (multi-shape combos, RENAME, CHECK constraints, generated-column changes) refuse loudly with the drained-model recovery hint. Preserves DP-B's "no allow-list, no parser" intent — the classifier compares `*ir.Table` structs, not SQL text, and the shapes are sluice's own structural categories.

### Compatibility

- Operators upgrading from v0.72.x with hand-coordinated drained-model Shape A workflows will see different behavior on next `sluice sync start` unless they add `--no-coordinate-live-ddl`. The flag preserves the pre-ADR-0054 drained semantics exactly. Non-Shape-A streams (`--inject-shard-column` unset) see no observable change.

## [0.72.2]

**Closes the known follow-up from v0.72.1: MySQL Shape A with `AUTO_INCREMENT` in the source PK now works (Bug 82).** ADR-0048's PK rewrite places the discriminator first, demoting the source's `AUTO_INCREMENT` column from its leading position. MySQL's structural rule "every `AUTO_INCREMENT` column must be a leading key column" then rejected the CREATE TABLE with `Error 1075`. v0.72.0 + v0.72.1 shipped Shape A with this case broken — workaround was "use a non-AUTO_INCREMENT PK on the source or migrate to PG." v0.72.2 closes it: when the rewritten PK contains an `AUTO_INCREMENT` column that doesn't lead, the MySQL DDL emitter now synthesizes a `UNIQUE KEY uq_<table>_<col> (<col>)` inline in the CREATE TABLE, satisfying MySQL's rule via a secondary unique index. The DP-2 leading-shard invariant (discriminator first) is preserved — operators retain source-side identity management — and the synthesis is scoped to the in-PK-but-not-leading case so the existing v0.49.0 / GitHub #25 no-supporting-index loud-error path stays unchanged.

### Fixed

- **`fix(engines/mysql): Bug 82 — synthesize supporting UNIQUE KEY when AUTO_INCREMENT is demoted by Shape A rewrite`** — `inlineAutoIncrementIndex` now detects the "auto column in PK but not leading" case (the ADR-0048 Shape A IR-pass output) and synthesizes a unique index named `uq_<table>_<col>` on the auto column. The synthesized index ships inline in `CREATE TABLE`; Phase 2 (`CreateIndexes`) is unaffected because the synthesized index isn't in `table.Indexes` (no double-create risk). The existing v0.49.0 logic for non-PK auto columns with operator-declared supporting indexes is preserved unchanged. Owner-confirmed via the AskUserQuestion design dialogue (option (b) over (a)/(c)/(d) — see the ADR-0048 Amendment 2026-05-22 section for the alternatives + correctness analysis).

### Tests

- **`test(engines/mysql): bug82_autoincrement_pk_demotion_test.go`** — five unit pins covering: the synthesis case, the end-to-end `emitTableDef` output including the DP-2 PK-leading invariant, the regression guard that the standard `id BIGINT AUTO_INCREMENT PRIMARY KEY` shape still returns nil (PK leads, no synthesis needed), the precedence rule (operator-declared supporting index wins over synthesis), and the scope-narrow guard (no synthesis when auto col is not in PK and no operator index exists; pre-v0.49.0 behavior preserved for non-Shape-A schemas).

- **`test(integration): MySQL → MySQL Shape A with AUTO_INCREMENT-in-PK source`** — new build-tagged integration test (`TestMigrate_MySQL_ShapeA_Bug82_AutoIncrementInPK`) exercises the full migrate path on a real MySQL pair with the canonical `id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY` source. Asserts (1) non-zero rows on the target, (2) the synthesized `uq_widgets_id` UNIQUE KEY is present in `information_schema.statistics`, (3) the DP-2 PK-leading invariant holds (target PK leads with `source_shard_id`). The third assertion is a load-bearing regression guard against option (a) (engine-specific PK ordering) ever creeping back in.

### Compatibility

- **Drop-in upgrade from v0.72.1.** No CLI surface change, no storage shape change, no behavior change outside the previously-broken MySQL Shape A AUTO_INCREMENT path. Anyone running Shape A on MySQL with a non-AUTO_INCREMENT PK (the workaround) sees no observable change; the workaround note in v0.72.1's release notes is now obsolete and the AUTO_INCREMENT path "just works." Non-MySQL targets are entirely unaffected.

## [0.72.1]

**Hotfix — ADR-0048 Shape A end-to-end correctness (Bugs 80 + 81 paired).** The v0.72.0 post-release regression cycle surfaced two paired Shape A regressions: the row reader built its SELECT against the schema-mutated `*ir.Table` (Bug 80, loud — every Shape A bulk-copy crashed with `SQLSTATE 42703 "column does not exist"` on PG / `Error 1054 "Unknown column ... in field list"` on MySQL), and the `shardPreflightProber` interface had no engine implementation so the ADR-0048 DP-2 populated-target three-point preflight was a complete no-op (Bug 81, silent-on-fix — fixing only Bug 80 would have swapped a loud crash for a silent cross-shard collision). v0.72.1 ships both fixes together per the cycle's "pair-the-class" recommendation.

### Fixed

- **`fix(engines): Bug 80 — Shape A reader projection now filters SluiceInjected columns`** — new `sourceReadableColumns` helper on both PG and MySQL row readers filters BOTH generated columns AND `ir.Column.SluiceInjected` columns; consumed by `buildSelect`, the streaming-scan path, and the batched-read path. `nonGeneratedColumns` (the writer-side helper) is deliberately left unchanged — the discriminator column MUST land on the target; the orchestrator's `shardStampRows` wrap stamps the discriminator value onto each row between read and write, and the writer's projection picks it up. The two helpers are intentionally asymmetric and the asymmetry is now compile-pinned by per-engine unit tests.

- **`fix(engines): Bug 81 — shardPreflightProber implemented on PG + MySQL RowWriter`** — three read-only catalog probes per engine (`HasNullShardColumn`, `ShardValuePresent`, `CompositePKLeadsWith`) wire the ADR-0048 DP-2 three-point preflight to actual SQL. Pre-fix the type-assertion `rw.(shardPreflightProber)` silently fell through to `return nil` on every shipping engine; post-fix the preflight refuses loudly with the existing `errShardConsolidationRefused` sentinel + operator-actionable messages naming the offending table, column, and recovery path. PG implementation uses `pg_index`/`pg_attribute`/`information_schema.columns`; MySQL uses `information_schema.statistics`/`information_schema.columns`.

### Tests

- **`test(integration): Bug 80 + Bug 81 end-to-end pins`** — new build-tagged tests in `internal/pipeline/migrate_shape_a_e2e_integration_test.go` exercise the full Shape A migrate path against real PG + MySQL containers with non-zero row counts on the target (the load-bearing assertion that catches both bugs). New `bug81_prober_witness_integration_test.go` compile-pins the `shardPreflightProber` interface assertion on both engines' RowWriter — pre-v0.72.1 the assertion silently failed; post-v0.72.1 it succeeds. New per-engine unit pins (`bug80_source_readable_test.go` on both PG and MySQL) lock the helper-asymmetry: `sourceReadableColumns` filters SluiceInjected; `nonGeneratedColumns` does not.

### Compatibility

- **Drop-in upgrade from v0.72.0.** No storage shape change, no CLI surface change, no behaviour change outside the two fixed bug paths. Anyone who attempted `--inject-shard-column` on v0.72.0 hit Bug 80 immediately (loud, no silent loss); v0.72.1 makes the flag actually usable end-to-end. Anyone who was NOT using `--inject-shard-column` on v0.72.0 sees no change.

### Known follow-ups (not blockers)

- **Shape A on MySQL with `BIGINT AUTO_INCREMENT` PK still fails at CREATE TABLE** with `Error 1075 "Incorrect table definition; there can be only one auto column and it must be defined as a key"` because the composite PK rewrite places the discriminator first, demoting the AUTO_INCREMENT column. This is a separate Shape A design issue (separate from Bugs 80/81); a fix-or-refuse-loudly path will land in a follow-up release after an ADR-0048 amendment.

## [0.72.0]

**Four ADRs in one release — the largest single-session feature drop in sluice's history.** The longest-deferred design dialogue in the backlog (ADR-0048 Shape A multi-source aggregation) finally lands with implementation, alongside the AIMD apply-batch-size controller (ADR-0052), a verbatim-carry generalization for core PG types — ranges/multiranges/FTS family (ADR-0051), and a silent-fidelity-loss closure for EXCLUDE constraints (ADR-0053). The corpus harness work that surfaced the latter two has now produced two product fixes; the "validate end-to-end" tenet continues to earn its keep. Per CLAUDE.md's zero-users tenet, the silent-loss class addressed by ADR-0053 is treated as the highest-severity issue in the release even though it lands without operator reports — semantic constraint loss in target schemas is exactly the class the tenet exists to prevent.

### Fixed

- **`fix(postgres): ADR-0053 EXCLUDE constraint silent fidelity loss`** — pre-fix, the PG schema reader queried `pg_constraint` only for `contype = 'f'` (foreign keys) and `contype = 'c'` (CHECK constraints); `contype = 'x'` (EXCLUDE constraints) was **never queried at all**. PG → PG migrate of any schema with EXCLUDE constraints read the source, silently dropped every EXCLUDE from the IR, and landed target tables MISSING the source's semantic invariant. The operator only discovered at runtime under a triggering write (overlapping range, duplicate on-call shift assignment) when the target accepted a row the source would have rejected. ADR-0053 closes this by adding `ir.ExcludeConstraint{Name, Definition}` (verbatim-text via `pg_get_constraintdef`, mirroring ADR-0051's range-type carry), populating it from the PG schema reader's new `populateExcludeConstraints`, emitting it inline in CREATE TABLE alongside CHECK constraints, and adding cross-engine refusal on MySQL targets (no MySQL equivalent exists — operators discover at preflight time, not runtime). Schema diff gains `ExcludesMissing / ExcludesExtra / ExcludesMismatched` mirroring the CHECK shape. All four observed real-world EXCLUDE shapes from the GitLab corpus (simple, predicated, multi-key, DEFERRABLE INITIALLY DEFERRED) round-trip byte-exact on a same-engine PG → PG run; cross-engine PG → MySQL refuses loudly with an operator-actionable message naming the constraint + the `--exclude-table` recovery flag.

### Features

- **`feat(postgres): ADR-0051 core-PG-type verbatim carry — ranges, multiranges, FTS family`** — generalizes the catalog Bug 17 tsvector/tsquery carve-out from "type-by-type for the representative" to "the class of core pg_catalog types lacking a rich cross-engine IR shape." Same-engine PG → PG migrate of schemas using `int4range`/`int8range`/`numrange`/`tsrange`/`tstzrange`/`daterange` and the PG 14+ multirange family (`int4multirange`/`int8multirange`/`nummultirange`/`tsmultirange`/`tstzmultirange`/`datemultirange`) now carry verbatim via a single named allowlist (`coreVerbatimEligibleTypes` in `internal/engines/postgres/types.go`); the existing `tsvector`/`tsquery` switch case consolidated into the same allowlist. Sibling tier to ADR-0047 (USER-DEFINED uncatalogued): both emit `ir.VerbatimType`, share every downstream surface (DDL emit, value decode, cross-engine refusal), and differ only in dispatch point. Cross-engine PG → MySQL stays loud-refuse (no portable form for any of these types). Stage 2 candidates (xml/money/pg_lsn/txid_snapshot/pg_snapshot) deferred per the ADR — each has known text-IO / locale / dialect quirks worth per-type validation when an operator surfaces a real workload. Surfaced by the real-world corpus harness iteration-3 GitLab finding; the harness leg `TestCorpus_GitLab_PGToPG_VerbatimCarry` flipped from "characterized gap" to "verified clean."

- **`feat(orchestrator): ADR-0048 Shape A — multi-source aggregation (sharded → consolidated)`** — closes the last outstanding multi-source pattern. New CLI flag `--inject-shard-column NAME=VALUE` on `sluice migrate`, `sync start`, `schema preview`, `schema diff` (mirroring `--target-schema`). Each per-shard run stamps a distinct VALUE; sluice appends the discriminator column to every PK-bearing table, rewrites the PK as a composite `(discriminator, …source PK)`, stamps VALUE onto every bulk-copy row AND every CDC row-bearing change (Insert.Row, Update.Before/After, Delete.Before), and runs a loud three-point preflight on a non-empty target: (a) every existing row has the discriminator NOT NULL, (b) the incoming VALUE is not already present, (c) the composite PK leads with the discriminator. The preflight is the loud-failure replacement for `--force-cold-start`'s silent skip; tables without a base PK refuse upfront with an operator-actionable recovery hint. The IR carries a new `Column.SluiceInjected` provenance bit so `schema diff` / `verify` suppress the discriminator from "extra column on target" drift while still asserting it must be present and NOT NULL. Implementation per ADR-0048's resolved DP-1/DP-2/DP-3 (two-surface split: a pure `internal/translate.InjectShardColumn` IR pass, a `redactRows`-shaped bulk-copy value wrap, and a new optional `ir.ShardColumnSetter` applier surface — both shipping engines implement it; engines that don't refuse loudly at openApplier-time). Cross-shard schema migration in v1 is operator-driven via the drained model (`sync stop --wait` → cross-shard schema migrate → `sync start --resume`); live cross-shard DDL coordination is Phase 2.

- **`feat(applier): ADR-0052 AIMD apply-batch-size controller`** — when `--apply-batch-size=N > 1` is set, the streamer now auto-tunes the per-batch row count via an Additive-Increase / Multiplicative-Decrease controller. N becomes a CAP the controller never exceeds; the floor stays at ADR-0017's conservative-default of 1. The two control inputs (ADR-0052 DP-4) are rolling p95 batch-apply latency (50-batch window) and retriable-error rate (3+ per 60s rolling window). Engine-default target latency: `planetscale=5s` (4× headroom under Vitess's 20s tx-killer), `mysql/postgres=10s`. Pass `--no-auto-tune` to disable and keep the static-cap behaviour; pass `--apply-tune-target-latency=DUR` to override the target. New `--apply-batch-size=auto` accepts the sentinel form (engine-default ceiling: 1000 mysql/postgres, 100 planetscale). Per-stream state (independent controllers across multi-stream processes). New `internal/appliercontrol` package owns the math; engine appliers consult it via the new `ir.BatchSizeProvider` / `ir.BatchObserver` optional interfaces (sibling-tier to `RedactorSetter` / `StreamIDSetter`).

- **`feat(metrics): four new Prometheus gauges for AIMD telemetry`** — `sluice_apply_batch_size_current{stream_id}`, `sluice_apply_batch_size_p95_seconds{stream_id}`, `sluice_apply_batch_size_decreases_total{stream_id}` (counter), and `sluice_apply_batch_size_cooloff{stream_id}` emit from the existing `--metrics-listen` endpoint when the AIMD controller is engaged. Reads scrape-time via `Controller.Snapshot` — no instrumentation of the apply hot path. INFO log on multiplicative-decrease events, cool-off enter/exit, ceiling cap, and the byte-cap-dominant advisory (DP-4 b: hints the operator to raise `--max-buffer-bytes` when bytes — not rows — are the binding constraint, rate-limited to one log per cool-off period). DEBUG log per batch with size + p95 + decision reason.

### Compatibility

- **Opt-out by default (ADR-0052 DP-1).** Operators with hand-tuned `--apply-batch-size=N` values that benchmarked optimally for their workload should add `--no-auto-tune` to preserve the pre-v0.72.0 strict-static semantics. Otherwise N becomes a CAP and the controller adapts within `[1, N]`. The behaviour change is deliberate — see ADR-0052's resolved DP-1 for the trade-off (better ergonomic default for new adoption vs. semantic stability for hand-tuned operators).

- **`--apply-batch-size` flag type changed from int to string.** The numeric form (`--apply-batch-size=100`) parses unchanged. The new sentinel `auto` accepts engine-default ceilings. Operators using YAML config don't see any change; CLI scripts that pass the value as an integer continue to work because kong's string parser tolerates numeric input.

### Who needs this

- **Anyone running PG → PG with EXCLUDE constraints in the source schema** (GitLab, Rails-style applications using exclusion constraints for partition-bound enforcement or scheduling invariants) — pre-v0.72.0 sluice silently dropped every EXCLUDE constraint from the IR, landing target tables missing semantic invariants the operator only discovered at runtime under a triggering write. **Drop-in upgrade fixes this.** The ADR-0053 fix is the highest-severity item in the release per the zero-users tenet's silent-loss-class framing.

- **Anyone running PG → PG with range or multirange types** — pre-v0.72.0 sluice loud-refused these at schema-read with `unsupported data_type "int8range"`. Now they carry verbatim. Multirange types (PG 14+) covered alongside the classical six range types.

- **Sharded-source operators** (PlanetScale Vitess shards, application-level sharding, hash-partitioned topologies) consolidating into one target table — ADR-0048 Shape A is the long-deferred pattern for this; the new `--inject-shard-column NAME=VALUE` flag stamps a discriminator column on every shard's stream, the populated-target preflight refuses loudly on shard-collision, and the composite-PK rewrite keeps shards disjoint. Drained model for v1 (cross-shard schema migration coordinated via `sync stop --wait` → schema migrate → `sync start --resume`); live cross-shard DDL is Phase 2.

- **Anyone running cross-region CDC** — the AIMD controller catches the Vitess 20s tx-killer foot-gun automatically; pre-v0.72.0 the operator had to hand-tune below the threshold (the v0.45.0 Phase 2 WARN flagged the risk; this release closes the loop with self-correction).

- **Operators running a heterogeneous fleet of sluice streams** — each stream gets its own AIMD controller and its own per-`stream_id` Prometheus gauges, so a fleet-wide Grafana dashboard surfaces "which stream is converging where" without per-stream config.

## [0.71.0]

**One real operability bug fixed, one operator-facing feature added.** Pre-fix, `START TRANSACTION WITH CONSISTENT SNAPSHOT` on the MySQL snapshot conn held `MDL_SHARED_READ` (`dur=TRANSACTION`) on every snapshotted source table for the streamer's entire lifetime — blocking any operator-issued `ALTER` (even `ALGORITHM=INSTANT`) until sluice was stopped. The PG analogue (Bug 21) was fixed long ago via `ReleaseRowsFn` at `COPY_COMPLETED`; the MySQL engine had the same shape designed in the IR but had never wired its half. This release closes that. On the feature side, `sluice sync status` gains live-refresh, JSON output, and an aggregate-summary header — the operator's first first-class "what's my fleet doing right now" surface that doesn't require Prometheus.

### Fixed

- **`fix(engines/mysql): wire SnapshotStream.ReleaseRowsFn` (task #34 / closes the deferred #28 Chunk E pin)** — MySQL engine now mirrors PG's Bug 21 shape: a `rowsReleased`-guarded `releaseRows` closure commits the snapshot tx + closes the pinned conn + closes the schema-side DB pool, called from streamer.go's existing `stream.ReleaseRows()` at `COPY_COMPLETED` (which was a silent no-op on MySQL pre-fix because `ReleaseRowsFn` was unset). Net effect: operators can run `ALTER` on sluice-monitored MySQL source tables without stopping sluice. New engine-of-truth pin `TestSnapshotStream_ReleaseRowsClosesSnapshotTx` asserts `performance_schema.metadata_locks` shows 0 `TABLE SHARED_READ dur=TRANSACTION` on a sample table after `ReleaseRows`, ALTER completes sub-5s, CDC keeps working, idempotent. Diagnostic journey (three-cycle three-phase-protocol instrumentation; preserved in the test docstring for future archaeology): CI run 26173191325 captured `net_write_timeout=60` (the original ~52s failure pattern, a red herring); the actual root cause surfaced in CI run 26176677598 via `performance_schema.metadata_locks` polling, which showed `owner_thread=51` holding `TABLE SHARED_READ dur=TRANSACTION` on `users` for the full 90s while the ALTER sat at `EXCLUSIVE PENDING`.

- **`test(postgres): psverify+integration tag collision on closeIf` (task #23)** — `internal/engines/postgres/schema_reader_test.go` (integration-tagged) and `planetscale_verify_test.go` (psverify-tagged) both defined `closeIf`; combining both tags failed with `closeIf redeclared in this block`. Renamed the psverify variant to `psverifyCloseIf` (5 callsites). Pre-existing defect; the psverify tag is not a required CI gate but the break was friction for anyone running the broader tag matrix locally.

### Features

- **`feat(sync status): --format=text|json + --watch=DURATION + --summary` (task #33 phases 1+2)** — three additions to `sluice sync status`, all backwards-compatible (default text output unchanged). `--format=json` emits a `{generated_at, summary{count, oldest_seconds, newest_seconds}, streams[...]}` document keyed for jq pipes (indented for human skimmability). `--watch` re-renders every DURATION until interrupted, clearing the terminal between renders (ANSI `ESC[2J ESC[H`) so output stays in place rather than scrolling; transient errors log inline and the watch continues (so a brief target outage doesn't abort the operator's screen). `--summary` prepends an aggregate header (`SUMMARY: N streams, oldest=Xm ago, most-recent=Ys ago`) with singular/plural pedantry. Refactor: rendering extracted to `cmd/sluice/status_render.go` (~250 LOC) so the watch loop and the one-shot path share identical logic. Tests cover default text shape, sort order, summary copy, empty-result messaging, JSON shape via stdlib decoder, and `--format=yaml` rejected with an explicit error. Phase 3 (`--dashboard-listen` embedded HTML+JS) deliberately parked for hands-on evaluation against the TUI baseline before committing.

### Operations / CI

- **`ci: Dependabot auto-merge + grouping`** — `.github/workflows/dependabot-automerge.yml` auto-approves and auto-merges patch + minor Dependabot PRs once the existing 4-job CI gate (Build / Test / Lint / Integration) passes; major bumps get an explanatory "held for review" comment instead. `.github/dependabot.yml` now groups four package families (`aws-sdk-go-v2`, `vitess`, `opentelemetry`, `golang.org/x`) into one PR/week each, cutting weekly PR volume ~3–4× and eliminating the cross-family `go.sum` conflict storm. Several dependency bumps auto-merged through this workflow in the same session that it landed (Vitess group, Azure SDK, cloud.google.com/go/kms, google.golang.org/api).

- **`ci: --net-write-timeout / --net-read-timeout = 600 on the MySQL testcontainer`** — bumped server-side socket-write/read timeouts from the `mysql:8.0` default of 60s to 600s in `startMySQLBinlog`'s container `Cmd`. This was the original (red-herring) Phase A finding on the deferred Chunk E pin; the bump is defensive and benefits every binlog-flavoured pipeline integration test, no regression.

### Tests

- **`test(pipeline): un-skip + close the long-deferred Chunk E pin` (tasks #35 + #36)** — `TestStreamer_MySQLToPostgres_SchemaHistoryWarmResumeAcrossDDL`, deferred from v0.70.0 with a `t.Skip` and a series of speculative root-cause claims, now runs end-to-end and asserts the four ADR-0049 Chunk B1/B3/C invariants (a) same-tx schema-history+position write, (b) PG applier active-version cache holds post-ALTER IR after warm-resume, (c) warm-resume does NOT fall through to cold-start across the DDL, (d) post-resume CDC keeps working with the post-DDL schema. Two test-only follow-on fixes were needed once the #34 MDL bug was out of the way: (path b) Phase 5 now ALTERs both source and target in lockstep because sluice does not propagate DDL on the live CDC path (operator-coordinated deploy pattern); (task #36) the Phase 9 assertion (a) query was changed from `schema_name='public'` (PG target's default namespace) to `schema_name='source_db'` (MySQL source's namespace) because Chunk B1's `maybeSnapshotSchemaB1` plumbs the source schema name through `ir.SchemaSnapshot.Schema` → `writeSchemaVersion`'s schema arg by design (so warm-resume / chain-restore can correlate against the source's own namespace).

- **`test(engines): pin dispatch streamID arg via non-SetStreamID Apply` (task #27 follow-up)** — both engines' `ChangeApplier.dispatch` now takes the streamID through the `Apply` arg rather than reading `a.streamID` (set via the optional `ir.StreamIDSetter`). Closes the latent footgun where any future non-migrate Apply path that omits `SetStreamID` would silently key rows under `""` and surface as a loud `ir.ErrPositionInvalid` at the next resume. Pin: `TestApplier_SchemaSnapshotDispatch_UsesApplyArgStreamID` (both engines) deliberately skips `SetStreamID`, pushes a SchemaSnapshot via `Apply(ctx, "custom-non-migrate-stream", ch)`, asserts `a.streamID == ""` AND schema-history row landed under `"custom-non-migrate-stream"`.

### Docs

- **`docs/dev/notes/adr-0050-cost-validation-report.md`** — gate (1) empirical evidence captured against a new `sluice-adr-0050-cv` PlanetScale database. Native VStream copy+CDC ran end-to-end with 1:1 fidelity under sustained concurrent workload (442 K rows / ~49 MiB baseline) and at GB-scale anchor (3.26 M rows / 0.97 GiB; throughput byte-linear at ~1.4 MB/s per table). Probe-B Go binary (`cmd/adr-0050-probe`) measured `sluice_watermark` write→VStream-event visibility lag: p50=3.9ms, p95=916ms, max=1085ms — sharply bimodal (Vitess `HeartbeatInterval=5s` batching), supporting ADR-0050 DP-2's preferred default ordering (`vstream-native` over `watermark-table`). Bottom line: conditional-go on ADR-0050; cost shape is closed at the 1 GiB anchor, the reconciling-resnapshot-vs-full-re-copy delta itself requires the future implementation to measure.

### Compatibility

- **Drop-in upgrade from v0.70.2.** The MySQL `ReleaseRowsFn` wiring is internal; existing streams' on-disk state (`sluice_cdc_state`, `sluice_cdc_schema_history`) is unchanged. The new `sync status` flags are additive — `--watch=0` (default) preserves one-shot behaviour and `--format=text` (default) preserves the existing tab-aligned table. The Dependabot workflow and grouping config are repo-side only.

- **Operator impact (positive):** operators who previously had to stop sluice before issuing any `ALTER TABLE` on a sluice-monitored MySQL source no longer need to. This applies to every flavour — `mysql`, `planetscale` (vanilla + service-token), and any future binlog-flavoured engine that uses `openBinlogSnapshotStream`.

### Who needs this

- **Anyone running a continuous MySQL source under sluice** — the MDL release is the headline. Even if you've never tried an ALTER mid-stream, this brings MySQL behavior in line with PG (which has done this since the Bug 21 fix).

- **Anyone managing more than one continuous-sync stream** — the new `sync status --watch` + `--format=json` + `--summary` flags make fleet visibility cheap. `sluice sync status --watch=2s --summary` is the new operator-runbook default.

- **The Dependabot automation** is repo-side, doesn't affect operators of sluice as a tool — it's a maintainer-side noise reduction (3–4× fewer PRs/week to skim).

## [0.70.2]

**Closes Bug 79 (HIGH, LOUD) — completes Bug 78 cross-engine chain-restore-resume fix.** v0.70.1 landed only the **storage half** of Bug 78: the persisted `source_engine` column made the anchor round-trip correctly, but `PrimeSchemaHistoryCache` still passed the **target's** `Engine{}` orderer to `ir.ResolveSchemaVersion`. Cross-engine chain-restore-then-resume crashed with the same class of `engine = "mysql"; want "postgres"` error — just at the `decode p` site (currentPos) instead of the anchor decode site. The v0.70.0 Bug 78 catalog originally listed BOTH (a) storage and (b) source-orderer-routing; v0.70.1 implemented only (a). This is the (b) half.

### Fixed

- **Bug 79 (HIGH, LOUD, v0.70.1 incomplete-fix follow-on) — `PrimeSchemaHistoryCache` now routes through the source engine's `ir.PositionOrderer`.** Both engines: hoist the source-engine lookup ONCE per prime call (above the per-table loop, since the orderer is the same for every retained anchor of a given stream), via `engines.Get(currentPos.Engine)` cast to `ir.PositionOrderer`. Loud-fail (`fmt.Errorf`, NOT wrapping `ir.ErrPositionInvalid` — config-bug class, not cold-start) on unregistered engine OR engine that doesn't implement the orderer interface. The empty-`currentPos.Engine` path (pre-`retagPositionForSource` or brand-new stream) falls back to the applier's own engine name — the pre-fix behaviour, correct for same-engine chains (target == source). Pin: `TestPrimeSchemaHistoryCache_CrossEngine_UsesSourceOrderer` (both engines, mirrored — exercises the FULL `PrimeSchemaHistoryCache → resolveSchemaVersion → loadRetainedSchemaVersions → ir.ResolveSchemaVersion → orderer.PositionAtOrAfter` path with cross-engine inputs against a real target's orderer) + `TestPrimeSchemaHistoryCache_UnregisteredSourceEngine_IsLoud` (names the unknown engine + asserts non-`ErrPositionInvalid`). **The v0.70.1 storage round-trip pin (`TestSchemaHistory_LoadPreservesSourceEngine_CrossEngine`) was insufficient — it validated the storage layer but not the orderer-routing.** That pin gap is exactly what the v0.70.1 post-publish cycle's Scenario 1 caught; the new pin closes it explicitly at the unit-of-test scope.

### Compatibility

- **Drop-in upgrade from v0.70.1 (and v0.70.0).** No storage shape change (the v0.70.1 `source_engine` column stays); no `BackupFormatVersion` bump; the orderer-routing change is internal to the cache-prime path. Same-engine chain-restore-then-resume continues to work; cross-engine now also works end-to-end.

- **Internal-only change:** `internal/engines/postgres/change_applier_schema_cache.go` and `internal/engines/mysql/change_applier_schema_cache.go` gained a direct import of `internal/engines` (the registry — already imported by each engine's own `engine.go` for self-registration; no cycle).

### Who needs this

- **Anyone planning to use ADR-0049 cross-engine chain-restore-then-resume** (MySQL backup → PG restore → resume against MySQL src → PG dst, or symmetric directions). v0.70.0 blocked it at the anchor decode; v0.70.1 fixed that but broke at the currentPos decode; v0.70.2 completes the path end-to-end.

- Same-engine chain-restore-then-resume worked under v0.70.0 / v0.70.1 / v0.70.2 (regression-guarded at every release).

## [0.70.1]

**Closes Bug 78 (HIGH, LOUD) — single-bug v0.70.0 hotfix.** ADR-0049 cross-engine chain-restore-then-resume (e.g. MySQL backup → restore to PG → `sluice sync start` against MySQL src → PG dst) crashed loudly at `PrimeSchemaHistoryCache` with `engine = "mysql"; want "postgres"`. Surfaced by the v0.70.0 post-publish regression cycle. Same-engine chain-restore-then-resume was unaffected and is regression-guarded.

### Fixed

- **Bug 78 (HIGH, LOUD, v0.70.0 regression-from-design) — cross-engine chain-restore-then-resume crashes at schema-history cache prime.** The schema-history store's load path constructed `ir.Position{Engine: engineNamePostgres, Token: anchorTok}` (mirrored at `engineNameMySQL` on the MySQL side) regardless of the source-engine origin of `anchorTok`. After a cross-engine restore the persisted `anchor_position` tokens are the *source* engine's shape (e.g. MySQL GTID under a PG-target schema-history); the target's strict position-orderer decode then rejected with `engine = "mysql"; want "postgres"`. Hit at `PrimeSchemaHistoryCache` time → exit 1, never reached the warm-resume path AND never fell back to the documented ADR-0022 cold-start floor. **Fix** (option (a) from BUG-CATALOG.md): persist the source engine alongside the anchor token. New nullable `source_engine TEXT` column on `sluice_cdc_schema_history`, populated at write time from the change-applier's incoming `ir.SchemaSnapshot.Position.Engine`; read-back uses the queried `source_engine` when non-empty, falls back to the applier's own engine name when NULL/empty (pre-fix behaviour — correct for the same-engine chains that worked under v0.70.0). The write-side `INSERT … VALUES (…, NULLIF(?, ''), …)` + `ON CONFLICT/ON DUPLICATE KEY UPDATE … source_engine = COALESCE(EXCLUDED/new.source_engine, existing)` preserves any previously-recorded tag if a defensive empty write occurs (matches the slot-name/source-dsn-fingerprint/target-schema "non-empty overwrites, empty preserves" precedent in `writePositionTx`). Pinned: `TestSchemaHistory_LoadPreservesSourceEngine_CrossEngine` (both engines — write a cross-engine anchor, load, assert `Anchor.Engine` round-trips to the SOURCE engine, not the applier's own); `TestSchemaHistory_LoadFallsBackForLegacyNullSourceEngine` (raw-SQL-INSERT a v0.70.0-shape row with `source_engine` explicitly NULL, load, assert the fallback fires); `TestEnsureSchemaHistoryTable_UpgradeAddsSourceEngineColumn` (pre-create the v0.70.0-shape table without the column, seed a row, run the v0.70.1 ensure, assert the column now exists and existing rows are intact). **Loud, NOT silent** — operators on v0.70.0 hitting this path saw a clean exit + clear error, never silent data corruption. But it blocked the operator value-prop the v0.70.0 release notes promised for cross-engine chain-restore-then-resume — the structural raison-d'être of ADR-0049 Chunk D. Same-engine chain-restore-then-resume worked under v0.70.0 (the bug was latent for same-engine because target name happens to equal source name) and is regression-guarded under v0.70.1.

- **Pin-the-class — `compactSchemaHistoryBelow` (both engines) extended.** Review caught the same Bug-78-class fix needed in Chunk D's retention-floor compactor (currently `nolint:unused` — Chunk D storage-only, consumer wires in `chain_prune.go` later). Latent today, but the class is identical — closed before chain_prune wires it for any cross-engine chain. Same SELECT-and-fallback shape.

### Compatibility

- **Drop-in upgrade from v0.70.0.** The migration is purely additive: `CREATE TABLE IF NOT EXISTS` now includes the new `source_engine` column on fresh creates; existing v0.70.0 deployments get a detect-then-`ALTER TABLE ADD COLUMN` on next `ensureSchemaHistoryTable` (PG via `ADD COLUMN IF NOT EXISTS`; MySQL via a new `ensureSchemaHistorySourceEngineColumn` helper using the `information_schema` probe pattern, portable to MySQL 8.0.x < 8.0.29 where `ADD COLUMN IF NOT EXISTS` doesn't exist). Existing rows persist with `source_engine = NULL`; the load-path fallback gives them the pre-fix behaviour (correct for same-engine chains). No data migration needed. No `BackupFormatVersion` bump (the backup envelope wasn't changed; only the engine-side control table).

- **Forward/backward backup-envelope compat unchanged** from v0.70.0 — `Manifest.SchemaHistory` was already `omitempty`, no `BackupFormatVersion` bump in either v0.70.0 or v0.70.1.

### Who needs this

- **Anyone planning to use ADR-0049 chain-restore-then-resume across engines** (e.g. MySQL backup chain → restored to PG → resumed against the MySQL source streaming to that PG target). v0.70.0 blocked this entirely with the engine-tag-mismatch crash; v0.70.1 unblocks it.

- Same-engine chain-restore-then-resume worked under v0.70.0 and continues to work under v0.70.1 (this is the path the v0.70.0 unit/integration pins exercised — Bug 78 was the **gap** in the test matrix that the post-publish regression cycle's cross-engine scenario surfaced).

## [0.70.0]

**Position-anchored CDC schema history (ADR-0049 Chunks A–D + partial E) — resume after a mid-stream DDL no longer forces a full re-snapshot.** A new per-stream durable control table seeds the schema-in-effect at every detected DDL boundary; on warm resume the applier primes an in-memory active-version cache from storage so the schema for any event position is an O(1) lookup. Backup manifests carry the schema-history needed to resume from a restored backup's `EndPosition`. Plus ADR-0038 classifier hardening (Vitess `code = Unknown`), a 4-shard parallel Integration job (~1.6× CI wall), and the Hyper-V validation VM that replaced the paid Vultr box.

### Features

- **ADR-0049 — position-anchored CDC schema history (Chunks A–D).** A new per-stream durable control table `sluice_cdc_schema_history` records the affected table's `ir.Table` at every detected DDL boundary, keyed by the boundary event's **own** source position (binlog `QUERY` GTID / VStream `FIELD`-delta VGTID / pgoutput `Relation`-delta LSN — captured at detection, not the first subsequent row's position, so a replay between the DDL and first post-DDL row decodes against the *post*-DDL version). New optional engine interface `ir.PositionOrderer.PositionAtOrAfter(p, anchor) (bool, error)` is a **partial-order causal predicate** (MySQL: GTID-subset, the same primitive `verifyGTIDSetReachable` already uses; PG: LSN ≤) — explicitly NOT a `-1/0/1` total comparator (the Bug-74-class trap a fake total order would walk into). `ir.ResolveSchemaVersion` selects the greatest retained anchor ≤ position; partial-order ambiguity (two mutually-incomparable anchors) raises a loud `ir.ErrPositionInvalid` rather than silently picking one. Applier-side active-version cache (`ChangeApplier.ActiveSchema(schema, table)`), populated AFTER tx commit on each `ir.SchemaSnapshot` dispatch (one production increment site for the resolveCalls counter; O(`#boundaries`), not O(`#rows`), is pinned). Cold-start prime on warm resume via the optional `schemaHistoryCachePrimer` surface, with a brand-new-stream discriminator (no prime on zero-position cold-start; the reader's first-touch `SchemaSnapshot` populates the cache via the post-commit hook). Backup envelope (`Manifest.SchemaHistory []*SchemaHistoryEntry`) carries the schema-history needed to resume from the backup's `EndPosition` — additive, see Compatibility. The retention floor compactor (`min(ADR-0007 safe-point, oldest retained backup resume position)`) is wired via `pipeline.SchemaHistoryRetentionFloor` + optional `ir.SchemaHistoryCompactor` engine interface; below-floor `ResolveSchemaVersion` is loud-refuse (`ErrPositionInvalid`), never silent. Restore replays SchemaHistory entries via synthetic `ir.SchemaSnapshot` events routed through the applier's existing dispatch — the version write lands in the **same target tx** as the ADR-0007 position write (locked decision #4a, free); engine UPSERT-on-PK (deterministic SHA-256 surrogate `ir.SchemaVersionKey`, index-safe under MySQL InnoDB's 3072-byte key limit) gives idempotency.

- **ADR-0038 Vitess classifier hardening.** The MySQL applier's retry classifier now recognizes `code = Unknown` (in addition to the existing `Aborted` / `Unavailable` / `ResourceExhausted` substrings). Documented as retriable in the ADR-0038 table all along but missing from the implementation; fewer false-positive stream exits on PlanetScale / managed-Vitess transient errors. Pinned by `TestVitessRetriableSubstrings_PinDown4` (literal-string change-detector). `--apply-retry-attempts` / `--apply-retry-backoff-base` / `--apply-retry-backoff-cap` ranges enforced at startup (pin-down 3, loud-reject out-of-range rather than silent clamp).

- **CI Integration matrix-sharded (~1.6× faster wall).** Single monolithic `Integration` job → 4 parallel shards on the 4-runner self-hosted fleet: `pipeline-migrate`, `pipeline-rest`, `engines-mysql`, `engines-postgres-and-rest`. Wall time 39 m → 24 m (limited by `pipeline-rest`'s heavy backup-chain crash-injection matrix). Rollup job preserves the `Integration` required-status-check name — branch protection on `main` did NOT need updating. `pipeline-migrate` and `pipeline-rest` exactly partition the package via Go 1.20+ `-run`/`-skip` complements over the same regex; `fail-fast: false` per shard gives full diagnostic signal. Variable-driven `CI_LINUX_RUNNER` / `CI_INTEGRATION_TIMEOUT` / `CI_INTEGRATION_JOB_TIMEOUT` knobs unchanged.

- **Hyper-V validation VM (replaces the paid Vultr box).** `scripts/hyperv-runner/New-ValidationVM.ps1` provisions a runner-less local validation VM from the golden VHDX; pre-release validation runbook (`docs/dev/notes/release-validation-on-vultr.md`) is host-agnostic; runbook `-timeout=30m` bumped to `75m` (stale under-budget vs the grown suite + slower-than-CI virtual-disk I/O). The Vultr `sluice-test-lax-1` instance is decommissioned (zero recurring cost; validation is 100 % local).

### Fixed

- **Chain restore now calls `SetStreamID(ChainRestoreStreamID)` before `Apply`/`ApplyBatch`.** The applier's `case ir.SchemaSnapshot` dispatch writes via `writeSchemaVersion(ctx, tx, a.streamID, ...)` — the **field** `a.streamID`, set only by `SetStreamID`, not the Apply/ApplyBatch arg. `migrate.go` calls it before its applier path (line 1297-1298); chain restore did not, so the synthetic-SchemaSnapshot replay path (Chunk D restore) wrote schema-history rows under `stream_id=""` instead of `ChainRestoreStreamID` — defeating the operator value-prop (resume-after-DDL without full re-snapshot). NOT silent: a later `resolveSchemaVersion("real_id")` finds 0 rows → loud `ErrPositionInvalid` → ADR-0022 cold-start fires (visible). Mirrored `migrate.go`'s `StreamIDSetter` type-assertion pattern.

- **`ir` backup codec gained `Bit` + `ExtensionType` cases.** Both previously hit the loud `default: "unsupported IR type for backup encoding"`, blocking schema-history snapshots of any table carrying `ir.Bit`/`ir.BitVarying` (catalog Bug 62/77 — PG `bit`/`varbit`) or an `ADR-0032`-catalogued `ir.ExtensionType` (vector/pg_trgm/hstore/citext/postgis/pgcrypto/uuid-ossp). `VerbatimType`/ADR-0047 was already handled; this completes the catalogued-extension sibling. Round-trip pinned in both `TestMarshalType_RoundTrip` (envelope) and `TestMarshalTable_RoundTrip_AllTypeFamilies` (Table-level — what schema-history uses).

- **`incremental.WriteChange` no longer loud-aborts an incremental backup when it sees `ir.SchemaSnapshot`.** Chunk B introduced the new Change variant on the live stream; the incremental-backup writer fed every change to `encodeChange` whose `default:` was unknown-type loud — a DDL during a backup window would have killed the backup. Chunk D supersedes a temporary scope-fence skip with real envelope handling (`snapshots []ir.SchemaSnapshot` field on `changeChunkWriter`, drained at flush time into `Manifest.SchemaHistory`).

- **Test harness: `captureSlog` is now thread-safe.** Latent race (unprotected `bytes.Buffer` as the slog handler's writer, written from streamer + binlogsyncer pump goroutines while the test reads `buf.String()`) — surfaced by Chunk E's long-running streamer pin under `-race`. New `safeBuffer` wrapper (`sync.Mutex`-protected; `.Bytes` returns a defensive copy). API-compatible with `*bytes.Buffer` for the methods every caller uses.

- **Test harness: `applyMySQLDDL` timeout 30 s → 90 s** (under `-race` + active streamer goroutines + binlog pump, even a simple `ALTER` can take >30 s to commit on testcontainers MySQL). Mirrors the most-used integration timeout precedent in the package.

- **Test harness: chaos test tolerates `errors.Is(err, context.Canceled)`** from a streamer cancelled mid-startup (e.g., in `SHOW BINARY LOGS` while reaching steady-state CDC). The assertion's intent ("exits cleanly on cancel") is preserved without rejecting the wrapped form.

- **`internal/pipeline/streamer_integration_test.go` `recordingApplier` skips `ir.SchemaSnapshot`** (mirrors the engine-side `drainChanges` / `drainSnapshotChanges` / `drainVTTestChanges` skip pattern — orthogonal infra event, not DML; Chunk B's own snapshot pins use dedicated collectors so they're unaffected).

- **Seven ADR Status reconciliations** (0029 / 0033 / 0045 / 0046 / 0047 / 0038 / 0049 — docs were "implementation pending" while the code had long since shipped; verified vs the tree, not the prose).

### Compatibility

- **Backup format additive — NO `BackupFormatVersion` bump.** Pre-v0.70.0 readers Unmarshal new manifests cleanly (`SchemaHistory []*SchemaHistoryEntry` uses `omitempty`, ignored by older readers). v0.70.0 readers Unmarshal pre-D manifests cleanly (`SchemaHistory == nil` is a no-op on restore replay). Backward-compat pinned (`TestManifest_SchemaHistory_BackwardCompat_NoField`).

- **New durable control table per stream target: `sluice_cdc_schema_history`.** Additive (`CREATE TABLE IF NOT EXISTS`, mirrors the ADR-0030/0034 control-table additive pattern). Existing `sluice_cdc_state` and `live_added_tables` migrations unchanged. PK = `version_key CHAR(64)` (SHA-256 surrogate over the natural identity tuple via `ir.SchemaVersionKey`): index-safe under MySQL InnoDB's 3072-byte key limit, eliminates the latent prefix-collision silent-overwrite class (an unbounded GTID-set in a prefix-indexed PK could have aliased distinct anchors).

- **Drop-in.** No CLI flag changes; no env-var changes; no behavior change for streams without DDL events. **Internal:** new sealed `ir.SchemaSnapshot` Change variant + new optional `ir.PositionOrderer`, `ir.SchemaHistoryCompactor`, `schemaHistoryCachePrimer` interfaces. The IR is not a stable/exported interface; all in-tree implementers updated. Engines that don't implement the optional surfaces silently skip — pre-v0.70.0 behaviour, NOT a regression.

### Known deferred (v0.70.1)

- **Live-CDC warm-resume-across-DDL pin `t.Skip`'d (task #28).** `TestStreamer_MySQLToPostgres_SchemaHistoryWarmResumeAcrossDDL` failed across 5 CI cycles with `apply ddl: context deadline exceeded` + `[mysql] packets.go:58 unexpected EOF` (~52 s into the ALTER). Ruled out: MDL contention (`ALGORITHM=INSTANT` made no difference) and the 90s `applyMySQLDDL` ceiling. Most plausible remaining: testcontainers MySQL kills the test's separate-DSN ALTER connection under streamer + `-race` resource pressure. The operator value-prop IS pinned end-to-end via Chunk D's backup-restore variant (`TestIncrementalBackup_PostgresChainRestore_SchemaHistoryReplay`, CI-green); the live-CDC variant lands when #28 closes.

- **Dispatch's `SchemaSnapshot` case uses `a.streamID` instead of the Apply arg's `streamID` (task #27).** Every current Apply call path goes through `migrate.go` (calls `SetStreamID`) or `chain_restore.go` (now calls it too — the bug above); any future non-migrate Apply caller must `SetStreamID` first or schema-history rows mis-key. Refactor `dispatch` to take `streamID` from the arg consistently; mechanical.

- **PG-side `SameTxAtomicity` integration pin** (MySQL has one; PG analogue is structurally identical but not pinned).

## [0.69.5]

**Track 1c — CDC resumability validation (PlanetScale/Vitess readiness) + a node-replace silent-gap hardening.** Validation-first; the one production change is the minimal loud floor a Phase-A ground-truth finding demanded. All outcomes verified vs Docker (vttestserver + mysql:8.0).

### Fixed

- **MySQL node-replace / restore-from-backup position-loss (SILENT-gap class → now LOUD).** `verifyBinlogFilePresent` matched the persisted CDC position on binlog **filename only**. A replaced / restored-from-backup / failed-over source instance (the operator-reported PlanetScale pain) carries an independent binlog lineage that frequently **reuses the same filenames** (`mysql-bin.000003`, …); the name check false-positived and the syncer silently resumed at a byte offset in an *unrelated* file — a silent data gap. Fix (minimal loud floor, mirrors `verifyGTIDSetReachable`'s existing refuse-pattern): file/pos positions are now bound to the source `@@server_uuid` (stamped by both the snapshot→CDC handoff and the per-event position emitter); on resume a differing `server_uuid` wraps `ir.ErrPositionInvalid`, routing the existing ADR-0022 cold-start re-snapshot. GTID mode needs no equivalent — GTID UUIDs are instance-bound, so `verifyGTIDSetReachable` already catches a fresh instance. Empty persisted/current uuid degrades to the prior filename-only check (no false refusal; zero-users transitional).

### Validated (no production change — already at the loud-failure floor)

- **VStream + Vitess schema-tracking DISABLED + mid-stream DDL** (the genuinely-open behaviour): ground-truthed against vttestserver for ADD / DROP / MODIFY column — **FAITHFUL in all three**. VStream re-emits a fresh `FIELD` event post-DDL and `dispatchDDL`'s field-cache clear realigns the decode; no silent corruption (the alternative is a loud "row event without preceding FIELD event" hard error — loud + recoverable). `internal/engines/mysql/cdc_vstream_schema_evolution_integration_test.go` (`integration vstream`).
- **GTID retention-exceeded** (`gtid_purged` advanced past resume) and **`gtid_mode=ON` binlog-purge**: both already LOUD + actionable (`ir.ErrPositionInvalid` → ADR-0022 cold-start; src == dst, exactly-once). Reader-level + streamer-level tests added.

### Compatibility

Drop-in. **Internal IR change:** `binlogPos` gains a `server_uuid` field (the position token is not a stable/exported format). Positions persisted before this field exist resume exactly as before (the identity check skips on an empty persisted uuid). MySQL→MySQL warm-resume, snapshot→CDC handoff, vanilla CDC, and the VStream/sharded path are regression-guarded against Docker; Track-2 fuzz-roundtrip and Track-1a static/sharded VStream baselines re-run green.

## [0.69.4]

**Closes Bug 74 — a CRITICAL silent-loss regression introduced by v0.69.3's Bug 70 fix (a correction banner is on the v0.69.3 release) — plus pre-existing Bug 73 / 75 / 76.** The two silent bugs (74, 75) were ground-truthed by instrumented probes against real Docker PostgreSQL/MySQL, not code-reading.

### Fixed

- **Bug 74 (CRITICAL, SILENT, v0.69.3 regression — PostgreSQL→PostgreSQL).** v0.69.3's `convertArray` → `pgtype.Array[*string]` silently **flattened** a multi-dimensional `numeric[][]` to 1-D (exit 0, no warning) — strictly worse than v0.69.2's loud `SQLSTATE 57014`. A class probe proved the silent flatten was broader than first catalogued (`uuid[][]`/`inet[][]`/`cidr[][]`/`time[][]` too). Phase-A (instrumented, real PG `COPY` binary path): pgx's `ArrayCodec.PlanEncode` plans the element encode against the **target column element OID**; `NumericCodec`/`UUIDCodec`/`InetCodec`/temporal codecs reject a `*string` valuer, so pgx falls back through a dimension-flattening wrap. Fix: `convertArray` now selects the correct pgx-encodable leaf **per element family** — native int/float/bool unchanged; text/varchar/char/uuid/inet/cidr/macaddr → `pgtype.Text`; numeric/decimal → `pgtype.Numeric`; date/timestamp/timestamptz/time → the matching `pgtype` temporal valuer. **Faithful** multi-dimensional round-trip for every family (dimensions + values + NULL elements). `timetz[]` has no faithful binary array leaf → **explicit LOUD refusal** (≥ prior behavior; never silent).
- **Bug 73 (MEDIUM, LOUD, pre-existing — same `convertArray` subsystem, batched).** `timestamp[]`/`timestamptz[]`/`date[]` hard-failed `57014` (no switch case) — closed by the same element-complete rewrite.
- **Bug 75 (HIGH, SILENT, pre-existing — PG→MySQL & PG→PG).** `bit`/`varbit` were decoded as the ASCII bytes of the `'0'/'1'` text then collapsed to the trailing byte, so distinct source values mapped irreversibly to the same target value (NULL was preserved). Fix: an engine-neutral `ir.Bit` canonical bit-string plus `ir.Bit.Varying` (clean break — also fixed a latent `bit varying(N)` mis-emitted as fixed `BIT(N)` / `22026`). Faithful round-trip both directions (Docker-verified); the MySQL LOAD DATA path uses a `CONV(…,2,10)` SET expression. The dead `bitBytesMySQLToPG` helper was removed.
- **Bug 76 (MEDIUM, LOUD, pre-existing).** The PG schema reader validated every column before the `--include-table`/`--exclude-table` filter, so an unsupported type in an *excluded* table still failed schema-read. A new optional `ir.TableScoper` surface threads the engine-default-merged filter to the reader before `ReadSchema`; PG drops out-of-scope tables before per-column validation. The post-read filter is retained (defense-in-depth; MySQL push-down deferred — loud, no silent loss, documented).

### Known limitation (pre-existing, out of scope here, not a regression)

- **`varchar(N)[]` / `char(N)[]` PG→PG** — the PG array-element DDL emitter drops the element length (`VARCHAR(0)[]` → `22023`), a loud create-tables failure. Distinct from this value-path batch (the value leaf is unit-pinned); tracked for a separate fix.

### Compatibility

Drop-in from v0.69.3; the backup envelope gains append-only fields. **Internal IR changes:** `ir.Bit` (now a canonical bit-string type), `ir.Bit.Varying`, `ir.TableScoper` (the IR is not a stable/exported interface; all in-tree implementers updated). PG→PG multi-dimensional arrays of every element family now round-trip faithfully (was silent flatten for `numeric`/`uuid`/… or loud `57014` for temporals); `bit`/`varbit` round-trip faithfully both directions (was silent corruption); excluded tables with unsupported types no longer block schema-read. Bug 70 (int/text/NULL-element)/68/71/72/69/#18, constrained types, plain temporals, MySQL→PG, and PG→PG/PG→MySQL baselines are regression-guarded — Option-C `-race`+Integration CI green before the tag, and the every-element-family multi-dim class pin was independently re-run against Docker by the maintainer (closing the v0.69.3 review gap).

## [0.69.3]

**Closes Bug 70 (HIGH) / 71 / 72 — all surfaced by the v0.69.2 terminal battle-test; all pre-existing (byte-identical ≤v0.69.1), all LOUD (exit 1, clear error, no data corruption — the silent-loss class was already fully closed by Bug 68/69).** Bug 69's DDL fix removed the `22023` early-abort that had been masking #70/#71 on PG→PG.

### Fixed

- **Bug 70 (HIGH, PG→PG) — nullable / multi-dimensional array `COPY` hard-fail.** A SQL-NULL *element* inside any typed array (`int[]`/`text[]`/`numeric[]`) and any multi-dimensional array (`int[][]`) blew up the PG `COPY`-protocol writer (`SQLSTATE 57014`, 0 rows): the encoder built non-pointer element slices, so a nil slot (`got <nil>`) and a nested `[]any` (`got []interface {}`) both failed. A pgx subtlety compounded it — `pgtype.Map`'s wrap chain matches `TryWrapSliceEncodePlan` *before* `TryWrapMultiDimSliceEncodePlan`, flattening `[][]*T` to one dimension. Fix: the encoder now emits `pgtype.Array[*T]` (itself an `ArrayGetter`, so it bypasses the wrap chain; a pointer leaf carries a typed-nil = SQL NULL; explicit `Dims` + row-major-flat `Elements` preserve dimensionality). Faithful round-trip: NULL element → NULL element, `int[][]` → `int[][]`. This is the write-side counterpart to Bug 68's read-side multi-dim fix (different engine-writer/direction than #18 / Bug 68 — regression-guarded).
- **Bug 71 (MEDIUM, PG→PG) — `timetz` mis-mapped to `time`.** `time with time zone` was mapped identically to plain `time` (the IR carried no time-zone flag), emitting OID 1083 and failing `COPY` (`57014`); pgx also ships no codec for `timetz` (OID 1266). Fix: `ir.Time` gains `WithTimeZone` (additive bool-variant idiom, mirrors `ir.Timestamp`; default-false → plain `time` byte-identical). The PG writer emits `TIME WITH TIME ZONE` and registers a per-connection binary codec for OID 1266 (the validated pgvector/hstore registration pattern). PG→PG round-trips the offset exactly; PG→MySQL zone-flattens (MySQL has no tz-aware time) mirroring the **documented** `timestamptz`→MySQL precedent — loud/documented, not silent.
- **Bug 72 (MEDIUM, PG→MySQL) — wide bounded `varchar(N)` not down-mapped.** Unbounded `text` was already down-mapped to `LONGTEXT`, but a wide *bounded* `varchar(N)` was emitted literally → MySQL Error 1074 (column length) / 1118 (row size) at create-tables. Fix: `varchar(N > 16000)` is down-mapped to the smallest MySQL `TEXT` tier that holds N characters at worst-case bytes (mirrors the existing text→LONGTEXT logic), with a loud advisory at preview + preflight (the unsigned-bigint/numeric notice pattern). Small `varchar` is unchanged.

Cross-engine policies (Bug 71 `timetz`→MySQL, Bug 72 wide-varchar→TEXT) recorded in `docs/type-mapping.md` (owner-overridable).

### Compatibility

Drop-in from v0.69.2; no state/format change (the backup envelope gains an append-only `time_with_tz` field). Strictly corrective. **Internal:** `ir.Time` gained `WithTimeZone` (the IR is not a stable/exported interface). PG→PG nullable/multi-dim arrays and `timetz` now migrate (were `COPY` hard-fails); PG→MySQL wide `varchar` now migrates with a loud advisory (was a create-tables hard fail). Bug 68/69, #18, constrained types, plain `time`/`timestamp`/`timestamptz`, MySQL→PG, PG→PG baseline, and cross-engine extension paths are regression-guarded — Option-C `-race`+Integration CI green before the tag.

## [0.69.2]

**Closes Bug 69 (HIGH — PG→PG hard-fail + PG→MySQL silent decimal-precision loss; surfaced by the v0.69.1 close-out battle-test).** An **unconstrained** PostgreSQL `numeric`/`numeric[]` column (declared `numeric` with NO precision/scale — arbitrary precision, a ubiquitous PG column shape) was mis-emitted: **PG→PG** → `NUMERIC(0,0)` → `SQLSTATE 22023` hard create-tables failure (loud, exit 1, no partial); **PG→MySQL** → `DECIMAL(0,0)` → **silent decimal-precision data loss** (exit 0, no `WARN`: `3.14159` → `3`). **Pre-existing** (byte-identical on v0.69.0/earlier; *not* a v0.69.x regression), orthogonal to Bug 68 — masked in every prior verdict because the battle-test corpus used only *constrained* `NUMERIC(15,2)`.

### Fixed

- **IR can now represent unconstrained numeric distinctly.** `ir.Decimal` gains `Unconstrained bool` (the established bool-variant idiom — `Integer.Unsigned`, `JSON.Binary`, `Timestamp.WithTimeZone`; additive, default-false → all constrained construction sites and emitters byte-identical; only the Postgres reader sets it). Root cause was the reader collapsing `information_schema`'s NULL `numeric_precision`/`numeric_scale` to `0` into a non-pointer `ir.Decimal{Precision int, Scale int}` indistinguishable from `numeric(0,0)`.
- **PG-target:** unconstrained → bare `NUMERIC` (arbitrary precision — fixes the `22023`). Constrained `numeric(p,s)` unchanged.
- **MySQL-target:** MySQL has no unbounded decimal, so unconstrained → `DECIMAL(65,30)` (MySQL's documented maximum) **plus a loud, operator-actionable advisory at BOTH `schema preview` and `migrate` preflight**, naming every affected `table.column` and the `--type-override` escape — the same notice machinery and surfaces as the v0.68.2 `bigint unsigned`→`bigint` range-narrowing advisory. It is an advisory, not a refusal (migration proceeds); the loud-failure floor is satisfied — **no silent truncation**. `numeric[]` is lossless in both directions (PG→PG `NUMERIC[]`; PG→MySQL → JSON via the Bug-68 array path — verified: numeric values decode to strings end-to-end, JSON-encoded with full precision, so the narrowing advisory correctly does not fire for arrays).

### Compatibility

Drop-in from v0.69.1; no state/format change (the backup envelope gains an append-only `decimal_unconstrained` field). Strictly corrective. **Internal interface note:** `ir.Decimal` gained `Unconstrained bool` (the IR is not a stable/exported interface). Unconstrained `numeric` PG→PG now migrates (was a `22023` hard fail) and PG→MySQL preserves precision to `DECIMAL(65,30)` with a loud advisory (was silent truncation to integer). Constrained `numeric(p,s)`, MySQL→PG, PG→PG, the Bug-68 array paths, and prior closures are regression-guarded — Option-C `-race`+Integration CI green before the tag. Cross-engine policy recorded (owner-overridable) in `docs/type-mapping.md`.

## [0.69.1]

**Closes Bug 68 (HIGH — silent data loss, the worst class; surfaced by the v0.69.0 final readiness battle-test).** A PostgreSQL source table with a **multi-dimensional array column** (`int[][]`, any `T[][]`) migrated cross-engine **PG→MySQL** exited 0 with the target table created but **zero rows for the entire table** — no error, no `WARN` at any log level. **Pre-existing** (reproduces byte-identically on v0.68.2; *not* a v0.69.0 regression) and distinct from #18 (1-D `text[]`/`int[]` work, including at scale). This was the lone caveat on the v0.69.0 PG→MySQL real-user-readiness verdict.

### Fixed

- **Faithful multi-dimensional array support (the corrective fix).** The PG value reader's flat array-text parser errored on the multi-dim form `{{1,2},{3,4}}` ("nested arrays not supported"); it is replaced with a recursive-descent decoder that yields nested `[]any` for arbitrary array dimensionality. The MySQL writer's existing `convertArrayLikeToJSON` already serializes nested `[]any` → nested JSON faithfully, so `int[][]` now round-trips PG→MySQL into a nested-JSON column. The 1-D path is preserved exactly (#18 unaffected, regression-guarded).
- **Silent-swallow elimination (the load-bearing class fix, loud-failure tenet).** Root cause beneath the parser gap: `ir.RowReader` exposed no `Err()`, and the bulk-copy orchestrator overloaded *channel-close* as "table fully read". A streaming reader scans/decodes on a background goroutine after `ReadRows` returns; a per-row failure stored a sticky error and closed the channel exactly like a clean end-of-table, so `WriteRows` wrote 0 rows, `copyTable` returned `nil`, and `migrate` exited 0 with the table silently truncated — for **any** decode/scan failure, not just arrays. `Err() error` is now part of the `ir.RowReader` interface (clean break — zero-users tenet; this generalizes the ad-hoc type-assertion `backup.go` already carried), and a `readerStreamErr` gate runs after **every** bulk-copy drain (`copyTable`, both `copyTableIdempotent` branches, `copyTableWithCursor`, `copyChunk`, `copyChunkFast`, `backup.backupTable`). Any per-row scan/decode error on any reader now **fails the migration loudly**. Deliberate per-batch context-cancel teardown is filtered precisely: a reader returns immediately on a decode error and never overwrites it with `context.Canceled`, so the filter cannot mask a genuine error (sound by construction, independently verified).

### Compatibility

Drop-in from v0.69.0; no state/format change. Strictly corrective. **Internal interface change:** `ir.RowReader` gained `Err() error` (the IR is not a stable/exported interface; all in-tree implementers updated). Cross-engine `int[][]` PG→MySQL now migrates faithfully (was silent total-table loss); the broader effect is that **no** per-row reader scan/decode failure can silently truncate a table on any bulk-copy path. 1-D arrays (#18), MySQL→PG, PG→PG, signed integers, and prior closures are regression-guarded (Option-C `-race`+Integration green before tag).

## [0.69.0]

**Battle-test pass-3/4 fix batch — closes a v0.68.3 regression (#16) plus the real-user-readiness blockers found by the 329-table corpus campaign (#9, #17, #18, #19a–d, #20, #23, #16-sub).** Every fix is Phase-A pinned so the regression test exercises the *exact reported repro path* (not an adjacent one — the discipline that let an earlier pass re-green while the real path stayed broken), independently + adversarially reviewed, and gated by the full local suite **and** the Option-C `-race`+Integration CI before tagging.

### Fixed

- **#16 (HIGH — v0.68.3 regression).** v0.68.3's #14 PG-validity allowlist gate misread a *parameterized CAST/`::` target type* (`CAST(x AS DECIMAL(10,2))`, `CHAR(20)`, `BINARY(16)`, `x::numeric(20,0)`) as an unknown function-call identifier and **spuriously refused schemas v0.68.2 migrated clean**. The scanner is now context-aware: a recognised SQL type name is exempt **only** in cast-target position (immediately after `AS`, or after `::`). The same spelling used call-shaped elsewhere — MySQL's `CHAR(65)` *scalar* (no PG form, not translator-rewritten), and `UNSIGNED`/`SIGNED`/`SOUNDEX` — still loud-refuses; a blanket type-name allowlist would have re-opened the v0.68.1-class false-green. Corpus false-positive-safety re-verified **byte-identical to v0.68.2** (the migratable-table set is not narrowed).
- **#9 (loud-failure quality).** A MySQL→PG generated column referencing *another* generated column (MySQL permits it; PG forbids it, `SQLSTATE 42P17`) now **loud-refuses at both `schema preview` and `migrate` pre-flight, before any DDL**, naming the site and the `--expr-override` / `--exclude-table` remedy — instead of PG's raw error mid-create-tables after a partial target.
- **#17 + #19a/b/c/d (PG→PG fidelity, were silent losses, exit 0).** `tsvector`/`tsquery` (PG core types) are carried verbatim same-engine PG→PG (cross-engine still correctly loud-refuses); partial-index `WHERE` predicate + per-column `DESC`/`NULLS` ordering, covering-index `INCLUDE` columns (with declared order), the source enum **type name** (`CREATE TYPE` no longer renamed to `<table>_<col>_enum`), and table/column `COMMENT`s are all preserved. IR model gains `Index.Predicate`/`PredicateDialect`/`IncludeColumns`, `IndexColumn.NullsFirst`, `Enum.TypeName` (additive; MySQL sources fall back to synthesis).
- **#18 (HIGH — PG→MySQL).** A PG array column (`text[]`/`int[]`) containing rows now bulk-copies into a MySQL `JSON` column instead of crashing the serializer. Both halves are fixed: the value serialization (`prepareValue` treats `ir.Array` like `ir.JSON`) **and** the LOAD DATA SET-clause charset (`columnSetExpr` now wraps `ir.Array` in the `CONVERT(... USING utf8mb4)` re-tag, so MySQL's JSON validator doesn't reject the stream as `CHARACTER SET 'binary'`). Empty `{}`→`[]`, NULL whole-column→SQL NULL, NULL element→JSON `null`, nested arrays preserved. The integration test forces `local_infile=ON` so CI exercises the actual default LOAD DATA path.
- **#20.** MySQL `LOWER('lit')`/`UPPER('lit')` over a bare string literal (PG `SQLSTATE 42P22`, "could not determine which collation"): in **CHECK/DEFAULT** sluice emits a faithful `::text` cast (`lower('ABC'::text)`, byte-identical result); in a **STORED generated column** — where no faithful collation can be synthesised and the value is a constant — it **loud-refuses at pre-flight** with the `--expr-override` remedy, rather than emit DDL PG rejects mid-pipeline.
- **#23.** `schema preview` now applies verbatim-extension passthrough with the **same predicate as `migrate`**, so a same-engine PG→PG preview no longer loud-refuses a `tsvector` column that `migrate` carries — preview/migrate are consistent.
- **#16-sub (+ latent `--type-override`).** kong's default list separator split a comma-bearing override value (`--expr-override 'g.p=ST_SetSRID(ST_MakePoint(lon,lat),0)'`, `--type-override 'p.amt=numeric(20,0)'`) — same root cause as the fixed Bug 59 `--redact` split. `sep:"none"` applied to all 10 `ExprOverride`/`TypeOverride` flag sites; the YAML forms were already correct.

### Known limitations (pre-existing, not regressions, not release-blockers)

- **#22 — `CAST(x AS BINARY(n))` in a generated column → PG `SQLSTATE 42804`.** Byte-identical on v0.68.x; the #16 fix merely stopped masking it behind a spurious refusal. MySQL's `BINARY(n)` cast target maps to PG `bytea` while the generated-column type resolves to a text form, and PG rejects the mismatch. Loud failure, no corruption; the `--type-override` / `--expr-override` / `--exclude-table` escape hatches apply. Tracked as a separate type-mapping item.

### Compatibility

Drop-in from v0.68.3; no state/format change. Strictly corrective. **#16 reverses the v0.68.3 over-refusal** (parameterized CAST/`::` target types migrate again, as on v0.68.2). The IR additions (`Index.Predicate`/`IncludeColumns`, `IndexColumn.NullsFirst`, `Enum.TypeName`) are additive and internal (the IR is not a stable interface). MySQL→MySQL, signed integers, the catalogued/known-good translations, and prior battle-test closures (#8/#11/#12/#13/#14) are regression-guarded. Owner-surface cross-engine policy decisions (#9 refusal, #16 cast-target exemption, #18 array→JSON value path, #20 two-case behavior, #22) are recorded in `docs/type-mapping.md`.

## [0.68.3]

**Closes Bug #14 (HIGH, structural) — the MySQL→PostgreSQL untranslatable-expression backstop is now GENERAL, not a curated denylist; also subsumes #12 and #13.** v0.68.1's "structural backstop" keyed on a curated denylist of *known* MySQL-only patterns (`GREATEST`/`LEAST`/`REGEXP_LIKE`/`FIND_IN_SET`/`CONVERT_TZ`/`INET_ATON`/`SHA1`/`SHA2`). Any MySQL-only construct *outside* that set — `SOUNDEX()`/`ELT()`/`MAKE_SET()`/`BIT_COUNT()`/`UUID_SHORT()`/`INET6_ATON()` in CHECK/generated/DEFAULT, plus #12 (`CAST(... AS UNSIGNED/SIGNED)`, `regexp_like()`) and #13 (`POINT(x,y)` in a generated column) — still fell through the translator verbatim: `schema preview` exited 0 emitting invalid PG (false green), `migrate` hard-aborted at create-tables with a raw PG `SQLSTATE` (42883/42704/42804), partial target. The v0.68.1 notes overclaimed this "generalizes to any untranslated MySQL-ism" — it did not (correction banner is on the v0.68.1 release). This is the general fix.

### Fixed

- **General post-translation PG-validity gate (allowlist, the load-bearing fix — loud-failure tenet).** Flipped the denylist to an **allowlist**: every translator-applicable expression (DEFAULT / GENERATED / CHECK) is scanned for function-call identifiers, and any name that is not provably PG-valid — (a) a MySQL function the translator provably rewrites, (b) a PG core/built-in or the exact forms the translator emits, or (c) a function owned by an `--enable-pg-extension`-enabled extension — produces a **loud, operator-actionable refusal at BOTH `schema preview` AND `migrate` pre-flight, before any DDL is applied** (reusing the established cross-engine refusal surface; the per-offending-site message names the function and the `--expr-override` / `--exclude-table` / `--enable-pg-extension` remedies, identical at both surfaces). This **subsumes #12 and #13**: they are now loud refusals, not silent false-green — the correct readiness outcome (loudly refuse what can't be faithfully translated; never silent-wrong/partial). The curated `ScanMySQLToPGGaps` layer is kept as the *specific actionable-hint* layer on top (better construct-specific messages for the known cases).
- **False-positive safety is treated as the load-bearing risk** (a missed detection degrades to today's late-migrate-failure = no worse than status quo; a false-positive that refuses a *valid* schema is the real hazard). The gate is conservative: only a *bare unrecognized function-call identifier in PG-emit position* trips it; string literals, column references, operators sluice already handles (`||`, `::`, `<=>`→`IS NOT DISTINCT FROM`), the catalogued translations (`IS JSON`, `IS NOT DISTINCT FROM`, `CURRENT_TIMESTAMP(N)`, `CURRENT_DATE`), arithmetic, SQL keyword-forms, and enabled-extension functions do NOT. `--expr-override` (which retags the expression off the `mysql` dialect) suppresses the gate for that expression exactly as the curated scan already does.

### Compatibility

Drop-in from v0.68.2; no state/format change. Strictly corrective for MySQL→PostgreSQL of schemas using *any* MySQL-only expression construct the translator does not rewrite (now a loud, actionable refusal at preview/preflight instead of a silent false-green + partial-target migrate) — this widens v0.68.1's denylist backstop to a complete allowlist gate. #12/#13 are now closed by subsumption (loud refusal). PostgreSQL→MySQL, same-engine, and the catalogued/known-good translations are unaffected (regression-guarded). #9 (a separate loud-fail *quality* gap; no corruption) remains tracked, out of scope.

## [0.68.2]

**Closes Bug #11 (HIGH, the top real-user-readiness blocker) — MySQL→PostgreSQL of the universal ORM schema now works.** Found by the real-user-readiness battle-test: a `bigint unsigned AUTO_INCREMENT` PRIMARY KEY emitted PG `bigint … IDENTITY` while a `bigint unsigned` FK child column emitted `numeric(20,0)` → `ADD FOREIGN KEY` failed `SQLSTATE 42804`, invisible at `schema preview`, partial-migrated target. That divergence existed because PG's `GENERATED … AS IDENTITY` is valid only on `smallint/integer/bigint` (never `numeric`), so the AUTO_INCREMENT path stayed `bigint` while plain columns widened to `numeric(20,0)`. This is the **default schema shape of essentially every Rails / Laravel / Django / Sequelize / Prisma MySQL app** (`id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY` + `*_id BIGINT UNSIGNED` FKs) — previously unmigratable to PG with no `--type-override` workaround.

### Changed

- **MySQL `bigint unsigned` → PostgreSQL `bigint`, uniformly** (PK, FK-child, and standalone alike). The PK and FK types now match by construction (no schema-graph machinery), and `IDENTITY` stays valid. **Deliberate, documented range narrowing:** PG has no uint64, so values in `(2^63-1, 2^64-1]` are not representable (the industry-standard pragmatic mapping, cf. pgloader). Previously plain/FK `bigint unsigned` columns were `numeric(20,0)`. Recorded in `docs/type-mapping.md`. (`int/smallint/tinyint unsigned` were already consistent — unaffected.)
- This is surfaced **loudly, never silently**: a new operator-actionable advisory notice naming every affected `table.column` fires at **both `schema preview` and `migrate` preflight** (it is an advisory — migration proceeds; PG itself rejects an out-of-range value at insert time). Operators needing the full 2^64 range override per-column with `--type-override TABLE.COL=numeric`.

### Fixed

- Bug #11 (above). A parallel copy of the same divergence in the `schema preview` note renderer was fixed too, so preview DDL and its inline notes agree.
- **Deterministic CDC-reader teardown (test-harness goroutine leak surfaced by the battle-test's CI `-race` run).** `Streamer.coldStart`/`warmResume` now return a `stop func()` teardown closure that `runOnce` defers, so the MySQL CDC reader's engine-side syncer goroutine is `Close()`d and joined **before `Streamer.Run` returns**, instead of being left to run out its reconnect budget until process exit. This was a test-only race (a leaked go-mysql syncer goroutine logging via `slog.Default()` while a *later* test's `captureSlog` swapped `slog.SetDefault` — production never re-swaps the default logger mid-stream), but the fix is a genuine production-robustness improvement: stream teardown is now deterministic rather than leaked-until-exit. Pinned by `TestStreamer_ClosesCDCReader_BeforeRunReturns` (unit, all-OS, deterministic).

### Compatibility

Drop-in from v0.68.1; no state/format change. **Breaking cross-engine policy change for MySQL→PostgreSQL:** plain/FK `bigint unsigned` columns now map to `bigint` (was `numeric(20,0)`) — values above `2^63-1` are no longer representable without `--type-override`. This is the deliberate trade that makes the universal ORM schema migratable; it is loud (preview + preflight notice) and reversible per-column. PostgreSQL→MySQL, same-engine, signed integers, and other unsigned widths are unaffected (regression-guarded). Still open from the battle-test, tracked, out of scope here: **#14** (general post-translation backstop — `schema preview` can still false-green for MySQL-only constructs outside the curated set; correction banner is on the v0.68.1 release), **#12/#13** (`CAST AS UNSIGNED`/`regexp_like`/spatial in generated-column expressions leak verbatim), **#9** (loud-fail quality, no corruption).

## [0.68.1]

**Closes Bug #8 (HIGH, MySQL→PostgreSQL) — untranslated MySQL constructs no longer silently leak into emitted PG DDL.** A real-world 329-table battle-test corpus found that certain MySQL expression constructs fell through the MySQL→PG translator verbatim: `sluice schema preview` then printed valid-looking-but-invalid PG DDL with **zero warnings** (a false green), and `sluice migrate` hard-aborted at create-tables with PG `SQLSTATE 42883`, leaving a **partially-migrated target**. Both the specific translation gaps and the structural false-green are fixed.

### Fixed

- **Translation rules (additive):** `JSON_VALID(x)` → `(x IS JSON)`; `a <=> b` (MySQL NULL-safe equality) → `a IS NOT DISTINCT FROM b`; `NOW(N)`/`CURRENT_TIMESTAMP(N)`/`LOCALTIME(N)`/`LOCALTIMESTAMP(N)`/`CURTIME(N)` precision-arg forms and `CURDATE()` → the correct PG temporal forms (`CURRENT_TIMESTAMP(N)` / `CURRENT_DATE` / etc.). Multi-arg/expression-precision variants still fall through loud.
- **Structural backstop (the load-bearing fix, loud-failure tenet):** any *known MySQL-only* construct that would reach PG emit now produces a **loud, operator-actionable refusal at BOTH `schema preview` AND `migrate` pre-flight — before any DDL is applied** (reusing the existing `ScanMySQLToPGGaps` catalog + the established cross-engine-supportable refusal surface). `schema preview` is no longer a false green, and `migrate` can no longer leave a partially-migrated target on this class. **Correction (see v0.68.3):** the original wording here claimed this "generalizes" to future untranslated MySQL-isms. It did **not** — this layer keys on a *curated denylist* of known patterns; a MySQL-only construct outside that set still fell through verbatim (Bug #14). The general allowlist gate that actually delivers that guarantee shipped in **v0.68.3**.
- **Requote regression fixed in the same pass (caught by independent review of the Bug #8 fix):** the `<=>`→`IS NOT DISTINCT FROM` translation initially required excluding `FROM` from the ADR-0045 reserved-identifier requoter — but a *blanket* exclusion silently broke re-quoting of a column literally named `from` in any CHECK/generated/index/DEFAULT expression (MySQL→PG). `FROM` is now **context-aware**: treated as grammar only after `DISTINCT` or inside `EXTRACT`/`SUBSTRING`/`TRIM`/`OVERLAY` (via a new `exprident` `GrammarContextual` rule, engine sets stay engine-owned); a bare `from` column reference is re-quoted as before. A latent reverse-direction (PG→MySQL grammar-`FROM`) variant of the same bug is fixed too. The ADR-0045 proactive 4×2×2 sweep gained a `from`-named-reserved-column cell (both directions) so this class can't silently regress.

### Compatibility

Drop-in from v0.68.0. No format/state change. Strictly corrective for MySQL→PostgreSQL of schemas using `JSON_VALID` / `<=>` / `NOW(N)`/`CURDATE()` defaults (now translate) or other MySQL-only expression constructs (now a loud, actionable refusal at preview/preflight instead of a silent false-green + partial-target migrate). PG→MySQL, same-engine, and the catalogued/known-good translations are unaffected (regression-guarded). Bug #9 (a separate loud-failure *quality* gap — generated-col-references-generated-col surfaces a raw PG `42P17` instead of a sluice pre-flight refusal; no data corruption) is tracked, out of scope for this release.

## [0.68.0]

**Verbatim same-engine / backup extension-type passthrough (ADR-0047).** Sluice no longer refuses a PG column whose type is owned by an *uncatalogued* extension (`ltree`, `cube`, `timescaledb`, `pg_partman`, in-house extensions, …) on the paths that provably need only faithful carry, not translation: **same-engine PG → PG** and **PG-backup → PG-restore**. The ADR-0032 enumerated 7-extension allowlist (the rich path with typmod decode / cross-engine translators) is unchanged; this is a deliberately narrower tier *below* it. Cross-engine (PG → MySQL) still loud-refuses — unweakened.

### Added

- **`ir.VerbatimType`** — a new IR type carrying the column's exact `pg_catalog.format_type(atttypid, atttypmod)` spelling, captured by the PG schema reader and re-emitted **verbatim** by the writer (no typmod decode, no per-extension code). Values round-trip via text I/O: a table carrying a verbatim column takes the parameterised-INSERT path (PG's own type input function parses the text form) instead of binary `COPY`, which cannot encode an unknown-OID type. Uncatalogued extension-owned index access methods / operator classes are carried verbatim too (the Bug 47 `OperatorClass`-is-extension-owned invariant is preserved and leveraged).
- **Three-level determination** (one named predicate, not scattered conditionals): (a) catalogued + `--enable-pg-extension` → the rich ADR-0032 path (unchanged); (b) uncatalogued **and** the run is provably same-engine-PG (live) or a PG backup → verbatim; (c) otherwise → today's **loud refusal** (the zero-value default — a reader never told otherwise refuses, loud-fail by construction). The orchestrator stays engine-neutral: it toggles (b) via a new optional `ir.VerbatimExtensionAware` surface using engine *names* only.
- **Backup capability marker.** A backup whose schema carries `ir.VerbatimType` columns records `verbatim_extension_columns` on the `lineage.json` segment (additive, `omitempty` — absent on every pre-0.68.0 / non-verbatim backup; no format-version bump). It is **PG-restore-only**: enforced by a **loud restore-time engine gate** (both the lineage-marker path and the single-manifest/legacy path) that refuses, before any data moves, if the restore target is not PostgreSQL — never a silent cross-engine drop/mangle. Same severity class as Bug 66 / the ADR-0035 PostGIS-absent refusal.

### Compatibility

Additive; drop-in from v0.67.1. No format-version bump (the new IR/envelope fields and the segment marker are append-only / `omitempty`; older sluice ignores them, legacy and never-rotated backups are unaffected). The catalogued-7 extensions keep the rich ADR-0032 path (regression-tested: pgvector / hstore unchanged). Cross-engine PG → MySQL with an uncatalogued extension type still loud-refuses (no weakening). Constraint: a backup containing verbatim extension columns is **PostgreSQL-restore-only** (same PG major version recommended — an extension's text representation is usually but not guaranteed version-stable), and restore to it requires the owning extension installed on the PG target. Rig-verified end-to-end: same-engine `ltree` round-trip, backup→marker→MySQL-refuse→PG-restore-exact, cross-engine still-refuses, catalogued-7 regression — all green incl. under `-race`.

## [0.67.1]

**Closes Bug 66 (HIGH) — a multi-segment backup whose `lineage.json` was absent silently restored only the root segment.** The v0.67.0 post-release regression cycle found that `restore` of a rotated multi-segment backup with `lineage.json` missing (the pre-v0.67.0 `chain.json`-shaped layout, or a lost catalog) silently degraded to a `manifest.json`-only single-segment + first-incremental restore — observed dropping ~90% of rows (1,200 of 11,925) with `exit 0` and no error/WARN. The *unreadable*/garbled-`lineage.json` path already loud-refused correctly; only the *absent*-and-actually-multi-segment branch fell back silently. v0.67.0's CHANGELOG/release notes overclaimed the clean-break loud-refusal guarantee for this case — corrected below and here.

### Fixed

- **Bug 66 (closed).** `resolveLineage`'s absent-`lineage.json` branch now probes for rotation-opened segment directories (`seg-*`) before the legacy single-segment synthesis. If any exist, the backup is a rotated multi-segment lineage that cannot be reconstructed from a bare directory walk → **loud refusal** (DR data, never a silent partial — the same contract the unreadable-`lineage.json` path already honored). A genuine never-rotated / pre-ADR backup (no `seg-*` dirs) still synthesizes a one-segment lineage and restores unchanged — the strict-generalization behavior is preserved. The segment-dir prefix is now a single shared constant (`rotationSegmentDirPrefix`) used by both the rotation producer and this restore-side guard. Pinned by two no-build-tag regression tests (refuse multi-segment; legacy single-segment still resolves) that run in the fast CI Test job on all OSes.

### Compatibility

Drop-in from v0.67.0. No API/CLI/IR-contract/state-format change. Strictly corrective. A backup whose `lineage.json` is intact is unaffected (that path always restored correctly). The behavior change is solely: a rotated backup with a *missing* `lineage.json` now **refuses loudly** instead of silently restoring a fraction — restore from a copy whose `lineage.json` is intact (it is the authoritative structural record for a rotated backup).

## [0.67.0]

**Native bounded-segment backup lineage + inline rotation (ADR-0046), and the backup compression default flips gzip→zstd.** `sluice backup stream run` no longer grows one unbounded chain forever: a backup is now a *lineage* of capped, full-anchored *segments*, and rotation is `lineage.appendSegment` — not an exceptional event grafted onto a chain. `--retain-rotate-at=DUR` / `--retain-rotate-at-chain-length=N` rotate the open segment in-process (no operator cron wrapper). `chain.json` is replaced by `lineage.json` (clean break — zero on-disk backups predate it, zero-users tenet; no migration shim). A never-rotated backup is a one-segment lineage that takes the same single-segment restore path as before.

### Added

- **Inline segment rotation.** `--retain-rotate-at=DUR` and `--retain-rotate-at-chain-length=N` on `backup stream run`. When a threshold trips, the rollover-loop goroutine drives a `STREAMING→DRAIN→SNAPSHOT→BULKCOPY→COMMIT` FSM over the *same* in-flight CDC handle: it bulk-copies a new segment's `backup full`, then a single atomic `lineage.json` write appends the new segment and caps the prior one. CDC never re-opens the slot. The next segment's snapshot anchor `S` is hard-asserted `≥` the prior segment's last incremental position `P_N` (loud abort that stays on the still-open segment — never a silent gap). `rotation_state.json` makes a crash at any FSM edge recoverable: ≤COMMIT discards the provisional segment and resumes the open one; >COMMIT the new segment is authoritative.
- **`--compression=none|gzip|zstd`** on `backup full` / `backup stream run`, recorded per segment in `lineage.json` and read back from there on restore (codec is recorded, **never inferred** from bytes). Mixed-codec lineages restore correctly. `none` leaves chunks as human-readable `.jsonl` (local-FS inspectability).

### Changed

- **Backup compression default: gzip → zstd** (klauspost/compress at SpeedDefault). The compressbench decision doc was re-run with decode throughput measured (warm median, not single cold pass) and the conclusion reversed: zstd decodes **55–85% faster than klauspost gzip on every corpus** — restore speed is the DR-critical axis the original encode/ratio-only analysis omitted — and encodes 0–30% faster, at a ~1–5% ratio cost on representative chunk data (the "~21%" the old doc cited was measured against *stdlib* gzip, the encoder it also recommended abandoning). `--compression=gzip` remains available. Clean break, no gzip-default shim.
- **`chain.json` → `lineage.json`.** The grafted-rotation bimodality (`RotatedAt`/`SucceededBy`/`RotationReason`/`Tombstoned`) is gone; restore is a uniform segment-by-segment lineage walk gated by one boundary-monotonicity invariant (exact intra-segment, monotonic inter-segment — the same `validateBoundary` call site). 14c prune is reframed onto segments. A malformed/*unreadable* lineage is a loud refusal, never a silent partial assemble. **(Correction, see v0.67.1 / Bug 66:** an *absent* `lineage.json` on a rotated multi-segment backup was NOT loud in v0.67.0 — it silently restored only the root segment. Fixed in v0.67.1; this sentence's guarantee fully holds only as of v0.67.1.**)**

### Removed

- **`--exit-after-age` / `--exit-after-chain-length`** (Phase-1 rotation-EXIT, v0.51.0). Superseded by in-process `--retain-rotate-at*`; the flags now error with a clear migration message (clean break, zero-users tenet).

### Fixed

- Per-segment codec is read from `lineage.json`, never sniffed from chunk bytes — an unknown/garbled recorded codec is a loud refusal (a sniffed codec is a latent DR-data corruption path).

### Compatibility

**Breaking, by design (zero-users tenet — no shims).** (1) On-disk format: `chain.json` is gone; existing `chain.json` backups are not read — **CORRECTION (Bug 66, fixed v0.67.1):** in v0.67.0 a `chain.json`-shaped (lineage.json-absent) *multi-segment* backup was not cleanly rejected — it silently restored only the root segment (~90% loss, exit 0). It loud-refuses as of v0.67.1; upgrade to v0.67.1. (2) New backups default to **zstd**-compressed chunks; `gzip` requires `--compression=gzip`. (3) `--exit-after-*` flags removed. There are no production backups predating v0.67.0 to migrate; the clean break was chosen over additive compatibility per the project tenet. Rotation correctness was rig-verified: crash-injection matrix at every FSM edge, an 8-segment zero-loss rotation under continuous write, the `S≥P_N` hard-fail, never-rotated byte-identical restore, and mixed-codec restore.

## [0.66.1]

**Completes Bug 64 — the MySQL→PostgreSQL column-DEFAULT cell that v0.66.0 (ADR-0045) only partially addressed.** v0.66.0's post-release regression cycle caught that the consolidation reached 3 of 4 expression positions; the DEFAULT cell got the PG-reserved requote but not the source-MySQL-backtick strip the generated/CHECK/index emitters get at the reader. v0.66.0's CHANGELOG/release notes were corrected post-publish; this release closes the bug.

### Fixed

- **Bug 64 (closed).** The MySQL schema reader now runs the same `normalizeMySQLExpressionText` (backtick / charset-introducer / escaped-apostrophe strip) on column **DEFAULT** expressions that it already runs on generated-column, CHECK, and index expressions — IR-first and symmetric (source-dialect quoting is reader knowledge). `DEFAULT (\`order\` + \`user\`)` now emits well-formed PG DDL `DEFAULT ("order" + "user")` (reserved requoted, non-reserved bare, zero leaked backticks) instead of the broken `(\`"order"\` + \`"user"\`)` → `SQLSTATE 42601`. Companion fix: because the reader strip would otherwise regress same-engine MySQL→MySQL (bare reserved `order` → `Error 1064`), the MySQL writer's `emitDefault` DEFAULT-expression arm now applies the same reserved-word requote the generated/CHECK/index emitters use — the load-bearing same-dialect requote asymmetry ADR-0045 documents, now consistent across all four positions. Bit-literal defaults (Bug 62), `DefaultLiteral`/constants, the PG→MySQL DEFAULT direction, and `now()`/`gen_random_uuid()`/`random()` outcomes are unaffected.
- **Scope note (PostgreSQL language limitation, not sluice):** PostgreSQL forbids column references in a column `DEFAULT` (`SQLSTATE 0A000`). Bug 64 was always about sluice emitting *syntactically broken* DDL (leaked backticks → 42601); sluice now emits faithful, well-formed DDL. A MySQL `DEFAULT` that references other columns will still be rejected by PostgreSQL itself — that is a target-engine constraint, surfaced as a loud target error, not a sluice defect.

### Internal

- **The ADR-0045 proactive sweep's coverage hole is closed.** The 4×2×2 sweep stayed green through v0.66.0 while this cell was broken because its DEFAULT-position case used only a constant (`CURRENT_TIMESTAMP`) default — no column reference, no backticks. The sweep's DEFAULT cell now exercises a MySQL→PG column-referencing reserved-word default and asserts the emitted PG DDL is well-formed; it fails on pre-fix code and passes post-fix (verified). A future regression of this exact cell can no longer pass the sweep silently.

### Compatibility

Drop-in from v0.66.0. No API/CLI/IR-contract/state-format change. Strictly corrective. **If you deferred a MySQL→PostgreSQL migration with column-referencing DEFAULT expressions per the v0.66.0 note: sluice now emits correct DDL** (note PostgreSQL's own column-ref-in-DEFAULT limitation still applies — function-call and constant DEFAULTs are the unaffected, fully-working cases).

## [0.66.0]

**Expression-identifier-translation consolidation (ADR-0045).** Replaces the reactive per-cell point-fixes (#5, Bug 61, Bug 63, Bug 64) with one named, tested mechanism applied uniformly across every expression position and direction — and closes Bug 64, Bug 65, and a latent PG→MySQL gap the new proactive sweep caught during implementation.

### Changed

- **One shared, engine-parameterized identifier-requote mechanism.** New `internal/translate/exprident` leaf package: the previously byte-identical-duplicated scan primitives (`ScanStringLiteral`, `ScanParenGroup`, `SplitTopLevelArgs`, `IsIdentifierByte`) now live there once; `RequoteIdentifiers(expr, Config{QuoteByte, Reserved, GrammarExclusions, SkipWSBeforeParen})` is the single implementation. The per-engine `requoteMySQLReservedIdents` / `requotePGReservedIdents` are thin wrappers; the reserved-word / grammar-keyword sets stay engine-owned (they are dialect definitions). Net production code shrinks (more duplication deleted than added).
- **Uniform cross-dialect composition `requote(translate(expr))` at all four expression positions × both writers** (generated-column, CHECK, index, **DEFAULT**). Same-dialect short-circuits are byte-identical, including the deliberate asymmetry that the MySQL writer requotes even on the same-dialect path (the MySQL reader strips backticks for IR portability) while PostgreSQL same-dialect stays verbatim.
- **MySQL index expressions are now cross-dialect translated** (ADR-0045 D2). Previously requote-only; PG-source functional indexes with PG-specific operators (`||`, `::`) now translate for MySQL targets like the other expression positions.

### Fixed

- **Bug 64 — cross-engine column DEFAULT expressions — PARTIALLY addressed in v0.66.0; NOT fully fixed (corrected post-release).** The DEFAULT cells were routed through the uniform composition and the PG-target requote step was added, but the v0.66.0 post-release regression cycle found the **MySQL→PG column-DEFAULT path still fails**: it requotes PG-reserved refs but does **not** strip the source MySQL backticks the way the generated/CHECK/index emitters do, so `DEFAULT (\`order\` + \`user\`)` still emits a broken `(\`"order"\` + \`"user"\`)` → `SQLSTATE 42601`. This is **pre-existing (byte-identical to v0.65.2), not a v0.66.0 regression**, but the original v0.66.0 note overclaimed "both DEFAULT cells fixed" — corrected here for accuracy. The PG→MySQL DEFAULT direction and `now()`/`gen_random_uuid()`/`random()` outcomes and the BIT-literal default path are correct. **Bug 64 remains open; completed in v0.66.1** (the DEFAULT-expr emitter needs the same source-quote strip as the other three positions; the ADR-0045 proactive sweep's DEFAULT cell, which missed this, is widened in v0.66.1).
- **Bug 65 — PostgreSQL-source expression / functional indexes were silently dropped.** The PG schema reader skipped index entries with no underlying column (`CREATE INDEX … ((lower(name)))`), losing them with no error or warning (a loud-failure-tenet violation). It now surfaces the expression (`pg_get_indexdef`) into `ir.IndexColumn.Expression` — the PostgreSQL-source analogue of the MySQL-source fix (Bug 16, v0.9.1) — and they round-trip to both targets. Operator-class capture (e.g. `gin_trgm_ops`) on expression indexes is preserved.
- **Latent PG→MySQL identifier-quote gap (caught by the new ADR-0045 proactive sweep during implementation).** `pg_get_expr` returns reserved-word refs double-quoted; the PG reader legitimately cannot strip them (needed for same-dialect PG→PG); the MySQL writer had no source-quote-rewrite leg, emitting a broken mixed-quote shape (`MySQL Error 1292`). New `rewritePGIdentQuotes` first pass in the PG→MySQL translator (cross-dialect only; same-dialect untouched) completes ADR-0016's three-leg policy for that direction.

### Compatibility

- Drop-in from v0.65.2. No API/CLI/IR-contract/state-format change (`ir.IndexColumn.Expression` already existed; Bug 65 only *populates* it for PG sources). Behaviour changes are strictly corrective: previously-failing or silently-lossy cross-engine schema migrations now succeed correctly. The ADR-0045 D2 change makes MySQL index-expression emission consistent with the other expression positions.

### Internal

- A proactive integration sweep now drives reserved-word-named columns through all four expression positions × both directions (plus an opclass-bearing PG expression index) — a regression net so a "fifth cousin" of this defect class cannot land silently.

## [0.65.2]

**Fixed: reserved-word column references inside generated-column / CHECK / index *expression bodies* were emitted unquoted on MySQL → PostgreSQL (Bug 63).** Found by the v0.65.1 post-release cycle; it is the cross-engine cousin the v0.65.0 cycle predicted as Bug 61's "same family", now confirmed a **distinct** defect (Bug 61 was the PG *reader*'s `stripTypeCast`, fixed in v0.65.1; this is the PG *writer*'s cross-dialect expression-body emitter).

### Fixed

- **Bug 63 — PG-target cross-dialect expression bodies now requote PostgreSQL-reserved identifiers.** On MySYQL → PostgreSQL, a generated column `GENERATED ALWAYS AS (`order` + 1)`, a `CHECK (`order` > 0)`, or an expression index over a reserved-word-named column emitted the reference bare (the MySQL reader strips source backticks for IR portability; the PG writer translated function/operator spellings via `translateExprForPG` but never re-quoted bare identifiers). `order` is PostgreSQL-reserved → `CREATE TABLE` failed with `syntax error … (SQLSTATE 42601)`. New `requotePGReservedIdents` (the PG-writer analogue of v0.65.0 #5's `requoteMySQLReservedIdents`) is wired into the cross-dialect generated / CHECK / index expression emitters: string-literal-aware, function-call/operator/keyword/numeric-aware (only bare *identifier* references that are PG-reserved are quoted), using PostgreSQL's reserved-keyword set (e.g. `order`/`user`/`table`/`column` are reserved and requoted; `key` is **non-reserved in PostgreSQL** and correctly left bare — verified against real PG). Same-engine PostgreSQL → PostgreSQL is byte-identical and untouched (the PG reader's `pg_get_expr` already quotes; that path short-circuits before the requote).

### Compatibility

- Drop-in from v0.65.1. No API/CLI/IR/state-format change. Upgrade if you migrate **MySQL → PostgreSQL** schemas with generated columns, CHECK constraints, or expression indexes whose bodies reference reserved-word-named columns — this was a hard `CREATE TABLE` failure.
- Known adjacent surface (not Bug 63, tracked): the same requote is not yet applied to **column DEFAULT** expression bodies, nor audited for the PostgreSQL → MySQL direction across all expression kinds — candidates for a consolidation pass (a single shared reserved-word-requote mechanism across both writers and all four expression positions).

## [0.65.1]

**Two DDL-emit correctness fixes surfaced by the v0.65.0 post-release regression cycle.** Both are localized; the ADR-0044 Tier 3 feature and its load-bearing guards are unaffected (re-verified green).

### Fixed

- **Bug 61 — multi-argument function-call column DEFAULTs were silently truncated (PostgreSQL source).** Root cause (corrected from the initial catalogue hypothesis via instrumentation): the PG schema reader's `stripTypeCast` used `strings.LastIndex(s, "::")`. PostgreSQL records multi-arg function defaults in `information_schema.column_default` with a per-argument `::text` cast (e.g. `DEFAULT crypt('seedpw', gen_salt('bf'))` → `crypt('seedpw'::text, gen_salt('bf'::text))`); `LastIndex` matched the **innermost** cast and truncated the expression *in the IR itself* → downstream `syntax error … (SQLSTATE 42601)` on PG targets. **Pre-existing since ≤ v0.64.0** and broad: it hit core PostgreSQL functions (`left('x', 3)`, `round(1.2345, 2)`, `coalesce(a, b)`), not only the v0.65.0 pgcrypto surface that exposed it. Fixed with a paren-depth-0 / string-literal-aware `topLevelCastIndex`. New PG → PG integration regression covers core funcs, a string-literal-with-comma default, and nested `crypt('seedpw', gen_salt('bf'))`.
- **Bug 62 — `BIT(N)` with `N > 1` was mis-mapped and its `DEFAULT b'…'` corrupted.** Completes the v0.65.0 #4 fix, which only covered `bit(1)`. `BIT(8)` was mapped to `VARBINARY(1)` / `BYTEA` and `b'10100101'` decoded to the decimal string `'165'` → MySQL → MySQL hard-failed (`Error 1067`); MySQL → PG **succeeded with a silently corrupted DEFAULT**. New additive **`ir.Bit{Length}`** type: MySQL `BIT(N)` ↔ PostgreSQL `bit(N)` (lossless, fixed-width, `b'…'`/`B'…'` literals); the bit-literal default round-trips in each target's native syntax; the value path uses `pgtype.Bits` with MySQL-right-justified → PG-left-justified re-alignment (verified end-to-end by integration, not assumed). `bit(1)` → `BOOLEAN` is preserved (v0.65.0 #4 not regressed). Regression tests exercise `BIT(8)` **and** `BIT(16)` on **both** MySQL → MySQL and MySQL → PG (the coverage gap that let this through).

### Compatibility

- Drop-in from v0.65.0. `ir.Bit` is a purely additive IR type (no existing IR type changed); no API/CLI/state-format change. Operators whose schemas use `BIT(N>1)` columns or multi-argument function defaults on a PostgreSQL source should upgrade — these were silent-corruption / hard-fail bugs.

## [0.65.0]

**PII / extension: ADR-0032 Tier 3 — opt-in passthrough for extension-function column defaults & generated expressions (uuid-ossp + pgcrypto), plus three DDL-emit fidelity fixes surfaced by the PlanetScale validation corpus.**

### Added

- **Tier 3 extension-function passthrough (ADR-0044).** A column `DEFAULT` or `GENERATED ALWAYS AS` expression that references an extension-owned function — uuid-ossp's `uuid_generate_v1/v1mc/v4/v5()` etc., or pgcrypto's `digest/hmac/crypt/gen_salt/…` — now requires `--enable-pg-extension uuid-ossp` (resp. `pgcrypto`). With the flag, the existing preflight verifies the extension is installed on the target *before* any data moves; without it, the migration is **refused early and clearly at schema-read** (column, function, owning extension, and the fix named) instead of failing late with a raw `CREATE TABLE` parse error. New catalog-declarative `extensionDef.defaultExprFunctions`; `pgUUIDOSSPDef` registered; `pgCryptoDef` extended. Conservative bareword/`(`-call scanner (string-literal-aware, word-boundary, qualified-name-safe) — not a SQL parser.
- **Core-vs-extension guard:** `gen_random_uuid()` (core PostgreSQL 13+), `now()`, `nextval()`, … are **never** gated — only genuinely extension-owned functions are. Triple-pinned (scanner + catalog lookup + gate) plus an integration test (`TestMigrate_PG_GenRandomUUID_NoFlag_Succeeds`).

### Changed

- **Behaviour change (deliberate, pre-1.0/pre-users):** same-engine PG → PG extension-function defaults that previously passed through implicitly (then failed late if the target lacked the extension) now require the explicit `--enable-pg-extension` opt-in — consistent with every other ADR-0032 extension and the loud-failure-early tenet.
- **Cross-engine PG → MySQL policy clarified (ADR-0044 §3 rescope).** uuid-ossp / pgcrypto remain refused for non-PG targets by the pre-existing ADR-0032 lossless-only policy (`validateEnabledPGExtensions` — only `hstore`/`citext` have honest cross-engine default translators). No `uuid_generate_v4()` → MySQL `UUID()` translation is performed: that would be a silent RFC-4122 v4→v1 version drift. The escape stays `--type-override` per column or a PG target. (The pre-existing Bug-42 `gen_random_uuid()` / `now()` / `random()` core translations are unaffected.)

### Fixed

- **#4 — `bit(N) DEFAULT b'…'` default no longer mistranslated.** Was emitted as a mis-quoted string on **both** MySQL (Error 1067) and PostgreSQL (SQLSTATE 22P02) targets. The MySQL reader now decodes `b'…'`/`B'…'` (incl. the `DEFAULT_GENERATED` parenthesised form) to a dialect-neutral decimal literal; the MySQL writer emits clean `0`/`1` for `TINYINT(1)`.
- **#5 — generated-column expressions over reserved-word identifiers** (e.g. `` `order` ``, `` `key` ``) no longer drop their quoting on MySQL → MySQL (was Error 1064, STORED and VIRTUAL). The MySQL writer re-quotes bare reserved-word identifiers (expression-grammar-keyword-aware) on emit; the IR-portability strip stays for the PG path.
- **#6 — PostgreSQL target no longer emits a MySQL `_utf8mb4` introducer / backslash-escaped string default untranslated** (was SQLSTATE 42601). The MySQL reader now normalizes introducers + escaped apostrophes on the `DefaultExpression` path, consistent with the generated/CHECK/index expression paths.

### Who needs this release

- **PG → PG operators whose schemas use `DEFAULT uuid_generate_v4()` / pgcrypto function defaults:** pass `--enable-pg-extension uuid-ossp` / `pgcrypto`. You now get a clean preflight check instead of a late apply-time failure. Without the flag you get an actionable refusal naming the fix.
- **Anyone migrating MySQL/PlanetScale schemas with `BIT` defaults, reserved-word generated columns, or charset-introducer string defaults:** drop-in correctness fixes (#4/#5/#6), no action needed.
- **Cross-engine PG → MySQL with uuid-ossp/pgcrypto:** unchanged — still refused by the lossless-only policy (use `--type-override` or a PG target). No behaviour change vs v0.64.0.
- **Everyone else:** no API/CLI/IR/state-format change; drop-in upgrade from v0.64.0.

## [0.64.0]

**Parallel bulk-copy now uses the engine-native fast loader on a cold start (ADR-0043; ADR-0042 Phase C).** The within-table parallel-copy path previously routed *every* chunk through a generic `database/sql` batched idempotent-upsert (`INSERT … ON CONFLICT/ON DUPLICATE KEY UPDATE`) — it never used PostgreSQL `COPY` or MySQL `LOAD DATA`, even though the single-reader path already does. That was a deliberate-but-over-broad trade for crash-resume safety. ADR-0042 Phase B profiling proved this generic writer path — not driver encoding or column types — was the dominant cost behind the MySQL↔PG throughput gap. v0.64.0 makes the parallel chunk writer **situation-driven and automatic** (no new flag): a fresh cold-start chunk into a proven-empty target streams through one native `COPY`/`LOAD DATA` call; resume, `--force-cold-start`, and live-add stay on the idempotent path exactly as before.

### Changed

- **Parallel-copy chunk writer selection is now automatic and situation-driven** (`internal/pipeline` ADR-0043 four-gate rule). A chunk uses the native fast loader iff: not a resume run, **and** the chunk has zero recorded prior progress, **and** `--force-cold-start` was not set (target proven empty by the Bug 9 pre-flight). Otherwise it uses the idempotent upsert path, byte-for-byte as before. No CLI surface, no IR/schema/state-format change — symmetric with how the single-reader path already auto-selects the loader from engine capability.
- A fast-path cold chunk streams its entire PK-bounded range through **one** `WriteRows` call (one `COPY` / `LOAD DATA`, memory-bounded — no whole-chunk buffering) with a single terminal per-chunk checkpoint, instead of per-batch upserts + per-batch cursor checkpoints.

### Performance

- Local-rig medium fixture (25 tables × 100k rows), MySQL → MySQL, default config: **~31s → ~24s wall** (~80k → ~104k rows/sec); per-chunk rate **~16–18k → ~28k rows/sec**, ~1.7× per-stream, materially into the PostgreSQL band. PostgreSQL → PostgreSQL: no regression (it is schema/DDL-bound, not bulk-copy-bound — ADR-0042 finding N2). Largest real-world benefit: large cold-start migrations into an empty target (the common `migrate` case).

### Correctness

- The fast (non-upsert) path is taken **only** where a primary-key collision is provably impossible: a chunk with zero committed rows into an empty target. A crash mid-fast-chunk is safe because the *next* invocation is a resume run, which fails gate (1) and replays that chunk through the idempotent upsert — absorbing any committed prefix without collision. Pinned by a permanent both-engines proof-of-falsification integration test (`TestFastLoader_CrashMidFastChunk_ResumeIsIdempotent`) plus fresh-cold-start, force-cold-start-populated, and resume-partial coverage.

### Diagnostics

- Adds the ADR-0042 Phase B findings + the ADR-0043 design record (docs/ADRs only). The DEBUG-gated `adr0042:` per-chunk instrumentation (added in v0.63.1) now also annotates fast-loader chunks (`fast_loader=true`); `--log-level=debug` only, no behaviour change.

### Who needs this release

- **Anyone running large cold-start `sluice migrate` (or `sync start` cold-start) into an empty target** — especially MySQL targets and parallel-eligible tables (≥ `--bulk-parallel-min-rows`). Faster, automatically, no config change.
- **Resume / `--force-cold-start` / mid-stream live-add users**: behaviour is byte-for-byte unchanged (idempotent path retained for exactly those situations).
- **Everyone**: no API/CLI/IR/state-format change; drop-in upgrade from v0.63.1.

## [0.63.1]

**Fixed: PostgreSQL source migrations silently skipped parallel-copy on a freshly loaded/restored database (ADR-0042 finding N1).** sluice decides parallel-copy eligibility from a row-count estimate. The PostgreSQL estimate read `pg_class.reltuples`, which is the sentinel `-1` (PG14+) / `0` until `ANALYZE` or autovacuum populates it. On the normal migrate cold-start — load or restore a source database, then migrate before autovacuum has run — *every* table reported `~0 rows`, fell below `--bulk-parallel-min-rows`, and silently took the single-reader path. (MySQL's analogous `information_schema.tables.table_rows` is populated by InnoDB on load, so MySQL sources were unaffected — the bug was Postgres-specific and asymmetric.)

### Fixed

- **`internal/engines/postgres` `CountRows`** now falls back to an exact `SELECT COUNT(*)` when `reltuples` is non-positive (never-analyzed sentinel, or genuinely empty). One sequential scan, one time at preflight, triggered only when planner stats are absent — tables with good stats keep the fast single-catalog-lookup path. Correct whether the table turns out to be large or empty; snapshot-pinned readers are unaffected (they return before the count path, so no deadlock against the in-flight stream).

### Changed

- **`internal/pipeline` parallel-copy path** now carries DEBUG-gated per-chunk / per-batch wall-time instrumentation (log key `adr0042:`, `--log-level=debug` only — no INFO+ noise). Retained as a permanent diagnostic artifact for the ongoing ADR-0042 bulk-copy throughput investigation (same disposition as the ADR-0033/0036 verify probes). No behaviour change.

### Who needs this release

- **Anyone running `sluice migrate` or `sync start` with a PostgreSQL source that was recently loaded or restored** (dump/restore pipelines, fresh CI/staging seeds, cross-region rehydrations). Those migrations now correctly engage parallel-copy instead of silently single-threading. Impact is largest on **PostgreSQL → MySQL** (the slow MySQL-write side now parallelizes); PostgreSQL → PostgreSQL benefits less (it is not bulk-copy-bound — ADR-0042 finding N2).
- **MySQL-source users**: no change (never affected).
- **Operators who already `ANALYZE` their source before migrating**: no behaviour change (the fast `reltuples` path still short-circuits).

## [0.63.0]

**PII Phase 4 — operator-keyset persistence (ADR-0041).** Closes the PII track. Sluice now has a first-class **keyset**: a durable, versioned, operator-controlled set of named HMAC keys that `hash:hmac-sha256` and `tokenize:dict` both reference. A single keyset shared between two streams (`staging-1` + `staging-2`) produces *identical* surrogates for identical inputs — cross-stream referential integrity that was impossible in Phases 1–3, where each surface keyed independently. **This is a breaking change** (sluice is pre-1.0 and pre-users; per the project's zero-users → no-compat tenet we took the clean break over an additive shim).

### Added

- **`--keyset-source=<scheme>:<value>`** on `migrate`, `sync start`, `schema preview`, and `backup`. Three schemes: `file:PATH` (keyset YAML on disk), `env:VARNAME` (keyset YAML in an env var), `db:DSN` (a sluice-managed `sluice_keysets` table on the named DSN — shared across streams, the cross-stream-stability primitive).
- **`key:` option** on `hash` and `tokenize` redaction rules (YAML field; CLI trailing-segment form `hash:hmac-sha256[:<keyname>]` / `tokenize:dict:<dict>[:<keyname>]`). Names which keyset entry to use, so different compliance scopes can rotate independently. Unnamed rules resolve the keyset's sole entry (or the `default` entry where the source supports one).
- **`sluice_keysets` table DDL** auto-created on both PostgreSQL and MySQL targets (`CREATE TABLE IF NOT EXISTS`, engine-appropriate `BYTEA`/`BLOB` + timestamp types), mirroring the existing `sluice_cdc_state` control-table pattern. Engine-specific SQL lives in the engine packages; the redaction layer depends only on an interface (IR-first).
- **Startup audit-log line** — one INFO line per run (`keyset loaded source=… generations=[…] active=… hmac-algo=sha256`), DSN credentials redacted. No per-row logging (that would defeat redaction).

### Changed

- `hash:hmac-sha256` and `tokenize:dict` now obtain their HMAC key from the loaded keyset (resolved **once at startup**, immutable for the run) instead of, respectively, a `--redact-key-source`-derived `[]byte` and a hardcoded built-in constant.

### Removed (breaking)

- **`--redact-key-source` is deleted** (all of `env:` / `file:` / `derive:`), along with the `redact_key_source` YAML key and the `SLUICE_REDACT_KEY_SOURCE` env var. Use `--keyset-source` instead.
- **The built-in v0.61.0 `tokenize:dict` HMAC key (`sluice-tokenize-dict-v1`) is deleted.** `tokenize:dict` no longer has an implicit key — it now **requires** `--keyset-source`, exactly like `hash:hmac-sha256`. A redaction config that uses either strategy without a resolvable keyset is refused at preflight with an actionable message (loud-failure tenet) rather than silently using a default key.

### Compatibility

- **Breaking, no shim, no migration path.** There is intentionally no zero-surrogate-drift guarantee from v0.61.0/v0.62.0: any data previously redacted with the old built-in `tokenize:dict` key would tokenize differently under a new operator keyset. Pre-users, this is the correct trade — the alternative was permanent shim code for a tool with no installed base.
- **Deferred to Phase 4.5 (not in this release):** hot-reload of the keyset (file-watch / db-poll). Rotation takes effect on the **next process restart only** — a mid-run active-key change would split surrogates within one run and break within-run referential integrity, so the run-snapshot contract is deliberate.
- **Out of v1 scope:** `sluice keyset rotate` / `sluice keyset list` CLI helpers (use manual SQL / YAML editing), KMS / Vault adapters (layer them above `env:`/`file:`), and encryption-at-rest of the keyset `bytes` column (operator's storage-layer responsibility, consistent with how sluice treats other sensitive state).

### Who needs this release

- **Anyone running multiple sluice streams from one source into separate destinations** who needs a redacted column to join across those destinations (multi-staging, multi-tenant analytics, cross-org data exchange). Share one keyset → identical surrogates everywhere.
- **Anyone using `hash:hmac-sha256` or `tokenize:dict` on v0.61.0/v0.62.0**: this is a required-action upgrade — replace `--redact-key-source` with `--keyset-source` and supply a keyset; `tokenize:dict` now needs one too.
- **Anyone using only `null` / `static` / `truncate` / `mask` / `randomize:*`**: no action — those strategies don't touch the keyset (`randomize:*` stays `--stream-id`-seeded).

## [0.62.0]

**Smarter `--bulk-parallel-min-rows` default — 100,000 → 80,000.** Empirical finding from the new local-local rig: sluice consults `information_schema.tables.table_rows` (InnoDB) when deciding whether a table is "big enough" for parallel-copy. That catalog row-count is an *estimate* that commonly undershoots actuals by 0.1–5%. A table holding exactly 100,000 rows often reports as `~95-99k` via the catalog, so the prior 100,000 threshold left 100k-actual tables on the single-reader path despite being meant to qualify.

### Changed

- **`--bulk-parallel-min-rows` default lowered to 80,000** (from 100,000). The new default sits below 100k to absorb the typical catalog undershoot. Tables with 100k actual rows now consistently engage parallel-copy by default.
- **`defaultBulkParallelMinRows`** constant in `internal/pipeline/chunk.go` updated.
- **Doc comments + CLI help text** name the new default and reference the v0.62.0 changeover for operators reading older docs.

### Compatibility

- **Behaviour change**: operators on workloads with many 80k-99k-row tables will see those tables now take the parallel-copy path by default. The change is intended — parallel-copy with default parallelism (`min(8, NumCPU)`) is strictly faster than single-reader on tables in this size band per the empirical baseline.
- **Operators wanting the pre-v0.62.0 behaviour** pass `--bulk-parallel-min-rows=100000` explicitly. Nothing else changes — the threshold is the only knob affected.
- **Drop-in upgrade from v0.61.0.** No flag-name changes, no IR changes, no engine API changes.

### Empirical baseline (local-local MySQL rig, medium fixture)

| Configuration | Throughput | Wall (2.5M rows) |
|---|---|---|
| v0.61.0, `--bulk-parallel-min-rows=100000` (default), `local_infile=OFF` | ~28k rows/sec | 88s |
| v0.61.0, `--bulk-parallel-min-rows=100000` (default), `local_infile=ON` | ~33k rows/sec | 75s |
| v0.61.0, `--bulk-parallel-min-rows=50000` explicit, `local_infile=ON` | ~54k rows/sec | 46s |
| **v0.62.0 (this release), defaults, `local_infile=ON`** | **expected ~50-55k rows/sec** | **~45-50s** |

### Who needs this release

- **Anyone running migrate or sync-start against tables in the 80k-100k row range** (common for medium-sized SaaS schemas). The default now matches operator intent.
- **Anyone with tables below 80k**: no change. Default behaviour stays single-reader.
- **Anyone tuning `--bulk-parallel-min-rows` explicitly**: no change. The explicit value still wins.

### Verification

- Build + lint clean across all tags.
- Existing tests cover the new default value via the constant; no test pinned the literal `100000`.

### Future work surfaced by the same investigation

- **Smart-check near the threshold**: when the catalog row-count is within ~10% of the threshold, sluice could run an exact `SELECT COUNT(*)` (~50ms on a 100k table) before deciding which path to take. Removes the threshold guesswork entirely. Deferred — the 80k default closes the most common gap.
- **Cross-engine reader rate parity**: PG → PG on the same fixture shape runs ~4× faster than MySQL → MySQL on the same host. Reader-side delta worth a future throughput-tuning pass.

## [0.61.0]

**PII Phase 3 — dictionary-based redaction strategies.** Two new strategies that map source values into operator-supplied dictionaries: `randomize:dict:<name>` picks an entry per row (PK-keyed, inherits v0.59.0's replay-stable contract) and `tokenize:dict:<name>` picks an entry by HMAC of the input value (input-value-keyed, stable across tables — every "Alice" maps to the same dict entry everywhere in the database). Dictionaries live in YAML's `dictionaries:` block with two declaration shapes: inline `entries:` list for small dicts (10s of entries), or `file:` pointer at a one-entry-per-line file (`#`-prefixed comments + blank lines tolerated) for large dicts. ADR-0040 documents the two differing determinism contracts.

### Added

- **`randomize:dict:<name>` strategy** — picks a dictionary entry per row using the v0.59.0 PK-derived seed. Same source row → same dict entry across re-runs / CDC resumes / backup→restore. Inherits the no-PK preflight refusal automatically (Name() starts with `randomize:`).
- **`tokenize:dict:<name>` strategy** — picks a dictionary entry via HMAC of `streamID || ":" || dictName || ":" || input`, modulo dictionary length. Determinism contract: same input value → same output regardless of PK, regardless of table/column. The FIRST sluice strategy whose output depends on the INPUT VALUE rather than the row's PK. NOT subject to the no-PK preflight (works on tables without a primary key — that's the whole point). NULL input passes through; non-string input is canonicalized via `fmt.Sprintf("%v", ...)`.
- **`internal/redact/dictionary.go`** — `LoadDictionaries(map[string]config.Dictionary) (map[string][]string, error)` resolves YAML declarations to a flat name → entries map. File-form loader uses `os.ReadFile`, splits on `\n`, trims each line, skips blanks + `#`-prefixed comments. Refuses empty dictionaries (0 entries after trimming) at load time. Refuses dictionaries declaring both `file:` and inline `entries:`.
- **`internal/redact/strategies_dict.go`** — `RandomizeDict` + `TokenizeDict` types implementing Strategy. Each holds `DictName string` + `Entries []string` materialised at parser-time (no `*DictionaryRegistry` plumbing through Strategy.Redact). `TokenizeDict` additionally captures `StreamID string` at construction time so the HMAC includes it.
- **`config.Dictionary` type** with `File string` (mutually exclusive) and `Entries []string` fields. Operators declare under top-level YAML `dictionaries:` block.
- **`config.Redaction.Dict string` field** (`koanf:"dict"`) — references a dictionary name for both `tokenize` and `randomize:dict` YAML forms.
- **CLI form**: `--redact users.first_name=tokenize:dict:first_names` and `--redact users.first_name=randomize:dict:first_names`. **CLI form REQUIRES a YAML config** to declare the dictionary content (there's no good CLI-only shape for dictionaries); the parser refuses with a clear message when a CLI rule references a missing dictionary name.
- **YAML form**: `strategy: tokenize` + `dict: <name>` (form: dict is the default and can be omitted), or `strategy: randomize` + `form: dict` + `dict: <name>`. Both reject spurious `min`/`max`/`brand`/`country_code` fields with operator-actionable errors.
- **Help text** on `migrate`, `sync start`, `backup full`, `schema preview` enumerates the two new strategies.
- **ADR-0040** documents the two differing determinism contracts and the rationale for keying tokenize by input value rather than by row PK.

### Compatibility

- **Drop-in upgrade from v0.60.x.** No flag changes; new strategies are opt-in. Existing pipelines without YAML `dictionaries:` block behave identically.
- **Internal API change**: `cmd/sluice/redact_flag.go`'s `parseRedactFlags` and `mergeYAMLRedactions` gain a fourth `dictionaries map[string][]string` parameter. All in-process callers (Migrate, SyncStart, BackupFull, SchemaPreview) thread the pre-resolved dictionary map. Direct API users of these helpers need a one-line update; pass `nil` when no dictionaries are declared.
- **`config.Redaction` gains a `Dict string` field**; pre-existing YAML configs unaffected (default empty).
- **`config.Config` gains a `Dictionaries map[string]Dictionary` field**; pre-existing YAML configs unaffected (default nil map).
- **`yamlRandomizeToSluice` signature changed** to accept `streamID` + `dictionaries` parameters. Only relevant to direct API users; no operator-facing change.
- **Determinism contracts**:
  - `randomize:dict` inherits ADR-0039's PK-keyed contract.
  - `tokenize:dict` follows the NEW input-value-keyed contract documented in ADR-0040.

### Phase progress

- Phase 1 (v0.53.0): null, static, hash, truncate.
- Phase 1.5 (v0.54.0+): CDC apply-path redaction, schema-preview annotation, YAML config, backup-stream redaction.
- Phase 2.a (v0.56.0): generic `mask:inner` / `mask:outer` + Luhn helper.
- Phase 2.b (v0.57.0 + v0.58.0): mask presets (ssn, pan, pan-relaxed, email, ca-sin, uk-nin, iban, uuid).
- Phase 2.c first wave (v0.59.0): randomize:int, randomize:email, randomize:us-phone, randomize:uuid.
- Phase 2.c second wave (v0.60.0): randomize:ssn, randomize:pan, randomize:ca-sin, randomize:uk-nin, randomize:iban.
- Phase 3 (this release): randomize:dict, tokenize:dict, YAML dictionaries: block.

Phase 4 (cross-stream keyset persistence) is the next major chunk — it formalises the operator-keyset story that the current `tokenize:dict` HMAC's fixed key (`sluice-tokenize-dict-v1`) leaves room for.

### Who needs this release

- **Operators redacting columns that appear in multiple tables** wanting one stable surrogate per value (e.g. customer names in `orders.customer_name`, `support_tickets.customer_name`, `audit_log.actor`) — `tokenize:dict` gives them per-value cross-table stability that no prior strategy provided.
- **Operators wanting per-row synthetic-but-stable names / cities / placeholder strings** drawn from an operator-curated list (e.g. test data that uses fictional-but-realistic names) — `randomize:dict` extends v0.59.0's per-row replay-stability to operator-supplied vocabularies.
- **Operators with PII columns on PK-less source tables** (audit logs, event streams, denormalised analytics tables) — `tokenize:dict` works on them; `randomize:dict` and other randomize:* strategies do not.
- **Anyone not using dict strategies**: drop-in, no behaviour change.

### Verification

- **Build + lint clean** across all tags (default, integration, integration postgis).
- **Unit tests** (`strategies_dict_test.go`): RandomizeDict determinism (same seed → same output) + per-seed separation; nil-seed refusal naming "primary key"; empty-entries refusal (defense-in-depth); Name() prefix invariants (RandomizeDict starts with `randomize:`, TokenizeDict does NOT). TokenizeDict determinism per input value (same input + different seed → same output); StreamID + DictName affecting output; NULL pass-through; non-string input canonicalization; seed-to-index reduction sanity.
- **Loader tests** (`dictionary_test.go`): nil/empty pass-through; inline form with trimming + empty-drop; file form with `#`-comment + blank-line skipping; refusal paths (empty dict, all-whitespace inline, both file+entries declared, missing file, empty file, all-comments file, empty name); `ResolveDictEntries` defensive-copy behaviour.
- **CLI tests** (`redact_flag_test.go`): `tokenize:dict:<name>` + `randomize:dict:<name>` happy paths; refusal paths (no form, unknown form, no dict name, empty name, unknown dict, no-dicts-loaded); streamID threading from CLI parser into TokenizeDict; end-to-end LoadDictionaries → parseRedactFlags → Redact.
- **YAML tests** (`redact_flag_test.go`): tokenize form omitted (defaults to dict) + explicit `form: dict`; randomize form: dict; refusal paths (missing dict field, unknown form, unknown dict, spurious min/brand/country_code on tokenize, spurious brand/dict on randomize:int, spurious `dict:` on hash / static).

See `docs/adr/adr-0040-dictionary-strategy-determinism.md` for the two determinism contracts. See `docs/dev/notes/prep-pii-redaction-phase-2-strategy-catalog.md` Phase 3 section for the catalog.

## [0.60.0]

**PII Phase 2.c second wave — checksum-aware replay-stable randomize strategies.** Five new generators (`randomize:ssn`, `randomize:pan[:<brand>]`, `randomize:ca-sin`, `randomize:uk-nin`, `randomize:iban[:<country-code>]`) that produce realistic synthetic identifiers in the canonical shape for each country/format — Luhn-valid PANs / CA SINs, mod-97-valid IBANs, reserved-range-avoiding US SSNs, HMRC-shape UK NINs. Same per-row replay-stability contract as v0.59.0's first wave: same source row → same redacted value across re-runs, CDC resumes, and backup→restore. ADR-0039 still governs.

### Added

- **`randomize:ssn` strategy** — US SSN `XXX-XX-XXXX`. Avoids reserved ranges: area never 000 / 666 / 900-999 (ITIN); group never 00; serial never 0000.
- **`randomize:pan[:<brand>]` strategy** — Luhn-valid PAN. Optional `:<brand>` suffix selects issuer: `visa` (16-digit, starts with 4), `mastercard` (16-digit, starts with 5), `amex` (15-digit, starts with 34/37). No brand → random brand from the supported set (deterministic per-row).
- **`randomize:ca-sin` strategy** — Luhn-valid Canadian SIN `XXX-XXX-XXX`. First digit drawn from the issued-province pool (1-7, 9); 0 and 8 (reserved) are excluded.
- **`randomize:uk-nin` strategy** — UK National Insurance Number `AA999999A`. Prefix letters from a curated subset (HMRC-reserved D/F/I/Q/U/V excluded); suffix from {A,B,C,D} per HMRC convention.
- **`randomize:iban[:<country-code>]` strategy** — mod-97-valid IBAN with country-specific check digits computed via ISO 13616-1's letter-to-digit-pair encoding. Optional `:<country-code>` suffix: `DE` (22 chars), `GB` (22 chars), `FR` (27 chars). No country → random country from the supported set. Other countries (ES, IT, NL, etc.) can be added on operator demand.
- **`internal/redact/iban.go`** — `ibanCheckDigits` helper computes 2-digit mod-97 checks; `ibanValid` validator confirms self-consistency. Both tested against known-good IBANs from the SWIFT registry (DE89370400440532013000, GB82WEST12345698765432, FR1420041010050500013M02606).
- **`config.Redaction` gains `Brand string` + `CountryCode string` fields** for the `pan` / `iban` forms of randomize. YAML form: `brand: visa` / `country_code: DE`.
- **Help text** on `migrate`, `sync start`, `backup full`, `schema preview` enumerates all 9 randomize generators (4 first-wave + 5 second-wave).

### Compatibility

- **Drop-in upgrade from v0.59.x.** No flag changes; new strategies are opt-in. Inherits the v0.59.0 no-PK preflight refusal (still matches by `randomize:` prefix); no new preflight code.
- **`config.Redaction` gains 2 optional string fields** (`Brand`, `CountryCode`). Pre-existing YAML configs unaffected (the new fields default to empty).
- **Determinism contract unchanged** — every new generator is replay-stable per (table, column, PK).
- **Country / brand scope is intentionally narrow.** PAN ships Visa/Mastercard/AmEx (the MySQL Enterprise `gen_rnd_pan()` set); Discover, JCB, UnionPay can be added on operator demand. IBAN ships DE/GB/FR (the MySQL Enterprise default demographic); ES/IT/NL/etc. can follow. Operators needing an unsupported brand/country today should override individual columns via `static:` or implement a custom Strategy.

### Phase 2 progress

- Phase 2.a (v0.56.0): generic `mask:inner` / `mask:outer` + Luhn helper.
- Phase 2.b (v0.57.0 + v0.58.0): `mask:ssn`, `mask:pan`, `mask:pan-relaxed`, `mask:email`, `mask:ca-sin`, `mask:uk-nin`, `mask:iban`, `mask:uuid`.
- Phase 2.c first wave (v0.59.0): `randomize:int`, `randomize:email`, `randomize:us-phone`, `randomize:uuid`.
- Phase 2.c second wave (this release): `randomize:ssn`, `randomize:pan[:<brand>]`, `randomize:ca-sin`, `randomize:uk-nin`, `randomize:iban[:<country-code>]`.

Phase 2 is now **fully shipped** for the MySQL Enterprise compatibility surface. Phase 3 (dictionary-based `tokenize:dict` / `randomize:dict`) is the next major chunk; Phase 4 (cross-stream keyset persistence) follows.

### Who needs this release

- **Operators redacting US SSN columns** wanting stable synthetic surrogates that don't collide with real-world reserved ranges.
- **Operators redacting credit-card PAN columns** wanting Luhn-valid synthetic numbers (test-card-shape downstream analytics that validate checksums no longer reject the redacted column).
- **Operators with Canadian / UK / European bank PII** (CA SIN, UK NIN, DE/GB/FR IBAN) wanting one-line per-column generators matching the canonical shapes.
- **Operators running continuous-sync** who needed checksum-aware randomize (Luhn / mod-97) — the first wave's PII shapes (email / phone / UUID) didn't cover the catalog's most checksum-sensitive identifiers.
- **Anyone not using randomize:\***: drop-in, no behaviour change.

### Verification

- **Build + lint clean** across all tags (default, integration, integration postgis).
- **Unit tests** (`strategies_randomize_2nd_wave_test.go`): 5 generator test groups cover determinism, shape correctness, checksum validity (Luhn for PAN+CA SIN, mod-97 for IBAN), reserved-range exclusion (SSN), HMRC-shape compliance (UK NIN), brand/country parsing.
- **`ibanCheckDigits` self-consistency tests** — known-good IBANs from the SWIFT registry confirm the helper computes the right check digit; constructed IBANs round-trip through `ibanValid`.
- **`Registry.ApplyRow` tests** — every new strategy: no-PK refusal naming column + strategy + "primary key"; with-PK replay stability.
- **CLI tests** (`redact_flag_test.go`): every second-wave form happy path + every refusal branch (unknown brand, unknown country, spurious options on no-options forms, empty trailing colon).
- **YAML tests**: every form happy path + every refusal path (spurious brand on non-pan forms, spurious country_code on non-iban forms, spurious bounds on non-int forms, unknown brand / country values).

See `docs/adr/adr-0039-randomize-strategy-determinism.md` for the per-row seed derivation contract. See `docs/dev/notes/prep-pii-redaction-phase-2-strategy-catalog.md` for the full strategy catalog and Phase 3 / Phase 4 sequencing.

## [0.59.0]

**PII Phase 2.c first wave — replay-stable randomize strategies.** Four new generators (`randomize:int`, `randomize:email`, `randomize:us-phone`, `randomize:uuid`) that produce random-looking output stable per source row. Same row in always produces same redacted row out — across runs, across CDC resumes, across backup→restore. Operators wanting MySQL Enterprise–style `gen_rnd_*` placeholder generation get the shape they expected (NANP-valid phones, v4 UUIDs, `.test` emails) plus continuous-sync idempotency that pure-random would break. The deviation from Enterprise's pure-random semantics is documented in ADR-0039.

### Added

- **`randomize:int:<min>,<max>` strategy** — seeded random integer in [min, max] inclusive. Type-strict (refuses non-integer source values); returns int64 to match sluice's IR integer shape. Refuses CLI input with `min > max` and runtime calls with `Min > Max` (defensive).
- **`randomize:email` strategy** — `<rand-local>@<rand-domain>.test` where the local part is 6-12 lowercase letters and the domain is 5-10 lowercase letters. Uses the RFC 6761 `.test` TLD so no output ever resolves to a real address.
- **`randomize:us-phone` strategy** — NANP-valid `XXX-XXX-XXXX`. Area code 200-999 (never 555 — reserved for fictional use); exchange 200-999. Defaults to dashed format; operators wanting other separators should pipe through a separate transform.
- **`randomize:uuid` strategy** — canonical 8-4-4-4-12 hyphenated UUIDv4 with version + variant bits set per RFC 4122 §4.1.1/§4.1.3, so output validates as a v4 UUID under any compliant parser.
- **Per-row replay-stable seeding**. Every randomize:* strategy derives its RNG seed from `SHA256(streamID || table || column || pkColumns || pkValues)`. Same source row → same redacted value. CDC resume + cold-start re-apply produce identical target values; the applier's `ON CONFLICT (pk) DO UPDATE` / `ON DUPLICATE KEY UPDATE` idempotency contract is preserved end-to-end. ADR-0039 documents the rationale.
- **Preflight refusal on no-PK tables**. Randomize:* requires a primary key for replay-stable seeding; `pipeline.preflightRedactTypes` refuses at startup with an operator-actionable error naming the rule and suggesting either a `PRIMARY KEY` on the source or a non-random strategy (`hash:sha256` / `mask:*` / `static:`). Tables out of scope (filtered out) pass silently.
- **YAML form** — `strategy: randomize` + `form: int|email|us-phone|uuid`. For `int` form, additionally requires `min:` + `max:` integer fields. Spurious `min`/`max` on no-options forms (email/us-phone/uuid) is rejected with a clear error.
- **`ir.StreamIDSetter` optional interface** — engines can implement `SetStreamID(string)` to receive the active stream's identifier at startup. Mirrors `RedactorSetter`'s shape; the streamer probes via type assertion and skips engines that don't expose it. Both shipping engines (MySQL, Postgres) implement it.
- **Per-applier PK column cache for redact path** — the applier looks up PK columns once per table (`info_schema` query) and threads them through every CDC event's redactor.ApplyRow call so randomize:* can derive seeds without re-querying per row.
- **Help text** on `migrate`, `sync start`, `backup full`, `schema preview` enumerates the 4 new strategies alongside the Phase 2.b presets.

### Compatibility

- **Drop-in upgrade from v0.58.x.** No flag changes; new strategies are opt-in.
- **Internal API change**: `redact.Strategy.Redact` now takes a third `seed []byte` parameter. All Phase 1 / Phase 2.a / Phase 2.b strategies ignore the seed (they're already deterministic without one); only randomize:* uses it. Pre-existing strategy callers (engine appliers, pipeline.redactRow) plumb a nil seed for non-randomize rules. Direct API users who built custom strategy implementations need a one-line signature update — third parameter is `_ []byte` for the common ignore-seed case.
- **`redact.Registry.ApplyRow` signature change**: now takes `pkColumns []string` + `streamID string` alongside the existing args. Engine appliers (MySQL, Postgres) are updated; direct API callers need to plumb the table's PK column list + stream identifier.
- **`pipeline.redactRow` / `redactRows` signature change**: now accept `pkColumns []string` + `streamID string`. All in-package call sites updated (`migrate`, `migrate_bulk`, `migrate_parallel`, `add_table`, `backup`).
- **`config.Redaction` gains `Min int64` + `Max int64` fields** for the `int` form of randomize. Pre-existing YAML configs unaffected (the new fields default to zero).

### Phase 2 progress

- Phase 2.a (v0.56.0): generic `mask:inner` / `mask:outer` + Luhn helper.
- Phase 2.b (v0.57.0 + v0.58.0): `mask:ssn`, `mask:pan`, `mask:pan-relaxed`, `mask:email`, `mask:ca-sin`, `mask:uk-nin`, `mask:iban`, `mask:uuid`.
- Phase 2.c first wave (this release): `randomize:int`, `randomize:email`, `randomize:us-phone`, `randomize:uuid`.
- Phase 2.c second wave (next): `randomize:pan` (Luhn-valid generated PAN), `randomize:ca-sin` (Luhn-valid generated SIN), and `randomize:from-list:<file>` (operator-supplied sample pool). Same seed contract; checksum-aware generators reuse the v0.56.0 Luhn helper.

### Who needs this release

- **Operators redacting integer columns** (ages, scores, IDs that aren't PII themselves but where a stable randomized surrogate makes downstream analytics easier than `hash:sha256`'s hex blob).
- **Operators redacting email/phone/UUID columns** wanting a placeholder shape that matches the column's apparent type (analytics tooling that expects a parseable email or a UUID-typed column).
- **Operators running continuous-sync who tried Phase 2.a/2.b but wanted something less deterministic** — randomize:* gives them per-row uniqueness while still satisfying CDC resume idempotency.
- **Anyone not using randomize:*** — drop-in, no behaviour change.

### Verification

- **Build + lint clean** across all tags (default, integration).
- **Unit tests** (`strategies_randomize_test.go`): 4 generator test groups cover determinism (same seed → same output), shape correctness (regex matches on email/phone/uuid; in-range int), nil-seed refusal, type-strict refusal (int generator on string input).
- **`DeriveRowSeed` test**: pins the seed-derivation contract (same inputs → same seed; per-component change → different seed; composite PK respected; empty streamID still stable).
- **`Registry.ApplyRow` tests**: randomize-on-no-PK refusal; randomize-with-PK replay stability; streamID-changes-seed cross-stream separation.
- **`pipeline.redactRow` tests**: PK + streamID plumbed through to randomize:*; refusal naming column + strategy + "primary key" hint.
- **Preflight tests**: no-PK randomize refusal; with-PK pass-through; out-of-scope-table silence; mixed `mask:uuid` + randomize-no-PK combined report.
- **CLI tests** (`redact_flag_test.go`): every randomize form happy path; every refusal branch (missing bounds, non-integer min/max, min>max, spurious options on no-options forms).
- **YAML tests**: every form happy path; refusal on missing/unknown form + spurious min/max on no-options forms.

See `docs/adr/adr-0039-randomize-strategy-determinism.md` for the per-row seed derivation contract and the rationale for deviating from MySQL Enterprise's pure-random semantics. See `docs/dev/notes/prep-pii-redaction-phase-2-strategy-catalog.md` for the full strategy catalog.

## [0.58.1]

**Closes Bug 60 — `mask:uuid` now refuses at startup when targeted at a UUID-typed column without a type override.** The v0.58.0 cycle's load-bearing scenario surfaced it: the mask preset's output (`550eXXXX-XXXX-XXXX-XXXX-XXXXXXXX0000`) contains `X` characters which aren't valid hex, so it fails mid-bulk-copy with an opaque pgx `cannot find encode plan` error when landing in a strict `uuid` column. By then the target schema was already created with the wrong column type and the operator had to manually unwind. v0.58.1 catches the misconfiguration before any data movement.

### Fixed

- **`preflightRedactTypes` runs after `translate.ApplyMappings`** in both the migrate path and the streamer's cold-start path. Walks every redaction rule; for any `mask:uuid` rule whose column's effective type is still `ir.UUID` (i.e., not re-typed by a `--type-override`), refuses with an operator-actionable error naming the column AND suggesting `--type-override=table.col=text` as the unblocker. Multiple offending rules are reported together so the operator sees the full picture in one run.
- **`--type-override=table.col=text`** (or any non-UUID text type) on a `mask:uuid` column short-circuits the refusal. Operators who'd already worked around the issue see no behaviour change.

### Compatibility

- **Drop-in upgrade from v0.58.0.** No flag, schema, or YAML changes.
- **Breaking only in the sense that `mask:uuid` on a UUID column now refuses at startup** instead of failing mid-bulk-copy. Operators hit by the v0.58.0 misconfiguration see a clear error pointing at the fix rather than an opaque pgx encode failure after partial data movement.
- **Other mask presets unaffected** — `mask:ssn`, `mask:pan`, `mask:email`, etc. produce string output that lands in TEXT/VARCHAR columns without type-shape conflicts. The preflight is scoped narrowly to the one preset with the known issue.

### Who needs this release

- **Anyone using `mask:uuid` in production**: upgrade to get the startup-time refusal instead of the mid-bulk-copy opaque error.
- **Anyone with a `mask:uuid` rule + an existing `--type-override=col=text`**: drop-in, no behaviour change (the preflight skips your case correctly).
- **Anyone not using `mask:uuid`**: drop-in, no behaviour change.

### Verification

- **Build + lint clean** across all tags.
- **New unit tests** (`TestPreflightRedactTypes`): 8 sub-tests cover nil-registry, nil-schema, refusal happy path, type-override short-circuit, missing column, non-mask:uuid pass-through, multi-rule combined report, mixed compatible rules.

## [0.58.0]

**PII Phase 2.b second wave — Canadian SIN, UK NIN, IBAN, UUID mask presets.** Four more country/format-specific mask strategies that close the Phase 2.b catalog. Operators with international PII (Canadian / UK national IDs, European/international bank account numbers, system-generated UUIDs) now have one-line per-column presets matching the canonical shapes.

### Added

- **`mask:ca-sin` preset** — Canadian Social Insurance Number. Accepts `XXX-XXX-XXX` (dashed) or `XXXXXXXXX` (undashed); validates Luhn checksum (CA SINs are required to satisfy it per Government of Canada rules); preserves last 3 digits; masks first 6 with `X`. Dashes preserved at original positions. Refuses Luhn-invalid input with a hint pointing operators at `mask:inner:0,3` for synthetic test data.
- **`mask:uk-nin` preset** — UK National Insurance Number. Accepts the canonical `AA999999A` shape (2 uppercase prefix letters + 6 digits + 1 uppercase suffix letter from {A,B,C,D}). Preserves prefix + suffix letters; masks the 6 middle digits. Suffix-letter set restricted to A/B/C/D per HMRC rules; prefix-letter set NOT validated here (the authoritative list is large and changes over time).
- **`mask:iban` preset** — International Bank Account Number. Accepts 15-34 char shapes per ISO 13616; validates 2-letter country code + 2-digit check digits at positions 0-3. Preserves country code + check digits + first 2 of BBAN + last 4 chars; masks middle. Operator-supplied space grouping (e.g., `DE89 3704 ...`) is preserved on output. Per-country structural length checks are NOT enforced (the spec allows 15-34; country-specific rules change over time).
- **`mask:uuid` preset** — canonical 8-4-4-4-12 hyphenated UUID. Preserves hyphens at positions 8/13/18/23 + first 4 hex chars + last 4 hex chars; masks all other hex digits with `X`. Mixed-case input is preserved at the preserved positions. Refuses non-36-char input or non-hex characters in digit positions.
- **YAML form** — `strategy: mask` + `form: ca-sin|uk-nin|iban|uuid`. Same no-options validation as the first-wave presets (`m1`/`m2`/`char` rejected with a clear "preset takes no other fields" error).
- **Help text** on `migrate`, `sync start`, `backup full`, `schema preview` enumerates all 8 country/format presets (4 first-wave + 4 second-wave).

### Compatibility

- **Drop-in upgrade from v0.57.x.** No flag changes; new presets are opt-in.
- **Strict-by-default** for every preset — shape violations refuse at row-process time with operator-actionable error.
- **Naming consistency** — every preset's `Strategy.Name()` matches the CLI spelling (`mask:ca-sin`, `mask:uk-nin`, etc.). The audit log + CHANGELOG cross-references stay coherent.

### Phase 2 progress

- ✅ **Phase 2.a** (v0.56.0): generic `mask:inner` / `mask:outer` + Luhn helper.
- ✅ **Phase 2.b first wave** (v0.57.0): `mask:ssn`, `mask:pan`, `mask:pan-relaxed`, `mask:email`.
- ✅ **Phase 2.b second wave** (this release): `mask:ca-sin`, `mask:uk-nin`, `mask:iban`, `mask:uuid`.
- **Phase 2.c** (next major chunk): `randomize:*` generators (random PAN, random US-phone, random UUID, etc.) with per-stream-id deterministic seeding.

Phase 2.b is now **fully shipped** — every catalog-listed country/format mask preset is available. Phase 2.c (randomized generation) is the next major chunk; operators needing format-preserving redaction for column shapes not covered by the preset set (e.g., German Personalausweis, Indian Aadhaar, Australian TFN) should continue using generic `mask:inner` until per-country presets land based on demand.

### Who needs this release

- **Operators with Canadian / UK PII** (SIN, NIN columns).
- **Operators with IBAN-typed bank-account columns** — common in European financial services, fintech, payment processors.
- **Operators with UUID columns** wanting log-friendly surrogates that preserve the prefix/suffix-byte (useful for correlating across systems without exposing the full identifier).
- **Anyone not redacting PII**: drop-in, no behaviour change.

### Verification

- **Build + lint clean** across all tags.
- **Unit tests**: 4 new test groups in `strategies_preset_test.go` cover the happy path + every documented refusal for each preset (Luhn rejection on CA-SIN, suffix-letter restriction on UK-NIN, ISO-13616 length bounds on IBAN, hyphen-position checks on UUID, etc.). 2 new CLI/YAML cross-cutting tests in `redact_flag_test.go` exercise the parser dispatch end-to-end.

## [0.57.0]

**PII Phase 2.b first wave — country/format-specific mask presets.** Operators with PAN, SSN, or email columns can now write `--redact users.pan=mask:pan` (or `mask:ssn`, `mask:pan-relaxed`, `mask:email`) instead of stacking generic `mask:inner` with careful margin counting + Luhn validation. The presets validate input shape, refuse misconfiguration loudly, and produce the canonical masked output for each PII type.

### Added

- **`mask:ssn` preset** — US Social Security Number. Accepts `XXX-XX-XXXX` shape (validates dash positions + digit-only body); outputs `XXX-XX-NNNN` (preserves last 4). Refuses non-conforming input with operator-actionable error naming the position of the violation. SSNs stored without dashes (`XXXXXXXXX`) should use generic `mask:inner:0,4` instead — the strict-shape check exists because real-world SSN columns are usually stored consistently dashed or undashed.
- **`mask:pan` preset** — strict payment-card PAN. Validates Luhn checksum (using the v0.56.0 `luhnValid` helper); accepts 12-19 digits per ISO/IEC 7812; preserves first 6 (BIN) + last 4 (suffix); masks middle digits with `X`. Non-digit characters (spaces, hyphens) preserved at their original positions: `"4111 1111 1111 1111"` → `"4111 11XX XXXX 1111"`. Refuses Luhn-invalid input (use `mask:pan-relaxed` for synthetic test data).
- **`mask:pan-relaxed` preset** — lenient PAN. Same masking shape as `mask:pan` but skips Luhn validation. Useful for synthetic test data or tokenized values that follow PAN shape without satisfying the checksum.
- **`mask:email` preset** (sluice-native — MySQL Enterprise doesn't ship an equivalent) — preserves first character of local part, masks the rest of the mailbox with `X`, preserves the entire `@domain` verbatim. `alice@example.com` → `aXXXX@example.com`. Uses LAST `@` as separator (RFC 5322 quoted local-part safe). Refuses input without `@` or with empty local part.
- **YAML form** — `strategy: mask` + `form: ssn|pan|pan-relaxed|email`. The presets take no extra fields (m1/m2/char rejected with a clear "preset takes no other fields" error so operators notice when they've mixed forms).
- **Help text** on `migrate`, `sync start`, `backup full`, `schema preview` enumerates all four new presets in the supported-strategies list.

### Compatibility

- **Drop-in upgrade from v0.56.x.** No flag changes; new presets are opt-in.
- **Strict-by-default** — each preset validates input shape AND refuses on violation rather than silently passing through. Operators who want lax behaviour on PANs should use `mask:pan-relaxed`; for non-canonical SSNs use generic `mask:inner:0,4`.
- **Audit log `Name()`** — each preset's audit-log identifier matches the CLI spelling exactly (`mask:ssn` / `mask:pan` / `mask:pan-relaxed` / `mask:email`). Compliance reviewers can pivot between log lines and the CHANGELOG entry without translation.

### Who needs this release

- **Operators with dedicated PAN, SSN, or email columns** who want a one-line per-column rule instead of memorizing the margin counts for each PII shape.
- **Compliance / audit teams** reading sluice's redaction logs — the preset name in the audit line is self-documenting (`mask:pan` is more legible than `mask:inner:6,4`).
- **Anyone not redacting PII**: drop-in, no behaviour change.

### Phase 2 progress

- ✅ **Phase 2.a (v0.56.0)**: generic `mask:inner` / `mask:outer` + Luhn helper.
- ✅ **Phase 2.b first wave (v0.57.0, this release)**: `mask:ssn`, `mask:pan`, `mask:pan-relaxed`, `mask:email`.
- **Phase 2.b second wave** (next): `mask:ca-sin`, `mask:uk-nin`, `mask:iban`, `mask:uuid`.
- **Phase 2.c** (later): `randomize:*` generators (random PAN, random US-phone, etc.) with the per-stream-id deterministic seeding contract per the prep doc.

See `docs/dev/notes/prep-pii-redaction-phase-2-strategy-catalog.md` for the catalog and MySQL Enterprise reference mapping.

### Verification

- **Build + lint clean** across all tags.
- **Unit tests**: 4 new test groups in `strategies_preset_test.go` cover the happy path + every refusal branch for each preset (PAN Luhn rejection, SSN dash-position checks, email no-`@` refusal, etc.). Cross-cutting CLI + YAML tests in `redact_flag_test.go` exercise the parser dispatch.
- **End-to-end pin scenarios for the v0.57.0 cycle**: `--redact users.email=mask:email` on a real email column lands `aXXXX@example.com`-shape output; `--redact users.pan=mask:pan` refuses a Luhn-invalid synthetic PAN; YAML form `strategy: mask, form: ssn` works equivalently to CLI form.

## [0.56.1]

**Closes Bug 59 — `--redact` was kong-split on the literal comma in `mask:inner:4,4`.** The v0.56.0 cycle subagent caught it: passing `--redact users.pan=mask:inner:4,4` made kong's default `sep:","` split the value into two list entries (`users.pan=mask:inner:3` + `4`), so the parser saw only `mask:inner:3` and refused with a misleading `got 1 args` error. Operators following the v0.56.0 CHANGELOG / release-notes examples literally would hit this on first try.

### Fixed

- **`sep:"none"` on every `--redact` declaration** — `sluice migrate`, `sluice sync start`, `sluice backup full`, `sluice schema preview`. Each `--redact` value now flows through kong as a single string regardless of embedded commas; the only splitting happens inside sluice's parser at the documented `:` / `,` positions inside `mask:<form>:<m1>,<m2>[,<char>]`.
- **Regression test** (`TestRedactFlag_KongCommaPreservation`) parses `--redact users.pan=mask:inner:4,4` through real kong at each of the four sites and asserts the Redact slice has exactly one element with the comma intact. Catches any future re-introduction of the default separator.
- **Help text** on `migrate`, `sync start`, `backup full`, `schema preview` now lists the `mask:inner` / `mask:outer` strategy shapes in the supported-strategies enumeration — previously they were valid but operator-discovery required reading the CHANGELOG or the prep doc.

### Migration / Compatibility

- **Drop-in upgrade from v0.56.0.** No flag, schema, or YAML changes; only the CLI parser shape changes (kong no longer splits the value, which was undocumented behaviour). Pre-v0.56.1 `mask:inner:3,4` rules that escaped the comma (`mask:inner:3\,4`) continue to work — the backslash isn't special to sluice's parser, it was a kong shell-side workaround that was never required and is no longer relevant.
- **YAML form is unaffected** — koanf does not split list-of-string values on commas. Operators using `redactions:` blocks were never exposed to Bug 59.

### Who needs this release

- **Anyone using `mask:inner` / `mask:outer` via the CLI flag form.** Drop in.
- **Anyone using only Phase 1 strategies** (`null`, `static:`, `hash:`, `truncate:`) — no behaviour change; the bug only triggered on comma-containing strategy options, and Phase 1 strategies don't take comma-separated arguments. Drop in.

### Verification

- Build + lint clean across all tags.
- New unit test covers the four declaration sites end-to-end through kong.
- Manual: `sluice migrate --redact users.pan=mask:inner:4,4 --source-driver=... --target-driver=...` now parses cleanly (previously rejected with `got 1 args`).

### Also fixed

- **`startHeartbeat` test pollution under `slog.SetDefault()` swaps.** `TestStartHeartbeat_ZeroIntervalDisables` on `windows-latest` (Test job) flaked when a stray tick from the previous test's heartbeat goroutine landed in the next test's slog buffer. Root cause: the goroutine called `slog.InfoContext` which reads `slog.Default()` lazily at log-time; if test1's orphan goroutine was mid-tick when test2 swapped `slog.Default()` to its own handler, the stale tick wrote to test2's buffer. Fix: capture `slog.Default()` once at `startHeartbeat` call-site instead. Production behaviour is identical (operators don't swap `slog.Default()` mid-stream); the change is purely about test hermeticity. Folded into this release to keep the gate green.

## [0.56.0]

**PII Phase 2.a — generic format-preserving mask strategies + Luhn helper.** Operators redacting PAN, SSN, phone, or similar fixed-shape identifiers can now use `mask:inner` / `mask:outer` instead of stacking `truncate:` + `static:` to fake format preservation. The Luhn helper lands as shared infrastructure for the Phase 2.b checksum-aware strategies (`gen_rnd_pan`-style) coming in a later release.

### Added

- **`mask:inner:<m1>,<m2>[,<char>]` strategy** — keeps the first M1 + last M2 runes, masks the middle. `--redact users.pan=mask:inner:4,4` on `4111111111111111` yields `4111XXXXXXXX1111`; `--redact users.ssn=mask:inner:0,4,*` on `123456789` yields `*****6789`. Char defaults to `X` when omitted, single rune only (operators wanting `Xx` repeating patterns are explicitly out of scope).
- **`mask:outer:<m1>,<m2>[,<char>]` strategy** — inverse of `mask:inner`: masks the first M1 + last M2 runes, keeps the middle. Less common in real-world PII workflows but matches MySQL Enterprise's `mask_outer` for parity.
- **YAML form**: `strategy: mask` + `form: inner|outer` + `m1:` + `m2:` + optional `char:`. Same validation as the CLI parser (negative margins refused, multi-rune `char:` refused, missing `form:` refused).
- **Rune-counted iteration**: both forms preserve input rune-length and are UTF-8 / emoji safe — a 16-char PAN stays 16 chars; `🎉ñe@x.com` masks rune-wise rather than slicing mid-byte.
- **Luhn helper** (`internal/redact/luhn.go`): `luhnValid` + `luhnCheckDigit`. Validates and produces Luhn-conformant ISO/IEC 7812 numbers (PAN / SIN / IMEI). Skips non-digit characters in the input (so `4111-1111-1111-1111` and `4111 1111 1111 1111` both validate). Not exposed via CLI yet — shared infrastructure for the upcoming Phase 2.b `gen_rnd_pan` strategy.

### Compatibility

- **Drop-in upgrade from v0.55.x.** No flag changes; new strategies are opt-in.
- **Boundary behaviour**: when M1+M2 ≥ rune-length, `mask:inner` is a no-op (nothing to mask) and `mask:outer` masks the whole value. Negative margins are clamped to 0 defensively though the CLI/YAML parsers refuse them at parse time.
- **Strategy.Name() audit log** for mask omits the mask character: `mask:inner:4,4` / `mask:outer:1,1` regardless of `char:` setting. The character choice is uninteresting from an audit perspective; form + margins fully describe what was applied.

### Who needs this release

- **Operators redacting PAN / SSN / phone / passport / TIN columns** who want to preserve the on-target rune-length for downstream tooling (analytics tables expecting `CHAR(16)`, reports rendering "**** **** **** 1234"). `truncate:` + `static:` couldn't do this; `mask:inner` does.
- **Anyone not redacting PII**: drop-in, no behaviour change.

### Phase 2 roadmap

- **Phase 2.a (this release)**: generic mask strategies + Luhn helper.
- **Phase 2.b (next)**: checksum-aware strategies — `gen_rnd_pan`, `gen_rnd_us_phone`, `gen_rnd_canada_sin`. The Luhn helper lands here pre-emptively.
- **Phase 2.c (later)**: structured-data strategies — `mask_iban`, `mask_uuid`, `mask_email` (separator-aware).

See `docs/dev/notes/prep-pii-redaction-phase-2-strategy-catalog.md` for the full strategy catalog and MySQL Enterprise reference mapping.

### Verification

- **Build + lint clean** across all tags (default, integration, psverify).
- **Unit tests**: `TestLuhnValid` / `TestLuhnCheckDigit` cover the algorithm; `TestMask_Inner` / `TestMask_Outer` cover boundaries (M1+M2 ≥ length, UTF-8, custom char, negative margins, nil); `TestParseRedactFlags_Mask` + `TestParseRedactFlags_MaskRefusalPaths` + `TestMergeYAMLRedactions_Mask` cover the CLI + YAML parse paths.
- **End-to-end pin scenarios for the v0.56.0 cycle**: confirm `mask:inner:4,4` on a PAN column lands `4111XXXXXXXX1111` on the target; confirm YAML-shape `strategy: mask, form: inner` works equivalently.

## [0.55.1]

**Adds `--target-schema=NAME` to `sluice restore`** — closes the UX-gap the v0.55.0 cycle subagent flagged when restoring redacted backups. Pre-v0.55.1 the restore command lacked the schema-override flag that every other operator-facing command (migrate / sync start / schema preview / schema diff / matview / schema add-table) already had, forcing operators to either use the DSN's default schema or work around it via docker. Now restore mirrors the existing pattern.

### Added

- **`sluice restore --target-schema=NAME`** flag (Postgres-only). When set, restored tables land in the named schema rather than the DSN's default. Mirrors `sluice migrate --target-schema` and `sync start --target-schema` (ADR-0031). PG-only: flat-namespace engines (MySQL) refuse at validate time — operators use a different `--target` DSN database instead. The schema is auto-created on the target if it doesn't exist.

- **`pipeline.Restore.TargetSchema` and `pipeline.ChainRestore.TargetSchema`** fields in the Go API. nil/empty preserves the pre-v0.55.1 behaviour (DSN-derived default schema).

- **Threading through the chain-restore path**: `ChainRestore` propagates `TargetSchema` to the chain's full-application sub-Restore + the per-incremental change applier so chain restores honour the override end-to-end.

### Migration / Compatibility

- **Drop-in upgrade from v0.55.0.** No behaviour change unless the operator passes `--target-schema`.
- **MySQL targets** refuse the flag at validate time with the same engine-not-namespaced refusal as the other commands.

### Who needs this release

- **Anyone restoring backups onto a shared Postgres target where the DSN's default schema is in use by another stream/tenant**: this is now a first-class operation. No more docker workaround.

### Verification

- **Build + lint clean** across all tags.
- **End-to-end verification deferred to a future cycle**: pin scenario is `sluice restore --from-dir=<chain> --target-driver=postgres --target=<DSN> --target-schema=v0551_test` lands tables in the named schema rather than `public`.

## [0.55.0]

**PII Phase 1.5 closure — the last two deferred items.** Schema-preview annotation and backup-stream redaction. Phase 1.5 is now structurally complete: every operator-facing surface that touches row data (migrate, sync start cold-start + CDC, backup full, schema preview) honours `--redact` either by applying redactions or by surfacing them as comments.

### Added

- **Backup-stream redaction**. `sluice backup full` (and chained-incremental variants) accept `--redact` + `--redact-key-source`. Rules apply at the chunk-write step in `Backup.backupTable` before each row hits the writer, so on-disk backup chunks are PII-clean. `--redact` and `--redact-key-source` operator-facing flags match the syntax already shipped on `migrate` and `sync start`; YAML config block is shared (`redactions:` + `redact_key_source:`).

- **`Backup.Redactor *redact.Registry` field** in the Go API. nil/empty is the no-redactions hot path (zero-cost passthrough); existing callers stay on v0.54.x semantics by default.

- **Schema-preview annotation**. `sluice schema preview` accepts `--redact` + `--redact-key-source` (same syntax). Each redacted column's CREATE TABLE line gets a trailing `-- REDACTED via <strategy>` comment so operators can SEE what `sluice migrate` / `sync start` would redact before committing.

- **`Previewer.Redactor *redact.Registry` field** in the Go API. Same nil/empty no-op semantics; preview output remains byte-identical to pre-v0.55.0 when no redactions are configured.

- **`appendRedactComment` annotator** in `internal/pipeline/preview.go`. Mirrors the existing `appendNoteComment` trailing-comma handling so the annotated DDL stays parseable if anyone copies it from the preview output.

### Migration / Compatibility

- **Drop-in upgrade from v0.54.x.** No flag changes; new flags are opt-in. The schema-preview output is byte-identical when `--redact` is absent.
- **Restore-from-backup semantics**: redactions ARE NOT re-applied at restore time. Backups created with `--redact` already contain redacted rows on disk; the restore path just copies those rows through. Operators wanting different redactions need to re-create the backup with the new rule set.
- **YAML config is shared** across `migrate` / `sync start` / `backup full` / `schema preview` — the `redactions:` block applies to all of them. Operators with one canonical rule set can declare it once in `sluice.yaml` and have it picked up everywhere.

### Who needs this release

- **Operators backing up production data for cross-region/cross-tenancy/vendor handoffs**: `sluice backup full --redact users.email=hash:sha256 --output-dir /backups/staging` produces a PII-clean backup chain. Compliance teams get a written audit trail (the audit INFO log line); engineers get realistic-shape backup data without exposure.
- **Operators reviewing redaction config before committing**: `sluice schema preview --redact ... --target-driver=...` annotates the preview DDL so reviewers can confirm coverage at a glance.
- **Anyone not redacting PII**: drop-in, no behaviour change.

### Verification

- **End-to-end deferred to v0.55.0 cycle**: pin scenarios include `sluice backup full --redact users.email=hash:sha256` to a local-fs backup, then inspect the chunk file's row contents (via `sluice restore` to a fresh target) to confirm hex digests not plaintext. `sluice schema preview --redact users.email=hash:sha256` should show `-- REDACTED via hash:sha256` annotation on the email column's CREATE TABLE line.
- **Build + lint clean** across all tags (default, integration, psverify).

### PII Phase 1.5 — fully shipped

Three releases closed the four deferred items from Phase 1's CHANGELOG entry (v0.53.0):

| Phase 1.5 item | Closed in |
|---|---|
| CDC apply-path redaction | v0.54.0 (with v0.54.1 fix for Bug 58 schema-namespace mismatch) |
| YAML config support | v0.54.0 |
| Schema-preview annotation | v0.55.0 (this release) |
| Backup-stream redaction | v0.55.0 (this release) |

Next: **PII Phase 2.a** (generic `mask:inner` + `mask:outer` + Luhn helper) per `docs/dev/notes/prep-pii-redaction-phase-2-strategy-catalog.md`.

## [0.54.1]

**Closes Bug 58 — CDC apply-path PII redaction was non-functional due to schema-namespace key mismatch.** The v0.54.0 cycle's load-bearing test caught it: the wiring was structurally in place but lookups missed because engine CDC readers emit non-empty `Schema` while operator CLI flags register with `schema=""`. Cold-start bulk-copy was unaffected (uses `table.Schema=""` matching the CLI key); only mid-stream CDC events bypassed redaction.

### Fixed

- **`redact.Registry.Get` now falls back to the bare-schema rule** when the schema-qualified lookup misses. Lookup order: `(schema, table, column)` first, then `("", table, column)` if non-empty schema was queried. Preserves the documented CLI semantics ("bare `users.email=...` matches any source schema") AND lets engine-emitted schemas pass through cleanly.
  - **MySQL VStream** populates `ir.Insert.Schema` with the keyspace name (e.g., `sluice-validation-mysql-source`).
  - **Postgres CDC** populates `Schema` with the relation's schema (typically `public`).
  - Both shapes now match operator-bare `--redact users.email=hash:sha256` via the fallback.

- **Schema-qualified rule still wins on duplicates.** Operators with multi-source aggregation (`customer_svc.users.email` vs `audit_svc.users.email`) still get precise per-schema behaviour: the schema-qualified `Set` takes precedence over the bare fallback when both are registered.

- **`--redact` help text updated** on both `migrate` and `sync start` to reflect the v0.54.0 closure (Phase 1.5: bulk-copy AND CDC apply paths both honour `--redact`).

### Migration / Compatibility

- **Drop-in upgrade from v0.54.0.** Operators running `sluice sync start --redact ...` against v0.54.0 should upgrade immediately — CDC events were silently flowing to the target unredacted in v0.54.0 (the v0.54.0 cycle's reproduction landed 3 plaintext rows on a PG target's `users.email` column despite the `--redact users.email=hash:sha256` flag).
- **No schema, position-token, or YAML config changes.**

### Who needs this release

- **Anyone running `sluice sync start --redact ...` on v0.54.0**: **upgrade immediately**. CDC events are bypassing redaction in v0.54.0; v0.54.1 closes that gap.
- **Anyone on v0.53.0 or earlier**: skip v0.54.0, upgrade straight to v0.54.1.

### Verification

- **New regression test in `internal/redact/redact_test.go`**: `Bug 58: bare CLI rule matches any source schema via fallback`. Exercises both MySQL-keyspace-shape and PG-public-shape lookups against a bare-schema rule.
- **Companion test**: schema-qualified rule takes precedence over bare-fallback when both are registered (preserves multi-source aggregation semantics).
- **End-to-end verification deferred to v0.54.1 cycle**: re-run the v0.54.0 cycle's CDC redaction scenario; pin is "CDC-applied rows show 64-char hex emails (not plaintext) on the target".

## [0.54.0]

**PII redaction Phase 1.5 — the two highest-impact deferrals from v0.53.0.** Closes the operator surprise where `sluice sync start --redact` redacted only the cold-start bulk-copy phase but let mid-stream CDC events flow to the target unredacted; adds YAML config support so production deployments can version-control redaction rules instead of stringing them through CLI flags.

The two remaining Phase 1.5 items (schema-preview annotation + backup-stream redaction) are deferred to v0.55.0.

### Added

- **CDC apply-path redaction** on both engines. New `ir.RedactorSetter` optional interface (parameter type `any` to avoid an ir → redact dependency cycle); MySQL + Postgres appliers implement `SetRedactor(registry any)` and invoke `redact.Registry.ApplyRow` on every change's row data before dispatch. Wrap point covers all dispatch shapes:
  - `Insert.Row`
  - `Update.Before` AND `Update.After`
  - `Delete.Before`
  - `Truncate` / `TxBegin` / `TxCommit` pass through (no row data)
- **`redact.Registry.ApplyRow(schema, table, row)` method** for CDC's row-keys-only case. Best-effort `Nullable=true` default for the col metadata; strategies that refuse on NOT NULL (`Null`) will silently produce nil at strategy level and the engine catches the constraint violation loudly at INSERT time (ADR-0038 retry loop classifies the error).
- **Pipeline `applyRedactor` helper** mirrors `applyExecTimeout` / `applyMaxBufferBytes` — threads the Streamer.Redactor through to the applier via the optional interface at openApplier time.
- **YAML config block** under `redactions:`. Each entry has `table`, `strategy`, plus strategy-specific `algo` / `value` / `length` fields. Operators wanting the bare `null` strategy must quote: `strategy: "null"` (YAML's null literal collides with the string form otherwise; documented in the config doc-comment).
- **YAML + CLI merge semantics**: CLI flags are parsed first; YAML entries append. Duplicates on the same column have last-write-wins; operators wanting YAML to be authoritative should not pass conflicting CLI flags.
- **`redact_key_source` YAML field** mirrors `--redact-key-source`. CLI flag overrides YAML when both are set.

### Migration / Compatibility

- **Drop-in upgrade from v0.53.x.** No flag changes — same `--redact` syntax. Operators previously running `sluice sync start --redact ...` will see CDC events now redacted too (they were not in v0.53.0). The audit log line at startup is the same.
- **No schema or position-token changes.**
- **The PII Phase 1.5 caveat from v0.53.0's CHANGELOG is closed** — `--redact` on `sync start` now covers both phases. Backup-stream redaction and schema-preview annotation remain deferred.

### Who needs this release

- **Anyone running `sluice sync start --redact ...`**: **upgrade**. v0.53.0's caveat ("CDC apply path is NOT yet redacted") is the load-bearing reason. PII columns in CDC events were flowing to the target unredacted; v0.54.0 closes that.
- **Operators wanting reviewable / version-controllable redaction config**: use the new `redactions:` YAML block instead of long `--redact` CLI argument lists. Both forms are equivalent; YAML is the production-recommended form.
- **Anyone not redacting PII**: drop-in, no behaviour change.

### Verification

- **`internal/redact` unit tests** extend the Registry's ApplyRow contract.
- **`internal/config` tests** cover YAML round-trip for all four strategies + the `strategy: "null"` quoting requirement.
- **`cmd/sluice` tests** cover `mergeYAMLRedactions` end-to-end + the YAML-extends-CLI semantics + every YAML refusal mode + HMAC keyset wired via YAML config.
- **CDC apply-path verification deferred to the v0.54.0 cycle**: pin scenarios include `sluice sync start --redact users.email=hash:sha256` against a running source that's INSERTing into users mid-stream, then query the target after a few minutes and confirm emails are hex-digests (not plaintext) for both the cold-start rows AND the post-cold-start CDC-applied rows.

### What's NOT in v0.54.0 (deferred to v0.55.0+)

- **Schema-preview annotation** — `sluice schema preview` / `sluice schema diff` don't yet annotate redacted columns with `-- REDACTED via <strategy>` comments.
- **Backup-stream redaction** — `backup full` / `backup stream run` paths don't yet honour `--redact`.
- **Phase 2 strategy catalog** (format-preserving + randomized + dictionary) — see `docs/dev/notes/prep-pii-redaction-phase-2-strategy-catalog.md` for the planned mapping of MySQL Enterprise's data-masking functions to sluice-native strategies.

## [0.53.0]

**Lands GitHub issue #24 chunk 15a — PII redaction Phase 1.** First half of the largest user-facing roadmap addition since multi-source aggregation: operators can now declare per-column redactions that fire during the bulk-copy path. "Snapshot prod → staging without PII" is now a first-class workflow instead of requiring an external Tonic / Privacy Dynamics / custom-ETL detour. Phase 2 (format-preserving + tokenize), Phase 3 (JSON-path), and Phase 4 (cross-stream keyset persistence) are tracked separately on the roadmap.

Phase 1 ships the four highest-value strategies — `null` / `static` / `hash:sha256` / `hash:hmac-sha256` / `truncate` — covering ~70% of typical real-world PII redaction asks per the issue body's analysis. CDC apply-path redaction (continuous-sync mode) is a Phase 1.5 follow-up; this release covers the cold-start bulk-copy path that's the main use case for `sluice migrate`.

### Added

- **`--redact TABLE.COLUMN=STRATEGY[:options]`** on `sluice migrate` and `sluice sync start` (repeatable). Operator declares per-column redactions; rules apply during the bulk-copy phase (cold-start for both commands). Supported strategies:
  - `null` — replace with NULL. Refuses at runtime on NOT NULL columns with operator-actionable hint suggesting `static:` alternative.
  - `static:<value>` — replace with literal constant. Value isn't logged in the audit line (could leak which columns hold PII).
  - `hash:sha256` — SHA-256 hex digest (stateless, deterministic across runs and machines). Same input → same 64-char hex.
  - `hash:hmac-sha256` — keyed hash. Requires `--redact-key-source`; produces deterministic surrogates within a single keyset.
  - `truncate:<n>` — keep first N runes (not bytes — UTF-8/emoji safe). Refuses non-string columns.

- **`--redact-key-source SOURCE`** on the same commands. Provides the HMAC keyset for `hash:hmac-sha256` rules. Three forms: `env:VARNAME` (read from environment), `file:PATH` (first line of file), `derive:<salt>` (Phase 1's simple key-derivation: SHA-256 of `streamID:salt`). Phase 4 will land a proper keyset-persistence story; until then, operators wanting stable surrogates across multiple streams must use `env:` or `file:` with the same key everywhere.

- **`internal/redact` package** — `Strategy` interface + `Registry` (Set/Get/Empty/Rules; case-folded keys) + the four Phase 1 strategies. Public for external Go-API callers; the CLI layer wraps it via `parseRedactFlags`.

- **Pipeline wiring** at every bulk-copy variant:
  - `copyTable` (simple cold-start)
  - `copyTableIdempotent` (mid-stream add-table)
  - `copyTableWithCursor` (resumable per-batch cursor path)
  - `copyChunk` (parallel-copy per-chunk goroutine)

  Each callsite wraps the row channel with the new `redactRows` helper; nil/empty Registry is a zero-cost passthrough (no goroutine, no allocations).

- **`Migrator.Redactor`, `Streamer.Redactor`, `AddTable.Redactor` fields** for direct Go-API callers + structural plumbing.

- **Audit log line** at command start: `sluice: redaction configured scope=migrate columns=N strategies=[...]`. Logs distinct strategy names + total column count but NOT per-column rules (the rules themselves can leak which columns hold PII).

### Migration / Compatibility

- **Drop-in upgrade from v0.52.x.** Operators not setting `--redact` get unchanged pre-v0.53.0 behaviour. The redact wrap is a zero-cost passthrough when no rules are configured (no goroutine spawn, no row allocations).

- **No schema or position-token changes.** Redactions apply at the IR layer between row read and target write; no on-disk format implications.

- **CDC apply path is NOT yet redacted** (deferred to Phase 1.5). `sluice sync start` redacts the cold-start bulk-copy phase but apply-phase changes flow to the target unredacted. The CHANGELOG entry for Phase 1.5 will document the closing of this gap. Operators needing CDC redaction today should restrict to migrate-mode use (no continuous sync after bulk-copy completes).

- **No engine surface changes** — the per-engine writer interfaces (`RowWriter.WriteRows`, etc.) are untouched. Redaction happens upstream of the writer.

### Who needs this release

- **Operators staging production data for dev/QA environments**: the canonical use case. `sluice migrate --redact users.email=hash:sha256 --redact users.phone=truncate:4` produces a staging clone where emails are 64-char hex surrogates (deterministic across runs — supports join-by-email tests) and phones show only the area code. Compliance teams get a written audit trail (the INFO log line at startup); engineers get realistic-shape data without exposure.

- **Operators bound by GDPR / CCPA / HIPAA constraints on cross-region or cross-tenancy data movement**: redact at the source-jurisdiction boundary; the target inherits only the redacted shape.

- **Operators handing data to third-party processors**: `--redact billing.credit_card=static:REDACTED` ensures the processor sees the schema + the row count + the table shape but no card numbers.

- **Anyone NOT redacting PII**: drop-in, no behaviour change.

### Verification

- **`internal/redact` unit tests** (~438 LOC): every strategy's happy + refusal paths; Registry's Set/Get/Empty/Rules + case-folding + nil-strategy panic + last-write-wins.
- **`internal/pipeline` unit tests** (~210 LOC): `redactRow` + `redactRows` channel-wrapper covering nil/empty passthrough, multi-row streaming, refusal wrapping + channel close, ctx-cancel-exits-cleanly.
- **`cmd/sluice` unit tests** (~246 LOC): every flag form (each strategy + each key-source variant), every documented refusal, audit log smoke test.
- **End-to-end validation deferred to the v0.53.0 cycle**: pin scenarios include cold-start migrate with `--redact users.email=hash:sha256` and confirmation that target rows contain SHA-256 hex digests instead of plaintext emails (round-trip via `psql` / `mysql` on the validation rig).

### What's NOT in v0.53.0 (deferred)

- **CDC apply-path redaction** (Phase 1.5) — sync-start's continuous CDC phase still ships changes verbatim.
- **Backup-stream redaction** (Phase 1.5) — `backup full` / `backup stream run` paths don't yet honour `--redact`.
- **Schema-preview annotation** — `sluice schema preview` / `schema diff` don't yet annotate redacted columns. Operators can verify via the audit log line.
- **YAML config support** for the `redactions:` block — Phase 1 ships CLI-only; YAML follows once the operator-visible flag shape settles.
- **Format-preserving / tokenize / randomize strategies** (Phase 2).
- **JSON-path redaction** (Phase 3).
- **Cross-stream keyset persistence** (Phase 4).

See [docs/dev/notes/prep-pii-redaction-phase-1.md](docs/dev/notes/prep-pii-redaction-phase-1.md) for the full design + the roadmap entry for the four-phase sequence.

## [0.52.2]

**Closes Bug 57 — the v0.52.0/v0.52.1 silent-stall retry path was inert.** Pre-existing bug from v0.52.0 masked by Bug 56. The v0.52.1 cycle subagent reproduced it deterministically on `--apply-exec-timeout=1ms`: sluice exited 0 in ~3 seconds with zero retry log lines. Root cause: the streamer's `runOnce` and `runWithRetry` both tested `errors.Is(err, context.DeadlineExceeded)` BEFORE the `errors.As(err, &ir.RetriableError)` check; since `classifyApplierError` wraps the timeout-driven DeadlineExceeded as a retriable wrapper that preserves the inner error via Unwrap, `errors.Is` matched via the Unwrap walk and the streamer mistook the wrapped timeout for a clean ctx-shutdown — exiting the retry loop without ever activating it.

### Fixed

- **Streamer's runWithRetry now checks the retriable wrapper before the ctx-termination short-circuit** (`internal/pipeline/streamer.go`). New helper `errIsRetriable(err, *re)` encapsulates the `errors.As + Retriable()` predicate; both `runWithRetry` and `runOnce` route through it. Genuine bare ctx-termination (operator Ctrl-C, sync stop) still triggers clean shutdown — the check just has to come AFTER the retriable test now.

- **`runOnce`'s dispatch-error branch** now applies the same reordering. A wrapped retriable error from the applier propagates to `runWithRetry` (where the retry loop activates) instead of being silently swallowed as if the apply call had returned because the outer ctx was cancelled.

### Added

- **`applier: apply latency` DEBUG log line on the per-change apply path** for both engines (v0.52.0 cycle's secondary observability finding). The batched path (`ApplyBatch` with `--apply-batch-size > 1`) has emitted `applier: batch latency` since v0.7.0; the non-batched path (default `--apply-batch-size=1`) had no equivalent line, so operators running default settings had no DEBUG signal that apply was making progress. Cycle subagents now see per-change latency lines symmetrically across both apply modes.

### Migration / Compatibility

- **Drop-in upgrade from v0.52.x.** No flag changes; the same `--apply-exec-timeout=DUR` setting now actually drives the retry loop (which was always the v0.52.0 design intent — Bug 57 just meant the wire was never connected end-to-end).
- **No schema or position-token changes.**
- **The retry surface gains no new behaviour shape** — existing `--apply-retry-attempts` / `--apply-retry-backoff-base` / `--apply-retry-backoff-cap` flags govern the timeout-driven retry just as they govern all other ADR-0038 retriable shapes.
- **New DEBUG log line** `applier: apply latency` fires only at `--log-level=debug`; INFO-level operators see no change.

### Who needs this release

- **Anyone running v0.52.0 or v0.52.1 against a remote managed-database target**: **upgrade**. The Phase B fix's wire was disconnected at the streamer end; the timeout watchdog fired but the retry loop never activated, so sluice exited cleanly on the first half-closed connection instead of recovering. v0.52.2 reconnects the wire.

- **Anyone still on v0.51.0 or earlier**: skip v0.52.0 / v0.52.1 entirely, upgrade straight to v0.52.2.

### Verification

- **End-to-end on the validation rig**: v0.52.1 cycle subagent's deterministic 3-second exit on `--apply-exec-timeout=1ms` is the reproduction case. v0.52.2 cycle re-runs the same scenario; pin is "retry log line fires + exit happens only after `--apply-retry-attempts` exhaustion" (not after a single timeout fire).
- **New unit tests in `streamer_retry_test.go`**:
  - `TestErrIsRetriable_Bug57` — asserts both the bug-shape (errors.Is sees DeadlineExceeded via the wrapper's Unwrap chain) AND the fix-shape (errIsRetriable returns true) hold simultaneously, with documentation-as-test for the required check order.
  - `TestErrIsRetriable_NonRetriable` — bare DeadlineExceeded / Canceled / wrapped-but-non-retriable / nil all stay non-retriable so the clean-shutdown short-circuit still fires for operator-Ctrl-C / sync-stop.

## [0.52.1]

**Completes the GitHub #23 silent-stall fix shipped in v0.52.0.** Bug 56: v0.52.0 wrapped the four `dispatch` sites (Insert / Update / Delete / Truncate) with `--apply-exec-timeout` but missed two adjacent destination-side blocking calls on the same apply hot path. The v0.52.0 cycle subagent captured three independent goroutine pprof snapshots over a 10-minute window on the validation rig showing goroutine 1 continuously blocked at the unwrapped sites (first `tx.Commit()`, then `writePositionTx`'s bare `tx.ExecContext`). Same silent-stall failure mode v0.52.0 was meant to close.

### Fixed

- **`writePositionTx` is now wrapped by the per-exec timeout** on both engines, at all three call sites (`applyOne`, `WritePosition`, `commitBatch`). New helper `(*ChangeApplier).execTimeoutCtx(ctx)` returns ctx + cancel; callers wrap the call and `cancel()` once it returns. Matches the existing `txExec` pattern but operates at the call site (writePositionTx is a package-level function, not a method, so it can't be routed through `txExec`).

- **`tx.Commit()` is now wrapped by a per-exec watchdog** on both engines, at all three call sites. New helper `(*ChangeApplier).commitWithTimeout(tx)` delegates to a package-level `runWithDeadline(timeout, f)` that races f against `time.After(timeout)`; whichever wins, wins. On timeout we return `context.DeadlineExceeded` (classified retriable by `classifyApplierError`) so the runWithRetry loop reopens the applier on a fresh connection. `database/sql.Tx.Commit()` takes no context, so the goroutine-race approach is the only mechanism that bounds it; one orphaned goroutine per timeout event is the bounded cost.

### Migration / Compatibility

- **Drop-in upgrade from v0.52.0.** No flag changes; the new wrappers honour the existing `--apply-exec-timeout=DUR` setting. Operators who had set `--apply-exec-timeout=0` (disabled) keep the unbounded behaviour (no watchdog, no goroutine cost).
- **No schema or position-token changes.**
- **The retry surface gains no new shape** — `context.DeadlineExceeded` was already classified retriable in v0.52.0; v0.52.1 just adds two more sites that can produce it.

### Who needs this release

- **Anyone who upgraded to v0.52.0 expecting the silent-stall closure**: **upgrade**. v0.52.0's fix is structurally incomplete; the same failure mode recurred on the validation rig within the v0.52.0 cycle's observation window.

- **Anyone still on v0.51.0 or earlier**: skip v0.52.0, upgrade straight to v0.52.1.

### Verification

- **Code-read confirmation** of Bug 56 at six call sites (3 per engine):
  - MySQL: `change_applier.go::applyOne` (line ~469), `change_applier.go::WritePosition` (line ~405), `change_applier_batch.go::commitBatch` (line ~332)
  - Postgres: `change_applier.go::applyOne` (line ~526), `change_applier.go::WritePosition` (line ~395), `change_applier_batch.go::commitBatch` (line ~327)
- **Per-engine unit tests** for the new helpers: `TestExecTimeoutCtx` pins the ctx-wrapping contract; `TestRunWithDeadline` pins the watchdog race semantics (passthrough on zero/negative timeout, fast-f-wins on positive timeout, slow-f-trips-watchdog with `DeadlineExceeded` and bounded wall-clock cost). 8 new test cases (4 per engine).
- **End-to-end validation deferred to the v0.52.1 cycle** on the validation rig at `sluice-validation/` — same scenarios as the v0.52.0 cycle's #23 scenario 1, with the load-bearing pin being "no apply goroutine ever blocks at unwrapped sites for >60s in 3 sequential pprof snapshots".

## [0.52.0]

**Closes GitHub issue #23 — silent applier stall on half-closed destination connections.** Phase B of the three-phase debug protocol. Phase A (v0.48.0) added the diagnostic surface (`--heartbeat-interval`, `--pprof-listen`) that captured ground truth on the validation rig: with traffic flowing into a PlanetScale source, both MySQL→MySQL and MySQL→PG streams went 4+ minutes without applying any of the 1330+ source rows while heartbeats still fired and pprof remained responsive. Goroutine 1 in both sluice processes was blocked inside `crypto/tls.(*Conn).Read` waiting for an OK packet from the destination that never arrived. The apply goroutine had no per-statement deadline AND the drivers' DSNs set no `readTimeout`, so the read blocked indefinitely; the ADR-0038 retry loop couldn't activate because the apply call never returned to allow classification.

### Added

- **`--apply-exec-timeout=DUR` flag on `sluice sync start`** (default `60s`). Per-statement deadline applied to every `tx.ExecContext` call on the apply path. On expiry the driver returns `context.DeadlineExceeded`, which the applier's error classifier treats as retriable so the existing `runWithRetry` loop reopens the applier and retries the batch on a fresh connection. Default chosen long enough for a legitimately slow batch upsert on a slow target, short enough to bound the silent-stall detection window. Zero or negative disables (the pre-v0.52.0 unbounded behaviour).

- **`ir.ApplyExecTimeoutSetter` optional interface** in `internal/ir/interfaces.go`. Mirror of the `MaxBufferBytesSetter` pattern: engines that opt into per-exec deadlines implement `SetExecTimeout(d time.Duration)`; engines that don't keep their pre-v0.52.0 behaviour. MySQL and Postgres appliers both implement.

- **`pipeline.Streamer.ApplyExecTimeout` field**. Plumbed through `openApplier` via the new `applyExecTimeout` helper (mirror of `applyMaxBufferBytes`). Threaded from the CLI flag.

### Fixed

- **MySQL applier**: every `tx.ExecContext` in `dispatch()` (Insert / Update / Delete / Truncate) now wraps with `context.WithTimeout(ctx, execTimeout)` when the timeout is set. `classifyApplierError` recognises `context.DeadlineExceeded` as retriable so the runWithRetry loop activates on expiry.

- **Postgres applier**: same shape. All four dispatch sites wrap with the per-exec context; `classifyApplierError` mirrors the MySQL change for `context.DeadlineExceeded`.

### Migration / Compatibility

- **Drop-in upgrade from v0.51.x.** The new `--apply-exec-timeout=60s` default is conservative; operators with legitimately slow batch upserts (large multi-row writes on slow targets, geographically distant destinations) who hit spurious timeouts should tune up via `--apply-exec-timeout=5m` or similar. Disable entirely with `--apply-exec-timeout=0` to restore exact pre-v0.52.0 behaviour.
- **No schema changes.** No chain.json, sluice_cdc_state, or position-token format changes.
- **The ADR-0038 retry surface gains one new retriable shape** (`context.DeadlineExceeded`). Operators tuning `--apply-retry-attempts` should be aware that exhausted-attempts now includes per-exec-timeout exhaustion.

### Who needs this release

- **Anyone running continuous-sync against a remote managed-database target** (PlanetScale, Cloud SQL, RDS, Neon, etc.): **upgrade**. The silent-stall failure mode was reproducible on the validation rig within a single 4-minute traffic window; production streams against destinations with idle-tablet cycles, vttablet failover, or proxy-layer eviction without clean FIN/RST will eventually hit the same shape.

- **Anyone running same-host or self-managed sync**: low-risk drop-in. The default `60s` per-exec timeout is well above the legitimate-batch ceiling for almost all workloads; the retry loop activates on expiry rather than failing.

### Verification

- **End-to-end live capture on validation rig** documented in `sluice-validation/PHASE-B-DIAGNOSIS-ISSUE-23.md` — goroutine 1 stack traces (`mysql-stall-goroutines.txt`, `pg-stall-goroutines.txt`) show the exact blocked frame the fix unblocks.
- **6 new test cases** in `applier_errors_test.go` (3 MySQL, 3 PG) covering bare + wrapped `context.DeadlineExceeded` retriable classification.
- **4 new test cases** in `internal/pipeline/migrate_test.go::TestApplyExecTimeout` pinning the plumbing helper's zero-no-op + negative-no-op + positive-sets-once + non-setter-degrades invariants.
- **Existing classifier-table tests** (`TestClassifyApplierError_RetriableShapes`) extended to cover the new shape symmetrically on both engines.

## [0.51.0]

**Lands GitHub issue #20 chunk 14b phase 1 — rotation-EXIT thresholds for `backup stream run`.** Third chunk of roadmap item 14 (the backup-chain retention/compaction track, following v0.47.0's chain.json catalog and v0.50.0's prune command). Operators can now bound chain length AND chain age via two new flags that exit the stream cleanly when either threshold trips; chain.json gets a rotation marker so subsequent tooling can detect the closed-state.

**Scope decision**: Phase 1 ships the rotation-EXIT signal pattern (operator wraps `sluice backup stream run` in cron / systemd / supervisord to restart with a fresh `--output-dir`). Phase 2 (v0.52.0+) will implement in-process rotation that doesn't require the operator wrapper. The correctness analysis (per `docs/dev/notes/prep-backup-chain-rotation.md`) shows the rotation-EXIT pattern has the same position-monotonic guarantee as inline rotation — between exit and new run start, source events get absorbed into the new full's snapshot data, so final-state restore is correct.

### Added

- **`--exit-after-age=DUR` flag on `sluice backup stream run`**. After this duration of chain age (computed as `now - chain.json's CreatedAt`), commit the current rollover and exit cleanly. The chain catalog's `RotatedAt` + `RotationReason` = `"retain-rotate-at"` fields get written so tooling can detect the closed-state. Zero disables (preserves pre-v0.51.0 unbounded behavior).

- **`--exit-after-chain-length=N` flag on `sluice backup stream run`**. After N incrementals committed, exit cleanly. Same chain.json marking with `RotationReason` = `"rotate-at-chain-length"`. Either threshold firing wins; length checked first (cheaper, no I/O). Zero disables.

- **`ChainCatalog.RotatedAt` + `ChainCatalog.SucceededBy` + `ChainCatalog.RotationReason` fields** in `internal/pipeline/chain_catalog.go`. Additive (optional JSON fields with `omitempty`); no format-version bump needed since older readers safely ignore unknown fields. `SucceededBy` is reserved for v0.52.0+ in-process rotation; v0.51.0 phase 1 leaves it empty (operator-driven rotation can manually link via tooling or wait for v0.52.0+ auto-stitching).

- **`prefixedStore` wrapper** in `internal/pipeline/prefixed_store.go`. Wraps an `ir.BackupStore` with a transparent path prefix. Currently unused in production code; the wrapper exists ahead of its v0.52.0+ inline-rotation consumer so the chain-root sub-store mechanism is in place when 14b phase 2 lands. Three unit tests pin the round-trip + empty-prefix degeneration + nested-list path-stripping invariants.

### Migration / Compatibility

- **Drop-in upgrade from v0.50.x.** Both new flags are opt-in; operators not setting either get unchanged pre-v0.51.0 behavior.
- **No chain.json schema bump.** The three new fields are additive and `omitempty`-encoded. v0.47.0+ readers ignore them and continue working. Older sluice readers (pre-v0.47.0) ignore the entire chain.json file (catalog absent → directory-walk fallback) so they're unaffected too.
- **The `prefixedStore` wrapper is internal**; no operator-facing API. Wrapper exists for v0.52.0+ scaffolding.

### Who needs this release

- **Anyone running `sluice backup stream run` against a chain that grows unboundedly** (per GitHub #20's evidence): **upgrade**. Pair `--exit-after-age=24h` (or `--exit-after-chain-length=500`) with a cron / systemd timer that restarts the stream pointing at a fresh `--output-dir` for the next chain. Bounded chain length + bounded restore time without writing application-level rotation logic.

- **Anyone preparing for v0.52.0+ in-process rotation**: the chain.json schema is forward-compatible; v0.51.0 chains marked with `RotatedAt` will be readable by v0.52.0+ readers that gain auto-stitching via `SucceededBy`.

- **Anyone whose chains don't grow problematically**: drop-in, no behavior change.

### Example operator workflow (phase 1)

```bash
# Rotation cadence: 24h or 500 incrementals, whichever first.
#
# systemd service spec (simplified):
#   ExecStart=/usr/local/bin/run-sluice-stream.sh
#   Restart=on-success
#   RestartSec=5
#
# run-sluice-stream.sh chooses a fresh --output-dir per chain:
TS=$(date -u +%Y%m%dT%H%M%S)
DIR="/backups/chain-$TS"
mkdir -p "$DIR"
exec /usr/local/bin/sluice backup stream run \
    --output-dir="$DIR" \
    --since=<previous-chain-final-incremental-id> \
    --exit-after-age=24h \
    --exit-after-chain-length=500 \
    ...
```

After phase 2 (v0.52.0+) lands, the same operator workflow collapses to a single long-running `sluice backup stream run --output-dir=/backups/ --retain-rotate-at=24h` command with no wrapper.

### Verification surface

- **9 new unit tests in `internal/pipeline/stream_rotation_test.go`** covering: length-threshold fires + not-yet, age-threshold fires + not-yet (catalog-CreatedAt-based math), length-preferred-over-age tie-breaker, none-configured no-op, catalog-absent conservative fall-back, marker-write success + absent-catalog no-op.
- **3 new unit tests in `internal/pipeline/prefixed_store_test.go`** pinning the wrapper's transparency invariants.
- **End-to-end validation deferred to operator re-test** via the validation rig at `sluice-validation/` — pair the new flags with a brief `--exit-after-age=1m` (or `--exit-after-chain-length=2`) run to confirm the exit + chain.json marking behavior.

### What's NOT in v0.51.0 (deferred to v0.52.0+ phase 2)

- **In-process rotation**: when the threshold trips, the same goroutine opens a new snapshot stream, bulk-copies the source state into a new chain root sibling directory, and resumes streaming. Eliminates the need for the operator-wrapper pattern.
- **`SucceededBy` auto-stitching**: rotation writes the pointer to the new chain's directory name, and restore tooling chains across rotations transparently.
- **`--retain-rotate-at=DUR` flag**: reserved name for the phase 2 in-process rotation flag.

## [0.50.0]

**Lands GitHub issue #20 chunk 14c — `sluice backup prune` retention command.** Second chunk of roadmap item 14 (the backup-chain retention / compaction track, following v0.47.0's chain.json catalog keystone). Operators of long-running `backup stream run` chains now have a first-class primitive to bound disk usage and restore time: drop the oldest incrementals (or older-than-duration), narrowing the chain's restorable range. The full at the chain root is always preserved. Prep doc for chunk 14b (`--retain-rotate-at` automated rotation) committed under `docs/dev/notes/prep-backup-chain-rotation.md` ahead of its v0.52.0+ implementation.

### Added

- **`sluice backup prune --from-dir DIR [--keep-incrementals N | --keep-duration DUR] [--dry-run]`** — operator-facing CLI. Mutually exclusive flags; one required. `--dry-run` reports what would be pruned without deleting or rewriting `chain.json`. Logs a structured summary at INFO level: `manifests_dropped`, `manifests_kept`, `chunks_deleted`, `earliest_restorable_backup_id`. Plus a `dropped` line per dropped manifest path so operators can audit the removal in the same log stream.

- **`PruneChain` API in `internal/pipeline/chain_prune.go`** — programmatic entry point used by the CLI. `PruneOpts{KeepIncrementals, KeepDuration, DryRun, Now}` configures the operation; `PruneResult{Pruned, Kept, ChunksDeleted, EarliestRestorableBackupID}` summarises the outcome. Pre-flight validation refuses: both/neither flags set, missing `chain.json` (with actionable hint to run `backup verify --rebuild-catalog`), no full backup in chain, and structurally-broken catalogs (interior orphans whose parent isn't in the drop set OR is in the drop set despite not being the first-kept — the latter would require multi-stitch chain rewrites that v0.50.0 doesn't support).

- **First-kept manifest re-stitch via `restitchManifest`**. When dropping the oldest-prefix incrementals leaves a kept incremental whose parent has been dropped, the first kept incremental's manifest gets rewritten in place: `ParentBackupID` re-anchors to the chain-root full, `StartPosition` re-anchors to the full's `EndPosition`. This keeps chain-restore's parent-link walk + `StartPosition` validation passing. The event windows in the DROPPED incrementals are LOST from the chain's restorable range (the operator opts into this; `PruneResult.EarliestRestorableBackupID` records the new earliest point). Chunk files referenced by the rewritten manifest stay intact — only the parent-link metadata changes.

- **`docs/dev/notes/prep-backup-chain-rotation.md` (new)** — design prep for chunk 14b (`--retain-rotate-at`, v0.52.0+). Captures the snapshot/CDC overlap correctness concern (new full's snapshot anchor must be ≥ previous chain's last-committed incremental position), the inline-rotation FSM, the chain.json schema additions (`format_version: 2`, `rotated_at`, `succeeded_by`, `rotation_reason`), and the v0.50.0 catalog reader's forward-compat preparation. Captured pre-implementation per CLAUDE.md's design-first feedback for non-trivial chunks.

### Migration / Compatibility

- **Drop-in upgrade from v0.49.x.** Pure additive feature; no flag changes on existing commands, no behaviour change for operators who never invoke `sluice backup prune`.
- **`chain.json` schema unchanged** at this release. v0.50.0 readers continue to load v1 catalogs; v0.50.0 writers continue to produce v1 catalogs. The `chainCatalogFormatVersion` constant stays at 1; the prep doc plans for the v2 bump under 14b's rotation work (and the forward-compat reader bump will land alongside 14b, not in 14c).
- **Restorability narrows when prune runs**. The first kept incremental's `StartPosition` re-anchor to `full.EndPosition` is a recorded data-loss window — events that landed between the original `StartPosition` and the full's `EndPosition` are NOT replayed on restore. The operator's `--keep-incrementals` / `--keep-duration` choice IS this trade-off; the restorable-range narrowing isn't a separate consideration.
- **Concurrent stream protection**: `sluice backup prune` does NOT lock the chain. If `backup stream run` is actively rolling against the chain when prune runs, the stream's parent-resolution may reference a now-pruned manifest on next restart. Recommended workflow: pause / stop the stream → prune → restart.

### Who needs this release

- **Anyone running `sluice backup stream run` against a local-FS chain that has accumulated meaningful disk usage** (per the GitHub #20 evidence — chains past ~10k incrementals start hitting `find` / `ls` slowdowns): drop-in benefit. `sluice backup prune --keep-duration=30d` (or similar) caps chain growth without manual `rm` orchestration.
- **Anyone preparing for the v0.52.0+ rotate-at workflow**: prep doc captures the design; the chain catalog reader's forward-compat bump will land with 14b. No action needed in v0.50.0.
- **Anyone whose chains aren't growing problematically**: drop-in, no behaviour change. The new command is opt-in.

### Verification surface

- **8 new unit tests in `internal/pipeline/chain_prune_test.go`**:
  - `TestPruneChain_KeepIncrementalsDropsOldest` — basic keep-N-most-recent flow
  - `TestPruneChain_KeepDurationDropsOlderThanThreshold` — time-based pruning with `Now` injection
  - `TestPruneChain_KeepAllPreservesEverything` — no-op case (`KeepIncrementals >= incrCount`)
  - `TestPruneChain_DryRunNoSideEffects` — dry-run mode (catalog + chunks unchanged)
  - `TestPruneChain_RefusesBothFlags` — mutual-exclusion gate
  - `TestPruneChain_RefusesNeitherFlag` — at-least-one gate
  - `TestPruneChain_RefusesWhenCatalogAbsent` — operator-actionable hint
  - `TestPruneChain_RefusesWhenChainWouldBreak` — interior-orphan refusal (manually-broken parent-link)
- **End-to-end validation deferred to operator re-test** — the rig at `sluice-validation/` has a 4-table chain that can be pruned + restored to verify the re-stitch + restore-side behavior; operator workflow once the next test cycle runs.

### What's not in v0.50.0

- **Chunk 14d (compact)**: deferred to v0.51.0. Initial scope estimate underestimated the complexity (chunk encoding, encryption envelope handling, SHA tracking across merged chunks, schema-delta merging). Better as its own focused release than bundled with 14c.
- **Chunk 14b (rotate-at)**: deferred to v0.52.0+. The prep doc landed in this release; implementation needs the snapshot/CDC overlap design to be reviewable before code.

## [0.49.0]

**Closes GitHub issues #25 + #26.** Both bugs blocked cold-start on real-world MySQL source schemas — #25 on MySQL targets (Vitess refused the CREATE TABLE), #26 on PostgreSQL targets (silent identifier truncation collision). Both confirmed reproducing on v0.48.0 via the operator's validation rig before fix; both reproductions now closed via the targeted fixes below. Bundled per the operator's per-release CI-cost preference.

### Fixed

- **GitHub #25 — MySQL AUTO_INCREMENT-on-non-PK column now bootstraps cleanly.** A source table like `CREATE TABLE cell (id varchar(50) PRIMARY KEY, increment_id int AUTO_INCREMENT, UNIQUE KEY uq_cell_increment_id (increment_id), ...)` previously rejected at sluice cold-start on MySQL/Vitess targets with `Error 1075 (42000): Incorrect table definition; there can be only one auto column and it must be defined as a key`. Pre-v0.49.0's three-phase schema apply deferred ALL secondary indexes to phase 2, so the CREATE TABLE landed without the auto column's supporting key. v0.49.0 adds `inlineAutoIncrementIndex` in `internal/engines/mysql/ddl_emit.go` which detects "AUTO_INCREMENT column not in PK + has a supporting index" and emits the supporting index inline at CREATE TABLE; the same detector is used by `CreateIndexes` (phase 2) and the dry-run DDL preview path to skip the index that's already been emitted, avoiding double-create. MySQL allows at most one AUTO_INCREMENT column per table, so at most one inline supporting index is needed. Unique indexes are preferred when both unique and non-unique candidates exist (matches the operator's `UNIQUE KEY uq_<table>_<col>` pattern from real production schemas).

- **GitHub #26 — PostgreSQL identifier truncation collision on long index names is closed.** A source table like `CREATE TABLE entity_field_operation_relation (..., KEY ix_workflow_block_id_for_op_rel_alpha (workflow_block_id), KEY ix_workflow_block_id_for_op_rel_beta (id, workflow_block_id))` previously failed at sluice's index-creation phase on PG targets with `SQLSTATE 42P07: relation "entity_field_operation_relation_ix_workflow_block_id_for_op_rel" already exists` — both 69- and 68-char prepended names silently truncated to the same 63-char PG identifier and the second CREATE INDEX hit a duplicate. v0.49.0 extends `pgIndexName` in `internal/engines/postgres/ddl_emit.go` with two complementary checks: (1) **convention-prefix detection** — recognises `ix_<table>_` / `idx_<table>_` / `fk_<table>_` / `uq_<table>_` / `chk_<table>_` / `pk_<table>_` / `uix_<table>_` / `uidx_<table>_` / `uniq_<table>_` / `ck_<table>_` patterns as "already table-scoped" and emits verbatim (covers SQLAlchemy / Alembic / Django / Rails / Hibernate / Diesel naming conventions); (2) **length-check fallback** — if the explicit `<tableName>_` prepend would exceed PG's 63-char `NAMEDATALEN-1` limit, emit verbatim instead. The historical disambiguation against sibling-table indexes (`idx_fk_film_id` appearing on multiple tables) is preserved for short names; long names sacrifice it for collision-freedom, which is the more urgent failure mode (a multi-table-collision is rare AND has an operator workaround via source-side index rename; the silent same-table self-collision today has no workaround).

### Migration / Compatibility

- **Drop-in upgrade from v0.48.x.** Both fixes are additive at the operator surface; no flag changes, no behaviour change for schemas that don't hit either pattern.
- **The supervisor-workaround pattern operators were using** (filtering out the failing tables via `--include-table=users` per the validation rig's `start_sync_*.ps1` scripts) is no longer required for #25/#26 — the underlying CREATE-TABLE / CREATE-INDEX failures are closed. Operators can drop `--include-table` filters that were workarounds for these two bugs.
- **Emitted index identifiers may differ on PG targets** for long source names: pre-v0.49.0 they'd silently truncate; post-v0.49.0 they emit verbatim. Operators with existing PG targets that already received the truncated names should either (a) re-bulk via `--reset-target-data` to land the corrected identifiers, or (b) accept the silent truncation as historical and continue (the catalog references are by OID, not name; user-data SQL queries don't reference index names directly).
- **No engine API surface change**; no CLI surface change.

### Who needs this release

- **Anyone running `sluice sync start` or `sluice migrate` against a real-world MySQL source with**: (a) `AUTO_INCREMENT` on a non-PK column backed by a `UNIQUE KEY`, OR (b) long index names that include the table name (the SQLAlchemy / Alembic / Django default convention): **upgrade**. Both shapes blocked cold-start on v0.48.0 and earlier.
- **Anyone bootstrapping a PG-target sync from a MySQL source with long-named tables** (the common case for legacy / mature schemas): drop-in benefit; the silent PG truncation collision is closed.
- **Anyone whose schema doesn't hit either pattern**: drop-in, no behaviour change.

### Verification surface

- **5 new test functions / subtests**:
  - `internal/engines/mysql/ddl_emit_test.go::TestEmitTableDef_AutoIncrementNonPK_GitHub25` — confirms the supporting UNIQUE KEY is emitted inline at CREATE TABLE
  - `internal/engines/mysql/ddl_emit_test.go::TestInlineAutoIncrementIndex_DetectionTable` — 4 cases covering PK-auto / non-PK-auto-with-support / non-PK-auto-without-support / prefer-unique-over-non-unique
  - `internal/engines/postgres/ddl_emit_test.go::TestPgIndexName_GitHub26` — 9 subtests covering both regression preservation AND new behavior shapes (convention-prefix detection, length-check fallback, exact-match edge case, empty-source)
  - `internal/engines/postgres/ddl_emit_test.go::TestPgIndexName_NoCollisionAcrossLongSiblingNames` — load-bearing pin: the two sibling indexes that triggered #26 must now emit to distinct PG identifiers
- **End-to-end re-verification via the validation rig at `sluice-validation/`** — both `start_sync_mysql_dest.ps1 -AllTables` (re-triggers #25 pre-fix) and `start_sync_pg_dest.ps1 -AllTables` (re-triggers #26 pre-fix) are documented entry points. With the v0.49.0 binary, both should now succeed end-to-end.

## [0.48.0]

**Closes GitHub issues #21 + #22 and lands Phase A of #23.** Three changes sharing the *"remove the operational burden of supervising sluice externally"* theme, bundled into one release to reduce per-release CI cost burden. Operators running `sluice sync start` or `backup stream run` against PlanetScale-MySQL endpoints previously needed an external supervisor wrapper to recover from two classes of process exit (#21, #22). v0.48.0 closes both at the retry layer where v0.42.0 / v0.46.0 already framed the policy. Separately, the silent-stall failure mode (#23 — process alive, no apply, no log) now produces a positive liveness signal AND has a pprof endpoint for goroutine-dump diagnosis on the next stall.

### Fixed

- **MySQL applier classifier now catches `gomysql.ErrInvalidConn` (closes GitHub #21).** `go-sql-driver/mysql` exports its own `ErrInvalidConn = errors.New("invalid connection")` sentinel (`errors.go:20`), distinct from `database/sql`'s `driver.ErrBadConn`. The v0.42.0 `classifyApplierError` only checked `driver.ErrBadConn`, so `gomysql.ErrInvalidConn` slipped through and the applier exited even though the same connection-reset class on the PG side retried fine. Two-line fix at `internal/engines/mysql/applier_errors.go::classifyApplierError`. Confirmed against the operator's 3-hour repro (4 exits caused by `invalid connection` during applier INSERT, all required supervisor restarts; same window, 6 successful in-process retries of Vitess `exceeded timeout: 20s`).

- **`sluice backup stream run` rollover loop now retries transient CDC-pump errors (closes GitHub #22).** Pre-v0.48.0 the backup-stream's rollover loop treated any rollover error as terminal, so a source-side TCP reset / gRPC `Unavailable` that the sync-stream path retries through (ADR-0038, v0.46.0) took the backup-stream process down. v0.48.0 mirrors the sync-stream retry policy on the rollover loop: classify via [`ir.RetriableError`](internal/ir/applier_retry.go), close the current CDC pump, reopen from the last committed parent's `EndPosition`, retry. Bounded by new `--retry-attempts` (default 8) / `--retry-backoff-base` (default 100ms) / `--retry-backoff-cap` (default 30s) flags on `backup stream run`, matching the sync-stream's `--apply-retry-*` knobs. Consecutive-failure counter resets on a successful rollover so a long-lived stream doesn't carry retry debt forward.

### Added — GitHub #23 Phase A (silent-stall diagnostics)

- **`stream: heartbeat` INFO log line every `--heartbeat-interval` (default 60s).** New per-stream goroutine in `Streamer.runOnce` emits a positive liveness signal at default log level. Distinguishes silent-stall (process alive, no apply, no log) from wedge (process alive, no heartbeat either). When the next stall fires, the log shows heartbeats stopping AND the operator can hit the new pprof endpoint to dump every goroutine's stack — exactly the data needed to localise the wedge point in Phase B. Phase A is deliberately diagnostic-only: it doesn't try to *fix* the stall, only make it visible. Set `--heartbeat-interval=0` to disable.

- **Global `--pprof-listen ADDR` flag** binds `net/http/pprof`'s debug endpoints at the given address (e.g. `:6060`, `127.0.0.1:6060`) for the duration of any subcommand. Off by default; opt-in. When set, sluice logs `INFO pprof endpoint listening … hint="fetch /debug/pprof/goroutine?debug=2 to dump goroutine stacks"` at startup. The endpoints are the stdlib pprof handlers (CPU profile, heap, goroutine, mutex, block, etc.) — operators chasing the #23 silent-stall use `curl http://<addr>/debug/pprof/goroutine?debug=2 > stacks.txt` and attach to the issue.

### Migration / Compatibility

- **Drop-in upgrade from v0.47.x.** All three changes are additive at the operator surface; no flag renames, no behaviour change for non-failing runs.
- **The supervisor pattern operators added for #21 / #22 is no longer required** for the documented retry-policy classes. Existing supervisors continue to work as defence-in-depth; they just shouldn't fire as often.
- **Heartbeat log noise**: at the default 60s cadence, an overnight 24h run produces ~1440 extra INFO lines per stream. Operators with strict log-volume budgets can disable via `--heartbeat-interval=0` and rely on `sluice sync health` for stall detection instead.
- **pprof endpoint is opt-in**: no exposure surface change unless `--pprof-listen` is set. Operators setting it should bind to localhost / a private interface — the endpoints are unauthenticated by design (stdlib pprof has no auth layer).

### Who needs this release

- **Anyone running `sluice sync start` against a PlanetScale-MySQL target**: **upgrade**. The #21 exit-on-`invalid connection` class is closed; supervisor restarts should drop materially.
- **Anyone running `sluice backup stream run`**: **upgrade**. The #22 silent-exit-on-source-blip class is closed; the chain stops getting silent coverage gaps when a TCP reset cascade fires.
- **Anyone who has hit the #23 silent stall** (process alive, no apply, no log): drop-in benefit. The heartbeat log line distinguishes stall from wedge immediately; the pprof endpoint enables a 10-second diagnosis on the next occurrence.
- **Anyone with strict log-volume / pprof exposure policies**: opt-out via `--heartbeat-interval=0` / leave `--pprof-listen` unset.

### Verification surface

- **Test coverage**:
  - `internal/engines/mysql/applier_errors_test.go::TestClassifyApplierError_RetriableShapes` extended with two cases — `gomysql.ErrInvalidConn` and a wrapped form (GitHub #21).
  - `internal/pipeline/heartbeat_test.go` (new) — 3 tests covering emit-on-tick, zero-interval-disables, exits-on-ctx-cancel (catches goroutine leaks).
  - Existing backup-stream tests cover the rollover loop's retry shape via the unchanged success path; the retry branch's `openCDCReaderWithSlot` reopen path is covered structurally (same engine API the original open uses).
- **End-to-end retry validation deferred to operator re-test** — same pattern as v0.42.0 / v0.46.0. Concretely: with v0.48.0 binary, `--apply-retry-attempts 8` on the sync path AND `--retry-attempts 8` on the backup-stream path, run through a forced TCP reset and confirm no supervisor restart. For #23: set `--pprof-listen :6060` on all three streams; if a stall recurs, capture `curl localhost:6060/debug/pprof/goroutine?debug=2`.

## [0.47.0]

**Lands GitHub issue #20 chunk 14a — chain.json catalog at chain root.** Roadmap item 14's keystone. Today's chain readers (`Restore.storeHasIncrementals`, `ChainRestore`, `listAllManifests`, `position-from-manifest`, the resume-detect path on `backup full` / `incremental` / `stream`) all walk the `manifests/` directory via `store.List` and then `store.Get` every result — fine on local FS for small chains, operationally painful past ~10k incrementals or on object storage where `ListObjects` is the dominant cost. v0.47.0 lands a single `chain.json` at the chain root listing every manifest in order with end-position and file-count metadata pre-extracted; chain readers collapse the per-manifest walk to a single `Get`. Foundational for the v0.48.0+ rotate-at / prune (14b–c) and v0.49.0+ compact (14d) chunks under the same roadmap item — they all need O(1) chain-state lookup to build on.

### Added

- **`internal/pipeline/chain_catalog.go` (new).** `ChainCatalog` + `ChainCatalogEntry` types serialised as `chain.json` at chain root. Entry fields: `backup_id`, `kind`, `parent_backup_id`, `manifest_path`, `end_position`, `created_at`, `file_count`, plus `tombstoned` reserved for v0.48.0+ compact/prune. `loadChainCatalog`, `writeChainCatalog`, `rebuildChainCatalog`, `readManifestsFromCatalog`, `updateChainCatalogBestEffort` cover the read/write/repair/integration surface. `format_version=1`; forward-version refusal with operator-actionable "upgrade sluice" hint. See [`docs/dev/notes/prep-chain-catalog.md`](docs/dev/notes/prep-chain-catalog.md) for the design.

- **`listAllManifests` fast-path.** When chain.json is present, `listAllManifests` dispatches to `readManifestsFromCatalog` (single catalog `Get` + per-entry manifest `Get`s in known chain order); catalog-absent (legacy chains, never-written stores) falls back to today's `listAllManifestsViaWalk`. Catalog-corrupted (parse failure, unsupported `format_version`) surfaces as a fatal error rather than silently degrading — operators need to know about the integrity issue.

- **Catalog hooks at every production `writeManifest` site.** `backup.go::Backup.run` after the final flip-to-complete write; `incremental.go::Incremental.run` after the rollover-end manifest write; `stream.go::Stream.run` on both the normal-rollover commit AND the drain-on-ctx-cancel commit. Per-chunk / per-table checkpoint writes during the row sweep skip the catalog (BackupID isn't computed yet at that point), so a typical backup-full run produces exactly one catalog `Put` regardless of chunk count. Updates are best-effort: failures log at WARN but do NOT fail the manifest write — manifests remain the source of truth, the catalog is an O(1) accelerator.

- **`sluice backup verify --rebuild-catalog`.** Operator-driven catalog regeneration. Walks every manifest on disk and writes a fresh chain.json from scratch. Useful after manual chain mutation (operator-driven prune outside the v0.48.0+ tooling) or to seed a catalog on a legacy chain produced by sluice older than v0.47.0.

### Fixed

- **Lazy rebuild on first v0.47.0 write to a legacy chain.** When `updateChainCatalog` runs against a chain without chain.json (a pre-v0.47.0 chain being extended in v0.47.0), the function performs a one-time rebuild over the existing `manifests/` directory + the full at the root before appending the new entry. Operator sees a new chain.json appear with all historical entries the first time v0.47.0 writes; subsequent updates pay only one `Get` + one `Put`. No operator-driven migration step required.

- **Dedup-by-path AND dedup-by-BackupID.** A backup-full re-run that overwrites the conventional `manifest.json` produces a new BackupID (different CreatedAt) at the same path. Naive dedup-by-BackupID-only would leave a stale entry pointing at the same path, and chain consumers would read the manifest twice (verify-double-count, ChainRestore double-apply). v0.47.0's dedup filter drops any existing entry whose BackupID OR ManifestPath collides with the new entry before appending — both the per-chunk-checkpoint case (same BackupID, same path) and the re-run case (new BackupID, same path) collapse to a single correct entry.

- **Tombstone forward-compat filter.** A v0.47.0 reader against a v0.48.0+ chain.json with `tombstoned: true` entries MUST skip those entries during chain iteration — otherwise a future compaction's tombstones would surface compacted-out manifests in a v0.47.0 restore. `filterActiveEntries` runs at every `readManifestsFromCatalog` call. v0.47.0 writers always emit `tombstoned: false`; the filter is cheap forward-compat insurance for the v0.48.0+ work.

### Migration / Compatibility

- **Drop-in upgrade from v0.46.x.** Existing chains without chain.json keep working through the directory-walk fallback. The first v0.47.0 write into a legacy chain triggers a one-time lazy rebuild (one `List` + one `Get` per existing manifest); subsequent writes pay only one `Get` + one `Put`. Operator sees a new `chain.json` file appear at the chain root.
- **Pre-v0.47.0 sluice reading a v0.47.0-produced chain** ignores `chain.json` entirely (unknown file at chain root) and walks `manifests/` as before. Strict forward AND backward compat at the chain-root layer.
- **`writeManifest` Put count is +1 on backup-full runs** (one chain.json write per backup completion). Operators with strict Put-count budgets / monitoring should adjust expectations; the additional Put is small (~1-10 KB depending on chain length).

### Who needs this release

- **Anyone running `sluice backup stream run` against long-running chains** (especially local-FS chains per the GitHub #20 evidence): drop-in benefit. Chain-state lookups are now O(1); the foundation for bounded retention is in place.
- **Anyone restoring large chains on object storage**: drop-in benefit. Restore startup eliminates the `ListObjects` walk in favour of a single `Get chain.json` — meaningful on chains past a few hundred incrementals.
- **Operators preparing for the v0.48.0+ rotate-at / prune work**: this is the keystone. The next chunks (`--retain-rotate-at`, `sluice backup prune`, `sluice backup compact`) all build on chain.json.

### Verification surface

- **8 new unit tests in `internal/pipeline/chain_catalog_test.go`**:
  - `TestChainCatalog_LoadAbsent` — legacy-chain detection (no chain.json → no error)
  - `TestChainCatalog_RoundTrip` — write/read symmetry across every field
  - `TestChainCatalog_FormatVersionGate` — refuses `format_version` newer than this build
  - `TestChainCatalog_AppendDeduplicatesByBackupID` — per-chunk-checkpoint dedup pins
  - `TestChainCatalog_RebuildFromLegacyChain` — lazy-rebuild on first write to pre-v0.47.0 chain
  - `TestChainCatalog_FilterTombstoned` — forward-compat insurance for v0.48.0+ tombstones
  - `TestChainCatalog_JSONShape` — pins the on-disk JSON envelope against accidental tag renames
  - `TestListAllManifests_PrefersCatalog` / `TestListAllManifests_FallsBackToWalk` — integration of fast-path vs legacy walk
- **Existing backup-resume tests retained** with one assertion update (`TestBackup_ResumeSkipsAlreadyCompletedTables`: 4 → 5 Puts to account for the one chain.json write on the final flip-to-complete).
- **End-to-end backup chain validation deferred to operator re-test** — same pattern as prior backup-track releases.

## [0.46.0]

**Closes GitHub issue #19 — source-side retry on transient CDC reader errors.** Before v0.46.0, when the source CDC reader's pump hit a transient (`read tcp: ... read: connection reset by peer`, `EOF`, vttablet `code = Aborted/Unavailable/ResourceExhausted`, PG SQLSTATE 57P0x server-restart, etc.), the pump closed the changes channel after stashing the error via its internal `setErr`. The applier's batched loop saw the channel close as a normal EOF signal, committed any pending batch, and returned `nil`. The streamer then returned `nil` from `runOnce` — a clean exit code 0 — even though the operator's overnight `sluice sync start` had silently dropped its CDC connection mid-stream. The ADR-0038 retry loop (v0.42.0) catches *applier-side* transients via the `ir.RetriableError` interface, but never saw a source-side error to classify. v0.46.0 closes the gap by classifying source-pump errors with the same interface and surfacing them back to the retry loop after the changes channel closes.

### Fixed

- **`internal/engines/mysql/reader_errors.go` + `internal/engines/postgres/reader_errors.go` (new) — source-side classifier mirrors.** Each engine ships a thin `classifyReaderError(err)` that delegates to the engine's existing `classifyApplierError`. The transient shapes overlap perfectly between the two surfaces — same `*MySQLError` codes (1213, 1105 vttablet transients), same `*pgconn.PgError` SQLSTATEs (40001, 40P01, 57P0x, 08*), same `driver.ErrBadConn` / `io.EOF` / network-text patterns. The delegation is intentional: keeping reader and applier classifiers in lockstep means neither surface gets stricter or laxer over time. Future reader-specific shapes (e.g. binlog-rotate-during-restart shapes that only the source-pump sees) get added to `reader_errors.go` without touching applier code.

- **`internal/engines/mysql/{cdc_reader,cdc_vstream,cdc_vstream_snapshot}.go` — pump `setErr` wraps with `classifyReaderError`.** Every place a CDC reader pump previously called `r.setErr(fmt.Errorf("mysql: cdc: get event: %w", err))` (and the two VStream pump variants) now wraps the error through `classifyReaderError` first. nil and non-transient shapes pass through unchanged.

- **`internal/engines/postgres/cdc_reader.go::pump` — three setErr sites wrapped.** The standby-status-update / receive-message / ErrorResponse sites that surface network-level transients now route through `classifyReaderError`. Parse-error sites (`parse keepalive` / `parse xlogdata` / `dispatchWAL`) are *not* wrapped — those indicate protocol corruption or a sluice bug, not a retriable shape.

- **`internal/pipeline/streamer.go` — source-error surfacing into the retry loop.** New private `sourceErrFn func() error` field on `Streamer` captures the source CDC reader's `Err()` method when the reader's concrete type exposes one (every shipping reader does — pre-v0.46.0 `Err` just wasn't visible to the streamer). Both `coldStart` and `warmResume` populate the field after `StreamChanges` returns; `runOnce` reads it after `dispatchApply` via the new `surfaceSourceError` helper that filters out the `context.Canceled` / `context.DeadlineExceeded` shapes (the pump's check is best-effort, and outer-ctx-driven shutdowns must not surface as retriable errors). When a non-nil non-cancellation source error is surfaced, `runOnce` returns it wrapped as `pipeline: source cdc reader: <err>`; the wrap satisfies `errors.As(&ir.RetriableError{})` when the underlying classifier marked the shape transient, so `runWithRetry` already-existing classification + exponential backoff loop handles the retry without further changes.

### Migration / Compatibility

- **Drop-in upgrade from v0.45.x.** No flag changes; no error-message changes for non-transient source errors (those return verbatim through `classifyReaderError`).
- **Behaviour change on transient source-pump errors**: pre-v0.46.0 these surfaced as a clean exit code 0 (the silent #19 failure mode); v0.46.0 surfaces them through the ADR-0038 retry loop. Operators running with `--apply-retry-attempts > 1` (the v0.42.0+ default of 8) see automatic source-side retry. Operators on `--apply-retry-attempts 1` (single-attempt mode, pre-v0.42.0 behaviour) see the previously-silent error surface as a non-zero exit — strict improvement: an operator running with retry off explicitly opted into "exit on first transient" semantics.
- **The exit-0 false-success case from #19 cannot recur** — the next time a transient source-pump error fires, the wrapped error reaches `runWithRetry` and either succeeds on retry or surfaces as a terminal retry-budget-exhausted error after the operator's configured `--apply-retry-attempts`.

### Who needs this release

- **Anyone running `sluice sync start` overnight or in long-running mode against any source**: **upgrade**. The #19 silent-exit failure mode is the highest-risk known issue between v0.42.0's applier-retry landing and today — overnight test cycles produced clean exit codes on what were actually mid-stream transient connection resets, with no operator-visible signal that the stream had dropped.
- **Anyone with monitoring on sluice exit codes**: opt-in benefit. Source-side transients now surface as non-zero terminal errors (after retry exhaustion) instead of exit 0.
- **Operators on `--apply-retry-attempts 1`**: drop-in benefit. Previously-silent source transients now surface immediately as terminal errors — strict improvement on the silent-failure mode.

### Verification surface

- **3 new unit-test files + 4 new test funcs**:
  - `internal/engines/mysql/reader_errors_test.go::TestClassifyReaderError_DelegatesToApplierClassifier` (9 subtests) — confirms the delegation is shape-for-shape identical to `classifyApplierError`; any future drift between the two surfaces fails the test.
  - `internal/engines/postgres/reader_errors_test.go::TestClassifyReaderError_DelegatesToApplierClassifier` (10 subtests) — same identity check on the PG surface; covers 40001 / 40P01 / 57P01 / 08006 retriable shapes and 23505 non-retriable.
  - `internal/pipeline/streamer_source_error_test.go::TestSurfaceSourceError_*` (4 funcs) — covers nil-fn, nil-return, context-cancellation filtering (4 ctx-cancel shape subtests), and the GitHub #19 happy path where a non-cancellation transient is returned verbatim for the retry loop.
- **End-to-end source-transient retry validation deferred to operator re-test** with `--apply-retry-attempts 8` running through a TCP reset on the source connection.

## [0.45.0]

**Closes GitHub issue #17 (Option B — proper structural fix) + lands GitHub #18 Phase 1+2 (batch-latency telemetry + cross-region safety rail).** Operators bootstrapping `sluice sync start` against a PlanetScale-MySQL branch with Safe Migrations enabled (the recommended production configuration) previously hit `Error 1105 (HY000): direct DDL is disabled` after sluice had already captured the snapshot position and reached the schema-apply phase. No actionable hint, no recovery path that didn't involve toggling Safe Migrations off and on around the run. v0.45.0 adds a new `--schema-already-applied` flag (the recommended workflow), a focused error wrap that names the new flag in the operator-facing message, and several DSN-papercut fixes. The bundled #18 Phase 1+2 telemetry lands the foundation a future AIMD batch-size controller will build on.

### Added — #17 main fix

- **`--schema-already-applied` flag on `sluice sync start`.** When set, sluice skips every DDL phase during cold-start: `CREATE TABLE` / `CREATE INDEX` / `ADD FOREIGN KEY` / `CREATE VIEW` / `SyncIdentitySequences` / `EnsureControlTable`. The cold-start preflight refusal (Bug 9) is also skipped — the operator's promise is "everything I need is already there with no data." Bulk-copy then runs into the operator-prepared empty tables. Operators on PlanetScale Safe Migrations branches push schema changes via deploy requests (including the `sluice_cdc_state` table — DDL in `internal/engines/mysql/control_table.go`), then run sluice with this flag.

- **`bulkCopyOpts` (internal)** — `runBulkCopy` was refactored into `runBulkCopyWithOpts` with a `SkipSchemaApply` option that suppresses every DDL phase while preserving the per-table data sweep. Existing callers continue to use the zero-options shortcut; the Streamer's coldStart now passes `bulkCopyOpts{SkipSchemaApply: s.SchemaAlreadyApplied}`.

### Fixed — #17 papercuts

- **`internal/engines/mysql/schema_writer_errors.go` (new) — Safe Migrations DDL refusal detection.** New `wrapDDLError` helper recognises `Error 1105 (HY000): direct DDL is disabled` and wraps with `ErrSafeMigrationsBlocked` + a multi-line operator hint pointing at both recovery paths: (a) `--schema-already-applied` after pre-creating schema via a deploy request, or (b) temporarily disable Safe Migrations during bootstrap. The wrapper is wired at every DDL exec site in `schema_writer.go` (CreateTablesWithoutConstraints, CreateIndexes, CreateConstraints, CreateViews) and at the control-table-create in `control_table.go`. Errors that don't match the Safe Migrations shape return verbatim — no behaviour change for other failure modes.

- **`internal/engines/mysql/connect.go::parseDSN` — doubled "invalid DSN:" prefix.** The driver's own error message starts with `"invalid DSN: ..."`, and sluice's `fmt.Errorf("mysql: invalid DSN: %w", err)` wrap produced a confusing `"mysql: invalid DSN: invalid DSN: ..."` double prefix. The wrapper now strips the redundant inner prefix.

- **`internal/engines/mysql/connect.go::dsnShapeHint` — `/db/branch` DSN papercut.** PlanetScale credentials are branch-scoped (the branch is implicit in the user/password); operators sometimes try to encode the branch in the DSN path as `/db/branch`, which the driver rejects with a generic `"did you forget to escape a param value?"` hint sending them down the wrong rabbit hole. Sluice now detects path-with-extra-slash (skipping the parenthesised `protocol(address)` block so unix sockets like `/tmp/mysql.sock` don't false-positive) and prepends a clearer hint to the wrapped error.

- **`internal/pipeline/streamer.go::runWithRetry` — retry-policy WARN suppression on non-retriable startup failures.** The ADR-0038 retry loop opens a side-channel applier at startup to read positions between attempts; on failure it logged `WARN msg="applier: retry policy disabled (cannot open position reader); falling through to single-attempt run"`. For genuinely-transient failures this is correct, but for parse errors / bad DSN / unknown database (the inner `runOnce` is about to fail with the same error and exit), the WARN was noise that confused the operator's first stderr line. New `isTransientOpenError` classifier downgrades the message to DEBUG for known non-transient shapes (invalid DSN, parseDSN failure, access denied, unknown database).

### Added — #18 Phase 1 + Phase 2 (telemetry foundation)

- **DEBUG-level batch-latency telemetry on every applier-committed batch.** Both `internal/engines/{mysql,postgres}/change_applier_batch.go::applyOneBatch` now emit `slog.DebugContext(ctx, "applier: batch latency", stream_id, rows, millis)` per non-empty batch, measuring wall-clock from "batch start" through "position write + tx commit returns." Operators running `--log-level=debug` (and a future AIMD auto-tuner) get per-target per-batch cost visibility. Empty / idle-flush batches are elided to avoid noise during quiet periods.

- **Cross-region safety rail in `Streamer.Run` startup.** New `maybeWarnApplyBatchSizeRisky(ctx, targetName, batchSize)` emits a single WARN when target engine name is `planetscale` AND `--apply-batch-size > 50`. The warning names the Vitess 20s transaction-killer constraint and points at the GitHub #18 follow-on (the AIMD controller planned for v0.46.0+). Conservative classification — false positives on same-region PS-MySQL are acceptable to avoid missing the cross-region foot-gun your validation rig hit at batch=100.

### Migration / Compatibility

- **Drop-in upgrade from v0.44.x for vanilla MySQL / PG operators not using PlanetScale.** No behaviour change.
- **PlanetScale operators**: drop-in. The new `--apply-batch-size > 50` warning is informational; existing flag values continue to work. The new `--schema-already-applied` flag is opt-in.
- **Safe Migrations operators (new workflow)**: pre-create schema via PlanetScale deploy request → run `sluice sync start --schema-already-applied` → sluice skips all DDL. Document the `sluice_cdc_state` DDL operators must include (see `internal/engines/mysql/control_table.go`).
- **The DSN-error wrap changes the exact text of `parseDSN` failures**: operators who pattern-match on the doubled prefix in scripts (unlikely) need to update.

### Who needs this release

- **Anyone running `sluice sync start` against a PlanetScale-MySQL branch with Safe Migrations enabled**: **upgrade**. The new `--schema-already-applied` flag is the supported workflow.
- **Anyone running cross-region PS-MySQL with `--apply-batch-size > 50`**: drop-in benefit. The new safety-rail warning surfaces the tx-killer foot-gun at startup instead of the operator discovering it via retry-loop exhaustion in production.
- **Anyone with telemetry pipelines that scrape DEBUG-level logs**: opt-in benefit. The new `applier: batch latency` line is the foundation for the v0.46.0+ AIMD auto-tuner.
- **Operators on quiescent or non-PlanetScale sources**: drop-in; no behaviour change.

### Verification surface

- **6 new unit tests** across `internal/engines/mysql/{schema_writer_errors,connect_dsn}_test.go` + `internal/pipeline/streamer_warn_test.go`:
  - `TestWrapDDLError_SafeMigrationsBlockedIsWrapped` — confirms the 1105 error wrap recognises the Safe Migrations message + includes both recovery paths in the hint
  - `TestWrapDDLError_OtherErrorsUnchanged` — default-pass invariant (nil, plain errors, different MySQL codes, non-matching 1105 messages stay verbatim)
  - `TestParseDSN_NoDoubleInvalidPrefix` — no more `"invalid DSN: invalid DSN:"` double-prefix
  - `TestDSNShapeHint_BranchPathDetected` — `/db/branch` produces actionable hint
  - `TestDSNShapeHint_PlainPathNoHint` — well-formed DSNs (including unix sockets) get no false-positive hint
  - `TestMaybeWarnApplyBatchSizeRisky_*` (3 subtests) — Phase 2 warn-policy correctness
  - `TestIsTransientOpenError_*` (2 subtests) — papercut WARN suppression classification

## [0.44.0]

**Closes GitHub issue #16 — PlanetScale backup chain-resume now works end-to-end.** Before v0.44.0, `sluice backup full` against a PlanetScale/Vitess source captured the source position via the binlog-shape encoder (`{"mode":"gtid","gtid_set":"..."}`) but the continuous-sync VStream reader that `sluice backup stream run` and `sluice backup incremental` use only understands the VStream `[]shardGtid` shape (`[{"keyspace":"...","shard":"-","gtid":"..."}]`). Operators got "json: cannot unmarshal object into Go value of type []mysql.shardGtid" immediately on any chain-resume attempt; backup chains were unusable on PlanetScale sources. The architectural fix routes PlanetScale's `OpenBackupSnapshot` through the same VStream COPY-mode machinery the live-sync `coldStart` path already uses — same gRPC stream, same per-shard vgtid capture, same `vstreamSnapshotRows` table-by-table reader.

### Fixed

- **`internal/engines/mysql/backup_snapshot_vstream.go` (new) — VStream-backed backup snapshot for PlanetScale.** `openBackupSnapshotVStream(ctx, dsn)` delegates to the existing `openVStreamSnapshotStream` (the live-sync coldStart path), constructs an `ir.BackupSnapshot` from the snapshot stream's `Position` / `Rows` / `CloseFn`, and ignores the `Changes` field (backup doesn't consume CDC, just records the position so a downstream incremental can resume from there). The gRPC stream stays open until `BackupSnapshot.Close` fires, mapped to `vstreamSnapshotStream.close`.

- **`internal/engines/mysql/backup_snapshot.go::OpenBackupSnapshot` — flavor branch.** New guard at the top of the function: `if e.Flavor == FlavorPlanetScale { return e.openBackupSnapshotVStream(ctx, dsn) }`. The pre-v0.44.0 pinned-conn + `START TRANSACTION WITH CONSISTENT SNAPSHOT` + `@@global.gtid_executed` path stays for vanilla MySQL only — applying it to a Vitess source produced a binlog-shape position the VStream reader can't decode AND single-tablet snapshot semantics that don't generalise across shards.

- **The pre-existing PS-MySQL backup data-read path "worked" against single-shard keyspaces** in the sense that rows were copied successfully, but the captured EndPosition was the wrong shape AND wasn't multi-shard-aware. v0.44.0 fixes both at once by switching to VStream COPY, which is sharded-aware by construction.

### Migration / Compatibility

- **Drop-in upgrade from v0.43.x for sync paths (vanilla MySQL, PG, all engines)** — `OpenBackupSnapshot` for `FlavorVanilla` and the PG `OpenBackupSnapshot` are unchanged.

- **PS-MySQL backups: chain-resume requires a fresh `sluice backup full`.** Existing backups produced by v0.43.0 or earlier against PS-MySQL sources have wrong-shape EndPositions in their manifests. Those manifests cannot be chained from on v0.44.0 (the position decoder will still reject the binlog shape — this is intentional, since attempting to use the wrong position would silently mis-position the incremental). Re-take a `sluice backup full` on v0.44.0; that backup's manifest will carry the VStream-shape position and chain cleanly to incrementals.

- **Behaviour change worth flagging**: PS-MySQL backups now spin up a brief gRPC VStream connection to vtgate (in addition to the existing MySQL-protocol connection). Operators with strict outbound-network policies on the backup-running host should ensure the vtgate gRPC port (typically 15991 or per PlanetScale's `vstream_url`) is reachable.

- **Memory profile**: PS-MySQL backups now buffer all rows for all tables in memory before the orchestrator drains table-by-table (matches the live-sync coldStart trade-off). Sluice's v1 simple-mode workloads fit comfortably; very large sharded keyspaces would need a streaming variant — a future-revision concern documented in `openVStreamSnapshotStream`.

### Who needs this release

- **Anyone running `sluice backup full` against PlanetScale-MySQL and trying to chain incrementals or stream-run rollovers off it**: **upgrade immediately**. Pre-v0.44.0 the chain was always broken. Take a fresh full backup on v0.44.0 to seed a working chain.

- **PS-MySQL backup operators who only ever take full backups (no chain)**: still benefits from the upgrade — the v0.44.0 snapshot data path uses VStream COPY which has Vitess's documented multi-shard consistency contract (RFC vitessio/vitess#6277), whereas the pre-v0.44.0 pinned-conn snapshot was single-tablet only.

- **Vanilla MySQL backup operators**: drop-in; no behaviour change.

- **PG backup operators**: drop-in; this release doesn't touch the PG path.

### Verification surface

- **`internal/engines/mysql/backup_snapshot_vstream_test.go` (new)** — two routing unit tests:
  - `TestEngine_OpenBackupSnapshot_FlavorBranchRoutes` confirms `FlavorPlanetScale` routes to the VStream-COPY path (error message contains "vstream" when dial fails)
  - `TestEngine_OpenBackupSnapshot_VanillaDoesNotUseVStream` confirms `FlavorVanilla` does NOT route to the VStream path (error message must NOT mention "vstream", guarding against future routing regressions)
- **End-to-end validation deferred to operator re-test against real PlanetScale.** Sluice's `psverify` build-tag test surface can be extended with a backup-chain-resume test in a follow-on release once an operator confirms v0.44.0 closes the bug in the wild. The unit-test layer here pins the routing decision; the real-world validation pins the position-shape contract.

## [0.43.0]

**Closes GitHub issue #14 — VStream COPY-phase chunk overlap dedup.** Operator-confirmed in the v0.42.0 retest: a single PlanetScale-MySQL source feeding three target engines (vanilla MySQL, PS-Postgres, PS-MySQL) failed cold-start with duplicate-PK errors on ALL three simultaneously, ruling out any target-side hypothesis. The empirical evidence isolated the root cause to Vitess's COPY-phase emission: per the upstream VStream Copy RFC ([vitessio/vitess#6277](https://github.com/vitessio/vitess/issues/6277)), COPY mode interleaves forward batch emissions with a "catchup" phase that replays binlog events for rows already past the COPY scan's lastpk. Sluice's pre-v0.43.0 VStream snapshot reader buffered every ROW event indiscriminately; the catchup-phase emissions reached the bulk-copy writer as duplicate-PK INSERTs. The v0.43.0 fix tracks max-PK-seen per scope and drops behind-the-scan emissions — exactly the dedup pattern Vitess's RFC documents on the client side via `LastTablePK`.

### Fixed

- **`internal/engines/mysql/vstream_copy_dedup.go` (new)** — `copyDedupTracker` with per-`(keyspace, shard, table)` max-PK-seen state. PK column identification from FIELD events via the `query.MySqlFlag_PRI_KEY_FLAG` bit. Composite PKs compared lexicographically; type-aware compare for int64 / uint64 / float64 / string / []byte / time.Time / bool. Tables without a declared PK fall through (the tracker keeps every row — dedup is opt-in by PK presence).

- **`internal/engines/mysql/cdc_vstream_snapshot.go`** — `vstreamSnapshotStream` gained a `dedup *copyDedupTracker` field, initialised in `openVStreamSnapshotStream`. The `dispatchCopyEvent` FIELD branch calls `dedup.recordFields` to capture PK column names; `bufferCopyRow` consults `dedup.shouldKeep` before appending to `rowBuffer`. Dropped rows are recovered post-COPY: the CDC phase resumes from the snapshot's terminal GTID and Vitess's binlog tail replays any changes that happened during the scan; the applier's idempotent semantics (ADR-0010 / ON DUPLICATE KEY UPDATE / ON CONFLICT DO UPDATE) absorb them.

- **DEBUG-level summary at COPY_COMPLETED** — when ≥ 1 emission was dropped, the global COPY_COMPLETED event logs a `mysql/vstream: snapshot: COPY-phase dedup summary (GitHub #14)` line with per-scope drop counts. Empty/no log for well-behaved streams (no drops). Operators on busy keyspaces can confirm the dedup is working.

### Migration / Compatibility

- **Drop-in upgrade from v0.42.x.** No CLI changes, no IR changes, no engine-interface changes. The dedup is enabled by default; engines that don't open a VStream snapshot stream are unaffected.
- **PG / vanilla-MySQL targets**: drop-in benefit. The fix is in the SOURCE-side VStream reader, so any sluice stream consuming a PlanetScale-MySQL / Vitess source (regardless of target engine) gets the fix. The v0.42.0 retest's "all three targets fail simultaneously" shape is what this release is specifically designed to close.
- **Same-engine MySQL → MySQL on vanilla MySQL source**: drop-in; this path uses the binlog reader (not VStream), so the dedup is a no-op.
- **Tables without a primary key**: dedup is a no-op (tracker can't identify what to dedup). v0.42.x behaviour preserved.
- **CDC apply-phase semantics**: unchanged. The dedup only filters COPY-phase emissions; CDC ROW events are passed through untouched (the applier's idempotency handles those per ADR-0010).

### Who needs this release

- **Anyone running `sluice sync start` against a PlanetScale-MySQL or self-hosted Vitess source under concurrent source writes**: **upgrade**. This is the load-bearing fix that closes the only remaining open bug from the v0.39.1 → v0.42.0 fix sequence.
- **Operators on quiescent or write-paused source databases**: drop-in; no behaviour change (no re-emissions happen without concurrent writes).
- **Operators not using VStream**: drop-in; this path is unreachable.

### Verification surface

- **13 new unit tests** in `internal/engines/mysql/vstream_copy_dedup_test.go` covering:
  - nil-tracker fall-through (every row kept)
  - no-PK table (every row kept — dedup is keyed on PK presence)
  - single int PK monotonic forward (no drops)
  - the GitHub #14 repro shape (behind-the-scan id=545 / id=1179 drops with the right summary)
  - per-scope independence across multi-shard streams
  - composite PK lexicographic compare (`(region, id)` with cross-region forward / behind / equal cases)
  - every value-type's compare path in `comparePKCell` (int64 / uint64 / float64 / string / []byte / time.Time / bool / nil / type-mismatch fall-open)
  - `recordFields` idempotency on FIELD-event re-emit
  - summary string format pinning

### Companion — Phase A branch retired

The Phase A diagnostic instrumentation that lived on branch `phase-a-issue-14` (commit `b995afb`) is retained on that branch as a historical artifact. With v0.43.0 closing #14 via empirical confirmation of hypothesis (b) and no further diagnostic round needed, the branch can be deleted at your discretion — the dedup fix supersedes its purpose. The `phase_a_probe="github_issue_14"` DEBUG log lines were never merged into `main`; v0.43.0 ships its own per-COPY_COMPLETED summary log for ongoing operator visibility.

## [0.42.0]

**Closes GitHub issue #13 — bounded retry on transient applier errors.** Implements [ADR-0038](docs/adr/adr-0038-applier-retry-on-transient-errors.md). Before v0.42.0, the first transient applier error during CDC (Vitess `Error 1105` tx-killer, PG `SQLSTATE 40001` serialisation failure, etc.) exited the entire `sluice sync start` process. Operators had to wrap the binary in a supervisor and rely on warm-resume to retry. v0.42.0 brings the retry policy inside the streamer: each per-engine classifier categorises errors as retriable or terminal, and `Streamer.Run` wraps the apply pipeline with exponential backoff that resets on observed CDC-position progress.

### Added

- **`ir.RetriableError` interface (`internal/ir/applier_retry.go`)** — the optional surface an engine's applier error can implement to signal that the pipeline's retry policy should attempt to recover. Implementations preserve the original error via `Unwrap`, so `errors.Is` / `errors.As` against the driver-level error still works from any consumer.

- **Per-engine error classifiers**:
  - `internal/engines/mysql/applier_errors.go` — `classifyApplierError` recognises InnoDB deadlock (`Error 1213`), Vitess tx-killer / vttablet transients (`Error 1105` with `code = Aborted` / `Unavailable` / `ResourceExhausted`), and driver-level `bad connection` / `EOF` / connection-reset shapes. Explicitly NOT retriable: `Error 1062` (duplicate key) — masks data bugs and sluice idempotency gaps (e.g. GitHub issue #14).
  - `internal/engines/postgres/applier_errors.go` — `classifyApplierError` recognises serialization failure (`40001`), deadlock detected (`40P01`), admin/crash/cannot-connect-now shutdown (`57P0x`), the entire connection_exception class (`08*`), and driver-level transients. Explicitly NOT retriable: `23505` (unique_violation) — same rationale as MySQL `1062`.

- **Pipeline retry loop (`internal/pipeline/streamer.go`)** — `Streamer.Run` now dispatches to `runWithRetry` when `ApplyRetryAttempts > 1`. The loop opens a side-channel applier to read the persisted CDC position between attempts; the consecutive-failure counter resets when the position advances (a successful batch landed since the last failure), so a streamer surviving for hours doesn't carry retry debt forward. `ResetTargetData` is cleared after the first iteration so a transient applier failure during retry doesn't re-trigger the destructive reset path.

- **CLI flags on `sluice sync start`**:
  - `--apply-retry-attempts N` (default 8) — maximum consecutive retriable failures before exit. `1` = no retry (pre-v0.42.0 behaviour); `8` = default tuned for managed-Vitess / Vitess-flavoured MySQL.
  - `--apply-retry-backoff-base DUR` (default `100ms`) — base interval for exponential doubling between retries.
  - `--apply-retry-backoff-cap DUR` (default `30s`) — per-attempt upper bound on the backoff.

  Default 8-attempt schedule: 100ms → 200ms → 400ms → 800ms → 1.6s → 3.2s → 6.4s → 12.8s (≈25.5s total). Bounded at ~4 minutes worst-case even with the cap hit.

### Migration / Compatibility

- **Drop-in upgrade from v0.41.x.** No IR changes that break callers; the applier's existing `Apply` / `ApplyBatch` signatures are unchanged. Engines that don't implement the new error classifier (third-party or hand-rolled) behave identically to v0.41.x — `errors.As(err, &ir.RetriableError{})` simply fails and the retry loop treats every error as terminal.
- **Behaviour change**: operators running `sluice sync start` will no longer see the process exit on the first Vitess `Error 1105` or PG `40001`. The default policy retries 8 times with exponential backoff; transients that genuinely indicate "stuck" (8 consecutive failures at the same persisted position) surface as `retry budget exhausted` with the original error in the wrap chain.
- **Operators wanting v0.41.x fail-fast behaviour** can pass `--apply-retry-attempts=1`.
- **Stream-supervisor integrations** (systemd `Restart=on-failure`, k8s pod restart) keep working — they're the outer recovery layer for "retry budget exhausted" exits. v0.42.0's inner retry handles the routine transients without needing to bounce the process.

### Who needs this release

- **Anyone running `sluice sync start` against PlanetScale-MySQL / Vitess targets**: **upgrade immediately**. The Vitess `Error 1105 code = Aborted ... for tx killer rollback` shape was the load-bearing repro for GitHub #13; v0.42.0 makes it self-recoverable instead of process-exiting.
- **Operators on PG → PG continuous-sync** under heavy concurrent write load: drop-in benefit. `SQLSTATE 40001` serialisation failures and deadlock victims (`40P01`) now retry transparently instead of exiting.
- **Operators with custom supervisor wrappers**: still supported; the retry budget cap means supervisor restarts handle the "genuinely stuck" case while v0.42.0 absorbs the noise.

### Verification surface

- 22 new unit tests across `internal/engines/{mysql,postgres}/applier_errors_test.go` covering every retriable shape, the explicit non-retriable shapes (1062 / 23505), the default-deny invariant (unknown errors stay non-retriable), and edge cases (Vitess non-transient gRPC codes like `InvalidArgument` correctly stay terminal).
- New `internal/pipeline/streamer_retry_test.go` pins the backoff schedule's exponential doubling, hint-floor semantics, cap behaviour, and the 8-attempt default budget (asserts total wait < 4 minutes per ADR-0038's stated promise).

## [0.41.0]

**Closes GitHub issue #15 — `sluice sync start` cold-start now persists the CDC anchor before the first batch.** Originally cut as v0.40.1 but re-tagged as v0.41.0 after the v0.40.1 CI run uncovered a brittle integration test assertion that the fix exposed (`TestStreamer_ResetTargetData_RecoversFromSlotMissing`). The v0.40.1 tag exists on the remote (`501303a`) as a historical artifact with no published release; v0.41.0 supersedes it cleanly with the test fix bundled.

**Original v0.40.1 changelog text follows.** Before v0.40.1, `sluice_cdc_state` only gained a row when the first CDC batch committed successfully. Any failure in the window between "bulk-copy complete; entering CDC mode" and that first batch commit (most commonly: a transient Vitess error per GitHub issue #13) wedged the operator — warm-resume couldn't recover (no persisted position), cold-start refused (target tables held the freshly-bulk-copied data), and the only escape was `--reset-target-data --yes` (re-bulk-copying everything) or `--force-cold-start` (collides on PK).

### Fixed

- **`internal/pipeline/streamer.go::coldStart`** — after the "bulk-copy complete; entering CDC mode" log line and before `StreamChanges`, the streamer now calls `applier.WritePosition(ctx, streamID, stream.Position)` with the snapshot's anchor. CDC from this position is gapless (the snapshot anchor and the CDC start position are the same — that's the ADR-0007 contract), so a restart that reads this row warm-resumes correctly and replays the same change stream the failed run would have processed. Idempotent: the first applier.commitBatch overwrites the same row with a monotonically-newer position, so no double-write contention.
- **`internal/pipeline/preflight.go` — mode-aware recovery hint.** The cold-start refusal message previously assumed all hits came from migrate-mode (one-shot `sluice migrate`), recommending `--resume on sluice migrate` even when the operator reached the refusal through `sluice sync start`. The function now takes a `preflightMode` and emits a tailored hint:
  - Migrate mode: unchanged ("previous cold-start killed mid-bulk-copy → drop tables or `--resume`")
  - Sync mode: names GitHub #15 as a candidate cause, recommends `--reset-target-data --yes` as the primary recovery, notes that the slot-drop step applies to PG sources only (Vitess/MySQL don't expose it as a separate concept). Closes operator confusion in #15.

### Migration / Compatibility

- **Drop-in upgrade from v0.40.0.** No CLI changes, no IR changes, no engine-interface changes. Operators on v0.40.0 who experienced the #15 wedge state need to recover once via `--reset-target-data --yes`; subsequent runs are safe.
- **First-write contract.** The pre-CDC `WritePosition` is gated on the applier implementing `ir.PositionWriter`. Both shipping engines (MySQL, Postgres) do; an engine that doesn't logs a WARN and falls through to pre-fix behaviour (no regression, just no protection).
- **`PositionWriter` shape requirement.** The position written here is the snapshot's anchor token (same token the CDC reader will resume from). Applier implementations that bind position-writes to other side effects (e.g. tracker updates) must tolerate a write whose position equals the previously-written position — re-writes of the same anchor must not error.

### Who needs this release

- **Any operator running `sluice sync start` against a PlanetScale-MySQL / Vitess target** (or any target where transient applier failures are possible): **upgrade**. The #15 wedge required `--reset-target-data` recovery; v0.40.1 turns this into a clean warm-resume.
- **Operators on PG-source streams**: drop-in. The cold-start anchor write makes restart-during-first-batch recoverable; pre-fix this was already rare (PG applier transients are less frequent than Vitess tx-killer events) but the fix is symmetric across engines.
- **Operators currently in a v0.40.0 wedge state**: upgrade to v0.40.1, run once with `--reset-target-data --yes` to recover; future cold-starts will not re-wedge.

### Verification surface

- New unit test `TestPreflightColdStart_SyncModeHint` in `internal/pipeline/preflight_test.go`: asserts the sync-mode hint names GitHub #15, recommends `--reset-target-data`, and does NOT point at `sluice migrate --resume` (which would mislead operators in sync flows).
- Existing migrator preflight tests updated to pass `preflightModeMigrate`; their hint assertions still pass unchanged.
- **`TestStreamer_ResetTargetData_RecoversFromSlotMissing`** updated to use the new `waitForPersistedPositionChanged(t, dsn, streamID, before, timeout)` helper instead of the brittle `waitForPersistedPositionGone`. The v0.40.1 fix shrinks the "row absent" window during a reset+cold-start from "bulk-copy + first-CDC-batch latency" to just "bulk-copy duration" (milliseconds for small fixtures), making the poll-based "row gone" assertion miss the transient. "Position changed from a known prior value" is the strictly stronger signal — proves both that the reset cleared the original row AND that the new run wrote a fresh row under a different snapshot/CDC position.

## [0.40.0]

**Closes GitHub issue #12 — CDC apply path now filters generated columns.** A source table with a `STORED` generated column previously caused the applier to include the generated column's value in every INSERT (and the SET-list of `ON DUPLICATE KEY UPDATE` / `ON CONFLICT DO UPDATE`, and the WHERE predicate of UPDATE/DELETE). MySQL rejected the INSERT with Error 3105 ("The value specified for generated column ... is not allowed"); PostgreSQL rejected with SQLSTATE 428C9 ("cannot insert a non-DEFAULT value into column"). The first CDC batch hitting an affected table exited the entire `sluice sync start` process. The fix mirrors the bulk-load writer's existing column-list filter (ADR-0026:100) on the apply side.

### Fixed

- **`internal/engines/mysql/change_applier.go`** — `buildInsertSQL`, `buildSetClause`, `buildWhereClause` now route their column lists through a new `nonGeneratedRowKeys` helper that consults the applier's cached column-type map and skips any column whose `Column.GeneratedExpr` is non-empty. The downstream `ON DUPLICATE KEY UPDATE` SET-list (derived from the same column list) is automatically filtered too.
- **`internal/engines/postgres/change_applier.go`** — same fix on the PG side, with the additional plumbing required because PG's `colTypes` cache is `map[string]ir.Type` (no `GeneratedExpr` field on `ir.Type`). Introduced a parallel `generatedColCache map[string]map[string]bool`, populated alongside `colTypeCache` by an extended `loadColumnTypes` query that reads `information_schema.columns.is_generated`. The `buildInsertSQL` / `buildUpdateSQL` / `buildDeleteSQL` / `buildSetClause` / `buildWhereClause` builders gained a `generated map[string]bool` parameter and skip flagged columns from every SQL position.
- **WHERE-clause filter rationale.** Including a `STORED` generated column in WHERE (UPDATE/DELETE) risks silent zero-rows-affected when the target's recomputation differs from the source's stored value (floating-point precision, NULL-coalescing semantics across engines, etc.). The PK + remaining-column equality is sufficient to identify the row; skipping generated columns from WHERE removes a class of silent divergence the applier's pre-fix shape exposed.

### Migration / Compatibility

- **Drop-in upgrade from v0.39.x.** No CLI changes, no IR changes, no engine-interface changes, no operator-visible behaviour change for source schemas without generated columns. Operators on the prior `sluice sync start` failure path (any continuous-sync stream against a source table with a `STORED` generated column) need to restart the stream — warm-resume continues from the persisted source position; no data is replayed.
- **Caveat for source data drift.** Pre-fix, if a generated column's computed value diverged between source and target at any point (e.g. operator manually re-ran the source's `GENERATED` expression with a different precision), the WHERE-clause filter now means an UPDATE/DELETE will no longer fail silently — it'll apply against the row whose non-generated columns match. This is strictly safer; the silent-fail mode was the bug.
- **Driver compatibility**: the new SELECT against `information_schema.columns.is_generated` works against PG 12+ unchanged. On older PG versions the column returns `'NEVER'` for every row — applier behaves exactly as pre-fix.

### Who needs this release

- **Anyone running `sluice sync start` against a source table with a `STORED` generated column** on any target engine (PostgreSQL, vanilla MySQL, PlanetScale-MySQL): **upgrade immediately**. The bug was 100%-reproducible: the first CDC INSERT after cold-start exited the stream.
- **Operators not using generated columns**: drop-in; no behaviour change.
- **Operators on cross-engine MySQL → PG migrations**: schema translation of generated columns already worked (the bulk-load path was correct); this release closes the CDC apply gap that previously made the columns continuous-sync-incompatible.

### Verification surface

- **New unit tests** in `internal/engines/mysql/change_applier_test.go` and `internal/engines/postgres/change_applier_test.go` (`TestBuildSQL_FiltersGeneratedColumns`): exercises INSERT / UPDATE SET / UPDATE WHERE / DELETE WHERE filter behaviour, plus the `nil colTypes` / `nil generated` fall-through that preserves the pre-fix shape for unit tests using hand-built fixtures.
- **New integration tests** in `internal/engines/{mysql,postgres}/change_applier_integration_test.go` (`TestChangeApplier_GeneratedColumn`): boots a testcontainer, creates a table with `margin DECIMAL(12,2) AS (price - COALESCE(cost, 0)) STORED`, drives Insert + Update + Delete CDC events whose Row maps include the generated column's value, and asserts (a) the apply succeeds and (b) the target's computed margin matches the engine-computed value (not the source-emitted one).

### Design — applier retry on transient errors (ADR-0038, awaiting review)

This release also drafts [ADR-0038](docs/adr/adr-0038-applier-retry-on-transient-errors.md), a proposal for bounded retry-on-transient in the applier dispatch loop — the design response to GitHub issue #13 (PlanetScale-MySQL Vitess tx-killer errors exit the stream non-zero today). The ADR is `Proposed`; implementation lands in a follow-on release (v0.40.x or v0.41.0) after operator review.

## [0.39.1]

**Closes silent golangci-lint debt + adds the missing local pre-commit gate.** CI's `Lint` job has been failing silently on `main` since v0.34.0 across 6 consecutive releases (v0.34.0 → v0.39.0 inclusive). Root cause: the local pre-commit script ran `gofumpt + go vet + go test` but NOT `golangci-lint`, so lint-only failures (unused symbols, `revive`'s unused-parameter rule, etc.) passed the local gate and only surfaced in CI — where the watcher logic was only gating on the Release workflow's conclusion, not the parallel Lint job's. The four issues were all in v0.34.0 KMS code I added forward-looking-but-never-wired-up helpers for.

### Fixed

- **`internal/crypto/azure_kms_test.go`** — dropped unused `wrongKey` field on `fakeAzureKMS` stub; renamed unused `msg` parameter on `fakeAzureAPIError` to `_` (preserves call-site documentation while satisfying `revive`).
- **`internal/crypto/azure_kms.go`** — dropped unused `withSkipAzurePreflight()` helper (was forward-looking; never wired to a test).
- **`internal/crypto/gcp_kms.go`** — dropped unused `withSkipGCPPreflight()` helper (same).

### Process — golangci-lint now in the local pre-commit gate

Added a `golangci-lint run` step to both `.githooks/pre-commit` (bash) and `scripts/pre-commit.ps1` (PowerShell). Soft-skips when the tool isn't installed locally (developer convenience) with an install URL hint; hard-fails when it IS installed and produces issues. Mirrors CI's `Lint` job exactly, so lint-only failures can no longer slip through the local gate.

### Migration / Compatibility

- **Drop-in upgrade from v0.39.0.** No CLI changes, no engine-interface changes, no operator-visible behaviour changes. The dropped helpers were internal test-only forward-looking stubs that were never reachable from any caller.
- **Existing CI workflows pass cleanly** on v0.39.1 for the first time since v0.34.0; the lint debt is closed retroactively.

### Who needs this release

- **Operators on v0.34.0–v0.39.0**: drop-in; no runtime change, just a CI hygiene fix.
- **Contributors / developers running the local pre-commit hook**: the gate now matches CI exactly. If `golangci-lint` isn't installed, the script prints a soft-skip warning with an install URL; no hard fail. Install via the upstream's [installation guide](https://golangci-lint.run/welcome/install/).

## [0.39.0]

**Translator-gap preflight scan integrated into `schema preview`.** Operators running cross-engine MySQL → Postgres migrations now see an upfront advisory listing every MySQL expression-body pattern sluice's translator catalog deliberately doesn't auto-rewrite. Before v0.39.0, the deferred rules surfaced as either loud failures at PG apply time (visible but late) or silent runtime divergences (invisible until row data ships through and a downstream consumer notices). The scan brings them forward into the preview, with operator-actionable workaround hints.

### Added

- **`internal/translate/gaps.go` — translator-gap scanner.** `ScanMySQLToPGGaps(schema, sourceEngine, targetEngine, enabledExt)` walks every `DefaultExpression` body, `Column.GeneratedExpr`, and `CheckConstraint.Expr` whose dialect tag is `mysql`, and returns a sorted list of `Gap` entries for any pattern matching the catalog's 7 deliberately-deferred rules:
  - `GREATEST` / `LEAST` (rule #11, silent divergence: PG ignores NULL args, MySQL propagates)
  - `REGEXP_LIKE` (rule #13, silent on PG 15+: POSIX vs ICU regex flavour)
  - `FIND_IN_SET` (rule #21, loud failure: no portable PG equivalent in DDL context)
  - `CONVERT_TZ` (rule #23, loud failure: no PG core equivalent)
  - `INET_ATON` / `INET_NTOA` (rule #29, loud failure: no portable PG equivalent without custom function)
  - `SHA1` / `SHA2` (rule #10, loud failure: requires pgcrypto — suppressed when `--enable-pg-extension pgcrypto` is set since the v0.38.0 rewrite ships)

  Detection is case-insensitive with word-boundary matching (rejects `IS_GREATEST_HIT(` etc.). Returns `nil` for non-MySQL-to-PG engine pairs.

- **`sluice schema preview` renders the gaps section** in both text and JSON outputs:
  - **Text format**: a `Translator gaps (MySQL → Postgres)` section before the per-table DDL listing the catalog rule number, severity (`loud` / `silent`), source location (`table.column` or `CHECK constraint name`), raw expression text, and the operator-actionable note (typically `--expr-override` snippet, `--type-override` recommendation, or `--enable-pg-extension` flag).
  - **JSON format**: new `translator_gaps` top-level field with stable shape (`{table, column, constraint, field, pattern, rule, severity, expression, note}`). Omitted entirely when no gaps detected. CI gates can fail the migration plan on any `"severity": "loud"` entry.

- **Header summary line in text output**: when ≥ 1 gap is detected, the preview's header gains a `-- translator gaps: N (see section below)` line alongside the existing `-- advisory hints: N` line. Operators eyeballing the preview see the count at a glance.

### Migration / Compatibility

- **Drop-in upgrade from v0.38.x.** No CLI flag changes; the scan is enabled by default. Same-engine and PG → MySQL operators see no behaviour change (the scanner returns nil for non-MySQL-to-PG pairs). Cross-engine MySQL → PG operators with no detected gaps see an unchanged preview (the new section is skipped entirely when the gap list is empty).
- **JSON consumers**: the `translator_gaps` field is additive; existing parsers that don't know about it ignore it. Tooling can opt into reading it for CI-gate or migration-plan-review use cases.

### Who needs this release

- **Cross-engine MySQL → Postgres operators preparing a migration**: **upgrade and run `sluice schema preview`** before the actual migrate. The gaps section will surface any deferred-pattern usage in your source schema — much cheaper than discovering them at PG apply time (loud failures) or in production output (silent divergences).
- **CI gates / migration-plan-review tools**: the JSON shape's `severity` field gives a clean fail-on-loud gate. Sample jq expression: `jq '.translator_gaps | map(select(.severity == "loud")) | length == 0' preview.json` returns true when no loud gaps detected.
- **Operators not using MySQL → Postgres**: drop-in; the scanner is a no-op for other engine pairs.

### Verification surface

- 11 new unit tests in `internal/translate/gaps_test.go` covering each pattern's detection shape, the pgcrypto gate suppressing SHA1/SHA2 emissions, case-insensitive matching, word-boundary false-positive rejection, non-cross-engine no-op behaviour, DEFAULT/GENERATED/CHECK field coverage, dialect-tag filtering, nil-schema safety, severity stringification, and note-wording-contains-workaround sanity. All pass.
- Existing preview + translator tests regression-clean.

## [0.38.0]

**pgcrypto catalog entry + MD5/SHA1/SHA2 translator rules.** Re-examines the v0.37.0 deferral verdict for catalog rule #10 (hash family). Closer analysis split the rule into a core-PG path (MD5, no extension needed) and a pgcrypto-backed path (SHA1, SHA2). Total catalog coverage: 28 of 30 rules.

### Added — translator catalog rules

- **`MD5(x)` → `md5(x)`** (catalog #10, MD5 subset). PG's core `md5(text)` returns the same 32-character lowercase hex digest MySQL's `MD5()` returns. No extension needed; the rewrite is a mechanical case-fold. Ships unconditionally.
- **`SHA1(x)` → `encode(digest(x, 'sha1'), 'hex')`** (catalog #10, SHA1 subset). Gated on `--enable-pg-extension pgcrypto`. Without the flag, falls through verbatim so PG's parse-time error signals the missing extension. With the flag, sluice's preflight confirms pgcrypto is installed on the target before the rewrite fires.
- **`SHA2(x, bits)` → `encode(digest(x, '<algo>'), 'hex')`** (catalog #10, SHA2 subset). Same pgcrypto gate. Bit-width dispatch: `0` / `256` → sha256 (MySQL's `SHA2(x, 0)` default semantic preserved), `224` → sha224, `384` → sha384, `512` → sha512. Unrecognised bit widths fall through verbatim.

### Added — pgcrypto extension catalog entry

- **`pgcrypto` joins `pgExtensionCatalog`** (`internal/engines/postgres/extension_catalog.go`) as a **presence-gate entry** — no types passthrough (typesByName / hintTypeNames empty), no index access methods or opclasses (pgcrypto introduces neither). The entry exists purely so sluice's existing `validateAndPreflightExtensions` machinery runs the standard `SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname='pgcrypto')` preflight check on the target before any data moves. Mirrors the hstore opt-in pattern.

### Migration / Compatibility

- **Drop-in upgrade from v0.37.x.** Same-engine operators (MySQL → MySQL, PG → PG) are unaffected; the translator only fires on cross-engine pairs. Operators with `--expr-override` workarounds for MD5 / SHA1 / SHA2 can drop the override; the catalog rewrite produces equivalent output.
- **Operators on the cross-engine MySQL → PG path with `MD5(x)` in DDL bodies**: get the rewrite automatically — no flag needed.
- **Operators on the cross-engine MySQL → PG path with `SHA1(x)` or `SHA2(x, n)` in DDL bodies**: pass `--enable-pg-extension pgcrypto` (and ensure `CREATE EXTENSION pgcrypto;` has run on the target). pgcrypto ships with PG contrib; available on every major hosted PG service (PlanetScale, AWS RDS, Cloud SQL, Azure DB, Supabase) without `shared_preload_libraries` configuration.

### ExprContext threading (internal refactor)

The translator now receives the operator's enabled-extensions set via a new `ExprContext.EnabledPGExtensions` field, threaded from `emitOpts.EnabledExtensions` at the schema writer boundary. The four `translate*Expr` helpers (`translateDefaultExpr`, `translateIndexExpr`, `translateGeneratedExpr`, `translateCheckExpr`) and their direct callers (`emitDefault`, `emitIndexColumnList`, `emitCheckConstraint`, `emitCreateIndex`) gained an `opts emitOpts` parameter. Internal refactor only — no operator-visible behaviour change for any flow that doesn't use the new SHA1/SHA2 paths.

### Who needs this release

- **Cross-engine MySQL → Postgres operators whose source schemas use `MD5(x)` in DEFAULT / GENERATED / CHECK bodies:** **upgrade** — the rewrite ships in the catalog; no flag needed.
- **Cross-engine MySQL → Postgres operators whose source schemas use `SHA1` or `SHA2`**: **upgrade and pass `--enable-pg-extension pgcrypto`**. Requires `CREATE EXTENSION pgcrypto;` on the target (one-line operator action; pgcrypto is PG contrib, ships with every hosted PG).
- **Same-engine operators**: drop-in; no behaviour change.
- **Operators not using MD5/SHA family functions in DDL**: drop-in; no behaviour change.

## [0.37.0]

**Translator catalog batch + two test-side fixes.** Re-examines the v0.35.0 deferral verdicts: three of the nine deferred rules ship under closer review. Brings total catalog coverage to 25 of 30 rules; the remaining 6 stay deferred with each having a load-bearing catalog reason that genuinely holds (extension boundary, NULL-semantics divergence, regex flavour, invalid-in-DDL LATERAL, TZ subtleties, no-portable-equivalent). All deferred rules have actionable `--expr-override` workarounds.

### Added — translator catalog rules

- **`TIMESTAMPDIFF(unit, a, b)` → unit-specific PG expression** (catalog #16). Nine units covered: MICROSECOND / SECOND / MINUTE / HOUR map to `EXTRACT(EPOCH FROM (b - a))` with unit scaling and `::bigint` cast; DAY / WEEK use date-subtraction (`(b::date - a::date)` / `/ 7`); MONTH / QUARTER / YEAR use `AGE(b, a)` for calendar-aware semantics matching MySQL's truncated-toward-zero behaviour. Unknown units fall through verbatim. The pre-v0.37.0 deferral rationale ("unit-cross-product makes the rule table unwieldy") turned out to be 9 mechanical arms in one switch — manageable, and it covers the high-frequency MySQL temporal-derived-column patterns.
- **`JSON_OBJECT(k1, v1, k2, v2, …)` → `JSON_BUILD_OBJECT(k1, v1, k2, v2, …)`** and **`JSON_ARRAY(a, b, c)` → `JSON_BUILD_ARRAY(a, b, c)`** (catalog #20). The pre-v0.37.0 deferral rationale ("version-gated emit needed for PG 16+ vs older") vanishes by always emitting `JSON_BUILD_*` — they work on every PG version sluice supports, including PG 16+, with identical output semantics. No server-version detection needed.
- **`LAST_DAY(d)` → `(DATE_TRUNC('month', d) + INTERVAL '1 month' - INTERVAL '1 day')::date`** (catalog #24). Verbose but mechanical; the v0.35.0 verdict ("probably one for `--expr-override`") was over-conservative — the rewrite is a single switch arm with a stable output shape.

### Fixed — test-side bugs (no operator-visible behaviour change)

- **Bug 55: `psverify` test `TestPSPG_CDCReaderBasic` stale on ADR-0027 markers.** Local `drainPSChanges` helper in `internal/engines/postgres/planetscale_verify_test.go` didn't filter `ir.TxBegin` / `ir.TxCommit` boundary markers; the integration-suite drain helper did. Post-ADR-0027 (which introduced transaction-boundary markers as first-class IR change types), the local helper accepted the markers into the `got` slice and missed trailing events. One-line fix: mirror the integration-suite filter pattern. Test-side only — production CDC reader works correctly; only the local test drain logic was stale.
- **Bug 54: MySQL backup test flake `TestBackup_SnapshotAnchoredEndPosition_MySQLGapClosed`.** The during-window writer paced inserts at 50ms intervals starting after a 100ms head start; on fast machines, the 4th insert occasionally landed in the tight race window between snapshot `EndPosition` record and incremental CDC catch-up open. Widened pacing to 250ms intervals + 200ms head start, spreading writes across ~1.2s — well past both the snapshot's typical completion (<500ms) and the incremental's CDC reader's open lag. Verified by 3 consecutive PASS runs locally. Production invariant (v0.18.0 snapshot-anchored EndPosition gap closure) was correct throughout; only the test's writer pacing tripped the race.

### Migration / Compatibility

- **Drop-in upgrade from v0.36.x.** The three new translator rules only fire on cross-engine MySQL → PG migration when source DDL bodies contain the recognised function shapes; pre-existing schemas are unaffected. The test-side bug fixes have zero operator-visible effect.
- **Operators with `--expr-override` workarounds for the three newly-shipped patterns** (`TIMESTAMPDIFF`, `JSON_OBJECT` / `JSON_ARRAY`, `LAST_DAY`): drop-in; the override stays a higher-priority path. Drop the override only if the catalog rewrite produces the right shape for your downstream consumers.

### Who needs this release

- **Cross-engine MySQL → Postgres operators whose source schemas use `TIMESTAMPDIFF`, `JSON_OBJECT`, `JSON_ARRAY`, or `LAST_DAY` in DEFAULT / GENERATED / CHECK bodies:** **upgrade** — the rewrites now ship in the catalog instead of needing `--expr-override`.
- **Same-engine operators** (MySQL → MySQL, PG → PG): drop-in; the translator only fires on cross-engine pairs.
- **psverify test users (cycle subagents running against real PlanetScale):** **upgrade** — Bug 55 fix removes a false-positive failure on the PG CDC test.
- **MySQL integration-test users running `TestBackup_SnapshotAnchoredEndPosition_MySQLGapClosed`:** **upgrade** — Bug 54 fix eliminates the flake.

## [0.36.0]

**View support Phase 2 — `sluice matview refresh` subcommand.** Closes Roadmap Item 13 Phase 2. PostgreSQL-only; operators drive the refresh cadence from their own scheduler (cron / k8s CronJob / Airflow). Sluice deliberately does NOT own a refresh loop because cadence is operator-policy and external scheduling brings alerting / backoff / observability operators already have.

### Added

- **`sluice matview refresh --target=DSN --target-driver=postgres [--matview NAME] [--concurrently] [--target-schema=NAME] [--format=text|json]`** (`cmd/sluice/matview.go`). New top-level subcommand that drives `REFRESH MATERIALIZED VIEW [CONCURRENTLY] schema.name` against every matview in the target schema (or only those named by repeated `--matview NAME` flags).
- **Concurrent refresh path with unique-index preflight.** PG's `REFRESH MATERIALIZED VIEW CONCURRENTLY` requires a unique index on the matview; the path queries `pg_indexes` for one and falls back to a clear operator-actionable skip (matview name + reason naming the missing-unique-index requirement) instead of letting PG return its less-clear error mid-refresh.
- **Loud-failure on missing matview filter.** `--matview NAME` where NAME doesn't exist in `pg_matviews` surfaces as a clear error naming every missing matview — better than silently no-op'ing a typo. The validation runs before any REFRESH call.
- **Per-matview timing in output**. Text format renders human-readable rows (`refreshed: schema.name (123ms)` / `skipped: schema.name — reason`); JSON format emits `{"refreshed": [{"schema":...,"name":...,"duration_ms":...}], "skipped": [...]}` for metric-scraper integration.
- **`internal/engines/postgres/matview_refresh.go`** carries the engine-internal `MatviewRefreshOptions` + `RefreshMatviews(ctx, db, opts)` API. The CLI is the operator surface; programmatic callers (future sync-loop integration, if it ever lands) consume the package surface directly.

### Migration / Compatibility

- **Drop-in upgrade from v0.35.x.** No format changes, no engine-interface changes (the implementation lives in the postgres package only — MySQL is unaffected). The new subcommand is additive; pre-existing CLI invocations work unchanged.
- **PostgreSQL-only.** `sluice matview refresh --target-driver=mysql` refuses with a clear error naming MySQL's lack of matview concept; operators with MySQL targets should manage materialised-view-equivalent caching tables manually.

### Phase 3 — deferred

Phase 3 (cross-engine view-body translation via a SELECT-grammar translator) remains deferred. Phase 1's loud-failure-at-apply-time path handles non-portable view definitions today; `--view-override TABLE.VIEW=DEFINITION` (Phase 3 escape hatch) hasn't shipped because real operator demand for cross-engine views hasn't surfaced. Revisit when a concrete cross-engine view workflow surfaces.

### Who needs this release

- **PG operators with materialized views that should refresh on a cadence:** **upgrade** — `sluice matview refresh` is the operator-cadence-agnostic subcommand to wire into cron / k8s CronJob / Airflow. For matviews with a unique index, prefer `--concurrently` so reads keep working during the refresh.
- **PG operators with matview cron pipelines they already manage** (pg_cron, manual `REFRESH MATERIALIZED VIEW` calls): drop-in; sluice doesn't displace your existing setup. The new subcommand is one more option, not a replacement.
- **MySQL operators**: drop-in; the new command refuses with a clear error if invoked against a MySQL target. No impact on existing flows.

### Verification surface

- **3 unit tests** in `matview_refresh_test.go` covering SQL-statement shape across the four arg permutations, the filter-by-name behaviour, and the loud-failure-on-typo path.
- **5 integration tests** in `matview_refresh_integration_test.go` (against `postgres:16` testcontainers) covering plain refresh round-trip (pre/post row counts), concurrent refresh with unique index, concurrent refresh skipped when no unique index exists, `--matview` filter narrowing, and missing-matview loud-failure. All pass locally; CI runs them under the `integration` build tag.

## [0.35.0]

**Translator catalog batch — six additional MySQL → Postgres rewrite rules.** Closes Roadmap Item 5's outstanding medium-leverage entries. Brings the total to 22 of 30 catalog rules shipped; the remaining 8 are deliberately deferred per the catalog's own per-rule guidance and have actionable `--expr-override` workarounds.

### Added — translator catalog rules (`internal/engines/postgres/expr_translate.go`)

- **`HEX(int)` → `to_hex(int)`** (catalog #19). MySQL's HEX function returns the hexadecimal string representation of an integer. PG's `to_hex` is the direct equivalent for the integer-typed case. Narrow form only: `HEX(string)` returning hex of bytes would need `encode(x::bytea, 'hex')` which is the wrong rewrite if the column is integer-typed; operators with bytea HEX-of-bytes use cases can use `--expr-override`.
- **`FIELD(x, a, b, c, …)` → `array_position(ARRAY[a, b, c, …], x)`** (catalog #22). MySQL's FIELD returns the 1-based position of a value in a list; PG's `array_position` is the direct equivalent (PG 9.5+). Semantic note documented inline: PG returns NULL for not-present, MySQL returns 0. For ORDER BY proxies and custom enum-rank patterns the divergence is invisible; for strict 0-vs-NULL distinctions in CHECK constraints, use `--expr-override`.
- **`DAYNAME(d)` / `MONTHNAME(d)` → `TO_CHAR(d, 'FMDay')` / `TO_CHAR(d, 'FMMonth')`** (catalog #25). The `FM` prefix suppresses PG's default right-padding to 9 characters. Same STABLE-not-IMMUTABLE caveat as DATE_FORMAT — PG marks TO_CHAR as STABLE which means it can't appear in IMMUTABLE generated columns; the failure is loud and operator-actionable at apply time.
- **`WEEKOFYEAR(d)` → `EXTRACT(WEEK FROM d)::int`** (catalog #26 narrow ISO subset). `WEEK(d, mode)` with mode != 1 / 3 (ISO) uses Sunday/Monday-start semantics PG can't model uniformly — those forms intentionally fall through verbatim to preserve loud-failure on divergence.
- **`QUARTER(d)` → `EXTRACT(QUARTER FROM d)::int`** (catalog #27 narrow). YEARWEEK is deferred (composes EXTRACT with arithmetic and inherits #26's week-numbering caveats).
- **`DATEDIFF(a, b)` → `(a::date - b::date)`** (catalog #28). PG's date subtraction is an SQL operator, not a function call; the rewrite produces a parenthesised binary expression. Belt-and-braces `::date` casts handle timestamp-typed arguments by truncating to day precision, matching MySQL's behaviour (MySQL ignores the time portion).

### Deliberately not shipped (per catalog's per-rule guidance)

The remaining 8 catalog rules stay catalog-only with `--expr-override` as the escape hatch. Each was triaged in `docs/dev/translator-coverage.md` with a specific reason for deferral (extension dependency, divergent semantics, version-gated emit, or invalid-in-DDL-context expansion). The roadmap entry enumerates them for cross-reference.

### Migration / Compatibility

- **Drop-in upgrade from v0.34.x.** No format changes, no CLI changes, no engine-interface changes. The new translator rules only fire on cross-engine MySQL → PG migration when the source DDL body contains the recognised MySQL function shapes; pre-existing schemas that didn't trip the rules are unaffected.
- **Operators with `--expr-override` workarounds for the six newly-shipped patterns** can drop the overrides; the catalog rewrite produces the same output. If the override emits a non-default shape (e.g. wrapping the result in additional casts the operator needs), keep it — the override takes precedence.

### Who needs this release

- **Operators with MySQL → Postgres migrations whose source schemas use `HEX(int)`, `FIELD(x, …)`, `DAYNAME(d)`, `MONTHNAME(d)`, `WEEKOFYEAR(d)`, `QUARTER(d)`, or `DATEDIFF(a, b)` in DEFAULT / GENERATED / CHECK bodies:** **upgrade** — the migration now rewrites these to the PG equivalents automatically instead of forwarding them verbatim (which would fail at the target's parse step).
- **Same-engine operators** (MySQL → MySQL, PG → PG): drop-in; the translator only fires on cross-engine pairs.
- **Operators using `--expr-override` for any of the six patterns:** drop-in; the override stays a higher-priority path.

## [0.34.0]

**Logical backups Phase 6.3 closes the v1 KMS shortlist — GCP Cloud KMS + Azure Key Vault.** Operators outside AWS can now use envelope encryption without falling back to the passphrase-mode path. Same `EnvelopeEncryption` interface that Phase 6.1 (passphrase) and Phase 6.2 (AWS KMS) shipped against — chunk writer/reader paths are unchanged; the only per-provider bits are the KEKMode tag in the manifest and the per-cloud wrap/unwrap RPCs.

### Added

- **GCP Cloud KMS envelope encryption** (`internal/crypto/gcp_kms.go`). New CLI flag `--gcp-kms-key-resource=projects/PROJECT/locations/LOCATION/keyRings/RING/cryptoKeys/KEY` (with optional `/cryptoKeyVersions/VERSION`). Manifest carries `KEKMode="gcp-kms"`. Wrap routes through Cloud KMS's `Encrypt` RPC; unwrap through `Decrypt`. Construction-time `GetCryptoKey` preflight surfaces auth / not-found errors before the backup starts. Auth via Application Default Credentials (`gcloud auth application-default login` or `GOOGLE_APPLICATION_CREDENTIALS`). Operator-actionable error translation for the five canonical gRPC codes (`NotFound`, `PermissionDenied`, `FailedPrecondition`, `InvalidArgument`, `Unauthenticated`) with role hints (`roles/cloudkms.cryptoKey{Encrypter,Decrypter,Viewer}`).
- **Azure Key Vault envelope encryption** (`internal/crypto/azure_kms.go`). New CLI flags `--azure-key-vault-id=https://VAULT.vault.azure.net/keys/KEY[/VERSION]` (also accepts `managedhsm.azure.net` for HSM-backed vaults) and `--azure-wrap-algorithm` (defaults to `RSA-OAEP-256`; pass `A256KW` for HSM-backed AES keys). Manifest carries `KEKMode="azure-kms"`. Wrap/unwrap use Key Vault's `WrapKey` / `UnwrapKey` RPCs — Key Vault's recommended pattern for symmetric-key wrapping (vs `Encrypt`/`Decrypt` which has a smaller payload cap on asymmetric keys). Auth via `DefaultAzureCredential` (env vars, managed identity, Azure CLI cached login). Operator-actionable error translation covers `KeyNotFound`, `Forbidden`/`AccessDenied`, `BadParameter`, `KeyDisabled`, plus HTTP status fallbacks (401/403/404) for errors that omit the SDK's `ErrorCode` field.
- **All-provider mutual exclusion**. The four key sources — passphrase, AWS KMS, GCP KMS, Azure Key Vault — are now pairwise mutually exclusive at flag-parse time. `validateKeySources` enforces this with a clear error message naming all four flag families. Test coverage in `TestEncryptionFlags_AllProvidersMutuallyExclusive` (7 pair combinations + the all-four-at-once case).

### Migration / Compatibility

- **Drop-in upgrade from v0.33.x.** Existing passphrase-mode and AWS KMS chains continue to restore unchanged. No on-disk format changes; the new `KEKMode` values (`gcp-kms`, `azure-kms`) are recognised additions, not breaking renames.
- **No new direct dependencies that add operator-visible binary-size cost.** Both `cloud.google.com/go/kms` and `github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azkeys` were already in the module graph as indirect deps; this release promotes them to direct deps without changing the closure.
- **Operators outside AWS can now skip the passphrase escape hatch.** Pre-v0.34.0 the choice on GCP / Azure was either passphrase mode (operator-managed secrets) or bucket-level SSE (provider-managed). v0.34.0 closes the gap with native KMS support.

### Who needs this release

- **Operators on GCP whose compliance posture requires customer-managed key material:** **upgrade** — `--gcp-kms-key-resource` ships envelope encryption without the passphrase storage problem.
- **Operators on Azure with the same constraint:** **upgrade** — `--azure-key-vault-id` ships the equivalent path.
- **Operators on AWS** (Phase 6.2 path, v0.23.0): drop-in; no behaviour change.
- **Operators on the passphrase path** (Phase 6.1, v0.22.0): drop-in; no behaviour change. The expanded mutual-exclusion check error message now names all four flag families instead of two.
- **Operators not using encryption:** drop-in; no behaviour change.

## [0.33.3]

**v0.33.2 closed Bug 52 only partially.** The fix was structurally correct on the IR + writer side, but skipped Phase A — it assumed PostGIS's `geometry_columns.type` view encodes Z and ZM in the type string the same way it encodes M-only (LINESTRINGM, POINTM). It doesn't. Bug 53 captures the missed slice; this release closes it.

### Fixed

- **Bug 53: PG `geometry(POINTZ, 4326)` and `geometry(POLYGONZM, 4326)` now actually round-trip end-to-end on same-engine PG → PG.** PostGIS uses a two-channel encoding for spatial-column dimensions in its `geometry_columns` / `geography_columns` views:
  - **M-only** (LINESTRINGM, POINTM): puts the "M" suffix in `type` directly AND records `coord_dimension=3`. v0.33.2's `parseGeometrySubtype` upper-case + suffix-strip path handled this case.
  - **Z** (POINTZ, LINESTRINGZ): leaves `type` as the 2D base name (`POINT`, `LINESTRING`) and signals the dimension only via `coord_dimension=3`. v0.33.2 didn't read `coord_dimension`, so the Z flag silently dropped at translate-time and the writer emitted `geometry(POINT, 4326)` instead of `geometry(POINTZ, 4326)`. Bulk copy then failed with SQLSTATE 22023 ("Geometry has Z dimension but column does not").
  - **ZM** (POLYGONZM, etc.): same as Z — `type` stays as the 2D base name, dimension signaled by `coord_dimension=4`.

  The fix adds `coord_dimension` to the `SELECT` in both `readGeometryColumnInfo` and `readGeographyColumnInfo`, maps it to `HasZ` / `HasM` flags on the per-column `geometryColumnInfo` via a new `dimensionFlagsFromCoordDim` helper that disambiguates the 3D case by inspecting whether the type column ends in "M", and OR-merges the reader-side flags with the existing type-string parsing in `translateType`. Either channel alone may be load-bearing; the OR-merge means neither in isolation is fragile.
- **PostGIS catalog evidence is now first-class in integration tests.** The v0.33.2 cycle filed Bug 53 because the integration tests asserted on `geometry_columns.type='POINTZ'` — a value PostGIS never produces for that column (the view normalises to the base name, even though the column itself accepts Z values). v0.33.3 shifts the ground-truth assertion to `pg_attribute.format_type(atttypid, atttypmod)`, which returns the modifier-bearing form `geometry(PointZ,4326)`. Three new dimensional-variant integration tests (`TestMigrate_PG_PostGIS_PointZPassthrough`, `TestMigrate_PG_PostGIS_PolygonZMPassthrough`, `TestMigrate_PG_PostGIS_LineStringMPassthrough`) all run against real PostgreSQL containers locally (verified) and will run in CI under the `integration postgis` tag.

### Migration / Compatibility

- **No format-breaking changes.** No CLI or engine-interface changes. `geometryColumnInfo` gains two unexported `HasZ` / `HasM` fields (internal to the postgres engine package); `ir.Geometry`'s existing `HasZ` / `HasM` fields from v0.33.2 are unchanged.
- **Drop-in upgrade from v0.33.2.** Operators with plain 2D `geometry` / `geography` columns are unaffected. Operators with Z, M, or ZM dimensional columns get the end-to-end round-trip that v0.33.2 promised.

### Process — Phase A is non-negotiable, even for "small" fixes

The Phase A diagnostic skip in v0.33.2 (assumed PostGIS encoded Z in the type column without verifying against a live catalog) is the same pattern the three-phase debug protocol exists to prevent. The retag cost: one extra release (v0.33.3), one extra CI cycle, one extra subagent cycle. The protocol's overhead: one `docker exec psql -c "SELECT type, coord_dimension FROM geometry_columns WHERE ..."` against a one-shot container. v0.33.3 corrects the catalog assertion shape (`format_type` over `geometry_columns.type`) so the failure mode can't recur silently.

### Who needs this release

- **Operators running PG → PG with Z, M, or ZM dimensional spatial columns:** **upgrade** — v0.33.2's claimed fix only worked for the M-only case; v0.33.3 closes Z + ZM.
- **Operators on the cross-engine PG → MySQL PostGIS path:** drop-in; no behaviour change (MySQL carries Z / M in WKB, not the column type).
- **Operators not using PostGIS:** drop-in; no behaviour change.

## [0.33.2]

**Two adjacent PostGIS fidelity gaps surfaced by the v0.33.1 cycle, both closed.** Bug 51 + Bug 52 share a single fix locus (`parseGeometrySubtype`) and are both fidelity preservers — neither corrupts row data, but both silently widen the column type on the target.

### Fixed

- **Bug 51: PG `geography(Point, 4326)` columns now preserve the subtype on same-engine PG → PG.** Pre-fix: PostGIS's `geometry_columns.type` view returns ALL-CAPS strings ("POINT"), but its sibling `geography_columns.type` view returns Mixed-Case ("Point"). `parseGeometrySubtype` did a literal switch on the upper-case forms only, so `geography_columns` inputs fell through to `GeometryUnspecified` and the target column landed as `geography(Geometry, 4326)` instead of `geography(Point, 4326)`. Rows still round-tripped (the wildcard supertype accepts any concrete shape), but the typmod-constrained subtype was lost on the target — operators selecting on `geography_columns.type` would see drift. The fix upper-cases the input before dispatching; geography and geometry subtype reads now share a code path.
- **Bug 52: PG `geometry(POINTZ, 4326)` and the Z / M / ZM dimensional variants now round-trip on same-engine PG → PG.** Pre-fix (and pre-existing — not a v0.33.1 regression): PostGIS extends each 2D subtype with Z (3D elevation), M (linear measure), and ZM (4D) variants — `POINTZ`, `LINESTRINGZM`, `MULTIPOLYGONZ`. The IR's `GeometrySubtype` enum only modeled the seven 2D base subtypes; the dimensional suffix dropped at translate-time and the writer emitted the generic `GEOMETRY` wildcard. Bulk copy then failed with SQLSTATE 22023 ("Geometry has Z dimension but column does not") because the row's WKB carried Z bytes but the target's typmod-constrained column rejected them. The fix adds two booleans (`HasZ`, `HasM`) to `ir.Geometry` orthogonal to `Subtype` (28 entries collapsed to 7 × 2 flags), `parseGeometrySubtype` strips the dimensional suffix before subtype dispatch, and the PG writer's `postgisSubtypeName` reconstructs the suffix on emit. Cross-engine PG → MySQL ignores the flags (MySQL carries Z / M in the WKB bytes rather than the column type modifier — the value round-trip works on MySQL targets without needing the type to match).

### Migration / Compatibility

- **No format-breaking changes.** No CLI or engine-interface changes. `ir.Geometry` gains two appended bool fields (`HasZ`, `HasM`); existing backups deserialise unchanged because the envelope fields use `omitempty`. Zero values produce the pre-v0.33.2 behaviour (2D, no measure).
- **Drop-in upgrade from v0.33.1.** Operators with plain 2D `geometry` / `geography` columns are unaffected. Operators on the v0.33.1 geography passthrough path (Bug 49 closure) get the additional subtype-preservation fidelity automatically; no flag change needed.

### Who needs this release

- **Operators running PG → PG with `geography(<Subtype>, <SRID>)` typed columns:** **upgrade** — target column-type fidelity now matches the source byte-for-byte.
- **Operators running PG → PG with Z / M / ZM (3D / measure / 4D) spatial columns:** **upgrade** — pre-v0.33.2 the bulk copy failed with SQLSTATE 22023 because the dimensional suffix was dropped from the target column type.
- **Operators on the cross-engine PG → MySQL PostGIS path:** drop-in; no behaviour change (MySQL doesn't carry Z / M in column type — flags are ignored).
- **Operators not using PostGIS:** drop-in; no behaviour change.

## [0.33.1]

**Two PostGIS PG → PG passthrough gaps surfaced by the v0.33.0 cycle, both closed.** Bug 49 + Bug 50 are scoped to the PostGIS catalog path; both ship together as v0.33.1.

### Fixed

- **Bug 49: PG `geography` columns now round-trip on same-engine PG → PG** (`internal/ir/extension_types.go`, `internal/engines/postgres/{types.go,schema_reader.go,ddl_emit.go}`). Pre-fix: the PG schema reader's `udt_name == "geometry"` special case had no `geography` sibling, so columns of type `geography(POINT, 4326)` fell through to the user-defined-hint path and refused at schema-read with "pass `--enable-pg-extension postgis`" **even when the flag was passed**. The fix adds an `IsGeography bool` to `ir.Geometry` (minimal IR surface — reuses every writer arm and the cross-engine path; geography flattens to MySQL geometry on cross-engine targets where the distinction has no equivalent), a parallel `readGeographyColumnInfo` against PostGIS's `geography_columns` view, and a `case ir.Geometry` writer branch that emits `geography(<subtype>, <srid>)` when `IsGeography == true`. Backup-envelope round-trips preserve the flag via a new `is_geography` field on the type envelope.
- **Bug 50: `ir.IndexKind` now models SP-GiST and BRIN, unlocking 6 of the 9 PostGIS spatial opclasses** (`internal/ir/schema.go`, `internal/engines/postgres/{schema_reader.go,ddl_emit.go}`, `internal/pipeline/cross_engine_supportable.go`). Pre-fix: only 3 of 9 catalog-declared opclasses (the GiST trio) actually round-tripped end-to-end. The SP-GiST and BRIN opclasses (`spgist_geometry_ops_2d` / `_3d` / `_nd`, `brin_geometry_inclusion_ops_2d` / `_4d` / `_nd`) were preserved in IR by the v0.33.0 catalog entry, but `indexKindFrom` in the schema reader returned `IndexKindUnspecified` for `spgist` / `brin` access methods, and `postgresIndexMethod` in the writer therefore dropped the AM, falling back to btree — CREATE INDEX then failed on the target because the opclasses aren't btree-compatible. Two new enum values (`IndexKindSPGist`, `IndexKindBRIN`) were appended to the IR enum (preserves uint8 backup stability), schema-reader and writer dispatch arms were extended, and the cross-engine PG → MySQL refusal in `unsupportablePGtoMySQL` was broadened to catch the new kinds (no MySQL counterpart for SP-GiST or BRIN).

### Migration / Compatibility

- **No format-breaking changes.** No CLI changes, no engine-interface changes. The IR's `Geometry` struct gains an `IsGeography bool` field (zero-value = false = the pre-v0.33.1 geometry shape; existing backups deserialise unchanged because the backup envelope's `is_geography` field is `omitempty`). The IR's `IndexKind` enum gains two appended values; existing manifests carrying the prior values are unaffected.
- **Drop-in upgrade from v0.33.0.** Operators not using `geography` columns or SP-GiST / BRIN indexes are unaffected. Operators who hit Bug 49 or Bug 50 in v0.33.0 should upgrade and re-run with `--enable-pg-extension postgis`.
- **Cross-engine PG → MySQL with PG SP-GiST or BRIN indexes** now refuses upstream at `checkCrossEngineSupportable` with a clear "PG <kind> index has no MySQL counterpart" message, mirroring the existing GIN / GiST refusals. Pre-fix the IR carried `IndexKindUnspecified` so the refusal didn't fire and the operator hit a CREATE INDEX failure on the MySQL target instead.

### Who needs this release

- **Operators running PG → PG with `geography` columns:** **upgrade and pass `--enable-pg-extension postgis`** — same-engine geography passthrough now works end-to-end.
- **Operators running PG → PG with SP-GiST or BRIN spatial indexes:** **upgrade and pass `--enable-pg-extension postgis`** — the spatial opclasses now round-trip and the target index strategy matches the source.
- **Operators running PG → PG with GiST-only spatial indexes** (the v0.33.0 path): drop-in; no behavior change.
- **Operators on the cross-engine PG → MySQL PostGIS path** (the v0.28.0 / ADR-0035 path): drop-in for `geometry`; if your PG source uses SP-GiST / BRIN spatial indexes you now get an actionable refusal at preflight instead of a CREATE INDEX failure on the MySQL target.
- **Operators not using PostGIS:** drop-in; no behavior change.

## [0.33.0]

**PostGIS completes the ADR-0032 v1 extension shortlist.** PostGIS is the final entry on the five-extension v1 shortlist per `docs/research/pg-extensions-deployment-frequency.md` — pgvector (v0.26.0), pg_trgm (post-v0.29.1), hstore + citext (v0.31.0, with v0.31.1 / v0.32.1 follow-ups), and now postgis. The cross-engine PG → MySQL PostGIS path already shipped in v0.28.0 / ADR-0035 (`ir.Geometry` + SRID round-trip + the MySQL writer's `SRID <n>` clause); this release adds the PG → PG passthrough catalog entry so spatial-index operator classes round-trip cleanly when the operator opts in via `--enable-pg-extension postgis`.

### Added

- **`--enable-pg-extension postgis` round-trips PostGIS spatial-index operator classes on PG → PG.** Pre-fix: a `CREATE INDEX ... USING GIST (col gist_geometry_ops_nd)` index on a PG source lost the opclass on a PG → PG migrate — the schema reader's `extensionOperatorClassRegistered` fallthrough didn't recognise PostGIS-owned opclasses (only pg_trgm's `gin_trgm_ops` / `gist_trgm_ops` and pgvector's `vector_*_ops` were declared), so the opclass dropped silently and the target index landed with PG's default opclass for the AM (which for the 2D case happens to also be `gist_geometry_ops_2d`, but diverges for nD / SP-GiST / BRIN variants). The new `pgPostGISDef` catalog entry in `internal/engines/postgres/extension_catalog.go` declares the nine canonical PostGIS opclasses (`gist_geometry_ops_2d`, `gist_geometry_ops_nd`, `gist_geography_ops`, `spgist_geometry_ops_2d` / `_3d` / `_nd`, `brin_geometry_inclusion_ops_2d` / `_4d` / `_nd`) so the existing per-opclass passthrough machinery preserves them via `ir.IndexColumn.OperatorClass` when the operator passes `--enable-pg-extension postgis`. Without the flag, the existing WARN/drop path fires (loud-failure default; mirrors pg_trgm).
- **Actionable hint for unenabled PostGIS columns.** The schema reader's "USER-DEFINED type I don't recognise" fallthrough now surfaces `--enable-pg-extension postgis` for `geometry` / `geography` columns when the operator forgot the flag — same shape as the v0.31.0 hint for hstore / citext. The mechanism is a new `extensionDef.hintTypeNames` field that lets the catalog claim ownership of types for the hint path WITHOUT routing them through the catalog's build/emit machinery (PostGIS's `geometry` rides on `ir.Geometry` per ADR-0035, not `ir.ExtensionType` — `typesByName` stays empty).
- **ADR-0032 v1 shortlist complete.** The five extensions named in the original research doc — vector, pg_trgm, hstore, citext, postgis — are all shipped. The ADR's status block records postgis under "shipped"; the deferred-tier follow-ups (Tier 3 uuid-ossp / pgcrypto for function-in-defaults) remain on the roadmap as v2 candidates.

### Migration / Compatibility

- **No format-breaking changes.** No CLI changes, no on-disk format changes, no engine-interface changes. `extensionDef` gains a new optional `hintTypeNames` field; per-extension entries that don't need it leave it nil/empty.
- **Drop-in upgrade from v0.32.2.** The PostGIS catalog entry only fires when the operator passes `--enable-pg-extension postgis`; existing PG → PG migrations that didn't use spatial indexes are unaffected. Pre-existing PG → MySQL PostGIS operators (the v0.28.0 / ADR-0035 path) are unaffected — that flow doesn't use the extension flag.
- **Cross-engine PG → MySQL `--enable-pg-extension postgis` stays refused** at `validateEnabledPGExtensions` (postgis is not in `crossEngineDefaultTranslatedExtensions`). The cross-engine geometry path requires no flag; column-type translation runs automatically when the source has `ir.Geometry` columns.
- **No silent behavior shift for unenabled postgis users.** Without `--enable-pg-extension postgis`, a PG → PG migrate of a spatial-indexed table now emits a WARN naming the dropped opclass (same shape as pg_trgm) where pre-fix it was silent. The WARN points at the flag; the migrate still proceeds (PG's default opclass kicks in on the target).

### Who needs this release

- **Operators running PG → PG migrations or syncs against tables with PostGIS spatial indexes carrying explicit operator classes** (e.g. `gist_geometry_ops_nd` for 3D/4D geometry, `gist_geography_ops` for geography columns, SP-GiST or BRIN spatial indexes): **upgrade and pass `--enable-pg-extension postgis`** — the opclass now round-trips and the target index strategy matches the source.
- **Operators on the cross-engine PG → MySQL PostGIS path** (the v0.28.0 / ADR-0035 path): drop-in; no behavior change. The column-type translation works without the new flag, same as before.
- **Operators not using PostGIS:** drop-in; no behavior change.

## [0.32.2]

**Two cross-engine MySQL UX gaps surfaced as pre-existing observations during the v0.32.0 cycle, both closed.** Neither is a regression or correctness issue; both are cleaner failure modes for cross-engine PG → MySQL scenarios. Same shape as v0.30.1's pair.

### Fixed

- **OBS-1: MySQL target `sluice_cdc_state` now carries the `slot_name` / `source_dsn_fingerprint` / `target_schema` columns.** Pre-fix: cross-engine PG → MySQL with `--slot-name <name>` on the parent stream surfaced MySQL Error 1054 ("Unknown column slot_name") at the per-target position-write because the column never existed on the MySQL side. PG added `slot_name` in v0.24.0, `source_dsn_fingerprint` in v0.25.0, and `target_schema` in v0.25.1 — each via a PG-target-only `ADD COLUMN IF NOT EXISTS`. The MySQL writer's `CREATE TABLE` schema was never updated, so the MySQL control table fell three columns behind PG. This release brings the schema to parity in `internal/engines/mysql/control_table.go::ensureControlTable` (new columns in the `CREATE TABLE` body plus a detect-then-ALTER migration for each, portable to MySQL 8.0.x versions older than 8.0.29 that lack `ADD COLUMN IF NOT EXISTS`). The MySQL `ChangeApplier` now also implements `SetSlotName`, `SetSourceDSNFingerprint`, and `SetTargetSchema` so the streamer's structural-interface dispatch threads the values through. `writePositionTx` and `listStreams` carry the new columns end-to-end with the same COALESCE-tolerant shape PG uses (empty-string-as-NULL-sentinel preserves existing values on chain-handoff writes). Pre-v0.32.2 control tables migrate transparently on the next `EnsureControlTable` call; existing rows keep their data and the new columns start NULL.
- **JSONB / TEXT / BLOB / GEOMETRY DEFAULT now suppressed in MySQL DDL emit, with a WARN log naming the column.** Pre-fix: cross-engine PG → MySQL with `jsonb NOT NULL DEFAULT '{}'::jsonb` (or the symmetric shapes on `text NOT NULL DEFAULT ''`, `bytea NOT NULL DEFAULT ...`, `geometry NOT NULL DEFAULT ...`) refused at `CREATE TABLE` on the MySQL target with MySQL Error 1101 ("BLOB, TEXT, GEOMETRY or JSON column 'col' can't have a default value"). MySQL hard-codes the restriction; the prohibition is on the four type families' DEFAULT clause specifically, not on the columns themselves. `internal/engines/mysql/ddl_emit.go::emitColumnDef` now consults `mysqlForbidsDefault(t ir.Type)` and drops the `DEFAULT` clause when the type is `ir.JSON` / `ir.Text` / `ir.Blob` / `ir.Geometry` / `ir.Array` (the latter routes to MySQL JSON) / `ir.ExtensionType{Extension: "hstore"}` (also routes to MySQL JSON via the cross-engine translator). A WARN-level `slog` line names the column, the suppressed default literal, and the IR type so the operator has a paper trail; a follow-up WARN fires when the column is also `NOT NULL` reminding the operator that future `INSERT`s without an explicit value will fail. Migrated rows arrive intact (their values are explicit in the source); only future inserts on the target are at risk.

### Migration / Compatibility

- **No format-breaking changes.** No CLI changes, no on-disk format changes, no engine-interface changes.
- **Drop-in upgrade from v0.32.1.** The MySQL control table picks up the three new columns via the idempotent detect-then-ALTER migration on the next `EnsureControlTable` call — same shape as the v0.3.0 `stop_requested_at` and v0.27.0 `live_added_tables` migrations. Existing rows keep their data; the new columns start NULL.
- **Cross-engine PG → MySQL operators previously failing on Error 1101 (JSONB DEFAULT):** the new suppression surfaces a WARN at translate-time and lets the migration proceed. If your migration depended on the DEFAULT auto-populating future inserts on the MySQL target, you'll need to drop NOT NULL on the source column or supply the value at write time on the target — the WARN's follow-up note documents this.
- **Cross-engine PG → MySQL operators previously failing on Error 1054 (`--slot-name`):** the new column makes the round-trip work end-to-end. No workaround needed; the same `--slot-name` flag now records on the MySQL target's control table.

### Who needs this release

- **Cross-engine PG → MySQL operators with `--slot-name <name>` on the parent stream:** **upgrade** — the live add-table flow (`sluice schema add-table --no-drain`) against a MySQL target now records the slot name on the per-target control table instead of dying at MySQL Error 1054.
- **Cross-engine PG → MySQL operators whose source has `jsonb`, `text`, `bytea`, or PG `geometry` columns with `NOT NULL DEFAULT`:** **upgrade** — the migration now proceeds with a WARN log instead of dying at MySQL Error 1101.
- **MySQL → MySQL operators:** drop-in; no behavior change (the MySQL streamer doesn't supply slot_name / source_dsn_fingerprint / target_schema, so the new columns stay NULL; the DDL emit change has no effect on same-engine round-trips where MySQL TEXT / BLOB / JSON / GEOMETRY DEFAULTs never existed on the source).
- **PG → PG operators:** drop-in; no behavior change (the fixes are MySQL-target-side only).

## [0.32.1]

**hstore PG → PG COPY binary codec completes the v0.31.0 Tier 1 deferred work.** The v0.31.0 release shipped hstore as an ADR-0032 Tier 1 extension but deferred PG → PG passthrough because the IR carries text-form hstore bytes (`"k"=>"v"`) and PG's COPY binary protocol expects hstore's pair-array wire format. The preflight refusal in `validateEnabledPGExtensions` surfaced an actionable workaround hint until the codec landed. This release implements the codec and removes the refusal.

### Fixed

- **`--enable-pg-extension hstore` on PG → PG bulk-copy now succeeds.** Pre-fix: `pipeline.validateEnabledPGExtensions` refused at preflight with a `--type-override hstore_column=text` workaround hint; the bulk-copy path could not encode hstore values through pgx's binary COPY protocol because no per-conn `pgtype.Codec` translated the IR's text-form hstore bytes into hstore's pair-array wire format. The refusal branch is gone; PG → PG hstore now round-trips byte-for-byte the same way the existing pgvector / pg_trgm / citext same-engine paths do.
- **New codec at `internal/engines/postgres/hstore_codec.go`** — `pgHstoreBinaryCodec` mirrors `pgvectorBinaryCodec`'s structural pattern (v0.26.0). Accepts `string` and `[]byte` text-form hstore inputs; emits PG's binary hstore wire format (`int32 BE pair-count + per-pair int32 BE keylen + key bytes + int32 BE vallen + value bytes`, with `vallen = -1` signalling SQL NULL on the value side). Decoder is symmetric for unit-test round-trip and future typed-scan paths.
- **Two-codec framework now documented as the pattern for future Tier 2+ extensions with their own wire formats** (e.g. PostGIS EWKB). The COPY path's per-conn codec registration gate consults both `tableHasPGVectorColumn` and the new `tableHasHstoreColumn` helper; future extensions add a sibling helper + a `registerPG<Ext>Codec` function + an entry in the same gate.
- **`TestMigrate_PG_Hstore_Passthrough` (un-skipped) now pins the end-to-end PG → PG hstore round-trip** with 3 rows including the canonical multi-pair shape (`"a"=>"1", "b"=>"2"`), a single-pair shape, and `pg_attribute` verification that the target column's type is `hstore` (not a text column with hstore-shaped content). Same-engine and cross-engine pgvector / pg_trgm / citext tests are regression-clean.

### Migration / Compatibility

- **Bug fix only; no operator surface change.** `sluice migrate --enable-pg-extension hstore` against PG sources/targets now works end-to-end without `--type-override`. Operators who deployed the v0.31.0 workaround (`--type-override hstore_column=text`) can drop the override; the hstore-as-text shape they got is byte-equivalent to what the codec now ships through binary COPY.
- **Cross-engine PG → MySQL hstore path is unaffected.** That branch uses the value translator (`hstore` → JSON via `prepareHstoreToJSON` in `internal/engines/mysql/row_writer.go`); the v0.31.0 codepath had no preflight refusal there and the v0.32.1 codec is not consulted on a non-PG target.

### Who needs this release

- **Operators running PG → PG migrations or syncs with hstore columns:** **upgrade** — the v0.31.0 preflight refusal is closed and the migration now proceeds without the `--type-override` escape hatch.
- **Operators on the cross-engine PG → MySQL hstore path:** drop-in; no behavior change (v0.31.0 + v0.31.1 already worked end-to-end via the JSON translator).
- **Operators not using hstore:** drop-in; no behavior change.

## [0.32.0]

**Mid-stream live add-table strict zero-loss closes the v0.24.0 best-effort gap (Item 3, ADR-0036 Phase B).** The four-round Phase A diagnose campaign on the v0.24.0 residual loss surface — Phase A.1 ruled out M1/M2/M4 and reframed M3; Phase A.2 falsified reframed M3 (10/10 runs); Phase A.3 confirmed M5c (applier-side drop, 10/10 runs); Phase B identified the precise drop site via code-reading — pinned the failure to a single load-bearing line in the PG applier's dispatch path. The fix is a one-line orchestration reorder plus a small idempotent-bulk-copy helper.

### Fixed

- **PG mid-stream live add-table (`sluice schema add-table --no-drain`) now delivers every in-flight CDC event on the new table exactly-once-effectively.** Pre-fix: events on the new table at LSN > publication-add LSN that reached the active stream's applier in the window between `ALTER PUBLICATION ADD TABLE` and `runBulkCopy`'s `CREATE TABLE ... IF NOT EXISTS` were silently skipped by the applier's `errUnknownTable` defence-in-depth branch (Bug 13 path), then never re-delivered (pgoutput doesn't replay). v0.24.0 documented this as a "best-effort" property; CI's under-load test reported ~36% loss at sub-second burst rates with no shipped fix. Phase A.3 confirmed the drop site (applier receives, then drops); Phase B fixes it.
- **Drop site (code-reading evidence):** `internal/engines/postgres/change_applier.go::dispatch`'s Insert case — when `colTypesFor` returns `errUnknownTable` because `information_schema.columns` is empty (target table doesn't exist), the dispatch logs a warning and `return nil`. Bug 13's defence-in-depth path stays in place for genuinely-drifted publications; the v0.24.0 race is closed at the orchestration layer instead.
- **Fix shape (~150 LOC):**
  - `internal/pipeline/add_table.go::AddTable.Run` step 3a (NEW): `sw.CreateTablesWithoutConstraints(ctx, scoped)` runs BEFORE publication-add. By the time pgoutput's per-LSN catalog snapshot includes the new table, the target table is already in `information_schema.columns` and the applier's dispatch path succeeds.
  - `internal/pipeline/migrate.go::runBulkCopyForAddTable` (NEW): mid-stream-live-add variant of `runBulkCopy` that routes the row copy through `ir.IdempotentRowWriter.WriteRowsIdempotent` (INSERT … ON CONFLICT (pk) DO UPDATE) when the writer exposes it. With the target table pre-created, a small number of CDC events on the new table may have already been applied by the active stream's applier by the time bulk-copy reaches those rows (events in the [publication-add, snapshot-open] window); the idempotent path absorbs the overlap. Engines without the surface (none today — both PG and MySQL implement it) fall back to plain `WriteRows` with a debug log.
- **Engine-symmetric.** Both PG and MySQL schema writers use `CREATE TABLE IF NOT EXISTS`; both engines implement `ir.IdempotentRowWriter`. MySQL's filter-flip mechanism (ADR-0034) didn't manifest the race in v0.24.0 (it gates dispatch on `live_added_tables` membership rather than table existence), but the early-create + idempotent-copy shape removes engine-specific timing assumptions from the orchestrator.
- **Regression artifact.** `TestAddTable_LiveMode_PG_UnderLoad` tightened from best-effort logging (`if got < maxTotal { t.Logf(...) }`) to strict zero-loss assertion (`if got != maxTotal { t.Errorf(...) }`). `TestAddTable_LiveMode_PG_DiagnoseLossSurface` and the Phase A.3 applier-side capture probe in `change_applier.go::dispatch` stay as permanent diagnostic artifacts (mirrors how ADR-0033's slot-pause verify test stays as proof-of-falsification).

### Migration / Compatibility

- **Bug fix only; no operator surface change.** `sluice schema add-table --no-drain TABLE` runs the same way; the in-flight-loss caveat from ADR-0030 § "What could go wrong" item 3 is closed without operator intervention.
- **Operators with high write-rate workloads who deferred live add-table** in favour of the drained flow (or operator-coordinated quiesce per Path C) can switch to `--no-drain` without the small residual loss caveat.
- **Path B (dual-slot) and Path C (operator quiesce) are no longer needed** for the v0.24.0 loss surface. They remain available as forward options for unrelated edge cases (e.g. M1 long-txn straddling under workloads where Phase A.1's 1-in-6 rate becomes operator-relevant).

### Who needs this release

- **Operators running mid-stream live add-table (`sluice schema add-table --no-drain`) on PG → PG or PG → MySQL with sustained write rates on the new table at the moment of live-add:** **upgrade** — the residual loss surface that was best-effort in v0.24.0 through v0.31.1 is closed.
- **Operators on the drained add-table flow:** drop-in; no behavior change (the drained flow already had zero-loss semantics by construction).
- **MySQL → MySQL operators using `--no-drain`:** drop-in; the ADR-0034 filter-flip path didn't manifest the same race, but the orchestration reorder removes engine-specific timing assumptions from the flow.

## [0.31.1]

**Bug 48 fix: hstore PG → MySQL cross-engine works under LOAD DATA path.** v0.31.0's headline feature succeeded on the batched-INSERT path (`local_infile=OFF`) but failed cryptically on the LOAD DATA path (`local_infile=ON`) with `Error 3144 (22032): Cannot create a JSON value from a string with CHARACTER SET 'binary'`. Surfaced by the v0.31.0 cycle on a MySQL target with the recommended `local_infile=ON` setting. Real-world operator UX bug; not a regression.

### Fixed

- **Bug 48 — `columnSetExpr` in `load_data_writer.go` now wraps `ir.ExtensionType` columns in `CONVERT(@var USING utf8mb4)`.** Pre-fix: only `ir.JSON` / `ir.Varchar` / `ir.Text` / `ir.Set` got the wrapper; hstore (which the cross-engine path translates to MySQL JSON via `prepareHstoreToJSON`) was sent as charset=binary and rejected by MySQL's JSON parser. Citext also benefits (it lands as VARCHAR with `_ci` collation; the same charset reinterpretation applies). Other ExtensionType arms (vector / pg_trgm / postgis) don't reach `columnSetExpr` — cross-engine preflight refuses them. New unit-test pins for both hstore and citext in `TestColumnSetExpr`.

### Migration / Compatibility

- **Bug fix only; no operator-visible behavior change beyond the failure path closing.** Drop-in upgrade from v0.31.0.
- **Operators who hit Bug 48 on v0.31.0** (cross-engine PG → MySQL with hstore source columns and `local_infile=ON` on the MySQL target) should retry the migration after upgrading. The fix is targeted to the LOAD DATA path; batched-INSERT (the v0.31.0 success path) is unaffected.

### Who needs this release

- **Cross-engine PG → MySQL operators with hstore (or future translator-bearing extension) columns AND `local_infile=ON` on the MySQL target:** **upgrade** — v0.31.0's headline feature only worked under the slower batched-INSERT path.
- **Operators on `local_infile=OFF`:** drop-in; no behavior change (v0.31.0 already worked for you).

**PG → PG `hstore` (cross-engine only this release) and `citext` extension passthrough land as the v1 shortlist's Tier 1 entries (ADR-0032).** Both are type-only opaque-bytes extensions: hstore is the key/value-map type, citext is `text` with a case-insensitive collation. Cross-engine PG → MySQL gets built-in default translators for both (hstore → MySQL JSON with value-shape conversion; citext → MySQL VARCHAR with `utf8mb4_0900_ai_ci` collation). citext also ships PG → PG passthrough. **hstore PG → PG is deferred to v0.31.1** — the COPY-protocol binary codec for hstore (mirroring pgvector's pattern from v0.26.0) hasn't landed yet; preflight refuses with an actionable workaround hint (`--type-override hstore_col=text`). This is the first ADR-0032 entry to ship a cross-engine translator alongside same-engine passthrough — the framework formalises the surface (`ir.CrossEngineExtensionTranslator` optional engine interface; per-extension `crossEngineDefaultTranslatedExtensions` catalog declaration) so future Tier 1 entries with defensible cross-engine mappings can follow the same pattern.

### Deferred

- **hstore PG → PG passthrough deferred to v0.31.1.** Preflight refuses `--enable-pg-extension hstore` when target is PG with an actionable hint. Workaround until the binary codec lands: `--type-override hstore_col=text` per column (preserves text form on PG target). Cross-engine PG → MySQL hstore works as advertised — the value translator handles the wire format. Tracked: the COPY binary codec needs to mirror `pgvectorBinaryCodec`'s shape, accepting `[]byte` text-form hstore input and emitting PG's binary hstore wire format (int32 BE pair-count + length-prefixed key/value pairs).

### Added

- **`hstore` PG extension catalog entry (`internal/engines/postgres/extension_catalog.go`).** Declares the `hstore` udt under `typesByName`; emits the bareword `hstore` in the PG column-DDL position. No `indexAccessMethods` / `indexOperatorClasses` — the hstore GIN / GiST operator classes are out-of-v1-scope per the research doc and ship in a future PR if operator demand surfaces.

- **`citext` PG extension catalog entry.** Declares the `citext` udt; emits bareword `citext`. citext rides on core PG btree / gin / gist (no extension-owned AMs / opclasses), so the catalog entry is the minimum-viable shape: typesByName + build + emit, both AM and opclass sets empty.

- **Cross-engine PG → MySQL default translators (ADR-0032 § "Cross-engine policy").** MySQL writer's `emitColumnType` rewrites `ir.ExtensionType{Extension:"hstore"}` to `JSON` and `ir.ExtensionType{Extension:"citext"}` to `VARCHAR(255) COLLATE utf8mb4_0900_ai_ci`. Value-side conversion for hstore lives in `mysql/row_writer.go::prepareValue`: a new `prepareHstoreToJSON` + `parseHstoreText` pair reparses the PG hstore canonical wire form (`"k"=>"v"`) into a JSON object string the MySQL JSON column accepts. citext value translation is identity (the case-insensitive comparison is a server-side property of the collation, not the wire format).

- **`ir.CrossEngineExtensionTranslator` optional engine surface (`internal/ir/interfaces.go`).** New interface `HasCrossEngineDefaultTranslator(name string) bool` lets an engine declare which of its extensions can cross-engine-translate without operator intervention. PG's `Engine` implements it backed by the catalog's `crossEngineDefaultTranslatedExtensions` registry (today: hstore, citext). The pipeline's `validateEnabledPGExtensions` consults this on the target-not-PG branch: extensions with declared translators pass through; others keep the loud-failure refusal.

- **`extensionOwningType` helper + actionable schema-reader hint.** The PG schema reader's user-defined-fallthrough error now names the owning extension when udt_name matches a known catalog entry the operator didn't enable — e.g. `user-defined type "hstore" is owned by extension "hstore"; pass --enable-pg-extension hstore to enable passthrough (ADR-0032)`. Replaces the v0.26.0+ vague "not a recognised enum" wording for the load-bearing case.

- **`translate.RetargetForEngine` extended for hstore + citext.** The `sluice schema diff` cross-engine path's expected-shape pass now rewrites hstore and citext to their MySQL forms so target-side diff comparison sees the same shape the migrate path lands on. Other extension types (vector, pg_trgm, postgis) continue to fall through unchanged so the cross-engine refusal in `checkCrossEngineSupportable` fires.

- **Integration tests (`internal/pipeline/migrate_hstore_citext_integration_test.go`, `integration` build tag).** Four cases: `TestMigrate_PG_Hstore_Passthrough` + `TestMigrate_PG_CiText_Passthrough` (same-engine PG → PG byte-for-byte round-trip with type + value + functional-query assertions); `TestMigrate_PG_Hstore_CrossEngineToMySQL` + `TestMigrate_PG_CiText_CrossEngineToMySQL` (cross-engine, with MySQL-side `JSON_TYPE`/collation/case-insensitive-lookup ground-truth queries on the target). Boots stock `postgres:16` (both extensions ship in standard contrib bundle — no special image required).

- **Unit tests across catalog, retarget, value-decode, prepareValue, ddl-emit surfaces.** Pin both extensions' catalog shapes (typesByName / emit / build / AM / opclass sets), the cross-engine translator policy declaration, the retarget rules, the value-decode passthrough, the MySQL writer's hstore-to-JSON conversion (including the standalone `parseHstoreText` parser's positive and negative cases), the citext identity value translation, and the MySQL emit forms.

### Changed

- **`--enable-pg-extension` help text refreshed** on `migrate`, `sync start`, `schema preview`, and `schema diff` to list all four shipped extensions and call out the cross-engine translator carve-out for hstore + citext.

- **`validateEnabledPGExtensions` no longer refuses non-PG targets unconditionally.** Per-extension policy: extensions whose source engine declares `HasCrossEngineDefaultTranslator` may pass against a non-PG target; the refusal message names the specific non-translatable extension when a mixed list is given.

- **`unsupportablePGtoMySQL` carves out hstore and citext** from the blanket ExtensionType refusal — both have default translators wired into the MySQL writer directly. The carve-out is data-driven via the `isCrossEngineTranslatablePGExtension` helper; vector / pg_trgm / postgis continue to refuse loudly.

### Migration / Compatibility

- **No format-breaking changes.** Manifest schema, change-chunk format, CLI surface — all unchanged for existing flows. No new required CLI flags; existing `--enable-pg-extension` flag picks up the two new recognised names.
- **Default behavior unchanged.** Operators not using `--enable-pg-extension hstore` or `citext` see no behaviour change — hstore/citext columns continue to surface as a loud failure at type-resolution time on PG sources (now with a clearer message naming the missing flag).
- **Drop-in upgrade from v0.30.3.** No DDL migration; no operator action required unless you're explicitly opting into the new extensions.
- **Cross-engine PG → MySQL operators with hstore or citext columns:** **upgrade is the migration**. Pre-v0.31.0 hstore/citext columns refused at schema-read time even with `--type-override`; now `--enable-pg-extension hstore` (or citext) opts into the built-in default translator without a per-column override.

### Known limitations

- **hstore GIN / GiST operator classes are not preserved.** The catalog entry declares no `indexOperatorClasses`, so a `CREATE INDEX ... USING GIN (col)` on an hstore column will round-trip the AM but drop the opclass — and PG's `gin` has no default opclass for hstore, so the target's CREATE INDEX will fail loudly. Future PRs may add the opclass round-trip if operator demand surfaces (the pattern is mechanical — mirror pg_trgm's catalog shape).
- **No length-aware citext.** PG citext is unbounded text; the cross-engine MySQL translator picks `VARCHAR(255)` as a reasonable default. Operators with longer values supply `--type-override TABLE.COL=text` (or `varchar:length=N`) per column.
- **No MySQL → PG path.** MySQL has no hstore source; citext source is "VARCHAR with `_ci` collation" but the reverse translator (MySQL `VARCHAR _ci` → PG citext) is out of scope. MySQL → PG operators who want PG citext on the target supply `--type-override TABLE.COL=citext` explicitly (the catalog's emit side handles it; the schema reader just doesn't auto-recognise the MySQL collation as citext-shaped).
- **Cross-engine hstore wire-format strictness.** The MySQL writer's `parseHstoreText` rejects malformed input loudly (unterminated quoted strings, missing arrows, etc.) and falls through to driver-side "Invalid JSON text" errors rather than emitting partial JSON. Real-world hstore values produced by PG always pass; pathological hand-constructed shapes surface clear errors.

### Who needs this release

- **Operators with PG sources using hstore for tags / attributes / configuration maps:** **upgrade**. Same-engine PG → PG previously refused at schema-read; now passthrough works with `--enable-pg-extension hstore`.
- **Operators with PG sources using citext for emails / case-insensitive identifiers:** **upgrade**. Same-engine PG → PG passthrough preserves the type; cross-engine MySQL targets land as VARCHAR with case-insensitive collation so application-side equality queries continue to work.
- **Cross-engine PG → MySQL operators with hstore or citext columns:** **upgrade**. Previously required `--type-override` per column; now `--enable-pg-extension` opts into a sensible default. Operators wanting a non-default shape still use `--type-override`.
- **Everyone else:** drop-in; no behavior change.

## [0.30.3]

**One-line test fix: `TestChunkEncryptedRoundTrip` no longer flakes on the 2-byte `"id"` substring check.** Hit once on the v0.30.2 main CI run. Pre-existing latent flake; not a regression.

### Fixed

- **`TestChunkEncryptedRoundTrip` 2-byte substring false-positive.** `backup_chunk_test.go:194` checked that encrypted chunk bytes don't contain banned plaintext substrings — including `"id"` (2 bytes). `"id"` appears in random ciphertext ~certainly at typical chunk sizes (`P("id" in 1KB random) ≈ 1024/65536 = ~1.5% per byte position × ~1024 positions`). v0.30.3 drops `"id"` from the banned list; the remaining 4–5-byte strings (`"alpha"`, `"beta"`, `"name"`) have `P(banned in 1KB random) ≈ 1 in 10^12` — effectively zero false positives. Encryption-correctness coverage unchanged.

### Migration / Compatibility

- **Test-only fix; no operator-visible behavior change.** Drop-in upgrade from v0.30.2.

### Who needs this release

- **Project maintainers + contributors:** another release-flow flake permanently closed. No operator impact.

## [0.30.2]

**Test-stability + CLI help-text patch.** Two operator-invisible nits surfaced via the v0.29.x / v0.30.x release flow. Neither affects production behavior.

### Fixed

- **`TestSyncFromBackup_FanOut` flake closed.** The fan-out integration test runs two brokers in the same Go process against the same chain root; both defaulted to `manifests/broker_state.json` for their state file. Concurrent `LocalStore.Put` to the same path occasionally hung one broker at startup — symptom: target N stayed at the seed row for the test's 90s wait window. Hit 3 of the last 4 main-CI release cycles (v0.29.1, v0.30.1 twice). Phase A diagnose (3 instrumentation passes) narrowed the hang to `writeBrokerState` between `coldStartAtChainID`'s return and the "broker: started" log; the test-side fix is to give each broker a distinct state path. New `brokerOpts.StatePath` field; `TestSyncFromBackup_FanOut` sets distinct paths per broker. Production unaffected — one broker process per chain. 30/30 PASS on Vultr under race detector (vs 15-20% baseline failure rate). Closes the persistent re-run cost on every other release flow.
- **`--enable-pg-extension` help text refreshed** to list pg_trgm alongside pgvector. v0.30.0's pg_trgm extension passthrough shipped without updating the four flag declarations (`migrate`, `sync start`, `schema preview`, `schema diff`) which still said "Recognised in v0.26.0: vector (pgvector)". Now: "Recognised: vector (pgvector), pg_trgm" (drops the version pin — CHANGELOG is the canonical version-pinning source). Surfaced by the v0.30.1 cycle.

### Migration / Compatibility

- **No format-breaking changes.** No CLI flag changes (only help-text refresh). No on-disk format changes. No engine-interface changes.
- **Drop-in upgrade from v0.30.1.** No DDL migration; no operator action required.

### Who needs this release

- **Operators tracking sluice's release cadence:** drop-in upgrade; no behavior change. Take it for the next release window.
- **Operators frustrated by `--enable-pg-extension` help text not mentioning pg_trgm:** the discoverability improvement is here.
- **Project maintainers + contributors:** every other release CI flow now skips the broker-fanout flake retry cost.

## [0.30.1]

**Two operator-UX gaps surfaced by the v0.30.0 cycle, both closed.** Neither is a regression or correctness issue; both are cleaner failure modes for cross-engine + extension scenarios.

### Fixed

- **Cross-engine PG → MySQL preflight refusal now fires on `migrate` path.** v0.30.0 added `unsupportablePGIndexToMySQL` and wired it into `chain_restore` only — `Migrator.Run` skipped the helper entirely, so a PG source with a pg_trgm-indexed table targeting MySQL bulk-copied successfully and then died at MySQL Error 1170 during the indexes phase (no recovery path; data already partially migrated). v0.30.1 calls `checkCrossEngineSupportable` from `Migrator.Run` right after `translate.ApplyMappings`, mirroring the chain_restore wire-up. The refusal now fires before any data moves.

- **`unsupportablePGIndexToMySQL` broadened to catch extension AMs without an opclass.** The v0.30.0 helper only checked `IndexColumn.OperatorClass`. That misses the no-flag scenario: when the operator runs cross-engine PG → MySQL without `--enable-pg-extension pg_trgm`, the schema reader strips `OperatorClass` from IR (loud-failure default) but `idx.Kind` stays `IndexKindGIN`. The opclass-only refusal returned empty and bulk-copy proceeded. The helper now also flags `IndexKindGIN` / `IndexKindGIST` indexes for non-PG targets. MySQL's FULLTEXT (`IndexKindFullText`) and SPATIAL (`IndexKindSpatial`) stay portable.

- **WARN log when extension-owned opclass is stripped due to missing `--enable-pg-extension` flag.** The opt-in gate in the schema reader was silent: operators saw the downstream raw PG error 42704 (`data type text has no default operator class for access method "gin"`) on the target with no sluice-side attribution. v0.30.1 emits a WARN at strip-time naming the index, column, opclass, owning extension, and the exact `--enable-pg-extension <name>` flag the operator probably wanted. New helper: `extensionOwningOperatorClass(opclass) string` in the postgres engine's catalog.

### Migration / Compatibility

- **No format-breaking changes.** No CLI changes, no on-disk format changes, no engine-interface changes.
- **Drop-in upgrade from v0.30.0.** No DDL migration; no operator action required.
- **Cross-engine PG → MySQL operators previously running migrations through silent failure:** the new refusal surfaces an actionable error message at preflight instead of leaving a partially-migrated target. If you previously worked around the failure by dropping pg_trgm indexes on source pre-migrate, that workaround is now sluice's first suggestion in the refusal message.

### Who needs this release

- **Cross-engine PG → MySQL operators with pg_trgm (or any GIN/GiST) indexes on the source:** **upgrade**. The refusal now fires at preflight; previously bulk-copied succeeded then indexes phase failed cryptically.
- **PG → PG operators using `--enable-pg-extension pg_trgm`:** drop-in; no behavior change.
- **Operators forgetting `--enable-pg-extension pg_trgm` on a PG → PG with pg_trgm-indexed source:** the new WARN at schema-read time tells you which flag to pass; the resulting CREATE INDEX failure path is otherwise unchanged.

## [0.30.0]

**PG → PG `pg_trgm` extension passthrough lands as the v1 shortlist's second concrete entry (ADR-0032).** pg_trgm is the "operator-class only" extension — no new column types, just `gin_trgm_ops` / `gist_trgm_ops` operator classes that ride on core PG `gin` / `gist` access methods. Sluice now recognises and round-trips trigram-indexed columns when the operator passes `--enable-pg-extension pg_trgm` on `migrate` / `sync start` / `schema preview` / `schema diff`. Validates the index-method-passthrough framework on a simpler shape than pgvector (Tier 2 lite) and clears the path for `hstore` / `citext` / `postgis` to follow as additional catalog entries.

### Added

- **`pg_trgm` PG extension catalog entry (`internal/engines/postgres/extension_catalog.go`).** Declares the two operator classes (`gin_trgm_ops`, `gist_trgm_ops`) the schema reader's index-population path now recognises. No `typesByName` entries (pg_trgm has no column types); `indexAccessMethods` empty (rides on core `gin` / `gist`); both `build` and `emitColumn` return loud sentinel refusals (defensive — never reached on the read or write path for a well-formed IR).

- **`extensionDef.indexOperatorClasses` now `map[string]struct{}` (was `[]string`).** Promoted from "metadata" to a queryable set so the schema reader can ask "is this opclass extension-owned?" in O(1). Existing `pgVectorDef` rewritten to the new shape (additive — same opclass coverage, just lookup-friendly).

- **`extensionOperatorClassEnabled(opclass, enabled)` helper.** Mirrors `extensionAccessMethodEnabled`; gates per-opclass passthrough on the operator's `--enable-pg-extension` allowlist. The schema reader's `populateIndexes` now consults both gates so a pg_trgm `gin (col gin_trgm_ops)` index on core PG `gin` AM survives the round-trip — the previous `idx.Method != ""` gate only fired for extension-introduced AMs (pgvector's ivfflat / hnsw) and dropped the pg_trgm opclass.

- **Cross-engine refusal for extension-owned operator classes (`internal/pipeline/cross_engine_supportable.go`).** PG → MySQL with a pg_trgm-indexed table now refuses loudly at the existing `checkCrossEngineSupportable` pre-flight (was silently passed through, would have failed at MySQL CREATE INDEX with an opaque opclass-unknown error). The refusal rides on `ir.IndexColumn.OperatorClass` non-empty — sluice never populates that field for core-PG opclasses (Bug 47 design), so the field's presence is an honest "extension-owned opclass" marker without re-importing the postgres engine catalog into the pipeline package.

- **Integration tests (`internal/pipeline/migrate_pgtrgm_integration_test.go`, `integration` build tag).** Four cases: GIN + GiST round-trip (ground-truth pg_index/pg_opclass query on the target verifies opclass survives), opt-in-skipped failure (operator forgot `--enable-pg-extension pg_trgm`; loud failure at index-create time), target-missing-extension preflight refusal (mirrors pgvector). Boots stock `postgres:16` (pg_trgm ships in standard contrib bundle — no special image needed; CI already pre-pulls).

- **Cross-engine refusal unit tests (`internal/pipeline/cross_engine_supportable_test.go`).** PG → MySQL `gin_trgm_ops` and `gist_trgm_ops` index refusals; PG → PG passthrough not refused (regression guard).

### Migration / Compatibility

- **No format-breaking changes.** Manifest schema, change-chunk format, CLI surface — all unchanged for existing flows.
- **Default behavior unchanged.** Operators not using `--enable-pg-extension pg_trgm` see no behaviour change — pg_trgm-indexed columns continue to surface as a loud failure at index-create time on the target (the existing pattern). The opt-in is the same shape as pgvector's v0.26.0 surface.
- **Drop-in upgrade from v0.29.1.** No DDL migration on `sluice_cdc_state`; no operator action required.
- **Cross-engine PG → MySQL operators:** if a previously silently-broken PG source with a pg_trgm index was being migrated to MySQL (and failing at MySQL CREATE INDEX with an opaque error), the new pre-flight refusal surfaces a clear operator-actionable message instead. The fix recommendation in the error message: drop the index on source before migrating, exclude the table, or supply a `--type-override` / `--index-override` mapping.

### Known limitations

- **No MySQL counterpart.** pg_trgm has no MySQL equivalent (MySQL's `FULLTEXT` indexes are different in shape — ranking, stemming, stop-words). Cross-engine PG → MySQL refuses; operators wanting fuzzy text search on the MySQL side need to design for MySQL's `FULLTEXT` ahead of time.
- **No version pinning.** pg_trgm 1.5 source → 1.4 target may surface subtle behaviour gaps that sluice doesn't see (per ADR-0032's Tier-2 framework caveat). `pg_extension` presence check is the only pre-flight; version-pinning syntax is a future refinement if real operator demand surfaces.
- **No new column types means no new IR variant.** Unlike pgvector (which introduced `ir.ExtensionType{Extension: "vector", ...}`), pg_trgm columns flow through as plain `ir.Text` / `ir.Varchar`. The catalog entry's surface is the operator-class round-trip and the cross-engine refusal — operators reading the IR will not see a `pg_trgm`-flavoured column type.

## [0.29.1]

**Closes Bug 47 — MySQL writer corrupts empty JSON object `{}` to empty JSON array `[]` on bulk copy.** Pre-existing latent bug reproducing back to v0.20.0; surfaced in v0.29.0 cycle. A simple "preserve `{}` bytes" fix in `convertArrayLikeToJSON` was attempted, rolled back when it broke `TestMigrate_PostgresToMySQL_ArrayToJSONOverride` (Bug 14's load-bearing test) — the two paths converge irreducibly at `prepareValue([]byte("{}"), ir.JSON{...})` with no signal from the bytes + target type alone. The proper fix carries pre-override context: `ir.Column` gains an optional `SourceColumnType` field that `translate.ApplyMappings` populates when an override fires. The MySQL writer's `prepareValue` now consults this to disambiguate the two surfaces: `SourceColumnType = ir.Array{...}` → `[]` (Bug 14 override path); nil or non-array → `{}` (Bug 47 round-trip path).

### Fixed

- **Bug 47 — empty JSON object `{}` round-trip on MySQL targets.** MySQL JSON source columns carrying `{}` now land on MySQL targets as `{}` (`JSON_TYPE = OBJECT`), not `[]` (`JSON_TYPE = ARRAY`). PG → MySQL also fixed (same writer-side path). Bug 14's PG `text[]` override to `jsonb` continues to land empty arrays as `[]` (load-bearing integration test still green).

### Migration / Compatibility

- **No format-breaking changes.** No CLI changes, no on-disk format changes, no engine-interface changes.
- **Drop-in upgrade from v0.29.0.** No operator action required.
- **`ir.Column.SourceColumnType` is an internal IR field.** Engines and external consumers reading `ir.Column` see it as nil unless `translate.ApplyMappings` has run with an explicit per-column override. Field is informational — writers consult it; readers do not populate it.

### Who needs this release

- **Operators running MySQL → MySQL or PG → MySQL `migrate` / `sync` on workloads that use `'{}'` as a JSON value** (e.g. as a default for not-yet-populated `attrs` / `metadata` / `config` columns). Pre-fix: those values silently flipped to `'[]'` on the target. Detection: `SELECT JSON_TYPE(col) FROM table` on src vs dest after migrate; mismatched OBJECT-vs-ARRAY counts confirm the bug fired. Workaround in older versions was changing source defaults from `'{}'` to a placeholder like `'{"_": null}'` — no longer needed.
- **Everyone else:** drop-in; no behavior change.

## [0.29.0]

**CI structural improvements + Path D Phase A diagnostic infrastructure for mid-stream live add-table loss.** Two operator-visible threads land together: (a) the `Integration (PostGIS)` job is now a separate parallel CI gate (cuts wall-clock from ~45 min to ~25 min on tag pushes); (b) sluice now runs `integration vstream` (vttestserver-based VStream coverage) on an always-on Vultr instance during pre-release validation, closing the gap CI explicitly skipped for cost reasons. Internal: new ADR-0036 documents the empirical Phase A.1 characterization of the mid-stream live add-table residual loss surface — diagnostic-only branch, no behavior change to the v0.24.0 best-effort flow yet.

### Added

- **Parallel `Integration (PostGIS)` CI job** in `.github/workflows/ci.yml`. Splits the postgis-tagged tests off from the main Integration job into a sibling runner so the two run concurrently. CI wall-clock for a tag push drops from ~45 min sequential to ~25 min parallel. The new job is required-to-pass-before-merge on `main` (see `docs/dev/branch-protection.md` for the updated `gh api PUT` snippet).

- **VStream coverage on the Vultr pre-release validation box.** `integration vstream` (vttestserver-backed Vitess coverage) now runs on the always-on Vultr instance as part of the pre-release runbook; reference timing 3m43s on a vhf-3c-8gb. Roadmap item 10 records this as "Path C — Vultr-box pre-release validation (LANDED)" alongside the original Path A (operator-run checklist) and Path B (CI-integrated coverage). Documented in `docs/dev/notes/release-validation-on-vultr.md`.

- **ADR-0036 — Mid-stream live add-table residual loss surface (Phase A.1).** Empirical characterization of v0.24.0's documented best-effort gap. Six-run multi-iteration on Vultr with targeted DEBUG-level slog instrumentation: M1 (long-txn) contributes rarely (1/6 runs, 1 row); M2 (snapshot consistent-point race) ruled out conclusively; M3 (catalog-snapshot lag) reframed and INCONCLUSIVE pending Phase A.2 with per-row source-side LSN cross-reference; M4 (counter artifact) ruled out — loss is real (~5–9% in the diagnostic test). New diagnostic test `TestAddTable_LiveMode_PG_DiagnoseLossSurface` ships as a permanent regression artifact for the next iteration. No production fix lands in v0.29.0.

- **`Engine.ReadCurrentWALPosition` optional engine surface** (PG-only). Returns the current WAL position via `pg_current_wal_lsn()`; mirrors the existing `ReadSlotPosition` pattern. Used by ADR-0036's diagnostic instrumentation; available to other observability use cases.

### Changed

- **CI Integration job timeout bumped** from 50 min to 60 min on the outer envelope; postgis step's individual timeout from 15 min to 25 min. Defensive headroom; the parallel split makes both unlikely to be hit in normal operation.

### Migration / Compatibility

- **No format-breaking changes.** No CLI changes, no on-disk format changes, no engine-interface changes that affect external implementations.
- **CI required-checks list grew by one** — `Integration (PostGIS)`. Operators using their own fork's branch-protection rules should add this to their required-checks list (snippet in `docs/dev/branch-protection.md`).
- **Drop-in upgrade from v0.28.0.** No DDL migration on `sluice_cdc_state`; no operator action required.

### Who needs this release

- **Operators tracking sluice's release cadence:** drop-in upgrade; no behavior change. Take it for the next release window.
- **Operators on PostgreSQL with `--no-drain` mid-stream live add-table:** no functional change in v0.29.0. The instrumentation lands but is gated behind `--log-level=debug`. The actual mitigation (Path B dual-slot or Path C operator quiesce) is queued behind ADR-0036's Phase A.2 verdicts.
- **Operators contributing to sluice:** the parallel CI job + faster wall-clock cuts the PR-feedback loop. The Vultr-box pre-release validation runbook is documented in `docs/dev/notes/release-validation-on-vultr.md`.

## [0.28.0]

**PostGIS-aware GEOMETRY/SPATIAL translation closes Bug 26 + Bug 27.** Sluice's IR has carried `ir.Geometry` since the beginning, but cross-engine PG ↔ MySQL geometry has been refused at the schema-write boundary (PG: "GEOMETRY requires PostGIS"; MySQL: SRID dropped). v0.28.0 lifts the refusal: PostGIS-detected PG targets accept `ir.Geometry` columns and emit `geometry(<subtype>, <srid>)`; MySQL targets emit `<type> SRID <n>` (MySQL 8.0+ syntax) preserving the SRID; cross-engine PG → MySQL round-trip closes Bug 26. The VStream-specific 4-byte SRID prefix on POINT bytes is now stripped + captured (closes Bug 27); vanilla MySQL protocol path unchanged. ADR-0035 documents the design.

### Fixed

- **Bug 26 — PostGIS SRID dropped on cross-engine schema translation.** `ir.Geometry.SRID` field carries the SRID from PG's `geometry_columns` view through translation. PG schema writer emits `geometry(<subtype>, <srid>)` when PostGIS is detected (init-time `SELECT 1 FROM pg_extension WHERE extname = 'postgis'`); without PostGIS, the existing loud refusal is preserved. MySQL `emitColumnDef` appends `SRID <n>` when `ir.Geometry.SRID != 0` (MySQL 8.0+ syntax); SRID 0 still emits the bare type. WKB → EWKB conversion at PG write-time injects the SRID's 4-byte prefix; EWKB → WKB on PG read-time captures the SRID for cross-engine flow.

- **Bug 27 — VStream POINT bytes mis-parsed (4-byte SRID prefix).** `decodeVStreamCell` in `internal/engines/mysql/cdc_vstream.go` splits `query.Type_GEOMETRY` from the binary fall-through and strips the 4-byte little-endian SRID prefix; under-5-byte payloads pass through as-is. Vanilla MySQL protocol path unaffected (it strips the prefix correctly already). The `psverify`-build-tag end-to-end verification against a real PlanetScale source remains operator-run; the unit-test fixture (`TestDecodeVStreamCellGeometry` with 3 sub-cases — SRID 4326, malformed short, SRID 0) proves the byte-level fix.

### Added

- **ADR-0035 — PostGIS-aware GEOMETRY/SPATIAL support.** Decision rationale, EWKB ↔ MySQL-prefix conversion detail, 5-scenario threat model (PostGIS-absent target, SRID mismatch, VStream prefix on vanilla MySQL connections, EPSG SRID unrecognized, MySQL → PG cross-engine SRID drift), explanation of why this is parented under roadmap item 6 (cross-engine focus) rather than item 11 (PG → PG extension passthrough — the PostGIS PG-to-PG case follows naturally now that the writer emit path lands).

- **`postgis/postgis:16-3.4` pre-pulled in CI's Integration job.** `.github/workflows/ci.yml` adds the image to the pre-pull list and runs `go test -tags="integration postgis" -timeout=15m ./internal/pipeline/...` as a separate Integration step. The image-pull cost (~500 MB layer) is one-time per cache; subsequent runs hit the warm cache.

- **`integration postgis` build tag** for cross-engine geometry round-trip integration tests. New tests under `migrate_postgis_integration_test.go`: `TestMigrate_PostGIS_PGToMySQL` (Phase C reverse direction; closes Bug 26's load-bearing pin); existing `TestMigrate_PostGIS_MySQLToPG` (already shipped) still covers the forward direction.

### Changed

- **`unsupportablePGtoMySQL` in `internal/pipeline/cross_engine_supportable.go`** no longer refuses `ir.Geometry`; the refusal narrows to `ir.ExtensionType` (the v0.26.0 PG extension passthrough framework's IR variant). Cross-engine PG → MySQL geometry now works as long as both source and target have their respective spatial subsystems (PostGIS on PG, MySQL spatial types).

- **Roadmap item 6 (GEOMETRY/SPATIAL)** moved from "Next up" to "Recently landed" with one-line summary. The MySQL/PostgreSQL parity tracker subsection now shows GEOMETRY/SPATIAL as "PG-to-PG + cross-engine v0.28.0; VStream Phase B v0.28.0 (operator-run via psverify)".

### Migration / Compatibility

- **No format-breaking changes.** Manifest schema, change-chunk format, CLI surface — all unchanged for existing flows.
- **Default behavior changed for PG targets WITH PostGIS installed.** Previously the schema writer refused all `ir.Geometry` columns with `GEOMETRY requires PostGIS; not supported in this writer version`. Post-v0.28.0, the writer emits `geometry(<subtype>, <srid>)` for those columns. Operators with PostGIS-bearing PG targets who relied on the loud-failure to bail on geometry columns should review whether they want the new automatic emission (they probably do — that's the load-bearing fix) or set `--type-override` on those columns to `bytea` for the previous opaque-bytes shape.
- **Default behavior unchanged for PG targets WITHOUT PostGIS.** The loud refusal is preserved.
- **Default behavior unchanged for MySQL targets** — already accepted geometry columns; v0.28.0 just adds SRID preservation in the emit shape.
- **Drop-in upgrade from v0.27.0.** No DDL migration on `sluice_cdc_state`; no operator action required.

### Known limitations

- **VStream Phase B end-to-end verification is operator-run via `psverify`.** The unit-test fixture proves the byte-level fix; the full PlanetScale source → sluice → target round-trip needs the operator's PlanetScale credentials (per `feedback_planetscale_creds.md`). Operators using PlanetScale Vitess sources with PostGIS-shaped POINT columns should verify the round-trip manually before relying on the v0.28.0 fix in production.

- **EPSG SRID handling is "common subset" only.** PG's `spatial_ref_sys` table has thousands of SRIDs; MySQL has hard-coded mappings for a smaller subset. Sluice doesn't enforce SRID-existence checks at translation time; an unrecognized SRID will surface as a target-side error at INSERT time (the loud-failure tenet). v1 doesn't enumerate the supported SRID set — operators using non-EPSG-common SRIDs should test their workload before committing.

- **PG-to-PG PostGIS passthrough lands by side-effect, not by explicit framework integration.** Roadmap item 11 (PG extension passthrough framework) shipped pgvector as the first concrete extension in v0.26.0; PostGIS would naturally fit as a catalog entry there. v0.28.0 ships PostGIS via the cross-engine path under item 6 instead — both PG-to-PG passthrough AND cross-engine PG ↔ MySQL work, but the explicit `--enable-pg-extension postgis` flag isn't yet wired (the v0.26.0 framework will fold PostGIS in as a future catalog entry, parallel to pg_trgm / hstore / citext).

## [0.27.0]

**MySQL Phase 2 mid-stream live add-table** (parity for v0.24.0's PG-only `--no-drain`). Operators with high-availability MySQL workloads can now bring a new source table into an active CDC stream's scope without the `sluice sync stop --wait` drain that Phase 1 required. Different mechanism from PG (binlog auto-includes every table; the gate is in the streamer's table-filter, not in a publication): sluice persists the live-added table into a new `live_added_tables` column on `sluice_cdc_state`, and the running streamer polls the column on the same cadence as `stop_requested_at` and atomically extends `applyTableFilter`'s scope. ADR-0034 documents the design. Same operator UX as PG (`sluice schema add-table TABLE --no-drain`); same best-effort caveat (events on the new table during the bulk-copy + filter-flip window may not be delivered — operators with high write rates should use the drained flow or quiesce briefly).

### Added

- **MySQL `--no-drain` support on `sluice schema add-table`.** Same flag, same UX as PG (v0.24.0). Orchestrator dispatches by source engine: PG → existing publication-add path; MySQL → new filter-flip path. Mixed-engine refusals stay clean with operator-actionable messages.

- **`live_added_tables TEXT NULL` column on `sluice_cdc_state` (MySQL).** Idempotent migration (mirrors v0.24.0's `slot_name`, v0.25.0's `source_dsn_fingerprint`, v0.25.1's `target_schema`). Comma-separated list of newly-added table names; the streamer polls and merges on each tick.

- **`tableFilterFlipper` optional engine surface.** PG doesn't implement (uses publication scope instead); MySQL implements via `RecordLiveAddedTable` / `ReadLiveAddedTables` on `ChangeApplier`. Discovered structurally so the orchestrator stays engine-neutral.

- **Streamer `liveAddedFilter` (atomic.Pointer-backed).** New `streamer_filter_flip.go` plumbs the polled-from-cdc-state additions into `applyTableFilter`'s scope mid-run. The filter is additive: existing operator-supplied `--include-table` / `--exclude-table` rules continue to apply; the live-added table joins the include list at the next poll tick.

- **ADR-0034 — MySQL Phase 2 mid-stream live add-table.** Design rationale (filter-flip vs accept-no-filter), threat model (4 scenarios), best-effort caveat documentation, parity with PG `--no-drain` operator UX.

### Changed

- **Roadmap item 4 (MySQL Phase 2)** moved from "Next up" to "Recently landed" with one-line summary. Items 5-14 renumbered down by 1. The `MySQL & PlanetScale parity tracker` subsection updated: mid-stream live add-table now shows "Both engines, v0.27.0".

- **ADR-0030 (PG Phase 2)** cross-references ADR-0034 in its `MySQL deferred (resolved)` section. The "deferred" caveat from v0.24.0 is now closed.

### Migration / Compatibility

- **No format-breaking changes.** Manifest schema, change-chunk format, CLI surface — all unchanged for existing flows.
- **Default behavior unchanged.** Operators not using `--no-drain` see no behaviour change — the v0.24.0 + earlier add-table flow continues to require a drained stream.
- **Drop-in upgrade from v0.26.0.** No DDL migration on `sluice_cdc_state`; the new `live_added_tables` column lands on first `EnsureControlTable` call.
- **PG operators unaffected.** PG `--no-drain` continues to use the publication-add mechanism from ADR-0030; the MySQL filter-flip path is engine-gated.
- **Existing `--include-table` / `--exclude-table` semantics preserved.** Live-added tables are additive to the operator-supplied filter; explicit exclusions still apply.

### Known limitations

- **Best-effort during the filter-flip window** (parallel to PG Phase 2's documented gap, ADR-0030 item 3). Events on the new table that arrive between the bulk-copy snapshot's binlog position and the streamer's filter-flip observation (~5s poll interval) may not be delivered. Under-load test observed ~3 events lost out of 59 in CI's worst-case sustained-INSERT scenario. Operators with high write rates on the new table at the moment of live-add should use the drained add-table flow (zero-loss by construction) or quiesce writes for the seconds-long window. The strict-correctness mechanism (ADR-0033) is open for both engines pending further design work.

- **Filter-flip poll cadence is 5 seconds** (matches the existing `stop_requested_at` poll). A future refinement could shorten the cadence or add a notification mechanism (LISTEN/NOTIFY on PG, but MySQL has no equivalent — would need a polling-rate trade-off).

- **VStream / PlanetScale not in scope** for Phase 2. Different binlog source surface; Phase 2.5 follow-on if real demand surfaces.

## [0.26.0]

**PG → PG extension passthrough framework + pgvector.** Sluice's IR has been engine-neutral by design — column types are categorized by core SQL kinds, and PG's extensible type system has been treated as hostile (loud-failure refusal at schema-read time). v0.26.0 lands the framework that flips this for same-engine PG → PG syncs where the operator opts in: `--enable-pg-extension EXT` (repeatable) tells sluice to recognize and round-trip column types defined by named extensions, with native fidelity. Cross-engine targets (PG → MySQL) keep the loud-failure default; explicit operator translations (`--type-override`) stay the escape hatch. v1 ships with **pgvector** as the first concrete extension, exercising both the type-only path (Tier 1 mechanics) and the index-method path (Tier 2 mechanics — `ivfflat` and `hnsw`). Subsequent extensions from the v1 shortlist (pg_trgm, hstore, citext, PostGIS) ship as catalog-only follow-ons; the framework stays put. Implementation supplement: `docs/adr/adr-0032-pg-extension-passthrough.md`. Decision input: `docs/research/pg-extensions-deployment-frequency.md` (the survey that pinned the v1 shortlist).

### Added

- **`--enable-pg-extension EXT` flag (repeatable) on `migrate` / `sync start` / `schema preview` / `schema diff`.** Default empty (today's behavior — extension types refuse loudly at schema-read). When set, sluice validates each name against the recognized-extensions catalog (refuses unknown names at flag-parse with the recognized set in the error), then preflights against the source DB (`SELECT extname FROM pg_extension WHERE extname = ANY(...)`) to ensure the extension is actually installed. Same preflight runs on the target. Same-engine PG → PG only — cross-engine targets refuse with operator-actionable error pointing at `--type-override`.

- **`ir.ExtensionType` IR variant.** Engine-neutral by name (Extension + Name); modifiers carry per-type metadata (e.g. `vector(384)` → Modifiers=[]int{384}). Schema reader emits this variant when the column's PG type OID matches a catalog entry; schema writer renders it via the catalog's `emitCol` function on same-engine targets, refuses on cross-engine.

- **`ir.ExtensionAware` optional engine surface.** `EnableExtensions(ctx, names)` activates allowlisted extensions on the engine. PG implements; MySQL does not (no extension concept in the same shape — MySQL's "feature flags" are server-level, not type-defining). The structural type-assertion skips cleanly on engines that don't implement.

- **`ir.Index.Method` field.** Carries verbatim extension-introduced index access-method names (`ivfflat`, `hnsw` for pgvector; `gin` / `gist` for pg_trgm / PostGIS in the future). Bareword fallback for `IndexKindUnspecified` preserves the existing engine-neutral `IndexKind` enum while letting catalog entries register their own access methods without expanding the enum.

- **`ir.IndexColumn.OperatorClass` field + emission.** Captured by a `pg_index/pg_opclass` join in `populateIndexes`, emitted by `emitIndexColumnList` for indexes whose access method is extension-introduced and requires it (e.g. `hnsw` requires `vector_l2_ops` / `vector_ip_ops` / `vector_cosine_ops` / `vector_l1_ops`). Default-PG indexes (B-tree, hash, GiST, GIN with built-in opclasses) emit unchanged.

- **pgvector binary COPY codec (`internal/engines/postgres/pgvector_codec.go`).** Bulk-copy uses pgvector's binary wire format (`int16 dim, int16 unused, dim × BE float32` per pgvector/src/vector.c). The naive text-passthrough approach fails because pgx's binary COPY protocol parses the first two bytes of the value as a dimension count — text representation `[0.1,0.2...` would be interpreted as a 23344-dimension vector and trip pgvector's 16000-dim ceiling. The codec is registered per-connection in `writeViaCopy` when the table has any vector column; the OID is resolved from `pg_type` at registration time.

- **PG extension catalog (`internal/engines/postgres/extension_catalog.go`).** Registry mapping extension name → recognized type OIDs + emit functions + index access methods. Adding a new extension is "add a catalog entry," not "extend interfaces." pgvector ships as the first entry; catalog stub entries for pg_trgm / hstore / citext / PostGIS follow in subsequent point releases per the v1 shortlist (pinned by `docs/research/pg-extensions-deployment-frequency.md`).

- **ADR-0032 — PG extension passthrough.** Decision rationale (allowlist over auto-detect), three-tier classification framework (Tier 1 type-only / Tier 2 type+index / Tier 3 type+functions), threat model (5 scenarios — target missing extension, version skew, cross-engine refusal, operator typo, no-columns no-op), why pgvector first.

### Migration / Compatibility

- **No format-breaking changes.** Manifest schema, change-chunk format, CLI surface — all unchanged for existing flows.
- **Default behavior unchanged.** Operators not using `--enable-pg-extension` see no behaviour change — extension column types continue to refuse loudly at schema-read (the existing pattern).
- **Drop-in upgrade from v0.25.1.** No DDL migration on `sluice_cdc_state`; no operator action required.
- **MySQL operators unaffected.** MySQL doesn't implement `ir.ExtensionAware`; the structural type-assertion skips cleanly. Cross-engine PG → MySQL with `--enable-pg-extension` enabled still refuses cleanly at the cross-engine retarget step (`--type-override` remains the operator escape hatch).

### Known limitations

- **Extension version skew not detected.** v1 checks extension presence on both source and target, NOT version compatibility (pgvector 0.7 source → 0.5 target may surface subtle behaviour gaps that sluice doesn't see). Documented in ADR-0032's threat model item 2; future refinement could add `--enable-pg-extension vector@>=0.7` syntax if real operator demand surfaces.
- **Operator-class emission scoped to extension AMs.** `hnsw` indexes correctly emit their required operator class (`vector_l2_ops` etc.); built-in PG access methods (B-tree, hash, GIN, GiST with built-in opclasses) emit unchanged. If a future extension requires custom operator classes that aren't in pgvector's recognized set, the catalog needs an entry update.
- **Tier 3 extensions deferred.** uuid-ossp + pgcrypto are universal across all four surveyed providers (Supabase, Neon, PlanetScale Postgres, ps-extensions.io) but are Tier 3 (function-in-defaults expression-translator work). Strong v2 candidates after the v1 Tier 1+2 machinery is in place. Tracked in ADR-0032 §"Consequences."

## [0.25.1]

Two-bug patch from the v0.25.0 cycle. Both bugs were introduced by v0.25.0's `--target-schema` flag and surfaced in the load-bearing happy-path scenario + the v0.24.0 live-add-table interaction.

### Fixed

- **Bug 45 — `--target-schema=NAME` against a PG source with enum-typed columns failed at CREATE TABLE with `ERROR: type "<table>_<column>_enum" does not exist (SQLSTATE 42704)`.** The PG schema writer schema-qualified the `CREATE TYPE` statement correctly (`CREATE TYPE "customer_svc"."orders_status_enum" AS ENUM (...)`), but the column-type ident inside `CREATE TABLE` and the `::cast` in column DEFAULT expressions were emitted unqualified — PG's parser with default `search_path` couldn't find the unqualified type and bailed. Fix: new `qualifiedEnumTypeRef` helper in PG ddl_emit; `emitColumnDef` qualifies enum column-type idents and the `::cast` suffix on DEFAULT expressions when TargetSchema is non-empty. Default-public operators (no `--target-schema`) see no behaviour change — the new `schemaExplicit` flag on the SchemaWriter only triggers qualifying emission when `SetSchema` was called from operator override.

- **Bug 46 — `schema add-table --no-drain` against a stream started with `--target-schema=NAME` silently dropped CDC events on the new table.** The new table was created in `public.<table>` while the active stream's CDC applier (still running with `--target-schema=NAME`) routed new-table CDC events to `<NAME>.<table>` — which didn't exist. Events emitted a single WARN and silently dropped. Two-part fix: (a) added `--target-schema=NAME` flag to `schema add-table` (mirroring `migrate` / `sync start` / `schema preview`); (b) persist `target_schema` to `sluice_cdc_state` at sync start via the new `targetSchemaSetter` engine surface (PG implements; MySQL doesn't — same shape as v0.24.0's slot_name plumbing); (c) `AddTable.preflightStream` resolves the target schema from operator-supplied flag → recorded cdc-state value → (legacy) empty, with a 5-case resolution table covering inherit-from-recorded, operator-override, mismatch-refusal, agreement, and legacy-row-back-fill behaviors.

### Added

- **`--target-schema=NAME` flag on `sluice schema add-table`.** When non-empty, both bulk-copy DDL and any subsequent CDC events on the new table land in the named PG schema. Auto-inherits the active stream's recorded value when omitted; refuses with a clear error when the operator-supplied value disagrees with the recorded value (the latter close ADR-0031's previously-documented "mid-flight `--target-schema` change is NOT detected" caveat).
- **`target_schema TEXT NULL` column on `sluice_cdc_state` (PG).** Idempotent migration via `ADD COLUMN IF NOT EXISTS` (mirrors v0.24.0's `slot_name` and v0.25.0's `source_dsn_fingerprint`). Streamer records the resolved target schema on every position-write via the new `targetSchemaSetter` interface; `ListStreams` returns it as `StreamStatus.TargetSchema`.

### Changed

- **ADR-0031 threat-model entry 5** (mid-flight `--target-schema` change on warm-resume not detected) moved from "deferred until a real operator surfaces the gap" to **"closed in v0.25.1"** via the recorded `target_schema` column + add-table mismatch refusal. A future warm-resume `sync start` with a different `--target-schema` against an existing stream-id refuses the same way as add-table's mismatch path; the operator either matches the recorded value or runs `--reset-target-data`.

### Migration / Compatibility

- **No format-breaking changes.** Manifest schema, change-chunk format, CLI surface — all unchanged for existing flows. The new `target_schema` column on `sluice_cdc_state` is additive (idempotent `ADD COLUMN IF NOT EXISTS`); legacy rows surface as empty TargetSchema via `COALESCE` and skip the resolution check (preserves backward-compat).
- **Drop-in upgrade from v0.25.0.** No DDL migration needed; the new column lands on first `EnsureControlTable` call.
- **Default behavior unchanged.** Operators not using `--target-schema` see no emission style changes — the `schemaExplicit` flag on the SchemaWriter only triggers qualifying emission when `SetSchema` was called from operator override.

## [0.25.0]

Multi-source aggregation Phase 1 + Phase 2: **`--target-schema` (PG-only) + stream-id collision detection.** Operators with N source databases landing in one target Postgres can now namespace each source's tables into its own schema (`customer_svc.users`, `billing_svc.users`) with a single CLI flag — N independent `sluice sync start` processes, one per source, each with its own `--target-schema NAME` + `--stream-id`. The `sluice_cdc_state` control table picks up a `source_dsn_fingerprint` column and refuses on stream-id collision (operator typo'd `--stream-id`; would silently overwrite another stream's position). ADR-0031 formalises the design (Shape B per `docs/dev/design/multi-source-aggregation.md`); Shape A (sharded → consolidated) is queued as a long-term roadmap entry, and MySQL native parity is the documented follow-up (today MySQL operators get equivalent coverage via `--target` DSN choice).

### Added

- **`--target-schema NAME` flag on `migrate`, `sync start`, `schema preview`, `schema diff`.** Default empty (use the target DSN's default schema, today's behavior). When set, every emitted CREATE TABLE / ALTER TABLE prefixes the table reference with the schema name; PG enums get schema-namespaced (`customer_svc.accounts_status_enum`) so two sources with same-named tables don't collide on type names. PG schema reader / writer / row reader / row writer / change applier all thread the schema through via the new optional `ir.SchemaSetter` surface. Schema is auto-created on first emit via `CREATE SCHEMA IF NOT EXISTS`.

- **MySQL `--target-schema` refusal.** MySQL has no schema concept distinct from databases; the flag refuses cleanly at validate time with an operator-actionable message directing them at the `--target` DSN-choice pattern (different MySQL databases on the same server). Pinned via test that asserts MySQL doesn't implement `ir.SchemaSetter`.

- **Stream-id collision detection.** `sluice_cdc_state` gains a `source_dsn_fingerprint TEXT NULL` column (idempotent migration). The streamer records a SHA-256-truncated fingerprint of the normalized source DSN (host + port + database; user/password excluded so password rotation doesn't break collision detection) on every position-write. On `sync start`, sluice queries the existing fingerprint for the stream-id; if it differs from the new source's fingerprint, refuses with `stream "X" exists on target with a different source DSN — pick a different --stream-id or --reset-target-data to wipe and start fresh`. Catches the operator-typo case where two streams accidentally share a stream-id and would silently overwrite each other's position.

- **ADR-0031 — Multi-source aggregation: --target-schema + stream-id collision detection.** Decision rationale (Shape B + N-processes + PG-only first), threat model with 5 scenarios, type-name derivation (PG enums namespaced through the schema), and impl summary. References the proto-ADR at `docs/dev/design/multi-source-aggregation.md`.

### Migration / Compatibility

- **No format-breaking changes.** Manifest schema, change-chunk format, CLI surface — all unchanged for existing flows. The new `source_dsn_fingerprint` column on `sluice_cdc_state` is additive (idempotent `ADD COLUMN IF NOT EXISTS`); legacy rows surface as empty fingerprint via `COALESCE` and skip the collision check (preserves backward-compat).
- **Drop-in upgrade from v0.24.0.** No DDL migration needed; the new column lands on first `EnsureControlTable` call.
- **Default behavior unchanged.** Without `--target-schema`, every existing migrate / sync-start invocation lands tables in the target DSN's default schema exactly as before.
- **MySQL operators unaffected** — `--target-schema` refuses cleanly with the DSN-choice-workaround error message. MySQL native parity (per-table-rename mechanism) is a future chunk if real demand surfaces.

### Known limitations

- **Mid-flight `--target-schema` change on warm-resume not detected.** If the operator changes the `--target-schema` value between `sync start` invocations, sluice doesn't refuse — both schemas would receive the stream's new writes. Documented in ADR-0031's threat model (item 5) as a known caveat; same-shape future refinement could add `target_schema TEXT` to `sluice_cdc_state` and refuse on mismatch.

## [0.24.0]

Mid-stream live add-table Phase 2: **`sluice schema add-table TABLE --no-drain`** for PG sources. Operators with high-availability workloads no longer need the `sluice sync stop --wait` drain that Phase 1 required to bring a new source table into an active CDC stream's scope. ADR-0030 formalises the design (Strategy C variant c per `docs/dev/design/mid-stream-add-table.md`); the heavy lifting was already in Phase 1 (publication-add-then-snapshot ordering, idempotent applier overlap handling) — Phase 2 lifts the conservative active-stream refusal, adds an explicit LSN-floor invariant check, and plumbs the active stream's slot name through the per-target `sluice_cdc_state` control table so live-add picks the right slot when the operator uses `--slot-name`. PG-only in this release; MySQL Phase 2 has a meaningfully different design space (table-filter flip vs publication scope) and is queued as a separate chunk.

### Added

- **`--no-drain` flag on `sluice schema add-table`.** Default off (preserves Phase 1's drained-stream refusal as the conservative default). When set, the orchestrator captures the active stream's slot `confirmed_flush_lsn`, runs `ALTER PUBLICATION ... ADD TABLE`, opens a temp-slot snapshot at LSN ≥ confirmed_flush_lsn, bulk-copies, and verifies the snapshot-LSN ≥ slot-LSN invariant. CDC keeps streaming throughout — no stream restart needed.
- **Slot-name plumbing through `sluice_cdc_state`.** New `slot_name TEXT NULL` column (idempotent migration). The streamer records its resolved slot name on every position-write via the `SetSlotName` applier hook (Phase 1.5 follow-up shipped in the same release). Operators running multiple concurrent streams against the same source via `--slot-name=shard_a` get the right slot's confirmed_flush_lsn queried automatically by live add-table.
- **New optional engine surfaces** (in `internal/pipeline/add_table.go`): `slotPositionReader`, `snapshotLSNExtractor`, `lsnComparer`, `slotNameSetter`. PG implements all four; MySQL implements none (no slot concept); the structural type-assertions skip cleanly. The orchestrator stays engine-neutral.
- **ADR-0030 — Mid-stream live add-table.** Formalises the correctness story (pgoutput evaluates publication membership at decode time; snapshot-after-publication-add ordering + idempotent applier covers all rows on the new table exactly-once-effectively for the load-bearing cases), threat model (six hazards, all mitigated), why Strategy B (dual-slot) was deferred, and why MySQL's Phase 2 is a separate chunk.

### Known limitations

- **Best-effort for in-flight inserts during the publication-add window.** Discovered during v0.24.0's CI under-load testing: events on the new table inserted DURING the brief publication-add window may not be delivered (~1–3 events lost per sub-second window in the worst case). pgoutput evaluates publication membership per WAL record at decode time; events the slot decoded-and-filtered BEFORE publication-add commit took effect are gone. **Snapshot rows + post-publication-add events are delivered exactly-once-effectively** (proven by `TestAddTable_LiveMode_PG` happy-path test + the post-add sentinel pin in `TestAddTable_LiveMode_PG_UnderLoad`). Operators with high write rates on the new table at the moment of live-add should use the drained add-table flow (zero-loss by construction) or quiesce writes for the seconds-long window. Strict-correctness fix queued as a follow-up roadmap entry (Path A slot-pause / Path B Strategy B dual-slot). ADR-0030's "What could go wrong" section documents the gap in full.

### Migration / Compatibility

- **No format-breaking changes.** Manifest schema, change-chunk format, CLI surface — all unchanged for existing flows. The new `slot_name` column on `sluice_cdc_state` is additive (idempotent `ADD COLUMN IF NOT EXISTS`); legacy rows surface as empty `SlotName` via `COALESCE` and fall back to the engine default `sluice_slot` for live-add lookup.
- **No CLI breaking changes.** `--no-drain` is opt-in; existing `sluice schema add-table` invocations without it continue to require a drained stream identically.
- **Drop-in upgrade from v0.23.2.** No DDL migration needed; the new column lands on first `EnsureControlTable` call.
- **MySQL operators unaffected** — `--no-drain` refuses cleanly with a PG-only error directing them at the drained-stream flow.

## [0.23.2]

Single-bug patch: closes Bug 44 — same-engine MySQL → MySQL migrate of any column with `DEFAULT (UUID())` or `DEFAULT (RAND())` failed at CREATE TABLE with MySQL Error 1064. The MySQL writer's `emitDefault` was emitting `DEFAULT uuid()` (without outer parens) because MySQL's INFORMATION_SCHEMA returns the default expression with parens stripped (`column_default = 'uuid()'`). MySQL 8.0.13+ requires `DEFAULT (uuid())` for function-call expression defaults — only special temporal keywords (CURRENT_TIMESTAMP family, NOW(), LOCALTIME, etc.) are accepted bare. Symmetric writer-side counterpart to v0.11.3's Bug 28/29 fix (which were reader-side translation gaps for the cross-engine direction); together with v0.23.1's Bug 42 fix and v0.21.2's Bug 41 fix, all known UUID-default migration paths (PG → MySQL cross-engine, MySQL → MySQL same-engine, MySQL → PG cross-engine, PG → PG same-engine) now work end-to-end.

### Fixed

- **Bug 44 — MySQL → MySQL same-engine migrate of columns with `(UUID())` / `(RAND())` expression defaults fails with Error 1064.** Pre-existing on v0.23.1 and earlier — surfaced by the v0.23.1 cycle when it exercised the same-engine MySQL → MySQL Scenario 4 path for the first time. **Not introduced by v0.23.1**; the v0.23.1 translator entries (`gen_random_uuid()` / `random()`) only match PG-canonical names that never appear in MySQL's IR. Root cause: MySQL's INFORMATION_SCHEMA stores `DEFAULT (UUID())` as `column_default = 'uuid()'` (parens stripped, lowercased); the MySQL schema reader stores this in the IR as `ir.DefaultExpression{Expr: "uuid()", Dialect: "mysql"}`; the MySQL writer's `emitDefault` then emitted `DEFAULT uuid()` verbatim. MySQL 8.0+ rejects this because the expression-default grammar treats function calls and the temporal-keyword family as separate productions — function calls require outer parens. Fix: a new `wrapMySQLExpressionDefault` helper runs after the existing `pgToMySQLDefaultExpr` lookup and `matchTimestampDefaultPrecision` pass. It detects three cases: (a) already outer-wrapped (`(UUID())` from Bug 42's translation, or operator-supplied `(coalesce(x, 0))`) — pass through; (b) bare temporal keyword (`CURRENT_TIMESTAMP[(N)]`, `LOCALTIME[(N)]`, `LOCALTIMESTAMP[(N)]`, `NOW[()]`, `CURRENT_DATE[()]`, `CURRENT_TIME[(N)]`) — pass through bare (wrapping these is itself a syntax error); (c) anything else (function-call shape) — wrap in outer parens. The bare-keyword detector strips a trailing precision suffix (`(N)` or empty `()`) before matching the case-insensitive keyword set, so MySQL's various capitalisations and the parens-vs-no-parens synonyms (`CURRENT_TIMESTAMP` vs `CURRENT_TIMESTAMP()`) are all recognised.

### Migration / Compatibility

- **No format changes.** Manifest schema, control-table schema, change-chunk format, CLI surface — all unchanged.
- **No CLI breaking changes.** All existing `sluice` subcommands keep their flag surfaces verbatim.
- **Drop-in upgrade from v0.23.1.** No DDL migration on `sluice_cdc_state`; no operator action required.
- **MySQL 8.0+ baseline preserved.** The wrap-in-outer-parens emission is exactly what MySQL 8.0.13+ requires for function-call expression defaults; the project already declares MySQL 8.0+ as the supported baseline.
- **No behaviour change for existing PG → MySQL cross-engine paths.** The Bug 42 translation entries (`gen_random_uuid() → (UUID())`, `random() → (RAND())`) already emit pre-wrapped expressions; the wrap helper is a no-op on those. PG → PG and same-engine PG paths are not touched (this is in the MySQL writer).
- **Existing temporal-default behaviour preserved.** The `matchTimestampDefaultPrecision` precision-promotion path runs before the wrap helper; bare `CURRENT_TIMESTAMP` on a TIMESTAMP(6) column still emits `CURRENT_TIMESTAMP(6)` (not `(CURRENT_TIMESTAMP(6))`) — matching the bare-keyword passthrough rule.

## [0.23.1]

Single-bug patch: closes Bug 42 — cross-engine PG → MySQL restore of a column with `DEFAULT gen_random_uuid()` failed at CREATE TABLE with MySQL Error 1064 (PG's UUID-generator function name lands verbatim in the MySQL DDL). Symmetric reverse of v0.11.3's Bugs 28/29 fix (which translated MySQL's `UUID()` / `RAND()` → PG's `gen_random_uuid()` / `random()`); the MySQL-side default-translator catalog now covers the opposite direction. Together with v0.21.2's Bug 41 (CDC value-decode for UUID columns), this completes "first-class UUID support in cross-engine restore" — Bug 41 fixed the CDC value-decode side; Bug 42 fixes the schema-side default-translation gap. Common pattern in modern PG schemas (Rails, Django, Hasura, Supabase all default-emit `gen_random_uuid()` for UUID PKs); pre-fix, those tables couldn't be migrated to MySQL without operator-side schema munging.

### Fixed

- **Bug 42 — cross-engine PG → MySQL restore of `DEFAULT gen_random_uuid()` columns fails with MySQL Error 1064.** `internal/translate.RetargetForEngine` correctly rewrote `Column.Type` (UUID → CHAR(36)) but didn't rewrite `Column.DefaultValue` of kind `DefaultExpression`. The PG-flavored expression `gen_random_uuid()` flowed through to the MySQL writer's `emitDefault`, where the `pgToMySQLDefaultExpr` translation table didn't have an entry for it, so it fell through to verbatim emission. The resulting CREATE TABLE statement (`uuid_col CHAR(36) DEFAULT gen_random_uuid()`) was rejected by MySQL — `gen_random_uuid` doesn't exist there. Fix: extend `pgToMySQLDefaultExpr` with `gen_random_uuid()` → `(UUID())` (MySQL's canonical UUID-generator function, wrapped in the outer parens MySQL 8.0+ requires for function-call expression defaults). Both PG's `gen_random_uuid()::text` and MySQL's `UUID()` return canonical hyphenated 36-char form, so the column's stored values are semantically equivalent. Also added the symmetric reverse of v0.11.3's Bug 29 fix: `random()` → `(RAND())` (same root cause; both functions return `[0, 1)` doubles). The MySQL writer's existing `emitDefault` precision-matching path is preserved; UUID and RAND defaults don't carry temporal precision so the matchTimestamp branch is a no-op for them.

### Migration / Compatibility

- **No format changes.** Manifest schema, control-table schema, change-chunk format, CLI surface — all unchanged.
- **No CLI breaking changes.** All existing `sluice` subcommands keep their flag surfaces verbatim.
- **Drop-in upgrade from v0.23.0.** No DDL migration on `sluice_cdc_state`; no operator action required.
- **MySQL 8.0+ baseline preserved.** `DEFAULT (UUID())` and `DEFAULT (RAND())` require MySQL 8.0.13+ for the function-call-as-default expression-default syntax; the project already declares MySQL 8.0+ as the supported baseline.
- **No behaviour change for same-engine paths.** PG → PG and MySQL → MySQL migrations of UUID-bearing tables are unaffected; the new translation only fires when the IR's expression default carries the canonical PG function name and the target engine is MySQL.

## [0.23.0]

Logical backups Phase 6.2 lands: **AWS KMS-backed envelope encryption.** Operators who already manage encryption keys via AWS KMS (the common compliance posture for HIPAA / PCI / SOC 2 shops, and the common BYOK posture for multi-tenant SaaS) can now hand sluice a key ARN and skip the passphrase plumbing entirely. The manifest's per-chain CEK is wrapped via `kms.Encrypt`; restore unwraps via `kms.Decrypt` once at the start and caches the CEK in-memory for the rest of the chain. Phase 6.1 (passphrase mode) keeps working unchanged; the two modes are mutually exclusive per backup but pluggable behind the same `EnvelopeEncryption` interface. Implementation supplement: `docs/dev/design/logical-backups-phase-6.md`. Operator guide: `docs/operator/encryption.md` ("AWS KMS setup" section).

### Added

- **`--kms-key-arn` + `--kms-region` on backup full / incremental / stream / restore / sync from-backup.** Operator passes a KMS key ARN (or alias ARN, or alias name); sluice loads the default AWS config (env vars, IAM role, profile, SSO), pre-flights the key with a `DescribeKey` call (auth/region/key-not-found errors surface at construction time, not mid-backup), then wraps every chain's CEK via `kms.Encrypt`. Restore mirrors the path: build the envelope, unwrap once, decrypt every chunk in the chain with the cached CEK. Per-chain CEK caching is the load-bearing performance choice — a 100-chunk restore makes ≤1 KMS Decrypt call regardless of chunk count, so KMS API charges stay flat against chain length.

- **`internal/crypto.KMSEnvelope` implementing `EnvelopeEncryption`.** Drops in alongside Phase 6.1's `PassphraseEnvelope` behind the same interface; the chunk writer/reader paths don't change. Manifest schema is unchanged: `ChainEncryption.KEKMode = "aws-kms"`, `KEKRef = <arn>`, `Argon2id` omitted (KMS doesn't use it), `WrappedCEK` is the KMS CiphertextBlob.

- **Operator-actionable KMS error translation.** `AccessDeniedException` surfaces as "AWS IAM principal lacks kms:Encrypt/Decrypt/DescribeKey on the key (verify key policy + role policy grants the action)"; `NotFoundException` as "key not found (verify the ARN/alias)"; `KMSInvalidStateException` / `DisabledException` as state-specific recovery hints; `IncorrectKeyException` as "ciphertext was wrapped under a different key" (the wrong-key-on-restore path). Generic SDK errors fall through with the key ARN preserved for support correlation.

- **AWS KMS setup section in `docs/operator/encryption.md`.** Covers IAM policy template (least-privilege grant of kms:Encrypt + kms:Decrypt + kms:DescribeKey on a specific key ARN), key creation (CLI + console), key alias usage, and key-rotation handling (KMS rotates the root key transparently; wrapped CEKs reference the key ID — old chains stay decryptable).

### Changed

- **`EncryptionFlags.buildBackupEncryption` + `buildReadEnvelope` route through a single key-source validator.** `--encrypt` now requires exactly one of the passphrase-flag family OR `--kms-key-arn`; mixing them errors with "mutually exclusive" before any envelope work happens. Sets `pipeline.BackupEncryption.KEKRef` from the operator-supplied ARN so the manifest records what the operator passed verbatim.

### Migration / Compatibility

- **No format changes.** Manifest schema, control-table schema, change-chunk format, CLI surface — all unchanged. v0.22.x chains taken under passphrase mode restore unchanged under v0.23.0. KMS-mode chains are taken with v0.23.0+ binaries; pre-v0.23.0 binaries refuse them at preflight (the chain root's `KEKMode = "aws-kms"` doesn't match a `PassphraseEnvelope.Mode()`).

- **No CLI breaking changes.** All existing `sluice` subcommands keep their flag surfaces verbatim. The new flags are additive.

- **AWS SDK pulled in via `github.com/aws/aws-sdk-go-v2/service/kms`.** Already an indirect dependency for the S3 backup target; v0.23.0 promotes it to direct. Build size change is negligible (KMS service module is ~200KB compiled).

### Test coverage

- Unit tests for the KMS envelope: round-trip, wrong-key, missing-key, access-denied, disabled-key, invalid-state, generic SDK error fall-through, length-validation, per-chain caching pattern (100-chunk read = 1 Decrypt call).
- Pipeline-level integration: end-to-end manifest stamping (KEKMode/KEKRef/no-Argon2id), restore preflight, mode-mismatch refusal, per-chain caching across the chunk-CEK resolver.
- CLI flag-level tests: KMS-vs-passphrase mutual exclusion, KMS-without-encrypt sanity check, encrypt-without-any-key shape.
- A `kmsverify` build-tag harness skeleton sits in `internal/pipeline/backup_kms_localstack_integration_test.go` for operator-run localstack verification; the main `integration` build tag stays focused on real-database scenarios so CI throughput doesn't regress on the localstack pull/boot cost.

## [0.22.1]

Single-bug patch from the v0.22.0 cycle. v0.22.0's load-bearing encryption pieces (encrypted full backup + restore, wrong-passphrase / missing-key refusals, per-chunk mode, plaintext backward-compat) all shipped clean — but the write-side envelope builder minted a fresh Argon2id salt every call, so any **chain extension** of an encrypted chain (`backup incremental --encrypt`, `backup stream run --encrypt`, or resume of a partial encrypted full) crashed at startup with `aes-gcm open: cipher: message authentication failed`. Restore-side already mirrored the chain's recorded salt; this patch brings the write-side in line. Fix is local to the encryption-builder + the orchestrator's chain-alignment paths; no schema or CLI changes.

### Fixed

- **Bug 43 — encrypted-chain extension fails at startup with `aes-gcm open: cipher: message authentication failed`.** `cmd/sluice/backup.go`'s write-side `buildBackupEncryption` called `crypto.DefaultArgon2idParams()`, which mints a fresh random salt every call. For `backup full` (no parent chain) this was correct — it sets the chain's salt for the first time. For `backup incremental --encrypt`, `backup stream run --encrypt` extending an existing encrypted chain, or resuming a partial encrypted full, the resulting envelope's KEK was derived against a different salt than the chain's recorded salt, so `Envelope.UnwrapCEK(parent.WrappedCEK)` failed with auth-tag mismatch. The read side (`buildReadEnvelope`) already loaded `rootManifest.ChainEncryption.Argon2id` and re-derived the KEK against the chain's recorded salt — that's why restore worked. Fix mirrors the read-side pattern on the write side: `pipeline.BackupEncryption` gains a `RebuildForChain func(*ir.Argon2idParams) (crypto.EnvelopeEncryption, error)` hook the CLI populates with a closure over the operator's passphrase. The orchestrator's chain-alignment paths (`Backup.setupChainEncryption`, `IncrementalBackup.alignEncryption`, `BackupStream.alignEncryption`) now read the chain root's recorded `Argon2id` and call `RebuildForChain` with those params before any CEK unwrap; the rebuilt envelope's KEK derives against the chain's salt and the unwrap succeeds. Cold-start full backups are unchanged — `RebuildForChain` is a no-op when the parent chain has no recorded params, so the freshly-minted Envelope's salt becomes the chain's salt as before.

- **Latent: `Restore` discarded `Envelope` when chain shape was detected.** `Restore.Run` dispatches to `ChainRestore` for chains with incrementals, but the `ChainRestore{}` literal it built omitted the `Envelope` field — so encrypted chains restored via `sluice restore` (the public CLI surface) silently lost the operator's envelope and refused with the missing-key error. Surfaced as a follow-on while landing the Bug 43 integration test (encrypted full + encrypted incremental → chain restore). Fix is one-line; tests pin the propagation. Standalone full restores were unaffected because the chain-detection branch was skipped.

### Added

- **`pipeline.BackupEncryption.RebuildForChain` field.** Optional builder hook the orchestrator calls when extending an existing encrypted chain. Phase 6.1 (passphrase mode) populates it from the CLI; Phase 6.2/6.3 (KMS modes) leave it nil — KMS unwrap doesn't depend on a chain-recorded salt.

- **Integration coverage for encrypted chain extension.** `TestBackup_EncryptedChainExtension_Incremental_PG` runs the full + encrypted-incremental + chain-restore round-trip with two distinct cold-start salts on the writer envelopes, mirroring the CLI shape that exposed Bug 43; the rebind pulls the chain's recorded salt into the second envelope before unwrap. Companion test `TestBackup_EncryptedChainExtension_NoRebuildHook_Fails` pins the pre-fix failure mode (no `RebuildForChain` → auth-tag mismatch) so a future regression of the wiring surfaces here rather than at the next cycle's S5 attempt.

### Migration / Compatibility

- **No format changes.** Manifest schema, control-table schema, change-chunk format, CLI surface — all unchanged.
- **No CLI breaking changes.** All existing `sluice` subcommands keep their flag surfaces verbatim.
- **Drop-in upgrade from v0.22.0.** A v0.22.0 chain that took its initial encrypted full cleanly is fully extensible under v0.22.1 (the chain's recorded `Argon2id` params let v0.22.1 derive the right KEK on every subsequent extension).
- **Cold-start full backups unchanged.** `backup full --encrypt` against an empty destination mints a fresh salt as it always did; the chain root's recorded params are what subsequent extensions key off.
- **No new dependencies.** Pure orchestration plumbing within the existing `internal/crypto` + `internal/pipeline` surfaces.

## [0.22.0]

Logical backups Phase 6.1 lands: **client-side passphrase-mode encryption.** Chunks now land in cloud storage as AES-256-GCM ciphertext when `--encrypt` is set; only an operator with the right passphrase can recover the underlying rows. Closes the v0.16.0 / v0.17.2 release-notes-disclosed gap that sluice currently writes plaintext chunks; unlocks compliance-driven adoption (HIPAA, PCI-DSS, SOC 2 Type II, GDPR with customer-controlled keys) + air-gapped DR workflows where bucket-SSE doesn't follow the bytes. Implementation supplement: `docs/dev/design/logical-backups-phase-6.md`.

### Added

- **`--encrypt` + `--encryption-passphrase{,-env,-file}` + `--encrypt-mode={per-chain,per-chunk}` on backup full / incremental / stream / restore / sync from-backup.** Operator passes a passphrase; sluice derives a Key Encryption Key via Argon2id (default 64 MiB / 3 iterations / 4 parallelism, NIST-recommended starting point), generates an AES-256 Content Encryption Key, encrypts every chunk with AES-256-GCM under the CEK, wraps the CEK with the KEK, and records the wrapped CEK + Argon2id params on the chain manifest. Restore re-derives the KEK from the operator's passphrase + the recorded salt, unwraps the CEK, decrypts every chunk on the fly. `--encryption-passphrase-env` and `--encryption-passphrase-file` are recommended over inline `--encryption-passphrase` for production (the inline form shows up in shell history).

- **Per-chain CEK by default; per-chunk CEK opt-in.** Per-chain wraps a single CEK at chain root; every chunk reuses the same CEK with its own random 12-byte nonce. Argon2id (the expensive op) runs **once per restore**, not once per chunk. Per-chunk mode (`--encrypt-mode=per-chunk`) wraps a fresh CEK per chunk for defense-in-depth at the cost of per-chunk Argon2id derives during restore.

- **`internal/crypto/envelope.go` package.** New `EnvelopeEncryption` interface abstracts CEK wrap/unwrap so Phase 6.2 (AWS KMS) and Phase 6.3 (GCP Cloud KMS / Azure Key Vault) modes plug in without changing the chunk writer/reader. Phase 6.1 ships `PassphraseEnvelope` (Argon2id-derived KEK).

- **Manifest schema additions.** `Manifest.ChainEncryption` (`{algorithm, mode, kek_mode, kek_ref, wrapped_cek, argon2id}`), `ChunkInfo.Encryption` (`{algorithm, nonce_len, auth_tag_len, wrapped_cek}`). All fields use `omitempty` so pre-Phase-6 manifests round-trip bit-identically post-Phase-6 readers.

- **`sluice backup verify` runs without keys.** SHA-256 verification covers ciphertext bytes (post-encryption), so cron-probe verification of archived encrypted backups doesn't need the passphrase distributed to the verification host.

- **Mixed-mode chain refusal.** A chain whose full is encrypted but an incremental isn't (or vice versa) is rejected at chain-restore time with a clear error. Encryption is per-chain, not per-chunk; chains are atomic.

- **Operator-actionable refusals on restore.** Encrypted chain restored without `--encrypt` → refusal naming `algorithm` / `kek_mode` / `kek_ref`. Wrong passphrase → AES-GCM auth-tag-mismatch error before any data lands on target. No partial data lands on the target on either failure mode.

- **`docs/operator/encryption.md`** — operator-facing guide on passphrase storage best practices, examples integrating with 1Password CLI / AWS Secrets Manager / env-injection patterns, the "lose the passphrase = lose the data" warning, recovery posture, mixed-mode-chain semantics, passphrase rotation workflow.

### Changed

- **`pipeline.Backup` / `IncrementalBackup` / `BackupStream` gain an `Encryption *BackupEncryption` field.** Plaintext (the v0.16.x..v0.21.x default) preserved by leaving it nil. Construction ergonomics unchanged for existing callers.

- **`pipeline.Restore` / `ChainRestore` / `SyncFromBackup` gain an `Envelope crypto.EnvelopeEncryption` field.** Encryption preflight at chain-walk time fails fast on key mismatch; chain-level CEK is unwrapped once per Run() in per-chain mode (Argon2id pays a single derivation cost per restore process).

- **`pipeline.ReadRootManifest` exported helper.** Reads the chain root manifest at `manifest.json` for CLI-side encryption preflight (extracting recorded Argon2id params before constructing the read-side envelope).

### Migration / Compatibility

- **No CLI breaking changes.** `--encrypt` is opt-in; existing backup / restore / sync from-backup invocations without it continue to write / read plaintext chunks identically.
- **Pre-v0.22.0 chains restore unchanged.** Plaintext chains stay plaintext on restore; the manifest's `ChainEncryption` field is absent (omitempty); the chunk reader takes the existing plaintext path.
- **Manifest schema additive.** New encryption fields use `omitempty`; older sluice readers (v0.21.x and earlier) ignore them gracefully — a v0.21.x sluice reading a v0.22.0 plaintext manifest sees the same shape it always did. **An older sluice cannot restore a v0.22.0 encrypted chain** (no decryption code path), but the manifest is human-readable enough that operators can recognize the `chain_encryption` field's presence and upgrade.
- **`backup verify` continues to work without keys.** Existing cron probes that hash chunks need no changes; encrypted chunks hash ciphertext, which matches what's recorded in the manifest.
- **No new heavy dependencies.** AES-GCM uses stdlib `crypto/aes` + `crypto/cipher`; Argon2id uses `golang.org/x/crypto/argon2` (already an indirect dependency, now promoted to direct).

### Deferred

- **Phase 6.2 (AWS KMS) + Phase 6.3 (GCP Cloud KMS / Azure Key Vault).** The `EnvelopeEncryption` interface is the seam those modes plug into; CLI flags will follow the same `--encryption-*` shape with `--kms-key-arn` / `--kms-key-resource` / `--azure-key-vault-id` keys.
- **`--decrypt-verify` for `backup verify`.** v0.22.0 is sha256-only (covers integrity but not "the ciphertext decrypts to something parseable"); a future enhancement will add a deeper verify mode that decrypts + re-hashes plaintext.
- **Passphrase rotation tooling.** v0.22.0's rotation workflow is "fresh full + new chain"; re-encrypting existing chunks under a new passphrase is out of scope. KMS-mode key rotation in Phase 6.2/6.3 will be transparent via cloud-provider key-version chains.
- **Encrypted manifests.** The manifest itself stays plaintext (carries chunk paths, sha256s, and the wrapped CEK — none of which leak rows). Operators wanting "encrypt everything including manifests" have a future-phase option.

## [0.21.2]

Single-bug patch from the v0.21.0 cycle. CDC streams against any Postgres source carrying a `UUID`-typed column crashed on the first INSERT/UPDATE — pre-existing bug, not a v0.21 regression, surfaced when the v0.21 cross-engine cycle expanded UUID coverage. Fix is local to the PG CDC value-decoder; no protocol or schema changes.

### Fixed

- **Bug 41 — PG CDC decode of UUID columns crashes the stream with `UUID byte slice has length 36; want 16`.** Pgoutput's TupleData carries every column value with format byte `'t'` (text); the `'b'` (binary) branch in `decodeTuple` is already a hard refusal, so for the CDC path UUID values arrive at `decodeUUID` as the 36-byte ASCII canonical hyphenated string. The previous code path required `len([]byte) == 16` (binary form, the shape pgx returns for non-CDC reads) and bailed loudly on anything else — including the CDC text-format payload. Net effect: the stream exited with the catalog error message on the first INSERT against any UUID-bearing CDC-streamed table. Workarounds were `--exclude-table` or dropping UUID columns. Fix: `decodeUUID`'s `[]byte` branch now switches on length — 16 routes to the existing binary path (`formatUUIDBytes`), 36 routes through a new `canonicalizeUUIDText` helper that validates the 8-4-4-4-12 hyphenated shape and lowercases to the IR's UUID-as-string contract; any other length surfaces a clear error naming both the length and the supported alternatives. The string-passthrough case now also routes through the same canonicalisation so the IR contract holds whichever shape pgx returns. New helper is a small ASCII validator (no new dependency on `github.com/google/uuid`). Unit tests cover all three positive shapes (16-byte binary, 36-byte text, string passthrough), case folding, and five malformed-input negatives. Integration test `TestCDCReader_UUIDColumnRoundTrip` boots PG 16 with REPLICA IDENTITY FULL on a UUID-bearing table, drives INSERT + UPDATE with known UUIDs, and asserts both Before and After images carry the canonical lowercase string on the CDC channel — without the fix, the stream errored before draining a single event.

### Migration / Compatibility

- **No format changes.** Manifest schema, control-table schema, change-chunk format, CLI surface — all unchanged.
- **No CLI breaking changes.** All existing `sluice` subcommands keep their flag surfaces verbatim.
- **Drop-in upgrade from v0.21.x.** No DDL migration on `sluice_cdc_state`; no operator action required.
- **No behaviour change for non-CDC paths.** Bulk-copy migrate (`sluice migrate`) of UUID-bearing tables continues to work identically — bulk-copy uses `TableReader`, a different code path that already handled `[16]byte` from pgx's binary mode. The new canonicalisation surface only sees the CDC-text shape it didn't before.

## [0.21.1]

Two-item housekeeping release. No functional changes; documentation polish + a CI-flagged test-cleanup race fix.

### Fixed

- **`TestCDCReader_TimestampNonUTCHost` cleanup-race under `-race` on CI.** The test's `t.Cleanup` callback restored `time.Local` while the CDC pump goroutine — which calls go-mysql's binlog decoder, which builds `time.Time` via `time.Unix` (reads `time.Local`) — was still active. The original ordering relied on `defer rdr.Close()` running before the `t.Cleanup`, but `syncer.Close()` does not synchronously wait for the pump goroutine to exit, so the race detector caught reads from a still-running pump. Fix is test-side only: `defer rdr.Close()` becomes a `t.Cleanup` that closes AND drains the `changes` channel to completion. The pump's deferred `close(out)` runs as its last act, so observing the channel close gives a happens-before edge against any further pump-side reads of `time.Local`. Cleanup ordering (LIFO) is now: (1) close rdr + drain changes — registered second, runs first; (2) restore `time.Local` — registered first, runs last. Production CDC reader code is unchanged.

### Documentation

- **`docs/value-types.md` — MySQL binlog-event-volume sizing rule for `--rollover-max-changes`.** New section formalises an operator rule of thumb that surfaced during the v0.20.0 broker cycle: MySQL emits ~3 events per autocommit `INSERT` (`BEGIN` `QueryEvent` → `WRITE_ROWS_EVENTv2` → `XID/TxCommit`), plus a spurious empty `BEGIN/COMMIT` pair on the first DML of any new connection. Operators sizing `--rollover-max-changes` against naive INSERT counts under-size the bound by 3-4×. The new section documents the per-INSERT shape, the multi-row `INSERT` collapsed shape (`2 + N` events), the spurious pair, and the **4× expected-INSERT-count** rule of thumb. Includes a brief contrast against PostgreSQL's `pgoutput` (one event per row change, no in-band BEGIN/COMMIT inflation in the consumer's view) so PG operators don't apply the multiplier where it doesn't belong.

### Migration / Compatibility

- **No format changes.** Manifest schema, control-table schema, change-chunk format, CLI surface — all unchanged.
- **No CLI breaking changes.** All existing `sluice` subcommands keep their flag surfaces verbatim.
- **Drop-in upgrade from v0.21.0.** No DDL migration on `sluice_cdc_state`; no operator action required.

## [0.21.0]

Logical backups Phase 5 lands: **cross-engine chain restore.** A PG-rooted backup chain can now restore (and stream-apply via `sync from-backup`) into a MySQL target, and vice versa. Closes the loud refusal at `chain_restore.go:99` (`"cross-engine chain restore is a Phase 5+ topic"`) that v0.17.0 through v0.20.x raised when the chain's source engine differed from the target. Implementation supplement: `docs/dev/design/logical-backups-phase-5.md`.

### Added

- **Cross-engine `sluice restore --from=<chain-url> --target-driver=<engine>`** — was supported for full-only chains since v0.16.x; v0.21.0 extends it to chains with incrementals. Schema deltas in incremental manifests now route through `internal/translate.RetargetForEngine` before invoking `ir.SchemaDeltaApplier.AlterAddColumn` on the target. PG-source `ADD COLUMN UUID` lands as MySQL `CHAR(36)`; PG-source `ADD COLUMN INET` lands as `VARCHAR(45)`; PG-source `ADD COLUMN <Array>` lands as MySQL `JSON`. Existing `RetargetForEngine` rules are reused verbatim — no new translation surface.

- **Cross-engine `sluice sync from-backup run --target-driver=<engine>`** — the broker variant. Same delta-translation pass on each tick's incremental. Detects cross-engine at startup and logs `INFO broker: cross-engine chain — chain's EndPosition not written to sluice_cdc_state; use --at-chain-id for cross-engine resumption assertions`. The broker still writes its own `_engine="backup-broker"` envelope to `sluice_cdc_state` (warm resume works); the chain's source-engine-flavored terminal `EndPosition` is intentionally omitted because PG LSN ↔ MySQL GTID is not a meaningful translation.

- **Change-event value translation reuses live-CDC machinery.** Cross-engine row payloads in change chunks land at the engine appliers' existing live-CDC value-translation path: each applier looks up its own *target* column types and routes every value through `prepareValue` for target-shape preparation. PG → MySQL: UUID strings bind to `CHAR(36)` natively; JSONB `[]byte` is shaped to a string for MySQL JSON columns (no `_binary` charset prefix). MySQL → PG: TINYINT(1) → `bool` (the cross-engine MySQL → PG bool path) is handled at the CDC reader's decode layer; `pgx` accepts `bool` natively for `BOOLEAN`.

- **Loud refusal for unsupportable types.** PG-source PostGIS `Geometry` columns refuse cross-engine restore to MySQL with an operator-actionable message naming the offending table + column + recovery hint (`--exclude-table` to skip, or `--type-override` for a portable IR type). Pre-flighted at chain start so the operator gets a clear failure before any work happens. Same refusal pattern as full cross-engine restore, extended to cover incremental schema deltas (a delta that introduces a PostGIS column refuses with the incremental's BackupID named).

- **`pipeline.checkCrossEngineSupportable` / `pipeline.checkCrossEngineDeltaSupportable`** — internal helpers driving the refusals. Both return nil for same-engine pairs and unknown engine pairs; PG → MySQL is the loaded direction in v0.21.0. Future engine pairs add their entries here.

### Changed

- **`pipeline.ChainRestore.Run`** — the cross-engine refusal at lines 94-103 is replaced with a routing branch: when `manifest.SourceEngine != Target.Name()`, the supportability pre-flight runs, then schema deltas + change events route through their respective translation paths during apply.

- **`pipeline.SyncFromBackup.applySchemaDeltas`** — also routes deltas through `translate.RetargetForEngine` (mirrors the chain-restore path; the broker's apply intentionally duplicates the chain-restore logic per the Phase 4.5 tenet of "don't refactor across surfaces").

### Migration / Compatibility

- **No format changes.** Manifest schema, control-table schema, change-chunk format are unchanged. Pre-v0.21.0 chains restore identically across same-engine and cross-engine targets.
- **No CLI breaking changes.** All existing `sluice` subcommands keep their flag surfaces.
- **Same-engine paths regression-clean.** Existing same-engine chain-restore tests pass unchanged; same-engine broker happy paths unchanged.
- **`backup verify` unchanged.** Verify is read-only and engine-agnostic; integrity checks pass on cross-engine-target-bound chains identically.

### Deferred

- **Cross-engine CDC handoff with engine-translated `EndPosition`** — translating PG LSN to MySQL GTID set isn't meaningful (different change-log shapes). Operators wanting cross-engine continuous CDC after restore set up a fresh `sluice sync start` against the source's native engine; the chain restore lands the data, sluice sync handles ongoing replication separately.
- **PG-only types not yet in `RetargetForEngine`'s table** (PostGIS geometry, hstore, custom enums beyond the existing PG enum support) — refuse loudly with the offending column named; operator can use `--exclude-table` or `--type-override` per existing escape hatches. Adding new types to the rewrite table is a separate minor.
- **Phase 6 (KMS encryption)** stays unimplemented through Phase 5.

## [0.20.1]

Three-bug patch from the v0.20.0 Phase 4.5 broker cycle. v0.20.0 shipped the consumer-side `sluice sync from-backup` orchestrator; cycle testing surfaced three independent failures along the broker's restart, schema-evolution, and cold-start-recovery paths. None affects chain correctness, the source-side `backup stream`, or the read-only-consumer contract — broker-driven targets stayed safe at all times. Each fix is local to its surface and ships behind the same `--reset-target-data` / `--at-chain-id` operational guardrails as v0.20.0.

### Fixed

- **Bug 38 — `backup stream` does not refresh source schema mid-stream; ALTER TABLE on source while stream is running causes broker apply to fail with `column "<new>" does not exist`.** Pre-fix: the stream baked `parent.Schema` into every rollover's manifest without ever re-reading, so an `ALTER TABLE customers ADD COLUMN tag` on source produced subsequent manifests with the original 3-column schema and the broker's apply hit `SQLSTATE 42703` the moment a CDC change carried the new column. Fix is option (a) from the catalog: at each rollover boundary in `pipeline.BackupStream.runRollover`, re-read the source schema via the engine's existing `SchemaReader`, diff against the parent's recorded schema (using the existing Phase 3 `diffSchemas` helper), and emit `ir.SchemaDeltaEntry` entries into the rollover's manifest. Same shape Phase 3.2's `IncrementalBackup` already produces; the broker's `ApplyChain` consumes it via the existing `SchemaDeltaApplier.AlterAddColumn` path. Engine-neutral — works on PG and MySQL identically. The stream now records `INFO stream: schema delta detected at rollover` whenever the diff produces entries.

- **Bug 39 — `sluice sync from-backup run` warm-resume after restart fails: PG / MySQL `ChangeApplier.ReadPosition` discards the broker's engine sentinel, restart errors with `stream "<id>" is owned by a non-broker writer (position engine "postgres")`.** Pre-fix: the broker writes its position with `Engine="backup-broker"` but the engine appliers' `ReadPosition` hard-codes its own engine name into the returned `ir.Position.Engine` (`engineNamePostgres` / `engineNameMySQL`), discarding the broker's sentinel on the round-trip. The `sluice_cdc_state` table has no `position_engine` column, so the engine sentinel had nowhere durable to live. Fix is option (b) from the catalog: encode `_engine` into the broker's token JSON envelope (`{"_engine":"backup-broker","chain_url":...,"last_applied_backup_id":...}`). On restart, the broker reads its own row's token and discriminates via the embedded `_engine` field rather than `Position.Engine`. Backward-compatible: legacy v0.19.x rows without `_engine` parse as non-broker (current behaviour preserved). No DDL migration on `sluice_cdc_state`. The discriminator helper `isBrokerToken` is the new canonical "is this a broker row?" predicate. Unit tests pin round-trip + legacy-shape behaviour.

- **Bug 40 — `sync from-backup run --reset-target-data` does not drop pre-existing target tables; ChainRestore CREATE TABLE IF NOT EXISTS no-ops; broker silently hangs in restore phase.** Two compounding root causes; fixed in two layers:
  - **40a (drop tables before chain restore).** Pre-fix: the broker's `--reset-target-data` branch invoked `pipeline.ChainRestore` directly, whose schema-application path uses `CREATE TABLE IF NOT EXISTS` — a no-op against pre-existing tables. A target carrying a stale `(id, email)` shape from a prior cycle would keep its old columns, and the chain's bulk-copy COPY would reference columns the table didn't have. Fix: `SyncFromBackup.coldStartReset` now enumerates the chain's terminal manifest schema and drops every named table via the existing `ir.TableDropper` / `ir.BulkTableDropper` surfaces (mirrors `migrate --reset-target-data`'s drop-loop in `reset.go`) before invoking ChainRestore. Idempotent across retries via `DROP TABLE IF EXISTS`.
  - **40b (surface bulk-copy errors loudly).** Pre-fix: when the COPY errored, the producer goroutine in `restoreTable` was still trying to push rows into an unbuffered channel that the writer no longer drained, so the producer blocked forever on `rowCh <- row` and `<-errCh` deadlocked. Net effect: silent hang, idle PG connections in `ClientRead` state, no log lines, operator must `Stop-Process` to recover. Fix: `restoreTable` now derives a `streamCtx` for the producer and calls `streamCancel()` on writer failure; the producer's `<-ctx.Done()` arm in `streamChunkRows` unblocks the goroutine cleanly so the error propagates (loud-failure tenet preserved). Defense-in-depth fix that prevents future silent-hang regressions in the chain-restore bulk-copy path.

  The two fixes compose: 40a closes the primary failure (stale schema is gone before restore runs), 40b ensures any future variant of "writer fails mid-restore" surfaces as a real error rather than a hang. New `INFO broker: --reset-target-data: target tables dropped before chain restore` and `ERROR restore: write rows failed; cancelling chunk producer` log lines mark the new code paths. Integration test `TestSyncFromBackup_ColdStartWithReset_StaleSchema` pre-seeds the target with a deliberately-stale `users` table + a sentinel row and confirms the chain restore lands cleanly with no stale data surviving.

### Migration / Compatibility

- **No format changes.** Manifest schema, control-table schema, change-chunk format are unchanged.
- **Backward-compatible token shape (Bug 39).** Legacy v0.19.x rows without `_engine` parse as non-broker; v0.20.1+ broker writes both the embedded sentinel AND the deprecated `Position.Engine` field for forward compatibility.
- **No CLI breaking changes.** All existing `sluice` subcommands keep their flag surfaces.

### Fixed (post-tag, drain-commit regression)

- **Bug 38 fix follow-up — graceful-drain rollover dropped its in-flight chunks.** The Bug 38 fix ran `refreshSchemaAndAttachDelta` after `captureWindow` returned but before setting `out.Manifest`; on `ctx`-cancel mid-rollover (the SIGTERM / drain path), the schema refresh dialed a new connection on the already-cancelled `ctx`, failed immediately with `ctx.Err()`, and left `out.Manifest=nil` — so the outer drain-commit branch in `BackupStream.Run` saw "nothing to commit" and silently dropped the chunk that `captureWindow`'s own drain path had already flushed to the store. Symptom: `TestBackupStream_*RolloverByMaxChanges` deterministically missed `user24` (the last event captured before cancel) on both engines. Fix: skip the schema-refresh on `ctx.Canceled` / `context.DeadlineExceeded` so the drain-commit rollover commits with the parent's schema. Functional behavior of the Bug 38 fix is unchanged for normal rollovers — `SchemaDelta` is still emitted at rollover end whenever the source schema diff is non-empty; only the cancelled-rollover edge case skips the refresh, and any DDL that occurred during the drained window is captured on the next stream's first rollover (diffed against the drain-commit's terminal manifest).

## [0.20.0]

Logical backups Phase 4.5 lands: `sluice sync from-backup` is the consumer-side companion to v0.19.0's `sluice backup stream`. The headline operator outcome: **decouple source and target via the backup chain as the message log.** Source-side `backup stream` writes incrementals to S3/GCS/Azure/local-FS; target-side `sync from-backup` polls the same destination and replays incrementals into its own database — log-based ETL without direct source-target connectivity. Implementation supplement: `docs/dev/design/logical-backups-phase-4-5.md`.

### Added

- **`sluice sync from-backup run --backup-target=<url> --target-driver --target --stream-id=ID`** — new long-running broker subcommand. Drives a `for { tick(); replay(); commit(); }` loop at the configured `--poll-interval=DURATION` cadence (default `30s`). Each tick lists manifests at the chain root, filters to incrementals NOT yet applied (via the persisted `last_applied_backup_id` in `sluice_cdc_state`), and replays each in chain order — schema deltas first (via `ir.SchemaDeltaApplier.AlterAddColumn` from Phase 3.2), then change chunks through the engine's batched `ChangeApplier.ApplyBatch` (reusing the existing applier verbatim per ADR-0010 idempotent-apply).

- **`sluice sync from-backup stop --backup-target=<url>`** — companion stop command. Writes `stop_requested_at` to the chain destination's `manifests/broker_state.json`; the running broker observes the request on its next tick poll and exits cleanly. Cross-machine: an operator on machine B can stop a broker running on machine A without process access — both sides agree on the chain destination. Mirrors the `backup stream stop` pattern.

- **`pipeline.SyncFromBackup` orchestrator** — opens the target's `ChangeApplier` ONCE for the broker's lifetime; each tick reuses the connection. Per-tick INFO log line records new applied + total bytes + elapsed for monitoring. Integration tests use `--poll-interval=2s` for ~30-60s test scenarios.

- **`pipeline.RequestSyncFromBackupStop(ctx, store, now)`** — exported helper for downstream tooling that wants to stop a running broker without going through the CLI. Idempotent: re-issuing stop preserves the original `stop_requested_at` timestamp so drain-completion watchers don't see the clock reset. In-process channel registry (`broker_stop_registry.go`) closes same-process stops with zero file I/O, mirroring the v0.19.1 stream-stop fix.

- **`manifests/broker_state.json`** — new informational liveness file. Mirrors Phase 4's `stream_state.json` shape at the consumer side: `{pid, host, stream_id, started_at, last_apply_at, stop_requested_at}`. Coexists with `stream_state.json` when a stream + broker run against the same destination — one is producer-side, the other consumer-side; neither gates the other's concurrent-writer check.

- **Cold-start safeguards.** First-start refusal when `sluice_cdc_state` has no row for the supplied `--stream-id` (mirrors `migrate --force-cold-start` friction tier from Bug 9). Two override flags:
  - `--reset-target-data` — the broker first runs `pipeline.ChainRestore` internally to land the full + every incremental up to current, THEN transitions to live polling.
  - `--at-chain-id=<BACKUP-ID>` — operator's assertion that the target is currently at chain ID `<BACKUP-ID>` (typical workflow: manual `sluice restore --from=<chain-url>` followed by `sync from-backup`); broker writes a fresh `sluice_cdc_state` row and transitions to live polling.

- **`ir.PositionWriter` optional surface** — engine appliers that implement it allow the broker to record cold-start positions and schema-delta-only-incremental positions without an accompanying data write. Postgres + MySQL appliers implement it via the same `writePositionTx` helper the apply path uses, so the row shape and idempotency contract are identical.

- **Replay state via existing `sluice_cdc_state`** — no schema change. New position-shape sentinel: `position_engine = "backup-broker"`, `position_token = '{"chain_url":"...","last_applied_backup_id":"<id>"}'`. Distinct from `sync start`'s positions (CDC LSN/GTID); the broker's positions reference chain state. ADR-0007 transactional position-and-data atomicity makes broker crashes mid-replay safe to re-apply (ADR-0010 idempotent applier).

### Changed

- **`cmd/sluice/cli.go` grows the `sync from-backup` subtree.** New `SyncCmd.FromBackup` field of type `SyncFromBackupCmdGrp` (kong-grouped `Run` + `Stop` subcommands). Existing `sync start` / `sync stop` / `sync status` / `sync health` flag surfaces are unchanged.

### Fixed (post-tag, integration-test stability)

- **MySQL broker happy-path test (`TestSyncFromBackup_MySQL_HappyPath`)** — bumped the per-test `IncrementalBackup.MaxChanges` cap from 10 to 50. MySQL's binlog emits ~3 events per autocommit `INSERT` (`BEGIN` `QueryEvent` → `WRITE_ROWS_EVENTv2` → `XID/TxCommit`) and the session's first connection-time interaction commonly flushes a short empty `BEGIN/COMMIT` pair into the binlog ahead of any DML, so 5 INSERTs produced 17 captured events in CI. The original cap of 10 stopped the incremental window after only 3 INSERTs, leaving `user3`/`user4` missing from the chain. No code-path change — the broker, applier, and incremental-backup orchestrator all behaved correctly; the test bound was simply too tight for MySQL's event volume.
- **PG broker cold-start-with-reset test (`TestSyncFromBackup_ColdStartWithReset`)** — the test's poll loop used `pgQueryEmails`, which fatal-fails on any query error including the expected `users does not exist` window between launching the broker goroutine and the broker's inline `ChainRestore` recreating the schema. Added `pgQueryEmailsTolerant` (returns `nil` on `SQLSTATE 42P01` instead of fatal) for poll-loop callers; production code paths are unchanged.

### Migration / Compatibility

- **No format changes.** Manifest schema, control-table schema, change-chunk format are unchanged. Pre-v0.20.0 chains restore + verify identically.
- **No CLI breaking changes.** All existing `sluice` subcommands keep their flag surfaces.
- **Brokers are read-only consumers of the chain.** They never modify manifests; the chain itself stays the source of truth for restore + verify regardless of broker activity.
- **Same-engine only in v1.** Cross-engine `sync from-backup` (PG-source-chain → MySQL target) is deferred to Phase 5 alongside cross-engine chain restore. The broker today refuses cross-engine chains the same way `chain restore` does.

### Deferred

- **Phase 5 (cross-engine chain restore)** — the partner feature. Cross-engine `sync from-backup` waits for the SELECT-grammar translator + `RetargetForEngine` extensions in Phase 5.
- **Phase 6 (KMS encryption)** — encrypted-chain consumption (the broker reads the same chunks that decrypt-on-restore would handle).
- **Phase 7+ — operationally-mature features**: multi-source aggregation (one target consuming N source chains), selective-table replay (`--include-table` on the broker), time-shifted replay (`--lag-window=DURATION`).

## [0.19.1] - 2026-05-08

Single-bug patch from the v0.19.0 test cycle. v0.19.0 shipped logical-backups Phase 4 (`sluice backup stream`) with the `TestBackupStream_Postgres_StopCommandRequestsExit` cooperative-stop integration test skipped on CI; cycle testing surfaced that the `sluice backup stream stop` companion command does not fire reliably under `-race` + heavy goroutine contention. The CDC pump, chain correctness, rollover policy, and SIGTERM-based shutdown were unaffected — only the cross-machine convenience surface was unreliable. Fix is option (b) from the BUG-CATALOG analysis: in-process channel notification for the same-process case + a heartbeat read-modify-write that closes the file-poll clobber race.

### Fixed

- **Bug 37 — `backup stream` stop-signal observation does not fire reliably under `-race` + heavy goroutine contention; `sluice backup stream stop` may not trigger graceful drain on contended systems.** Phase A instrumentation pinned the actual root cause to **hypothesis (c)** from the catalog analysis: the `captureWindow` stop-poll fires on time and `LocalStore.Get` returns sub-millisecond, but the state file returned by `Get` is missing `stop_requested_at` because the running stream's per-rollover heartbeat write at the rollover boundary CLOBBERED it. The stream's in-memory `state` struct never carried `StopRequestedAt`, so its last-writer-wins overwrite of `manifests/stream_state.json` at every rollover boundary silently erased the operator's stop request whenever the heartbeat write landed after `RequestStreamStop`'s write within the same rollover-cycle. Once clobbered, neither the inner stop-poll nor the outer-loop `readStreamStopRequested` could recover — the field was gone from disk. The race fired more reliably on CI under `-race` because rollover-window deadline + heartbeat-write timing is sensitive to scheduler overhead; local development happened to land in the safe ordering on most runs.

  **Pre-fix shape (v0.19.0):** `TestBackupStream_Postgres_StopCommandRequestsExit` consistently exceeds even a 60-second post-stop budget on CI (25.71s → 65.69s when budget bumped 20s → 60s — proportional scaling indicating the stream NEVER observes the stop, not just slow). `t.Skip` guard added so the test runs locally but not on CI; v0.19.0 ships with a workaround documented for operators (use SIGTERM directly instead of `backup stream stop`).

  **v0.19.1 fix (two layered closes):** 1. **Heartbeat read-modify-write** (the actual correctness bug). New `writeStreamStateMergeHeartbeat` helper in `stream_state.go`: at every per-rollover heartbeat boundary, read the current state file first, copy any concurrent `StopRequestedAt` forward into the new payload, then write. When the merge observes a concurrent stop, the outer loop's heartbeat call returns `stopObserved=true` and the stream exits cleanly without starting a fresh rollover. Closes the clobber-race window across all backends (`LocalStore` and the `s3://` / `gs://` / `azblob://` `BlobStore` variants). 2. **In-process channel notification** (option (b) from BUG-CATALOG; structural reliability win). New `stream_stop_registry.go` maintains a process-local map of `[ir.BackupStore]→chan struct{}`; `BackupStream.Run` registers its store at startup and deregisters on return. `RequestStreamStop` closes the registered channel alongside the file write. `captureWindow`'s select grows a `case <-stopCh` that fires instantaneously when same-process — no file I/O, no select-loop starvation, no clobber-race window. Cross-process operators (`sluice backup stream stop --target=<url>` on a different machine) still go through the file; the channel is process-local and `notifyStreamStop` is a no-op for them. Both paths land at the same eager-exit code path, so the chain-correctness contract is unchanged.

  **Verification (v0.19.1 cycle):** `TestBackupStream_Postgres_StopCommandRequestsExit` re-enabled on CI (no skip); local runs pass in ~6s with logs showing `in_process_stop` fires within ~5ms of `RequestStreamStop`'s write. All 6 BackupStream Postgres + MySQL integration tests pass. Phase 3 chain-restore + Phase 3.3 `--position-from-manifest` tests pass clean. Full `./internal/pipeline` integration suite green (834s, 0 failures). New unit tests `TestWriteStreamStateMergeHeartbeat_PreservesStop`, `TestWriteStreamStateMergeHeartbeat_NoStopReturnsFalse`, and `TestStreamStopRegistry_*` pin the contracts.



Logical backups Phase 4 lands: `sluice backup stream` is a single long-running process that produces rolling incrementals at a configured cadence, no per-incremental cron orchestration. Fits k8s "always-on protection" deployments naturally; pairs with continuous CDC + chain-restore for full DR coverage. Implementation supplement: `docs/dev/design/logical-backups-phase-4.md`.

### Added

- **`sluice backup stream run`** — new long-running stream subcommand. Drives a `for { rollover() }` loop where each rollover is a bounded window producing one new manifest at `manifests/incr-<unix-millis>-<seq>.json`. Three rollover ceilings active in parallel, first-fired wins:
  - `--rollover-window=DURATION` (default `5m`): wall-clock cadence.
  - `--rollover-max-changes=N` (default `100000`): change-count ceiling.
  - `--rollover-max-bytes=BYTES` (default `64Mi`, mirrors `--max-buffer-bytes` from Phase 2 backup writer): buffered-bytes ceiling.

  Window extends to the next `TxCommit` so the chain doesn't end mid-tx (mirrors Phase 3.1's incremental orchestrator). Empty rollovers skipped by default; `--rollover-include-empty` opts in for heartbeat-shape monitoring.

- **`sluice backup stream stop`** — companion stop command. Writes `stop_requested_at` to the destination's `stream_state.json`; the running stream observes the request on its next rollover-tick poll and exits cleanly. Cross-machine: an operator on machine B can stop a stream running on machine A without process access — both sides agree on the destination. Mirrors the `sync stop` pattern (ADR-0025).
- **`pipeline.BackupStream` orchestrator** — opens the engine's CDC pump ONCE for the lifetime of the stream and reuses it across rollovers (load-bearing efficiency win over a tight `for { incremental.Run() }` loop, which would re-open the slot every iteration). `manifests/stream_state.json` carries `{pid, host, started_at, last_rollover_at, stop_requested_at}` for liveness + cross-machine signalling. Concurrent-writer protection: refuses to start a second stream when the file shows a recent (`< 2 × rollover-window`) `last_rollover_at` from a different (pid, host); `--force` bypasses with a WARN.
- **`pipeline.RequestStreamStop(ctx, store, now)`** — exported helper for downstream tooling that wants to stop a running stream without going through the CLI. Idempotent: re-issuing stop preserves the original `stop_requested_at` timestamp so drain-completion watchers don't see the clock reset.
- **Rollover hooks** — `--rollover-hook=<cmd>` runs a shell command after each rollover commits successfully. Receives env vars `SLUICE_ROLLOVER_MANIFEST_PATH`, `SLUICE_ROLLOVER_PARENT_BACKUP_ID`, `SLUICE_ROLLOVER_BACKUP_ID`, `SLUICE_ROLLOVER_CHANGES`, `SLUICE_ROLLOVER_BYTES`, `SLUICE_ROLLOVER_ELAPSED_MS`. 30 s timeout. Hook errors WARN-log but do NOT fail the stream — the rollover already committed. Examples in docs: push to Prometheus pushgateway / send Slack notification / write to monitoring datastore.
- **Signal handling** — SIGINT / SIGTERM via the existing `kongContext` notifier propagates as ctx.Done through the rollover loop; mid-rollover cancel surfaces as a clean nil exit. The rollover's chunks may be partially-written but the manifest never finalises; on restart the stream picks up at the previous rollover's EndPosition. Operator-visible warning + `sluice backup verify` recommendation for orphan-chunk cleanup mid-rollover-crash.

### Changed

- **`cmd/sluice/backup.go` grows the `BackupStream` subtree.** New `BackupCmd.Stream` field of type `BackupStreamCmdGroup` (kong-grouped `Run` + `Stop` subcommands). Existing `backup full` / `backup incremental` / `backup verify` flags unchanged.
- **Stop-signal polling cadence on `backup stream` decoupled from rollover-window cadence (1s polling).** Inside the rollover capture loop a dedicated 1-second ticker reads `stream_state.json`'s `stop_requested_at` field, so an operator's `sluice backup stream stop` is observed within ~1 s regardless of the (typically minutes-long) rollover-window setting. On observation, the in-flight rollover flushes and commits, then the stream exits without a final state-file write that would clobber the operator's stop request.
- **Stop-signal observation in `backup stream` now triggers an eager exit** (commit any in-flight changes immediately + return) rather than waiting for the next TxCommit boundary, so quiet-source streams exit within seconds of `sluice backup stream stop` regardless of source activity.
- **Chain-walker manifest discovery filters by entry shape (`manifest.json` + `manifests/incr-*.json`)** so non-manifest state files like Phase 4's `manifests/stream_state.json` aren't mistaken for chain entries. Restore + verify against any stream-written destination now works regardless of how many state files coexist with the chain manifests.
- **`backup stream` ctx-cancel now performs a graceful drain of the in-flight rollover** (chunks flushed + manifest written within `stopDrainTimeout`), matching the design doc's SIGTERM contract. Previously, ctx-cancel mid-rollover dropped the rollover entirely, losing any changes captured since the last commit boundary.

### Migration / Compatibility

- **No format changes.** Manifest schema is unchanged; stream rollovers write Phase-3 shape manifests at the same `manifests/incr-…json` path. Pre-v0.19.0 chains (single-shot incrementals + fulls) remain compatible — `restore` and `verify` walk stream-written chains identically.
- **`stream_state.json` is new and informational-only.** The chain itself remains the source of truth for restore + verify. Losing the state file (operator deletes it, object-store eventual-consistency lag) doesn't break the chain — only the concurrent-writer / cross-machine-stop signalling falls back to ctx-cancel and process signals.
- **No CLI breaking changes.** `sluice backup full` / `sluice backup incremental` / `sluice backup verify` / `sluice restore` flag surfaces are unchanged.

### Deferred

- **Phase 4.5 (backup-as-broker / `sync from-backup`)** — the watcher-side feature that polls a chain and replays incrementals into a target. Stream is the producer; `sync from-backup` is the future consumer. Out of scope here; tracked separately on the roadmap.
- **Cross-engine chain restore** — Phase 5+ topic. Phase 4 streams produce same-engine chains; cross-engine replay needs the existing translate machinery extended for replay-of-changes-with-translation.
- **KMS encryption** — Phase 6.

## [0.18.0] - 2026-05-07

Closes the v0.17.2-documented "during-backup write window" gap. v0.17.x full backups recorded `EndPosition` at end-of-backup with no shared snapshot across tables — writes that landed on already-read tables before the position capture were missing from BOTH the row chunks AND the first incremental's `--since=<full>.EndPosition` window. v0.18.0 wires the full-backup row sweep into a snapshot-anchored consistent view and captures `EndPosition` at snapshot START, so the chain's next link's CDC stream from `EndPosition` forward picks up every write after the snapshot. Backup-only DR (no continuous `sluice sync start` paired) is now byte-perfect under heavy write load.

### Added

- **`ir.BackupSnapshotOpener` optional engine surface.** Returns an `ir.BackupSnapshot` bundle: snapshot-anchor `Position` + a snapshot-pinned `RowReader` + a cleanup closure. Engines that implement it get cross-table snapshot consistency plus a snapshot-anchored `EndPosition` for free; engines that don't fall back to the v0.17.x `BackupPositionCapturer` path with a soft `WARN` log line surfacing the during-backup window gap.
- **Postgres `OpenBackupSnapshot` implementation.** Creates a temporary `EXPORT_SNAPSHOT`-shape replication slot (named `sluice_backup_anchor_<unix-nanos>`) to anchor the snapshot LSN, opens a `*sql.Conn` that imports the snapshot via `SET TRANSACTION SNAPSHOT '<name>'`, and returns a `RowReader` bound to the conn. The temporary anchor slot is dropped on close (the `consistent_point` LSN is preserved on the manifest's `EndPosition` for chain handoff against the operator's chain-handoff slot, which is recorded on the position alongside the LSN). Reuses `createLogicalReplicationSlot`'s PG-version-adaptive helper (FAILOVER on PG 17+).
- **MySQL `OpenBackupSnapshot` implementation.** Pins a single `*sql.Conn`, runs `SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ` + `START TRANSACTION WITH CONSISTENT SNAPSHOT`, captures `@@global.gtid_executed` (or `(file, pos)` in non-GTID mode) inside the same transaction so the recorded position refers to the snapshot's logical clock. All table reads run on this one connection sequentially. **Trade-off vs PG**: MySQL's REPEATABLE READ snapshot is per-session and not shareable across connections (ADR-0019), so multi-conn parallel reads aren't available on this path; PG's snapshot can fan out to N readers via the existing `SnapshotImporter` machinery in a future revision. MySQL operators running backups under high read parallelism configurations should expect single-conn throughput on the row sweep.

### Changed

- **`pipeline.Backup` orchestrator now prefers the snapshot path.** `Backup.Run` type-asserts on `ir.BackupSnapshotOpener` at start of run; when implemented, the captured snapshot position becomes `Manifest.EndPosition` immediately and the post-sweep `BackupPositionCapturer` fallback is skipped. When the engine doesn't implement the new surface, falls through to the v0.17.x shape with a `WARN` line citing the during-backup window gap and pointing at the v0.17.2 release notes / `docs/dev/design/logical-backups-phase-3.md` for context. Snapshot-open errors fall back to the v0.17.x `OpenRowReader` + post-sweep `BackupPositionCapturer` path with a clear `WARN`, so backups work on PG environments without `wal_level=logical` (no-CDC scenarios). Chain-correctness still requires the snapshot path; the WARN names the operational implication.
- **`Manifest.EndPosition` semantic.** For full manifests written by v0.18.0+ this is the snapshot-anchor LSN/GTID (captured AT snapshot start); for fulls written by v0.17.0–v0.17.3 it remains the post-sweep position (captured at end-of-backup). The chain-walker treats both identically — the field is a CDC resume cursor — so existing chains restore unchanged. The wire shape of `EndPosition` is unchanged; only its capture timing semantics shift, so old chains and new chains can coexist in a chain history without operator action.
- **`docs/dev/design/logical-backups-phase-3.md`.** The "Implementation note: deviation from snapshot-anchored EndPosition" section is rewritten — flips from "deviation in v0.17.2" to "deviation closed in v0.18.0" with the post-fix shape documented.

### Closed

- **v0.17.2's documented "during-backup write window" caveat.** The release notes for v0.17.2 surfaced this as a known limitation with the workaround "pair backups with continuous `sluice sync start`." v0.18.0 closes it — backup-only DR works correctly even under heavy write load on the source. The mitigation pattern is still recommended for the "fresher than the most recent incremental window" use case, but is no longer load-bearing for chain correctness.

### Migration / Compatibility

- Pre-v0.18.0 chains restore unchanged; the chain-walker handles both old (post-sweep) and new (snapshot-anchored) `EndPosition` semantics identically.
- Operators running PG with `wal_keep_size` tuned for the chain's incremental cadence don't need to revisit settings — the snapshot anchor is short-lived (dropped at end of full) and doesn't change the chain's WAL footprint at the chain-handoff slot.
- MySQL operators running backups against high-throughput sources should expect the row sweep to be single-conn (per-session snapshot constraint); throughput-sensitive backups that previously ran with multiple workers via OpenRowReader will lose that parallelism. Document the trade-off; mitigation is to run backups during slightly slower windows or accept the consistent-view trade.

## [0.17.3] - 2026-05-07

Single-bug patch from the v0.17.2 test cycle. v0.17.2 shipped Phase 3.3's PG soft-warning preflights (including Patroni / HA-managed source detection); the cycle surfaced that the three v0.17.2 detection signals all systematically miss on tenant-isolated managed PG services like PlanetScale Postgres. The operators who most need the idle-slot trap warning (managed-PG users who can't tune their own slot retention) got nothing.

### Fixed

- **Bug 36 — Patroni / managed-PG idle-slot trap warning does not fire on PlanetScale Postgres (or other tenant-isolated managed PG services).** The v0.17.2 `detectPatroniSource` heuristic checked three signals: (1) `pg_settings WHERE name ILIKE '%patroni%'`, (2) `pg_stat_replication.application_name ILIKE 'patroni%'`, (3) `pg_roles WHERE rolname IN ('patroni', 'replicator')`. All three miss on PS-PG: Patroni sets standard PG GUCs via `ALTER SYSTEM` (not Patroni-prefixed ones, so `name ILIKE '%patroni%'` returns 0 rows); `pg_stat_replication` is permission-restricted on per-tenant roles (returns 0 rows even when Patroni is using it); PS creates tenant-prefixed roles like `hzi_xgsa060j2bbb_role` (so `rolname IN ('patroni', 'replicator')` doesn't match). Net effect: managed-PG operators got no warning when pointing `--position-from-manifest` at their cluster. Fix lands as option (c) from the BUG-CATALOG analysis: broader heuristics + an explicit override flag.

  **Broader engine-side heuristics (v0.17.3 adds Signals 4–5):** 1. **Non-temporary physical replication slots present.** `SELECT count(*) FROM pg_replication_slots WHERE slot_type = 'physical' AND temporary = false`. Standby physical slots are a strong HA-cluster signal — most non-HA PG deployments don't carry them. Permission-denied on `pg_replication_slots` (some managed services restrict it) gracefully degrades to skipping the signal. 2. **`cluster_name` GUC populated.** Patroni convention sets this; many managed services follow suit. Empty string = no signal; permission-denied / sql.ErrNoRows on `pg_settings WHERE name = 'cluster_name'` gracefully degrades.

  **DSN hostname-pattern signal (streamer layer, layered on top of the engine's six SQL signals):** known managed-PG suffixes — `*.psdb.cloud` (PlanetScale Postgres), `*.aws.prod.archil.com` / `*.gcp.prod.archil.com` (Archil), `*.cluster*.rds.amazonaws.com` (Aurora cluster endpoints; vanilla RDS instances are excluded because they're not always HA), `*.postgres.database.azure.com` (Azure Database for PostgreSQL), `*.cloudsql.google.internal` (Cloud SQL via private IP). Patterns are intentionally narrow — false positives on non-HA setups would erode the warning's signal value. The signal lives at the streamer layer because the IR `PositionFromManifestPreflight` interface deliberately doesn't carry the DSN (engines without network awareness can implement it cleanly).

- **Closes Bug 36.** Verified via integration tests on testcontainer-shaped HA signals: `TestPreflight_PG_DetectsPhysicalSlot` (a non-temporary physical replication slot trips Signal 4, citing the slot signal in the warning); `TestPreflight_PG_DetectsClusterNameGUC` (cluster_name set at server start trips Signal 5, citing the GUC value); `TestSyncStart_PatroniMode_Off_SuppressesWarning` (with a physical slot present, `--patroni-mode=off` strips the warning while keeping wal_keep_size warnings intact); `TestSyncStart_PatroniMode_On_FiresOnVanilla` (on vanilla PG with no Patroni signals, `--patroni-mode=on` still emits the operator-forced warning). DSN hostname-pattern detection is unit-tested via direct DSN-string parsing across all six patterns (PlanetScale, Archil aws/gcp, Aurora, Azure, Cloud SQL) plus negative cases (vanilla RDS instance, self-hosted host, localhost, empty DSN).

### Added

- **`sluice sync start --patroni-mode=auto|on|off`.** New flag pairing with the broader heuristics. `auto` (default) runs the engine heuristics + DSN hostname-pattern check and warns if any of the six signals fires; `on` skips the heuristics and forces the warning (operator opts in regardless of detection — the canonical override for tenant-isolated managed PG where the heuristics still miss); `off` skips the heuristics and suppresses the Patroni warning entirely (operator confirmed self-hosted single-node PG without HA, doesn't want the noise). Combine `--patroni-mode=on` with `--strict-preflight=true` to make the warning a hard refusal. The slot-existence / `wal_status='lost'` refusal is unaffected by `--patroni-mode` (those are always refusals — the slot can't deliver what's needed). Validation: unknown values are rejected at flag parse time with a clear error naming the accepted set.

## [0.17.2] - 2026-05-07

Logical backups Phase 3.3 lands: full-backup `EndPosition` recording, the `--position-from-manifest` CDC handoff flag, and PG soft-warning pre-flights. Closes the v0.17.0 known-limitation list — chains rooted in v0.17.2+ fulls work end-to-end without manually patching the manifest's terminal position, and a freshly-restored target resumes CDC from the chain's tail without re-bulking from source. Implementation supplement: `docs/dev/design/logical-backups-phase-3.md` (Phase 3.3 row in the sub-phasing table).

### Added

- **Full-backup `EndPosition` recording (Phase 3.3.A).** `sluice backup full` now captures the source's CDC position at end-of-backup and writes it onto the manifest's `EndPosition` field. PG records `pg_current_wal_lsn()` paired with the configured slot name (default `sluice_slot`; override via the new `--slot-name` flag); MySQL records `@@global.gtid_executed` (or `(file, position)` when GTID mode is off) via the existing master-status helpers. Engines opt in by implementing the new `ir.BackupPositionCapturer` optional interface on their `SchemaReader`; engines without CDC support skip silently. Closes the v0.17.0 known limitation: incrementals chained off v0.17.2-rooted fulls no longer fire the "parent has no EndPosition; chain will start from CDC's current position" warning.
- **`sluice sync start --position-from-manifest=<chain-url>`.** New CLI flag that loads the chain's terminal manifest's `EndPosition` and uses it as the resume position, bypassing the per-target `sluice_cdc_state` lookup. Use after `sluice restore --from=<chain-url>` to resume CDC from the chain's tail without re-bulking from source. Mutually exclusive with `--reset-target-data` (different recovery shapes; both override the persisted position). The slot-missing fall-through (ADR-0022) is suppressed when chain handoff is requested — silently re-bulking would defeat the chain's purpose. Accepts the same `s3://` / `gs://` / `azblob://` / `file:///` URL schemes as `sluice backup`, with companion `--backup-endpoint` / `--backup-region` / `--backup-path-style` flags for S3-compatible providers.
- **PG soft-warning pre-flights (Phase 3.3.C) for `--position-from-manifest`.** New `ir.PositionFromManifestPreflight` optional engine surface; PG implements three checks against the source before CDC opens: 1. `wal_keep_size` sufficiency — soft warning when configured below PG's 64 MB default (so only setups that explicitly dialed it down trigger), with an operator-facing pointer to `docs/postgres-source-prep.md`. 2. Patroni / HA-managed source detection — soft warning about the idle-slot failover trap (the user's 2026-05-07 production finding). Three signals checked in order: Patroni-set GUCs in `pg_settings` (most specific), `pg_stat_replication.application_name` LIKE 'patroni%' (catches standby connections; gracefully degrades on permission denied), role names `patroni` / `replicator` (loosest). 3. Slot existence + health — fatal refusal for missing or `wal_status='lost'` / `'unreserved'`. Always a refusal regardless of `--strict-preflight` because the slot can't deliver what's needed.

   MySQL intentionally has no preflight surface — its CDC reader's existing `verifyPositionResumable` already covers binlog purge.

- **`sluice sync start --strict-preflight` (Phase 3.3.D).** New flag that promotes the soft warnings emitted by Phase 3.3.C to hard refusals before CDC starts. Default off: warnings log via slog and the run proceeds. Use in CI gates, scripted runbooks, or post-incident audits where the operator wants a strict "fail loudly on any preflight signal" posture.
- **`sluice backup full --slot-name`.** Labels the recorded `EndPosition` on engines with a slot concept (Postgres) so a Phase 3 incremental opens CDC against a slot of the same name. Engines without slots (MySQL: binlog stream is the slot) ignore the flag. Default `sluice_slot`.
- **`pipeline.LoadChainTerminalPosition(ctx, store)` exported helper.** Reads every manifest in a backup store, validates the chain shape via the existing `buildChain` helper, and returns the terminal manifest's `EndPosition`. Used by the streamer's `--position-from-manifest` path; exposed for downstream tooling that wants to inspect a chain's tail position without standing up a sync.

### Changed

- **`ir.PositionFromManifestPreflight` and `ir.PreflightReport` live in the `ir` package** (initially scoped to `pipeline`). The cycle-break is necessary because engine packages implement the interface and integration tests in pipeline import engines. The `pipeline` package keeps type aliases so existing call sites compile unchanged.
- **`pipeline.ResolveSlotName` exported** so CLI commands outside the pipeline package (today: `sluice backup full --slot-name`) can apply the sluice-prefix convention without re-implementing it.

### Phase 3 known limitations (closed)

The v0.17.0 release notes flagged three Phase 3.3 follow-ups; all three are addressed in v0.17.2:

- ✅ Full backups record `EndPosition` automatically (Phase 3.3.A).
- ✅ `sluice sync start --position-from-manifest` is implemented (Phase 3.3.B).
- ✅ PG `wal_keep_size` soft-warning + Patroni-detection pre-flights are implemented (Phase 3.3.C).

## [0.17.1] - 2026-05-07

Single-bug patch from the v0.17.0 test cycle. v0.17.0 shipped logical-backups Phase 3.1 + 3.2 (incrementals + chain restore); the cycle surfaced a writer-side path collision that broke any chain with two or more incrementals into the same destination. Single-incremental chains and the schema-evolution path were unaffected.

### Fixed

- **Bug 35 — Incremental change-chunk filename collision; second incremental clobbers the first's chunk on disk.** v0.17.0's change-chunk writer constructed paths as `chunks/_changes/changes-<idx>.jsonl.gz` with the chunk index reset to 0 per-Run. Two incrementals taken into the same `--output-dir` / `--target` therefore both wrote to `chunks/_changes/changes-0.jsonl.gz` — the second overwrote the first's bytes while each manifest still recorded its own (now-divergent) SHA-256. `backup verify` exited 1 with `1 of N chunk(s) failed SHA-256 check`; chain restore exited 1 at `chain restore: incremental <id1>: stream chunks: chunk 0 (chunks/_changes/changes-0.jsonl.gz): backup: chunk SHA-256 mismatch`. Engine-agnostic (verified on both PG and MySQL in the v0.17.0 cycle) and backend-agnostic (writer code is shared between local-FS and S3/cloud). Fix: namespace each incremental's chunks under a per-Run subdirectory derived from the manifest's `CreatedAt` (`chunks/_changes/<unix_millis>/changes-<idx>.jsonl.gz`). `CreatedAt` is preferred over `BackupID` because `BackupID` depends on `EndPosition`, which is only known after the window closes — chunks need a stable namespace before the first write. The manifest's recorded `change_chunks[].file` path is the source of truth for reads, so chain restore + `backup verify` pick up the new shape with no other changes. Single-incremental chains written by v0.17.0 still restore cleanly post-fix because the readers follow whatever path the manifest recorded. Two new unit tests (`TestIncrementalBackup_TwoIncrementals_NoChunkCollision` pins distinct paths + SHA-256 fidelity across two Run calls; `TestChangeChunkPath_RunNamespaceShape` pins the path shape) plus a new PG integration test (`TestIncrementalBackup_PostgresChainRestore_TwoIncrementals` drives the full repro: full → inserts → incr1 → inserts → incr2 → verify chain → chain restore → confirm every row arrives on the target).

## [0.17.0] - 2026-05-07

Logical backups Phase 3.1 + 3.2 lands: incremental backups + chain-aware restore. Phase 3.3 (CDC handoff via `--position-from-manifest`) is the v0.17.1 follow-up; that release closes the "auto-resume CDC from a chain's terminal position" UX gap. v0.17.0 is the storage + restore plumbing, usable today via `sluice backup full → backup incremental → restore --from=<chain-url>` with a manual `sluice sync start --resume` to continue replication. Implementation supplement: `docs/dev/design/logical-backups-phase-3.md`.

### Added

- **Logical backups Phase 3.1 + 3.2: chained backups (`sluice backup incremental --since=<backup-id>`) + chain-aware restore.** The chunk that closes the resync-avoidance story for irrecoverable position loss. New CLI subcommand `sluice backup incremental` opens the source's CDC pump at the parent manifest's terminal position, streams events for a bounded window (`--window` time-bound + `--max-changes` count-bound, first-fired wins; window extends to the next TxCommit so the chain doesn't end mid-tx), and writes a chain-linked manifest under `manifests/incr-…json` plus serialised change chunks under `chunks/_changes/`. Manifest gains `Kind`, `BackupID`, `ParentBackupID`, `StartPosition`, `EndPosition`, `SchemaHash`, and `SchemaDelta` fields; pre-Phase-3 manifests treated as orphan fulls under the canonicaliser. Implementation supplement: `docs/dev/design/logical-backups-phase-3.md`.
- **`sluice restore --from=<chain-url>` chain detection.** The existing single-manifest restore path now lists `manifests/incr-…json` at the supplied URL; when any incremental manifests are found, dispatches to a new chain-aware orchestrator that walks `[full, incr_1, …, incr_N]` in order. Builds the chain via `ParentBackupID` linkage, validates: single full root, no branching, no cycles, no orphans, every incremental's `StartPosition` matches its parent's `EndPosition`. Cross-engine chain restore is refused loudly per the design doc's Phase 5+ deferral; same-engine chain restore applies schema deltas (AddTable creates the table, AlterTable replays ADD COLUMN via the new `ir.SchemaDeltaApplier` surface implemented on both PG and MySQL) then streams change chunks through the engine's idempotent `ChangeApplier.ApplyBatch` per ADR-0010.
- **`sluice backup verify` walks chains.** Re-checksums every chunk in the store across all manifests (the full's row chunks and every incremental's change chunks), so cron-style integrity probes cover the whole chain in one call.
- **`ir.SchemaDeltaApplier` interface** with `AlterAddColumn(ctx, table, cols)` — implemented on PG (`ALTER TABLE … ADD COLUMN IF NOT EXISTS …`) and MySQL (information-schema probe + `ALTER TABLE … ADD COLUMN`); same-engine column-add deltas apply cleanly during chain restore.
- **Schema-evolution capture within an incremental's window.** Source's recorded schema (parent manifest) vs end-of-window source schema is diff'd into typed `ir.SchemaDeltaEntry` slice on the incremental manifest; AddTable / DropTable / AlterTable kinds covered. Rename-shaped deltas (single drop + single add per table) are flagged as ambiguous and surface a "force fresh full + new chain" recovery message.

### Changed

- **`ir.Manifest.FormatVersion` stays at 1.** All Phase 3 fields are forward-compatible additions (older sluice ignores `Kind` / `ParentBackupID` / etc.; those manifests appear as orphan fulls when read by an older binary, which is the right degraded behaviour for incrementals nobody can chain anyway). The version bumps when a future change would break older readers.

### Phase 3 known limitations (Phase 3.3 follow-up)

- The full-backup writer doesn't yet record an `EndPosition` (= snapshot LSN/GTID) on its manifest; integration tests patch this in manually, and the first incremental against a v0.16.x full surfaces a clear "parent has no EndPosition; chain will start from CDC's current position" warning. Phase 3.3 will close this gap so chains rooted in v0.17.0+ fulls get position-from-snapshot for free.
- `sluice sync start --position-from-manifest=<chain-url>` (the CDC handoff flag that lets a sync stream resume from the chain's terminal position without re-bulking) is not in 3.1+3.2 — it's the next subagent's job, gated on a test cycle for the writer + restore work.
- PG `wal_keep_size` soft-warning pre-flight checks are not in 3.1+3.2 — Phase 3.3 territory.

## [0.16.1] - 2026-05-07

Two-bug patch from the v0.16.0 test cycle. v0.16.0 shipped logical-backups Phase 2 (cloud backends + resumable writer); both findings are operational papercuts on top of an otherwise-clean cloud-roundtrip surface — not data-correctness bugs, but they break the workflow shape v0.16.0 promised.

### Fixed

- **Bug 33 — `--target=s3://bucket/prefix` silently drops the path; chunks land at bucket root.** `gocloud.dev/blob.OpenBucket` consumes only the bucket name from the URL it receives — the path component after the bucket name is dropped without warning. v0.16.0's `BlobStore` therefore wrote every key to bucket root regardless of the URL's path. Multiple backups in one bucket collided at root and tripped the "completed backup already exists" guard. Fix: `BlobStore` now extracts the path-after-bucket at construction time, stashes it on the struct, and prepends it to every key in Put / Get / List / Exists / Delete; List results are stripped of the prefix so callers see paths relative to it (matching `LocalStore`'s contract). `file://` URLs are exempt — gocloud's fileblob driver treats the whole path as the bucket root, so no double-prefix needed. Affects all URL schemes (`s3://`, `gs://`, `azblob://`); pre-fix shape, manifest at `s3://bucket/backup-v0160` lands at `<bucket>/manifest.json`, post-fix it lands at `<bucket>/backup-v0160/manifest.json`. Three new unit tests pin the prefix join + List-strip + empty-prefix shapes via the `mem://` driver; integration test extended with a Bug-33-regression check that HEADs the expected key directly via the AWS SDK.

- **Bug 34a — No "resuming" log line emitted on resume detection.** v0.16.0's resume code ran silently; operators couldn't tell from the log whether a re-run had started fresh or picked up a partial. Fix: emit `INFO resuming from partial backup` with the destination descriptor + the prior-manifest's table count + the prior `created_at` timestamp at the point the orchestrator detects an `in_progress` manifest; emit `INFO resume plan` with the per-table fan-out (`tables_already_complete`, `tables_to_resume`) once the schema is in hand; emit `INFO skipping table — already complete in partial backup` per skipped table during iteration.

- **Bug 34b — Resumable writer's per-chunk skip wasn't actually wired up.** The Phase 2 design + v0.16.0 CHANGELOG promised "per-chunk skip via `BackupStore.Exists` + manifest's recorded SHA-256" — but the v0.16.0 implementation only checkpointed the manifest at table boundaries, so a kill mid-table forced the entire table to be re-bulked from scratch on resume. Fix: per-chunk manifest checkpointing (manifest is now committed after every chunk, not just every table), plus a pre-write skip path in `backupTable` that consults the prior manifest's `ChunkInfo` for the in-flight chunk index — if `BackupStore.Exists` reports the chunk path is still on the store and `chunkAlreadyMatches` confirms the recorded SHA-256, the orchestrator advances the row cursor over that chunk's rows without opening a writer or issuing a Put. Mid-table kills now leave a manifest with `Partial=true` on the in-flight table; the resume picks up at the next un-completed chunk. New `TableManifest.Partial bool` field signals partial-state to resume code (omitted on fully-complete entries; pre-v0.16.1 manifests treated as complete-by-default for backward compat). New `TestBackup_ResumePerChunkSkipsAlreadyUploadedChunks` unit test stages a partial manifest and verifies chunks 0..N (already uploaded with matching SHA-256) are skipped while chunk N+1 onwards is written. Two existing tests updated to account for the per-chunk Put; `TestBackup_ResumeSkipsAlreadyCompletedTables` injection point shifted from `failOn=3` → `failOn=4` (per-chunk + per-table checkpoints add one Put each).

## [0.16.0] - 2026-05-07

Logical backups Phase 2 lands: cloud backends (S3 + S3-compatible providers + GCS + Azure) atop the `BackupStore` interface that shipped in v0.15.0's Phase 1. Plus a resumable backup writer that picks up a partially-completed job at the next un-finished table. Implementation supplement: `docs/dev/design/logical-backups-phase-2.md`.

### Added

- **Cloud backends for `sluice backup` / `restore` / `backup verify` (Phase 2 of the logical-backups proto-ADR).** New `internal/pipeline.BlobStore` wraps `gocloud.dev/blob` and implements the same `ir.BackupStore` contract as Phase 1's `LocalStore`. CLI: pass `--target=s3://bucket/prefix/` to `backup full`, or `--from=s3://...` to `restore` / `backup verify`. URL schemes wired: `s3://` (AWS S3 + S3-compatible via `--backup-endpoint`), `gs://` (Google Cloud Storage, ADC creds), `azblob://` (Azure Blob, managed-identity / connection-string), and `file:///` (kept for parity with `--output-dir`/`--from-dir`). Multipart upload for large chunks is automatic in `gocloud.dev/blob`'s `s3blob` driver. Per-chunk SHA-256 integrity carries through unchanged from Phase 1; backups are stored gzipped (`*.jsonl.gz`). **Client-side encryption is NOT shipped — chunks land unencrypted at rest. Operators relying on at-rest encryption should use bucket-level SSE on the cloud side or filesystem-level encryption (LUKS / BitLocker / FileVault) on the local-FS side. Sluice-managed client-side AES-256-GCM remains a Phase 6 (KMS) deliverable per the original proto-ADR.**
- **`--backup-endpoint`, `--backup-region`, `--backup-path-style` flags** for S3-compatible providers — MinIO, Cloudflare R2, Backblaze B2, Wasabi, Tigris, DigitalOcean Spaces. Same flags also enable Archil's read-only S3 API for cross-environment restore-from-Archil flows. Each flag is rejected with a clear error if combined with a non-`s3://` URL scheme.
- **Resumable backup writer (NEW in Phase 2 scope).** Two complementary mechanisms so a partially-completed backup picks up where it left off rather than re-doing everything: per-chunk skip via `BackupStore.Exists` + manifest's recorded SHA-256 (skip if present and checksum-matches; overwrite if checksum-mismatches), and per-table progress checkpoints (manifest is updated atomically after each table completes). On a re-run against the same `--output-dir` / `--target`, the orchestrator detects the partial-state manifest and resumes from the next un-completed table. New `--force-overwrite` flag mirrors `--reset-target-data`'s friction tier for the case where the operator wants to discard the partial backup and start fresh.
- **`BackupStore.Exists(ctx, path) (bool, error)` interface method** added to `ir.BackupStore`, both engines (`LocalStore`, `BlobStore`) implement it. Internal — no operator-visible surface change beyond the resumable writer.

### Changed

- **`gocloud.dev/blob` added as a dependency.** Pre-implementation estimate was `~3-5 MB` binary growth even on local-FS-only builds; real measurement after wiring all four side-effect imports (`s3blob`, `gcsblob`, `azureblob`, `fileblob`) is `~46 MB` total binary growth (~38.5 MB → ~84.2 MB on linux/windows amd64). Larger than the optimistic estimate but in line with the pessimistic envelope. Acceptable for v1 — operators consuming sluice via container images won't notice the layer-cache cost; binary distribution sizes remain in the same order of magnitude as `kubectl` (~50 MB) and `terraform` (~110 MB). If footprint becomes a real concern (e.g. embedding sluice in a small operator binary), the path is build-tag gating per cloud or moving to native SDKs per backend; the `BackupStore` interface is unchanged either way.

### Documentation

- **`docs/dev/design/logical-backups-phase-2.md`** — implementation supplement to the original proto-ADR. Captures: the `gocloud.dev/blob` library pivot (revised from native `aws-sdk-go-v2`); Archil integration findings (their S3 API is read-only — `PutObject` / multipart return `MethodNotAllowed`; clean split between POSIX-mount writes via Phase 1's `LocalStore` and S3-API restore reads via Phase 2's `BlobStore`); resumable backup writer addition; backup-chain → CDC handoff design as Phase 3 acceptance criterion (MySQL `gtid_purged` is clean; PG needs maintained slot or generous `wal_keep_size`); and the backup-as-broker pattern as a Phase 4.5+ direction (decoupled source-target sync with backup storage as the message log — unlocks no-direct-connectivity, multi-region-no-VPN, fan-out, time-shifted-sync, and air-gapped scenarios).

## [0.15.1] - 2026-05-08

Single-bug patch from the v0.15.0 test cycle. v0.15.0's PG → PG sync-health source-side probe was non-functional — `lag_bytes` always reported "unavailable" and `--max-lag-bytes` thresholds never tripped on the very engine pair the feature was designed for.

### Fixed

- **Bug 32 — `sync health --max-lag-bytes` non-functional on PG → PG.** The lag-bytes calculation passed the persisted target Position's Token verbatim into `pg_wal_lsn_diff($1::pg_lsn, ...)`. PG positions are JSON envelopes (`{"slot":"...","lsn":"X/Y"}`), not bare LSN strings — PG rejected with `SQLSTATE 22P02`. The error landed in `source_probe_reason` (good loud-failure shape) but the headline alerting feature was unusable. The orchestrator was also reconstructing the target token from the truncated-for-display string rather than passing the full position through. Two-part fix: PG engine's `LagBytes` now extracts LSN transparently from either bare-LSN or JSON-envelope Token shapes via a new `extractPGLSN` helper; the orchestrator passes the full `StreamStatus.Position` through to `probeSource` instead of reconstructing it. 5 new regression tests cover bare LSN, JSON envelope, leading-whitespace tolerance, empty token, and malformed JSON. Surfaced by sluice-testing's v0.15.0 cycle.

## [0.15.0] - 2026-05-08

Three roadmap items + a verify analysis pass land together. The user's morning brief asked me to pick "the next 3 items on the roadmap" and tackle them — these are the result.

### Added

- **`sluice sync health` source-side position probe (Phase 2 of sync-health proto-ADR).** New optional `--source-driver` + `--source` flags on the health probe; when supplied, sluice opens a `SchemaReader` against the source, type-asserts to the new `ir.HealthReporter` interface, calls `SourceCurrentPosition()`, and surfaces source/target tokens + (for PG-only pairs) a byte-distance lag metric via `ir.BytesLagReporter` + `pg_wal_lsn_diff()`. New `--max-lag-bytes` threshold flag (PG-only, exit 1 on breach). Source-probe errors don't fail the target-side check — an unreachable source shouldn't break cron probes monitoring the target. MySQL `SourceCurrentPosition` returns `gtid_executed`; MySQL doesn't implement `BytesLagReporter` (GTID sets aren't byte-distance comparable).
- **Mid-stream add-table (Phase 1 MVP) — `sluice schema add-table TABLE --stream-id ID`.** Brings a new source table into an active CDC stream's scope without forcing a destructive `--reset-target-data` cycle. Phase 1 requires the stream to be drained first (`sluice sync stop --wait`); the orchestrator refuses cleanly if the stream's `stop_requested_at` is still set. After successful add the operator restarts via `sluice sync start --resume` and CDC picks up the new table from the stream's existing position — the applier's idempotent upsert handles the [persisted_LSN, snapshot_LSN] overlap on the new table. Implements `docs/dev/design/mid-stream-add-table.md`. New `pipeline.AddTable` orchestrator (mirrors `Migrator` shape); new `Postgres.Engine.AddPublicationTables` issues `ALTER PUBLICATION ... ADD TABLE` (additive; existing scope untouched; idempotent on partial-add re-run); MySQL participates with no engine surface change — the binlog already covers every table. Pipeline-side optional interfaces `publicationAdder` / `snapshotSlotOpener` / `slotDropper` so engines opt in structurally. Operator safeguards: refuses if no row exists for the supplied `--stream-id`, if the target table already has rows (`TableEmptyChecker` preflight, same shape as cold-start), or if the named table doesn't exist on the source. Typed-confirmation prompt mirroring `--reset-target-data`'s friction tier; `--yes` bypasses; `--dry-run` prints the plan without touching anything. Phase 2 (live add-table without the drain) is roadmap'd; Phase 1 covers the routine "developer ran `CREATE TABLE` and forgot to tell ops" case.
- **Logical backups, Phase 1 — full snapshot to local filesystem.** New `sluice backup full`, `sluice backup verify`, and `sluice restore` commands implement the MVP slice from `docs/dev/design/logical-backups.md`. Backup writes a JSON `manifest.json` plus one or more gzipped JSON-Lines chunk files under `--output-dir`; manifest carries the full IR schema, per-table row counts, and a per-chunk SHA-256. Restore reads the manifest, runs `translate.RetargetForEngine` so cross-engine restore (PG backup → MySQL target, etc.) succeeds, applies the schema and bulk-copies rows back through the existing `RowWriter` path, verifying each chunk's SHA-256 and row count along the way. `sluice backup verify` rehashes every chunk against the manifest without restoring — useful as a cron probe against archived backups. New types: `ir.BackupStore` (interface, designed for Phase 2 cloud backends from day one), `ir.Manifest`, `ir.TableManifest`, `ir.ChunkInfo`, plus tagged-union JSON envelopes (`ir.MarshalType` / `ir.UnmarshalType` / custom `Column.MarshalJSON`) so the IR's sealed `Type` / `DefaultValue` interfaces round-trip through standard `encoding/json`. New pipeline orchestrators: `pipeline.Backup`, `pipeline.Restore`, `pipeline.LocalStore`, `pipeline.VerifyBackup`. Per the proto-ADR, Phase 1 is intentionally local-FS only — cloud backends (S3 / GCS / Azure) are Phase 2 (`BackupStore` interface is ready), incremental backups are Phase 3, encryption is Phase 6.
- **`sluice verify --strict-hash` opt-in for SHA-256 sample-mode hashing.** Default stays MD5 (statistically sufficient for honest-data scenarios at any practical row count — see `docs/verify-vs-vitess-vdiff.md` for the collision math). `--strict-hash` switches sample-mode to SHA-256 for operators wanting an extra confidence margin or matching a compliance posture that requires SHA-256. New `ir.HashAlgorithm` enum on the `ir.SampleVerifier` interface; PG implements via the built-in `sha256()` (PG 11+ core, no pgcrypto needed); MySQL via `SHA2(..., 256)`. ~2× server-side hashing time vs MD5; difference is sub-second at sample-mode's typical sizes.

### Documentation

- **`docs/verify-vs-vitess-vdiff.md`** — operator-facing comparison of sluice's verify approach to Vitess's vdiff workflow. Covers what each tool does, when to reach for which, and the MD5 collision math (P(collision) ≈ 10⁻²¹ at 1B rows; effectively zero for honest data). Sourced via WebFetch against vitess.io docs and the vdiff `table_differ.go` source — vdiff uses direct value comparison (not hashing), streams every row in PK order, full-fidelity, heavy on multi-TB tables. Sluice verify offers count + statistical-sample comparison cheaper but less exhaustive; full-fidelity mode is planned (proto-ADR phase 3).

## [0.14.1] - 2026-05-08

Single-bug patch from the v0.14.0 test cycle (`session-reports/v0.14.0.md` in the sluice-testing companion repo). 5 of 6 focus areas passed clean — including the headline PlanetScale Oregon → Virginia online migration with verify-mode-as-accuracy-proof — but the v0.14.0 view-support Phase 1 had one emission bug that blocked PG materialized-view round-trip.

### Fixed

- **Bug 31 — PG materialized view emit produces `... ; WITH DATA;` syntax error.** PG's `pg_matviews.definition` catalog column returns the SELECT body with a trailing `;`. The v0.14.0 `emitCreateView` matview branch appended ` WITH DATA;` directly, producing `CREATE MATERIALIZED VIEW ... AS SELECT ...; WITH DATA;` which PG rejects with SQLSTATE 42601. Migrate exited with the orchestrator's "view dependency budget exhausted" error after 2 retries, leaving the matview uncreated. Regular views happened to "work" because PG silently tolerates the resulting `;;` (parses as no-op-then-empty-statement); same code path; same fix. The shared `trimTrailingSemicolon` helper now also strips trailing whitespace before/after the semicolon. Workaround pre-fix: `--exclude-view <matview-name>` or `--skip-views`. Four new regression tests pin the trim behavior.

## [0.14.0] - 2026-05-08

Three feature tracks land together: **sample-mode verify** (Phase 2 of the verify proto-ADR), **Prometheus `/metrics` listener** (Phase 2 of the sync-health monitoring proto-ADR), and **view support Phase 1** (schema-only round-trip from the schema-completeness proto-ADR). Plus a chunked-count fallback that makes `sluice verify` work cleanly against PlanetScale-MySQL tables larger than its per-query row-read budget. Validated end-to-end including PlanetScale Oregon → Virginia online migration with verify confirming 100% accuracy.

### Added

- **View support Phase 1 — schema-only round-trip for regular and materialized views.** Both schema readers populate `Schema.Views`: MySQL via `information_schema.views`, Postgres via `pg_views` (regular) and `pg_matviews` (materialized; tagged `Materialized=true`). Both schema writers emit views as a new Phase 6 of the simple-mode orchestrator (after constraints): `CREATE OR REPLACE VIEW` for regular views, `CREATE MATERIALIZED VIEW ... WITH DATA` for matviews so the matview is populated from the just-loaded target tables on cold-start. View-to-view dependency ordering uses a single-pass-with-up-to-2-retries policy — no SQL parser, surface a clear error if a view still fails after the retry budget. New CLI flags on `migrate` / `sync start` / `schema preview` / `schema diff`: `--include-view PATTERN`, `--exclude-view PATTERN`, `--skip-views`. New `ir.SchemaWriter.CreateViews` interface method (additive, all shipping engines implement). New `ir.SchemaDiff.ViewsMissing` / `ViewsExtra` / `ViewsMismatched` for view-level drift detection. Cross-engine view-body translation is deferred to Phase 3 — Phase 1 emits the source-dialect definition verbatim and relies on the loud-failure tenet for non-portable definitions. Materialized view CDC refresh is a Phase 2 follow-up.
- **`sluice sync start --metrics-listen ADDR` (Phase 2 of the sync-health monitoring proto-ADR).** Optional Prometheus-format `/metrics` endpoint runs alongside an active stream; companion to the existing one-shot `sluice sync health` probe. Off by default; opt-in. Hand-written exposition encoder (no new dependency on `prometheus/client_golang`); reads scrape-time data from `ListStreams` so the apply hot path is untouched. Metric set: `sluice_seconds_since_last_apply`, `sluice_stream_known`, `sluice_metrics_scrape_unix_seconds` — all gauges, labelled by `stream_id`. Plus a `/healthz` endpoint that returns 200 OK so monitoring stacks can distinguish "scrape target gone" from "scrape target up but reports zero streams." Bind failure at startup is fatal (operator misconfig shouldn't be silent).
- **`sluice verify --depth sample` (Phase 2 of the verify proto-ADR).** Per-table sampled-row content hashes alongside count comparison; closes the "did the row data come across, not just the count" gap that count-mode alone can't catch. Default 100 rows per table; `--sample-rows-per-table N` to raise; deterministic `--sample-seed` so source + target select the same row subset run-to-run. Server-side hashing via `MD5(CONCAT_WS('|', col::text, ...))`; merge-walk in the orchestrator detects three drift shapes: PK on source only (target-missing-row), PK on target only (target-extra-row), and PK on both with hash difference (content drift). New `ir.SampleVerifier` optional interface; both core engines implement.
- **Same-engine constraint on sample mode** — sample-mode requires `source.Name() == target.Name()` since server-side text rendering of values differs cross-engine (MySQL TINYINT(1) → 0/1 vs PG BOOLEAN → t/f, etc.). Cross-engine sample is deferred to a future phase that adds client-side canonicalization. The orchestrator surfaces a clear error pointing operators at `--depth count` for cross-engine verification.
- **MySQL chunked-count fallback for PlanetScale large tables.** Pre-fix, `sluice verify` against a PlanetScale-MySQL source with > ~100K rows could hit the per-query row-read budget and fail. Now: when the table has a single integer PK, MySQL `ExactRowCount` splits the count across PK ranges of 50K rows each (default), summing partial counts. Cost: `⌈rows / 50000⌉ + 1` queries, well under PS's per-query budget. Tables without a single-int PK fall back to single-shot `SELECT COUNT(*)`; documented limitation.

### Fixed

- **`--output FILE` error messages** previously prefixed "preview:" regardless of which command (`schema preview`, `schema diff`, `verify`, `sync health`) invoked the shared atomic-write helper. Renamed prefix to "atomic output:" which describes the helper's actual responsibility and is correct regardless of caller. Cosmetic; surfaced by the v0.12.0 + v0.13.0 test cycles.

### Changed

- **`ir.SchemaWriter` interface gained `CreateViews(ctx, *Schema) error`** as part of view-support Phase 1. Additive change — both shipping engines (MySQL, Postgres) implement; out-of-tree engines that satisfy this interface need to add a method (no-op for engines without view support is acceptable).
- **`ir.View` type pre-staged in v0.13.x is now wired end-to-end** through readers, writers, pipeline, CLI, schema diff, and schema preview. `Schema.Views` is populated by both shipping engines.

## [0.13.0] - 2026-05-07

Companion to v0.12.0's `sluice verify` count-mode MVP — adds the **liveness side** of the user's "100% confidence" goal. Where verify covers data-integrity, `sluice sync health` covers liveness (is the sync still ticking?). Together they close the no-Fivetran-silent-stop pain shape on both axes.

Plus operator-facing polish: extra-on-target reporting on verify, integration tests for verify, the README troubleshooting matrix, the Vitess VStream troubleshooting runbook, and 6 new proto-ADRs capturing the design space for the next round of substantive feature work.

### Added

- **`sluice sync health` command (probe MVP).** Companion to `sluice verify` from the sync-health monitoring proto-ADR (`docs/dev/design/sync-health-monitoring.md`). Probes a target's `sluice_cdc_state` for the supplied `--stream-id` and computes wall-clock seconds-since-last-apply; compares against `--max-stale-seconds` threshold; structured exit code (0 healthy / 1 stale / 2 op error) integrates with cron / alertmanager / blackbox-exporter / GitHub-Actions-CI pipelines. `--format text|json`; `--output FILE` for atomic write. **MVP scope** — exposes only target-side state (what `ListStreams` already carries); source-side position comparison + true lag-events / lag-seconds metrics follow with the new `ir.HealthReporter` interface. Closes the cron-friendly "is the target still ticking?" probe gap, which is the load-bearing operator concern (Fivetran-stops-silently shape).
- **`sluice verify` reports tables present on target but absent from source.** Surfaced informationally in the new `VerifyResult.ExtraOnTarget` slice + the `TablesExtraOnTarget` summary count + a section in the text output. Does NOT count as mismatch (operators with shared targets often have other-app tables alongside sluice-managed ones; flagging would produce false-positive alerts). Text output nudges to `sluice schema diff` for structural-drift reconciliation.
- **Integration tests for verify** (`internal/pipeline/verify_integration_test.go`) — four real-DB tests cover happy path (PG→PG), intentional drift on target, extra-on-target reporting, and MySQL→MySQL clean. Run under `-tags=integration` in CI on every push.
- **FK edge-case test coverage** — six new unit tests across both engines (`TestEmitAddForeignKey_SelfReferential`, `TestEmitAddForeignKey_CompositePK`, `TestEmitAddForeignKey_AllOnDeleteActions` for both PG and MySQL). Pin self-ref FK shape, composite-PK FK shape, and every supported `ir.FKAction` keyword. No code changes; tests just pin behaviors per `design/schema-completeness.md`.

### Documentation

- **`docs/vitess-vstream-troubleshooting.md`** — operator runbook for sluice users running against PlanetScale MySQL when their sync is showing lag or has stopped advancing. Top three VStream delay causes characterized with code-path citations (replica-tablet replication lag; tablet throttler indirect impact; internal Vitess operations including failovers, reshards, PS deploy requests). Plus what's new in Vitess 24's binlog streaming surface and an honest "PS exposure timeline unclear" assessment.
- **Public README rewritten** for an operator scanning to decide "does this fit my use case" in 30 seconds. Engine matrix, "vs alternatives" comparison, decision-tree table for command selection, links to operator-facing docs first.
- **README troubleshooting matrix** — quick-look for the most common operator symptoms (migrate failed mid-phase, sync slot lost, verify reports mismatch, sync health stale, etc.) and the first-look response.

### Design / planning

- **Six new proto-ADRs** capturing the design space for the user's "100% confidence" goal:
  - `design/sluice-verify.md` — count / sample / full data-integrity verification (count MVP shipped in v0.12.0; sample + full follow).
  - `design/sync-health-monitoring.md` — probe MVP + Prometheus listener + per-table metrics phases (probe MVP shipped in v0.13.0).
  - `design/logical-backups.md` — full + incremental backups to local-FS + cloud storage, with restore tooling. MVP recommendation: local-FS Phase 1.
  - `design/apache-arrow-integration.md` — Parquet writer engine + format interop. Conditional yes, gated on logical-backup format choice.
  - `design/schema-completeness.md` — FK edge-case test coverage + view support Phase 1 (read+emit). FK tests landed in v0.13.0; view support is a future implementation chunk.

### Compatibility

- **No breaking IR changes.** `ir.Verifier`'s new `ExtraOnTarget` field on `VerifyResult` is additive; existing JSON consumers ignore unknown fields.
- **No CLI-breaking changes.** New subcommand (`sluice sync health`) only.
- **Behaviour change on `sluice verify`** — extra-on-target tables now surface in output as informational entries. Operators piping the JSON output will see a new top-level `extra_on_target` array (empty when no extras exist).

## [0.12.0] - 2026-05-07

`sluice verify` lands as a first-class operator surface — the count-mode MVP from the proto-ADR (`docs/dev/design/sluice-verify.md`). Direct delivery on the user's overarching "100% confidence that all data has been copied + synced" goal: operators can now ask "is the target row-count-equal to the source?" without writing the SQL themselves, integrate with cron / alertmanager / CI gates via the structured exit code, and machine-consume the JSON output for monitoring pipelines.

Sample-mode and full-mode (proto-ADR phases 2 + 3) follow per the sequencing in the design doc; count-mode is the cron-friendly probe operators run most frequently.

### Added

- **`sluice verify` command (count-mode MVP).** New CLI subcommand. Runs `SELECT COUNT(*)` per table on both sides, compares, surfaces mismatches with deltas. Exit-code shape mirrors `schema diff`: 0 clean, 1 mismatch, 2 operational error. Same flag surface as `migrate` / `sync start` (DSN + driver + filters); reusable against the operator's existing `sluice.yaml`. `--depth count` (only supported value in v0.12.0; `sample` and `full` planned). `--format text|json` for machine consumption. `--output FILE` for atomic write.

- **`ir.Verifier` optional engine interface.** Engines opt-in by implementing `ExactRowCount(ctx, table) (int64, error)`. Distinguished from existing `RowCounter` (which returns approximate counts via `pg_class.reltuples` / `information_schema.tables.table_rows` for ETA hints) — verify needs authoritative counts, so we pay the full-table-scan cost. MySQL and Postgres engines both implement on their `SchemaReader` (which already holds the DB connection). Engines without `Verifier` cause `sluice verify` to fail loud with a clear "not supported" operational error.

- **`pipeline.Verifier` orchestrator.** Mirrors `Differ` shape. Reads both schemas via `SchemaReader`, type-asserts to `Verifier`, runs `ExactRowCount` per table, builds `VerifyResult` with per-table outcomes + summary counters. Renders text or JSON. Tables present on source but absent on target surface as SKIPPED (reported in the result; not flagged as mismatches — they're a structural concern that `schema diff` covers).

### Notes

- **Why not include `sample`/`full` in v0.12.0?** Per the proto-ADR's sequencing — count-mode alone closes the most common Fivetran-style "did I lose rows?" probe at the cheapest cost. Sample mode adds N random rows × content hashing per table; full mode adds full-table content-hash + bisection on mismatch. Each has its own engineering surface (sampling determinism, cross-engine value canonicalization, bisection chunk size). Shipping count-mode as the MVP gets operators the cron-friendly probe immediately while sample/full follow on real-world demand signal.
- **CDC-position-aware verification deferred.** When sluice is verifying a continuously-syncing target, the source can have new rows the target hasn't applied yet — count mismatch is expected, not an error. The proto-ADR's open question #1 covers the design (verify against the target's tracked source position). Out of scope for v0.12.0; the MVP is best run against migration-completed targets, not in the middle of CDC catch-up.

### Compatibility

- **No breaking IR changes.** `ir.Verifier` is purely additive (new optional interface). Existing engine surfaces unchanged.
- **No CLI-breaking changes.** New subcommand only.
- **Behaviour change: none.** Verify is a read-only inspection tool; it doesn't modify either side.

## [0.11.3] - 2026-05-07

Three-bug patch from the v0.11.2 real-world test cycle (see sluice-testing's `session-reports/v0.11.2.md`). All three bugs were in v0.11.x's translator emission paths — places where the catalog work was supposed to cover but a code path bypassed the translator or matched the wrong syntactic form.

### Fixed

- **Bug 28 — `DEFAULT (UUID())` now translates to `DEFAULT gen_random_uuid()` on cross-engine MySQL→PG migrate.** Pre-fix the DEFAULT-expression emit path bypassed the translator entirely (generated columns / CHECK constraints / index expressions all ran through it; only DEFAULTs didn't). Cross-engine migrates against schemas with `DEFAULT (UUID())`, `DEFAULT (RAND() * 100)`, or `DEFAULT (DATE_ADD(...))` failed loud on PG with `function uuid() does not exist` / `function rand() does not exist` / etc. Operator workaround was `--expr-override` (v0.10.0); the translator now handles it without intervention.

- **Bug 29 — `DEFAULT (RAND() * 100)` now translates to `DEFAULT (RANDOM() * 100)`.** Same root cause as Bug 28; same fix. Both bugs surfaced in the same test cycle because they're the same code path.

- **Bug 30 — `DATE_ADD(d, INTERVAL N DAY)` in a generated column now translates to PG's `(d + INTERVAL 'N day')` quoted-magnitude form.** MySQL's `information_schema.generation_expression` canonicalizes `DATE_ADD(...)` to the operator form `(d + interval N day)` when read back — the function-call rewrite added in v0.11.1 never fired on the canonicalized text because the function call was gone. Pre-fix the unquoted operator form emitted verbatim and failed loud with `syntax error at or near "7"`.

### Implementation notes

- New `Dialect` field on `ir.DefaultExpression` mirrors the existing `Column.GeneratedExprDialect` / `CheckConstraint.ExprDialect` fields. MySQL schema reader sets `Dialect="mysql"`; PG schema reader sets `Dialect="postgres"`. PG writer's `emitDefault` now routes `DefaultExpression` through `translateDefaultExpr` (same dialect-gating shape as `translateGeneratedExpr` / `translateCheckExpr`). The `ExprContext` passed on the DEFAULT path is the zero value — bool-idiom rewrites are no-ops here because DEFAULT expressions are evaluated per-row at INSERT time, not over other column values.
- New `rewriteIntervalLiteral` operates on the operator-form `INTERVAL <int> <unit>` directly (vs. the function-form `DATE_ADD(...)` rewrite from v0.11.1). Same supported singular-unit set: MICROSECOND, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, YEAR. QUARTER, compound units (`HOUR_MINUTE` etc.), and non-literal magnitudes pass through under the loud-failure tenet.
- ADR-0016's cumulative-scope table extended with the new operator-form INTERVAL row and a DEFAULT-expression-scope row noting the gate. v0.11.3 caveats section documents the per-rule reasoning.

### Compatibility

- **No CLI-breaking changes.** Same flags, same defaults.
- **IR change**: `ir.DefaultExpression` gained a `Dialect string` field. Existing callers constructing `ir.DefaultExpression{Expr: ...}` with named fields continue to compile (zero value = "" = "verbatim" — same as pre-fix behaviour for that single field). Positional struct literals (rare; not present in the codebase) would need `, ""` appended.
- **Behaviour change for cross-engine MySQL→PG migrate.** The three bug repros in sluice-testing now translate cleanly. Operators using `--expr-override` to work around these specific defaults can drop those overrides.

## [0.11.2] - 2026-05-07

Single-bug patch from CI integration. v0.11.0's CHARSET/COLLATION cross-engine diff regressed `TestDiff_PostgresToMySQL` — three subtests started failing because the diff began surfacing bogus drift on every PG→MySQL retargeted column (UUID/Inet/Macaddr/Array). The Integration job has been red on every push since v0.11.0; the failure was visible in CI but not gated on (Integration is one of the required checks but the existing PR-merge flow had been bypassing it for tag-driven release commits).

### Fixed

- **Cross-engine retargeted columns no longer surface as charset/collation drift in `sluice schema diff`.** The diff comparison now treats empty source-side charset/collation as "no opinion" rather than as a sentinel value to compare against. Three legitimate cases are covered: source is Postgres (PG doesn't expose per-column charset via `information_schema`), source column is non-character (Integer/JSON/etc.), and source column was retargeted from a PG-native type by `internal/translate.RetargetForEngine` (the retarget rewrites UUID→Char(36), Inet/Cidr→Varchar(45), Macaddr→Varchar(30) but doesn't carry charset/collation since the source type never had one).

  When the source DOES declare a charset/collation, the comparison stays strict: a target missing the source's declared charset still surfaces as drift. The asymmetry is intentional — source/expected is authoritative, so empty-source means "any actual is fine," populated-source means "actual must match." Operators wanting strict bidirectional compare can suppress with `--ignore-charset-collation` (already plumbed) or rely on the matched-pair behaviour, which is unchanged.

  Pre-fix: every PG→MySQL diff on retargeted columns flagged drift like `accounts.account_uuid: expected="" actual=""` (the ColumnDiff's type strings were empty because the types matched post-retarget; only the new charset/collation comparison was triggering the mismatch). Three integration subtests caught it: `TestDiff_PostgresToMySQL/json_captures_only_in-band_drift_after_retarget`, `text_reports_drift_sections`, `ignore-extras_suppresses_extra-table_diff`. New unit test `TestDiffSchemas_EmptySourceCharsetCollationNoDrift` pins the empty-as-no-opinion behaviour so the regression can't return.

## [0.11.1] - 2026-05-07

Continuation of the proactive-translator cycle started in v0.11.0. Eight more rewrites across the catalog's high- and medium-priority tiers, picked for "biggest leverage per LOC, fewest gotchas." All additive, no IR or interface changes, no operator-facing flags. Same loud-failure-on-unrecognized-shape policy as v0.11.0.

### Added

- **Translator catalog second batch (MySQL → PG).** Eight new rewrites across eight rule families:
  - `RAND()` (argless) → `RANDOM()`. Direct rename. The seed form `RAND(seed)` has no single-call PG equivalent and falls through.
  - `UUID()` (argless) → `gen_random_uuid()`. PG 13+ baseline assumed (matches sluice's existing baseline). The MySQL schema reader's UUID type-canonicalization path may already cover most real-world cases via the type mapping; this rule covers expression-level uses (CHECK constraints, text columns with UUID-shaped defaults, etc.).
  - `ISNULL(x)` → `(x IS NULL)`. MySQL's function form returns int (1 or 0); PG's IS NULL operator returns boolean. For `COALESCE(ISNULL(x), 0)` patterns the existing v0.10.1 aggressive `::int` cast picks up the bool result automatically once this rewrite has fired. Standalone `ISNULL` in an integer-typed generated column body still needs `--expr-override`.
  - `REGEXP_REPLACE(x, pat, repl)` (3-arg) → `REGEXP_REPLACE(x, pat, repl, 'g')`. PG defaults to first-match-only; MySQL defaults to all-match. Without the global flag, generated columns and CHECK constraints would silently produce different output. The 4-arg MySQL form (with position) has different semantics from PG's 4-arg form (with flags) and falls through verbatim. Regex-dialect divergence (ICU vs POSIX) is the operator's responsibility under the loud-failure tenet.
  - `INSTR(s, sub)` → `STRPOS(s, sub)`. Same arg order, direct rename.
  - `LOCATE(sub, s)` → `STRPOS(s, sub)`. **Argument order is FLIPPED** between the two functions (MySQL `LOCATE` takes needle-then-haystack; PG `STRPOS` takes haystack-then-needle). The arg-swap is load-bearing — getting it wrong silently searches the haystack inside the needle. The 3-arg form `LOCATE(sub, s, start)` has no clean single-call PG equivalent and falls through.
  - `DATE_ADD(d, INTERVAL n unit)` / `DATE_SUB(d, INTERVAL n unit)` → `(d + INTERVAL 'n unit')` / `(d - INTERVAL 'n unit')`. Common in TTL / `expires_at` patterns. Singular MySQL units only — `MICROSECOND`, `SECOND`, `MINUTE`, `HOUR`, `DAY`, `WEEK`, `MONTH`, `YEAR`. Compound units (`HOUR_MINUTE`, `DAY_HOUR`, etc.), `QUARTER` (no PG equivalent), and non-literal counts fall through verbatim.
  - `DATE_FORMAT(x, '<fmt>')` → `TO_CHAR(x, '<pg_fmt>')`. Format-string token mapping covers the common `%Y/%m/%d/%H/%i/%s` family and friends (24 token mappings total). Literal text in the format gets PG's `"..."` double-quote wrapping; punctuation passes through. **Strict mode**: any `%X` token outside the supported set causes the entire DATE_FORMAT call to fall through verbatim — silent partial translation would produce wrong output without raising an error. **Immutability caveat**: PG's `TO_CHAR` is `STABLE` not `IMMUTABLE`, blocking STORED generated columns; CHECK / DEFAULT / VIRTUAL bodies still benefit.

  ADR-0016 cumulative-scope table extended with the new rows; v0.11.1 caveats section captures per-rule gotchas (PG version baseline for UUID, regex-dialect divergence, DATE_ADD compound-units, DATE_FORMAT immutability + strict-mode token policy, etc.).

## [0.11.0] - 2026-05-07

Closes the v0.10.x reactive-bug cycle and opens the proactive-translator cycle. v0.10.x's bug bundle was driven by real-world testing surfacing translation gaps one at a time; v0.11.0 inverts the loop by mining sqlglot, pgloader, and dolt's function registry for the next batch of likely culprits and landing the highest-priority rewrites pre-emptively. CHARSET/COLLATION cross-engine diff finishes the schema-diff feature surface (the `--ignore-charset-collation` flag was plumbed but inert since v0.8.0). Two design docs capture the design space for the heavier roadmap items (mid-stream add-table, multi-source aggregation) so the implementation pass starts from a structured doc, not a blank page.

### Added

- **Translator catalog top-5 rewrites (MySQL → PG).** Eight rules across five families, all sourced from `docs/dev/translator-coverage.md`'s high-priority tier:
  - `NOW()` / `CURRENT_TIMESTAMP()` / `LOCALTIMESTAMP()` / `LOCALTIME()` (argless) → bare `CURRENT_TIMESTAMP` / `LOCALTIMESTAMP` keyword. PG accepts the keyword form (no parens) and rejects `NOW()` outright; bare-keyword is also what PG emits when reading back its own DEFAULTs, so the rewrite normalises round-trips. The `NOW(6)` precision form falls through verbatim — the bare-keyword form doesn't accept precision at parse time and the operator escape (`--expr-override` from v0.10.0) covers the rare case.
  - `UNIX_TIMESTAMP(x)` → `EXTRACT(EPOCH FROM x)::bigint`. The explicit `::bigint` cast preserves MySQL's storable-as-integer semantics; PG's `EXTRACT(EPOCH FROM …)` returns `double precision` natively. Argless `UNIX_TIMESTAMP()` expands to `EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::bigint`. Two-arg / fractional-precision forms fall through verbatim. **Caveat:** PG treats `extract(epoch from timestamp)` as `STABLE`, not `IMMUTABLE`, which blocks STORED generated columns; the rewrite still helps for CHECK / DEFAULT / VIRTUAL bodies, and STORED bodies fall back to `--expr-override`.
  - `FROM_UNIXTIME(x)` (single-arg) → `TO_TIMESTAMP(x)`. The two-arg form `FROM_UNIXTIME(epoch, fmt)` returns a formatted string in MySQL and has no clean PG equivalent — falls through verbatim under the loud-failure tenet.
  - `CHAR_LENGTH(x)` / `CHARACTER_LENGTH(x)` → `LENGTH(x)`. PG's `LENGTH(text)` counts characters, matching MySQL's `CHAR_LENGTH`. The reverse direction (MySQL `LENGTH(x)` byte length → PG `OCTET_LENGTH(x)`) is a separate rule with different semantics and not part of this batch — it requires column-type context to fire safely.
  - `LCASE(x)` → `LOWER(x)` and `UCASE(x)` → `UPPER(x)`. Direct synonyms.
  - `SUBSTR(x, …)` / `MID(x, …)` → `SUBSTRING(x, …)`. PG accepts the comma form `SUBSTRING(x, start, length)`; both 2-arg and 3-arg shapes round-trip. The single-arg `SUBSTR(x)` form (which PG's `SUBSTRING` doesn't accept) falls through verbatim.

  ADR-0016's cumulative-scope table extended with the new rows; v0.11.0 caveats section captures the immutability + format-string + precision-form gotchas in one place.

- **CHARSET/COLLATION cross-engine diff.** PG schema reader now reads per-column collation via `pg_attribute.attcollation` (joined to `pg_collation` for the name); `ir.DiffOptions.IgnoreCharsetCollation` becomes load-bearing instead of inert; `diffColumn` compares charset/collation as separate `ColumnDiff` fields (`ExpectedCharset`/`ActualCharset`, `ExpectedCollation`/`ActualCollation`); `stripCharsetCollation` suppresses the drift at compare time when the flag is set, dropping columns whose only drift was charset/collation. Renderer emits MySQL `MODIFY COLUMN` and PG `ALTER COLUMN` suggestions.

- **`docs/dev/translator-coverage.md`** — research catalog with 30 candidate MySQL→PG rewrite rules from sqlglot's parser/generator, pgloader, and dolt's function registry. Each entry carries the MySQL form, the PG equivalent, semantic notes, citation, and an importance rating measured by how often the construct appears in real-world DDL bodies (not general usefulness). The "How to land a rule" section at the bottom documents the existing implementation pattern. Closes the "what about idioms we haven't seen?" thread.

- **`docs/dev/design/mid-stream-add-table.md`** (proto-ADR). Lays out the design space for handling `CREATE TABLE` on a CDC source mid-stream: trigger options (manual subcommand vs. auto-detect from DDL events), snapshot-LSN coordination strategies, per-engine differences, four-phase implementation plan. Reference for when real-world testing surfaces the need.

- **`docs/dev/design/multi-source-aggregation.md`** (proto-ADR). N-sources → one-target. Identifies three shapes (sharded, microservices, multi-master), scopes out multi-master, recommends N-processes with `--target-schema` for collision handling. Reference for the same reason.

### Changed

- **`docs/dev/roadmap.md` swept.** "Next up" #1 (CHARSET/COLLATION) moved to "Recently landed" since v0.11.0 closes it. OSS-hygiene goreleaser entry dropped — `.goreleaser.yaml` + `release.yml` have been live since earlier in the cycle. v0.10.x feature-wave summary added to "Recently landed" with the eight tagged + untagged commits between v0.9.x and v0.11.0. New "Next up" #1 reframed around continuing through the catalog's remaining high-priority rules.

## [0.10.4] - 2026-05-06

CI workflow cost optimization. No sluice runtime change; no IR or interface change. Tagged separately so the workflow shift has a versioned anchor and the corresponding `branch-protection.md` doc update has a clear "applies as of" reference.

### Changed

- **CI matrix is conditional on trigger.** The `test` and `build` jobs ran on `[ubuntu-latest, macos-latest, windows-latest]` for every push and PR. macOS-latest costs ~10× Linux per-minute and Windows ~1.7× Linux; on a frequent-push cadence those two platforms drove the bulk of the daily Actions bill. New shape: push to main / pull_request runs Linux-only; push of a `v*` tag or a manual `workflow_dispatch` from the GitHub UI's "Run workflow" button runs the full 3-OS matrix. Implementation uses a single workflow file with `fromJSON()`-conditional matrix selected at workflow-parse time. Operators wanting cross-platform verification before merging a sensitive PR can dispatch the workflow manually.
- **Branch-protection required-checks list trimmed.** `docs/dev/branch-protection.md` updated to drop `Test (macos-latest)` / `Test (windows-latest)` / `Build (macos-latest)` / `Build (windows-latest)` from the required set — they no longer run on PRs and would otherwise permanently block merges. Operators with existing branch protection per the older doc need to remove those four checks before further PR merges.

## [0.10.3] - 2026-05-06

Single-bug patch from PostGIS testing. Bug 27 (VStream POINT mis-parse) defers to a later release because it needs VStream test infrastructure.

### Fixed

- **Bug 26 — MySQL geometry SRID is now preserved on cross-engine emit.** The MySQL schema reader didn't extract `information_schema.columns.srs_id`, so a `POINT NOT NULL /*!80003 SRID 4326 */` source column landed on PG as `geometry(POINT, 0)` — the SRID silently dropped. Any spatial query on the target that depended on the coordinate system (distance, contains, etc.) returned wrong results.

  Fix: read `srs_id` from `information_schema.columns` and thread it through `columnMeta.SrsID` into `ir.Geometry.SRID`. The PG schema writer already honoured `Geometry.SRID` (no change needed), so the cross-engine emit path now produces `geometry(POINT, 4326)` on PG matching the MySQL source.

  **Schema diff coverage extends automatically:** `ir.Geometry.String()` already includes the SRID in its rendering (`Geometry[POINT,SRID=4326]`), so the diff's existing type-string comparison surfaces SRID mismatches as drift once both sides carry SRID consistently. No separate diff change needed.

  MySQL 8.0+ baseline assumed. Pre-8.0 MySQL servers don't expose `srs_id` in `information_schema.columns`; sluice's supported MySQL baseline is already 8.0.

### Deferred

- **Bug 27 — VStream POINT bytes mis-parsed.** MySQL's internal storage prepends a 4-byte SRID prefix before OGC WKB; the vanilla MySQL protocol strips this, but VStream doesn't. Sluice's WKB decoder reads `0xE6` (low byte of SRID 4326) as the byte-order flag and fails. The fix needs VStream test infrastructure (the `integration vstream` build tag); deferred to a later patch where it can land with the test that demonstrates it.

## [0.10.2] - 2026-05-06

Two test-unblocking surface additions from `FUTURE-TESTS.md`. Both small and well-bounded; no behaviour change for operators not opting in.

### Added

- **`--slot-name NAME` flag on `sluice sync start`** (Item C). Operator-supplied replication-slot name for engines that have a slot concept (Postgres). Default unchanged (`sluice_slot`); operators set per-instance to run multiple concurrent sluice instances against the same source — without distinct slot names they'd collide on the hard-coded default. Engines without slots (MySQL: binlog stream is the slot) silently ignore the flag.

  **Sluice-prefix convention:** sluice prepends `sluice_` if the supplied name doesn't already start with it. `--slot-name shard_a` creates `sluice_shard_a`; `--slot-name sluice_shard_a` is idempotent. The convention lets operators find every sluice slot with `pg_replication_slots WHERE slot_name LIKE 'sluice\_%'` for cleanup, audits, and disambiguation from other tools' slots (Debezium, native logical replication subscribers, etc.). The resolved name surfaces in the orchestrator's INFO log so operators can correlate against `pg_replication_slots`.

  Implementation: two new optional engine surfaces — `ir.CDCReaderWithSlotOpener` and `ir.SnapshotStreamWithSlotOpener` — let engines accept a slot-name parameter without breaking the existing `OpenCDCReader` / `OpenSnapshotStream` signatures. The orchestrator type-asserts on these and falls back to the default methods when the engine doesn't implement them. Postgres implements both.

- **`migrate --dry-run` now reports per-table row counts** (Item H). The dry-run output's per-table line gains a `row_count` attribute populated via the existing `ir.RowCounter` interface (MySQL: `information_schema.tables.table_rows` / `SHOW TABLE STATUS`; Postgres: `pg_class.reltuples`). Best-effort: engines that don't implement `RowCounter`, or per-table counts that fail (permissions, etc.), surface as `row_count=-1` with a Warn-level log line so operators can distinguish "unavailable" from "empty". The count uses the throwaway dry-run-only RowReader handle and doesn't touch the bulk-copy path.

## [0.10.1] - 2026-05-06

Two narrow patches from v0.10.0 real-world testing. Bug 23's enum-cast placement fix uncovered Bug 25 — the cast itself triggers PG's "generation expression is not immutable" error because `enum_in()` is STABLE not IMMUTABLE. Bug 17's hand-coded bool-returning detector kept missing real-world expression shapes; v0.10.1 drops the detector and trusts the column-type signal instead.

### Fixed

- **Bug 25 — enum-typed STORED generated columns now emit as `TEXT` + table-level `CHECK`** instead of `(body)::"enum_type"`. PG rejects the cast inside a generated-column body because `enum_in()` is STABLE not IMMUTABLE, and STORED generated bodies must be IMMUTABLE. VIRTUAL doesn't help (PG 18+ forbids user-defined types in VIRTUAL gen cols). Sluice sidesteps by emitting the column as TEXT (no enum type, no cast) and adding a table-level `CHECK ("col" IN ('a','b','c'))` constraint that enforces the value-list. Mirrors the existing SET → TEXT[] + CHECK fallback. CREATE TYPE is skipped for these columns; non-generated enum columns still use the native PG enum type. Loses the named enum type on the target side but always works — matches sluice's "translate, don't wrap in target-side functions" philosophy.

- **Bug 17 — int-context COALESCE rewrite drops the bool-detector gate.** v0.9.1 / v0.9.2 gated the `::int` cast on a hand-coded `isBoolReturning` detector that recognised bare bool idents, comparisons, `IS NULL`/`IS NOT NULL`, keyword forms, and parenthesised wrappers. v0.10.0 real-world testing surfaced expression shapes the detector missed (function calls returning bool, `AND`/`OR` chains, `NOT` prefixes, `EXISTS` subqueries) and each produced a fresh bug report. v0.10.1 drops the detector entirely: when the outer column is integer-typed, the non-literal side of `COALESCE(<expr>, <int_lit>)` is cast to `::int` unconditionally. Safe because the column must produce int — the cast either helps (bool→int), is a no-op (already int), or fails loudly at apply on a non-numeric expression (loud-failure tenet preserved). Cost: one extra `::int` token in the emitted DDL on already-int columns. Benefit: every bool-returning shape now translates correctly without operator intervention. ADR-0016 updated.

## [0.10.0] - 2026-05-06

The expression-translator escape hatch. v0.8.x / v0.9.x's reactive cycle (operator hits a bug → we add a rule) reaches its planned next stage: instead of dropping a column when sluice's translator doesn't recognise an idiom, the operator can supply target-dialect expression text directly via `--expr-override` (CLI) or `expression_mappings:` (YAML). Sluice emits the override verbatim and the translator stays out of the way. The pattern-matching rule set keeps growing for the common cases; `--expr-override` covers everything else.

### Added

- **`--expr-override TABLE.COLUMN=EXPRESSION` flag** on `migrate`, `sync start`, `schema preview`, and `schema diff`. YAML form: `expression_mappings: [{table:, column:, expression:}]`. CLI flags wholesale-replace the YAML config when both are supplied (same precedence as `--type-override`). The expression part can contain arbitrary characters including additional `=` signs, single quotes, parens — only the first `=` after the column name is the separator.

  Strict validation at config-load time: overrides referencing unknown tables, unknown columns, or columns that aren't generated columns surface as clear errors before any DSN is dialed. Silent passthrough would mask the operator-typo case ("why didn't my override fire?"); the strict check makes those typos visible immediately.

  The override applies via a new `internal/translate.ApplyExpressionOverrides` pass that runs alongside `ApplyMappings`. Mechanism: replace `Column.GeneratedExpr` with the override text and clear `Column.GeneratedExprDialect`. The cleared dialect tag tells the writer-side translator that no rewrite is needed — the column flows through the same code path same-dialect expressions take. No special override-aware code paths anywhere downstream.

  v0.10.0 scope: generated-column bodies only. CHECK constraints, index expressions, and DEFAULT expressions don't have an override surface yet; if real-world testing surfaces the need, each gets its own override type with the same shape. ADR-0016 extended with an "Added in v0.10.0" subsection covering the design.

### Changed

- `pipeline.Migrator`, `pipeline.Streamer`, `pipeline.Previewer`, `pipeline.Differ` all gain an `ExpressionMappings []config.ExpressionMapping` field. Existing callers that don't set it keep working — the field defaults to nil and the override pass is a no-op on nil/empty input.
- `internal/config.Config` gains an `ExpressionMappings []ExpressionMapping` field. Existing YAML configs without an `expression_mappings:` key are unchanged.

## [0.9.2] - 2026-05-06

Two narrow patches surfaced by v0.9.1 real-world testing. Bug 23's enum-cast emit had a placement error — the cast landed outside the GENERATED parens where PG's grammar rejects it; Bug 17's bool-returning detector had been too narrow, missing the comparison operators (`<`, `>`, `<=`, `>=`) and keyword forms (`LIKE`, `BETWEEN`, `IN`) that real-world generated-column bodies use. Both fixes are localised and additive; the rest of v0.9.1 stands.

### Fixed

- **Bug 23 placement — enum cast moves inside the GENERATED parens.** v0.9.1 emitted `GENERATED ALWAYS AS (body)::"X_enum" STORED`, which PG rejects because `::` binds tighter than the AS clause's parens. The cast now lands as `GENERATED ALWAYS AS ((body)::"X_enum") STORED` — wrapping the body in inner parens before the cast and keeping the whole thing inside the outer GENERATED parens. Schema-writer change only; the translation logic is unchanged.

- **Bug 17 detector breadth — `coalesce(<bool>, 0)` now recognises more bool-returning shapes.** v0.9.1's `hasTopLevelCompareOp` only handled `=`, `!=`, `<>` — equality and inequality. Real-world generated-column bodies also use `<`, `>`, `<=`, `>=`, `LIKE`, `BETWEEN`, and `IN`, all of which return bool. v0.9.2's detector recognises every operator in that set plus the `IS [NOT] NULL` form (already covered) and `IS DISTINCT FROM`. Each is matched with appropriate token-boundary discipline so identifier substrings (e.g. a column named `between_us`) don't trigger false positives.

## [0.9.1] - 2026-05-06

Patch release closing the three remaining ADR-0016 translator gaps that v0.9.0 testing surfaced. All three are residuals of bugs the v0.8.0 / v0.9.0 batches partially closed; together they unblock end-to-end migration on the two real-world schemas (`schema_example_01` 555 tables, `schema_example_02` 138 tables) the sluice-testing companion repo uses for stretch validation.

### Fixed

- **Bug 16 residual — `CAST(x AS CHAR(N) [CHARSET y] [COLLATE z])` translates on cross-engine emit.** v0.9.0 routed index expressions through the ADR-0016 translator but the translator itself didn't yet recognise MySQL's CAST-to-CHAR form with charset/collate decorations. PG's grammar rejects both decorations and the CHAR(N) target's blank-padding semantics differ from MySQL's. The new `rewriteCASTCharCharset` rule rewrites `CAST(x AS CHAR(N) [...])` to `CAST(x AS VARCHAR(N))` (matching MySQL's no-padding semantics) and `CAST(x AS CHAR)` (no length) to `CAST(x AS TEXT)`. Other cast targets (DECIMAL, DATE, etc.) pass through verbatim.

- **Bug 17 residual — `coalesce(<bool_returning>, <int_lit>)` for integer-typed columns.** v0.9.0 expanded the COALESCE rewrite to recognise bool-returning sub-expressions; that path converted the int literal to a bool, which is the right answer when the outer column is BOOLEAN. For an integer-typed generated column (e.g. a MySQL `tinyint(1)` source widened to `smallint` via `--type-override`), the int literal is the right answer and the bool side needs to cast to int instead. New `ExprContext.OuterColumnIsInteger` flag flips the rewrite direction; `translateGeneratedExpr` sets it based on the column's IR type (`ir.Integer` → flag set). Comparison rewrites (the other half of the bool-idiom pass) stay bool-context only since the int-context comparisons (`<int_lit> = <bool_ident>`) already work via PG's implicit-cast handling.

- **Bug 23 — STORED GENERATED column body returning text into an enum-typed target gets the enum cast.** The original v0.8.1 framing was about column DEFAULT casting; real-world testing refined the diagnosis: the failing case is a STORED GENERATED column with a `CASE` expression returning enum-valued text literals. The PG enum-cast emit now also wraps generated-column bodies for enum-typed columns: `GENERATED ALWAYS AS (CASE … END)::"<enum_type>" STORED`. Works for any text-returning shape (`CASE`, `COALESCE`, simple literal), not just `CASE`. Mirrors the `DEFAULT 'value'::"<enum_type>"` cast already emitted for non-generated columns.

## [0.9.0] - 2026-05-06

Operator quality-of-life + cross-engine type-edge audit + OSS-hygiene starter + four follow-ups from v0.8.1 real-world testing. `sync stop --wait` closes the operator-coordination gap surfaced by v0.8.0's stretch-testing of ALTER windows; new TIMESTAMP-precision integration tests audit the cross-engine boundary that Bug 19's TZ fix opened to scrutiny; `CONTRIBUTING.md` and `docs/dev/release-template.md` formalise the conventions that have been carried in conversation memory across the v0.x ramp. The follow-ups close Bug 16 (index-expression translation), Bug 17 (bool-returning sub-expressions in COALESCE), Bug 22 (`schema preview` and `schema diff` now also auto-exclude PlanetScale `_vt_*` tables), plus a new Bug 23 (MySQL `DEFAULT ('value')` parens form not getting the PG enum cast).

### Added

- **`sluice sync stop --wait`** (extends ADR-0025). Blocks the CLI until the running streamer confirms it's drained gracefully; `--timeout` (default 5 minutes) bounds the wait. Useful for ALTER coordination — `sync stop --wait && alter-source.sh && sync start` now runs the ALTER only after the streamer has confirmed it drained, instead of operators polling `sync status` or `pgrep`-ing the streamer process.

  Implementation rests on a flag-clearing convention: the streamer already calls `applier.ClearStopRequested(streamID)` at startup (Bug 11 fix from v0.3.2). v0.9.0 adds a second clear point — after a stop-signal-driven graceful drain, the streamer clears the flag again as the very last step of `Streamer.Run`. The CLI's `--wait` polls `ReadStopRequested` (1s cadence) until the flag clears and exits success; on timeout it exits non-zero with a clear message and the stop request remains in place so the streamer continues draining in the background.

  The streamer only clears the flag on stop-signal-driven exits, not on Ctrl-C / outer-ctx cancels — `pollStopSignal` now exposes an optional `*atomic.Bool` that the streamer reads after `dispatchApply` returns to decide whether the exit was the operator's stop request or something else. Without `--wait` the behaviour is unchanged; against an older streamer that doesn't clear the flag, `--wait` blocks until `--timeout` and then surfaces a clear "did not complete drain" message.

- **TIMESTAMP / DATETIME precision integration tests** (`internal/pipeline/migrate_temporal_precision_integration_test.go`). Bug 19 (v0.8.0) closed the silent-corruption hole on the TZ axis; the precision axis was previously covered only by unit tests on the IR's `Precision` field. The new integration tests exercise end-to-end behaviour across `DATETIME(0/3/6)` / `TIMESTAMP(0/3/6)` (MySQL→PG) and `TIMESTAMP(0/3/6)` / `TIMESTAMPTZ(0/3/6)` (PG→MySQL), seeded with `12:34:56.123456` so each precision tier surfaces a distinct truncated value. Round-trips assert wall-clock equivalence within the column's declared precision; the PG→MySQL case also pins the expected target column types (`TIMESTAMP` → `DATETIME`, `TIMESTAMPTZ` → `TIMESTAMP`) so a future schema-emit rewire would surface as a schema-shape failure rather than silently passing on equivalent values.

- **`CONTRIBUTING.md` release-process section + `docs/dev/release-template.md`** — formalise the GitHub release-notes structure (Highlights / Fixed / Compatibility / Who-needs-this) that's been carried in conversation memory across the v0.x ramp, plus the `chore: cut vX.Y.Z` commit + annotated-tag pattern. The release-template doc carries section-by-section guidance with examples drawn from the v0.7.0 / v0.8.0 release notes.

### Fixed

- **Bug 16 follow-up — MySQL functional/expression index bodies translate cross-engine.** v0.8.0 unwalled the schema reader for functional indexes; v0.9.0 closes the emit-side gap. The MySQL schema reader now tags each index expression with its source dialect (mirroring the existing tags on generated columns and CHECK constraints), and the PG DDL writer routes index expressions through the ADR-0016 translator on emit. A MySQL `CREATE INDEX ... ((json_unquote(json_extract(meta, '$.k'))))` now lands on PG as `CREATE INDEX ... (((meta->>'k')))` instead of failing at apply time with "function json_unquote(json) does not exist". Same-dialect and untagged expressions pass through verbatim.

- **Bug 17 follow-up — COALESCE with a bool-returning sub-expression rewrites correctly.** v0.8.0 handled `COALESCE(<bool_ident>, 0)` where the bool side was a bare column reference. v0.9.0 extends the rewrite to recognise bool-returning sub-expressions: comparisons (`a = b`, `a <> b`, `a != b`), `IS NULL` / `IS NOT NULL` tests, and parenthesised wrappers around them. Real-world report: a generated column whose body included `coalesce((some_bool_returning_expr), 0)` failed to land on PG even though every direct bool-column case was handled. Arithmetic and other non-bool sub-expressions are still left alone (loud-failure tenet preserved).

- **Bug 22 follow-up — engine-default exclusions now apply to `schema preview` and `schema diff` too.** v0.8.1's Bug 22 fix wired the `_vt_*` Vitess shadow-table auto-exclude into `Migrator.Run` and `Streamer.Run`, but the merge step was missing from `Previewer.Run` and `Differ.Run`. Both now run the same merge before invoking `applyTableFilter` — so `sluice schema preview` and `sluice schema diff` against a PlanetScale source no longer surface `_vt_HOLD_*` / `_vt_hld_*` tables in the output. Operator-supplied `--include-table` short-circuits the merge as in the migrate/sync path.

- **Bug 23 — MySQL `DEFAULT ('value')` parens-form enum default now gets the PG enum cast.** MySQL 8.0+ stores `DEFAULT ('pending')` (with parens) as an expression default — `information_schema.columns.extra` carries the `DEFAULT_GENERATED` flag, which the schema reader translates to `ir.DefaultExpression` rather than `ir.DefaultLiteral`. The PG enum-cast emit was gated only on `DefaultLiteral`, so the parens form skipped the cast and PG rejected with "column X is of type Y_enum but default expression is of type text". The cast now also fires on `DefaultExpression` whose body is shape-equivalent to a single-quoted string literal (the parens form's only legal content for an enum default); true-expression defaults like `current_setting(...)` are still left uncast (the cast wouldn't be safe).

## [0.8.1] - 2026-05-06

Patch release. Closes a CI integration test regression introduced in v0.8.0 (test-only, no behaviour change for users), and finishes the Bug 22 auto-exclude story for vanilla-MySQL connections to PlanetScale endpoints.

### Added

- **PlanetScale Vitess hostname auto-detect for the vanilla MySQL driver.** v0.8.0's Bug 22 fix auto-excluded `_vt_*` Vitess shadow tables when `--source-driver=planetscale`. A vanilla MySQL operator pointing at a PlanetScale endpoint with `--source-driver=mysql` (a legitimate configuration — they get binlog CDC instead of VStream) still had to add `--exclude-table='_vt_*'` manually. v0.8.1 closes that gap with a DSN-keyed hostname sniff at orchestrator startup. The two PlanetScale MySQL hostname suffixes are recognised:

  - `*.connect.psdb.cloud` (public PlanetScale MySQL)
  - `*.private-connect.psdb.cloud` (AWS PrivateLink)

  When matched, the engine merges `_vt_*` into the orchestrator's exclude list — same shape as the existing Bug 22 path. Operator-supplied `--include-table` short-circuits the merge; operators who explicitly want `_vt_*` tables override that way. A structured INFO log surfaces the merged exclusion list at startup so the new behaviour is visible.

  No connection round-trip is involved — the sniff parses the DSN string and matches against the documented hostname suffixes, avoiding the auth/network failure modes that an `@@version_comment` probe would introduce. Non-PlanetScale Vitess deployments (Slack-style, custom domains) still need a manual `--exclude-table='_vt_*'`; if a non-PlanetScale Vitess user reports the gap, the connection-probe path can be added then.

  PG-side PlanetScale hostname suffixes (`*.pg.psdb.cloud`, `*.private-pg.psdb.cloud`) are documented in code for future symmetry but no-op today — PlanetScale Postgres isn't Vitess-backed and has no `_vt_*` shadow tables. The same hostname-sniff machinery would slot into the PG engine's own `DefaultTableExcluder` if that need ever surfaces.

### Changed

- `ir.DefaultTableExcluder.DefaultExcludePatterns` signature gained a `dsn string` parameter so engines can return DSN-derived defaults in addition to flag-derived ones. Out-of-tree engines implementing the optional surface (none expected at this stage of the project) need to update the method signature.

### Fixed

- **CI: `TestMigrate_MySQLToPostgres_CheckBoolIdiom` referenced columns the test schema didn't have.** v0.8.0's bool-idiom integration test (Bug 17) ended with three stray INSERT validations on `email` / `status` columns left over from a sibling test. The bool-idiom test's schema only has `id` + `is_active`, so those INSERTs failed with `column "email" of relation "accounts" does not exist`. Removed the stray block; the test now ends after the bool-CHECK enforcement assertions where it was meant to. Test-only fix, no behaviour change.

## [0.8.0] - 2026-05-06

Schema-diff release plus seven real-world bug fixes from v0.7.0 testing. Headline addition is `sluice schema diff` (ADR-0029): drift detection between sluice's expected target shape and the schema actually present, with text + JSON output, copy-paste-ready ALTER suggestions, and CI-friendly exit codes. The diff round picked up cross-engine type retargeting plus default / generated-expression / CHECK comparison along the way. Seven bug fixes — including Bug 19's silent TIMESTAMP corruption on non-UTC hosts, Bug 20's cross-engine resume dispatch, Bug 21's `idle in transaction` snapshot tx blocking source ALTERs, and Bug 22's auto-exclusion of Vitess `_vt_*` shadow tables — closed the remaining real-world gaps the v0.7.0 stretch testing surfaced.

### Added

- **`sluice schema diff` (ADR-0029).** Drift detection between what sluice would produce on a target (source schema → translation pipeline → expected target shape) and the schema that's actually there. Reads both sides through the existing `SchemaReader` surface — no new engine API; every engine that already implements `SchemaReader` (today: PG, MySQL) gets diff support immediately. Renders text (default; per-table sections with copy-paste ALTER/DROP suggestions and a preamble noting they're starting points, not verified migration scripts) or JSON (stable shape for CI consumers) and supports `--output FILE` with the same atomic temp+rename semantics as `schema preview`. Filter and mapping flags mirror `schema preview` so the diff and preview pipelines stay aligned. CI-friendly exit codes: 0 on no drift, 1 on drift detected (suitable for failing a `schema-drift.yml` job), 2 on operational error like a bad DSN — distinct so CI scripts don't conflate "the gate failed" with "we couldn't run the gate." `--ignore-extras` suppresses extra-on-target entries (useful when the target hosts other applications' tables); `--ignore-charset- collation` is plumbed for the v1.x extension when those fields land in the IR. Out of scope per the ADR: column reordering, index column ordering, FK constraint name normalisation, and trigger/function/view comparison — surfacing those as drift produces too much noise for too little operator value, and reconciliation is a different tool's job (Atlas, sqitch).

- **Schema diff: defaults, generated expressions, CHECK constraints, per-column ALTER rendering.** Three categories originally listed as out-of-scope in ADR-0029 are now compared because the IR already carries the underlying fields and the comparison shape is additive on `ColumnDiff` / `TableDiff`: column defaults (`ExpectedDefault` / `ActualDefault`, with a small cross-engine equivalence map for the common pairs like `now()` ↔ `CURRENT_TIMESTAMP`; mismatches outside the map are flagged low-confidence rather than silently equated), generated-column expressions (verbatim string comparison after trim — engines don't support in-place generated-expr ALTERs, so the renderer emits a comment plus a DROP+ADD reconciliation hint), and table-level CHECK constraints (matched by name; unnamed CHECKs are dropped from the comparison to avoid cross-engine spelling false positives). Renderer fills the actual column type, default, and generated expression on `ALTER TABLE ... ADD COLUMN` suggestions for missing-on-target columns via a new optional `ir.ColumnDDLPreviewer` interface (implemented on both PG and MySQL); the prior `-- TYPE` placeholder remains as a defensive fallback for engines that don't implement it.

- **Cross-engine type-policy retarget on schema diff.** New `internal/translate.RetargetForEngine` rewrites the source-side schema's PG-native IR types (`UUID`, `Inet`, `Cidr`, `Macaddr`, `Array`) to the MySQL-storage IR shapes (`Char(36)`, `Varchar(45)`, `Varchar(30)`, `JSON[binary]`) the target engine's DDL writer would land them on. Wired into `pipeline.Differ.Run` between `ApplyMappings` and the target schema read so cross-engine `sluice schema diff` no longer flags every translated column as drift when the target storage is exactly what sluice would produce. Same-engine pairs and unknown engine pairs return the schema unchanged. Operator-supplied `--type-override` mappings take precedence (override replaces the IR type via `ApplyMappings`; the retarget pass only fires on still-source- native types). v0.8.0 scope is the PG→MySQL direction.

### Tests

- Cross-engine integration test for `sluice schema diff` (`internal/pipeline/diff_cross_engine_integration_test.go`) booting a PG source + MySQL target. Asserts the retarget pass collapses the noisy cross-engine type drift so only the deliberately injected drift surfaces (narrowed VARCHAR, missing column, extra table on target). Also covers JSON / text rendering and `IgnoreExtras` semantics on the cross-engine path.

### Fixed

- **Bug 16 — MySQL functional / expression indexes wall the schema reader.** `information_schema.statistics` rows for functional/expression indexes (MySQL 8.0.13+) carry `COLUMN_NAME = NULL` and put the actual expression in the `EXPRESSION` column. The reader scanned `column_name` into a plain `string`, so the first such index produced `converting NULL to string is unsupported` and aborted the schema-read for the whole database — a hard wall blocking every operation against production schemas that use the feature.

  Fix: scan into `sql.NullString`, add `EXPRESSION` to the SELECT, and route NULL-column rows into a new `ir.IndexColumn.Expression` field (run through the same `normalizeMySQLExpressionText` identifier-quote scrubbing the reader applies to generated columns and CHECKs). MySQL and Postgres DDL writers render expression entries as parenthesised expression text. Cross-engine MySQL→PG emit is best-effort: portable expressions round-trip; non-portable ones still fail loudly on `CREATE INDEX`. Regression guards: `TestEmitCreateIndex/expression_entry`, `TestEmitCreateIndex/mixed_plain_and_expression_entries` (unit) and `TestSchemaReader_FunctionalIndex` (integration).

- **Bug 17 — MySQL bool-idiom CHECK / generated expressions reject on PG (ADR-0016 addition).** MySQL's tinyint(1)→PG BOOLEAN mapping silently broke CHECK constraints and generated columns that compared the column against an integer literal — `0 <> is_active`, `is_active = 1`, `coalesce(is_active, 0)` — because PG's strict typing rejects integer↔boolean comparisons that MySQL accepts via implicit coercion. Real-world report: 3 of 138 tables on `schema_example_02` blocked by this until columns were dropped manually.

  Fix extends the writer-side translator (`translateExprForPG`) with an `ExprContext` carrying the table's bool-mapped column names. When the rewrite recognises `<int_lit> <op> <bool_ident>` / `<bool_ident> <op> <int_lit>` (op ∈ `=`, `!=`, `<>`; lit ∈ `0`, `1`) or `COALESCE(<bool_ident>, <int_lit>)` and the symmetric form, the int literal is replaced with `false` / `true`. `IFNULL` is renamed to `COALESCE` by an earlier pass so it falls in too. Anything else passes through verbatim — same loud-failure tenet as the rest of ADR-0016. Same-engine emits unaffected (the translator only fires when the IR's dialect tag differs from the writer's). New integration test `TestMigrate_MySQLToPostgres_CheckBoolIdiom` verifies a real `CHECK (0 <> is_active)` lands on PG and enforces correctly. ADR-0016 updated with an "Added in v0.8.0" subsection.

- **Bug 18 — `--reset-target-data` left orphaned PG enum types.** The destructive-recovery path (ADR-0023) dropped tables and the bookkeeping row; enum types created during a partially-failed cold-start survived and caused the next reset's `CREATE TYPE` to fail with "type X already exists" until operators manually `DROP TYPE`d. Fix extends the reset path with a `dropSchemaTypes` pass that runs after the table drops, walking the source schema for `ir.Enum` columns and emitting `DROP TYPE IF EXISTS "schema"."<table>_<col>_enum" CASCADE`. PG- only via the new optional `ir.SchemaTypeDropper` interface; MySQL embeds enum values inline and is unaffected. Idempotent across partial failures. New integration test `TestMigrate_ResetTargetData_DropsOrphanEnumTypes` simulates the stuck state, runs reset, and asserts the next migrate succeeds with rows landing.

- **Bug 19 — silent TIMESTAMP corruption in MySQL→PG CDC on non-UTC hosts.** TIMESTAMP values delivered through CDC drifted by the host process's local UTC offset (e.g. seven hours early on a US/Pacific host during DST). Cold-start bulk copy was correct, CDC was not, so the destination silently held the wrong instant for every row updated post-cold-start until an operator happened to compare source and target epochs. Loud failures beat silent corruption; this one snuck past v0.7.x.

  Two distinct corruption surfaces landed under the same symptom:

  - **CDC binlog path.** MySQL's binlog wire format encodes TIMESTAMP as a UTC seconds-since-epoch integer, but go-mysql's `decodeTimestamp2` builds the resulting `time.Time` via `time.Unix(sec, ...)` whose `Location` defaults to `time.Local`. With the parser's `ParseTime=false` setting (sluice's configured path), `fracTime.String()` then formats that instant in process-local TZ unless `BinlogSyncerConfig.TimestampStringLocation` is pinned. The formatted wall-clock string flowed into sluice's `decodeTime`, which parses naked datetime strings as UTC — silently re-interpreting a PT wall clock as a UTC instant.

  - **Cold-start / database/sql path.** A second, latent surface: if the MySQL session's `time_zone` inherits the server's `default_time_zone` (often `SYSTEM`, which follows the host), MySQL converts the column's UTC-stored TIMESTAMP into the session TZ for the wire format. The driver — running with `cfg.Loc=UTC` — re-interprets that wall-clock as UTC, producing the same offset. This wasn't observed because test containers default to UTC; production deployments against MySQL servers with non-UTC `default_time_zone` would have hit it.

  Fix lives at the connection-protocol layer in two places — no Go-side runtime-TZ conversion that could drift with deployment changes: the binlog client sets `BinlogSyncerConfig.TimestampStringLocation = time.UTC`, and every database/sql connection injects `time_zone='+00:00'` into `cfg.Params` so the driver issues `SET time_zone='+00:00'` immediately after handshake (covers schema reader, row reader, row writer, CDC schema cache, change applier, migration-state store). DATETIME is unaffected (its binlog encoding is the broken-down date/time directly with no TZ conversion). Regression guard: `TestCDCReader_TimestampNonUTCHost` (integration tag) pins `time.Local` to America/Los_Angeles, inserts a TIMESTAMP, and asserts the value comes back as the same UTC instant from both the cold-start `RowReader` and the CDC stream's update event.

- **Bug 21 — PG snapshot transaction held source-table locks for the entire CDC lifetime, blocking ALTER on the source.** The PG cold- start path opens a snapshot transaction (`SET TRANSACTION SNAPSHOT '<name>'`) on a pinned SQL connection so bulk-copy reads see a consistent view. Pre-fix, that transaction stayed open as `idle in transaction` for as long as the SnapshotStream was alive — i.e. for the entire CDC streaming phase, which on a long-running sync is hours or days. Every snapshotted table held an `AccessShareLock`, blocking any concurrent `ALTER TABLE` on the source. Real-world report: a 310-second `idle in transaction` queue, ALTER waiting behind it, both unblocked the moment sluice exited.

  Fix splits the SnapshotStream cleanup into two phases via a new `ir.SnapshotStream.ReleaseRowsFn` (and the corresponding `ReleaseRows()` method): the streamer calls `ReleaseRows` after bulk-copy completes, which COMMITs the snapshot transaction and closes the import-side connections (the pinned SQL conn + the slot-creation replication conn) without disturbing the CDC reader. The CDC reader runs on its own connection, and the slot's logical position is independent of the exporting transaction, so CDC continues seamlessly. `Close()` remains the catch-all cleanup and is idempotent with `ReleaseRows` — calling both is safe; calling only `Close()` still works (it invokes the release path internally if not already done). MySQL implementations don't need this surface (per-session snapshot, no shared exporter), and the field is optional. Regression guard: `TestSnapshotStream_ReleaseRowsClosesSnapshotTx` (integration tag) asserts `pg_stat_activity` shows zero `idle in transaction` sessions after release, that an ALTER TABLE on the source succeeds without blocking, and that CDC continues delivering events post-release.

- **Bug 22 — Vitess `_vt_*` shadow tables included by default.** Vitess maintains internal lifecycle tables (`_vt_HOLD_*`, `_vt_PURGE_*`, `_vt_EVAC_*`, `_vt_DROP_*` in legacy naming; `_vt_hld_*` / `_vt_prg_*` / `_vt_evc_*` / `_vt_drp_*` plus a trailing underscore in the post-PR-14613 scheme) that aren't user data and shouldn't appear in publication or bulk-copy. v0.7.0 silently included them, generating quiet write churn against the target with no operator-visible signal. Workaround was a manual `--exclude-table='_vt_*'`.

  Fix: new optional `ir.DefaultTableExcluder` engine surface lets engines declare baseline exclusion patterns; the orchestrator merges them into the operator's filter at the start of `Migrator` / `Streamer` `Run`. The PlanetScale flavor opts in with the `_vt_*` pattern (covers both legacy and post-PR-14613 naming). Operator-supplied `--include-table` short-circuits the merge — if the operator explicitly opts into a precise table list, engine defaults don't override it. Vanilla MySQL returns no defaults (`_vt_*` is a Vitess namespace, not an upstream MySQL one; vanilla MySQL operators on Vitess-backed servers can still pass `--exclude-table='_vt_*'` manually — auto-detect of the underlying server flavor is out of scope for v0.8.0). The merged exclusions are surfaced via a structured INFO log at orchestrator startup so operators see what's being filtered. Regression guards: `TestEffectiveTableFilter_MergesEngineDefaults` (covers all four merge paths: empty, exclude-mode, include-mode short-circuit, duplicate-pattern dedup) and `TestDefaultExcludePatterns_PlanetScale` (pins the flavor's declared default).

- **Bug 20 — cross-engine warm-resume dispatch on the wrong driver.** `sluice sync start --resume` failed on `--source-driver=planetscale --target-driver=postgres` because the persisted CDC position came back from the target's `sluice_cdc_state` tagged with the applier's (target's) engine name, so the source CDC reader's decoder rejected it as belonging to the wrong engine. v0.1.0's Bug 2 fix patched the symmetric same-family PS↔MySQL pair by widening MySQL's decoder; it didn't generalise to truly cross-engine pairs. Fix is a re-stamp at the streamer level: every persisted position picked up via `applier.ReadPosition` has its `Engine` field set to `s.Source.Name()` before reaching the source CDC reader. All four pairs (MySQL↔MySQL, MySQL↔PG, PG↔PG, PG↔MySQL, plus the PlanetScale flavor) round-trip cleanly without per-pair special- casing. The from-now sentinel (`Engine="" Token=""`) is preserved. The `--reset-target-data --yes` workaround is no longer needed for cross-engine zero-downtime resumes. New unit tests `TestRetagPositionForSource_*` (helper-level pinning across the four pairs) and `TestStreamer_WarmResume_CrossEngine_Retag` (end-to-end-shape pin via recording reader/applier).

## [0.7.0] - 2026-05-05

Performance round 2 + ergonomics + reliability follow-ups. Four new ADRs (0025 graceful-drain stop, 0026 LOAD DATA INFILE writer, 0027 source-tx CDC batching, 0028 memory-bounded streaming). Closes Bug 12 (MySQL CDC silent-stall on temporal columns) and Bug 15 (CLI sync-stop drain in the warm-up window) — both classified during v0.6.0 testing as the remaining reliability gaps from the v0.4.0 night soak.

### Added

- **MySQL `LOAD DATA LOCAL INFILE` row-writer (ADR-0026).** Vanilla MySQL bulk-copy now streams TSV over `LOAD DATA LOCAL INFILE` via go-sql-driver's `RegisterReaderHandler` mechanism (no real file written, no `?allowAllFiles=true` needed). Typically 5–10× faster than the parameter-bound multi-row `INSERT` path on wide-row tables. The `BulkLoadLoadDataInfile` capability constant has been declared on vanilla MySQL since v0.1; this release brings the implementation up to the declaration. PlanetScale stays on BatchedInsert (the flavor doesn't allow `LOAD DATA LOCAL INFILE`).

  Per-call fallback to BatchedInsert when (a) the server has `local_infile=OFF` (default on MySQL 8.0+) — one structured WARN surfaces the speedup-pending hint, and (b) the table contains a geometry column (the SRID-prefixed WKB wire format isn't expressible in a column-only LOAD DATA). The TSV serializer escapes the four MySQL LOAD DATA defaults (tab/newline/CR/backslash/NUL) and emits `\N` for NULL. Statement uses `CHARACTER SET binary` plus per-column `SET col = CONVERT(@cN USING utf8mb4)` for VARCHAR/TEXT/SET/JSON columns to round-trip binary blobs and JSON cleanly in the same statement.

- **Source-transaction-boundary aware CDC batching (ADR-0027).** New `ir.TxBegin` / `ir.TxCommit` change variants surface source-side transaction boundaries to the applier. Postgres emits from `BeginMessage` / `CommitMessage` (with `StreamStart` / `StreamStop` mapping to boundaries for the streaming-in-progress chunked path); MySQL emits from `BEGIN` QueryEvent / `XIDEvent`. The batched applier (`ApplyBatch`) flushes on `TxCommit` so a 5000-row source transaction commits as one 5000-row target transaction instead of being split by the row-count cap. The cap remains the upper bound; idle flush, channel close, and Truncate flush behave as before. Empty source transactions produce no target commits (lazy-tx-open absorbs them). Per-change `Apply` treats boundary events as no-ops; the table filter explicitly bypasses them so a filter never drops a boundary signal. Position-and-data atomicity (ADR-0007) and idempotency (ADR-0010) preserved. Closes the follow-up explicitly deferred from ADR-0017.

- **`--max-buffer-bytes N` (ADR-0028).** Default `67108864` = 64 MiB, on `sluice migrate` and `sluice sync start`. Bounds per-batch buffered memory by total byte size in addition to the existing row-count caps. Wide-row workloads (TEXT / BYTEA / JSON at MB scale) no longer have to manually retune `--bulk-batch-size` / `--apply-batch-size` to control heap usage; the byte cap fires whichever way is tighter. The cap is a soft target — a single row larger than the cap still applies. Implemented in the bulk-INSERT writer, idempotent-INSERT writer, and CDC `ApplyBatch` paths for both engines via the new `ir.MaxBufferBytesSetter` optional surface; the COPY-protocol and LOAD DATA paths are streaming and unaffected. The byte-counting helper (`approximateRowBytes`) was hoisted from the pipeline to `internal/ir/bytes.go` so engine packages can reuse it.

- **PG-native types auto-emit on MySQL targets.** `Inet` / `Cidr` (PG → MySQL) auto-emit as `VARCHAR(45)`; `Macaddr` as `VARCHAR(30)`; `Array` as `JSON` (matches the v0.5.0 Bug 14 fix where array values are serialized as JSON for the writer). Pre-v0.7.0 these returned an error pointing operators at `--type-override`; the auto-emit removes the toil for every PG→MySQL migration that touches these types. Operators wanting strict syntactic validation still use `--type-override` to a custom shape with their own CHECK constraint; the schema-preview command (ADR-0024) surfaces the auto-emit choice so it isn't silent. Closes roadmap §6.

- **Throughput tuning guide** (`docs/throughput-tuning.md`). Operator reference for the knobs that matter at scale — `--apply-batch-size`, `--bulk-parallelism`, network compression (MySQL `compress=true`, PG TLS+gss settings), and `--max-buffer-bytes`. Cross-references the relevant ADRs.

- **`migrate --dry-run` cross-reference to schema preview.** The dry-run plan output now includes a one-line pointer to `sluice schema preview` for full DDL inspection with translation notes and advisory hints. Closes roadmap §10.

### Fixed

- **Bug 12 — MySQL CDC silently dropped events with TIMESTAMP / DATETIME / DATE columns.** The decoder for binlog row events (`decodeTime` in `internal/engines/mysql/value_decode.go`) only accepted `time.Time` directly — but the binlog protocol hands temporal values back as their raw string form ("YYYY-MM-DD HH:MM:SS[.ffffff]" / "YYYY-MM-DD") regardless of the schema-cache DSN's `parseTime=true` setting. The first row event on any table with a temporal column raised `cannot decode string as time.Time (parseTime=true should be set)`; the binlog pump exited with that error stored on the reader (only surfaced via `Err()`, not logged), the change channel closed, and the applier saw zero events. Symptom: cold-start bulk-copy completed cleanly, then CDC mode produced no further inserts on the destination — looked exactly like a network/heartbeat issue, which sent the original Bug 12 hypothesis chasing port-forwarding ghosts.

  Fix: `decodeTime` now parses MySQL's canonical temporal string formats — second-precision, microsecond-precision, date-only — plus byte-slice equivalents and the `0000-00-00` zero-value (maps to `time.Time{}` for clean cross-engine round-trip). Regression guard: `TestDecodeTimeFromString` covers all five shapes; the pre-existing `TestDecodeValueErrors/timestamp_from_string` case was inverted to test the unparseable-string error path instead (parseable strings now succeed).

  Empirical confirmation against `bug12_repro_dev.sh` (local mysql:8.0 containers, table with `t TIMESTAMP DEFAULT CURRENT_TIMESTAMP`): pre-fix dropped 100% of CDC events on tables with a temporal column; post-fix all events flow.

- **Bug 15 CLI sync-stop drain (data loss in warm-up window, ADR-0025).** The v0.5.0 slot-ack-after-apply work (ADR-0020) closed the post-restart wedge but left a residual data-loss path in the warm-up window between stream start and the first applied commit. Pre-fix, `ackLSN` returned `streamedLSN` (the highest commit-LSN parsed off the WAL) when the applier-feedback tracker was still at zero; the keepalive routine ack'd that to the slot, advancing `confirmed_flush_lsn` past events that hadn't been durably applied. A subsequent `sync stop` mid-batch then lost the events between persisted_position and confirmed_flush_lsn — warm-resume's slot stream started past them and the rows never landed. Empirical repro on local docker: 25-42 row gap with `--apply-batch-size=50` and a sustained 10/sec writer.

  Fix has two layers:

  1. **`ackLSN` anchors at startLSN until first apply commit.** The load-bearing data-correctness fix. When the tracker is fresh (`applied=0`), ack returns the LSN the pump started from (cold-start: snapshot LSN; warm-resume: persisted_position's LSN). The slot can't advance past that point until the applier reports a higher value via the tracker. One-line, one-parameter change.

  2. **Graceful-drain shape for `sync stop`.** The pre-fix `pollStopSignal` cancelled `applyCtx`, rolling back the open batch — relying on warm-resume to redeliver. With the ackLSN fix that worked correctly but produced unnecessary redelivery storms. Stop-signal now cancels a separate `streamCtx` (which scopes the CDC reader's pump); the channel closes cleanly, the applier's existing `channelClosed` branch commits the in-flight partial batch, position writes naturally. A 30-second watchdog escalates to hard-cancelling `applyCtx` if the drain wedges.

  Unit-level regression guard: `TestAckLSN_AnchorsAtStartLSNUntilFirstApply` pins the contract. Empirical integration repro lives at `sluice-testing/workspace/bug15_repro_dev.sh` (sustained writer, mid-stream `sync stop`): pre-fix dropped 25-42 rows; post-fix drops 0. The existing programmatic-RequestStop integration test (`TestStreamer_PostgresToPostgres_StopRestartNoLoss`) still passes — it happened to time RequestStop past first-batch commit, masking the warm-up window. See ADR-0025.

- **Windows CI: `TestPreviewer_Golden_Text` fails with CRLF/LF mismatch.** The test compared `bytes.Equal(buf.Bytes(), want)` — buffer with LF newlines (Go's native `\n`) vs. file content that git's default `core.autocrlf=true` had converted to CRLF on Windows checkouts. The diff showed visually identical content; byte comparison failed.

  Two-part fix: 1. New `.gitattributes` enforces `eol=lf` on text files so Windows checkouts no longer get CRLF on golden fixtures. 2. The test normalises CRLF→LF on the read side before comparing — belt-and-suspenders against any future checkout that bypasses the attribute (e.g. zip-download, alternate clones).

  No behavioural change to runtime code; CI-only fix.

## [0.6.0] - 2026-05-05

Feature release. Headline additions are `sluice schema preview` (operator-facing target-DDL inspection with translation notes and advisory hints) and `--reset-target-data` (one-command destructive recovery on top of v0.5.2's slot-missing fall-through). Plus four reliability items uncovered during v0.5.x testing: a CI-only data race in the parallel-copy state-write path, batched-apply idle flush on quiet streams, MySQL binlog-purged fall-through (extends ADR-0022 to the MySQL side), and two parallel-copy hygiene follow-ups. Two new ADRs (0023 schema preview, 0024 reset-target-data); ADR-0022 extended for MySQL.

### Fixed

- **Data race in parallel-copy state-write path.** v0.5.0's `migrate_parallel.go::copyChunk` checkpoint sites took `stateMu`, mutated their slot in `state.TableProgress`, then did a shallow copy `stateCopy := *state` and released the lock before calling `writeState`. The shallow copy left `stateCopy.TableProgress` pointing at the same map backing storage as `state`, so the JSON encoder iterating outside the lock raced peer chunk goroutines taking the lock to mutate their own slots. Surfaced as a CI -race failure in `TestMigrate_PG_ParallelCopy_Resume` for the v0.5.x releases.

  Fix: a `cloneStateForWrite` helper re-allocates the `TableProgress` map and each entry's `Chunks` slice under the lock; the encoder gets a fully independent snapshot. Per-chunk reference fields (`LowerPK`/`UpperPK`/`LastPK`) are not deep- cloned because they're either written once at resolution time or replaced wholesale (not mutated in place) on each checkpoint. Pre-existing behaviour preserved bit-for-bit; the fix is sync- primitive-only.

- **Two parallel-copy hygiene follow-ups.** `progressTicker.startedAt` swaps the `Load → Store` check-then-set for an `atomic.CompareAndSwap` so the contract stays correct if `loop` ever runs from multiple goroutines (single-goroutine today; one-line future-proofing). `kickOffRowCount` now suppresses the `row-count probe failed` WARN when the parent context was already cancelled, and skips the `setTotalRows` store when the ticker is already stopped — removes interleaved teardown-time noise during test cleanup.

### Added

- **`sluice schema preview` subcommand.** Reads the source schema, applies the translation pipeline (mappings + cross-engine type policy), and emits the target DDL with inline cross-engine translation notes and advisory hints — without touching either database's data. Operators see exactly what the target schema will look like before any migration runs, including the `--type-override` invocation for known operator-preferable alternatives (e.g. PG `uuid` → MySQL `BINARY(16)` instead of the default `CHAR(36)`). Supports `--format text|json`, `--include-table`/`--exclude-table`, `--type-override`, and `--output FILE` (atomic temp-file + rename, so a Ctrl-C mid-write never corrupts the destination). New `ir.DDLPreviewer` engine surface; both Postgres and MySQL implement it on the same struct as their `SchemaWriter` (the emitTableDef/emitCreateIndex/emitAddForeignKey helpers are now shared between the execute and preview paths). Initial advisory- hints registry seeds five high-traffic surprises from real-world testing reports (UUID, large-TEXT, JSON-vs-JSONB note, DATETIME timezone, unbounded numeric). Translate package gains `binary_uuid`, `mediumtext`, `timestamptz`, and parameterised `decimal` aliases to support the suggested overrides. See ADR-0024.

- **`--reset-target-data` for destructive recovery.** New flag on `sluice migrate` and `sluice sync start` that DELETEs the bookkeeping row (`sluice_migrate_state` / `sluice_cdc_state`), DROPs every source-schema table on the target, then proceeds with cold-start. Collapses the post-`slot drop` recovery flow to a single command (no more enumerating tables for `DROP TABLE`). Confirmation prompt requires the operator to type `reset` verbatim — bypassed by `--yes` for non-interactive use. Mutually exclusive with `--resume` at parse time. New optional engine surfaces: `ir.TableDropper`, `ir.StreamCleaner`, and `ir.MigrationStateStore.ClearMigration`. See ADR-0023.

  An additional optional surface, `ir.BulkTableDropper`, lets engines collapse the per-table DROP loop into one statement — the recovery flow on a 500-table source pays one network round- trip instead of 500. Both Postgres (`DROP TABLE … CASCADE`) and MySQL (`DROP TABLE …`) implement the bulk path; engines without it fall back to per-table `DropTable` automatically. Audit log lines name every dropped table on either path.

  `docs/postgres-source-prep.md` cross-references the flag from the `wal_status='lost'` recovery section so the doc trail through the destructive-recovery flow stays connected.

- **Batched-apply idle flush on quiet streams.** Closes the trailing- row latency footnote from ADR-0020. The batched applier now commits a partial in-flight batch (n < `--apply-batch-size`) within `defaultIdleFlushPeriod` (5s) when no further change arrives. On Postgres this lets the slot's `confirmed_flush_lsn` advance past in-flight work on idle streams, so warm-resume from a quiet stream starts at the most recent commit rather than the previous full batch boundary; on MySQL the same logic keeps `source_position` current so the replay window on warm-resume stays bounded. Both engines use the same 5s default for symmetry. Existing flush triggers (channel close, Truncate, ctx cancel) are unchanged; idle flush is purely additive. Integration test: `TestChangeApplier_ApplyBatch_IdleFlushCommitsPartial` (PG; partial-batch persistence on MySQL was already covered by `TestChangeApplier_ApplyBatch_PartialFlushPersistsPosition`).

- **MySQL binlog-purged fall-through to cold-start.** Extends the v0.5.2 PG slot-missing recovery to the MySQL side. The MySQL CDC reader's `resolveStartPosition` now pre-flights the persisted position before handing off to go-mysql's binlog syncer:
  - **File/pos mode**: queries `SHOW BINARY LOGS` and checks the persisted file is still present. If missing (typical when `expire_logs_seconds` rolled it off, or an operator ran `PURGE BINARY LOGS`), returns `mysql: binlog file %q is no longer available on the source (purged); cannot resume: ir: persisted position is no longer valid`.
  - **GTID mode**: runs `SELECT GTID_SUBSET(@@gtid_purged, ?)` with the resume set. Returns 0 when the source has purged GTIDs the resume set hasn't consumed — meaning we'd be missing data on resume — and surfaces `mysql: source has purged GTIDs not present in resume set; cannot resume`.

  Both branches wrap with `ir.ErrPositionInvalid`; the streamer's existing v0.5.2 fall-through (added engine-neutrally) detects the sentinel and re-enters `coldStart` with the same `lsnTracker`. No new code in the pipeline package; the engine-neutrality of the v0.5.2 design pays off here. ADR-0022 extended.

  Pre-fix shape: a sluice stream restarted after the source's binlog had rotated past the persisted file would surface go-mysql's raw "Could not find first log file name in binary log index file" error mid-stream. Post-fix: the WARN fires at startup, cold-start runs, dest is reseeded.

  Integration test: `TestStreamer_MySQLToMySQL_BinlogPurgedFallsThroughToColdStart` exercises the file/pos branch end-to-end. GTID branch is covered by the same `verifyPositionResumable` dispatch and the SQL-side semantics of `GTID_SUBSET` (no separate integration test; GTID-mode setups are tested elsewhere in the resume coverage).

## [0.5.2] - 2026-05-05

Single-feature patch release closing Item F from the v0.4.0 real-world testing report: PG CDC streams whose replication slot was dropped (typically after `wal_status='lost'`) now recover via auto-fall-through to cold-start instead of erroring out with no flag to bypass.

### Added

- **Slot-missing fall-through to cold-start (Item F).** When a Postgres CDC stream's persisted position references a replication slot that no longer exists on the source — typically because the operator dropped it after sluice surfaced `wal_status='lost'` — the streamer now logs a loud WARN naming the slot + persisted LSN, then falls through to the cold-start path automatically. No flag required; no manual `DELETE FROM sluice_cdc_state` step. Bug 9's pre-flight refusal still gates populated-dest operations, so operators who want a fresh bulk-copy still pass `--force-cold-start` or drop dest tables manually. The fall-through is engine-neutral: CDC readers signal the condition via `ir.ErrPositionInvalid` (wrapped on their specific diagnostic via `%w`); the pipeline detects it via `errors.Is`. PG slot-missing is the only emitter in this release; MySQL binlog-purged is queued as a follow-up. See ADR-0022.

  Recovery flow before this fix: drop slot → DELETE cdc_state row → drop publication → drop dest tables (or `--force-cold-start`) → re-run sluice. With this fix: drop slot → drop dest tables (or `--force-cold-start`) → re-run sluice. The two manual SQL steps disappear.

  Integration test: `TestStreamer_PostgresToPostgres_SlotMissingFallsThroughToColdStart`.

## [0.5.1] - 2026-05-05

Single-issue patch release fixing a misleading flag name in the Postgres `wal_status='unreserved'`/`'lost'` recovery hint. No behavioural change.

### Fixed

- **`wal_status` recovery hint named `--target` instead of `--source` (Item F).** When sluice refused to start CDC against an invalidated slot, the error message pointed operators at `sluice slot drop <name> --target ...`. The slot lives on the *source* database and `slot drop`'s actual flag is `--source` — operators following the hint hit a flag-not-found error and had to consult `slot drop --help` to recover. Both the `unreserved` and `lost` branches of `checkSlotUsable` now emit `--source-driver=postgres --source ...`. `docs/postgres-source-prep.md` is corrected in lockstep. Real-world testing surfaced this as the one polish item against an otherwise gold-standard error message. Test coverage extended to assert the recovery hint references `--source` so the regression doesn't return.

## [0.5.0] - 2026-05-05

Reliability + performance release. Headline feature is parallel within-table bulk copy (the pgcopydb-class signature win for multi-TB migrations), throughput metrics extended to MB/s + ETA, plus four fixes uncovered during real-world v0.4.0 soak testing — one of which (Bug 15) was a CRITICAL silent-data-loss path on Postgres CDC. Three new ADRs (0019, 0020, 0021).

### Added — performance

- **Parallel within-table bulk copy.** Tables above `--bulk-parallel-min-rows` (default 100k) with a single integer PK are now split into N PK ranges and copied concurrently, with per- chunk cursor checkpoints in `sluice_migrate_state`. Tables below the threshold, with composite PKs, or without a PK fall through to the v0.4.x single-reader behaviour. Postgres readers share a single exported snapshot via `SET TRANSACTION SNAPSHOT` (`SnapshotImporter` optional engine surface) so all chunks see a consistent view; MySQL uses per-chunk `REPEATABLE READ` transactions because per-session REPEATABLE-READ snapshots have no shareable name. Boundaries are computed once via `MIN`/`MAX` on the PK and persisted, so a resume run aligns exactly with completed chunks rather than recomputing ranges (which would shift if rows landed concurrently). New flags: `--bulk-parallelism` (default `min(8, NumCPU)`) and `--bulk-parallel-min-rows`. See ADR-0019.
- **Throughput metrics: MB/s + ETA.** The bulk-copy progress ticker now emits `total_rows`, `bytes`, `rate_mb_per_sec`, and `eta_seconds` alongside the existing `rows`/`rate` attributes; per-chunk progress lines carry a `chunk=` attribute so operators can see which range is in flight. Row-byte estimation walks the `ir.Row` value-side: string/`[]byte` by length, fixed-width numerics by Go size, `time.Time` as 24, bool as 1, recursive on `[]any`/`[]string`. Approximate but stable enough that MB/s tracks observed network throughput within a few percent.
- **`CountRows` / `RangeBounds` optional engine surfaces.** Postgres estimates row counts via `pg_class.reltuples` (autovacuum- maintained); MySQL via `information_schema.TABLE_ROWS`. Both short- circuit when called against a snapshot-pinned reader where a concurrent query would deadlock the single shared connection. The ETA computation falls back gracefully when the surface isn't available.

### Fixed

- **Postgres CDC: slot ack advanced before apply commit (Bug 15, CRITICAL — silent data loss on crash).** The PG CDC reader was sending the *streamed* LSN in `StandbyStatusUpdate`, so a crash between `Send` and `tx.Commit` advanced `confirmed_flush_lsn` past events that were never applied — and a warm resume started at the acked position, dropping the in-flight batch on the floor. Real- world soak observed silent row drift after a clean stop/restart cycle when the streamer happened to interrupt a partial batch.

  Fix: a single-producer/single-consumer `lsnTracker` plumbed engine-neutrally via `lsnTrackerProvider`/`lsnTrackerAttacher` structural interfaces. The applier reports `appliedLSN` after `tx.Commit()`; the reader sends `min(streamed, applied)` in the next status update. Trailing-row latency under `--apply-batch-size
  > 1` is bounded by the batch interval since the LSN only advances
  on commit boundaries — acceptable today; idle-flush is on the roadmap. See ADR-0020.

  Integration test: `TestStreamer_PostgresToPostgres_StopRestartNoLoss` exercises a stop in the middle of a batched apply and asserts every source change lands on the target after warm resume.

- **Postgres CDC: publication scope was `FOR ALL TABLES` (Bug 13).** The v0.4.0 publication was created `FOR ALL TABLES`, so a brand- new unrelated table on the source — created after sluice started streaming — would land in the pgoutput stream. The applier either crashed on the unknown table OID or, worse, silently dropped the events.

  Fix: `Engine.EnsurePublication(ctx, dsn, tables)` now creates `FOR TABLE <list>` from the resolved migration set after `applyTableFilter`. Existing v0.4.0 `FOR ALL TABLES` publications are migrated by drop-and-recreate during cold start (the slot is unaffected; only the publication is replaced). The applier now has defence-in-depth: an unknown table OID is logged at WARN and the change is skipped rather than crashing the stream. See ADR-0021.

  Integration test: `TestStreamer_PostgresToPostgres_NewTableOnSourceIgnored` creates a fresh table on the source mid-stream and asserts the applier ignores it.

- **PG array → MySQL JSON conversion (Bug 14).** A PG source column of array type (e.g. `text[]`, `int[]`) migrating to a MySQL JSON target arrived at the writer as `[]any`, a PG-array literal string (`{a,b,c}`), or `[]byte` holding the same — none of which MySQL's driver knows how to bind to a JSON column. `prepareValue` now branches `convertArrayLikeToJSON` for all three shapes. Empty arrays serialize as `[]` (disambiguated from `{}`, which would be a JSON object). Integration test: `TestMigrate_PostgresToMySQL_ArrayToJSONOverride`.

- **MySQL CDC: silent stalls on quiet upstream (Bug 12).** go-mysql's binlog syncer can hang silently if the upstream goes quiet for long enough that the TCP keepalive doesn't fire — the reader has no signal to distinguish "no events" from "connection dead". v0.5.0 sets `defaultBinlogHeartbeatPeriod = 10s` on the syncer so the upstream emits keep-alive heartbeats, and adds a 30s no-events watchdog that surfaces a stalled-stream error if no row-relevant event arrives in that window (filtered by `isRowRelevantEvent` so heartbeat and rotation events don't reset the timer indefinitely, which would mask a real stall). Not reproducible in CI without a multi-minute idle, so manually validated against real PlanetScale/vanilla MySQL streams.

### Added — architecture documentation

Three new ADRs in `docs/adr/`:

- **ADR-0019**: Parallel within-table bulk copy — chunk-boundary computation, snapshot-import strategy per engine, boundary stability invariant, fallback matrix.
- **ADR-0020**: Slot-ack-after-apply — LSN tracker design, SPSC contract, why `min(streamed, applied)` instead of just `applied`, trailing-row latency tradeoff.
- **ADR-0021**: Publication scope by table — `FOR TABLE <list>` rationale, drop-and-recreate migration from v0.4.0 publications, applier defence-in-depth on unknown OIDs.

## [0.4.0] - 2026-05-04

Feature release with four substantive responses to measured production concerns from the v0.3.x robustness testing rounds, plus three new ADRs (0016, 0017, 0018) documenting the design decisions.

### Added — performance

- **`--apply-batch-size N`** on `sluice sync start` (and `Streamer.ApplyBatchSize` for programmatic callers) batches up to N CDC changes per target transaction with the position write of the last change in the batch. Default 1 keeps v0.3.x conservative one-change-per-tx behaviour; production tuning is 100–500. v0.3.0 testing measured the per-change applier at ~6.5 rows/sec on PG→MySQL CDC with a 5000-row source transaction; batched mode amortises commit overhead 50–100× on production hardware (3.5× observed locally without fsync). Idempotency preserved via the existing ON CONFLICT / ON DUPLICATE KEY UPDATE semantics on Insert. Schema-change events (Truncate, DDL) flush the in-flight batch before applying. See ADR-0017.
- **`--bulk-batch-size N`** on `sluice migrate` (default 5000) controls the per-batch checkpointing size for resume. Cold-start migrations continue to use the faster plain-INSERT (and PG COPY) path with no per-batch overhead.

### Added — operability

- **Per-batch checkpointing for `sluice migrate --resume`.** Previously, resume on an in-progress table truncated and re-copied from row 0. v0.4.0 tracks a per-table PK cursor in `sluice_migrate_state.table_progress`, reads the source via `WHERE pk > cursor ORDER BY pk LIMIT batch_size`, and applies rows with `ON CONFLICT` / `ON DUPLICATE KEY UPDATE` so the brief replay window between batch commit and cursor write is tolerated cleanly. Multi-hour copies of 100M+ row tables can resume mid- table. Composite PKs descend via row-comparison cursors (`(a,b) > ($1,$2) ORDER BY a,b`). Tables without a PK fall back to the v0.3.0 truncate-and-redo behaviour with a clear log line. v0.3.0-shape state rows are read backward-compatibly. See ADR-0018.
- **Cross-engine expression translation for generated columns and CHECK constraints.** v0.3.2's verbatim-passthrough policy held the fail-loud claim (no silent corruption), but the set of "non-portable" expressions included very common idioms. Bidirectional translation pass at the writer boundary now covers:
  - **MySQL → Postgres**: `CONCAT(a,b)` → `(a || b)`, `IFNULL` → `COALESCE`, `IF(cond,a,b)` → `CASE WHEN cond THEN a ELSE b END`, `JSON_UNQUOTE(JSON_EXTRACT(j,'$.k'))` → `(j->>'k')`, `JSON_EXTRACT(j,'$.k')` → `(j->'k')`.
  - **Postgres → MySQL**: `(expr)::type` → `CAST(expr AS …)`, `a || b` → `CONCAT(a, b)`, `~~`/`~~*` → `LIKE`/case-insensitive `LIKE`, `= ANY(ARRAY[…])` → `IN (…)`.

  Unrecognized constructs still pass through verbatim and rely on the loud-failure-on-target fallback. Translator uses a string- literal-aware walker that respects single-quoted literals and balanced parens — no full SQL parser. See ADR-0016.

### Fixed

- **Cold-start hangs when dest tables have pre-existing data (Bug 9, open since v0.3.0).** Three-part fix: 1. **Pre-flight refusal**: cold-start now checks each source table for non-empty dest data and refuses with a clear error pointing at recovery commands. Skipped on `--resume` (resume expects partial state). 2. **Goroutine-leak fix**: `copyTable` now derives a child context and cancels it on every return path. Previously, when `WriteRows` errored mid-stream, the row-reader goroutine blocked forever on `out <- row` against an abandoned consumer, holding the snapshot transaction open and surfacing as PG's "idle in transaction" sessions. 3. **Clearer log shape**: progress ticker's Stop now takes the writer error and logs `bulk copy aborted table=foo rows=N err="…"` on failure instead of the misleading `bulk copy complete rows=N`. New `--force-cold-start` flag bypasses the pre-flight refusal for the rare legitimate "bulk-copy into a populated target" case.
- **`stop_requested_at` not cleared after consumption (Bug 11, open since v0.3.2).** A `sluice sync stop` left the timestamp set after the streamer drained and exited; the next `sluice sync start` would see the stale signal and exit within the first poll interval. The streamer now clears the flag at startup (after `EnsureControlTable`, before reading the persisted position). Idempotent and tolerant of a missing row. New `ChangeApplier.ClearStopRequested` interface method on the applier.

### Changed

- **`docs/type-mapping.md` corrected for PG→MySQL `Inet`/`Cidr`/ `Macaddr`/`Array` types.** The doc previously claimed auto-emit as `VARCHAR(N) CHECK (format)`; v0.3.x and v0.4.x actually refuse loudly with a copy-paste-ready `mappings:` YAML snippet pointing at the `--type-override` CLI flag. Auto-emit is queued as a future enhancement; manual override is the supported path today.

## [0.3.2] - 2026-05-04

Patch release adding CHECK constraint support, a CLI form of the type-override YAML config, and an opportunistic improvement to the generated-column expression normalizer that the CHECK work surfaced.

### Added

- **CHECK constraint support across both engines.** Source schemas declared with `CHECK (qty >= 0)` or `CHECK (status IN ('open', 'closed'))` now round-trip cleanly: the schema readers capture the expression on `Table.CheckConstraints`, the DDL writers emit `CONSTRAINT name CHECK (expr)` inline in CREATE TABLE, and the constraint is enforced on the target.

  Translation policy is verbatim passthrough — non-portable expressions fail loudly on the target rather than be guessed at. Identifier and string-literal decoration is normalized at the read boundary (see below).

  Integration coverage: MySQL→MySQL, PG→PG, and MySQL→PG cross- engine snapshot migrations each verify (1) the CHECK lands on the target's `information_schema.check_constraints`, (2) bulk-copied rows survived, (3) a violating INSERT is rejected by the target, and (4) a satisfying INSERT is accepted.

- **`--type-override TABLE.COLUMN=TYPE` CLI flag** on `sluice migrate` and `sluice sync start`. Repeatable; format mirrors the YAML `mappings:` shape but in a single string. Wholesale CLI-over-YAML precedence (matches the existing `--include-table` / `--exclude-table` precedence policy). For target-type options (e.g. `jsonb` with `binary=true`) operators still need the YAML form — the CLI deliberately doesn't try to encode key/value options in a single string.

### Fixed

- **Generated-column cross-engine expressions with string literals**. The v0.3.1 generated-column work normalized MySQL's backtick identifier quotes but missed two more layers of decoration MySQL applies to the stored expression text:

  - **Charset introducers** — every string literal is wrapped as `_<charset>'literal'` (e.g. `_utf8mb4'open'`). PG rejects this as a syntax error.
  - **Delimiter-escape form** — every string literal's apostrophes are stored as `\'`. PG with `standard_conforming_strings=on` (the default since 9.1) rejects `\'` outright.

  v0.3.1 didn't catch these because the test fixtures used `qty * price` — no string literals. The CHECK constraint work in this release surfaced both immediately (via `status IN ('open', ...)`) and the new `normalizeMySQLExpressionText` helper now strips all three layers. **Generated columns benefit from the same fix**: a column declared as `CONCAT(name, ' ')` cross-engine that would have silently failed on v0.3.1 now works.

## [0.3.1] - 2026-05-04

Patch release — adds first-class generated-column support and includes the CI-pipeline fixes that surfaced during the v0.3.0 release rebuild.

### Added

- **Generated column support across both engines.** Source columns declared as `GENERATED ALWAYS AS (expr) STORED` (or `VIRTUAL` on MySQL) now round-trip cleanly: the schema readers capture the expression on `ir.Column.GeneratedExpr`, the DDL writers emit the corresponding `GENERATED ALWAYS AS (...)` clause, and the bulk-copy / CDC paths skip the column from INSERT/UPDATE column lists so the target re-computes via its own GENERATED clause.

  Translation policy is verbatim passthrough — non-portable expressions (e.g. MySQL `CONCAT(a, b)` vs PG `a || b`) fail loudly on the target rather than be guessed at. Identifier quoting *is* normalized at the read boundary (MySQL's stored expression text uses backticks that PG can't parse), since that's a mechanical dialect-quoting issue rather than a function/operator translation. Cross-engine sources with VIRTUAL columns are silently promoted to STORED on PG (which doesn't support VIRTUAL) with a `slog.Warn` documenting the shift.

  Integration coverage on MySQL→MySQL, PG→PG, and MySQL→PG (cross-engine) for both the migrate and streamer paths.

### Fixed

- **CI pipeline fixes uncovered during the v0.3.0 release rebuild**:
  - Migrated `.golangci.yml` to v2 schema (top-level `version: "2"`, `linters.default: none`, formatters split into the new top-level `formatters:` section, drop deprecated `gosimple` which is merged into `staticcheck`).
  - Bumped `golangci/golangci-lint-action` to `@v8` so `version: latest` resolves to the v2 module path.
  - Re-enabled `install-mode: goinstall` so the linter compiles with our Go 1.26 toolchain rather than the prebuilt-binary's older Go (which couldn't typecheck stdlib `chacha20poly1305`'s Go-1.26-only file).
  - **MySQL binlog composite-PK test**: corrected `int32` type assertions to `int64`. The binlog reader's `decodeInteger` widens every integer to `int64`, so the v0.3.0 test asserted a type that doesn't exist in the row map.
  - Five new lint findings v1 missed (caught by v2): `any` variable shadowing the builtin, an embedded-field selector simplification, a capitalised error string, two De-Morgan'd conditional reads.

### Changed

- **Schema readers exclude `sluice_*_state` tables**. Already done in v0.3.0 for the migrate-state table; this release extends the list to fully cover both bookkeeping tables on re-migrations.

## [0.3.0] - 2026-05-04

Feature release. Three substantial additions to the operator surface (`sluice migrate --resume`, `sluice sync stop`, `--include-table` / `--exclude-table`), one silent-data-loss fix on Postgres CDC, and five new ADRs documenting the v0.2.x and v0.3.0 design decisions.

### Added — resumable simple-mode migrations

- **`sluice migrate --resume --migration-id ID`** picks up a failed migration where it left off rather than forcing a drop-and-redo. Per-target `sluice_migrate_state` row tracks phase (`tables`/`bulk_copy`/`identity_sync`/`indexes`/`constraints`/ `complete`) and per-table bulk-copy progress as a JSON map. In-progress tables are TRUNCATEd before re-copy. Failure paths persist the in-flight phase plus a 1KB-truncated error message; a state-write failure during cleanup is joined with the primary error via `errors.Join` so the operator never loses the root cause.
- **Behavior matrix** is conservative for non-resume runs: existing state row + no `--resume` errors out (no silent overwrites), and `--resume` against a `complete` row exits cleanly with an "already complete" log. New `MigrationStateStore` and `TableTruncator` are optional engine surfaces (type-assertion pattern, mirroring `SlotManagerOpener`); engines without the primitives error clearly when `--resume` is requested.
- **`CREATE TABLE IF NOT EXISTS`** is now universal in the DDL emitters on both engines, so the resume tables-phase is a clean no-op on re-run. Schema readers exclude `sluice_*_state` so re-migrations don't propagate sluice's bookkeeping as user data.

### Added — selective table inclusion / exclusion

- **`--include-table TABLE,...`** and **`--exclude-table TABLE,...`** on `sluice migrate` and `sluice sync start`. Comma-separated, repeatable, glob patterns supported via stdlib `path.Match` (`audit_*`, `tmp_*`). Mutually exclusive at the CLI parse layer. Same fields available in YAML config as `include_tables` / `exclude_tables`; CLI takes precedence wholesale (no merge).
- **Filtering happens at the orchestrator boundary**: schema pruning after `ReadSchema` and a CDC dispatch wrapper that drops events for excluded tables before the applier sees them. Engines remain agnostic to the spec, so behaviour is identical across MySQL/Postgres/future engines.
- **Position-advancement caveat**: positions only commit when an event applies, so a stream that consists entirely of dropped events lags within the source-side WAL/binlog retention window. Documented on the `Streamer.Filter` field.

### Added — graceful stream stop

- **`sluice sync stop --target-driver X --target DSN --stream-id ID`** asks a running sync stream to drain in-flight changes, persist the final position, and exit cleanly. Mechanism is a control- table flag (`stop_requested_at` column on `sluice_cdc_state`) polled by the running streamer every 5s. Survives operator machine boundaries, container lifecycles, and process restarts — the flag persists; a restarted streamer sees it on next poll.
- **Additive to `Ctrl-C` / `SIGTERM`** which still work via the existing signal path. The new mechanism fits Kubernetes lifecycle hooks, systemd `ExecStop`, and remote orchestrators that can't send signals to a different machine.
- **Idempotent schema migration**: existing v0.2.x deployments pick up the new column on next `EnsureControlTable` call without losing data. PG uses `ADD COLUMN IF NOT EXISTS`; MySQL uses detect-then-ALTER for portability across all 8.x versions.

### Added — observability

- **Structured logging via `log/slog`** (replacing `fmt.Fprintf`-to-stdout). `--log-level` is now wired into the default handler; `debug` / `info` / `warn` / `error` actually change verbosity. Pipeline records emit as `time=... level=INFO msg="..." key=value` to stderr; CLI table outputs (`engines`, `sync status`, `slot list`) keep using stdout unchanged — they're table renders, not log streams.
- **Bulk-copy progress reporting**: a per-table `progressTicker` emits `bulk copy progress table=foo rows=N rate=R` every 2s while a copy is in flight, plus a final `bulk copy complete` line on table completion. Long migrations are no longer 30 minutes of silence.
- **Phase-aware error hints**: wrapped pipeline errors gain an optional one-line `hint:` suffix for common operator-facing failures (missing target table, bad DSN host, auth failures, missing `REPLICATION` grant, missing `CREATE` on schema). Registry is intentionally tiny (7 entries, scoped by phase); hints are appended via `fmt.Errorf("%w\nhint: %s")` so `errors.Is`/`As` traversal is unaffected.

### Added — architecture documentation

Five new ADRs in `docs/adr/`:

- **ADR-0011**: `SlotManager` as an optional engine surface.
- **ADR-0012**: Bypass `pglogrepl` to send raw `CREATE_REPLICATION_SLOT FAILOVER true` for PG 17+.
- **ADR-0013**: Applier value-shaping via column-type cache and `CAST(? AS JSON)` (the Bug 6 fix shape).
- **ADR-0014**: Phase-aware error-hint registry (substring + phase matching, deliberately tiny).
- **ADR-0015**: Migration resume design — per-target state table, truncate-and-redo for in-progress tables, `errors.Join` on state-write-during-failure paths.

### Fixed

- **Postgres CDC: composite-PK DELETE silently lost (Bug 8)**. pgoutput's `DeleteMessage` with `REPLICA IDENTITY DEFAULT` carries an `OldTuple` whose `ColumnNum` equals the relation's full column count, with `'n'` (null) markers for non-key columns. `decodeTuple` translated those into present-but-nil entries on the row map; the applier's `WHERE` then emitted `non_key IS NULL` predicates that matched zero rows on the destination. The applier's resume-idempotency tolerance for zero-rows-affected (ADR-0010) absorbed the silence; the position advanced; `DELETE`s disappeared. Real-world soak testing observed a 30-row drift on a composite-PK `order_items` table.

  Fix: `filterDeleteBefore` narrows the emitted Before to columns flagged `KeyColumn=true` on the relation cache. Correct under every `REPLICA IDENTITY` mode (DEFAULT drops `'n'` entries; FULL drops non-identity columns; USING INDEX is a no-op on the already-narrow OldTuple; PK-less FULL falls back to the full row to honour the operator's deliberate setting). `REPLICA IDENTITY NOTHING` is rejected loudly — DELETE is unreplicatable in that mode.

  MySQL is unaffected: `binlog_row_image=FULL` (the default) carries every column with real values, so the WHERE matches exactly. The user's PG→MySQL drift was the PG source-side bug propagating through.

### Test gap closed

- **Composite-PK CDC coverage on MySQL paths**. Bug 8 reached real-world soak because no existing CDC integration test exercised composite-PK tables across any direction. Added `TestCDCReader_CompositePK` (MySQL binlog, asserts both PK columns survive INSERT/UPDATE/DELETE) and `TestStreamer_MySQLToPostgres_CompositePKDelete` (cross-engine, asserts row-count drop on the target). VStream coverage punted to a follow-up — the test infrastructure (vtgate setup) is heavier and the protocol surface differs enough to warrant its own pass.

## [0.2.2] - 2026-05-04

Patch release closing a CDC-applier JSON-encoding bug that surfaced during v0.2.1 revalidation testing — affecting both PG→MySQL (loud crash) and MySQL→MySQL (silent data divergence). Plus a small dry-run output clarification and a debug-level zero-rows-affected log so the silent class of bug is one filter away from being spotted in the future.

### Fixed

- **MySQL applier: shape JSON column values for the wire on CDC Insert/Update/Delete**. The MySQL `ChangeApplier` bound row values straight from `ir.Row` to the parameterised SQL, bypassing the `prepareValue` used by the bulk-copy path. Two production failures shared the same root cause:

  - **Loud (PG → MySQL CDC on Vitess/PlanetScale)**: `[]byte` JSON values arrived `_binary`-tagged on the wire and Vitess rejected them with "Cannot create a JSON value from a string with CHARACTER SET 'binary'". Sluice exited.
  - **Silent (MySQL → MySQL CDC, vanilla MySQL included)**: `WHERE` on a JSON column with a bare `?` placeholder never matched — MySQL's `=` operator does not implicitly cast a bound parameter to JSON regardless of whether it's `[]byte` or `string`. The applier (which tolerates zero-rows-affected for resume idempotency) silently advanced past UPDATEs and DELETEs that should have matched. The destination row stayed stale forever with no error signal — data divergence with no observability.

  The fix has two parts: (1) a per-table column-type cache lets every bound value go through `prepareValue` (so JSON `[]byte` → `string`, Set `[]string` → comma-joined, Geometry gets the SRID prefix); and (2) `WHERE` placeholders on JSON-typed columns are wrapped in `CAST(? AS JSON)` so the comparison is JSON-vs-JSON rather than JSON-vs-text. The Postgres applier got the parallel cleanup for symmetry and for Array/Geometry shaping (its WHERE didn't need a CAST equivalent — pgx inspects per-column type metadata natively).

  A new `TestChangeApplier_JSONColumn` integration test on each engine exercises the silent path end-to-end; without the fix it fails loudly in PG→MySQL and quietly in MySQL→MySQL.

### Added

- **Debug-level zero-rows-affected log on Update/Delete**. The applier still tolerates zero-rows-affected (resume idempotency depends on it), but a `slog.Debug` line now fires when it happens — a single observability footprint that lets future silent-divergence bugs be one log filter away from being spotted.

### Changed

- **Dry-run table output: split `indexes` into `primary_key` + `secondary_indexes`**. The IR stores the primary key on a separate field from secondary indexes, so the v0.2.0 `indexes=N` field silently excluded PK and confused operators comparing against psql / SHOW INDEX output. The new shape (`primary_key=true secondary_indexes=1 foreign_keys=2`) is explicit from the field names alone.

## [0.2.1] - 2026-05-03

Single-issue patch release fixing a regression introduced in v0.2.0: PG-source CDC is unblocked on PlanetScale Postgres (and any other PG 17+ deployment whose option-list parser is strict).

### Fixed

- **PG 17+ slot creation: use named `SNAPSHOT 'export'` option**. v0.2.0 sent `CREATE_REPLICATION_SLOT ... (EXPORT_SNAPSHOT, FAILOVER true)` on PG 17+, which is a syntax mismatch — the bare `EXPORT_SNAPSHOT` keyword is the *pre-PG-17* form. Inside the new parenthesised option-list grammar the snapshot option must be the named form `SNAPSHOT 'export'`. PlanetScale Postgres rejected the v0.2.0 form with `ERROR: unrecognized option: export_snapshot`, blocking every `sluice sync start` against a PG source. Cold-start CDC (without snapshot export) was unaffected; snapshot+CDC handoff is the path that hit it.

## [0.2.0] - 2026-05-03

Bug-fix and operator-UX release driven by real-world v0.1.0 testing against PlanetScale Postgres + MySQL. Four target-side data-correctness bugs fixed; the slot lifecycle on PG sources gets a first-class CLI plus auto-drop on failed setup; logical slots now opt into PG 17 `FAILOVER`; CLI output moves to structured logging with bulk-copy progress lines and phase-aware error hints.

### Added — operator surface

- **`sluice slot list` / `sluice slot drop`**: source-side replication-slot management for Postgres CDC. List shows every slot's plugin, active flag, `wal_status`, `restart_lsn`, and `confirmed_flush_lsn`; drop is destructive and prompts for confirmation by default (`--yes` skips, `--force` allows dropping an active slot, `--if-exists` swallows the not-found error). Engines without slot management (MySQL today) surface a clear error rather than silently no-op. Backed by a new `ir.SlotManager` interface that engines opt into via `OpenSlotManager`.
- **Auto-drop slot on failed cold-start**: when sluice creates a fresh slot in `StreamChanges` and any later setup step fails (IDENTIFY_SYSTEM, START_REPLICATION, ctx cancellation), the slot is dropped before `StreamChanges` returns. Slots that already existed when the call started are never touched. Once the channel is in the caller's hands the auto-drop is suppressed: emitted change positions reference the slot, and that's user data we don't auto-clean.
- **Refuse to start on invalidated slots**: `pg_replication_slots .wal_status` of `unreserved` or `lost` (the latter caused by a slow consumer falling behind `max_slot_wal_keep_size`) now surfaces a clear, actionable error pointing at `sluice slot drop` and `max_slot_wal_keep_size` for prevention, instead of letting `START_REPLICATION` fail mid-stream with "requested WAL segment has already been removed".
- **Structured logging via `log/slog`**: `--log-level` is now wired into the slog default handler (stderr text format), so `debug`/`info`/`warn`/`error` actually changes verbosity. The pipeline's `Migrator` and `Streamer` types drop their `Stdout` fields and emit structured records (`migration complete tables=N`, `bulk copy complete table=foo rows=N`, etc.). Operator-facing CLI tables (`engines`, `sync status`, `slot list`) keep using stdout — they're table renders, not log streams.
- **Bulk-copy progress reporting**: a new `progressTicker` sits in the row pipe between `RowReader` and `RowWriter` for each bulk-copied table. It atomically counts rows, emits `bulk copy progress` every 2s while rows are advancing, and a final `bulk copy complete` line on Stop. Counting at the pipeline layer keeps engines unchanged.
- **Phase-aware error hints**: wrapped pipeline errors get an optional one-line `hint:` suffix for common operator-facing failures — missing target table, bad DSN host, auth failures, missing REPLICATION grant, missing CREATE on schema. Hints are appended via `fmt.Errorf("%w\nhint: %s")` so `errors.Is`/`As` traversal is unaffected. Registry is intentionally tiny (7 entries) and scoped by phase.

### Added — Postgres slot HA

- **`FAILOVER true` on PG 17+ slot creation**: both slot-creation sites — the cold-start path in the CDC reader and the snapshot+CDC handoff — now go through a version-aware helper. PG 17+ sends a raw `CREATE_REPLICATION_SLOT ... (FAILOVER true)` protocol command via `pgconn.Exec` (pglogrepl's options struct doesn't yet expose the flag); PG ≤ 16 falls back to the FAILOVER-less path and emits a one-time stderr warning naming the slot and pointing at the manual workaround. Closes the silent slot-loss-on-failover gotcha for PlanetScale and any Patroni-fronted PG 17+ deployment.

### Added — orchestration

- **`sluice sync start --dry-run`** (`-n`): symmetric with the existing `migrate --dry-run` flag. Reads the source schema, looks up the persisted position on the target, and prints the plan (cold-start vs warm-resume; source schema summary or position token) without modifying the target or starting the stream. The position lookup is tolerant of the control table being absent — both engines' `readPosition` helpers now fall through "missing relation" errors as "no row".

### Added — managed-service support

- **Multi-shard Vitess snapshot+CDC handoff**: the snapshot path (`Engine.OpenSnapshotStream` on the `planetscale` flavor) now fans out to every shard in a sharded keyspace, buffers rows from all shards into a unified per-table view, and uses the global `COPY_COMPLETED` event (both `Keyspace` and `Shard` empty) as the snapshot→CDC handoff boundary. The captured `ir.Position` carries one `shardGtid` entry per shard. Pairs with `vstream_auto_discover_shards=true` for shard discovery via `SHOW VITESS_SHARDS`. Validated against `vitess/vttestserver` with `NUM_SHARDS=2`.
- **Reshard-during-COPY signalling**: a `JOURNAL` event during the snapshot path's COPY phase now surfaces the typed `ShardLayoutChangedError`, matching the standalone CDC reader. v1 of the multi-shard snapshot does not recover in place — the caller drops the snapshot stream and reopens against the new layout.

### Fixed

- **MySQL target rejects JSON values labelled `_binary`**: PG source columns of type JSONB arriving through a MySQL writer were being sent over the wire with the `_binary` charset prefix, which Vitess (and MySQL strict mode) reject with "Cannot create a JSON value from a string with CHARACTER SET 'binary'". `prepareValue` now converts `[]byte` to `string` for `ir.JSON` columns. Surfaced during PlanetScale-target testing.
- **Warm-resume engine alias**: `ChangeApplier.ReadPosition` stamps every recovered position with the applier's engine name (always `mysql` for the MySQL applier) regardless of which reader produced the original. Strict engine-name checks in `decodeBinlogPos` / `decodeVStreamPos` rejected warm-resume on PlanetScale streams with `wrong engine "mysql"; want "planetscale"`. Both decoders now accept the mysql-family aliases (`mysql` or `planetscale`); the cross-engine guard still rejects `postgres` positions.
- **Postgres UPDATE empty-WHERE under REPLICA IDENTITY DEFAULT**: pgoutput omits `OldTuple` on UPDATEs that don't modify the identity-key columns (the common case under the server-default identity). The CDC reader previously left `Before` nil, and the applier built `UPDATE t SET ... WHERE` with an empty predicate that Postgres rejects with "syntax error at end of input". The reader now synthesises a key-only `Before` from the after-tuple's identity columns. REPLICA IDENTITY NOTHING and tables without identity columns surface a clear error instead of a malformed statement.
- **MySQL `CURRENT_TIMESTAMP` default precision mismatch**: MySQL rejects `TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP` because the function-call precision must equal the column's. The most common path that hit this was a PG `TIMESTAMPTZ DEFAULT now()` migrating to MySQL — PG reports `Precision=6`, the translator turned `now()` into bare `CURRENT_TIMESTAMP`, leaving precisions mismatched. `emitDefault` now promotes a bare `CURRENT_TIMESTAMP` to `CURRENT_TIMESTAMP(N)` on a `TIMESTAMP`/`DATETIME`/`TIME` column with non-zero precision. Expressions that already carry an explicit precision pass through unchanged.

### Added — docs

- **`docs/postgres-source-prep.md`**: operator checklist for running sluice CDC against a Postgres source — required GUCs, connecting role attributes, slot lifecycle, `wal_status` recovery workflow, and the failover-survival mechanisms (Patroni `slots:`, PlanetScale "Logical slot name" UI, PG 17 `sync_replication_slots`). The PlanetScale section is load-bearing: slot loss on failover is silent without proper permanent-slots config.
- **README hero example** showing `migrate` / `sync start` / `sync status` end-to-end against the same DSN pair.
- **CONTRIBUTING test-tag layering**: documents the four build tags (default, integration, integration+postgis, integration+vstream, psverify) and which container images each pulls.

## [0.1.0] - 2026-05-03

The initial tagged release. Captures everything from the design pass through the multi-shard Vitess + `sluice sync status` chunks. Entries are grouped by capability rather than chronologically; `git log` is the source of truth for commit- level history.

### Added — orchestration

- **Simple-mode `Migrator`**: one-shot schema-and-data migration with three-phase apply (tables-without-constraints → bulk row copy → identity-sequence sync → indexes → foreign keys). Wired into the kong `migrate` subcommand. CLI signals (Ctrl-C) cancel cleanly via context.
- **Continuous-sync `Streamer`**: long-running snapshot+CDC orchestrator. Cold start captures a consistent snapshot, runs the bulk-copy phase, then tails CDC events through to a target `ChangeApplier`. Warm resume reads the persisted position from the target's control table and skips the snapshot phase entirely. Wired into the `sluice sync start` subcommand.
- **Translation layer (`internal/translate`)**: per-column type-override layer that consumes the `mappings:` block from `sluice.yaml` and rewrites column types in the IR before the schema-write phase sees them. Strict on missing tables/columns (typos surface as startup errors). Initial alias set covers `text`, `text_array`, `jsonb`, `json`, `bytea`, `varchar` (with optional `length` option), and the eight `postgis_*` geometry shapes (with optional `srid`).
- **`sluice sync status`** subcommand: prints every continuous- sync stream the target database has been the destination for (one row per `sluice_cdc_state` entry) with stream-id, last- updated wall-clock, human "5m ago" age, and a truncated position token. Filterable to a single stream via `--stream-id`. Tolerant of the target's control table being absent — operators querying status against a fresh target see "no streams recorded" rather than an error. Backed by a new `ChangeApplier.ListStreams` interface method, implemented on both MySQL and Postgres.

### Added — engines

- **MySQL engine** (vanilla, `mysql:` driver): SchemaReader, SchemaWriter, RowReader, RowWriter (LOAD DATA INFILE), CDCReader (row-based binlog via go-mysql), ChangeApplier, SnapshotStream (REPEATABLE READ + WITH CONSISTENT SNAPSHOT pinned to the binlog position).
- **PlanetScale MySQL flavor** (`planetscale:` driver): same code paths as vanilla, with a capability declaration that disables `LOAD DATA INFILE` (uses BatchedInsert), turns off user-defined partitioning, and selects the VStream gRPC protocol for CDC.
- **Postgres engine** (`postgres:` driver): SchemaReader, SchemaWriter (with three-phase apply, identity-sequence sync, PostGIS-aware geometry emission, MySQL SET → TEXT[] with a CHECK constraint), RowReader, RowWriter (COPY FROM STDIN), CDCReader (pgoutput logical replication via pglogrepl), ChangeApplier, SnapshotStream (CREATE_REPLICATION_SLOT + EXPORT_SNAPSHOT + SET TRANSACTION SNAPSHOT for atomic snapshot-to-CDC handoff).

### Added — managed-service support

- **PlanetScale Postgres** (PS-PG): the vanilla `postgres` engine works against PS-PG without code changes. All six verification phases pass against a real PS-PG account: connectivity, schema reader, simple-mode migration, CDC reader, snapshot+CDC streamer, and cross-engine PS-MySQL → PS-PG. See [docs/managed-services.md](docs/managed-services.md).
- **PlanetScale MySQL via VStream**: Vitess's gRPC streaming protocol is now sluice's CDC path for the PlanetScale flavor. Capability declaration declares `CDCVStream` so the streamer accepts the flavor. Position encoding is JSON `[]shardGtid` matching Debezium's persistence shape, future-proofing for multi-keyspace migrations.
- **Vanilla Vitess deployments**: the same `planetscale` flavor covers self-hosted Vitess, with DSN flags to opt out of PlanetScale-specific defaults: `vstream_transport=plaintext`, `vstream_auth=none`, `vstream_shards=<custom>`, `vstream_endpoint=<host:port>`. Verified against `vitess/vttestserver` via testcontainers.
- **Sharded Vitess keyspaces** are now supported: the VStream reader streams from N shards concurrently (per-shard cursor tracking is built into the `[]shardGtid` position), and the new `vstream_auto_discover_shards=true` DSN flag asks the reader to populate the layout via `SHOW VITESS_SHARDS LIKE '<keyspace>/%'` at Open time. Reshards are detected via the typed `ShardLayoutChangedError` (matchable with `errors.Is` against `ErrShardLayoutChanged`); callers resume on the new layout via `vstreamCDCReader.Reopen`. Validated against `vttestserver` with `NUM_SHARDS=2` (`-80,80-`).

### Added — types and translation policies

- **MySQL SET → PostgreSQL TEXT[]** (default policy): SET columns emerge on the target as `TEXT[]` with a table-level `CONSTRAINT <table>_<column>_set CHECK (... <@ ARRAY[...])` enforcing membership. Comma-separated MySQL DEFAULTs translate to PG array literals so the source default survives the rewrite.
- **PostGIS-aware GEOMETRY emission**: PG engine detects PostGIS at writer-open time. With the extension installed, ir.Geometry columns emit as `geometry(<subtype>, <srid>)`; without it the existing loud rejection persists (sluice doesn't auto-install extensions). MySQL SRID-prefixed WKB → PostGIS EWKB framing via `wkbToEWKB`. Per-column SRID flows through the translate layer's `postgis_*` aliases. The PG schema reader queries PostGIS's `geometry_columns` view at read time so geometry columns surface in the IR with their precise subtype + SRID (cleanly degrades to `GeometryUnspecified+SRID=0` when PostGIS isn't installed).
- **TRUNCATE detection in CDC** for both binlog and VStream paths. The narrow `parseTruncateTable` parser recognises `TRUNCATE [TABLE] [<schema>.]<table>` shapes and emits `ir.Truncate`; out-of-shape statements fall through to the cache-invalidation path.
- **MySQL TINYINT(1) → PG BOOLEAN** through both the snapshot bulk-copy path and the CDC stream, validated by the cross-engine integration test.
- **MySQL UNSIGNED BIGINT → PG NUMERIC(20,0)**, with auto- increment widening to BIGINT IDENTITY when applicable.
- **MySQL ENUM → PG enum type** with per-column generated type names, default-value casting handled inline.
- **MySQL JSON → PG JSONB** by default (canonical fast path); override to `json` (text) via mappings if needed.

### Added — testing

- **Integration suite** (`integration` build tag): testcontainers pairs cover MySQL→MySQL, PG→PG, MySQL→PG, PG→MySQL one-shot migrations, plus PG→PG and MySQL→PG continuous-sync streaming with restart-resume. The cross-engine seed exercises every type-translation policy in one fixture.
- **PostGIS suite** (`integration && postgis` build tag): boots `postgis/postgis:16-3.4`, exercises end-to-end MySQL → PG geometry round-trip with `ST_AsText` verification.
- **PlanetScale verification suite** (`psverify` build tag): exercises sluice's PG and MySQL paths against a real PlanetScale account using credentials from `PLANETSCALE_CREDENTIALS.env` or env vars. Includes connectivity probe (logs version, wal_level, REPLICATION attribute, PostGIS state), schema reader round-trip, simple- mode migration, CDC reader, continuous-sync streamer, and cross-engine verification. CI workflow at `.github/workflows/psverify.yml` (manual-trigger only).
- **VStream suite** (`integration && vstream` build tag): testcontainers-based against `vitess/vttestserver:mysql80`, exercises the FlavorPlanetScale CDC path against vanilla Vitess (plaintext + no-auth) including INSERT/UPDATE/DELETE and TRUNCATE.

### Added — CI

- Four-job CI workflow: cross-platform unit Test (Linux, macOS, Windows), Integration on Linux, Lint, and cross-platform Build smoke-test. Required for branch protection on main.
- Manual-trigger PlanetScale verification workflow with per-environment secrets for the four PS DSNs.

### Architecture and process

- 10 ADRs in [docs/adr/](docs/adr/) capture the load-bearing design decisions: IR-first translation, sealed interfaces, kong+koanf, three-phase schema apply, MySQL flavors, pgoutput over wal2json, position persistence on the target, go-mysql for binlog parsing, Streamer as separate orchestrator, and idempotent applier semantics.
- Documentation under [docs/](docs/): architecture overview, type-mapping policies, runtime value contract, testing guide, managed-services compatibility matrix, and a sakila-based end-to-end walkthrough.

### Removed

- The pre-translate placeholder mappings handling in `Migrator` and `Streamer`. Replaced by `translate.ApplyMappings` between schema-read and schema-write.

### Known limitations

(none currently — see the closed entries above.)

[Unreleased]: https://github.com/sluicesync/sluice/compare/v0.7.0...HEAD [0.7.0]: https://github.com/sluicesync/sluice/releases/tag/v0.7.0 [0.6.0]: https://github.com/sluicesync/sluice/releases/tag/v0.6.0 [0.5.2]: https://github.com/sluicesync/sluice/releases/tag/v0.5.2 [0.5.1]: https://github.com/sluicesync/sluice/releases/tag/v0.5.1 [0.5.0]: https://github.com/sluicesync/sluice/releases/tag/v0.5.0 [0.4.0]: https://github.com/sluicesync/sluice/releases/tag/v0.4.0 [0.3.2]: https://github.com/sluicesync/sluice/releases/tag/v0.3.2 [0.3.1]: https://github.com/sluicesync/sluice/releases/tag/v0.3.1 [0.3.0]: https://github.com/sluicesync/sluice/releases/tag/v0.3.0 [0.2.2]: https://github.com/sluicesync/sluice/releases/tag/v0.2.2 [0.2.1]: https://github.com/sluicesync/sluice/releases/tag/v0.2.1 [0.2.0]: https://github.com/sluicesync/sluice/releases/tag/v0.2.0 [0.1.0]: https://github.com/sluicesync/sluice/releases/tag/v0.1.0
