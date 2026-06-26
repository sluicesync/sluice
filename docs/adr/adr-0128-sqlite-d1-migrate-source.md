# ADR-0128: SQLite / Cloudflare D1 migrate source engine

## Status

**Proposed (2026-06-26).** Roadmap item 49 — design captured and QUEUED, not yet
implemented. Prompted by `planetscale/cli` PR #1278 (an offline one-shot importer from
a Cloudflare D1 *SQLite export* into PlanetScale Postgres, wrapping pgloader). This ADR
records how sluice would do the equivalent natively; it is the self-contained design to
pick up when the item is scheduled.

## Context

A recurring ask is "migrate my SQLite / Cloudflare D1 database into Postgres or MySQL."
PlanetScale's PR #1278 does this as an offline, one-shot import from a **D1 SQLite
export file** (not the live D1 HTTP API) into **PlanetScale Postgres only**, with **no
continuous sync**, by wrapping **pgloader** for bulk load plus a command suite
(`doctor` / `lint` / `convert-schema` / `start --dry-run` / `verify` / `status` /
`complete`).

sluice's IR-first, engine-registry architecture is built for exactly this: a new source
engine implements the `ir` reader interfaces, declares its `Capabilities`, and
self-registers via `init()` (`engines.Register`); the `Migrator` orchestrator, parallel
cold-copy, keyset chunker, type translation to **both** Postgres and MySQL, deferred
index/constraint creation, `--dry-run` plan, `verify`, `diagnose`, and the loud-failure
value discipline are all reused unchanged. `Capabilities.CDC = CDCNone` is a first-class
shape (a migrate-only source needs no CDC machinery), and the code already anticipates
SQLite — `capabilities.go` carries `CDCTriggers // Trigger-based CDC (e.g. SQLite
future)`. So a SQLite **migrate source** is among the cleanest possible engine adds:
read-only, file-based, no replication, no roles.

The single hard part is **SQLite's dynamic typing**. A column has a declared type with
an *affinity* (TEXT / NUMERIC / INTEGER / REAL / BLOB), but each *row* can store any
storage class regardless of the column's declared type. Mapping that to sluice's typed
IR — faithfully and loudly — is the load-bearing decision (the value-fidelity tenet).

## Decision (proposed)

1. **A new `internal/engines/sqlite` source engine**, registered under `sqlite`,
   implementing `SchemaReader` + `RowReader` (+ the row-count / range-bounds helpers the
   chunker uses). Migrate-only: `Capabilities.CDC = CDCNone`, no roles, no extensions —
   declared honestly so the orchestrator and `diagnose` report the truth. Write-side
   (SQLite as a *target*) is out of scope; this is an import source.

2. **Driver: `modernc.org/sqlite` (pure-Go, no CGO)** — matches sluice's `CGO_ENABLED=0`
   posture (the `-race`/build story). `mattn/go-sqlite3` is rejected: it reintroduces
   CGO, which would break the Windows-CGO-off builds and the existing toolchain.

3. **Schema read** from `sqlite_master` + `PRAGMA table_info` / `foreign_key_list` /
   `index_list` / `index_info` → IR tables, columns, PKs, FKs, indexes. `rowid`/`INTEGER
   PRIMARY KEY` is the chunk key when present (`WITHOUT ROWID` tables use their declared
   PK).

4. **Type-affinity mapping policy (the value-fidelity heart — gets its own pin matrix).**
   Map each column's declared type to an IR type via SQLite's affinity rules
   (INTEGER→int, REAL→float, TEXT→text, BLOB→bytes, NUMERIC→decimal/bestfit), and decode
   each row's actual storage class. The hazard is a per-row storage-class *mismatch*
   (e.g. a TEXT value in an INTEGER-affinity column, or a non-ISO string in a column the
   schema implies is a date). The policy MUST be loud, not silently coercing: a value
   whose storage class can't be faithfully represented in the resolved IR/target type is
   **refused** (with an opt-in override echoing the `--zero-date` / type-override
   pattern), never clamped to a wrong-but-plausible value. SQLite has no native DATE/
   TIME/BOOL storage (dates are TEXT/INTEGER/REAL by convention, bools are 0/1 INTEGER) —
   so date/bool interpretation is a declared *policy* (e.g. `--sqlite-date-cols` /
   affinity heuristics), surfaced explicitly, not guessed silently.

5. **Cloudflare D1: consume a `wrangler d1 export` SQLite/SQL dump as the source file**
   — the same offline approach PR #1278 took, requiring zero D1-specific code (it's just
   "a SQLite file"). A native D1 HTTP-API reader (paginated REST + token auth) is a
   possible later `d1` flavor, not needed for the import use case and explicitly
   deferred.

6. **No new commands.** It reuses `sluice migrate` (and the `sync` cold-start path, which
   would simply complete and stop since CDC is `CDCNone`); `--dry-run`, `verify`, and
   `diagnose` already provide PR #1278's `lint`/`start --dry-run`/`verify`/`status`
   equivalents. No pgloader dependency — the native cold-copy engine does the bulk load.

## Consequences (anticipated)

- `sluice migrate --source-driver sqlite --source ./app.db --target-driver postgres …`
  (and `--target-driver mysql`) imports SQLite/D1 into **either** target natively, with
  the existing parallel copy, type translation, index/constraint deferral, dry-run, and
  `verify` — a broader, dependency-free story than the PG-only pgloader wrapper.
- The whole effort is one source-engine package plus its type/value pin matrix; the
  orchestrator and every cross-cutting feature come for free (the engine-neutral design
  paying off).
- Continuous SQLite→X sync (trigger-based CDC, the `CDCTriggers` the code anticipates)
  is a separate, larger, demand-gated follow-up — out of scope here.

## Alternatives considered

- **Wrap an external tool (pgloader), like PR #1278.** Rejected: adds a non-Go runtime
  dependency, is PG-target-only, and bypasses sluice's own value-fidelity refusals and
  `verify`/`diagnose`. A native engine reuses the trusted pipeline and serves both
  targets.
- **Native D1 HTTP-API source first.** Rejected for v1: more surface (auth, pagination,
  rate limits) for no extra reach — the export file covers the import use case; the API
  reader can come later if live-D1 pull is demanded.
- **CGO SQLite driver (mattn/go-sqlite3).** Rejected: breaks the CGO-off build/`-race`
  posture; `modernc.org/sqlite` is pure-Go.

## Value-fidelity requirement (when built)

Because SQLite's per-row storage class is independent of the column's declared affinity,
the pins must cover the **class**: each affinity (INTEGER / REAL / TEXT / BLOB / NUMERIC)
× each actual storage class that can appear in it (incl. NULL and the mismatched cases)
× each target (Postgres and MySQL), asserting faithful translation OR a loud refusal —
never a silent coercion. Date/bool convention handling and any override get their own
shapes. A `value-fidelity-reviewer` pass is required before it lands, per the Bug-74
corollary.
