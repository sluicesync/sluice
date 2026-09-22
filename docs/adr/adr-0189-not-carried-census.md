# ADR-0189: The not-carried census — one preflight that names every source object class sluice will not carry

- **Status:** Proposed — filed 2026-09-22 from the gap census (`docs/dev/audit-backlog.md` "## 2026-09-22", Tier-4 item 2). Build decision pending; this is the design.
- **Date:** 2026-09-22
- **Related:** `internal/pipeline/largeobject_preflight.go` and `foreign_table_preflight.go` (the two existing per-class advisories this generalizes — each is one optional prober interface plus one warn function wired at two of the five cold-copy doors); `docs/production-readiness.md` §"Known limitations" (the honest list this makes mechanical); roadmap item 50 (triggers/routines, demand-gated — the capability half; this ADR is the loudness half and is NOT demand-gated); ADR-0056 (`sluice diagnose`, the pull-side sibling); the docsync marker pattern `TestFilteredSyncEngineListMatchesTheCode`.

## Context

The 2026-09-22 census classified every object class each source engine can hold by what sluice does with it. The SILENT-DROP column is long and, on inspection, mostly the same shape: the schema reader never queries the catalog for the class, so nothing downstream can warn. Postgres: triggers, event triggers, functions/procedures, rules, grants/ACLs, default privileges, ownership, extended statistics, security labels, text-search configs, tablespaces, `reloptions` (incl. view `security_invoker`), matview indexes, `GENERATED ALWAYS` identity (downgraded to BY DEFAULT), PG 18 VIRTUAL generated columns (promoted to STORED), UNLOGGED (becomes LOGGED). MySQL: triggers, routines, events, partitioning, non-InnoDB storage engines (forced to InnoDB), `CHECK … NOT ENFORCED` (promoted to ENFORCED), view DEFINER/SQL SECURITY/CHECK OPTION, INVISIBLE columns and indexes, ENUM/SET collations. SQLite: views (never read), triggers, virtual tables (FTS5/rtree), WITHOUT ROWID, STRICT, per-column COLLATE.

Two things are true about that list at once. First, most of it is a deliberate non-goal: sluice is a data-movement tool, not `pg_dump`, and roadmap item 50 records the decision to leave procedural objects demand-gated. Second, the *silence* is not a decision anyone made. `warnLargeObjects` and `warnForeignTables` exist because two of these classes were once found the hard way; each got its own prober interface, its own warn function, and its own two call sites — and the census found both absent from backup, add-table and the multi-database cold start (A-postgres S12). Adding a third class the same way would add a third partially-wired advisory.

The operator's stated bar (2026-09-22) is that a user must be able to rely on what sluice says it does. A migration that "succeeds" and leaves the target without its audit triggers, its GRANTs, or its tenant-isolation policy's `ENABLE ROW LEVEL SECURITY` has told the user nothing false and nothing true. `sluice verify` will report MATCH, because rows are not where the loss is. The cheapest honest fix is not to carry these objects; it is to *say so, per class, per run, before the copy starts*, in one place that is provably wired to every door.

## Decision

**One optional engine surface, one pipeline preflight, one operator command, one docsync marker.**

### 1. The engine surface

```go
// internal/ir
type UncarriedObject struct {
    Class  string   // stable slug, e.g. "trigger", "routine", "grant", "identity-always", "check-not-enforced"
    Name   string   // qualified object name, or table.column for column-scoped classes
    Detail string   // one short clause the WARN can print, e.g. "ENABLE ROW LEVEL SECURITY on tenants"
    Table  string   // owning table for scope filtering; empty for database-level objects
}

type UncarriedCensus interface {
    // UncarriedCensus probes the catalog for every object class this reader
    // does not model and returns one entry per object. It runs BEFORE
    // ReadSchema (the Bug 205 ordering: a class that refuses at ReadSchema
    // must still be named). Probe failure is an error the caller logs and
    // continues past; nothing here feeds a refusal by default.
    UncarriedCensus(ctx context.Context) ([]UncarriedObject, error)
    // UncarriedClasses returns the full roster of Class slugs this reader
    // CAN report, so a gate can hold the operator doc to it without a
    // database.
    UncarriedClasses() []string
}
```

Each engine's roster is the census's SILENT-DROP column for that engine, minus classes that already refuse or warn elsewhere (those stay where they are and are listed in the ADR's appendix as exempt-with-reason). The two existing probers (`LargeObjectCensus`, `ForeignTableCensus`) are folded in as classes `large-object` and `foreign-table`; their warn functions become thin wrappers or are deleted (the sibling-sweep enumeration in the commit decides which).

### 2. The pipeline preflight

`warnUncarried(ctx, handle, caps, filter)` in `internal/pipeline`, wired at **every** door that reads a source schema for a copy: `migrate`, `sync start` single-database cold start, `sync start` multi-database cold start, `backup` (full), and `schema add-table`. It emits ONE WARN per class per run, grep-stable marker `NOT-CARRIED`, with the count, up to N names, and the class's one-line consequence ("the target will accept application-supplied ids the source rejected"). Objects on filtered-out tables are not named (the class still counts). Default posture is WARN-and-proceed, matching ADR-0063 and the two existing advisories. `--strict-uncarried` (zero-value-safe: off) promotes any non-empty class to a coded refusal `SLUICE-E-UNCARRIED-OBJECTS` so a CI-driven migration can fail on a schema it does not expect. Both flags ride the copy-phase parity gate (`TestCopyPhaseFlagParityMigratorStreamer`) so migrate and sync cannot drift.

**The wiring gate.** `TestUncarriedCensusReachesEveryCopyDoor` derives the door list from the AST — every non-test function in `internal/pipeline` that calls `ReadSchema(` on a source reader — and asserts each either calls `warnUncarried` before it or is in an exemption map with a reason (e.g. `verify` reads a schema but copies nothing). Anti-vacuity floor ≥5 doors. This is what S12 (the two existing advisories reaching two of five doors) lacked.

### 3. The operator command

`sluice schema census --source-driver X --source DSN [--include-table …] [--format json|text]` — read-only, no target. Prints per class: count, names, disposition (`not carried`, `translated with loss: …`, `warned elsewhere: …`), and the doc pointer. This is the pre-migration "what will I lose" report the peer diff (B-copy GAP 4) found in AWS SCT and `ps-discovery`, in its cheapest form: no effort scoring, no conversion — just the truth about the schema in front of the operator, before anything moves. `migrate --dry-run`'s plan gains the same block.

### 4. The doc marker

`docs/production-readiness.md`'s Known-limitations list gains `<!-- not-carried-classes: postgres=…; mysql=…; sqlite=… -->`, held by `TestNotCarriedClassesMarkerMatchesTheCode` to the union of every registered reader's `UncarriedClasses()` (registry walk, floor ≥3 engines and ≥20 classes). A class added to a reader without a line on the page fails the build; a line on the page no reader reports fails the build. The list can no longer rot in either direction, which is the 2026-09-22 census's D1 finding made permanent.

## What this deliberately does not do

- It does not carry any of the objects. Carrying triggers/routines is roadmap item 50 and stays demand-gated; a class moving from `not carried` to `carried` removes it from the roster, which the marker gate then forces onto the page.
- It does not fix the value-level silent alterations the census found (identity ALWAYS → BY DEFAULT, VIRTUAL → STORED, NOT ENFORCED → ENFORCED, InnoDB coercion, SQLite COLLATE). Those are per-column translations, not absent objects; the census NAMES them (they are classes on the roster with `Detail` naming the column) so the operator learns of them, and each gets its own fix (GC-3/6/7/5) with a family matrix. The roster entry is the floor, not the fix.
- It does not run on CDC boundaries. A trigger created mid-sync is a DDL event the schema-forward path already refuses or ignores per ADR-0091; extending the census to boundaries is a follow-up once GC-2 (the forward classifier's blind classes) is fixed.

## Loud-failure discipline

- Probe failure is logged at WARN with the class list it could not check — not DEBUG, because a census that silently skipped is indistinguishable from an empty schema (the 2026-08-01 rule: name the independent expected value or say there is none).
- `--strict-uncarried` refuses before any target write.
- Every class slug is a stable string operators can grep and a docsync-held row on the honest list.

## Tests

- Per engine, a family-matrix integration test: one fixture per class on the roster on a real server, asserting the census returns exactly that object (floor: every class on `UncarriedClasses()` has a fixture — a roster gate over the fixture map).
- The wiring gate (§2) and the marker gate (§4), each mutation-run in both directions before landing.
- `schema census --format json` golden per engine, with the JSON shape versioned.

## Size

M. The surface is small (one interface, one preflight, one command, one marker) and the catalog queries are the census workers' own queries. Most of the cost is the per-class fixtures, which is also most of the value.
