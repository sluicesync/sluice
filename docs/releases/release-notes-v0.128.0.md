# sluice v0.128.0

> **Correction (added with v0.128.1).** The claim that domains are handled across *“every path that dispatches on a column's type”* was overstated: the Postgres CDC source **read** path still halted at the first change on a domain-typed table (continuous sync from a domain-using Postgres source did not work end-to-end here; migrate and apply were unaffected). Fixed in **v0.128.1**.

**The Domain-transparency release — a Postgres `CREATE DOMAIN` column is now handled faithfully everywhere sluice dispatches on a column's type, plus three continuous-CDC correctness fixes.** This ships the remediation of the 2026-08-17 Tier-3 blind audit (grade B+) in full, and it is a minor bump for the breadth: a PostgreSQL domain is a named constraint wrapper over a base type, and roughly a dozen column-type dispatch sites across the migrate, sync, backup, predicate, and redaction surfaces read the raw declared type — matching `ir.Domain` against no branch and mishandling the column in ways from over-refusal to a silent-corruption hazard. Every such site now reads the storage (base) type, which is identity for a non-domain column, so nothing else changes. Alongside it: a privilege-hidden existing target table now halts instead of skipping, the `rows_applied` counter stops counting absent-target skips, and a per-event catalog-probe crawl is gone. **Drop-in upgrade from v0.127.x — no operator action required, no schema/format/flag change.**

## Fixed

**Postgres `CREATE DOMAIN` columns are now handled transparently across every path that dispatches on a column's type (the Bug 233 class; audit A-1/A-2/A-3).** A domain's base type is what actually stores, copies, and compares — and PostgreSQL itself reports the base type's OID on the wire for a domain column, so once the branch is chosen correctly the value path is byte-identical to the bare base type. The problem was the branch: many sites read `col.Type` directly and so sent a domain-wrapped column down a default or wrong path. Each now reads the storage type via `ir.UnwrapDomain` (identity for a non-domain, so every non-domain column is byte-for-byte unchanged). The fixes, each pinned: the parquet-export metadata and raw-copy-exclusion dispatch (A-1/A-2) — a domain over a wire-sensitive base (geometry, an extension type, bit, an array) was wrongly eligible for the `--raw-copy-format binary` byte-pipe fast path, which would mis-frame the value, and domain-over-geometry silently omitted its GeoParquet `crs` (projected metres read back as degrees); the Postgres physical-write predicates (COPY-vs-INSERT routing and per-connection codec registration) for domains over verbatim, interval, pgvector, hstore, timetz, and macaddr8[]; `--where` predicate family classification (a domain over anything fell to `FamilyUnsupported` and over-refused an otherwise faithfully evaluable predicate) and its cidr-vs-inet rendering; the redaction preflights, where a domain-over-uuid skipped the `mask:uuid` warning and a domain-over-int skipped the overflow check — the latter re-opening the Bug-105 silent-clamp PII window for that column; and the lineage verbatim-extension marker, where a domain-over-verbatim column never recorded the marker the restore-time refusal depends on. A domain over an ENUM is refused loudly at DDL emit, not handled silently. A per-package `domaingate` gate plus a self-deriving meta-walker now fail the build on any new un-gated raw column-type dispatch, and the walker follows a dispatch routed through a single-assignment local so the shape `t := col.Type; switch t.(type)` can no longer hide a site.

**A privilege-invisible EXISTING target table now halts loudly instead of silently skipping every routed event (audit PG-1, both engines, silent-loss).** The unknown-target-table skip classifies a load failure by whether the table is visible in the catalog — but a table that EXISTS while the sync role lacks privilege on it reads as absent, so every event for it skipped and the stream advanced past them at exit 0. Both appliers now distinguish privilege-invisible-but-present (via `to_regclass` on PostgreSQL, errno 1142 on MySQL) from genuinely-absent: the former halts with a permissions error naming the table, the latter keeps the recoverable skip. Sibling-swept across both engines and pinned.

**`rows_applied` no longer counts changes skipped because their target table is absent (audit PG-2).** A change routed to the absent-table skip writes zero rows but still advanced the `rows_applied` progress counter, so a bulk load into a missing target showed the counter climbing with nothing applied — phantom progress (the true skip count already lives durably in the skip ledger). `dispatch` now reports whether it skipped, and all six counter paths — both engines' serial and deferred position writes, the shared batch loop, and the concurrent lane counter — gate their increment on it. Observability only; no row or resume-position behavior changes.

**The CDC flush-before-durable-position ordering is now gated on both engines (audit PG-4).** The invariant that a skip-ledger flush lands before the position it protects becomes durable was unpinned; a regression test now holds it on both the MySQL and PostgreSQL appliers.

## Performance

**The unknown-target-table skip verdict is negative-cached, ending a per-event catalog-probe crawl (audit P-1, both engines).** When a sync targets a table sluice has confirmed missing, every subsequent event for it re-ran the full catalog-probe chain (enum sweep, column load, PostGIS views). A confirmed-missing verdict is now cached with a 30-second TTL and dropped on same-stream DDL, short-circuiting the crawl in the shared `colTypesFor` that serial, batch, and concurrent-lane dispatch all use — so both engines and every apply mode benefit. The cache is refuse-to-guess: only a recoverable confirmed-absent verdict is cached, never a probe error or a routing-fault halt.

## Changed

**The VStream mid-transaction checkpoint's VGTID-ordering premise is now bound by a test (audit C-1).** The concurrent applier's mid-run-checkpoint safety rests on VStream stamping a row event with the position that EXCLUDES its own transaction, so a resume from a mid-transaction checkpoint replays the partly-applied transaction rather than skipping its tail. That premise was documented UNVERIFIED; an integration test against a real Vitess test server now asserts it — a transaction's rows carry a token distinct from a later transaction's, and a resume from a mid-transaction row position replays the whole transaction — so a reader refactor that regressed it (a silent tail-drop at exit 0) would fail the build.

**Audit-hardening gates (no runtime effect).** The errclassgate setErr classifier is now enforced across every engine by a self-deriving meta-gate (A-4); two Lint-job coverage guards gained anti-vacuity floors and a quoted-pattern check so they cannot green on a broken extraction (T-3/T-6); the doc `file:line` citations in `docs/dev/` are graded for existence and in-range (G-13); and the fuzz-roundtrip and required PostGIS/vstream CI legs are guarded against vacuous-green (T-1/T-2).

## Compatibility

Drop-in from v0.127.1 — no schema, format, or flag change. The Domain fixes only alter behavior for Postgres `CREATE DOMAIN` source columns, which were previously mishandled and are now faithful; a non-domain column takes a byte-identical path. PG-1 changes an error path (a privilege-hidden existing table now halts rather than skipping), and PG-2 corrects an observability counter.

**Anyone migrating or continuously syncing from a Postgres source that uses `CREATE DOMAIN` columns**: upgrade. Depending on the column's base type, a domain column could previously be over-refused in a `--where` filter, mis-rendered (cidr as inet), skipped by a redaction preflight (re-opening a silent-clamp window for a domain-over-int), routed onto the wrong physical write path, or — with `--raw-copy-format binary` over a domain-over-geometry/extension/bit/array column, or a parquet export of a domain-over-geometry column — silently mis-encoded. If you ran any of those on a domain-typed column under an earlier release, re-verify against this version; from here every domain column is handled as its base type.

**Anyone running continuous CDC sync**: upgrade for PG-1 (a table your sync role cannot see now halts loudly instead of silently skipping), P-1 (a missing target table no longer triggers a per-event probe crawl), and PG-2 (accurate `rows_applied`). **Everyone else: no action — this is a drop-in upgrade.**

## Install

```
# Homebrew
brew upgrade sluice

# Scoop (Windows)
scoop update sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.128.0
```

Container images: `ghcr.io/sluicesync/sluice:0.128.0` (multi-arch; the image tag carries no `v` prefix).
