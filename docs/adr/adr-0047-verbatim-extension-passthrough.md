# ADR-0047 — verbatim same-engine / backup extension-type passthrough (uncatalogued extensions)

**Status:** **Accepted (2026-05-16).** Design signed off after a design dialogue; the three load-bearing calls were operator-confirmed (new `ir.VerbatimType` not an `ir.ExtensionType` variant; implicit live determination + recorded backup capability marker as primary, explicit flag only as fallback; ADR committed Proposed then Accepted on sign-off). Implementation pending, ships **v0.68.0**. Roadmap §16. Extends [ADR-0032](adr-0032-pg-extension-passthrough.md); the backup capability marker depends on [ADR-0046](adr-0046-inline-backup-chain-rotation.md)'s lineage-as-authoritative-structural-record (settled v0.67.0 + Bug 66/v0.67.1).

## Context

ADR-0032's `pgExtensionCatalog` is an **enumerated 7-extension allowlist** (`vector`, `pg_trgm`, `hstore`, `citext`, `postgis`, `pgcrypto`, `uuid-ossp`). A column whose type is owned by any **uncatalogued** extension (`ltree`, `cube`, `timescaledb`, `pg_partman`, `age`, `h3`, in-house extensions) hits the `USER-DEFINED → enum/loud-failure` fallthrough inside `ReadSchema`, and `--enable-pg-extension foo` for an uncatalogued `foo` is itself refused at preflight. That refusal fires identically for **PG → PG sync/migrate AND for `backup`/`restore`**.

For **cross-engine** (PG → MySQL) the refusal is correct: there is no portable MySQL equivalent and silently mishandling the type would violate the loud-failure tenet. But for the paths that **provably do not need semantic understanding** — same-engine PG → PG, and PG-backup → PG-restore — the type only needs to be *carried faithfully*, not translated. The ADR-0032 catalog entry carries machinery (modifier rendering, typmod decode, index AM/opclass round-trip, cross-engine translators, the ADR-0044 default-expr gate) that a same-engine carry does not require. So the enumerated allowlist imposes a per-extension-code tax on a path where no per-extension knowledge is actually needed. This is the gap roadmap §16 records.

Tenet alignment: this does **not** propagate Postgres-ecosystem sprawl into the orchestrator — it is contained to a PG-side, same-engine/backup-only fallback that is *loud by default everywhere it cannot guarantee fidelity*. It is the opposite of silent auto-handling.

## Decision

### 1. A new passthrough tier *below* the ADR-0032 catalog

The rich catalog path is unchanged for the 7 catalogued extensions. This is the fallback for **uncatalogued `USER-DEFINED` types only**. The cross-engine loud-refusal default is fully preserved. Determination is three-level:

- **(a)** catalogued + `--enable-pg-extension`-enabled → rich ADR-0032 path (unchanged).
- **(b)** uncatalogued `USER-DEFINED` type **AND** the run is provably same-engine-PG (live) or backup-marked-PG-only → **verbatim** passthrough (this ADR).
- **(c)** otherwise (cross-engine, or uncatalogued with no same-engine guarantee) → today's **loud refusal** (unchanged).

### 2. Mechanism: capture and re-emit verbatim; values via text I/O

The PG schema reader captures the column's exact `pg_catalog.format_type(atttypid, atttypmod)` string and the writer re-emits it **verbatim** — no typmod decode, no `emitColumn`, no modifier synthesis. Values round-trip via the type's text I/O (pgx text format / the type's input/output functions). Index access methods and operator classes for uncatalogued extensions are carried verbatim the same way.

New IR shape: a distinct **`ir.VerbatimType`** (carrying the verbatim `format_type` string), *not* an `ir.ExtensionType` variant — keeping `ir.ExtensionType`'s catalog-dispatch contract (build/emitColumn) clean and unambiguous. `ir.VerbatimType` has no `emitColumn` dispatch by construction; the writer emits its string literally.

### 3. Determination is implicit, with one recorded marker for backup

This is the load-bearing design question and its answer:

- **Live PG → PG (sync/migrate): fully implicit, no flag.** The orchestrator already knows source and target engine identity; when both are provably PG it enables the verbatim path automatically.
- **Backup: the only case needing a mechanism.** The restore-target engine is unknown at backup time, so a chunk written with verbatim extension types is **PG-restore-only**. The right mechanism is **not an operator opt-in flag** but a **recorded capability marker** on the lineage segment / manifest (e.g. `verbatim_extension_columns: [...]` or a segment-level `pg_restore_only: true`), enforced **loudly at restore** against the *actual* target engine. This matches the codebase's record-never-sniff / fail-loud-on-mismatch idioms (`DefaultExpression.Dialect`, the recorded-never-sniffed per-segment codec, and ADR-0046 / Bug 66's "lineage.json is the authoritative structural record, missing/ambiguous → loud refuse"). The marker slots into ADR-0046's `LineageSegment` metadata — now a settled, authoritative home post-v0.67.1.
- An explicit `--allow-verbatim-extension-passthrough` flag is **only** a fallback for a conscious "I accept this backup is PG-restore-only" acknowledgement, *if* implicit + recorded-marker proves insufficient in review. Not the primary surface.

### 4. Index AM / opclass — reconcile with the Bug 47 invariant

Sluice only populates `ir.IndexColumn.OperatorClass` for catalogued extension-owned opclasses (Bug 47 design: a non-empty `OperatorClass` is an honest "extension-owned" marker that the cross-engine refusal path keys on). Carrying uncatalogued opclasses verbatim **keeps that invariant true** and is in fact *beneficial*: a verbatim-passthrough backup then correctly refuses a cross-engine restore via the existing non-empty-`OperatorClass` signal. Same-engine, an unknown AM/opclass is verbatim-or-the-target-PG's-own-parser-rejects-it (a loud failure at `CREATE INDEX`, acceptable — mirrors the ADR-0035 PostGIS-absent-target behavior).

## Consequences

**Positive.** Closes the "every extension under the sun" gap for exactly the two paths where semantic understanding is provably unnecessary, with **zero per-extension code** for the long tail, and **without weakening the cross-engine loud-refusal default**. Operators running PG → PG with niche/in-house extensions, and operators backing up such PG databases for PG restore, stop hitting a spurious refusal.

**Costs / residual edges (each gets an explicit loud-failure branch per the tenet).**
- Value fidelity rests on text I/O. Covers nearly all extension types; residual: binary-only types, arrays of extension types, composite/domain types wrapping extension types. Documented contract: **same PG major version** for restore (an extension's text representation is usually but not guaranteed version-stable).
- The backup capability marker is **load-bearing**: a verbatim-marked backup restored to MySQL (any non-PG target) MUST refuse loudly at restore preflight — never silently drop/mangle. This is the same severity class as Bug 66 and gets the same treatment (recorded, checked, loud).
- A verbatim-typed column restored to a PG instance missing that extension fails at the target's own `CREATE` — loud and acceptable (consistent with ADR-0035's PostGIS-absent-target refusal).

**Neutral.** MySQL has no analogue (no extension concept); MySQL source/target is unaffected. The catalogued 7 are untouched (no regression — they keep the rich path).

## Alternatives considered

- **Status quo (enumerated-only).** Rejected: it is precisely the gap — a per-extension-code tax on paths that need no per-extension knowledge.
- **Explicit operator opt-in flag as the primary surface.** Rejected as primary: the same-engine guarantee is *determinable* from engine identity; an always-required flag is friction for the live path. Retained only as an optional conscious-acknowledgement fallback.
- **Auto-promote an uncatalogued extension into the rich ADR-0032 catalog.** Rejected: the catalog entry carries semantic machinery (typmod decode, modifier rendering, cross-engine translators) that a bare passthrough cannot synthesize — that is *why* ADR-0032 is an enumerated allowlist. Verbatim passthrough is a deliberately narrower, lower-fidelity-but-faithful tier, not a catalog shortcut.

## Scope

**In (v1):** PG same-engine (sync/migrate) + PG backup/restore; verbatim type + index AM/opclass passthrough for uncatalogued `USER-DEFINED` extension types; implicit live determination; the backup capability marker + loud restore-time engine gate; `ir.VerbatimType`.

**Out:** cross-engine (stays loud-refuse); auto-promotion into the ADR-0032 catalog; MySQL (no extension concept); any chunk-format/serialization change (orthogonal — see `docs/dev/notes/serialization-benchmark.md`).

## Testing

- Same-engine PG → PG with an uncatalogued ext type (`ltree`/`cube`) round-trips bit-faithfully (type DDL verbatim; values via text I/O).
- Backup of the above → capability marker recorded on the segment; restore to PG succeeds exact.
- Restore of a verbatim-marked backup to a MySQL target → **loud refusal** at preflight (the load-bearing safety pin; mirrors Bug 66's regression-pin discipline).
- Uncatalogued extension index (`USING <unknown_am> (col <unknown_opclass>)`) round-trips same-engine; cross-engine refuses via the non-empty-`OperatorClass` signal (Bug 47 invariant preserved).
- Regression guards: the catalogued 7 still take the rich ADR-0032 path; never-rotated/legacy backups unaffected; cross-engine PG → MySQL with an uncatalogued type still loud-refuses (no weakening).

## Sequencing

Post-v0.67.x. Pairs with ADR-0046: the backup capability marker lands in the now-settled `LineageSegment` metadata (ADR-0046 + Bug 66/v0.67.1 made lineage.json the authoritative structural record with a loud missing/ambiguous contract — the marker's natural home). Estimated ~400–700 LOC + this ADR. Not concurrency-touching, but the backup/restore-correctness surface → the §35 Option-C (push-first, CI-Integration-green-before-tag) flow applies.
