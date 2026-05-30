# ADR-0070: Stage 2 verbatim-carry promote — xml / money / pg_lsn / txid_snapshot / pg_snapshot

## Status

**Accepted (2026-05-30).** Promotes the five Stage 2 candidates from
[ADR-0051](adr-0051-core-pg-type-verbatim-carry.md) §"Stage 2
candidates" into `coreVerbatimEligibleTypes` after the per-type
round-trip integration pins shipped in v0.90.0.

## Context

ADR-0051 (Stage 1, 2026-05-21) established a named allowlist —
`internal/engines/postgres/types.go::coreVerbatimEligibleTypes` — for
core `pg_catalog` types that carry verbatim on a same-engine PG run
when no first-class IR shape is appropriate. Stage 1 landed
tsvector/tsquery + the range family + the multirange family. Stage 2
candidates were listed but deferred until per-type round-trip evidence
shipped:

> Stage 2 candidates (deferred per ADR-0051 §"Stage 2 candidates"):
> xml, money, pg_lsn, txid_snapshot, pg_snapshot. Each has a known
> text-IO / locale / dialect concern worth a per-type round-trip
> integration test before adding to the allowlist. Do NOT add a Stage 2
> entry without updating ADR-0051 and pinning the per-type round-trip.

v0.90.0 shipped the per-type round-trip pins as part of the
broader-mining + Stage-2 type integration sweep:

- `migrate_pg_xml_type_integration_test.go` (#105)
- `migrate_pg_money_type_integration_test.go` (#102)
- `migrate_pg_lsn_type_integration_test.go` (#106)
- `migrate_pg_txid_snapshot_type_integration_test.go` (#107)
- `migrate_pg_snapshot_type_integration_test.go` (#108)

Each pin asserts the **three-outcome shape** the loud-failure tenet
requires:

1. **(a)** Migrator refuses-loudly with the type name in the error
   message; OR
2. **(b)** Target type is text/varchar — `SILENT-TYPE-LOSS` fail
   (the silent-flatten regression class this pin catches); OR
3. **(c)** Target preserves `typname` AND a representative value
   text-round-trips byte-equal.

Pre-promotion the pins fall into (a) — `translateType` refuses with
`postgres: unsupported data_type ...`. Post-promotion the pins fall
into (c) — `translateType` returns `ir.VerbatimType{Definition:
c.FormatType}` and the round-trip preserves both `typname` and the
representative value byte-equal.

## Decision

Add the five Stage 2 types to `coreVerbatimEligibleTypes`:

```go
// Stage 2 (ADR-0070, promoted 2026-05-30). Per-type round-trip
// pins live in `internal/pipeline/migrate_pg_*_type_integration_test.go`.
"xml":            true,
"money":          true,
"pg_lsn":         true,
"txid_snapshot":  true,
"pg_snapshot":    true,
```

Same posture as Stage 1: **same-engine PG → PG only**. The
schema reader sets `VerbatimEligible=true` only on a same-engine PG
run; the cross-engine refusal in
`internal/pipeline/cross_engine_supportable.go` rejects
`ir.VerbatimType` regardless. PG → MySQL still refuses loudly at
preflight.

## Per-type rationale (why each Stage 2 type qualifies)

- **xml** — opaque text-IO from sluice's perspective. PG round-trips
  XMLPARSE-normalized form (declaration, entity-encoding, namespaces).
  No MySQL equivalent; cross-engine continues to refuse.
- **money** — locale-dependent text representation; on a
  same-engine PG run the source's `lc_monetary` and the target's
  `lc_monetary` are operator-controlled. The pin asserts byte-equal
  round-trip on the default `en_US.UTF-8` LC; operators with
  divergent `lc_monetary` between source and target should use a
  first-class numeric type. No MySQL equivalent.
- **pg_lsn** — opaque 16-character text (`X/X` hex). No locale, no
  dialect quirks. Naturally same-engine-only (the type exists to
  reference PG WAL positions).
- **txid_snapshot** — pre-PG-13 transaction-snapshot type. Format
  `xmin:xmax:xip_list`. Deprecated in PG 13+ in favour of
  `pg_snapshot`, but still present in legacy audit tables on
  long-running PG deployments. Same-engine PG-only by definition.
- **pg_snapshot** — PG 13+ replacement for `txid_snapshot`; same
  wire format, xid8-backed. Same-engine PG-only.

## Consequences

**Operator surface:** none of the five types now refuses on a default
`sluice migrate` PG → PG run; each round-trips with `typname`
preserved.

**Cross-engine PG → MySQL behaviour:** unchanged. Still refuses loudly
at preflight via `ir.VerbatimType`'s default rejection in
`cross_engine_supportable.go`.

**Code surface:** five lines added to the allowlist; the existing
five integration pins now hit path (c) preserve instead of path (a)
refuse-loudly. The `t.Logf` in each pin's path-(c) branch confirms the
new posture.

**Backwards compatibility:** additive. Operators not using these
types see no change; operators previously blocked by the refuse-loudly
on a same-engine PG → PG run with these columns now migrate cleanly.

## Stage 3 — closing the door

The remaining `pg_catalog` types that might warrant verbatim-carry
have either:

- a first-class IR shape that supersedes verbatim (numeric, text,
  date/time, geometry, range, json/jsonb),
- a cross-engine equivalent (`uuid`, `bytea`, network types), where
  refusal is the wrong answer anyway,
- or an inherent same-engine-only constraint and an extension owner
  (PostGIS / pgvector / hstore / citext / pg_trgm — covered by
  ADR-0032 extension catalog, not this allowlist).

No Stage 3 is planned. A future addition would need the same evidence
this ADR did: per-type round-trip integration pin in
`internal/pipeline/`, ADR documenting the per-type rationale, and the
cross-engine refusal verified.
