# ADR-0051 — Core pg_catalog type same-engine verbatim carry

**Status:** **Accepted (2026-05-21).** Sibling tier to
[ADR-0047](adr-0047-verbatim-extension-passthrough.md) (which covers
*uncatalogued USER-DEFINED* types). This ADR covers the *core
pg_catalog* types information_schema reports by their literal type
name — they never reach the USER-DEFINED branch, so the ADR-0047
mechanism does not engage. Surfaced by the real-world schema corpus
(GitLab `db/structure.sql`, iteration 3, 2026-05-19; see
[`docs/dev/notes/real-world-corpus-findings.md`](../dev/notes/real-world-corpus-findings.md)
§"Iteration 3"). Generalizes catalog Bug 17's tsvector/tsquery
carve-out from "type-by-type for the representative" to "the class of
core pg_catalog types lacking a rich cross-engine IR shape."

## Context

### What broke

A same-engine PG → PG migrate of a schema containing a range-type
column (`int8range`, `daterange`, `tstzrange`, …) **loud-refused** at
schema-read with:

```
pipeline: read source schema: postgres: read columns:
table "ci_partitions" column "builds_id_range":
postgres: unsupported data_type "int8range" (udt "int8range")
```

Loud-not-silent → tenet holds, no corruption. But the *same shape*
that PG → PG handles fine for `tsvector` (the post-Bug-17 path) failed
for range types. Loud refusal blocks PG → PG sync of any schema using
range types — common in partition bounds, scheduling, and analytics
workloads (GitLab, Rails, Django).

### Why the existing carve-out didn't generalize

Catalog Bug 17 added an explicit `case "tsvector", "tsquery":` branch
in `internal/engines/postgres/types.go`'s core-type switch, dispatching
through `ir.VerbatimType` when the run carried a same-engine-PG
guarantee. The fix was *correct for the representative* but did not
generalize to the *class* — every other core pg_catalog type with no
rich cross-engine IR shape still hit the generic fallthrough loud
refusal.

This is the project's "pin the class, not the representative" lesson
(Bug 74), but at the product level on real-world schema data.

### Why ADR-0047 does not cover this

ADR-0047's mechanism dispatches inside the `USER-DEFINED` branch of
`translateType`: it sets `VerbatimEligible` when the schema reader
sees an uncatalogued USER-DEFINED type AND the run carries a
same-engine-PG guarantee. Range types are NOT USER-DEFINED —
information_schema reports `data_type = 'int8range'` literally — so
they never reach the USER-DEFINED branch. The two tiers are siblings
covering disjoint surfaces:

| Tier | data_type | Mechanism |
|---|---|---|
| ADR-0047 | `USER-DEFINED` (uncatalogued) | translator's user-defined branch dispatches via `VerbatimEligible` + `FormatType` |
| ADR-0051 (this) | core pg_catalog (e.g. `int8range`, `tsvector`) reported by name | translator checks allowlist + `VerbatimEligible` + `FormatType` before the generic fallthrough |

Both tiers emit the same IR type (`ir.VerbatimType`) and share every
downstream surface: writer (DDL emit literal `FormatType`), value
decode (opaque text/bytes), cross-engine refusal (always loud — no
portable MySQL form). The two ADRs differ only in where the eligibility
check fires; the downstream pipeline is identical.

## Decision

A single named allowlist of core pg_catalog type names is eligible for
verbatim carry on a same-engine-PG run. The translator's existing
`VerbatimEligible` gate (already set by the schema reader for every
non-USER-DEFINED column on a same-engine-PG run — see
`schema_reader.go` `populateColumns`) is the same boolean that gates
ADR-0047; this ADR adds an allowlist check before the generic
fallthrough loud refusal, consolidating the existing tsvector/tsquery
switch-case into the same allowlist.

### Stage 1 allowlist (this chunk)

The cohesive type families with no text-IO / locale / dialect quirks:

- **FTS family** (the catalog Bug 17 carve-out, consolidated):
  `tsvector`, `tsquery`
- **Range family**: `int4range`, `int8range`, `numrange`, `tsrange`,
  `tstzrange`, `daterange`
- **Multirange family** (PG 14+; the multirange variant of every
  range type): `int4multirange`, `int8multirange`, `nummultirange`,
  `tsmultirange`, `tstzmultirange`, `datemultirange`

Pin the class, not the representative: shipping ranges without
multiranges leaves the same gap for the next operator with a
multirange column.

### Stage 2 candidates (deferred to per-type validation)

These core pg_catalog types are *candidates* for the allowlist, but
each has potential text-IO / locale / dialect quirks worth validating
when an operator surfaces a real workload — they are NOT in Stage 1.
Add on demand:

- **`xml`** — PG's xml input parser is stricter than the lex output
  format; round-trip-safety needs verification.
- **`money`** — locale-dependent text format (`'$1,234.56'` vs
  `'1234,56 €'`); a same-PG-locale round-trip likely works but a
  source-LC_MONETARY ≠ target-LC_MONETARY would silently mangle. Needs
  a documented locale-pinning requirement at minimum.
- **`pg_lsn`** — opaque hex pair (`'16/B374D848'`); text I/O is
  symmetric but the type has comparison semantics that may interact
  with index opclass passthrough in subtle ways. Verify the GiST/B-tree
  opclass case when added.
- **`txid_snapshot`** / **`pg_snapshot`** — transactional metadata
  types; rarely appear in user schemas as durable columns. Add only on
  observed demand.

Each Stage 2 type ships with a per-type integration test that proves
the text-IO round-trip on the relevant PG-major target before being
added to the allowlist.

### Implementation pattern

Single named set in `types.go` (NOT a scattered switch-case), checked
**after** the existing core-type switch and **before** the generic
fallthrough refusal:

```go
var coreVerbatimEligibleTypes = map[string]bool{
    "tsvector": true, "tsquery": true,
    "int4range": true, "int8range": true, "numrange": true,
    "tsrange": true, "tstzrange": true, "daterange": true,
    "int4multirange": true, "int8multirange": true, "nummultirange": true,
    "tsmultirange": true, "tstzmultirange": true, "datemultirange": true,
}

// At the end of translateType, before the generic refusal:
if coreVerbatimEligibleTypes[c.DataType] && c.VerbatimEligible {
    if c.FormatType == "" { return nil, fmt.Errorf("…sluice bug…") }
    return ir.VerbatimType{Definition: c.FormatType}, nil
}
```

The existing `case "tsvector", "tsquery":` switch branch is removed —
the allowlist subsumes it. Cross-engine still hits the loud refusal
(VerbatimEligible is false off the same-engine-PG path; see
`schema_reader.go:662-666`).

### Why this pattern (rejected alternatives)

- **Default fall-through ("anything VerbatimEligible + non-empty
  FormatType = verbatim")** — rejected. Violates the loud-failure
  tenet's spirit: any new type PG adds in a future major version (or
  any type sluice's translator hasn't considered) would silently
  reach the verbatim path with no review. Bug-74 lesson points the
  other direction.
- **Per-type switch-case (mirror tsvector/tsquery's existing shape)**
  — rejected. Scatters the decision across the file; adding a type
  touches the switch body; harder to audit "which core types is
  sluice's verbatim tier promising to handle?". The allowlist is the
  audit surface.
- **Combine ADR-0047 + ADR-0051 into one ADR retroactively** —
  rejected. The two surfaces (USER-DEFINED vs core pg_catalog) are
  genuinely disjoint in the schema reader and information_schema
  reporting; describing them as "one mechanism" papers over the
  dispatch-point difference. Sibling-ADR with cross-references is
  honest.

## Out of scope

The following surfaces are NOT addressed by this ADR (each is a real
adjacent concern; each gets its own follow-up if surfaced by operator
workload):

- **`EXCLUDE USING gist (… WITH &&)` constraints** on range columns —
  a separate IR surface (constraint shape). The GitLab corpus hits
  this; without the type fix the EXCLUDE constraint is moot because
  the column refuses first. With the type fix, the EXCLUDE may need
  its own verbatim-carry path. Tracked as a follow-up; the corpus
  harness will characterize it.
- **GiST index access method on range columns** — the GiST AM is
  core pg_catalog (no extension); the existing index emit path should
  handle it. Verified in this ADR's integration test; no code change
  needed. Range-specific opclasses (`range_ops`) are default opclasses
  and emit naturally.
- **Stage 2 types** (xml/money/pg_lsn/txid_snapshot/pg_snapshot) —
  enumerated above with per-type concerns. Each adds via a one-line
  allowlist entry + a per-type round-trip integration test + an ADR
  update recording the entry.
- **Cross-engine translation of any verbatim-eligible core type** —
  stays loud-refuse. No portable MySQL form for ranges/multiranges/FTS
  (and Stage 2 types are PG-only by nature). The
  `cross_engine_supportable.go` refusal for `ir.VerbatimType` covers
  this; no change needed there either.
- **PG → MySQL of a range column with a documented operator-supplied
  `--type-override`** — explicit override is the always-works escape
  hatch (mirrors ADR-0016); not in this ADR's scope.

## Consequences

### Positive

- PG → PG of range-using schemas (partition bounds, scheduling,
  analytics — common Rails/Django/GitLab patterns) "just works" with
  no operator flag. Mirrors what tsvector already does post-Bug-17.
- The single-allowlist pattern makes the "which core types?" question
  auditable in one place. Future expansion is an additive one-line
  change plus an ADR update.
- ADR-0047's downstream surfaces (cross-engine refusal, DDL emit,
  value decode, row writer prepareValue) are reused unchanged.

### Negative / load-bearing

- The cross-engine loud-refuse on `ir.VerbatimType` (carried for
  ranges/multiranges) is *load-bearing*: a future change that
  weakened it would convert this ADR's PG-only carry into silent loss
  on cross-engine. The refusal at
  `internal/pipeline/cross_engine_supportable.go:217` is the gate.
- Schema reader's same-engine guarantee
  (`schema_reader.go:662-666`, the non-USER-DEFINED
  `VerbatimEligible` set) is the *other* load-bearing gate. Any
  change that flipped a cross-engine run to `VerbatimEligible=true`
  would weaken the refusal. Tests pin both directions.
- Adding a Stage 2 type via just the allowlist entry without the
  per-type round-trip integration test would risk silent text-IO
  drift (money's locale dependence is the canonical hazard).

### Testing matrix

- **Unit tests** (translator) — for each Stage 1 type:
  - Eligible + non-empty FormatType → `ir.VerbatimType{Definition: …}`
  - Not eligible (cross-engine) → loud refusal preserved
  - Eligible + empty FormatType → loud sluice-bug error
- **Integration test** (PG → PG migrate) — table with int8range +
  daterange + tstzrange + at least one multirange column; values
  round-trip via text I/O; the type spelling on the target column
  matches the source (`pg_catalog.format_type` equality).
- **Real-world corpus harness** —
  `TestCorpus_GitLab_PGToPG_VerbatimCarry` auto-flips from
  "characterized gap" to "verified clean" the moment the fix lands
  (its existing `t.Log` message says: "If this passes, the core-range-
  type gap below was fixed; tighten this leg to an assertion." Done in
  this chunk).

## References

- Catalog Bug 17 (tsvector/tsquery, the predecessor representative)
- [ADR-0047](adr-0047-verbatim-extension-passthrough.md) — sibling
  tier covering uncatalogued USER-DEFINED types
- `docs/dev/notes/real-world-corpus-findings.md` §"Iteration 3"
- [Roadmap §17](../dev/roadmap.md) — the implement-ready entry this
  ADR closes
- Bug 74 lesson ("pin the class, not the representative")
