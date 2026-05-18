# Prep — Track 2 Phase 2: Vitess engine-flavor for the fuzz harness

Design contract. **Stop after the design for review** (per the
design-first working agreement). Phase-A study is done (this doc cites
file:line); do not start coding until the open questions below are
resolved.

## Scope — resolving the prep-doc vs readiness-note ambiguity

Two sources describe "Track 2 Phase 2" differently:

- `prep-generative-roundtrip-fuzz-harness.md` decision #5: *"Phase 2
  (Track 1) extends the same generator+oracle to Vitess/PlanetScale
  flavours — an extension, not a rewrite."*
- `prep-planetscale-vitess-readiness.md` §Synergy: *"Track 2's fuzz
  harness is engine-parameterized — its generator+oracle extend to a
  Vitess source/target flavor. Track 2-Phase-2 (cross-engine value
  oracle) ∩ Track 1 should share that, not duplicate."*

**Resolution (the load-bearing decision for review):** these are the
same work viewed from two angles. Phase 2 = **add Vitess as a third
`engineKind`** so the generator/oracle exercise round-trip *value
correctness* across the Vitess flavor. "Cross-engine value oracle" is
not a separate artifact — it is what the existing oracle already does
(canonical-text compare), now reaching Vitess directions. The "∩ Track
1, share don't duplicate" constraint means: **reuse Track 1's Vitess
testcontainer substrate; do not build a second one.** Track 1 owns
reshard/CDC *mechanics*; Track 2 Phase 2 owns *value/round-trip
correctness*. The shared seam is the vttestserver boot helper.

## Phase-A findings — the exact seam (code-verified)

The Phase 1 harness is enum-parameterized, not interface-abstracted
(deliberate per decision #5: extension, not rewrite). Touch points:

| Seam | File:line | Change |
|---|---|---|
| `engineKind` enum | `fuzzgen_registry.go:39-44` | add `engineVitess` + `String()` case `"vitess"` |
| `family` struct | `fuzzgen_registry.go:140-169` | add `vitessType string` (3rd spelling) |
| `columnDDL()` | `fuzzgen_registry.go:191` | 2-case → 3-case switch on `src` |
| `canSource()` | `fuzzgen_registry.go:173` | Vitess: no arrays (like MySQL) |
| `allDirections()` | `fuzzgen_registry.go:129-136` | add Vitess directions (see below) |
| every `family()` ctor | `fuzzgen_registry.go:293-359` | populate `vitessType` (≈MySQL spelling for most) |
| per-family `expect()` | each closure | add Vitess-direction branches |
| `renderScript()` | `fuzzgen_generator.go:227-248` | Vitess DDL branch (≈MySQL: `ENGINE=InnoDB`, no arrays) |
| `bootDirection()` | `migrate_fuzz_roundtrip_integration_test.go:121-168` | `case engineVitess` → `startVitessTestServer()` |
| oracle | `fuzzgen_oracle.go` | **no change** — canonical-text compare is engine-agnostic |

**Reuse (do not rebuild):** `startShardedVTTestServer()` in
`internal/pipeline/shapea_spike_vstream_integration_test.go:74-137`
(`vitess/vttestserver:mysql80`, build tag `integration vstream`) is
the boot helper. Extract it to a shared test helper rather than
copy-paste; Track 1's `vitess_cluster_reshard_integration_test.go`
(heavier `vitessreshard` tag) is **not** needed here — Phase 2 is
value correctness on a static keyspace, not reshard.

The `pgType`/`myType` 2-field model not scaling past 3 flavors is a
known wart; do **not** refactor to an interface now (zero-users tenet
notwithstanding — this is test scaffolding, decision #5 explicitly
chose enum-extension; an interface refactor is a separate, deferrable
chunk). Note it in a comment, move on.

## Directions to add (priority order)

Vitess *is* MySQL-wire; the product-relevant audience paths:

1. **Vitess→PG** and **PG→Vitess** — the make-or-break PlanetScale-
   MySQL ↔ Postgres audience. Highest value.
2. **Vitess→Vitess** — sanity / self-consistency baseline.
3. **MySQL→Vitess / Vitess→MySQL** — lower value (Vitess≈MySQL wire);
   include only if cheap.

## Vitess family expectations (the real content)

Vitess ≈ MySQL semantics with constraints: **no arrays** (all array
shapes → loud-refuse or N/A like MySQL), **no native** `timetz`/
`uuid`/`inet`/`cidr`/`macaddr` (→ lossy-document or loud per the
existing MySQL policy), unsigned ints supported. The bulk of
`vitessType` = the `myType` spelling; the `expect()` Vitess branches
largely mirror the MySQL branches. The non-trivial ones are
PlanetScale-specific (no `LOCAL INFILE` copy path, vtgate
`information_schema` differences) — but those are *migration-path*
concerns better covered by Track 1b against real PS; the fuzz harness
on vttestserver covers *value* fidelity, not the PS product envelope.

## Fold in the Finding-1/2 fix (mandatory, not optional)

The overnight fuzz run found a **systematic loud-refuse false-positive**
(`FINDINGS.md`, seeds 532390945 / 401494023): a loud-refuse family at
an array shape whose generated value is all-NULL asserts a refusal
that vacuously never fires. Phase 2 adds more loud-refuse families
(Vitess arrays), so it **must** ship the generator-side fix first:
guarantee ≥1 non-NULL element in ≥1 row for a loud-refuse family at an
array shape, so the refused path is genuinely exercised. Doing Phase 2
on top of the buggy oracle would multiply the false-positives.

## Open questions (resolve before coding)

1. Confirm the direction priority above (esp. whether MySQL↔Vitess is
   worth the boot cost given Vitess≈MySQL wire).
2. Build tag: Phase 2 tests under `integration vstream` (reuses the
   vttestserver image already pulled by Track 1) — confirm acceptable
   vs a new tag.
3. Sequencing vs the Finding-1/2 harness fix: ship the generator fix
   as its own small PR first (it stands alone, pins a real bug), then
   Phase 2 on top? (Recommended.)
4. Is `internal/pipeline`'s registration of the Vitess MySQL flavor
   (`engines.Get("planetscale")` / `"vitess"`) the one the harness
   should target, or vanilla mysql-against-vtgate? (Affects whether
   capability-gated paths are exercised.)

## Suggested first-cut prompt

> Read CLAUDE.md, this doc, and FINDINGS.md. Ship the generator-side
> loud-refuse fix (Finding 1/2) as a standalone pinned change first.
> Then add `engineVitess` per the seam table, reusing the extracted
> vttestserver helper. Stop after the design delta for review if any
> open question's answer changes the seam.
