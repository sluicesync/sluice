# ADR-0177: Postgres publication column lists — capability survey (no adoption)

## Status

**Proposed (Discovery — research-only).** No implementation, no demand today. Filed 2026-07-21 alongside [ADR-0176](adr-0176-pg-publication-row-filter-pushdown.md) from a survey of the PG 15+ publication features, so that the capability is on record with its fit assessed *before* someone reaches for it — rather than being rediscovered mid-chunk and adopted without the analysis below.

The conclusion is deliberately negative: **sluice has no surface this feature serves today.** This ADR exists to make that a recorded finding with stated preconditions, not an omission.

## Context

Postgres 15+ allows a publication to carry a **column list** per table — replicating a subset of columns rather than all of them:

```sql
ALTER PUBLICATION p SET TABLE t (id, country, created_at);
```

Columns outside the list are never decoded into the change stream. It is the column-granularity sibling of the row filter ([ADR-0176](adr-0176-pg-publication-row-filter-pushdown.md)) and of `FOR TABLES IN SCHEMA` (noted as a deferred WAL-volume optimisation at `cdc_reader.go:2244`).

sluice emits none of the three. `formatPublicationTableList` (`cdc_reader.go:2437`) renders bare schema-qualified identifiers.

### Where it would fit — and doesn't

sluice filters at four granularities today: namespace (`--include/exclude-database` / `-schema`), table (`--include/exclude-table`), row (`--where`, ADR-0173), and value (`--redact`). **Column is not among them**, and that is the crux: there is no `--include-column` / `--exclude-column` surface for a publication column list to implement.

Each plausible consumer fails on inspection:

- **`--redact` (ADR-0039/0040/0041).** The obvious-looking candidate, and the wrong one. Redaction *transforms* a value — 26 strategies producing masked, hashed, tokenized, or synthesized output. It needs the column to **arrive** so it can be rewritten. A publication column list would drop the column from the stream entirely, so the target's column would go `NULL`/stale rather than redacted — a silent divergence, not an optimisation. The two features are opposites: one changes what a column contains, the other removes it.
- **A narrower target schema.** Doesn't arise: sluice creates the target from the source schema, so the target has the source's columns by construction. There is no "target lacks this column" state for a column list to serve.
- **Wide-table / TOAST WAL reduction.** Real in principle — skipping a large rarely-read `bytea`/`text` column would cut decode and wire volume materially. But it requires an operator-facing way to say *which* columns are droppable, i.e. the missing `--include-column` surface. The feature is the easy half; the surface, its semantics, and its failure modes are the hard half (below).

### Why the surface is harder than it looks

If a column-subset feature were ever proposed, these are the questions that make it a real chunk rather than a flag:

- **What lands on the target for an excluded column?** Absent (target DDL omits it, diverging from the source schema), or present-but-`NULL`? The second is a silent-loss shape by default — a column that *looks* replicated and isn't — and would need the same loud-refusal discipline `--where`'s FK-orphan case got.
- **Replica identity.** PG requires the replica identity's columns to be included in the column list. An exclusion that clipped a key column must refuse loudly, not silently produce an unapplicable change.
- **Interaction with `--redact`.** Excluding and redacting the same column is an operator error worth refusing rather than silently letting one win.
- **`verify`.** Like `--where` (which needs the identical predicate passed to `verify` or it false-reports), a column subset would make full/sample verification compare columns the target was never meant to hold. `verify` would need matching column-scope awareness or it becomes a permanent false-positive generator.
- **Engine neutrality.** MySQL binlog has no column-list analogue; a source-side column filter there would be client-side. So the flag would be engine-asymmetric in *where* it is enforced, which per the IR-first tenet needs to be a declared capability rather than a hidden behavioral difference.

That list is the actual reason not to build it speculatively: the publication column list is perhaps 10% of the work, and adopting it first would invert the design order — mechanism before contract.

## Decision

**Do not adopt publication column lists.** Record the capability, its preconditions, and the analysis above.

**Preconditions for revisiting**, all of which must hold:

1. A concrete operator use case for column-subset replication exists (most likely: a wide table with a large, rarely-needed payload column dominating WAL volume, where `--exclude-table` is too blunt).
2. A column-scope surface has been designed as its own contract — target-schema semantics, replica-identity refusal, `--redact` interaction, `verify` awareness, and the cross-engine capability declaration — **before** any publication DDL changes.
3. [ADR-0175](adr-0175-postgres-publication-scope-isolation.md) has landed and publications are per-stream. A column list is a per-table publication attribute with exactly the shared-publication clobber hazard ADR-0176 documents: two streams with identical table sets but different column lists would silently overwrite each other's, and ADR-0175's table-set-narrowing guard would pass both. This is why ADR-0175's guard is specified over the whole publication *definition* rather than the table set.

Absent (1), (2) and (3), the correct action is none.

## Consequences

- **No code, no flag, no version gate.** Behavior is unchanged.
- **The capability is on record** with its fit assessed, so a future encounter with PG 15+ publication features starts from this analysis instead of re-deriving it — and starts with the surface question rather than the mechanism.
- **The `--redact` distinction is written down**, which is the specific confusion most likely to cause a wrong adoption: the two features look adjacent and are semantically opposite.
- If a column-subset feature is ever built, the publication column list is the natural **PG-source enforcement mechanism** for it — this ADR becomes the implementation note rather than a rejection.

## Alternatives considered

- **Adopt it now as a pure WAL optimisation, driven by internal heuristics** (e.g. auto-exclude columns the target doesn't have). Rejected: sluice creates the target from the source, so the premise doesn't hold; and an implicit, operator-invisible column exclusion is the definition of silent loss.
- **Expose a raw `--publication-column-list` escape hatch** and let operators own the consequences. Rejected: it leaks a Postgres-specific catalog concept into the CLI in violation of the IR-first tenet, gives no `verify` story, and would be enforced in an engine-specific place with no capability declaration — the "contain Postgres complexity" tenet argues against surfacing this shape at all.
- **File nothing, revisit if asked.** Rejected: the `--redact` adjacency makes a wrong adoption plausible enough that the negative result is worth the page.
