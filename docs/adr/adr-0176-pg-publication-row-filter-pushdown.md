# ADR-0176: Postgres publication row-filter push-down (`sync --where` Phase 4, PG-source leg)

## Status

**Proposed (2026-07-21).** Phase 4 of the row-level-filter arc: [ADR-0173](adr-0173-row-level-where-filter.md) (the `--where` surface + the row-move truth table), [ADR-0174](adr-0174-filtered-sync-mysql-vstream.md) (Phase 3 — make continuous filtered sync *work* on MySQL-family sources, incl. the VStream server-side push-down and the A0 PAD-SPACE fallback). This ADR proposes the **Postgres** analogue of ADR-0174's Piece 2: push the predicate into the publication so out-of-scope rows are never decoded or sent.

Filed after a survey of the PG 15+ publication features (prompted by the Supabase `etl` project's PG 15+ recommendation). Companion: [ADR-0177](adr-0177-pg-publication-column-lists.md) (column lists — the same catalog surface, no current use case).

**Depends on [ADR-0175](adr-0175-postgres-publication-scope-isolation.md).** A row filter is a per-table attribute of a *shared* publication today; this ADR must not be implemented until publications are per-stream. See "Ordering constraint" below.

**Prerequisite chunk — per-stream publications with the control-state ratchet (recorded 2026-07-23; the opening chunk of this ADR whenever it is greenlit).** ADR-0175 rejected per-stream publications as an *unconditional default-flip* because the publication name rides every `START_REPLICATION`, including warm resume — a silently-derived new default breaks every running PG deployment. That objection is about naive deployment, not the idea; the compatibility ratchet that avoids it:

1. **Record the publication name in the stream's `sluice_cdc_state` row** (new `publication_name` column via the established `ADD COLUMN IF NOT EXISTS` migration pattern — five precedents on that table). Warm resume already reads its own row (that is where the position lives), so resume continuity needs no source-side state; the target-split argument that killed a control-table *guard* does not apply to per-stream *resume continuity*, which only ever needs the stream's own row.
2. **Ratchet, never flip:** an existing stream (a `sluice_cdc_state` row with no recorded publication) keeps the legacy shared `sluice_pub` forever; a stream with a recorded name uses it; only NEW streams that opt into per-table attributes (a `--where` predicate on a PG source, i.e. this ADR's feature) get a per-stream default (`sluice_<stream-id>`, normalized like `--slot-name`). Plain unfiltered streams keep the shared default indefinitely — no behavior change without the feature that requires it.
3. **Cleanup parity:** whatever teardown path drops the slot must drop a per-stream publication too, or dead publications accumulate on the source. Same lifecycle, same command surface.
4. **The ADR-0175 guard stays load-bearing:** `--publication-name` can still deliberately point two streams at one publication, and every pre-ratchet deployment shares `sluice_pub` — the existence-semantics guard (ADR-0175 residual closure, 2026-07-23) remains the safety net for both populations. Per the ADR-0175 amendment, when this chunk lands the guard must widen its comparison from the member *set* to the full publication *definition* (`pg_publication_rel` + `prqual`/`prattrs`), because two identical table sets with different row filters are a scope conflict one level down.

Sequenced this way, the risky part of per-stream publications (the default change) ships only when the feature that requires it does, with continuity designed in — never speculatively.

## Context

`sync --where TABLE=<predicate>` (ADR-0173 Phase 2) scopes both legs of a continuous sync. On a **Postgres source** the CDC leg evaluates the predicate **entirely client-side**: every change for a filtered table is decoded by `pgoutput`, sent over the wire, and then either kept or dropped by `internal/rowpredicate` according to the row-move truth table. The source does no filtering at all.

ADR-0174 Piece 2 closed exactly this gap for the VStream flavor by pushing the predicate into the VStream filter rule (`select * from t where <pred>`). Postgres has had the equivalent capability since **PG 15** — a per-table row filter on the publication:

```sql
ALTER PUBLICATION p SET TABLE t WHERE (country IN ('US','CA'));
```

sluice does not use it. `formatPublicationTableList` (`cdc_reader.go:2437`) renders bare qualified identifiers and nothing else. Nor does it use the sibling PG 15+ features (column lists — ADR-0177; `FOR TABLES IN SCHEMA` — noted as a deferred WAL-volume optimisation at `cdc_reader.go:2244`).

Version floor is not an obstacle: the test matrix runs `postgres:16` (dominant), 17, 18, and 19 — nothing below 16 — and `serverVersionNum` (`server_version.go`) already exists as the gating helper, currently used for `pgVersionFailoverSupport = 170000`.

### Why this is a good fit — three independent alignments

Verified against the PostgreSQL documentation (`logical-replication-row-filter`), not assumed:

**1. PG's UPDATE transformation is identical to ADR-0173's truth table.** PG evaluates the filter against *both* the old and new row and rewrites the operation:

| old row | new row | PG emits | ADR-0173 truth table |
|---|---|---|---|
| no match | no match | (not replicated) | drop |
| no match | match | **INSERT** | **INSERT** (move-in) |
| match | no match | **DELETE** | **DELETE** (move-out) |
| match | match | UPDATE | apply as-is |

The move-in/move-out semantics that ADR-0173 identified as "the whole difficulty of filtered replication" are implemented natively, with the same resolution sluice chose.

**2. PG's replica-identity requirement is already satisfied.** For publications that replicate UPDATE/DELETE, PG requires the row-filter `WHERE` clause to reference only columns covered by the table's `REPLICA IDENTITY`. sluice **already** forces `REPLICA IDENTITY FULL` on every filtered table, preflighted with `SLUICE-E-WHERE-CDC-BEFORE-IMAGE` — which covers all columns, so the requirement is met before this ADR does anything.

**3. sluice's accepted grammar is strictly narrower than PG's.** PG permits only simple expressions: no user-defined functions/operators/types/collations, no system columns, no non-immutable built-ins. `internal/rowpredicate` accepts only column-vs-literal comparisons (`= != <> < <= > >=`), `IN`, `IS [NOT] NULL`, combined with `AND`/`OR`/`NOT` and parentheses. Every predicate sluice accepts should therefore be a legal PG row filter — the containment runs the safe direction.

### The hazard: this is the A0 shape

ADR-0174's hard-won lesson must be restated here, because the structure is identical and the outcome there was a Critical.

Today sluice's client-side evaluator is the **correctness authority** — it sees every change and decides. Push the predicate to the source and the server becomes authoritative for *what is delivered*; the client-side evaluator degrades to a redundant second pass that can only ever see what already survived. **Any case where the server's evaluation is stricter than sluice's is silent loss** — a row sluice would have kept, dropped at the source, invisible to every client-side check.

On VStream that divergence was real and non-obvious: the server-side filter evaluates NO-PAD regardless of the column's actual `PAD_ATTRIBUTE`, so a `--where` on a PAD-SPACE legacy collation silently dropped rows the source's own `=` would have matched. It was caught by ground-truthing on a real cluster, not by reasoning, and the fix (ADR-0174 A0) was to stream those tables unfiltered server-side and filter them client-side.

The Postgres risk is **structurally lower** — it is the same engine, evaluating the same expression it would evaluate for `migrate --where`'s push-down, under the deterministic-collation-only restriction `rowpredicate` already enforces — but "lower" is not "zero," and the discipline (`CLAUDE.md`: pin the class, not the representative) says ground-truth it per type family rather than argue it. Candidate divergence sites: literal-to-column type coercion (PG coerces per its own rules; the Go evaluator compares in Go — this is precisely why float equality is already refused), `NULL` three-valued-logic edges in `NOT`/`OR` combinations, and text comparison under a non-default deterministic collation.

## Decision

Push the predicate into the publication's per-table row filter on PG 15+, **keeping the client-side evaluator active as a verification layer rather than removing it**, and gate the whole thing behind a per-family equivalence proof.

### 1. Emit the row filter

`formatPublicationTableList` grows an optional per-table predicate, rendering `schema.table WHERE (<predicate>)` for filtered tables and the bare identifier otherwise. The predicate text is the **same single source** the snapshot leg already uses (ADR-0173's "single-sourced snapshot↔CDC predicate"), so the three evaluation sites — snapshot `SELECT`, publication filter, client-side eval — cannot drift apart.

Gated on `serverVersionNum >= 150000`. Below that, behavior is exactly today's (client-side only), so the feature degrades silently-safely rather than refusing.

### 2. Keep client-side evaluation on — as belt, not as filter

The client-side evaluator continues to run over every delivered change. Post-push-down it should be a no-op: the server has already excluded everything it would exclude. That makes it a **cheap, continuous, production equivalence check** rather than dead code.

When the client-side evaluator would have *dropped* a row the server delivered, that is benign (server stricter is the dangerous direction; server laxer is merely wasteful) — log at debug. The dangerous direction is unobservable from the client by construction, which is exactly why §4's ground-truth gate is non-negotiable.

### 3. Do not relax the `REPLICA IDENTITY FULL` requirement

Tempting secondary win: since PG does the move-detection at the source, sluice arguably no longer needs the full before-image to classify move-outs, so the `SLUICE-E-WHERE-CDC-BEFORE-IMAGE` preflight could narrow from "FULL" to "filter columns covered by the replica identity" (PG's own requirement).

**Rejected for v1.** The full before-image is load-bearing beyond filter evaluation, and relaxing it would couple this change to the apply path's before-image assumptions — a second, independent risk surface bolted onto a change whose whole hazard is silent divergence. Revisit as a separate ADR once push-down is field-proven.

### 4. Ship behind a real-server family-matrix gate

Per the Bug 74 discipline and the ADR-0174 precedent, the gate is an **equivalence oracle on a real PG server**, not a unit test: for each predicate family × value family, assert that the set of rows delivered under push-down is **identical** to the set delivered under client-side-only evaluation, on the same workload.

Matrix axes (the families the evaluator dispatches over):

- **Predicate shape:** `=`, `!=`/`<>`, ordering (`< <= > >=`), `IN`, `IS NULL`, `IS NOT NULL`, and `AND`/`OR`/`NOT` compositions incl. a `NOT (… OR …)` three-valued-logic case.
- **Value family:** integer, numeric/decimal, boolean, text/varchar under the default collation, text under an explicit deterministic collation (`"C"`), date, timestamp (tz-naive — tz-aware is already refused), and `NULL` in every position.
- **Row-move coverage:** each of the four truth-table cells must be exercised, since the transformation is where server and client semantics could disagree most consequentially.

A cell that diverges does not get "fixed" by adjusting the client evaluator to match PG — it gets **excluded from push-down** and streamed unfiltered with client-side filtering, exactly as ADR-0174's A0 fallback does for PAD-SPACE columns. The fallback mechanism must therefore exist before the first cell is enabled, not after the first divergence is found.

### Ordering constraint (hard)

A row filter is a **per-table attribute of the publication**, so with today's single shared `sluice_pub`, two streams with an identical table set but different `--where` predicates would silently clobber each other's filters — a fresh instance of the ADR-0175 silent-loss class, one level down, and one that ADR-0175's table-set narrowing check would pass cleanly.

**Therefore: this ADR must not land until publications are per-stream.** ADR-0175 ships `--publication-name` and the definition-level conflict guard; this ADR's implementation additionally makes a per-stream publication the *default* for any stream that carries per-table attributes. That is the migration path that gets per-stream isolation without the breaking upgrade ADR-0175 rejected.

## Consequences

- **Less WAL decoded and less data on the wire** for filtered PG-source syncs — the actual win, and the same one ADR-0174 Piece 2 delivered for VStream. Magnitude scales with predicate selectivity; a highly selective filter on a high-write table is the best case.
- **The client-side evaluator becomes a continuous equivalence monitor** rather than the filter, which is a genuinely better testing posture than either alternative (removing it, or never pushing down).
- **A new divergence class exists that cannot be detected from the client.** This is the honest cost. It is mitigated by the family-matrix gate, the A0-style per-cell fallback, and the narrow grammar — not eliminated.
- **PG < 15 sources keep today's behavior exactly**, with no refusal and no flag.
- `ensurePublication`'s reconciliation logic gets meaningfully more complex: comparing publication *definitions* (tables + predicates) rather than table sets, and rendering per-table `WHERE` clauses. This is the main implementation cost, and it lands partly in ADR-0175 already.

## Alternatives considered

- **Push down and drop client-side evaluation.** Rejected: it removes the only continuously-running check on server/client agreement, in exchange for a per-change evaluation cost that is negligible next to decode and network. Keeping it is nearly free and is what makes divergence observable in the benign direction.
- **Push down only, with no fallback mechanism.** Rejected: ADR-0174 established that a divergent cell *will* be found, and the recovery has to exist before it is needed. Building the fallback after the first Critical is exactly the sequence this project's tenets exist to avoid.
- **Refuse push-down for any predicate touching text.** Over-broad: text under a deterministic collation is the common case and is byte-exact on both sides. The family matrix can establish this per-cell rather than excluding the family wholesale.
- **Relax `REPLICA IDENTITY FULL` in the same change.** Deferred — §3.
- **Do nothing; client-side filtering is correct today.** Legitimate, and the status quo is *correct* (not a silent-loss bug) — this is a throughput optimization, not a correctness fix. It should be prioritized on evidence of a real filtered-PG-sync workload where decode volume hurts, and explicitly not ahead of ADR-0175, which fixes an actual silent-loss hole.

## Testing

- **The equivalence oracle** (§4) is the gate, on a real PG 16+ container: same workload, two streams (push-down on / off), assert identical delivered row sets per matrix cell. Non-vacuous by construction — a cell where push-down drops a row the client would keep fails.
- **The four row-move cells end to end**, since the UPDATE→INSERT/DELETE transformation happens at the source and must reach the target as the right operation.
- **Version gating:** a PG 14 (or version-spoofed) path takes the client-side-only branch and emits no `WHERE` in the publication DDL.
- **The A0-style fallback**: a deliberately-excluded cell streams unfiltered server-side and is still filtered correctly client-side.
- **Interaction with ADR-0175's guard**: two streams, identical table sets, different predicates → refusal (pre-per-stream-publication) or clean isolation (post).
- **CLI-layer pin** that a `--where` predicate reaches the publication DDL through the real kong parser (the Bug 180 lesson).

**Residual risk.** The equivalence oracle proves agreement for the families it exercises on the PG versions it runs against. PG 15, 16, 17, 18 and 19 do not necessarily evaluate an expression identically in every edge (collation provider changes across major versions are the obvious candidate), so the matrix should run against at least the floor and the newest tested major, and a new PG major joining the test matrix should re-run it rather than inherit the result.
