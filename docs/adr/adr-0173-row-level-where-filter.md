# ADR-0173: Row-level filtering — a per-table `--where` predicate for migrate (and the continuous-sync design)

## Status

**Proposed** (design-first; **not implemented**; filed 2026-07-17 from an operator feature request). Phase 1 (migrate-time filtering) is a modest, well-precedented chunk that fully covers the motivating use case; Phase 2 (continuous *filtered* sync) is a genuinely harder design deferred behind demand. This ADR pins the surface, the migrate mechanics, and — most importantly — the **defined semantics** the sync phase would need, so the hard part is settled on paper before any code.

## Context

sluice already filters what it copies at three granularities, but **not at the row level**:

- **Namespace** — `--include/exclude-database` (MySQL, ADR-0074), `--include/exclude-schema` (PG, ADR-0075).
- **Table** — `--include/exclude-table` (glob-aware; scopes the bulk copy *and* the CDC snapshot).
- **Value** — `--redact` (source-keyed per-column value redaction), `--type-override`.

The missing granularity is **row-level**: "copy only the rows of table `T` matching predicate `P`." The motivating use case (operator, 2026-07-17): extract only the rows belonging to users in certain countries/regions (or any other criteria) and send just those to the destination — e.g. a per-tenant / per-region carve-out, a GDPR-scoped extract, or seeding a staging DB with a realistic subset.

**Two precedents make the surface obvious:**

- **`pscale database dump --wheres`** — PlanetScale's dumper takes per-table SQL `WHERE` predicates for exactly this (one-shot filtered dump).
- **sluice's own `backfill --where`** (`cmd/sluice/backfill.go:32`) — already takes a *native-SQL row predicate* (`Where string`, "Native-SQL predicate scoping which rows are backfilled"), evaluated on the source. The mechanism is proven in-tree; it's just scoped to the same-database backfill, not the cross-engine copy.

**The read path has a clean injection point.** Each engine's source read builds `SELECT cols FROM t` in `buildSelect` (`internal/engines/postgres/row_reader.go:285`, mysql sibling), and the chunker (`internal/migcore/chunk.go`) ANDs keyset/range bounds onto it. A user `WHERE` composes as one more conjunct.

**The crux is the migrate-vs-sync split.** Filtering a *one-shot bulk copy* is easy (push the predicate to the source read). Filtering a *continuous change stream* is a different, much harder problem (below). This ADR treats them as two phases.

## Decision

### Surface (both phases)

`--where TABLE=<predicate>` — repeatable, **source-keyed by table name**, the value a **native source-SQL boolean predicate**. This matches the existing per-table flag family (`--type-override TABLE.COL=type`, `--redact`, `--map-database SRC=DST`) and both precedents above. Table keys are the *source* names (a `--map-database`/`--map-schema` rename still matches on the original, like `--redact`). Globs may be supported later; v1 is exact table names. The predicate is quoted operator SQL — it is the operator's own argv (same trust model as `backfill --where` and pscale `--wheres`), so SQL-injection is not the threat model; but sluice **always wraps it in parentheses** so a disjunctive predicate (`a OR b`) can't break the `chunk_bounds AND (…)` composition.

### Phase 1 — migrate (and the migrate/snapshot leg of a filtered sync): TRACTABLE, ships first

- **Push-down to the source read.** `buildSelect` gains an optional predicate: `SELECT cols FROM t WHERE <chunk_bounds> AND (<predicate>)`. The predicate is evaluated **on the source** (efficient; no client-side row evaluation; the source's own indexes apply). Composes transparently with the ADR-0076 cross-table pool and the keyset/range chunker.
- **Verify must thread the same predicate.** `verify --depth count/sample` counts the source rows *matching the WHERE* against the target (which holds only the filtered subset), else it false-reports a mismatch. The predicate flows into the verify read the same way.
- **Referential integrity is the load-bearing gotcha.** Filtering a *parent* table's rows orphans its children → the deferred `ADD CONSTRAINT FOREIGN KEY` fails with SQLSTATE 23503 on the target. v1 handling, in order of effort:
  1. **Document it loudly** + reuse the existing **`--allow-degraded-fks`** (PG target: retry as `NOT VALID`, surface the degraded constraint). Cheap, honest, ships with Phase 1.
  2. A dedicated coded refusal/hint when a `--where` run hits 23503 without `--allow-degraded-fks`, naming the filtered parent (steer the operator to filter consistently or opt into degraded FKs).
  3. **Deferred:** *referential-aware* filtering that auto-includes the parent rows referenced by the filtered child set (transitive closure over FKs — the pg_dump `--table` / Jailer subsetting problem). Genuinely hard (cyclic FKs, self-references, performance of the closure), a separate chunk, demand-gated.
- **Dialect note:** the predicate is native *source* SQL evaluated on the source, so it is intentionally **not** cross-engine-portable (a MySQL source predicate uses MySQL syntax). Documented, exactly as `backfill --where` already is.

### Phase 2 — continuous *filtered* sync (CDC): DEFERRED behind demand, semantics pinned here

Applying a `WHERE` to a *change stream* is the classic **partial-replication** problem and is materially harder for two reasons:

1. **No source-side stream filtering exists.** binlog / logical-replication slot / VStream all deliver *every* change; none filters by an arbitrary predicate. sluice would have to evaluate the predicate **client-side, per change event**, over the decoded `ir.Row`. That needs either (a) a small SQL-expression evaluator over IR values, or (b) a deliberately **restricted predicate grammar** (equality/`IN`/comparison on a column — enough for "country IN (…)"), refusing anything it can't faithfully evaluate. Full source-SQL semantics client-side (functions, subqueries, collation-sensitive comparisons) is a non-goal — it would silently diverge from the source's own evaluation.
2. **Row-move in/out of the filter is the killer, and it defines the semantics.** An `UPDATE` can change a row so it *newly matches* or *no longer matches* the predicate. Evaluate the predicate on **both** the before- and after-image and translate to the correct **target** operation:

   | before matches? | after matches? | source op | target op |
   |---|---|---|---|
   | no | no | INSERT/UPDATE/DELETE | **drop** (never in scope) |
   | yes | yes | INSERT/UPDATE/DELETE | apply as-is |
   | **no** | **yes** | UPDATE (row moved IN) | **INSERT** the after-image (the target never had this row) |
   | **yes** | **no** | UPDATE (row moved OUT) | **DELETE** by key (else a stale, now-out-of-scope row leaks) |
   | n/a | matches | INSERT | INSERT; else drop |
   | matches | n/a | DELETE | DELETE; else drop |

   The move-IN→INSERT and move-OUT→DELETE rows are the whole difficulty: a naive "filter each event independently" implementation silently leaks out-of-scope rows (move-OUT dropped instead of deleted) and silently misses newly-in-scope rows (move-IN dropped because the *event* is an UPDATE the target has no base row for). Getting this right requires the before-image (so `binlog_row_image=FULL` / `REPLICA IDENTITY FULL` become effectively required for a filtered stream — tie-in to Bug-193 / ADR-0172).
3. **Snapshot→CDC handoff must apply the identical predicate.** The Phase-1 filtered snapshot seeds the target; the CDC leg must use the *same* predicate (same table key) so the two agree on scope. A predicate mismatch between snapshot and stream is a silent-scope bug — the config is shared, single-sourced.

Phase 2 is filed, not built. Build it only when a user needs *continuous* filtered replication (as opposed to a one-shot filtered migrate, which Phase 1 delivers). When built, it needs the row-move table above as coded, pinned behavior + a `binlog_row_image=FULL`/`REPLICA IDENTITY FULL` preflight.

## Consequences

- **Phase 1 gives the operator the stated use case in full** — a one-shot, criteria-filtered extract — with a small, well-precedented surface and push-down efficiency. Cost: the FK-orphan caveat (mitigated by `--allow-degraded-fks` + a clear refusal), and threading the predicate through read + chunker + verify.
- **The predicate is source-native and source-evaluated** — powerful and efficient, but source-dialect and (deliberately) not portable; the operator owns its correctness, as with `backfill --where`.
- **The hard sync semantics are settled on paper** (the row-move table), so a future Phase-2 build starts from a decided design rather than re-litigating partial-replication semantics under time pressure.
- **New docs surface:** once Phase 1 ships, a "filtered / subset migration" guide (the operator's guide idea) + a field note on the FK-orphan trap.

## Alternatives considered

- **Client-side row filtering for migrate (evaluate the predicate in sluice after reading every row).** Rejected for Phase 1: it reads the whole table over the wire only to discard rows, defeats source-index push-down, and re-implements SQL semantics client-side. Push-down to the source read is strictly better for the bulk copy. (Client-side eval is unavoidable for Phase-2 CDC — but that's the change stream, where there is no push-down option.)
- **Copy everything, then `DELETE` the unwanted rows on the target.** Rejected: wasteful (full copy + a large delete), leaves target bloat/dead tuples, and gives no help at all to the CDC path.
- **Referential-aware subsetting up front** (auto-include FK-referenced parents, pg_dump/Jailer-style). Deferred, not rejected — it's the right eventual answer for the FK-orphan problem, but it's a hard standalone chunk (transitive FK closure, cycles, self-refs, closure performance). Phase 1 ships the simple predicate + `--allow-degraded-fks` and files this as the follow-on.
- **A portable predicate DSL** (engine-neutral filter language sluice translates per source). Rejected for v1: native source SQL is the proven, zero-surprise choice (matches `backfill --where` + pscale `--wheres`); a portable DSL is a much bigger design with its own fidelity risks, and would only matter if the *same* filter had to run against multiple source engines — not the use case.
- **Reusing `backfill --where` for extraction.** Doesn't fit: `backfill` is a *same-database* keyset-chunked UPDATE, not a cross-engine copy. The predicate *mechanism* is reused; the command is not.

## Testing (Phase 1, when built)

- **Predicate push-down + chunk composition:** the emitted read is `SELECT … WHERE <chunk_bounds> AND (<predicate>)`; a disjunctive predicate (`a OR b`) is correctly parenthesized so it doesn't escape the chunk bounds (pin the exact SQL). Only matching rows land; a `--where` that matches zero rows copies an empty (but created) table.
- **Verify threads the predicate:** `verify --depth count` on a filtered migrate compares matching-source vs target and passes; without the threading it would (wrongly) fail — pin both.
- **FK-orphan behavior:** filtering a parent table without `--allow-degraded-fks` fails loudly (23503, naming the parent); with `--allow-degraded-fks` on a PG target it degrades to `NOT VALID` and surfaces the degraded constraint. No silent orphan.
- **Cross-engine + per-table:** exercise on MySQL and PG sources, multiple `--where TABLE=…` keys in one run, a table with no `--where` (unfiltered) alongside filtered tables.
- **Rename interaction:** `--where` keys match the *source* table name even under `--map-database`/`--map-schema` (like `--redact`).
