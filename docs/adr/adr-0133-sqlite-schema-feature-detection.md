# ADR-0133: SQLite schema-feature carry — generated columns, CHECK constraints, partial/expression indexes

## Status

**Accepted (2026-06-27).** Roadmap item 49 follow-up (#2 of the SQLite source queue).

> **Follow-up landed (2026-07-03) — SQLite→canonical EXPRESSION translator.** The
> deferred "translate rather than pass-through-and-warn" increment shipped: a shared
> `internal/translate.SQLiteExprToPG` / `SQLiteExprToMySQL` translator rewrites a narrow,
> value-fidelity-reviewed PROVABLY-portable subset to the target's canonical form, fully
> parenthesised so target precedence can't change the meaning; anything else returns
> ok=false.
>
> **Allowlist (post-review shrink):** column/literal refs; `+ - *`, all comparisons,
> `AND/OR/NOT`, `IS [NOT] NULL`; `||` concat (PG `||`, MySQL `CONCAT`); `/` on Postgres
> ONLY (integer division matches SQLite; MySQL `/` is always decimal); `abs`, `coalesce`,
> `ifnull→coalesce`, `nullif`, `length` (PG `LENGTH` / MySQL `CHAR_LENGTH`), 1-arg
> `trim/ltrim/rtrim`; `substr/substring` with a LITERAL start ≥ 1 (and literal len ≥ 0 —
> SQLite's negative start counts from the end); `min/max`→`LEAST/GREATEST` on MySQL ONLY
> (LEAST/GREATEST propagate NULL like SQLite; PG's skip NULLs); `cast AS text|real` on both
> and `cast AS numeric` on PG only; the current-instant keywords. **Excluded** (each a
> proven silent-divergence a per-representative test would have passed): `%` on BOTH
> (integer-coercion), `upper/lower` (ASCII vs Unicode fold), `round` (half-away vs
> half-even on floats), `cast AS integer` (truncate vs round) and `cast AS blob` (byte
> semantics), plus the temporal/`strftime`/`glob`/`typeof`/… set and the double-quoted
> misfeature.
>
> **Non-portable policy differs by context (F6):** a generated column and a CHECK are
> DATA-load-bearing, so a body with no provably-portable translation is REFUSED LOUDLY at
> the emit path (`refuseNonPortableSQLiteExpr{PG,MySQL}`, table/column-named) — NOT emitted
> verbatim, because a construct like `%` or `a / b` is SYNTACTICALLY valid on the target
> and would be SILENTLY accepted with divergent semantics (verbatim only fails loudly for
> non-portable *functions*, not operators/casts). A non-portable partial/expression INDEX
> body stays WARN-skipped (an index is a performance object). **Still deferred:**
> `strftime`/epoch format-string + epoch-base translation and a per-column encoding map.
> Wired in `internal/engines/{postgres,mysql}/ddl_emit.go` + `schema_writer_check.go`;
> src==dst value-ground-truthed on real PG + MySQL.

> **Revision note (2026-06-27, pre-implementation).** This ADR's first cut decided
> "detect + loud-WARN only" on the stated premise that *the IR does not model these
> features, so carrying them would need new IR shape across every engine*. That premise
> was **wrong** — verified against the code before any carry shipped. `ir.Column`
> already carries `GeneratedExpr`/`GeneratedStored`/`GeneratedExprDialect`,
> `ir.Table.CheckConstraints` is a first-class `[]*ir.CheckConstraint`, and
> `ir.Index.Predicate`/`ir.IndexColumn.Expression` model partial and expression indexes
> — and the Postgres and MySQL engines already **read and emit all of them** with the
> ADR-0016 layered translation. SQLite was simply the one engine whose reader never
> populated the existing fields. The decision below is the corrected one: **carry these
> into the existing IR fields** (the foundation exists; SQLite just wasn't wired in),
> with the loud-WARN retained for the unavoidable third-dialect verbatim tail.

## Context

The SQLite/D1 reader (ADR-0128/0129/0130/0132) reads columns/PK/FK/plain indexes via
PRAGMAs and silently omitted three schema features the IR *already models*:

- **Generated columns** — `PRAGMA table_info` returns them as ordinary columns, so the
  *computed values are copied* (no data loss), but the column landed as a **regular**
  column on the target and the generation expression was dropped. The IR models this via
  `ir.Column.GeneratedExpr` + `GeneratedStored` (+ `GeneratedExprDialect`); PG/MySQL
  `translateGeneratedExpr` + `ddl_emit` already emit generated columns.
- **CHECK constraints** — live only in the `CREATE TABLE` SQL (no PRAGMA). The IR models
  them via `ir.Table.CheckConstraints []*ir.CheckConstraint{Name, Expr, ExprDialect}`;
  PG's `emitSetCheckConstraint`/`translateCheckExpr` and MySQL's `emitCheckConstraint`
  already emit them.
- **Partial / expression indexes** — a partial index has `index_list.partial = 1` and a
  `WHERE` predicate in its `CREATE INDEX` SQL; an expression index has a NULL column in
  `index_info` and an expression in its DDL. The IR models these via
  `ir.Index.Predicate` (+ `PredicateDialect`) and `ir.IndexColumn.Expression` (+
  `ExpressionDialect`); PG's `translateIndexPredicate` already emits partial predicates.
  `ir.Index.Predicate`'s own doc cites "catalog Bug 19a": dropping the predicate
  silently turns a partial UNIQUE index into a full one — a silently widened uniqueness
  scope, the exact silent-correctness class the tenets weight highest.

**The one real obstacle is the third dialect.** ADR-0016's translation pass is
mysql↔postgres only. The writers dispatch as `if ExprDialect == "" || ExprDialect ==
self → verbatim; else → translate`, and the `else` branch is hardcoded to the *one other
known engine* (PG runs `translateExprForPG`, built for MySQL input; MySQL runs
`translateExprForMySQL`, built for PG input). A SQLite-tagged expression would fall into
that `else` and be **mistranslated through the MySQL↔PG translator** — a silent
corruption. There is no SQLite→canonical translator today.

## Decision

**Carry all three into the existing IR fields, with a writer-dialect guard so the
unknown SQLite dialect passes through verbatim (never mistranslated), and a one-time WARN
on the verbatim tail so the operator knows to verify non-portable constructs.**

1. **Reader: populate the existing IR fields.** The SQLite/D1 schema reader extracts the
   expression text from `sqlite_master` and populates:
   - `ir.Column.GeneratedExpr` / `GeneratedStored` (from the `CREATE TABLE` column
     definition; `PRAGMA table_xinfo` `hidden` ∈ {2 virtual, 3 stored} identifies which
     columns are generated and their stored-ness), tagged `GeneratedExprDialect = "sqlite"`.
   - `ir.Table.CheckConstraints` (each `CHECK(expr)`, table- and column-level, extracted
     paren/string/identifier-aware from the `CREATE TABLE` SQL), tagged `ExprDialect = "sqlite"`.
   - `ir.Index.Predicate` for partial indexes (the `WHERE` clause of `CREATE INDEX`),
     tagged `PredicateDialect = "sqlite"`; and `ir.IndexColumn.Expression` for expression
     indexes where the per-column expression is cleanly extractable, tagged
     `ExpressionDialect = "sqlite"`.
   - Flip `Capabilities.SupportsCheckConstraint` / `SupportsGeneratedColumns` to `true`.

2. **Writer: dialect guard — translate only from the specific known source.** Change the
   three dispatch sites (`translateGeneratedExpr`, `translateCheckExpr`,
   `translateIndexPredicate`, plus the `IndexColumn.Expression` emit) on **both** engines
   from "translate unless self/empty" to "translate **only** when `ExprDialect` is the
   specific other engine this writer can translate from" (PG translates iff
   `== "mysql"`; MySQL translates iff `== "postgres"`). Every other value — `"sqlite"`,
   `""`, any future/unknown dialect — emits **verbatim**. This is also a latent-bug fix:
   today any unrecognized dialect is silently fed through the wrong translator.

3. **Verbatim → loud target rejection, never silent guess.** A carried SQLite expression
   emits verbatim. Portable constructs (`a + b`, `length(x)`, `x || y`, comparisons)
   work on PG/MySQL; non-portable ones (`strftime(...)`, SQLite-specific functions) cause
   the target `CREATE`/index DDL to **fail loudly** — which is the ADR-0016 philosophy
   already in force ("anything not covered falls through verbatim … non-portable
   constructs surface as a target rejection rather than be guessed-at"). No silent
   mistranslation path exists after the guard in (2).

4. **One-time WARN on the verbatim tail (honesty for the silent-semantics edge).** For
   each table/index that carries a `"sqlite"`-dialect generated/CHECK/predicate/expression
   body, emit one WARN that the expression is carried verbatim and that the operator
   should verify it on the target — because the residual risk after (2)/(3) is a
   *bare operator with different semantics under SQLite's loose typing* (e.g. affinity in
   a comparison), which would neither translate nor loudly reject. The WARN makes that
   edge visible. (When/if a SQLite→canonical translator or allowlist lands, the WARN
   narrows to genuinely untranslatable bodies.)

5. **Applies to both transports** — the file/`.sql` reader and the `d1` query-API reader
   share the schema path, so both carry the features and both emit the WARNs.

## Consequences

- A SQLite/D1 source's generated columns land as **generated** columns on the target
  (re-deriving, not static copies), CHECK constraints are **enforced** on the target, and
  partial indexes keep their predicate — closing the Bug-19a-class silent uniqueness
  widening. Data is unaffected either way (generated values were always copied).
- The writer-dialect guard removes a latent silent-mistranslate path for any
  non-{self, the-one-known-other} dialect — a correctness improvement beyond SQLite.
- Non-portable SQLite expressions fail **loudly** at target DDL time (recoverable: the
  operator edits the source or re-adds on the target), never silently. The one residual
  silent edge — a portable-looking operator with divergent SQLite semantics — is surfaced
  by the per-table WARN.
- **SQLite→canonical expression translator — LANDED 2026-07-03** (see the Status
  follow-up note). The portable subset is now *translated* to the target dialect instead
  of passed-through-and-warned; the verbatim tail is reserved for the genuinely
  non-portable constructs (still a loud target reject for gencol/CHECK, a WARN-skip for
  indexes). Expression-index bodies that can't be cleanly parsed from the `CREATE INDEX`
  SQL still WARN-skip at read time (ADR-0133 §A.4) rather than carrying a guessed
  expression. **Still deferred:** `strftime`/`julianday`/`unixepoch`/`date·time·datetime`
  format-string + epoch-base translation, and a per-column encoding map.

## Alternatives considered

- **Detect + loud-WARN only, carry nothing** (this ADR's rejected first cut). Safe, zero
  silent risk, but drops portable features the IR + writers already support — burying real,
  achievable work behind a stale "the IR can't model it" premise. Rejected once the premise
  was disproven.
- **Carry but tag `"mysql"`/`"postgres"` to reuse the translator.** Rejected — it is a
  *lie about provenance* that runs SQLite text through the wrong translator: silent
  corruption, the cardinal sin.
- **Build the SQLite→canonical translator now.** Rejected as the first increment — larger
  and fragile; verbatim-passthrough-with-loud-rejection (the ADR-0016 policy) plus the
  WARN is the correct, faithful first cut. The translator is the tracked next step.
- **Refuse loudly on any of them.** Rejected — blocks a migratable dataset over a
  target-side feature the operator can re-add; WARN + carry is the right severity.
