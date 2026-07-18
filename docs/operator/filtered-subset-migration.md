# Filtered / subset migration — copying only the rows you want

sluice can move a **subset** of a database rather than the whole thing: a per-tenant or per-region carve-out, a GDPR-scoped extract, or a realistic slice of production to seed a staging target. The mechanism is a per-table `--where` predicate (ADR-0173), available on both `migrate` (one-shot) and `sync` (continuous). It is fully additive — without `--where`, nothing changes.

sluice already filters *what* it copies at three coarser granularities: the namespace (`--include/exclude-database` / `-schema`), the table (`--include/exclude-table`), and the value (`--redact`). `--where` adds the missing one — **rows**.

## The flag

```
--where TABLE=<predicate>
```

Repeatable, **source-keyed by table name**, and the value is a **native source-SQL boolean predicate** — the same convention as `--redact` and `--type-override`. The table key is the *source* name (a `--map-database` / `--map-schema` rename still matches on the original, exactly like `--redact`), and v1 matches exact table names (no globs yet).

```
sluice migrate --source-driver postgres --source "$SRC" --target-driver postgres --target "$DST" \
  --where "users=country IN ('US','CA')" \
  --where "orders=created_at >= '2026-01-01'"
```

The predicate is **your own argv** — quoted operator SQL, the same trust model as `pscale database dump --wheres` and sluice's own `backfill --where`. sluice always wraps it in parentheses, so a disjunctive predicate (`a OR b`) can't break out of the internal chunk-bounds composition.

It is **native source SQL, evaluated on the source** — so it is intentionally source-dialect and **not cross-engine-portable**. A MySQL-source predicate uses MySQL syntax; a Postgres-source predicate uses Postgres syntax. You own its correctness, as you do with `backfill --where`.

## `migrate --where` — one-shot filtered copy

For a one-shot migrate, the predicate is **pushed down into the source read**:

```
SELECT cols FROM t WHERE <chunk_bounds> AND (<predicate>)
```

Evaluation happens **on the source** — index-aware, no client-side row scanning — and it composes transparently with the cross-table parallel copy and the keyset/range chunker. A `--where` that matches zero rows copies an empty (but created) table. Because a filter is active, the raw-copy fast path is disabled for that table (the byte-pipe would bypass the `WHERE`).

**Verify with the same predicate.** `verify --depth count/sample` compares source rows against the target, but the target holds only the filtered subset — so run `verify --where` with the **identical** predicate, or it will false-report a mismatch:

```
sluice verify --source-driver postgres --source "$SRC" --target-driver postgres --target "$DST" \
  --where "users=country IN ('US','CA')"
```

(Plain `verify` without the predicate is still useful: it will *correctly* flag `source=100 target=50` for a filtered table — that's the check confirming the subset is a subset.)

## The one gotcha: referential integrity

This is the load-bearing caveat. **Filtering a *parent* table orphans its children.** The child rows you copied still reference parent rows the filter excluded, so the deferred `ADD CONSTRAINT FOREIGN KEY` fails with SQLSTATE 23503 on the target.

sluice never leaves a silent orphan. It refuses **loudly** — `SLUICE-E-WHERE-FK-ORPHAN`, naming the parent constraint — and points you at the two ways forward:

- **Filter consistently.** Filter the child table with a predicate that only admits rows whose parent survives the parent filter (or don't filter the parent at all). This is the clean answer when the schema allows it.
- **`--allow-degraded-fks`** (Postgres target). Copy the rows anyway and degrade *that* constraint to `NOT VALID`: the FK is still attached on the target catalog and rejects new writes that would orphan rows, but the existing orphans are tolerated. sluice surfaces the degraded constraint at the end of the run; you run `ALTER TABLE … VALIDATE CONSTRAINT <name>` after you reconcile the data. MySQL has no per-constraint `NOT VALID` semantic, so this flag refuses loudly against a MySQL target.

Auto-including the FK-referenced parent rows for a filtered child set (the pg_dump `--table` / Jailer-style transitive closure) is a deferred follow-on — for now, filter consistently or use `--allow-degraded-fks`.

## `sync --where` — continuous *filtered* replication

The **same** predicate scopes **both** legs of a sync. The cold-start snapshot pushes it down into the source read exactly like migrate; the CDC leg then evaluates it **client-side, per change event**, with full **row-move semantics**. That row-move handling is the whole difficulty of filtered replication, and getting it wrong silently leaks or drops rows:

| before matches? | after matches? | source op | target op |
|---|---|---|---|
| no | no | any | drop (never in scope) |
| yes | yes | any | apply as-is |
| **no** | **yes** | UPDATE (moved **in**) | **INSERT** the after-image |
| **yes** | **no** | UPDATE (moved **out**) | **DELETE** by key |

An `UPDATE` that moves a row *into* scope becomes a target `INSERT` (the target never had that row); an `UPDATE` that moves a row *out of* scope becomes a target `DELETE` (else a stale, now-out-of-scope row leaks). A naive "filter each event independently" implementation gets both of these wrong.

### Two requirements the CDC leg preflights at sync-start

**1. Full before-images.** The row-move decision needs both the before- and after-image, so each filtered table needs full before-images: MySQL `binlog_row_image=FULL`, Postgres `REPLICA IDENTITY FULL`. sluice preflights this at sync-start and refuses with `SLUICE-E-WHERE-CDC-BEFORE-IMAGE`, naming the table and the exact remedy. (This is why the default PK-only before-image — an efficiency sluice normally *wants* — is opted out per filtered table.)

**2. A faithfully-evaluable predicate.** Because there is no source-side stream filter, sluice evaluates the predicate itself over the decoded row — so it accepts only a **restricted grammar** where a byte-exact client-side evaluation is guaranteed to agree with the source's own:

- **Accepted:** a column compared to a literal (`= != <> < <= > >=`), `IN`, `IS [NOT] NULL`, combined with `AND` / `OR` / `NOT` and parentheses — on numeric, boolean, case-sensitive-string, and timezone-naive temporal columns. The motivating `country IN ('US','CA')` case works.
- **Refused loudly at sync-start** (`SLUICE-E-WHERE-CDC-UNSUPPORTED-PREDICATE`): function calls, subqueries, string *ordering* (`< <= > >=`), `=`/`IN` on **case- or accent-insensitive collations**, and **timezone-aware temporal** comparisons. In each case a client-side compare could diverge from the source's evaluation, so sluice refuses rather than risk a silent leak or drop.

Need something outside the grammar? Use `migrate --where` for a one-shot filtered copy with full source SQL, or normalize on the source (e.g. a generated lower-cased column) so the comparison becomes case-sensitive.

## Choosing between them

| | `migrate --where` | `sync --where` |
|---|---|---|
| Shape | one-shot filtered copy | continuous filtered replication |
| Predicate power | full source SQL (pushed down) | restricted client-eval grammar |
| Requirements | none beyond a normal migrate | full before-images per filtered table |
| Use it for | a point-in-time extract / staging seed | a continuously-maintained filtered replica |

Both take the identical `--where TABLE=<predicate>` surface, so moving from a one-shot extract to a maintained replica is the same predicate on a different verb.

## See also

- [ADR-0173](../adr/adr-0173-row-level-where-filter.md) — the design, the row-move truth table, and the grammar-restriction rationale.
- [`docs/operator/error-codes.md`](error-codes.md) — `SLUICE-E-WHERE-FK-ORPHAN`, `SLUICE-E-WHERE-CDC-BEFORE-IMAGE`, `SLUICE-E-WHERE-CDC-UNSUPPORTED-PREDICATE`.
- [`docs/redaction.md`](../redaction.md) — the sibling per-column value filter (`--redact`).
