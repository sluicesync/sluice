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

**One inherent hazard of the move-OUT → DELETE cell: a filtered *parent* + `--allow-degraded-fks`.** The move-OUT `DELETE` has no source analog — the source still holds the row; only its filter scope changed. With *enforced* target FKs, deleting a parent whose children remain surfaces loudly (`SLUICE-E-WHERE-FK-ORPHAN`). But under `--allow-degraded-fks` (the FK left `NOT VALID`) the parent `DELETE` succeeds and the target diverges from the source: the source keeps parent + child, the target now has neither. This is intrinsic to filtering a parent table in *continuous* sync (not just the one-shot migrate orphan above) — sluice generates the move-OUT `DELETE` faithfully rather than silently reshape it, and you own the referential-integrity reconciliation. Filter children consistently with their parents, or don't filter parent tables you replicate with degraded FKs.

### Two requirements the CDC leg preflights at sync-start

**1. Full before-images.** The row-move decision needs both the before- and after-image, so each filtered table needs full before-images: MySQL `binlog_row_image=FULL`, Postgres `REPLICA IDENTITY FULL`. sluice preflights this at sync-start and refuses with `SLUICE-E-WHERE-CDC-BEFORE-IMAGE`, naming the table and the exact remedy. (This is why the default PK-only before-image — an efficiency sluice normally *wants* — is opted out per filtered table.)

**2. A faithfully-evaluable predicate.** A binlog / logical-replication CDC stream has no server-side row filter, so sluice evaluates the predicate itself over each decoded change — and it accepts only a **restricted grammar** where its client-side evaluation is guaranteed to agree with the source's own `WHERE`. (On the PlanetScale/Vitess VStream flavor the cold-start *snapshot* copy AND the warm-resume CDC stream additionally push a server-side `WHERE` down — ADR-0174 Piece 2 + Batch B — so only in-scope rows/changes transfer; the client-side eval still runs as the correctness authority. This server-side push-down is NO-PAD; for a PAD-SPACE-collation column sluice streams that table unfiltered server-side and filters it client-side instead — see the caveat below.)

- **Accepted:** a column compared to a literal (`= != <> < <= > >=`), `IN`, `IS [NOT] NULL`, combined with `AND` / `OR` / `NOT` and parentheses — on numeric, boolean, string, and timezone-naive temporal columns. The motivating `country IN ('US','CA')` case works.
- **Case- and accent-insensitive collations are faithfully evaluated, not refused (ADR-0174).** A MySQL-family `*_ci` / `*_ai` collation's `=` / `IN` is reproduced client-side using MySQL's own collation comparator, including PAD SPACE trailing-space semantics — so `region = 'EU'` matches a stored `'EU '` exactly as the source does. A Postgres **deterministic** named collation (libc `"C"` / `en_US`, deterministic ICU) is byte-exact and also accepted.
- **Refused loudly at sync-start** (`SLUICE-E-WHERE-CDC-UNSUPPORTED-PREDICATE`): function calls, subqueries, string *ordering* (`< <= > >=`), **equality (`= != IN`) on a FLOAT/DOUBLE column** (the client compares the literal exactly while the source coerces it to a 64-bit double, so a high-precision literal can diverge — float *ordering* IS allowed, compared as float64), **timezone-aware temporal** comparisons, and the string collations sluice cannot reproduce faithfully — a **non-UTF-8 charset** collation (latin1, gbk, …), an **unrecognized** collation, and a Postgres **non-deterministic** ICU collation (`deterministic=false`, whose `=` is collation-aware). In each case a client-side compare could diverge from the source, so sluice refuses rather than risk a silent leak or drop.
- **VStream (PlanetScale/Vitess) PAD-SPACE collations are handled automatically via a client-side fallback.** The server-side push-down (below) evaluates the pushed `WHERE` **NO-PAD** regardless of the column's real `PAD_ATTRIBUTE`, so a `--where` on a **PAD-SPACE legacy collation** (e.g. `utf8mb4_general_ci`) can't be filtered faithfully at the source. sluice detects this and, for those tables, streams them **unfiltered** server-side and filters them **client-side** with the PAD-faithful comparator — so the trailing-space `'EU '` a `region = 'EU'` filter should keep IS kept, on both the cold-start copy and the CDC stream, exactly as the source's own `=` does. The only trade is more wire traffic for that one table (it isn't reduced at the source). NO-PAD `utf8mb4_0900_*` collations, non-string predicates, and the non-VStream flavors (vanilla MySQL binlog, Postgres) are reduced at the source as usual.

**`--where-strict-collation` (opt-out).** A compliance operator who wants the strict byte-exact guarantee can pass `--where-strict-collation` on `sync start`: it disables the ci/ai faithful-comparator path so a case/accent-insensitive column's `=` is **refused** rather than reproduced. Byte-exact collations (`*_bin`, deterministic named, the default) are unaffected. The default (flag omitted) is the faithful behavior above.

Need something still outside the grammar? Use `migrate --where` for a one-shot filtered copy with full source SQL, or normalize on the source and filter on that.

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
