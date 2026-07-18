# sluice v0.99.276

**Row-level filtering** — migrate or continuously sync **only the rows you want**, with a per-table `--where` predicate (ADR-0173). This is a new user-facing capability and it is fully additive: without `--where`, nothing changes.

sluice already filtered *what* it copies at the namespace level (`--include/exclude-database`/`-schema`), the table level (`--include/exclude-table`), and the value level (`--redact`). `--where` adds the missing granularity — **rows**.

## Added

### `migrate --where TABLE=<predicate>` (and `verify --where`)

Copy only the rows of a table matching a native source-SQL boolean predicate — repeatable, source-keyed (same convention as `--redact` / `--type-override`):

```
sluice migrate --source-driver postgres --source "$SRC" --target-driver postgres --target "$DST" \
  --where "users=country IN ('US','CA')" \
  --where "orders=created_at >= '2026-01-01'"
```

The predicate is **pushed into the source read** (`SELECT … WHERE <chunk_bounds> AND (<predicate>)`) and evaluated **on the source** — efficient, index-aware, no client-side row scanning — and composes with the cross-table parallel copy and the keyset chunker. Use it for per-tenant / per-region carve-outs, GDPR-scoped extracts, or seeding a staging database with a realistic subset. Run `verify --where` with the SAME predicate so its counts compare matching-source rows against the (already-filtered) target.

**Referential integrity** is the one thing to know: filtering a *parent* table orphans its children, so the deferred foreign-key add fails (SQLSTATE 23503). sluice refuses **loudly** — `SLUICE-E-WHERE-FK-ORPHAN`, naming the parent — or, with `--allow-degraded-fks`, degrades that constraint to `NOT VALID` (run `VALIDATE CONSTRAINT` after you reconcile). Never a silent orphan. (The raw-copy fast path is also disabled when a filter is active — the byte-pipe would bypass the `WHERE`.)

The predicate is native **source** SQL, evaluated on the source, so it is intentionally source-dialect and not cross-engine-portable.

### `sync --where TABLE=<predicate>` — continuous *filtered* replication

The SAME predicate scopes **both** legs of a sync: the cold-start snapshot pushes it down into the source read (exactly like migrate), and the CDC leg evaluates it client-side per change with full **row-move semantics** —

- an `UPDATE` that moves a row **into** scope becomes a target `INSERT`,
- an `UPDATE` that moves a row **out of** scope becomes a target `DELETE`,

so a filtered stream never leaks an out-of-scope row nor misses a newly-in-scope one (the classic partial-replication trap).

Because there is no source-side stream filter, the CDC leg accepts a **restricted, faithfully-evaluable grammar**: a column compared to a literal (`= != <> < <= > >=`), `IN`, `IS [NOT] NULL`, combined with `AND` / `OR` / `NOT` and parentheses. Anything that could make a client-side evaluation *diverge* from the source's own — a function call, a subquery, an ordering or case/accent-insensitive-collation string comparison, a timezone-aware temporal comparison — is **refused loudly at sync-start** (`SLUICE-E-WHERE-CDC-UNSUPPORTED-PREDICATE`) rather than risk a silent leak or drop. (Need something outside the grammar? Use `migrate --where` for a one-shot filtered copy with full source-SQL, or normalize on the source — e.g. a generated lower-cased column.)

Filtered CDC requires **full row before-images** so the row-move can be decided on both the before and after image — MySQL `binlog_row_image=FULL`, Postgres `REPLICA IDENTITY FULL` on each filtered table — preflighted at sync-start with `SLUICE-E-WHERE-CDC-BEFORE-IMAGE` naming the table and the exact remedy.

## Compatibility

- **Additive — no behavior change without `--where`.** `migrate --where` = one-shot filtered copy with full source-SQL; `sync --where` = continuous filtered replication over the restricted client-side grammar. The predicate is source-dialect (not cross-engine-portable). Referential-aware auto-inclusion of FK-referenced parent rows (pg_dump `--table` / Jailer-style subsetting) is a deferred follow-on — for now, filter consistently or use `--allow-degraded-fks`.

## Who needs this

Anyone who wants to move a **subset** rather than the whole database: multi-tenant operators carving out one region or tenant, teams building a GDPR-scoped or anonymized extract, or anyone seeding a staging/dev target with a realistic slice of production — as a one-shot `migrate` or as a continuously-maintained `sync`.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.99.276
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:v0.99.276`
