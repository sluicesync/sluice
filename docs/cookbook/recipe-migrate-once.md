# Recipe — one-shot migration MySQL → Postgres

The simplest sluice workflow: move every row of a source database into a
target database, then stop. No CDC, no cutover, no resume — just `sluice
migrate`.

## When to use this recipe

- You're consolidating two MySQL databases into one Postgres warehouse
  and the source can be made read-only during the migration window.
- You're populating a fresh analytics warehouse from a snapshot.
- You're validating sluice's behavior on a real schema before standing
  up the continuous-sync workflow.

If you need **low-downtime** migration (snapshot + CDC catch-up +
cutover), see [recipe-bidirectional-cutover.md](recipe-bidirectional-cutover.md)
instead.

## What you need

- A sluice binary on your PATH (`sluice --version` works).
- The source DSN — a connection string with read access to all tables
  you want to migrate.
- The target DSN — a connection string with `CREATE TABLE` permission
  on the target database.

## The command

```sh
sluice migrate \
    --source-driver mysql \
    --source 'root:rootpw@tcp(localhost:3306)/app' \
    --target-driver postgres \
    --target 'postgres://postgres:pgpw@localhost:5432/app?sslmode=disable'
```

That's the whole recipe for the happy path. sluice runs five phases in
order, each named in the log lines so you can correlate failures to a
phase:

1. **`tables`** — `CREATE TABLE` on the target for every source table.
   No constraints yet (FK / CHECK / EXCLUDE land in phase 5).
2. **`bulk_copy`** — for each table, parallel `COPY` (PG target) or
   `LOAD DATA` (MySQL target) of every row. Threshold-gated parallelism
   for tables above the configured row count.
3. **`identity_sync`** — `setval(pg_get_serial_sequence(...), MAX(id))`
   so future inserts on the target don't collide with the bulk-copied
   PKs.
4. **`indexes`** — every non-PRIMARY index. Deferred until after
   `bulk_copy` so the bulk load isn't slowed by index maintenance.
5. **`constraints`** — foreign keys, CHECKs, EXCLUDEs. Last so the
   bulk-copy phase doesn't trip on inter-table ordering.

If any phase fails, sluice exits non-zero with the loud error from the
offending table. Tables completed before the failure have their data,
but their secondary indexes haven't been created yet (the `indexes`
phase only runs after every table finishes `bulk_copy`). The
post-error `hint:` line names the recovery — `--resume` after fixing
the offending table, or `--exclude-table=<name>` to skip it.

## Preview before you run

Always worth doing on an unfamiliar schema:

```sh
sluice schema preview \
    --source-driver mysql --source ... \
    --target-driver postgres --target ...
```

This emits the target DDL sluice *would* run, without executing
anything. Catches type-translation surprises, cross-engine refusals
(e.g. a PG DOMAIN refusal when the target is MySQL on older versions),
and column-name collisions before they bite mid-migrate.

## Resume on failure

If `bulk_copy` aborts halfway, the documented recovery is:

```sh
sluice migrate ... --resume
```

Use the same `--migration-id` you used originally (sluice synthesizes
one if you didn't pass it; the failure error line includes the
synthesized id). Resume picks up at the failed table — either fixing
its underlying issue (you removed the offending row, granted the
missing permission, etc.) or skipping it with `--exclude-table=<name>`.

## What's NOT in this recipe

- **Continuous sync.** Use `sluice sync start` for that — see
  [recipe-bidirectional-cutover.md](recipe-bidirectional-cutover.md).
- **Verifying every row landed.** Use `sluice verify` after migrate
  completes. The fastest variant is `--depth=count`; for stronger
  guarantees use `--depth=sample` (row-hash on a random sample) or
  `--depth=full`.
- **Type-override knobs.** If a specific column needs a target type
  sluice's translation policy didn't pick, use `--type-override
  TABLE.COL=<target_type>`. See [`docs/type-mapping.md`](../type-mapping.md).

## See also

- [`docs/examples/quickstart.md`](../examples/quickstart.md) — the
  10-minute walkthrough with a real sakila MySQL container.
- [`docs/type-mapping.md`](../type-mapping.md) — what sluice does on
  every type/source/target combination.
- [`docs/postgres-source-prep.md`](../postgres-source-prep.md) — what
  the source needs (very little for `migrate`-only workflows; more for
  CDC).
