# Recipe — migrating off Heroku Postgres (slot-less, non-superuser)

Heroku Postgres deliberately restricts what its database role can do:
no `REPLICATION` attribute, no `CREATE_REPLICATION_SLOT`, no
`CREATE EXTENSION` outside the allowlist. This blocks the
slot-and-publication CDC pattern most replication tools use — yours
included if you've ever tried.

This recipe is the **slot-less path**: a one-shot migrate via
`sluice migrate`, then ongoing replication via sluice's
`postgres-trigger` engine (when it ships its sync surface) or a
`pg_dump`-style operational cutover (today). The recipe also covers
the *correct* expectations to set going in — Heroku Postgres won't
behave like an unrestricted PG, and a few sluice features intentionally
refuse loudly when pointed at it.

## When to use this recipe

- You're migrating off Heroku Postgres to a self-managed PG / RDS /
  Crunchy Bridge / Supabase target.
- You're moving Heroku Postgres data to a MySQL or PlanetScale-MySQL
  target.
- You're consolidating multiple Heroku Postgres apps into one warehouse.

If your source isn't Heroku, this recipe still applies as the
canonical "managed PG without `REPLICATION`" template — it covers
the same constraints AWS RDS without the right grants would impose,
or Supabase Free / Crunchy Bridge starter tier.

## What Heroku Postgres doesn't grant

Three blockers most replication tools hit:

1. **`rolreplication = false`** on the connecting role. No logical
   replication slots, no streaming replication, no
   `CREATE_REPLICATION_SLOT`.
2. **No `CREATE EVENT TRIGGER`.** Trigger-based capture tools that
   want to install an event trigger for DDL detection can't.
3. **No `CREATE EXTENSION` outside the allowlist.** Anything pgvector
   / hstore / pg_trgm / custom-isn't an option unless the operator
   pre-installs from Heroku's allowlist.

Heroku Help explicitly notes that third-party replication tools
won't work in the slot-based model:
[Why can't I use third-party tools to replicate my Heroku Postgres database](https://help.heroku.com/E10ZZ6IJ).

## What sluice does on Heroku today

`sluice migrate` works fine — it doesn't need slots or extensions
on the source. It needs:

- A connection string with `SELECT` on every table you want to copy.
- That's it.

```sh
sluice migrate \
    --source-driver postgres \
    --source "$(heroku config:get DATABASE_URL --app myapp)?sslmode=require" \
    --target-driver postgres \
    --target 'postgres://...your-target...'
```

sluice's PG schema reader doesn't need any special privileges; it
queries `information_schema` and `pg_catalog` like any other tool.
Cross-engine to MySQL works the same way (substitute the target
DSN). All the v0.95.x → v0.97.0 Bug 113 / Bug 114 / Bug 117 fixes
apply — DOMAIN-typed columns, range types, EXCLUDE constraints all
get handled cleanly per [case-gitlab.md](case-gitlab.md).

## What sluice deliberately refuses on Heroku

`sluice sync start` for the **slot-based** PG CDC reader refuses
loudly when the source role lacks `rolreplication`. The error names
the missing attribute and points at the documented recovery (which
on Heroku is "use a different target tier" or "use trigger-based
sync when it ships"). This is the correct behavior — silently
falling back to a polling model would hide the constraint from the
operator and produce surprising latency.

## The postgres-trigger path (where it ships)

sluice's `postgres-trigger` engine is the slot-less CDC variant:
plpgsql triggers on the source tables write into a
`sluice_change_log` capture table; the reader tails the log.
**Setup is `sluice trigger setup --dsn=... --tables=...`**
on the Heroku source, then the same `sluice sync start` you'd
use for slot-based CDC.

Today the `postgres-trigger` engine is the focus of an active
work track — see the [postgres-vs-bucardo comparison](../comparison-bucardo.md)
for the measured head-to-head. The recipe block below is the
shape it lands as when the sync surface ships; for migrate today,
use the `sluice migrate` block above.

```sh
# (forthcoming) Slot-less CDC sync on Heroku:
sluice trigger setup \
    --dsn "$(heroku config:get DATABASE_URL --app myapp)?sslmode=require" \
    --tables 'users,orders,items'

sluice sync start \
    --source-driver postgres-trigger \
    --source "$(heroku config:get DATABASE_URL --app myapp)?sslmode=require" \
    --target-driver postgres \
    --target 'postgres://...your-target...' \
    --stream-id heroku-myapp
```

Tear down with `sluice trigger teardown` — verified on
Heroku standard-0 to leave **zero residue** (no leftover triggers,
no leftover `sluice_change_log` table). This is one of sluice's
deliberate operability wins versus comparable trigger tools; see
the [Bucardo comparison](../comparison-bucardo.md#on-the-source-residue)
for the contrast.

## The "use `pg_dump` for schema + sluice for data" pattern

A pragmatic shape that minimizes time-on-source-DB:

```sh
# Step 1: dump schema only via pg_dump (Heroku-allowed)
heroku pg:psql --app myapp -- pg_dump --schema-only > schema.sql

# Step 2: apply to target, review, edit
psql ... target -f schema.sql

# Step 3: sluice migrate with the schema already in place
sluice migrate \
    --source-driver postgres \
    --source "$(heroku config:get DATABASE_URL --app myapp)?sslmode=require" \
    --target-driver postgres \
    --target ... \
    --schema-only=false \
    --no-create-schema
```

This is sometimes preferred over a full `sluice migrate` for Heroku
sources because:

- The schema lands as text first, gets code-reviewed, gets edited if
  the operator wants to e.g. drop unused indexes on the target.
- sluice's bulk-copy phase touches only the data path, which is the
  bigger-budget step.
- The cutover-time downtime window only needs to cover the data
  pull, not the DDL apply.

## Cutover from Heroku to the new target

This is where the rubber meets the road. Whatever your data-copy
plan, the cutover sequence is the same:

```sh
# 1. Stop application writes to Heroku (or set the Heroku app to
#    read-only via app config).
heroku maintenance:on --app myapp

# 2. Final sync / catch-up.
sluice migrate ... --resume   # if using migrate-only path
# OR
sluice sync stop --stream-id heroku-myapp --wait   # if using sync

# 3. Prime the target's sequences past Heroku's MAX(id) per
#    identity column with a margin.
sluice cutover \
    --source-driver postgres \
    --source "$(heroku config:get DATABASE_URL --app myapp)?sslmode=require" \
    --target-driver postgres \
    --target ... \
    --cutover-sequence-margin=1000

# 4. Switch app DATABASE_URL to point at the new target. On Heroku,
#    that's usually:
heroku config:set DATABASE_URL='postgres://...your-target...' --app myapp
heroku maintenance:off --app myapp
```

The margin matters: if any in-flight writes between step 2 and step
4 land on the old database (or get caught by a late CDC apply), the
target's next-inserted IDs need to be far enough past them to avoid
collisions.

## Heroku-specific gotchas

- **TLS is mandatory.** Heroku Postgres rejects non-TLS connections.
  Use `?sslmode=require` (or `verify-full` if you've pinned the CA).
- **Connection limits are tier-specific.** standard-0 has 120
  connections; sluice's parallel-copy plus PG's idle background
  workers can eat that quickly. Pass `--bulk-parallelism 4` (or
  fewer) on smaller tiers to leave room for the app.
- **`pg_stat_activity` lag during migrate.** The bulk-copy can
  briefly hold an `idle in transaction` connection per parallel
  reader, which Heroku's monitoring sometimes flags. The connection
  is intentional and short-lived (it covers the consistent-snapshot
  window); not a defect.
- **`DATABASE_URL` rotates.** Heroku may rotate the connection
  string under load (failover, follower promotion). For long-running
  syncs, prefer reading the DSN fresh at each invocation rather than
  caching it.

## What this recipe doesn't cover

- **PlanetScale-Postgres source.** Different constraints; see
  [`docs/postgres-source-prep.md`](../postgres-source-prep.md) §
  PlanetScale.
- **AWS RDS source without grants.** Similar constraints; the same
  slot-less paths apply, but RDS *does* allow event triggers under
  specific parameter-group settings.
- **Heroku → Heroku.** Yes, you can move between Heroku Postgres
  apps using this exact flow with two `DATABASE_URL`s, but
  `heroku pg:copy` is purpose-built for that and usually the right
  answer.

## See also

- [`docs/postgres-source-prep.md`](../postgres-source-prep.md) — the
  per-feature PG source requirements.
- [`docs/comparison-bucardo.md`](../comparison-bucardo.md) — the
  head-to-head with the other slot-less trigger-based PG tool.
- [recipe-bidirectional-cutover.md](recipe-bidirectional-cutover.md)
  — the general cutover sequence with all the flag knobs.
