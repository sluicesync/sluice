# Migrating PlanetScale Postgres to Neki

A tested procedure for moving a PlanetScale Postgres database to PlanetScale Neki, with or without a cutover window.

Everything here was measured end to end on 2026-09-10 against a real PlanetScale Postgres database (`PostgreSQL 18.6`, PS-10, `us-east`) and a real Neki database (`PostgreSQL 18.6 … (Neki)`, PS-10, same region): a 12-table schema covering every value family sluice knows about, migrated both ways described below, with every table verified byte-identical afterwards. Where something is *not* tested, this page says so rather than implying coverage.

Background on Neki's behaviour as a target — including the parts that change when you shard — is in [`managed-services.md`](../managed-services.md#planetscale-neki-sharded-postgres). Read that first if you plan to shard.

## What you are working with

Both sides are PostgreSQL 18.6, which removes a whole class of version-skew concern: no type was introduced or removed between them, and rendering matches. sluice uses the ordinary `postgres` driver for both ends — **there is no `neki` engine and no `--target-driver neki`**. sluice detects Neki from the server's `version()` string and adapts on its own.

```bash
sluice migrate \
  --source-driver postgres --source "$PS_POSTGRES_DSN" \
  --target-driver postgres --target "$NEKI_DSN"
```

## Before you start

**1. Connection strings.** `pscale role reset-default <db> <branch> --format json` returns a `database_url` carrying `sslmode=verify-full`. That works anywhere the system CA store is populated. In a container without CA certificates it fails with `root certificate file "/root/.postgresql/root.crt" does not exist` — use `sslmode=require`, or mount a CA bundle. This applies to both ends.

**2. Extensions must exist on the target first.** sluice does not install them, deliberately — extensions are surfaced, never silently auto-handled. Check what the source has and create the same set on the target:

```sql
-- on the source
SELECT extname, extversion FROM pg_extension ORDER BY 1;
-- on the target, for each one your schema actually uses
CREATE EXTENSION IF NOT EXISTS btree_gist;
```

`btree_gist`, `btree_gin`, `pgcrypto` and `postgis` are all available on Neki. If you miss one, sluice refuses with `SLUICE-E-SCHEMA-EXTENSION-NOT-ENABLED` naming the extension and the exact command — it does not fail with PostgreSQL's raw "no default operator class" message.

**3. Nothing to configure for CDC.** Measured on PlanetScale Postgres: `wal_level` is already `logical`, and the default `postgres` role already has `rolreplication = true`. There is no operator action here, unlike most managed PostgreSQL. `max_replication_slots` was 20.

**4. Tables without a primary key need a replica identity — only if you use `sync`.** A one-shot `migrate` does not care. For `sync`, PostgreSQL cannot identify rows for `UPDATE`/`DELETE` without one, and sluice refuses up front with `SLUICE-E-SOURCE-REPLICA-IDENTITY` naming the table:

```sql
ALTER TABLE public.tt_nopk REPLICA IDENTITY FULL;
```

or take the table out of scope with `--exclude-table`. Do this before starting, not after — the refusal happens at preflight, before anything is copied.

## Option A — one-shot migration (with downtime)

Simplest, and right when you can take a window.

```bash
sluice migrate \
  --source-driver postgres --source "$PS_POSTGRES_DSN" \
  --target-driver postgres --target "$NEKI_DSN" \
  --migration-id ps-to-neki
```

Measured: 12 tables, exit 0, and every table byte-identical to the source afterwards.

## Option B — minimal downtime (`sync`)

Copies, then tails changes until you cut over.

```bash
sluice sync start \
  --source-driver postgres --source "$PS_POSTGRES_DSN" \
  --target-driver postgres --target "$NEKI_DSN" \
  --stream-id ps-to-neki \
  --slot-name psneki \
  --publication-name sluice_pub_psneki
```

The cold copy runs, then the stream reports `entering CDC mode` and applies changes continuously. Measured live: inserts, updates and deletes made on the PlanetScale Postgres source after the handoff — including `NaN`, `-0` and `Infinity` in float and numeric columns, astral-plane text, an enum change, a range change, a composite-key delete and an insert into the keyless table — all landed on the Neki target, with every table still byte-identical.

**Use a dedicated `--publication-name` per stream.** sluice refuses to re-scope a publication another slot is reading, which is the right behaviour and easy to trip over if you run more than one stream against the same source.

### Cutover

1. Stop writes to the PlanetScale Postgres source.
2. Wait for the stream to drain (`sluice sync status --stream-id ps-to-neki`).
3. Verify (below).
4. Point the application at Neki.
5. `sluice sync stop --stream-id ps-to-neki`.

## Verifying

Do not take exit 0 as proof. Compare values, computed the same way on both servers:

```sql
-- run on BOTH, per table, with the source's column list
SELECT md5(coalesce(string_agg(rowtext, ',' ORDER BY rowtext), '')) AS ck, count(*) AS n
FROM (SELECT coalesce("col1"::text,'~NULL~') || '|' || coalesce("col2"::text,'~NULL~')
      FROM public."your_table") s(rowtext);
```

Casting every column to `text` on the server compares what each side actually stores, rather than what a client decoded. Take the column list from the **source** so a dropped or invented column shows up as a mismatch rather than being excluded from both sides.

## Things to know before you shard

A migration lands on an **unsharded** Neki database, where everything above holds and PostgreSQL semantics are intact. Sharding later changes two things that matter:

- **Uniqueness becomes per-shard.** A `PRIMARY KEY`, `UNIQUE` or `EXCLUDE` constraint is enforced only within a shard unless its columns contain the shard key. Neki accepts the constraint declaration either way, so the guarantee weakens silently. Measured: a table with `email text NOT NULL UNIQUE` accepted two rows with the same email once they routed to different shards.
- **sluice requires the shard key in the primary key** to keep syncing into a sharded table, and refuses with `SLUICE-E-TARGET-SHARD-KEY-NOT-IN-UPSERT-KEY` otherwise. That refusal exists because `ON CONFLICT` on a sharded table is evaluated only on the shard the incoming row routes to, so an upsert can insert a duplicate of the key instead of updating.

Both point the same way: **if you intend to shard, get the shard key into the primary key of the tables that will be sharded, ideally before the migration.**

## Not supported: Neki as a continuous-sync source

`migrate` **out of** Neki works, including from a sharded database. `sync` out of it does not, and that is a platform limitation rather than missing work: a Neki replication connection can export a snapshot but there is no way to import one (`pg_export_snapshot()` and `SET TRANSACTION SNAPSHOT` are unimplemented), so there is no consistent handoff from the bulk copy to the change stream.

Plan a cutover window if you ever need to move back off Neki.

## What has not been tested

Stated so this page cannot be read as broader than it is:

- Migrating **into an already-sharded** Neki database. Every measurement here targeted an unsharded one, which is where a migration lands by default.
- Databases at scale — the fixture was correctness-shaped (every value family, adversarial values), not volume-shaped.
- Multiple schemas. Everything here used `public`.
- MoveTables and online DDL running underneath a live stream. A **reshard** underneath a live stream is tested and clean; those two are not.
