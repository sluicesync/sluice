# Sluice Quickstart

A 10-minute walkthrough that migrates a real ~30 MB dataset between MySQL and Postgres using sluice. By the end you'll have done a one-shot bulk migration MySQL → Postgres and watched continuous-sync replicate live changes from the source to the target.

## What you'll build

- A MySQL 8.0 source pre-loaded with the [sakila](https://dev.mysql.com/doc/sakila/en/) sample database — the canonical "DVD rental store" schema with 16 tables, ~1000 films, FKs, indexes, the works.
- An empty Postgres 16 target ready to receive the migrated data.
- A sluice binary on your local PATH.
- One simple-mode migration and one continuous-sync stream.

## Prerequisites

- **Go 1.25+** (for `go install`).
- **Docker** + **Docker Compose**.
- About 5 minutes for the first-run sakila download.

## 1. Install sluice

```sh
$ go install github.com/orware/sluice/cmd/sluice@latest

$ sluice --version
sluice version v0.x.x

$ sluice engines
NAME          BULK LOAD           CDC
mysql         load-data-infile    binlog
planetscale   batched-insert      vstream
postgres      copy                logical-replication
```

If `sluice` isn't found, ensure `$(go env GOPATH)/bin` is on your `PATH`.

## 2. Start the databases

```sh
$ cd docs/examples/quickstart
$ docker compose up -d
[+] Running 2/2
 ✔ Container sluice-quickstart-mysql     Healthy
 ✔ Container sluice-quickstart-postgres  Healthy
```

The first start downloads sakila (~30 MB) into MySQL. Tail the logs if you want to watch:

```sh
$ docker compose logs -f mysql | grep "sakila"
==> Downloading sakila sample database...
==> Extracting sakila...
==> Loading sakila schema...
==> Loading sakila data...
==> Sakila loaded (1000 films)
```

## 3. Verify the source

```sh
$ docker compose exec mysql mysql -uroot -prootpw sakila -e "SELECT COUNT(*) FROM film"
+----------+
| count(*) |
+----------+
|     1000 |
+----------+
```

The Postgres side starts empty — `pagila` exists as a database, but no tables. Sluice will create them in the next step.

## 4. Migrate MySQL → Postgres (simple mode)

### 4a. Trim sakila for the demo

Sakila ships two columns that don't have clean cross-engine translations and which sluice rejects with a clear error rather than silently dropping:

- **`address.location`** — a `GEOMETRY` (POINT) column. Postgres has no native geometry type without the PostGIS extension; sluice declines to fake it.
- **`film.special_features`** — a MySQL `SET('Trailers','Commentaries',...)` column. Postgres has no native SET; the tool would need to translate to `TEXT[]`, which is a per-column policy decision.

For the walkthrough, drop both up front. In production you'd handle these via a per-column type override in `sluice.yaml` (see [docs/examples/sluice.yaml](sluice.yaml)) or by recreating PostGIS-shaped columns by hand on the target.

```sh
$ docker compose exec mysql mysql -uroot -prootpw sakila \
    -e "ALTER TABLE address DROP COLUMN location" \
    -e "ALTER TABLE film DROP COLUMN special_features"
```

### 4b. Dry-run first

```sh
$ sluice migrate \
    --source-driver mysql \
    --source "root:rootpw@tcp(localhost:3306)/sakila" \
    --target-driver postgres \
    --target "postgres://postgres:postgres@localhost:5432/pagila?sslmode=disable" \
    --dry-run

DRY RUN — would migrate mysql → postgres
  16 tables to create, populate, and constrain:
    - actor          (4 columns, 2 indexes, 0 foreign keys)
    - address        (8 columns, 2 indexes, 1 foreign keys)
    - category       (3 columns, 1 indexes, 0 foreign keys)
    ...
```

Dry-run reads the source schema and prints what would happen. No writes to the target.

### 4c. Apply the migration

```sh
$ sluice migrate \
    --source-driver mysql \
    --source "root:rootpw@tcp(localhost:3306)/sakila" \
    --target-driver postgres \
    --target "postgres://postgres:postgres@localhost:5432/pagila?sslmode=disable"

pipeline: migrated 16 tables
```

What just happened:

1. **Schema phase 1**: sluice translated each MySQL `CREATE TABLE` to its Postgres equivalent (handling type translation per [docs/type-mapping.md](../type-mapping.md)) and applied them with PRIMARY KEY only — no secondary indexes or foreign keys yet.
2. **Bulk-copy**: ~16K rows total were streamed from MySQL via `database/sql` and written to Postgres via `COPY FROM STDIN` (the canonical fast path; see [ADR-0006](../adr/adr-0004-three-phase-apply.md) for the phase model and [docs/dev/notes/prep-postgres-copy-writer.md](../dev/notes/prep-postgres-copy-writer.md) for the COPY implementation).
3. **Identity-sequence sync**: each Postgres `IDENTITY` column's underlying sequence was advanced past the bulk-copied max so user-initiated `INSERT`s pick up the next ID without colliding (see [docs/dev/notes/prep-translation-policy-edges.md](../dev/notes/prep-translation-policy-edges.md)).
4. **Schema phase 2 + 3**: secondary indexes and foreign keys were added.

### 4d. Spot-check the result

```sh
$ docker compose exec postgres psql -U postgres pagila -c "SELECT COUNT(*) FROM film"
 count
-------
  1000

$ docker compose exec postgres psql -U postgres pagila \
    -c "SELECT nextval(pg_get_serial_sequence('public.film','film_id'))"
 nextval
---------
    1001
```

The sequence advanced past the bulk-copied 1000 — inserting a new row picks up `1001`, no collision.

## 5. Continuous sync (MySQL → Postgres)

Reset the target so we can demonstrate the snapshot+CDC handoff cleanly:

```sh
$ docker compose exec postgres psql -U postgres pagila \
    -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public; GRANT ALL ON SCHEMA public TO postgres;"
```

In **terminal 1**, start the streamer:

```sh
$ sluice sync start \
    --source-driver mysql \
    --source "root:rootpw@tcp(localhost:3306)/sakila" \
    --target-driver postgres \
    --target "postgres://postgres:postgres@localhost:5432/pagila?sslmode=disable" \
    --stream-id quickstart

pipeline: stream_id = "quickstart"
pipeline: cold start; snapshot captured at {token={"slot":"sluice_slot","lsn":"0/...."}}
pipeline: bulk-copy complete; entering CDC mode
```

The streamer is now running. Bulk copy completed under the snapshot's logical clock; CDC is streaming any further changes from the source binlog through to the target.

In **terminal 2**, make a change on the source:

```sh
$ docker compose exec mysql mysql -uroot -prootpw sakila \
    -e "INSERT INTO actor (first_name, last_name) VALUES ('Test', 'Actor')"
```

Within a second, see it on the target:

```sh
$ docker compose exec postgres psql -U postgres pagila \
    -c "SELECT first_name, last_name FROM actor WHERE last_name='Actor'"
 first_name | last_name
------------+-----------
 Test       | Actor
```

### 5a. Stop and resume

In terminal 1, hit `Ctrl-C`. The streamer logs a clean exit and returns. Then restart it with the same command:

```sh
^C
$ sluice sync start \
    --source-driver mysql \
    --source "root:rootpw@tcp(localhost:3306)/sakila" \
    --target-driver postgres \
    --target "postgres://postgres:postgres@localhost:5432/pagila?sslmode=disable" \
    --stream-id quickstart

pipeline: stream_id = "quickstart"
pipeline: warm resume from persisted position {token=...}
```

Notice the difference: **warm resume** instead of **cold start**. The streamer found the persisted position in the target's `sluice_cdc_state` table (created during the first run) and skipped the snapshot+bulk-copy phase entirely. Insert another row in terminal 2 and watch it propagate — the resumed CDC picks up cleanly.

## 6. What you've seen

In ~10 minutes, sluice exercised most of its v1 surface against a real dataset:

- **Schema translation** across MySQL ↔ Postgres ([ADR-0001](../adr/adr-0001-ir-first-translation.md))
- **Three-phase apply** with the post-bulk-copy identity-sync step ([ADR-0004](../adr/adr-0004-three-phase-apply.md))
- **COPY-protocol bulk load** on Postgres ([ADR-0008](../adr/adr-0008-go-mysql.md) for the symmetric MySQL CDC choice)
- **Snapshot-to-CDC handoff** with no gap and no overlap ([ADR-0010](../adr/adr-0010-idempotent-applier.md))
- **Position persistence** on the target enabling warm resume ([ADR-0007](../adr/adr-0007-position-persistence.md))
- **Idempotent applier semantics** that make resume safe ([ADR-0010](../adr/adr-0010-idempotent-applier.md))

## 7. What's not handled

The walkthrough is honest about what sluice does *not* do:

- **Triggers, stored procedures, views, functions.** Sakila ships several of each. Sluice translates schema and row data only — procedural code stays where it is. If your migration needs them, you'll recreate them on the target by hand or from your application's migration scripts.
- **GEOMETRY columns** (the reason §4a's pre-migration trim drops `address.location`). Postgres has no native geometry type without PostGIS; sluice errors clearly rather than fake it. To handle GEOMETRY in production, install PostGIS on the target and use a per-column type override in `sluice.yaml`.
- **MySQL `SET` columns** (the reason §4a drops `film.special_features`). Postgres has no SET; the right translation is policy-dependent (`TEXT[]`, multiple booleans, a separate junction table). v1 requires the operator to make the call via a per-column type override.
- **MySQL fulltext indexes.** Sakila has a fulltext index on `film.description`. Postgres's fulltext is a different mechanism (`tsvector` + GIN); sluice doesn't translate between them. The migration will succeed and skip the unsupported index kind; the target won't have fulltext search until you add a `tsvector` column and a GIN index by hand.
- **Schema drift during continuous sync.** If the source schema changes mid-stream, sluice's CDC reader invalidates its schema cache but doesn't replay the DDL on the target. For v1, schema changes during sync require a planned sync stop, manual DDL on both sides, and a fresh sync start.

## 8. Cleanup

```sh
$ docker compose down -v
[+] Running 4/4
 ✔ Container sluice-quickstart-mysql      Removed
 ✔ Container sluice-quickstart-postgres   Removed
 ✔ Volume    quickstart_mysql_data        Removed
 ✔ Volume    quickstart_postgres_data     Removed
```

The `-v` removes the data volumes. A fresh `docker compose up -d` re-downloads sakila and starts over.

## Where to next

- **[docs/architecture.md](../architecture.md)** — the design overview and the engine pattern that makes adding new database engines a contained operation.
- **[docs/adr/](../adr/)** — architecture decision records, one per load-bearing design choice.
- **[docs/type-mapping.md](../type-mapping.md)** — the cross-engine type-translation policy.
- **[docs/dev/development.md](../dev/development.md)** — local dev environment setup, including the integration-test gotchas (Rancher Desktop ryuk reaper, Windows-specific notes).
- **[docs/dev/roadmap.md](../dev/roadmap.md)** — what's planned next.
