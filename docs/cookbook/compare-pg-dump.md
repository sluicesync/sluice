# sluice vs. `pg_dump` + `pg_restore`

The first question every PG operator asks when they hear "schema +
data migration tool" is reasonable: **why not just `pg_dump`?** It's
in every PG distribution, it's been around forever, it works, and
it's free. This page is the honest answer.

**Short version.** If your migration fits `pg_dump`'s shape — PG
source, PG target, brief downtime acceptable, no cross-engine,
no live writes during the migration window — `pg_dump` is the right
answer and sluice doesn't replace it. sluice solves a different
problem: **continuous sync, cross-engine targets, redaction
pipelines, and low-downtime cutover with CDC catch-up.**

## When `pg_dump` is the right tool

- **One-shot PG → PG copy** where the source can be made read-only
  during the dump → restore window.
- **Schema-only snapshots** for version control (`pg_dump --schema-only`).
- **Per-table or per-schema partial exports** that you'll hand-edit
  before loading.
- **Backup-as-text** — a `.sql` file is the simplest possible
  restore target (`psql -f`) and the most portable across PG versions.

Don't reach for sluice for any of the above. `pg_dump` is faster,
simpler, and battle-tested.

## When sluice is the right tool

The shape that pushes operators toward sluice (or any CDC tool) is
when one or more of these applies:

### Continuous sync, not one-shot

`pg_dump` is a point-in-time snapshot. The dump captures the
database state at the moment it ran; after that moment, any writes
on the source are invisible to the snapshot.

For migrations where you can't afford a read-only window — production
databases, multi-day cutover validation, dual-read testing — you need
CDC: an ongoing stream of changes from the source to the target so
the target stays current. sluice's `sync start` runs that stream.
`pg_dump` doesn't.

### Cross-engine targets

`pg_restore` only loads into Postgres. If your target is MySQL,
PlanetScale-MySQL, or any other engine, `pg_dump` isn't an option —
the SQL dialect, type system, and DDL grammar all differ. sluice's
IR-first design routes the migration through a typed schema model
that engine writers consume; MySQL targets get MySQL-syntax DDL,
MySQL-target value encoding, MySQL CHECK constraints (on 8.0.16+),
all from the same `sluice migrate` command that targets a PG dst.

### Cutover-time sequence priming

`pg_dump` + `pg_restore` of a database with `bigserial` / `serial`
columns leaves the target's sequences at their initial value (1).
The bulk-loaded rows have IDs from the source, but the next-inserted
row on the target gets `id = 1`, which collides.

The canonical fix is to run `setval(pg_get_serial_sequence(...),
MAX(id))` per table after the restore. Operators do it by hand or
with a script. sluice's `cutover` subcommand reads the source's
current `MAX(id)` per identity column and primes the target with a
configurable margin (`--cutover-sequence-margin=1000`) so a CDC
stream catching up after the dump doesn't introduce mid-flight
collisions. See [recipe-bidirectional-cutover.md](recipe-bidirectional-cutover.md).

### Built-in redaction pipeline

Want to ship a copy of production to staging without PII? `pg_dump`
gives you the schema + data; redaction is your problem to solve
externally (Tonic, Privacy Dynamics, custom ETL between dump and
restore, etc.).

sluice composes 27 redaction strategies into the IR pipeline —
`null`, `static`, `hash:hmac-sha256`, `mask:ssn`, `mask:pan`,
`tokenize:dict` (deterministic), `randomize:email` (replay-stable),
plus a persisted keyset so two sluice streams against the same
source produce **the same** surrogate for the same input. See
[recipe-redaction-keyset.md](recipe-redaction-keyset.md).

### Loud-failure tenet on edge cases

`pg_dump` is conservative — it dumps what's in the catalog and lets
`pg_restore` complain if the target can't accept it. For PG → PG
this is usually fine. For PG → MySQL via `pg_dump --inserts` →
manual conversion, the failure surface is your own conversion
script; whether silent-loss classes (DOMAIN constraints dropped,
range types silently downcast, EXCLUDE constraints lost) hide in
the conversion is on you to verify.

sluice's design tenet is **refuse loudly** on any shape that can't
be carried faithfully. PG `tsvector` to MySQL? Refused at preflight
with the column named. EXCLUDE constraint to MySQL? Refused.
DOMAIN with a CHECK that can't translate? WARN to observability
with the column named, downgrade to base type. Every silent-loss
class we've caught has a structured refuse-loudly message; see the
case study in [case-gitlab.md](case-gitlab.md).

### Backup chain with at-rest encryption

`pg_dump` + GPG-encrypt-the-file is a workable backup story for
many shops. sluice's `backup full` + `backup stream run` is the
continuous-incremental version: chunked, resumable, segment-rotated,
with at-rest encryption (passphrase or AWS KMS), per-chunk passphrase
rotation, and a loud refuse at backup time if the operator rotated
mid-chain in a way restore couldn't recover. See [recipe-backup-encrypted.md](recipe-backup-encrypted.md).

If you're using `pg_dump` for one-shot DR snapshots, keep doing
that — sluice's backup chain is overkill for "weekly dump to
S3." If you're moving toward continuous-incremental DR with point-
in-time restore, that's where sluice's backup family fits.

## The "use both" pattern

A common shape: `pg_dump` for the schema-only initial copy
(operators want the `.sql` for code review / version control / audit),
then sluice for the data + continuous sync.

```sh
# Schema via pg_dump → code review → apply to target
pg_dump --schema-only ... | psql ... target

# Data + sync via sluice from this point forward
sluice migrate ... --schema-only=false --no-create-schema
sluice sync start ... --resume
```

This is fine. sluice doesn't have a problem with target schemas
that already exist as long as they match the source's expected
shape (it'll diff against the IR's expectation and refuse if
they don't line up — `sluice schema diff` is the cheap dry-run).

## What sluice doesn't try to be

- **A `pg_dump` replacement for ad-hoc PG → PG dumps.** Use `pg_dump`.
  It's simpler.
- **A backup tool for text-based recovery.** sluice's backup format
  is structured chunks, not SQL text — restoring with anything other
  than `sluice restore` isn't supported.
- **A schema-only snapshot tool.** `pg_dump --schema-only` is right.

## See also

- [`docs/comparison.md`](../comparison.md) — sluice vs. Debezium /
  AWS DMS / Fivetran / pgcopydb / HVR / Striim / Qlik.
- [`docs/comparison-bucardo.md`](../comparison-bucardo.md) — sluice
  vs. Bucardo (the open-source PG → PG CDC comparison).
- [`docs/use-cases.md`](../use-cases.md) — operator-persona
  breakdown of when sluice fits.
