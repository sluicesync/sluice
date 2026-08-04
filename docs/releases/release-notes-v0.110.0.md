# sluice v0.110.0

**Two correctness fixes on paths that failed quietly, and one refusal that will now stop a migration that used to appear to succeed.**

Both fixes come out of the same review pass, and both are the same shape: code that assumed it was handed a complete row, and a check that could not have noticed it wasn't.

## Fixed

**`backup compact --smart-compaction` could drop a large column's value from a backup chain, silently.** Collapsing an INSERT and a later UPDATE into a single INSERT kept the UPDATE's column values and discarded the INSERT's — correct only if every after-image is a complete row, and against a Postgres source it is not. Postgres omits an unchanged out-of-line (TOASTed) column from an UPDATE's after-image, where a missing column means "leave the target's value alone". That reading is right for an UPDATE and wrong the moment the event becomes an INSERT, which has no existing value to leave alone: the column arrived on restore as NULL or its default.

The same fault had a second form the original report did not name. Collapsing two UPDATEs kept only the later after-image, so a column the *first* UPDATE wrote and the second left untouched was carried by neither the merged event nor the target — which still held the value from before the first UPDATE. Compaction now unions the after-images: the later event wins for every column it carries, and a column only an earlier event carries survives.

`backup verify` could not have caught either form. Compaction re-stamps the hashes that verify subsequently checks, so the check and the thing being checked share an artifact.

**A partial `UNIQUE` index was carried to a MySQL-family target as a whole-table one, making the target stricter than the source.** Postgres and SQLite can constrain uniqueness over a subset of rows — `CREATE UNIQUE INDEX … ON t (email) WHERE deleted_at IS NULL` forbids duplicate live emails while permitting any number of soft-deleted ones. MySQL has no partial-index feature, and the predicate was dropped without comment, leaving a `UNIQUE KEY (email)` that forbids both. The target then refuses rows the source holds legally.

sluice now refuses this, and names the rewrite that reproduces the semantics on MySQL 8: a generated column that is NULL outside the predicate, indexed `UNIQUE`, since MySQL permits unlimited NULLs in a unique key. Dropping the predicate on the source or excluding the table are the other ways forward.

> **Correction (2026-08-04).** This section first said the refusal happens "before any data moves". That holds for the two `CREATE TABLE` inline paths and **not** for an ordinary secondary index, which is emitted in the deferred `CreateIndexes` phase — *after* the bulk copy. On a large table the refusal therefore arrives having already copied every row. Passing `--upfront-indexes` moves index creation ahead of the copy and restores the early refusal today; moving the check into preflight so it is unconditional is tracked as follow-up work. The refusal itself is correct in every case — only its timing was misstated, and that timing was the stated reason for preferring a refusal to a warning.

## Changed

**`--auto-prune-change-log` now warns that it is single-stream only.** The prune cuts at one stream's durably-applied frontier and deletes by change-log id, without regard for which tables a row belongs to. A source change log is shared by every sync reading that database, so a second, slower sync loses the rows between the two frontiers before it ever reads them.

sluice cannot currently detect the peer: the change log carries no consumer registry, and each stream's position lives on its own target, out of reach of the source-side pruner. So the flag says so — at startup and in its help text — rather than refusing, which would break the single-stream case that is both common and entirely safe.

## Compatibility

**One behaviour change that can stop a migration that previously ran.** A partial `UNIQUE` index against a MySQL, MariaDB, PlanetScale or Vitess target is now refused at translation. This is deliberate. The old behaviour failed mid-copy, potentially hours in, if a collision already existed in the data — and otherwise migrated clean and then rejected the operator's first ordinary write, long after the migration had been called a success. One message instead of a mid-copy duplicate-key error covers both cases (see the timing correction above).

Partial **non-unique** indexes are unaffected and still carried with a warning: the widened index covers a superset of the rows, so cost changes and correctness does not.

No flags were added, renamed or removed. No error codes changed. Backup chain formats and schema fingerprints are unchanged, so no chain is repartitioned and no existing backup needs re-taking.

## Who needs this

- **Anyone running `backup compact --smart-compaction` against a Postgres source.** The flag is off by default, so an operator who has never set it is unaffected. If you have used it on a chain containing tables with large `text`, `bytea`, `json` or `jsonb` columns, a restore from that chain may hold a NULL or a stale value in those columns. **Assume every table in such a chain is exposed.**

> **Correction (2026-08-04).** As first published, this bullet ended "Only tables *without* a row filter are affected — a filtered table already received the complete image, for unrelated reasons." That sentence is true of the *sync* path and meaningless here: no `backup` subcommand accepts `--where`, so the completeness backfill that exempts a filtered table is never engaged when building a chain. It read as a narrowing, and the narrowing does not exist. It was written from the mechanism (the backfill has exactly one caller, on the filtered path) without asking whether a filtered table can occur in a backup at all — an error in the under-warning direction, which is the expensive one.
- **Anyone migrating Postgres or SQLite to a MySQL-family target whose schema uses partial unique indexes.** Soft-delete tables are the common case. You will now get a refusal at translation naming the index, the predicate and the rewrite, instead of a duplicate-key failure later.
- **Anyone using `--auto-prune-change-log` with more than one sync off a single trigger-CDC source.** Read the new warning; the combination is unsafe today and the flag will tell you so.

## Install

```
brew install sluicesync/tap/sluice
scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket && scoop install sluice
```

Binaries for Linux, macOS and Windows are attached below; container images are published to `ghcr.io/sluicesync/sluice`.
