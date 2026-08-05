# sluice v0.112.0

**A backup you could take, verify clean, and never restore — plus the two constraint classes `schema diff` could not see.**

All three fixes here were found by reading rather than by a failing test, which is the point worth drawing out: each one had a green signal sitting on top of it. `backup verify` returned 0 on an unrestorable backup. `schema diff` reported "in sync" on a target that accepts rows the source rejects. The copy's AIMD retry logic visibly shrank its parallelism right up until it aborted the run. A check that cannot fail is not a check, and all three are now checks.

## Fixed

**A backup containing a row larger than 64 MiB could be written, verified clean, and then fail to restore — on both the full and the incremental path.** The chunk reader caps a single line at 64 MiB. The writer had no matching cap, so an over-long row was written happily:

```
backup full     rc=0
backup verify   rc=0        <-- reports the backup fine
restore         rc=1, 0 rows   "chunk reader: scan: bufio.Scanner: token too long"
```

The operator finds out at the one moment the backup matters. Measured: a 60 MiB row round-trips, a 70 MiB row does not, and `migrate` of the *same* rows succeeds — the data was always fine; only the backup format's read path was not.

**Why `verify` could not see it is the more interesting half.** `backup verify` rehashes the chunk *bytes* and never parses a row, so its evidence is the same artifact it is checking. It can tell you the bytes are intact while saying nothing about whether they can be read back. The independent evidence — whether the chunk's own reader can scan it — was available and unused.

The limit is now one shared constant that **all three write cores** refuse on and **both readers** size their scanner from, so no two of them can drift apart again; the refusal names what would otherwise have happened. Refusing at write time is deliberate: raising the reader's limit would move the wall without removing it, and would do nothing for backups already written, whereas refusing converts an unrestorable artifact with a green verify into a loud failure while the operator can still act.

**Three write cores, and the third one is worth saying out loud** — because the first cut of this fix closed two, and its own regression test asserted "both write cores", meaning both cores *in one file*. The change-chunk writer, which is what `backup incremental` writes through, had no refusal at all, and its reader capped a line at a literal rather than the shared constant. A change event carries a whole row image, so the identical sequence reproduced on the incremental path. Worse, the test was pinned to the number 2, so it would have *failed* on a correct fix — a check whose name says "both write cores" and whose reach is "the two in this file" stops the next reader from looking. The roster is now derived from the package instead of counted by substring: every function that writes a record to a chunk stream must refuse over-long lines, every scanner that reads one back must be sized from the shared constant, and a walker that stops matching the code fails loudly rather than passing on zero findings.

**Existing backups that already contain such a row are still unrestorable, and `verify` still cannot detect the condition on them** — it does not parse rows. This release prevents new ones. A verify depth that scans each chunk through the real reader is filed as its own item rather than folded in here, because the residual is the whole verify surface's evidence model, not this one row size.

One consequence worth knowing about: `backup compact --smart-compaction` can now refuse a *collapsed* event that is larger than any of the events it replaced, because collapsing merges after-images into a union of their columns. Refusing is right — the rewritten chunk would not restore — and the message tells you to re-run with `--smart-compaction-off`, which leaves the events uncollapsed and within the limit.

**`sluice schema diff` could not see index drift that changes which rows are legal.** Indexes were compared by *name only*, so an index present on both sides under one name compared equal however its definition differed. A source `UNIQUE (email(10))` against a target `UNIQUE (email)` was reported as in sync — the exact silent constraint weakening sluice refuses at migration time. The one surface an operator uses to ask "does my target still enforce what my source enforces?" answered yes.

The diff now compares the attributes that decide **which rows are legal**: the column list rendered *including* any per-column prefix length, uniqueness, and the partial `WHERE` predicate — on secondary indexes and the primary key alike. The text report leads with the consequence rather than the attribute (`the target ACCEPTS ROWS THE SOURCE REJECTS`), because that is what decides urgency.

Index *kind* (btree/hash/GIN) and column *ordering* are deliberately still not compared. The first is a performance property no cross-engine pair agrees on, and comparing it would make every MySQL↔Postgres diff permanently noisy; the second is out of scope for this surface's v1. Both are stated rather than left implied.

**`sluice schema diff` could not see foreign-key drift at all.** The per-table diff carried no foreign-key fields whatsoever, so a dropped constraint, a re-pointed parent table, and a weakened referential action all compared equal.

Foreign keys are now compared by constraint name, on what the constraint actually *enforces*: the referencing and referenced column lists and the parent table (a re-pointed FK constrains something else entirely), `ON DELETE` / `ON UPDATE` (a source `CASCADE` against a target `RESTRICT` changes what deleting a parent row *does*), `MATCH FULL` vs `MATCH SIMPLE` (a constraint-strength difference — `MATCH FULL` rejects a partially-NULL composite key that `MATCH SIMPLE` accepts), and deferrability, reported as a unit because the two flags are only meaningful together.

**Unnamed foreign keys are counted, not silently skipped.** They have no stable identity to match on across two schema reads, so they cannot be compared — and a clean report that quietly omitted them would imply coverage it does not have, which is the same defect shape as the rest of this fix one level down. `foreign_keys_unnamed` reports how many were skipped. MySQL always names FK constraints, so it is normally zero.

**A transient shortage of target connection slots could abort a whole table's copy without retrying anything.** The parallel bulk copy shrinks its parallelism and backs off when the target reports SQLSTATE 53300 (`too many connections`), giving up after a bounded number of retries. That retry budget was counted across the whole *table* rather than per *chunk*.

At the shipped default of 4×4 = 16 concurrent workers, sixteen workers meeting the shortage at nearly the same moment consumed the entire budget of 6 between them — **the seventh aborted the run while the first six were still parked in their first backoff, having executed zero retries.** The stated behaviour, "a transient slot shortage degrades to slower-but-correct", did not hold at any parallelism of 7 or more, which is every default multi-core run. What hid it is that the shrink half worked perfectly: an operator watched the AIMD visibly do its job right up until the abort.

The give-up budget is now per chunk. The shrink stays deliberately run-wide — many simultaneous shortages *should* collapse parallelism hard and fast, and that is the AIMD working as intended. A single chunk still gives up after its own bounded number of retries, so a genuinely saturated target still fails loudly rather than stalling forever.

The test that should have caught this had pinned the defect *as the contract* — "many chunks hammering concurrently produce exactly `MaxRetries` successes and the rest give up" — which is why this had to be found by reading. It is replaced by two tests asserting both halves: sixteen concurrent first attempts all proceed, **and** the effective parallelism still collapses to the floor.

## Compatibility

No flags were added, renamed or removed. No error codes changed. Backup chain fingerprints are unchanged, and no chain crosses an epoch boundary.

**`sluice schema diff` will now report drift it previously called in sync, and it exits 1 when it finds any.** If you gate CI on `schema diff`, a pipeline that has been green may go red on the *first* run against an unchanged pair of databases — because the index or foreign-key difference was always there and could not be seen. That is the fix working. Read the report before assuming it is a false positive: a differing index prefix or a weakened `ON DELETE` means the two sides genuinely do not enforce the same thing.

Five additive JSON fields on `schema diff --format json`, all `omitempty`: `indexes_mismatched`, `foreign_keys_missing`, `foreign_keys_extra`, `foreign_keys_mismatched`, `foreign_keys_unnamed`. Existing consumers that read only the fields they already knew about are unaffected.

`sluice backup` now refuses a row whose serialized form exceeds 64 MiB, where it previously wrote it — on `full` and `incremental` alike. This is a new loud failure on a shape that previously produced a silently unrestorable backup. If you meet it, the row is too wide for the backup format and the refusal says so. `backup compact --smart-compaction` can meet it on a *collapsed* row that no single source event reached; `--smart-compaction-off` is the documented way through.

One new exported symbol, `blobcodec.ErrChunkLineTooLong`, so callers that know a path-specific remedy can add one. Internal to the module.

## Who needs this

- **Anyone relying on `backup verify` as their backup-is-good signal**, on any engine. A green verify never covered readability, and if your source has rows in the tens of megabytes — a wide `mediumtext`, `json`, `bytea` or `BLOB` column will do it — you may already be holding a backup that will not restore. **Incremental backups are affected as much as full ones**, and a CDC event carries the whole row image, so an ordinary UPDATE to a wide column reaches it. This release stops new ones; it cannot detect existing ones.
- **Anyone using `sluice schema diff` to gate a deploy or verify a migration.** Two whole constraint classes were invisible to it. Re-run it against a pair you believe is in sync.
- **Anyone running `migrate` or `sync` cold-start against a busy Postgres target**, especially a managed one with a low `max_connections`. If a copy has ever aborted on "too many connections" shortly after starting, this is why, and it now retries as documented.

## Install

```
brew install sluicesync/tap/sluice
scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket && scoop install sluice
```

Binaries for Linux, macOS and Windows are attached below; container images are published to `ghcr.io/sluicesync/sluice`. Every artifact carries a build attestation:

```
gh attestation verify sluice_0.112.0_Linux_x86_64.tar.gz --repo sluicesync/sluice
```
