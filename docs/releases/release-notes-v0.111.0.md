# sluice v0.111.0

**A Postgres column type that could never migrate at all, a primary key that was silently widened, and every "the target cannot represent this index" refusal moved ahead of the copy.**

All three came out of the same place: the v0.110.1 post-release regression cycle, and the sibling sweeps that its findings set off. Two of the three were found not by the bug that was reported but by asking *what else has this shape* — which is why the fixes below are class fixes with the roster written down, rather than three repairs of three instances.

## Added

**An index the target cannot represent is now refused before any data moves, on every entry point.** sluice builds secondary indexes after the bulk copy, and every "this index cannot be represented on the target" refusal lived in the emitter for that phase. So each one was loud, zero-loss and correct — and arrived after the whole table had already been copied. On a large table that is hours of work discarded to report something knowable from the schema alone, before a single row moves.

Three refusals were affected, one per engine, each travelling in a different direction: a MySQL partial `UNIQUE` (whose ordinary path is the deferred `ADD INDEX`), a SQLite index prefix length (whose only call site was `CREATE INDEX`, with no early path at all), and a Postgres index prefix length on the deferred `ADD CONSTRAINT … UNIQUE`. The target engine is now asked before the copy at `migrate`, both `sync` cold-start paths, `add-table`, `restore` and chain `restore` — six entry points, not the two the original report named.

**Nothing about what is refused changes — only when.** That distinction is load-bearing: correctness here is pinned as *agreement with the real emitter* over a per-engine shape matrix, so a preflight that refuses something the emitter would have accepted fails just as loudly as one that misses a case. An over-refusal breaks migrations that work today, which is the worse failure of the two.

Two previous releases claimed the timing this actually delivers. Those entries carry dated corrections.

## Fixed

**A Postgres `macaddr8` column could not be migrated or synced to a Postgres target at all.** sluice read both `macaddr` and `macaddr8` into one IR type that carried no width, and the target emitter always wrote `MACADDR` — so an 8-byte column was narrowed to 6 and the copy failed outright, on `migrate`, on `sync start`, and on the `macaddr8[]` sibling. Zero-loss and loud, which is exactly why it survived so long: nobody with such a column ever had a working migration to notice the loss of.

The IR now carries the width, both reader paths set it (the schema reader and the CDC relation reader, which have drifted from each other before), and the emitter picks `MACADDR8`. **Existing backup chains are unaffected** — the width rides the manifest so a restore rebuilds the column correctly, but it is deliberately excluded from the chain's schema fingerprint, so no chain is repartitioned and none becomes unrestorable.

**The array form needed a fix of its own, and it was not sluice's code.** With the emitter corrected, `macaddr8[]` still failed: pgx registers a codec for the `macaddr8` scalar and for `macaddr[]`, but not for `macaddr8[]`. sluice's path is byte-identical for both widths — the driver's is not, which is the same shape as the v0.69.3 array regression that became Bug 74. sluice now registers the missing array codec itself. A `{macaddr, macaddr8} × {scalar, array}` probe against a real server is what found it: three of those four cells pass on stock pgx and the fourth does not.

**A `--where` filter on a `macaddr8` column silently dropped every CDC change to the rows it matched.** Postgres widens a 6-byte address to EUI-64 on input — `08:00:2b:01:02:03` is stored and delivered as `08:00:2b:ff:fe:01:02:03` — so the spelling an operator naturally writes never equalled the delivered value. It is now refused, with the widened form named.

This also **lifts a deliberate over-refusal shipped in v0.110.1**: an 8-byte literal on a `macaddr8` column was refused even though it was correct, because sluice could not tell the two widths apart. It can now, so it compiles.

**A MySQL `PRIMARY KEY` prefix length was silently dropped against a SQLite target.** v0.110.1 refused this on a secondary `UNIQUE KEY` and stopped there — but the same prefix on the primary key reaches a different renderer, one that had never consulted it. So `PRIMARY KEY (email(20), id)` became `PRIMARY KEY (email, id)`: the source forbids two rows sharing the first 20 characters of `email`, the target admits them, the migration exits 0, and nothing anywhere says so.

It is now refused, both at the schema emitter and — ahead of that — at the new pre-copy preflight, so no DDL runs at all. The refusal names a way forward that a primary key actually has: SQLite's `PRIMARY KEY` clause takes column names, so unlike a secondary index it cannot simply move to a `substr(…)` expression. Widen the key on the source, or key the table on something else and reproduce the source's rule as a unique expression index alongside it.

Postgres was never affected — its primary key renders through the same gated path as its unique constraints — and MySQL carries prefixes natively. Both exemptions are now pinned by a test rather than asserted in a comment, because the MySQL one rests on a fact about the *target*, and a fact about the world outside sluice's code is exactly the kind of premise that should be checked by a machine rather than remembered by a person.

## Changed

**Two internal gates that could not fail on what their names claimed were repaired.** One pinned an IR column's manifest round trip by setting three of its eight wire fields to zero and asserting four of them, hand-picked — both halves vacuous in the same direction, since a dropped field comes back zero. Deleting a real field assignment from the decoder left it green: silent schema loss through every backup manifest, with a green gate over it. The other checked that every MySQL index-emit site consults the predicate policy, using a curated two-entry allowlist that skipped anything not on the list — so it could not fail on an *added* site, which is the only way that defect ever arrives.

Both are now derived from the code rather than from a list. Neither had ever caught anything, and neither would have.

## Compatibility

**One behaviour change can stop a migration that previously ran to completion: a MySQL `PRIMARY KEY` carrying a prefix length is now refused against a SQLite target.** It ran before by silently weakening the key, so the target has been admitting rows the source forbids for as long as it has existed. If you have such a schema, the refusal names the column, the prefix width and the options.

**`migrate --dry-run` against a schema with an unrepresentable index now reports the refusal instead of printing a plan.** That matches every other gate in the same phase, and a plan that could never execute should not exit 0.

**Postgres `macaddr8` columns migrate for the first time.** If you previously excluded such a table to get a migration through, it no longer needs excluding. Backup chains written by earlier versions restore unchanged.

**Existing backup chains are not repartitioned by this release.** The new IR width rides the manifest but is excluded from the schema fingerprint, deliberately, so no chain crosses an epoch boundary and none becomes unrestorable.

No flags were added, renamed or removed, and no error codes changed.

## Who needs this

- **Anyone with a Postgres `macaddr8` or `macaddr8[]` column.** Migration and sync of that column were impossible before this release, in every direction, on every prior version.
- **Anyone migrating MySQL → SQLite with a prefixed `PRIMARY KEY`, including a SQLite file bound for D1.** Your target's key is weaker than your source's today, silently. This release refuses the schema instead of continuing. (There is no D1 *target* engine — `d1` is a migrate source only — so a D1-bound migration goes through the `sqlite` engine and is reached by this. Earlier notes said "SQLite or D1 target"; those carry a correction.)
- **Anyone migrating large tables to any target.** If a schema contains an index the target cannot represent, you now learn it before the copy rather than after it.
- **Anyone using `--where` on a `macaddr8` column.** The filter was silently matching nothing; it is now refused with the correct spelling named, or accepted where it is genuinely correct.

## Install

```
brew install sluicesync/tap/sluice
scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket && scoop install sluice
```

Binaries for Linux, macOS and Windows are attached below; container images are published to `ghcr.io/sluicesync/sluice`. Every artifact carries a build attestation:

```
gh attestation verify sluice_0.111.0_Linux_x86_64.tar.gz --repo sluicesync/sluice
```
