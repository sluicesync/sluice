# sluice v0.103.1

Two of v0.103.0's fixes were shipped inert — present in the code, unreachable from the path that would have used them. Both were found by the post-release regression cycle running the released binary against real databases, and both are corrected here. Drop-in upgrade; one CLI change affects `metrics-watch` org-wide mode only.

### Fixed

**A warm resume with a changed `--where` was still accepted on every source engine except Postgres.** v0.103.0 announced this guard as engine-neutral. It was not: the check sat inside a phase that begins with a type assertion on the publication-scope capability and returns immediately when it fails, and Postgres is that capability's only implementer. So on MySQL, Vitess, SQLite and the trigger-CDC engines the recorded hash was never written and the comparison never ran — a narrowed, widened or removed predicate all resumed cleanly. A warm resume re-snapshots nothing, so the target kept exactly what the original predicate had copied while the continuous leg classified under the new one: a narrowed filter strands out-of-scope rows on the target permanently, and a widened one never backfills what the first cold start skipped. The contract now lives in its own phase that runs for every engine.

**A `DEFERRABLE PRIMARY KEY` was still dropped on migration.** v0.103.0 fixed the flag that gated the attribute and the emitter that writes it, and neither could run, because the reader's catalog join matched only `UNIQUE` constraints and a primary key is a different constraint type — the data those branches test for never arrived. The classic bulk key shift `UPDATE t SET id = id + 1` therefore still committed on the source and aborted on the migrated target. The join now covers both constraint types.

The schema-fidelity oracle stayed green through that because of a mistake worth naming: its allowlist entries match a statement prefix, and the entry excusing a known primary-key *naming* difference on that table also excused the missing `DEFERRABLE INITIALLY DEFERRED` clause later on the same line. The two axes are now on separate tables — the attribute table needs no exception at all, and the naming exception lives on a table that carries no attributes, so it cannot hide anything else.

**The generated-column refusal named the wrong cause for MySQL.** v0.103.0's message, release notes and docs said MySQL's binlog omits generated columns. It carries them; sluice's decoder drops them deliberately so the target's own `GENERATED` clause recomputes the value rather than freezing the source's. The refusal is unchanged and correct — the decoded row has no such key either way — but an operator reading the old wording could have gone looking for a MySQL setting or version that would change the behaviour, and none exists. Only Postgres genuinely omits them on the wire, and only before PG 18.

### Changed

**`metrics-watch` org-wide mode now requires an explicit `--fleet` flag.** It used to be inferred from omitting `--planetscale-metrics-db`, so a wrapper script whose database variable happened to be unset silently fanned out across every database in the org — also flipping the persisted record's identity from `metrics-watch:<db>` to `metrics-watch:<org>` and inverting `--planetscale-metrics-branch`, where unset then means every branch. Exactly one of the two flags is now required, and passing neither refuses with a message naming both.

### Compatibility

Drop-in apart from one case: if you invoke `metrics-watch` org-wide by leaving `--planetscale-metrics-db` out, add `--fleet`. Nothing else changes — single-database `metrics-watch` is unaffected, and the identically-named telemetry filter on `sync start`, `backup` and `diagnose` is a different flag on a different command and is untouched. Org-wide mode shipped three days ago in v0.102.0, which is why this is being corrected now rather than carried.

Streams established under an earlier version keep resuming: the `--where` drift check accepts the hash spelling recorded by older versions. A stream whose predicate genuinely changed while the check was inert will now refuse on its next resume — that refusal is the bug surfacing, not a new restriction, and it names the recovery.

## Who needs this

- **Anyone running `sync --where` on a MySQL, Vitess, SQLite or trigger-CDC source** — the drift guard v0.103.0 announced was inert for you. If you have ever restarted such a stream with a changed `--where`, the target has been diverging silently ever since: rows the old predicate excluded were never added, rows it included were never removed. Upgrade, then restart — the resume now refuses and tells you how to recover.
- **Anyone who migrated a Postgres schema with a `DEFERRABLE PRIMARY KEY`** — the target has an immediate one. A bulk key shift that commits on the source aborts there. Re-run the schema step, or add the attribute by hand.
- **Anyone invoking `metrics-watch` org-wide by omitting `--planetscale-metrics-db`** — add `--fleet`.
- Everyone else: no action needed beyond upgrading.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.103.1
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:0.103.1`

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
