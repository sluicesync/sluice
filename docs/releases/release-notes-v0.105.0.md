# sluice v0.105.0

**Large migrations into PlanetScale, hardened at the two places they were breaking: foreign-key creation and the deploy-request index build.** This batch came out of driving a real 122 GB / 153-million-row dataset into PlanetScale-MySQL and finding that the whole errno-3024 statement-wall story had been solved for indexes and left unsolved for foreign keys — so a schema with foreign keys, which is most schemas, could not complete a large migration. It also makes the deploy-request index fallback wait for the build instead of giving up after an hour, and fixes three smaller correctness and UX gaps found along the way.

**If you migrate large tables into PlanetScale-MySQL, this is a recommended upgrade.**

### Added

**Foreign keys on a large PlanetScale-MySQL table now complete — and stay loud on a dirty source.** A post-copy `ALTER … ADD FOREIGN KEY` on a big table hits PlanetScale's ~900-second statement wall, because InnoDB validates every child row against the parent, and on 153 million rows that validation cannot finish in time — the migration died in the constraints phase and a `--resume` loop re-hit the identical wall forever. sluice now adds the constraint **metadata-only** under `foreign_key_checks=0`: the child rows were just copied from a source where the constraint held, so re-validating them is redundant, and the metadata-only add returns in milliseconds (measured: 0.082 s on the 153-million-row table that had been walling at fifteen minutes). To keep the loud-on-dirty-source guarantee that skipping validation would otherwise break, the add is followed by a **bounded chunked orphan scan** — primary-key ranges each small enough to stay under the wall — that proves the child rows satisfy the constraint rather than assuming it; a clean source completes, and any orphan makes sluice drop the key and refuse with `SLUICE-E-FK-SOURCE-ORPHAN` (exit 3) naming the row. Both instant and loud. Arms only for an unfiltered migrate into a Vitess-flavor target; same-engine MySQL→MySQL and all Postgres targets are unaffected.

### Changed

**`backup stream` waits out a deploy-request index build instead of abandoning it after an hour.** A large PlanetScale index build routed to a deploy request runs asynchronously and can take hours on a throttled table; sluice waited only one hour, exited as though it had failed, and told the operator to delete a dev branch the in-progress deployment still depended on. The wait is now unbounded by default (`--planetscale-deploy-timeout`, `0` = indefinite), polls to a terminal state with progress and throttle reporting, fails fast on a genuinely terminal error, names an approval gate instead of looking hung, and keeps the branch on the still-deploying path. The approval/deployable wait keeps a one-hour cap even when the deploy wait is unbounded — unbounded is for machine work with visible progress, not for a human gate.

**`slot drop` speaks sluice's own naming convention.** `--slot-name` is a suffix that sluice prefixes with `sluice_`; `slot drop` took the literal name and never prefixed it, so dropping a slot by the suffix you created it with reported "not found." It still takes the literal name, but when the literal is absent and the `sluice_`-prefixed slot exists, it names it and gives the exact command — and surfaces the same as a warning under `--if-exists` while still exiting 0.

**`migrate --resume` skips create-tables for tables already complete.** Re-issuing `CREATE TABLE` for finished tables is pointless everywhere and dead-ends on a Safe-Migrations PlanetScale branch (the direct `CREATE` is refused, so the run never reached the index phase where the deploy-request fallback lives). Resume now skips the phase when every in-scope table is recorded complete.

### Fixed

**The forked-chain restore refusal carries its error code and exits 3.** It was the one shape of `SLUICE-E-BACKUP-MANIFEST-INVALID` that `restore` raised with exit 1 and no code — while `verify` on the same chain, and `restore` on the other two shapes of that refusal, exited 3 with the code — so a DR script keyed on the documented code or on exit 3 could not detect the fork. The seven lineage-walk refusals predated the exit-3 taxonomy and were never migrated; they now carry the code. The message is unchanged.

### Compatibility

Drop-in. No backup format change, no fingerprint epoch, no flag removals. One new error code, `SLUICE-E-FK-SOURCE-ORPHAN` (exit 3), raised only when a migrate's own source has a foreign-key-violating row on the metadata-only path — a case that previously could not complete at all. `SLUICE-E-CONSTRAINT-STATEMENT-TIME-LIMIT` is new for the constraints-phase wall cases the metadata path does not cover. The forked-chain restore refusal now exits 3 where it previously exited 1 — relevant only if a script branched on that exit 1.

### Who needs this

**Anyone migrating a large dataset with foreign keys into PlanetScale-MySQL.** Before this release, the copy and indexes could complete and the migration would still fail — permanently — at the first large foreign key. This is the release that closes that.

**Anyone relying on the deploy-request index fallback at scale.** A real 106 GB, four-index build takes hours; the previous one-hour wait meant the common outcome was a "failed" exit over a deploy that was progressing normally.

**Measured, for context:** across three runs of the same 122 GB load, the reparent-classifier fix (v0.104.7) took the copy from aborting at ~8 million rows to completing all 153 million; this release takes the *foreign-key* phase from "cannot complete" to 0.082 seconds. A separate finding worth knowing: raising PlanetScale's query timeout does **not** make a direct client-side `ALTER … ADD KEY` reliable on a large table. Even raised to its 3600-second maximum, the timeout is not enough — a four-index build on 153 million rows runs for the full hour and is then killed by the statement wall, indexes rolled back. Use `--upfront-indexes` or the deploy-request path for large index builds; the metadata-only path above for foreign keys.

> **Correction (2026-07-31, after publication):** the first published version of this paragraph said the direct `ALTER` was "killed by a routine vtgate restart (a `GOAWAY`)". That was wrong. A single observed `GOAWAY` on one run was a coincidental one-off — a re-run of the identical statement, with no maintenance in flight, ran the full 3600 seconds and was killed by the statement-time limit, not by a restart. The corrected reason above is the reproducible one; the recommendation is unchanged.

### Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.105.0
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:0.105.0`

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
