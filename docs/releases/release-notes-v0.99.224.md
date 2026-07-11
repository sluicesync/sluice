# sluice v0.99.224

**A follow-up to v0.99.223 that closes the case its own regression cycle caught (Bug 184) — and, via adversarial review of that fix, a Vitess/PlanetScale false-negative. v0.99.223 refuses an emptied change-chunk list only when the incremental carries no schema content; but every real data incremental carries a routine schema-history snapshot, so emptying a data window's chunks while leaving that snapshot slipped through and silently dropped every event. The completeness check now keys on the schema anchor's *position*, not its presence — and is engine-aware so it holds on VStream, where a snapshot can share a position with the rows it precedes. No behavior change for a valid backup.**

## Security

- **Emptying a data incremental's chunk list is now refused even when its routine schema snapshot is left behind (Bug 184).** v0.99.223 hardened against emptying an unsigned encrypted incremental's `change_chunks` to `[]`, but only for a window with *no* schema content — and every real data incremental carries a routine first-touch schema-history snapshot, so an adversary who emptied the chunks and left that snapshot was misclassified as a legitimate schema-only window and silently dropped every event (exit 0, `EndPosition` overstated). The completeness backstop no longer keys on the *presence* of schema history but on *position*: an incremental legitimately reaches `EndPosition` iff its replayed change-chunk tail ends at `EndPosition`, or a schema-history snapshot is anchored *exactly* at `EndPosition`. A genuine DDL-only window advances `EndPosition` to the schema snapshot's own WAL position (so its last anchor equals `EndPosition` and it still restores); a routine data-window snapshot is anchored *before* its rows, so an emptied-data window matches neither and is refused loudly with `SLUICE-E-BACKUP-INCOMPLETE`, on both the offline `restore` and the live `sync from-backup` broker paths.

- **The check is now engine-aware, closing a Vitess/PlanetScale (VStream) false-negative found by adversarial review.** VStream stamps CDC positions per-transaction-commit *after* the rows the commit covers (the VGTID follows its rows), so a schema snapshot and the row changes in the same transaction share one position. That means an emptied-data VStream window whose final transaction first-touched a table would leave a snapshot anchored *at* `EndPosition` and pass a position-only check. A new engine capability, `CDCPositionCommitsAfterRows` (declared for the PlanetScale and Vitess VStream flavors), is recorded on each incremental manifest at backup time; when set, restore and the broker do not trust a schema anchor at `EndPosition` as proof the window's data was applied — only an actually-replayed change-chunk tail counts. Postgres and MySQL-binlog, whose schema anchor strictly precedes its rows, keep trusting the anchor and restore legitimate DDL-only windows unchanged.

## Compatibility

**No behavior change for a valid backup.** The refused shapes are ones the writer never produces: a real data window carries its chunks (the replay reaches `EndPosition`), a genuine DDL-only window's schema snapshot is anchored at `EndPosition`, and a real no-op window does not advance `EndPosition`. Only a *tampered/emptied* manifest now refuses loudly. Manifests written before this release, and Postgres / MySQL-binlog manifests, are unaffected. As with the v0.99.222–223 backstops, a fully-coherent unsigned manifest edit remains signing-only; signing (`--sign` / `--sign-key` + `--require-signature`) closes the whole class.

## Who needs this — action required

- **Nobody needs to act.** If you take encrypted backups without signing them, restore/sync now refuses an emptied-chunk-list incremental — including the routine-schema-snapshot and Vitess/PlanetScale variants — loudly instead of silently restoring short. Sign your chains and restore with `--require-signature` for full manifest tamper-proofing.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.224 · **Container:** ghcr.io/sluicesync/sluice:0.99.224
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
