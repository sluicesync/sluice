# sluice v0.99.223

**A follow-up to v0.99.222 that closes the mirror of its unsigned-backup backstops. The v0.99.222 checks refuse a *truncated* change-chunk list (some entries deleted) and a *zeroed* row count; this release additionally refuses an *emptied* list (`[]`) and a table with no chunks but a recorded row count — the same silent-loss class, one shape further. Found by the v0.99.222 post-release regression cycle (Bug 183). No behavior change for a valid backup.**

## Security

- **Restore and the broker now refuse an emptied chunk list, not just a truncated one (Bug 183).** v0.99.222 hardened against *truncating* an unsigned encrypted backup's change-chunk list and *zeroing* a row count. A store-level adversary who instead **emptied** the list to `[]` slipped past both: (i) a full-manifest table with no chunks but a recorded row count restored silently *empty* (the empty-table early-return ran before the row-count check), and (ii) an incremental with an empty change-chunk list but an advanced `EndPosition` silently dropped every one of its events. Both are now refused loudly with `SLUICE-E-BACKUP-INCOMPLETE`, on both the offline `restore` and the live `sync from-backup` broker paths: a table with no chunks but a positive recorded row count is rejected, and a zero-chunk incremental whose `EndPosition` advances beyond `StartPosition` with no schema content (a pure-data window whose list was emptied) is rejected.

## Compatibility

**No behavior change for a valid backup.** The refused shape is one the writer never produces: an incremental's `EndPosition` is recorded as its last change's position, so a real zero-change window has an empty `EndPosition` (which the guard skips), a real data window carries its chunks, and a schema-only window carries its schema deltas. Every valid backup restores and syncs exactly as on v0.99.222; only a *tampered/emptied* manifest now refuses loudly instead of restoring short. As with the v0.99.222 backstops, a fully-consistent manifest edit remains signing-only, and signing (`--sign` / `--sign-key` + `--require-signature`) closes the whole class.

## Who needs this — action required

- **Nobody needs to act.** If you take encrypted backups without signing them, restore/sync now refuses an *emptied*-chunk-list backup loudly (in addition to the truncated/zeroed cases v0.99.222 already refused) rather than silently restoring short. Sign your chains and restore with `--require-signature` for full manifest tamper-proofing.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.223 · **Container:** ghcr.io/sluicesync/sluice:0.99.223
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
