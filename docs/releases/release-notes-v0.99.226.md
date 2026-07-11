# sluice v0.99.226

**Follow-up hardening from the v0.99.221→224 confirming audit, on top of the v0.99.225 SEC-MIRROR fix: `backup verify` now flags the plaintext-chunk splice it previously reported green; supplying an encryption key against a backup that claims plaintext is now refused (a whole-chain downgrade signal); and a writer-side backstop converts a CDC-reader soundness property the Bug 184 completeness net relies on into a checked invariant. No behavior change for a legitimate backup.**

## Security

- **`backup verify` flags a spliced plaintext chunk (previously reported green).** v0.99.225 taught `restore` and the broker to refuse a chunk with no encryption metadata on an encrypted chain (a store adversary's downgrade). `backup verify` only rehashed bytes and ran a decrypt-probe that no-ops on a plaintext chunk, so it reported that tamper as VALID — a false all-clear that only surfaced later at restore. `verify` now flags a plaintext chunk on an encrypted chain (row and change chunks) as a failure, so it catches what restore refuses.

- **A key supplied against a plaintext-claiming backup is refused.** On an unsigned chain a store adversary can strip the chain's encryption marker and forge every chunk as plaintext — a whole-chain encrypted→plaintext downgrade. An operator restoring it would pass `--encrypt`, expecting encryption, but the key was silently ignored (the encryption preflight early-returned on a plaintext-claiming manifest) and the forged plaintext was applied. Restore, chain-restore, and the broker now refuse when a key is supplied but the chain records no encryption metadata (`SLUICE-E-BACKUP-CHUNK-AUTH-FAILED`) — "you gave me a key but this backup says it is unencrypted" is the loud signal that catches the downgrade.

## Fixed

- **A writer-side backstop makes a CDC-reader soundness property a checked invariant.** The Bug 184 completeness net tells a genuine schema-only (DDL) incremental from an emptied-data window by whether a schema snapshot is anchored exactly at the window's `EndPosition`. That is sound only because, on engines whose CDC positions do not commit after their rows (Postgres, MySQL-binlog), a schema anchor strictly precedes the rows it introduces — so a data-bearing window's `EndPosition` (its last row) never coincides with a schema anchor. The incremental writers now assert this when finalizing a manifest and fail the backup loudly if a future reader change violated it, rather than persist a manifest whose completeness check would be unsound. VStream engines legitimately co-locate a snapshot with its transaction's rows (which is why they carry the `CDCPositionCommitsAfterRows` marker and restore distrusts their anchors), so the assertion is scoped to non-VStream engines.

## Compatibility

**No behavior change for a legitimate backup.** A genuinely-plaintext backup restores without a key exactly as before; only supplying a key against a plaintext-claiming chain now refuses (remove `--encrypt`, or the chain was downgraded — sign it to make that tamper-evident). Every chunk of an encrypted chain is stamped with encryption metadata by the writer, so the `verify` and restore refusals fire only on a genuine splice. The writer invariant asserts a property the shipping readers already satisfy, so it never fires on a real backup.

## Who needs this — action required

- **Nobody needs to act.** If you take encrypted backups without signing them, `backup verify` now catches a plaintext-chunk splice (rather than passing it and refusing only at restore), and restore refuses a whole-chain plaintext downgrade when you supply a key. Sign your chains and restore with `--require-signature` for full manifest tamper-proofing.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.226 · **Container:** ghcr.io/sluicesync/sluice:0.99.226
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
