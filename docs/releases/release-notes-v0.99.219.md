# sluice v0.99.219

**A single security follow-up surfaced by a fresh confirming audit of the v0.99.213→217 remediation: unsigned encrypted backups now bind every row chunk to its parent table, extending the v0.99.214 SEC-F1 chunk-swap defense to the one class it hadn't reached. No behavior change for any well-formed backup — every valid backup verifies, restores, and behaves exactly as on v0.99.218.**

## Security

- **Unsigned encrypted backups now table-bind their row chunks (SEC-1 — completes the SEC-F1 chunk-swap defense for the unsigned-encrypted class).** v0.99.214 (SEC-F1) bound each encrypted row chunk's AES-GCM AAD to its parent `(schema, table)`, so a store-write adversary who swaps the row-chunk lists of two tables with the same column set — same-schema shards, multi-tenant clones, an `orders_2023`/`orders_2024` pair — fails the GCM tag instead of decrypting cleanly into the wrong table. But the backup **FormatVersion 7** stamp that gates that binding was applied only when signing was requested, so an *unsigned* encrypted backup stayed on FormatVersion 5 with the pre-SEC-F1 `(identity, path)` AAD — leaving the same-column chunk-swap open for backups that are encrypted but not signed. The decrypt-layer binding was always signing-agnostic (AES-GCM enforces the AAD whether or not a manifest signature exists); it simply never reached unsigned backups because the *stamp* was gated on signing. A fresh encrypted full is now stamped FormatVersion 7 whether or not it is signed, so its row chunks are table-bound in every encrypted backup. Every row-chunk-writing path — the initial `backup`, a `backup stream` rotation-born segment full, and a reactive re-snapshot — funnels through the same full-backup setup, so streaming encrypted backups are covered too; incremental change chunks bind a replay ordinal (not a table) and are unchanged. A resumed pre-v7 encrypted chain keeps its prior FormatVersion so its already-written unbound chunks still decrypt, and an unsigned FormatVersion 7 manifest presented to a pre-v7 binary refuses loudly at the version gate exactly as a signed one does. The FormatVersion never asserts a signature — signedness is decided by the detached `.sig` artifact's presence, never by the version — so the now-dead `IsSignedFormat` version-threshold helper (zero production callers) was removed rather than left to imply an unsigned v7 backup is signed.

## Compatibility

**No behavior change for any well-formed backup.** Every valid backup — signed or unsigned, encrypted or plaintext — verifies, restores, and behaves exactly as on v0.99.218. The only observable difference is that a **freshly written unsigned encrypted backup** is stamped FormatVersion 7 (was 5); as with every prior FormatVersion bump this is proportional and fail-closed — an older sluice binary that predates FormatVersion 7 refuses such a manifest loudly at the preflight with an "upgrade sluice" version gap, never a silent mis-decrypt. Existing v5/v6 encrypted chains are untouched: resuming one keeps its prior FormatVersion, and restoring one is byte-for-byte unchanged. Plaintext backups keep their schema-derived version.

## Who needs this — action required

- **Anyone taking encrypted backups without signing them** (`--encrypt` without `--sign`/`--sign-key`) gets the parent-table chunk-swap defense automatically on the next fresh backup — no flag, no migration. If you also sign your encrypted backups, you already had this defense since v0.99.214; nothing changes for you.
- **One operational note:** a fresh unsigned encrypted backup written by v0.99.219 is FormatVersion 7, so restore it with v0.99.219 or newer. A pre-v7 binary refuses it loudly (it does not mis-restore). Roll your restore hosts forward alongside your backup hosts, as with any FormatVersion bump.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.219 · **Container:** ghcr.io/sluicesync/sluice:0.99.219
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
