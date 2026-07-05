# sluice v0.99.184

**A single-fix release closing Bug 179, an un-restorable-backup defect on the encrypted-chain path: a chain whose segments disagreed on encryption mode (`per-chunk` vs `per-chain`) could build and `verify` cleanly yet fail loudly at restore. The fix makes the chain's mode authoritative for every segment — inherited when `--encrypt-mode` is omitted, refused loudly when an explicit flag conflicts — across incrementals, streams, and full-resume. No silent data loss existed (the failure was always loud at restore); this converts a verify-OK-but-un-restorable chain into an inherit-and-restore or refuse-at-build outcome.**

## Fixed

**Bug 179 — encrypted-chain segments now agree on encryption mode; a conflicting `--encrypt-mode` is refused at build (pre-existing, loud).** An encrypted backup chain must use one encryption mode for every segment, but the chain-extending code resolved it from two different sources: the chain-CEK derivation inherited the parent chain's mode, while the chunk-writing path read *this* invocation's `--encrypt-mode` (which defaults to `per-chain` when omitted). So a `per-chunk` chain extended by a mode-omitted `backup incremental` — or a `backup stream` rollover, or a crashed `backup full` resumed with a changed mode — wrote its chunks under the wrong mode. The result built and passed `backup verify`, but restore (which resolves a single chain mode and CEK from the root full) met those mis-moded chunks and aborted loudly with `chain CEK is unset` — only the full backup's rows landed. There was never any silent loss (the failure is loud, at restore) and it required an operator to omit or mismatch `--encrypt-mode` mid-chain, but a backup that verifies yet cannot restore is a trust defect worth closing.

The chain's mode is now authoritative for every segment. When `--encrypt-mode` is omitted, the segment inherits the chain's mode so the chunk-writer and the chain agree; when an explicit `--encrypt-mode` conflicts with the established chain, the backup is refused loudly at build time — the earliest possible point — rather than producing an un-restorable artifact. The fix covers all three chain-extending paths: `backup incremental`, `backup stream` rollovers, and `backup full` resume. It is pinned across the family (each orchestrator × parent per-chunk/per-chain × omitted-inherit / explicit-match / explicit-conflict-refuse), including an assertion that the chunk-writer, after inheriting, actually writes in the chain's mode — the specific split that produced the un-restorable chain.

## Compatibility

**No breaking changes for correctly-built chains.** A chain built with a consistent `--encrypt-mode` on every segment (the normal case) was always restorable and is byte-identical — same manifest, chunk, and on-disk formats; no format-version change. The only new refusals are for chains that mixed encryption modes, which were un-restorable anyway. One intentional tightening: a `per-chain` chain root extended by an explicit `--encrypt-mode=per-chunk` segment happened to restore before (restore keys off per-chunk metadata), and is now refused at build — enforcing the one-mode-per-chain invariant instead of relying on that fragile coincidence. Plaintext (unencrypted) backups are entirely unaffected.

## Who needs this — action required

- **Anyone using encrypted backup chains (`--encrypt` + `backup incremental` / `backup stream` / a resumed `backup full`).** Upgrade so a mode mismatch is caught at build instead of at restore. If you have an existing encrypted chain, a quick `sluice backup verify` plus a test `restore` confirms it is sound; a chain that restores today is unaffected by this change.
- **Everyone else: no action.** Plaintext backups, single (non-chained) encrypted backups with a consistent mode, and all migrate / sync paths are unchanged.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.184 · **Container:** ghcr.io/sluicesync/sluice:0.99.184
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
