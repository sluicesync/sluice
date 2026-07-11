# sluice v0.99.222

**A security-hardening release for the backup broker and encrypted incremental chains, from a dedicated change-chunk/broker crypto audit. It fixes a confirmed silent-loss on the `sync from-backup` live-apply path (a mid-incremental chunk failure could make a restart skip the incremental and drop its un-applied tail), brings the broker's chain/manifest verification up to parity with offline `restore`, and adds a signing-independent backstop against tail-truncation of unsigned encrypted incrementals. No behavior change for a well-formed backup — every valid backup verifies, restores, and syncs exactly as before.**

## Fixed

- **The `sync from-backup` broker no longer silently loses an incremental's un-applied tail on a mid-apply chunk failure (BRK-1).** The broker recorded its resume checkpoint per *incremental* but commits per *chunk/batch*. So if a later chunk of a multi-chunk incremental failed — a tampered or corrupt chunk, a dropped blob, or a transient fetch error — *after* an earlier chunk's changes had already committed, the broker persisted the whole incremental's position and a restart **skipped that incremental entirely**, permanently and silently dropping its un-applied changes. The broker now streams each incremental at the *parent* (last-fully-applied) position and advances to the incremental's own position only after every one of its chunks streams cleanly, so a mid-incremental failure re-applies the whole incremental on restart (idempotent) instead of skipping it. Reproduced end-to-end on both the serial and concurrent apply paths; pinned with a regression test.

## Security

- **The broker now verifies signed chains and refuses tampered ones — parity with offline `restore` (BRK-2/3/4).** The broker previously did no manifest-signature verification and **silently ignored `--verify-key` / `--require-signature`**, so a signed chain that offline `restore` would refuse (a truncated change-list, a dropped-newest link, an invalid/rolled-back signature) was applied on the live path without complaint. It also accepted a **plaintext chunk spliced into an encrypted chain** (applying attacker-supplied changes), and would crash on a null-structural-element manifest rather than refuse. The broker now runs the same signature, mixed-mode-encryption, schema-hash, and structural-validation gates `restore` runs — via shared, exported wrappers so the two paths can't drift — with the verify flags threaded through from the CLI.

- **A signing-independent backstop catches tail-truncation of an unsigned encrypted incremental (F1), and the row-count check can no longer be turned off by a zeroed count (F3).** For an *unsigned* encrypted backup, a store-level adversary could delete the tail of an incremental's change-chunk list — the survivors still decrypt cleanly (their AAD ordinals are unchanged) — and `restore`/broker would apply fewer changes, exit 0, and leave the manifest's `EndPosition` overstating the data, poisoning a later CDC resume so the dropped events are never re-streamed. Both replay paths now assert the applied stream **reaches** the recorded `EndPosition` and refuse loudly (`SLUICE-E-BACKUP-INCOMPLETE`) on a shortfall — no signature required. Separately, the layer-2 row-count check was guarded by `RowCount > 0`, so zeroing a table's recorded count (and dropping its chunks) *disabled* the check and silently restored the table empty; it now refuses a zeroed count that still decodes rows. A fully-consistent two-sided manifest rewrite remains signing-only (now documented); signing plus `--require-signature` closes the whole class.

## Compatibility

**No behavior change for any well-formed backup.** Every valid backup — signed or unsigned, encrypted or plaintext — verifies, restores, and syncs exactly as on v0.99.221. The changes only affect *failure* handling: a mid-incremental broker failure now re-applies instead of skipping; the broker now verifies signed chains and refuses tampered/mixed-mode ones (if you pass `--verify-key`/`--require-signature` to `sync from-backup`, they are now honored — previously ignored); and a truncated or zeroed-row-count unsigned backup now refuses loudly instead of restoring short. There is one new coded error, `SLUICE-E-BACKUP-INCOMPLETE` (a Refusal, exit 3).

## Who needs this — action required

- **Anyone running `sync from-backup` (the broker) should upgrade** — it closes a silent-loss on a mid-incremental failure and, if you sign your chains, actually verifies them now (it previously ignored your verify flags on this path). If you pass `--verify-key` / `--require-signature` to `sync from-backup`, confirm your chain is signed and the key is correct, since those flags now take effect.
- **If you take encrypted backups without signing them**, restore/sync now refuses a truncated or zeroed-count backup loudly (`SLUICE-E-BACKUP-INCOMPLETE`) instead of silently restoring short. For full tamper-proofing of the manifest (including a fully-consistent rewrite), sign the chain (`--sign` / `--sign-key`) and restore with `--require-signature`.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.222 · **Container:** ghcr.io/sluicesync/sluice:0.99.222
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
