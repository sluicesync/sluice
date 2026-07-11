# sluice v0.99.225

**A security fix from the v0.99.221→224 confirming audit: offline `restore` and `chain restore` now refuse a plaintext chunk spliced into an encrypted chain, closing a silent-injection gap where only the live broker was protected. On an encrypted-but-unsigned backup a store-write adversary could downgrade one chunk to forged plaintext and have its rows applied by offline restore. No behavior change for a legitimate backup.**

## Security

- **Offline restore now refuses a spliced plaintext chunk (audit HIGH).** The v0.99.222 broker hardening refuses a change chunk whose manifest entry has no encryption metadata (`Encryption == nil`) on an encrypted chain — a store adversary's downgrade that would otherwise open as cleartext. That guard was on the *broker* only; the two offline paths — `chain restore`'s change-chunk resolver and `restore`'s row-chunk resolver — opened such a chunk via the legacy plaintext codec. The mixed-mode check keys on *any* chunk in an incremental being encrypted, so a single plaintext chunk among encrypted siblings slipped through. On an encrypted-but-**unsigned** backup, an adversary with store-write access could set one chunk's `Encryption` to nil, write a forged plaintext chunk, fix its recorded SHA-256, and have its rows applied by offline `restore` / `chain restore` — silently, exit 0. This defeats the tamper protection encryption alone is meant to provide: an encrypted-but-unsigned backup catches chunk tamper at *decrypt* (via the GCM authentication tag), and downgrading a chunk to plaintext removes the decrypt step entirely. The broker refused it; the offline paths applied it.

  All three change-apply consumers — the broker and both offline restore paths — now share a single coded refusal (`SLUICE-E-BACKUP-CHUNK-AUTH-FAILED`), so the guard cannot drift between them again; a fix that lands in one apply path and not its siblings is precisely how this gap arose. The broker's own refusal, which previously shipped without its coded class, is now coded too.

## Compatibility

**No behavior change for a legitimate backup.** Every chunk of an encrypted chain is stamped with encryption metadata by the writer — full row chunks, incremental change chunks, rotation-born and resnapshot segments, in both per-chain and per-chunk mode — so a nil marker is only ever a splice. A genuinely-plaintext (unencrypted) backup carries no chain-encryption marker, so its plaintext chunks open exactly as before. The refused shape is one the writer never produces. Signing (`--sign` / `--sign-key` + `--require-signature`) remains the closure for the coarser whole-chain residuals (a whole-backup rollback or a whole-chain plaintext downgrade), which no per-chunk check can catch on an unsigned backup.

## Who needs this — action required

- **If you take encrypted backups without signing them, upgrade.** Offline restore now refuses a plaintext-chunk splice on an encrypted chain instead of applying attacker rows. This matches the protection the live `sync from-backup` broker already had. Sign your chains and restore with `--require-signature` for full manifest tamper-proofing.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.225 · **Container:** ghcr.io/sluicesync/sluice:0.99.225
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
