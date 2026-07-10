# sluice v0.99.220

**A small consistency follow-up to v0.99.219, surfaced by its own regression cycle: a tampered or corrupt chunk in an encrypted-but-unsigned backup now fails restore with the coded `SLUICE-E-BACKUP-CHUNK-AUTH-FAILED` refusal (exit 3) — the machine-readable twin of the signed path's `SLUICE-E-BACKUP-SIGNATURE-INVALID` — instead of a bare exit-1 crypto error. No behavior change: the refusal was already loud and fail-closed; only its exit code and error code are now unified with the signed path.**

## Changed

- **Encrypted-but-unsigned chunk tamper now exits with a coded refusal.** An unsigned encrypted backup carries no manifest signature, so a swapped, spliced, reordered, or bit-rotted chunk is caught at *decrypt* by the AES-GCM AAD binding — the ADR-0152 chunk-position binding plus the v0.99.214/v0.99.219 SEC-F1/SEC-1 parent-table binding — rather than at `backup verify` time. That refusal was already loud and fail-closed (no wrong data was ever restored), but it surfaced as an uncoded exit-1 crypto error (`crypto: aes-gcm open: cipher: message authentication failed …`), whereas the equivalent tamper on a *signed* backup gives the coded exit-3 `SLUICE-E-BACKUP-SIGNATURE-INVALID`. The two are now unified: any encrypted chunk that fails its authenticated-decryption check on restore, chain-replay, or broker apply exits with the coded `SLUICE-E-BACKUP-CHUNK-AUTH-FAILED` (a Refusal-class exit 3), so an agent or script scripting the exit code gets the same stable signal whether or not the chain was signed. The coding does not conflate tamper with a wrong key — by the time a chunk decrypts, the chain key has already unwrapped (its wrap is itself authenticated), so an auth failure here is always tamper/corruption, and a wrong passphrase (caught earlier at the key unwrap) keeps its own error. A SHA-256 mismatch, an I/O error, or a too-short ciphertext are unchanged.

## Compatibility

**No behavior change.** Every well-formed backup verifies and restores exactly as on v0.99.219. The only observable difference is the *exit code and error code* of a restore that hits a tampered/corrupt chunk in an encrypted-but-unsigned backup: it now exits 3 with `SLUICE-E-BACKUP-CHUNK-AUTH-FAILED` instead of exiting 1 with an uncoded crypto error. In both cases the restore refuses and no wrong data lands. Scripts that check only for a non-zero exit are unaffected; scripts that branch on the specific exit code now get the Refusal class (3) that the signed path already produced.

## Who needs this — action required

- **Nobody needs to act.** If you script `restore`/`broker` against exit codes or parse error codes for alerting, a tampered/corrupt chunk in an encrypted-but-unsigned backup now gives you the coded `SLUICE-E-BACKUP-CHUNK-AUTH-FAILED` (exit 3) that the signed path already produced — consistent tamper signalling across signed and unsigned encrypted backups. If you want tamper caught at `backup verify` time (before a restore is attempted), sign the chain (`--sign` / `--sign-key`): an unsigned encrypted backup is protected only at decrypt.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.220 · **Container:** ghcr.io/sluicesync/sluice:0.99.220
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
