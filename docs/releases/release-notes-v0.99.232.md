# sluice v0.99.232

**Pre-launch polish: a coded refusal for corrupt/tampered backup chunks, and a fleet-TUI glyph fix so the dashboard renders cleanly on every terminal.**

## Added

- **`SLUICE-E-BACKUP-CHUNK-CORRUPT` — a coded refusal for a backup chunk whose stored bytes don't match the SHA-256 recorded in the manifest** (at-rest corruption / bit-rot, or a tamper of the stored bytes). `restore`, broker replay, and `backup verify` already caught this and failed safe, but the refusal surfaced as a raw error without a machine-readable code — unlike its sibling `SLUICE-E-BACKUP-CHUNK-AUTH-FAILED` (the AES-GCM authenticated-decryption check). This byte-level check runs before decryption, so it fires on plaintext and encrypted chunks alike, and operators scripting against backup integrity now have a stable code to match. It was surfaced by real testing: a 75 GB backup-to-object-storage run flipped a byte in one uploaded chunk and confirmed the refusal. No behavior change beyond the code and Refusal class.

## Fixed

- **The fleet TUI (`sluice sync tui`) no longer shows a `?`-in-a-box in the RESTARTS column.** It used `↻` (U+21BB) for the column header and each row's prefix, which the default Windows terminal font can't render (a "tofu" glyph) even though it draws the `·` and `↑↓` used elsewhere in the same view. The header is now the plain word `RESTARTS` and cells show just the count — pure ASCII, so the dashboard renders correctly everywhere.

## Compatibility

- No format change. Backups written by any prior version restore identically. The new error code is additive.

## Who needs this

Everyone — this is polish ahead of wider sharing. If you script against `backup verify`/`restore` exit behavior, you can now match `SLUICE-E-BACKUP-CHUNK-CORRUPT`; if you run the fleet dashboard on Windows, the RESTARTS column now reads cleanly.

---

Install / upgrade: see the [README](https://github.com/sluicesync/sluice#install). Verify downloads against `checksums.txt`.
