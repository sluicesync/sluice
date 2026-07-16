# sluice v0.99.228

> **Correction (2026-07-15):** the headline's "closing the last unsigned silent-loss vector signing-independently" was inaccurate. `ComputeBackupID` is a keyless hash, so an adversary flipping the flag coherently recomputes the ID — the claim overstated what an ID-recompute can guarantee without a signature. The genuinely signing-independent closures shipped afterwards: v0.99.229 re-derives commit-after-rows semantics from the source engine's own registered capability, and v0.99.231 removed the anchor-trust acceptance branch for all engines. Unsigned chains on v0.99.228 alone did not have this vector closed; signed chains (`--require-signature`) were always safe.

**Two backup-integrity hardenings from the security-audit carried tail: the manifest identity now covers the VStream position-semantics flag (closing the last unsigned silent-loss vector signing-independently), and `--strict-float` refuses a FLOAT re-read that silently patched nothing.**

## Fixed

- **`CDCPositionCommitsAfterRows` folded into the manifest `BackupID` (item 57).** Restore and the live-apply broker recompute-verify every manifest's recorded `BackupID` and refuse a mismatch with `SLUICE-E-BACKUP-MANIFEST-INVALID`, backstopping corruption or a lazy post-hoc edit of an identity-covered field before any data is applied. v0.99.228 extends that identity to cover `CDCPositionCommitsAfterRows` — the flag restore reads to decide whether a schema anchor at the recorded position proves the window's data landed (Bug 184) — so an unsigned flip of it can no longer pass unnoticed. Signed chains already covered this via the signature; the fold closes it for unsigned encrypted and plaintext chains too. Gated on a new per-manifest `FormatVersion 8` so existing backups keep their legacy id and still verify clean, and mixed-version chains stay coherent.

- **`--strict-float` refuses a 0-patched-of-N FLOAT re-read (item M0.1).** A total primary-key-match failure during the exact FLOAT re-read (every row silently retaining its VStream display-rounding) now refuses under `--strict-float` with `SLUICE-E-VSTREAM-FLOAT-LOSSY` instead of exiting 0. Partial misses stay tolerated. Defense-in-depth — the known trigger is already closed at plan time.

## Compatibility

- VStream (PlanetScale/Vitess) backups written by v0.99.228+ carry manifest `FormatVersion 8` and will be refused (loudly, at the preflight) by pre-v0.99.228 binaries. Non-VStream backups are unaffected. Existing backups restore normally on v0.99.228.

## Who needs this

Anyone running encrypted or plaintext **unsigned** backup chains from a **PlanetScale/Vitess** source — the fold closes an unsigned silent-loss vector you previously needed `--sign` to cover. Anyone relying on `--strict-float` for exact FLOAT archival from a VStream source gets a genuine refusal where a systemic key-divergence would previously have passed silently.

---

Install / upgrade: see the [README](https://github.com/sluicesync/sluice#install). Verify downloads against `checksums.txt`.
