# sluice v0.99.215

**Follow-up fix (Bug 182, MEDIUM, loud, zero data-loss) completing v0.99.214's M0.4 hardening: `backup verify` no longer panics on a tampered or bit-rotted manifest carrying a null structural element — it now refuses with the coded `SLUICE-E-BACKUP-SIGNATURE-INVALID`. Verify already failed closed (it never accepted the bad backup) and restore was unaffected, so there was no data-loss path; this turns a crash into the coded refusal it should always have been. Found by the v0.99.214 post-release regression cycle.**

## Fixed

- **`backup verify` rejects a manifest with a null table/chunk with a coded refusal instead of panicking (Bug 182).** v0.99.214's M0.4 hardening taught the signature-canonicalization pass to skip a null `*TableManifest` / row-chunk, but a SECOND verify traversal — the chunk-rehash loop in `VerifyBackupWith` — ran unguarded and dereferenced the null from a tampered `"tables":[null]` or `chunks:[null]` manifest, crashing `backup verify` with exit 2 and a Go stack trace instead of the coded refusal (on a *signed* chain, it panicked even after correctly logging the signature as INVALID). The verify was already failing CLOSED — it never accepted the tampered backup — and `restore` reconstructs tables from the lineage catalog and was unaffected, so there was no data-loss path; the defect was that a corrupt manifest crashed the tool where a coded exit is required. A structural-validation pass now rejects any manifest carrying a null table, row-chunk, or change-chunk up front with the coded `SLUICE-E-BACKUP-SIGNATURE-INVALID` (a Refusal-class exit 3), before any traversal can dereference it. A legitimate manifest never contains a null structural element (the signer emits none), so this fires only on corrupt or tampered input. Pinned by a family-matrix unit test and a wiring test that drives the real `VerifyBackupWith` and asserts the coded refusal rather than a panic.

## Compatibility

**No behavior change for any valid backup.** Every well-formed backup — signed or unsigned, encrypted or plaintext, any FormatVersion — verifies, restores, and behaves exactly as on v0.99.214. This only changes the outcome for a *structurally corrupt* manifest (a null table/chunk from tampering or bit-rot): `backup verify` now exits 3 with `SLUICE-E-BACKUP-SIGNATURE-INVALID` instead of crashing with exit 2 and a stack trace. In both the old and new code the corrupt backup is refused (never accepted); only the failure mode changed from a panic to a coded refusal. No migrate, sync, backup-write, or restore path is affected.

## Who needs this — action required

- **Nobody needs to act.** If you run `backup verify` as a scheduled integrity check and script against its exit code, a corrupt/tampered manifest now gives you the clean `SLUICE-E-BACKUP-SIGNATURE-INVALID` (exit 3) your other tamper cases already produce, instead of a crash — so a `case`/`if` on the coded exit now covers this shape too. Otherwise nothing changes.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.215 · **Container:** ghcr.io/sluicesync/sluice:0.99.215
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
