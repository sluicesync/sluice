# sluice v0.99.218

**Two small fix-quality follow-ups surfaced by a fresh confirming audit of the v0.99.213→217 remediation — both loud-either-way, zero data-loss, no behavior change for a well-formed backup. `sluice restore` now refuses a tampered manifest with a null structural element with the coded error instead of panicking (completing the v0.99.215 Bug-182 fix on the restore path too), and the batched exact-FLOAT repair no longer risks exceeding the database's bind-parameter limit on an unusually wide FLOAT table.**

## Fixed

- **`restore` refuses a null-structural-element manifest with the coded error, not a panic (Bug 182, restore-path half).** v0.99.215 taught `backup verify` to reject a tampered/bit-rotted manifest carrying a `"tables":[null]` or `chunks:[null]` with `SLUICE-E-BACKUP-SIGNATURE-INVALID`, but the guard was not on the restore path — so a hand-tampered UNSIGNED manifest fed to `restore` still crashed with a nil-pointer Go stack trace instead of the coded refusal (a SIGNED manifest was already caught, since a null chunk shifts the recorded count into a signature mismatch). The structural-validation pass now runs up front in both the single-manifest and chain restore paths, before any chunk traversal. Restore still fails closed in both the old and new code — it never restored the tampered backup — this only turns the crash into the coded refusal.

- **Batched exact-FLOAT repair caps its batch by column count (wide-table bind-parameter safety).** v0.99.217's batched repair binds `(primary-key + FLOAT) × batch` parameters per `UPDATE`, and both Postgres and MySQL cap a statement at 65,535 bind parameters — so a table with roughly 130+ single-precision FLOAT columns at the fixed 500-row batch could have overflowed the ceiling and failed the repair. The batch size is now derived from the column count (kept under a 60,000-parameter budget): a very wide table transparently uses a smaller batch, a normal table keeps the full 500. No value or behavior change — only the batch granularity for an unusually wide FLOAT table.

## Documentation

- **FormatVersion 7 is now documented** in `docs/backup-format-versioning.md` — the signed-encrypted manifest whose row chunks bind their parent table into the AES-GCM AAD (shipped v0.99.214, ADR-0154 SEC-F1). The doc previously stopped at FormatVersion 6.

## Compatibility

**No behavior change for any well-formed backup.** Every valid backup verifies, restores, and behaves exactly as on v0.99.217. The restore change only affects a *structurally corrupt* manifest (a null table/chunk from tampering or bit-rot): `restore` now exits with the coded refusal instead of crashing — in both cases the corrupt backup is refused, never restored. The FLOAT-repair change only affects the *batch granularity* on a very wide (~130+ single-precision-FLOAT-column) table; repaired values are unchanged.

## Who needs this — action required

- **Nobody needs to act.** If you script `restore` against its exit code, a corrupt/tampered manifest now gives you the clean `SLUICE-E-BACKUP-SIGNATURE-INVALID` (exit 3) that `verify` already produced, instead of a crash. If you run the exact-FLOAT repair on an exceptionally wide FLOAT table, it can no longer hit the bind-parameter ceiling. Otherwise nothing changes.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.218 · **Container:** ghcr.io/sluicesync/sluice:0.99.218
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
