# sluice v0.99.230

**The last H-1 residual is closed: the PG/MySQL emptied-data "anchor-forge" is now refused signing-independently, backed by a real-database ground-truth investigation.**

## Security

- **The PG/MySQL SchemaHistory anchor-forge is closed signing-independently (audit item 60).** sluice's backup-completeness net accepts a 0-chunk incremental when a schema-history snapshot is anchored exactly at the recorded `EndPosition` — the legitimate resume-after-DDL case. On an **unsigned** Postgres/MySQL chain a store adversary could take an emptied-data window's routine first-touch snapshot — whose anchor position is covered by nothing signing-independent (not the `BackupID`, not the schema hash, not the chunk AAD) — and edit it to equal `EndPosition`, so the window's dropped events were silently accepted on restore or broker apply. Restore and the broker now trust an anchor at `EndPosition` only when the window **also carries a non-empty schema delta**, refusing the forge with `SLUICE-E-BACKUP-INCOMPLETE`.

  This is backed by a ground-truth investigation against real Postgres and MySQL: a schema snapshot anchors at `EndPosition` only for a real column-signature DDL (which the schema diff always records as a delta), while an emptied-data window's forged anchor has an empty delta — and a pure DDL-only window emits no snapshot at all (empty `EndPosition`, skipped). So the gate refuses the forge with **zero false-positive risk** on legitimate DDL-only restores. The investigation's assertions ship as permanent integration tests on both engines. `--require-signature` remains the belt-and-suspenders control for the whole unsigned manifest-edit class.

## Compatibility

- No format change; no change to any legitimate backup or restore. This only tightens which unsigned, tampered manifests the restore/broker completeness check will refuse.

## Who needs this

Anyone running **unsigned** Postgres or MySQL backup chains where the object store is not fully trusted: this closes the last emptied-data silent-loss vector that previously required `--sign` to cover, completing the audit-2026-07-11 H-1 series (v0.99.229 closed the VStream half; this closes the PG/MySQL half).

---

Install / upgrade: see the [README](https://github.com/sluicesync/sluice#install). Verify downloads against `checksums.txt`.
