# sluice v0.99.231

**The PG/MySQL emptied-data "anchor-forge" is now closed for real — completing and correcting v0.99.230. Restore and the broker no longer trust a schema anchor as proof of completeness at all.**

## Security

- **The PG/MySQL anchor-forge is closed for real (audit-2026-07-12, superseding v0.99.230's item 60).** v0.99.230 accepted a 0-chunk incremental when it carried a schema anchor at `EndPosition` *and* a non-empty schema delta, and described that as closing the forge signing-independently. A fresh audit found it did not: the schema delta and the anchor position are **both** outside every signing-independent cover (neither is in the `BackupID`, the schema hash, or the chunk AAD), so a store adversary emptying an unsigned window's change chunks could re-anchor its routine snapshot to `EndPosition` and append one no-op schema-delta entry (an `ALTER TABLE` to the current shape, which restore skips) to pass the gate — silently dropping the window's rows on restore and poisoning the CDC resume position. Trusting the anchor was a bar-raise from one forgeable field to two, not a closure.

  A ground-truth investigation on real Postgres and MySQL is decisive: no legitimate window ever presents a schema anchor at a position-bearing `EndPosition`. A DDL-only window emits its snapshot with an **empty** `EndPosition` (so the completeness check is skipped and the change is applied through the schema delta), and a data window reaches `EndPosition` through its change-chunk tail. So the "anchor at a position-bearing `EndPosition` with no chunks" shape has no legitimate producer — it is only ever a forgery. Restore and the live-apply broker therefore no longer trust a schema anchor as proof of completeness at all; completeness now rests solely on the signing-independent change-chunk tail, which an adversary deleting chunks cannot satisfy. This closes the PG/MySQL anchor-forge **and** subsumes the VStream shared-position case (Bug 184) at once, with **zero false-positive risk** on legitimate DDL-only restores (re-confirmed on both engines). `--require-signature` remains the belt-and-suspenders control for the whole unsigned manifest-edit class.

## Compatibility

- No backup-format change; no change to any legitimate backup or restore. This only tightens which unsigned, tampered manifests the restore/broker completeness check will refuse. Chains written by any prior version restore identically.

## Who needs this

Anyone running **unsigned** Postgres or MySQL backup chains where the object store is not fully trusted. If you upgraded to v0.99.230 for the item-60 fix, upgrade to v0.99.231: the v0.99.230 gate raised the cost of the forge but did not close it, and this release does.

---

Install / upgrade: see the [README](https://github.com/sluicesync/sluice#install). Verify downloads against `checksums.txt`.
