# sluice v0.99.229

**Audit follow-up: the unsigned VStream flag-flip is now genuinely closed signing-independently (correcting a v0.99.228 overstatement), `backup verify --encrypt` no longer false-GREENs a plaintext downgrade, and the `--strict-float` net stops being silent on the default posture.**

## Security

- **The unsigned VStream flag-flip is closed signing-independently — correcting the v0.99.228 claim (audit H-1).** v0.99.228 folded `CDCPositionCommitsAfterRows` into the manifest `BackupID` and described that as closing the last signing-independent silent-loss vector. It did not: `BackupID` is a keyless public hash, so a store adversary who flips the flag and recomputes the id passes the verify clean (and could also blank an incremental's id, or downgrade `FormatVersion` below 8, to escape the fold). Restore and the live-apply broker now re-derive whether a source commits its CDC positions after its rows from the **source engine's own registered capability** — which no manifest edit can influence, since `SourceEngine` is itself `BackupID`-covered and, on encrypted chains, bound into the AES-GCM AAD — so an unsigned flip on any registered VStream source is caught regardless of the manifest's `FormatVersion` or signedness, and the emptied-data window it would re-open (Bug 184) is refused with `SLUICE-E-BACKUP-INCOMPLETE`. An incremental carrying an empty `BackupID` (never writer-legitimate) is now refused too. The sibling residual — forging a Postgres/MySQL schema-history anchor position, whose field is outside every signing-independent cover — remains closed by signing (`--require-signature`), deferred as a tracked follow-up pending a false-positive-free fix.

- **`backup verify --encrypt` no longer returns a false GREEN on a whole-chain plaintext downgrade (audit M-1).** v0.99.226 taught the restore and broker apply paths to refuse an encryption key supplied against a chain that records no encryption metadata (a store adversary stripping the chain's marker and forging plaintext chunks); `backup verify` did not get the same guard, so it reported the exact downgrade restore refuses as VALID — a false integrity attestation. `verify` now refuses it too (`SLUICE-E-BACKUP-CHUNK-AUTH-FAILED`).

## Fixed

- **A systemic `--strict-float` FLOAT re-read failure is no longer silent under the default posture (audit M-2).** v0.99.228's zero-patched tripwire only fired under `--strict-float`; under the default posture the same systemic primary-key-rendering divergence archived every single-precision `FLOAT` display-rounded with no signal, while the sibling rounded-fallbacks warn loudly. The default posture now emits a loud WARN naming the table, and a table whose exact re-read found rows but whose COPY streamed none WARNs in both postures (never refuses — a table legitimately empty at the snapshot position and filled during the window must not be false-refused).

- **Finished the compile-time interface-pin sweep and coded two more value-refusals (audit M-3).** Pinned the runtime-dispatched optional surfaces still unpinned after v0.99.227 — most importantly the index-verification net, the driver-host-mismatch preflight, and change-log pruning (an unpinned drift there would let the trigger-CDC change-log grow unbounded) — so a method-set drift is a build error, not a silent runtime downgrade. The Postgres/MySQL "infinity / BC timestamp has no representable target" refusals are now the registered `SLUICE-E-VALUE-UNREPRESENTABLE` coded refusal.

## Compatibility

- No format change. `FormatVersion 8` (introduced v0.99.228 for VStream backups) is unchanged; this release only strengthens how restore *reads* the position-semantics signal. All existing backups restore exactly as before.

## Who needs this

Anyone running **unsigned** backup chains from a **PlanetScale/Vitess** source: the flag-flip vector v0.99.228 claimed to close is now actually closed signing-independently. Anyone who relies on `backup verify --encrypt` as an integrity gate: it no longer attests a downgraded chain as valid. Anyone using `--strict-float` or the default FLOAT archival from a VStream source now gets a loud signal where a systemic key-divergence would previously have been silent.

---

Install / upgrade: see the [README](https://github.com/sluicesync/sluice#install). Verify downloads against `checksums.txt`.
