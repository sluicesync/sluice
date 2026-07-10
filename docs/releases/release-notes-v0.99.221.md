# sluice v0.99.221

**An audit-cleanup release. A fresh confirming audit of the v0.99.218→220 delta found no correctness or security defect — the v0.99.219 SEC-1 change survived adversarial refutation on all five attack vectors and the silent-loss dive came back empty — but it surfaced a handful of low-severity fix-quality items in the just-shipped fixes. All are closed here, with no behavior change for a well-formed backup.**

## Changed

- **A wrong backup-encryption passphrase can no longer be mislabeled as chunk tamper (robustness).** v0.99.220 made a tampered encrypted chunk refuse with the coded `SLUICE-E-BACKUP-CHUNK-AUTH-FAILED`. The separation between "wrong key" and "tampered chunk" was correct, but it depended on *which call site* an error flowed through rather than the error's own content — the CEK-unwrap path runs the same AES-GCM primitive as chunk decryption, so a genuine wrong-passphrase error also carried the chunk-auth sentinel, and a future refactor could have relabeled a wrong key as a "tampered/spliced store" refusal. A new, deliberately **disjoint** `crypto.ErrCEKUnwrapFailed` sentinel now tags CEK-unwrap failures, so a wrong passphrase can never be coded as chunk tamper regardless of routing. A wrong passphrase stays a wrong passphrase.

- **The coded chunk-auth refusal now also covers chain compaction.** Tamper met during a `backup compact` (a fifth encrypted-chunk decrypt site) previously failed loudly but *without* the coded `SLUICE-E-BACKUP-CHUNK-AUTH-FAILED` the restore/replay paths emit. It is now coded too, so a script or agent watching exit codes gets the same machine-readable refusal from a compact that it gets from a restore.

- **The coded refusal gained an end-to-end row-chunk pin.** The exit-3 coded refusal was previously e2e-tested only on the change-chunk tamper path; it now has a full-backup row-chunk tamper test driving the real restore path, closing a "fix the class, cover each instance" coverage gap.

- **Three stale docs corrected.** A `--require-signature` refusal against an unsigned FormatVersion-7 backup no longer prints a misleading "(FormatVersion 6)"; ADR-0154 gained an inline marker so the superseded pre-SEC-1 "v7 is signed-only" wording isn't mistaken for current behavior; and the `backup-chain-operator` skill guide's claim that `prune` takes no encryption flags is corrected — a *signed* chain's `prune` renumbers link positions and must re-sign the surviving links, so it **does** require `--encrypt` + the chain's key (it is expected, not a usage error).

## Compatibility

**No behavior change for any backup.** Every backup verifies, restores, compacts, and prunes exactly as on v0.99.220. The changes are: a wrong-passphrase error now carries a distinct (more accurate) sentinel; tamper during `compact` now exits 3 with the coded refusal instead of an uncoded error; and the corrected docs. Nothing about a well-formed backup's format or handling changes.

## Who needs this — action required

- **Nobody needs to act.** If you script `backup compact` against exit codes, a tampered/corrupt chunk now gives you the same coded `SLUICE-E-BACKUP-CHUNK-AUTH-FAILED` (exit 3) that restore already produced. If you drive `prune` on a *signed* chain from the `backup-chain-operator` skill, the corrected guidance now tells you (accurately) to pass `--encrypt` + the key. Otherwise nothing changes.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.221 · **Container:** ghcr.io/sluicesync/sluice:0.99.221
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
