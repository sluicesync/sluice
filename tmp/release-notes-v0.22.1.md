# sluice v0.22.1

Single-bug patch from the v0.22.0 cycle. v0.22.0 shipped Phase 6.1 client-side passphrase encryption and the load-bearing pieces — encrypted full backup + restore, ciphertext-on-disk verification, wrong-passphrase / missing-key refusals, per-chunk mode, plaintext backward-compat — all worked cleanly. But the write-side envelope builder minted a fresh Argon2id salt on every call, so any **extension of an existing encrypted chain** (`backup incremental --encrypt`, `backup stream run --encrypt`, or resuming a partial encrypted full) crashed at startup with `aes-gcm open: cipher: message authentication failed`. The "encrypted continuous backup" promise was half-fulfilled; this fixes the other half. Fix is local to the encryption-builder + chain-alignment paths; no schema or CLI changes.

## Fixed

- **Bug 43 — encrypted-chain extension fails with auth-tag mismatch.** `cmd/sluice/backup.go`'s write-side `buildBackupEncryption` derived its KEK against a fresh random Argon2id salt every call. Cold-start `backup full --encrypt` was fine (the fresh salt becomes the chain's salt). But `backup incremental --encrypt` / `backup stream run --encrypt` extending an existing encrypted chain ended up with an envelope whose KEK was tied to the wrong salt, so `Envelope.UnwrapCEK(parentChain.WrappedCEK)` failed with `aes-gcm open: cipher: message authentication failed (wrong key or tampered ciphertext)`. The read-side path (`buildReadEnvelope`) already loaded `rootManifest.ChainEncryption.Argon2id` and re-derived the KEK against the chain's recorded salt — that's why `restore` and `backup verify` worked on encrypted chains while `backup incremental` and `backup stream run` did not. Fix mirrors the read-side pattern on the write side: `pipeline.BackupEncryption` gains a `RebuildForChain` hook the CLI populates with a closure over the operator's passphrase; the orchestrator's chain-alignment paths read the parent chain's recorded `Argon2id` params and call `RebuildForChain` with them before any CEK unwrap. Cold-start backups are unaffected — `RebuildForChain` is a no-op when the parent chain has no recorded params.

- **Encrypted chain restore via `sluice restore` silently dropped the envelope.** Latent gap from Phase 6.1: `Restore.Run` dispatches to `ChainRestore` when the destination contains incrementals, but the `ChainRestore{}` literal it built omitted the `Envelope` field. Encrypted chains restored via the `sluice restore` CLI surface refused with the missing-key error even when the operator passed `--encrypt --encryption-passphrase-...`. Single-full restores were unaffected (the chain-detection branch was skipped). Surfaced and pinned alongside the Bug 43 integration test; fix is a one-line propagation.

## Compatibility

- **No format changes.** Manifest schema, control-table schema, change-chunk format, CLI surface — all unchanged.
- **No CLI breaking changes.** All existing `sluice` subcommands keep their flag surfaces verbatim.
- **Drop-in upgrade from v0.22.0.** Any v0.22.0 encrypted full (the part that did work) is fully extensible under v0.22.1 — the chain's recorded `Argon2id` params let v0.22.1 derive the right KEK for every subsequent incremental / stream rollover. No need to re-take the full.
- **Operators who already hit Bug 43 on v0.22.0** can re-run their `backup incremental --encrypt` / `backup stream run --encrypt` against the same destination with v0.22.1; the orchestrator now derives its envelope's KEK against the chain's recorded salt, so the unwrap succeeds and the chain extends cleanly.
- **No new dependencies.** Pure orchestration plumbing inside the existing `internal/crypto` + `internal/pipeline` surfaces.

## Who needs this

- **Anyone running `backup incremental --encrypt` or `backup stream run --encrypt` against an existing encrypted chain on v0.22.0** — Bug 43 affects you. v0.22.0's behaviour means your encrypted continuous-backup workflow can't extend the chain (the full ran fine; the extension refuses); v0.22.1 closes that gap end-to-end.
- **Anyone restoring an encrypted chain that has incrementals (not just a single full) via `sluice restore --encrypt`** — the latent envelope-discard bug also affects you on v0.22.0. Standalone encrypted fulls restored fine; chains with incrementals refused with the missing-key error.
- **v0.22.0 cold-start encrypted-full operators with no chain extensions yet** — no impact, but upgrade anyway: v0.22.1 unblocks the next incremental / stream against your chain.

## What's next

Phase 6.2 (AWS KMS) is the next backup-track chunk. The `EnvelopeEncryption` interface that Phase 6.1 plugged into is mode-agnostic; `RebuildForChain` is opt-in (passphrase-mode-only by design — KMS unwrap doesn't depend on a chain-recorded salt). Phase 6.1's "encrypted continuous backup" promise is now fully delivered with v0.22.1.
