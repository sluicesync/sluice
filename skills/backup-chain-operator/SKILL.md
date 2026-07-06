---
name: backup-chain-operator
description: Use to plan and operate an encrypted logical-backup chain (full → incremental → verify → compact → prune → restore-test). Drives `sluice backup *` and `sluice restore`. Gated — writes to the backup store; prune/compact without --dry-run irreversibly drop history and need human approval. Trigger when the user asks to back up / restore a database, build a backup chain, take an incremental backup, or test a restore.
---

# backup-chain-operator

Plan and operate a sluice backup chain safely. State-changing (writes to the backup store); the history-dropping commands are destructive and approval-gated.

## When to use
The user wants a logical backup, an incremental chained off a prior backup, an encrypted chain, or to restore/verify one. Also for chain hygiene (compact/prune) and for proving a chain is restorable.

## Inputs you need
- Source DSN + driver (backup reads the source; incrementals need a CDC-capable engine).
- Destination: a local dir (`--output-dir DIR`) OR a store URL (`--target s3://… | gs://… | azblob://… | file:///…`) — mutually exclusive. S3-compatible providers add `--backup-endpoint` / `--backup-region` / `--backup-path-style`.
- If encrypting: a passphrase source (`--encryption-passphrase-env VAR` or `--encryption-passphrase-file PATH` — prefer these over `--encryption-passphrase`, which leaks into shell history) or a KMS key (`--kms-key-arn` / `--gcp-kms-key-resource` / `--azure-key-vault-id`).

## Steps

1. **Take the full backup.** `sluice backup full --format json --source-driver <drv> --source "$SLUICE_SOURCE" --output-dir <DIR>` (or `--target <URL>`). To encrypt, add `--encrypt` + a key source and choose `--encrypt-mode per-chain` (one CEK/chain; one unwrap per restore) or `--encrypt-mode per-chunk` (one CEK/chunk; defense-in-depth). To chain incrementals off a Postgres full with zero gap, add `--chain-slot` (provisions the persistent slot named by `--slot-name`; costs source WAL retention until consumed). Tune with `--chunk-size`, `--compression none|gzip|zstd`, `--table-parallelism`, `--bulk-parallelism`.

2. **Take incrementals.** `sluice backup incremental --source-driver <drv> --source "$SLUICE_SOURCE" --output-dir <DIR> [--since <BACKUP-ID>] [--window 5m] [--max-changes N]`. `--since` empty chains off the most recent manifest. **OMIT `--encrypt-mode` on an incremental** so it INHERITS the chain's mode — passing an explicit mode that conflicts with the chain is refused at build (the v0.99.185 / Bug-180 rule; **one encryption mode per chain**). Supply the same `--encrypt` + key source the full used. **Note: `backup incremental` has NO `--format` flag** — unlike `backup full` (Step 1) and `restore` (Step 5), it is not a JSON-envelope verb, so passing `--format json` is a kong usage error (exit 80). Don't generalize the full's `--format json` onto it.

3. **(Optional) run a rolling stream.** `sluice backup stream run` produces rolling incrementals at a cadence; `sluice backup stream stop` commits the in-flight rollover and exits cleanly.

4. **Verify the chain.** `sluice backup verify --from-dir <DIR>` (or `--from <URL>`) re-checksums every chunk (SHA-256) and reports mismatches — read-only, safe to run anytime. Supply the encryption flags for an encrypted chain.

5. **Test-restore before trusting the chain.** `sluice restore --format json --from-dir <DIR> --target-driver <drv> --target "$SLUICE_TARGET"` (or `--from <URL>`), then run `fidelity-verify`. A chain you have never restored is unproven. Tune with `--table-parallelism` / `--bulk-parallelism` / `--apply-concurrency` (incremental-replay lanes).

6. **Chain hygiene — DESTRUCTIVE, gate these.** `sluice backup compact` merges adjacent segments; `sluice backup prune` drops the oldest incrementals to bound disk/restore time. Both **irreversibly rewrite the catalog / drop history** when run without `--dry-run`. ALWAYS run `--dry-run` first, show the plan, and get explicit human approval for that specific invocation before running for real. **`prune`/`compact` take NO encryption flags** — they operate on the catalog, not chunk bytes, so `--encrypt` on `prune` is a kong usage error (exit 80). (The encryption flags belong on `full`/`incremental`/`verify`/`restore`, which touch chunk bytes — don't carry them onto `prune`/`compact`.)

## What you return
- **Plan:** full/incremental cadence, destination, encryption mode (one per chain), retention intent.
- **Commands run + results:** backup IDs, chunk counts, `backup verify` outcome, and the **test-restore + fidelity-verify** result (the trust signal).
- **Destructive steps (if any):** the exact `prune`/`compact` invocation, its `--dry-run` output, flagged as needing human approval before the non-dry-run.
- **Slot note (PG `--chain-slot`):** the retained slot is named **`sluice_<name>`** — sluice prefixes the `--slot-name` value. Abandoning the chain means dropping it with `sluice slot drop --source-driver <drv> --source "$SLUICE_SOURCE" sluice_<name>` — the slot name is a **positional argument, not `--slot-name`** (passing `--slot-name` is exit 80). Note `slot drop` currently prompts `[y/N]` and aborts if unconfirmed, so it needs an interactive confirm (a known rough edge for non-interactive/agent use).

Never run `prune`/`compact` for real, or overwrite a prior backup with `--force-overwrite`, without explicit approval for that invocation.

## References (canonical — don't duplicate)
`docs/cookbook/recipe-backup-encrypted.md` · `docs/operator/encryption.md` · `docs/cookbook/recipe-broker-replay.md` (chain replay) · `AGENTS.md` (destructive-flags list, envelope) · `sluice backup --help` / `sluice backup full --help` / `sluice restore --help`.
