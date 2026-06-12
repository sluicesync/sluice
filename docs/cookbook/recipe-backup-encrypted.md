# Recipe — backup chain with at-rest encryption

Periodic full + incremental backups of a live database, encrypted at
rest with either a passphrase or AWS KMS.

## When to use this recipe

- You need durable, restorable copies of a database for disaster
  recovery, compliance, or moving data between disconnected
  environments (analytics warehouse fed from prod backups, etc.).
- You want backups encrypted at rest because the storage tier isn't
  inherently trusted (S3 bucket shared with other tenants, on-prem
  storage with weaker access controls than the source DB, etc.).
- You don't want to manage a separate backup daemon — sluice's backup
  surface is the same binary as the migrate/sync surface.

If you don't need encryption, leave off the `--encrypt` flags and the
recipe still works.

## The flow at a glance

1. **`sluice backup full`** — one full backup snapshot of the source.
2. **`sluice backup stream run`** — long-running process that emits
   incremental backups as changes land on the source. Rolls over into
   new segments on configurable cadence.
3. **`sluice backup verify`** — periodic verification that the chain
   on disk matches its manifests (and, with `--encrypt`, that the
   operator's envelope can still unwrap every chunk).
4. **`sluice restore`** — when you actually need to recover, restore
   the chain to a fresh target.

## Step 1: full backup

On a Postgres source, add `--chain-slot` to the full: it provisions the
persistent replication slot (named by `--slot-name`) as the snapshot
anchor and ensures the publication, so step 2's incrementals chain with
zero gap and no manual slot setup.

### Passphrase mode

```sh
sluice backup full \
    --source-driver postgres \
    --source ... \
    --output-dir /var/backups/myapp \
    --encrypt --encryption-passphrase 'pick-a-real-passphrase'
```

The chain root manifest records the Argon2id parameters used to derive
the KEK from the passphrase. Future incrementals and restores
re-derive against those recorded parameters — operators never need to
remember the salt, only the passphrase.

### AWS KMS mode

```sh
sluice backup full \
    --source-driver postgres \
    --source ... \
    --output-dir /var/backups/myapp \
    --encrypt --encrypt-kek-mode=kms --encrypt-kek-ref='arn:aws:kms:us-east-1:...:key/...'
```

The chain root records the KMS key ARN; sluice calls `Encrypt` /
`Decrypt` against the KMS endpoint at backup and restore time. KMS
mode and passphrase mode can't be mixed within a chain — sluice
refuses loudly if an incremental tries to extend a chain encrypted
with the other mode.

### Per-chunk vs per-chain mode

```sh
# Per-chain (default): every chunk wraps the same CEK.
--encrypt-mode=per-chain

# Per-chunk: every chunk wraps its own CEK.
--encrypt-mode=per-chunk
```

Per-chunk mode makes it possible to **rotate the operator passphrase
between incrementals**: each chunk's WrappedCEK is independent, so a
later incremental can land under a different envelope. sluice will
**loudly refuse** if you try to rotate the passphrase mid-chain in
per-chain mode (since later chunks couldn't be unwrapped by the new
envelope), and in per-chunk mode it probes the operator's envelope
against the parent chain's existing chunks at incremental start —
catching rotation typos at backup time rather than at restore time.

## Step 2: incremental stream

```sh
sluice backup stream run \
    --source-driver postgres \
    --source ... \
    --output-dir /var/backups/myapp \
    --encrypt --encryption-passphrase 'pick-a-real-passphrase' \
    --retain-rotate-at-chain-length 50 \
    --retain-rotate-on-age 24h \
    --rollover-window 5m
```

Operationally this is a long-running process — run it under systemd /
k8s. It tails the source's change stream and writes incremental
backups into the same store the full landed in. The rollover knobs
control segment cadence:

- `--retain-rotate-at-chain-length 50` — rotate into a new segment
  after 50 incrementals.
- `--retain-rotate-on-age 24h` — also rotate every 24 hours.
- `--rollover-window 5m` — group changes within a 5-minute window
  into a single incremental.

`sluice backup prune --keep-incrementals N` removes older incremental
segments while preserving the chain root's restorability — see the
per-feature docs.

## Step 3: verify periodically

```sh
sluice backup verify \
    --from-dir /var/backups/myapp \
    --encrypt --encryption-passphrase 'pick-a-real-passphrase'
```

Without the `--encrypt` flag, this is SHA-only — chunk hashes are
compared against the manifest's recorded hashes. With `--encrypt`,
sluice **also** probes each chunk's WrappedCEK against the operator's
envelope — catching the case where the operator rotated their
passphrase but didn't notice the chain stopped being restorable. The
loud failure is `unwrap chunk cek (passphrase rotated mid-chain?):
crypto: aes-gcm open: cipher: message authentication failed`.

Run this on whatever cadence your DR policy requires — daily is
common.

## Step 4: restore (when you actually need it)

```sh
sluice restore \
    --from-dir /var/backups/myapp \
    --target-driver postgres \
    --target ... \
    --encrypt --encryption-passphrase 'pick-a-real-passphrase'
```

The restore re-applies the chain from the root manifest forward.
Restore is **all-or-nothing** — if any chunk fails to unwrap, the
restore exits non-zero before any rows land on the target. There's no
silent partial restore.

For cross-engine restore (e.g. PG-source backup → MySQL target),
sluice refuses loudly when the source schema uses PG-specific shapes
the target can't represent (verbatim extension types, EXCLUDE
constraints) rather than silently dropping them.

## Common pitfalls

- **Lost the passphrase.** There's no recovery. sluice deliberately
  doesn't store a hint or recovery key — the operator's passphrase is
  the only thing that can decrypt the chain. Store it the same way
  you'd store a TLS private key.
- **Rotated the passphrase between full and incremental in per-chain
  mode.** sluice refuses loudly at incremental start. Switch to
  per-chunk mode if you need rotation across incrementals.
- **Backup store on the same disk as the source.** Don't do this.
  Backup hygiene includes "the backup survives loss of the source
  storage." Use a different physical / cloud / region tier.

## What's NOT in this recipe

- **Multi-store fan-out** (writing the same backup to multiple
  stores). Run multiple `backup stream run` processes against
  different destinations (`--output-dir` / `--target`).
- **Compaction of older segments.** See `sluice backup prune` and the
  `--smart-compaction` mode in the backup-chain docs.
- **The cloud-store backends** (S3, GCS, Azure Blob). The write-side
  `--target` and read-side `--from` URLs accept `s3://bucket/prefix`,
  `gs://bucket/prefix`, and `azblob://container/prefix` with the
  appropriate environment credentials.

## See also

- [`docs/architecture.md`](../architecture.md) — the backup-chain
  format, lineage manifests, and the segment / rotation model.
- The Phase 6 encryption ADRs in [`docs/dev/adr/`](../dev/adr/) — the
  KEK-mode dispatch, the recorded-not-sniffed codec policy, and the
  Bug 117 verify + ingestion probe story.
