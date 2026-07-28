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
   on disk matches its manifests, and — with `--encrypt` + the chain's
   key material — that every encrypted chunk still opens under the same
   key and binding `restore` would use.
4. **`sluice restore`** — when you actually need to recover, restore
   the chain to a fresh target.

## Which source engines

The backup path is engine-neutral: it drives the source through the same
`SchemaReader` / `RowReader` interfaces the migrate path uses, so a
**full** logical backup (`sluice backup full`) works for any registered
source — including a **SQLite file** (`--source-driver sqlite`). The
chunks land in the same manifest/compression/encryption format regardless
of source engine, and `restore` replays them into any target.

Incremental chains (`sluice backup stream run`) additionally need a
**CDC-capable** source: Postgres logical replication, MySQL binlog,
Vitess VStream, or a trigger-CDC engine (`postgres-trigger`,
`sqlite-trigger`, `d1-trigger`). The base `sqlite` engine declares
`CDC: None`, so a plain SQLite file supports full backups but not the
incremental stream — point `--source-driver sqlite-trigger` at it (after
`sluice trigger setup`) if you want a continuous backup chain from SQLite.

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
    --encrypt --kms-key-arn='arn:aws:kms:us-east-1:...:key/...'
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
    --retain-rotate-at 24h \
    --rollover-window 5m
```

Operationally this is a long-running process — run it under systemd /
k8s. It tails the source's change stream and writes incremental
backups into the same store the full landed in. The rollover knobs
control segment cadence:

- `--retain-rotate-at-chain-length 50` — rotate into a new segment
  after 50 incrementals.
- `--retain-rotate-at 24h` — also rotate every 24 hours.
- `--rollover-window 5m` — group changes within a 5-minute window
  into a single incremental.

`sluice backup prune --keep-incrementals N` retires older WHOLE
segments while preserving the chain root's restorability. Retention is
segment-granular: `N` is rounded UP to the nearest segment boundary, so
prune keeps at least the `N` you asked for and often a few more, and it
logs both numbers when they differ. A never-rotated chain has no segment
boundary for `--keep-incrementals` to land on — use `--keep-duration`
there. On an
encrypted chain "the chain root" means the root `manifest.json`
specifically: it carries the Argon2id salt the restore side re-derives
your KEK from and the CEK every change chunk is sealed under, so prune
keeps that one file even when it retires the segment it belonged to.
Prune proves the chain still reads both before and after its delete
pass and refuses under `SLUICE-E-BACKUP-CHAIN-UNREADABLE` (exit 3)
rather than report success over a chain it made unreadable — pass
`--encrypt` with the chain's key material so that check can prove the
key still unwraps, not just that the identity survived.

Retention is **segment-granular**, and prune rounds your request **up**
to the nearest segment boundary. Trimming leading incrementals *inside* a
segment would leave its full anchored before a gap, so prune retires only
whole segments — which means it keeps **more** than you asked for, never
fewer. Ask to keep 7 on a chain whose boundaries fall at 5 and 12 and it
keeps 12, and says so at INFO with both numbers. Size your retention
expecting that, and read the line rather than assuming the request was
honoured exactly.

A chain that has never rotated is a single segment with no boundary below
its root, so there is nothing prune can retire and it refuses with
`SLUICE-E-BACKUP-CHAIN-UNREADABLE`, exit 3, nothing deleted. That is not
a bug to work around: the segment's full is the only base those
incrementals replay onto. Rotate the chain (a `backup stream` rollover
starts a new segment with a fresh full) if you want retention to have
something to drop, or use `--keep-duration` with a cutoff older than
every incremental to retire the lot.

## Step 3: verify periodically

```sh
sluice backup verify \
    --from-dir /var/backups/myapp \
    --encrypt --encryption-passphrase 'pick-a-real-passphrase'
```

Verify has **two depths**, and the difference is what a green result
promises.

Without the `--encrypt` flag it is sha256-only — chunk bytes are hashed
and compared against the manifest's recorded hashes. That proves the
bytes have not rotted. It does **not** prove the chain is readable:
bytes that hash correctly can still be sealed under a key or a binding
the restore path will not reproduce.

With `--encrypt` + the chain's key material, sluice **also** performs
the real AES-GCM authenticated open of every encrypted chunk — the same
CEK and the same AAD binding `restore` uses, in the default per-chain
mode as well as per-chunk. This is the depth to run before you trust a
DR archive. A chunk `restore` cannot read (tampered, spliced, or sealed
under the wrong key) fails here as
`SLUICE-E-BACKUP-CHUNK-AUTH-FAILED`, exit 3.

Both depths walk the chain's lineage first, the way `restore` does, so
a chain a `prune` or `compact` left un-walkable is refused either way —
verify never reports a chain healthy that `restore` will not start on.

**Read the `decrypted=` count, not just the exit status.** The summary
line reports `chunks=N decrypted=M`; `M` is how many chunks were
actually opened, and `decrypted=0` on an encrypted chain means you only
got the sha256 depth (no key material reached the command). A wrong key
fails earlier, at the key unwrap rather than at a chunk: `unwrap chain
cek` in the default per-chain mode, and `unwrap chunk cek (passphrase
rotated mid-chain?)` in `--encrypt-mode=per-chunk`, where each chunk
carries its own wrapped CEK and a mid-chain rotation shows up on the
first chunk written under the new passphrase. That second string is the
**wrong-key case only** — a tampered or spliced chunk is not a key
problem and surfaces as `SLUICE-E-BACKUP-CHUNK-AUTH-FAILED`.

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
