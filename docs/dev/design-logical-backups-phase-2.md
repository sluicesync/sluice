# Logical Backups Phase 2 — Implementation Design

Supplement to [`design-logical-backups.md`](design-logical-backups.md) (the original proto-ADR). This file captures the implementation-time decisions for Phase 2 (cloud backends), the Archil integration findings, the backup-chain → CDC handoff design (a Phase 3 enabler), and the backup-as-broker pattern (a Phase 4.5+ direction). The original doc remains the authoritative high-level design; this one carries the deltas.

## Decisions revised since the original proto-ADR

### Binary-size cost of `gocloud.dev/blob` (post-implementation measurement)

The pre-implementation estimate was "`~3-5 MB` even on local-FS-only builds." Real-world measurement after wiring all four side-effect imports (`s3blob`, `gcsblob`, `azureblob`, `fileblob`):

| Build | Size (Linux/Windows amd64) |
|---|---|
| Phase 1 (no cloud SDKs linked) | ~38.5 MB |
| Phase 2 (gocloud + S3 + GCS + Azure + fileblob linked) | ~84.2 MB |
| **Delta** | **~46 MB** |

Larger than the optimistic estimate, in line with the pessimistic envelope. Acceptable for v1 — operators consuming sluice via container images won't notice the layer-cache cost, and binary distribution sizes are still in the same order of magnitude as `kubectl` (~50 MB) and `terraform` (~110 MB). If the footprint becomes a real concern (e.g. embedding sluice in a small operator binary), the path is build tags per cloud or moving to native SDKs per backend; the `BackupStore` interface is unchanged either way.

### Library: `gocloud.dev/blob` (revised from native `aws-sdk-go-v2`)

The original recommended pure native SDKs per cloud backend. Revised to **`gocloud.dev/blob`** for Phase 2 onwards.

**Why the pivot.**

- Operator demand wants multi-cloud (S3 + GCS + Azure) within a few cycles, not "S3 first then maybe later."
- `gocloud.dev/blob` makes GCS / Azure native essentially free (`~50` LOC of URL-scheme registration vs `~600` LOC of SDK plumbing each).
- The cost is binary growth (`~3-5 MB` even on local-FS-only builds, since gocloud's drivers transitively pull each cloud SDK) and loss of provider-specific tuning knobs (multipart part-size per provider, S3 storage classes, etc.). For a backup writer the defaults are fine.
- The `BackupStore` interface stays unchanged; gocloud is an internal implementation detail. Reversible if footprint becomes a real concern.

**`BackupStore` impl shape**:

```go
// internal/pipeline/blob_store.go
import "gocloud.dev/blob"
import _ "gocloud.dev/blob/s3blob"
import _ "gocloud.dev/blob/gcsblob"
import _ "gocloud.dev/blob/azureblob"
import _ "gocloud.dev/blob/fileblob"

type BlobStore struct {
    bucket *blob.Bucket
}

// Construct from URL — gocloud parses the scheme and routes:
//   s3://bucket/prefix?endpoint=...&region=...&use_path_style=true
//   gs://bucket/prefix
//   azblob://container/prefix
//   file:///absolute/path  (also covered by Phase 1 LocalStore — kept for parity)
func OpenBlobStore(ctx context.Context, urlStr string) (*BlobStore, error) { ... }
```

### Encryption: NOT shipped in Phase 2; remains a Phase 6 (KMS) deliverable

The original proto-ADR proposed client-side AES-256-GCM as the default for the MVP, with `--no-encryption` as an SSE-truster opt-out. **That recommendation was not implemented in Phase 1 and is not implemented in Phase 2.** Backups land unencrypted at rest on both local-FS and cloud paths. Operators relying on at-rest encryption should use bucket-level SSE on the cloud side or filesystem-level encryption (LUKS / BitLocker / FileVault) on the local-FS side. Sluice-managed AES-256-GCM (passphrase or KMS-derived key) is parked as Phase 6 alongside the BYOK / KMS UX.

This doc previously claimed encryption "carried through from Phase 1 unchanged"; that claim was inaccurate (originated in the v0.16.0 release-notes draft and propagated here). v0.16.1 will not change this — the gap is documented honestly and the operator workarounds are real. Re-opening encryption is a Phase 6 conversation.

### NEW in Phase 2 scope: resumable backup writer

Two complementary mechanisms so a partially-completed backup doesn't restart from scratch:

1. **Skip-already-uploaded chunks.** Manifest records per-chunk completion + SHA-256. On restart, `HEAD` each completed chunk against the store; skip if present + checksum-matches. Same shape as v0.5.x's parallel-bulk-copy resume from PK cursor.
2. **Per-table progress checkpoints.** Backup writer commits manifest updates after each table completes. Resume picks up at the next un-completed table.

Both fit naturally on top of the existing Phase 1 manifest (`ChunkInfo` slice + `TableManifest`); the additions are state-tracking + the skip-on-resume logic. `~200-300` LOC.

## Archil integration

Archil is a POSIX-mountable elastic storage layer backed by S3/GCS-compatible object storage, with a caching/consistency layer in front. Auth is per-disk shared tokens or AWS IAM (STS role assumption). It exposes three surfaces relevant to sluice: POSIX FUSE mount (read-write), S3-compatible HTTP API (**read-only**), and a control-plane REST API + TS SDK (out of scope for direct sluice integration).

**Pricing note:** $0.20/GiB/month with **no per-request charges, no egress fees**. Time-weighted average storage. This is `~9x` AWS S3 storage cost but pays back fast for restore-heavy patterns (cross-region restore drills, dev-DB clones from prod backups). For backup-once-restore-rarely, AWS + Glacier transitions are cheaper.

### Critical finding: S3 API is read-only

`PutObject`, `DeleteObject`, multipart upload — all return `MethodNotAllowed`. Writes happen via POSIX mount. This splits the integration cleanly:

| Direction | Path | Sluice work | Notes |
|---|---|---|---|
| **Backup writes → Archil** | POSIX mount via Phase 1's `LocalStore` | Zero | Operator runs `archil mount <disk-id> /mnt/archil`, then `sluice backup full --backup-dir=/mnt/archil/...`. Mount is out-of-band; sluice never knows it's not a regular FS. |
| **Restore reads ← Archil** | S3-compatible API via Phase 2's `BlobStore` (`--backup-endpoint`) | Zero beyond the generic endpoint flag | Useful for restore-from-elsewhere flows where the restore host doesn't want to mount. Cross-environment restore. |

### S3 API quirks (bake into the optional integration test)

- **Path-style addressing required.** Virtual-host style is not supported.
- **Region string** is the full code: `aws-us-east-1`, `aws-us-west-2`, `aws-eu-west-1`, `gcp-us-central1` (not `us-east-1`).
- **Endpoint URL by region**:
  - `https://s3.green.us-east-1.aws.prod.archil.com`
  - `https://s3.green.us-west-2.aws.prod.archil.com`
  - `https://s3.green.eu-west-1.aws.prod.archil.com`
  - `https://s3.blue.us-central1.gcp.prod.archil.com`
- **LIST delimiter** must be `/`. Other delimiters → 400.
- **Signature v4** standard (default in any modern SDK).
- **Per-disk credential scope** — credentials grant access to one disk only; cross-bucket requests return `AccessDenied`.

### Setup checklist (operator harness)

1. Create a disk in Archil web console (region close to the test environment)
2. Disk → Details → Authorized Users → Add User → **S3 credentials**. Copy Access Key ID + Secret Access Key **immediately** — secret can't be retrieved later.
3. Save the credentials to a gitignored env file for local runs.
4. For mount-path testing: install the `archil` CLI on the test host; `archil mount <disk-id> /mnt/archil`.
5. For S3-API testing: pass `--backup-endpoint <region-endpoint>` + path-style flag; sluice's `BlobStore` handles the rest.

Archil testing stays operator-run, not main sluice CI. An optional `archilverify` build tag (analogous to `psverify`) lets the test live in the codebase without forcing every CI run to need credentials.

## Backup-chain → CDC handoff (Phase 3 acceptance criterion)

Phase 3 (incrementals) ships the ability to restore a chain of `[full, incr, incr, ...]` into a fresh target. To complete the resync-avoidance story, the restored target must be **CDC-resumable from the chain's terminal position**, not require a re-bulk from source.

### MySQL (binlog + GTID)

```
After restore: target's gtid_executed = manifest's terminal_GTID
SET GLOBAL gtid_purged = '<manifest_GTID>'  -- target now declares "I have everything up to this set"
sluice sync start --resume                  -- source streams everything NOT in target's GTID set
```

Clean: GTIDs are content-addressed, source has no per-target state. Works as long as source binlog covers the gap (default `binlog_expire_logs_seconds=7d` is plenty for most disaster windows).

### Postgres (LSN + logical replication slot)

```
After restore: target has data through manifest's terminal_LSN
sluice sync start --resume --lsn <manifest_LSN>  -- new flag (Phase 3)
```

**Operational catch**: `pg_create_logical_replication_slot()` creates a slot at the **current** server LSN, not an arbitrary historical one. To resume from `manifest_LSN`, the WAL between `manifest_LSN` and the slot's current `restart_lsn` must still be on disk. Two ways to guarantee this:

1. **Continuous-incremental mode (Phase 4) maintains a long-lived slot** so `restart_lsn` advances incrementally and the WAL is retained. This is the recommended pattern.
2. **Operator sets `wal_keep_size` (PG 13+) or `max_slot_wal_keep_size`** generously enough to cover the worst-case recovery window.

If neither is in place, the handoff falls back to today's cold-start re-bulk (the source DB hasn't lost any data; it just needs to re-bulk the target). Phase 3 must surface this as a clear operator-actionable warning, not a silent footgun.

This goes in `docs/postgres-source-prep.md` as a "if you want zero-rebuild disaster recovery" subsection.

### Acceptance criterion

**Phase 3 is not done until**: a restore-chain-into-fresh-target → start-CDC roundtrip works on both engines without any source-side re-bulk, validated by integration tests in both directions.

## Backup-as-broker pattern (Phase 4.5+ direction)

Once Phase 4 (continuous-incremental) ships, an additional `~500` LOC enables **decoupled source/target sync via the backup chain as the message log**:

```
[Sluice A — source-side]                  [Sluice B — target-side]
Source DB ──CDC──> Sluice A ──writes──> Backup Storage ──reads──> Sluice B ──writes──> Target DB
```

New surface: `sluice sync from-backup --backup-dir=<url> --target-driver=... --target=...` polls the manifest for new incrementals and replays them via the existing applier. The applier is unchanged — incrementals already encode the same `ir.Change` events the live CDC pump emits.

### Use cases this unlocks

- **No direct source-target connectivity** (compliance / firewall constraints)
- **Multi-region replication without VPN** — backup bucket as rendezvous
- **One source → many targets fan-out** (prod analytics + dev refresh + staging clone, all from one source's incrementals)
- **Time-shifted sync** ("staging is always 1h behind prod")
- **Sync-itself disaster recovery** (target down for hours; Sluice A keeps writing; Sluice B catches up later)
- **Air-gapped target replication** (backup → SneakerNet → restore at target site)

### Tradeoffs

- Latency floor = backup write cadence + Sluice B poll interval (seconds-to-minutes vs sub-second for direct CDC)
- Cost = backup storage + double bandwidth
- Schema changes coordinate via the chain (target consumes a "schema delta" entry before the rows that depend on it — already a Phase 3 design item per Open Question #1)

### Implementation sequencing

1. Phase 2 (this work) — cloud backends.
2. Phase 3 — incrementals + handoff acceptance criterion above.
3. Phase 4 — continuous-incremental.
4. **Phase 4.5 — `sync from-backup` / backup-as-broker.** Thin layer once Phases 3 + 4 are in.

## Concrete Phase 2 implementation plan

| Sub-phase | Scope | LOC est. |
|---|---|---|
| **2.1 — `BlobStore` over `gocloud.dev/blob`** | New `internal/pipeline/blob_store.go` implementing `BackupStore` via `blob.Bucket`. URL parsing via `blob.OpenBucket`. Imports for `s3blob`, `gcsblob`, `azureblob`, `fileblob` drivers. | 400-500 |
| **2.1.1 — Resumable writer** | Skip-already-uploaded chunks via manifest-state + `HEAD`; per-table progress checkpoint. | 200-300 |
| **2.2 — `--backup-endpoint` + region + path-style flags** | Custom-endpoint flag covers MinIO, R2, B2, Wasabi, Tigris, Archil-read. Wired through to `gocloud.dev/blob`'s `s3blob` query-string params (`endpoint=...`, `region=...`, `s3ForcePathStyle=true`). | 100-150 |
| **2.3 — GCS + Azure URL schemes** | URL scheme registration; ADC creds for GCS; managed-identity / connection-string for Azure. CI smoke test optional. | 50-100 |
| **CI** | MinIO testcontainer roundtrip (S3-compatible). Cross-engine restore (PG backup → MySQL target via existing `RetargetForEngine`) on the cloud path. | 200-300 |
| **Total** | | ~1000-1400 |

### Out of scope for Phase 2 (deferred)

- KMS-managed encryption keys (Phase 6)
- Lifecycle policies / retention enforcement (Phase 7+)
- Cross-region replication of backup objects (Phase 7+)
- `sluice backup verify --deep` over cloud objects (Phase 3+ pairing)
- The `sync from-backup` broker (Phase 4.5)

### Definition of done

1. `sluice backup full --target=s3://...` rountrips against MinIO in CI.
2. `sluice backup full --target=s3://... --backup-endpoint=...` works against an arbitrary S3-compatible endpoint (validated against the Archil read-path in the operator harness).
3. Resumable writer: kill the backup process mid-job, restart, completes from where it left off (integration test).
4. Cross-engine restore from cloud (PG backup → MySQL target) works.
5. Encryption default-on; `--no-encryption` opt-out works.
6. `gocloud.dev/blob` dep added to `go.mod`; binary size delta documented.

## See also

- [`design-logical-backups.md`](design-logical-backups.md) — original proto-ADR
- [`design-sluice-verify.md`](design-sluice-verify.md) — verify command (paired feature for the "100% confidence" goal)
- [`postgres-source-prep.md`](../postgres-source-prep.md) — operator-facing PG setup, will gain a "zero-rebuild disaster recovery" subsection in Phase 3
