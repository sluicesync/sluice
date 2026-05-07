# Design: logical backups to operator-owned storage

**Status:** Proto-ADR / design exploration. Not yet a numbered ADR. Captures the design space, motivating use cases, and a phased implementation plan for sluice growing a logical-backup-and-restore product surface that writes to operator-controlled storage — cloud object stores for most users, local filesystem for the air-gapped / dev / no-cloud-account audience.

## Context

### Why this exists

Sluice today moves data between live databases. The two engines — `Migrator` (`internal/pipeline/migrate.go`) and `Streamer` (`internal/pipeline/streamer.go`) — already do most of what a backup tool does:

- Read source schemas through `SchemaReader` and produce dialect-neutral `*ir.Schema` values.
- Bulk-copy rows through `RowReader` / `RowWriter` with parallel-per-table chunking, per-batch checkpointing, memory-bounded streaming, and a shared `SnapshotStream` that pins a consistent capture point (`internal/ir/snapshot.go`).
- Capture the source CDC position at snapshot time and stream every event after it through `CDCReader` (`internal/ir/change.go`'s `Position` and `Change` types).
- Persist progress in a control table (`sluice_cdc_state`, ADR-0007) so a crash mid-stream resumes correctly.

A logical backup is essentially "a snapshot + a CDC tail, written to durable storage with a manifest." The building blocks already exist; what's missing is a writer side that emits to durable storage (local filesystem or cloud object store) instead of a target database, and a reader side that consumes that storage to reconstitute one.

### Why operators want this

The "I want my own backup, not the vendor's" use case is real and recurring:

- **PlanetScale and similar managed-MySQL customers.** Built-in physical backups live inside the vendor's system. Operators want a logical copy in an account they control — different cloud, different region, sometimes on-prem. Vendor-lock-in mitigation, audit/compliance, and "what if our PlanetScale account is compromised" disaster planning all push the same direction.
- **Self-hosted MySQL / Postgres operators.** No managed-service backups at all; today they cobble together `mysqldump` / `pg_dump` + cron + an upload script. Every operator re-implements it.
- **Multi-cloud / multi-region DR.** Compliance contexts (GDPR, HIPAA, SOC 2) sometimes require backups in a different jurisdiction or vendor than the primary database.
- **Migration prep.** Dump current state to a portable format, restore into a different vendor or engine for evaluation. Today the only path is "stand up a target DB, run `sluice migrate`" — heavier than necessary for "I just want to see what my schema looks like on PG."
- **Forensic / point-in-time recovery.** "Take me back to T=2026-04-12 14:30 UTC" is operationally common (failed migration, accidental DROP, ransomware). Vendor PITR is bounded to the vendor's retention window.
- **Local-only backups (air-gapped / dev / pre-prod / no-cloud-account).** Not every operator wants — or is allowed — to push backups to a cloud object store. Air-gapped environments forbid it. Dev and pre-prod workflows want a fast same-machine dump-and-restore loop without network round-trips. Some compliance contexts require backups stay on-prem. And a meaningful slice of operators simply don't want the friction of setting up an S3 / GCS / Azure account, managing credentials, and paying egress for what is fundamentally a local operation. For these users, "write the backup to a directory I name" is the entire UX they want — and it's also the cheapest place for sluice itself to validate manifest format and restore correctness, with zero credentials surface and zero external infrastructure.

The first persona — PlanetScale customers wanting vendor-independent logical backups — is the one most likely to drive adoption. It's also the most awkwardly served by existing tools: `mysqldump` against a Vitess shard is slow and its output isn't trivially parallelisable on restore. Sluice's existing parallel bulk-read path (ADR-0019) is a real advantage here.

## Design space

### Backup shape (what gets written to storage)

Five candidate formats. The decision determines what "restore" means and which target engines are reachable.

**(a) SQL-text dumps (mysqldump-style).** Familiar, portable, human-inspectable. Large, slow to load, single-threaded restore. Engine-bound: a MySQL dump only restores into MySQL. **Reject as primary format** — the "restore into a different vendor" persona is exactly where this fails.

**(b) Binary CSV/TSV per table (pg_dump custom-format-style).** Smaller, structured, fast to load via the target's bulk-load path. Same engine-binding problem as (a).

**(c) Engine-neutral IR-format.** Sluice's `ir.Schema` and `ir.Row` serialised to a stable on-disk format (JSON for the schema, a typed binary format for rows). Every IR type already round-trips between engines today; serialising the IR makes a backup that can be restored into any sluice-supported target. Plays directly to the IR-first tenet. Cost: sluice owns the format, including forward-compat across releases.

**(d) Apache Parquet.** Columnar compression, query-without-restore via DuckDB / Athena / BigQuery. Read-side semantics for restoring into MySQL/PG are non-trivial — Parquet's logical types don't map 1:1 to all SQL types (no MySQL-style temporal precisions, geometry/spatial via convention only). **Depends on the parallel Arrow research subagent.** If sluice adopts Arrow internally, Parquet falls out almost for free; otherwise it's a sizable new dep.

**(e) Hybrid manifest + chunks.** A JSON manifest (`manifest.json`) lists tables, per-table row counts, schema-version, source CDC position at backup time, per-chunk URLs, per-chunk checksums, encryption metadata. The chunk format is one of (b)/(c)/(d) — pluggable. Restore reads the manifest, then parallel-fetches and applies chunks. This is what `pgcopydb`, `wal-g`, AWS Backup, and most modern backup tools converge on.

**Recommendation: (e) hybrid, with (c) IR-format chunks for v1, leaving the door open for (d) Parquet chunks once the Arrow decision lands.** The manifest is the load-bearing public contract — operators interact with it directly (`sluice backup list`, `sluice backup show <id>`), tooling depends on it, restore reads it first. The chunk format is internal and can evolve once the manifest is pinned.

The IR-format chunk choice means sluice leans on the IR's existing per-engine type-translation machinery. A chunk written from MySQL can be restored into Postgres because that's exactly what the IR promises today. No second translation layer to maintain.

### Manifest shape

Sketch (full spec lives in the eventual ADR):

```json
{
  "sluice_backup_version": 1,
  "backup_id": "2026-05-06T14:30:00Z-ab12cd",
  "backup_kind": "full",
  "previous_backup_id": null,
  "source_engine": "mysql",
  "captured_at": "2026-05-06T14:30:00Z",
  "source_position": { "engine": "mysql", "token": "..." },
  "schema_path": "schema/schema.json.zst",
  "schema_sha256": "...",
  "tables": [{
    "name": "users",
    "row_count": 1234567,
    "chunks": [{ "path": "data/users/00000.ir.zst", "rows": 100000, "sha256": "..." }]
  }],
  "encryption": { "kind": "none" }
}
```

The schema serialises separately so `sluice backup show <id>` can render the schema and table list without fetching chunk data. Chunks are addressable; restore can resume by checksumming what's already on the target side.

### Full + incremental composition

**Full backup** = current snapshot of every selected table + the source CDC position at snapshot start. Built directly on `Migrator`'s existing snapshot path with the row-write side redirected to a `BackupStore` instead of a target database.

**Incremental backup** = the CDC events from the previous backup's recorded position to "now," packaged into a self-contained chunk file. Built on `Streamer`'s CDC pump with the applier replaced by one that writes serialised `ir.Change` events to a `BackupStore`.

**Restore** = apply the most recent full + every incremental since, in order. The restore side is the inverse: read the manifest, restore the schema, restore the snapshot chunks (existing bulk-copy path with a `RowReader` that reads from a `BackupStore`), then replay incrementals (existing applier with a `CDCReader` that reads from a `BackupStore`). Idempotent application is required because a crashed restore should be re-runnable; ADR-0010's idempotent-applier semantics already cover this for live CDC and apply equally to replay.

The composition story is clean because sluice's existing engines already operate on `ir.Row` and `ir.Change` streams. A "backup" is a stream that lands in a `BackupStore`; a "restore" is a stream that originates from one. The pipeline orchestrator doesn't need to know which backend the store is backed by.

### CLI surface

Three candidate shapes:

**(1) Overload `migrate` and `sync start` with a `backup://...` URL.** Lean — no new top-level commands. Risk: backups are operationally distinct from migrations (retention, lifecycle, listing) and shoehorning that into `migrate` flags creates a confused UX.

**(2) New `sluice backup` / `sluice restore` top-level subcommands.** Distinct verb surface; each command's flags stay focused. Closest to operator mental models — every other backup tool (`wal-g`, `pgbackrest`, AWS Backup) has a backup verb separate from migrate.

**(3) Hybrid: backup operations under `sluice backup`, restore reuses `migrate --source backup://...`.** Saves CLI code at the cost of conflating two operator intents.

**This design assumes (2)** with the explicit acknowledgement that the underlying machinery is shared and lives in `internal/pipeline`.

### Continuous-incremental as a "live continuous backup"

A natural extension: `sluice backup stream --interval 60s`. Same machinery as `sync start`, except the applier writes serialised `ir.Change` events to rolling chunk files. Each chunk gets a manifest entry; chunks roll over by time or size. The result is effectively WAL archiving, but operator-controlled and engine-portable. **Not v1** — get the explicit-trigger flows working first; continuous mode is a deployment shape on top of incrementals once the storage format is solid.

### Storage backends (destination targets)

Backups need somewhere to land. Cloud object stores serve most users, but local filesystem is a first-class destination — air-gapped environments, dev/pre-prod loops, fast same-machine restore, and operators who don't want to set up an object-storage account all want it. Both are exposed through the same interface so the writer and restore paths don't care which backend they're talking to.

#### `BackupStore` interface

```go
type BackupStore interface {
    Put(ctx context.Context, path string, r io.Reader) error
    Get(ctx context.Context, path string) (io.ReadCloser, error)
    List(ctx context.Context, prefix string) ([]string, error)
    Delete(ctx context.Context, path string) error
}
```

Same shape covers every backend. Backup writer calls `Put` for each chunk and the manifest; restore calls `List` + `Get`. `Delete` is for retention pruning (Phase 2+). Local-FS is one implementation; S3, GCS, Azure are others.

#### Backends in scope

- **Local filesystem** (`file:///path/to/backup/` or `--backup-dir=/path/`). Reference implementation. Zero external dependencies, zero credentials, zero network. `Put` is `os.Create` + `io.Copy`; `Get` is `os.Open`; `List` walks the prefix. This is the MVP backend — it lets sluice validate the manifest format and restore correctness without any cloud infrastructure, and it directly serves the air-gapped / dev / no-cloud-account audience.
- **S3 and S3-compatible** (`s3://bucket/prefix/`, also covers MinIO, Backblaze B2, Cloudflare R2, DigitalOcean Spaces, Wasabi, Tigris via `--backup-endpoint`). Multipart upload for large chunks. AWS SDK defaults for auth: env vars (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`), IAM roles on EC2/ECS/EKS, profile files, AWS SSO.
- **GCS** (`gs://bucket/prefix/`). ADC credentials.
- **Azure Blob** (`azblob://container/prefix/`). Managed-identity by preference.

#### Library choice

Two reasonable Go paths:

- **`gocloud.dev/blob`** — single abstraction over S3, GCS, Azure, and local filesystem (via its `fileblob` driver). Mature, single auth-and-URL model. Heavier dep tree (`aws-sdk-go-v2`, `google-cloud-go`, `azure-sdk-for-go`).
- **Pure stdlib for local-FS, native SDKs per cloud backend.** Lighter overall; local-FS pays nothing for the cloud-SDK weight, and each cloud backend pulls only its own SDK.

**Recommendation: pure-stdlib local-FS for the MVP, then per-backend native SDKs as cloud backends ship.** This preserves the "zero external dependencies" property of the local-FS path — operators who never need cloud never pay for the cloud-SDK weight. The `BackupStore` interface is internal, so the choice is reversible if `gocloud.dev/blob`'s single-abstraction value outweighs its footprint later.

For the cloud side, **S3-compatible only via `aws-sdk-go-v2` is the v1 cloud backend** when cloud lands in Phase 2. Operators on GCS / Azure use S3-compatible gateways or wait. GCS / Azure native are Phase 4.

### Encryption + integrity

Backups outlive the systems that produced them; both in-flight and at-rest threats matter, and the threat model differs by backend.

**At-rest encryption — backend asymmetry.** Server-side encryption (S3 SSE / GCS default-encryption / Azure SSE) is configured by the operator at the bucket level and **does not apply to local-FS** — there is no server-side equivalent for a local directory. Filesystem-level encryption (LUKS / BitLocker / FileVault) is the operator's responsibility on local-FS, not sluice's. Client-side encryption (sluice does AES-GCM before `Put`) covers both backends uniformly: same protection on local disk as on S3, and the only at-rest option that actually defends a local-FS backup against host-level threats.

The asymmetry — server-side is cloud-only, client-side is universal — argues for client-side as the **recommended default** because uniform behavior is easier to reason about than per-backend behavior. Client-side also matters for the "I don't fully trust the storage provider" persona and for portable backups handed off between environments.

**Recommendation:**

- **Client-side AES-256-GCM as the default**, key supplied by operator (passphrase-derived via Argon2id for the MVP; KMS-supplied in Phase 6). Per-chunk IV recorded in the manifest. Decryption happens in the restore path before any IR reconstruction.
- **`--no-encryption` opt-out** for operators who rely on server-side bucket encryption alone, or who trust filesystem-level encryption on the local-FS path.
- **Server-side encryption documented but operator-configured.** Sluice doesn't manage SSE settings.

The MVP can ship client-side with a passphrase-derived key (cheap, no KMS dependency); KMS / BYOK with full key-management UX is Phase 6.

**Integrity.** Per-chunk SHA-256 in the manifest, verified before restore. Periodic self-verification (`sluice backup verify <id>`) re-fetches every chunk and re-checksums. Independent of encryption; runs on every restore. Non-negotiable: the no-silent-corruption tenet means "the file got bit-flipped on the way to S3" — or "the local disk had a bad block" — can't be a silent failure mode.

**Compression.** Zstandard (level 3) per-chunk, before encryption. Default on; `--no-compression` for benchmarks.

### Restore correctness — the 100% confidence story

Verification has three layers, each more expensive:

1. **Per-chunk checksum.** In the manifest. Catches corruption in transit and at rest.
2. **Per-table row count.** After restore, count rows on target and compare to manifest's `row_count`. Catches "the restore silently dropped chunks."
3. **Per-table content checksum.** Hash a deterministic serialisation of every row (PK-ordered, IR-typed values) on the source at backup time and on the target after restore. Catches "the restore silently mistranslated values."

For v1: layers 1 and 2 mandatory; layer 3 opt-in via `--verify=full`. The cross-engine type-translation contract (`docs/value-types.md`) defines what "byte-perfect modulo translation" means for layer 3.

The story for "100% confidence": **same-engine restore (MySQL→MySQL or PG→PG) is byte-perfect at all three layers**; **cross-engine restore is byte-perfect modulo the IR's documented type translations** (e.g. MySQL `TINYINT(1)` → PG `BOOLEAN`, MySQL `JSON` → PG `JSONB`). If an operator wants pure byte-perfect, they restore into the same engine; if they want portability, they accept the translation contract sluice already publishes.

### Tenet check

- **IR-first.** Strong fit. The recommended chunk format is the IR serialised; backup writers / readers plug into the existing `RowReader` / `RowWriter` interfaces. No new translation surface.
- **Contain Postgres complexity.** Sluice should be opinionated: **logical backups only**, not `pg_basebackup` / WAL-archive territory. `wal-g` and `pgbackrest` exist and are excellent. Sluice's value is the cross-engine and operator-owned-storage angle, not competing with WAL-shipping tools. Hard scope line.
- **Validate end-to-end.** Restore-roundtrip tests as a CI integration job: take a populated database, back it up, restore into a fresh database, schema-diff and content-checksum the result. Required before the feature is considered done.
- **Loud failure beats silent corruption.** Layer 2 row-count verification on restore is mandatory.
- **Clean, elegant code.** The `BackupStore` abstraction is small (~5 methods); the `Manifest` is a single struct; backup and restore paths are thin wrappers around existing pipeline machinery.

The tension is **scope discipline**. "Logical backup" is a small word that grows: encryption, KMS, lifecycle policies, retention enforcement, cross-region replication, PITR UX, backup search. The MVP must say no to most of them.

## Concrete implementation plan

Phased so each is independently shippable. The MVP is intentionally **local-FS-only** because that's the smallest slice that validates the load-bearing parts of the design — manifest format, restore correctness across engines, encryption defaults — without coupling them to cloud-storage infrastructure or credentials. Cloud backends slot in afterwards as additional `BackupStore` implementations; the interface is designed for it.

### Phase 1: Full backup → local filesystem (MVP)

- `internal/backup/manifest.go` — manifest type, JSON marshal, version field for forward-compat.
- `internal/backup/store.go` — `BackupStore` interface; `LocalStore` implementation (pure stdlib, no cloud SDK).
- `internal/backup/writer.go` and `reader.go` — IR-format chunk writer/reader; zstd; per-chunk SHA-256.
- `internal/backup/crypto.go` — client-side AES-256-GCM with passphrase-derived key (Argon2id); per-chunk IV in manifest.
- `internal/pipeline/backup.go` — orchestrator that drives `SchemaReader` + `SnapshotStream` and emits to a `BackupStore`. Reuses the parallel chunked bulk-read path (`internal/pipeline/migrate_parallel.go`).
- New CLI: `sluice backup full --target=file:///path/` (or `--backup-dir=/path/`), `sluice backup list`, `sluice backup show`, `sluice restore --backup=file:///path/`.
- Layer-1 (per-chunk checksum) and layer-2 (per-table row count) verification on restore.
- Client-side encryption default-on with passphrase; `--no-encryption` opt-out.
- CI integration job: same-engine and cross-engine roundtrip tests using local-FS in a tmpdir (no testcontainers needed for storage).

Why local-FS as the MVP backend:

- Zero external dependencies — no cloud SDK, no `gocloud.dev/blob`, no credential plumbing. Pure `os` + `filepath` + the chunk writer.
- Zero credentials surface — nothing to misconfigure, nothing to leak.
- Validates the load-bearing parts of the design (manifest format, restore correctness, cross-engine type fidelity) without coupling them to cloud-storage testing infrastructure.
- Directly serves a real audience (the air-gapped / dev / pre-prod / no-cloud-account use case) from day one.
- Cheapest test surface — local-FS roundtrips run in CI without any extra container.

Estimated size: ~2000-2800 LOC including tests + ADR. (Smaller than a cloud-MVP would be because the storage-backend code is ~200 LOC of stdlib instead of ~800 LOC of SDK plumbing.)

### Phase 2: Cloud backends (S3-compatible)

- `internal/backup/s3_store.go` — `S3Store` implementation via `aws-sdk-go-v2`; multipart upload for large chunks.
- New CLI: `sluice backup full --target=s3://bucket/prefix/`, `sluice restore --backup=s3://...`. Same shape as Phase 1; only the URL scheme changes.
- Auth follows AWS SDK defaults: env vars, IAM roles, profile files, AWS SSO. B2 / R2 / MinIO / DigitalOcean Spaces / Wasabi override the endpoint via `--backup-endpoint`.
- CI integration job: roundtrip against MinIO via testcontainers.

Estimated size: ~1000-1500 LOC.

### Phase 3: Incremental backups

- `internal/backup/cdc_writer.go` and `cdc_reader.go` — applier and reader shapes for serialised `ir.Change` chunks.
- New CLI: `sluice backup incremental --since <backup-id>`.
- Restore semantics: walk the chain (full + every incremental since) in order. Idempotency on replay relies on ADR-0010.
- Layer-3 verification opt-in via `--verify=full`.
- CI extension: incremental-chain roundtrip.

Estimated size: ~1500-2000 LOC.

### Phase 4: Continuous-incremental mode

`sluice backup stream` — long-running process producing rolling incrementals at configurable intervals. Most machinery exists from Phase 3; this phase is rollover policy, manifest update under concurrent writes, operator UX.

Estimated size: ~800-1200 LOC.

### Phase 5: Multi-cloud (GCS, Azure native)

GCS and Azure Blob via per-backend SDKs (or `gocloud.dev/blob` if its single-abstraction value outweighs the footprint by then). Decision point at the start of the phase.

Estimated size: ~600-1000 LOC.

### Phase 6: KMS-backed encryption

KMS integration on top of the MVP's passphrase-derived encryption: AWS KMS first, GCP KMS / Azure Key Vault to follow. BYOK story lands here.

Estimated size: ~800-1200 LOC plus a key-management ADR.

### Phase 7+: Operationally-mature features (TBD)

Retention policies, lifecycle integration, cross-region replication of backup objects, PITR UX (`sluice restore --as-of <ts>`), backup search. Each individually reasonable; collectively scope creep. Land only when real-world testing surfaces them.

## Open questions

1. **Schema-evolution within an incremental chain.** What if `ALTER TABLE` runs between full and incremental? Current schema-change runbook is "stop the stream, ALTER, resume" — analogous fits backups (stop, ALTER, take a fresh full), but heavier than expected. A lighter design: incremental manifests carry a schema-fingerprint; on schema change, a "schema delta" entry. Restore applies deltas in order. Out of scope for v1; pin down before incrementals ship.
2. **Multi-table-source backups for sharded sources.** If the source is Vitess-sharded, does `sluice backup full` produce one backup per shard or a consolidated one? Probably per-shard. Multi-source aggregation (`docs/dev/design-multi-source-aggregation.md`) and backups interact here.
3. **Backup-of-encrypted-source.** TDE-protected sources are decrypted at the connection layer — the backup contains plaintext. The MVP's client-side AES-GCM (default-on) protects the at-rest backup; operators who need stronger key-management (KMS-managed keys, BYOK) wait for Phase 6. Document the threat model.
4. **Restore into a populated target.** Mirror `migrate`'s `--force-cold-start` and `--reset-target-data` (ADR-0023). Probably reuse the existing `Migrator` directly — restore is a migrate with a backup-store source.
5. **Format versioning across sluice releases.** Manifest carries `sluice_backup_version: 1`; future versions add fields. Old sluice refuses newer manifests with a clear error; new sluice always reads older.
6. **PlanetScale-specific gotchas.** Backups should inherit the existing `_vt_*` shadow-table exclusion (Bug 22). Incrementals against PlanetScale Postgres need the Patroni / `Logical slot name` configuration documented in `docs/postgres-source-prep.md`.

## Dependency on parallel Apache Arrow research

A separate research subagent investigated whether sluice should adopt Apache Arrow / Parquet as a writer surface. **Result: conditional yes on Shape A (Parquet writer, local-FS only, behind a build tag, ~2 weeks)** — explicitly gated on whether *this* backup research picks Parquet as its on-disk format.

The dependency now flows in both directions:

- **Backup → Arrow.** The chunk-format choice here (option (c) IR-format vs option (d) Parquet) determines whether Arrow's conditional-yes converts to an unconditional-yes. If the backup MVP picks IR-format chunks, Arrow's standalone case stays speculative; if it picks Parquet, Arrow Shape A subsumes the backup writer and the combined effort yields data-lake offload + portable backups from one engineering investment.
- **Arrow → Backup.** If Arrow Shape A ships first (with Parquet + local-FS), the backup MVP could reuse its Parquet writer and the local-FS plumbing rather than write its own IR-format chunk encoder.

**Sequencing convergence.** Both proto-ADRs converge on the same MVP shape: Parquet (or IR-format) chunks + local-FS + behind a build tag + ~2-3 weeks of work each. Built together as a single Parquet-format MVP, the combined effort is roughly **3-4 weeks** instead of 2 + 3 weeks serially, and the shared local-FS-only first slice means there's no cloud-coordination cost gating the validation. The shared-local-FS MVP is the natural scoping point: no cloud backends, no Arrow IPC, no GeoParquet, no incremental backup.

**Recommendation on chunk format, given Arrow's result:** **Lean toward Parquet chunks** if the Arrow Shape A work is being seriously considered, because the convergent MVP is the cheapest version of either project. **Stay with IR-format chunks** if Arrow is being deferred indefinitely, because the manifest abstraction is chunk-format-agnostic and IR-format is the lower-dependency path for backups alone. The manifest version field (`sluice_backup_version: 1`) is the migration path either way: Arrow / Parquet support can land as a v2 chunk format if not chosen up-front.

The previous recommendation in this doc was "proceed with IR-format chunks in v1 regardless." With Arrow's conditional-yes in hand, that softens to "proceed with whichever chunk format the joint MVP picks; both are forward-compatible via the manifest version bump."

## Recommendation

**Yes, with a local-FS-only MVP.** Logical backups are a real operator need, the building blocks already exist in sluice's pipeline, and the local-FS-only MVP is the cheapest possible validation slice — no cloud infrastructure, no credentials, no testcontainers-for-storage, no external auth surface.

The local-storage addition makes the "yes" answer easier to defend than a cloud-first framing did:

- **Less external infrastructure to coordinate.** A local-FS MVP roundtrips in CI with `t.TempDir()` and zero extra setup. Cloud-MVP would have needed MinIO testcontainers from day one.
- **Smaller credentials surface.** Phase 1 has none. Operators can adopt the feature without provisioning anything.
- **Real audience served on day one.** Air-gapped, dev/pre-prod, no-cloud-account operators get a useful tool from Phase 1; cloud users wait until Phase 2 but that's weeks not quarters.
- **Cheaper to validate the load-bearing decisions.** Manifest format, restore correctness, cross-engine type fidelity, encryption defaults — all of these get validated against local-FS first, with no cloud entanglement clouding the failure modes.

The conditions to commit are now **looser** than the previous version of this doc suggested:

1. **Real-world testing has produced at least one operator request for "I want my own backup."** Same as before — without a request, even a cheap MVP risks being well-built and unused. *But:* the local-only audience (the air-gapped / dev / no-cloud-account persona added above) makes this a lower bar — operators who want local backups are a known persona that doesn't depend on PlanetScale-customer pull.
2. **The IR has stabilised through one more release cycle.** Same as before. The chunk format is a public contract.
3. **(No longer required) The Arrow research has reported.** It has — Shape A conditional-yes, gated on this doc. The two efforts can converge into a joint MVP if maintainers want to ship both surfaces from one engineering cycle, or this MVP can ship alone with IR-format chunks.

If conditions 1 and 2 are met, **ship Phase 1 (full backup → local filesystem) as a single release** — possibly bundled with Arrow Shape A if the chunk-format choice converges on Parquet. Phase 2 (cloud backends) follows once Phase 1 sees adoption; it's real engineering but isolated to the storage-backend layer, with no risk to the manifest / restore / encryption decisions already validated.

If the conditions aren't met, the design sits as a reference. Building it ahead of demand burns scope on a feature no operator has yet asked for — but the local-FS MVP framing means the cost-when-built is lower than it would have been under a cloud-first design.

## See also

- `docs/architecture.md` — the IR + engine pattern this design extends.
- `docs/value-types.md` — the cross-engine value translation contract that defines "byte-perfect modulo translation."
- `internal/pipeline/migrate.go` and `internal/pipeline/streamer.go` — the snapshot and CDC paths backups would tap into.
- `internal/ir/snapshot.go` — `SnapshotStream` is the load-bearing primitive for full-backup capture.
- `internal/ir/change.go` — `Position` and `Change` types are what incremental chunks serialise.
- `docs/adr/adr-0007-position-persistence.md` — the position-store machinery backups inherit.
- `docs/adr/adr-0010-idempotent-applier.md` — idempotent-apply semantics that make restore-replay safe.
- `docs/adr/adr-0019-parallel-within-table-bulk-copy.md` — the parallel bulk-read path that makes backups fast on large tables.
- `docs/adr/adr-0023-reset-target-data.md` — the destructive-recovery pattern restore-into-populated-target should mirror.
- `docs/dev/design-mid-stream-add-table.md`, `docs/dev/design-multi-source-aggregation.md` — sibling proto-ADRs in the same shape.
