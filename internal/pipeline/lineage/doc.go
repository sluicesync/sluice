// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package lineage is the engine-neutral backup manifest-IO +
// lineage-catalog + chunk-encryption substrate carved DOWN out of the
// internal/pipeline root (audit 3.7b-0 / Theme 5).
//
// It holds the pieces the backup/restore/chain "cluster" and root's
// streaming/broker/incremental layer BOTH consume, and that were
// previously woven bidirectionally through incremental.go, backup.go,
// chain_restore.go, chain_catalog.go, and stream_rotation.go:
//
//   - the manifest-IO layer: read/write a manifest at a path, the
//     directory-walk over the conventional one-segment layout, the
//     ADR-0086 in-progress sidecar replay, and the manifest.json /
//     manifests/ path conventions;
//   - the lineage-catalog layer (ADR-0046 lineage.json): the
//     Catalog / Segment on-disk model, its load/write/
//     resolve/update helpers, the segment store view, and the strict
//     boundary-monotonicity chain builders (BuildLineageChain /
//     BuildBrokerChain) that assemble a restore/broker link list;
//   - the chunk-encryption substrate: BackupEncryption (the chunk-
//     writer-side envelope config) plus the chain-root / per-chunk
//     decrypt-probe helpers.
//
// The package imports ONLY the typed IR (internal/ir, internal/ir/backup),
// internal/pipeline/blobcodec, internal/crypto, stdlib, and third-party
// libraries (migcore/translate/sluicecode are within the permitted lower
// set but not currently needed) — NEVER internal/pipeline root, and NEVER
// the not-yet-carved internal/pipeline/backup. That one-directional edge
// (pipeline-root →
// lineage, never the reverse) is the load-bearing property: the manifest-IO
// + lineage-catalog substrate was the mutually-recursive knot between root's
// incremental.go and the cluster, so pulling it DOWN is what unblocks lifting
// the backup/restore/chain cluster out of the root in the 3.7b-2 chunk as a
// clean lineage consumer.
//
// internal/crypto is a pure leaf (it imports nothing else from sluice) and is
// ALREADY in this package's transitive closure via blobcodec; the direct
// import here only surfaces what BackupEncryption / ProbeChunkDecrypt already
// depended on. It does not weaken the acyclic invariant — the only edge that
// matters is "no internal/pipeline root import", and that holds.
//
// What deliberately did NOT move: the backup/restore ORCHESTRATION (Backup /
// Restore / ChainRestore / BackupStream / IncrementalBackup and their drive/
// apply loops, the table pools, compaction, prune). Those dispatch through
// the migration-state store, redaction, and shard-stamping — orchestrator
// state, not neutral substrate — so they stay in root. Only the manifest/
// lineage/encryption substrate they lean on is core.
package lineage
