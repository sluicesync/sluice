// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package migcore is the engine-neutral shared migration core carved
// DOWN out of the internal/pipeline root (audit A-1 / Theme 5).
//
// It holds the pure, orchestration-free pieces that the migrate,
// streamer, backup, and restore orchestrators all consume: chunk-
// boundary math, the copy-parallelism/connection-budget/AIMD-backoff
// primitives, the cold-copy grow-gate coordinator, the structural
// schema-delta diff and cross-engine supportability checks, the
// operator-facing error-hint layer, the run-summary collector, the
// reparent-touched-table tracker, the shared TableFilter config type,
// the target-schema / table-scope / verbatim-passthrough configurators,
// the per-row redaction apply, the apply-concurrency + headroom
// resolvers, the idempotent DDL-phase / views-phase reparent-retry
// drivers, and the copy-path leaf helpers (CloseIf, ReaderStreamErr,
// ReadChunkBatch, PKTracker, RowChanBuffer, DefaultBulkBatchSize).
//
// The package imports ONLY the typed IR (internal/ir, internal/ir/backup),
// internal/translate, internal/sluicecode, internal/redact,
// internal/appliercontrol, stdlib, and third-party libraries — NEVER
// internal/pipeline root. internal/redact (RedactRow) and
// internal/appliercontrol (HeadroomDivisor's telemetry thresholds) are
// both verified clean leaves — they import nothing from internal/pipeline
// — so adding those edges keeps migcore acyclic (audit 3.7b-1). That
// one-directional edge (pipeline-root → migcore, never the reverse) is
// the load-bearing property: it breaks the bidirectional root↔backup
// cycle so the backup and restore domains can be carved out of the root
// in a later chunk (3.7b-2) as clean migcore consumers.
//
// What deliberately did NOT move: the copy ORCHESTRATION (copyChunk /
// runChunks / resolveChunks / runBulkCopyTablePool / bulkCopyOneTable and
// friends). Those dispatch through resumeContext, the migration-state
// store, redaction, and shard-stamping — orchestrator state, not neutral
// core — so they stay in root and would force a root import if moved. Only
// the chunk-BOUNDARY math is core; the chunk-COPY loop is orchestration.
package migcore
