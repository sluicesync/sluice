// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package backup is the backup / restore / chain-lineage orchestration
// domain carved OUT of the flat internal/pipeline root (audit 3.7b-2 /
// Theme 5).
//
// It holds the domain-specific orchestrators the CLI and the broker drive:
//
//   - Backup — the full logical-backup writer (snapshot → chunked COPY →
//     manifest), with its parallel per-table pool and within-table chunking;
//   - Restore — the manifest-driven restore (schema → chunked apply →
//     index/constraint create → reparent reconciliation), with its parallel
//     table/chunk pools and headroom clamp;
//   - ChainRestore — the lineage-chain replay that stitches a base backup and
//     its incremental links into a point-in-time target;
//   - CompactChain / PruneChain — the chain-maintenance operations (segment
//     compaction, smart same-key compaction, retention prune);
//   - VerifyBackupWith — the restore-parity / dump-parity verifier and its
//     allowlist.
//
// The load-bearing invariant: this package MUST NEVER import
// internal/pipeline root. It consumes the shared substrate DOWN —
// internal/pipeline/{migcore,lineage,blobcodec} — plus the typed IR
// (internal/ir, internal/ir/backup), internal/crypto, internal/translate,
// internal/redact, internal/appliercontrol, engine-neutral leaves, stdlib,
// and third-party libraries. Root (broker.go, stream_rotation.go,
// incremental.go, cmd/sluice) imports THIS package to construct the entry
// types; that one-directional edge (root → backup, never the reverse) is what
// the whole 3.7b arc existed to establish: the manifest-IO + lineage-catalog
// substrate that used to weave the cluster and root bidirectionally was pulled
// DOWN into internal/pipeline/lineage in 3.7b-0, the shared migrate/copy
// primitives into migcore in 3.7b-1, and the reparent/reconcile/chain-preflight
// leaves into migcore in the 3.7b-2 pre-lift — leaving this cluster
// self-contained enough to lift wholesale.
package backup
