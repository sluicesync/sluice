// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package compressbench is a build-tagged micro-benchmark harness that
// compares compression algorithms against sluice's Phase 1 backup chunk
// shape (JSON Lines of tagged-value envelopes — see
// `internal/pipeline/backup_chunk.go`).
//
// It exists to inform the Phase 1 → Phase 2 swap question — Phase 1
// shipped stdlib gzip because correctness mattered most; Phase 2 will
// pick the lowest-cost-of-ownership algorithm for the four corpus
// shapes operators actually run.
//
// Why a separate build tag (`compressbench`): the harness pulls in
// `github.com/klauspost/compress` directly (already in the module
// graph as an indirect dependency of pgx, so no new dep cost) and a
// few hundred lines of corpus generators. The default build doesn't
// need any of it — including it would bloat the production binary
// for no operator-visible reason.
//
// Running locally:
//
//	go test -tags=compressbench -bench=. -benchtime=3x \
//	    ./internal/pipeline/internal/compressbench/
//
// The harness records compressed-size, encode and decode CPU time,
// and (when `runtime.GC()`-bracketed) peak allocator pressure. The
// markdown emitter prints a table the decision doc renders verbatim.
//
// Outputs feed `docs/dev/notes/compression-benchmark.md` — the
// shipped decision artefact.
package compressbench
