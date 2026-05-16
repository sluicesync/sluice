// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package jsonbench is a build-tagged micro-benchmark + fidelity harness
// that compares JSON encode/decode libraries against sluice's Phase 1
// backup chunk record shapes (the tagged-value envelope and the CDC
// change wrapper — see `internal/pipeline/backup_chunk.go` and
// `internal/pipeline/backup_change_chunk.go`).
//
// It exists to inform one decision: should the backup chunk path keep
// stdlib `encoding/json` (what ships today — see
// `internal/pipeline/backup_chunk.go` / `backup_change_chunk.go`), or is
// a faster library a compelling replacement for the DR-critical restore
// (decode) axis? Speed is secondary to correctness here: a library that
// is lossy or semantically divergent on the sluice tagged-value envelope
// is DISQUALIFIED regardless of throughput (the loud-fail / value-types
// correctness tenet — see `docs/value-types.md`).
//
// Why a separate build tag (`jsonbench`, parallel to `compressbench`):
// the harness pulls in `github.com/go-json-experiment/json` (the
// importable upstream prototype of `encoding/json/v2`),
// `github.com/goccy/go-json` (already in the module graph as an indirect
// dep), and — amd64 only — `github.com/bytedance/sonic`. The default
// build doesn't need any of it; including it would bloat the production
// binary for no operator-visible reason and is its own cost surface
// (sonic in particular pulls a JIT assembler).
//
// Running locally (default ~50k rows/corpus, ~30s):
//
//	go test -tags=jsonbench -run TestRunAllAndEmit \
//	    ./internal/pipeline/internal/jsonbench/
//
// Decision-grade 1M-row pass, emitting the markdown report to a file:
//
//	SLUICE_JSONBENCH_ROWS=1000000 \
//	SLUICE_JSONBENCH_OUT=C:\Temp\json-benchmark.md \
//	go test -tags=jsonbench -timeout=30m -run TestRunAllAndEmit \
//	    ./internal/pipeline/internal/jsonbench/
//
// The harness records encode + decode wall time (warm median of 5, one
// discarded warm-up), allocs/op, B/op, and a coarse heap-delta proxy.
// The fidelity gate (TestFidelityGate) is a real failing test, not a
// printf: every candidate must reproduce the sluice envelope bit-exact /
// semantically identical before any speed number is reported for it.
package jsonbench
