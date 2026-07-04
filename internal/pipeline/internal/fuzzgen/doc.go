// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package fuzzgen is the pure (container-free) half of the generative
// round-trip migrate fuzz harness (Track 2, Phase 1 — design contract
// at docs/dev/notes/prep-generative-roundtrip-fuzz-harness.md): the
// type-family registry (registry.go), the deterministic schema+data
// generator (generator.go), and the three-outcome oracle (oracle.go).
// The integration driver that boots real databases and runs Migrator
// against the generated cases stays in the pipeline package
// (migrate_fuzz_roundtrip_integration_test.go, `integration` tag),
// consuming the exported surface: EngineKind / Direction /
// AllDirections, GenerateCase / Case, ExpectationFor / Classify /
// FaithfulColumnsFor / RowCountKey / Verdict.
//
// It lives under internal/pipeline/internal (the jsonbench /
// compressbench precedent) rather than in the pipeline package itself
// because it is test tooling that carries per-dialect DDL-emission
// strings: neither belongs in the shipped orchestrator package's
// compile unit (repo-audit 2026-07-03 finding A-3). Nothing here is
// reachable from cmd/sluice; the package must never be imported by
// production pipeline code.
package fuzzgen
