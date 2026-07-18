// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

//go:build integration && race

package pipeline

// testRaceEnabled reports whether the test binary was built with -race. The
// race detector serializes memory access and adds large per-access overhead,
// which erases the wall-clock speedup of a parallel path on a shared CI
// runner — so timing-comparison assertions (e.g. "parallel is faster than
// serial") are meaningless under -race and are skipped, keeping only the
// correctness (zero-loss) assertions. Build-tag pair with race_flag_norace_test.go.
const testRaceEnabled = true
