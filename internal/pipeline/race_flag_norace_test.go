// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

//go:build integration && !race

package pipeline

// testRaceEnabled — see race_flag_race_test.go. False when built without -race,
// where wall-clock timing comparisons are meaningful.
const testRaceEnabled = false
