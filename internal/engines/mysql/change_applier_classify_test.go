// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"strings"
	"testing"
)

// TestClassifyMissingTargetMySQL_SL1 pins the audit-SL-1 branch: a
// schema-existence PROBE ERROR must HALT (propagate the error), never be
// classified as the errUnknownTargetTable skip sentinel — classifying it as a
// skip would advance the CDC stream position past the dropped event, a silent
// loss on a transient deadline/reset that re-opens the exact window M-2 closed.
// The three non-error branches are pinned alongside so the whole decision is
// covered, not just the representative.
func TestClassifyMissingTargetMySQL_SL1(t *testing.T) {
	const schema, table = "app_eu", "orders"
	probeErr := errors.New("dial tcp 10.0.0.1:3306: i/o timeout")

	// Probe SUCCEEDED, schema present -> the recoverable skip sentinel.
	if err := classifyMissingTargetMySQL(schema, table, true, nil); !errors.Is(err, errUnknownTargetTable) {
		t.Errorf("present schema must yield the errUnknownTargetTable skip; got %v", err)
	}

	// Probe SUCCEEDED, schema absent -> a loud routing-fault halt, NOT the skip.
	if err := classifyMissingTargetMySQL(schema, table, false, nil); errors.Is(err, errUnknownTargetTable) {
		t.Error("absent schema must HALT (routing fault), not return the skip sentinel")
	} else if !strings.Contains(err.Error(), "routing fault") {
		t.Errorf("absent-schema halt must name the routing fault; got %v", err)
	}

	// SL-1: probe ERROR -> HALT with the probe error, NEVER the skip sentinel.
	if err := classifyMissingTargetMySQL(schema, table, false, probeErr); errors.Is(err, errUnknownTargetTable) {
		t.Error("SL-1: a probe error must NOT be the skippable-missing-table sentinel (skipping advances the position past dropped events — silent loss)")
	} else if !errors.Is(err, probeErr) {
		t.Errorf("SL-1: a probe error must be propagated (wrapped); got %v", err)
	}

	// SL-1 belt: an error dominates even a (spurious) schemaOK=true.
	if err := classifyMissingTargetMySQL(schema, table, true, probeErr); errors.Is(err, errUnknownTargetTable) || !errors.Is(err, probeErr) {
		t.Errorf("SL-1: a probe error must dominate schemaOK=true and halt; got %v", err)
	}
}
