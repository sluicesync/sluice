// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"strings"
	"testing"
)

// TestClassifyMissingTargetPostgres_SL1 pins the audit-SL-1 branch on the PG
// applier lane (the sibling the prior audit reviewed lightly): a
// schema-existence PROBE ERROR must HALT (propagate), never be classified as
// the errUnknownTable skip sentinel — a skip would advance the position past
// the dropped event, a silent loss on a transient deadline/reset.
func TestClassifyMissingTargetPostgres_SL1(t *testing.T) {
	const schema, table = "app_eu", "orders"
	probeErr := errors.New("read tcp 10.0.0.1:5432: connection reset by peer")

	// Probe SUCCEEDED, schema present -> the recoverable skip sentinel.
	if err := classifyMissingTargetPostgres(schema, table, true, nil); !errors.Is(err, errUnknownTable) {
		t.Errorf("present schema must yield the errUnknownTable skip; got %v", err)
	}

	// Probe SUCCEEDED, schema absent -> a loud routing-fault halt, NOT the skip.
	if err := classifyMissingTargetPostgres(schema, table, false, nil); errors.Is(err, errUnknownTable) {
		t.Error("absent schema must HALT (routing fault), not return the skip sentinel")
	} else if !strings.Contains(err.Error(), "routing fault") {
		t.Errorf("absent-schema halt must name the routing fault; got %v", err)
	}

	// SL-1: probe ERROR -> HALT with the probe error, NEVER the skip sentinel.
	if err := classifyMissingTargetPostgres(schema, table, false, probeErr); errors.Is(err, errUnknownTable) {
		t.Error("SL-1: a probe error must NOT be the skippable-missing-table sentinel (skipping advances the position past dropped events — silent loss)")
	} else if !errors.Is(err, probeErr) {
		t.Errorf("SL-1: a probe error must be propagated (wrapped); got %v", err)
	}

	// SL-1 belt: an error dominates even a (spurious) schemaOK=true.
	if err := classifyMissingTargetPostgres(schema, table, true, probeErr); errors.Is(err, errUnknownTable) || !errors.Is(err, probeErr) {
		t.Errorf("SL-1: a probe error must dominate schemaOK=true and halt; got %v", err)
	}
}
