// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"strings"
	"testing"
)

// TestClassifyMissingTargetMySQL pins every branch of the missing-target
// disambiguation: SL-1 (schema-probe error halts, never skips), M-2 (absent
// database is a routing-fault halt), the recoverable C-11 skip, and the PG-1
// sibling (a table that EXISTS but is privilege-invisible must HALT naming the
// privilege, never advance the position past DML for an existing table). Pure
// function. Signature:
// classifyMissingTargetMySQL(schema, table, schemaOK, scErr, tableExists, tableErr).
func TestClassifyMissingTargetMySQL(t *testing.T) {
	const schema, table = "app_eu", "orders"
	probeErr := errors.New("dial tcp 10.0.0.1:3306: i/o timeout")

	// Recoverable skip: database present, table genuinely absent (1146).
	if err := classifyMissingTargetMySQL(schema, table, true, nil, false, nil); !errors.Is(err, errUnknownTargetTable) {
		t.Errorf("present db + absent table must yield the errUnknownTargetTable skip; got %v", err)
	}

	// M-2: absent database -> routing-fault halt, NOT the skip.
	if err := classifyMissingTargetMySQL(schema, table, false, nil, false, nil); errors.Is(err, errUnknownTargetTable) {
		t.Error("absent database must HALT (routing fault), not return the skip sentinel")
	} else if !strings.Contains(err.Error(), "routing fault") {
		t.Errorf("absent-db halt must name the routing fault; got %v", err)
	}

	// SL-1: schema-probe ERROR -> HALT with the probe error, never the skip.
	if err := classifyMissingTargetMySQL(schema, table, false, probeErr, false, nil); errors.Is(err, errUnknownTargetTable) {
		t.Error("SL-1: a schema-probe error must NOT be the skip sentinel (silent-loss)")
	} else if !errors.Is(err, probeErr) {
		t.Errorf("SL-1: a schema-probe error must be propagated; got %v", err)
	}
	// SL-1 belt: a probe error dominates even a spurious schemaOK=true.
	if err := classifyMissingTargetMySQL(schema, table, true, probeErr, false, nil); errors.Is(err, errUnknownTargetTable) || !errors.Is(err, probeErr) {
		t.Errorf("SL-1: a schema-probe error must dominate schemaOK=true and halt; got %v", err)
	}

	// PG-1 sibling: db present, table EXISTS but privilege-invisible (1142) ->
	// HALT naming the privilege, never the skip.
	if err := classifyMissingTargetMySQL(schema, table, true, nil, true, nil); errors.Is(err, errUnknownTargetTable) {
		t.Error("PG-1 sibling: a privilege-invisible existing table must HALT, not skip")
	} else if !strings.Contains(err.Error(), "PRIVILEGE") && !strings.Contains(err.Error(), "privilege") {
		t.Errorf("PG-1 sibling: the halt must name the privilege fault; got %v", err)
	}

	// PG-1 sibling belt: a table access-probe ERROR must HALT, never skip.
	tblErr := errors.New("access probe: read timeout")
	if err := classifyMissingTargetMySQL(schema, table, true, nil, false, tblErr); errors.Is(err, errUnknownTargetTable) {
		t.Error("PG-1 sibling: a table access-probe error must NOT be the skip sentinel")
	} else if !errors.Is(err, tblErr) {
		t.Errorf("PG-1 sibling: a table access-probe error must be propagated; got %v", err)
	}
}
