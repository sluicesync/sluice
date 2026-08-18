// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"strings"
	"testing"
)

// TestClassifyMissingTargetPostgres pins every branch of the three-way
// missing-target disambiguation: SL-1 (probe error halts, never skips), M-2
// (absent schema is a routing-fault halt), the recoverable C-11 skip, and PG-1
// (a table that EXISTS in the catalog but is privilege-invisible must HALT
// naming the privilege, never advance the position past DML for an existing
// table). Pure function, so no server needed. Signature:
// classifyMissingTargetPostgres(schema, table, schemaOK, scErr, tableInCatalog, catErr).
func TestClassifyMissingTargetPostgres(t *testing.T) {
	const schema, table = "app_eu", "orders"
	probeErr := errors.New("read tcp 10.0.0.1:5432: connection reset by peer")

	// Recoverable skip: schema present, table genuinely absent from the catalog.
	if err := classifyMissingTargetPostgres(schema, table, true, nil, false, nil); !errors.Is(err, errUnknownTable) {
		t.Errorf("present schema + absent table must yield the errUnknownTable skip; got %v", err)
	}

	// M-2: absent schema -> routing-fault halt, NOT the skip.
	if err := classifyMissingTargetPostgres(schema, table, false, nil, false, nil); errors.Is(err, errUnknownTable) {
		t.Error("absent schema must HALT (routing fault), not return the skip sentinel")
	} else if !strings.Contains(err.Error(), "routing fault") {
		t.Errorf("absent-schema halt must name the routing fault; got %v", err)
	}

	// SL-1: schema-probe ERROR -> HALT with the probe error, NEVER the skip.
	if err := classifyMissingTargetPostgres(schema, table, false, probeErr, false, nil); errors.Is(err, errUnknownTable) {
		t.Error("SL-1: a schema-probe error must NOT be the skip sentinel (silent-loss)")
	} else if !errors.Is(err, probeErr) {
		t.Errorf("SL-1: a schema-probe error must be propagated; got %v", err)
	}
	// SL-1 belt: a probe error dominates even a spurious schemaOK=true.
	if err := classifyMissingTargetPostgres(schema, table, true, probeErr, false, nil); errors.Is(err, errUnknownTable) || !errors.Is(err, probeErr) {
		t.Errorf("SL-1: a schema-probe error must dominate schemaOK=true and halt; got %v", err)
	}

	// PG-1: schema present, catalog SHOWS the table (privilege-invisible) ->
	// HALT naming the privilege, never the skip.
	if err := classifyMissingTargetPostgres(schema, table, true, nil, true, nil); errors.Is(err, errUnknownTable) {
		t.Error("PG-1: a catalog-visible table must HALT (privilege fault), not skip an EXISTING table")
	} else if !strings.Contains(err.Error(), "PRIVILEGE") && !strings.Contains(err.Error(), "privilege") {
		t.Errorf("PG-1: the halt must name the privilege fault; got %v", err)
	}

	// PG-1 belt: a catalog-probe ERROR must HALT (refuse to guess), never skip.
	catErr := errors.New("to_regclass: statement timeout")
	if err := classifyMissingTargetPostgres(schema, table, true, nil, false, catErr); errors.Is(err, errUnknownTable) {
		t.Error("PG-1: a catalog-probe error must NOT be the skip sentinel")
	} else if !errors.Is(err, catErr) {
		t.Errorf("PG-1: a catalog-probe error must be propagated; got %v", err)
	}
}
