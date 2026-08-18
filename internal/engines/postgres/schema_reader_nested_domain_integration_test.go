//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestSchemaReader_NestedDomain_RefusedLoudly pins Bug 255 HALF 2 (DEFERRED):
// a nested domain — CREATE DOMAIN d2 AS d1, where d1 is itself a domain — is
// refused at schema-read with a message that names it as a NESTED DOMAIN and
// points at the fix, rather than the generic "user-defined type not supported"
// naming the inner domain (which misdirects operators).
//
// The mechanism: information_schema.columns / the pg_type join unwrap a domain
// column exactly ONE level, so the reader sees the immediate base domain
// (dom_int) as the column's udt_name. The reader detects that the immediate
// base is itself a domain (it appears in the domain-checks map, keyed over
// every typtype='d' in the schema) and refuses specifically. Modelling nested
// domains is deferred (it needs per-level base-type metadata the one-level
// unwrap does not supply); the refusal is loud and zero silent loss.
func TestSchemaReader_NestedDomain_RefusedLoudly(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	const ddl = `
		DROP TABLE IF EXISTS nd_t CASCADE;
		DROP DOMAIN IF EXISTS dom_dd;
		DROP DOMAIN IF EXISTS dom_int;
		CREATE DOMAIN dom_int AS integer CHECK (VALUE > 0);
		CREATE DOMAIN dom_dd  AS dom_int CHECK (VALUE < 100);
		CREATE TABLE nd_t (
		  id bigserial PRIMARY KEY,
		  x  dom_dd NOT NULL
		);
	`
	applyDDL(t, dsn, ddl)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	r, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(r)

	_, err = r.ReadSchema(ctx)
	if err == nil {
		t.Fatal("ReadSchema accepted a nested domain (domain-over-domain); Bug 255 HALF 2 expects a loud refusal")
	}
	msg := err.Error()
	// The refusal must name the shape (so operators know it is nesting, not a
	// random unsupported type) and both domain levels.
	for _, want := range []string{"nested domain", "dom_dd", "dom_int"} {
		if !strings.Contains(msg, want) {
			t.Errorf("nested-domain refusal missing %q; got: %v", want, err)
		}
	}
}
