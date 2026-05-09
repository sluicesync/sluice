// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// Unit tests for the SetSchema / SetSourceDSNFingerprint surfaces
// added under ADR-0031 (multi-source aggregation `--target-schema`).
// These tests exercise the in-memory state mutations only — the
// integration tests in internal/pipeline/migrate_pg_integration_test.go
// cover the full PG → PG round-trip with target-schema namespacing.

import (
	"strings"
	"testing"
)

func TestSchemaWriter_SetSchema(t *testing.T) {
	t.Run("empty input is no-op", func(t *testing.T) {
		w := &SchemaWriter{schema: "public", schemaEnsured: true}
		w.SetSchema("")
		if w.schema != "public" {
			t.Errorf("schema = %q; want unchanged 'public'", w.schema)
		}
		if !w.schemaEnsured {
			t.Errorf("schemaEnsured = false; want unchanged true")
		}
	})

	t.Run("override sets schema and clears ensured flag", func(t *testing.T) {
		w := &SchemaWriter{schema: "public", schemaEnsured: true}
		w.SetSchema("customer_svc")
		if w.schema != "customer_svc" {
			t.Errorf("schema = %q; want customer_svc", w.schema)
		}
		if w.schemaEnsured {
			t.Errorf("schemaEnsured = true; want false (must re-ensure new schema)")
		}
	})

	t.Run("override to same name preserves ensured flag", func(t *testing.T) {
		w := &SchemaWriter{schema: "customer_svc", schemaEnsured: true}
		w.SetSchema("customer_svc")
		if w.schema != "customer_svc" {
			t.Errorf("schema = %q; want customer_svc", w.schema)
		}
		if !w.schemaEnsured {
			t.Errorf("schemaEnsured = false; want preserved true on no-change set")
		}
	})
}

func TestSchemaReader_SetSchema(t *testing.T) {
	t.Run("empty input is no-op", func(t *testing.T) {
		r := &SchemaReader{schema: "public"}
		r.SetSchema("")
		if r.schema != "public" {
			t.Errorf("schema = %q; want unchanged 'public'", r.schema)
		}
	})

	t.Run("override sets schema", func(t *testing.T) {
		r := &SchemaReader{schema: "public"}
		r.SetSchema("customer_svc")
		if r.schema != "customer_svc" {
			t.Errorf("schema = %q; want customer_svc", r.schema)
		}
	})
}

func TestRowReader_SetSchema(t *testing.T) {
	t.Run("override sets schema", func(t *testing.T) {
		r := &RowReader{schema: "public"}
		r.SetSchema("customer_svc")
		if r.schema != "customer_svc" {
			t.Errorf("schema = %q; want customer_svc", r.schema)
		}
	})

	t.Run("empty input is no-op", func(t *testing.T) {
		r := &RowReader{schema: "public"}
		r.SetSchema("")
		if r.schema != "public" {
			t.Errorf("schema = %q; want unchanged 'public'", r.schema)
		}
	})
}

func TestRowWriter_SetSchema(t *testing.T) {
	t.Run("override sets schema", func(t *testing.T) {
		w := &RowWriter{schema: "public"}
		w.SetSchema("customer_svc")
		if w.schema != "customer_svc" {
			t.Errorf("schema = %q; want customer_svc", w.schema)
		}
	})

	t.Run("empty input is no-op", func(t *testing.T) {
		w := &RowWriter{schema: "public"}
		w.SetSchema("")
		if w.schema != "public" {
			t.Errorf("schema = %q; want unchanged 'public'", w.schema)
		}
	})
}

func TestChangeApplier_SetSchema_ControlSchemaPinned(t *testing.T) {
	// The split between user-data schema (mutable) and control-table
	// schema (pinned at construction) is the load-bearing invariant
	// for ADR-0031: per-source target-schema overrides must NOT move
	// sluice_cdc_state out of the DSN's default schema.
	a := &ChangeApplier{schema: "public", controlSchema: "public"}
	a.SetSchema("customer_svc")
	if a.schema != "customer_svc" {
		t.Errorf("user-data schema = %q; want customer_svc", a.schema)
	}
	if a.controlSchema != "public" {
		t.Errorf("controlSchema = %q; want pinned 'public'", a.controlSchema)
	}
}

func TestChangeApplier_SetSourceDSNFingerprint(t *testing.T) {
	a := &ChangeApplier{}
	a.SetSourceDSNFingerprint("abcd1234ef56")
	if a.sourceFingerprint != "abcd1234ef56" {
		t.Errorf("sourceFingerprint = %q; want abcd1234ef56", a.sourceFingerprint)
	}

	// Idempotent re-set with a new value (warm resume on a new
	// streamer instance with the same source DSN — fingerprint stays
	// identical, so this exercises that path).
	a.SetSourceDSNFingerprint("9876fedc5432")
	if a.sourceFingerprint != "9876fedc5432" {
		t.Errorf("sourceFingerprint = %q; want updated value", a.sourceFingerprint)
	}

	// Empty input updates to empty (caller is responsible for
	// computing the fingerprint; the recorder is a passthrough).
	a.SetSourceDSNFingerprint("")
	if a.sourceFingerprint != "" {
		t.Errorf("sourceFingerprint = %q; want empty after empty set", a.sourceFingerprint)
	}
}

// TestEnumTypeName_PreservesShape pins the enum-type naming policy
// so a future refactor doesn't accidentally break the schema-prefix
// collision-prevention. Per ADR-0031, multi-source operators rely on
// the schema to namespace types across sources — the type-name
// portion stays bare and the writer's emit qualifies it with the
// configured schema.
func TestEnumTypeName_PreservesShape(t *testing.T) {
	got := enumTypeName("accounts", "status")
	want := "accounts_status_enum"
	if got != want {
		t.Errorf("enumTypeName = %q; want %q", got, want)
	}
}

// TestEmitCreateEnumType_QualifiesWithSchema verifies that
// CREATE TYPE statements include the schema qualifier — the
// load-bearing invariant for ADR-0031's "two sources both have
// accounts.status enum" collision avoidance.
func TestEmitCreateEnumType_QualifiesWithSchema(t *testing.T) {
	got := emitCreateEnumType("customer_svc", "accounts", "status", []string{"active", "inactive"})
	wantSubstr := `CREATE TYPE "customer_svc"."accounts_status_enum"`
	if !strings.Contains(got, wantSubstr) {
		t.Errorf("emitCreateEnumType = %q; want to contain %q", got, wantSubstr)
	}
}
