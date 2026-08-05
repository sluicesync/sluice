//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// End-to-end pins for the schema-diff blind spots closed in the
// B-10 / C-7 arc, against a REAL Postgres catalog on both sides.
//
// # Why these exist when the unit tests already pass
//
// The unit pins in internal/ir/diff run against hand-built IR. They prove
// the COMPARISON is right and say nothing about whether the schema reader
// populates the fields being compared — and a reader that left
// RLSEnabled at its zero value on both sides would make the comparison
// see false-vs-false and report "in sync" with every unit test still
// green. Two facts each pinned, with nothing binding them.
//
// These bind them: source and target are both read by the real
// ir.SchemaReader, and the drift is introduced with real DDL against the
// target. The independent expected value is the DDL this test issues —
// the diff never sees it, so agreement is evidence rather than a
// tautology.

package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestDiff_RowLevelSecurityAndSequenceDrift_Postgres is the B-10 case
// end to end: a target whose row-level security was switched off and
// whose standalone sequence was re-optioned used to report "in sync",
// exit 0.
func TestDiff_RowLevelSecurityAndSequenceDrift_Postgres(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	// Identical structure on both sides EXCEPT the three things under
	// test, so any reported drift can only have come from them.
	const commonDDL = `
		CREATE TABLE tenants (
			id BIGINT NOT NULL PRIMARY KEY
		);
		CREATE TABLE orders (
			id        BIGINT NOT NULL PRIMARY KEY,
			tenant_id BIGINT NOT NULL
		);
	`
	const sourceDDL = commonDDL + `
		ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
		ALTER TABLE orders FORCE ROW LEVEL SECURITY;
		CREATE POLICY tenant_isolation ON orders
			FOR ALL TO PUBLIC
			USING (tenant_id = 1)
			WITH CHECK (tenant_id = 1);
		ALTER TABLE orders
			ADD CONSTRAINT fk_orders_tenant FOREIGN KEY (tenant_id)
			REFERENCES tenants (id) ON DELETE RESTRICT;
		CREATE SEQUENCE order_number_seq START WITH 1000 INCREMENT BY 5 MAXVALUE 999999;
	`
	// The target: RLS never enabled (so the policy is absent too), the FK
	// re-declared with a CASCADE that silently destroys child rows, and
	// the sequence left at PG's defaults.
	const targetDDL = commonDDL + `
		ALTER TABLE orders
			ADD CONSTRAINT fk_orders_tenant FOREIGN KEY (tenant_id)
			REFERENCES tenants (id) ON DELETE CASCADE;
		CREATE SEQUENCE order_number_seq;
	`
	applyPGDDL(t, sourceDSN, sourceDDL)
	applyPGDDL(t, targetDSN, targetDDL)

	pg, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	var buf bytes.Buffer
	d := &Differ{
		Source: pg, Target: pg,
		SourceDSN: sourceDSN, TargetDSN: targetDSN,
		Format: "json", Out: &buf,
	}
	diff, err := d.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if diff == nil || !diff.HasChanges() {
		t.Fatalf("a target with RLS off, a CASCADE'd FK and a reset sequence reported IN SYNC: %+v", diff)
	}

	var got DiffJSON
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("decode JSON: %v\n%s", err, buf.String())
	}

	// Each assertion names what the DDL above did, which the diff never saw.
	if got.Summary.RLSMismatched != 1 {
		t.Errorf("summary.rls_mismatched = %d; want 1 — the target never ran ENABLE ROW LEVEL SECURITY",
			got.Summary.RLSMismatched)
	}
	if got.Summary.PoliciesMissing != 1 {
		t.Errorf("summary.policies_missing = %d; want 1 (tenant_isolation)", got.Summary.PoliciesMissing)
	}
	if got.Summary.SequencesMismatched != 1 {
		t.Errorf("summary.sequences_mismatched = %d; want 1 — target sequence took PG's defaults, not START 1000 INCREMENT 5",
			got.Summary.SequencesMismatched)
	}
	if got.Summary.ForeignKeysMismatched != 1 {
		t.Errorf("summary.foreign_keys_mismatched = %d; want 1 (RESTRICT vs CASCADE)",
			got.Summary.ForeignKeysMismatched)
	}

	// The reader really did populate the RLS flags — the premise the unit
	// pins assume. Reading it off the diff rather than asserting the
	// reader directly keeps this a binding check.
	for _, td := range diff.TablesMismatched {
		if td.Name != "orders" {
			continue
		}
		if !td.ExpectedRLSEnabled {
			t.Error("expected_rls_enabled = false on the SOURCE side — the PG reader did not populate RLSEnabled, so the comparison would compare false-vs-false and report in sync")
		}
		if !td.ExpectedRLSForced {
			t.Error("expected_rls_forced = false on the SOURCE side — the PG reader did not populate RLSForced")
		}
		if td.ActualRLSEnabled {
			t.Error("actual_rls_enabled = true; the target never enabled RLS")
		}
	}

	// And the referential actions render as KEYWORDS, not as the raw
	// uint8 code points the v0.112.0 conversion produced. This is the one
	// assertion that ground-truths the C-7 symptom-3 fix against a real
	// catalog read rather than a constructed FKAction.
	for _, td := range diff.TablesMismatched {
		for _, fd := range td.ForeignKeysMismatched {
			if fd.ExpectedOnDelete != "RESTRICT" || fd.ActualOnDelete != "CASCADE" {
				t.Errorf("on-delete pair = %q/%q; want RESTRICT/CASCADE", fd.ExpectedOnDelete, fd.ActualOnDelete)
			}
		}
	}

	// The text render must SAY all of it. Exit 1 with an empty section is
	// the C-7 defect restated.
	buf.Reset()
	d.Format = "text"
	if _, err := d.Run(ctx); err != nil {
		t.Fatalf("Run (text): %v", err)
	}
	out := buf.String()
	for _, want := range []string{
		"EVERY ROW IS VISIBLE TO EVERY ROLE",
		"tenant_isolation",
		"SILENTLY DESTROYS child rows",
		"PRODUCES DIFFERENT VALUES",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("text render missing %q:\n%s", want, out)
		}
	}
}

// TestDiff_IdenticalRLSAndSequences_Postgres is the must-not-break
// control: the same DDL on both sides stays in-sync, so none of the new
// comparisons leaks a false positive into a CI gate. Without this, a
// comparison that reported drift unconditionally would pass every
// assertion in the test above.
func TestDiff_IdenticalRLSAndSequences_Postgres(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	const ddl = `
		CREATE TABLE tenants (
			id BIGINT NOT NULL PRIMARY KEY
		);
		CREATE TABLE orders (
			id        BIGINT NOT NULL PRIMARY KEY,
			tenant_id BIGINT NOT NULL
		);
		ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
		ALTER TABLE orders FORCE ROW LEVEL SECURITY;
		CREATE POLICY tenant_isolation ON orders
			FOR ALL TO PUBLIC
			USING (tenant_id = 1)
			WITH CHECK (tenant_id = 1);
		ALTER TABLE orders
			ADD CONSTRAINT fk_orders_tenant FOREIGN KEY (tenant_id)
			REFERENCES tenants (id) ON DELETE RESTRICT;
		CREATE SEQUENCE order_number_seq START WITH 1000 INCREMENT BY 5 MAXVALUE 999999;
	`
	applyPGDDL(t, sourceDSN, ddl)
	applyPGDDL(t, targetDSN, ddl)

	pg, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	var buf bytes.Buffer
	d := &Differ{
		Source: pg, Target: pg,
		SourceDSN: sourceDSN, TargetDSN: targetDSN,
		Format: "text", Out: &buf,
	}
	diff, err := d.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if diff.HasChanges() {
		t.Fatalf("identical schemas reported drift (%s):\n%s", diff.Summary(), buf.String())
	}

	// The sequence POSITION differs the moment either side calls nextval,
	// and SequenceDiff documents that as deliberately uncompared. Advance
	// the source's and confirm the pair still reads in-sync — a diff that
	// cried drift on every healthy run would be suppressed, and a
	// suppressed check hides the structural drift it exists to report.
	applyPGDDL(t, sourceDSN, `SELECT nextval('order_number_seq'); SELECT nextval('order_number_seq');`)
	buf.Reset()
	diff, err = d.Run(ctx)
	if err != nil {
		t.Fatalf("Run after nextval: %v", err)
	}
	if diff.HasChanges() {
		t.Errorf("advancing the source sequence reported drift (%s) — SequenceDiff documents position as uncompared:\n%s",
			diff.Summary(), buf.String())
	}
}
