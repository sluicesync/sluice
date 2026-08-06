// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the roadmap-item-140 pre-copy foreign-key check: the
// refuse/warn/no-op verdict matrix, the two deliberate NON-exemptions
// (--skip-foreign-keys, and a pair with no storage-shape mapping), the
// capability exemption, and the ordering property that matters most —
// the refusal fires before the CREATE and COPY phases run at all.

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// fkGateTable is [gateTable] plus foreign keys — the shape a TARGET
// catalog read returns for a database branched from an existing one.
func fkGateTable(name string, fks []*ir.ForeignKey, cols ...*ir.Column) *ir.Table {
	t := gateTable(name, cols...)
	t.ForeignKeys = fks
	return t
}

func fkTo(name, parent string) *ir.ForeignKey {
	return &ir.ForeignKey{Name: name, Columns: []string{"name"}, ReferencedTable: parent, ReferencedColumns: []string{"id"}}
}

// assertPreexistingFKRefusal pins the coded refusal and the three things
// an operator needs out of it: which child, which constraint, which
// parent. It returns the refusal's HINT (not the *CodedError, which
// errcheck would require every caller to consume) so the remedy
// assertions can live at the call sites that care.
func assertPreexistingFKRefusal(t *testing.T, err error, child, constraint, parent string) string {
	t.Helper()
	if err == nil {
		t.Fatal("want the coded pre-existing-foreign-key refusal; got nil")
	}
	coded, ok := sluicecode.FromError(err)
	if !ok || coded.Code != sluicecode.CodeTargetPreexistingForeignKey {
		t.Fatalf("err = %v; want %s", err, sluicecode.CodeTargetPreexistingForeignKey)
	}
	msg := err.Error()
	for _, want := range []string{child, constraint, parent} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal %q does not name %q", msg, want)
		}
	}
	return coded.Hint
}

// TestMigrateFKGate_VerdictMatrix runs the whole verdict set under every
// registered target-engine family NAME, mirroring the shape gate's
// matrix: the check itself is engine-neutral, but the name is what picks
// the retarget rule (and therefore which branch of plan() the check is
// reached from), so a verdict proven on one name is not proven on the
// others.
func TestMigrateFKGate_VerdictMatrix(t *testing.T) {
	for _, engine := range []string{"mysql", "postgres", "planetscale"} {
		t.Run(engine, func(t *testing.T) {
			// users is the parent, orders the child — the reporter's shape.
			intended := &ir.Schema{Tables: []*ir.Table{
				gateTable("users", gateCols()...),
				gateTable("orders", gateCols()...),
			}}

			t.Run("no target foreign keys leaves the run untouched", func(t *testing.T) {
				logs := captureLogs(t)
				m, _ := gateMigrator(engine, engine, &ir.Schema{Tables: []*ir.Table{
					gateTable("users", gateCols()...),
					gateTable("orders", gateCols()...),
				}})
				if _, err := m.phasePlanExistingTables(context.Background(), intended); err != nil {
					t.Fatalf("a pre-existing target with NO foreign keys must not be refused: %v", err)
				}
				if strings.Contains(logs.String(), "foreign key") {
					t.Errorf("an FK-less target must produce no foreign-key output:\n%s", logs.String())
				}
			})

			t.Run("parent in scope refuses coded before anything moves", func(t *testing.T) {
				m, _ := gateMigrator(engine, engine, &ir.Schema{Tables: []*ir.Table{
					gateTable("users", gateCols()...),
					fkGateTable("orders", []*ir.ForeignKey{fkTo("fk_orders_users", "users")}, gateCols()...),
				}})
				_, err := m.phasePlanExistingTables(context.Background(), intended)
				hint := assertPreexistingFKRefusal(t, err, "orders", "fk_orders_users", "users")
				for _, want := range []string{"--skip-foreign-keys", "--exclude-table", "--reset-target-data"} {
					if !strings.Contains(hint, want) {
						t.Errorf("hint %q missing the remedy %q", hint, want)
					}
				}
				// The load-bearing sentence: the flag operators reach for
				// first does not clear a constraint already on the target.
				if !strings.Contains(hint, "ALONE does not clear this") {
					t.Errorf("hint %q does not say --skip-foreign-keys alone is insufficient", hint)
				}
			})

			t.Run("self-referencing foreign key refuses", func(t *testing.T) {
				// The parent IS the child, so the parent is in scope by
				// construction — an employees.manager_id -> employees.id
				// shape can reject a row referencing one copied later.
				selfRef := &ir.Schema{Tables: []*ir.Table{gateTable("employees", gateCols()...)}}
				m, _ := gateMigrator(engine, engine, &ir.Schema{Tables: []*ir.Table{
					fkGateTable("employees", []*ir.ForeignKey{fkTo("fk_emp_mgr", "employees")}, gateCols()...),
				}})
				_, err := m.phasePlanExistingTables(context.Background(), selfRef)
				assertPreexistingFKRefusal(t, err, "employees", "fk_emp_mgr", "employees")
			})

			t.Run("parent out of scope warns and proceeds", func(t *testing.T) {
				// orders is copied; its parent `customers` is not in this
				// run's scope, so the target's existing customer rows may
				// well satisfy every child. Refusing here would block a
				// working configuration.
				onlyOrders := &ir.Schema{Tables: []*ir.Table{gateTable("orders", gateCols()...)}}
				logs := captureLogs(t)
				m, _ := gateMigrator(engine, engine, &ir.Schema{Tables: []*ir.Table{
					fkGateTable("orders", []*ir.ForeignKey{fkTo("fk_orders_customers", "customers")}, gateCols()...),
				}})
				got, err := m.phasePlanExistingTables(context.Background(), onlyOrders)
				if err != nil {
					t.Fatalf("an out-of-scope parent must NOT be refused: %v", err)
				}
				if got == nil {
					t.Fatal("plan returned a nil schema on the warn path")
				}
				out := logs.String()
				if !strings.Contains(out, "PARENT tables are NOT in scope") {
					t.Errorf("no advisory WARN emitted:\n%s", out)
				}
				for _, want := range []string{"orders", "fk_orders_customers", "customers"} {
					if !strings.Contains(out, want) {
						t.Errorf("advisory WARN does not name %q:\n%s", want, out)
					}
				}
			})

			t.Run("foreign key on an out-of-scope table is ignored", func(t *testing.T) {
				// The target's `audit` table carries an FK, but this run
				// never writes to it, so the constraint cannot fire.
				onlyUsers := &ir.Schema{Tables: []*ir.Table{gateTable("users", gateCols()...)}}
				logs := captureLogs(t)
				m, _ := gateMigrator(engine, engine, &ir.Schema{Tables: []*ir.Table{
					gateTable("users", gateCols()...),
					fkGateTable("audit", []*ir.ForeignKey{fkTo("fk_audit_users", "users")}, gateCols()...),
				}})
				if _, err := m.phasePlanExistingTables(context.Background(), onlyUsers); err != nil {
					t.Fatalf("an FK on a table this run never copies into must be ignored: %v", err)
				}
				if strings.Contains(logs.String(), "fk_audit_users") {
					t.Errorf("an out-of-scope child table's FK leaked into the output:\n%s", logs.String())
				}
			})

			t.Run("unnamed constraint still renders", func(t *testing.T) {
				m, _ := gateMigrator(engine, engine, &ir.Schema{Tables: []*ir.Table{
					gateTable("users", gateCols()...),
					fkGateTable("orders", []*ir.ForeignKey{fkTo("", "users")}, gateCols()...),
				}})
				_, err := m.phasePlanExistingTables(context.Background(), intended)
				assertPreexistingFKRefusal(t, err, "orders", "(unnamed)", "users")
			})
		})
	}
}

// TestMigrateFKGate_SkipForeignKeysIsNotAnExemption is the deliberate
// divergence from the roadmap filing's stated remedy, pinned so it is a
// decision rather than an oversight.
//
// --skip-foreign-keys strips foreign keys from the IR (applySkipForeignKeys)
// so the constraints phase creates none. It issues NO DDL against the
// target, so a constraint the target already carries survives it and
// still rejects the copy — exempting the flag would wave the run through
// to the identical failure. The real-server half of this claim is
// TestMigrate_PreExistingTargetForeignKeys_MySQL's skip-flag leg, which
// reads the constraint back off the target after the refusal.
func TestMigrateFKGate_SkipForeignKeysIsNotAnExemption(t *testing.T) {
	intended := &ir.Schema{Tables: []*ir.Table{
		gateTable("users", gateCols()...),
		gateTable("orders", gateCols()...),
	}}
	m, _ := gateMigrator("mysql", "mysql", &ir.Schema{Tables: []*ir.Table{
		gateTable("users", gateCols()...),
		fkGateTable("orders", []*ir.ForeignKey{fkTo("fk_orders_users", "users")}, gateCols()...),
	}})
	m.SkipForeignKeys = true

	_, err := m.phasePlanExistingTables(context.Background(), intended)
	assertPreexistingFKRefusal(t, err, "orders", "fk_orders_users", "users")
}

// TestMigrateFKGate_RunsOnAPairWithNoStorageShapeMapping pins the other
// non-exemption: mysql→postgres has no retarget rule, so the COLUMN
// compare bails out with a WARN (audit 2026-07-16 HIGH-1). The
// foreign-key check is name-level, not storage-level, and must still
// run — that pair hits the child-before-parent failure exactly as a
// same-engine pair does.
func TestMigrateFKGate_RunsOnAPairWithNoStorageShapeMapping(t *testing.T) {
	intended := &ir.Schema{Tables: []*ir.Table{
		gateTable("users", gateCols()...),
		gateTable("orders", gateCols()...),
	}}
	m, _ := gateMigrator("mysql", "postgres", &ir.Schema{Tables: []*ir.Table{
		gateTable("users", gateCols()...),
		fkGateTable("orders", []*ir.ForeignKey{fkTo("fk_orders_users", "users")}, gateCols()...),
	}})
	_, err := m.phasePlanExistingTables(context.Background(), intended)
	assertPreexistingFKRefusal(t, err, "orders", "fk_orders_users", "users")
}

// TestMigrateFKGate_FKBypassingTargetIsExempt pins the SQLite shape: a
// target whose copy path opens every writable connection with FK
// enforcement OFF cannot fail child-before-parent, so refusing it would
// be a pure false positive. The capability is the engine-neutral way to
// ask; internal/docsync's roster keeps the declarations honest.
func TestMigrateFKGate_FKBypassingTargetIsExempt(t *testing.T) {
	intended := &ir.Schema{Tables: []*ir.Table{
		gateTable("users", gateCols()...),
		gateTable("orders", gateCols()...),
	}}
	m, tgt := gateMigrator("sqlite", "sqlite", &ir.Schema{Tables: []*ir.Table{
		gateTable("users", gateCols()...),
		fkGateTable("orders", []*ir.ForeignKey{fkTo("fk_orders_users", "users")}, gateCols()...),
	}})
	tgt.caps = ir.Capabilities{BulkCopyBypassesForeignKeys: true}

	logs := captureLogs(t)
	if _, err := m.phasePlanExistingTables(context.Background(), intended); err != nil {
		t.Fatalf("a target whose copy bypasses FK enforcement must not be refused: %v", err)
	}
	if strings.Contains(logs.String(), "fk_orders_users") {
		t.Errorf("an exempt target must produce no foreign-key output:\n%s", logs.String())
	}

	// Mutation half: the SAME target with the capability at its zero
	// value IS refused, so the exemption is doing the work rather than
	// the test shape being vacuous.
	m2, _ := gateMigrator("sqlite", "sqlite", &ir.Schema{Tables: []*ir.Table{
		gateTable("users", gateCols()...),
		fkGateTable("orders", []*ir.ForeignKey{fkTo("fk_orders_users", "users")}, gateCols()...),
	}})
	_, err := m2.phasePlanExistingTables(context.Background(), intended)
	assertPreexistingFKRefusal(t, err, "orders", "fk_orders_users", "users")
}

// TestMigrateFKGate_UncomputableCatalogWarnsAndProceeds keeps the gate
// from inventing a new failure mode: a target whose catalog cannot be
// read falls back to today's behaviour, and the WARN says the FK check
// went unrun too (a WARN naming only the shape compare would have let a
// reader outage read as "the FK check passed").
func TestMigrateFKGate_UncomputableCatalogWarnsAndProceeds(t *testing.T) {
	intended := &ir.Schema{Tables: []*ir.Table{gateTable("orders", gateCols()...)}}
	logs := captureLogs(t)
	m, tgt := gateMigrator("mysql", "mysql", nil)
	tgt.readSchemaErr = errors.New("catalog probe refused")

	got, err := m.phasePlanExistingTables(context.Background(), intended)
	if err != nil {
		t.Fatalf("an unreadable target catalog must never fail the run: %v", err)
	}
	if got != intended {
		t.Error("fallback must pass the schema through unchanged")
	}
	if !strings.Contains(logs.String(), "pre-existing-foreign-key check") {
		t.Errorf("the fallback WARN does not disclose that the FK check went unrun:\n%s", logs.String())
	}
}

// TestMigrate_PreExistingForeignKeyRefusalPrecedesEveryWritePhase is the
// ORDERING half of the gate, asserted on phase calls rather than on the
// message: an operator's protection here is worth nothing if the
// refusal arrives after rows have already landed. recordingEngine's
// phaseLog records every schema/row-writer call, so an empty log is
// proof that nothing was created and nothing was written.
func TestMigrate_PreExistingForeignKeyRefusalPrecedesEveryWritePhase(t *testing.T) {
	source := newRecordingEngine("mysql")
	source.schema = &ir.Schema{Tables: []*ir.Table{
		gateTable("users", gateCols()...),
		gateTable("orders", gateCols()...),
	}}
	target := newRecordingEngine("mysql")
	target.schema = &ir.Schema{Tables: []*ir.Table{
		gateTable("users", gateCols()...),
		fkGateTable("orders", []*ir.ForeignKey{fkTo("fk_orders_users", "users")}, gateCols()...),
	}}

	m := &Migrator{Source: source, Target: target, SourceDSN: "src", TargetDSN: "tgt"}
	err := m.Run(context.Background())
	assertPreexistingFKRefusal(t, err, "orders", "fk_orders_users", "users")

	if len(target.phaseLog) != 0 {
		t.Errorf("target phases ran before the refusal: %v (the refusal must precede every write phase)", target.phaseLog)
	}
	if len(target.createdTables) != 0 {
		t.Errorf("tables created before the refusal: %v", target.createdTables)
	}
	// The row WRITER is opened before the gate — the populated-target
	// preflight needs it — so openRowWriterCalls is deliberately not
	// asserted here. What matters is that nothing was written THROUGH
	// it, which the empty phaseLog above is the evidence for. The real
	// server-side proof (zero rows on the target after the refusal) is
	// TestMigrate_PreExistingTargetForeignKeys_MySQL.

	// Mutation half, in-test: the same run against a target carrying NO
	// foreign key must reach the write phases, so an empty phaseLog is
	// evidence of the refusal rather than of a Migrator that never runs
	// under this fixture.
	clean := newRecordingEngine("mysql")
	clean.schema = &ir.Schema{Tables: []*ir.Table{
		gateTable("users", gateCols()...),
		gateTable("orders", gateCols()...),
	}}
	ok := &Migrator{Source: newRecordingEngine("mysql"), Target: clean, SourceDSN: "src", TargetDSN: "tgt"}
	ok.Source.(*recordingEngine).schema = source.schema
	if err := ok.Run(context.Background()); err != nil {
		t.Fatalf("the FK-less control run must succeed: %v", err)
	}
	if len(clean.phaseLog) == 0 {
		t.Error("the control run recorded no phases at all; the empty phaseLog above proves nothing")
	}
}
