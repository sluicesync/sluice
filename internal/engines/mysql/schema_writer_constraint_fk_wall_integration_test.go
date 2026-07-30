//go:build integration

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

// TestCreateConstraints_FKMetadataOnlyAndNoLeak_Integration is the real-InnoDB
// proof for the roadmap-item-109 FK statement-wall recovery (option B). A
// PlanetScale/Vitess statement-time wall can't be reproduced in a testcontainer,
// so this pins the RECOVERY ACTION and its correctness invariants directly
// against real MySQL:
//
//  1. addForeignKeyMetadataOnly adds a foreign key over a child table that
//     holds an ORPHAN row — proving `SET SESSION foreign_key_checks=0` makes
//     the ADD a metadata-only change that skips InnoDB's child-row validation
//     (the same behaviour proven live on PlanetScale: 0.082 s, real FK created).
//  2. Connection-scoping / no-leak: with the pool pinned to ONE connection, a
//     SUBSEQUENT ordinary validating ADD on a DIFFERENT orphaned child — run
//     through the normal (disarmed) CreateConstraints path — FAILS loudly with
//     errno 1452. That is only possible if foreign_key_checks was restored to 1
//     on the shared connection; a leak of the relaxed setting would have let
//     the orphan through silently. This is the load-bearing safety pin: the
//     recovery must never disable validation for a FK it was not recovering.
func TestCreateConstraints_FKMetadataOnlyAndNoLeak_Integration(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "item109_fk_wall")
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	seed, err := sql.Open("mysql", dsn+"&multiStatements=true")
	if err != nil {
		t.Fatalf("sql.Open seed: %v", err)
	}
	defer closeIf(seed)

	// parent + two children, each child carrying an ORPHAN row (a customer_id
	// with no matching parent) so a VALIDATING ADD FOREIGN KEY would fail 1452.
	const ddl = `
CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY);
CREATE TABLE events   (id BIGINT NOT NULL PRIMARY KEY, customer_id BIGINT NOT NULL, KEY (customer_id));
CREATE TABLE orders   (id BIGINT NOT NULL PRIMARY KEY, customer_id BIGINT NOT NULL, KEY (customer_id));
INSERT INTO customers (id) VALUES (1);
INSERT INTO events (id, customer_id) VALUES (1, 999);
INSERT INTO orders (id, customer_id) VALUES (1, 888);`
	if _, err := seed.ExecContext(ctx, ddl); err != nil {
		t.Fatalf("seed DDL/data: %v", err)
	}

	swHandle, err := Engine{}.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer closeIf(swHandle)
	sw := swHandle.(*SchemaWriter)

	// Pin the pool to ONE connection so the no-leak assertion is deterministic:
	// the metadata-only ADD and the later validating ADD share the SAME physical
	// connection, so a failure to restore foreign_key_checks would be observable.
	sw.db.SetMaxOpenConns(1)

	// (1) Metadata-only ADD over the orphaned `events` child — must succeed.
	eventsFK := &ir.ForeignKey{
		Name:              "events_customer_fk",
		Columns:           []string{"customer_id"},
		ReferencedTable:   "customers",
		ReferencedColumns: []string{"id"},
	}
	stmt, err := emitAddForeignKey("", "events", eventsFK)
	if err != nil {
		t.Fatalf("emitAddForeignKey(events): %v", err)
	}
	if err := sw.addForeignKeyMetadataOnly(ctx, "events", eventsFK, stmt); err != nil {
		t.Fatalf("addForeignKeyMetadataOnly over an orphaned child must succeed (foreign_key_checks=0 skips validation): %v", err)
	}

	// The FK genuinely exists afterward.
	var n int
	if err := sw.db.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM information_schema.TABLE_CONSTRAINTS
		 WHERE CONSTRAINT_SCHEMA = DATABASE() AND TABLE_NAME = 'events'
		   AND CONSTRAINT_NAME = 'events_customer_fk' AND CONSTRAINT_TYPE = 'FOREIGN KEY'`,
	).Scan(&n); err != nil {
		t.Fatalf("probe events FK: %v", err)
	}
	if n != 1 {
		t.Fatalf("events_customer_fk not present after metadata-only add (found %d)", n)
	}

	// (2) No-leak / validation-intact: a DISARMED CreateConstraints over the
	// orphaned `orders` child must FAIL with 1452. If foreign_key_checks had
	// leaked as 0 onto the shared connection, this would silently succeed.
	if sw.copiedRowsFKConsistent {
		t.Fatal("writer must be disarmed here — OpenSchemaWriter default is false")
	}
	ordersSchema := &ir.Schema{Tables: []*ir.Table{{
		Name: "orders",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "customer_id", Type: ir.Integer{Width: 64}},
		},
		PrimaryKey: &ir.Index{Name: "PRIMARY", Columns: []ir.IndexColumn{{Column: "id"}}},
		ForeignKeys: []*ir.ForeignKey{{
			Name:              "orders_customer_fk",
			Columns:           []string{"customer_id"},
			ReferencedTable:   "customers",
			ReferencedColumns: []string{"id"},
		}},
	}}}
	err = sw.CreateConstraints(ctx, ordersSchema)
	if err == nil {
		t.Fatal("disarmed CreateConstraints over an orphaned child must FAIL loudly (errno 1452) — a leaked foreign_key_checks=0 would have let the orphan through")
	}
	var mysqlErr *gomysql.MySQLError
	if !errors.As(err, &mysqlErr) || mysqlErr.Number != 1452 {
		t.Fatalf("expected errno 1452 (foreign key constraint fails) proving validation is active; got: %v", err)
	}
}

// TestCreateConstraints_SameEngineNoOp_Integration pins that with the writer
// ARMED (as the migrate orchestrator would on an unfiltered run) but the flavor
// VANILLA, the constraints phase is byte-identical to before item 109: it runs
// the ordinary validating ADD, so a clean schema's FKs land AND an orphaned
// child still fails loudly. Vanilla MySQL has no errno-3024 wall, so the
// recovery must be entirely inert — no foreign_key_checks=0 shortcut.
func TestCreateConstraints_SameEngineNoOp_Integration(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "item109_fk_noop")
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	seed, err := sql.Open("mysql", dsn+"&multiStatements=true")
	if err != nil {
		t.Fatalf("sql.Open seed: %v", err)
	}
	defer closeIf(seed)

	const ddl = `
CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY);
CREATE TABLE events    (id BIGINT NOT NULL PRIMARY KEY, customer_id BIGINT NOT NULL, KEY (customer_id));
INSERT INTO customers (id) VALUES (1);
INSERT INTO events (id, customer_id) VALUES (1, 1);` // clean: customer_id=1 exists
	if _, err := seed.ExecContext(ctx, ddl); err != nil {
		t.Fatalf("seed DDL/data: %v", err)
	}

	swHandle, err := Engine{}.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer closeIf(swHandle)
	sw := swHandle.(*SchemaWriter)

	// Arm it the way the migrate orchestrator would; flavor stays vanilla
	// (Engine{} default), so fkWallRecoveryArmed() is false and the FK phase is
	// the plain validating ADD.
	sw.SetCopiedRowsForeignKeyConsistent(true)
	if sw.fkWallRecoveryArmed() {
		t.Fatal("vanilla flavor must never arm the wall recovery — the same-engine no-op invariant")
	}

	schema := &ir.Schema{Tables: []*ir.Table{{
		Name: "events",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "customer_id", Type: ir.Integer{Width: 64}},
		},
		PrimaryKey: &ir.Index{Name: "PRIMARY", Columns: []ir.IndexColumn{{Column: "id"}}},
		ForeignKeys: []*ir.ForeignKey{{
			Name:              "events_customer_fk",
			Columns:           []string{"customer_id"},
			ReferencedTable:   "customers",
			ReferencedColumns: []string{"id"},
		}},
	}}}
	if err := sw.CreateConstraints(ctx, schema); err != nil {
		t.Fatalf("clean-schema CreateConstraints (vanilla, armed) must succeed via the ordinary validating ADD: %v", err)
	}
	var n int
	if err := sw.db.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM information_schema.TABLE_CONSTRAINTS
		 WHERE CONSTRAINT_SCHEMA = DATABASE() AND TABLE_NAME = 'events'
		   AND CONSTRAINT_NAME = 'events_customer_fk' AND CONSTRAINT_TYPE = 'FOREIGN KEY'`,
	).Scan(&n); err != nil {
		t.Fatalf("probe events FK: %v", err)
	}
	if n != 1 {
		t.Fatalf("events_customer_fk not present after the vanilla no-op path (found %d)", n)
	}
}
