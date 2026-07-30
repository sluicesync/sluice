//go:build integration

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"
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

// eventsSchema is the child+parent IR the orphan-scan driver walks (PK on id so
// the chunk-boundary sampler can bound the scan).
func eventsSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{
		{
			Name:       "customers",
			Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
			PrimaryKey: &ir.Index{Name: "PRIMARY", Columns: []ir.IndexColumn{{Column: "id"}}},
		},
		{
			Name: "events",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "customer_id", Type: ir.Integer{Width: 64}},
			},
			PrimaryKey: &ir.Index{Name: "PRIMARY", Columns: []ir.IndexColumn{{Column: "id"}}},
			Indexes:    []*ir.Index{{Name: "events_customer_ix", Columns: []ir.IndexColumn{{Column: "customer_id"}}}},
			ForeignKeys: []*ir.ForeignKey{{
				Name:              "events_customer_fk",
				Columns:           []string{"customer_id"},
				ReferencedTable:   "customers",
				ReferencedColumns: []string{"id"},
			}},
		},
	}}
}

func countEventsFK(ctx context.Context, t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM information_schema.TABLE_CONSTRAINTS
		 WHERE CONSTRAINT_SCHEMA = DATABASE() AND TABLE_NAME = 'events'
		   AND CONSTRAINT_NAME = 'events_customer_fk' AND CONSTRAINT_TYPE = 'FOREIGN KEY'`,
	).Scan(&n); err != nil {
		t.Fatalf("probe events FK: %v", err)
	}
	return n
}

// addEventsFKMetadataOnlyAndRecord adds the events FK metadata-only (over
// whatever rows exist, orphans included) and records it as unvalidated — the
// state the constraints phase leaves after a wall recovery, reproduced directly
// (a real PlanetScale statement wall cannot be reproduced in a container).
func addEventsFKMetadataOnlyAndRecord(ctx context.Context, t *testing.T, sw *SchemaWriter) {
	t.Helper()
	fk := eventsSchema().Tables[1].ForeignKeys[0]
	stmt, err := emitAddForeignKey("", "events", fk)
	if err != nil {
		t.Fatalf("emitAddForeignKey: %v", err)
	}
	if err := sw.addForeignKeyMetadataOnly(ctx, "events", fk, stmt); err != nil {
		t.Fatalf("addForeignKeyMetadataOnly: %v", err)
	}
	sw.recordUnvalidatedFK("events", fk)
}

// TestValidateUnvalidatedForeignKeys_DetectsOrphanDropsAndRefuses_Integration is
// the end-to-end loud-failure gate for the item-109 extension (the coordinator's
// core ask): a metadata-only-added FK over an orphaned source is PROVEN dirty by
// the bounded chunked orphan scan, the FK is DROPPED, and the run refuses with
// the coded SLUICE-E-FK-SOURCE-ORPHAN naming the constraint — the loud signal
// the wall killed before a validating ADD could produce it.
func TestValidateUnvalidatedForeignKeys_DetectsOrphanDropsAndRefuses_Integration(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "item109_fk_orphan_refuse")
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	seed, err := sql.Open("mysql", dsn+"&multiStatements=true")
	if err != nil {
		t.Fatalf("sql.Open seed: %v", err)
	}
	defer closeIf(seed)

	// events row id=3 references customer_id=999, which has no parent — an orphan.
	const ddl = `
CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY);
CREATE TABLE events (id BIGINT NOT NULL PRIMARY KEY, customer_id BIGINT NOT NULL, KEY events_customer_ix (customer_id));
INSERT INTO customers (id) VALUES (1),(2);
INSERT INTO events (id, customer_id) VALUES (1,1),(2,2),(3,999);`
	if _, err := seed.ExecContext(ctx, ddl); err != nil {
		t.Fatalf("seed: %v", err)
	}

	swHandle, err := Engine{}.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer closeIf(swHandle)
	sw := swHandle.(*SchemaWriter)

	addEventsFKMetadataOnlyAndRecord(ctx, t, sw)
	if countEventsFK(ctx, t, sw.db) != 1 {
		t.Fatal("precondition: the metadata-only FK should be present before the scan")
	}

	tr, err := Engine{}.OpenRowReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer closeIf(tr)

	scanErr := migcore.ValidateUnvalidatedForeignKeys(ctx, sw, eventsSchema(), tr)
	if scanErr == nil {
		t.Fatal("the orphan scan must REFUSE — child row id=3 references a missing customer 999")
	}
	coded, ok := sluicecode.FromError(scanErr)
	if !ok || coded.Code != sluicecode.CodeFKSourceOrphan {
		t.Fatalf("want SLUICE-E-FK-SOURCE-ORPHAN; got %v (coded=%v)", scanErr, ok)
	}
	if !strings.Contains(scanErr.Error(), "events_customer_fk") || !strings.Contains(scanErr.Error(), "customers") {
		t.Errorf("refusal must name the FK and referenced table: %v", scanErr)
	}
	// The trusted-wrong FK must be GONE — dropped before the refusal.
	if n := countEventsFK(ctx, t, sw.db); n != 0 {
		t.Fatalf("the violated FK must be dropped before refusing; still present (%d)", n)
	}
}

// TestValidateUnvalidatedForeignKeys_CleanSourcePassesFKStands_Integration is
// the happy path: a metadata-only-added FK over a clean source is PROVEN clean
// by the scan (no orphan), so it stands and the run continues. This is the
// common by-construction-clean case, now proven rather than assumed.
func TestValidateUnvalidatedForeignKeys_CleanSourcePassesFKStands_Integration(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "item109_fk_orphan_clean")
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
CREATE TABLE events (id BIGINT NOT NULL PRIMARY KEY, customer_id BIGINT NOT NULL, KEY events_customer_ix (customer_id));
INSERT INTO customers (id) VALUES (1),(2),(3);
INSERT INTO events (id, customer_id) VALUES (1,1),(2,2),(3,3);` // every customer_id has a parent
	if _, err := seed.ExecContext(ctx, ddl); err != nil {
		t.Fatalf("seed: %v", err)
	}

	swHandle, err := Engine{}.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer closeIf(swHandle)
	sw := swHandle.(*SchemaWriter)

	addEventsFKMetadataOnlyAndRecord(ctx, t, sw)

	tr, err := Engine{}.OpenRowReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer closeIf(tr)

	if err := migcore.ValidateUnvalidatedForeignKeys(ctx, sw, eventsSchema(), tr); err != nil {
		t.Fatalf("a clean source must pass the orphan scan; got %v", err)
	}
	if n := countEventsFK(ctx, t, sw.db); n != 1 {
		t.Fatalf("a clean FK must STAND after the scan; present=%d", n)
	}
}

// TestScanForeignKeyOrphan_BoundedRange_Integration pins the completeness-
// critical bounded predicate on real InnoDB: a chunk that EXCLUDES the orphan
// reports clean, and the chunk that INCLUDES it reports the violating child PK.
// A gap here (a range that skips the orphan) would let the whole scan read
// clean over a dirty source — silent loss.
func TestScanForeignKeyOrphan_BoundedRange_Integration(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "item109_fk_orphan_bounded")
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	seed, err := sql.Open("mysql", dsn+"&multiStatements=true")
	if err != nil {
		t.Fatalf("sql.Open seed: %v", err)
	}
	defer closeIf(seed)

	// Orphan is at id=3 (customer_id=999). Also a NULL-fk row at id=4 which must
	// NOT be flagged (MATCH SIMPLE: a NULL referencing key is not an orphan).
	const ddl = `
CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY);
CREATE TABLE events (id BIGINT NOT NULL PRIMARY KEY, customer_id BIGINT NULL, KEY events_customer_ix (customer_id));
INSERT INTO customers (id) VALUES (1),(2),(5);
INSERT INTO events (id, customer_id) VALUES (1,1),(2,2),(3,999),(4,NULL),(5,5);`
	if _, err := seed.ExecContext(ctx, ddl); err != nil {
		t.Fatalf("seed: %v", err)
	}

	swHandle, err := Engine{}.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer closeIf(swHandle)
	sw := swHandle.(*SchemaWriter)

	child := eventsSchema().Tables[1]
	fk := child.ForeignKeys[0]

	// Range (nil, id<=2]: clean (ids 1,2 are valid).
	if _, found, err := sw.ScanForeignKeyOrphan(ctx, child, fk, nil, []any{int64(2)}); err != nil || found {
		t.Fatalf("range (,2] must be clean; found=%v err=%v", found, err)
	}
	// Range (id>2, id<=4]: contains the orphan at id=3 (and the NULL row id=4,
	// which must NOT be flagged).
	key, found, err := sw.ScanForeignKeyOrphan(ctx, child, fk, []any{int64(2)}, []any{int64(4)})
	if err != nil {
		t.Fatalf("range (2,4] scan errored: %v", err)
	}
	if !found {
		t.Fatal("range (2,4] must find the orphan at id=3")
	}
	if len(key) != 1 || fkPKToInt64(key[0]) != 3 {
		t.Fatalf("violating child PK = %v; want [3]", key)
	}
	// Range (id>4, nil]: clean (id=5 is valid; id=4's NULL is below the bound).
	if _, found, err := sw.ScanForeignKeyOrphan(ctx, child, fk, []any{int64(4)}, nil); err != nil || found {
		t.Fatalf("range (4,] must be clean; found=%v err=%v", found, err)
	}
}

// fkPKToInt64 coerces a driver-scanned PK value (int64 or []byte) to int64 for
// assertions.
func fkPKToInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case []byte:
		var n int64
		for _, b := range x {
			if b < '0' || b > '9' {
				return -1
			}
			n = n*10 + int64(b-'0')
		}
		return n
	default:
		return -1
	}
}
