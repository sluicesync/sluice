// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Roadmap item 66 unit pins for the control-table bootstrap contract
// (ADR-0165), against a scriptable fake driver (the
// control_table_widen_test.go precedent):
//
//   - EVERY mysql control-table DDL site — the CREATEs behind each
//     Ensure* and each detect-then-ALTER column migration — classifies
//     the PlanetScale safe-migrations refusal (Error 1105 "direct DDL
//     is disabled") into the coded SLUICE-E-PS-DIRECT-DDL-BLOCKED
//     refusal naming the deploy-ddl bootstrap path. Pinned per SITE,
//     not per representative: the sites are independent functions, so
//     a green pin on one proves nothing about the others (the Bug 74
//     lesson applied to error classification).
//   - The detect-then-create gate: when the tables and columns are
//     already current, the ensure paths issue ZERO DDL statements —
//     the property that lets sync/backfill/migrate start against a
//     safe-migrations branch whose control tables were bootstrapped
//     via `sluice deploy-ddl`.
//   - Engine.ControlTableDDL single-sourcing: the printed statements
//     are byte-identical to what the ensure paths execute.

package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// bootScript scripts the fake driver: tableExists / columnExists
// answer the information_schema detect queries, execErr (when non-nil)
// is returned by every DDL exec, and ddls records the executed DDL
// statements.
type bootScript struct {
	tableExists  bool
	columnExists bool
	execErr      error

	mu   sync.Mutex
	ddls []string
}

func (s *bootScript) record(ddl string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ddls = append(s.ddls, ddl)
}

func (s *bootScript) executed() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.ddls...)
}

type bootDriver struct{ script *bootScript }

type bootConn struct{ script *bootScript }

func (d bootDriver) Open(string) (driver.Conn, error) { return bootConn(d), nil }

func (bootConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (bootConn) Close() error                        { return nil }
func (bootConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }

func (c bootConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.script.record(query)
	if c.script.execErr != nil {
		return nil, c.script.execErr
	}
	return driver.RowsAffected(0), nil
}

func (c bootConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	switch {
	case strings.Contains(query, "DATA_TYPE"):
		// The item-65a widen detect: already LONGTEXT ⇒ no DDL.
		return &bootRows{value: "longtext"}, nil
	case strings.Contains(query, "information_schema.TABLES"):
		return &bootRows{value: countValue(c.script.tableExists)}, nil
	case strings.Contains(query, "information_schema.COLUMNS"):
		return &bootRows{value: countValue(c.script.columnExists)}, nil
	}
	return &bootRows{done: true}, nil
}

func countValue(exists bool) driver.Value {
	if exists {
		return int64(1)
	}
	return int64(0)
}

// bootRows serves a single-column result with exactly one row.
type bootRows struct {
	value driver.Value
	done  bool
}

func (*bootRows) Columns() []string { return []string{"v"} }
func (*bootRows) Close() error      { return nil }

func (r *bootRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	dest[0] = r.value
	r.done = true
	return nil
}

func newBootDB(t *testing.T, script *bootScript) *sql.DB {
	t.Helper()
	name := "sluice-boot-test-" + t.Name()
	sql.Register(name, bootDriver{script: script})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("open boot db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// directDDLDisabled is the real driver-error shape PlanetScale/Vitess
// returns under safe migrations.
func directDDLDisabled() error {
	return &gomysql.MySQLError{Number: 1105, Message: "direct DDL is disabled"}
}

// wantBootstrapRefusal asserts the coded, remedy-bearing shape every
// site must produce: SLUICE-E-PS-DIRECT-DDL-BLOCKED, the sentinel
// preserved, and the two bootstrap commands named.
func wantBootstrapRefusal(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("want the coded bootstrap refusal; got nil")
	}
	var coded *sluicecode.CodedError
	if !errors.As(err, &coded) {
		t.Fatalf("want *sluicecode.CodedError; got %T: %v", err, err)
	}
	if coded.Code != sluicecode.CodePSDirectDDLBlocked {
		t.Errorf("code = %s; want %s", coded.Code, sluicecode.CodePSDirectDDLBlocked)
	}
	if !errors.Is(err, ErrSafeMigrationsBlocked) {
		t.Errorf("errors.Is(err, ErrSafeMigrationsBlocked) = false; err = %v", err)
	}
	if !strings.Contains(err.Error(), "deploy-ddl") {
		t.Errorf("refusal %q should name the deploy-ddl bootstrap path", err)
	}
}

// TestControlTableCreateSites_1105ClassifiedPerSite pins the CREATE
// half of the site matrix: every table-create site, driven end to end
// through its own ensure function against a 1105-returning driver.
func TestControlTableCreateSites_1105ClassifiedPerSite(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name   string
		ensure func(ctx context.Context, db *sql.DB) error
	}{
		{"cdc state", func(ctx context.Context, db *sql.DB) error {
			return ensureControlTable(ctx, db, "")
		}},
		{"schema history", func(ctx context.Context, db *sql.DB) error {
			return ensureSchemaHistoryTable(ctx, db, "")
		}},
		{"shard consolidation lease", func(ctx context.Context, db *sql.DB) error {
			return ensureShardConsolidationLeaseTable(ctx, db, "")
		}},
		{"migrate state (header)", func(ctx context.Context, db *sql.DB) error {
			return newMigrationStateStore(db, upsertRowAlias).EnsureControlTable(ctx)
		}},
		{"keysets", func(ctx context.Context, db *sql.DB) error {
			return (&mysqlKeysetStore{db: db}).EnsureKeysetTable(ctx)
		}},
		{"target metrics history", ensureTargetMetricsHistoryTable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			script := &bootScript{execErr: directDDLDisabled()}
			db := newBootDB(t, script)
			wantBootstrapRefusal(t, tc.ensure(ctx, db))
			if got := script.executed(); len(got) != 1 || !strings.Contains(got[0], "CREATE TABLE") {
				t.Errorf("executed DDL = %q; want exactly the one refused CREATE", got)
			}
		})
	}
}

// TestControlTableAlterSites_1105ClassifiedPerSite pins the
// column-migration half of the matrix: each detect-then-ALTER site
// against an existing table whose column is genuinely missing (the
// only case where these fire — the detect gate already guarantees no
// DDL otherwise), with the ALTER refused by safe migrations. The coded
// refusal echoes the exact ALTER, since the printed bootstrap set
// carries only the CREATEs.
func TestControlTableAlterSites_1105ClassifiedPerSite(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name  string
		alter func(ctx context.Context, db *sql.DB) error
		want  string // fragment of the echoed ALTER
	}{
		{"cdc state: stop_requested_at", func(ctx context.Context, db *sql.DB) error {
			return ensureStopRequestedColumn(ctx, db, "")
		}, "ADD COLUMN stop_requested_at"},
		{"cdc state: live_added_tables", func(ctx context.Context, db *sql.DB) error {
			return ensureLiveAddedTablesColumn(ctx, db, "")
		}, "ADD COLUMN live_added_tables"},
		{"cdc state: parity column", func(ctx context.Context, db *sql.DB) error {
			return ensureCrossEngineParityColumn(ctx, db, "", "slot_name", "VARCHAR(255) NULL")
		}, "ADD COLUMN `slot_name`"},
		{"lease: anchor_position", func(ctx context.Context, db *sql.DB) error {
			return ensureShardLeaseColumn(ctx, db, "", "anchor_position", "LONGTEXT NULL")
		}, "ADD COLUMN `anchor_position`"},
		{"schema history: source_engine", func(ctx context.Context, db *sql.DB) error {
			return ensureSchemaHistorySourceEngineColumn(ctx, db, "")
		}, "ADD COLUMN source_engine"},
		{"migrate state: state_format", func(ctx context.Context, db *sql.DB) error {
			return newMigrationStateStore(db, upsertRowAlias).ensureStateFormatColumn(ctx)
		}, "ADD COLUMN state_format"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			script := &bootScript{tableExists: true, columnExists: false, execErr: directDDLDisabled()}
			db := newBootDB(t, script)
			err := tc.alter(ctx, db)
			wantBootstrapRefusal(t, err)
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("refusal %q should echo the ALTER (%s)", err, tc.want)
			}
		})
	}
}

// TestEnsurePathsIssueZeroDDLWhenCurrent pins the detect-then-create
// contract on a fully-bootstrapped target: table exists, columns
// present, position columns already LONGTEXT ⇒ not one DDL statement
// leaves sluice. This is what makes the deploy-ddl bootstrap
// sufficient — on a safe-migrations branch even a no-op CREATE TABLE
// IF NOT EXISTS is refused (live-caught 2026-07-15), so "no DDL" is
// the only safe idempotent shape.
func TestEnsurePathsIssueZeroDDLWhenCurrent(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name   string
		ensure func(ctx context.Context, db *sql.DB) error
	}{
		{"cdc state", func(ctx context.Context, db *sql.DB) error {
			return ensureControlTable(ctx, db, "")
		}},
		{"schema history", func(ctx context.Context, db *sql.DB) error {
			return ensureSchemaHistoryTable(ctx, db, "")
		}},
		{"shard consolidation lease", func(ctx context.Context, db *sql.DB) error {
			return ensureShardConsolidationLeaseTable(ctx, db, "")
		}},
		{"migrate state", func(ctx context.Context, db *sql.DB) error {
			return newMigrationStateStore(db, upsertRowAlias).EnsureControlTable(ctx)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			script := &bootScript{tableExists: true, columnExists: true}
			db := newBootDB(t, script)
			if err := tc.ensure(ctx, db); err != nil {
				t.Fatalf("ensure on a current target: %v", err)
			}
			if got := script.executed(); len(got) != 0 {
				t.Errorf("ensure issued DDL on a current target: %q", got)
			}
		})
	}
}

// TestEngineControlTableDDL_SingleSourcedWithEnsurePaths pins the
// printer contract: Engine.ControlTableDDL returns the five bootstrap
// tables in a stable order, and each statement is byte-identical to
// what the corresponding ensure path executes against a fresh target
// (captured through the fake driver) — the printed DDL can never
// drift from what sluice would create.
func TestEngineControlTableDDL_SingleSourcedWithEnsurePaths(t *testing.T) {
	ctx := context.Background()
	stmts := Engine{}.ControlTableDDL()

	wantTables := []string{
		migrateStateTableName,
		migrateProgressTableName,
		controlTableName,
		schemaHistoryTableName,
		shardConsolidationLeaseTableName,
	}
	if len(stmts) != len(wantTables) {
		t.Fatalf("ControlTableDDL returned %d statements; want %d", len(stmts), len(wantTables))
	}
	byTable := map[string]string{}
	for i, s := range stmts {
		if s.Table != wantTables[i] {
			t.Errorf("statement %d is for %q; want %q", i, s.Table, wantTables[i])
		}
		if !strings.HasPrefix(s.DDL, "CREATE TABLE IF NOT EXISTS") {
			t.Errorf("%s DDL does not start with CREATE TABLE IF NOT EXISTS:\n%s", s.Table, s.DDL)
		}
		byTable[s.Table] = s.DDL
	}

	// Capture what the ensure paths actually execute on a fresh target
	// and compare byte-for-byte.
	script := &bootScript{}
	db := newBootDB(t, script)
	if err := ensureControlTable(ctx, db, ""); err != nil {
		t.Fatalf("ensureControlTable: %v", err)
	}
	if err := ensureSchemaHistoryTable(ctx, db, ""); err != nil {
		t.Fatalf("ensureSchemaHistoryTable: %v", err)
	}
	if err := ensureShardConsolidationLeaseTable(ctx, db, ""); err != nil {
		t.Fatalf("ensureShardConsolidationLeaseTable: %v", err)
	}
	if err := newMigrationStateStore(db, upsertRowAlias).EnsureControlTable(ctx); err != nil {
		t.Fatalf("migrate-state EnsureControlTable: %v", err)
	}
	executed := map[string]bool{}
	for _, ddl := range script.executed() {
		if !strings.Contains(ddl, "CREATE TABLE") {
			continue
		}
		executed[ddl] = true
	}
	for table, ddl := range byTable {
		if !executed[ddl] {
			t.Errorf("printed DDL for %s is not byte-identical to what the ensure path executes:\n%s", table, ddl)
		}
	}
}
