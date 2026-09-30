// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// ensureScript is a fake catalog for the detect-first ensure paths: whether
// every relation the probes ask about exists, which columns the column probe
// reports missing, and what an executed DDL statement returns.
type ensureScript struct {
	present bool
	missing []string
	execErr error

	mu       sync.Mutex
	executed []string
}

func (s *ensureScript) ddl() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.executed...)
}

type ensureConnector struct{ s *ensureScript }

func (c ensureConnector) Connect(context.Context) (driver.Conn, error) { return ensureConn(c), nil }
func (c ensureConnector) Driver() driver.Driver                        { return nil }

type ensureConn struct{ s *ensureScript }

func (ensureConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (ensureConn) Close() error                        { return nil }
func (ensureConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }

// CheckNamedValue accepts the []string the column probe binds as text[].
func (ensureConn) CheckNamedValue(*driver.NamedValue) error { return nil }

func (c ensureConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.s.mu.Lock()
	c.s.executed = append(c.s.executed, query)
	c.s.mu.Unlock()
	if c.s.execErr != nil {
		return nil, c.s.execErr
	}
	return driver.RowsAffected(0), nil
}

func (c ensureConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	switch {
	case strings.Contains(query, "unnest"):
		rows := make([][]driver.Value, 0, len(c.s.missing))
		for _, m := range c.s.missing {
			rows = append(rows, []driver.Value{m, c.s.present})
		}
		return &ensureRows{cols: []string{"name", "found"}, rows: rows}, nil
	case strings.Contains(query, "to_regclass"):
		return &ensureRows{cols: []string{"present"}, rows: [][]driver.Value{{c.s.present}}}, nil
	}
	return nil, errors.New("ensureConn: unexpected query: " + query)
}

type ensureRows struct {
	cols []string
	rows [][]driver.Value
	i    int
}

func (r *ensureRows) Columns() []string { return r.cols }
func (r *ensureRows) Close() error      { return nil }
func (r *ensureRows) Next(dest []driver.Value) error {
	if r.i >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.i])
	r.i++
	return nil
}

func newEnsureDB(t *testing.T, s *ensureScript) *sql.DB {
	t.Helper()
	db := sql.OpenDB(ensureConnector{s})
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// everyPostgresEnsure is the roster of this package's control-table ensure
// doors. TestControlTableDDLLiterals_OnlyInDetectFirstBuilders is what keeps
// a new door from being written outside it.
func everyPostgresEnsure() map[string]func(context.Context, *sql.DB) error {
	return map[string]func(context.Context, *sql.DB) error{
		"cdc state":      func(ctx context.Context, db *sql.DB) error { return ensureControlTable(ctx, db, "public") },
		"skipped tables": func(ctx context.Context, db *sql.DB) error { return ensureSkippedTablesTable(ctx, db, "public") },
		"shard lease": func(ctx context.Context, db *sql.DB) error {
			return ensureShardConsolidationLeaseTable(ctx, db, "public")
		},
		"schema history":         func(ctx context.Context, db *sql.DB) error { return ensureSchemaHistoryTable(ctx, db, "public") },
		"apply marks":            func(ctx context.Context, db *sql.DB) error { return ensureApplyMarksTable(ctx, db, "public") },
		"unforwarded refusal":    func(ctx context.Context, db *sql.DB) error { return ensureUnforwardedRefusalColumn(ctx, db, "public") },
		"target metrics history": func(ctx context.Context, db *sql.DB) error { return ensureTargetMetricsHistoryTable(ctx, db, "public") },
		"migrate state": func(ctx context.Context, db *sql.DB) error {
			return (&MigrationStateStore{db: db, schema: "public"}).EnsureControlTable(ctx)
		},
		"keysets": func(ctx context.Context, db *sql.DB) error {
			return (&pgKeysetStore{db: db, schema: "public"}).EnsureKeysetTable(ctx)
		},
		"heartbeat": func(ctx context.Context, db *sql.DB) error {
			return (&SchemaReader{db: db, schema: "public"}).EnsureHeartbeatTable(ctx, "sluice_heartbeat")
		},
	}
}

// TestControlTableEnsure_IssuesZeroDDLWhenCurrent pins GC-40 (a) (Bug 291):
// on a target where every control table, column and index already exists,
// no ensure door issues a single DDL statement. PostgreSQL checks CREATE on
// the schema before IF NOT EXISTS and table ownership before ADD COLUMN IF
// NOT EXISTS, so any DDL here is what stopped a DML-only role at startup.
func TestControlTableEnsure_IssuesZeroDDLWhenCurrent(t *testing.T) {
	for name, ensure := range everyPostgresEnsure() {
		t.Run(name, func(t *testing.T) {
			s := &ensureScript{present: true}
			if err := ensure(context.Background(), newEnsureDB(t, s)); err != nil {
				t.Fatalf("ensure on a current target: %v", err)
			}
			if got := s.ddl(); len(got) != 0 {
				t.Errorf("ensure issued DDL on a current target: %q", got)
			}
		})
	}
}

// TestControlTableEnsure_RefusalNamesTheMissingObject pins the other half of
// GC-40 (a): DDL that is genuinely needed and refused fails loudly, naming the
// table or column, the statement to run as the owner and the
// `control-tables ddl` remedy, with the driver error still in the chain; and
// only the missing column's ALTER is attempted.
func TestControlTableEnsure_RefusalNamesTheMissingObject(t *testing.T) {
	denied := &pgconn.PgError{Code: "42501", Message: "must be owner of table sluice_cdc_state"}

	s := &ensureScript{present: true, missing: []string{"rows_applied"}, execErr: denied}
	err := ensureControlTable(context.Background(), newEnsureDB(t, s), "public")
	for _, want := range []string{
		ControlTableDDLRequiredMarker,
		`"public"."sluice_cdc_state" lacks column rows_applied`,
		"ALTER TABLE \"public\".\"sluice_cdc_state\" ADD COLUMN IF NOT EXISTS rows_applied BIGINT NOT NULL DEFAULT 0",
		"sluice control-tables ddl --engine postgres",
	} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("missing-column refusal = %v; want it to contain %q", err, want)
		}
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
		t.Errorf("driver error dropped from the chain: %v", err)
	}
	if got := s.ddl(); len(got) != 1 || !strings.Contains(got[0], "rows_applied") {
		t.Errorf("attempted %q; want only the missing column's ALTER", got)
	}

	s = &ensureScript{present: false, execErr: denied}
	err = ensureSkippedTablesTable(context.Background(), newEnsureDB(t, s), "public")
	if err == nil || !strings.Contains(err.Error(), `"public"."sluice_cdc_skipped_tables" does not exist`) {
		t.Errorf("missing-table refusal = %v; want it to name the table", err)
	}

	// The persisted-refusal door on a target with no sluice_cdc_state at all
	// (Bug 292's `--schema-already-applied` shape) names the table instead of
	// surfacing the ALTER's bare "relation … does not exist".
	s = &ensureScript{present: false, missing: []string{"unforwarded_refusal"}}
	err = ensureUnforwardedRefusalColumn(context.Background(), newEnsureDB(t, s), "public")
	if err == nil || !strings.Contains(err.Error(), `"public"."sluice_cdc_state" does not exist`) {
		t.Errorf("refusal-column door on an absent table = %v; want it to name the table", err)
	}
	if got := s.ddl(); len(got) != 0 {
		t.Errorf("refusal-column door issued %q against an absent table", got)
	}
}

// TestControlTableDDLLiterals_OnlyInDetectFirstBuilders is the roster gate
// for GC-40 (a). It walks every non-test Go file in this package and finds
// each string literal that creates or extends a table or index with
// IF NOT EXISTS, keyed by file and enclosing function. Each must sit either
// in a [controlTable] builder (whose DDL only [controlTable.ensure] runs, and
// only for a missing object), in one of the two hand-written detect-first
// doors, or in the user-schema DDL emitters — anything else is a new ensure
// that would issue DDL on every start and stop a DML-only role. Every entry
// must still match (a stale entry fails too).
func TestControlTableDDLLiterals_OnlyInDetectFirstBuilders(t *testing.T) {
	allowed := map[string]string{
		"control_table.go:cdcStateTable":                      "controlTable builder",
		"control_table.go:skippedTablesTable":                 "controlTable builder",
		"control_table.go:shardConsolidationLeaseTable":       "controlTable builder",
		"schema_history.go:schemaHistoryTable":                "controlTable builder",
		"migration_state.go:migrateStateTables":               "controlTable builder",
		"keyset_store.go:keysetTable":                         "controlTable builder",
		"target_metrics_history.go:targetMetricsHistoryTable": "controlTable builder",
		"control_table_ensure.go:addColumnDDL":                "run by controlTable.ensureColumns for a missing column only",
		"apply_marks.go:applyMarksTableDDL":                   "run by ensureApplyMarksTable behind applyMarksTableExists",
		"heartbeat_writer.go:EnsureHeartbeatTable":            "run behind relationPresent",
		"ddl_emit.go:emitTableDef":                            "user-schema DDL for migrate/cold start, not a control table",
		"schema_writer.go:AlterAddColumn":                     "user-schema ADD COLUMN, not a control table",
		"schema_writer.go:buildOneIndex":                      "user-schema index build, not a control table",
		"schema_writer.go:CreateShapeIndex":                   "user-schema index build, not a control table",
		"schema_writer.go:alterEnumAddValues":                 "user-schema enum values, not a control table",
	}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	seen := map[string]bool{}
	files, literals := 0, 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		files++
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				lit, ok := n.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					return true
				}
				v, err := strconv.Unquote(lit.Value)
				if err != nil {
					return true
				}
				u := strings.ToUpper(strings.Join(strings.Fields(v), " "))
				if !strings.Contains(u, "TABLE IF NOT EXISTS") && !strings.Contains(u, "ADD COLUMN IF NOT EXISTS") &&
					!strings.Contains(u, "INDEX IF NOT EXISTS") && !strings.Contains(u, "ADD VALUE IF NOT EXISTS") {
					return true
				}
				// Prose (an error or log message) that mentions the form is
				// not a statement.
				if !strings.HasPrefix(strings.TrimSpace(u), "CREATE") && !strings.HasPrefix(strings.TrimSpace(u), "ALTER") &&
					!strings.HasPrefix(strings.TrimSpace(u), "ADD COLUMN") && !strings.HasPrefix(strings.TrimSpace(u), "INDEX") {
					return true
				}
				literals++
				key := name + ":" + fn.Name.Name
				if _, ok := allowed[key]; !ok {
					t.Errorf("%s at %s holds IF NOT EXISTS DDL outside a detect-first builder: an ensure that runs it on every start stops a DML-only role (GC-40 (a)); build it as a controlTable or list it here with a reason",
						key, fset.Position(lit.Pos()))
					return true
				}
				seen[key] = true
				return true
			})
		}
	}
	if files < 100 || literals < 12 {
		t.Errorf("walked %d files, %d DDL literals; the scan is not seeing the package", files, literals)
	}
	for key := range allowed {
		if !seen[key] {
			t.Errorf("allowlist entry %q matches nothing; remove it", key)
		}
	}
}
