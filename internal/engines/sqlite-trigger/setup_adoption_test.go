// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strings"
	"testing"

	_ "modernc.org/sqlite" // pure-Go driver; keeps these in the unit gate

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/engines/sqlite"
	"sluicesync.dev/sluice/internal/ir"
)

// Roadmap item 149b — the trigger engines' change-log tables are created with
// CREATE TABLE IF NOT EXISTS, so before this a pre-existing table at one of
// those names was ADOPTED at setup and only failed later, on the operator's own
// write path. These tests pin the refusal on BOTH sqlite-trigger transports
// (local file, D1 over HTTP) and, just as hard, pin the OVER-refusal direction:
// a healthy re-setup and an install that predates the tables added since must
// stay silent.
//
// The oracle throughout is the DATABASE's OWN catalog (sqlite_master /
// PRAGMA table_info), never sluice's return value — a refusal that still
// installed the triggers would pass a test that only read the error.

// --- local transport --------------------------------------------------------

// TestSetup_RefusesAForeignTableAtTheChangeLogName is the repro: a user table
// that merely happens to be named `sluice_change_log`. Before the fix setup
// returned nil and installed the full trigger set against it.
func TestSetup_RefusesAForeignTableAtTheChangeLogName(t *testing.T) {
	path := newSourceFile(
		t,
		`CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`,
		// A perfectly ordinary user table. It carries `id` (so it is not
		// distinguishable by a bare existence probe, nor by "has an id"), and
		// nothing else sluice's change log has.
		`CREATE TABLE sluice_change_log (id INTEGER PRIMARY KEY, note TEXT)`,
		`INSERT INTO sluice_change_log (note) VALUES ('the operator''s own audit trail')`,
	)

	_, err := Setup(bg(), path, SetupOptions{Tables: []string{"t"}})
	if err == nil {
		t.Fatal("setup ADOPTED a foreign table at the change-log name; want a loud refusal")
	}
	for _, want := range []string{"refuse-loudly", "sluice_change_log", "op", "captured_at"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal should name %q; got: %v", want, err)
		}
	}

	// Oracle: the source's own catalog. Nothing was installed and the user's
	// table is untouched.
	if trigs := catalogNames(t, path, "trigger"); len(trigs) != 0 {
		t.Errorf("refused setup still installed triggers: %v", trigs)
	}
	tables := catalogNames(t, path, "table")
	for _, unwanted := range []string{ChangeLogMetaTable, ChangeLogColumnsTable, ChangeLogConsumersTable} {
		if contains(tables, unwanted) {
			t.Errorf("refused setup still created %q", unwanted)
		}
	}
	if got := tableCols(t, path, ChangeLogTable); !equalStrings(got, []string{"id", "note"}) {
		t.Errorf("the user's table was altered: columns = %v; want [id note]", got)
	}
	var rows int
	if err := queryRow(t, path, `SELECT COUNT(*) FROM sluice_change_log`).Scan(&rows); err != nil {
		t.Fatalf("count user rows: %v", err)
	}
	if rows != 1 {
		t.Errorf("the user's row count = %d; want 1", rows)
	}
}

// TestSetup_DryRunRefusesTheForeignTableToo pins that `--dry-run` surfaces the
// refusal rather than printing a plan that would silently adopt on apply.
func TestSetup_DryRunRefusesTheForeignTableToo(t *testing.T) {
	path := newSourceFile(
		t,
		`CREATE TABLE t (id INTEGER PRIMARY KEY)`,
		`CREATE TABLE sluice_change_log (id INTEGER PRIMARY KEY, note TEXT)`,
	)
	if _, err := Setup(bg(), path, SetupOptions{Tables: []string{"t"}, DryRun: true}); err == nil {
		t.Fatal("dry-run setup returned a plan for a source it must refuse")
	}
	// A dry run must still write nothing — the probe opens read-only.
	if trigs := catalogNames(t, path, "trigger"); len(trigs) != 0 {
		t.Errorf("dry-run installed triggers: %v", trigs)
	}
}

// TestSetup_HealthyReRunStaysSilent is the OVER-refusal guard, and it is the
// half that would break every existing install if the probe were wrong: setup
// is idempotent, so the second run finds all four of its own tables present and
// must accept them.
func TestSetup_HealthyReRunStaysSilent(t *testing.T) {
	path := newSourceFile(t, `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`)
	for i := 1; i <= 3; i++ {
		if _, err := Setup(bg(), path, SetupOptions{Tables: []string{"t"}}); err != nil {
			t.Fatalf("setup run %d on a healthy install: %v", i, err)
		}
	}
	if !contains(catalogNames(t, path, "table"), ChangeLogTable) {
		t.Fatal("change log missing after three setups")
	}
}

// TestSetup_AcceptsInstallsFromEarlierAndLaterReleases pins the two shapes an
// upgrade actually produces, because a probe that refused either would break
// working installs rather than protect them:
//
//   - an OLDER install predating the tables added since (the fingerprint table
//     arrived after the change-log/meta pair, the consumer registry at
//     schema_version 2) — those are simply absent, and absent means "create it",
//   - a NEWER install whose change log carries a column this binary does not
//     know about — a superset still satisfies the floor.
func TestSetup_AcceptsInstallsFromEarlierAndLaterReleases(t *testing.T) {
	t.Run("older install: only the change-log + meta pair exists", func(t *testing.T) {
		path := newSourceFile(
			t,
			`CREATE TABLE t (id INTEGER PRIMARY KEY)`,
			// Verbatim the DDL the ORIGINAL release emitted (recovered from this
			// file's own history), not a paraphrase — a fixture written from
			// today's values could not tell you anything about compatibility
			// with what an older binary actually wrote.
			`CREATE TABLE "sluice_change_log" (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    op           TEXT NOT NULL,
    tbl          TEXT NOT NULL,
    before       TEXT,
    after        TEXT,
    captured_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
)`,
			`CREATE TABLE "sluice_change_log_meta" (
    singleton_pk   INTEGER PRIMARY KEY CHECK (singleton_pk = 1),
    schema_version INTEGER NOT NULL,
    installed_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
)`,
			`INSERT INTO "sluice_change_log_meta" (singleton_pk, schema_version) VALUES (1, 1)`,
		)
		if _, err := Setup(bg(), path, SetupOptions{Tables: []string{"t"}}); err != nil {
			t.Fatalf("setup refused a legitimate v1 install: %v", err)
		}
		if !contains(catalogNames(t, path, "table"), ChangeLogConsumersTable) {
			t.Error("the v1→v2 migration did not create the consumer registry")
		}
	})

	t.Run("newer install: an extra column the floor does not know", func(t *testing.T) {
		path := newSourceFile(
			t,
			`CREATE TABLE t (id INTEGER PRIMARY KEY)`,
			`CREATE TABLE sluice_change_log (
				id INTEGER PRIMARY KEY AUTOINCREMENT, op TEXT NOT NULL, tbl TEXT NOT NULL,
				before TEXT, after TEXT, captured_at TEXT NOT NULL, future_col TEXT)`,
		)
		if _, err := Setup(bg(), path, SetupOptions{Tables: []string{"t"}}); err != nil {
			t.Fatalf("setup refused a superset change log: %v", err)
		}
	})
}

// TestSetup_RefusesAForeignTableAtEveryInternalName is the sibling sweep: the
// probe must reach all FOUR engine-internal names, not just the change log.
// Driven off the floor map itself, so a new internal table joins automatically.
//
// Measured by mutation run, so the gate is not read as broader than it is: with
// the probe disabled, `sluice_change_log` and `sluice_change_log_consumers` are
// ADOPTED SILENTLY, while `sluice_change_log_meta` and
// `sluice_change_log_columns` already failed loudly (setup's own upsert into
// them hits the missing column at setup time). The probe still earns its place
// on all four — it refuses BEFORE any DDL is applied, so a refused source is
// left untouched rather than half-installed — but only two of the four names
// were silent-loss shapes.
func TestSetup_RefusesAForeignTableAtEveryInternalName(t *testing.T) {
	if len(internalTableColumnFloor) < 4 {
		t.Fatalf("floor roster has %d entries; expected at least 4 (anti-vacuity)", len(internalTableColumnFloor))
	}
	for name := range internalTableColumnFloor {
		t.Run(name, func(t *testing.T) {
			path := newSourceFile(
				t,
				`CREATE TABLE t (id INTEGER PRIMARY KEY)`,
				`CREATE TABLE "`+name+`" (unrelated TEXT)`,
			)
			if _, err := Setup(bg(), path, SetupOptions{Tables: []string{"t"}}); err == nil {
				t.Fatalf("setup adopted a foreign table at %q", name)
			}
		})
	}
}

// --- D1 transport -----------------------------------------------------------

// TestD1Setup_RefusesAForeignTableAtTheChangeLogName pins the SAME refusal on
// the second transport (ADR-0136), which is where this class has historically
// been half-fixed.
//
// The mock here is deliberately NOT the canned dispatcher the other D1 tests
// use: it EXECUTES the incoming SQL against a real modernc SQLite database and
// serialises the result into a D1 envelope. That makes real SQLite — not the
// test's own expectations — the thing answering `PRAGMA table_info`, so the
// pin covers the executor's actual wire SQL rather than a hand-written reply.
func TestD1Setup_RefusesAForeignTableAtTheChangeLogName(t *testing.T) {
	tbl := &ir.Table{
		Name:       "t",
		Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}

	t.Run("foreign table is refused", func(t *testing.T) {
		conn, db := startSQLiteBackedD1(
			t,
			`CREATE TABLE t (id INTEGER PRIMARY KEY)`,
			`CREATE TABLE sluice_change_log (id INTEGER PRIMARY KEY, note TEXT)`,
		)
		b := d1TestBackend(conn, &ir.Schema{Tables: []*ir.Table{tbl}})
		_, err := setup(bg(), b, SetupOptions{Tables: []string{"t"}})
		if err == nil {
			t.Fatal("D1 setup ADOPTED a foreign table at the change-log name")
		}
		if !strings.Contains(err.Error(), "refuse-loudly") || !strings.Contains(err.Error(), ChangeLogTable) {
			t.Errorf("unexpected refusal text: %v", err)
		}
		// Oracle: the backing database's own catalog.
		var n int
		if err := db.QueryRowContext(bg(),
			`SELECT COUNT(*) FROM sqlite_master WHERE type='trigger'`).Scan(&n); err != nil {
			t.Fatalf("count triggers: %v", err)
		}
		if n != 0 {
			t.Errorf("refused D1 setup still installed %d trigger(s)", n)
		}
	})

	t.Run("clean source and a healthy re-run both proceed", func(t *testing.T) {
		conn, db := startSQLiteBackedD1(t, `CREATE TABLE t (id INTEGER PRIMARY KEY)`)
		b := d1TestBackend(conn, &ir.Schema{Tables: []*ir.Table{tbl}})
		for i := 1; i <= 2; i++ {
			if _, err := setup(bg(), b, SetupOptions{Tables: []string{"t"}}); err != nil {
				t.Fatalf("D1 setup run %d: %v", i, err)
			}
		}
		var n int
		if err := db.QueryRowContext(bg(),
			`SELECT COUNT(*) FROM sqlite_master WHERE type='trigger'`).Scan(&n); err != nil {
			t.Fatalf("count triggers: %v", err)
		}
		if n != len(triggerOps) {
			t.Errorf("installed %d triggers; want %d", n, len(triggerOps))
		}
	})
}

// startSQLiteBackedD1 boots an httptest `/query` server that runs each incoming
// statement against a real temp SQLite file, and returns a D1Conn pointed at it
// plus a handle on the backing database for catalog assertions.
func startSQLiteBackedD1(t *testing.T, seed ...string) (*sqlite.D1Conn, *sql.DB) {
	t.Helper()
	path := newSourceFile(t, seed...)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open backing db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var req struct {
			SQL    string   `json:"sql"`
			Params []string `json:"params"`
		}
		_ = json.Unmarshal(raw, &req)
		args := make([]any, len(req.Params))
		for i, p := range req.Params {
			args[i] = p
		}
		rows, qerr := db.QueryContext(r.Context(), req.SQL, args...)
		if qerr != nil {
			// Not every statement returns rows; fall back to Exec.
			if _, eerr := db.ExecContext(r.Context(), req.SQL, args...); eerr != nil {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(eerr.Error()))
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(d1OK(nil))
			return
		}
		defer func() { _ = rows.Close() }()
		cols, cerr := rows.Columns()
		if cerr != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var out []map[string]any
		for rows.Next() {
			cells := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range cells {
				ptrs[i] = &cells[i]
			}
			if serr := rows.Scan(ptrs...); serr != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			row := make(map[string]any, len(cols))
			for i, c := range cols {
				row[c] = d1JSONCell(cells[i])
			}
			out = append(out, row)
		}
		if rerr := rows.Err(); rerr != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(rerr.Error()))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(d1OK(out))
	}))
	t.Cleanup(srv.Close)
	return sqlite.D1ConnForTest(srv.URL, "acct", "db", "tok"), db
}

// d1JSONCell renders one scanned SQLite value the way the D1 API would carry it
// (text/number/null; []byte as its string form — none of these tests key on a
// blob).
func d1JSONCell(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case []byte:
		return string(x)
	default:
		return x
	}
}

// --- the floor gate ---------------------------------------------------------

// TestInternalTableColumnFloorMatchesTheRenderedDDL is the forcing function
// behind [internalTableColumnFloor]. It derives the roster from what
// renderSetupDDL actually emits, so:
//
//   - a NEW engine-internal table with no floor entry fails the build (it would
//     otherwise be created with IF NOT EXISTS and ungraded — the exact defect
//     149b closes), and
//   - a RENAMED or REMOVED column fails the build, because the floor would then
//     refuse installs that are healthy.
//
// It deliberately does NOT assert the reverse for ADDED columns as a silent
// pass: an added column makes the sets differ, this test fails, and whoever
// adds it has to decide whether existing installs get migrated. That is the
// decision the gate exists to force. Scope, stated rather than implied: this
// grades the sqlite-trigger render only; the pgtrigger twin has its own copy.
func TestInternalTableColumnFloorMatchesTheRenderedDDL(t *testing.T) {
	rendered := renderSetupDDL([]*ir.Table{{
		Name:       "t",
		Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}})

	got := parseCreatedTableColumns(t, rendered)
	if len(got) < 4 {
		t.Fatalf("parsed %d CREATE TABLE statements out of the render; expected at least 4 "+
			"(anti-vacuity: the parser has probably stopped matching)", len(got))
	}
	if len(got) != len(internalTableColumnFloor) {
		t.Fatalf("render creates %d internal tables %v but the floor has %d entries — "+
			"every table setup creates with IF NOT EXISTS must be graded",
			len(got), sortedKeys(got), len(internalTableColumnFloor))
	}
	for name, cols := range got {
		floor, ok := internalTableColumnFloor[name]
		if !ok {
			t.Errorf("no floor entry for internal table %q", name)
			continue
		}
		if !equalStrings(sortedCopy(cols), sortedCopy(floor)) {
			t.Errorf("table %q: rendered columns %v != floor %v — a rename/removal breaks existing "+
				"installs; an addition needs a migration decision before the floor moves",
				name, cols, floor)
		}
	}
}

// TestInternalTablesAreOnTheControlTableRoster binds the floor to the OTHER
// roster that must know these names: a table sluice creates on a source but
// that the schema readers do not exclude would be enumerated as user data.
func TestInternalTablesAreOnTheControlTableRoster(t *testing.T) {
	for name := range internalTableColumnFloor {
		if !appliershared.IsControlTable(name) {
			t.Errorf("%q is created by trigger setup but is not on the control-table roster", name)
		}
	}
}

var createTableRe = regexp.MustCompile(`(?s)CREATE TABLE IF NOT EXISTS "([^"]+)"\s*\((.*)`)

// parseCreatedTableColumns extracts {table: [columns]} from rendered DDL. The
// column name is the first token of each line inside the parens — the shape
// every statement in renderSetupDDL uses.
func parseCreatedTableColumns(t *testing.T, stmts []string) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	for _, s := range stmts {
		m := createTableRe.FindStringSubmatch(s)
		if m == nil {
			continue
		}
		var cols []string
		for _, line := range strings.Split(m[2], "\n") {
			f := strings.Fields(strings.TrimSpace(line))
			if len(f) < 2 || f[0] == ")" || strings.HasPrefix(f[0], "CONSTRAINT") {
				continue
			}
			cols = append(cols, strings.Trim(f[0], `"`))
		}
		out[m[1]] = cols
	}
	return out
}

// --- small helpers ----------------------------------------------------------

func catalogNames(t *testing.T, path, kind string) []string {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.QueryContext(bg(),
		`SELECT name FROM sqlite_master WHERE type = ? ORDER BY name`, kind)
	if err != nil {
		t.Fatalf("read sqlite_master: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("scan sqlite_master: %v", err)
		}
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate sqlite_master: %v", err)
	}
	return out
}

func tableCols(t *testing.T, path, table string) []string {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open for table_info: %v", err)
	}
	defer func() { _ = db.Close() }()
	cols, err := (&localExecutor{db: db}).tableColumns(bg(), table)
	if err != nil {
		t.Fatalf("table_info(%q): %v", table, err)
	}
	return cols
}

func queryRow(t *testing.T, path, q string) *sql.Row {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open for query: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db.QueryRowContext(bg(), q)
}

func contains(hay []string, needle string) bool {
	for _, h := range hay {
		if h == needle {
			return true
		}
	}
	return false
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

func sortedKeys(m map[string][]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

var _ = context.Background
