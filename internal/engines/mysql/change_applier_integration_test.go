//go:build integration

// Integration test for the MySQL ChangeApplier. Boots a MySQL
// container, opens the applier directly (bypassing CDC — we feed
// hand-built ir.Change events through the channel), and asserts:
//
//   - Insert/Update/Delete/Truncate land correctly on the target.
//   - Replaying the same event stream is idempotent (the
//     load-bearing property for CDC resume).
//   - Tables without a PRIMARY KEY fall back to plain INSERT.
//   - NULL columns in Before images produce IS NULL predicates.

package mysql

import (
	"context"
	"database/sql"
	"sort"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"

	"github.com/testcontainers/testcontainers-go"
	mysqltc "github.com/testcontainers/testcontainers-go/modules/mysql"
)

func startMySQLForApplier(t *testing.T) (dsn string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := mysqltc.Run(ctx,
		"mysql:8.0",
		mysqltc.WithDatabase("target_db"),
		mysqltc.WithUsername("root"),
		mysqltc.WithPassword("rootpw"),
	)
	if err != nil {
		t.Fatalf("start container: %v", err)
	}

	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}

	conn, err := container.ConnectionString(ctx, "parseTime=true")
	if err != nil {
		terminate()
		t.Fatalf("connection string: %v", err)
	}
	return conn, terminate
}

func applyMySQLApplier(t *testing.T, dsn, sqlText string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn+"&multiStatements=true")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, sqlText); err != nil {
		t.Fatalf("apply sql: %v", err)
	}
}

// pumpChanges pushes events into the applier on a goroutine and
// returns a function that closes the channel and waits for Apply to
// return. Mirrors how the production Streamer wires the channel.
//
// streamID is required by the applier interface (§5 control table);
// tests use a fixed value so position writes go to a single row.
const testStreamID = "test-stream"

func pumpChanges(t *testing.T, ctx context.Context, applier ir.ChangeApplier, events []ir.Change) {
	t.Helper()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}
	ch := make(chan ir.Change, len(events))
	for _, e := range events {
		ch <- e
	}
	close(ch)
	if err := applier.Apply(ctx, testStreamID, ch); err != nil {
		t.Fatalf("Apply: %v", err)
	}
}

// TestChangeApplier_ApplyAndIdempotency walks the canonical proof:
// apply Insert/Update/Delete/Truncate, assert state, replay the
// same events, assert state UNCHANGED. Idempotency is the property
// that lets continuous-sync resume work safely.
func TestChangeApplier_ApplyAndIdempotency(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	applyMySQLApplier(t, dsn, `
		CREATE TABLE users (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL,
			active TINYINT(1)  NOT NULL DEFAULT 1,
			PRIMARY KEY (id),
			UNIQUE KEY (email)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	// First pass: insert R1..R3, update R1, delete R2.
	events := []ir.Change{
		ir.Insert{Schema: "target_db", Table: "users", Row: ir.Row{"id": int64(1), "email": "r1@example.com", "active": true}},
		ir.Insert{Schema: "target_db", Table: "users", Row: ir.Row{"id": int64(2), "email": "r2@example.com", "active": true}},
		ir.Insert{Schema: "target_db", Table: "users", Row: ir.Row{"id": int64(3), "email": "r3@example.com", "active": true}},
		ir.Update{
			Schema: "target_db", Table: "users",
			Before: ir.Row{"id": int64(1), "email": "r1@example.com", "active": true},
			After:  ir.Row{"id": int64(1), "email": "r1@example.com", "active": false},
		},
		ir.Delete{
			Schema: "target_db", Table: "users",
			Before: ir.Row{"id": int64(2), "email": "r2@example.com", "active": true},
		},
	}
	pumpChanges(t, ctx, applier, events)

	got := selectAllUsers(t, dsn, "target_db")
	want := []userRow{
		{ID: 1, Email: "r1@example.com", Active: false},
		{ID: 3, Email: "r3@example.com", Active: true},
	}
	if !equalUsers(got, want) {
		t.Fatalf("after first apply: got %+v; want %+v", got, want)
	}

	// Second pass: replay the SAME events. With upsert on Insert and
	// tolerant Update/Delete (zero rows affected is fine), the state
	// should be unchanged.
	pumpChanges(t, ctx, applier, events)

	got2 := selectAllUsers(t, dsn, "target_db")
	if !equalUsers(got2, want) {
		t.Fatalf("after replay: got %+v; want %+v (idempotency violated)", got2, want)
	}
}

// TestChangeApplier_NoPKInsert verifies the documented fallback for
// tables without a PRIMARY KEY: plain INSERT. Replaying inserts on
// such a table produces duplicates — that's the documented best-
// effort behavior. Operators are warned in the package comment.
func TestChangeApplier_NoPKInsert(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	applyMySQLApplier(t, dsn, `
		CREATE TABLE events (
			payload VARCHAR(255) NOT NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	pumpChanges(t, ctx, applier, []ir.Change{
		ir.Insert{Schema: "target_db", Table: "events", Row: ir.Row{"payload": "first"}},
		ir.Insert{Schema: "target_db", Table: "events", Row: ir.Row{"payload": "second"}},
	})

	if got := countAllRows(t, dsn, "target_db", "events"); got != 2 {
		t.Errorf("after first apply: rows = %d; want 2", got)
	}

	// Replay: plain INSERT path produces duplicates. This is the
	// documented behavior for no-PK tables; the package comment
	// names it as best-effort.
	pumpChanges(t, ctx, applier, []ir.Change{
		ir.Insert{Schema: "target_db", Table: "events", Row: ir.Row{"payload": "first"}},
	})
	if got := countAllRows(t, dsn, "target_db", "events"); got != 3 {
		t.Errorf("after replay on no-PK table: rows = %d; want 3 (best-effort behavior — replays duplicate)", got)
	}
}

// TestChangeApplier_NullInWhere verifies the IS-NULL predicate path:
// an Update or Delete whose Before image carries a NULL column must
// produce a WHERE clause that actually matches that row.
func TestChangeApplier_NullInWhere(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	applyMySQLApplier(t, dsn, `
		CREATE TABLE notes (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			title VARCHAR(255) NULL,
			body  VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		INSERT INTO notes (id, title, body) VALUES (1, NULL, 'untitled');
	`)

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	// Delete the NULL-titled row. Before image carries title=nil;
	// the WHERE clause must use IS NULL, not = NULL.
	pumpChanges(t, ctx, applier, []ir.Change{
		ir.Delete{
			Schema: "target_db", Table: "notes",
			Before: ir.Row{"id": int64(1), "title": nil, "body": "untitled"},
		},
	})

	if got := countAllRows(t, dsn, "target_db", "notes"); got != 0 {
		t.Errorf("after delete: rows = %d; want 0 (WHERE IS NULL match failed)", got)
	}
}

// TestChangeApplier_Truncate verifies the TRUNCATE path empties the
// table and that replaying TRUNCATE on an already-empty table is a
// no-op (idempotent).
func TestChangeApplier_Truncate(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	applyMySQLApplier(t, dsn, `
		CREATE TABLE users (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		INSERT INTO users (email) VALUES ('a@x'), ('b@x'), ('c@x');
	`)

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	pumpChanges(t, ctx, applier, []ir.Change{
		ir.Truncate{Schema: "target_db", Table: "users"},
	})
	if got := countAllRows(t, dsn, "target_db", "users"); got != 0 {
		t.Fatalf("after truncate: rows = %d; want 0", got)
	}

	// Replay: idempotent.
	pumpChanges(t, ctx, applier, []ir.Change{
		ir.Truncate{Schema: "target_db", Table: "users"},
	})
	if got := countAllRows(t, dsn, "target_db", "users"); got != 0 {
		t.Errorf("after replay truncate: rows = %d; want 0", got)
	}
}

// ---- Test helpers ----

type userRow struct {
	ID     int64
	Email  string
	Active bool
}

func selectAllUsers(t *testing.T, dsn, schema string) []userRow {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, "SELECT id, email, active FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var out []userRow
	for rows.Next() {
		var u userRow
		if err := rows.Scan(&u.ID, &u.Email, &u.Active); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, u)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func countAllRows(t *testing.T, dsn, schema, table string) int {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

func equalUsers(a, b []userRow) bool {
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
