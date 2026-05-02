//go:build integration

// Integration test for the MySQL snapshot+CDC handoff. Boots a MySQL
// container with binlog enabled, seeds rows R1..R5, opens a snapshot
// stream, inserts R6 on a separate connection (so it commits *after*
// the snapshot's logical clock), and asserts:
//
//   - bulk-copy via stream.Rows yields exactly R1..R5 (no overlap),
//   - CDC via stream.Changes yields exactly the R6 insert (no gap).
//
// This is the canonical no-gap, no-overlap proof for the §4 chunk.

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

// startMySQLForSnapshotCDC boots a MySQL container with binlog
// enabled. Identical to the standalone CDC test's helper but kept
// local so this file is self-contained.
func startMySQLForSnapshotCDC(t *testing.T) (dsn string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := mysqltc.Run(ctx,
		"mysql:8.0",
		mysqltc.WithDatabase("source_db"),
		mysqltc.WithUsername("root"),
		mysqltc.WithPassword("rootpw"),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Cmd: []string{
					"mysqld",
					"--server-id=1",
					"--log-bin=mysql-bin",
					"--binlog-format=ROW",
					"--binlog-row-image=FULL",
				},
			},
		}),
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

func applyMySQLSnap(t *testing.T, dsn, sqlText string) {
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

// TestSnapshotStream_NoGapNoOverlap is the load-bearing test for §4.
//
// Sequence:
//
//  1. Seed R1..R5 (committed before snapshot).
//  2. Open SnapshotStream — captures binlog position P.
//  3. On a separate connection (NOT the snapshot conn), INSERT R6.
//     This commits at a position > P, AFTER the snapshot's logical clock.
//  4. Drain stream.Rows → assert exactly {R1..R5}.
//  5. Drain stream.Changes → assert exactly the R6 insert.
//
// The properties that make this load-bearing:
//
//   - If R6 appears in step 4, there's overlap (snapshot wasn't pinned
//     to the captured position).
//   - If R6 doesn't appear in step 5, there's a gap (CDC missed
//     events between snapshot and stream start).
func TestSnapshotStream_NoGapNoOverlap(t *testing.T) {
	dsn, cleanup := startMySQLForSnapshotCDC(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		INSERT INTO users (email) VALUES
			('r1@example.com'),
			('r2@example.com'),
			('r3@example.com'),
			('r4@example.com'),
			('r5@example.com');
	`
	applyMySQLSnap(t, dsn, seedDDL)

	eng := Engine{Flavor: FlavorVanilla}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	stream, err := eng.OpenSnapshotStream(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSnapshotStream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// Step 3 — concurrent insert on a SEPARATE connection. Same DSN
	// so it's the same database; different *sql.DB pool so the
	// snapshot tx doesn't see this connection's changes.
	applyMySQLSnap(t, dsn, "INSERT INTO users (email) VALUES ('r6@example.com');")

	// Step 4 — drain stream.Rows. Build the schema for the read.
	usersTable := schemaForUsers()
	bulkRows := drainAllRows(t, ctx, stream.Rows, usersTable)
	bulkEmails := emailsOf(bulkRows)
	want := []string{"r1@example.com", "r2@example.com", "r3@example.com", "r4@example.com", "r5@example.com"}
	if !equalStringSlices(bulkEmails, want) {
		t.Fatalf("bulk rows = %v; want exactly %v (overlap or missing rows)", bulkEmails, want)
	}

	// Step 5 — start CDC from the captured position. Should yield
	// exactly the R6 insert. Block until it shows up; cdc receive
	// loop runs in its own goroutine.
	changes, err := stream.Changes.StreamChanges(ctx, stream.Position)
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	got := drainSnapshotChanges(t, ctx, changes, 1, 30*time.Second)
	if len(got) != 1 {
		t.Fatalf("got %d changes; want 1 (R6 insert)", len(got))
	}
	insR6, ok := got[0].(ir.Insert)
	if !ok {
		t.Fatalf("change[0] = %T; want ir.Insert", got[0])
	}
	if email, _ := insR6.Row["email"].(string); email != "r6@example.com" {
		t.Errorf("R6 insert email = %#v; want r6@example.com", insR6.Row["email"])
	}
}

// schemaForUsers returns an [ir.Table] matching the seed DDL above —
// just enough for the RowReader to issue its SELECT and decode rows.
func schemaForUsers() *ir.Table {
	return &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
			{Name: "email", Type: ir.Varchar{Length: 255}},
		},
	}
}

// drainAllRows reads every row that ReadRows produces for the given
// table. Returns the slice in arrival order.
func drainAllRows(t *testing.T, ctx context.Context, rr ir.RowReader, table *ir.Table) []ir.Row {
	t.Helper()
	ch, err := rr.ReadRows(ctx, table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	var out []ir.Row
	for row := range ch {
		out = append(out, row)
	}
	return out
}

// drainSnapshotChanges is the same drainer pattern used by the
// standalone CDC integration test: take up to want events with a
// timeout. Returns whatever it has if the timeout fires or the
// channel closes.
func drainSnapshotChanges(
	t *testing.T,
	ctx context.Context,
	changes <-chan ir.Change,
	want int,
	timeout time.Duration,
) []ir.Change {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	got := make([]ir.Change, 0, want)
	for len(got) < want {
		select {
		case c, ok := <-changes:
			if !ok {
				return got
			}
			got = append(got, c)
		case <-deadline.C:
			return got
		case <-ctx.Done():
			return got
		}
	}
	return got
}

func emailsOf(rows []ir.Row) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		s, _ := r["email"].(string)
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

func equalStringSlices(a, b []string) bool {
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
