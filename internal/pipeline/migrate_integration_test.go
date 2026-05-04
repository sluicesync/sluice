//go:build integration

// End-to-end integration test for the simple-mode orchestrator.
// Boots one MySQL container, creates a source and a target database,
// seeds the source, runs pipeline.Migrator, and verifies the target
// matches.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"
	"github.com/orware/sluice/internal/ir"

	// Register the mysql engine so engines.Get("mysql") works.
	_ "github.com/orware/sluice/internal/engines/mysql"

	"github.com/testcontainers/testcontainers-go"
	mysqltc "github.com/testcontainers/testcontainers-go/modules/mysql"
)

// startMySQL boots a MySQL container, then creates the two databases
// the migration test needs. Returns DSNs for source_db and target_db
// plus a cleanup callback. Skips the test cleanly when no Docker
// provider is available.
func startMySQL(t *testing.T) (sourceDSN, targetDSN string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := mysqltc.Run(ctx,
		"mysql:8.0",
		mysqltc.WithDatabase("source_db"),
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

	srcConn, err := container.ConnectionString(ctx, "parseTime=true")
	if err != nil {
		terminate()
		t.Fatalf("connection string: %v", err)
	}

	// Create the target database alongside the source. We connect via
	// the source DSN (which authorises root) and run a CREATE DATABASE.
	db, err := sql.Open("mysql", srcConn+"&multiStatements=true")
	if err != nil {
		terminate()
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.ExecContext(ctx, "CREATE DATABASE target_db"); err != nil {
		terminate()
		t.Fatalf("create target_db: %v", err)
	}

	// Build the two DSNs — same host/credentials, different db names.
	tgtConn, err := buildMySQLDSN(srcConn, "target_db")
	if err != nil {
		terminate()
		t.Fatalf("build target DSN: %v", err)
	}

	return srcConn, tgtConn, terminate
}

// buildMySQLDSN replaces the database name in a MySQL DSN. The MySQL
// DSN format (driver-specific, not a standard URL) places the DB name
// after the host portion: `user:pass@tcp(host:port)/dbname?params`.
// We swap in the new dbname while preserving everything else.
func buildMySQLDSN(orig, newDB string) (string, error) {
	// Find the slash that begins the DB name. The DSN looks like:
	//   user:pass@tcp(host:port)/dbname?params
	// We can locate it via the LAST `/` before any `?`.
	q := orig
	params := ""
	if idx := indexByte(orig, '?'); idx >= 0 {
		q = orig[:idx]
		params = orig[idx:]
	}
	slash := lastIndexByte(q, '/')
	if slash < 0 {
		return "", fmt.Errorf("DSN has no db-name separator: %q", orig)
	}
	return q[:slash+1] + newDB + params, nil
}

// stdlib bytes helpers (kept inline so the file is self-contained).
func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

func lastIndexByte(s string, b byte) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == b {
			return i
		}
	}
	return -1
}

// applyMySQLDDL runs an arbitrary multi-statement script against a
// MySQL DSN.
func applyMySQLDDL(t *testing.T, dsn, ddl string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn+"&multiStatements=true")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		t.Fatalf("apply ddl: %v", err)
	}
}

// TestMigrate_MySQLToMySQL exercises the orchestrator end-to-end:
// schema apply, data copy, indexes, foreign keys.
func TestMigrate_MySQLToMySQL(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQL(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE users (
			id        BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
			email     VARCHAR(255)    NOT NULL,
			active    TINYINT(1)      NOT NULL DEFAULT 1,
			created_at TIMESTAMP(0)   NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (id),
			UNIQUE KEY users_email_unique (email)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		CREATE TABLE posts (
			id      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
			user_id BIGINT UNSIGNED NOT NULL,
			body    TEXT            NOT NULL,
			PRIMARY KEY (id),
			KEY posts_user_id_idx (user_id),
			CONSTRAINT posts_user_id_fk FOREIGN KEY (user_id)
				REFERENCES users (id) ON DELETE CASCADE
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		INSERT INTO users (email, active) VALUES
			('alice@example.com', 1),
			('bob@example.com',   0);

		INSERT INTO posts (user_id, body) VALUES
			(1, 'first post'),
			(1, 'second post'),
			(2, 'a post by bob');
	`
	applyMySQLDDL(t, sourceDSN, seedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	mig := &Migrator{
		Source:    mysqlEng,
		Target:    mysqlEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	// ---- Verify target schema matches what we expect ----
	sr, err := mysqlEng.OpenSchemaReader(ctx, targetDSN)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(sr)

	got, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	if len(got.Tables) != 2 {
		t.Fatalf("target tables = %d; want 2 (have: %v)", len(got.Tables), targetTableNames(got))
	}

	users := findTable(got, "users")
	posts := findTable(got, "posts")
	if users == nil || posts == nil {
		t.Fatalf("missing target tables; have %v", targetTableNames(got))
	}

	// PK check
	if users.PrimaryKey == nil || users.PrimaryKey.Columns[0].Column != "id" {
		t.Errorf("users PK = %#v; want PK on id", users.PrimaryKey)
	}

	// Secondary unique index
	if len(users.Indexes) != 1 || !users.Indexes[0].Unique || users.Indexes[0].Name != "users_email_unique" {
		t.Errorf("users indexes = %#v; want one unique on email", users.Indexes)
	}

	// FK: posts.user_id → users.id with ON DELETE CASCADE
	if len(posts.ForeignKeys) != 1 {
		t.Fatalf("posts FKs = %d; want 1", len(posts.ForeignKeys))
	}
	fk := posts.ForeignKeys[0]
	if fk.ReferencedTable != "users" || fk.OnDelete != ir.FKActionCascade {
		t.Errorf("posts FK = %+v; want users on-delete cascade", fk)
	}

	// ---- Verify target data matches what we wrote on the source ----
	rr, err := mysqlEng.OpenRowReader(ctx, targetDSN)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer closeIf(rr)

	usersRows := readAll(t, ctx, rr, users)
	if len(usersRows) != 2 {
		t.Errorf("target users rows = %d; want 2", len(usersRows))
	}
	// Rows are returned in PK order, so usersRows[0] is alice.
	if email, ok := usersRows[0]["email"].(string); !ok || email != "alice@example.com" {
		t.Errorf("users[0].email = %#v; want 'alice@example.com'", usersRows[0]["email"])
	}
	if active, ok := usersRows[0]["active"].(bool); !ok || !active {
		t.Errorf("users[0].active = %#v; want true", usersRows[0]["active"])
	}

	postsRows := readAll(t, ctx, rr, posts)
	if len(postsRows) != 3 {
		t.Errorf("target posts rows = %d; want 3", len(postsRows))
	}
}

// readAll drains rr.ReadRows(table) into a slice for assertion.
func readAll(t *testing.T, ctx context.Context, rr ir.RowReader, table *ir.Table) []ir.Row {
	t.Helper()
	ch, err := rr.ReadRows(ctx, table)
	if err != nil {
		t.Fatalf("ReadRows %q: %v", table.Name, err)
	}
	var out []ir.Row
	for row := range ch {
		out = append(out, row)
	}
	return out
}

// findTable searches by name. Returns nil when no match.
func findTable(s *ir.Schema, name string) *ir.Table {
	for _, t := range s.Tables {
		if t.Name == name {
			return t
		}
	}
	return nil
}

func targetTableNames(s *ir.Schema) []string {
	out := make([]string, 0, len(s.Tables))
	for _, t := range s.Tables {
		out = append(out, t.Name)
	}
	return out
}

// reflect.DeepEqual import keeper — referenced from the helpers above
// even when tests don't explicitly call it; keeps the import set
// honest if assertions expand.
var _ = reflect.DeepEqual
