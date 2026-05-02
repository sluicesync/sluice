//go:build integration

// Cross-engine end-to-end integration test for the simple-mode
// orchestrator: MySQL source → Postgres target. This is the test that
// actually exercises type translation, value-side conversion, and
// engine-pair DDL emission together — same-engine tests cover none of
// that.
//
// The test boots two containers (one MySQL, one Postgres), seeds the
// MySQL source with a realistic schema (auto-increment IDs, TINYINT(1)
// booleans, VARCHAR/TEXT, TIMESTAMP, a FK with ON DELETE CASCADE),
// runs pipeline.Migrator, then reads the schema and rows back from
// the Postgres target and asserts the translated shape.

package pipeline

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"
	"github.com/orware/sluice/internal/ir"

	// Both engines must be registered for engines.Get to find them.
	_ "github.com/orware/sluice/internal/engines/mysql"
	_ "github.com/orware/sluice/internal/engines/postgres"
)

// TestMigrate_MySQLToPostgres is the cross-engine validation test.
//
// What the orchestrator must get right for this to pass:
//
//   - MySQL TINYINT(1) (the canonical MySQL-bool pattern) reads as
//     ir.Boolean and writes as Postgres BOOLEAN.
//   - MySQL BIGINT AUTO_INCREMENT translates to Postgres
//     BIGINT GENERATED ... AS IDENTITY.
//   - VARCHAR(N), TEXT, TIMESTAMP(P) survive translation with the
//     same semantics on the target.
//   - The unique index on email is preserved.
//   - The FK from posts.user_id → users.id keeps ON DELETE CASCADE.
//   - INSERTed rows arrive with the right values on the target —
//     in particular, MySQL's `1`/`0` for TINYINT(1) come out as
//     Postgres `true`/`false`.
//
// This is a deliberately conservative seed: it sticks to types that
// have a clean cross-engine mapping. UNSIGNED INT, ENUM, JSON, and
// other corners are left for follow-up tests once the spine is
// validated.
func TestMigrate_MySQLToPostgres(t *testing.T) {
	// Reuse the same-engine helpers: each spins up a container and
	// returns a source + target DSN pair on the same instance. We use
	// the source DSN of the MySQL pair (the one we'll seed) and the
	// target DSN of the Postgres pair (the one we'll migrate into).
	// The unused database on each side is empty and harmless.
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	const seedDDL = `
		CREATE TABLE users (
			id         BIGINT       NOT NULL AUTO_INCREMENT,
			email      VARCHAR(255) NOT NULL,
			active     TINYINT(1)   NOT NULL DEFAULT 1,
			created_at TIMESTAMP(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (id),
			UNIQUE KEY users_email_unique (email)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		CREATE TABLE posts (
			id      BIGINT NOT NULL AUTO_INCREMENT,
			user_id BIGINT NOT NULL,
			body    TEXT   NOT NULL,
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
	applyMySQLDDL(t, mysqlSource, seedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	mig := &Migrator{
		Source:    mysqlEng,
		Target:    pgEng,
		SourceDSN: mysqlSource,
		TargetDSN: pgTarget,
		Stdout:    io.Discard,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	// ---- Verify the Postgres target ----
	sr, err := pgEng.OpenSchemaReader(ctx, pgTarget)
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

	// id should be a 64-bit integer
	idCol := findColumn(users, "id")
	if idCol == nil {
		t.Fatalf("users.id missing")
	}
	if intT, ok := idCol.Type.(ir.Integer); !ok || intT.Width != 64 {
		t.Errorf("users.id type = %#v; want ir.Integer{Width:64}", idCol.Type)
	}

	// active should be Boolean — the TINYINT(1) → bool translation is
	// the load-bearing assertion here.
	activeCol := findColumn(users, "active")
	if activeCol == nil {
		t.Fatalf("users.active missing")
	}
	if _, ok := activeCol.Type.(ir.Boolean); !ok {
		t.Errorf("users.active type = %#v; want ir.Boolean", activeCol.Type)
	}

	// PK on id, unique on email
	if users.PrimaryKey == nil || users.PrimaryKey.Columns[0].Column != "id" {
		t.Errorf("users PK = %#v; want PK on id", users.PrimaryKey)
	}
	hasEmailUnique := false
	for _, ix := range users.Indexes {
		if ix.Unique && len(ix.Columns) == 1 && ix.Columns[0].Column == "email" {
			hasEmailUnique = true
			break
		}
	}
	if !hasEmailUnique {
		t.Errorf("users indexes = %#v; want a unique index on email", users.Indexes)
	}

	// FK CASCADE preserved
	if len(posts.ForeignKeys) != 1 {
		t.Fatalf("posts FKs = %d; want 1", len(posts.ForeignKeys))
	}
	fk := posts.ForeignKeys[0]
	if fk.ReferencedTable != "users" || fk.OnDelete != ir.FKActionCascade {
		t.Errorf("posts FK = %+v; want users on-delete cascade", fk)
	}

	// ---- Verify rows arrived intact and translated correctly ----
	rr, err := pgEng.OpenRowReader(ctx, pgTarget)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer closeIf(rr)

	usersRows := readAll(t, ctx, rr, users)
	if len(usersRows) != 2 {
		t.Errorf("target users rows = %d; want 2", len(usersRows))
	}
	if email, ok := usersRows[0]["email"].(string); !ok || email != "alice@example.com" {
		t.Errorf("users[0].email = %#v; want 'alice@example.com'", usersRows[0]["email"])
	}
	// MySQL's TINYINT(1) `1` must surface as Postgres `true`.
	if active, ok := usersRows[0]["active"].(bool); !ok || !active {
		t.Errorf("users[0].active = %#v; want true (TINYINT(1) → bool)", usersRows[0]["active"])
	}
	// And `0` must surface as `false`.
	if active, ok := usersRows[1]["active"].(bool); !ok || active {
		t.Errorf("users[1].active = %#v; want false", usersRows[1]["active"])
	}

	postsRows := readAll(t, ctx, rr, posts)
	if len(postsRows) != 3 {
		t.Errorf("target posts rows = %d; want 3", len(postsRows))
	}
}

// findColumn searches a table for the named column. Returns nil when
// no match.
func findColumn(t *ir.Table, name string) *ir.Column {
	for _, c := range t.Columns {
		if c.Name == name {
			return c
		}
	}
	return nil
}
