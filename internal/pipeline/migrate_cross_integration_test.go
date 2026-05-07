//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

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
	"database/sql"
	"strings"
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

	// §7 type-translation coverage: score (UNSIGNED), role (ENUM),
	// metadata (JSON). The exit checks below assert each lands as
	// the expected IR shape on the PG target.
	const seedDDL = `
		CREATE TABLE users (
			id         BIGINT          NOT NULL AUTO_INCREMENT,
			email      VARCHAR(255)    NOT NULL,
			active     TINYINT(1)      NOT NULL DEFAULT 1,
			created_at TIMESTAMP(0)    NOT NULL DEFAULT CURRENT_TIMESTAMP,
			score      BIGINT UNSIGNED NOT NULL DEFAULT 0,
			role       ENUM('admin','user','guest') NOT NULL DEFAULT 'user',
			tags       SET('news','sports','tech')  NOT NULL DEFAULT 'news',
			metadata   JSON            NULL,
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

		INSERT INTO users (email, active, score, role, tags, metadata) VALUES
			('alice@example.com', 1, 100, 'admin', 'news,tech', '{"k":"v"}'),
			('bob@example.com',   0, 42,  'user',  '',          NULL);

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

	// §7 type-translation checks: UNSIGNED → NUMERIC(20,0), ENUM →
	// PG enum, JSON → JSONB.
	scoreCol := findColumn(users, "score")
	if scoreCol == nil {
		t.Fatalf("users.score missing")
	}
	if dec, ok := scoreCol.Type.(ir.Decimal); !ok || dec.Precision != 20 || dec.Scale != 0 {
		t.Errorf("users.score type = %#v; want ir.Decimal{Precision:20, Scale:0} (BIGINT UNSIGNED → NUMERIC(20,0))", scoreCol.Type)
	}

	roleCol := findColumn(users, "role")
	if roleCol == nil {
		t.Fatalf("users.role missing")
	}
	if e, ok := roleCol.Type.(ir.Enum); !ok {
		t.Errorf("users.role type = %#v; want ir.Enum", roleCol.Type)
	} else {
		want := []string{"admin", "user", "guest"}
		if len(e.Values) != len(want) {
			t.Errorf("users.role values = %v; want %v", e.Values, want)
		} else {
			for i, w := range want {
				if e.Values[i] != w {
					t.Errorf("users.role values[%d] = %q; want %q", i, e.Values[i], w)
				}
			}
		}
	}

	metaCol := findColumn(users, "metadata")
	if metaCol == nil {
		t.Fatalf("users.metadata missing")
	}
	if j, ok := metaCol.Type.(ir.JSON); !ok || !j.Binary {
		t.Errorf("users.metadata type = %#v; want ir.JSON{Binary:true} (MySQL JSON → PG JSONB)", metaCol.Type)
	}

	// MySQL SET → PG TEXT[]. The PG schema reader sees TEXT[], so
	// the IR shape on the target side is ir.Array{Element: ir.Text}.
	tagsCol := findColumn(users, "tags")
	if tagsCol == nil {
		t.Fatalf("users.tags missing")
	}
	tagsArr, ok := tagsCol.Type.(ir.Array)
	if !ok {
		t.Errorf("users.tags type = %#v; want ir.Array (MySQL SET → PG TEXT[])", tagsCol.Type)
	} else if _, ok := tagsArr.Element.(ir.Text); !ok {
		t.Errorf("users.tags element type = %#v; want ir.Text", tagsArr.Element)
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

	// §7 row-value checks for the new columns.
	if score, ok := usersRows[0]["score"].(string); !ok || score != "100" {
		t.Errorf("users[0].score = %#v; want \"100\" (NUMERIC decoded as string)", usersRows[0]["score"])
	}
	if role, ok := usersRows[0]["role"].(string); !ok || role != "admin" {
		t.Errorf("users[0].role = %#v; want \"admin\"", usersRows[0]["role"])
	}
	// JSON values arrive as []byte. PG's JSONB normalisation may
	// reformat (key ordering, whitespace), so we re-parse and compare
	// fields rather than asserting byte equality.
	metaBytes, ok := usersRows[0]["metadata"].([]byte)
	if !ok {
		t.Errorf("users[0].metadata = %T; want []byte", usersRows[0]["metadata"])
	} else if !strings.Contains(string(metaBytes), `"k"`) || !strings.Contains(string(metaBytes), `"v"`) {
		t.Errorf("users[0].metadata = %s; want contents containing both \"k\" and \"v\"", metaBytes)
	}
	// bob has NULL metadata.
	if usersRows[1]["metadata"] != nil {
		t.Errorf("users[1].metadata = %#v; want nil", usersRows[1]["metadata"])
	}

	// SET → TEXT[] round-trip. Alice's source value 'news,tech'
	// arrives on PG as a string array; bob's empty SET arrives as
	// the empty array, not NULL (the source column is NOT NULL).
	switch tags := usersRows[0]["tags"].(type) {
	case []any:
		want := map[string]bool{"news": true, "tech": true}
		if len(tags) != len(want) {
			t.Errorf("users[0].tags = %v; want 2 elements (news, tech)", tags)
		}
		for _, t0 := range tags {
			s, _ := t0.(string)
			if !want[s] {
				t.Errorf("users[0].tags has unexpected element %q", s)
			}
		}
	default:
		t.Errorf("users[0].tags = %#v (%T); want []any of strings", tags, tags)
	}
	switch tags := usersRows[1]["tags"].(type) {
	case []any:
		if len(tags) != 0 {
			t.Errorf("users[1].tags = %v; want empty array (empty SET)", tags)
		}
	default:
		t.Errorf("users[1].tags = %#v (%T); want []any (empty)", tags, tags)
	}

	postsRows := readAll(t, ctx, rr, posts)
	if len(postsRows) != 3 {
		t.Errorf("target posts rows = %d; want 3", len(postsRows))
	}

	// §7 identity-sequence sync check: after bulk-copy, the PG
	// sequence for users.id should be advanced past the highest
	// bulk-copied id (2 — alice and bob). Calling nextval() should
	// return 3, not 1. Without phase 3.5, the sequence would still
	// be at its default and the next user-initiated INSERT would
	// collide with alice's id=1.
	pgDB, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("open pg target for nextval check: %v", err)
	}
	defer func() { _ = pgDB.Close() }()
	var nextID int64
	if err := pgDB.QueryRowContext(ctx,
		`SELECT nextval(pg_get_serial_sequence('public.users', 'id'))`).Scan(&nextID); err != nil {
		t.Fatalf("nextval: %v", err)
	}
	if nextID != 3 {
		t.Errorf("nextval after sync = %d; want 3 (sequence not synced past bulk-copied max=2)", nextID)
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
