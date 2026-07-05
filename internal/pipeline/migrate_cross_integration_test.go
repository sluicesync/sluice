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

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	// Both engines must be registered for engines.Get to find them.
	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
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
	defer migcore.CloseIf(sr)

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

	// §7 type-translation checks: BIGINT UNSIGNED → BIGINT (Bug 11
	// uniform mapping — was NUMERIC(20,0) pre-fix; the divergence
	// broke FK-to-AUTO_INCREMENT-PK creation for every default ORM
	// schema), ENUM → PG enum, JSON → JSONB.
	scoreCol := findColumn(users, "score")
	if scoreCol == nil {
		t.Fatalf("users.score missing")
	}
	if iv, ok := scoreCol.Type.(ir.Integer); !ok || iv.Width != 64 {
		t.Errorf("users.score type = %#v; want ir.Integer{Width:64} (BIGINT UNSIGNED → BIGINT, Bug 11)", scoreCol.Type)
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
	defer migcore.CloseIf(rr)

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
	// Bug 11: BIGINT UNSIGNED now lands as PG BIGINT, decoded as
	// int64 (was NUMERIC, decoded as string pre-fix).
	if score, ok := usersRows[0]["score"].(int64); !ok || score != 100 {
		t.Errorf("users[0].score = %#v; want int64(100) (BIGINT decoded as int64)", usersRows[0]["score"])
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

// TestMigrate_MySQLToPostgres_Bug8Constructs is the cross-engine
// regression test for validation-rig Bug 8: MySQL→PG migrate of the
// 8a/8b/8c construct class (JSON_VALID CHECK, `<=>` NULL-safe
// equality CHECK, NOW(N)/CURDATE() DEFAULTs) must (1) translate to
// well-formed PG DDL and (2) complete migrate cleanly — pre-v0.68.1
// each aborted the pipeline at the CREATE TABLE phase after partial
// table creation.
func TestMigrate_MySQLToPostgres_Bug8Constructs(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	// 8a: JSON_VALID() in a CHECK. 8b: <=> in a CHECK. 8c: now(3) and
	// curdate() parenthesised DEFAULTs. All three share the
	// expr_translate.go gap that pre-v0.68.1 leaked verbatim into PG.
	const seedDDL = `
		CREATE TABLE bug8 (
			id          INT          NOT NULL AUTO_INCREMENT,
			metadata    LONGTEXT     NOT NULL,
			a           INT          NULL,
			b           INT          NULL,
			created_at  DATETIME(3)  NOT NULL DEFAULT (now(3)),
			the_date    DATE         NOT NULL DEFAULT (curdate()),
			PRIMARY KEY (id),
			CONSTRAINT chk_json     CHECK (JSON_VALID(metadata)),
			CONSTRAINT chk_nullsafe CHECK (a <=> b OR a IS NULL)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		INSERT INTO bug8 (metadata, a, b) VALUES
			('{"k":"v"}', 1, 1),
			('[]',        NULL, 7);
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
		t.Fatalf("Migrator.Run (Bug 8 constructs must migrate cleanly): %v", err)
	}

	pgDB, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("open pg target: %v", err)
	}
	defer func() { _ = pgDB.Close() }()

	// All 2 rows must have arrived (CHECKs are satisfied by the seed).
	var n int
	if err := pgDB.QueryRowContext(ctx, `SELECT count(*) FROM bug8`).Scan(&n); err != nil {
		t.Fatalf("count bug8: %v", err)
	}
	if n != 2 {
		t.Errorf("bug8 row count = %d; want 2", n)
	}

	// 8a + 8b: both CHECK constraints must exist on the PG target
	// (translated, not dropped). Query pg_constraint for the table.
	var checkCount int
	if err := pgDB.QueryRowContext(ctx, `
		SELECT count(*) FROM pg_constraint
		WHERE conrelid = 'public.bug8'::regclass AND contype = 'c'`).Scan(&checkCount); err != nil {
		t.Fatalf("count check constraints: %v", err)
	}
	if checkCount < 2 {
		t.Errorf("bug8 CHECK constraints = %d; want >= 2 (chk_json + chk_nullsafe translated)", checkCount)
	}

	// 8a functional: the IS JSON predicate must reject malformed JSON
	// on the target (proves JSON_VALID → IS JSON, not a no-op).
	if _, err := pgDB.ExecContext(ctx,
		`INSERT INTO bug8 (metadata, a, b) VALUES ('not json', 1, 1)`); err == nil {
		t.Error("expected chk_json (IS JSON) to reject malformed JSON on PG; insert succeeded")
	}

	// 8c functional: the now(3)/curdate() defaults must be live PG
	// expressions — an INSERT omitting them must populate both.
	if _, err := pgDB.ExecContext(ctx,
		`INSERT INTO bug8 (metadata, a, b) VALUES ('{}', NULL, NULL)`); err != nil {
		t.Fatalf("insert relying on translated NOW(3)/CURDATE() defaults: %v", err)
	}
	var hasTs, hasDate bool
	if err := pgDB.QueryRowContext(ctx, `
		SELECT created_at IS NOT NULL, the_date IS NOT NULL
		FROM bug8 WHERE metadata = '{}'`).Scan(&hasTs, &hasDate); err != nil {
		t.Fatalf("read defaulted row: %v", err)
	}
	if !hasTs || !hasDate {
		t.Errorf("translated defaults not applied: created_at set=%v the_date set=%v", hasTs, hasDate)
	}
}

// TestMigrate_MySQLToPostgres_Bug11UnsignedBigintFK is the cross-engine
// regression pin for validation-rig Bug 11: a MySQL `bigint unsigned
// AUTO_INCREMENT` PRIMARY KEY plus a `bigint unsigned` FK child column
// referencing it. Pre-v0.68.2 the PK emitted PG `bigint ... IDENTITY`
// but the FK child widened to `numeric(20,0)`, so `ALTER TABLE ... ADD
// FOREIGN KEY` failed SQLSTATE 42804 (datatype mismatch) AFTER the
// target was partially created, invisible at `schema preview`. This is
// the DEFAULT schema shape of essentially every Rails/Laravel/Django/
// Sequelize/Prisma MySQL app. The fix maps `bigint unsigned` uniformly
// to PG `bigint`, so the FK matches the IDENTITY PK by construction.
//
// Two sub-cases:
//
//	minimal — the exact #11 repro (one parent PK, one child FK).
//	orm     — the ORM-default multi-table shape (users / posts /
//	          post_tags join table with a composite FK), modelling the
//	          real Rails/Laravel schema. Every FK must be enforced on
//	          the PG target (pg_get_constraintdef) and data must
//	          round-trip with referential integrity intact.
func TestMigrate_MySQLToPostgres_Bug11UnsignedBigintFK(t *testing.T) {
	t.Run("minimal", func(t *testing.T) {
		mysqlSource, _, mysqlCleanup := startMySQL(t)
		defer mysqlCleanup()
		_, pgTarget, pgCleanup := startPostgres(t)
		defer pgCleanup()

		// The exact #11 minimal repro: unsigned-bigint AUTO_INCREMENT
		// PK + unsigned-bigint FK child referencing it.
		const seedDDL = `
			CREATE TABLE parent (
				id   BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
				name VARCHAR(64)     NOT NULL,
				PRIMARY KEY (id)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

			CREATE TABLE child (
				id        BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
				parent_id BIGINT UNSIGNED NOT NULL,
				PRIMARY KEY (id),
				KEY child_parent_id_idx (parent_id),
				CONSTRAINT child_parent_fk FOREIGN KEY (parent_id)
					REFERENCES parent (id) ON DELETE CASCADE
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

			INSERT INTO parent (name) VALUES ('root'), ('other');
			INSERT INTO child (parent_id) VALUES (1), (1), (2);
		`
		applyMySQLDDL(t, mysqlSource, seedDDL)

		runBug11Migrate(t, mysqlSource, pgTarget)

		pgDB, err := sql.Open("pgx", pgTarget)
		if err != nil {
			t.Fatalf("open pg target: %v", err)
		}
		defer func() { _ = pgDB.Close() }()

		// The FK must exist AND be enforced on the PG target. Pre-fix
		// the migrate aborted before this constraint was ever created.
		assertFKEnforced(t, ctx2min(t), pgDB, "public.child", "child_parent_fk")

		// parent.id / child.parent_id must both be PG bigint (uniform
		// mapping). Pre-fix child.parent_id was numeric(20,0).
		for _, c := range []struct{ tbl, col string }{
			{"parent", "id"}, {"child", "id"}, {"child", "parent_id"},
		} {
			var dt string
			if err := pgDB.QueryRow(`
				SELECT data_type FROM information_schema.columns
				WHERE table_name = $1 AND column_name = $2`, c.tbl, c.col).Scan(&dt); err != nil {
				t.Fatalf("read %s.%s data_type: %v", c.tbl, c.col, err)
			}
			if dt != "bigint" {
				t.Errorf("%s.%s data_type = %q; want \"bigint\" (Bug 11 uniform mapping)", c.tbl, c.col, dt)
			}
		}

		// Referential integrity: the FK rejects an orphan child.
		if _, err := pgDB.Exec(
			`INSERT INTO child (parent_id) VALUES (999999)`,
		); err == nil {
			t.Error("expected child_parent_fk to reject orphan parent_id=999999; insert succeeded")
		}

		// Data round-trips.
		var childCount int
		if err := pgDB.QueryRow(`SELECT count(*) FROM child`).Scan(&childCount); err != nil {
			t.Fatalf("count child: %v", err)
		}
		if childCount != 3 {
			t.Errorf("child row count = %d; want 3", childCount)
		}
	})

	t.Run("orm", func(t *testing.T) {
		mysqlSource, _, mysqlCleanup := startMySQL(t)
		defer mysqlCleanup()
		_, pgTarget, pgCleanup := startPostgres(t)
		defer pgCleanup()

		// The ORM-default multi-table shape: every `id` is
		// `bigint unsigned AUTO_INCREMENT PRIMARY KEY` and every FK
		// child column is `bigint unsigned` — the canonical
		// Rails/Laravel/Django schema. post_tags is a join table with
		// a composite PK and two FKs.
		const seedDDL = `
			CREATE TABLE users (
				id    BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
				email VARCHAR(255)    NOT NULL,
				PRIMARY KEY (id),
				UNIQUE KEY users_email_unique (email)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

			CREATE TABLE posts (
				id      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
				user_id BIGINT UNSIGNED NOT NULL,
				title   VARCHAR(255)    NOT NULL,
				PRIMARY KEY (id),
				KEY posts_user_id_idx (user_id),
				CONSTRAINT posts_user_fk FOREIGN KEY (user_id)
					REFERENCES users (id) ON DELETE CASCADE
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

			CREATE TABLE tags (
				id   BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
				name VARCHAR(64)     NOT NULL,
				PRIMARY KEY (id)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

			CREATE TABLE post_tags (
				post_id BIGINT UNSIGNED NOT NULL,
				tag_id  BIGINT UNSIGNED NOT NULL,
				PRIMARY KEY (post_id, tag_id),
				KEY post_tags_tag_id_idx (tag_id),
				CONSTRAINT post_tags_post_fk FOREIGN KEY (post_id)
					REFERENCES posts (id) ON DELETE CASCADE,
				CONSTRAINT post_tags_tag_fk FOREIGN KEY (tag_id)
					REFERENCES tags (id) ON DELETE CASCADE
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

			INSERT INTO users (email) VALUES ('a@x.com'), ('b@x.com');
			INSERT INTO posts (user_id, title) VALUES (1, 'hello'), (1, 'world'), (2, 'hi');
			INSERT INTO tags (name) VALUES ('red'), ('blue');
			INSERT INTO post_tags (post_id, tag_id) VALUES (1, 1), (1, 2), (2, 1), (3, 2);
		`
		applyMySQLDDL(t, mysqlSource, seedDDL)

		runBug11Migrate(t, mysqlSource, pgTarget)

		pgDB, err := sql.Open("pgx", pgTarget)
		if err != nil {
			t.Fatalf("open pg target: %v", err)
		}
		defer func() { _ = pgDB.Close() }()
		ctx := ctx2min(t)

		// All three FKs (including both legs of the composite-PK join
		// table) must be enforced on the PG target.
		assertFKEnforced(t, ctx, pgDB, "public.posts", "posts_user_fk")
		assertFKEnforced(t, ctx, pgDB, "public.post_tags", "post_tags_post_fk")
		assertFKEnforced(t, ctx, pgDB, "public.post_tags", "post_tags_tag_fk")

		// Data integrity: every join-table row resolves to a live
		// post and tag.
		var joinCount int
		if err := pgDB.QueryRowContext(ctx, `
			SELECT count(*) FROM post_tags pt
			JOIN posts p ON p.id = pt.post_id
			JOIN tags  g ON g.id = pt.tag_id`).Scan(&joinCount); err != nil {
			t.Fatalf("join post_tags: %v", err)
		}
		if joinCount != 4 {
			t.Errorf("resolved join rows = %d; want 4 (referential integrity broken)", joinCount)
		}

		// FK enforcement is live: orphan insert into the join table is
		// rejected.
		if _, err := pgDB.ExecContext(ctx,
			`INSERT INTO post_tags (post_id, tag_id) VALUES (1, 999999)`); err == nil {
			t.Error("expected post_tags_tag_fk to reject orphan tag_id; insert succeeded")
		}
	})
}

// runBug11Migrate runs a MySQL→PG migration and fails the test if it
// returns an error. Pre-v0.68.2 the Bug 11 schemas aborted here at the
// constraint phase (SQLSTATE 42804) after partial table creation.
func runBug11Migrate(t *testing.T, mysqlSource, pgTarget string) {
	t.Helper()
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
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Migrator.Run (Bug 11 unsigned-bigint FK must migrate cleanly): %v", err)
	}
}

// assertFKEnforced asserts a named foreign-key constraint exists on
// the given PG relation and is a FOREIGN KEY (pg_get_constraintdef).
func assertFKEnforced(t *testing.T, ctx context.Context, db *sql.DB, relation, conname string) {
	t.Helper()
	var def string
	err := db.QueryRowContext(ctx, `
		SELECT pg_get_constraintdef(c.oid)
		FROM pg_constraint c
		WHERE c.conrelid = $1::regclass AND c.conname = $2 AND c.contype = 'f'`,
		relation, conname).Scan(&def)
	if err != nil {
		t.Fatalf("FK %q on %s not found / not enforced: %v", conname, relation, err)
	}
	if !strings.Contains(strings.ToUpper(def), "FOREIGN KEY") {
		t.Errorf("constraint %q on %s = %q; want a FOREIGN KEY definition", conname, relation, def)
	}
}

// ctx2min returns a 2-minute context bound to the test's lifetime.
// Shared by the Bug 11 sub-tests so each doesn't repeat the
// context.WithTimeout + cleanup boilerplate.
func ctx2min(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return ctx
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

// TestMigrate_MySQLToPostgres_NoPKUniqueKey_IdempotentCopy is the Bug 125
// cross-engine pin (MySQL source -> PG target): a MySQL no-PK table with
// a NOT-NULL UNIQUE key migrates to PG with the chosen unique key
// inline-promoted as a UNIQUE CONSTRAINT (so PG's ON CONFLICT can infer
// against it), the row count matches, and a second idempotent copy pass
// (simulating a VStream COPY catch-up re-emission) neither duplicates nor
// errors. A truly-keyless MySQL table is refused loudly on the idempotent
// path.
func TestMigrate_MySQLToPostgres_NoPKUniqueKey_IdempotentCopy(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	// connections: NO PRIMARY KEY, a NOT-NULL UNIQUE key on id (the
	// real-world metrics.connections shape). keyless: no PK, no unique.
	const seedDDL = `
		CREATE TABLE connections (
			id      BIGINT       NOT NULL,
			payload VARCHAR(255) NULL,
			UNIQUE KEY connections_uq_id (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		INSERT INTO connections (id, payload) VALUES
			(1, 'a'), (2, 'b'), (3, 'c');
	`
	applyMySQLDDL(t, mysqlSource, seedDDL)

	mysqlEng, _ := engines.Get("mysql")
	pgEng, _ := engines.Get("postgres")

	mig := &Migrator{
		Source:    mysqlEng,
		Target:    pgEng,
		SourceDSN: mysqlSource,
		TargetDSN: pgTarget,
	}
	ctx := ctx2min(t)
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run (no-PK unique-key table must migrate): %v", err)
	}

	pgdb, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("open pg: %v", err)
	}
	defer func() { _ = pgdb.Close() }()

	// (1) The non-null UNIQUE key must physically exist on the target as
	// a UNIQUE constraint/index — that's the precondition for ON CONFLICT
	// to infer against it during the idempotent copy.
	var uniqueCount int
	if err := pgdb.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM pg_constraint c
		WHERE c.conrelid = 'public.connections'::regclass
		  AND c.contype = 'u'`).Scan(&uniqueCount); err != nil {
		t.Fatalf("probe unique constraint: %v", err)
	}
	if uniqueCount == 0 {
		t.Fatal("no UNIQUE constraint on connections; the cold-start COPY's ON CONFLICT would have nothing to infer against (Bug 125 inline-promotion missing)")
	}

	// (2) Row count matches the source.
	var n int
	if err := pgdb.QueryRowContext(ctx, "SELECT COUNT(*) FROM connections").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 3 {
		t.Fatalf("connections rows = %d; want 3", n)
	}

	// (3) A second idempotent copy pass against the SAME rows (the
	// VStream COPY catch-up re-emission shape) must upsert, not duplicate
	// or error on the unique key. Read the source schema for the table
	// shape, then drive WriteRowsIdempotent directly with re-emitted rows.
	sr, err := pgEng.OpenSchemaReader(ctx, pgTarget)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer migcore.CloseIf(sr)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	tbl := findTable(schema, "connections")
	if tbl == nil {
		t.Fatal("connections table missing on target")
	}
	if tbl.PrimaryKey != nil {
		t.Fatalf("connections has a PrimaryKey on target = %#v; want none (no-PK shape)", tbl.PrimaryKey)
	}

	rw, err := pgEng.OpenRowWriter(ctx, pgTarget)
	if err != nil {
		t.Fatalf("OpenRowWriter: %v", err)
	}
	defer migcore.CloseIf(rw)
	iw, ok := rw.(ir.IdempotentRowWriter)
	if !ok {
		t.Fatal("pg RowWriter does not implement IdempotentRowWriter")
	}

	rows := make(chan ir.Row, 3)
	rows <- ir.Row{"id": int64(1), "payload": "a-replayed"}
	rows <- ir.Row{"id": int64(2), "payload": "b"}
	rows <- ir.Row{"id": int64(3), "payload": "c"}
	close(rows)
	if err := iw.WriteRowsIdempotent(ctx, tbl, rows); err != nil {
		t.Fatalf("WriteRowsIdempotent replay on no-PK unique-key table: %v (must upsert, not collide)", err)
	}
	if err := pgdb.QueryRowContext(ctx, "SELECT COUNT(*) FROM connections").Scan(&n); err != nil {
		t.Fatalf("count after replay: %v", err)
	}
	if n != 3 {
		t.Errorf("connections rows after idempotent replay = %d; want 3 (no duplication)", n)
	}
	// The replayed row's non-key column must have been refreshed (DO
	// UPDATE SET), proving the upsert is a real upsert.
	var payload string
	if err := pgdb.QueryRowContext(ctx, "SELECT payload FROM connections WHERE id = 1").Scan(&payload); err != nil {
		t.Fatalf("read back payload: %v", err)
	}
	if payload != "a-replayed" {
		t.Errorf("connections.id=1 payload = %q; want %q (idempotent replay must refresh non-key column)", payload, "a-replayed")
	}
}

// TestMigrate_MySQLToPostgres_KeylessRefusedOnIdempotentCopy pins the
// loud refusal: a truly-keyless table (no PK, no non-null UNIQUE) reaching
// PG's idempotent copy writer is rejected with a Bug-125 error rather than
// silently plain-INSERTed (which would duplicate catch-up re-emissions).
func TestMigrate_MySQLToPostgres_KeylessRefusedOnIdempotentCopy(t *testing.T) {
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	applyPGDDL(t, pgTarget, `
		CREATE TABLE log_lines (
			ts   TIMESTAMP NOT NULL,
			msg  TEXT NULL
		);
	`)

	pgEng, _ := engines.Get("postgres")
	ctx := ctx2min(t)

	sr, err := pgEng.OpenSchemaReader(ctx, pgTarget)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer migcore.CloseIf(sr)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	tbl := findTable(schema, "log_lines")
	if tbl == nil {
		t.Fatal("log_lines table missing")
	}

	rw, err := pgEng.OpenRowWriter(ctx, pgTarget)
	if err != nil {
		t.Fatalf("OpenRowWriter: %v", err)
	}
	defer migcore.CloseIf(rw)
	iw := rw.(ir.IdempotentRowWriter)

	rows := make(chan ir.Row)
	close(rows)
	err = iw.WriteRowsIdempotent(ctx, tbl, rows)
	if err == nil {
		t.Fatal("WriteRowsIdempotent on keyless table: err=nil; want loud Bug-125 refusal")
	}
	if !strings.Contains(err.Error(), "log_lines") || !strings.Contains(err.Error(), "Bug 125") {
		t.Errorf("error %q; want it to name the table and Bug 125", err.Error())
	}
}
