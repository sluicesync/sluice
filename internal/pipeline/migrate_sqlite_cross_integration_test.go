//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Cross-engine end-to-end integration test for the SQLite / Cloudflare D1
// migrate SOURCE (ADR-0128): a temp SQLite file migrates faithfully into
// BOTH a Postgres target and a MySQL target via pipeline.Migrator. This is
// the headline prototype proof — a SQLite/D1 export round-trips into either
// target with matching row counts, an ordered value compare, and the
// affinity-resolved types landing correctly.
//
// A Cloudflare D1 export (`wrangler d1 export`) is just a SQLite file, so
// the same path serves D1 with zero D1-specific code.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite" // pure-Go driver for seeding the temp source file

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"

	// Register the engines engines.Get needs.
	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
)

// seedSQLiteSource writes a temp SQLite file exercising every affinity
// (INTEGER / TEXT / NUMERIC / REAL / BLOB) and a representative value mix
// (int, real, text, blob, NULL), plus a foreign key with ON DELETE
// CASCADE. Returns the file path.
func seedSQLiteSource(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "d1export.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite seed: %v", err)
	}
	defer func() { _ = db.Close() }()

	stmts := []string{
		`CREATE TABLE users (
			id    INTEGER PRIMARY KEY,
			name  TEXT NOT NULL,
			score NUMERIC,
			rate  REAL,
			photo BLOB
		)`,
		`INSERT INTO users (id, name, score, rate, photo) VALUES
			(1, 'alice', 100, 3.5, x'cafe'),
			(2, 'bob',   NULL, NULL, NULL)`,
		`CREATE TABLE posts (
			id      INTEGER PRIMARY KEY,
			user_id INTEGER NOT NULL,
			body    TEXT,
			FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
		)`,
		`INSERT INTO posts (id, user_id, body) VALUES
			(1, 1, 'hello'),
			(2, 1, 'world'),
			(3, 2, 'hi')`,
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(context.Background(), s); err != nil {
			t.Fatalf("seed exec: %v", err)
		}
	}
	return path
}

// TestMigrate_SQLiteToPostgres is the SQLite→Postgres half of the proof.
func TestMigrate_SQLiteToPostgres(t *testing.T) {
	src := seedSQLiteSource(t)
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	sqliteEng, ok := engines.Get("sqlite")
	if !ok {
		t.Fatal("sqlite engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	mig := &Migrator{
		Source:    sqliteEng,
		Target:    pgEng,
		SourceDSN: src,
		TargetDSN: pgTarget,
	}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Migrator.Run (SQLite→PG): %v", err)
	}

	assertSQLiteRoundTrip(t, pgEng, pgTarget)
}

// TestMigrate_SQLiteToMySQL is the SQLite→MySQL half of the proof.
func TestMigrate_SQLiteToMySQL(t *testing.T) {
	src := seedSQLiteSource(t)
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	sqliteEng, ok := engines.Get("sqlite")
	if !ok {
		t.Fatal("sqlite engine not registered")
	}
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	mig := &Migrator{
		Source:    sqliteEng,
		Target:    mysqlEng,
		SourceDSN: src,
		TargetDSN: mysqlTarget,
	}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Migrator.Run (SQLite→MySQL): %v", err)
	}

	assertSQLiteRoundTrip(t, mysqlEng, mysqlTarget)
}

// sqliteDumpSource is a `.sql` TEXT dump (ADR-0130) shaped like a real
// `wrangler d1 export`: a leading PRAGMA, D1's internal `_cf_KV` table (which
// the engine must auto-skip), and the SAME users/posts/values as
// seedSQLiteSource so assertSQLiteRoundTrip applies unchanged. The engine
// sniffs the missing SQLite magic header and materializes this in-process.
const sqliteDumpSource = `PRAGMA defer_foreign_keys=TRUE;
CREATE TABLE _cf_KV (key TEXT PRIMARY KEY, value BLOB);
INSERT INTO _cf_KV (key, value) VALUES ('d1-internal', x'00');
CREATE TABLE users (
	id    INTEGER PRIMARY KEY,
	name  TEXT NOT NULL,
	score NUMERIC,
	rate  REAL,
	photo BLOB
);
INSERT INTO users (id, name, score, rate, photo) VALUES (1, 'alice', 100, 3.5, x'cafe');
INSERT INTO users (id, name, score, rate, photo) VALUES (2, 'bob', NULL, NULL, NULL);
CREATE TABLE posts (
	id      INTEGER PRIMARY KEY,
	user_id INTEGER NOT NULL,
	body    TEXT,
	FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);
INSERT INTO posts (id, user_id, body) VALUES (1, 1, 'hello');
INSERT INTO posts (id, user_id, body) VALUES (2, 1, 'world');
INSERT INTO posts (id, user_id, body) VALUES (3, 2, 'hi');
`

// seedSQLiteDumpSource writes sqliteDumpSource to a temp `.sql` file and
// returns its path — the direct-dump-ingest analogue of seedSQLiteSource.
func seedSQLiteDumpSource(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "d1export.sql")
	if err := os.WriteFile(path, []byte(sqliteDumpSource), 0o600); err != nil {
		t.Fatalf("write sqlite dump: %v", err)
	}
	return path
}

// TestMigrate_SQLiteDumpToPostgres is the direct `.sql`-dump-ingest proof
// (ADR-0130): a D1-shaped `.sql` dump migrates into Postgres with the SAME row
// counts/values as the binary `.db` path, and D1's internal `_cf_KV` table does
// NOT land on the target (auto-skip).
func TestMigrate_SQLiteDumpToPostgres(t *testing.T) {
	runSQLiteDumpRoundTrip(t, "postgres", startPostgres)
}

// TestMigrate_SQLiteDumpToMySQL is the MySQL half of the dump-ingest proof.
func TestMigrate_SQLiteDumpToMySQL(t *testing.T) {
	runSQLiteDumpRoundTrip(t, "mysql", startMySQL)
}

// runSQLiteDumpRoundTrip migrates the `.sql` dump source into the named target,
// reuses assertSQLiteRoundTrip (which already asserts EXACTLY 2 target tables,
// so a leaked _cf_KV would fail it), and adds an explicit no-_cf_* assertion.
func runSQLiteDumpRoundTrip(t *testing.T, targetName string, start func(*testing.T) (string, string, func())) {
	src := seedSQLiteDumpSource(t)
	_, target, cleanup := start(t)
	defer cleanup()

	sqliteEng, ok := engines.Get("sqlite")
	if !ok {
		t.Fatal("sqlite engine not registered")
	}
	targetEng, ok := engines.Get(targetName)
	if !ok {
		t.Fatalf("%s engine not registered", targetName)
	}

	mig := &Migrator{
		Source:    sqliteEng,
		Target:    targetEng,
		SourceDSN: src,
		TargetDSN: target,
	}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Migrator.Run (SQLite dump→%s): %v", targetName, err)
	}

	assertSQLiteRoundTrip(t, targetEng, target)

	// Explicit: no `_cf_*` table reached the target.
	ctx := ctx2min(t)
	sr, err := targetEng.OpenSchemaReader(ctx, target)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(sr)
	got, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	for _, tbl := range got.Tables {
		if strings.HasPrefix(strings.ToLower(tbl.Name), "_cf_") {
			t.Errorf("D1 internal table %q leaked to the target; have %v", tbl.Name, targetTableNames(got))
		}
	}
}

// assertSQLiteRoundTrip reads the migrated schema + rows back through the
// TARGET engine and asserts the affinity-resolved types landed and every
// value round-tripped exactly-once. Engine-neutral: it reads via the
// target's own RowReader, whose canonical Go value shapes are identical
// across PG and MySQL for these types (decimal→string, float→float64,
// blob→[]byte, text→string, integer→int64).
func assertSQLiteRoundTrip(t *testing.T, eng ir.Engine, dsn string) {
	t.Helper()
	ctx := ctx2min(t)

	sr, err := eng.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(sr)
	got, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	if len(got.Tables) != 2 {
		t.Fatalf("target tables = %d; want 2 (%v)", len(got.Tables), targetTableNames(got))
	}
	users := findTable(got, "users")
	posts := findTable(got, "posts")
	if users == nil || posts == nil {
		t.Fatalf("missing target tables; have %v", targetTableNames(got))
	}

	// Type landing: INTEGER affinity → integer; TEXT → text; NUMERIC →
	// decimal; REAL → float; BLOB → bytes. We assert the IR kind on the
	// target's read-back (each engine reports its own canonical IR type).
	assertColKind(t, users, "id", "integer")
	assertColKind(t, users, "name", "text")
	assertColKind(t, users, "score", "decimal")
	assertColKind(t, users, "rate", "float")
	assertColKind(t, users, "photo", "bytes")

	// Row-value compare (ordered by PK). usersRows[0]=alice, [1]=bob.
	rr, err := eng.OpenRowReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer closeIf(rr)

	usersRows := readAll(t, ctx, rr, users)
	if len(usersRows) != 2 {
		t.Fatalf("users rows = %d; want 2", len(usersRows))
	}
	if name, _ := usersRows[0]["name"].(string); name != "alice" {
		t.Errorf("users[0].name = %#v; want alice", usersRows[0]["name"])
	}
	if got := asFloat(t, usersRows[0]["score"]); got != 100 {
		t.Errorf("users[0].score = %#v; want 100", usersRows[0]["score"])
	}
	if got := asFloat(t, usersRows[0]["rate"]); got != 3.5 {
		t.Errorf("users[0].rate = %#v; want 3.5", usersRows[0]["rate"])
	}
	if photo, ok := usersRows[0]["photo"].([]byte); !ok || len(photo) != 2 || photo[0] != 0xca || photo[1] != 0xfe {
		t.Errorf("users[0].photo = %#v; want bytes {0xca,0xfe}", usersRows[0]["photo"])
	}
	// bob's NULLs must survive as NULL, not a coerced zero value.
	if usersRows[1]["score"] != nil {
		t.Errorf("users[1].score = %#v; want nil", usersRows[1]["score"])
	}
	if usersRows[1]["rate"] != nil {
		t.Errorf("users[1].rate = %#v; want nil", usersRows[1]["rate"])
	}
	if usersRows[1]["photo"] != nil {
		t.Errorf("users[1].photo = %#v; want nil", usersRows[1]["photo"])
	}

	postsRows := readAll(t, ctx, rr, posts)
	if len(postsRows) != 3 {
		t.Errorf("posts rows = %d; want 3", len(postsRows))
	}

	// FK landed: posts.user_id → users.id ON DELETE CASCADE.
	if len(posts.ForeignKeys) != 1 {
		t.Fatalf("posts FKs = %d; want 1", len(posts.ForeignKeys))
	}
	if fk := posts.ForeignKeys[0]; fk.ReferencedTable != "users" || fk.OnDelete != ir.FKActionCascade {
		t.Errorf("posts FK = %+v; want users ON DELETE CASCADE", fk)
	}
}

// assertColKind checks a target column's IR type family by name.
func assertColKind(t *testing.T, tbl *ir.Table, col, kind string) {
	t.Helper()
	c := findColumn(tbl, col)
	if c == nil {
		t.Fatalf("%s.%s missing", tbl.Name, col)
	}
	var ok bool
	switch kind {
	case "integer":
		_, ok = c.Type.(ir.Integer)
	case "text":
		_, ok = c.Type.(ir.Text)
	case "decimal":
		_, ok = c.Type.(ir.Decimal)
	case "float":
		_, ok = c.Type.(ir.Float)
	case "bytes":
		switch c.Type.(type) {
		case ir.Blob, ir.Varbinary, ir.Binary:
			ok = true
		}
	}
	if !ok {
		t.Errorf("%s.%s type = %#v; want IR %s", tbl.Name, col, c.Type, kind)
	}
}

// asFloat coerces a decimal-as-string or float64 row value to float64 for
// numeric comparison, so the assertion is robust to engine-specific
// decimal formatting (PG "100", MySQL "100" / "100.00000", etc.).
func asFloat(t *testing.T, v any) float64 {
	t.Helper()
	switch x := v.(type) {
	case float64:
		return x
	case string:
		f, err := strconv.ParseFloat(x, 64)
		if err != nil {
			t.Fatalf("parse %q as float: %v", x, err)
		}
		return f
	case int64:
		return float64(x)
	default:
		t.Fatalf("value %#v (%T) not numeric", v, v)
		return 0
	}
}

// seedSQLiteTemporal writes a temp SQLite file with DECLARED DATE, DATETIME,
// and BOOLEAN columns (ADR-0129). With iso=true the values are ISO TEXT and
// 0/1; with iso=false the happened_at DATETIME is stored as a unix-epoch
// INTEGER (the unixepoch encoding path). All rows describe the same instants
// so the same assertions verify both encodings.
func seedSQLiteTemporal(t *testing.T, iso bool) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "events.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite seed: %v", err)
	}
	defer func() { _ = db.Close() }()

	var stmts []string
	if iso {
		stmts = []string{
			`CREATE TABLE events (
				id          INTEGER PRIMARY KEY,
				happened_on   DATE     NOT NULL,
				happened_at   DATETIME NOT NULL,
				happened_time TIME     NOT NULL,
				is_active   BOOLEAN  NOT NULL
			)`,
			`INSERT INTO events (id, happened_on, happened_at, happened_time, is_active) VALUES
				(1, '2024-01-02', '2024-01-02 03:04:05', '03:04:05', 1),
				(2, '2025-12-31', '2025-12-31 23:59:59', '23:59:59', 0)`,
		}
	} else {
		// The date encoding is GLOBAL per source, so under unixepoch BOTH
		// temporal columns are unix-epoch INTEGERs (a mixed-encoding column
		// would loud-refuse — the policy working). happened_on is midnight of
		// the date: 1704153600 = 2024-01-02 00:00:00 UTC, 1767139200 =
		// 2025-12-31 00:00:00 UTC. happened_at: 1704164645 = 2024-01-02
		// 03:04:05 UTC, 1767225599 = 2025-12-31 23:59:59 UTC.
		stmts = []string{
			`CREATE TABLE events (
				id          INTEGER PRIMARY KEY,
				happened_on DATE     NOT NULL,
				happened_at DATETIME NOT NULL,
				is_active   BOOLEAN  NOT NULL
			)`,
			`INSERT INTO events (id, happened_on, happened_at, is_active) VALUES
				(1, 1704153600, 1704164645, 1),
				(2, 1767139200, 1767225599, 0)`,
		}
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(context.Background(), s); err != nil {
			t.Fatalf("seed exec: %v", err)
		}
	}
	return path
}

// TestMigrate_SQLiteTemporalToPostgres pins the ADR-0129 declared-temporal/
// bool round-trip into Postgres: the DATE/DATETIME/BOOLEAN source columns
// land as PG date/timestamp/boolean (NOT numeric) with correct values.
func TestMigrate_SQLiteTemporalToPostgres(t *testing.T) {
	runSQLiteTemporalRoundTrip(t, "postgres", startPostgres, true)
}

// TestMigrate_SQLiteTemporalToMySQL is the MySQL half of the proof.
func TestMigrate_SQLiteTemporalToMySQL(t *testing.T) {
	runSQLiteTemporalRoundTrip(t, "mysql", startMySQL, true)
}

// TestMigrate_SQLiteTemporalUnixEpochToPostgres pins the
// --sqlite-date-encoding=unixepoch variant: an INTEGER unix-epoch DATETIME
// source column migrates to a PG timestamp with the correct instant.
func TestMigrate_SQLiteTemporalUnixEpochToPostgres(t *testing.T) {
	runSQLiteTemporalRoundTrip(t, "postgres", startPostgres, false)
}

// TestMigrate_SQLiteTemporalUnixEpochToMySQL is the MySQL unixepoch half.
func TestMigrate_SQLiteTemporalUnixEpochToMySQL(t *testing.T) {
	runSQLiteTemporalRoundTrip(t, "mysql", startMySQL, false)
}

// runSQLiteTemporalRoundTrip migrates a temporal/bool SQLite source into the
// named target and asserts the target column types and values. iso selects
// the ISO-TEXT encoding (default); !iso selects unixepoch (an INTEGER
// happened_at) by appending the sqlite_date_encoding=unixepoch DSN param.
func runSQLiteTemporalRoundTrip(t *testing.T, targetName string, start func(*testing.T) (string, string, func()), iso bool) {
	src := seedSQLiteTemporal(t, iso)
	if !iso {
		src += "?sqlite_date_encoding=unixepoch"
	}
	_, target, cleanup := start(t)
	defer cleanup()

	sqliteEng, ok := engines.Get("sqlite")
	if !ok {
		t.Fatal("sqlite engine not registered")
	}
	targetEng, ok := engines.Get(targetName)
	if !ok {
		t.Fatalf("%s engine not registered", targetName)
	}

	mig := &Migrator{
		Source:    sqliteEng,
		Target:    targetEng,
		SourceDSN: src,
		TargetDSN: target,
	}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Migrator.Run (SQLite→%s, iso=%v): %v", targetName, iso, err)
	}

	ctx := ctx2min(t)
	sr, err := targetEng.OpenSchemaReader(ctx, target)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(sr)
	got, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	events := findTable(got, "events")
	if events == nil {
		t.Fatalf("missing target table events; have %v", targetTableNames(got))
	}

	// Type landing: DATE → date, DATETIME → a temporal (timestamp/datetime),
	// BOOLEAN → boolean — NOT the prototype's numeric/decimal.
	assertTemporalKind(t, events, "happened_on", "date")
	assertTemporalKind(t, events, "happened_at", "timestampish")
	assertTemporalKind(t, events, "is_active", "boolean")
	if iso {
		// ir.Time is the third temporal family with a distinct target-write
		// path (Bug-74 corollary) — pin it lands as a target time column too.
		// (Only the ISO seed carries a TIME column; a TIME-of-day under a
		// unix-epoch encoding is semantically muddy and out of scope.)
		assertTemporalKind(t, events, "happened_time", "timeish")
	}

	rr, err := targetEng.OpenRowReader(ctx, target)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer closeIf(rr)
	rows := readAll(t, ctx, rr, events)
	if len(rows) != 2 {
		t.Fatalf("events rows = %d; want 2", len(rows))
	}

	// Row 1: 2024-01-02 / 2024-01-02 03:04:05 / true.
	if d := asTime(t, rows[0]["happened_on"]).Format("2006-01-02"); d != "2024-01-02" {
		t.Errorf("events[0].happened_on = %q; want 2024-01-02", d)
	}
	if ts := asTime(t, rows[0]["happened_at"]).UTC().Format("2006-01-02 15:04:05"); ts != "2024-01-02 03:04:05" {
		t.Errorf("events[0].happened_at = %q; want 2024-01-02 03:04:05", ts)
	}
	if b := asBool(t, rows[0]["is_active"]); !b {
		t.Errorf("events[0].is_active = %v; want true", b)
	}
	if iso {
		// happened_time landed as a target time column; its value (string or
		// time.Time depending on the target reader) must carry 03:04:05.
		if s := fmt.Sprint(rows[0]["happened_time"]); !strings.Contains(s, "03:04:05") {
			t.Errorf("events[0].happened_time = %q; want it to contain 03:04:05", s)
		}
	}
	// Row 2: 2025-12-31 / 2025-12-31 23:59:59 / false.
	if ts := asTime(t, rows[1]["happened_at"]).UTC().Format("2006-01-02 15:04:05"); ts != "2025-12-31 23:59:59" {
		t.Errorf("events[1].happened_at = %q; want 2025-12-31 23:59:59", ts)
	}
	if b := asBool(t, rows[1]["is_active"]); b {
		t.Errorf("events[1].is_active = %v; want false", b)
	}
}

// assertTemporalKind checks a target column's IR type family for the
// temporal/bool kinds (engine-neutral: PG and MySQL each report their own
// canonical temporal IR types).
func assertTemporalKind(t *testing.T, tbl *ir.Table, col, kind string) {
	t.Helper()
	c := findColumn(tbl, col)
	if c == nil {
		t.Fatalf("%s.%s missing", tbl.Name, col)
	}
	var ok bool
	switch kind {
	case "date":
		_, ok = c.Type.(ir.Date)
	case "timestampish":
		switch c.Type.(type) {
		case ir.Timestamp, ir.DateTime:
			ok = true
		}
	case "timeish":
		_, ok = c.Type.(ir.Time)
	case "boolean":
		_, ok = c.Type.(ir.Boolean)
	}
	if !ok {
		t.Errorf("%s.%s type = %#v; want IR %s (NOT numeric)", tbl.Name, col, c.Type, kind)
	}
}

// asTime coerces a temporal row value to time.Time.
func asTime(t *testing.T, v any) time.Time {
	t.Helper()
	tm, ok := v.(time.Time)
	if !ok {
		t.Fatalf("value %#v (%T) is not time.Time", v, v)
	}
	return tm
}

// asBool coerces a boolean row value to bool, tolerating the int64/[]byte
// shapes a target driver might surface for a TINYINT(1)-backed boolean.
func asBool(t *testing.T, v any) bool {
	t.Helper()
	switch x := v.(type) {
	case bool:
		return x
	case int64:
		return x != 0
	default:
		t.Fatalf("value %#v (%T) is not boolean", v, v)
		return false
	}
}

// seedSQLiteFeatures writes a temp SQLite file exercising the ADR-0133 carry: a
// STORED generated column with a portable expression (qty*price), a portable
// named table CHECK, and a partial index (WHERE active=1). Returns the path.
func seedSQLiteFeatures(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "features.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite seed: %v", err)
	}
	defer func() { _ = db.Close() }()
	stmts := []string{
		`CREATE TABLE line_items (
			id     INTEGER PRIMARY KEY,
			qty    INTEGER NOT NULL,
			price  INTEGER NOT NULL,
			total  INTEGER GENERATED ALWAYS AS (qty * price) STORED,
			active INTEGER NOT NULL DEFAULT 1,
			CONSTRAINT qty_nonneg CHECK (qty >= 0)
		)`,
		`INSERT INTO line_items (id, qty, price, active) VALUES (1, 2, 10, 1), (2, 3, 5, 0)`,
		`CREATE INDEX line_items_active_idx ON line_items(price) WHERE active = 1`,
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(context.Background(), s); err != nil {
			t.Fatalf("seed exec: %v", err)
		}
	}
	return path
}

// TestMigrate_SQLiteFeaturesToPostgres pins the ADR-0133 cross-engine carry into
// Postgres: the STORED generated column lands as a REAL generated column (the
// target re-derives it), its values are exact, the CHECK is ENFORCED on the
// target (a violating insert is rejected), and the partial index keeps its
// predicate. All four prove the feature carried, not silently dropped.
func TestMigrate_SQLiteFeaturesToPostgres(t *testing.T) {
	src := seedSQLiteFeatures(t)
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	sqliteEng, _ := engines.Get("sqlite")
	pgEng, _ := engines.Get("postgres")
	mig := &Migrator{Source: sqliteEng, Target: pgEng, SourceDSN: src, TargetDSN: pgTarget}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Migrator.Run (SQLite features→PG): %v", err)
	}

	db, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("open pg: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := ctx2min(t)

	// 1. total is a REAL generated column (re-derived), not a plain column.
	var isGen string
	if err := db.QueryRowContext(ctx,
		`SELECT is_generated FROM information_schema.columns
		 WHERE table_name = 'line_items' AND column_name = 'total'`).Scan(&isGen); err != nil {
		t.Fatalf("query is_generated: %v", err)
	}
	if isGen != "ALWAYS" {
		t.Errorf("line_items.total is_generated = %q; want ALWAYS (it must land as a GENERATED column)", isGen)
	}

	// 2. Generated values exact + row count exact.
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM line_items`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 2 {
		t.Errorf("row count = %d; want 2", n)
	}
	var t1, t2 int
	if err := db.QueryRowContext(ctx, `SELECT total FROM line_items WHERE id = 1`).Scan(&t1); err != nil {
		t.Fatalf("select total id=1: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT total FROM line_items WHERE id = 2`).Scan(&t2); err != nil {
		t.Fatalf("select total id=2: %v", err)
	}
	if t1 != 20 || t2 != 15 {
		t.Errorf("totals = (%d, %d); want (20, 15) (re-derived qty*price)", t1, t2)
	}

	// 3. CHECK enforced: a qty < 0 row is rejected on the target.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO line_items (id, qty, price, active) VALUES (99, -1, 1, 1)`); err == nil {
		t.Error("INSERT violating CHECK (qty >= 0) succeeded; want a rejection (CHECK must have carried)")
	}

	// 4. Partial index present WITH its predicate.
	var indexdef string
	if err := db.QueryRowContext(ctx,
		`SELECT indexdef FROM pg_indexes WHERE tablename = 'line_items' AND indexdef ILIKE '%WHERE%'`).Scan(&indexdef); err != nil {
		t.Fatalf("query partial index def (none found?): %v", err)
	}
	if !strings.Contains(strings.ToUpper(indexdef), "WHERE") {
		t.Errorf("partial index def = %q; want a WHERE predicate", indexdef)
	}
}

// TestMigrate_SQLiteFeaturesToMySQL is the MySQL half (generated + CHECK): the
// STORED generated column re-derives on MySQL and the CHECK is enforced.
func TestMigrate_SQLiteFeaturesToMySQL(t *testing.T) {
	src := seedSQLiteFeatures(t)
	_, myTarget, myCleanup := startMySQL(t)
	defer myCleanup()

	sqliteEng, _ := engines.Get("sqlite")
	myEng, _ := engines.Get("mysql")
	mig := &Migrator{Source: sqliteEng, Target: myEng, SourceDSN: src, TargetDSN: myTarget}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Migrator.Run (SQLite features→MySQL): %v", err)
	}

	db, err := sql.Open("mysql", myTarget)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := ctx2min(t)

	// total re-derives on MySQL.
	var t1 int
	if err := db.QueryRowContext(ctx, `SELECT total FROM line_items WHERE id = 1`).Scan(&t1); err != nil {
		t.Fatalf("select total: %v", err)
	}
	if t1 != 20 {
		t.Errorf("total id=1 = %d; want 20 (re-derived qty*price)", t1)
	}
	// is generated per information_schema.
	var extra string
	if err := db.QueryRowContext(ctx,
		`SELECT extra FROM information_schema.columns
		 WHERE table_name = 'line_items' AND column_name = 'total'`).Scan(&extra); err != nil {
		t.Fatalf("query extra: %v", err)
	}
	if !strings.Contains(strings.ToUpper(extra), "GENERATED") {
		t.Errorf("line_items.total extra = %q; want it to mark a GENERATED column", extra)
	}
	// CHECK enforced (MySQL 8.0.16+).
	if _, err := db.ExecContext(ctx,
		`INSERT INTO line_items (id, qty, price, active) VALUES (99, -1, 1, 1)`); err == nil {
		t.Error("INSERT violating CHECK (qty >= 0) succeeded on MySQL; want a rejection")
	}
}

// TestMigrate_SQLiteNonPortableGeneratedToPostgres pins the loud-rejection edge:
// a generated column using a SQLite-only function (strftime) is carried VERBATIM
// and the target CREATE fails LOUDLY (naming the rejected function) — NOT a
// silent drop or silent mistranslation.
func TestMigrate_SQLiteNonPortableGeneratedToPostgres(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nonportable.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite seed: %v", err)
	}
	stmts := []string{
		`CREATE TABLE evt (
			id  INTEGER PRIMARY KEY,
			ts  TEXT NOT NULL,
			day TEXT GENERATED ALWAYS AS (strftime('%Y-%m-%d', ts)) STORED
		)`,
		`INSERT INTO evt (id, ts) VALUES (1, '2024-01-02 03:04:05')`,
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(context.Background(), s); err != nil {
			t.Fatalf("seed exec: %v", err)
		}
	}
	_ = db.Close()

	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()
	sqliteEng, _ := engines.Get("sqlite")
	pgEng, _ := engines.Get("postgres")
	mig := &Migrator{Source: sqliteEng, Target: pgEng, SourceDSN: path, TargetDSN: pgTarget}

	err = mig.Run(ctx2min(t))
	if err == nil {
		t.Fatal("Migrator.Run succeeded; want a LOUD target rejection of the non-portable strftime() generated column")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "strftime") {
		t.Errorf("error = %v; want it to name the rejected strftime() (carried verbatim, target-rejected)", err)
	}
}
