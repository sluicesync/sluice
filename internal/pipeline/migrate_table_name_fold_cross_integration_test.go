//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Two source TABLES that fold to one MySQL table name must be refused BEFORE
// any data moves — and the SAME pair must still migrate when the target server
// does not fold (roadmap item 149). End to end, on a real Postgres source and
// two real MySQL targets that differ only in `lower_case_table_names`.
//
// # Why this needs TWO servers, which is the whole shape of the item
//
// Item 148's sibling on SQLite could be pinned against one target, because
// SQLite's fold is a property of the engine. MySQL's is a property of the
// SERVER: the identical schema merges on a server initialized with
// lower_case_table_names=1 and is two ordinary tables on one initialized with
// 0 (the stock Linux default). A gate that only proved the refusal would be
// satisfied by a check that refuses always — which would break every
// case-sensitive MySQL migration that works today. So the refusal and the
// non-refusal are pinned as one matrix, against servers with the variable
// actually set, never simulated.
//
// # The oracle
//
// The target's own information_schema and row counts, read here with
// database/sql, never anything sluice reports — because the defect's entire
// character is that sluice reports success. The merged run exits 0 with a row
// count that is correct for the surviving name, and `verify` compares that name
// against that name and agrees — FALSE; see object_namespace.go. verify would
// catch this; the MIGRATION is what reports success.
//
// # The pre-fix behaviour these tests pin against, measured
//
// With both doors removed (mutations M1/M2 in item 149's roadmap entry), the
// PG → MySQL(lct=1) case leaves ONE table `orders` holding FOUR rows — two from
// `public.orders` and two from `public."Orders"` — at exit 0.

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"

	"github.com/testcontainers/testcontainers-go"
	mysqltc "github.com/testcontainers/testcontainers-go/modules/mysql"
)

// startMySQLFolding boots a MySQL container INITIALIZED with
// --lower-case-table-names=1 — the setting that makes the server compare table
// names case-insensitively, and the premise of everything in this file.
//
// It is a sibling of startMySQLCaseSensitive (which pins 0) rather than a
// parameter on it, for the same reason that helper gave: the flag is
// load-bearing for the test's claim, so each value gets a helper whose name
// states which claim it serves. MySQL fixes the setting at datadir
// initialization, so it MUST arrive as a start argument on the container's
// first boot — it cannot be SET later.
func startMySQLFolding(t *testing.T) (targetDSN string, cleanup func()) {
	t.Helper()

	// UPSTREAM image, not the pre-baked one: `lower_case_table_names` is fixed
	// when the data directory is initialized, and the pre-baked image ships one
	// already initialized at 0 — booting it at 1 aborts with MY-011087. See
	// runMySQLImageWithRetry. This helper therefore pays a cold init.
	container := runMySQLImageWithRetry(
		t,
		"mysql:8.0",
		mysqltc.WithDatabase("source_db"),
		mysqltc.WithUsername("root"),
		mysqltc.WithPassword("rootpw"),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Cmd: []string{"mysqld", "--lower-case-table-names=1"},
			},
		}),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

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

	db, err := sql.Open("mysql", conn+"&multiStatements=true")
	if err != nil {
		terminate()
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.ExecContext(ctx, "CREATE DATABASE target_db"); err != nil {
		terminate()
		t.Fatalf("create target_db: %v", err)
	}

	tgtConn, err := buildMySQLDSN(conn, "target_db")
	if err != nil {
		terminate()
		t.Fatalf("build target DSN: %v", err)
	}
	return tgtConn, terminate
}

// mysqlUserTables returns every table in the DSN's own database with its row
// count, read from the server's catalog. The independent oracle: nothing here
// derives from anything sluice reports.
//
// Like its SQLite twin it returns the WHOLE map rather than one name's count,
// because under a folding server the surviving spelling is not necessarily the
// one you would probe for — the server stores what the FIRST create wrote, and
// a probe keyed on the other spelling reads "absent" for a table holding both
// tables' rows.
//
// sluice's own control tables (`sluice_migrate_state`,
// `sluice_migrate_table_progress`) are excluded, the same way the SQLite twin
// excludes `sqlite_%`: a successful migrate creates them, and they are
// bookkeeping rather than migrated data.
func mysqlUserTables(t *testing.T, dsn string) map[string]int {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mysql target: %v", err)
	}
	defer func() { _ = db.Close() }()

	rows, err := db.Query(
		`SELECT table_name FROM information_schema.tables
		  WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'
		    AND table_name NOT LIKE 'sluice\_%' ORDER BY table_name`,
	)
	if err != nil {
		t.Fatalf("list mysql target tables: %v", err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan table name: %v", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate table names: %v", err)
	}
	_ = rows.Close()

	out := make(map[string]int, len(names))
	for _, name := range names {
		var n int
		if err := db.QueryRow("SELECT COUNT(*) FROM `" + name + "`").Scan(&n); err != nil {
			t.Fatalf("count %q on the mysql target: %v", name, err)
		}
		out[name] = n
	}
	return out
}

// TestMySQLTableNameFoldGroundTruth is the PREMISE, asserted against real
// servers rather than cited from the manual.
//
// Everything in engines/mysql/table_name_fold.go rests on three facts about the
// world outside sluice, and CLAUDE.md's premise-naming rule says a safety
// argument that cites an environmental fact owes that fact a check. These are
// those checks:
//
//  1. under lct=1 the colliding `CREATE TABLE IF NOT EXISTS` is a WARNING, not
//     an error, and the rows written under both spellings land in one table;
//  2. under lct=0 the same pair is two tables, so a refusal there would be
//     over-refusal;
//  3. under lct=1 the server folds NON-ASCII case too, which is the one cell
//     that decides whether sluice's Go-side [strings.ToLower] imitation is
//     over- or under-approximating the server on such a pair.
//
// Fact 3 is here because the first draft of that file's doc asserted the
// opposite from plausibility.
func TestMySQLTableNameFoldGroundTruth(t *testing.T) {
	t.Run("lct=1 folds, silently, and merges the rows", func(t *testing.T) {
		dsn, cleanup := startMySQLFolding(t)
		defer cleanup()

		db, err := sql.Open("mysql", dsn+"&multiStatements=true")
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer func() { _ = db.Close() }()

		var lct int
		if err := db.QueryRow("SELECT @@global.lower_case_table_names").Scan(&lct); err != nil {
			t.Fatalf("read lower_case_table_names: %v", err)
		}
		if lct != 1 {
			t.Fatalf("container reports lower_case_table_names=%d; the whole file assumes 1. MySQL fixes "+
				"this at datadir initialization, so a container that ignored the flag makes every "+
				"assertion below vacuous.", lct)
		}

		mustExec(t, db, "CREATE TABLE `orders` (id BIGINT PRIMARY KEY, tag VARCHAR(32))")
		// THE DEFECT'S MECHANISM: this returns nil, not an error.
		if _, err := db.Exec("CREATE TABLE IF NOT EXISTS `Orders` (id BIGINT PRIMARY KEY, tag VARCHAR(32))"); err != nil {
			t.Fatalf("the colliding CREATE returned an ERROR (%v). If MySQL has started refusing this, "+
				"the silent-merge premise is gone and item 149's refusal may be over-refusal — do not "+
				"just relax this test.", err)
		}
		mustExec(t, db, "INSERT INTO `orders` VALUES (1, 'lower')")
		mustExec(t, db, "INSERT INTO `Orders` VALUES (101, 'upper')")

		if got := mysqlUserTables(t, dsn); len(got) != 1 || got["orders"] != 2 {
			t.Errorf("target holds %v; want exactly one table `orders` with BOTH rows — that merge, at "+
				"exit 0, is the defect item 149 refuses", got)
		}

		// Fact 3: non-ASCII case folds too.
		mustExec(t, db, "CREATE TABLE `é` (id BIGINT PRIMARY KEY)")
		if _, err := db.Exec("CREATE TABLE IF NOT EXISTS `É` (id BIGINT PRIMARY KEY)"); err != nil {
			t.Fatalf("non-ASCII colliding CREATE errored: %v", err)
		}
		got := mysqlUserTables(t, dsn)
		if _, both := got["É"]; both {
			t.Errorf("target holds %v — the server did NOT fold the non-ASCII pair. sluice's Go-side "+
				"strings.ToLower does, so the refusal on that pair is now OVER-refusal; the residual "+
				"documented in engines/mysql/table_name_fold.go has flipped direction.", got)
		}
	})

	t.Run("lct=0 does not fold, so there is nothing to refuse", func(t *testing.T) {
		_, dsn, cleanup := startMySQLCaseSensitive(t)
		defer cleanup()

		db, err := sql.Open("mysql", dsn+"&multiStatements=true")
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer func() { _ = db.Close() }()

		var lct int
		if err := db.QueryRow("SELECT @@global.lower_case_table_names").Scan(&lct); err != nil {
			t.Fatalf("read lower_case_table_names: %v", err)
		}
		if lct != 0 {
			t.Fatalf("container reports lower_case_table_names=%d; want 0", lct)
		}

		mustExec(t, db, "CREATE TABLE `orders` (id BIGINT PRIMARY KEY)")
		mustExec(t, db, "CREATE TABLE IF NOT EXISTS `Orders` (id BIGINT PRIMARY KEY)")
		got := mysqlUserTables(t, dsn)
		if len(got) != 2 {
			t.Errorf("target holds %v; want BOTH `orders` and `Orders`. If a stock Linux MySQL has "+
				"started folding, item 149's refusal is no longer scoped by the server setting.", got)
		}
	})
}

// mustExec runs one statement and fails the test on error, so the premise
// assertions above read as the sequence they are.
func mustExec(t *testing.T, db *sql.DB, stmt string) {
	t.Helper()
	if _, err := db.Exec(stmt); err != nil {
		t.Fatalf("exec %q: %v", stmt, err)
	}
}

// dropMySQLTables clears the named tables so a subtest that shares a container
// with the migrate cases below leaves the target as it found it.
func dropMySQLTables(t *testing.T, dsn string, names ...string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mysql target: %v", err)
	}
	defer func() { _ = db.Close() }()
	for _, name := range names {
		if _, err := db.Exec("DROP TABLE IF EXISTS `" + name + "`"); err != nil {
			t.Fatalf("drop %q: %v", name, err)
		}
	}
}

// pgCaseFoldedTablePair installs the source shape both migrate tests below use:
// two ordinary PostgreSQL relations differing only in case.
//
// THE KEY RANGES ARE DISJOINT ON PURPOSE, for the reason item 148 measured on
// the SQLite sibling: overlapping primary keys make the merged INSERT fail on a
// UNIQUE violation, which is luck rather than a guarantee (a source with
// disjoint keys, or none, gets no such warning) and would pin an unrelated
// failure. With these ids the pre-fix run exits 0.
func pgCaseFoldedTablePair(t *testing.T, dsn string) {
	t.Helper()
	applyPGDDL(t, dsn, `
		DROP TABLE IF EXISTS public."Orders";
		DROP TABLE IF EXISTS public.orders;
		DROP TABLE IF EXISTS public.order_items;
		CREATE TABLE public.orders   (id BIGINT PRIMARY KEY, tag TEXT NOT NULL);
		CREATE TABLE public."Orders" (id BIGINT PRIMARY KEY, tag TEXT NOT NULL);
		INSERT INTO public.orders   (id, tag) VALUES (1, 'lower-1'), (2, 'lower-2');
		INSERT INTO public."Orders" (id, tag) VALUES (101, 'upper-1'), (102, 'upper-2');
	`)
}

// TestMigrate_TableNameFold_PGToMySQL_FoldingServer is the REFUSAL half.
func TestMigrate_TableNameFold_PGToMySQL_FoldingServer(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()

	mysqlTarget, mysqlCleanup := startMySQLFolding(t)
	defer mysqlCleanup()

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	t.Run("the folding pair is refused before anything is created", func(t *testing.T) {
		pgCaseFoldedTablePair(t, pgSource)

		mig := &Migrator{
			Source: pgEng, Target: mysqlEng,
			SourceDSN: pgSource, TargetDSN: mysqlTarget,
			MigrationID: "item149-pg-to-mysql-fold-refused",
		}
		err := mig.Run(ctx2min(t))
		if err == nil {
			t.Fatalf("migrate SUCCEEDED into a server that folds table names. The target now holds %v, "+
				"where the source holds TWO tables of two rows each — at exit 0, with a count that "+
				"verifies clean against the surviving name.", mysqlUserTables(t, mysqlTarget))
		}
		if ce, coded := sluicecode.FromError(err); !coded || ce.Code != sluicecode.CodeSchemaTableNameCollision {
			t.Errorf("refusal must carry %s; got %v", sluicecode.CodeSchemaTableNameCollision, err)
		}
		for _, want := range []string{"orders", "Orders", "lower_case_table_names=1", "Note 1050"} {
			if !contains(err.Error(), want) {
				t.Errorf("refusal does not mention %q; got: %v", want, err)
			}
		}
		// THE INDEPENDENT ORACLE. Pre-fix (mutation M1) this read map[orders:4].
		if got := mysqlUserTables(t, mysqlTarget); len(got) != 0 {
			t.Errorf("the target holds %v after the refusal; the check runs beside the other pre-DDL "+
				"gates, so nothing should have been created and no row copied", got)
		}
	})

	// THE SECOND DOOR, pinned on its own. The migrate test above is satisfied
	// by either door, so without this the writer-side check could be deleted
	// and every test would stay green — which is how a defense-in-depth pair
	// quietly becomes a single point of failure.
	t.Run("the writer door refuses the same schema, on its own connection", func(t *testing.T) {
		ctx := ctx2min(t)
		sw, err := mysqlEng.OpenSchemaWriter(ctx, mysqlTarget)
		if err != nil {
			t.Fatalf("open target schema writer: %v", err)
		}
		defer migcore.CloseIf(sw)

		colliding := &ir.Schema{Tables: []*ir.Table{
			{Schema: "public", Name: "widgets", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
			{Schema: "public", Name: "Widgets", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
		}}
		err = sw.CreateTablesWithoutConstraints(ctx, colliding)
		if err == nil {
			t.Fatalf("CreateTablesWithoutConstraints ACCEPTED two tables that fold to one name; the "+
				"target now holds %v and the second table was silently not created",
				mysqlUserTables(t, mysqlTarget))
		}
		if ce, coded := sluicecode.FromError(err); !coded || ce.Code != sluicecode.CodeSchemaTableNameCollision {
			t.Errorf("writer refusal must carry %s; got %v", sluicecode.CodeSchemaTableNameCollision, err)
		}
		if got := mysqlUserTables(t, mysqlTarget); len(got) != 0 {
			t.Errorf("the target holds %v; the refusal must precede the first CREATE", got)
		}

		// The control for this door, in the other direction.
		distinct := &ir.Schema{Tables: []*ir.Table{
			{Schema: "public", Name: "widgets", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
			{Schema: "public", Name: "gadgets", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
		}}
		if err := sw.CreateTablesWithoutConstraints(ctx, distinct); err != nil {
			t.Fatalf("the writer door refused two DISTINCT table names: %v", err)
		}
		if got := mysqlUserTables(t, mysqlTarget); len(got) != 2 {
			t.Errorf("target holds %v; want both widgets and gadgets", got)
		}
		dropMySQLTables(t, mysqlTarget, "widgets", "gadgets")
	})

	// THE CONTROL, and it is not optional: a check that refused any schema with
	// two tables would satisfy the assertion above.
	t.Run("distinct table names still migrate to the same folding server", func(t *testing.T) {
		applyPGDDL(t, pgSource, `
			DROP TABLE IF EXISTS public."Orders";
			DROP TABLE IF EXISTS public.orders;
			DROP TABLE IF EXISTS public.order_items;
			CREATE TABLE public.orders      (id BIGINT PRIMARY KEY, tag TEXT NOT NULL);
			CREATE TABLE public.order_items (id BIGINT PRIMARY KEY, tag TEXT NOT NULL);
			INSERT INTO public.orders      (id, tag) VALUES (1, 'o-1'), (2, 'o-2');
			INSERT INTO public.order_items (id, tag) VALUES (1, 'i-1'), (2, 'i-2'), (3, 'i-3');
		`)

		mig := &Migrator{
			Source: pgEng, Target: mysqlEng,
			SourceDSN: pgSource, TargetDSN: mysqlTarget,
			MigrationID: "item149-pg-to-mysql-fold-distinct",
		}
		if err := mig.Run(ctx2min(t)); err != nil {
			t.Fatalf("two tables with DISTINCT names must still migrate to a folding server: %v", err)
		}
		got := mysqlUserTables(t, mysqlTarget)
		if got["orders"] != 2 || got["order_items"] != 3 {
			t.Errorf("target tables = %v; want orders:2 and order_items:3 — every row in its own table", got)
		}
	})
}

// TestMigrate_TableNameFold_PGToMySQL_CaseSensitiveServer is the OVER-REFUSAL
// gate, and it is the half this item could most easily have got wrong.
//
// The identical source pair, the identical code path, a server that does not
// fold — and the migration must SUCCEED with both tables intact. A refusal that
// ignored the server setting would break every stock Linux MySQL migration
// carrying two tables whose names differ only in case, which is a shape
// PostgreSQL sources produce legitimately.
func TestMigrate_TableNameFold_PGToMySQL_CaseSensitiveServer(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()

	_, mysqlTarget, mysqlCleanup := startMySQLCaseSensitive(t)
	defer mysqlCleanup()

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	pgCaseFoldedTablePair(t, pgSource)

	mig := &Migrator{
		Source: pgEng, Target: mysqlEng,
		SourceDSN: pgSource, TargetDSN: mysqlTarget,
		MigrationID: "item149-pg-to-mysql-case-sensitive-allowed",
	}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("the SAME pair must migrate to a case-SENSITIVE MySQL server, where `orders` and "+
			"`Orders` are two ordinary tables: %v", err)
	}
	got := mysqlUserTables(t, mysqlTarget)
	if len(got) != 2 || got["orders"] != 2 || got["Orders"] != 2 {
		t.Errorf("target tables = %v; want BOTH orders:2 and Orders:2. Anything less means the refusal "+
			"fired on a server that does not fold — the over-refusal direction, which breaks migrations "+
			"that work today.", got)
	}
}
