//go:build integration

// Cross-engine integration tests for the orchestrator-side table
// filter (MySQL → Postgres). Three cases:
//
//   - --exclude-table drops one of three source tables on the target.
//   - --include-table picks exactly one source table to migrate.
//   - --exclude-table with a glob pattern drops a family of tables.
//
// Reuses startMySQL / startPostgres from the existing cross-engine
// suite. The filter is engine-neutral, so MySQL→PG is sufficient
// to validate that the prune happens at the orchestrator boundary
// and that no engine reader sees a filter argument.

package pipeline

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"
	"github.com/orware/sluice/internal/ir"

	_ "github.com/orware/sluice/internal/engines/mysql"
	_ "github.com/orware/sluice/internal/engines/postgres"
)

// TestMigrate_FilterExcludesTable verifies that --exclude-table
// keeps the named table off the target while the rest land
// normally. The seed creates three tables; the filter excludes
// `audit_log`; the assertion is that the PG target has only two
// of them.
func TestMigrate_FilterExcludesTable(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT PRIMARY KEY AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL
		) ENGINE=InnoDB;
		CREATE TABLE orders (
			id      BIGINT PRIMARY KEY AUTO_INCREMENT,
			user_id BIGINT NOT NULL
		) ENGINE=InnoDB;
		CREATE TABLE audit_log (
			id   BIGINT PRIMARY KEY AUTO_INCREMENT,
			what VARCHAR(255) NOT NULL
		) ENGINE=InnoDB;
		INSERT INTO users (email) VALUES ('alice@example.com');
		INSERT INTO orders (user_id) VALUES (1);
		INSERT INTO audit_log (what) VALUES ('seed');
	`
	applyMySQLDDL(t, mysqlSource, seedDDL)

	mysqlEng, _ := engines.Get("mysql")
	pgEng, _ := engines.Get("postgres")

	filter, err := NewTableFilter(nil, []string{"audit_log"})
	if err != nil {
		t.Fatalf("NewTableFilter: %v", err)
	}
	mig := &Migrator{
		Source:    mysqlEng,
		Target:    pgEng,
		SourceDSN: mysqlSource,
		TargetDSN: pgTarget,
		Filter:    filter,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	got := pgTargetTableNames(t, ctx, pgEng, pgTarget)
	want := []string{"orders", "users"}
	if !equalSorted(got, want) {
		t.Errorf("target tables = %v; want %v (audit_log should be excluded)", got, want)
	}
}

// TestMigrate_FilterIncludesOnly verifies that --include-table picks
// exactly one of three source tables.
func TestMigrate_FilterIncludesOnly(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT PRIMARY KEY AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL
		) ENGINE=InnoDB;
		CREATE TABLE orders (
			id      BIGINT PRIMARY KEY AUTO_INCREMENT,
			user_id BIGINT NOT NULL
		) ENGINE=InnoDB;
		CREATE TABLE audit_log (
			id   BIGINT PRIMARY KEY AUTO_INCREMENT,
			what VARCHAR(255) NOT NULL
		) ENGINE=InnoDB;
		INSERT INTO users (email) VALUES ('alice@example.com');
		INSERT INTO orders (user_id) VALUES (1);
		INSERT INTO audit_log (what) VALUES ('seed');
	`
	applyMySQLDDL(t, mysqlSource, seedDDL)

	mysqlEng, _ := engines.Get("mysql")
	pgEng, _ := engines.Get("postgres")

	filter, err := NewTableFilter([]string{"users"}, nil)
	if err != nil {
		t.Fatalf("NewTableFilter: %v", err)
	}
	mig := &Migrator{
		Source:    mysqlEng,
		Target:    pgEng,
		SourceDSN: mysqlSource,
		TargetDSN: pgTarget,
		Filter:    filter,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	got := pgTargetTableNames(t, ctx, pgEng, pgTarget)
	want := []string{"users"}
	if !equalSorted(got, want) {
		t.Errorf("target tables = %v; want %v (only users should be present)", got, want)
	}
}

// TestMigrate_FilterGlobPattern verifies stdlib path.Match glob
// semantics: an exclude pattern of `audit_*` drops every table
// whose name starts with `audit_`.
func TestMigrate_FilterGlobPattern(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT PRIMARY KEY AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL
		) ENGINE=InnoDB;
		CREATE TABLE audit_login (
			id   BIGINT PRIMARY KEY AUTO_INCREMENT,
			what VARCHAR(255) NOT NULL
		) ENGINE=InnoDB;
		CREATE TABLE audit_logout (
			id   BIGINT PRIMARY KEY AUTO_INCREMENT,
			what VARCHAR(255) NOT NULL
		) ENGINE=InnoDB;
		INSERT INTO users (email) VALUES ('alice@example.com');
		INSERT INTO audit_login (what) VALUES ('seed');
		INSERT INTO audit_logout (what) VALUES ('seed');
	`
	applyMySQLDDL(t, mysqlSource, seedDDL)

	mysqlEng, _ := engines.Get("mysql")
	pgEng, _ := engines.Get("postgres")

	filter, err := NewTableFilter(nil, []string{"audit_*"})
	if err != nil {
		t.Fatalf("NewTableFilter: %v", err)
	}
	mig := &Migrator{
		Source:    mysqlEng,
		Target:    pgEng,
		SourceDSN: mysqlSource,
		TargetDSN: pgTarget,
		Filter:    filter,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	got := pgTargetTableNames(t, ctx, pgEng, pgTarget)
	want := []string{"users"}
	if !equalSorted(got, want) {
		t.Errorf("target tables = %v; want %v (audit_* should be excluded)", got, want)
	}
}

// pgTargetTableNames reads the target's schema and returns sorted
// table names. Used by the filter tests, which only care about
// presence/absence of tables, not their shape.
func pgTargetTableNames(t *testing.T, ctx context.Context, eng ir.Engine, dsn string) []string {
	t.Helper()
	sr, err := eng.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(sr)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	out := make([]string, 0, len(schema.Tables))
	for _, tab := range schema.Tables {
		out = append(out, tab.Name)
	}
	sort.Strings(out)
	return out
}

// equalSorted compares two string slices after sorting both.
// Helper to keep filter assertions order-independent.
func equalSorted(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	aa := append([]string(nil), a...)
	bb := append([]string(nil), b...)
	sort.Strings(aa)
	sort.Strings(bb)
	for i := range aa {
		if aa[i] != bb[i] {
			return false
		}
	}
	return true
}
