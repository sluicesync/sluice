//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for ADR-0101 native-MySQL concurrent multi-table
// cold-copy. Boots the shared MySQL container (binlog enabled, FTWRL
// available under root), seeds a multi-table DB, opens a CONCURRENT snapshot
// (copy_table_parallelism=2), and asserts:
//
//   - the engine surfaces a disjoint concurrent-copy partition (≥2 groups)
//     via ir.ConcurrentCopyPartitioner — so the ADR-0100 pipeline consumer
//     drives W = 2 read→write pipelines;
//   - every in-scope table is read from its group's connection with the
//     EXACT source row count (no gap/dup — the consistent multi-table
//     snapshot is correct);
//   - a post-snapshot INSERT (committed AFTER the ONE recorded position)
//     surfaces on CDC from that single position (clean handoff, no gap);
//   - the FTWRL-denied path (a restricted user without RELOAD) falls back to
//     the SERIAL single-snapshot path rather than producing an inconsistent
//     N-snapshot (the silent-loss guard).

package mysql

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// concSnapTable builds the IR table for the concurrent-snapshot tables
// (id PK + a value column), mirroring schemaForUsers's shape.
func concSnapTable(name string) *ir.Table {
	return &ir.Table{
		Name: name,
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
			{Name: "v", Type: ir.Varchar{Length: 255}},
		},
	}
}

// TestNativeConcurrentSnapshot_MultiTable is the load-bearing ADR-0101
// integration pin: N=2 concurrent cold-copy of a 4-table DB lands every
// table at its exact source count from a consistent snapshot, and CDC
// resumes cleanly from the ONE recorded position.
func TestNativeConcurrentSnapshot_MultiTable(t *testing.T) {
	dsn, cleanup := startMySQLForSnapshotCDC(t)
	defer cleanup()

	// Seed 4 tables with distinct row counts so a per-table count assertion
	// catches any gap/dup or a table read from the wrong connection.
	const seedDDL = `
		CREATE TABLE t_a (id BIGINT NOT NULL AUTO_INCREMENT, v VARCHAR(255), PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		CREATE TABLE t_b (id BIGINT NOT NULL AUTO_INCREMENT, v VARCHAR(255), PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		CREATE TABLE t_c (id BIGINT NOT NULL AUTO_INCREMENT, v VARCHAR(255), PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		CREATE TABLE t_d (id BIGINT NOT NULL AUTO_INCREMENT, v VARCHAR(255), PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`
	applyMySQLSnap(t, dsn, seedDDL)

	tables := []string{"t_a", "t_b", "t_c", "t_d"}
	wantCounts := map[string]int{"t_a": 7, "t_b": 11, "t_c": 13, "t_d": 17}
	for tbl, n := range wantCounts {
		var b []byte
		for i := 0; i < n; i++ {
			b = append(b, []byte(fmt.Sprintf("INSERT INTO %s (v) VALUES ('%s-%d');", tbl, tbl, i))...)
		}
		applyMySQLSnap(t, dsn, string(b))
	}

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Open the CONCURRENT snapshot: copy_table_parallelism=2 over a 4-table
	// scope → N=2 readers, 2 disjoint groups.
	concDSN := dsn + "&copy_table_parallelism=2"
	stream, err := eng.OpenSnapshotStreamForTables(ctx, concDSN, tables)
	if err != nil {
		t.Fatalf("OpenSnapshotStreamForTables(concurrent): %v", err)
	}
	defer func() { _ = stream.Close() }()

	// The reader MUST surface ≥2 disjoint groups covering exactly the in-scope
	// tables (the ADR-0100 consumer drives W=2 pipelines off this).
	part, ok := stream.Rows.(ir.ConcurrentCopyPartitioner)
	if !ok {
		t.Fatal("concurrent snapshot Rows does not implement ir.ConcurrentCopyPartitioner")
	}
	groups := part.ConcurrentCopyGroups()
	if len(groups) < 2 {
		t.Fatalf("ConcurrentCopyGroups = %v; want ≥2 groups (the W-pipeline consumer must drain ≥2 groups)", groups)
	}
	assertPartitionCoversExactly(t, groups, tables)

	// Step: a post-snapshot INSERT on a SEPARATE pool, committed AFTER the
	// recorded position — must surface on CDC (no gap), not in the snapshot
	// (no overlap).
	applyMySQLSnap(t, dsn, "INSERT INTO t_a (v) VALUES ('post-snapshot');")

	// Drain each table through the multi-snapshot router and assert exact
	// counts. The router dispatches each table to its group's connection.
	for _, tbl := range tables {
		rows := drainAllRows(t, ctx, stream.Rows, concSnapTable(tbl))
		if got := len(rows); got != wantCounts[tbl] {
			t.Errorf("table %q snapshot row count = %d; want %d (gap/dup or wrong-connection read)", tbl, got, wantCounts[tbl])
		}
	}

	// CDC handoff: from the ONE recorded position, the post-snapshot t_a
	// insert must surface exactly once.
	changes, err := stream.Changes.StreamChanges(ctx, stream.Position)
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	got := drainSnapshotChanges(t, ctx, changes, 1, 30*time.Second)
	if len(got) != 1 {
		t.Fatalf("CDC got %d changes; want 1 (the post-snapshot t_a insert — clean handoff from the recorded position)", len(got))
	}
	ins, ok := got[0].(ir.Insert)
	if !ok {
		t.Fatalf("change[0] = %T; want ir.Insert", got[0])
	}
	if v, _ := ins.Row["v"].(string); v != "post-snapshot" {
		t.Errorf("CDC insert v = %#v; want post-snapshot", ins.Row["v"])
	}
}

// TestNativeConcurrentSnapshot_DefaultEngagesConcurrent pins the perf-parity
// gap-3 default flip AGAINST A REAL SOURCE: with NO copy_table_parallelism
// knob anywhere (no DSN param, no CLI override), a multi-table cold-copy now
// opens the FTWRL-coordinated concurrent snapshot with
// min(defaultNativeCopyTableParallelism, len(tables)) readers — and still
// copies every table at its exact count. If someone reverts the resolver
// default to 1, the partition assertion here fails (the serial reader
// surfaces no groups).
func TestNativeConcurrentSnapshot_DefaultEngagesConcurrent(t *testing.T) {
	dsn, cleanup := startMySQLForSnapshotCDC(t)
	defer cleanup()

	// 5 tables > the auto default of 4, so the clamp resolves to exactly
	// defaultNativeCopyTableParallelism groups (pinning the clamp too).
	const seedDDL = `
		CREATE TABLE d_a (id BIGINT NOT NULL AUTO_INCREMENT, v VARCHAR(255), PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		CREATE TABLE d_b (id BIGINT NOT NULL AUTO_INCREMENT, v VARCHAR(255), PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		CREATE TABLE d_c (id BIGINT NOT NULL AUTO_INCREMENT, v VARCHAR(255), PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		CREATE TABLE d_d (id BIGINT NOT NULL AUTO_INCREMENT, v VARCHAR(255), PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		CREATE TABLE d_e (id BIGINT NOT NULL AUTO_INCREMENT, v VARCHAR(255), PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO d_a (v) VALUES ('a1'), ('a2'), ('a3');
		INSERT INTO d_b (v) VALUES ('b1'), ('b2');
		INSERT INTO d_c (v) VALUES ('c1');
		INSERT INTO d_d (v) VALUES ('d1'), ('d2'), ('d3'), ('d4');
		INSERT INTO d_e (v) VALUES ('e1'), ('e2');
	`
	applyMySQLSnap(t, dsn, seedDDL)

	tables := []string{"d_a", "d_b", "d_c", "d_d", "d_e"}
	wantCounts := map[string]int{"d_a": 3, "d_b": 2, "d_c": 1, "d_d": 4, "d_e": 2}

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// NO knob: the plain DSN. The resolver default must engage concurrency.
	stream, err := eng.OpenSnapshotStreamForTables(ctx, dsn, tables)
	if err != nil {
		t.Fatalf("OpenSnapshotStreamForTables(default): %v", err)
	}
	defer func() { _ = stream.Close() }()

	part, ok := stream.Rows.(ir.ConcurrentCopyPartitioner)
	if !ok {
		t.Fatal("default multi-table snapshot Rows does not implement ir.ConcurrentCopyPartitioner — the gap-3 default flip did not engage (serial reader returned)")
	}
	groups := part.ConcurrentCopyGroups()
	if len(groups) != defaultNativeCopyTableParallelism {
		t.Fatalf("default ConcurrentCopyGroups = %d groups; want %d (min(default, 5 tables))", len(groups), defaultNativeCopyTableParallelism)
	}
	assertPartitionCoversExactly(t, groups, tables)

	for _, tbl := range tables {
		rows := drainAllRows(t, ctx, stream.Rows, concSnapTable(tbl))
		if got := len(rows); got != wantCounts[tbl] {
			t.Errorf("table %q default-concurrent snapshot count = %d; want %d (gap/dup)", tbl, got, wantCounts[tbl])
		}
	}
}

// TestNativeConcurrentSnapshot_FTWRLDeniedFallsBackSerial pins the
// silent-loss guard (ADR-0101 §4): when FTWRL is unavailable (a restricted
// user without RELOAD) AND copy_table_parallelism>1 is requested, the opener
// falls back to the SERIAL single-snapshot path — consistent by construction
// — rather than opening N independent snapshots (which could capture an
// inconsistent multi-table view). It must still copy every table.
func TestNativeConcurrentSnapshot_FTWRLDeniedFallsBackSerial(t *testing.T) {
	dsn, cleanup := startMySQLForSnapshotCDC(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE t_a (id BIGINT NOT NULL AUTO_INCREMENT, v VARCHAR(255), PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		CREATE TABLE t_b (id BIGINT NOT NULL AUTO_INCREMENT, v VARCHAR(255), PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO t_a (v) VALUES ('a1'), ('a2'), ('a3');
		INSERT INTO t_b (v) VALUES ('b1'), ('b2');
	`
	applyMySQLSnap(t, dsn, seedDDL)

	// Create a restricted user WITHOUT RELOAD (so FTWRL fails) but with
	// SELECT + REPLICATION privileges so the snapshot + CDC can still run.
	host, port, _, _ := sharedPrimitives()
	dbName := dbNameFromDSN(t, dsn)
	const restrictedUser = "ftwrl_denied"
	const restrictedPass = "denied_pw"
	grantDDL := fmt.Sprintf(`
		DROP USER IF EXISTS '%s'@'%%';
		CREATE USER '%s'@'%%' IDENTIFIED BY '%s';
		GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT, SHOW DATABASES ON *.* TO '%s'@'%%';
		GRANT ALL PRIVILEGES ON `+"`%s`"+`.* TO '%s'@'%%';
		FLUSH PRIVILEGES;
	`, restrictedUser, restrictedUser, restrictedPass, restrictedUser, dbName, restrictedUser)
	applyMySQLSnap(t, dsn, grantDDL)
	defer applyMySQLSnap(t, dsn, fmt.Sprintf("DROP USER IF EXISTS '%s'@'%%';", restrictedUser))

	restrictedDSN := sharedDSN(host, port, restrictedUser, restrictedPass, dbName) + "&copy_table_parallelism=2"

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	stream, err := eng.OpenSnapshotStreamForTables(ctx, restrictedDSN, []string{"t_a", "t_b"})
	if err != nil {
		t.Fatalf("OpenSnapshotStreamForTables(restricted, FTWRL-denied): %v (must fall back to serial, not error)", err)
	}
	defer func() { _ = stream.Close() }()

	// Fallback ⇒ the SERIAL single-snapshot reader, which does NOT surface a
	// concurrent partition (the silent-loss guard: no N independent
	// snapshots).
	if part, ok := stream.Rows.(ir.ConcurrentCopyPartitioner); ok {
		if g := part.ConcurrentCopyGroups(); len(g) > 1 {
			t.Fatalf("FTWRL-denied fallback surfaced %d concurrent groups; want the SERIAL path (no N-snapshot)", len(g))
		}
	}

	// And it must still copy every table correctly (serial, consistent).
	if got := len(drainAllRows(t, ctx, stream.Rows, concSnapTable("t_a"))); got != 3 {
		t.Errorf("t_a serial-fallback count = %d; want 3", got)
	}
	if got := len(drainAllRows(t, ctx, stream.Rows, concSnapTable("t_b"))); got != 2 {
		t.Errorf("t_b serial-fallback count = %d; want 2", got)
	}
}

// assertPartitionCoversExactly checks the disjoint-partition invariant: the
// union of all groups == the in-scope tables, with no duplicates (a table in
// zero groups is silently un-copied; a table in two is double-read).
func assertPartitionCoversExactly(t *testing.T, groups [][]string, tables []string) {
	t.Helper()
	seen := map[string]int{}
	for _, g := range groups {
		for _, name := range g {
			seen[name]++
		}
	}
	for _, tbl := range tables {
		if seen[tbl] == 0 {
			t.Errorf("table %q is in NO partition group (silently un-copied)", tbl)
		}
		if seen[tbl] > 1 {
			t.Errorf("table %q is in %d partition groups; want exactly 1 (double-read)", tbl, seen[tbl])
		}
	}
	var union []string
	for name := range seen {
		union = append(union, name)
	}
	sort.Strings(union)
	want := append([]string(nil), tables...)
	sort.Strings(want)
	if !equalStringSlices(union, want) {
		t.Errorf("partition union = %v; want exactly %v", union, want)
	}
}

// dbNameFromDSN extracts the database name from a sluice MySQL DSN via the
// engine's own parser (so the test mirrors production parsing).
func dbNameFromDSN(t *testing.T, dsn string) string {
	t.Helper()
	cfg, err := parseDSN(dsn)
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	return cfg.DBName
}
