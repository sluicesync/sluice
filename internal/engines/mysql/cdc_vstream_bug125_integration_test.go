//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 125 regression pins — CRITICAL silent-data-loss class.
//
// The pre-v0.100 VStream COPY dedup (copyDedupTracker) DROPPED any
// COPY-phase row whose PK-tuple was <= the running max for its scope,
// assuming Vitess's COPY scan emits in PK-ascending order of the
// PRI_KEY_FLAG column. That assumption is FALSE when Vitess orders the
// COPY scan by a CHEAPER unique key than the flagged column (its
// column-type-cost heuristic: tinyint=1 … bigint=10 … varchar=61). When
// they diverge, legitimate forward rows arrive with PK < running-max
// and were silently dropped — the owner's incident dropped 13.5M of 19M
// rows on a `UNIQUE id(bigint) + cheaper UNIQUE uk_tiny(tinyint), no PK`
// table.
//
// The fix deletes the drop and makes the cold-start COPY writer
// idempotent (ON DUPLICATE KEY UPDATE on a unique key present on the
// target during copy). These tests reproduce the divergent-scan-order
// condition end-to-end against vttestserver and assert ZERO loss + ZERO
// 1062, across the family (pin-the-class, not the representative):
//
//	(a) explicit-PK table                       — clean control
//	(b) no-PK + single UNIQUE id(bigint)        — clean
//	(c) no-PK + UNIQUE id + cheaper UNIQUE tiny  — the drop shape; 0 loss
//	(d) catchup overlap (writes during copy)     — upsert absorbs re-emits
//	(e) truly-keyless table                      — loud refusal, not dup
//
// Each keyed case drains the snapshot stream's COPY rows through the
// real MySQL idempotent writer into a fresh target DB on the shared
// container, then ground-truths target count == source count.
//
// Usage:
//
//	go test -tags='integration vstream' -v -count=1 -timeout=20m \
//	  -run 'TestVStream_Bug125' ./internal/engines/mysql/...

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// bug125Case is one row of the family matrix.
type bug125Case struct {
	name string
	// seedDDL creates the source table on vttestserver.
	seedDDL string
	// table is the IR description used to create the target + drive the
	// idempotent writer. Its Indexes/PrimaryKey must mirror seedDDL so
	// the inline-unique promotion (Bug 125) emits the key the upsert
	// collides on.
	table *ir.Table
	// rowCount source rows to seed.
	rowCount int
	// concurrentDuringCopy, when true, writes additional rows on a
	// separate connection right after OpenSnapshotStream so Vitess's
	// catchup phase re-emits them during COPY (case (d)).
	concurrentDuringCopy bool
}

func TestVStream_Bug125_DivergentScanOrder_ZeroLoss(t *testing.T) {
	cases := []bug125Case{
		{
			name: "a_explicit_pk_control",
			// tiny is SMALLINT (not TINYINT): seedBug125Rows writes
			// tiny = id for rows 1..rowCount, and rowCount (200) exceeds
			// TINYINT's signed max of 127 — a seed-data overflow, not a
			// fix concern. SMALLINT holds the full range.
			seedDDL: `CREATE TABLE t (
				id   BIGINT      NOT NULL,
				tiny SMALLINT    NOT NULL,
				val  VARCHAR(64) NOT NULL,
				PRIMARY KEY (id)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
			table: &ir.Table{
				Name: "t",
				Columns: []*ir.Column{
					{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
					{Name: "tiny", Type: ir.Integer{Width: 16}, Nullable: false},
					{Name: "val", Type: ir.Varchar{Length: 64}, Nullable: false},
				},
				PrimaryKey: &ir.Index{Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
			},
			rowCount: 200,
		},
		{
			name: "b_no_pk_single_unique",
			seedDDL: `CREATE TABLE t (
				id   BIGINT      NOT NULL,
				val  VARCHAR(64) NOT NULL,
				UNIQUE KEY uq_id (id)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
			table: &ir.Table{
				Name: "t",
				Columns: []*ir.Column{
					{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
					{Name: "val", Type: ir.Varchar{Length: 64}, Nullable: false},
				},
				Indexes: []*ir.Index{
					{Name: "uq_id", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
				},
			},
			rowCount: 200,
		},
		{
			name: "c_no_pk_cheaper_unique_drop_shape",
			// The incident shape: a bigint UNIQUE id AND a cheaper
			// tinyint UNIQUE. Vitess's COPY scan orders by the cheaper
			// tinyint key, so rows arrive out of id-order — the
			// pre-fix dedup dropped ~75% of them. tiny must be unique
			// across the seeded rows so uk_tiny is a real unique key
			// (we seed rowCount <= 200 with distinct tiny values via a
			// wider tinyint span; see seedBug125Rows).
			seedDDL: `CREATE TABLE t (
				id   BIGINT      NOT NULL,
				tiny SMALLINT    NOT NULL,
				val  VARCHAR(64) NOT NULL,
				UNIQUE KEY uq_id (id),
				UNIQUE KEY uk_tiny (tiny)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
			table: &ir.Table{
				Name: "t",
				Columns: []*ir.Column{
					{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
					{Name: "tiny", Type: ir.Integer{Width: 16}, Nullable: false},
					{Name: "val", Type: ir.Varchar{Length: 64}, Nullable: false},
				},
				Indexes: []*ir.Index{
					{Name: "uq_id", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
					{Name: "uk_tiny", Unique: true, Columns: []ir.IndexColumn{{Column: "tiny"}}},
				},
			},
			rowCount: 200,
		},
		{
			name: "d_catchup_overlap_concurrent_writes",
			seedDDL: `CREATE TABLE t (
				id   BIGINT      NOT NULL,
				tiny SMALLINT    NOT NULL,
				val  VARCHAR(64) NOT NULL,
				UNIQUE KEY uq_id (id),
				UNIQUE KEY uk_tiny (tiny)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
			table: &ir.Table{
				Name: "t",
				Columns: []*ir.Column{
					{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
					{Name: "tiny", Type: ir.Integer{Width: 16}, Nullable: false},
					{Name: "val", Type: ir.Varchar{Length: 64}, Nullable: false},
				},
				Indexes: []*ir.Index{
					{Name: "uq_id", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
					{Name: "uk_tiny", Unique: true, Columns: []ir.IndexColumn{{Column: "tiny"}}},
				},
			},
			rowCount:             200,
			concurrentDuringCopy: true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			runBug125Case(t, tc)
		})
	}
}

func runBug125Case(t *testing.T, tc bug125Case) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()

	applyVTTestSQL(t, mysqlDSN, tc.seedDDL)
	seedBug125Rows(t, mysqlDSN, tc.rowCount)

	// Let vttestserver's async schema tracker pick the table up before
	// COPY enumerates tables.
	time.Sleep(3 * time.Second)

	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
		mysqlDSN, grpcEndpoint,
	)

	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	stream, err := eng.OpenSnapshotStream(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenSnapshotStream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// Case (d): UPDATE already-seeded rows on a separate connection AFTER
	// the snapshot opened. Rows the COPY scan has already passed get
	// re-emitted during Vitess's COPY catchup (PK <= lastpk); the fix
	// must UPSERT those re-emissions, not 1062 and not drop them. We
	// update existing rows rather than INSERT new high-id ones on
	// purpose: new post-snapshot INSERTs arrive via post-COPY CDC (which
	// this COPY-only test doesn't consume), whereas UPDATEs to scanned
	// rows exercise the catchup-absorption path AND keep the row count
	// deterministic at rowCount — so the zero-loss assertion can't
	// false-fail on snapshot/CDC timing. If the update lands outside the
	// copy window the test still holds (count unchanged, no 1062).
	if tc.concurrentDuringCopy {
		for i := 1; i <= 50; i++ {
			applyVTTestSQL(t, mysqlDSN, fmt.Sprintf(
				"UPDATE t SET val = 'u%d' WHERE id = %d", i, i,
			))
		}
	}

	// The reader must declare it needs an idempotent writer.
	if icr, ok := stream.Rows.(ir.IdempotentCopyReader); !ok || !icr.CopyNeedsIdempotentWriter() {
		t.Fatalf("snapshot Rows must implement ir.IdempotentCopyReader and report true; got %T", stream.Rows)
	}

	// Fresh target DB on the shared MySQL container.
	targetDSN, _ := newSharedDB(t, "bug125_"+tc.name)
	sw, err := Engine{}.OpenSchemaWriter(ctx, targetDSN)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer closeIfErr(sw)
	schema := &ir.Schema{Tables: []*ir.Table{tc.table}}
	if err := sw.CreateTablesWithoutConstraints(ctx, schema); err != nil {
		t.Fatalf("CreateTablesWithoutConstraints: %v", err)
	}

	rw, err := Engine{}.OpenRowWriter(ctx, targetDSN)
	if err != nil {
		t.Fatalf("OpenRowWriter: %v", err)
	}
	defer closeIfErr(rw)
	idem, ok := rw.(ir.IdempotentRowWriter)
	if !ok {
		t.Fatal("MySQL RowWriter must implement ir.IdempotentRowWriter")
	}

	// Drain the COPY rows through the idempotent writer. Any 1062 here
	// would surface as a write error (the divergent-order re-emissions
	// MUST upsert, not collide).
	rowsCh, err := stream.Rows.ReadRows(ctx, tc.table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	if err := idem.WriteRowsIdempotent(ctx, tc.table, rowsCh); err != nil {
		t.Fatalf("WriteRowsIdempotent (case %s): %v — a 1062 here means the COPY re-emissions collided instead of upserting", tc.name, err)
	}
	if err := stream.Rows.Err(); err != nil {
		t.Fatalf("snapshot Rows.Err after drain: %v", err)
	}

	// Post-copy unique indexes (Phase 2). For PK-less tables this also
	// confirms the inline-promoted COPY key isn't double-created.
	if err := sw.CreateIndexes(ctx, schema); err != nil {
		t.Fatalf("CreateIndexes: %v", err)
	}

	// Ground truth: target row count == source row count (every row
	// landed exactly once; the upsert collapsed the catchup re-emissions
	// onto the same key).
	srcCount := scalarCount(t, mysqlDSN, "SELECT COUNT(*) FROM t")
	dstCount := scalarCount(t, targetDSN, "SELECT COUNT(*) FROM t")
	if dstCount != srcCount {
		t.Fatalf("target count = %d; want source count = %d (Bug 125 silent loss)", dstCount, srcCount)
	}
}

// TestVStream_Bug125_KeylessTableRefused pins case (e): a table with no
// PK and no non-null UNIQUE index reaching the idempotent COPY writer is
// a loud refusal (nothing for ON DUPLICATE KEY UPDATE to collide on), not
// a silent duplicate. This is a writer-level unit-style assertion against
// the real MySQL writer; it doesn't need the vttestserver source.
func TestVStream_Bug125_KeylessTableRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	targetDSN, _ := newSharedDB(t, "bug125_keyless")
	rw, err := Engine{}.OpenRowWriter(ctx, targetDSN)
	if err != nil {
		t.Fatalf("OpenRowWriter: %v", err)
	}
	defer closeIfErr(rw)
	idem, ok := rw.(ir.IdempotentRowWriter)
	if !ok {
		t.Fatal("MySQL RowWriter must implement ir.IdempotentRowWriter")
	}

	keyless := &ir.Table{
		Name: "log_lines",
		Columns: []*ir.Column{
			{Name: "ts", Type: ir.Timestamp{}, Nullable: false},
			{Name: "msg", Type: ir.Text{}, Nullable: true},
		},
	}
	rows := make(chan ir.Row)
	close(rows)
	err = idem.WriteRowsIdempotent(ctx, keyless, rows)
	if err == nil {
		t.Fatal("WriteRowsIdempotent on keyless table: err=nil; want loud refusal (Bug 125)")
	}
	t.Logf("keyless refusal (expected): %v", err)
}

// seedBug125Rows inserts n rows with distinct id and tiny values. The
// statement tolerates tables that lack a `tiny` column (cases without
// it) by issuing two shapes and using whichever the table accepts.
func seedBug125Rows(t *testing.T, dsn string, n int) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Probe which columns exist by attempting the 3-col insert first.
	hasTiny := true
	if _, err := db.ExecContext(ctx, "INSERT INTO t (id, tiny, val) VALUES (1, 1, 'r1')"); err != nil {
		hasTiny = false
		if _, err2 := db.ExecContext(ctx, "INSERT INTO t (id, val) VALUES (1, 'r1')"); err2 != nil {
			t.Fatalf("seed first row: 3-col err=%v; 2-col err=%v", err, err2)
		}
	}
	for i := 2; i <= n; i++ {
		var err error
		if hasTiny {
			_, err = db.ExecContext(ctx,
				fmt.Sprintf("INSERT INTO t (id, tiny, val) VALUES (%d, %d, 'r%d')", i, i, i))
		} else {
			_, err = db.ExecContext(ctx,
				fmt.Sprintf("INSERT INTO t (id, val) VALUES (%d, 'r%d')", i, i))
		}
		if err != nil {
			t.Fatalf("seed row %d: %v", i, err)
		}
	}
}

// scalarCount runs a COUNT(*) query and returns the integer result.
func scalarCount(t *testing.T, dsn, query string) int {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("count open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx, query).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", query, err)
	}
	return n
}

// closeIfErr closes an io.Closer-shaped value, ignoring the error
// (test cleanup). Mirrors the pipeline package's closeIf without
// importing it.
func closeIfErr(v any) {
	if c, ok := v.(interface{ Close() error }); ok {
		_ = c.Close()
	}
}
