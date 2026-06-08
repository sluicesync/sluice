// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// zeroDateTable is the temporal-class fixture: one nullable column per
// IR temporal family (DATE/DATETIME/TIMESTAMP), so a single read exercises
// every family rather than a single representative (the Bug-74 lesson).
func zeroDateTable() *ir.Table {
	return &ir.Table{
		Name: "zerodates",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
			{Name: "d", Type: ir.Date{}, Nullable: true},
			{Name: "dt", Type: ir.DateTime{}, Nullable: true},
			{Name: "ts", Type: ir.Timestamp{}, Nullable: true},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
}

// readZeroDateRows drains a full RowReader scan of the fixture table and
// returns the rows keyed by their id, or the sticky Err.
func readZeroDateRows(t *testing.T, db *sql.DB) (map[int64]ir.Row, error) {
	t.Helper()
	rr := &RowReader{q: db}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ch, err := rr.ReadRows(ctx, zeroDateTable())
	if err != nil {
		return nil, err
	}
	out := map[int64]ir.Row{}
	for row := range ch {
		id, ok := row["id"].(int64)
		if !ok {
			t.Fatalf("id column is %T, want int64", row["id"])
		}
		out[id] = row
	}
	return out, rr.Err()
}

// TestZeroDate_ReadPath is the end-to-end Vector A pin against a real
// MySQL. It seeds zero and partial dates (storable only under a relaxed
// source sql_mode) and proves:
//
//  1. GROUND TRUTH — under the driver's parseTime=true, a non-CAST scan
//     silently NORMALIZES a partial date (2026-00-00 → 2025-11-30). This
//     is the live-driver evidence for the silent-corruption class.
//  2. The sluice read path (CAST(... AS CHAR) + decodeTime) surfaces the
//     same value as a loud refusal under the default --zero-date=error,
//     across the whole temporal family.
//  3. --zero-date=null carries the zero/partial dates as SQL NULL, and
//     --zero-date=epoch substitutes 1970-01-01 — while a genuinely valid
//     row always round-trips unchanged.
func TestZeroDate_ReadPath(t *testing.T) {
	const dbName = "sluice_zerodate"
	host, port, user, password := ensureSharedMySQL(t)
	resetSharedDB(t, dbName)
	dsn := sharedDSN(host, port, user, password, dbName) + "&multiStatements=true"

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `
		CREATE TABLE zerodates (
			id INT PRIMARY KEY,
			d  DATE      NULL,
			dt DATETIME  NULL,
			ts TIMESTAMP NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Seed under a relaxed sql_mode on a dedicated connection so the
	// zero/partial dates are actually stored (sluice forces strict mode
	// on its own connections, which is irrelevant to reading them back).
	seed, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("seed conn: %v", err)
	}
	if _, err := seed.ExecContext(ctx, "SET SESSION sql_mode = ''"); err != nil {
		t.Fatalf("relax sql_mode: %v", err)
	}
	// Row 1: valid. Rows 2-4: all-zero / zero-month / zero-day in the
	// DATE column, paired with zero datetime+timestamp.
	if _, err := seed.ExecContext(ctx, `INSERT INTO zerodates (id,d,dt,ts) VALUES
		(1,'2026-06-07','2026-06-07 12:34:56','2026-06-07 12:34:56'),
		(2,'0000-00-00','0000-00-00 00:00:00','0000-00-00 00:00:00'),
		(3,'2026-00-15','2026-00-15 01:02:03','0000-00-00 00:00:00'),
		(4,'2026-06-00','2026-06-00 01:02:03','0000-00-00 00:00:00')`); err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	_ = seed.Close()

	// (1) GROUND TRUTH: a parseTime=true scan WITHOUT the CAST detour
	// silently normalizes the partial date — exactly what sluice must NOT
	// do. Row 4's DATE 2026-06-00 normalizes to 2026-05-31.
	t.Run("driver_silently_normalizes_without_cast", func(t *testing.T) {
		var got time.Time
		if err := db.QueryRowContext(ctx, "SELECT d FROM zerodates WHERE id = 4").Scan(&got); err != nil {
			t.Fatalf("scan: %v", err)
		}
		want := time.Date(2026, 5, 31, 0, 0, 0, 0, time.UTC)
		if !got.Equal(want) {
			t.Fatalf("driver returned %v for 2026-06-00; expected the silent normalization %v "+
				"(driver behavior changed — revisit the Vector A fix)", got, want)
		}
	})

	// (2) Default policy: the sluice read path refuses loudly.
	t.Run("error_policy_refuses_loudly", func(t *testing.T) {
		withZeroDatePolicy(t, zeroDateRefuse)
		_, err := readZeroDateRows(t, db)
		if err == nil {
			t.Fatal("ReadRows Err = nil; want a zero-date refusal")
		}
		if !strings.Contains(err.Error(), "zero/partial date") {
			t.Errorf("Err = %q; want it to name the zero/partial date", err)
		}
	})

	// (3a) null policy: zero/partial dates become NULL; valid row intact.
	t.Run("null_policy_carries_null", func(t *testing.T) {
		withZeroDatePolicy(t, zeroDateAsNull)
		rows, err := readZeroDateRows(t, db)
		if err != nil {
			t.Fatalf("ReadRows Err = %v; want nil under null policy", err)
		}
		if len(rows) != 4 {
			t.Fatalf("read %d rows; want 4", len(rows))
		}
		// Valid row round-trips.
		if v := rows[1]["d"]; v != time.Date(2026, 6, 7, 0, 0, 0, 0, time.UTC) {
			t.Errorf("row 1 d = %v; want 2026-06-07", v)
		}
		// Rows 2-4 carry a zero/partial value in every temporal column
		// (all-zero, zero-month, zero-day respectively in d/dt; zero ts),
		// so under the null policy all three cells of each become NULL.
		for _, id := range []int64{2, 3, 4} {
			for _, col := range []string{"d", "dt", "ts"} {
				if v := rows[id][col]; v != nil {
					t.Errorf("row %d %s = %v; want NULL", id, col, v)
				}
			}
		}
	})

	// (3b) epoch policy: zero/partial dates become 1970-01-01.
	t.Run("epoch_policy_substitutes_epoch", func(t *testing.T) {
		withZeroDatePolicy(t, zeroDateAsEpoch)
		rows, err := readZeroDateRows(t, db)
		if err != nil {
			t.Fatalf("ReadRows Err = %v; want nil under epoch policy", err)
		}
		epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		// Row 2 is all-zero across all three temporal columns.
		for _, col := range []string{"d", "dt", "ts"} {
			got, ok := rows[2][col].(time.Time)
			if !ok {
				t.Fatalf("row 2 %s = %T; want time.Time", col, rows[2][col])
			}
			if !got.Equal(epoch) {
				t.Errorf("row 2 %s = %v; want epoch %v", col, got, epoch)
			}
		}
	})
}

// readZeroDateRowsBatched drains the fixture through the CHUNKED cursor
// path (ReadRowsBatch) rather than the full scan, with a small limit so
// several pages are stitched. This is the >100k-row parallel-copy path a
// real legacy migrate actually takes, so the zero-date decode + policy
// must hold there too — not just on the full-scan ReadRows (gap closed
// from the Vector A value-fidelity review).
func readZeroDateRowsBatched(t *testing.T, db *sql.DB, limit int) (map[int64]ir.Row, error) {
	t.Helper()
	rr := &RowReader{q: db}
	table := zeroDateTable()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out := map[int64]ir.Row{}
	var cursor []any
	for {
		ch, err := rr.ReadRowsBatch(ctx, table, cursor, limit)
		if err != nil {
			return nil, err
		}
		var last ir.Row
		n := 0
		for row := range ch {
			id, ok := row["id"].(int64)
			if !ok {
				t.Fatalf("id column is %T, want int64", row["id"])
			}
			if _, dup := out[id]; dup {
				t.Fatalf("id %d returned twice across pages (cursor stitch bug)", id)
			}
			out[id] = row
			last = row
			n++
		}
		if err := rr.Err(); err != nil {
			return nil, err
		}
		if n < limit {
			break
		}
		cursor = []any{last["id"]}
	}
	return out, nil
}

// TestZeroDate_BatchedReadPath mirrors TestZeroDate_ReadPath through the
// chunked ReadRowsBatch path (limit=2 so the four-row fixture spans pages).
// It pins that the Vector A zero-date decode + --zero-date policies behave
// identically on the keyset-paginated copy path as on the full scan.
func TestZeroDate_BatchedReadPath(t *testing.T) {
	const dbName = "sluice_zerodate_batched"
	host, port, user, password := ensureSharedMySQL(t)
	resetSharedDB(t, dbName)
	dsn := sharedDSN(host, port, user, password, dbName) + "&multiStatements=true"

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `
		CREATE TABLE zerodates (
			id INT PRIMARY KEY,
			d  DATE      NULL,
			dt DATETIME  NULL,
			ts TIMESTAMP NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	seed, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("seed conn: %v", err)
	}
	if _, err := seed.ExecContext(ctx, "SET SESSION sql_mode = ''"); err != nil {
		t.Fatalf("relax sql_mode: %v", err)
	}
	if _, err := seed.ExecContext(ctx, `INSERT INTO zerodates (id,d,dt,ts) VALUES
		(1,'2026-06-07','2026-06-07 12:34:56','2026-06-07 12:34:56'),
		(2,'0000-00-00','0000-00-00 00:00:00','0000-00-00 00:00:00'),
		(3,'2026-00-15','2026-00-15 01:02:03','0000-00-00 00:00:00'),
		(4,'2026-06-00','2026-06-00 01:02:03','0000-00-00 00:00:00')`); err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	_ = seed.Close()

	t.Run("error_policy_refuses_loudly", func(t *testing.T) {
		withZeroDatePolicy(t, zeroDateRefuse)
		_, err := readZeroDateRowsBatched(t, db, 2)
		if err == nil {
			t.Fatal("batched read Err = nil; want a zero-date refusal")
		}
		if !strings.Contains(err.Error(), "zero/partial date") {
			t.Errorf("Err = %q; want it to name the zero/partial date", err)
		}
	})

	t.Run("null_policy_carries_null", func(t *testing.T) {
		withZeroDatePolicy(t, zeroDateAsNull)
		rows, err := readZeroDateRowsBatched(t, db, 2)
		if err != nil {
			t.Fatalf("batched read Err = %v; want nil under null policy", err)
		}
		if len(rows) != 4 {
			t.Fatalf("read %d rows; want 4 (cursor stitch lost a page)", len(rows))
		}
		if v := rows[1]["d"]; v != time.Date(2026, 6, 7, 0, 0, 0, 0, time.UTC) {
			t.Errorf("row 1 d = %v; want 2026-06-07", v)
		}
		for _, id := range []int64{2, 3, 4} {
			for _, col := range []string{"d", "dt", "ts"} {
				if v := rows[id][col]; v != nil {
					t.Errorf("row %d %s = %v; want NULL", id, col, v)
				}
			}
		}
	})

	t.Run("epoch_policy_substitutes_epoch", func(t *testing.T) {
		withZeroDatePolicy(t, zeroDateAsEpoch)
		rows, err := readZeroDateRowsBatched(t, db, 2)
		if err != nil {
			t.Fatalf("batched read Err = %v; want nil under epoch policy", err)
		}
		if len(rows) != 4 {
			t.Fatalf("read %d rows; want 4", len(rows))
		}
		epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		for _, col := range []string{"d", "dt", "ts"} {
			got, ok := rows[2][col].(time.Time)
			if !ok {
				t.Fatalf("row 2 %s = %T; want time.Time", col, rows[2][col])
			}
			if !got.Equal(epoch) {
				t.Errorf("row 2 %s = %v; want epoch %v", col, got, epoch)
			}
		}
	})
}

// TestZeroDate_TemporalPKPagination is the end-to-end defense for the
// cursor-qualification fix: a table whose PRIMARY KEY is itself a temporal
// column is read through ReadRowsBatch with a limit far below the row
// count, so the keyset cursor walks many page boundaries. The PK column is
// projected as CAST(... AS CHAR); the ORDER BY / WHERE must bind the real
// DATE column (table-qualified) so the time.Time cursor value compares
// date-typed. Every seeded row must come back exactly once, in date order.
// (Valid ISO dates paginate correctly under either sort — this proves no
// skip/dup regardless; the unit test pins the qualified shape directly.)
func TestZeroDate_TemporalPKPagination(t *testing.T) {
	const dbName = "sluice_temporal_pk"
	host, port, user, password := ensureSharedMySQL(t)
	resetSharedDB(t, dbName)
	dsn := sharedDSN(host, port, user, password, dbName) + "&multiStatements=true"

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	for _, tc := range []struct {
		name    string
		ddlType string
		irType  ir.Type
		mk      func(i int) string // SQL literal for row i
		mkGo    func(i int) time.Time
	}{
		{
			name:    "DATE_pk",
			ddlType: "DATE",
			irType:  ir.Date{},
			mk: func(i int) string {
				return time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, i).Format("2006-01-02")
			},
			mkGo: func(i int) time.Time { return time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, i) },
		},
		{
			name:    "DATETIME6_pk",
			ddlType: "DATETIME(6)",
			irType:  ir.DateTime{Precision: 6},
			mk: func(i int) string {
				return time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(i) * 1234567 * time.Microsecond).Format("2006-01-02 15:04:05.000000")
			},
			mkGo: func(i int) time.Time {
				return time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(i) * 1234567 * time.Microsecond)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS tpk"); err != nil {
				t.Fatalf("drop: %v", err)
			}
			if _, err := db.ExecContext(ctx, "CREATE TABLE tpk (k "+tc.ddlType+" PRIMARY KEY, v INT NOT NULL) ENGINE=InnoDB"); err != nil {
				t.Fatalf("create: %v", err)
			}
			const n = 25
			var b strings.Builder
			b.WriteString("INSERT INTO tpk (k,v) VALUES ")
			for i := 0; i < n; i++ {
				if i > 0 {
					b.WriteString(",")
				}
				fmt.Fprintf(&b, "('%s',%d)", tc.mk(i), i)
			}
			if _, err := db.ExecContext(ctx, b.String()); err != nil {
				t.Fatalf("seed: %v", err)
			}

			table := &ir.Table{
				Name: "tpk",
				Columns: []*ir.Column{
					{Name: "k", Type: tc.irType, Nullable: false},
					{Name: "v", Type: ir.Integer{Width: 32}, Nullable: false},
				},
				PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "k"}}},
			}

			withZeroDatePolicy(t, zeroDateRefuse) // valid dates only; policy irrelevant
			rr := &RowReader{q: db}
			rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()

			const limit = 4 // forces ~7 pages across 25 rows
			seenV := map[int]bool{}
			var prevK time.Time
			havePrev := false
			var cursor []any
			total := 0
			for {
				ch, err := rr.ReadRowsBatch(rctx, table, cursor, limit)
				if err != nil {
					t.Fatalf("ReadRowsBatch: %v", err)
				}
				var last ir.Row
				pageN := 0
				for row := range ch {
					k, ok := row["k"].(time.Time)
					if !ok {
						t.Fatalf("k = %T, want time.Time", row["k"])
					}
					if havePrev && !k.After(prevK) {
						t.Fatalf("rows not strictly ascending: %v after %v (cursor/order mismatch)", k, prevK)
					}
					prevK, havePrev = k, true
					v, ok := row["v"].(int64) // decodeInteger normalizes all widths to int64
					if !ok {
						t.Fatalf("v = %T, want int64", row["v"])
					}
					if seenV[int(v)] {
						t.Fatalf("v=%d returned twice (page stitch skipped/repeated a boundary row)", v)
					}
					seenV[int(v)] = true
					last = row
					pageN++
					total++
				}
				if err := rr.Err(); err != nil {
					t.Fatalf("Err: %v", err)
				}
				if pageN < limit {
					break
				}
				cursor = []any{last["k"]}
			}
			if total != n {
				t.Fatalf("paginated %d rows; want %d (skip or premature stop)", total, n)
			}
			// Spot-check the cursor actually carried the temporal value
			// faithfully (the v==i invariant ties row identity to date order).
			for i := 0; i < n; i++ {
				if !seenV[i] {
					t.Errorf("missing v=%d (date %v)", i, tc.mkGo(i))
				}
			}
		})
	}
}

// TestZeroDate_NullPolicyRefusesNotNull pins the precise loud refusal:
// --zero-date=null cannot silently drop a zero date into a NOT NULL
// column, so it refuses naming the column rather than deferring to a
// late constraint violation.
func TestZeroDate_NullPolicyRefusesNotNull(t *testing.T) {
	const dbName = "sluice_zerodate_nn"
	host, port, user, password := ensureSharedMySQL(t)
	resetSharedDB(t, dbName)
	dsn := sharedDSN(host, port, user, password, dbName) + "&multiStatements=true"

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `
		CREATE TABLE nn (
			id INT PRIMARY KEY,
			d  DATE NOT NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	seed, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("seed conn: %v", err)
	}
	if _, err := seed.ExecContext(ctx, "SET SESSION sql_mode = ''"); err != nil {
		t.Fatalf("relax sql_mode: %v", err)
	}
	if _, err := seed.ExecContext(ctx, "INSERT INTO nn (id,d) VALUES (1,'0000-00-00')"); err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	_ = seed.Close()

	withZeroDatePolicy(t, zeroDateAsNull)
	rr := &RowReader{q: db}
	table := &ir.Table{
		Name: "nn",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
			{Name: "d", Type: ir.Date{}, Nullable: false},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
	rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ch, err := rr.ReadRows(rctx, table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	for range ch { //nolint:revive // drain to surface the sticky Err
	}
	err = rr.Err()
	if err == nil {
		t.Fatal("Err = nil; want a NOT NULL refusal under --zero-date=null")
	}
	if !strings.Contains(err.Error(), "NOT NULL") {
		t.Errorf("Err = %q; want it to name the NOT NULL conflict", err)
	}
}
