//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The one arm of the Bug 193 belt that is deliberately SKIPPED, and the
// premise it is skipped on.
//
// `cdc_reader.go`'s DELETE arm refuses a partial row image only for a
// PK-less table. With a real PK it lets a partial image straight through,
// because [filterBeforeToPK] narrows the before-image to the PK columns and
// the comment there asserts that narrowing is "correct by construction":
//
//	A PK column missing from the Before-image is structurally impossible
//	under any binlog_row_image setting (the PK is always carried; that's
//	the whole point of MINIMAL).
//
// That is a claim about what MySQL writes into a rows-event, not about
// sluice — the premise-naming class. Nothing checked it. If it were ever
// false for some PK shape, `filterBeforeToPK` copies the missing column
// through as nil (its own comment says so), the applier's WHERE becomes
// `pk IS NULL`, the DELETE matches zero rows, ADR-0010 absorbs the miss for
// resume idempotency, and the position advances: a deleted row silently
// survives on the target forever. The belt that would have caught it is
// switched off precisely here.
//
// So this is the family matrix, not a representative. The dispatch is on PK
// SHAPE (what `mark_columns_used_by_index_no_reset` marks) crossed with row
// IMAGE (which columns the server then clears), so both axes are enumerated:
// every PK shape MySQL will accept × {MINIMAL, NOBLOB} × the FULL control.
//
// WHAT IT ASSERTS AND WHY THAT IS STRONGER THAN THE BITMAP CHECK the audit
// entry proposed ("~10 lines asserting the skipped-columns bitmap still
// carries the PK"): it asserts the emitted [ir.Delete].Before carries every
// PK column with its exact value. go-mysql leaves a skipped column as nil in
// `RowsEvent.Rows`, so a PK missing from the bitmap arrives as a present-but-
// nil entry — which is the shape that produces the zero-matching WHERE. The
// bitmap property is therefore necessary for this assertion to hold, and the
// assertion additionally covers the decode, the narrowing, and the belt not
// firing. It is measured on the product's own reader.
//
// The overrides are SESSION-level on the writer connection, which is the
// exact shape the belt exists for: it slips past the GLOBAL preflight by
// design, and it is also how a resume replaying a MINIMAL-era binlog segment
// looks.

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// pkCarriageCase is one PK SHAPE. ddl creates the table, seed populates it,
// del removes exactly one row, and wantPK is the before-image the narrowing
// must produce for that row — every PK column, with its real value.
//
// requiresGIPK marks the cell that needs `sql_generate_invisible_primary_key`
// (MySQL 8.0.30+); it is skipped with a named reason on a server without it
// rather than silently dropped.
type pkCarriageCase struct {
	name string
	// base is the table name the ddl/seed/del statements are written
	// against; each (shape, image) cell substitutes its own suffixed name so
	// one cell's DELETE can never be the row another cell asserts on.
	base         string
	ddl          string
	seed         string
	del          string
	wantPK       ir.Row
	requiresGIPK bool
	// wantRefusal marks the shape whose identity sluice CANNOT carry, so
	// the cell asserts the coded refusal instead of a before-image. This
	// is the finding the matrix produced: it is a property of sluice's
	// decoder, not of the row image, so it holds under FULL too.
	wantRefusal bool
}

func pkCarriageCases() []pkCarriageCase {
	return []pkCarriageCase{
		{
			// The representative every existing pin uses. Here as the
			// control, not as the coverage.
			name: "pk_first_column",
			base: "c_first",
			ddl: `CREATE TABLE c_first (
				id     BIGINT      NOT NULL,
				status VARCHAR(32) NOT NULL,
				body   TEXT,
				PRIMARY KEY (id)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
			seed:   `INSERT INTO c_first (id, status, body) VALUES (1,'new','x'), (2,'new','y')`,
			del:    `DELETE FROM c_first WHERE id = 1`,
			wantPK: ir.Row{"id": int64(1)},
		},
		{
			// The PK is the LAST column. A bitmap built from the wrong end,
			// or an off-by-one in the skipped-column complement, shows up
			// here and not in the representative above.
			name: "pk_last_column",
			base: "c_last",
			ddl: `CREATE TABLE c_last (
				status VARCHAR(32) NOT NULL,
				body   TEXT,
				id     BIGINT      NOT NULL,
				PRIMARY KEY (id)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
			seed:   `INSERT INTO c_last (status, body, id) VALUES ('new','x',1), ('new','y',2)`,
			del:    `DELETE FROM c_last WHERE id = 1`,
			wantPK: ir.Row{"id": int64(1)},
		},
		{
			// Composite PK spanning NON-adjacent columns, so the marked set
			// is not a contiguous run.
			name: "pk_composite_split",
			base: "c_split",
			ddl: `CREATE TABLE c_split (
				tenant BIGINT      NOT NULL,
				filler VARCHAR(32) NOT NULL,
				body   TEXT,
				code   BIGINT      NOT NULL,
				PRIMARY KEY (tenant, code)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
			seed:   `INSERT INTO c_split (tenant, filler, body, code) VALUES (7,'f','x',11), (7,'f','y',12)`,
			del:    `DELETE FROM c_split WHERE tenant = 7 AND code = 11`,
			wantPK: ir.Row{"tenant": int64(7), "code": int64(11)},
		},
		{
			// A PREFIX PK on a BLOB column. This is the NOBLOB interaction
			// worth naming: NOBLOB strips BLOB/TEXT columns from the image
			// UNLESS they are needed to identify the row, so the whole
			// argument here is that MySQL's PRI_KEY_FLAG carve-out really
			// fires. A prefix index also means the INDEX holds 16 bytes
			// while the image must carry the FULL value.
			name: "pk_blob_prefix",
			base: "c_blob",
			ddl: `CREATE TABLE c_blob (
				k    VARBINARY(64) NOT NULL,
				body TEXT,
				PRIMARY KEY (k(16))
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
			// The two seeds must differ within the 16-byte PREFIX the key
			// covers, or the second INSERT is a duplicate-key error — a
			// reminder that the index sees 16 bytes while the row image
			// must carry all 20.
			seed:   `INSERT INTO c_blob (k, body) VALUES ('alpha-000000000001AA','x'), ('bravo-000000000002BB','y')`,
			del:    `DELETE FROM c_blob WHERE k = 'alpha-000000000001AA'`,
			wantPK: ir.Row{"k": []byte("alpha-000000000001AA")},
		},
		{
			// PK on a STORED generated column — the cell that found the
			// defect. MySQL carries the value in the row image (this fails
			// under FULL as well as MINIMAL/NOBLOB, which is the tell that
			// the loss is sluice-side): [decodeBinlogRow] drops every
			// generated column, so the narrowing produced `{"g": nil}` and
			// the applier would render an empty WHERE or `g IS NULL`.
			// Refused loudly now; see cdc_generated_pk.go.
			name:        "pk_stored_generated",
			base:        "c_gen",
			wantRefusal: true,
			ddl: `CREATE TABLE c_gen (
				a      BIGINT NOT NULL,
				b      BIGINT NOT NULL,
				g      BIGINT GENERATED ALWAYS AS (a * 1000 + b) STORED,
				filler TEXT,
				PRIMARY KEY (g)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
			seed:   `INSERT INTO c_gen (a, b, filler) VALUES (1,1,'x'), (1,2,'y')`,
			del:    `DELETE FROM c_gen WHERE g = 1001`,
			wantPK: ir.Row{"g": int64(1001)},
		},
		{
			// PK on a user-declared INVISIBLE column (MySQL 8.0.23+). The
			// column is absent from `SELECT *` but present in the row image
			// and in information_schema — a reader that filtered on
			// visibility anywhere would lose the identity.
			name: "pk_invisible_column",
			base: "c_invis",
			ddl: `CREATE TABLE c_invis (
				id     BIGINT      NOT NULL INVISIBLE,
				status VARCHAR(32) NOT NULL,
				PRIMARY KEY (id)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
			seed:   `INSERT INTO c_invis (id, status) VALUES (1,'new'), (2,'new')`,
			del:    `DELETE FROM c_invis WHERE id = 1`,
			wantPK: ir.Row{"id": int64(1)},
		},
		{
			// The server-generated invisible primary key (MySQL 8.0.30+):
			// the table is declared with NO key at all and mysqld mints
			// `my_row_id`. Under MINIMAL the before-image is keyed on a
			// column the DDL never mentions.
			name: "pk_generated_invisible_my_row_id",
			base: "c_gipk",
			ddl: `CREATE TABLE c_gipk (
				a      BIGINT      NOT NULL,
				status VARCHAR(32) NOT NULL
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
			seed:         `INSERT INTO c_gipk (a, status) VALUES (10,'new'), (20,'new')`,
			del:          `DELETE FROM c_gipk WHERE a = 10`,
			wantPK:       ir.Row{"my_row_id": int64(1)},
			requiresGIPK: true,
		},
	}
}

// TestCDCReader_MinimalDeleteCarriesThePK is the matrix. Every cell streams
// through the real CDC reader, so a cell that the belt wrongly refuses fails
// just as loudly as one whose identity went missing.
func TestCDCReader_MinimalDeleteCarriesThePK(t *testing.T) {
	dsn, cleanup := startMySQLRowImageForCDC(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	eng := Engine{Flavor: FlavorVanilla}
	gipk := serverSupportsGIPK(t, dsn)

	graded := 0
	for _, tc := range pkCarriageCases() {
		for _, image := range []string{"MINIMAL", "NOBLOB", "FULL"} {
			t.Run(tc.name+"/"+image, func(t *testing.T) {
				if tc.requiresGIPK && !gipk {
					t.Skipf("server has no sql_generate_invisible_primary_key (added 8.0.30); cell not exercised")
				}
				// A fresh table per cell: the same shape is exercised under
				// each image independently, so one cell's DELETE can never
				// be the row another cell asserts on.
				tbl := tc.base + "_" + strings.ToLower(image)
				retable := func(stmt string) string { return strings.ReplaceAll(stmt, tc.base, tbl) }
				setGlobalRowImage(t, dsn, "FULL")
				setup := retable(tc.ddl) + ";" + retable(tc.seed) + ";"
				if tc.requiresGIPK {
					setup = "SET SESSION sql_generate_invisible_primary_key = ON;" + setup
				}
				applyMySQL(t, dsn, setup)

				rdr, err := eng.OpenCDCReader(ctx, dsn)
				if err != nil {
					t.Fatalf("OpenCDCReader: %v", err)
				}
				defer func() { _ = rdr.(*CDCReader).Close() }()
				changes, err := rdr.StreamChanges(ctx, ir.Position{})
				if err != nil {
					t.Fatalf("StreamChanges: %v", err)
				}
				time.Sleep(300 * time.Millisecond) // syncer registration boundary

				stmts := []string{retable(tc.del)}
				if image != "FULL" {
					stmts = append([]string{"SET SESSION binlog_row_image = '" + image + "'"}, stmts...)
				}
				execOnPinnedConn(t, ctx, dsn, stmts...)

				if tc.wantRefusal {
					// Drain to close and assert the coded refusal. A shape
					// whose identity cannot be carried must stop the stream,
					// never emit a change the applier can only mis-apply.
					deadline := time.After(45 * time.Second)
					for open := true; open; {
						select {
						case c, more := <-changes:
							open = more
							if more {
								if del, isDel := c.(ir.Delete); isDel {
									t.Fatalf("the stream EMITTED a delete for a generated-PK table instead of "+
										"refusing: Before=%#v. Under %s that before-image narrows to a key "+
										"with no value, and the apply renders an empty WHERE (hard error) or "+
										"`g IS NULL` (matches nothing, target row survives, position advances)",
										del.Before, image)
								}
							}
						case <-deadline:
							t.Fatal("stream did not stop within 45s")
						}
					}
					streamErr := rdr.(*CDCReader).Err()
					ce, ok := sluicecode.FromError(streamErr)
					if !ok || ce.Code != sluicecode.CodeCDCGeneratedPrimaryKey {
						t.Fatalf("want %s under %s; got %T: %v",
							sluicecode.CodeCDCGeneratedPrimaryKey, image, streamErr, streamErr)
					}
					t.Logf("PROVEN refused under %s: %v", image, streamErr)
					graded++
					return
				}

				got := drainChanges(t, ctx, changes, 1, 45*time.Second)
				if len(got) != 1 {
					if streamErr := rdr.(*CDCReader).Err(); streamErr != nil {
						t.Fatalf("the DELETE was refused instead of replayed under %s. This arm skips the Bug 193 "+
							"belt for a keyed table, so a refusal here means the PK is not visible to "+
							"loadPrimaryKeyDB for this shape and the PK-less fallback took over: %v",
							image, streamErr)
					}
					t.Fatalf("got %d changes under %s; want 1", len(got), image)
				}
				del, ok := got[0].(ir.Delete)
				if !ok {
					t.Fatalf("change[0] = %T; want ir.Delete", got[0])
				}

				// The assertion the premise owes: every PK column present,
				// non-nil, and equal to the value the row actually held.
				for col, want := range tc.wantPK {
					raw, present := del.Before[col]
					if !present {
						t.Fatalf("delete.Before is missing PK column %q under binlog_row_image=%s: %#v. "+
							"filterBeforeToPK copies a missing PK through as nil, the applier renders "+
							"`%s IS NULL`, the DELETE matches zero rows, and ADR-0010 absorbs the miss while "+
							"the position advances — the target keeps a row the source deleted, silently",
							col, image, del.Before, col)
					}
					if raw == nil {
						t.Fatalf("delete.Before[%q] is nil under binlog_row_image=%s — the server omitted a "+
							"PRIMARY KEY column from the before-image, which cdc_reader.go's DELETE arm calls "+
							"structurally impossible and skips its partial-image belt on. Full before-image: %#v",
							col, image, del.Before)
					}
					if !sameCarriedValue(raw, want) {
						t.Errorf("delete.Before[%q] = %#v (%T) under %s; want %#v (%T)",
							col, raw, raw, image, want, want)
					}
				}
				// The narrowing itself: nothing but the PK survives, which
				// is what makes a partial image safe on this arm.
				if len(del.Before) != len(tc.wantPK) {
					t.Errorf("delete.Before carries %d columns under %s (%#v); want exactly the %d PK columns. "+
						"A non-PK column that survived the narrowing renders `col IS NULL` when the image "+
						"omitted it — Bug 88 all over again",
						len(del.Before), image, del.Before, len(tc.wantPK))
				}
				graded++
			})
		}
	}

	// Anti-vacuity floor. Two axes, and a skip on one cell must not quietly
	// empty the matrix: 7 shapes × 3 images = 21, minus at most the 3 GIPK
	// cells on an older server.
	if graded < 18 {
		t.Errorf("graded only %d matrix cells; want at least 18 (7 PK shapes × 3 row images, allowing the 3 "+
			"generated-invisible-PK cells to skip on a pre-8.0.30 server). A matrix that skipped itself is "+
			"not a family pin", graded)
	}
}

// serverSupportsGIPK reports whether the server has
// sql_generate_invisible_primary_key (MySQL 8.0.30+; absent on MariaDB).
func serverSupportsGIPK(t *testing.T, dsn string) bool {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var name, value string
	err = db.QueryRowContext(ctx, "SHOW VARIABLES LIKE 'sql_generate_invisible_primary_key'").Scan(&name, &value)
	return err == nil
}

// execOnPinnedConn runs stmts in order on ONE pinned writer connection —
// the vehicle for the session-scoped binlog_row_image override, which is
// what reaches the belt-skipped arm (a GLOBAL setting is refused at the
// preflight by design).
func execOnPinnedConn(t *testing.T, ctx context.Context, dsn string, stmts ...string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("pin writer conn: %v", err)
	}
	defer func() { _ = conn.Close() }()
	for _, stmt := range stmts {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("writer session %q: %v", stmt, err)
		}
	}
}

// sameCarriedValue compares a decoded before-image value against the
// expected one, tolerating the int-width and []byte/string forms the
// decoder legitimately produces.
func sameCarriedValue(got, want any) bool {
	if g, ok := toInt64(got); ok {
		if w, ok2 := toInt64(want); ok2 {
			return g == w
		}
	}
	gb, gok := carriedBytes(got)
	wb, wok := carriedBytes(want)
	if gok && wok {
		return string(gb) == string(wb)
	}
	return fmt.Sprintf("%v", got) == fmt.Sprintf("%v", want)
}

func carriedBytes(v any) ([]byte, bool) {
	switch b := v.(type) {
	case []byte:
		return b, true
	case string:
		return []byte(b), true
	default:
		return nil, false
	}
}
