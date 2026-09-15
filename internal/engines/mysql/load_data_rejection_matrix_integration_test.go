//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Audit 2026-09-15 A0915-MYSQL-MEDIUM-1. rowSkippingWarningCodes is
// documented as "the codes that mean the server DROPPED the row", and it
// held two of at least four: a FOREIGN KEY violation (1452) and a row no
// PARTITION admits (1526) both drop the row under LOAD DATA LOCAL and
// were classified as coercions. The set is now DERIVED by this file: for
// every rejection family, on every server this engine ships against, load
// a violating row and assert that a landed-row shortfall is explained by
// a code the writer classifies. A code a server adds tomorrow is a red
// build here, not a row silently reclassified as a clamp.
//
// The second half — the replay exemption the shortfall witness carried —
// is driven through the real writer on the FOREIGN KEY family, in both
// transient shapes item 114 models, so the operator-visible outcome is
// pinned on a real server rather than derived from the pure function.

package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

// rejectionFamily is one way a target can refuse a row LOAD DATA LOCAL
// sends it. ddl creates the table (and any parent it needs) under a
// name; payload is three rows of which exactly one violates.
type rejectionFamily struct {
	name string
	ddl  func(table string) string
	// payload is TSV for the writer's (id, v) statement shape.
	payload string
	// unique marks the family whose drop code is the duplicate-key code
	// the replay accounting owns rather than a member of the skip set.
	unique bool
}

var rejectionFamilies = []rejectionFamily{
	{
		name: "CHECK",
		ddl: func(t string) string {
			return fmt.Sprintf(`CREATE TABLE %s (id BIGINT NOT NULL, v VARCHAR(16) NULL, PRIMARY KEY (id),
				CONSTRAINT %s_nonneg CHECK (id >= 0)) ENGINE=InnoDB;`, t, t)
		},
		payload: "1\ta\n-1\tb\n3\tc\n",
	},
	{
		name: "FOREIGN KEY",
		ddl: func(t string) string {
			return fmt.Sprintf(`CREATE TABLE %s_parent (id BIGINT NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB;
				INSERT INTO %s_parent (id) VALUES (1), (3);
				CREATE TABLE %s (id BIGINT NOT NULL, v VARCHAR(16) NULL, PRIMARY KEY (id),
				CONSTRAINT %s_fk FOREIGN KEY (id) REFERENCES %s_parent (id)) ENGINE=InnoDB;`, t, t, t, t, t)
		},
		payload: "1\ta\n2\tb\n3\tc\n",
	},
	{
		name: "PARTITION range",
		ddl: func(t string) string {
			return fmt.Sprintf(`CREATE TABLE %s (id BIGINT NOT NULL, v VARCHAR(16) NULL, PRIMARY KEY (id)) ENGINE=InnoDB
				PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10));`, t)
		},
		payload: "1\ta\n50\tb\n3\tc\n",
	},
	{
		name: "UNIQUE",
		ddl: func(t string) string {
			return fmt.Sprintf(`CREATE TABLE %s (id BIGINT NOT NULL, v VARCHAR(16) NULL, PRIMARY KEY (id),
				UNIQUE KEY %s_v (v)) ENGINE=InnoDB;`, t, t)
		},
		payload: "1\ta\n2\ta\n3\tc\n",
		unique:  true,
	},
}

// loadWithCodes runs one LOAD DATA LOCAL in the writer's own statement
// form under the given session sql_mode and returns the server's
// rows-inserted plus every SHOW WARNINGS code, in order. It observes the
// server directly — no writer policy in the loop.
func loadWithCodes(ctx context.Context, t *testing.T, db *sql.DB, sqlMode, table, payload string) (affected int64, codes []string) {
	t.Helper()
	name, err := mintReaderName()
	if err != nil {
		t.Fatalf("mintReaderName: %v", err)
	}
	driver.RegisterReaderHandler(name, func() io.Reader { return bytes.NewReader([]byte(payload)) })
	defer driver.DeregisterReaderHandler(name)

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("pin conn: %v", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "SET SESSION sql_mode = ?", sqlMode); err != nil {
		t.Fatalf("SET SESSION sql_mode=%q: %v", sqlMode, err)
	}
	cols := []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "v", Type: ir.Varchar{Length: 16}},
	}
	res, err := conn.ExecContext(ctx, buildLoadDataStmt("", table, cols, name))
	if err != nil {
		t.Fatalf("LOAD DATA into %s: %v", table, err)
	}
	affected, _ = res.RowsAffected()
	rows, err := conn.QueryContext(ctx, "SHOW WARNINGS")
	if err != nil {
		t.Fatalf("SHOW WARNINGS: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var level, code, msg string
		if err := rows.Scan(&level, &code, &msg); err != nil {
			t.Fatalf("scan warning: %v", err)
		}
		codes = append(codes, code)
		t.Logf("%s: %s %s: %s", table, level, code, msg)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate warnings: %v", err)
	}
	return affected, codes
}

// rejectionServer is one server the matrix runs on. dsn must be a root
// DSN on a server with local_infile enabled.
type rejectionServer struct {
	name  string
	start func(t *testing.T) (dsn string, cleanup func())
}

var rejectionServers = []rejectionServer{
	{"mysql:8.0", func(t *testing.T) (string, func()) {
		dsn, cleanup := startMySQL(t)
		enableLocalInfile(t, dsn)
		return dsn, cleanup
	}},
	{"mysql:8.4", func(t *testing.T) (string, func()) {
		return startMySQLM2PreflightImage(t, "mysql:8.4", "--local-infile=1")
	}},
	{"mariadb:11", func(t *testing.T) (string, func()) {
		return newMariaDBDedicatedForCDC(t, mariadb114Image, "--local-infile=1")
	}},
}

// TestLoadDataLocal_RejectionFamilyCodesAreClassified is the derivation
// behind rowSkippingWarningCodes: {CHECK, FOREIGN KEY, PARTITION range,
// UNIQUE} × {mysql:8.0, mysql:8.4, mariadb:11} × {strict, relaxed
// sql_mode}, each cell loading three rows of which one violates, and
// asserting
//
//	rows landed < rows sent  ⟹  every observed code is one the writer
//	                            classifies (the skip set, or 1062 for the
//	                            UNIQUE family the replay accounting owns)
//
// plus the anti-vacuity half: the server must actually have dropped the
// row and warned, or the cell proves nothing about classification.
func TestLoadDataLocal_RejectionFamilyCodesAreClassified(t *testing.T) {
	for _, srv := range rejectionServers {
		t.Run(srv.name, func(t *testing.T) {
			dsn, cleanup := srv.start(t)
			defer cleanup()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			db := openTestDB(t, dsn)
			defer db.Close()

			for _, fam := range rejectionFamilies {
				for modeName, mode := range map[string]string{"strict": "STRICT_ALL_TABLES", "relaxed": ""} {
					t.Run(fam.name+"/"+modeName, func(t *testing.T) {
						table := "rej_" + strings.ToLower(strings.Fields(fam.name)[0]) + "_" + modeName
						applyDDL(t, dsn, fam.ddl(table))
						affected, codes := loadWithCodes(ctx, t, db, mode, table, fam.payload)

						// Anti-vacuity: the premise of the whole set is that the
						// server DROPS the row and WARNS. A cell where it errored
						// instead, or landed all three, is a changed premise.
						if affected >= 3 {
							t.Fatalf("%s/%s/%s: all 3 rows landed — the server no longer rejects this family under "+
								"LOAD DATA LOCAL, so the premise rowSkippingWarningCodes rests on has changed here",
								srv.name, fam.name, modeName)
						}
						if len(codes) == 0 {
							t.Fatalf("%s/%s/%s: %d of 3 rows landed with NO warning — a drop the writer cannot see at "+
								"all; the shortfall witness is the only thing left", srv.name, fam.name, modeName, affected)
						}
						classified := 0
						for _, code := range codes {
							switch {
							case fam.unique && code == mysqlDupKeyWarningCode:
								classified++
							case !fam.unique && rowSkippingWarningCodes[code]:
								classified++
							case !fam.unique && code == mysqlDupKeyWarningCode:
								t.Errorf("%s/%s/%s: the server reported the drop as duplicate-key %s — the replay "+
									"accounting would tolerate this family as an already-landed row", srv.name, fam.name, modeName, code)
							default:
								t.Errorf("%s/%s/%s: %d of 3 rows landed and the server reported code %s, which "+
									"rowSkippingWarningCodes does not hold. Under --mysql-sql-mode='' the writer would "+
									"WARN this as a clamped value and exit 0 short of a row; under strict mode it would "+
									"refuse with a --type-override remedy. Add the code to the set (load_data_writer.go) "+
									"and name the family in rowsSkippedError.", srv.name, fam.name, modeName, affected, code)
							}
						}
						if classified == 0 {
							t.Errorf("%s/%s/%s: no observed code (%v) is classified", srv.name, fam.name, modeName, codes)
						}
						t.Logf("%s/%s/%s: landed %d of 3, codes %v", srv.name, fam.name, modeName, affected, codes)
					})
				}
			}
		})
	}
}

// TestRowWriter_LoadData_ForeignKeySkipRefusesOnReplay is the audit's
// failing input through the real writer on a real mysql:8.0: a target
// that carries a FOREIGN KEY during the copy (the --schema-already-applied
// shape), a segment whose one orphan row the server drops with 1452, and
// a classified transient that forces the segment to be REPLAYED — in both
// shapes item 114 models (rolled back before exec; committed but unacked
// after exec) and both sql_modes. Before the fix the replay's shortfall
// witness was disarmed and 1452 was not a skip code, so under
// --mysql-sql-mode=” this WARNed "clamped or truncated" and returned nil
// with 2 of 3 rows on the target.
func TestRowWriter_LoadData_ForeignKeySkipRefusesOnReplay(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()
	enableLocalInfile(t, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	relaxed, strict := "", "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"
	for _, cell := range []struct {
		name  string
		mode  *string
		phase loadDataSegmentPhase
	}{
		{"relaxed_rolled_back_attempt", &relaxed, loadDataBeforeExec},
		{"relaxed_committed_but_unacked", &relaxed, loadDataAfterExec},
		{"strict_rolled_back_attempt", &strict, loadDataBeforeExec},
		{"strict_committed_but_unacked", &strict, loadDataAfterExec},
	} {
		t.Run(cell.name, func(t *testing.T) {
			table := "fkr_" + cell.name
			applyDDL(t, dsn, rejectionFamilies[1].ddl(table))
			irTable := readPinTable(ctx, t, dsn, table)

			withFastReparentBackoff(t, 12)
			replays := 0
			loadDataSegmentFailHookForTest = func(_ int, replay bool, phase loadDataSegmentPhase) error {
				if replay {
					// The hook fires at BOTH phases of every attempt; count
					// the replay once, at its post-exec phase.
					if phase == loadDataAfterExec {
						replays++
					}
					return nil
				}
				if phase == cell.phase {
					return vttabletUnavailable()
				}
				return nil
			}
			defer func() { loadDataSegmentFailHookForTest = nil }()

			db, err := openDB(ctx, mustParseDSN(t, dsn), cell.mode)
			if err != nil {
				t.Fatalf("openDB: %v", err)
			}
			defer db.Close()
			w := &RowWriter{db: db, sqlMode: cell.mode, bulkLoad: ir.BulkLoadLoadDataInfile}
			in := make(chan ir.Row, 3)
			for _, r := range []ir.Row{{"id": int64(1), "v": "a"}, {"id": int64(2), "v": "orphan"}, {"id": int64(3), "v": "c"}} {
				in <- r
			}
			close(in)
			err = w.WriteRows(ctx, irTable, in)
			if replays != 1 {
				t.Fatalf("%s: the segment was replayed %d times; want exactly 1 — without a replay this cell "+
					"grades the first-attempt witness, not the replay accounting", cell.name, replays)
			}
			if err == nil {
				t.Fatalf("%s: WriteRows returned nil with the orphan row dropped (target holds %d of 3) — the "+
					"replay's FK skip was accepted (audit 2026-09-15 A0915-MYSQL-MEDIUM-1)", cell.name, countRowsIn(t, ctx, dsn, table))
			}
			if errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("%s: WriteRows timed out: %v", cell.name, err)
			}
			for _, want := range []string{loadDataRowsSkippedMarker, "SKIPPED 1 row", "1452"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("%s: refusal does not say %q: %v", cell.name, want, err)
				}
			}
			if strings.Contains(err.Error(), "--type-override") {
				t.Errorf("%s: refusal prescribes --type-override, which cannot address a FOREIGN KEY: %v", cell.name, err)
			}
			// The independent number: the FK cannot have admitted the orphan.
			if n := countRowsIn(t, ctx, dsn, table); n != 2 {
				t.Fatalf("%s: target holds %d rows; want 2 (the two rows with a parent, exactly once)", cell.name, n)
			}
		})
	}
}
