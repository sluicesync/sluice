// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"testing"
)

// Audit 2026-09-15 A0915-MYSQL-MEDIUM-1: rowSkippingWarningCodes
// held the two CHECK codes and nothing else, and the shortfall witness was
// disarmed on a replay. So a FOREIGN KEY (1452) or PARTITION (1526) skip
// on a replayed segment reached the coercion policy — refused with a
// --type-override remedy under strict mode, WARNed past as "clamped
// values" at exit 0 under --mysql-sql-mode=''.
//
// Two fixes, two pins. The set now holds the codes the real-server matrix
// (TestLoadDataLocal_RejectionFamilyCodesAreClassified) measures; this file
// pins that a member of the set refuses in every sql_mode. And the replay
// witness now subtracts only the ACCOUNTED duplicates; unexplainedShortfall
// is pure and pinned here across its whole decision matrix, then driven
// through the real reportLoadDataWarnings over a fake session so the belt
// for a code NOBODY has listed is proven live.

func TestDecideBulkWriteWarnings_ForeignKeyAndPartitionSkipsRefuseInEverySQLMode(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	relaxed, strict := "", "STRICT_TRANS_TABLES"

	for _, family := range []struct {
		name, code, detail, remedyWord string
	}{
		{"FOREIGN KEY", "1452", "Warning 1452: Cannot add or update a child row: a foreign key constraint fails", "FOREIGN KEY"},
		{"PARTITION", "1526", "Warning 1526: Table has no partition for value 50", "partition"},
	} {
		for name, mode := range map[string]*string{"relaxed": &relaxed, "strict": &strict, "default": nil} {
			t.Run(family.name+"/"+name, func(t *testing.T) {
				t.Parallel()
				// readShowWarnings is what fills Skipped from the code; drive it
				// through the same classification rather than hand-setting the
				// field, so a code dropped from the set fails HERE.
				sw := showWarnings{Visible: 1, NonDup: 1, Details: []string{family.detail}}
				if rowSkippingWarningCodes[family.code] {
					sw.Skipped = 1
				}
				w := &RowWriter{sqlMode: mode}
				err := w.decideBulkWriteWarnings(ctx, "child", sw, 1)
				if err == nil {
					t.Fatalf("%s %s: a %s skip was accepted — under --mysql-sql-mode='' that is the relaxed "+
						"WARN-and-continue path reporting a DROPPED row as a coerced value at exit 0 (audit "+
						"2026-09-15 A0915-MYSQL-MEDIUM-1)", family.name, name, family.code)
				}
				for _, want := range []string{loadDataRowsSkippedMarker, "SKIPPED 1 row", family.code, family.remedyWord} {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("%s %s: refusal does not say %q: %v", family.name, name, want, err)
					}
				}
				if strings.Contains(err.Error(), "--type-override") {
					t.Errorf("%s %s: refusal prescribes --type-override, which cannot address a %s: %v",
						family.name, name, family.name, err)
				}
			})
		}
	}
}

// TestUnexplainedShortfall is the matrix behind the replay half of the
// fix. The conservative direction is "refuse" everywhere the list is
// complete; the one deliberate abstention (a capped list on a replay) is
// pinned as an abstention so it cannot quietly become a tolerance.
func TestUnexplainedShortfall(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name         string
		segRows      int
		inserted     int64
		replay       bool
		listComplete bool
		visibleDups  int
		want         int64
	}{
		{"first attempt: every missing row is a drop", 3, 2, false, true, 0, 1},
		{"first attempt: a capped list changes nothing", 3, 1, false, false, 0, 2},
		{"first attempt: nothing missing", 3, 3, false, true, 0, 0},
		{"replay: shortfall fully explained by duplicates", 3, 0, true, true, 3, 0},
		{"replay: partial prior commit, remainder inserted", 5000, 1200, true, true, 3800, 0},
		// The crux — the audit's shape. Two rows landed before, one row the
		// server dropped for a reason the duplicates cannot explain.
		{"replay: one row beyond the duplicates is a drop", 3, 0, true, true, 2, 1},
		{"replay: no duplicates at all, every missing row is a drop", 3, 2, true, true, 0, 1},
		{"replay: a truncated-AND-duplicate row is still one duplicate", 3, 1, true, true, 2, 0},
		// A capped list cannot count its duplicates; the witness abstains and
		// the caller's capped-list arm decides (loud under strict).
		{"replay: capped list abstains", 5000, 0, true, false, 1024, 0},
		{"unknown rows-inserted is not evidence", 3, -1, true, true, 0, 0},
		{"server inserted more rows than sent", 3, 4, false, true, 0, 0},
	}
	sawRefuse, sawTolerate := false, false
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := unexplainedShortfall(tc.segRows, tc.inserted, tc.replay, tc.listComplete, tc.visibleDups)
			if got != tc.want {
				t.Errorf("unexplainedShortfall(%d, %d, replay=%v, complete=%v, dups=%d) = %d; want %d",
					tc.segRows, tc.inserted, tc.replay, tc.listComplete, tc.visibleDups, got, tc.want)
			}
			if got > 0 {
				sawRefuse = true
			} else {
				sawTolerate = true
			}
		})
	}
	if !sawRefuse || !sawTolerate {
		t.Errorf("matrix produced only one verdict (refuse=%v tolerate=%v)", sawRefuse, sawTolerate)
	}
}

// warningsFakeDriver answers the two post-load probes from its DSN —
// "codes=1062,1062,9999" for SHOW WARNINGS (one row per code, in order)
// and "count=N" for SELECT @@warning_count (defaults to the number of
// codes; set lower to model a capped list, i.e. count > visible).
// Everything else errors, so the writer cannot quietly run a statement
// the fake does not model.
type warningsFakeDriver struct{}

type warningsFakeConn struct {
	codes []string
	count int64
}

func (warningsFakeDriver) Open(dsn string) (driver.Conn, error) {
	c := &warningsFakeConn{}
	for _, kv := range strings.Split(dsn, "|") {
		k, v, _ := strings.Cut(kv, "=")
		switch k {
		case "codes":
			if v != "" {
				c.codes = strings.Split(v, ",")
			}
			c.count = int64(len(c.codes))
		case "count":
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, err
			}
			c.count = n
		}
	}
	return c, nil
}

func (c *warningsFakeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected Prepare")
}
func (c *warningsFakeConn) Close() error              { return nil }
func (c *warningsFakeConn) Begin() (driver.Tx, error) { return nil, errors.New("unexpected Begin") }

func (c *warningsFakeConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	switch query {
	case "SHOW WARNINGS":
		rows := &warningsFakeRows{cols: []string{"Level", "Code", "Message"}}
		for _, code := range c.codes {
			rows.vals = append(rows.vals, []driver.Value{"Warning", code, "fake warning " + code})
		}
		return rows, nil
	case "SELECT @@warning_count":
		return &warningsFakeRows{cols: []string{"@@warning_count"}, vals: [][]driver.Value{{c.count}}}, nil
	}
	return nil, errors.New("unexpected query: " + query)
}

type warningsFakeRows struct {
	cols []string
	vals [][]driver.Value
	next int
}

func (r *warningsFakeRows) Columns() []string { return r.cols }
func (r *warningsFakeRows) Close() error      { return nil }
func (r *warningsFakeRows) Next(dest []driver.Value) error {
	if r.next >= len(r.vals) {
		return io.EOF
	}
	copy(dest, r.vals[r.next])
	r.next++
	return nil
}

func newWarningsFakeConn(t *testing.T, spec string) *sql.Conn {
	t.Helper()
	name := "sluice-warnings-fake"
	registered := false
	for _, d := range sql.Drivers() {
		if d == name {
			registered = true
		}
	}
	if !registered {
		sql.Register(name, warningsFakeDriver{})
	}
	db, err := sql.Open(name, spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// TestReportLoadDataWarnings_ReplayShortfallBeyondDuplicatesRefuses drives
// the real replay decision over a fake session, with a drop code NO set
// holds (9999). That isolates the belt from the set: a server that adds a
// rejection code tomorrow is caught by the shortfall arithmetic today,
// before the matrix test has had a chance to name it. The legitimate
// replay and the capped-list abstention are pinned alongside so the belt
// cannot be satisfied by refusing every replay.
func TestReportLoadDataWarnings_ReplayShortfallBeyondDuplicatesRefuses(t *testing.T) {
	// No t.Parallel: the relaxed-mode arm swaps the global slog default.
	ctx := context.Background()
	relaxed, strict := "", "STRICT_TRANS_TABLES"

	t.Run("a replay whose shortfall exceeds its duplicates refuses as SKIPPED, every sql_mode", func(t *testing.T) {
		for name, mode := range map[string]*string{"relaxed": &relaxed, "strict": &strict} {
			w := &RowWriter{sqlMode: mode}
			// Three rows re-sent; two landed before (1062 each), one the
			// server dropped under a code this build has never heard of.
			conn := newWarningsFakeConn(t, "codes=1062,1062,9999")
			err := w.reportLoadDataWarnings(ctx, conn, "child", 3, 0, true)
			if err == nil {
				t.Fatalf("%s: a replayed segment short one row beyond its duplicates was accepted — this is the "+
					"blanket replay exemption audit 2026-09-15 A0915-MYSQL-MEDIUM-1 removed", name)
			}
			for _, want := range []string{loadDataRowsSkippedMarker, "SKIPPED 1 row", "9999"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("%s: refusal does not say %q: %v", name, want, err)
				}
			}
			if strings.Contains(err.Error(), "--type-override") {
				t.Errorf("%s: refusal prescribes --type-override for a dropped row: %v", name, err)
			}
		}
	})

	t.Run("a replay whose shortfall IS its duplicates still converges", func(t *testing.T) {
		var buf bytes.Buffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
		defer slog.SetDefault(prev)

		w := &RowWriter{sqlMode: &strict}
		conn := newWarningsFakeConn(t, "codes=1062,1062,1062")
		if err := w.reportLoadDataWarnings(ctx, conn, "t", 3, 0, true); err != nil {
			t.Fatalf("a byte-identical replay of a fully-committed segment must be tolerated: %v", err)
		}
		if !strings.Contains(buf.String(), "landed exactly once") {
			t.Errorf("the tolerated replay did not log its accounting:\n%s", buf.String())
		}
	})

	t.Run("a replay over a CAPPED list abstains from the witness and stays loud under strict", func(t *testing.T) {
		// 1024 visible duplicates, 3000 true warnings: the witness cannot
		// count the hidden duplicates, so it must not call the 1976-row
		// shortfall a drop — and the capped-list arm refuses under strict,
		// as before, rather than tolerating blind.
		codes := strings.TrimSuffix(strings.Repeat("1062,", 1024), ",")
		w := &RowWriter{sqlMode: &strict}
		conn := newWarningsFakeConn(t, "codes="+codes+"|count=3000")
		err := w.reportLoadDataWarnings(ctx, conn, "t", 3000, 0, true)
		if err == nil {
			t.Fatal("a capped replay list under strict mode must refuse (the hidden warnings cannot be classified)")
		}
		if strings.Contains(err.Error(), loadDataRowsSkippedMarker) {
			t.Errorf("a capped replay was refused as SKIPPED rows — the witness subtracted only the VISIBLE "+
				"duplicates and overstated the drop; it must abstain when the list is incomplete: %v", err)
		}
	})
}
