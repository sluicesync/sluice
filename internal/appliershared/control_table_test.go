// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// These tests pin the SHARED control-table CRUD's control flow at the
// seam level (ADR-0081 tier c): the ErrNoRows / missing-table
// tolerance ladders, the error-shape wrapping (byte-identical to the
// pre-extraction per-engine helpers), the RowsAffectedIsChangedRows
// divergence (BOTH branches — the changed-rows existence-probe shape
// and the matched-rows zero-rows check), the ListStreamsFallback
// consult rules, and the 9-column lease-row scan projection. The
// engine packages keep their own behaviour oracles — the
// control_table_integration_test.go suites ×2 run the same flows
// against real databases and must pass unchanged across the
// extraction.
//
// Mechanism: a scripted database/sql fake driver answers each
// statement from a FIFO of steps (rows, a result, or an error), so
// every tolerance branch is reachable without a database and the
// statements' arrival order is assertable.

// errMissingTable is the scripted stand-in for the dialect's
// missing-control-table error (MySQL 1146 / PG 42P01); the test
// config's IsMissingTable classifier matches it by substring, the
// same shape the engines' classifiers use.
var errMissingTable = errors.New("MISSING_TABLE: relation is not there")

type ctStep struct {
	rows   *ctRows
	result driver.Result
	err    error
}

// ctConn is a minimal scripted driver connection: QueryContext and
// ExecContext both pop the next step; the statements seen are
// recorded for order assertions. Single test goroutine — no locking.
type ctConn struct {
	steps *[]ctStep
	seen  *[]string
}

func (c ctConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("appliershared: ct fake conn has no statements")
}
func (c ctConn) Close() error { return nil }
func (c ctConn) Begin() (driver.Tx, error) {
	return nil, errors.New("appliershared: ct fake conn has no tx")
}

func (c ctConn) pop(query string) (ctStep, error) {
	*c.seen = append(*c.seen, query)
	if len(*c.steps) == 0 {
		return ctStep{}, errors.New("appliershared: ct fake conn script exhausted")
	}
	s := (*c.steps)[0]
	*c.steps = (*c.steps)[1:]
	return s, nil
}

func (c ctConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	s, err := c.pop(query)
	if err != nil {
		return nil, err
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.rows, nil
}

func (c ctConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	s, err := c.pop(query)
	if err != nil {
		return nil, err
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.result, nil
}

type ctConnector struct{ conn ctConn }

func (c ctConnector) Connect(context.Context) (driver.Conn, error) { return c.conn, nil }
func (c ctConnector) Driver() driver.Driver                        { return ctDriver{} }

type ctDriver struct{}

func (ctDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("appliershared: ct fake driver opens via connector only")
}

type ctRows struct {
	cols []string
	data [][]driver.Value
	i    int
}

func (r *ctRows) Columns() []string { return r.cols }
func (r *ctRows) Close() error      { return nil }
func (r *ctRows) Next(dest []driver.Value) error {
	if r.i >= len(r.data) {
		return io.EOF
	}
	copy(dest, r.data[r.i])
	r.i++
	return nil
}

type ctResult struct {
	n   int64
	err error
}

func (r ctResult) LastInsertId() (int64, error) { return 0, nil }
func (r ctResult) RowsAffected() (int64, error) { return r.n, r.err }

// ctFixture opens a scripted DB and returns it with the seam config
// and the recorded-statement slice.
func ctFixture(t *testing.T, steps []ctStep) (*sql.DB, *ControlTableConfig, *[]string) {
	t.Helper()
	seen := &[]string{}
	db := sql.OpenDB(ctConnector{conn: ctConn{steps: &steps, seen: seen}})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	cfg := &ControlTableConfig{
		EngineName: "fake",
		IsMissingTable: func(err error) bool {
			return err != nil && strings.Contains(err.Error(), "MISSING_TABLE")
		},
		ErrStreamNotFound: errors.New("fake: stream not found"),
	}
	return db, cfg, seen
}

func TestReadPosition_Shapes(t *testing.T) {
	ctx := context.Background()
	posCols := []string{"source_position"}

	t.Run("found", func(t *testing.T) {
		db, cfg, _ := ctFixture(t, []ctStep{{rows: &ctRows{cols: posCols, data: [][]driver.Value{{"tok-1"}}}}})
		token, ok, err := ReadPosition(ctx, db, cfg, "Q", "s1")
		if err != nil || !ok || token != "tok-1" {
			t.Fatalf("got (%q, %v, %v); want (tok-1, true, nil)", token, ok, err)
		}
	})
	t.Run("no row", func(t *testing.T) {
		db, cfg, _ := ctFixture(t, []ctStep{{rows: &ctRows{cols: posCols}}})
		_, ok, err := ReadPosition(ctx, db, cfg, "Q", "s1")
		if err != nil || ok {
			t.Fatalf("got (ok=%v, err=%v); want (false, nil)", ok, err)
		}
	})
	t.Run("missing table tolerated", func(t *testing.T) {
		db, cfg, _ := ctFixture(t, []ctStep{{err: errMissingTable}})
		_, ok, err := ReadPosition(ctx, db, cfg, "Q", "s1")
		if err != nil || ok {
			t.Fatalf("got (ok=%v, err=%v); want (false, nil)", ok, err)
		}
	})
	t.Run("other error wrapped with engine prefix", func(t *testing.T) {
		db, cfg, _ := ctFixture(t, []ctStep{{err: errors.New("boom")}})
		_, _, err := ReadPosition(ctx, db, cfg, "Q", "s1")
		if err == nil || err.Error() != "fake: read position: boom" {
			t.Fatalf("err = %v; want fake: read position: boom", err)
		}
	})
}

func TestReadStopRequested_Shapes(t *testing.T) {
	ctx := context.Background()
	cols := []string{"flag"}

	t.Run("flag set", func(t *testing.T) {
		db, cfg, _ := ctFixture(t, []ctStep{{rows: &ctRows{cols: cols, data: [][]driver.Value{{true}}}}})
		got, err := ReadStopRequested(ctx, db, cfg, "Q", "s1")
		if err != nil || !got {
			t.Fatalf("got (%v, %v); want (true, nil)", got, err)
		}
	})
	t.Run("no row means no stop", func(t *testing.T) {
		db, cfg, _ := ctFixture(t, []ctStep{{rows: &ctRows{cols: cols}}})
		got, err := ReadStopRequested(ctx, db, cfg, "Q", "s1")
		if err != nil || got {
			t.Fatalf("got (%v, %v); want (false, nil)", got, err)
		}
	})
	t.Run("missing table tolerated", func(t *testing.T) {
		db, cfg, _ := ctFixture(t, []ctStep{{err: errMissingTable}})
		got, err := ReadStopRequested(ctx, db, cfg, "Q", "s1")
		if err != nil || got {
			t.Fatalf("got (%v, %v); want (false, nil)", got, err)
		}
	})
	t.Run("other error wrapped", func(t *testing.T) {
		db, cfg, _ := ctFixture(t, []ctStep{{err: errors.New("boom")}})
		_, err := ReadStopRequested(ctx, db, cfg, "Q", "s1")
		if err == nil || err.Error() != "fake: read stop flag: boom" {
			t.Fatalf("err = %v; want fake: read stop flag: boom", err)
		}
	})
}

func streamCols() []string {
	return []string{"stream_id", "source_position", "updated_at", "slot_name", "source_dsn_fingerprint", "target_schema"}
}

func TestListStreams_ScanAndEngineStamp(t *testing.T) {
	updated := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	db, cfg, _ := ctFixture(t, []ctStep{{rows: &ctRows{
		cols: streamCols(),
		data: [][]driver.Value{{"s1", "tok-1", updated, "slot-a", "fp-a", "schema-a"}},
	}}})
	out, err := ListStreams(context.Background(), db, cfg, "Q", "engine-x")
	if err != nil {
		t.Fatalf("ListStreams: %v", err)
	}
	want := ir.StreamStatus{
		StreamID:             "s1",
		Position:             ir.Position{Engine: "engine-x", Token: "tok-1"},
		UpdatedAt:            updated,
		SlotName:             "slot-a",
		SourceDSNFingerprint: "fp-a",
		TargetSchema:         "schema-a",
	}
	if len(out) != 1 || out[0] != want {
		t.Fatalf("out = %+v; want [%+v]", out, want)
	}
}

func TestListStreams_MissingTableIsEmptyNotFallback(t *testing.T) {
	db, cfg, _ := ctFixture(t, []ctStep{{err: errMissingTable}})
	fallbackCalled := false
	cfg.ListStreamsFallback = func(context.Context, error) ([]ir.StreamStatus, bool, error) {
		fallbackCalled = true
		return nil, true, errors.New("must not be reached")
	}
	out, err := ListStreams(context.Background(), db, cfg, "Q", "e")
	if err != nil || out == nil || len(out) != 0 {
		t.Fatalf("got (%v, %v); want (empty non-nil, nil)", out, err)
	}
	if fallbackCalled {
		t.Fatal("ListStreamsFallback consulted on a missing-table error; missing-table tolerance must win first")
	}
}

func TestListStreams_FallbackHandledAndUnhandled(t *testing.T) {
	t.Run("handled", func(t *testing.T) {
		db, cfg, _ := ctFixture(t, []ctStep{{err: errors.New("Unknown column 'slot_name'")}})
		legacy := []ir.StreamStatus{{StreamID: "legacy-1"}}
		cfg.ListStreamsFallback = func(_ context.Context, queryErr error) ([]ir.StreamStatus, bool, error) {
			if !strings.Contains(queryErr.Error(), "Unknown column") {
				return nil, false, nil
			}
			return legacy, true, nil
		}
		out, err := ListStreams(context.Background(), db, cfg, "Q", "e")
		if err != nil || len(out) != 1 || out[0].StreamID != "legacy-1" {
			t.Fatalf("got (%+v, %v); want the legacy fallback result", out, err)
		}
	})
	t.Run("unhandled falls through to the wrapped error", func(t *testing.T) {
		db, cfg, _ := ctFixture(t, []ctStep{{err: errors.New("boom")}})
		cfg.ListStreamsFallback = func(context.Context, error) ([]ir.StreamStatus, bool, error) {
			return nil, false, nil
		}
		_, err := ListStreams(context.Background(), db, cfg, "Q", "e")
		if err == nil || err.Error() != "fake: list streams: boom" {
			t.Fatalf("err = %v; want fake: list streams: boom", err)
		}
	})
}

func TestListStreams_ScanErrorNeverFallsBack(t *testing.T) {
	// A scan error (here: a NULL stream_id into a string) must wrap as
	// "scan streams", NOT consult the fallback — pre-extraction, only
	// the *query* error path fell back to the legacy column set.
	db, cfg, _ := ctFixture(t, []ctStep{{rows: &ctRows{
		cols: streamCols(),
		data: [][]driver.Value{{nil, "tok", time.Now(), "", "", ""}},
	}}})
	fallbackCalled := false
	cfg.ListStreamsFallback = func(context.Context, error) ([]ir.StreamStatus, bool, error) {
		fallbackCalled = true
		return nil, true, nil
	}
	_, err := ListStreams(context.Background(), db, cfg, "Q", "e")
	if err == nil || !strings.HasPrefix(err.Error(), "fake: scan streams: ") {
		t.Fatalf("err = %v; want fake: scan streams: …", err)
	}
	if fallbackCalled {
		t.Fatal("ListStreamsFallback consulted on a scan error")
	}
}

func TestRequestStop_MatchedRows(t *testing.T) {
	ctx := context.Background()

	t.Run("row updated", func(t *testing.T) {
		db, cfg, seen := ctFixture(t, []ctStep{{result: ctResult{n: 1}}})
		if err := RequestStop(ctx, db, cfg, "", "UPDATE-Q", "s1"); err != nil {
			t.Fatalf("RequestStop: %v", err)
		}
		if len(*seen) != 1 || (*seen)[0] != "UPDATE-Q" {
			t.Fatalf("statements = %v; want exactly [UPDATE-Q] (no existence probe on matched-rows drivers)", *seen)
		}
	})
	t.Run("zero rows means stream not found", func(t *testing.T) {
		db, cfg, _ := ctFixture(t, []ctStep{{result: ctResult{n: 0}}})
		err := RequestStop(ctx, db, cfg, "", "UPDATE-Q", "s1")
		if !errors.Is(err, cfg.ErrStreamNotFound) {
			t.Fatalf("err = %v; want errors.Is ErrStreamNotFound", err)
		}
		if want := `fake: stream not found: "s1"`; err.Error() != want {
			t.Fatalf("err = %q; want %q", err, want)
		}
	})
	t.Run("exec error wrapped", func(t *testing.T) {
		db, cfg, _ := ctFixture(t, []ctStep{{err: errors.New("boom")}})
		err := RequestStop(ctx, db, cfg, "", "UPDATE-Q", "s1")
		if err == nil || err.Error() != "fake: request stop: boom" {
			t.Fatalf("err = %v; want fake: request stop: boom", err)
		}
	})
	t.Run("rows-affected error wrapped", func(t *testing.T) {
		db, cfg, _ := ctFixture(t, []ctStep{{result: ctResult{err: errors.New("boom")}}})
		err := RequestStop(ctx, db, cfg, "", "UPDATE-Q", "s1")
		if err == nil || err.Error() != "fake: request stop: rows affected: boom" {
			t.Fatalf("err = %v; want fake: request stop: rows affected: boom", err)
		}
	})
}

func TestRequestStop_ChangedRows(t *testing.T) {
	ctx := context.Background()
	oneCol := []string{"1"}

	t.Run("probe then update, RowsAffected ignored", func(t *testing.T) {
		// The changed-rows wart pin: the UPDATE reporting 0 rows
		// affected (same flag value rewritten) must still succeed —
		// the existence probe, not RowsAffected, decides not-found.
		db, cfg, seen := ctFixture(t, []ctStep{
			{rows: &ctRows{cols: oneCol, data: [][]driver.Value{{int64(1)}}}},
			{result: ctResult{n: 0}},
		})
		cfg.RowsAffectedIsChangedRows = true
		if err := RequestStop(ctx, db, cfg, "EXISTS-Q", "UPDATE-Q", "s1"); err != nil {
			t.Fatalf("RequestStop: %v", err)
		}
		if len(*seen) != 2 || (*seen)[0] != "EXISTS-Q" || (*seen)[1] != "UPDATE-Q" {
			t.Fatalf("statements = %v; want [EXISTS-Q UPDATE-Q]", *seen)
		}
	})
	t.Run("missing row surfaces the sentinel before any update", func(t *testing.T) {
		db, cfg, seen := ctFixture(t, []ctStep{{rows: &ctRows{cols: oneCol}}})
		cfg.RowsAffectedIsChangedRows = true
		err := RequestStop(ctx, db, cfg, "EXISTS-Q", "UPDATE-Q", "s1")
		if !errors.Is(err, cfg.ErrStreamNotFound) {
			t.Fatalf("err = %v; want errors.Is ErrStreamNotFound", err)
		}
		if len(*seen) != 1 {
			t.Fatalf("statements = %v; want the probe only", *seen)
		}
	})
	t.Run("probe error wrapped", func(t *testing.T) {
		db, cfg, _ := ctFixture(t, []ctStep{{err: errors.New("boom")}})
		cfg.RowsAffectedIsChangedRows = true
		err := RequestStop(ctx, db, cfg, "EXISTS-Q", "UPDATE-Q", "s1")
		if err == nil || err.Error() != "fake: request stop: existence check: boom" {
			t.Fatalf("err = %v; want fake: request stop: existence check: boom", err)
		}
	})
}

func TestTolerantExec_Shapes(t *testing.T) {
	ctx := context.Background()

	t.Run("ok", func(t *testing.T) {
		db, cfg, _ := ctFixture(t, []ctStep{{result: ctResult{n: 1}}})
		if err := TolerantExec(ctx, db, cfg, "clear stream", "Q", "s1"); err != nil {
			t.Fatalf("TolerantExec: %v", err)
		}
	})
	t.Run("missing table tolerated", func(t *testing.T) {
		db, cfg, _ := ctFixture(t, []ctStep{{err: errMissingTable}})
		if err := TolerantExec(ctx, db, cfg, "clear stop signal", "Q", "s1"); err != nil {
			t.Fatalf("TolerantExec: %v", err)
		}
	})
	t.Run("other error wrapped with the op label", func(t *testing.T) {
		db, cfg, _ := ctFixture(t, []ctStep{{err: errors.New("boom")}})
		err := TolerantExec(ctx, db, cfg, "lease delete", "Q", "t1")
		if err == nil || err.Error() != "fake: lease delete: boom" {
			t.Fatalf("err = %v; want fake: lease delete: boom", err)
		}
	})
}

func TestGuardedExec_Shapes(t *testing.T) {
	ctx := context.Background()

	t.Run("row changed", func(t *testing.T) {
		db, cfg, _ := ctFixture(t, []ctStep{{result: ctResult{n: 1}}})
		won, err := GuardedExec(ctx, db, cfg, "lease heartbeat", "Q")
		if err != nil || !won {
			t.Fatalf("got (%v, %v); want (true, nil)", won, err)
		}
	})
	t.Run("guard rejected", func(t *testing.T) {
		db, cfg, _ := ctFixture(t, []ctStep{{result: ctResult{n: 0}}})
		won, err := GuardedExec(ctx, db, cfg, "lease finalize", "Q")
		if err != nil || won {
			t.Fatalf("got (%v, %v); want (false, nil)", won, err)
		}
	})
	t.Run("exec error wrapped", func(t *testing.T) {
		db, cfg, _ := ctFixture(t, []ctStep{{err: errors.New("boom")}})
		_, err := GuardedExec(ctx, db, cfg, "lease record ddl", "Q")
		if err == nil || err.Error() != "fake: lease record ddl: boom" {
			t.Fatalf("err = %v; want fake: lease record ddl: boom", err)
		}
	})
	t.Run("rows-affected error wrapped", func(t *testing.T) {
		db, cfg, _ := ctFixture(t, []ctStep{{result: ctResult{err: errors.New("boom")}}})
		_, err := GuardedExec(ctx, db, cfg, "lease heartbeat", "Q")
		if err == nil || err.Error() != "fake: lease heartbeat: rows affected: boom" {
			t.Fatalf("err = %v; want fake: lease heartbeat: rows affected: boom", err)
		}
	})
}

func leaseCols() []string {
	return []string{
		"target_table_full_name", "lease_holder_stream_id", "lease_expires_at",
		"ddl_text", "ddl_checksum", "applied_schema_version", "applied_at",
		"anchor_position", "source_engine",
	}
}

func TestSelectShardLease_ScanProjectionAndTolerance(t *testing.T) {
	ctx := context.Background()
	expires := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)

	t.Run("full row incl. NULL anchors", func(t *testing.T) {
		db, cfg, _ := ctFixture(t, []ctStep{{rows: &ctRows{
			cols: leaseCols(),
			data: [][]driver.Value{{"db.t1", "stream-a", expires, "ALTER …", "abc123", int64(7), nil, nil, nil}},
		}}})
		row, ok, err := SelectShardLease(ctx, db, cfg, "Q", "db.t1")
		if err != nil || !ok {
			t.Fatalf("got (ok=%v, err=%v); want (true, nil)", ok, err)
		}
		want := ShardLeaseRow{
			TargetTableFullName:  "db.t1",
			LeaseHolderStreamID:  "stream-a",
			LeaseExpiresAt:       sql.NullTime{Time: expires, Valid: true},
			DDLText:              "ALTER …",
			DDLChecksum:          "abc123",
			AppliedSchemaVersion: 7,
		}
		if row != want {
			t.Fatalf("row = %+v; want %+v", row, want)
		}
	})
	t.Run("absent row", func(t *testing.T) {
		db, cfg, _ := ctFixture(t, []ctStep{{rows: &ctRows{cols: leaseCols()}}})
		_, ok, err := SelectShardLease(ctx, db, cfg, "Q", "db.t1")
		if err != nil || ok {
			t.Fatalf("got (ok=%v, err=%v); want (false, nil)", ok, err)
		}
	})
	t.Run("missing table tolerated", func(t *testing.T) {
		db, cfg, _ := ctFixture(t, []ctStep{{err: errMissingTable}})
		_, ok, err := SelectShardLease(ctx, db, cfg, "Q", "db.t1")
		if err != nil || ok {
			t.Fatalf("got (ok=%v, err=%v); want (false, nil)", ok, err)
		}
	})
	t.Run("other error wrapped", func(t *testing.T) {
		db, cfg, _ := ctFixture(t, []ctStep{{err: errors.New("boom")}})
		_, _, err := SelectShardLease(ctx, db, cfg, "Q", "db.t1")
		if err == nil || err.Error() != "fake: lease select: boom" {
			t.Fatalf("err = %v; want fake: lease select: boom", err)
		}
	})
}

func TestListShardLeases_ScanAndTolerance(t *testing.T) {
	ctx := context.Background()
	applied := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)

	t.Run("rows scanned in projection order", func(t *testing.T) {
		db, cfg, _ := ctFixture(t, []ctStep{{rows: &ctRows{
			cols: leaseCols(),
			data: [][]driver.Value{
				{"db.t1", "stream-a", nil, "", "", int64(0), nil, nil, nil},
				{"db.t2", "stream-b", nil, "ALTER …", "def", int64(3), applied, "pos-1", "mysql"},
			},
		}}})
		out, err := ListShardLeases(ctx, db, cfg, "Q")
		if err != nil || len(out) != 2 {
			t.Fatalf("got (%d rows, %v); want (2, nil)", len(out), err)
		}
		if out[1].AnchorPosition != (sql.NullString{String: "pos-1", Valid: true}) ||
			out[1].AnchorEngine != (sql.NullString{String: "mysql", Valid: true}) ||
			out[1].AppliedAt != (sql.NullTime{Time: applied, Valid: true}) {
			t.Fatalf("row 2 anchors/applied = %+v; want pos-1/mysql/%v", out[1], applied)
		}
	})
	t.Run("missing table is empty not error", func(t *testing.T) {
		db, cfg, _ := ctFixture(t, []ctStep{{err: errMissingTable}})
		out, err := ListShardLeases(ctx, db, cfg, "Q")
		if err != nil || out == nil || len(out) != 0 {
			t.Fatalf("got (%v, %v); want (empty non-nil, nil)", out, err)
		}
	})
	t.Run("query error wrapped", func(t *testing.T) {
		db, cfg, _ := ctFixture(t, []ctStep{{err: errors.New("boom")}})
		_, err := ListShardLeases(ctx, db, cfg, "Q")
		if err == nil || err.Error() != "fake: list leases: boom" {
			t.Fatalf("err = %v; want fake: list leases: boom", err)
		}
	})
}

// TestShardLeaseRow_ToIR pins the Null-shape → HasX-bool conversion
// (ADR-0081 tier d; previously byte-identical in both engines'
// lease wrapper files). The load-bearing rule is the anchor
// conjunction: BOTH AnchorPosition and AnchorEngine must be non-NULL
// for HasAnchor — a half-populated row is treated as anchor-absent so
// the v0.76.0 lease GC sweep defensively retains it. The matrix
// covers each nullable field × {valid, NULL} and all four anchor
// presence combinations.
func TestShardLeaseRow_ToIR(t *testing.T) {
	expires := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	applied := time.Date(2026, 6, 10, 12, 5, 0, 0, time.UTC)

	base := ShardLeaseRow{
		TargetTableFullName:  "db.t1",
		LeaseHolderStreamID:  "stream-a",
		DDLText:              "ALTER …",
		DDLChecksum:          "abc123",
		AppliedSchemaVersion: 7,
	}
	wantBase := ir.ShardConsolidationLeaseRow{
		TargetTableFullName:  "db.t1",
		LeaseHolderStreamID:  "stream-a",
		DDLText:              "ALTER …",
		DDLChecksum:          "abc123",
		AppliedSchemaVersion: 7,
	}

	cases := []struct {
		name   string
		mutate func(r *ShardLeaseRow)
		want   func(w *ir.ShardConsolidationLeaseRow)
	}{
		{
			name:   "all nullables NULL",
			mutate: func(*ShardLeaseRow) {},
			want:   func(*ir.ShardConsolidationLeaseRow) {},
		},
		{
			name: "lease_expires_at set",
			mutate: func(r *ShardLeaseRow) {
				r.LeaseExpiresAt = sql.NullTime{Time: expires, Valid: true}
			},
			want: func(w *ir.ShardConsolidationLeaseRow) {
				w.LeaseExpiresAt = expires
				w.HasLeaseExpiresAt = true
			},
		},
		{
			name: "applied_at set",
			mutate: func(r *ShardLeaseRow) {
				r.AppliedAt = sql.NullTime{Time: applied, Valid: true}
			},
			want: func(w *ir.ShardConsolidationLeaseRow) {
				w.AppliedAt = applied
				w.HasAppliedAt = true
			},
		},
		{
			name: "anchor fully populated",
			mutate: func(r *ShardLeaseRow) {
				r.AnchorPosition = sql.NullString{String: "pos-1", Valid: true}
				r.AnchorEngine = sql.NullString{String: "mysql", Valid: true}
			},
			want: func(w *ir.ShardConsolidationLeaseRow) {
				w.AnchorPosition = ir.Position{Engine: "mysql", Token: "pos-1"}
				w.HasAnchor = true
			},
		},
		{
			name: "anchor position without engine is absent",
			mutate: func(r *ShardLeaseRow) {
				r.AnchorPosition = sql.NullString{String: "pos-1", Valid: true}
			},
			want: func(*ir.ShardConsolidationLeaseRow) {},
		},
		{
			name: "anchor engine without position is absent",
			mutate: func(r *ShardLeaseRow) {
				r.AnchorEngine = sql.NullString{String: "postgres", Valid: true}
			},
			want: func(*ir.ShardConsolidationLeaseRow) {},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			row, want := base, wantBase
			tc.mutate(&row)
			tc.want(&want)
			if got := row.ToIR(); got != want {
				t.Fatalf("ToIR() = %+v; want %+v", got, want)
			}
		})
	}
}
