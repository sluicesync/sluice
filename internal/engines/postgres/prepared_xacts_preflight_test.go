// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

// preparedXactsDriver is a minimal fake driver whose DSN selects the
// pg_prepared_xacts census result: "pending" (two rows, the older
// first), "empty" (no rows — the healthy default posture), anything
// else errors (privilege / broken-connection sim).
type preparedXactsDriver struct{}

type preparedXactsConn struct{ scenario string }

func (preparedXactsDriver) Open(dsn string) (driver.Conn, error) {
	return &preparedXactsConn{scenario: dsn}, nil
}

func (c *preparedXactsConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected Prepare")
}
func (c *preparedXactsConn) Close() error { return nil }
func (c *preparedXactsConn) Begin() (driver.Tx, error) {
	return nil, errors.New("unexpected Begin")
}

func (c *preparedXactsConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if !strings.Contains(query, "pg_prepared_xacts") {
		return nil, errors.New("unexpected query: " + query)
	}
	switch c.scenario {
	case "pending":
		return &preparedXactsRows{rows: [][]driver.Value{
			{"orphaned_gid_1", "app", "owner1", int64(3600)},
			{"fresh_gid_2", "other_db", "owner2", int64(2)},
		}}, nil
	case "empty":
		return &preparedXactsRows{}, nil
	default:
		return nil, errors.New("permission denied for view pg_prepared_xacts")
	}
}

type preparedXactsRows struct {
	rows [][]driver.Value
	sent int
}

func (r *preparedXactsRows) Columns() []string {
	return []string{"gid", "database", "owner", "age_seconds"}
}
func (r *preparedXactsRows) Close() error { return nil }
func (r *preparedXactsRows) Next(dest []driver.Value) error {
	if r.sent >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.sent])
	r.sent++
	return nil
}

var registerPreparedXactsOnce sync.Once

func newPreparedXactsDB(t *testing.T, scenario string) *sql.DB {
	t.Helper()
	registerPreparedXactsOnce.Do(func() { sql.Register("sluice-preparedxacts-test", preparedXactsDriver{}) })
	db, err := sql.Open("sluice-preparedxacts-test", scenario)
	if err != nil {
		t.Fatalf("open prepared-xacts db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// capturePreparedXactWarnLog swaps the default slog handler for a
// WARN-level text handler writing into the returned buffer.
func capturePreparedXactWarnLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

// TestWarnPreparedTransactions pins the P3 WARN in both directions:
// pending prepared transactions WARN naming the gids, their databases,
// ages and the unblock remedy; an empty pg_prepared_xacts — the
// healthy posture on every source that doesn't use 2PC, including
// max_prepared_transactions=0 servers where the view exists and is
// empty — stays silent.
//
// Mutation-verified in both directions (2026-08-26; mutants
// grep-confirmed, targeted-revert after): forcing
// warnPreparedTransactions to return before its final WARN fails the
// firing cell; dropping the len(xacts)==0 early return (warn always)
// fails the silent cell.
func TestWarnPreparedTransactions(t *testing.T) {
	t.Run("pending_warns", func(t *testing.T) {
		buf := capturePreparedXactWarnLog(t)
		warnPreparedTransactions(context.Background(), newPreparedXactsDB(t, "pending"), "sluice_slot")
		out := buf.String()
		if !strings.Contains(out, preparedXactMarker) {
			t.Fatalf("no %s WARN for pending prepared txs; log: %s", preparedXactMarker, out)
		}
		for _, phrase := range []string{
			"orphaned_gid_1",  // names the gid
			"other_db",        // a prepared tx in ANOTHER database blocks too
			"age=1h0m0s",      // the age, rendered
			"COMMIT PREPARED", // the unblock remedy
			"ROLLBACK PREPARED",
			"sluice_slot", // names the slot about to block
		} {
			if !strings.Contains(out, phrase) {
				t.Errorf("WARN missing %q; log: %s", phrase, out)
			}
		}
	})
	t.Run("empty_stays_silent", func(t *testing.T) {
		buf := capturePreparedXactWarnLog(t)
		warnPreparedTransactions(context.Background(), newPreparedXactsDB(t, "empty"), "sluice_slot")
		if out := buf.String(); strings.Contains(out, preparedXactMarker) {
			t.Fatalf("empty pg_prepared_xacts must stay silent; log: %s", out)
		}
	})
}

// TestWarnPreparedTransactions_ProbeErrorWarns: a failed probe WARNs
// ("could not rule the block out") and passes instead of silently
// skipping — the SL-1 discipline. The slot creation immediately after
// surfaces a genuinely broken connection loudly, so this never masks
// one.
func TestWarnPreparedTransactions_ProbeErrorWarns(t *testing.T) {
	buf := capturePreparedXactWarnLog(t)
	warnPreparedTransactions(context.Background(), newPreparedXactsDB(t, "err"), "sluice_slot")
	out := buf.String()
	if !strings.Contains(out, preparedXactMarker) || !strings.Contains(out, "could not read pg_prepared_xacts") {
		t.Fatalf("probe error must emit the degraded %s WARN; log: %s", preparedXactMarker, out)
	}
}
