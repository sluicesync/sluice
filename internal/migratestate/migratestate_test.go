// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migratestate

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// These tests pin the SHARED migrate-state store's control flow at
// the seam level (ADR-0082): the header/progress-row merge on Read,
// the missing-table tolerance ladders, the one-time legacy-blob
// upgrade (statement order, sorted inserts, tx commit/rollback), the
// header-only vs full-snapshot Write split, and the single-statement
// WriteTableProgress hot path. The engine packages keep their own
// behaviour oracles — the migration_state_integration_test.go suites
// ×2 run the same flows against real databases.
//
// Mechanism: a scripted database/sql fake driver answers each
// statement from a FIFO of steps (rows, a result, or an error) and
// records every statement + tx boundary, so every branch is reachable
// without a database and the statements' arrival order is assertable.
// Same shape as appliershared's control_table_test.go.

// errMissingTable is the scripted stand-in for the dialect's
// missing-table error (MySQL 1146 / PG 42P01); the test config's
// IsMissingTable classifier matches it by substring, the same shape
// the engines' classifiers use.
var errMissingTable = errors.New("MISSING_TABLE: relation is not there")

type msStep struct {
	rows *msRows
	err  error
}

// msConn is a minimal scripted driver connection: QueryContext and
// ExecContext both pop the next step; the statements seen (plus
// BEGIN/COMMIT/ROLLBACK markers) are recorded for order assertions.
// Single test goroutine — no locking.
type msConn struct {
	steps *[]msStep
	seen  *[]string
}

func (c msConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("migratestate: ms fake conn has no statements")
}
func (c msConn) Close() error { return nil }
func (c msConn) Begin() (driver.Tx, error) {
	return nil, errors.New("migratestate: use BeginTx")
}

func (c msConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	*c.seen = append(*c.seen, "BEGIN")
	return msTx{seen: c.seen}, nil
}

func (c msConn) pop(query string, args []driver.NamedValue) (msStep, error) {
	rendered := query
	if len(args) > 0 {
		vals := make([]string, len(args))
		for i, a := range args {
			vals[i] = fmt.Sprint(a.Value)
		}
		rendered += " | " + strings.Join(vals, ",")
	}
	*c.seen = append(*c.seen, rendered)
	if len(*c.steps) == 0 {
		return msStep{}, errors.New("migratestate: ms fake conn script exhausted")
	}
	s := (*c.steps)[0]
	*c.steps = (*c.steps)[1:]
	return s, nil
}

func (c msConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	s, err := c.pop(query, args)
	if err != nil {
		return nil, err
	}
	if s.err != nil {
		return nil, s.err
	}
	if s.rows == nil {
		return &msRows{}, nil
	}
	return s.rows, nil
}

func (c msConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	s, err := c.pop(query, args)
	if err != nil {
		return nil, err
	}
	if s.err != nil {
		return nil, s.err
	}
	return driver.RowsAffected(1), nil
}

type msTx struct{ seen *[]string }

func (t msTx) Commit() error {
	*t.seen = append(*t.seen, "COMMIT")
	return nil
}

func (t msTx) Rollback() error {
	*t.seen = append(*t.seen, "ROLLBACK")
	return nil
}

type msRows struct {
	cols []string
	vals [][]driver.Value
	i    int
}

func (r *msRows) Columns() []string { return r.cols }
func (r *msRows) Close() error      { return nil }
func (r *msRows) Next(dest []driver.Value) error {
	if r.i >= len(r.vals) {
		return io.EOF
	}
	copy(dest, r.vals[r.i])
	r.i++
	return nil
}

type msConnector struct {
	steps *[]msStep
	seen  *[]string
}

func (c msConnector) Connect(context.Context) (driver.Conn, error) {
	return msConn(c), nil
}
func (c msConnector) Driver() driver.Driver { return nil }

// newScriptedStore wires a Store over the scripted driver. The SQL
// statements are recognisable tokens (not real SQL) so order
// assertions read clearly.
func newScriptedStore(steps []msStep) (*Store, *[]msStep, *[]string) {
	stepsPtr := &steps
	seen := &[]string{}
	db := sql.OpenDB(msConnector{steps: stepsPtr, seen: seen})
	db.SetMaxOpenConns(1)
	return &Store{
		DB: db,
		Config: Config{
			EngineName: "fake",
			IsMissingTable: func(err error) bool {
				return err != nil && strings.Contains(err.Error(), "MISSING_TABLE")
			},
		},
		SQL: SQL{
			ReadHeader:         "READ_HEADER",
			ReadProgressRows:   "READ_PROGRESS",
			UpsertHeader:       "UPSERT_HEADER",
			UpsertProgressRow:  "UPSERT_PROGRESS",
			MarkUpgraded:       "MARK_UPGRADED",
			DeleteHeader:       "DELETE_HEADER",
			DeleteProgressRows: "DELETE_PROGRESS",
		},
	}, stepsPtr, seen
}

// headerRow builds a scripted header-row result in the ReadHeader
// projection order: (phase, table_progress, state_format, started_at,
// updated_at, last_error).
func headerRow(phase string, blob any, format int, started, updated time.Time, lastError any) *msRows {
	return &msRows{
		cols: []string{"phase", "table_progress", "state_format", "started_at", "updated_at", "last_error"},
		vals: [][]driver.Value{{phase, blob, int64(format), started, updated, lastError}},
	}
}

func progressRows(rows ...[]driver.Value) *msRows {
	return &msRows{cols: []string{"table_name", "progress", "updated_at"}, vals: rows}
}

var (
	t1 = time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	t2 = time.Date(2026, 6, 1, 11, 0, 0, 0, time.UTC)
	t3 = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
)

// TestRead_MergesHeaderAndProgressRows pins the format-2 read shape:
// header fields from the header row, TableProgress from the progress
// rows, UpdatedAt = the most recent timestamp across both.
func TestRead_MergesHeaderAndProgressRows(t *testing.T) {
	store, _, seen := newScriptedStore([]msStep{
		{rows: headerRow("bulk_copy", UpgradedBlobSentinel, FormatPerTableRows, t1, t2, nil)},
		{rows: progressRows(
			[]driver.Value{"users", `"complete"`, t1},
			[]driver.Value{"orders", `{"state":"in_progress","last_pk":[42],"rows_copied":42}`, t3},
		)},
	})

	got, ok, err := store.Read(context.Background(), "m1")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !ok {
		t.Fatal("Read ok=false; want true")
	}
	if got.Phase != ir.MigrationPhaseBulkCopy {
		t.Errorf("Phase = %q; want bulk_copy", got.Phase)
	}
	if got.TableProgress["users"].State != ir.TableProgressComplete {
		t.Errorf("users state = %q; want complete", got.TableProgress["users"].State)
	}
	if got.TableProgress["orders"].RowsCopied != 42 {
		t.Errorf("orders rows_copied = %d; want 42", got.TableProgress["orders"].RowsCopied)
	}
	if !got.UpdatedAt.Equal(t3) {
		t.Errorf("UpdatedAt = %v; want progress-row max %v", got.UpdatedAt, t3)
	}
	want := []string{"READ_HEADER | m1", "READ_PROGRESS | m1"}
	assertSeen(t, *seen, want)
}

// TestRead_MissingTableAndMissingRowTolerated pins the dry-run /
// pre-EnsureControlTable tolerance: both shapes read as "no row".
func TestRead_MissingTableAndMissingRowTolerated(t *testing.T) {
	t.Run("missing table", func(t *testing.T) {
		store, _, _ := newScriptedStore([]msStep{{err: errMissingTable}})
		_, ok, err := store.Read(context.Background(), "m1")
		if err != nil || ok {
			t.Fatalf("Read = ok=%v err=%v; want ok=false err=nil", ok, err)
		}
	})
	t.Run("missing row", func(t *testing.T) {
		store, _, _ := newScriptedStore([]msStep{{rows: &msRows{}}})
		_, ok, err := store.Read(context.Background(), "m1")
		if err != nil || ok {
			t.Fatalf("Read = ok=%v err=%v; want ok=false err=nil", ok, err)
		}
	})
}

// TestRead_LegacyBlobIsReadOnly pins the audit-2026-07-08 §4.4 split:
// Read on a format-1 header decodes the blob and returns the state
// WITHOUT writing anything — no upgrade transaction, so inspection
// works under a read-only target user and older binaries stay locked
// out only once a write actually mutates the row.
func TestRead_LegacyBlobIsReadOnly(t *testing.T) {
	blob := `{"users":"complete","orders":{"state":"in_progress","last_pk":[7],"rows_copied":7},"events":"no_pk_truncate_and_redo"}`
	store, _, seen := newScriptedStore([]msStep{
		{rows: headerRow("bulk_copy", blob, FormatLegacyBlob, t1, t2, nil)},
	})

	got, ok, err := store.Read(context.Background(), "m1")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !ok {
		t.Fatal("Read ok=false; want true")
	}
	if len(got.TableProgress) != 3 {
		t.Errorf("TableProgress len = %d; want 3", len(got.TableProgress))
	}
	if got.TableProgress["orders"].RowsCopied != 7 {
		t.Errorf("orders rows_copied = %d; want 7", got.TableProgress["orders"].RowsCopied)
	}
	// THE pin: the header SELECT is the only statement — a legacy Read
	// issues no writes.
	assertSeen(t, *seen, []string{"READ_HEADER | m1"})
}

// TestWrite_LegacyRowUpgradesOnFirstWrite pins the write-deferred
// ADR-0082 one-time upgrade: after Read flags a format-1 header, the
// FIRST write path explodes the blob into per-table rows inside ONE
// transaction — orphan-row delete first, inserts in sorted table
// order (the deadlock-ordering contract), then the sentinel+format
// flip — BEFORE its own statement, and only once.
func TestWrite_LegacyRowUpgradesOnFirstWrite(t *testing.T) {
	blob := `{"users":"complete","orders":{"state":"in_progress","last_pk":[7],"rows_copied":7},"events":"no_pk_truncate_and_redo"}`
	store, _, seen := newScriptedStore([]msStep{
		{rows: headerRow("bulk_copy", blob, FormatLegacyBlob, t1, t2, nil)},
		{}, // DELETE_PROGRESS   (upgrade tx)
		{}, // UPSERT_PROGRESS events
		{}, // UPSERT_PROGRESS orders
		{}, // UPSERT_PROGRESS users
		{}, // MARK_UPGRADED
		{}, // UPSERT_PROGRESS orders (the write itself)
		{}, // UPSERT_PROGRESS users  (second write — no re-upgrade)
	})

	ctx := context.Background()
	if _, _, err := store.Read(ctx, "m1"); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if err := store.WriteTableProgress(ctx, "m1", "orders",
		ir.TableProgress{State: ir.TableProgressInProgress, LastPK: []any{int64(9)}, RowsCopied: 9}); err != nil {
		t.Fatalf("WriteTableProgress: %v", err)
	}
	// Second write must NOT re-run the upgrade.
	if err := store.WriteTableProgress(ctx, "m1", "users",
		ir.TableProgress{State: ir.TableProgressComplete}); err != nil {
		t.Fatalf("second WriteTableProgress: %v", err)
	}
	want := []string{
		"READ_HEADER | m1",
		"BEGIN",
		"DELETE_PROGRESS | m1",
		`UPSERT_PROGRESS | m1,events,"no_pk_truncate_and_redo"`,
		`UPSERT_PROGRESS | m1,orders,{"state":"in_progress","last_pk":[{"_t":"i64","v":7}],"rows_copied":7}`,
		`UPSERT_PROGRESS | m1,users,"complete"`,
		"MARK_UPGRADED | " + UpgradedBlobSentinel + ",2,m1",
		"COMMIT",
		`UPSERT_PROGRESS | m1,orders,{"state":"in_progress","last_pk":[{"_t":"i64","v":9}],"rows_copied":9}`,
		`UPSERT_PROGRESS | m1,users,"complete"`,
	}
	assertSeen(t, *seen, want)
}

// TestWrite_HeaderOnlyOnLegacyRowUpgradesFirst pins the loss guard: a
// header-only Write on a Read-flagged legacy row must explode the blob
// into per-table rows BEFORE the header upsert replaces the blob with
// the sentinel — otherwise the recorded progress would be lost.
func TestWrite_HeaderOnlyOnLegacyRowUpgradesFirst(t *testing.T) {
	store, _, seen := newScriptedStore([]msStep{
		{rows: headerRow("bulk_copy", `{"users":"complete"}`, FormatLegacyBlob, t1, t2, nil)},
		{}, // DELETE_PROGRESS   (upgrade tx)
		{}, // UPSERT_PROGRESS users
		{}, // MARK_UPGRADED
		{}, // UPSERT_HEADER (the write itself)
	})

	ctx := context.Background()
	if _, _, err := store.Read(ctx, "m1"); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if err := store.Write(ctx, ir.MigrationState{MigrationID: "m1", Phase: ir.MigrationPhaseTables}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	want := []string{
		"READ_HEADER | m1",
		"BEGIN",
		"DELETE_PROGRESS | m1",
		`UPSERT_PROGRESS | m1,users,"complete"`,
		"MARK_UPGRADED | " + UpgradedBlobSentinel + ",2,m1",
		"COMMIT",
		"UPSERT_HEADER | m1,tables," + UpgradedBlobSentinel + ",2,<nil>",
	}
	assertSeen(t, *seen, want)
}

// TestWrite_LegacyUpgradeFailureRollsBackAndErrors pins the
// crash-safety shape: an upgrade-statement failure on the write path
// rolls the tx back and surfaces loudly — the header stays format 1
// with the blob intact, and the note stays pending so the NEXT write
// re-runs the whole upgrade.
func TestWrite_LegacyUpgradeFailureRollsBackAndErrors(t *testing.T) {
	store, _, seen := newScriptedStore([]msStep{
		{rows: headerRow("bulk_copy", `{"users":"complete"}`, FormatLegacyBlob, t1, t2, nil)},
		{},                             // DELETE_PROGRESS
		{err: errors.New("disk full")}, // UPSERT_PROGRESS users
		{},                             // DELETE_PROGRESS  (retry)
		{},                             // UPSERT_PROGRESS users
		{},                             // MARK_UPGRADED
		{},                             // UPSERT_PROGRESS orders (the write)
	})

	ctx := context.Background()
	if _, _, err := store.Read(ctx, "m1"); err != nil {
		t.Fatalf("Read: %v", err)
	}
	err := store.WriteTableProgress(ctx, "m1", "orders", ir.TableProgress{State: ir.TableProgressComplete})
	if err == nil {
		t.Fatal("WriteTableProgress succeeded; want upgrade error")
	}
	if !strings.Contains(err.Error(), "upgrade migrate-state row") {
		t.Errorf("err = %v; want upgrade wording", err)
	}
	if last := (*seen)[len(*seen)-1]; last != "ROLLBACK" {
		t.Errorf("last statement = %q; want ROLLBACK", last)
	}
	// The retry (note stayed pending) upgrades, then the write lands.
	if err := store.WriteTableProgress(ctx, "m1", "orders", ir.TableProgress{State: ir.TableProgressComplete}); err != nil {
		t.Fatalf("retry WriteTableProgress: %v", err)
	}
	tail := (*seen)[len(*seen)-6:]
	wantTail := []string{
		"BEGIN",
		"DELETE_PROGRESS | m1",
		`UPSERT_PROGRESS | m1,users,"complete"`,
		"MARK_UPGRADED | " + UpgradedBlobSentinel + ",2,m1",
		"COMMIT",
		`UPSERT_PROGRESS | m1,orders,"complete"`,
	}
	assertSeen(t, tail, wantTail)
}

// TestRead_LegacyEmptyBlobStillUpgradesOnWrite pins the pending-row
// shape: a format-1 header whose blob is NULL (fresh pending row from
// an old binary) still flips to format 2 + sentinel on the first write
// so later per-table writes are never invisible behind a legacy
// header.
func TestRead_LegacyEmptyBlobStillUpgradesOnWrite(t *testing.T) {
	store, _, seen := newScriptedStore([]msStep{
		{rows: headerRow("pending", nil, FormatLegacyBlob, t1, t2, nil)},
		{}, // DELETE_PROGRESS   (upgrade tx)
		{}, // MARK_UPGRADED
		{}, // UPSERT_HEADER (the write itself)
	})

	ctx := context.Background()
	got, ok, err := store.Read(ctx, "m1")
	if err != nil || !ok {
		t.Fatalf("Read = ok=%v err=%v; want ok=true err=nil", ok, err)
	}
	if got.TableProgress != nil {
		t.Errorf("TableProgress = %v; want nil", got.TableProgress)
	}
	if err := store.Write(ctx, ir.MigrationState{MigrationID: "m1", Phase: ir.MigrationPhaseBulkCopy}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	want := []string{
		"READ_HEADER | m1",
		"BEGIN",
		"DELETE_PROGRESS | m1",
		"MARK_UPGRADED | " + UpgradedBlobSentinel + ",2,m1",
		"COMMIT",
		"UPSERT_HEADER | m1,bulk_copy," + UpgradedBlobSentinel + ",2,<nil>",
	}
	assertSeen(t, *seen, want)
}

// TestClearMigration_DropsPendingUpgradeNote pins that clearing a
// Read-flagged legacy migration forgets its needs-upgrade note: a
// subsequent same-id write must NOT resurrect the cleared blob's
// progress rows.
func TestClearMigration_DropsPendingUpgradeNote(t *testing.T) {
	store, _, seen := newScriptedStore([]msStep{
		{rows: headerRow("bulk_copy", `{"users":"complete"}`, FormatLegacyBlob, t1, t2, nil)},
		{}, // DELETE_PROGRESS (clear)
		{}, // DELETE_HEADER   (clear)
		{}, // UPSERT_HEADER   (fresh write — no upgrade tx)
	})

	ctx := context.Background()
	if _, _, err := store.Read(ctx, "m1"); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if err := store.ClearMigration(ctx, "m1"); err != nil {
		t.Fatalf("ClearMigration: %v", err)
	}
	if err := store.Write(ctx, ir.MigrationState{MigrationID: "m1", Phase: ir.MigrationPhasePending}); err != nil {
		t.Fatalf("Write after clear: %v", err)
	}
	want := []string{
		"READ_HEADER | m1",
		"DELETE_PROGRESS | m1",
		"DELETE_HEADER | m1",
		"UPSERT_HEADER | m1,pending," + UpgradedBlobSentinel + ",2,<nil>",
	}
	assertSeen(t, *seen, want)
}

// TestWrite_HeaderOnlyIsSingleStatement pins the hot phase-transition
// shape: an empty TableProgress map writes the header in one
// statement — no transaction, no progress-row touches — always at
// format 2 with the sentinel blob.
func TestWrite_HeaderOnlyIsSingleStatement(t *testing.T) {
	store, _, seen := newScriptedStore([]msStep{{}})
	err := store.Write(context.Background(), ir.MigrationState{
		MigrationID: "m1",
		Phase:       ir.MigrationPhaseTables,
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	want := []string{
		"UPSERT_HEADER | m1,tables," + UpgradedBlobSentinel + ",2,<nil>",
	}
	assertSeen(t, *seen, want)
}

// TestWrite_FullSnapshotIsSortedTx pins the full-snapshot shape:
// header + every entry in ONE transaction, inserts in sorted table
// order (deadlock-ordering contract).
func TestWrite_FullSnapshotIsSortedTx(t *testing.T) {
	store, _, seen := newScriptedStore([]msStep{
		{}, // UPSERT_HEADER
		{}, // UPSERT_PROGRESS a_users
		{}, // UPSERT_PROGRESS b_orders
	})
	err := store.Write(context.Background(), ir.MigrationState{
		MigrationID: "m1",
		Phase:       ir.MigrationPhaseBulkCopy,
		TableProgress: map[string]ir.TableProgress{
			"b_orders": {State: ir.TableProgressInProgress, LastPK: []any{int64(9)}, RowsCopied: 9},
			"a_users":  {State: ir.TableProgressComplete},
		},
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	want := []string{
		"BEGIN",
		"UPSERT_HEADER | m1,bulk_copy," + UpgradedBlobSentinel + ",2,<nil>",
		`UPSERT_PROGRESS | m1,a_users,"complete"`,
		`UPSERT_PROGRESS | m1,b_orders,{"state":"in_progress","last_pk":[{"_t":"i64","v":9}],"rows_copied":9}`,
		"COMMIT",
	}
	assertSeen(t, *seen, want)
}

// TestWriteTableProgress_SingleRowUpsert pins THE load-bearing
// ADR-0082 property: one checkpoint = one progress-row upsert,
// regardless of how many peer tables exist.
func TestWriteTableProgress_SingleRowUpsert(t *testing.T) {
	store, _, seen := newScriptedStore([]msStep{{}})
	err := store.WriteTableProgress(context.Background(), "m1", "orders",
		ir.TableProgress{State: ir.TableProgressInProgress, LastPK: []any{int64(5000)}, RowsCopied: 5000})
	if err != nil {
		t.Fatalf("WriteTableProgress: %v", err)
	}
	want := []string{
		`UPSERT_PROGRESS | m1,orders,{"state":"in_progress","last_pk":[{"_t":"i64","v":5000}],"rows_copied":5000}`,
	}
	assertSeen(t, *seen, want)
}

// TestClearMigration_DeletesRowsThenHeader pins the delete order
// (progress rows first — see the method comment for the crash
// argument) and the per-statement missing-table tolerance.
func TestClearMigration_DeletesRowsThenHeader(t *testing.T) {
	t.Run("both present", func(t *testing.T) {
		store, _, seen := newScriptedStore([]msStep{{}, {}})
		if err := store.ClearMigration(context.Background(), "m1"); err != nil {
			t.Fatalf("ClearMigration: %v", err)
		}
		assertSeen(t, *seen, []string{"DELETE_PROGRESS | m1", "DELETE_HEADER | m1"})
	})
	t.Run("both tables missing", func(t *testing.T) {
		store, _, _ := newScriptedStore([]msStep{{err: errMissingTable}, {err: errMissingTable}})
		if err := store.ClearMigration(context.Background(), "m1"); err != nil {
			t.Fatalf("ClearMigration on missing tables: %v", err)
		}
	})
}

// TestValidationErrors pins the empty-argument refusals with the
// engine-prefixed wording the pre-extraction stores used.
func TestValidationErrors(t *testing.T) {
	store, _, _ := newScriptedStore(nil)
	ctx := context.Background()
	if _, _, err := store.Read(ctx, ""); err == nil || !strings.Contains(err.Error(), "fake: migrate-state Read") {
		t.Errorf("Read(\"\") err = %v; want engine-prefixed refusal", err)
	}
	if err := store.Write(ctx, ir.MigrationState{Phase: "x"}); err == nil || !strings.Contains(err.Error(), "MigrationID is empty") {
		t.Errorf("Write without id err = %v", err)
	}
	if err := store.Write(ctx, ir.MigrationState{MigrationID: "m"}); err == nil || !strings.Contains(err.Error(), "Phase is empty") {
		t.Errorf("Write without phase err = %v", err)
	}
	if err := store.WriteTableProgress(ctx, "", "t", ir.TableProgress{}); err == nil || !strings.Contains(err.Error(), "migrationID is empty") {
		t.Errorf("WriteTableProgress without id err = %v", err)
	}
	if err := store.WriteTableProgress(ctx, "m", "", ir.TableProgress{}); err == nil || !strings.Contains(err.Error(), "tableName is empty") {
		t.Errorf("WriteTableProgress without table err = %v", err)
	}
	if err := store.ClearMigration(ctx, ""); err == nil || !strings.Contains(err.Error(), "migrationID is empty") {
		t.Errorf("ClearMigration(\"\") err = %v", err)
	}
}

func assertSeen(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("statements seen = %q; want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("statement[%d] = %q; want %q", i, got[i], want[i])
		}
	}
}
