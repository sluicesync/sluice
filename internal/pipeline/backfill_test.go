// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit tests for the `sluice backfill` orchestrator (ADR-0159): --set
// parsing, the coded refusals, the keyset chunk loop (cursor advance,
// batch bound, resume, completed-state no-op, --restart), and the
// dry-run write-nothing contract — all against an in-memory fake
// engine so no database is involved.

package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// ---- ParseBackfillSets ----

func TestBackfill_ParseSets(t *testing.T) {
	cases := []struct {
		name    string
		in      []string
		want    []ir.BackfillSet
		wantErr string
	}{
		{
			name: "single valid",
			in:   []string{"new_col = old_col * 2"},
			want: []ir.BackfillSet{{Column: "new_col", Expr: "old_col * 2"}},
		},
		{
			name: "multiple valid",
			in:   []string{"a = 1", "b = UPPER(name)"},
			want: []ir.BackfillSet{{Column: "a", Expr: "1"}, {Column: "b", Expr: "UPPER(name)"}},
		},
		{
			name: "expr containing '=' splits at the FIRST '='",
			in:   []string{"flag = CASE WHEN status = 'x' THEN 1 ELSE 0 END"},
			want: []ir.BackfillSet{{Column: "flag", Expr: "CASE WHEN status = 'x' THEN 1 ELSE 0 END"}},
		},
		{
			name: "no whitespace around '='",
			in:   []string{"a=b"},
			want: []ir.BackfillSet{{Column: "a", Expr: "b"}},
		},
		{name: "missing '='", in: []string{"new_col old_col"}, wantErr: "no '='"},
		{name: "empty column", in: []string{"= old_col"}, wantErr: "empty column"},
		{name: "empty expression", in: []string{"new_col = "}, wantErr: "empty expression"},
		{name: "duplicate column", in: []string{"a = 1", "a = 2"}, wantErr: "more than one --set"},
		{name: "no sets at all", in: nil, wantErr: "at least one"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseBackfillSets(tc.in)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("ParseBackfillSets(%v) err = %v; want containing %q", tc.in, err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseBackfillSets(%v): %v", tc.in, err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %d sets; want %d", len(got), len(tc.want))
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("set[%d] = %+v; want %+v", i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestBackfill_MigrationIDStableAndSpecSensitive(t *testing.T) {
	sets := []ir.BackfillSet{{Column: "a", Expr: "b"}}
	id1 := BackfillMigrationID("t", sets, "a IS NULL")
	id2 := BackfillMigrationID("t", sets, "a IS NULL")
	if id1 != id2 {
		t.Errorf("same spec hashed differently: %q vs %q", id1, id2)
	}
	if !strings.HasPrefix(id1, "backfill:t:") {
		t.Errorf("id %q missing backfill:<table>: prefix", id1)
	}
	if other := BackfillMigrationID("t", sets, "a IS NOT NULL"); other == id1 {
		t.Errorf("different where hashed identically: %q", other)
	}
	if other := BackfillMigrationID("t", []ir.BackfillSet{{Column: "a", Expr: "c"}}, "a IS NULL"); other == id1 {
		t.Errorf("different expr hashed identically: %q", other)
	}
}

// ---- fakes ----

// backfillFakeRow is one row of the in-memory table: an int64 PK, a
// source value, and a nullable target value.
type backfillFakeRow struct {
	pk  int64
	old int64
	new *int64
}

// backfillFakeExecutor implements ir.BackfillExecutor over an ordered
// in-memory row slice. Its "where" semantics are fixed to the test
// spec `new IS NULL`, and its "set" semantics to `new = old + 1`.
type backfillFakeExecutor struct {
	rows []backfillFakeRow // ascending pk

	execCalls    int
	maxChunkSpan int // rows walked per chunk (bound check)
	limitsSeen   []int
	aftersSeen   [][]any
	closed       bool

	// afterExec, when set, runs at the end of each ExecBackfillChunk —
	// the "concurrent writer" lever for the --verify catch-up pins
	// (e.g. insert a fresh row BEHIND the cursor mid-walk).
	afterExec func(f *backfillFakeExecutor)
}

func (f *backfillFakeExecutor) idx(after []any) int {
	if len(after) == 0 {
		return 0
	}
	cursor := after[0].(int64)
	for i, r := range f.rows {
		if r.pk > cursor {
			return i
		}
	}
	return len(f.rows)
}

func (f *backfillFakeExecutor) NextChunkUpperBound(_ context.Context, _ *ir.Table, after []any, limit int) (upper []any, ok bool, err error) {
	f.limitsSeen = append(f.limitsSeen, limit)
	f.aftersSeen = append(f.aftersSeen, after)
	start := f.idx(after)
	if start >= len(f.rows) {
		return nil, false, nil
	}
	end := start + limit
	if end > len(f.rows) {
		end = len(f.rows)
	}
	return []any{f.rows[end-1].pk}, true, nil
}

func (f *backfillFakeExecutor) ExecBackfillChunk(_ context.Context, _ *ir.Table, _ []ir.BackfillSet, _ string, after, upper []any) (int64, error) {
	f.execCalls++
	start := f.idx(after)
	up := upper[0].(int64)
	var n int64
	span := 0
	for i := start; i < len(f.rows) && f.rows[i].pk <= up; i++ {
		span++
		if f.rows[i].new == nil { // the fixed `new IS NULL` guard
			v := f.rows[i].old + 1
			f.rows[i].new = &v
			n++
		}
	}
	if span > f.maxChunkSpan {
		f.maxChunkSpan = span
	}
	if f.afterExec != nil {
		f.afterExec(f)
	}
	return n, nil
}

func (f *backfillFakeExecutor) BackfillStatement(*ir.Table, []ir.BackfillSet, string) (string, error) {
	return "UPDATE fake SET new = old + 1 WHERE (pk) > (?) AND (pk) <= (?) AND (new IS NULL)", nil
}

func (f *backfillFakeExecutor) CountRemaining(context.Context, *ir.Table, string) (int64, error) {
	var n int64
	for _, r := range f.rows {
		if r.new == nil {
			n++
		}
	}
	return n, nil
}

func (f *backfillFakeExecutor) Close() error {
	f.closed = true
	return nil
}

// backfillFakeStore is an in-memory ir.MigrationStateStore.
type backfillFakeStore struct {
	mu       sync.Mutex
	headers  map[string]ir.MigrationState
	progress map[string]map[string]ir.TableProgress
	writes   int
}

func newBackfillFakeStore() *backfillFakeStore {
	return &backfillFakeStore{
		headers:  map[string]ir.MigrationState{},
		progress: map[string]map[string]ir.TableProgress{},
	}
}

func (s *backfillFakeStore) EnsureControlTable(context.Context) error { return nil }

func (s *backfillFakeStore) Read(_ context.Context, id string) (ir.MigrationState, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.headers[id]
	if !ok {
		return ir.MigrationState{}, false, nil
	}
	st.TableProgress = map[string]ir.TableProgress{}
	for k, v := range s.progress[id] {
		st.TableProgress[k] = v
	}
	return st, true, nil
}

func (s *backfillFakeStore) Write(_ context.Context, state ir.MigrationState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writes++
	s.headers[state.MigrationID] = ir.MigrationState{MigrationID: state.MigrationID, Phase: state.Phase, LastError: state.LastError}
	for k, v := range state.TableProgress {
		if s.progress[state.MigrationID] == nil {
			s.progress[state.MigrationID] = map[string]ir.TableProgress{}
		}
		s.progress[state.MigrationID][k] = v
	}
	return nil
}

func (s *backfillFakeStore) WriteTableProgress(_ context.Context, id, table string, p ir.TableProgress) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writes++
	if s.progress[id] == nil {
		s.progress[id] = map[string]ir.TableProgress{}
	}
	s.progress[id][table] = p
	return nil
}

func (s *backfillFakeStore) ClearMigration(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.headers, id)
	delete(s.progress, id)
	return nil
}

func (s *backfillFakeStore) Close() error { return nil }

// backfillFakeSchemaReader serves a fixed schema.
type backfillFakeSchemaReader struct{ schema *ir.Schema }

func (r backfillFakeSchemaReader) ReadSchema(context.Context) (*ir.Schema, error) {
	return r.schema, nil
}

// backfillFakeEngine implements ir.Engine (via stubEngineBase) plus
// the backfill opener and the migrate-state opener.
type backfillFakeEngine struct {
	stubEngineBase
	schema *ir.Schema
	ex     *backfillFakeExecutor
	store  *backfillFakeStore
}

func (e *backfillFakeEngine) Name() string { return "fake" }
func (e *backfillFakeEngine) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	return backfillFakeSchemaReader{schema: e.schema}, nil
}

func (e *backfillFakeEngine) OpenBackfillExecutor(context.Context, string) (ir.BackfillExecutor, error) {
	return e.ex, nil
}

func (e *backfillFakeEngine) OpenMigrationStateStore(context.Context, string) (ir.MigrationStateStore, error) {
	return e.store, nil
}

// noBackfillEngine implements ir.Engine but NOT the backfill opener —
// the unsupported-engine refusal shape.
type noBackfillEngine struct {
	stubEngineBase
	schema *ir.Schema
}

func (e *noBackfillEngine) Name() string { return "sqlite" }
func (e *noBackfillEngine) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	return backfillFakeSchemaReader{schema: e.schema}, nil
}

// ---- fixtures ----

func backfillTestSchema(pk *ir.Index) *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Name: "items",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "old_col", Type: ir.Integer{Width: 64}},
			{Name: "new_col", Type: ir.Integer{Width: 64}},
		},
		PrimaryKey: pk,
	}}}
}

func backfillIntPK() *ir.Index {
	return &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}}
}

func backfillFakeRows(n int) []backfillFakeRow {
	rows := make([]backfillFakeRow, n)
	for i := range rows {
		rows[i] = backfillFakeRow{pk: int64(i + 1), old: int64(i + 1)}
	}
	return rows
}

func newTestBackfiller(eng ir.Engine) *Backfiller {
	return &Backfiller{
		Engine: eng,
		DSN:    "dsn",
		Table:  "items",
		Sets:   []ir.BackfillSet{{Column: "new_col", Expr: "old_col + 1"}},
		Where:  "new_col IS NULL",
	}
}

func wantBackfillCode(t *testing.T, err error, code sluicecode.Code) {
	t.Helper()
	if err == nil {
		t.Fatal("want a coded refusal; got nil error")
	}
	var coded *sluicecode.CodedError
	if !errors.As(err, &coded) {
		t.Fatalf("want *sluicecode.CodedError; got %T: %v", err, err)
	}
	if coded.Code != code {
		t.Errorf("code = %s; want %s", coded.Code, code)
	}
}

// ---- orchestrator behaviour ----

func TestBackfill_LoopAdvancesCursorAndBoundsBatches(t *testing.T) {
	ex := &backfillFakeExecutor{rows: backfillFakeRows(25)}
	eng := &backfillFakeEngine{schema: backfillTestSchema(backfillIntPK()), ex: ex, store: newBackfillFakeStore()}
	b := newTestBackfiller(eng)
	b.BatchSize = 10

	res, err := b.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.RowsUpdated != 25 || res.Chunks != 3 {
		t.Errorf("RowsUpdated=%d Chunks=%d; want 25, 3", res.RowsUpdated, res.Chunks)
	}
	if ex.maxChunkSpan > 10 {
		t.Errorf("a chunk spanned %d rows; batch bound is 10", ex.maxChunkSpan)
	}
	// Cursor advance: the walk's `after` tuples are nil, [10], [20].
	if len(ex.aftersSeen) < 3 || ex.aftersSeen[0] != nil ||
		ex.aftersSeen[1][0].(int64) != 10 || ex.aftersSeen[2][0].(int64) != 20 {
		t.Errorf("afters = %v; want nil, [10], [20], ...", ex.aftersSeen)
	}
	for _, r := range ex.rows {
		if r.new == nil || *r.new != r.old+1 {
			t.Fatalf("row pk=%d not backfilled correctly: %v", r.pk, r.new)
		}
	}
	// Terminal state: header complete + table-progress complete.
	id := BackfillMigrationID("items", b.Sets, b.Where)
	if got := eng.store.headers[id].Phase; got != ir.MigrationPhaseComplete {
		t.Errorf("header phase = %s; want complete", got)
	}
	if got := eng.store.progress[id]["items"].State; got != ir.TableProgressComplete {
		t.Errorf("table progress state = %s; want complete", got)
	}
	if !ex.closed {
		t.Error("executor not closed")
	}
}

func TestBackfill_ZeroBatchSizeUsesDefault(t *testing.T) {
	ex := &backfillFakeExecutor{rows: backfillFakeRows(3)}
	eng := &backfillFakeEngine{schema: backfillTestSchema(backfillIntPK()), ex: ex, store: newBackfillFakeStore()}
	b := newTestBackfiller(eng) // BatchSize unset (0)

	if _, err := b.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(ex.limitsSeen) == 0 || ex.limitsSeen[0] != migcore.DefaultBulkBatchSize {
		t.Errorf("limit = %v; want first call at migcore.DefaultBulkBatchSize (%d)", ex.limitsSeen, migcore.DefaultBulkBatchSize)
	}
}

func TestBackfill_RefusesNoPrimaryKey(t *testing.T) {
	eng := &backfillFakeEngine{schema: backfillTestSchema(nil), ex: &backfillFakeExecutor{}, store: newBackfillFakeStore()}
	_, err := newTestBackfiller(eng).Run(context.Background())
	wantBackfillCode(t, err, sluicecode.CodeBackfillNoPrimaryKey)
}

func TestBackfill_RefusesNonOrderablePrimaryKey(t *testing.T) {
	schema := backfillTestSchema(backfillIntPK())
	schema.Tables[0].Columns[0].Type = ir.JSON{} // JSON PK: not orderable
	eng := &backfillFakeEngine{schema: schema, ex: &backfillFakeExecutor{}, store: newBackfillFakeStore()}
	_, err := newTestBackfiller(eng).Run(context.Background())
	wantBackfillCode(t, err, sluicecode.CodeBackfillNoPrimaryKey)
}

func TestBackfill_RefusesUnknownSetColumn(t *testing.T) {
	eng := &backfillFakeEngine{schema: backfillTestSchema(backfillIntPK()), ex: &backfillFakeExecutor{}, store: newBackfillFakeStore()}
	b := newTestBackfiller(eng)
	b.Sets = []ir.BackfillSet{{Column: "nope", Expr: "1"}}
	_, err := b.Run(context.Background())
	wantBackfillCode(t, err, sluicecode.CodeBackfillUnknownColumn)
}

func TestBackfill_RefusesUnsupportedEngine(t *testing.T) {
	eng := &noBackfillEngine{schema: backfillTestSchema(backfillIntPK())}
	_, err := newTestBackfiller(eng).Run(context.Background())
	wantBackfillCode(t, err, sluicecode.CodeBackfillUnsupportedEngine)
}

func TestBackfill_RefusesMissingTable(t *testing.T) {
	eng := &backfillFakeEngine{schema: backfillTestSchema(backfillIntPK()), ex: &backfillFakeExecutor{}, store: newBackfillFakeStore()}
	b := newTestBackfiller(eng)
	b.Table = "absent"
	if _, err := b.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("err = %v; want table-not-found", err)
	}
}

func TestBackfill_CompletedStateIsNoOp(t *testing.T) {
	ex := &backfillFakeExecutor{rows: backfillFakeRows(5)}
	store := newBackfillFakeStore()
	eng := &backfillFakeEngine{schema: backfillTestSchema(backfillIntPK()), ex: ex, store: store}
	b := newTestBackfiller(eng)
	id := BackfillMigrationID(b.Table, b.Sets, b.Where)
	store.headers[id] = ir.MigrationState{MigrationID: id, Phase: ir.MigrationPhaseComplete}

	var out bytes.Buffer
	b.Out = &out
	res, err := b.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !res.AlreadyComplete {
		t.Error("AlreadyComplete = false; want true")
	}
	if ex.execCalls != 0 {
		t.Errorf("execCalls = %d; a completed spec must touch no rows", ex.execCalls)
	}
	if !strings.Contains(out.String(), "--restart") {
		t.Errorf("no-op notice %q should name the --restart remedy", out.String())
	}
}

func TestBackfill_RestartClearsCompletedState(t *testing.T) {
	ex := &backfillFakeExecutor{rows: backfillFakeRows(5)}
	store := newBackfillFakeStore()
	eng := &backfillFakeEngine{schema: backfillTestSchema(backfillIntPK()), ex: ex, store: store}
	b := newTestBackfiller(eng)
	b.Restart = true
	id := BackfillMigrationID(b.Table, b.Sets, b.Where)
	store.headers[id] = ir.MigrationState{MigrationID: id, Phase: ir.MigrationPhaseComplete}

	res, err := b.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.AlreadyComplete {
		t.Error("AlreadyComplete = true; --restart must start over")
	}
	if ex.execCalls == 0 {
		t.Error("--restart ran no chunks")
	}
}

func TestBackfill_ResumeUsesStoredCursor(t *testing.T) {
	ex := &backfillFakeExecutor{rows: backfillFakeRows(20)}
	// Rows 1..10 already done (a previous run's chunk).
	for i := 0; i < 10; i++ {
		v := ex.rows[i].old + 1
		ex.rows[i].new = &v
	}
	store := newBackfillFakeStore()
	eng := &backfillFakeEngine{schema: backfillTestSchema(backfillIntPK()), ex: ex, store: store}
	b := newTestBackfiller(eng)
	b.BatchSize = 10
	id := BackfillMigrationID(b.Table, b.Sets, b.Where)
	store.headers[id] = ir.MigrationState{MigrationID: id, Phase: backfillPhaseRunning}
	store.progress[id] = map[string]ir.TableProgress{
		"items": {State: ir.TableProgressInProgress, LastPK: []any{int64(10)}, RowsCopied: 10},
	}

	res, err := b.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !res.Resumed {
		t.Error("Resumed = false; want true")
	}
	// The first walk call must start AT the stored cursor, not row 0.
	if len(ex.aftersSeen) == 0 || len(ex.aftersSeen[0]) != 1 || ex.aftersSeen[0][0].(int64) != 10 {
		t.Errorf("first after = %v; want [10] (the persisted cursor)", ex.aftersSeen)
	}
	// Total carries the previously-recorded rows plus this run's.
	if res.RowsUpdated != 20 {
		t.Errorf("RowsUpdated = %d; want 20 (10 previous + 10 now)", res.RowsUpdated)
	}
}

// TestBackfill_ConcurrentRunHeartbeatGuard pins the audit L-D0-9 guard:
// a spec whose state row is still in the `backfill` phase with a FRESH
// heartbeat refuses coded (SLUICE-E-BACKFILL-CONCURRENT-RUN) before any
// UPDATE or state write — including under --restart, which must not
// clear the row out from under the live walker — while a STALE
// heartbeat (a crashed runner) resumes exactly as before, and a fresh
// heartbeat on a COMPLETE row keeps the no-op path (no live walk to
// collide with).
func TestBackfill_ConcurrentRunHeartbeatGuard(t *testing.T) {
	now := time.Now()
	setup := func(phase ir.MigrationPhase, updatedAt time.Time) (*backfillFakeExecutor, *backfillFakeStore, *Backfiller) {
		ex := &backfillFakeExecutor{rows: backfillFakeRows(5)}
		store := newBackfillFakeStore()
		eng := &backfillFakeEngine{schema: backfillTestSchema(backfillIntPK()), ex: ex, store: store}
		b := newTestBackfiller(eng)
		b.now = func() time.Time { return now }
		id := BackfillMigrationID(b.Table, b.Sets, b.Where)
		store.headers[id] = ir.MigrationState{MigrationID: id, Phase: phase, UpdatedAt: updatedAt}
		return ex, store, b
	}

	t.Run("fresh heartbeat refuses", func(t *testing.T) {
		ex, store, b := setup(backfillPhaseRunning, now.Add(-time.Minute))
		_, err := b.Run(context.Background())
		wantBackfillCode(t, err, sluicecode.CodeBackfillConcurrentRun)
		var coded *sluicecode.CodedError
		if !errors.As(err, &coded) || !strings.Contains(coded.Hint, "wait") {
			t.Errorf("hint %q should say to wait, not name a force flag", coded.Hint)
		}
		if ex.execCalls != 0 || store.writes != 0 {
			t.Errorf("execCalls=%d store writes=%d; the refusal must touch nothing", ex.execCalls, store.writes)
		}
	})

	t.Run("fresh heartbeat refuses --restart too, keeping the state row", func(t *testing.T) {
		ex, store, b := setup(backfillPhaseRunning, now.Add(-time.Minute))
		b.Restart = true
		_, err := b.Run(context.Background())
		wantBackfillCode(t, err, sluicecode.CodeBackfillConcurrentRun)
		id := BackfillMigrationID(b.Table, b.Sets, b.Where)
		if _, ok := store.headers[id]; !ok {
			t.Error("--restart cleared the live walker's state row despite the refusal")
		}
		if ex.execCalls != 0 {
			t.Errorf("execCalls = %d; want 0", ex.execCalls)
		}
	})

	t.Run("stale heartbeat proceeds", func(t *testing.T) {
		ex, _, b := setup(backfillPhaseRunning, now.Add(-backfillHeartbeatFreshFor-time.Second))
		res, err := b.Run(context.Background())
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if res.RowsUpdated != 5 || ex.execCalls == 0 {
			t.Errorf("RowsUpdated=%d execCalls=%d; a stale (crashed-runner) row must walk", res.RowsUpdated, ex.execCalls)
		}
	})

	t.Run("fresh heartbeat on a complete row stays the no-op", func(t *testing.T) {
		ex, _, b := setup(ir.MigrationPhaseComplete, now.Add(-time.Minute))
		res, err := b.Run(context.Background())
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if !res.AlreadyComplete || ex.execCalls != 0 {
			t.Errorf("AlreadyComplete=%v execCalls=%d; want the documented no-op", res.AlreadyComplete, ex.execCalls)
		}
	})
}

// TestBackfill_RefusesSuspectLegacyCursor pins the CRITICAL-2/HIGH-1
// trust gate: a persisted cursor bearing a pre-envelope mangling
// fingerprint (U+FFFD-replaced binary bytes, float64-drifted large
// integer) must be the coded SLUICE-E-BACKFILL-CORRUPT-CURSOR refusal
// with the --restart hint, before any UPDATE runs — and --restart must
// clear it and walk clean.
func TestBackfill_RefusesSuspectLegacyCursor(t *testing.T) {
	cases := []struct {
		name   string
		cursor []any
	}{
		{"float64-drifted integer", []any{float64(1.75e18)}},
		{"U+FFFD-mangled string", []any{"��A�\x10"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ex := &backfillFakeExecutor{rows: backfillFakeRows(20)}
			store := newBackfillFakeStore()
			eng := &backfillFakeEngine{schema: backfillTestSchema(backfillIntPK()), ex: ex, store: store}
			b := newTestBackfiller(eng)
			id := BackfillMigrationID(b.Table, b.Sets, b.Where)
			store.headers[id] = ir.MigrationState{MigrationID: id, Phase: backfillPhaseRunning}
			store.progress[id] = map[string]ir.TableProgress{
				"items": {State: ir.TableProgressInProgress, LastPK: tc.cursor, RowsCopied: 10},
			}

			_, err := b.Run(context.Background())
			wantBackfillCode(t, err, sluicecode.CodeBackfillCorruptCursor)
			var coded *sluicecode.CodedError
			if !errors.As(err, &coded) || !strings.Contains(coded.Hint, "--restart") {
				t.Errorf("hint %q missing --restart", coded.Hint)
			}
			if ex.execCalls != 0 {
				t.Errorf("refusal ran %d chunk UPDATEs; must run none", ex.execCalls)
			}

			// --restart clears the poisoned state and walks clean.
			b2 := newTestBackfiller(eng)
			b2.Restart = true
			res, err := b2.Run(context.Background())
			if err != nil {
				t.Fatalf("--restart run: %v", err)
			}
			if res.RowsUpdated != 20 {
				t.Errorf("--restart RowsUpdated = %d; want 20", res.RowsUpdated)
			}
		})
	}
}

// TestBackfill_LegacyPlainIntCursorStillResumes pins the compat
// contract for live control tables: a pre-envelope plain-integer
// cursor (decoded exact via the int64-first legacy parse) resumes
// normally, no refusal.
func TestBackfill_LegacyPlainIntCursorStillResumes(t *testing.T) {
	ex := &backfillFakeExecutor{rows: backfillFakeRows(20)}
	for i := 0; i < 10; i++ {
		v := ex.rows[i].old + 1
		ex.rows[i].new = &v
	}
	store := newBackfillFakeStore()
	eng := &backfillFakeEngine{schema: backfillTestSchema(backfillIntPK()), ex: ex, store: store}
	b := newTestBackfiller(eng)
	b.BatchSize = 10
	id := BackfillMigrationID(b.Table, b.Sets, b.Where)
	// Decode the legacy wire form for real: the store's own decoder is
	// what must turn the bare 10 into an exact int64.
	var legacy ir.TableProgress
	if err := json.Unmarshal([]byte(`{"state":"in_progress","last_pk":[10],"rows_copied":10}`), &legacy); err != nil {
		t.Fatalf("decode legacy entry: %v", err)
	}
	store.headers[id] = ir.MigrationState{MigrationID: id, Phase: backfillPhaseRunning}
	store.progress[id] = map[string]ir.TableProgress{"items": legacy}

	res, err := b.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !res.Resumed {
		t.Error("Resumed = false; want true")
	}
	if len(ex.aftersSeen) == 0 || len(ex.aftersSeen[0]) != 1 || ex.aftersSeen[0][0].(int64) != 10 {
		t.Errorf("first after = %v; want [int64(10)] (the legacy cursor, exact)", ex.aftersSeen)
	}
	if res.RowsUpdated != 20 {
		t.Errorf("RowsUpdated = %d; want 20 (10 previous + 10 now)", res.RowsUpdated)
	}
}

func TestBackfill_DryRunWritesNothing(t *testing.T) {
	ex := &backfillFakeExecutor{rows: backfillFakeRows(7)}
	store := newBackfillFakeStore()
	eng := &backfillFakeEngine{schema: backfillTestSchema(backfillIntPK()), ex: ex, store: store}
	b := newTestBackfiller(eng)
	b.DryRun = true
	var out bytes.Buffer
	b.Out = &out

	res, err := b.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if ex.execCalls != 0 {
		t.Errorf("execCalls = %d; --dry-run must not update", ex.execCalls)
	}
	if store.writes != 0 {
		t.Errorf("state-store writes = %d; --dry-run must not touch the control table", store.writes)
	}
	if res.Remaining != 7 {
		t.Errorf("Remaining = %d; want 7", res.Remaining)
	}
	if res.Statement == "" || !strings.Contains(out.String(), res.Statement) {
		t.Errorf("dry-run output %q should contain the statement %q", out.String(), res.Statement)
	}
	if !strings.Contains(out.String(), "7") {
		t.Errorf("dry-run output %q should report the estimate", out.String())
	}
}

// ---- Phase 2: the --verify / --verify-only completion gate ----

func TestBackfill_VerifyCleanAfterWalk(t *testing.T) {
	ex := &backfillFakeExecutor{rows: backfillFakeRows(12)}
	eng := &backfillFakeEngine{schema: backfillTestSchema(backfillIntPK()), ex: ex, store: newBackfillFakeStore()}
	b := newTestBackfiller(eng)
	b.Verify = true
	var out bytes.Buffer
	b.Out = &out

	res, err := b.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// All rows started unfilled, so a 0-remaining verify proves the
	// count ran AFTER the walk, not before it.
	if !res.Verified || res.VerifiedRemaining != 0 {
		t.Errorf("Verified=%v VerifiedRemaining=%d; want true, 0", res.Verified, res.VerifiedRemaining)
	}
	if res.RowsUpdated != 12 {
		t.Errorf("RowsUpdated = %d; want 12 (the walk must still run)", res.RowsUpdated)
	}
	if !strings.Contains(out.String(), "safe to run the contract step") {
		t.Errorf("completion report %q missing the safe-to-contract signal", out.String())
	}
}

func TestBackfill_VerifyIncompleteIsCodedAndKeepsCompletedState(t *testing.T) {
	ex := &backfillFakeExecutor{rows: backfillFakeRows(10)}
	// A "concurrent writer" inserts a fresh row BEHIND the cursor after
	// the first chunk: the walk never revisits it, so the post-pass
	// count must catch it.
	injected := false
	ex.afterExec = func(f *backfillFakeExecutor) {
		if injected {
			return
		}
		injected = true
		f.rows = append([]backfillFakeRow{{pk: 0, old: 0}}, f.rows...)
	}
	store := newBackfillFakeStore()
	eng := &backfillFakeEngine{schema: backfillTestSchema(backfillIntPK()), ex: ex, store: store}
	b := newTestBackfiller(eng)
	b.BatchSize = 4
	b.Verify = true

	_, err := b.Run(context.Background())
	wantBackfillCode(t, err, sluicecode.CodeBackfillIncomplete)
	if !strings.Contains(err.Error(), "1 row(s)") {
		t.Errorf("err %q should name the remaining count", err)
	}
	// The walk itself succeeded: a failed verify must NOT mark the
	// migration state failed — its persisted work stands.
	id := BackfillMigrationID(b.Table, b.Sets, b.Where)
	if got := store.headers[id].Phase; got != ir.MigrationPhaseComplete {
		t.Errorf("header phase after failed verify = %s; want complete", got)
	}
	if got := store.progress[id]["items"].State; got != ir.TableProgressComplete {
		t.Errorf("table progress after failed verify = %s; want complete", got)
	}
}

func TestBackfill_VerifyRunsAfterCompletedNoOp(t *testing.T) {
	// Pre-set completed state + one fresh row: the no-op short-circuit
	// walks nothing, and the verify post-pass must still fire and
	// report the new-writes-since-completion catch-up signal.
	ex := &backfillFakeExecutor{rows: backfillFakeRows(3)}
	store := newBackfillFakeStore()
	eng := &backfillFakeEngine{schema: backfillTestSchema(backfillIntPK()), ex: ex, store: store}
	b := newTestBackfiller(eng)
	b.Verify = true
	id := BackfillMigrationID(b.Table, b.Sets, b.Where)
	store.headers[id] = ir.MigrationState{MigrationID: id, Phase: ir.MigrationPhaseComplete}

	_, err := b.Run(context.Background())
	wantBackfillCode(t, err, sluicecode.CodeBackfillIncomplete)
	if ex.execCalls != 0 {
		t.Errorf("execCalls = %d; the completed no-op must still touch no rows", ex.execCalls)
	}

	// Same shape with every row done: the no-op verifies clean.
	for i := range ex.rows {
		v := ex.rows[i].old + 1
		ex.rows[i].new = &v
	}
	res, err := b.Run(context.Background())
	if err != nil {
		t.Fatalf("Run after all rows done: %v", err)
	}
	if !res.AlreadyComplete || !res.Verified {
		t.Errorf("AlreadyComplete=%v Verified=%v; want true, true", res.AlreadyComplete, res.Verified)
	}
}

func TestBackfill_VerifyOnlyTouchesNothing(t *testing.T) {
	ex := &backfillFakeExecutor{rows: backfillFakeRows(4)}
	for i := range ex.rows {
		v := ex.rows[i].old + 1
		ex.rows[i].new = &v
	}
	store := newBackfillFakeStore()
	eng := &backfillFakeEngine{schema: backfillTestSchema(backfillIntPK()), ex: ex, store: store}
	b := newTestBackfiller(eng)
	b.VerifyOnly = true
	var out bytes.Buffer
	b.Out = &out

	res, err := b.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !res.Verified {
		t.Error("Verified = false; want true")
	}
	if ex.execCalls != 0 || store.writes != 0 {
		t.Errorf("execCalls=%d store writes=%d; --verify-only must touch neither rows nor the control table", ex.execCalls, store.writes)
	}
	if !strings.Contains(out.String(), "safe to run the contract step") {
		t.Errorf("report %q missing the safe-to-contract signal", out.String())
	}

	// The incomplete shape: still no writes, just the coded error.
	ex.rows[2].new = nil
	_, err = b.Run(context.Background())
	wantBackfillCode(t, err, sluicecode.CodeBackfillIncomplete)
	if ex.execCalls != 0 || store.writes != 0 {
		t.Errorf("execCalls=%d store writes=%d after incomplete verify; must stay 0", ex.execCalls, store.writes)
	}
}

func TestBackfill_VerifyOnlyNeedsNoPKAndNoSets(t *testing.T) {
	// A verify-only run issues no bounded UPDATEs, so a no-PK table and
	// an absent --set are both fine — the walk's refusals don't apply.
	ex := &backfillFakeExecutor{rows: backfillFakeRows(2)}
	for i := range ex.rows {
		v := ex.rows[i].old + 1
		ex.rows[i].new = &v
	}
	eng := &backfillFakeEngine{schema: backfillTestSchema(nil), ex: ex, store: newBackfillFakeStore()}
	b := newTestBackfiller(eng)
	b.VerifyOnly = true
	b.Sets = nil

	res, err := b.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !res.Verified {
		t.Error("Verified = false; want true")
	}
}

func TestBackfill_VerifyOnlyStillRefusesUnknownSetColumn(t *testing.T) {
	eng := &backfillFakeEngine{schema: backfillTestSchema(backfillIntPK()), ex: &backfillFakeExecutor{}, store: newBackfillFakeStore()}
	b := newTestBackfiller(eng)
	b.VerifyOnly = true
	b.Sets = []ir.BackfillSet{{Column: "nope", Expr: "1"}}
	_, err := b.Run(context.Background())
	wantBackfillCode(t, err, sluicecode.CodeBackfillUnknownColumn)
}

func TestBackfill_VerifyModesRequireWhere(t *testing.T) {
	for _, tc := range []struct {
		name string
		mut  func(*Backfiller)
	}{
		{"--verify", func(b *Backfiller) { b.Verify = true }},
		{"--verify-only", func(b *Backfiller) { b.VerifyOnly = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newTestBackfiller(stubEngine{})
			b.Where = ""
			tc.mut(b)
			if _, err := b.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "--where") {
				t.Errorf("err = %v; want the --where-required refusal", err)
			}
		})
	}
}

func TestBackfill_VerifyContradictoryCombos(t *testing.T) {
	for _, tc := range []struct {
		name string
		mut  func(*Backfiller)
	}{
		{"--verify-only with --dry-run", func(b *Backfiller) { b.VerifyOnly = true; b.DryRun = true }},
		{"--verify-only with --restart", func(b *Backfiller) { b.VerifyOnly = true; b.Restart = true }},
		{"--verify with --dry-run", func(b *Backfiller) { b.Verify = true; b.DryRun = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newTestBackfiller(stubEngine{})
			tc.mut(b)
			if _, err := b.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "contradictory") {
				t.Errorf("err = %v; want a contradictory-flags refusal", err)
			}
		})
	}
}

// TestBackfill_UnsupportedEngineWinsOverMissingTable pins the Phase-2
// refusal-order hoist: an engine with no backfill surface gets the
// coded refusal even when the table is ALSO missing — the coded,
// operator-actionable answer must not be shadowed by an uncoded
// table-not-found from the schema read.
func TestBackfill_UnsupportedEngineWinsOverMissingTable(t *testing.T) {
	eng := &noBackfillEngine{schema: backfillTestSchema(backfillIntPK())}
	b := newTestBackfiller(eng)
	b.Table = "absent"
	_, err := b.Run(context.Background())
	wantBackfillCode(t, err, sluicecode.CodeBackfillUnsupportedEngine)
}

func TestBackfill_ValidateRequiredFields(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*Backfiller)
	}{
		{"nil engine", func(b *Backfiller) { b.Engine = nil }},
		{"empty dsn", func(b *Backfiller) { b.DSN = "" }},
		{"empty table", func(b *Backfiller) { b.Table = "" }},
		{"no sets", func(b *Backfiller) { b.Sets = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := newTestBackfiller(stubEngine{})
			tc.mut(b)
			if _, err := b.Run(context.Background()); err == nil {
				t.Error("want validation error; got nil")
			}
		})
	}
}
