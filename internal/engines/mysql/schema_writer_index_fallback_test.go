// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the ADR-0148 deploy-request index-build fallback (engine
// half): the walled-error classifier, the route-vs-surface decision tree
// on BOTH deferred-build call sites (whole-schema CreateIndexes and the
// VStream serial overlap), the still-pending re-probe that excludes
// already-built indexes, the per-table one-call batching, and the
// DATA_LENGTH skip-the-doomed-attempt probe with its
// unavailable-falls-through-to-direct contract.

package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

// ---- fakes ----

// fbRecorder scripts the fallback tests' fake MySQL: per-substring ALTER
// failures, catalog probes answered from the SUCCESSFUL ALTERs (plus a
// static pre-existing set), and a per-table DATA_LENGTH answer.
type fbRecorder struct {
	mu sync.Mutex
	// execs records every attempted ALTER; okExecs only the successful
	// ones (the catalog answer source — a failed ALTER must not probe as
	// existing).
	execs   []string
	okExecs []string
	// failContains maps a substring (index or table name, backtick-quoted
	// by the caller's choice) to the error every matching ALTER returns.
	failContains map[string]error
	// preExisting index names probe as existing before any ALTER runs.
	preExisting map[string]bool
	// dataLength answers the information_schema DATA_LENGTH probe, keyed
	// by table name.
	dataLength map[string]int64
}

func (r *fbRecorder) recordExec(q string) (fail error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.execs = append(r.execs, q)
	for sub, err := range r.failContains {
		if err != nil && strings.Contains(q, sub) {
			return err
		}
	}
	r.okExecs = append(r.okExecs, q)
	return nil
}

func (r *fbRecorder) nameExists(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.preExisting[name] {
		return true
	}
	for _, q := range r.okExecs {
		if strings.Contains(q, "`"+name+"`") {
			return true
		}
	}
	return false
}

func (r *fbRecorder) alterCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.execs)
}

func (r *fbRecorder) tableBytes(table string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.dataLength[table]
}

type fbDriver struct{ rec *fbRecorder }

func (d fbDriver) Open(string) (driver.Conn, error) { return fbConn(d), nil }

type fbConn struct{ rec *fbRecorder }

func (fbConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (fbConn) Close() error                        { return nil }
func (fbConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }

func (c fbConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	if strings.HasPrefix(strings.TrimSpace(query), "ALTER") {
		if err := c.rec.recordExec(query); err != nil {
			return nil, err
		}
	}
	return driver.RowsAffected(0), nil
}

// QueryContext serves the three probe shapes the fallback paths issue:
// the DATA_LENGTH pre-probe, the batched catalog-pair probe (reusing the
// overlap harness's batchedProbeShape/pairRows), and — defensively — a
// one-row bool for anything else.
func (c fbConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if strings.Contains(query, "DATA_LENGTH") {
		table := ""
		if len(args) >= 2 {
			table, _ = args[1].Value.(string)
		}
		return &boolRow{val: c.rec.tableBytes(table)}, nil
	}
	if nTables, nNames, ok := batchedProbeShape(query); ok {
		argStr := func(i int) string {
			s, _ := args[i].Value.(string)
			return s
		}
		var pairs [][2]string
		for t := 0; t < nTables; t++ {
			for n := 0; n < nNames; n++ {
				table, name := argStr(1+t), argStr(1+nTables+n)
				if c.rec.nameExists(name) {
					pairs = append(pairs, [2]string{table, name})
				}
			}
		}
		return &pairRows{pairs: pairs}, nil
	}
	return &boolRow{val: 0}, nil
}

func newFBFakeDB(t *testing.T, rec *fbRecorder) *sql.DB {
	t.Helper()
	name := fmt.Sprintf("sluice-index-fallback-fake-%s-%d", t.Name(), indexFakeSeq.Add(1))
	sql.Register(name, fbDriver{rec: rec})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("open fallback fake db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// fallbackCall is one recorded BuildIndexDDL invocation.
type fallbackCall struct {
	table string
	ddls  []string
	cause error
}

// recordingIndexFallback is the fake ir.IndexBuildFallback.
type recordingIndexFallback struct {
	mu    sync.Mutex
	calls []fallbackCall
	err   error
}

func (f *recordingIndexFallback) BuildIndexDDL(_ context.Context, table string, ddls []string, cause error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fallbackCall{table: table, ddls: append([]string(nil), ddls...), cause: cause})
	return f.err
}

func (f *recordingIndexFallback) snapshot() []fallbackCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fallbackCall(nil), f.calls...)
}

// ---- fixtures ----

func fbSchemaOneTable(indexes ...*ir.Index) *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Name: "orders",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "val", Type: ir.Integer{Width: 64}},
			{Name: "body", Type: ir.Text{}},
		},
		Indexes: indexes,
	}}}
}

func fbBTreeIdx(name, col string) *ir.Index {
	return &ir.Index{Name: name, Columns: []ir.IndexColumn{{Column: col}}}
}

func fbFulltextIdx(name, col string) *ir.Index {
	return &ir.Index{Name: name, Columns: []ir.IndexColumn{{Column: col}}, Kind: ir.IndexKindFullText}
}

func err3024() error {
	return &gomysql.MySQLError{Number: 3024, Message: "Query execution was interrupted, maximum statement execution time exceeded"}
}

func err1105DirectDDL() error {
	return &gomysql.MySQLError{Number: 1105, Message: "direct DDL is disabled"}
}

func newFallbackWriter(t *testing.T, rec *fbRecorder, fb ir.IndexBuildFallback) *SchemaWriter {
	t.Helper()
	if rec.failContains == nil {
		rec.failContains = map[string]error{}
	}
	if rec.preExisting == nil {
		rec.preExisting = map[string]bool{}
	}
	if rec.dataLength == nil {
		rec.dataLength = map[string]int64{}
	}
	return &SchemaWriter{
		db:                 newFBFakeDB(t, rec),
		schema:             "testdb",
		flavor:             FlavorPlanetScale,
		indexBuildFallback: fb,
	}
}

// ---- classifier ----

// TestIsIndexBuildWalled pins the two-shape classifier: errno 3024 always,
// errno 1105 only with the safe-migrations wording — everything else
// (transient 1105s, real DDL faults, non-MySQL errors) stays out so the
// reparent retry / loud failure keep their lanes.
func TestIsIndexBuildWalled(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"3024 bare", err3024(), true},
		{"3024 wrapped", fmt.Errorf("mysql: create indexes on %q: %w", "t", err3024()), true},
		{"1105 direct DDL disabled", err1105DirectDDL(), true},
		{"1105 direct DDL disabled, wrapped by wrapDDLError", wrapDDLError(err1105DirectDDL()), true},
		{"1105 direct DDL disabled, mixed case", &gomysql.MySQLError{Number: 1105, Message: "Direct DDL is Disabled"}, true},
		{"1105 vttablet transient stays out", &gomysql.MySQLError{Number: 1105, Message: "vttablet: rpc error: code = Unavailable desc = not serving"}, false},
		{"1061 duplicate key name stays out", &gomysql.MySQLError{Number: 1061, Message: "Duplicate key name 'idx'"}, false},
		{"plain error stays out", errors.New("maximum statement execution time exceeded"), false},
		{"nil stays out", nil, false},
	}
	for _, c := range cases {
		if got := isIndexBuildWalled(c.err); got != c.want {
			t.Errorf("%s: isIndexBuildWalled = %v; want %v", c.name, got, c.want)
		}
	}
}

// ---- whole-schema CreateIndexes call site ----

// TestCreateIndexes_WalledErrorRoutesToDeployFallback pins the load-bearing
// path: a 3024 on the direct ALTER routes the table's pending index DDL to
// the fallback (one call, cause = the walled error) and the phase succeeds.
func TestCreateIndexes_WalledErrorRoutesToDeployFallback(t *testing.T) {
	rec := &fbRecorder{failContains: map[string]error{"`idx_val`": err3024()}}
	fb := &recordingIndexFallback{}
	w := newFallbackWriter(t, rec, fb)

	if err := w.CreateIndexes(context.Background(), fbSchemaOneTable(fbBTreeIdx("idx_val", "val"))); err != nil {
		t.Fatalf("CreateIndexes = %v; want nil (fallback recovered)", err)
	}
	calls := fb.snapshot()
	if len(calls) != 1 {
		t.Fatalf("fallback calls = %d; want 1", len(calls))
	}
	if calls[0].table != "orders" {
		t.Errorf("fallback table = %q; want orders", calls[0].table)
	}
	if len(calls[0].ddls) != 1 || !strings.Contains(calls[0].ddls[0], "ADD INDEX `idx_val`") {
		t.Errorf("fallback ddls = %q; want one ALTER carrying idx_val", calls[0].ddls)
	}
	var mysqlErr *gomysql.MySQLError
	if !errors.As(calls[0].cause, &mysqlErr) || mysqlErr.Number != 3024 {
		t.Errorf("fallback cause = %v; want the errno-3024 direct failure", calls[0].cause)
	}
}

// TestCreateIndexes_DirectDDLDisabledBatchesOneCallPerTable pins BOTH the
// 1105 trigger class AND the batching contract: a table whose direct build
// dies on its FIRST statement hands ALL its pending index DDL — the
// combined BTREE ALTER and the separate FULLTEXT ALTER — to the fallback
// in ONE call (one dev branch / deploy request per table).
func TestCreateIndexes_DirectDDLDisabledBatchesOneCallPerTable(t *testing.T) {
	rec := &fbRecorder{failContains: map[string]error{"ALTER": err1105DirectDDL()}}
	fb := &recordingIndexFallback{}
	w := newFallbackWriter(t, rec, fb)

	schema := fbSchemaOneTable(fbBTreeIdx("idx_a", "id"), fbBTreeIdx("idx_b", "val"), fbFulltextIdx("ft_body", "body"))
	if err := w.CreateIndexes(context.Background(), schema); err != nil {
		t.Fatalf("CreateIndexes = %v; want nil", err)
	}
	calls := fb.snapshot()
	if len(calls) != 1 {
		t.Fatalf("fallback calls = %d; want exactly 1 (one branch/DR per table)", len(calls))
	}
	ddls := calls[0].ddls
	if len(ddls) != 2 {
		t.Fatalf("fallback ddls = %d statements %q; want 2 (combined BTREE + separate FULLTEXT)", len(ddls), ddls)
	}
	joined := strings.Join(ddls, "\n")
	for _, name := range []string{"idx_a", "idx_b", "ft_body"} {
		if !strings.Contains(joined, "`"+name+"`") {
			t.Errorf("fallback ddls missing index %q: %q", name, ddls)
		}
	}
}

// TestCreateIndexes_ReprobeExcludesAlreadyBuilt pins the re-derivation:
// when the combined BTREE ALTER lands and only the FULLTEXT statement hits
// the wall, the fallback receives ONLY the still-pending FULLTEXT DDL —
// never a re-send of what already built.
func TestCreateIndexes_ReprobeExcludesAlreadyBuilt(t *testing.T) {
	rec := &fbRecorder{failContains: map[string]error{"`ft_body`": err3024()}}
	fb := &recordingIndexFallback{}
	w := newFallbackWriter(t, rec, fb)

	schema := fbSchemaOneTable(fbBTreeIdx("idx_a", "id"), fbBTreeIdx("idx_b", "val"), fbFulltextIdx("ft_body", "body"))
	if err := w.CreateIndexes(context.Background(), schema); err != nil {
		t.Fatalf("CreateIndexes = %v; want nil", err)
	}
	calls := fb.snapshot()
	if len(calls) != 1 {
		t.Fatalf("fallback calls = %d; want 1", len(calls))
	}
	ddls := calls[0].ddls
	if len(ddls) != 1 || !strings.Contains(ddls[0], "`ft_body`") {
		t.Fatalf("fallback ddls = %q; want only the pending FULLTEXT ALTER", ddls)
	}
	if strings.Contains(ddls[0], "`idx_a`") || strings.Contains(ddls[0], "`idx_b`") {
		t.Errorf("fallback ddls re-send already-built indexes: %q", ddls)
	}
}

// TestCreateIndexes_NilFallbackKeepsOriginalError pins the no-token path:
// with no fallback injected the walled error surfaces unchanged (so the
// registered errno-3024 --upfront-indexes/--resume hint keeps firing on
// the exact same error text).
func TestCreateIndexes_NilFallbackKeepsOriginalError(t *testing.T) {
	rec := &fbRecorder{failContains: map[string]error{"`idx_val`": err3024()}}
	w := newFallbackWriter(t, rec, nil)

	err := w.CreateIndexes(context.Background(), fbSchemaOneTable(fbBTreeIdx("idx_val", "val")))
	var mysqlErr *gomysql.MySQLError
	if !errors.As(err, &mysqlErr) || mysqlErr.Number != 3024 {
		t.Fatalf("CreateIndexes = %v; want the original errno-3024 error", err)
	}
	if !strings.Contains(err.Error(), "maximum statement execution time") {
		t.Errorf("error text lost the hint-matching wording: %v", err)
	}
}

// TestCreateIndexes_FallbackUnavailableSurfacesOriginalError pins the
// refuse/hint-when-off contract: an unavailable fallback (safe migrations
// off, bad token) must surface the ORIGINAL direct error — text and typed
// chain intact — never its own.
func TestCreateIndexes_FallbackUnavailableSurfacesOriginalError(t *testing.T) {
	rec := &fbRecorder{failContains: map[string]error{"`idx_val`": err3024()}}
	fb := &recordingIndexFallback{err: fmt.Errorf("%w: safe migrations is off", ir.ErrIndexBuildFallbackUnavailable)}
	w := newFallbackWriter(t, rec, fb)

	err := w.CreateIndexes(context.Background(), fbSchemaOneTable(fbBTreeIdx("idx_val", "val")))
	var mysqlErr *gomysql.MySQLError
	if !errors.As(err, &mysqlErr) || mysqlErr.Number != 3024 {
		t.Fatalf("CreateIndexes = %v; want the original errno-3024 error", err)
	}
	if errors.Is(err, ir.ErrIndexBuildFallbackUnavailable) {
		t.Errorf("the unavailability sentinel leaked into the surfaced error: %v", err)
	}
	if len(fb.snapshot()) != 1 {
		t.Errorf("fallback calls = %d; want 1 (it was consulted, then declined)", len(fb.snapshot()))
	}
}

// TestCreateIndexes_FallbackFailureSurfacesCodedError pins the loud
// deploy-request failure: a real fallback error (not unavailability) fails
// the phase, wrapping the fallback's error and naming the direct failure
// it was recovering from.
func TestCreateIndexes_FallbackFailureSurfacesCodedError(t *testing.T) {
	drErr := errors.New("deploy request #7 failed (deployment_state \"error\")")
	rec := &fbRecorder{failContains: map[string]error{"`idx_val`": err3024()}}
	fb := &recordingIndexFallback{err: drErr}
	w := newFallbackWriter(t, rec, fb)

	err := w.CreateIndexes(context.Background(), fbSchemaOneTable(fbBTreeIdx("idx_val", "val")))
	if !errors.Is(err, drErr) {
		t.Fatalf("CreateIndexes = %v; want the fallback's error in the chain", err)
	}
	if !strings.Contains(err.Error(), "maximum statement execution time") {
		t.Errorf("error should name the direct failure it was recovering from: %v", err)
	}
}

// TestCreateIndexes_NonWalledErrorNeverRoutes pins the classifier gate at
// the call site: a real DDL fault (1061 duplicate key name) fails loudly
// without consulting the fallback.
func TestCreateIndexes_NonWalledErrorNeverRoutes(t *testing.T) {
	dup := &gomysql.MySQLError{Number: 1061, Message: "Duplicate key name 'idx_val'"}
	rec := &fbRecorder{failContains: map[string]error{"`idx_val`": dup}}
	fb := &recordingIndexFallback{}
	w := newFallbackWriter(t, rec, fb)

	err := w.CreateIndexes(context.Background(), fbSchemaOneTable(fbBTreeIdx("idx_val", "val")))
	var mysqlErr *gomysql.MySQLError
	if !errors.As(err, &mysqlErr) || mysqlErr.Number != 1061 {
		t.Fatalf("CreateIndexes = %v; want the 1061 fault", err)
	}
	if len(fb.snapshot()) != 0 {
		t.Errorf("fallback was consulted for a non-walled error")
	}
}

// ---- DATA_LENGTH pre-probe ----

// TestCreateIndexes_HugeTableSkipsDirectAttempt pins the recorded ADR-0148
// optimization: a fallback-armed table past the conservative DATA_LENGTH
// threshold routes straight to the deploy request — zero direct ALTERs,
// cause == nil (preemptive).
func TestCreateIndexes_HugeTableSkipsDirectAttempt(t *testing.T) {
	rec := &fbRecorder{dataLength: map[string]int64{"orders": indexFallbackHugeTableBytes}}
	fb := &recordingIndexFallback{}
	w := newFallbackWriter(t, rec, fb)

	if err := w.CreateIndexes(context.Background(), fbSchemaOneTable(fbBTreeIdx("idx_val", "val"))); err != nil {
		t.Fatalf("CreateIndexes = %v; want nil", err)
	}
	if n := rec.alterCount(); n != 0 {
		t.Errorf("direct ALTERs = %d; want 0 (the doomed attempt must be skipped)", n)
	}
	calls := fb.snapshot()
	if len(calls) != 1 || calls[0].cause != nil {
		t.Fatalf("fallback calls = %+v; want one preemptive call with nil cause", calls)
	}
}

// TestCreateIndexes_HugeTableUnavailableFallsThroughToDirect pins the
// never-worse contract on the probe path: when the fallback declines, the
// direct attempt still runs (and here succeeds).
func TestCreateIndexes_HugeTableUnavailableFallsThroughToDirect(t *testing.T) {
	rec := &fbRecorder{dataLength: map[string]int64{"orders": indexFallbackHugeTableBytes * 2}}
	fb := &recordingIndexFallback{err: fmt.Errorf("%w: no safe migrations", ir.ErrIndexBuildFallbackUnavailable)}
	w := newFallbackWriter(t, rec, fb)

	if err := w.CreateIndexes(context.Background(), fbSchemaOneTable(fbBTreeIdx("idx_val", "val"))); err != nil {
		t.Fatalf("CreateIndexes = %v; want nil (direct attempt succeeded after the fallback declined)", err)
	}
	if n := rec.alterCount(); n != 1 {
		t.Errorf("direct ALTERs = %d; want 1", n)
	}
}

// TestCreateIndexes_BelowThresholdAttemptsDirectFirst pins the routing
// boundary: under the threshold the direct build runs first and, on
// success, the fallback is never consulted.
func TestCreateIndexes_BelowThresholdAttemptsDirectFirst(t *testing.T) {
	rec := &fbRecorder{dataLength: map[string]int64{"orders": indexFallbackHugeTableBytes - 1}}
	fb := &recordingIndexFallback{}
	w := newFallbackWriter(t, rec, fb)

	if err := w.CreateIndexes(context.Background(), fbSchemaOneTable(fbBTreeIdx("idx_val", "val"))); err != nil {
		t.Fatalf("CreateIndexes = %v; want nil", err)
	}
	if n := rec.alterCount(); n != 1 {
		t.Errorf("direct ALTERs = %d; want 1", n)
	}
	if len(fb.snapshot()) != 0 {
		t.Errorf("fallback consulted despite a healthy direct build")
	}
}

// ---- the VStream serial overlap call site ----

// TestVStreamSerialOverlap_WalledRoutesToDeployFallback pins the SECOND
// deferred-build call site — buildEachAsCopiedSerial, the path every
// production PlanetScale-target migrate actually takes — so the fallback
// coverage is the call-site class, not one representative. Build-then-mark
// must hold: the per-table IndexesBuilt callback fires after the fallback
// recovery, exactly as after a direct build.
func TestVStreamSerialOverlap_WalledRoutesToDeployFallback(t *testing.T) {
	rec := &fbRecorder{failContains: map[string]error{"`idx_val`": err1105DirectDDL()}}
	fb := &recordingIndexFallback{}
	w := newFallbackWriter(t, rec, fb)

	var markedMu sync.Mutex
	var marked []string
	w.SetTableIndexedCallback(func(table *ir.Table) {
		markedMu.Lock()
		marked = append(marked, table.Name)
		markedMu.Unlock()
	})

	schema := fbSchemaOneTable(fbBTreeIdx("idx_val", "val"))
	ch := make(chan *ir.Table, 1)
	ch <- schema.Tables[0]
	close(ch)
	if err := w.BuildTableIndexesFromChannel(context.Background(), schema, ch); err != nil {
		t.Fatalf("BuildTableIndexesFromChannel = %v; want nil (fallback recovered)", err)
	}
	if calls := fb.snapshot(); len(calls) != 1 || calls[0].table != "orders" {
		t.Fatalf("fallback calls = %+v; want one for orders", calls)
	}
	markedMu.Lock()
	defer markedMu.Unlock()
	if len(marked) != 1 || marked[0] != "orders" {
		t.Errorf("IndexesBuilt callbacks = %v; want [orders] (build-then-mark after fallback recovery)", marked)
	}
}
