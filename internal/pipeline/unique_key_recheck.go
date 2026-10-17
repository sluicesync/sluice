// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// The unique-key door's mid-stream checks (UNIQUE-KEY-STOPPED-WINDOW-DOOR);
// the stream-open half and the rule are in unique_key_door.go.

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// uniqueKeyHoldCap bounds how many changes of one transaction the periodic
// re-check holds while it waits to learn whether the transaction touches a
// watched table: past it, the re-check runs anyway and the held changes go
// on.
const uniqueKeyHoldCap = 1024

// uniqueKeyChecks configures [interceptUniqueKeyChecks].
type uniqueKeyChecks struct {
	// period and periodic arm the periodic re-check (nil periodic: none —
	// a source that sees unique-key DDL live). watches names the tables
	// it covers.
	period   time.Duration
	periodic func(context.Context) (*ir.SourceUniqueKeyAbsentError, error)
	watches  func(schema, table string) bool
	// boundary checks one table at a schema boundary, forwarded or not
	// (nil: none). seedCols holds each table's column list as the
	// stream-open door read it, keyed by colsKey; a snapshot is checked when
	// its column list differs from the last one known for its table,
	// starting from that seed.
	boundary    func(ctx context.Context, schema, table string) (*ir.SourceUniqueKeyAbsentError, error)
	seedCols    map[string]string
	colsKey     func(schema, table string) string
	maxFailures int
}

func (c uniqueKeyChecks) armed() bool { return c.periodic != nil || c.boundary != nil }

// interceptUniqueKeyChecks runs the door mid-stream, on the change channel
// AFTER the schema-forward intercepts (so a forwarded ALTER is already on
// the target when its SchemaSnapshot passes):
//
//   - PERIODIC (a source whose change stream carries no DDL): once a period
//     has passed, the next transaction is HELD from its TxBegin until it
//     either touches a watched table (the re-check runs, then the held
//     changes go on) or commits without touching one (they go on unchecked:
//     a transaction that writes no watched table cannot merge through one,
//     and an idle or heartbeat-only stream costs nothing). A row change
//     outside a transaction is checked before it is forwarded. A refusal is
//     stored and the channel closed before the transaction's first change is
//     forwarded, so the stream stops between source transactions.
//   - BOUNDARY (every source): a SchemaSnapshot whose column list differs
//     from the last one known for its table runs the door for that table
//     before the snapshot is forwarded. "Known" starts from the stream-open
//     door's own read of the table, so the table's FIRST snapshot of the
//     run is checked too when it differs from that read — pgoutput emits a
//     snapshot at each table's first Relation of a session, so a DROP COLUMN
//     made before the table's first change in the run arrives exactly there
//     (round-2 review HIGH-1: skipping first snapshots let such a DROP
//     COLUMN — the first boundary after a cold start, which schema-forward
//     does not forward, so the target kept the column and its key — merge
//     a keyless duplicate onto another row at exit 0). A
//     table the open door did not read (a live-added one) is checked from
//     its second snapshot on.
//
// A periodic catalog-read failure WARNs ([SourceUniqueKeyRecheckFailedMarker])
// and the next due check tries again; maxFailures consecutive failures stop
// the stream with that marker (restartable — the next start's open door reads
// again). A BOUNDARY read failure stops the stream at once (same marker,
// restartable, outside that budget): the boundary is checked once, and a
// pgoutput or binlog stream has no periodic re-check to catch it later. A
// cancelled context stops quietly.
//
// # The window it leaves, stated
//
// The re-check runs AFTER the change it precedes was read from the source,
// so a source DROP committed before that change is visible to it. What it
// does not cover is the transactions forwarded between the DROP and the next
// due re-check — up to one period of them. For the shapes the apply guard
// sees (a table with a primary key, or with two or more unique keys and rows
// differing in a non-NULL key column) those still refuse at their first
// colliding row; for the two it cannot see (no primary key and one unique
// key; rows differing only in NULL or partial-predicate columns) a merge in
// that window is applied, and the re-check's refusal then names a re-copy.
// The trigger engines refuse a keyless table at setup, so on them that arm
// cannot occur; VStream can carry one.
func interceptUniqueKeyChecks(
	ctx context.Context,
	in <-chan ir.Change,
	cfg uniqueKeyChecks,
	errStore *atomic.Pointer[error],
	now func() time.Time,
) <-chan ir.Change {
	if !cfg.armed() {
		return in
	}
	out := make(chan ir.Change, migcore.RowChanBuffer)
	prevCols := make(map[string]string, len(cfg.seedCols))
	for k, v := range cfg.seedCols {
		prevCols[k] = v
	}
	r := &uniqueKeyRunner{cfg: cfg, errStore: errStore, now: now, out: out, last: now(), prevCols: prevCols}
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case c, ok := <-in:
				if !ok {
					return
				}
				if !r.step(ctx, c) {
					return
				}
			}
		}
	}()
	return out
}

// uniqueKeyRunner is the intercept's state, owned by its one goroutine.
type uniqueKeyRunner struct {
	cfg      uniqueKeyChecks
	errStore *atomic.Pointer[error]
	now      func() time.Time
	out      chan<- ir.Change

	last     time.Time
	failures int
	held     []ir.Change
	holding  bool
	inTx     bool
	prevCols map[string]string
}

// step handles one change; false ends the intercept.
func (r *uniqueKeyRunner) step(ctx context.Context, c ir.Change) bool {
	if r.holding {
		r.held = append(r.held, c)
		if isTxCommit(c) {
			// Whichever arm below releases the hold, the transaction is over
			// (the cap can land on its commit; round-2 review L-b).
			r.inTx = false
		}
		switch {
		case r.touchesWatched(c) || len(r.held) >= uniqueKeyHoldCap:
			if !r.runPeriodic(ctx) {
				return false
			}
			return r.flush(ctx)
		case isTxCommit(c):
			return r.flush(ctx)
		}
		return true
	}
	switch c.(type) {
	case ir.TxBegin:
		r.inTx = true
		if r.periodicDue() {
			r.holding, r.held = true, []ir.Change{c}
			return true
		}
	case ir.TxCommit:
		r.inTx = false
	default:
		if !r.inTx && r.periodicDue() && r.touchesWatched(c) && !r.runPeriodic(ctx) {
			return false
		}
	}
	return r.emit(ctx, c)
}

func isTxCommit(c ir.Change) bool {
	_, ok := c.(ir.TxCommit)
	return ok
}

func (r *uniqueKeyRunner) periodicDue() bool {
	return r.cfg.periodic != nil && r.now().Sub(r.last) >= r.cfg.period
}

// touchesWatched reports a row change on a table the periodic re-check
// covers.
func (r *uniqueKeyRunner) touchesWatched(c ir.Change) bool {
	if r.cfg.watches == nil {
		return false
	}
	switch e := c.(type) {
	case ir.Insert:
		return r.cfg.watches(e.Schema, e.Table)
	case ir.Update:
		return r.cfg.watches(e.Schema, e.Table)
	case ir.Delete:
		return r.cfg.watches(e.Schema, e.Table)
	}
	return false
}

func (r *uniqueKeyRunner) flush(ctx context.Context) bool {
	held := r.held
	r.held, r.holding = nil, false
	for _, h := range held {
		if !r.emit(ctx, h) {
			return false
		}
	}
	return true
}

// emit forwards c, running the boundary check first for a SchemaSnapshot
// whose column list moved.
func (r *uniqueKeyRunner) emit(ctx context.Context, c ir.Change) bool {
	if snap, ok := c.(ir.SchemaSnapshot); ok && r.cfg.boundary != nil && snap.IR != nil {
		key := snap.QualifiedName()
		if r.cfg.colsKey != nil {
			key = r.cfg.colsKey(snap.Schema, snap.Table)
		}
		cols := columnList(snap.IR)
		prev, seen := r.prevCols[key]
		if seen && prev != cols {
			refusal, err := r.cfg.boundary(ctx, snap.Schema, snap.Table)
			if !r.settleBoundary(ctx, snap.QualifiedName(), refusal, err) {
				return false
			}
		}
		r.prevCols[key] = cols
	}
	return forwardChange(ctx, r.out, c)
}

// settleBoundary acts on a boundary check's result. A read failure stops
// the stream (restartable) instead of WARNing: the boundary is judged once,
// and forwarding past it unjudged would leave the table unwatched for the
// rest of the run on a source with no periodic re-check (round-2 review
// MEDIUM-1). It is not counted toward the periodic failure budget.
func (r *uniqueKeyRunner) settleBoundary(ctx context.Context, table string, refusal *ir.SourceUniqueKeyAbsentError, err error) bool {
	switch {
	case ctx.Err() != nil:
		return false
	case err != nil:
		stop := fmt.Errorf("%s: the unique-key check after a schema change of %s could not read a catalog, so the "+
			"change cannot be judged and the stream stops before anything after it is applied; the next start "+
			"compares the catalogs again: %w", SourceUniqueKeyRecheckFailedMarker, table, err)
		r.errStore.Store(&stop)
		return false
	case refusal != nil:
		var e error = refusal
		r.errStore.Store(&e)
		return false
	}
	return true
}

func columnList(t *ir.Table) string {
	names := make([]string, 0, len(t.Columns))
	for _, c := range t.Columns {
		if c != nil {
			names = append(names, c.Name)
		}
	}
	sort.Strings(names)
	return strings.Join(names, "\x00")
}

func (r *uniqueKeyRunner) runPeriodic(ctx context.Context) bool {
	r.last = r.now()
	refusal, err := r.cfg.periodic(ctx)
	return r.settle(ctx, refusal, err)
}

// settle acts on one check's result; false ends the intercept.
func (r *uniqueKeyRunner) settle(ctx context.Context, refusal *ir.SourceUniqueKeyAbsentError, err error) bool {
	switch {
	case ctx.Err() != nil:
		return false
	case err != nil:
		r.failures++
		if r.failures >= r.cfg.maxFailures {
			stop := fmt.Errorf("%s: the unique-key re-check could not read a catalog %d times in a row, and on this "+
				"source it is the only mid-stream protection against a unique key the source dropped, so the stream "+
				"stops rather than run unwatched; the next start compares the catalogs again: %w",
				SourceUniqueKeyRecheckFailedMarker, r.failures, err)
			r.errStore.Store(&stop)
			return false
		}
		slog.WarnContext(ctx, SourceUniqueKeyRecheckFailedMarker+": a mid-stream re-check of the target's unique keys "+
			"against the source's could not read a catalog; the next due check tries again",
			slog.Int("consecutive_failures", r.failures),
			slog.Int("stops_after", r.cfg.maxFailures),
			slog.String("err", err.Error()))
		return true
	case refusal != nil:
		var e error = refusal
		r.errStore.Store(&e)
		return false
	}
	r.failures = 0
	return true
}

// uniqueKeyChecksFor builds the stream's mid-stream checks from what the
// stream-open door recorded; a zero value (nothing armed) when it recorded
// nothing.
func (s *Streamer) uniqueKeyChecksFor() uniqueKeyChecks {
	door := s.uniqueKeyDoor
	if door == nil {
		return uniqueKeyChecks{}
	}
	cfg := uniqueKeyChecks{period: s.uniqueKeyRecheckPeriod(), maxFailures: uniqueKeyRecheckMaxFailures}
	multi := len(door.scopes) > 1 || (len(door.scopes) == 1 && door.scopes[0].namespace != "")
	cfg.seedCols = door.openCols
	cfg.colsKey = func(schema, table string) string {
		if !multi {
			schema = ""
		}
		return uniqueKeyColsKey(schema, table)
	}
	cfg.boundary = func(ctx context.Context, schema, table string) (*ir.SourceUniqueKeyAbsentError, error) {
		i := door.scopeFor(schema)
		absent, _, err := s.compareUniqueKeys(ctx, door.scopes, uniqueKeyCompare{
			only: map[int][]string{i: {table}}, collations: s.uniqueKeyCollationsArmed(false),
		})
		if err != nil || len(absent) == 0 {
			return nil, err
		}
		return s.uniqueKeyRefusal(absent, ir.UniqueKeyDoorAtSchemaChange), nil
	}
	if !door.periodic {
		return cfg
	}
	watched := map[string]bool{}
	for i, tables := range door.watched {
		for _, t := range tables {
			watched[door.scopes[i].namespace+"\x00"+strings.ToLower(t)] = true
		}
	}
	cfg.watches = func(schema, table string) bool {
		if !multi {
			schema = ""
		}
		return watched[schema+"\x00"+strings.ToLower(table)]
	}
	cfg.periodic = func(ctx context.Context) (*ir.SourceUniqueKeyAbsentError, error) {
		absent, _, err := s.compareUniqueKeys(ctx, door.scopes, uniqueKeyCompare{only: door.watched, collations: s.uniqueKeyCollationsArmed(false)})
		if err != nil || len(absent) == 0 {
			return nil, err
		}
		return s.uniqueKeyRefusal(absent, ir.UniqueKeyDoorMidStream), nil
	}
	return cfg
}

// scopeFor returns the scope index of a source namespace (0 on a
// single-namespace stream).
func (d *uniqueKeyDoorState) scopeFor(schema string) int {
	for i, sc := range d.scopes {
		if sc.namespace != "" && strings.EqualFold(sc.namespace, schema) {
			return i
		}
	}
	return 0
}
