// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import "sync"

// RunSummary is an optional, concurrency-safe collector for the
// end-of-run per-table facts the CLI's `--format json` result envelope
// renders (docs/research/ai-friendly-sluice.md recommendation #2). The
// orchestrators record into it at the same points their human slog
// summary lines fire — it is presentation plumbing over numbers the
// text output already announces, not new instrumentation.
//
// A nil *RunSummary is a no-op on every method, so orchestrator call
// sites record unconditionally and only callers that asked for an
// envelope (the CLI's json mode) pay for the bookkeeping.
type RunSummary struct {
	mu     sync.Mutex
	order  []tableStatKey
	tables map[tableStatKey]*TableRunStat
}

type tableStatKey struct{ schema, name string }

// TableRunStat is one table's end-of-run stats. Rows is nil when the
// orchestrator finished the table without an aggregated row total (the
// migrate bulk-copy path counts rows per chunk inside its progress
// tickers and never aggregates a per-table total; nil renders as
// "unknown" rather than a misleading 0). When non-nil it is the number
// of rows the run handled for the table — accumulated across calls, so
// a chain restore that re-applies a table across segments sums up.
type TableRunStat struct {
	Schema string
	Name   string
	Rows   *int64
}

// RecordTable notes that the run touched schema.name, without a row
// total. Nil-safe. A later RecordTableRows for the same table attaches
// a total; a repeat RecordTable is a no-op.
func (s *RunSummary) RecordTable(schema, name string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entryLocked(schema, name)
}

// RecordTableRows adds rows to schema.name's running total, creating
// the entry when absent. Nil-safe.
func (s *RunSummary) RecordTableRows(schema, name string, rows int64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.entryLocked(schema, name)
	if e.Rows == nil {
		e.Rows = new(int64)
	}
	*e.Rows += rows
}

// entryLocked returns the stat entry for schema.name, creating it (and
// recording first-seen order) when absent. Callers hold s.mu.
func (s *RunSummary) entryLocked(schema, name string) *TableRunStat {
	k := tableStatKey{schema: schema, name: name}
	if e, ok := s.tables[k]; ok {
		return e
	}
	if s.tables == nil {
		s.tables = make(map[tableStatKey]*TableRunStat)
	}
	e := &TableRunStat{Schema: schema, Name: name}
	s.tables[k] = e
	s.order = append(s.order, k)
	return e
}

// Tables returns the recorded stats in first-recorded order. The
// returned slice and its Rows pointers are copies — safe to hold after
// further recording. Nil-safe (returns nil).
func (s *RunSummary) Tables() []TableRunStat {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]TableRunStat, 0, len(s.order))
	for _, k := range s.order {
		e := s.tables[k]
		stat := TableRunStat{Schema: e.Schema, Name: e.Name}
		if e.Rows != nil {
			rows := *e.Rows
			stat.Rows = &rows
		}
		out = append(out, stat)
	}
	return out
}
