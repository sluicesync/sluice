//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Shared harness for the GC-44 target-witnessed first boundary integration
// tests (schema_forward_witness.go): one source and one target per cell, a
// stream started and stopped as an operator would, and the verdict read
// back from the target's own catalog and rows — the independent expected
// value, never the stream's own logs.

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/logcapture"
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// twfbDB is one side of a cell: an engine and a DSN reachable with that
// engine's driver.
type twfbDB struct {
	engine string
	dsn    string
}

func (d twfbDB) driver() string {
	if d.engine == "postgres" {
		return "pgx"
	}
	return "mysql"
}

// exec runs a (possibly multi-statement) script.
func (d twfbDB) exec(t *testing.T, script string) {
	t.Helper()
	dsn := d.dsn
	statements := []string{script}
	switch {
	case d.engine == "planetscale":
		// A vtgate source: the VStream parameters are the stream's, not the
		// driver's, and vtgate takes one statement at a time (the scripts
		// here carry no ';' inside a statement).
		dsn, _, _ = strings.Cut(dsn, "&vstream_")
		statements = strings.Split(script, ";")
	case d.driver() == "mysql":
		dsn += "&multiStatements=true"
	}
	db, err := sql.Open(d.driver(), dsn)
	if err != nil {
		t.Fatalf("open %s: %v", d.engine, err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for _, stmt := range statements {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v\n%s", d.engine, err, stmt)
		}
	}
}

// scalar returns the first column of the first row as text, or "<err>".
func (d twfbDB) scalar(t *testing.T, query string, args ...any) string {
	t.Helper()
	db, err := sql.Open(d.driver(), d.dsn)
	if err != nil {
		return "<" + err.Error() + ">"
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var v sql.NullString
	if err := db.QueryRowContext(ctx, query, args...).Scan(&v); err != nil {
		return "<" + err.Error() + ">"
	}
	return v.String
}

// columnType renders a target column's declared type the way an operator
// would read it off the catalog: PG format_type, MySQL COLUMN_TYPE.
func (d twfbDB) columnType(t *testing.T, table, column string) string {
	t.Helper()
	if d.engine == "postgres" {
		return d.scalar(t, `SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a
			JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE c.relname = $1 AND a.attname = $2 AND n.nspname = 'public' AND NOT a.attisdropped`, table, column)
	}
	return d.scalar(t, `SELECT COLUMN_TYPE FROM information_schema.columns
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?`, table, column)
}

// hasRow reports whether table holds a row with the given id.
func (d twfbDB) hasRow(t *testing.T, table string, id int) bool {
	t.Helper()
	return d.scalar(t, fmt.Sprintf("SELECT count(*) FROM %s WHERE id = %d", table, id)) == "1"
}

// waitRow polls for a row, failing fast if the stream exits.
func (d twfbDB) waitRow(t *testing.T, table string, id int, run *twfbRun, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if d.hasRow(t, table, id) {
			return true
		}
		if run != nil && run.exited() {
			return false
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// twfbRun is one Streamer.Run, started in the background.
type twfbRun struct {
	cancel context.CancelFunc
	done   chan error
	err    error
	ended  bool
}

func startTWFBRun(s *Streamer) *twfbRun {
	ctx, cancel := context.WithCancel(context.Background())
	r := &twfbRun{cancel: cancel, done: make(chan error, 1)}
	go func() { r.done <- s.Run(ctx) }()
	return r
}

// exited reports whether Run has returned on its own.
func (r *twfbRun) exited() bool {
	if r.ended {
		return true
	}
	select {
	case r.err = <-r.done:
		r.ended = true
		return true
	default:
		return false
	}
}

// stop cancels the run and returns its error.
func (r *twfbRun) stop(t *testing.T) error {
	t.Helper()
	r.cancel()
	if r.ended {
		return r.err
	}
	select {
	case r.err = <-r.done:
		r.ended = true
	case <-time.After(60 * time.Second):
		t.Fatal("Streamer.Run did not return after cancel")
	}
	return r.err
}

// twfbCell is one source/target pair with a stream between them.
type twfbCell struct {
	src, tgt twfbDB
	streamID string
	// exclude is the stream's --exclude-table list.
	exclude []string
	// mappings is the stream's --type-override list.
	mappings []config.Mapping
	// schemaChanges is the stream's --schema-changes ("" is the default,
	// forward).
	schemaChanges string
	// databases, when set, makes it a multi-database stream over these
	// source databases / schemas (--include-database); src.dsn is then a
	// server-level DSN.
	databases []string
}

// unforwarded reports whether the cell's stream forwards no source DDL, so
// its boundaries go through the unforwarded-stream check
// (schema_change_refuse.go) rather than the forward intercept.
func (c twfbCell) unforwarded() bool {
	return c.schemaChanges == "refuse" || len(c.databases) > 0
}

func (c twfbCell) streamer() *Streamer {
	srcEng, ok := engines.Get(c.src.engine)
	if !ok {
		panic("engine not registered: " + c.src.engine)
	}
	tgtEng, ok := engines.Get(c.tgt.engine)
	if !ok {
		panic("engine not registered: " + c.tgt.engine)
	}
	filter, err := migcore.NewTableFilter(nil, c.exclude)
	if err != nil {
		panic(err)
	}
	s := &Streamer{
		Source: srcEng, Target: tgtEng,
		SourceDSN: c.src.dsn, TargetDSN: c.tgt.dsn,
		StreamID:      c.streamID,
		Filter:        filter,
		Mappings:      c.mappings,
		SchemaChanges: c.schemaChanges,
		// The CLI's --apply-retry-attempts default; the zero value would
		// disable the ADR-0038 retry the in-process cell exercises.
		ApplyRetryAttempts: 8,
	}
	if len(c.databases) > 0 {
		s.DatabaseFilter = DatabaseFilter{Include: c.databases}
	}
	return s
}

// persistedPosition reads the stream's persisted resume position off the
// target ("" until there is one).
func (c twfbCell) persistedPosition(t *testing.T) string {
	t.Helper()
	q := `SELECT source_position FROM sluice_cdc_state WHERE stream_id = ?`
	if c.tgt.engine == "postgres" {
		q = `SELECT source_position FROM sluice_cdc_state WHERE stream_id = $1`
	}
	if v := c.tgt.scalar(t, q, c.streamID); !strings.HasPrefix(v, "<") {
		return v
	}
	return ""
}

// waitStreaming waits until the cold start has handed off to CDC — the
// resume position is persisted — so a stop leaves a warm-resumable stream.
func (c twfbCell) waitStreaming(t *testing.T, run *twfbRun) {
	t.Helper()
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		if c.persistedPosition(t) != "" {
			// Let the CDC open settle past its own startup reads.
			time.Sleep(2 * time.Second)
			return
		}
		if run.exited() {
			t.Fatalf("the stream exited before persisting a position: %v", run.err)
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatal("the cold start never persisted a resume position")
}

// coldStartAndStop runs a cold start until table's seed row (id) is on the
// target and the stream is in CDC, then stops it cleanly.
func (c twfbCell) coldStartAndStop(t *testing.T, table string, id int) {
	t.Helper()
	run := startTWFBRun(c.streamer())
	if !c.tgt.waitRow(t, table, id, run, 180*time.Second) {
		err := run.stop(t)
		t.Fatalf("the cold start never delivered %s row %d (stream: %v)", table, id, err)
	}
	c.waitStreaming(t, run)
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("the cold start's clean stop returned %v", err)
	}
}

// captureLogs routes slog to a buffer at DEBUG for the rest of the test.
func twfbCaptureLogs(t *testing.T) *logcapture.Buffer {
	t.Helper()
	var buf logcapture.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

// twfb log markers the tests read — each the literal text of a log line
// the code under test emits, so a renamed line fails the anti-vacuity
// checks rather than passing silently.
const (
	twfbLogMatch       = "schema-forward: first boundary matches the target"
	twfbLogForwarded   = "the first schema boundary after a (re)start differs from the target"
	twfbLogUnwitnessed = "the target cannot witness this table's first schema boundary"
	twfbLogTargetOnly  = "the target holds columns the source no longer has"
	twfbLogTargetWider = "the target column is WIDER than the source's"

	// The unforwarded-stream check's lines (schema_change_refuse.go).
	refuseLogMatch       = "schema change check: boundary matches the target"
	refuseLogUnwitnessed = "the target cannot witness this table's schema boundary"
	refuseLogTargetOnly  = "the target holds columns the source does not have"
	refuseLogAhead       = "schema change check: the target column is wider than the source's"
)

// logLinesFor returns the captured lines carrying marker whose table
// attribute names table (bare, or qualified by any schema).
func logLinesFor(buf *logcapture.Buffer, marker, table string) []string {
	var out []string
	for _, line := range strings.Split(buf.String(), "\n") {
		if !strings.Contains(line, marker) {
			continue
		}
		for _, field := range strings.Fields(line) {
			v, ok := strings.CutPrefix(field, "table=")
			if ok && (v == table || strings.HasSuffix(v, "."+table)) {
				out = append(out, line)
				break
			}
		}
	}
	return out
}
