//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// errStoppedAtTheAnchor is the panic a test's anchor hook raises to stop a
// cold start the way a killed process stops: no deferred record write runs
// with a result to record.
var errStoppedAtTheAnchor = errors.New("stopped right after the cold start's anchor write")

// stopAfterTheAnchor runs s to its cold start's anchor write and stops it
// there, as a kill landing in that window does.
func stopAfterTheAnchor(t *testing.T, s *Streamer) {
	t.Helper()
	coldStartAnchorWrittenForTest = func() { panic(errStoppedAtTheAnchor) }
	defer func() { coldStartAnchorWrittenForTest = nil }()
	done := make(chan any, 1)
	go func() {
		defer func() { done <- recover() }()
		_ = s.Run(context.Background())
	}()
	select {
	case got := <-done:
		if got != errStoppedAtTheAnchor { //nolint:errorlint // the recovered panic value itself
			t.Fatalf("the cold start did not stop at its anchor: recovered %v", got)
		}
	case <-time.After(3 * time.Minute):
		t.Fatal("the cold start never reached its anchor")
	}
}

// coldStartStoppedAtTheAnchor is Bug 321 R9b, the CI failure of
// TestIncludeTableScopeDrift_GlobRewidenRefuses made deterministic. A
// stream's FIRST cold start has no control row until its CDC anchor, and its
// record was written only after the anchor; a stop between the two (a slow
// -race run's stop landed there) left a row with a position and no record —
// judged like a stream an older release cold-started, on which a glob scope
// convicts nothing. The narrowed restart then recorded no narrowing, and the
// re-widen streamed kx onto a copy missing the narrowed window's changes, at
// exit 0. The anchor now records the copy it describes: the narrowed restart
// warm-resumes (the anchor's purpose) and narrows kx to dropped, and the
// re-widen refuses.
func coldStartStoppedAtTheAnchor(t *testing.T, engine, src, tgt string, applySrc func(string)) {
	t.Helper()
	eng, ok := engines.Get(engine)
	if !ok {
		t.Fatalf("%s engine not registered", engine)
	}
	run := func(include string) *Streamer {
		f, err := migcore.NewTableFilter([]string{include}, nil)
		if err != nil {
			t.Fatal(err)
		}
		s := &Streamer{Source: eng, Target: eng, SourceDSN: src, TargetDSN: tgt, StreamID: "rs", Filter: f}
		if engine == "postgres" {
			s.SlotName = "sluice_rs"
		}
		return s
	}
	stopAfterTheAnchor(t, run("k*"))

	app, err := eng.OpenChangeApplier(context.Background(), tgt)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := app.(ir.EstablishedTablesStore).ReadEstablishedRecord(context.Background(), "rs")
	migcore.CloseIf(app)
	if err != nil || !rec.Recorded || !reflect.DeepEqual(rec.Established, []string{"kd", "kx"}) {
		t.Fatalf("after a stop at the anchor the record reads %+v (%v); want the copy, established [kd kx]", rec, err)
	}

	// The narrowed restart: warm-resumes from the anchor and narrows kx.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run("kd").Run(ctx) }()
	applySrc(`INSERT INTO kd VALUES (2, 'b')`)
	deadline := time.Now().Add(2 * time.Minute)
	for anchorTestRows(t, engine, tgt, "kd") != 2 && time.Now().Before(deadline) {
		select {
		case err := <-done:
			cancel()
			t.Fatalf("the narrowed restart exited: %v", err)
		case <-time.After(500 * time.Millisecond):
		}
	}
	applySrc(`UPDATE kx SET e = 'Y' WHERE id = 2`)
	time.Sleep(2 * time.Second)
	cancel()
	<-done

	ctx3, cancel3 := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel3()
	err = run("k*").Run(ctx3)
	if !errors.Is(err, ir.ErrIncludeTableScopeDrift) || !strings.Contains(err.Error(), "dropped record") {
		t.Fatalf("the re-widen after a stop at the anchor and a narrowed restart: %v; want INCLUDE-TABLE-SCOPE-DRIFT, kx dropped", err)
	}
}

// anchorTestRows is a table's row count, -1 when it cannot be read.
func anchorTestRows(t *testing.T, engine, dsn, table string) int {
	t.Helper()
	if engine == "mysql" {
		return pollRowCountMySQL(dsn, table)
	}
	return pollRowCount(dsn, table)
}

func TestIncludeTableScopeDrift_PgoutputStopAtTheAnchorRefuses(t *testing.T) {
	src, tgt, cleanup := startPostgresLogical(t)
	t.Cleanup(cleanup)
	applyDDL(t, src, `CREATE TABLE kd (id INT PRIMARY KEY, k VARCHAR(20)); INSERT INTO kd VALUES (1, 'a');
		CREATE TABLE kx (id INT PRIMARY KEY, e VARCHAR(20)); INSERT INTO kx VALUES (1, 'x'), (2, 'y');`)
	coldStartStoppedAtTheAnchor(t, "postgres", src, tgt, func(q string) { applyDDL(t, src, q) })
}

func TestIncludeTableScopeDrift_BinlogStopAtTheAnchorRefuses(t *testing.T) {
	src, tgt, cleanup := startMySQLBinlog(t)
	t.Cleanup(cleanup)
	applyDDLMySQL(t, src, `CREATE TABLE kd (id INT PRIMARY KEY, k VARCHAR(20)); INSERT INTO kd VALUES (1, 'a');
		CREATE TABLE kx (id INT PRIMARY KEY, e VARCHAR(20)); INSERT INTO kx VALUES (1, 'x'), (2, 'y');`)
	coldStartStoppedAtTheAnchor(t, "mysql", src, tgt, func(q string) { applyDDLMySQL(t, src, q) })
}
