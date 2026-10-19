//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
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
// TestIncludeTableScopeDrift_GlobRewidenRefuses made deterministic: a
// stream's FIRST cold start has no control row until its CDC anchor, so its
// in-progress mark could not be set before the copy, and a stop between the
// anchor and the record write (a slow -race run's stop landed there) left a
// row with a position and no record — judged like a stream an older release
// cold-started, on which a glob scope convicts nothing. The re-widen after
// a narrowed restart then streamed kx onto a copy missing the narrowed
// window's changes, at exit 0. The anchor now carries the mark: the next
// start refuses COLD-START-INCOMPLETE.
func coldStartStoppedAtTheAnchor(t *testing.T, engine, src, tgt string) {
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
	if err != nil || !rec.ColdStartInProgress {
		t.Fatalf("after a stop at the anchor the record reads %+v (%v); want cold start in progress", rec, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	err = run("kd").Run(ctx)
	if !errors.Is(err, ir.ErrIncludeTableScopeDrift) || !strings.Contains(err.Error(), "COLD-START-INCOMPLETE") {
		t.Fatalf("the restart after a stop at the anchor: %v; want COLD-START-INCOMPLETE", err)
	}
}

func TestIncludeTableScopeDrift_PgoutputStopAtTheAnchorRefuses(t *testing.T) {
	src, tgt, cleanup := startPostgresLogical(t)
	t.Cleanup(cleanup)
	applyDDL(t, src, `CREATE TABLE kd (id INT PRIMARY KEY, k VARCHAR(20)); INSERT INTO kd VALUES (1, 'a');
		CREATE TABLE kx (id INT PRIMARY KEY, e VARCHAR(20)); INSERT INTO kx VALUES (1, 'x'), (2, 'y');`)
	coldStartStoppedAtTheAnchor(t, "postgres", src, tgt)
}

func TestIncludeTableScopeDrift_BinlogStopAtTheAnchorRefuses(t *testing.T) {
	src, tgt, cleanup := startMySQLBinlog(t)
	t.Cleanup(cleanup)
	applyDDLMySQL(t, src, `CREATE TABLE kd (id INT PRIMARY KEY, k VARCHAR(20)); INSERT INTO kd VALUES (1, 'a');
		CREATE TABLE kx (id INT PRIMARY KEY, e VARCHAR(20)); INSERT INTO kx VALUES (1, 'x'), (2, 'y');`)
	coldStartStoppedAtTheAnchor(t, "mysql", src, tgt)
}
