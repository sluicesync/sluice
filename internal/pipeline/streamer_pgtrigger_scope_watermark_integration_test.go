//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-43 (a) — a postgres-trigger stream with --include-table must keep
// moving through a burst of change-log rows for a CAPTURED table the sync
// excludes.
//
// From v0.145.0 to v0.156.8 the reader dropped such a row without moving
// its watermark. Once a full poll window (defaultBatchSize = 10000) of them
// sat above the watermark, every poll re-read and re-dropped the same
// window: the stream applied nothing forever, persisted last_id never
// moved, and the only signal was a CAPTURE-RELATION-MOVED WARN that falsely
// claimed the excluded table had changed schemas (GC-43 (c)).
//
// This drives the REAL Streamer, on both a live stream and a warm resume:
//
//   - live: 10,101 rows into the excluded table, then one row into the
//     synced table. The row must land and the persisted last_id must reach
//     its change-log id.
//   - restart: stopped, 10,050 more excluded rows plus one synced row, then
//     a fresh Streamer resumes from the persisted position. This is also the
//     shape of a stream an older binary had already wedged — its persisted
//     position sits below a full window of excluded rows — so it pins that
//     an upgrade unsticks one on restart.
//
// The expected values are independent of the reader: the target's own row
// set, and the source change log's own max(id) for the synced table.

package pipeline

import (
	"context"
	"database/sql"
	"log"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/pgtrigger"
	"sluicesync.dev/sluice/internal/logcapture"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

const (
	gc43Synced   = "gc43_synced"
	gc43Excluded = "gc43_excluded"
)

func TestPGTriggerStreamer_ExcludedTableBurstDoesNotStall(t *testing.T) {
	logBuf := &logcapture.Buffer{}
	prevDefault := slog.Default()
	prevWriter, prevFlags := log.Writer(), log.Flags()
	slog.SetDefault(slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer func() {
		slog.SetDefault(prevDefault)
		log.SetOutput(prevWriter)
		log.SetFlags(prevFlags)
	}()

	src, tgt, cleanup := startPGTrigStreamerPGPair(t)
	defer cleanup()

	pgTrigStreamerExec(t, src, `
		CREATE TABLE `+gc43Synced+` (id INTEGER PRIMARY KEY, note TEXT NOT NULL);
		CREATE TABLE `+gc43Excluded+` (id INTEGER PRIMARY KEY, note TEXT NOT NULL);
		INSERT INTO `+gc43Synced+` VALUES (1, 'seed-1'), (2, 'seed-2');
	`)
	// Both tables are captured; only one is synced. That is the shape the
	// stall needs: the excluded table's rows reach the change log.
	if _, err := pgtrigger.Setup(context.Background(), src, pgtrigger.SetupOptions{
		Tables: []string{gc43Synced, gc43Excluded},
		Schema: "public",
	}); err != nil {
		t.Fatalf("pgtrigger.Setup: %v", err)
	}

	trigEng, ok := engines.Get(pgtrigger.EngineName)
	if !ok {
		t.Fatal("postgres-trigger engine not registered")
	}
	tgtEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	const streamID = "gc43-scope-watermark"
	newStreamer := func() *Streamer {
		s := &Streamer{
			Source:    trigEng,
			Target:    tgtEng,
			SourceDSN: src,
			TargetDSN: tgt,
			StreamID:  streamID,
			Filter:    mustNewFilter(t, []string{gc43Synced}, nil),
		}
		s.ApplyBatchSize = 1000
		return s
	}
	tgtDB := gc43Open(t, tgt)
	srcDB := gc43Open(t, src)

	r := gc43Run(t, newStreamer())
	waitForCDCStateRow(t, tgt, streamID, 90*time.Second)

	// A burst past a full poll window, in one transaction, then the row
	// the stream must still deliver.
	pgTrigStreamerExec(t, src, `INSERT INTO `+gc43Excluded+` SELECT g, 'burst-' || g FROM generate_series(1, 10101) g`)
	pgTrigStreamerExec(t, src, `INSERT INTO `+gc43Synced+` VALUES (3, 'after-gap')`)
	gc43AwaitRowAndWatermark(t, r, logBuf, srcDB, tgtDB, streamID, 3)
	r.stop(t)

	// Stopped: the next burst lands above the persisted position, so the
	// resume starts below a full window of excluded rows.
	pgTrigStreamerExec(t, src, `INSERT INTO `+gc43Excluded+` SELECT g, 'burst-' || g FROM generate_series(20001, 30050) g`)
	pgTrigStreamerExec(t, src, `INSERT INTO `+gc43Synced+` VALUES (4, 'after-restart-gap')`)
	r = gc43Run(t, newStreamer())
	gc43AwaitRowAndWatermark(t, r, logBuf, srcDB, tgtDB, streamID, 4)
	r.stop(t)

	logs := logBuf.String()
	for _, marker := range []string{"CAPTURE-RELATION-MOVED", "CAPTURE-OUT-OF-SCOPE", "CHANGE-LOG-WATERMARK-STALLED"} {
		if strings.Contains(logs, marker) {
			t.Errorf("a table the sync's own filter excludes logged %s; it is neither a moved table nor a foreign capture", marker)
		}
	}
}

// gc43Running is one Streamer.Run in flight.
type gc43Running struct {
	cancel context.CancelFunc
	errc   chan error
}

func gc43Run(t *testing.T, s *Streamer) *gc43Running {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := &gc43Running{cancel: cancel, errc: make(chan error, 1)}
	go func() { r.errc <- s.Run(ctx) }()
	t.Cleanup(cancel)
	return r
}

func (r *gc43Running) stop(t *testing.T) {
	t.Helper()
	r.cancel()
	select {
	case err := <-r.errc:
		if err != nil && !strings.Contains(err.Error(), "context canceled") {
			t.Errorf("Streamer.Run: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Error("Streamer.Run did not return after ctx cancel")
	}
}

// gc43AwaitRowAndWatermark waits for the synced row id to land on the
// target AND the persisted last_id to reach the source change log's
// highest id for the synced table — the row's own change-log id. A stream
// that ends first fails the test with its own error.
func gc43AwaitRowAndWatermark(t *testing.T, r *gc43Running, logBuf *logcapture.Buffer, srcDB, tgtDB *sql.DB, streamID string, id int) {
	t.Helper()
	deadline := time.Now().Add(120 * time.Second)
	var (
		landed  bool
		lastID  int64
		wantMin int64
	)
	for time.Now().Before(deadline) {
		select {
		case err := <-r.errc:
			t.Fatalf("GC-43 (a): the stream ended before synced row id=%d landed: %v", id, err)
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = tgtDB.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM `+gc43Synced+` WHERE id = $1)`, id).Scan(&landed)
		_ = srcDB.QueryRowContext(ctx,
			`SELECT COALESCE(max(id), 0) FROM public.`+pgtrigger.ChangeLogTable+` WHERE table_name = $1`, gc43Synced).Scan(&wantMin)
		cancel()
		lastID = pgTrigWMQueryLastID(t, tgtDB, streamID)
		if landed && wantMin > 0 && lastID >= wantMin {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	var warns []string
	for _, line := range strings.Split(logBuf.String(), "\n") {
		if strings.Contains(line, `"level":"WARN"`) || strings.Contains(line, `"level":"ERROR"`) {
			warns = append(warns, line)
		}
	}
	t.Fatalf("GC-43 (a): synced row id=%d landed=%v; persisted last_id=%d, synced row's change-log id=%d — "+
		"a stream stalled behind a full window of excluded-table rows; WARN/ERROR lines:\n%s",
		id, landed, lastID, wantMin, strings.Join(warns, "\n"))
}

func gc43Open(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db.SetMaxOpenConns(2)
	t.Cleanup(func() { _ = db.Close() })
	return db
}
