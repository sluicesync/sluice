//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pin for audit finding MED-D0-9 — slot invalidation is a
// TERMINAL CRITICAL page, never a false "condition cleared".
//
// The audit OBSERVED the pre-fix inversion live on PG16: a real slot
// driven past `max_slot_wal_keep_size` and invalidated by CHECKPOINT
// reports `wal_status='lost'` with NULL lag; the reporter mapped the
// NULL lag to 0 bytes, the evaluator computed 0% pressure, decided
// "clean", and emitted "condition cleared" at the exact terminal
// moment — paging stopped when it mattered most. This test reproduces
// the audit's recipe (cap-exceeding WAL + CHECKPOINT on a real PG
// container), then drives the REAL probe loop with the REAL postgres
// reporter and asserts the fixed contract: exactly one CRITICAL page
// with the lost/re-snapshot framing, the terminal ERROR log line, and
// NO "condition cleared" anywhere.

package pipeline

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/notify"
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	"github.com/testcontainers/testcontainers-go"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// syncLogBuffer is a mutex-guarded bytes.Buffer so the probe loop's
// goroutine can write log lines while the test goroutine reads them.
type syncLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestSlotHealthLostSlot_TerminalPage invalidates a real replication
// slot (the audit's observed recipe: cap-exceeding WAL + CHECKPOINT on
// PG16 with a small max_slot_wal_keep_size) and asserts the terminal
// contract end-to-end through the real reporter + real probe loop.
func TestSlotHealthLostSlot_TerminalPage(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	// wal_level=logical for the slot; a deliberately tiny retention cap
	// so a few switched WAL segments push the (unconsumed) slot past it.
	container, err := pgtc.Run(
		ctx,
		pgPrebakedImage,
		pgtc.WithDatabase("source_db"),
		pgtc.WithUsername("test"),
		pgtc.WithPassword("test"),
		pgtc.BasicWaitStrategies(),
		pgPrebakedWaitStrategy(),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Cmd: []string{
					"-c", "wal_level=logical",
					"-c", "max_wal_senders=8",
					"-c", "max_replication_slots=8",
					"-c", "max_slot_wal_keep_size=1MB",
				},
			},
		}),
	)
	if err != nil {
		t.Fatalf("start PG container: %v", err)
	}
	defer func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}()

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	const slotName = "med_d0_9_lost_slot"
	if _, err := db.ExecContext(ctx,
		"SELECT pg_create_logical_replication_slot($1, 'pgoutput')", slotName); err != nil {
		t.Fatalf("create slot: %v", err)
	}

	// The audit's recipe: generate WAL past the cap with no consumer
	// attached, then CHECKPOINT so Postgres invalidates the slot. Each
	// round writes ~1 MB, switches the WAL segment (so the lag jumps to
	// the next 16 MB segment boundary), and checkpoints; poll until
	// wal_status reads 'lost'.
	if _, err := db.ExecContext(ctx, "CREATE TABLE wal_junk (id int, filler text)"); err != nil {
		t.Fatalf("create wal_junk: %v", err)
	}
	walStatus := ""
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := db.ExecContext(ctx,
			"INSERT INTO wal_junk SELECT g, repeat('x', 1000) FROM generate_series(1, 1000) g"); err != nil {
			t.Fatalf("generate WAL: %v", err)
		}
		if _, err := db.ExecContext(ctx, "SELECT pg_switch_wal()"); err != nil {
			t.Fatalf("pg_switch_wal: %v", err)
		}
		if _, err := db.ExecContext(ctx, "CHECKPOINT"); err != nil {
			t.Fatalf("checkpoint: %v", err)
		}
		if err := db.QueryRowContext(ctx,
			"SELECT wal_status FROM pg_replication_slots WHERE slot_name = $1", slotName).Scan(&walStatus); err != nil {
			t.Fatalf("read wal_status: %v", err)
		}
		if walStatus == "lost" {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if walStatus != "lost" {
		t.Fatalf("slot never reached wal_status='lost' inside the deadline; last status %q", walStatus)
	}

	// Ground-truth the reporter surface: the invalidated slot must
	// surface WALStatus verbatim (this is the field the pre-fix
	// evaluator ignored while the NULL lag read as zero pressure).
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	sr, err := pgEng.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer migcore.CloseIf(sr)
	reporter, ok := sr.(ir.SlotHealthReporter)
	if !ok {
		t.Fatalf("postgres SchemaReader no longer implements ir.SlotHealthReporter: %T", sr)
	}
	snap, ok, err := reporter.SlotHealth(ctx, slotName)
	if err != nil {
		t.Fatalf("SlotHealth: %v", err)
	}
	if !ok {
		t.Fatal("SlotHealth: expected ok=true for the invalidated (still-existing) slot")
	}
	if snap.WALStatus != "lost" {
		t.Fatalf("reporter WALStatus = %q; want lost", snap.WALStatus)
	}

	// Drive the REAL probe loop against the real reporter with a
	// capturing sink and a captured slog stream.
	logBuf := &syncLogBuffer{}
	prevLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(prevLogger)

	captured := &capturingNotifier{}
	loopCtx, loopCancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		slotHealthProbeLoop(loopCtx, reporter, slotName, "stream-lost", DefaultSlotHealthThresholds(), 50*time.Millisecond, captured)
		close(done)
	}()

	// Wait for the terminal page, then let a generous number of further
	// probes elapse: the latch must hold the total to exactly one.
	pageDeadline := time.After(10 * time.Second)
	for captured.count() == 0 {
		select {
		case <-pageDeadline:
			t.Fatal("no page delivered within 10s of probing a lost slot")
		case <-time.After(20 * time.Millisecond):
		}
	}
	time.Sleep(1 * time.Second) // ~20 more probe ticks against the lost slot
	loopCancel()
	<-done

	if got := captured.count(); got != 1 {
		t.Fatalf("lost slot paged %d times; want exactly 1 (terminal latch)", got)
	}
	page := captured.got[0]
	if page.Level != notify.LevelCritical {
		t.Errorf("page level = %q; want critical", page.Level)
	}
	if page.Category != notify.CategorySlotHealth {
		t.Errorf("page category = %q; want slot-health", page.Category)
	}
	if !strings.Contains(page.Title, "LOST") || !strings.Contains(page.Title, "re-snapshot") {
		t.Errorf("page title %q missing the LOST / re-snapshot framing", page.Title)
	}
	if !strings.Contains(page.Body, fmt.Sprintf("slot %q", slotName)) || !strings.Contains(page.Body, "terminal") {
		t.Errorf("page body %q missing the slot name / terminal framing", page.Body)
	}

	logs := logBuf.String()
	if !strings.Contains(logs, "slot INVALIDATED") {
		t.Errorf("logs missing the terminal ERROR line; got:\n%s", logs)
	}
	// THE MED-D0-9 pin: pre-fix, this exact log line fired here.
	if strings.Contains(logs, "condition cleared") {
		t.Errorf("false 'condition cleared' emitted for a lost slot (the MED-D0-9 inversion); logs:\n%s", logs)
	}
}
