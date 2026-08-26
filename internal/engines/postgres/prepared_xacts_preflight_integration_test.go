//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// M2 P3 residual — the pg_prepared_xacts WARN against a real PG 16
// booted with max_prepared_transactions=10, end to end through the
// slot-creation chokepoint: with no prepared transaction pending, a
// cold-start CDC open stays silent; with one pending, the open WARNs
// (naming the gid and the unblock remedy) BEFORE blocking at
// CREATE_REPLICATION_SLOT, and resolving the transaction unblocks the
// open — pinning both the detector and the observed blocking mechanism
// it exists to make legible.

package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"

	"sluicesync.dev/sluice/internal/ir"
)

// startPostgresForPreparedXacts boots a dedicated PG container with
// logical replication enabled AND prepared transactions allowed
// (max_prepared_transactions defaults to 0, so the shared TestMain
// container cannot host this scenario). Per-test boot, mirroring
// startPostgresForCDCWithSmallDecodeMem's shape.
func startPostgresForPreparedXacts(t *testing.T) (dsn string, cleanup func()) {
	t.Helper()

	container := runPGWithRetry(
		t, sharedPGImage,
		pgtc.WithDatabase("source_db"),
		pgtc.WithUsername("test"),
		pgtc.WithPassword("test"),
		pgtc.BasicWaitStrategies(),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Cmd: []string{
					"-c", "wal_level=logical",
					"-c", "max_wal_senders=4",
					"-c", "max_replication_slots=4",
					"-c", "max_prepared_transactions=10",
				},
			},
		}),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}
	srcConn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		terminate()
		t.Fatalf("connection string: %v", err)
	}
	return srcConn, terminate
}

// lockedBuffer is a mutex-guarded log sink: the CDC open runs on its
// own goroutine in the pending direction (it blocks at slot creation
// by design), so the WARN write and the test's poll race without it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestCDCReader_PreparedXactWarnBeforeSlotCreate(t *testing.T) {
	dsn, cleanup := startPostgresForPreparedXacts(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	logs := &lockedBuffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(ctx, `CREATE TABLE t (id INT PRIMARY KEY)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// --- Silent direction: no prepared transaction pending — the
	// cold-start open (which creates the slot) must not warn.
	t.Run("empty_stays_silent", func(t *testing.T) {
		rdr, err := Engine{}.OpenCDCReader(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenCDCReader: %v", err)
		}
		if _, err := rdr.(*CDCReader).StreamChanges(ctx, ir.Position{}); err != nil {
			t.Fatalf("StreamChanges: %v", err)
		}
		if err := rdr.(*CDCReader).Close(); err != nil {
			t.Fatalf("close reader: %v", err)
		}
		if out := logs.String(); strings.Contains(out, preparedXactMarker) {
			t.Fatalf("empty pg_prepared_xacts must stay silent at slot creation; log: %s", out)
		}
		// Release the slot so the pending direction creates it fresh
		// (the WARN sits on the slot-CREATE path only). The walsender
		// may take a beat to detach after Close; retry briefly.
		deadline := time.Now().Add(30 * time.Second)
		for {
			_, err := db.ExecContext(ctx, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots`)
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("drop replication slot: %v", err)
			}
			time.Sleep(500 * time.Millisecond)
		}
	})

	// --- Pending direction: PREPARE TRANSACTION, then open. The WARN
	// must surface (naming the gid) while the open is BLOCKED at
	// CREATE_REPLICATION_SLOT; COMMIT PREPARED then unblocks it.
	t.Run("pending_warns_then_unblocks", func(t *testing.T) {
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("pin conn: %v", err)
		}
		defer func() { _ = conn.Close() }()
		for _, stmt := range []string{
			`BEGIN`,
			`INSERT INTO t (id) VALUES (1)`,
			`PREPARE TRANSACTION 'sluice_p3_orphan'`,
		} {
			if _, err := conn.ExecContext(ctx, stmt); err != nil {
				t.Fatalf("%s: %v", stmt, err)
			}
		}
		// If the test dies before the unblock, resolve the tx so the
		// container teardown isn't stuck behind it.
		resolved := false
		defer func() {
			if !resolved {
				_, _ = db.ExecContext(ctx, `ROLLBACK PREPARED 'sluice_p3_orphan'`)
			}
		}()

		rdr, err := Engine{}.OpenCDCReader(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenCDCReader: %v", err)
		}
		defer func() { _ = rdr.(*CDCReader).Close() }()

		streamErr := make(chan error, 1)
		go func() {
			_, err := rdr.(*CDCReader).StreamChanges(ctx, ir.Position{})
			streamErr <- err
		}()

		// The WARN precedes CREATE_REPLICATION_SLOT, so it must appear
		// while the open is still blocked on the prepared tx.
		warnDeadline := time.Now().Add(60 * time.Second)
		for !strings.Contains(logs.String(), preparedXactMarker) {
			select {
			case err := <-streamErr:
				t.Fatalf("StreamChanges returned (err=%v) before the %s WARN — the open did not block, or "+
					"the WARN never fired", err, preparedXactMarker)
			default:
			}
			if time.Now().After(warnDeadline) {
				t.Fatalf("no %s WARN within 60s while slot creation was blocked; log: %s",
					preparedXactMarker, logs.String())
			}
			time.Sleep(200 * time.Millisecond)
		}
		out := logs.String()
		for _, phrase := range []string{"sluice_p3_orphan", "COMMIT PREPARED", "ROLLBACK PREPARED"} {
			if !strings.Contains(out, phrase) {
				t.Errorf("WARN missing %q; log: %s", phrase, out)
			}
		}

		// Resolving the prepared tx unblocks the slot creation — the
		// blocked open completes rather than erroring.
		if _, err := db.ExecContext(ctx, `COMMIT PREPARED 'sluice_p3_orphan'`); err != nil {
			t.Fatalf("COMMIT PREPARED: %v", err)
		}
		resolved = true
		select {
		case err := <-streamErr:
			if err != nil {
				t.Fatalf("StreamChanges after COMMIT PREPARED: %v", err)
			}
		case <-time.After(60 * time.Second):
			t.Fatal("StreamChanges still blocked 60s after the prepared transaction was resolved")
		}
	})
}
