//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 193 — binlog_row_image preflight + partial-image belt, pinned on
// a real mysqld across the FULL / MINIMAL / NOBLOB family (the Bug-74
// class discipline: the dispatch varies by row-image setting, so every
// setting is a cell, not a representative).
//
//   - MINIMAL / NOBLOB → the coded refusal at CDC start
//     (SLUICE-E-CDC-ROW-IMAGE-PARTIAL), on BOTH chokepoints: the plain
//     StreamChanges open and the snapshot opener (which must refuse
//     BEFORE the bulk copy).
//   - warm resume → a stream started under FULL, the global flipped to
//     MINIMAL mid-life, resume from the persisted position ⇒ the same
//     refusal (the preflight re-runs on every StreamChanges).
//   - FULL → streams exactly as before, with the Bug 193 UPDATE
//     narrowing pinned at the emit contract (Before = PK-only, After =
//     complete).
//   - the belt → a writer session with a SESSION-level
//     binlog_row_image=MINIMAL override slips past the GLOBAL preflight
//     by design; its partial UPDATE image must stop the stream loudly
//     (never a silent zero-row apply).
//
// This test boots its OWN container (not the shared TestMain mysqld):
// it mutates the *global* binlog_row_image, which would leak MINIMAL
// semantics into any other CDC test whose writer session opens inside
// the flip window — the same isolation rationale as
// startMySQLGTIDForCDC's PURGE BINARY LOGS.

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	mysqltc "github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/wait"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// startMySQLRowImageForCDC boots a dedicated MySQL container with the
// standard binlog flags (ROW + FULL row image — the same posture as
// the shared container); the test flips @@GLOBAL.binlog_row_image at
// runtime, which is exactly the live-flip shape the preflight must
// catch (Azure's parameter set is dynamic too). Boot retry schedule
// mirrors startMySQLGTIDForCDC.
func startMySQLRowImageForCDC(t *testing.T) (dsn string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	var (
		container *mysqltc.MySQLContainer
		lastErr   error
	)
	for attempt := 1; attempt <= sharedMySQLBootAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), sharedMySQLBootTimeout)
		c, err := mysqltc.Run(
			ctx,
			sharedMySQLImage,
			mysqltc.WithDatabase("source_db"),
			mysqltc.WithUsername("root"),
			mysqltc.WithPassword("rootpw"),
			testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
				ContainerRequest: testcontainers.ContainerRequest{
					Cmd: []string{
						"mysqld",
						"--server-id=1",
						"--log-bin=mysql-bin",
						"--binlog-format=ROW",
						"--binlog-row-image=FULL",
					},
				},
			}),
			testcontainers.WithWaitStrategyAndDeadline(
				sharedMySQLBootTimeout,
				wait.ForLog("port: 3306  MySQL Community Server").
					WithStartupTimeout(sharedMySQLBootTimeout),
			),
		)
		cancel()
		if err == nil {
			container = c
			if attempt > 1 {
				log.Printf("startMySQLRowImageForCDC boot attempt %d/%d succeeded",
					attempt, sharedMySQLBootAttempts)
			}
			break
		}
		if c != nil {
			_ = c.Terminate(context.Background())
		}
		lastErr = err
		if attempt < sharedMySQLBootAttempts {
			backoff := sharedMySQLBootBackoff(attempt)
			log.Printf("startMySQLRowImageForCDC boot attempt %d/%d failed: %v; retrying in %s",
				attempt, sharedMySQLBootAttempts, err, backoff)
			time.Sleep(backoff)
			continue
		}
		log.Printf("startMySQLRowImageForCDC boot attempt %d/%d failed: %v; giving up",
			attempt, sharedMySQLBootAttempts, err)
	}
	if container == nil {
		t.Fatalf("start container: %d attempts exhausted: %v", sharedMySQLBootAttempts, lastErr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}
	conn, err := container.ConnectionString(ctx, "parseTime=true")
	if err != nil {
		terminate()
		t.Fatalf("connection string: %v", err)
	}
	return conn, terminate
}

// setGlobalRowImage flips @@GLOBAL.binlog_row_image on the container —
// the live-dynamic operation Azure's `az mysql flexible-server
// parameter set` performs.
func setGlobalRowImage(t *testing.T, dsn, value string) {
	t.Helper()
	applyMySQL(t, dsn, fmt.Sprintf("SET GLOBAL binlog_row_image = '%s';", value))
}

// wantRowImageRefusal asserts err is the Bug 193 coded refusal.
func wantRowImageRefusal(t *testing.T, err error, site string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: accepted a partial binlog_row_image source; want the coded refusal", site)
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeCDCRowImagePartial {
		t.Fatalf("%s: want %s; got %T: %v", site, sluicecode.CodeCDCRowImagePartial, err, err)
	}
}

// TestCDCReader_RowImagePreflight is the Bug 193 matrix pin. Sequential
// sub-flows share one container; each flow restores FULL when done so
// the next starts from the safe posture.
func TestCDCReader_RowImagePreflight(t *testing.T) {
	dsn, cleanup := startMySQLRowImageForCDC(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE orders (
			id     BIGINT       NOT NULL,
			status VARCHAR(32)  NOT NULL,
			total  INT          NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO orders (id, status, total) VALUES (1, 'new', 100), (2, 'new', 200);
	`
	applyMySQL(t, dsn, seedDDL)

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	openReader := func(t *testing.T) *CDCReader {
		t.Helper()
		rdr, err := eng.OpenCDCReader(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenCDCReader: %v", err)
		}
		t.Cleanup(func() { _ = rdr.(*CDCReader).Close() })
		return rdr.(*CDCReader)
	}

	// --- MINIMAL and NOBLOB: coded refusal at BOTH CDC-start chokepoints.
	for _, image := range []string{"MINIMAL", "NOBLOB"} {
		t.Run("refuses_"+image, func(t *testing.T) {
			setGlobalRowImage(t, dsn, image)
			defer setGlobalRowImage(t, dsn, "FULL")

			// Chokepoint 1: the plain CDC stream open (warm resume /
			// backup incremental path).
			_, err := openReader(t).StreamChanges(ctx, ir.Position{})
			wantRowImageRefusal(t, err, "StreamChanges")

			// Chokepoint 2: the snapshot opener — a sync cold start must
			// refuse BEFORE the bulk copy, not hours later at handoff.
			if snap, err := eng.OpenSnapshotStream(ctx, dsn); err == nil {
				_ = snap.Close()
				t.Fatal("OpenSnapshotStream: accepted a partial binlog_row_image source; want the coded refusal before any copy")
			} else {
				wantRowImageRefusal(t, err, "OpenSnapshotStream")
			}
		})
	}

	// --- FULL regression + the warm-resume flip pin. Under FULL the
	// stream must run exactly as before, with the Bug 193 emit contract:
	// Update.Before narrowed to the PK, Update.After complete. Then the
	// global flips to MINIMAL mid-life and the resume from the persisted
	// position must refuse.
	t.Run("FULL_streams_then_flip_refuses_resume", func(t *testing.T) {
		setGlobalRowImage(t, dsn, "FULL")

		rdr := openReader(t)
		changes, err := rdr.StreamChanges(ctx, ir.Position{})
		if err != nil {
			t.Fatalf("StreamChanges under FULL: %v", err)
		}
		time.Sleep(200 * time.Millisecond) // syncer registration boundary

		applyMySQL(t, dsn, `
			INSERT INTO orders (id, status, total) VALUES (3, 'new', 300);
			UPDATE orders SET status = 'shipped', total = 150 WHERE id = 1;
			DELETE FROM orders WHERE id = 2;
		`)
		got := drainChanges(t, ctx, changes, 3, 30*time.Second)
		if len(got) != 3 {
			if streamErr := rdr.Err(); streamErr != nil {
				t.Fatalf("got %d changes; want 3 (stream error: %v)", len(got), streamErr)
			}
			t.Fatalf("got %d changes; want 3", len(got))
		}
		upd, ok := got[1].(ir.Update)
		if !ok {
			t.Fatalf("change[1] = %T; want ir.Update", got[1])
		}
		// The Bug 193 emit contract under FULL: PK-only Before,
		// complete After (a multi-column UPDATE — the exact shape the
		// Azure probe watched silently no-op under MINIMAL).
		if id, _ := upd.Before["id"].(int64); id != 1 {
			t.Errorf("update.Before[id] = %#v; want int64(1)", upd.Before["id"])
		}
		if _, present := upd.Before["status"]; present {
			t.Errorf("update.Before carries non-PK status (narrowing regressed?): %+v", upd.Before)
		}
		if s, _ := upd.After["status"].(string); s != "shipped" {
			t.Errorf("update.After[status] = %#v; want shipped", upd.After["status"])
		}
		if tot, _ := upd.After["total"].(int64); tot != 150 {
			t.Errorf("update.After[total] = %#v; want int64(150)", upd.After["total"])
		}
		resumePos := upd.Pos()
		_ = rdr.Close()

		// Flip mid-life (the persisted position is still valid — the
		// binlog is intact) and resume: the preflight must refuse
		// before any position work.
		setGlobalRowImage(t, dsn, "MINIMAL")
		defer setGlobalRowImage(t, dsn, "FULL")
		_, err = openReader(t).StreamChanges(ctx, resumePos)
		wantRowImageRefusal(t, err, "warm-resume StreamChanges")
	})

	// --- The belt: a SESSION-level binlog_row_image override on a
	// writer slips past the GLOBAL preflight by design. Its partial
	// UPDATE image must stop the stream loudly — the exact residue the
	// dispatch-time belt exists for. (This is also the shape of a
	// resume replaying a MINIMAL-era binlog segment after the global
	// was fixed.)
	t.Run("belt_stops_stream_on_partial_update_image", func(t *testing.T) {
		setGlobalRowImage(t, dsn, "FULL")

		rdr := openReader(t)
		changes, err := rdr.StreamChanges(ctx, ir.Position{})
		if err != nil {
			t.Fatalf("StreamChanges under FULL: %v", err)
		}
		time.Sleep(200 * time.Millisecond)

		// One writer CONNECTION with a session-scoped MINIMAL: its
		// UPDATE's binlog image carries only the PK before-image and
		// the changed-column after-image.
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			t.Fatalf("open writer: %v", err)
		}
		defer func() { _ = db.Close() }()
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("pin writer conn: %v", err)
		}
		defer func() { _ = conn.Close() }()
		if _, err := conn.ExecContext(ctx, "SET SESSION binlog_row_image = 'MINIMAL'"); err != nil {
			t.Fatalf("session row-image override: %v", err)
		}
		if _, err := conn.ExecContext(ctx, "UPDATE orders SET status = 'stealthy' WHERE id = 1"); err != nil {
			t.Fatalf("partial-image update: %v", err)
		}

		// The stream must terminate (channel closed) with the coded
		// belt error — never deliver the partial update silently.
		deadline := time.After(30 * time.Second)
		for {
			select {
			case c, open := <-changes:
				if !open {
					wantRowImageRefusal(t, rdr.Err(), "dispatch belt")
					return
				}
				if _, isUpd := c.(ir.Update); isUpd {
					t.Fatalf("partial UPDATE image was emitted instead of refused: %+v", c)
				}
			case <-deadline:
				t.Fatal("stream did not stop on the partial UPDATE image within 30s")
			}
		}
	})
}
