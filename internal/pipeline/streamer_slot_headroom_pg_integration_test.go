//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// End-to-end pins for the replication-headroom preflight (roadmap item
// 68d) on real Postgres:
//
//   - A FRESH cold start against a source whose max_replication_slots
//     are all in use refuses UPFRONT with the coded
//     SLUICE-E-CDC-REPLICATION-HEADROOM — naming the usage, the
//     ceiling, and the occupying slot — instead of the pre-fix raw
//     mid-cold-start `all replication slots are in use` (53400).
//   - A WARM RESUME with an existing slot NEVER probes/refuses: the
//     resume reuses its own slot (consuming no new one), so it must
//     proceed even when the server is at the ceiling.

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// startPostgresLogicalMaxSlots boots one wal_level=logical PG container
// with an operator-chosen max_replication_slots ceiling (the
// startPostgresLogical shape, ceiling parameterized) and a source_db /
// target_db pair.
func startPostgresLogicalMaxSlots(t *testing.T, maxSlots int) (sourceDSN, targetDSN string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

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
					"-c", fmt.Sprintf("max_replication_slots=%d", maxSlots),
				},
			},
		}),
	)
	if err != nil {
		t.Fatalf("start container: %v", err)
	}
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
	db, err := sql.Open("pgx", srcConn)
	if err != nil {
		terminate()
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "CREATE DATABASE target_db"); err != nil {
		terminate()
		t.Fatalf("create target_db: %v", err)
	}
	tgtConn, err := buildPGDSN(srcConn, "target_db")
	if err != nil {
		terminate()
		t.Fatalf("build target DSN: %v", err)
	}
	return srcConn, tgtConn, terminate
}

// occupySlot creates an inactive logical slot that consumes one entry
// of max_replication_slots.
func occupySlot(t *testing.T, dsn, name string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx,
		fmt.Sprintf(`SELECT pg_create_logical_replication_slot('%s', 'pgoutput')`, name)); err != nil {
		t.Fatalf("occupy slot %q: %v", name, err)
	}
}

// TestStreamer_ColdStart_PG_SlotHeadroomRefusal: every slot in use ⇒
// the fresh cold start refuses upfront, coded, naming usage + ceiling +
// the occupying slot, before touching the source.
func TestStreamer_ColdStart_PG_SlotHeadroomRefusal(t *testing.T) {
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	src, tgt, cleanup := startPostgresLogicalMaxSlots(t, 1)
	defer cleanup()

	applyDDL(t, src, `CREATE TABLE headroom_t (id BIGINT PRIMARY KEY, v INT);
		INSERT INTO headroom_t (id, v) VALUES (1, 1);`)
	occupySlot(t, src, "wave_1_leftover")

	streamer := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: src,
		TargetDSN: tgt,
		StreamID:  "headroom-refusal",
	}
	err := streamer.Run(context.Background())
	if err == nil {
		t.Fatal("expected the slot-headroom refusal; Run returned nil")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeCDCReplicationHeadroom {
		t.Fatalf("expected coded %s; got %v (err=%v)", sluicecode.CodeCDCReplicationHeadroom, ce, err)
	}
	msg := err.Error()
	for _, want := range []string{
		"all 1 of max_replication_slots are in use", // ceiling + usage
		`"wave_1_leftover" (inactive)`,              // the occupier named
		"sluice slot list",                          // the remedy
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal missing %q\nfull: %s", want, msg)
		}
	}
	// The refusal fires BEFORE slot creation — the occupier must still be
	// the only slot (no sluice_slot debris).
	if got := pgSlotCount(t, src); got != 1 {
		t.Fatalf("source slot count after refusal = %d; want 1 (only the occupier)", got)
	}
}

// TestStreamer_WarmResume_PG_FullSlots_NeverProbesRefuses: a stream
// whose cold start succeeded resumes fine on a server that is NOW at
// the slot ceiling — the warm resume reuses its existing slot and the
// headroom preflight must not fire on that path.
func TestStreamer_WarmResume_PG_FullSlots_NeverProbesRefuses(t *testing.T) {
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	// Room for exactly two slots: the stream's own + the occupier added
	// between the cold start and the resume.
	src, tgt, cleanup := startPostgresLogicalMaxSlots(t, 2)
	defer cleanup()

	applyDDL(t, src, `CREATE TABLE headroom_t (id BIGINT PRIMARY KEY, v INT);
		INSERT INTO headroom_t (id, v) SELECT g, g FROM generate_series(1, 50) g;`)

	run := func(label string) (context.CancelFunc, chan error) {
		streamer := &Streamer{
			Source:    pgEng,
			Target:    pgEng,
			SourceDSN: src,
			TargetDSN: tgt,
			StreamID:  "headroom-resume",
		}
		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() { errCh <- streamer.Run(ctx) }()
		t.Logf("%s stream started", label)
		return cancel, errCh
	}

	// Cold start: copies the 50 rows and creates the stream's slot.
	cancel1, err1 := run("cold-start")
	if !waitForExactRowCount(tgt, "headroom_t", 50, 2*time.Minute) {
		select {
		case err := <-err1:
			t.Fatalf("cold start exited before copying: %v", err)
		default:
			t.Fatalf("cold start never delivered 50 rows (got %d)", pollRowCount(tgt, "headroom_t"))
		}
	}
	cancel1()
	select {
	case <-err1:
	case <-time.After(20 * time.Second):
		t.Fatal("cold-start Run did not return after ctx cancel")
	}

	// Fill the server to its ceiling: the stream's slot + the occupier.
	occupySlot(t, src, "wave_2_leftover")
	if got := pgSlotCount(t, src); got != 2 {
		t.Fatalf("source slot count = %d; want 2 (stream slot + occupier)", got)
	}

	// Warm resume on the full server: must proceed (no probe, no
	// refusal) and keep applying live changes through its existing slot.
	cancel2, err2 := run("warm-resume")
	defer cancel2()
	applyDDL(t, src, `INSERT INTO headroom_t (id, v) VALUES (51, 51);`)
	if !waitForExactRowCount(tgt, "headroom_t", 51, 2*time.Minute) {
		select {
		case err := <-err2:
			var ce *sluicecode.CodedError
			if errors.As(err, &ce) && ce.Code == sluicecode.CodeCDCReplicationHeadroom {
				t.Fatalf("warm resume REFUSED on slot headroom — the preflight must never run on the resume path: %v", err)
			}
			t.Fatalf("warm resume exited: %v", err)
		default:
			t.Fatalf("warm resume never applied the live change (got %d rows)", pollRowCount(tgt, "headroom_t"))
		}
	}
	cancel2()
	select {
	case <-err2:
	case <-time.After(20 * time.Second):
		t.Fatal("warm-resume Run did not return after ctx cancel")
	}
}
