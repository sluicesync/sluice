//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// XID-epoch safety of the §2 safety-lag hold-back and the Bug-94
// snapshot anchor (the 2026-07-08 epoch fix).
//
// A change-log row's system `xmin` is a 32-bit epoch-LESS xid;
// `pg_snapshot_xmin(pg_current_snapshot())` is a 64-bit epoch-carrying
// xid8. The original poll predicate and anchor arm compared the two
// directly, so once a cluster's lifetime txid count crossed 2^32
// (epoch ≥ 1 — routine on the long-lived managed-PG tier this engine
// targets) the hold-back was ALWAYS true (in-flight rows no longer
// held back → overlap-commit gap) and the anchor's `>=` arm NEVER
// matched (COALESCE → MAX(id) → the Bug-94 cold-start gap). The fixed
// queries compare the trigger-recorded `txid` column
// (pg_current_xact_id()::text::bigint — xid8 on both sides).
//
// These tests run the REAL trigger + reader against a server whose
// XID epoch is force-bumped to 5 via pg_resetwal. On an epoch-0
// container (every other test in this package) the broken and fixed
// predicates are behaviorally IDENTICAL — pinning this class requires
// the bumped epoch; that is why this harness exists.

package pgtrigger

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"

	"github.com/testcontainers/testcontainers-go"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// epochBumpTarget is the XID epoch pg_resetwal stamps on the probe
// cluster. Any value ≥ 1 exercises the class; 5 keeps the resulting
// xid8 values (~5·2^32) unmistakable in failure output.
const epochBumpTarget = "5"

// startEpochBumpedPGForTrigger boots postgres:16, cleanly stops it,
// force-bumps the cluster's XID epoch via pg_resetwal against the data
// volume (run as the postgres user in a throwaway helper container —
// the server must be down and the data dir is 0700 postgres-owned),
// restarts it, and verifies the bump took. The returned DSN reflects
// the post-restart mapped port (docker start may remap it).
func startEpochBumpedPGForTrigger(t *testing.T) (dsn string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	container, err := pgtc.Run(
		ctx,
		"postgres:16",
		pgtc.WithDatabase("source_db"),
		pgtc.WithUsername("test"),
		pgtc.WithPassword("test"),
		pgtc.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start container: %v", err)
	}
	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}

	// Locate the anonymous volume the postgres image declares for the
	// data dir; pg_resetwal runs against it while the server is down.
	inspect, err := container.Inspect(ctx)
	if err != nil {
		terminate()
		t.Fatalf("inspect container: %v", err)
	}
	dataVolume := ""
	for _, m := range inspect.Mounts {
		if m.Destination == "/var/lib/postgresql/data" {
			dataVolume = m.Name
		}
	}
	if dataVolume == "" {
		terminate()
		t.Fatal("postgres container has no /var/lib/postgresql/data volume")
	}

	// Clean shutdown — pg_resetwal refuses a data dir the server did
	// not exit cleanly from.
	stopTimeout := 60 * time.Second
	if err := container.Stop(ctx, &stopTimeout); err != nil {
		terminate()
		t.Fatalf("stop container for epoch bump: %v", err)
	}

	// pg_resetwal -e <epoch> in a helper container sharing the volume.
	helper, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "postgres:16",
			User:  "postgres",
			Cmd:   []string{"pg_resetwal", "-e", epochBumpTarget, "/var/lib/postgresql/data"},
			Mounts: testcontainers.ContainerMounts{
				testcontainers.VolumeMount(dataVolume, "/var/lib/postgresql/data"),
			},
			WaitingFor: wait.ForExit().WithExitTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		terminate()
		t.Fatalf("run pg_resetwal helper: %v", err)
	}
	state, err := helper.State(ctx)
	if err == nil && state.ExitCode != 0 {
		err = fmt.Errorf("pg_resetwal exited %d", state.ExitCode)
	}
	{
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		_ = helper.Terminate(shutdown)
		c()
	}
	if err != nil {
		terminate()
		t.Fatalf("pg_resetwal helper: %v", err)
	}

	if err := container.Start(ctx); err != nil {
		terminate()
		t.Fatalf("restart container after epoch bump: %v", err)
	}
	dsn, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		terminate()
		t.Fatalf("connection string after restart: %v", err)
	}

	// container.Start does not re-run the pgtc wait strategy — poll
	// until the server accepts queries, then gate on the epoch having
	// actually moved. Refusing to run against an epoch-0 server is the
	// harness's own loud-failure net: a silently-failed bump would turn
	// both tests below into no-op passes (the class this file exists to
	// pin is invisible at epoch 0).
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		terminate()
		t.Fatalf("open after restart: %v", err)
	}
	defer func() { _ = db.Close() }()
	var snapXmin int64
	deadline := time.Now().Add(60 * time.Second)
	for {
		err = db.QueryRowContext(ctx,
			`SELECT pg_snapshot_xmin(pg_current_snapshot())::text::bigint`).Scan(&snapXmin)
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		terminate()
		t.Fatalf("server did not come back after epoch bump: %v", err)
	}
	if snapXmin < 1<<32 {
		terminate()
		t.Fatalf("epoch bump did not take: snapshot xmin = %d < 2^32", snapXmin)
	}
	return dsn, terminate
}

// TestCDCReader_XIDEpochBump pins the xid8-domain safety-lag and
// anchor behavior on an epoch-bumped server. Two sub-scenarios share
// the (expensive: boot → resetwal → reboot) container:
//
//   - SafetyLag_OverlappingTxns: the epoch-bumped twin of
//     TestCDCReader_SafetyLag_OverlappingTxns. Against the broken
//     xmin-based predicate, B's committed row is emitted while A is
//     still in-flight (32-bit xmin < 64-bit snapshot xmin is always
//     true), the watermark advances past A's lower id, and A's row is
//     skipped forever once it commits — the drain below then sees one
//     event instead of two.
//   - SnapshotAnchor: the Bug-94 anchor must land BELOW a committed
//     row whose allocating txid is not yet provably settled. Against
//     the broken arm the anchor degenerates to MAX(id).
func TestCDCReader_XIDEpochBump(t *testing.T) {
	dsn, cleanup := startEpochBumpedPGForTrigger(t)
	defer cleanup()

	applyPGSQL(t, dsn, `CREATE TABLE items (id BIGINT PRIMARY KEY, label TEXT);`)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"items"}, Schema: "public"}); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	t.Run("SafetyLag_OverlappingTxns", func(t *testing.T) {
		e := Engine{}
		reader, err := e.OpenCDCReader(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenCDCReader: %v", err)
		}
		defer func() {
			if c, ok := reader.(interface{ Close() error }); ok {
				_ = c.Close()
			}
		}()

		out, err := reader.StreamChanges(ctx, ir.Position{})
		if err != nil {
			t.Fatalf("StreamChanges: %v", err)
		}

		// txA opens first (allocating the LOWER change-log id via the
		// trigger), txB opens second and commits FIRST — the exact
		// overlap the hold-back exists for.
		dbA, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatalf("open A: %v", err)
		}
		defer func() { _ = dbA.Close() }()
		dbB, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatalf("open B: %v", err)
		}
		defer func() { _ = dbB.Close() }()

		txA, err := dbA.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin A: %v", err)
		}
		if _, err := txA.ExecContext(ctx, `INSERT INTO items (id, label) VALUES (1, 'A')`); err != nil {
			_ = txA.Rollback()
			t.Fatalf("insert A: %v", err)
		}
		if _, err := dbB.ExecContext(ctx, `INSERT INTO items (id, label) VALUES (2, 'B')`); err != nil {
			_ = txA.Rollback()
			t.Fatalf("insert+commit B: %v", err)
		}

		// While A is in-flight NOTHING may be emitted: B's row must be
		// held back even though B committed. This is the assertion the
		// broken predicate fails post-epoch-1 (it emits B here, and A
		// is then lost below).
		if early := drainEvents(t, out, 1, 3*time.Second); len(early) != 0 {
			t.Errorf("hold-back failed: %d event(s) emitted while tx-A was in-flight (epoch-wrap silent-gap class)", len(early))
		}

		if err := txA.Commit(); err != nil {
			t.Fatalf("commit A: %v", err)
		}

		got := drainEvents(t, out, 2, 10*time.Second)
		if len(got) != 2 {
			t.Fatalf("got %d events; want 2 (commit-order safety on an epoch-bumped cluster)", len(got))
		}
		// Emission is allocation order (id ASC): A then B.
		wantLabels := []string{"A", "B"}
		for i, ev := range got {
			ins, ok := ev.(ir.Insert)
			if !ok {
				t.Fatalf("event %d: got %T; want ir.Insert", i, ev)
			}
			if lbl := fmt.Sprint(ins.Row["label"]); lbl != wantLabels[i] {
				t.Errorf("event %d: label = %q; want %q (id-order emission)", i, lbl, wantLabels[i])
			}
		}
	})

	t.Run("SnapshotAnchor", func(t *testing.T) {
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer func() { _ = db.Close() }()

		var maxID int64
		if err := db.QueryRowContext(ctx,
			`SELECT COALESCE(MAX(id), 0) FROM public.sluice_change_log`).Scan(&maxID); err != nil {
			t.Fatalf("read MAX(id): %v", err)
		}

		// txC opens and captures change-log id maxID+1, staying
		// in-flight; txD captures maxID+2 and commits. The anchor must
		// land at maxID+1 (below D's committed-but-unsettled row). The
		// broken arm never matches post-epoch-1 and returns MAX(id) =
		// maxID+2 — a too-high anchor that reopens the Bug-94 gap.
		dbC, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatalf("open C: %v", err)
		}
		defer func() { _ = dbC.Close() }()
		txC, err := dbC.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin C: %v", err)
		}
		defer func() { _ = txC.Rollback() }()
		if _, err := txC.ExecContext(ctx, `INSERT INTO items (id, label) VALUES (3, 'C')`); err != nil {
			t.Fatalf("insert C: %v", err)
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO items (id, label) VALUES (4, 'D')`); err != nil {
			t.Fatalf("insert+commit D: %v", err)
		}

		anchor, err := readChangeLogAnchor(ctx, db, "public")
		if err != nil {
			t.Fatalf("readChangeLogAnchor: %v", err)
		}
		if want := maxID + 1; anchor != want {
			t.Errorf("anchor = %d; want %d (below the unsettled committed row; %d = MAX(id) is the epoch-degenerate Bug-94 gap)",
				anchor, want, maxID+2)
		}

		if err := txC.Commit(); err != nil {
			t.Fatalf("commit C: %v", err)
		}
	})
}
