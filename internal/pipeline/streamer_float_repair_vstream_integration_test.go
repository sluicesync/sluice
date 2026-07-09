//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// End-to-end pins for the VStream-COPY FLOAT display-rounding repair
// (roadmap open-bug 2026-07-09) on the SYNC cold-start path — the
// load-bearing target-observable pin the mysql-engine reader test
// (TestVStream_FloatCarrierParity) cannot make, because the raw VStream
// COPY reader stays rounded by design and the fix lives at the pipeline
// layer (post-copy exact SQL re-read + PK-keyed target UPDATE).
//
// Source is a real vttestserver (planetscale flavor over VStream); target
// is a real Postgres. The distinguishing torture value is 8388608 (2^23):
// vttablet's rowstreamer renders it as 8388610 over the text protocol, so
// WITHOUT the repair the target lands 8388610, and WITH it the repair
// re-reads 8388608 exactly and UPDATEs the target before CDC begins.
//
// TestStreamer_ prefix + VStream substring: required by the
// extended-suites vstream-pipeline `-run` filter
// (^(TestMigrate_VStream|TestStreamer_.*VStream|TestSpikeShapeA_)) and
// enforced by scripts/check-run-filter-coverage.sh.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// pollTargetFloat32 reads the target's fl column for id, returning its
// float32 view once the row exists; ok=false until then / on timeout.
func pollTargetFloat32(t *testing.T, db *sql.DB, table string, id int, timeout time.Duration) (float32, bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var v sql.NullFloat64
		err := db.QueryRow(fmt.Sprintf("SELECT fl FROM %s WHERE id = $1", table), id).Scan(&v)
		if err == nil && v.Valid {
			return float32(v.Float64), true
		}
		time.Sleep(250 * time.Millisecond)
	}
	return 0, false
}

// waitTargetFloat32Equals polls until the target's fl for id equals want
// (float32-exact), or the timeout expires.
func waitTargetFloat32Equals(t *testing.T, db *sql.DB, table string, id int, want float32, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got, ok := pollTargetFloat32(t, db, table, id, time.Second); ok {
			if math.Float32bits(got) == math.Float32bits(want) {
				return true
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	return false
}

func seedFloatSource(t *testing.T, mysqlDSN string) {
	t.Helper()
	applySQL(t, mysqlDSN, `
		CREATE TABLE metrics (
			id  BIGINT NOT NULL AUTO_INCREMENT,
			fl  FLOAT  NULL,
			dbl DOUBLE NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	// id 1: 8388608 (rounds to 8388610); id 2: -123456.789 (rounds to
	// -123457); id 3: 1234567.75 (>6-sig, rounds). Values are kept well
	// within the FLOAT range so vtgate's strict mode accepts the seed
	// (float32-max lives in the writer's family×shape unit/integration pin).
	applySQL(t, mysqlDSN, "INSERT INTO metrics (fl, dbl) VALUES (8388608, 8388608), (-123456.789, -123456.789), (1234567.75, 1234567.75)")
	// Let the async schema tracker see the table before the VStream opens.
	time.Sleep(3 * time.Second)
}

func floatRepairStreamer(t *testing.T, mysqlDSN, grpcEndpoint, targetDSN, streamID string, noReread bool) *Streamer {
	t.Helper()
	srcEng, ok := engines.Get("planetscale")
	if !ok {
		t.Fatal("planetscale engine not registered")
	}
	tgtEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	sourceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
		mysqlDSN, grpcEndpoint,
	)
	return &Streamer{
		Source:             srcEng,
		Target:             tgtEng,
		SourceDSN:          sourceDSN,
		TargetDSN:          targetDSN,
		StreamID:           streamID,
		NoFloatExactReread: noReread,
	}
}

// TestStreamer_FloatRepair_ColdStart_VStream_PG pins that the DEFAULT
// (repair ON) sync cold-start lands single-precision FLOAT columns
// float32-EXACT on the target — the rounded COPY value is repaired by the
// post-copy exact re-read before CDC begins.
func TestStreamer_FloatRepair_ColdStart_VStream_PG(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanupSrc := startShardedVTTestServer(t, "commerce", 1)
	defer cleanupSrc()
	targetDSN, cleanupTgt := startPGTarget(t)
	defer cleanupTgt()

	seedFloatSource(t, mysqlDSN)

	streamer := floatRepairStreamer(t, mysqlDSN, grpcEndpoint, targetDSN, "float-repair-on", false)
	streamCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	tgtDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	// Every seeded FLOAT must land float32-exact (NOT the rounded COPY
	// value). 8388608 is the sentinel: rounded it would be 8388610.
	for _, tc := range []struct {
		id   int
		want float32
	}{
		{1, float32(8388608)},
		{2, float32(-123456.789)},
		{3, float32(1234567.75)},
	} {
		if !waitTargetFloat32Equals(t, tgtDB, "metrics", tc.id, tc.want, 120*time.Second) {
			got, _ := pollTargetFloat32(t, tgtDB, "metrics", tc.id, time.Second)
			select {
			case e := <-runErr:
				t.Fatalf("id %d: FLOAT never reached exact %v (last %v); Run returned: %v", tc.id, tc.want, got, e)
			default:
			}
			t.Fatalf("id %d: FLOAT never reached exact %v (last %v) — repair did not run", tc.id, tc.want, got)
		}
	}

	cancel()
	select {
	case <-runErr:
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// TestStreamer_FloatRepair_Disabled_VStream_PG pins that
// --no-float-exact-reread leaves the VStream COPY display-rounding in
// place: 8388608 lands as the rounded 8388610 (a DIFFERENT float32),
// proving the repair — not something else — is what restores exactness in
// the sibling test.
func TestStreamer_FloatRepair_Disabled_VStream_PG(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanupSrc := startShardedVTTestServer(t, "commerce", 1)
	defer cleanupSrc()
	targetDSN, cleanupTgt := startPGTarget(t)
	defer cleanupTgt()

	seedFloatSource(t, mysqlDSN)

	streamer := floatRepairStreamer(t, mysqlDSN, grpcEndpoint, targetDSN, "float-repair-off", true)
	streamCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	tgtDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	// Wait for the copy to land id 1, then assert it is the ROUNDED value,
	// not the exact one. The rounded FLOAT for 8388608 is 8388610.
	if !waitTargetFloat32Equals(t, tgtDB, "metrics", 1, float32(8388610), 120*time.Second) {
		got, ok := pollTargetFloat32(t, tgtDB, "metrics", 1, time.Second)
		select {
		case e := <-runErr:
			t.Fatalf("id 1: never observed the rounded 8388610 (last %v, present=%v); Run returned: %v", got, ok, e)
		default:
		}
		// If it landed EXACT with the repair disabled, the opt-out regressed.
		if ok && math.Float32bits(got) == math.Float32bits(float32(8388608)) {
			t.Fatalf("id 1: --no-float-exact-reread still produced the EXACT 8388608 — the repair ran despite the opt-out")
		}
		t.Fatalf("id 1: never observed the rounded 8388610 (last %v, present=%v)", got, ok)
	}

	cancel()
	select {
	case <-runErr:
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}
