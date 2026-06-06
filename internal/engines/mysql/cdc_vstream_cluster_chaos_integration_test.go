//go:build integration && vitesscluster && chaos

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// CHAOS scenarios — four real-infrastructure-failure tests on top of the
// full Vitess cluster harness. Each one boots the cluster, starts a sluice
// VStream sync with continuous background writes, injects a fault
// mid-flight, drives/awaits recovery, and asserts THE invariant
// (assertZeroLossOrLoud): zero loss + zero dup, OR a loud error — never a
// silent partial / silent hang.
//
// Run (heavy + slow — own `chaos` tag, NOT in the per-PR gate):
//
//	go test -tags='integration vitesscluster chaos' -v -count=1 -timeout=40m \
//	  -run 'TestVitessChaos' ./internal/engines/mysql/...
//
// Shared scenario shape (each test follows it):
//
//	1. startChaosCluster(t)                  — boot + readiness gate
//	2. chaosSeedTable + chaosInsertBatch     — seed the source
//	3. eng.OpenSnapshotStream / OpenCDCReader — start the sync
//	4. drain the cold-start rows / attach CDC
//	5. continuousWriter(...)                  — live source workload
//	6. <inject fault>                         — the scenario's distinct part
//	7. <drive/await recovery>
//	8. stop the writer; let CDC catch up
//	9. assertZeroLossOrLoud(...)              — the invariant

package mysql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// chaosVStreamDSN builds the sluice DSN that drives a VStream off the
// chaos cluster's vtgate (primary tablet type so the sync survives a
// replica being promoted / a primary-only window during reparent).
func chaosVStreamDSN(cc *chaosCluster) string {
	return fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0&vstream_tablet_type=primary",
		cc.mysqlDSN, cc.grpcEndpoint,
	)
}

// chaosTable is the IR description of the canonical chaos source table
// (see chaosSeedTable): id BIGINT auto-inc PK + payload VARCHAR.
func chaosTable(name string) *ir.Table {
	return &ir.Table{
		Name: name,
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
			{Name: "payload", Type: ir.Varchar{Length: 255}},
		},
	}
}

// readerErr extracts the reader's terminal error from a CDCReader if it is
// the concrete vstream reader (the only impl this harness produces). A
// non-nil result is the LOUD-failure signal the invariant accepts.
func readerErr(c ir.CDCReader) error {
	if cdc, ok := c.(*vstreamCDCReader); ok {
		return cdc.Err()
	}
	return nil
}

// ----------------------------------------------------------------------
// Scenario 1 — PRIMARY FAILOVER (the highest-value scenario)
//
// Exercises BOTH reparent paths mid-sync:
//   - graceful PlannedReparentShard (operator-initiated clean failover),
//   - hard EmergencyReparentShard after KILLING the primary tablet.
//
// Asserts sluice's VStream follows the NEW primary across each handoff,
// the CDC position survives, and the invariant holds.
// ----------------------------------------------------------------------

func TestVitessChaos_PrimaryFailover_PRS_and_ERS(t *testing.T) {
	cc := startChaosCluster(t)
	defer cc.cleanup()

	const table = "failover_t"
	const seedRows = 200
	chaosSeedTable(t, cc.mysqlDSN, table)
	chaosInsertBatch(t, cc.mysqlDSN, table, 1, seedRows)
	// Let the tablet schema engine pick the table up before COPY opens.
	time.Sleep(3 * time.Second)

	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	stream, err := eng.OpenSnapshotStream(ctx, chaosVStreamDSN(cc))
	if err != nil {
		t.Fatalf("OpenSnapshotStream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	drain := newChaosDistinctDrain(table)

	// Cold-start COPY — drain every snapshot row into the distinct set so
	// the post-fault CDC continues the SAME exactly-once accounting.
	rowsCh, err := stream.Rows.ReadRows(ctx, chaosTable(table))
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	snap := 0
	for r := range rowsCh {
		if id, ok := chaosRowID(r); ok {
			drain.seen[id] = struct{}{}
		}
		snap++
	}
	if rerr := stream.Rows.Err(); rerr != nil {
		t.Fatalf("snapshot Rows.Err = %v; want nil", rerr)
	}
	if snap != seedRows {
		t.Fatalf("snapshot copied %d; want %d", snap, seedRows)
	}

	changes, err := stream.Changes.StreamChanges(ctx, stream.Position)
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	// Live workload across both reparents.
	stop := continuousWriter(t, cc.mysqlDSN, table, 150*time.Millisecond)
	writerStopped := false
	defer func() {
		if !writerStopped {
			stop()
		}
	}()

	// --- Fault A: GRACEFUL PlannedReparentShard primary(100) -> replica(101).
	// Let some CDC flow first so the reparent lands mid-tail.
	time.Sleep(4 * time.Second)
	cc.plannedReparent(t, tabletAliasReplica)
	cc.waitForPrimaryAlias(t, tabletAliasReplica, 3*time.Minute)
	cc.waitForWritablePrimaryHandle(t, 2*time.Minute)
	// Give the VStream time to reconnect to the new primary and resume.
	if reason := drainUntil(changes, drain, seedRows+10, 90*time.Second); reason == "closed" {
		// Reader terminated across the graceful reparent — that is a LOUD
		// outcome; finish the accounting and assert.
		stop()
		writerStopped = true
		srcCount, srcDistinct := sourceRowStats(t, cc.mysqlDSN, table)
		assertZeroLossOrLoud(t, "PrimaryFailover/PRS", drain, readerErr(stream.Changes), srcCount, srcDistinct)
		return
	}
	t.Logf("survived graceful PRS: %d distinct delivered so far", drain.count())

	// --- Fault B: HARD EmergencyReparentShard. Kill the CURRENT primary
	// (now uid 101) and promote the survivor (uid 100) with ERS.
	cc.killContainer(t, svcTabletReplica, "SIGKILL") // 101 is primary now
	cc.emergencyReparent(t, tabletAliasPrim)
	cc.waitForPrimaryAlias(t, tabletAliasPrim, 4*time.Minute)
	cc.waitForWritablePrimaryHandle(t, 3*time.Minute)

	// Drain a while longer so post-ERS inserts flow (or the reader errors).
	_ = drainUntil(changes, drain, seedRows+30, 2*time.Minute)

	// Stop the writer and let CDC catch up to the final source state.
	committed := stop()
	writerStopped = true
	t.Logf("writer committed ~%d live rows across both reparents", committed)
	_ = drainUntil(changes, drain, 1<<30, 45*time.Second) // drain to deadline

	srcCount, srcDistinct := sourceRowStats(t, cc.mysqlDSN, table)
	assertZeroLossOrLoud(t, "PrimaryFailover/PRS+ERS", drain, readerErr(stream.Changes), srcCount, srcDistinct)
}

// ----------------------------------------------------------------------
// Scenario 2 — TABLET KILL DURING COLD-START COPY
//
// Kills the primary vttablet (variant: its mysqld) DURING the COPY phase,
// the real-crash analog of the SIGKILL simulation. Asserts v0.99.9's
// durable-watermark resume + the Gap-1 gRPC-transient auto-retry recover
// with zero loss, or fail loud.
// ----------------------------------------------------------------------

func TestVitessChaos_TabletKill_MidColdStart(t *testing.T) {
	// Two variants: kill the whole tablet container, and kill only mysqld
	// underneath a live vttablet. Both must resume-or-fail-loud.
	variants := []struct {
		name    string
		killOne func(t *testing.T, cc *chaosCluster)
	}{
		{
			name: "tablet-container",
			killOne: func(t *testing.T, cc *chaosCluster) {
				cc.killContainer(t, svcTabletPrimary, "SIGKILL")
				cc.startService(t, svcTabletPrimary)
			},
		},
		{
			name: "tablet-mysqld",
			killOne: func(t *testing.T, cc *chaosCluster) {
				cc.killTabletMySQL(t, svcTabletPrimary)
				// vttablet stays up; mysqlctl is restarted by recovering the
				// container's mysqld. Bounce the tablet to re-init MySQL.
				cc.restartContainer(t, svcTabletPrimary)
			},
		},
	}

	for _, v := range variants {
		v := v
		t.Run(v.name, func(t *testing.T) {
			cc := startChaosCluster(t)
			defer cc.cleanup()

			const table = "coldstart_t"
			// Seed enough rows that COPY is non-trivial work — the kill must
			// land WHILE the snapshot is still streaming.
			const seedRows = 5000
			chaosSeedTable(t, cc.mysqlDSN, table)
			chaosInsertBatch(t, cc.mysqlDSN, table, 1, seedRows)
			time.Sleep(3 * time.Second)

			eng := Engine{Flavor: FlavorPlanetScale}
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
			defer cancel()

			stream, err := eng.OpenSnapshotStream(ctx, chaosVStreamDSN(cc))
			if err != nil {
				t.Fatalf("OpenSnapshotStream: %v", err)
			}
			defer func() { _ = stream.Close() }()

			drain := newChaosDistinctDrain(table)

			rowsCh, err := stream.Rows.ReadRows(ctx, chaosTable(table))
			if err != nil {
				t.Fatalf("ReadRows: %v", err)
			}

			// Drain the COPY rows; after the first ~10% land, inject the
			// kill mid-COPY, then keep draining. The COPY either resumes
			// (durable watermark + gRPC-transient retry) to completion or
			// the reader surfaces a loud error.
			injectAt := seedRows / 10
			injected := false
			snap := 0
			for r := range rowsCh {
				if id, ok := chaosRowID(r); ok {
					drain.seen[id] = struct{}{}
				}
				snap++
				if !injected && snap >= injectAt {
					injected = true
					t.Logf("injecting tablet kill at ~%d/%d COPY rows", snap, seedRows)
					v.killOne(t, cc)
					cc.waitForWritablePrimaryHandle(t, 4*time.Minute)
				}
			}
			rowsErr := stream.Rows.Err()

			// If the COPY surfaced a loud error, that satisfies the
			// invariant (loud failure). Otherwise it must have copied every
			// row exactly once.
			if rowsErr != nil {
				t.Logf("[TabletKill/%s] LOUD-FAILURE on COPY (acceptable): %v "+
					"(delivered %d/%d before terminating)", v.name, rowsErr, drain.count(), seedRows)
				return
			}

			srcCount, srcDistinct := sourceRowStats(t, cc.mysqlDSN, table)
			assertZeroLossOrLoud(t, "TabletKill/"+v.name, drain, nil, srcCount, srcDistinct)
		})
	}
}

// ----------------------------------------------------------------------
// Scenario 3 — VTGATE RESTART MID-SYNC
//
// `docker restart` the vtgate (sluice's gRPC VStream endpoint) mid-sync.
// Asserts the reader reconnects + resumes, invariant holds.
// ----------------------------------------------------------------------

func TestVitessChaos_VtgateRestart_MidSync(t *testing.T) {
	cc := startChaosCluster(t)
	defer cc.cleanup()

	const table = "vtgate_t"
	const seedRows = 100
	chaosSeedTable(t, cc.mysqlDSN, table)
	chaosInsertBatch(t, cc.mysqlDSN, table, 1, seedRows)
	time.Sleep(3 * time.Second)

	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	stream, err := eng.OpenSnapshotStream(ctx, chaosVStreamDSN(cc))
	if err != nil {
		t.Fatalf("OpenSnapshotStream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	drain := newChaosDistinctDrain(table)

	rowsCh, err := stream.Rows.ReadRows(ctx, chaosTable(table))
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	snap := 0
	for r := range rowsCh {
		if id, ok := chaosRowID(r); ok {
			drain.seen[id] = struct{}{}
		}
		snap++
	}
	if rerr := stream.Rows.Err(); rerr != nil {
		t.Fatalf("snapshot Rows.Err = %v; want nil", rerr)
	}
	if snap != seedRows {
		t.Fatalf("snapshot copied %d; want %d", snap, seedRows)
	}

	changes, err := stream.Changes.StreamChanges(ctx, stream.Position)
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	stop := continuousWriter(t, cc.mysqlDSN, table, 150*time.Millisecond)
	writerStopped := false
	defer func() {
		if !writerStopped {
			stop()
		}
	}()

	// Let CDC flow, then take vtgate (sluice's gRPC endpoint) fully DOWN for
	// a window before bringing it back — a stronger fault than a single
	// `restart` because there is a real interval where the endpoint is gone
	// and the reader's dial fails. (restartContainer is the lighter bounce;
	// the stop+gap+start here forces the reconnect path through a true
	// outage.)
	time.Sleep(4 * time.Second)
	cc.stopContainer(t, svcVtgate)
	time.Sleep(6 * time.Second) // endpoint-gone window: reader must dial-fail then retry
	cc.startService(t, svcVtgate)
	// vtgate needs to re-advertise a healthy primary before writes/streams
	// resume — reuse the same readiness gate the bring-up uses.
	cc.waitForWritablePrimaryHandle(t, 3*time.Minute)

	// The reader must reconnect to the bounced vtgate and resume. Drain a
	// while for post-restart inserts to land (or the reader to error).
	_ = drainUntil(changes, drain, seedRows+30, 2*time.Minute)

	committed := stop()
	writerStopped = true
	t.Logf("writer committed ~%d live rows across the vtgate restart", committed)
	_ = drainUntil(changes, drain, 1<<30, 45*time.Second)

	srcCount, srcDistinct := sourceRowStats(t, cc.mysqlDSN, table)
	assertZeroLossOrLoud(t, "VtgateRestart", drain, readerErr(stream.Changes), srcCount, srcDistinct)
}

// ----------------------------------------------------------------------
// Scenario 4 — ROLLING UPGRADE MID-SYNC
//
// Swaps a component image tag (vitess/lite:v24.0.1 -> a newer documented
// minor) and rolling-restarts mid-sync, asserting sluice survives the
// version bump.
//
// FEASIBILITY CAVEAT (flagged for the local session): a true in-test
// rolling upgrade needs (a) a second pinned image tag that is image-pull-
// available and protocol-compatible with the vendored vitess.io/vitess
// v0.24.x client, and (b) a compose override that re-creates one service
// at a time with the new tag while the rest of the cluster keeps serving.
// Within a single docker-compose project, `compose up -d --no-deps
// <service>` with an override file that pins the new tag re-creates ONLY
// that service. The skeleton below drives that, but is gated behind a
// t.Skip documenting the manual steps because the exact compatible newer
// tag (e.g. vitess/lite:v24.0.2 if released, or a v25 client+image bump)
// must be chosen and pull-verified locally first. Promote it by removing
// the Skip once the local session confirms the tag + override.
// ----------------------------------------------------------------------

func TestVitessChaos_RollingUpgrade_MidSync(t *testing.T) {
	t.Skip("rolling-upgrade chaos: DRAFT SKELETON — choose + pull-verify a compatible newer vitess/lite tag " +
		"and the docker-compose.chaos-upgrade.yml override locally, then remove this Skip. " +
		"Manual steps: (1) pick a newer minor of vitess/lite within the same MAJOR as the vendored " +
		"vitess.io/vitess client (cross-major skew exercises a different _vt_* lifecycle and is NOT the baseline); " +
		"(2) set CHAOS_UPGRADE_IMAGE to that tag; (3) `compose -f docker-compose.yml -f docker-compose.chaos-upgrade.yml " +
		"up -d --no-deps <service>` per component (vtgate, then each vttablet), waiting for waitForWritablePrimary " +
		"between each; (4) assert the invariant. See the skeleton below for the in-test driver.")

	cc := startChaosCluster(t)
	defer cc.cleanup()

	const table = "upgrade_t"
	const seedRows = 100
	chaosSeedTable(t, cc.mysqlDSN, table)
	chaosInsertBatch(t, cc.mysqlDSN, table, 1, seedRows)
	time.Sleep(3 * time.Second)

	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	stream, err := eng.OpenSnapshotStream(ctx, chaosVStreamDSN(cc))
	if err != nil {
		t.Fatalf("OpenSnapshotStream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	drain := newChaosDistinctDrain(table)

	rowsCh, err := stream.Rows.ReadRows(ctx, chaosTable(table))
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	for r := range rowsCh {
		if id, ok := chaosRowID(r); ok {
			drain.seen[id] = struct{}{}
		}
	}
	if rerr := stream.Rows.Err(); rerr != nil {
		t.Fatalf("snapshot Rows.Err = %v; want nil", rerr)
	}

	changes, err := stream.Changes.StreamChanges(ctx, stream.Position)
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	stop := continuousWriter(t, cc.mysqlDSN, table, 150*time.Millisecond)
	writerStopped := false
	defer func() {
		if !writerStopped {
			stop()
		}
	}()
	time.Sleep(4 * time.Second)

	// Rolling-restart each component onto the new tag, one at a time,
	// waiting for the cluster to re-stabilise between each. The override
	// file (docker-compose.chaos-upgrade.yml) pins CHAOS_UPGRADE_IMAGE; the
	// per-service `--no-deps` recreate leaves the rest of the stack serving.
	overrideFile := chaosUpgradeOverridePath(t)
	for _, svc := range []string{svcVtgate, svcTabletReplica, svcTabletPrimary} {
		cc.recreateServiceWithOverride(t, overrideFile, svc)
		cc.waitForWritablePrimaryHandle(t, 4*time.Minute)
		_ = drainUntil(changes, drain, drain.count()+5, 60*time.Second)
	}

	committed := stop()
	writerStopped = true
	t.Logf("writer committed ~%d live rows across the rolling upgrade", committed)
	_ = drainUntil(changes, drain, 1<<30, 45*time.Second)

	srcCount, srcDistinct := sourceRowStats(t, cc.mysqlDSN, table)
	assertZeroLossOrLoud(t, "RollingUpgrade", drain, readerErr(stream.Changes), srcCount, srcDistinct)
}
