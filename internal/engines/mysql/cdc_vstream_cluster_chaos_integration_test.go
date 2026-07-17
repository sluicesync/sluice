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
	// Tighten the F3 progress windows for the chaos suite: the seed tables are
	// tiny and copy in seconds on a local cluster (no multi-minute slow start),
	// so a wedged stream/COPY after a fault should flip loud fast rather than
	// wait out the production-safe defaults (45s CDC / 10m COPY). This keeps
	// each scenario well under its timeout while still proving the loud path.
	return fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0&vstream_tablet_type=primary"+
			"&vstream_progress_timeout=20s&vstream_copy_progress_timeout=30s",
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
	// The CDC reader is a *vstreamCDCReader for the standalone path and a
	// *vstreamSnapshotStream for the cold-start (OpenSnapshotStream) path the
	// chaos scenarios use. Both expose Err(); assert on the interface, not the
	// concrete type, or a snapshot-stream loud error reads back as nil and a
	// genuine loud failure is misreported as a silent partial.
	if e, ok := c.(interface{ Err() error }); ok {
		return e.Err()
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

	// Stop the writer and let CDC catch up to the final source state. Generous
	// window: after ERS, vtgate must detect the dead primary, finish the
	// promotion, and re-route the VStream to the new primary before the reader
	// resumes — that can take tens of seconds. If the reader is going to
	// recover at all, it does so well within this window; if it stays stuck
	// here, that's a genuine silent-wedge-on-failover bug, not impatience.
	committed := stop()
	writerStopped = true
	t.Logf("writer committed ~%d live rows across both reparents", committed)
	_ = drainUntil(changes, drain, 1<<30, 4*time.Minute) // drain to deadline

	srcCount, srcDistinct := sourceRowStats(t, cc.mysqlDSN, table)
	assertZeroLossOrLoud(t, "PrimaryFailover/PRS+ERS", drain, readerErr(stream.Changes), srcCount, srcDistinct)
}

// ----------------------------------------------------------------------
// Scenario 2 — TABLET KILL DURING COLD-START COPY
//
// Kills the primary vttablet (variant: its mysqld) DURING the COPY phase —
// the real-crash analog of the SIGKILL simulation — then EmergencyReparents
// onto the surviving replica (the VTOrc/operator action a real deployment
// takes; this minimal harness runs no VTOrc). Asserts the COPY either
// resumes across the reparent with zero loss (v0.99.5 in-place reconnect +
// v0.99.8/9 durable-watermark resume carrying the cursor onto the new
// primary) or fails LOUD — never a silent partial.
// ----------------------------------------------------------------------

func TestVitessChaos_TabletKill_MidColdStart(t *testing.T) {
	// RECONNECT-BUDGET BACKGROUND: the COPY pump's in-place reconnect budget
	// originally reset on ANY event (cdc_vstream_snapshot.go), so an
	// unproductive reconnect loop after a tablet death (reconnect →
	// non-progress events → error → repeat) never exhausted reconnectMax and
	// never failed loud — it churned. FIXED by gating the budget reset on
	// actual COPY progress (a ROW buffered) so an unproductive loop burns its
	// budget and surfaces a LOUD failCopy (~reconnectMax × backoff), which the
	// invariant accepts. This scenario exercises the recovery path: if the
	// reparent restores a serving primary before the budget exhausts the COPY
	// resumes (zero-loss); if not, it fails loud. Both are acceptable; a
	// silent partial is not (commit 68c7486).
	//
	// Two variants: kill the whole tablet container, and kill only mysqld
	// underneath a live vttablet. Both recover via the same ERS.
	// Each variant KILLS the COPY-source primary (uid 100) a different way;
	// recovery is then driven uniformly by an EmergencyReparentShard onto the
	// surviving replica (see the inject block). We deliberately do NOT restart
	// the killed tablet before the ERS — mirroring the proven failover
	// scenario, leaving the old primary dead makes the ERS deterministic
	// (no flapping-primary race), and the reparent (not the dead tablet) is
	// what restores a writable primary.
	variants := []struct {
		name    string
		killOne func(t *testing.T, cc *chaosCluster)
	}{
		{
			name: "tablet-container",
			killOne: func(t *testing.T, cc *chaosCluster) {
				cc.killContainer(t, svcTabletPrimary, "SIGKILL")
			},
		},
		{
			name: "tablet-mysqld",
			killOne: func(t *testing.T, cc *chaosCluster) {
				// Kill only mysqld; vttablet stays up but its backing MySQL is
				// gone — the "storage crashed under a live tablet" fault.
				cc.killTabletMySQL(t, svcTabletPrimary)
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
					// Recover the shard the way a real deployment (VTOrc / an
					// operator) would: EmergencyReparentShard the dead primary's
					// duties onto the surviving replica (uid 101, fully replicated
					// before the kill). This both (a) gives the COPY pump a primary
					// to resume its in-place reconnect against — the v0.99.5
					// reconnect + v0.99.8/9 durable-watermark resume must carry the
					// cursor across the reparent — and (b) restores a writable
					// primary for the final source-count read. The invariant then
					// holds either way: the COPY resumes to zero-loss across the
					// reparent, or the reconnect budget exhausts first and it fails
					// LOUD (the churn the progress-gated reset, commit 68c7486,
					// converts into failCopy). It must never silently partial.
					cc.emergencyReparent(t, tabletAliasReplica)
					cc.waitForPrimaryAlias(t, tabletAliasReplica, 4*time.Minute)
					cc.waitForWritablePrimaryHandle(t, 3*time.Minute)
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
	// and the reader's dial fails. The stop+gap+start forces the reconnect
	// path through a true outage rather than a momentary bounce.
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
// Boots the cluster on the PRIOR same-major minor (chaosUpgradeFromImage)
// and rolls every component forward to the vendored-client-matching target
// (chaosUpgradeToImage) one service at a time, mid-sync, asserting the
// invariant survives a real version bump.
//
// TAG CHOICE: within MAJOR 24 (the vendored vitess.io/vitess v0.24.1
// client) the only pull-available minors are v24.0.0 and v24.0.1, so the
// baseline upgrade is v24.0.0 -> v24.0.1 (target == the vendored client).
// A cross-major skew exercises a different online-DDL / `_vt_*` lifecycle
// than the code under test and is NOT the real upgrade baseline.
//
// ORDERING is the production zero-downtime pattern, NOT a naive
// recreate-the-primary-in-place (which would drop the only serving primary
// while its container rebuilds): upgrade the stateless vtgate first, then
// the replica, then PlannedReparentShard onto the now-upgraded replica so
// the OLD primary can be upgraded as a replica with no primary-write
// outage. The two tablets carry NAMED VOLUMES (see docker-compose.yml) so
// a `--force-recreate` onto the new image preserves each tablet's MySQL
// data + identity (mysqlctl `init || start` resumes the existing datadir).
//
// EXPECTED OUTCOME: as with the other cold-start chaos scenarios, any
// stream disruption mid-cold-start surfaces LOUD (the F3 watchdog +
// snapshot-reader Err() delegation) rather than silently — so this test
// most often passes via the loud branch of assertZeroLossOrLoud. That is
// the point: a version bump must never silently corrupt; loud-or-zero-loss
// is the contract.
// ----------------------------------------------------------------------

// chaosUpgradeFromImage / chaosUpgradeToImage pin the rolling-upgrade
// endpoints. Both are same-major (Vitess 24) minors so the bump exercises
// the real upgrade path the vendored v0.24.1 client supports; the target
// matches the vendored client exactly. Override CHAOS_UPGRADE_IMAGE /
// VITESS_LITE_IMAGE to retarget when a newer compatible minor ships.
const (
	chaosUpgradeFromImage = "vitess/lite:v24.0.0"
	chaosUpgradeToImage   = "vitess/lite:v24.0.1"
)

func TestVitessChaos_RollingUpgrade_MidSync(t *testing.T) {
	// Boot on the prior minor; the override + recreate rolls forward to the
	// target. baseEnv carries both so the bring-up and the per-service
	// recreate pick up the right tags.
	cc := startChaosCluster(
		t,
		"VITESS_LITE_IMAGE="+chaosUpgradeFromImage,
		"CHAOS_UPGRADE_IMAGE="+chaosUpgradeToImage,
	)
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

	// Zero-downtime rolling upgrade. The override file pins
	// CHAOS_UPGRADE_IMAGE; the per-service `--no-deps --force-recreate`
	// recreates ONLY that component onto the new tag while the rest keep
	// serving. If the reader errors loudly at any point the channel closes —
	// we keep driving the cluster steps (they are idempotent) and let
	// assertZeroLossOrLoud accept the loud outcome at the end.
	overrideFile := chaosUpgradeOverridePath(t)

	// 1. vtgate — the stateless gRPC endpoint sluice's VStream dials.
	cc.recreateServiceWithOverride(t, overrideFile, svcVtgate)
	cc.waitForWritablePrimaryHandle(t, 4*time.Minute)
	_ = drainUntil(changes, drain, drain.count()+5, 60*time.Second)

	// 2. the replica tablet — its named volume preserves data across the
	// image swap so it rejoins as a caught-up replica.
	cc.recreateServiceWithOverride(t, overrideFile, svcTabletReplica)
	cc.waitForWritablePrimaryHandle(t, 4*time.Minute)
	// The recreated replica's vttablet needs to finish booting before we can
	// reparent onto it — `compose up` returns at container-start, not at
	// tablet-serving, and a PRS issued too early fails "tablet is shutdown".
	// 5m (was 3m): the recreate boots vitess/lite:v24.0.1; the extended-
	// suites chaos job now pre-warms that tag into the local cache (see the
	// "Warm the rolling-upgrade image cache" step), but the extra headroom
	// covers a slow container-start under -race CPU pressure so a boot-timing
	// blip doesn't surface as a ping timeout (the ~2026-07-12 intermittent).
	cc.waitForTabletPing(t, tabletAliasReplica, 5*time.Minute)
	_ = drainUntil(changes, drain, drain.count()+5, 60*time.Second)

	// 3. promote the upgraded replica so the old primary can be upgraded
	// without a primary-write outage (the production pattern).
	cc.plannedReparent(t, tabletAliasReplica)
	cc.waitForPrimaryAlias(t, tabletAliasReplica, 3*time.Minute)
	cc.waitForWritablePrimaryHandle(t, 2*time.Minute)
	_ = drainUntil(changes, drain, drain.count()+5, 60*time.Second)

	// 4. the old primary (now a replica) — last component onto the new tag.
	cc.recreateServiceWithOverride(t, overrideFile, svcTabletPrimary)
	cc.waitForWritablePrimaryHandle(t, 4*time.Minute)

	committed := stop()
	writerStopped = true
	t.Logf("writer committed ~%d live rows across the rolling upgrade", committed)
	_ = drainUntil(changes, drain, 1<<30, 45*time.Second)

	srcCount, srcDistinct := sourceRowStats(t, cc.mysqlDSN, table)
	assertZeroLossOrLoud(t, "RollingUpgrade", drain, readerErr(stream.Changes), srcCount, srcDistinct)
}
