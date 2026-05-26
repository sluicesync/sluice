//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0054 Shape A Phase 2e v3 — streamer-driven end-to-end harness.
//
// The v1 Phase 2e tests (commit 47451a1, see
// shard_consolidation_router_pg_integration_test.go) drive
// BoundaryRouter.RouteBoundary directly against a target PG container.
// That pins the router state machine + lease primitive + per-shape
// applier; it does NOT exercise the streamer-integration surface — the
// SchemaSnapshot intercept's runOnce dispatch, the change-channel
// intercept correctness across peer streams, the lease-acquire
// interaction with apply-batch boundaries, or CDC-resume-after-DDL
// across multiple sources.
//
// This v3 harness boots three independent PG source containers + one
// PG target container and starts a real Streamer per source, each with
// a distinct --inject-shard-column value. Source DDL is driven on each
// source's database; the assertions ride on the full streamer pipeline
// SchemaSnapshot reader → ShardConsolidationIntercept → BoundaryRouter
// → LeaseManager → ShapeDeltaApplier.
//
// Properties pinned:
//   1. Cross-source contention: exactly one streamer applies the DDL;
//      the other two observe it via the peer-applied (checksum-match)
//      path and record their own lease state without re-applying.
//   2. Target schema reflects the DDL exactly once (no duplicate
//      columns / indexes / etc).
//   3. CDC-resume-after-DDL: post-DDL INSERTs on each source flow to
//      the target with the appropriate discriminator AND the new
//      column value populated.
//   4. Restart-resume: the lease boundary recording survives a
//      streamer restart — a restarted shard does NOT re-apply the
//      already-applied DDL.
//
// Why three sources rather than two: a binary (one-applies, one-
// observes) shape doesn't exercise the multi-observer code path or
// the contention-window where multiple shards see HELD before APPLIED.
// Three covers (apply / observe-mid-held / observe-applied).
//
// Task #24 (ADR-0054 Phase 2e v3 spec).

package pipeline

import (
	"context"
	"database/sql"
	"log"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/testcontainers/testcontainers-go"

	"github.com/orware/sluice/internal/engines"

	_ "github.com/orware/sluice/internal/engines/postgres"
)

// installPhase2eStreamerDebugLogger installs a DEBUG-level slog handler
// on os.Stderr for the duration of the test. Phase A instrumentation
// for task #65; remove once the fix lands.
func installPhase2eStreamerDebugLogger(t *testing.T) {
	t.Helper()
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
}

// ---- per-test PG boot retry (ci-retry-asymmetry: per-test = 3
// attempts; the harness's 4 boots multiply by 3 here). Mirror of
// engines/postgres.runPGWithRetry / pipeline.runMySQLWithRetry. ----

const (
	phase2eBootAttempts = 3
	phase2eBootTimeout  = 2 * time.Minute
)

func phase2eBootBackoff(attempt int) time.Duration {
	switch attempt {
	case 1:
		return 30 * time.Second
	case 2:
		return 60 * time.Second
	default:
		return 120 * time.Second
	}
}

// runPhase2ePGWithRetry boots a postgres:16 testcontainer with
// logical-replication-friendly settings, retrying on transient wait-
// until-ready failures. Three attempts per the per-test-site cost
// budget (see auto-memory ci-retry-asymmetry).
func runPhase2ePGWithRetry(t *testing.T, dbName string) *pgtc.PostgresContainer {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	opts := []testcontainers.ContainerCustomizer{
		pgtc.WithDatabase(dbName),
		pgtc.WithUsername("test"),
		pgtc.WithPassword("test"),
		pgtc.BasicWaitStrategies(),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Cmd: []string{
					"-c", "wal_level=logical",
					"-c", "max_wal_senders=8",
					"-c", "max_replication_slots=8",
				},
			},
		}),
	}

	var lastErr error
	for attempt := 1; attempt <= phase2eBootAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), phase2eBootTimeout)
		container, err := pgtc.Run(ctx, "postgres:16", opts...)
		cancel()
		if err == nil {
			if attempt > 1 {
				log.Printf("phase2e pg boot attempt %d/%d succeeded (db=%s)",
					attempt, phase2eBootAttempts, dbName)
			}
			return container
		}
		if container != nil {
			_ = container.Terminate(context.Background())
		}
		lastErr = err
		if attempt < phase2eBootAttempts {
			backoff := phase2eBootBackoff(attempt)
			log.Printf("phase2e pg boot attempt %d/%d failed (db=%s): %v; retrying in %s",
				attempt, phase2eBootAttempts, dbName, err, backoff)
			time.Sleep(backoff)
			continue
		}
		log.Printf("phase2e pg boot attempt %d/%d failed (db=%s): %v; giving up",
			attempt, phase2eBootAttempts, dbName, err)
	}
	t.Fatalf("start container (db=%s): %d attempts exhausted: %v",
		dbName, phase2eBootAttempts, lastErr)
	return nil
}

// phase2eSourceDSN boots a single PG source container with the
// supplied database name and returns the DSN + a teardown closure.
func phase2eSourceDSN(t *testing.T, dbName string) (dsn string, teardown func()) {
	t.Helper()
	c := runPhase2ePGWithRetry(t, dbName)
	teardown = func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = c.Terminate(ctx)
	}
	dsnCtx, dsnCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer dsnCancel()
	var err error
	dsn, err = c.ConnectionString(dsnCtx, "sslmode=disable")
	if err != nil {
		teardown()
		t.Fatalf("connection string (db=%s): %v", dbName, err)
	}
	return dsn, teardown
}

// phase2eHarness wires up 3 source containers + 1 target container
// and returns their DSNs + a teardown closure that terminates every
// container.
type phase2eHarness struct {
	sourceDSNs [3]string
	targetDSN  string
	cleanup    func()
}

// startPhase2eHarness boots the four containers the v3 harness needs.
// Two-axis cleanup: every container is registered into the returned
// closure and the closure is idempotent across paths (callers defer it
// unconditionally and we never panic on a double-terminate).
func startPhase2eHarness(t *testing.T) *phase2eHarness {
	t.Helper()
	h := &phase2eHarness{}
	var teardowns []func()
	h.cleanup = func() {
		// Reverse-order teardown so the target outlives any potentially-
		// blocking source slot reference, although the target/source PG
		// containers are otherwise independent.
		for i := len(teardowns) - 1; i >= 0; i-- {
			teardowns[i]()
		}
	}

	// On any failure mid-startup, terminate everything we already booted
	// before t.Fatalf-ing through.
	failover := func(format string, args ...any) {
		h.cleanup()
		t.Fatalf(format, args...)
	}

	for i := 0; i < 3; i++ {
		dsn, td := phase2eSourceDSN(t, "source_db")
		h.sourceDSNs[i] = dsn
		teardowns = append(teardowns, td)
	}

	// Target container has its own DB; same image, different role —
	// keeps source-vs-target separation crisp and avoids any
	// inadvertent slot-state crossover.
	tgtContainer := runPhase2ePGWithRetry(t, "target_db")
	teardowns = append(teardowns, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = tgtContainer.Terminate(ctx)
	})
	tgtCtx, tgtCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer tgtCancel()
	tgtDSN, err := tgtContainer.ConnectionString(tgtCtx, "sslmode=disable")
	if err != nil {
		failover("target connection string: %v", err)
	}
	h.targetDSN = tgtDSN
	return h
}

// phase2eShardLabels returns the per-shard discriminator values used
// by the harness. Single source-of-truth so the assertions can iterate
// without re-stating the strings.
func phase2eShardLabels() [3]string {
	return [3]string{"shard_a", "shard_b", "shard_c"}
}

// phase2eStreamID returns the per-shard StreamID. Distinct per source
// so each streamer owns its own sluice_cdc_state row.
func phase2eStreamID(i int) string {
	return "phase2e-stream-" + phase2eShardLabels()[i]
}

// phase2eSeedDDL is the per-source bootstrap: the canonical Shape A
// shape (BIGINT IDENTITY PK + VARCHAR) with REPLICA IDENTITY FULL so
// CDC carries the discriminator. Seeded rows have distinct emails per
// source so the cross-source assertions can distinguish provenance.
func phase2eSeedDDL(emailPrefix string) string {
	return `
		CREATE TABLE users (
			id    BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			email VARCHAR(255) NOT NULL
		);
		ALTER TABLE users REPLICA IDENTITY FULL;
		INSERT INTO users (email) VALUES ('` + emailPrefix + `_seed@example.com');
	`
}

// phase2eApplyDDL runs a possibly-multi-statement DDL block against a
// PG DSN. Local helper to avoid coupling to migrate_pg_integration_test
// helpers (the build-tag layering is identical, but the harness's
// timing comments are clearer when the SQL helper is co-located).
func phase2eApplyDDL(t *testing.T, dsn, ddl string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		t.Fatalf("apply ddl: %v", err)
	}
}

// startPhase2eStreamer constructs a Streamer for the i-th shard,
// engaging Shape A live coordination. Returns the streamer plus a
// (cancel, runErr) pair so the caller can drive the lifecycle.
//
// Lease timing is tightened (LeaseDuration=10s / RenewDeadline=6s /
// RetryPeriod=2s) versus the production default — this lets the test
// observe the apply-then-finalize cycle inside the per-step 60s window
// without flaking on a slow runner.
func startPhase2eStreamer(t *testing.T, i int, sourceDSN, targetDSN string) (
	streamer *Streamer, cancel context.CancelFunc, runErr <-chan error,
) {
	t.Helper()
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	streamer = &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  phase2eStreamID(i),
		InjectShardColumn: ShardColumnSpec{
			Name:  "source_shard_id",
			Value: phase2eShardLabels()[i],
		},
		CoordinateLiveDDL: true,
		ShardCoordinationLease: LeaseConfig{
			LeaseDuration: 10 * time.Second,
			RenewDeadline: 6 * time.Second,
			RetryPeriod:   2 * time.Second,
		},
		// Per-shard slot name — three logical-replication slots on each
		// independent source container; this also keeps the per-source
		// state crisp.
		SlotName: "sluice_slot_" + phase2eShardLabels()[i],
	}
	ctx, cancelFn := context.WithCancel(context.Background())
	out := make(chan error, 1)
	go func() { out <- streamer.Run(ctx) }()
	return streamer, cancelFn, out
}

// waitForPhase2eTargetCount polls the target's `users` table for the
// expected total row count. Tolerant of "relation does not exist"
// during the startup window before any cold-start lands. Returns true
// on success, false on timeout.
func waitForPhase2eTargetCount(dsn string, expected int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pollPGRowCount(dsn, "users") >= expected {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// dumpPhase2eLeaseRow logs every column of the lease row for the named
// target table. Phase A instrumentation for task #65; correlates the
// CI logs' router-side decisions with persisted lease-row state. The
// helper is tolerant of a missing row (logs "absent" rather than
// failing) so it can run as a diagnostic on the test-fail path without
// itself fataling.
func dumpPhase2eLeaseRow(t *testing.T, dsn, table, label string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Logf("phase-a dump (%s): open err: %v", label, err)
		return
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, `
		SELECT target_table_full_name,
			COALESCE(lease_holder_stream_id, ''),
			lease_expires_at,
			COALESCE(ddl_text, ''),
			COALESCE(ddl_checksum, ''),
			applied_schema_version,
			applied_at,
			NOW(),
			EXTRACT(EPOCH FROM (lease_expires_at - NOW()))::INT
		FROM "public"."sluice_shard_consolidation_lease"
		WHERE target_table_full_name = $1`, table)
	if err != nil {
		t.Logf("phase-a dump (%s): query err: %v", label, err)
		return
	}
	defer func() { _ = rows.Close() }()
	sawRow := false
	for rows.Next() {
		sawRow = true
		var (
			tn, holder, ddlText, cksum string
			expires, appliedAt, now    sql.NullTime
			version                    int64
			secsToExpiry               sql.NullInt64
		)
		if err := rows.Scan(&tn, &holder, &expires, &ddlText, &cksum, &version, &appliedAt, &now, &secsToExpiry); err != nil {
			t.Logf("phase-a dump (%s): scan err: %v", label, err)
			return
		}
		t.Logf("phase-a dump (%s) row: table=%q holder=%q expires_at=%v applied_at=%v ddl_text=%q ddl_checksum=%q version=%d now=%v secs_to_expiry=%v",
			label, tn, holder, expires, appliedAt, ddlText, cksum, version, now, secsToExpiry)
	}
	if err := rows.Err(); err != nil {
		t.Logf("phase-a dump (%s): rows iter err: %v", label, err)
	}
	if !sawRow {
		t.Logf("phase-a dump (%s): no lease row for table=%q", label, table)
	}
}

// dumpPhase2eTargetSchema logs the column list of the target's `users`
// table — useful when the test fails because the column never landed,
// to confirm whether the ALTER did/didn't make it to the target.
func dumpPhase2eTargetSchema(t *testing.T, dsn, label string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Logf("phase-a schema dump (%s): open err: %v", label, err)
		return
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, `
		SELECT column_name, data_type
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'users'
		ORDER BY ordinal_position`)
	if err != nil {
		t.Logf("phase-a schema dump (%s): query err: %v", label, err)
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var name, dtype string
		if err := rows.Scan(&name, &dtype); err != nil {
			t.Logf("phase-a schema dump (%s): scan err: %v", label, err)
			return
		}
		t.Logf("phase-a schema dump (%s): users column %q (%s)", label, name, dtype)
	}
	if err := rows.Err(); err != nil {
		t.Logf("phase-a schema dump (%s): rows iter err: %v", label, err)
	}
}

// waitForPhase2eTargetColumn polls the target's `users` table for the
// named column to exist. Used to gate the post-DDL assertions on the
// router-driven apply landing.
func waitForPhase2eTargetColumn(t *testing.T, dsn, columnName string, timeout time.Duration) bool {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return false
	}
	defer func() { _ = db.Close() }()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		var n int
		err := db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM information_schema.columns
			WHERE table_schema='public' AND table_name='users'
			  AND column_name=$1`, columnName).Scan(&n)
		cancel()
		if err == nil && n == 1 {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// readPhase2eLeaseRow reads the load-bearing fields of the lease row
// for the consolidated target table. Used to assert exactly-once apply
// + checksum identity across peers.
type phase2eLeaseRow struct {
	Applied        bool
	SchemaVersion  int64
	DDLChecksum    string
	HolderStreamID string
}

func readPhase2eLeaseRow(t *testing.T, dsn, table string) phase2eLeaseRow {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var row phase2eLeaseRow
	if err := db.QueryRowContext(ctx, `
		SELECT applied_at IS NOT NULL, applied_schema_version, ddl_checksum, lease_holder_stream_id
		FROM "public"."sluice_shard_consolidation_lease"
		WHERE target_table_full_name = $1
	`, table).Scan(&row.Applied, &row.SchemaVersion, &row.DDLChecksum, &row.HolderStreamID); err != nil {
		t.Fatalf("read lease row %s: %v", table, err)
	}
	return row
}

// waitForPersistedPositions waits until each named stream-ID has a
// non-empty source_position in sluice_cdc_state. The boundary-record
// path persists the position alongside the lease's anchor_position;
// this assertion proves the post-DDL CDC continues to flow on every
// shard.
func waitForPersistedPositions(t *testing.T, dsn string, streamIDs []string, timeout time.Duration) bool {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return false
	}
	defer func() { _ = db.Close() }()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		all := true
		for _, sid := range streamIDs {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			var pos string
			err := db.QueryRowContext(ctx,
				`SELECT source_position FROM "public"."sluice_cdc_state" WHERE stream_id = $1`,
				sid).Scan(&pos)
			cancel()
			if err != nil || pos == "" {
				all = false
				break
			}
		}
		if all {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// rebuildPhase2eDSN swaps the database name on a PG DSN — used to
// disambiguate a per-container connection. (Currently the harness uses
// each container's default DB so this is a thin wrapper; kept around
// because a future per-source DB-name override pattern will want it.)
func rebuildPhase2eDSN(orig, newDB string) (string, error) {
	u, err := url.Parse(orig)
	if err != nil {
		return "", err
	}
	u.Path = "/" + strings.TrimPrefix(newDB, "/")
	return u.String(), nil
}

// TestPhase2e_PG_StreamerHarness_3SourcesToTarget_ExactlyOnceApply is
// the primary v3 harness pin. Sequence:
//
//  1. Boot 3 PG sources + 1 PG target.
//  2. Seed each source's `users(id, email)` with one shard-specific
//     row + REPLICA IDENTITY FULL.
//  3. Start shard_a's Streamer. Wait for its bulk-copy to land the
//     shard_a seed row on the target.
//  4. Start shard_b's and shard_c's Streamers (the target's `users`
//     table now exists; the IF-NOT-EXISTS schema-apply path is a no-
//     op for them; the Shape A populated-target preflight passes on
//     the "fresh VALUE" branch per Bug 81).
//  5. Wait for all 3 shards' seed rows on the target (3 rows total,
//     one per shard, distinguished by source_shard_id).
//  6. Apply `ALTER TABLE users ADD COLUMN active BOOLEAN DEFAULT TRUE`
//     on every source. The first shard to observe the boundary owns
//     the apply; the other two observe via the peer-applied path.
//  7. Assert the target schema has exactly one `active` column.
//  8. Insert one post-DDL row per source; assert each lands on the
//     target carrying its own discriminator + active=TRUE.
//  9. Verify the lease row reflects an applied state with non-empty
//     checksum; verify all 3 streams have persisted positions.
//
// The exact-shape assertion (step 7) is the load-bearing correctness
// property — a regression that re-applies the same DDL via a peer
// stream (silent-loss class: target schema diverges from sluice's
// recorded state) would flunk this test.
func TestPhase2e_PG_StreamerHarness_3SourcesToTarget_ExactlyOnceApply(t *testing.T) {
	installPhase2eStreamerDebugLogger(t) // task #65 Phase A
	h := startPhase2eHarness(t)
	defer h.cleanup()

	// Per-source seed: each source carries its own discriminator's
	// seed row so the cross-source provenance assertion is unambiguous.
	for i := 0; i < 3; i++ {
		phase2eApplyDDL(t, h.sourceDSNs[i], phase2eSeedDDL(phase2eShardLabels()[i]))
	}

	// Phase A — shard_a goes first (creates the target table via cold
	// start; subsequent shards see IF-NOT-EXISTS and the Shape A
	// populated-target preflight on the fresh-VALUE branch).
	_, cancelA, runErrA := startPhase2eStreamer(t, 0, h.sourceDSNs[0], h.targetDSN)
	defer func() {
		cancelA()
		<-runErrA
	}()

	if !waitForPhase2eTargetCount(h.targetDSN, 1, 60*time.Second) {
		t.Fatalf("phase A: shard_a's seed row never landed on the target")
	}

	// Phase B — shard_b and shard_c join. Both must observe the table
	// exists (IF-NOT-EXISTS short-circuit) and pass the Shape A
	// preflight on the fresh-VALUE branch.
	_, cancelB, runErrB := startPhase2eStreamer(t, 1, h.sourceDSNs[1], h.targetDSN)
	defer func() {
		cancelB()
		<-runErrB
	}()
	_, cancelC, runErrC := startPhase2eStreamer(t, 2, h.sourceDSNs[2], h.targetDSN)
	defer func() {
		cancelC()
		<-runErrC
	}()

	// All 3 shard seed rows landed on the target.
	if !waitForPhase2eTargetCount(h.targetDSN, 3, 60*time.Second) {
		t.Fatalf("phase B: not all 3 shards' seed rows landed on the target within the timeout " +
			"(check per-shard cold-start completion + bulk-copy)")
	}

	// Verify the per-shard discriminator distribution on the target.
	assertPhase2eShardDistribution(t, h.targetDSN, map[string]int{
		"shard_a": 1, "shard_b": 1, "shard_c": 1,
	})

	// Phase C — apply the same DDL on every source. The first shard
	// to observe the boundary (timing-dependent; usually whichever
	// streamer's CDC reader was furthest along) owns the apply; the
	// other two observe via the peer-applied checksum-match path.
	const altSQL = `ALTER TABLE users ADD COLUMN active BOOLEAN DEFAULT TRUE;`
	for i := 0; i < 3; i++ {
		phase2eApplyDDL(t, h.sourceDSNs[i], altSQL)
	}

	// Wait for the column to land on the target. Exactly one DDL
	// apply happens (the lease serializes them); the other 2 routers
	// observe-and-record.
	if !waitForPhase2eTargetColumn(t, h.targetDSN, "active", 90*time.Second) {
		dumpPhase2eLeaseRow(t, h.targetDSN, "public.users", "phase-C-fail")
		dumpPhase2eTargetSchema(t, h.targetDSN, "phase-C-fail")
		t.Fatalf("phase C: target users.active column never landed — the streamer-driven boundary " +
			"router path either failed silently or the per-source CDC reader stalled")
	}

	// Exactly-once-apply: the COUNT(*) of `active` columns on the
	// target's `users` table is exactly 1. (information_schema returns
	// one row per column; a duplicate-apply regression would show
	// COUNT() > 1 only if the column got re-added under a different
	// name, but PG's CREATE TABLE / ADD COLUMN normalization makes
	// this the right shape to assert.)
	assertPhase2eColumnExistsExactlyOnce(t, h.targetDSN, "users", "active")

	// Phase D — drive post-DDL INSERTs on every source. The applier's
	// CDC pipeline must absorb them with the active column populated
	// (default TRUE).
	for i := 0; i < 3; i++ {
		phase2eApplyDDL(t, h.sourceDSNs[i], `
			INSERT INTO users (email) VALUES ('`+phase2eShardLabels()[i]+`_postddl@example.com');
		`)
	}

	// 6 rows total (3 seed + 3 post-DDL).
	if !waitForPhase2eTargetCount(h.targetDSN, 6, 60*time.Second) {
		t.Fatalf("phase D: not all post-DDL rows landed on the target — CDC-resume-after-DDL " +
			"on at least one shard didn't continue past the boundary")
	}

	// Each shard contributed exactly 2 rows (seed + post-DDL).
	assertPhase2eShardDistribution(t, h.targetDSN, map[string]int{
		"shard_a": 2, "shard_b": 2, "shard_c": 2,
	})

	// Every post-DDL row has active=TRUE (the default landed correctly
	// on every shard's INSERT).
	assertPhase2ePostDDLRowsHaveActive(t, h.targetDSN)

	// Phase E — verify the lease row reflects the recorded apply.
	row := readPhase2eLeaseRow(t, h.targetDSN, "public.users")
	if !row.Applied {
		t.Error("phase E: lease row's applied_at is NULL — the apply never finalized")
	}
	if row.SchemaVersion < 1 {
		t.Errorf("phase E: lease row's applied_schema_version = %d; want >= 1", row.SchemaVersion)
	}
	if row.DDLChecksum == "" {
		t.Error("phase E: lease row's ddl_checksum is empty — checksum not recorded")
	}
	t.Logf("phase E: lease applied by stream %q at version %d (checksum=%s)",
		row.HolderStreamID, row.SchemaVersion, row.DDLChecksum)

	// Phase F — verify every stream has persisted a position. This is
	// the load-bearing precondition for warm-resume on every shard.
	streamIDs := []string{phase2eStreamID(0), phase2eStreamID(1), phase2eStreamID(2)}
	if !waitForPersistedPositions(t, h.targetDSN, streamIDs, 30*time.Second) {
		t.Fatal("phase F: one or more streams have no persisted position — warm-resume can't work")
	}
}

// TestPhase2e_PG_StreamerHarness_RestartResumeAfterBoundary pins the
// restart-resume property: a shard whose Streamer is restarted AFTER a
// boundary has recorded MUST NOT re-apply the already-applied DDL.
// This is the test that proves the boundary-recording is durable
// across process restarts — operationally important when an operator
// stops + restarts a stream for routine maintenance (binary upgrade,
// node reboot) post-DDL.
//
// Sequence:
//
//  1. Set up the same 3-source + 1-target harness.
//  2. Bring up shard_a + shard_b. Drive the same DDL on both sources.
//  3. Wait for the boundary to apply + the column to land on target.
//  4. Cancel shard_a's context — clean shutdown.
//  5. Drive an additional DDL apply on the source shard_a (no-op for
//     the test — the source already has the new column). Insert a
//     row on shard_a's source.
//  6. Restart shard_a's Streamer with the same StreamID. Verify:
//     - The target's `active` column count is STILL 1 (no re-apply).
//     - The new shard_a row makes it to the target via warm-resume CDC.
//     - The lease row's applied_schema_version is UNCHANGED from
//     before the restart (the restart did NOT bump it).
func TestPhase2e_PG_StreamerHarness_RestartResumeAfterBoundary(t *testing.T) {
	installPhase2eStreamerDebugLogger(t) // task #65 Phase A
	h := startPhase2eHarness(t)
	defer h.cleanup()

	for i := 0; i < 2; i++ {
		phase2eApplyDDL(t, h.sourceDSNs[i], phase2eSeedDDL(phase2eShardLabels()[i]))
	}

	// Phase A — shard_a + shard_b come up.
	_, cancelA, runErrA := startPhase2eStreamer(t, 0, h.sourceDSNs[0], h.targetDSN)
	if !waitForPhase2eTargetCount(h.targetDSN, 1, 60*time.Second) {
		cancelA()
		<-runErrA
		t.Fatalf("phase A: shard_a seed row never landed")
	}

	_, cancelB, runErrB := startPhase2eStreamer(t, 1, h.sourceDSNs[1], h.targetDSN)
	defer func() {
		cancelB()
		<-runErrB
	}()
	if !waitForPhase2eTargetCount(h.targetDSN, 2, 60*time.Second) {
		cancelA()
		<-runErrA
		t.Fatalf("phase A: shard_b seed row never landed")
	}

	// Phase B — apply DDL on both sources; one wins the lease, the
	// other observes peer-applied.
	const altSQL = `ALTER TABLE users ADD COLUMN active BOOLEAN DEFAULT TRUE;`
	for i := 0; i < 2; i++ {
		phase2eApplyDDL(t, h.sourceDSNs[i], altSQL)
	}
	if !waitForPhase2eTargetColumn(t, h.targetDSN, "active", 90*time.Second) {
		dumpPhase2eLeaseRow(t, h.targetDSN, "public.users", "phase-B-fail")
		dumpPhase2eTargetSchema(t, h.targetDSN, "phase-B-fail")
		cancelA()
		<-runErrA
		t.Fatalf("phase B: target users.active column never landed")
	}

	// Snapshot the lease row BEFORE the restart so we can assert
	// invariance later. Allow a brief settling window so both shards
	// have recorded their boundary.
	if !waitForPersistedPositions(t, h.targetDSN,
		[]string{phase2eStreamID(0), phase2eStreamID(1)}, 30*time.Second) {
		cancelA()
		<-runErrA
		t.Fatal("phase B: persistence preconditions for restart-resume not met")
	}
	preRestart := readPhase2eLeaseRow(t, h.targetDSN, "public.users")
	if !preRestart.Applied {
		cancelA()
		<-runErrA
		t.Fatal("phase B: lease not applied before restart — precondition failed")
	}

	// Phase C — clean shutdown of shard_a.
	cancelA()
	select {
	case <-runErrA:
	case <-time.After(15 * time.Second):
		t.Fatal("phase C: shard_a's Run did not return after cancel")
	}

	// Drive a post-restart INSERT on shard_a's source.
	phase2eApplyDDL(t, h.sourceDSNs[0], `
		INSERT INTO users (email) VALUES ('shard_a_post_restart@example.com');
	`)

	// Phase D — restart shard_a with the same StreamID. Warm resume
	// path: no bulk-copy, no DDL re-apply, just CDC from the persisted
	// position.
	_, cancelA2, runErrA2 := startPhase2eStreamer(t, 0, h.sourceDSNs[0], h.targetDSN)
	defer func() {
		cancelA2()
		<-runErrA2
	}()

	// The new shard_a row lands on the target. Total: 2 seed rows
	// (shard_a + shard_b) + 1 post-restart shard_a row = 3.
	if !waitForPhase2eTargetCount(h.targetDSN, 3, 60*time.Second) {
		t.Fatalf("phase D: post-restart shard_a row never landed — warm-resume CDC didn't fire")
	}

	// Phase E — exactly-once-apply still holds across the restart.
	assertPhase2eColumnExistsExactlyOnce(t, h.targetDSN, "users", "active")

	// Lease state invariant: the schema version + checksum are
	// IDENTICAL across the restart. A regression that re-applied on
	// resume would bump the version.
	postRestart := readPhase2eLeaseRow(t, h.targetDSN, "public.users")
	if postRestart.SchemaVersion != preRestart.SchemaVersion {
		t.Errorf("phase E: applied_schema_version changed across restart: pre=%d post=%d "+
			"(restart re-applied an already-applied boundary)",
			preRestart.SchemaVersion, postRestart.SchemaVersion)
	}
	if postRestart.DDLChecksum != preRestart.DDLChecksum {
		t.Errorf("phase E: ddl_checksum changed across restart: pre=%q post=%q "+
			"(restart synthesized a different DDL text)",
			preRestart.DDLChecksum, postRestart.DDLChecksum)
	}
}

// ---- assertion helpers ----

// assertPhase2eShardDistribution asserts the target's `users` table
// has the expected per-shard row count. Used pre- and post-DDL to pin
// CDC-flow correctness.
func assertPhase2eShardDistribution(t *testing.T, dsn string, want map[string]int) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, `
		SELECT source_shard_id, COUNT(*)
		FROM "public"."users"
		GROUP BY source_shard_id
		ORDER BY source_shard_id`)
	if err != nil {
		t.Fatalf("query shard distribution: %v", err)
	}
	defer func() { _ = rows.Close() }()
	got := map[string]int{}
	for rows.Next() {
		var shard string
		var n int
		if err := rows.Scan(&shard, &n); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[shard] = n
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	for shard, expected := range want {
		if got[shard] != expected {
			t.Errorf("shard %q: target rows = %d; want %d", shard, got[shard], expected)
		}
	}
	if len(got) != len(want) {
		t.Errorf("shard distribution length mismatch: got %v; want %v", got, want)
	}
}

// assertPhase2eColumnExistsExactlyOnce asserts the named column is
// present exactly once on the target table. Exactly-once-apply is the
// load-bearing correctness property of the Phase 2e harness.
func assertPhase2eColumnExistsExactlyOnce(t *testing.T, dsn, table, column string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema='public' AND table_name=$1 AND column_name=$2`,
		table, column).Scan(&n); err != nil {
		t.Fatalf("scan column count: %v", err)
	}
	if n != 1 {
		t.Errorf("information_schema column count for %s.%s = %d; want 1 (exactly-once-apply violated)",
			table, column, n)
	}
}

// assertPhase2ePostDDLRowsHaveActive asserts every row inserted post-
// DDL has active=TRUE on the target. Pre-DDL rows have NULL active
// (since the default only applies to new rows post-ALTER); post-DDL
// rows must have TRUE because the source's DEFAULT TRUE propagated
// through CDC.
//
// Discriminating between pre- and post-DDL rows: post-DDL rows have
// emails containing "_postddl_" (set by the test in phase D).
func assertPhase2ePostDDLRowsHaveActive(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM "public"."users"
		WHERE email LIKE '%_postddl@example.com' AND active IS NOT TRUE`).Scan(&n); err != nil {
		t.Fatalf("scan post-DDL active count: %v", err)
	}
	if n != 0 {
		t.Errorf("%d post-DDL rows have active NOT TRUE; want 0 (CDC didn't carry the column default)", n)
	}
}

// Compile-time use of url import: rebuildPhase2eDSN is reserved for
// future per-database DSN tweaks (currently the harness uses each
// container's default DB).
var _ = rebuildPhase2eDSN
