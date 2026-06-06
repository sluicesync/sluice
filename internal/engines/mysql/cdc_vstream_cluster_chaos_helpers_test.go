//go:build integration && vitesscluster && chaos

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// CHAOS harness — fault-injection layer on top of the full Vitess
// cluster harness (cdc_vstream_cluster_integration_test.go). Where the
// `vitesscluster`-only suites prove sluice survives the *expected* Vitess
// lifecycle (online-DDL cutover, primary-only topology), this suite drives
// real INFRASTRUCTURE FAILURES — killed tablets, reparents, restarted
// vtgate, a rolling version bump — under an in-flight sluice sync and
// asserts the load-bearing tenet:
//
//	the first real migration that silently corrupts data ends the
//	project's credibility permanently.
//
// THE ONE INVARIANT every chaos scenario asserts (see assertZeroLossOrLoud):
// after the fault + recovery, EITHER
//
//	target COUNT(*) == source  AND  COUNT(DISTINCT pk) == COUNT(*)
//	(zero loss, zero dup)
//
// OR sluice surfaced a LOUD error (the stream's Err() != nil / a non-nil
// error returned to the caller). NEVER a silent partial, NEVER a silent
// hang. Loud failure is an acceptable outcome; silent loss is not.
//
// Build tag: this suite carries its OWN extra tag (`chaos`) on top of
// `integration && vitesscluster` so the heavy fault-injection runs are
// opt-in and are NOT pulled into the default `vitesscluster` runs. Run:
//
//	go test -tags='integration vitesscluster chaos' -v -count=1 -timeout=40m \
//	  -run 'TestVitessChaos' ./internal/engines/mysql/...
//
// All injection helpers shell out to the SAME `docker`/`vtctldclient`
// vehicles the base harness uses (no testcontainers API), so the chaos
// layer adds zero dependency surface. They take the compose project name
// (returned implicitly via the chaosCluster handle) and operate on the
// running services by their compose-service names.

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// Compose service names in testdata/vitesscluster/docker-compose.yml.
// Centralised so the scenarios refer to topology by name, not string
// literals scattered through the tests.
const (
	svcEtcd            = "etcd"
	svcVtctld          = "vtctld"
	svcVtgate          = "vtgate"
	svcTabletPrimary   = "vttablet"         // uid 100, the elected primary
	svcTabletReplica   = "vttablet-replica" // uid 101, the replica
	tabletAliasPrim    = "zone1-0000000100"
	tabletAliasReplica = "zone1-0000000101"
	chaosKeyspace      = "test"
	chaosShard         = "0"
)

// chaosCluster is the handle the chaos scenarios drive. It bundles the
// connection coordinates startVitessCluster already returns with the
// compose plumbing (binary, file, project, env) the injection helpers
// need to address individual services. It is built by startChaosCluster,
// a thin wrapper over startVitessCluster that also captures the compose
// addressing so faults can target services by name.
type chaosCluster struct {
	mysqlDSN     string
	grpcEndpoint string
	keyspace     string

	dockerBin   string
	composeFile string
	project     string
	baseEnv     []string

	cleanup func()
}

// startChaosCluster boots the full cluster (via the same compose file +
// readiness gate as startVitessCluster) and returns a handle that also
// carries the compose addressing the fault helpers need. It deliberately
// re-derives the project name + env the SAME way startVitessCluster does
// so the helpers target the very stack the base harness brought up.
//
// We re-implement the bring-up here (rather than calling startVitessCluster
// and trying to recover its private project name) because the fault
// helpers must know the exact `-p <project>` to address services, and that
// value is local to startVitessCluster. Keeping a parallel bring-up here is
// the smaller wart than widening startVitessCluster's return signature for
// a chaos-only need; the bring-up logic is intentionally identical.
func startChaosCluster(t *testing.T) *chaosCluster {
	t.Helper()

	dockerBin := findDocker(t)
	composeFile := composeFilePath(t)
	project := fmt.Sprintf("sluice-vitesschaos-%d", os.Getpid())

	baseEnv := append(
		os.Environ(),
		"COMPOSE_PROJECT="+project,
		fmt.Sprintf("VTGATE_MYSQL_PORT=%d", chaosMySQLPort),
		fmt.Sprintf("VTGATE_GRPC_PORT=%d", chaosGRPCPort),
	)

	cc := &chaosCluster{
		dockerBin:   dockerBin,
		composeFile: composeFile,
		project:     project,
		baseEnv:     baseEnv,
	}

	cc.cleanup = func() {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		if out, err := cc.runCompose(ctx, "down", "-v", "--remove-orphans"); err != nil {
			t.Logf("chaos cluster teardown: %v\n%s", err, out)
		}
	}

	upCtx, upCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer upCancel()
	if out, err := cc.runCompose(upCtx, "up", "-d"); err != nil {
		cc.cleanup()
		t.Fatalf("chaos docker compose up: %v\n%s", err, out)
	}

	cc.mysqlDSN = fmt.Sprintf(
		"root@tcp(127.0.0.1:%d)/%s?parseTime=true&interpolateParams=true",
		chaosMySQLPort, chaosKeyspace,
	)
	cc.grpcEndpoint = fmt.Sprintf("127.0.0.1:%d", chaosGRPCPort)
	cc.keyspace = chaosKeyspace

	if err := waitForWritablePrimary(t, cc.mysqlDSN, 4*time.Minute); err != nil {
		out, _ := cc.runCompose(context.Background(), "logs", "--tail", "40")
		cc.cleanup()
		t.Fatalf("chaos cluster never reached writable PRIMARY: %v\nrecent logs:\n%s", err, out)
	}

	return cc
}

// Distinct host ports from the base + primary-only harnesses so a chaos
// stack never collides with one of those (or a stale stack from a crashed
// run).
const (
	chaosMySQLPort = 15506
	chaosGRPCPort  = 15791
)

// runCompose runs a `docker compose` subcommand against THIS chaos
// project. Mirrors the closure inside startVitessCluster.
func (cc *chaosCluster) runCompose(ctx context.Context, args ...string) ([]byte, error) {
	full := append([]string{"compose", "-f", cc.composeFile, "-p", cc.project}, args...)
	cmd := exec.CommandContext(ctx, cc.dockerBin, full...)
	cmd.Env = cc.baseEnv
	return cmd.CombinedOutput()
}

// ----------------------------------------------------------------------
// chaos_helpers — fault injection
//
// All helpers address compose SERVICES (not raw container IDs): `docker
// compose -p <project> kill <service>` resolves the running container for
// us, so the helpers stay robust to compose's container-naming scheme.
// ----------------------------------------------------------------------

// killContainer hard-kills a compose service (SIGKILL by default) — the
// abrupt-crash fault. The container stops immediately with no graceful
// shutdown, the closest in-test analog to a process OOM/segfault/host
// loss. Pass a non-empty signal to override (e.g. "SIGTERM").
func (cc *chaosCluster) killContainer(t *testing.T, service, signal string) {
	t.Helper()
	args := []string{"kill"}
	if signal != "" {
		args = append(args, "--signal", signal)
	}
	args = append(args, service)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if out, err := cc.runCompose(ctx, args...); err != nil {
		t.Fatalf("kill %s (signal=%q): %v\n%s", service, signal, err, out)
	}
	t.Logf("CHAOS: killed service %s (signal=%q)", service, signal)
}

// stopContainer gracefully stops a compose service (SIGTERM then a
// timeout-bounded SIGKILL). Used where a clean shutdown is the fault under
// test (e.g. a controlled vtgate drain) rather than an abrupt crash.
func (cc *chaosCluster) stopContainer(t *testing.T, service string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if out, err := cc.runCompose(ctx, "stop", service); err != nil {
		t.Fatalf("stop %s: %v\n%s", service, err, out)
	}
	t.Logf("CHAOS: stopped service %s", service)
}

// startService (re)starts a previously stopped/killed compose service.
// Pairs with killContainer/stopContainer to model crash→recover.
func (cc *chaosCluster) startService(t *testing.T, service string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	if out, err := cc.runCompose(ctx, "up", "-d", service); err != nil {
		t.Fatalf("start %s: %v\n%s", service, err, out)
	}
	t.Logf("CHAOS: (re)started service %s", service)
}

// chaosUpgradeOverridePath resolves the rolling-upgrade compose override
// (docker-compose.chaos-upgrade.yml) next to the base compose file. The
// override re-pins the component images to CHAOS_UPGRADE_IMAGE so a
// per-service recreate lands the new tag. See scenario 4.
func chaosUpgradeOverridePath(t *testing.T) string {
	t.Helper()
	base := composeFilePath(t)
	return strings.Replace(base, "docker-compose.yml", "docker-compose.chaos-upgrade.yml", 1)
}

// recreateServiceWithOverride recreates a SINGLE service onto the upgrade
// override (the new image tag) without touching its dependencies — the
// per-component step of the rolling upgrade. `--no-deps` keeps the rest of
// the cluster serving; `--force-recreate` guarantees the container is
// rebuilt onto the overridden image even if compose thinks config is
// unchanged.
//
// It layers the override file ON TOP of the base compose by passing both
// `-f` files; baseEnv must carry CHAOS_UPGRADE_IMAGE (the local session
// sets it before running the un-skipped scenario).
func (cc *chaosCluster) recreateServiceWithOverride(t *testing.T, overrideFile, service string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	full := []string{
		"compose", "-f", cc.composeFile, "-f", overrideFile, "-p", cc.project,
		"up", "-d", "--no-deps", "--force-recreate", service,
	}
	cmd := exec.CommandContext(ctx, cc.dockerBin, full...)
	cmd.Env = cc.baseEnv
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("recreate %s onto upgrade override: %v\n%s", service, err, out)
	}
	t.Logf("CHAOS: recreated %s onto the upgrade image", service)
}

// restartContainer issues a single `docker compose restart` — the
// stop+start of an already-running service in one step (vtgate-restart
// fault). Unlike kill+start it preserves the container (same identity),
// modelling a process bounce / rolling restart rather than a crash.
func (cc *chaosCluster) restartContainer(t *testing.T, service string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	if out, err := cc.runCompose(ctx, "restart", service); err != nil {
		t.Fatalf("restart %s: %v\n%s", service, err, out)
	}
	t.Logf("CHAOS: restarted service %s", service)
}

// killTabletMySQL kills ONLY the mysqld inside a tablet container, leaving
// the vttablet process running — the "storage layer crashed under a live
// tablet" fault, distinct from killing the whole tablet container. It
// shells into the tablet container and SIGKILLs mysqld; vttablet then sees
// its backing MySQL vanish (the real-crash analog the SIGKILL simulation
// approximates).
//
// NOTE for the local session: the exact pkill availability inside
// vitess/lite must be confirmed; if `pkill` is absent, fall back to
// `mysqlctl --tablet-uid <uid> shutdown` or kill by matching the mysqld
// pid via /proc. This is flagged UNCERTAIN in the report.
func (cc *chaosCluster) killTabletMySQL(t *testing.T, service string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// `compose exec -T` (no TTY) runs the kill inside the running service.
	out, err := cc.runCompose(ctx, "exec", "-T", service, "sh", "-c",
		"pkill -9 mysqld || kill -9 $(pidof mysqld) || true")
	if err != nil {
		t.Fatalf("kill mysqld in %s: %v\n%s", service, err, out)
	}
	t.Logf("CHAOS: killed mysqld inside %s", service)
}

// vtctldclient runs a vtctldclient command INSIDE the vtctld container
// against the in-cluster vtctld grpc address (vtctld:15999 — the address
// the compose init one-shot uses). Returns combined output. This is the
// v24 invocation: `vtctldclient --server vtctld:15999 <Command> ...`.
//
// NOTE for the local session: confirm `--server vtctld:15999` resolves
// from inside the vtctld container itself (it does in the init one-shot,
// which targets the same address from a sibling container; from within
// vtctld, `localhost:15999` also works — the healthcheck uses that). If
// the in-container DNS name doesn't resolve, switch to `localhost:15999`.
func (cc *chaosCluster) vtctldclient(ctx context.Context, args ...string) ([]byte, error) {
	full := append([]string{
		"exec", "-T", svcVtctld,
		"vtctldclient", "--server", "localhost:15999",
	}, args...)
	return cc.runCompose(ctx, full...)
}

// plannedReparent performs a GRACEFUL PlannedReparentShard, promoting
// newPrimaryAlias (e.g. tabletAliasReplica) to PRIMARY. Used to model an
// operator-initiated, clean failover under a live sync. v24 form:
//
//	vtctldclient PlannedReparentShard <keyspace>/<shard> --new-primary <alias>
func (cc *chaosCluster) plannedReparent(t *testing.T, newPrimaryAlias string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	target := chaosKeyspace + "/" + chaosShard
	out, err := cc.vtctldclient(ctx, "PlannedReparentShard", target, "--new-primary", newPrimaryAlias)
	if err != nil {
		t.Fatalf("PlannedReparentShard %s --new-primary %s: %v\n%s", target, newPrimaryAlias, err, out)
	}
	t.Logf("CHAOS: PlannedReparentShard %s -> %s\n%s", target, newPrimaryAlias, out)
}

// emergencyReparent performs an EmergencyReparentShard — the hard
// failover used when the current primary is DEAD (we kill it first). It
// promotes the surviving replica with no cooperation from the old primary.
// v24 form:
//
//	vtctldclient EmergencyReparentShard <keyspace>/<shard> [--new-primary <alias>]
//
// We pass --new-primary explicitly to make the promotion deterministic in
// the two-tablet topology (otherwise ERS picks the most-advanced replica,
// which here is the only survivor anyway).
func (cc *chaosCluster) emergencyReparent(t *testing.T, newPrimaryAlias string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	target := chaosKeyspace + "/" + chaosShard
	out, err := cc.vtctldclient(ctx, "EmergencyReparentShard", target, "--new-primary", newPrimaryAlias)
	if err != nil {
		t.Fatalf("EmergencyReparentShard %s --new-primary %s: %v\n%s", target, newPrimaryAlias, err, out)
	}
	t.Logf("CHAOS: EmergencyReparentShard %s -> %s\n%s", target, newPrimaryAlias, out)
}

// waitForPrimaryAlias polls GetTablets until the named alias is the
// serving PRIMARY (or times out). The recovery gate after a reparent:
// the test must not resume asserting until vtgate routes to the new
// primary, or it races the healthcheck exactly as the bring-up does.
func (cc *chaosCluster) waitForPrimaryAlias(t *testing.T, wantPrimaryAlias string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastOut []byte
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		out, err := cc.vtctldclient(ctx, "GetTablets", "--keyspace", chaosKeyspace)
		cancel()
		lastOut = out
		if err == nil {
			// GetTablets prints one tablet per line; the primary line
			// contains both the alias and the "primary" type token.
			for _, line := range strings.Split(string(out), "\n") {
				if strings.Contains(line, wantPrimaryAlias) && strings.Contains(line, "primary") {
					t.Logf("CHAOS: %s is now the serving PRIMARY", wantPrimaryAlias)
					return
				}
			}
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timed out waiting for %s to become PRIMARY within %v; last GetTablets:\n%s",
		wantPrimaryAlias, timeout, lastOut)
}

// waitForWritablePrimaryHandle re-uses waitForWritablePrimary against this
// cluster's DSN — the post-recovery "vtgate routes to a healthy writable
// primary again" gate. Thin alias for readability in the scenarios.
func (cc *chaosCluster) waitForWritablePrimaryHandle(t *testing.T, timeout time.Duration) {
	t.Helper()
	if err := waitForWritablePrimary(t, cc.mysqlDSN, timeout); err != nil {
		t.Fatalf("cluster did not return to a writable PRIMARY after the fault: %v", err)
	}
}

// ----------------------------------------------------------------------
// Source-write + invariant helpers
// ----------------------------------------------------------------------

// chaosSeedTable creates the canonical chaos source table: a single
// BIGINT auto-increment PK + a payload column. Every scenario syncs this
// shape so the invariant helper can assert COUNT(*) and COUNT(DISTINCT
// id) uniformly.
func chaosSeedTable(t *testing.T, dsn, table string) {
	t.Helper()
	//nolint:gosec // table is a test-controlled literal, not user input.
	applyClusterSQL(t, dsn, fmt.Sprintf(`
		CREATE TABLE %s (
			id      BIGINT       NOT NULL AUTO_INCREMENT,
			payload VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, table))
}

// chaosInsertBatch inserts rows [start, start+n) into table as
// payload='chaos-<i>' in one multi-statement exec.
func chaosInsertBatch(t *testing.T, dsn, table string, start, n int) {
	t.Helper()
	var b strings.Builder
	for i := start; i < start+n; i++ {
		fmt.Fprintf(&b, "INSERT INTO %s (payload) VALUES ('chaos-%d');", table, i)
	}
	applyClusterSQL(t, dsn+"&multiStatements=true", b.String())
}

// continuousWriter fires single-row inserts into table at a steady cadence
// until ctx is cancelled, modelling the live write workload a real sync
// runs against. It returns a stop function that cancels and waits for the
// writer to drain, plus a pointer to the count of successfully-committed
// inserts (read AFTER stop). Writes that fail because the cluster is
// mid-fault (no healthy primary) are tolerated and retried on the next
// tick — they do NOT count toward the committed total, so the invariant
// uses the source COUNT(*) as ground truth rather than this counter.
//
// The writer opens its OWN *sql.DB so it survives vtgate restarts (the
// driver reconnects); it does not share the test's connections.
func continuousWriter(t *testing.T, dsn, table string, every time.Duration) (stop func() int) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("continuousWriter open: %v", err)
	}
	// Keep the pool small; vtgate reconnects are cheaper with few conns.
	db.SetMaxOpenConns(2)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)

	go func() {
		committed := 0
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		seq := 0
		for {
			select {
			case <-ctx.Done():
				_ = db.Close()
				done <- committed
				return
			case <-ticker.C:
				seq++
				ectx, ecancel := context.WithTimeout(ctx, 5*time.Second)
				//nolint:gosec // table is a test-controlled literal.
				_, err := db.ExecContext(ectx, fmt.Sprintf(
					"INSERT INTO %s (payload) VALUES ('live-%d')", table, seq,
				))
				ecancel()
				if err == nil {
					committed++
				}
				// On error (mid-fault: no healthy primary), just drop this
				// tick and try again next cadence — the source COUNT(*) is
				// the ground truth, not this best-effort counter.
			}
		}
	}()

	return func() int {
		cancel()
		select {
		case n := <-done:
			return n
		case <-time.After(30 * time.Second):
			t.Fatal("continuousWriter did not stop within 30s")
			return 0
		}
	}
}

// sourceRowStats returns COUNT(*) and COUNT(DISTINCT id) of the table
// through vtgate. The chaos invariant compares the sluice-applied target
// against COUNT(*) and requires COUNT(DISTINCT id)==COUNT(*) so a
// duplicate-applied row (a re-delivery the resume logic failed to
// dedup) is caught as loudly as a lost row.
//
// In these in-engine chaos tests the "target" sluice writes to is the
// SAME logical source (we drive a VStream off the cluster and re-apply
// nothing — the engine test exercises the READER's resilience, asserting
// the reader either delivers every change exactly once or fails loud).
// The helper is written to take an explicit target DSN+table so a future
// cross-engine chaos test (sluice writing to a separate target) reuses it
// unchanged.
func sourceRowStats(t *testing.T, dsn, table string) (count, distinctPK int) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("sourceRowStats open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	//nolint:gosec // table is a test-controlled literal.
	row := db.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT COUNT(*), COUNT(DISTINCT id) FROM %s", table,
	))
	if err := row.Scan(&count, &distinctPK); err != nil {
		t.Fatalf("sourceRowStats scan %s: %v", table, err)
	}
	return count, distinctPK
}

// assertZeroLossOrLoud is THE chaos invariant. It is given:
//
//   - the number of DISTINCT PKs the sluice reader delivered exactly once
//     (deliveredDistinct — the test counts these by tracking the id of
//     each Insert it drained, ignoring duplicates);
//   - the reader's terminal error (streamErr — non-nil iff sluice failed
//     loudly);
//   - the source ground-truth stats.
//
// It passes iff EITHER:
//
//	(loud)      streamErr != nil — sluice surfaced the fault loudly; OR
//	(zero-loss) deliveredDistinct == source COUNT(*) AND
//	            source COUNT(DISTINCT id) == source COUNT(*)
//	            (the reader delivered every row exactly once, no dup).
//
// It FAILS on the forbidden middle ground: streamErr == nil but
// deliveredDistinct < source COUNT(*) (SILENT PARTIAL) — the silent-loss
// class the tenet forbids.
func assertZeroLossOrLoud(t *testing.T, scenario string, drain *chaosDistinctDrain, streamErr error, srcCount, srcDistinct int) {
	t.Helper()
	deliveredDistinct := drain.count()

	if streamErr != nil {
		// Loud failure is an ACCEPTABLE chaos outcome — sluice did not
		// silently corrupt; it stopped and said so. Log it and pass.
		t.Logf("[%s] LOUD-FAILURE outcome (acceptable): reader surfaced err=%v "+
			"(delivered %d distinct, %d dup re-deliveries; source count=%d distinct=%d)",
			scenario, streamErr, deliveredDistinct, drain.dups, srcCount, srcDistinct)
		return
	}

	// No loud error ⇒ sluice must have delivered everything exactly once.
	if srcDistinct != srcCount {
		t.Fatalf("[%s] source itself has duplicate PKs (COUNT=%d, DISTINCT=%d) — test/seed bug, not a sluice signal",
			scenario, srcCount, srcDistinct)
	}
	if deliveredDistinct != srcCount {
		t.Fatalf("[%s] SILENT PARTIAL (the forbidden outcome): reader delivered %d distinct rows but source has %d, "+
			"and Err()==nil — sluice neither completed nor failed loudly (silent-loss tenet violation)",
			scenario, deliveredDistinct, srcCount)
	}
	t.Logf("[%s] ZERO-LOSS outcome: reader delivered all %d rows exactly once across the fault "+
		"(no loss; %d benign re-deliveries were deduped by the distinct set)",
		scenario, srcCount, drain.dups)
}

// chaosDistinctDrain accumulates the DISTINCT set of `id` PKs the reader
// has delivered on Insert events for a single table, across an arbitrary
// number of drain calls (cold-start row deliveries + post-fault CDC).
// Tracking distinctness across the whole scenario — not per-drain — is
// what lets assertZeroLossOrLoud catch a re-delivered duplicate (counted
// once in distinct, but flagged in dups) as well as a lost row.
type chaosDistinctDrain struct {
	table string
	seen  map[int64]struct{}
	dups  int
}

func newChaosDistinctDrain(table string) *chaosDistinctDrain {
	return &chaosDistinctDrain{table: table, seen: map[int64]struct{}{}}
}

// record folds one change into the distinct set if it is an Insert on the
// tracked table. Update/Delete/Tx boundary/Schema events are ignored (the
// chaos scenarios drive insert-only workloads so COUNT(*) is the clean
// ground truth). It returns true when the change was a counted insert.
func (d *chaosDistinctDrain) record(c ir.Change) bool {
	ins, ok := c.(ir.Insert)
	if !ok || ins.Table != d.table {
		return false
	}
	id, ok := chaosRowID(ins.Row)
	if !ok {
		return false
	}
	if _, dup := d.seen[id]; dup {
		d.dups++
		return false
	}
	d.seen[id] = struct{}{}
	return true
}

func (d *chaosDistinctDrain) count() int { return len(d.seen) }

// chaosRowID coerces ins.Row["id"] to int64. The VStream decoder may hand
// an int64, a smaller int kind, or a numeric []byte/string depending on
// the column width and driver path, so all numeric shapes are accepted.
func chaosRowID(row ir.Row) (int64, bool) {
	switch v := row["id"].(type) {
	case int64:
		return v, true
	case int32:
		return int64(v), true
	case int:
		return int64(v), true
	case uint64:
		return int64(v), true
	case []byte:
		return parseInt64(string(v))
	case string:
		return parseInt64(v)
	default:
		return 0, false
	}
}

func parseInt64(s string) (int64, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}
	var n int64
	if _, err := fmt.Sscan(s, &n); err != nil {
		return 0, false
	}
	return n, true
}

// drainUntil pumps the changes channel into the distinct drain until
// EITHER the drain has accumulated at least wantAtLeast distinct ids
// (returns reason "complete"), the channel closes (returns "closed" — the
// reader terminated; the caller inspects Err()), or the deadline fires
// (returns "deadline" — the caller decides whether that is acceptable
// given Err()). It never fails the test itself; outcome interpretation is
// the scenario's job via assertZeroLossOrLoud.
func drainUntil(
	changes <-chan ir.Change,
	drain *chaosDistinctDrain,
	wantAtLeast int,
	deadline time.Duration,
) (reason string) {
	timeout := time.After(deadline)
	for drain.count() < wantAtLeast {
		select {
		case c, ok := <-changes:
			if !ok {
				return "closed"
			}
			drain.record(c)
		case <-timeout:
			return "deadline"
		}
	}
	return "complete"
}
