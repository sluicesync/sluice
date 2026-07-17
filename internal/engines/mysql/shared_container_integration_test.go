//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Shared MySQL testcontainer for the engine's integration suite.
//
// Backlog #56 / CI Option B: prior to this file, every integration
// test booted its own mysql:8.0 container via testcontainers-go,
// paying the ~10–15 s cold-start cost 62 times in the engines-mysql
// shard (~12 minutes of pure container startup). TestMain below
// lazily boots ONE container the first time any helper is called and
// terminates it once the shard finishes — paying the startup cost
// once and letting each test reset its own database for isolation.
//
// Isolation model: every helper does
//   DROP DATABASE IF EXISTS <name>;
//   CREATE DATABASE <name> CHARACTER SET utf8mb4;
// against the shared container before returning the DSN. Tests
// continue to see "fresh state" semantics — empty schema, no
// leftover tables, no stale rows — exactly as if a new container
// had been booted. Per-database reset is sub-second on a warm
// container; the helper's interface (return (dsn, cleanup)) is
// unchanged so the 62 call sites needed zero modifications.
//
// One exception remains per-test-container: startMySQLGTIDForCDC.
// Its sole caller, TestCDCReader_GTIDPositionLoss_DetectedLoud,
// runs FLUSH BINARY LOGS / PURGE BINARY LOGS which mutate *global*
// binlog state on the shared mysqld. Sharing that container would
// truncate other CDC tests' binlog history mid-shard. The cost is
// one extra container boot per shard run — negligible against the
// 60+ boots reclaimed.
//
// The shared container's flag set is the union of every per-test
// helper's prior flags except GTID-mode (left OFF — the GTID test
// keeps its own container; all other CDC tests work fine without
// GTID-mode and with it OFF we don't risk enforce-gtid-consistency
// edge cases). server-id + log-bin + ROW format + FULL row-image
// match every CDC helper's prior request.

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	mysqltc "github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/wait"
)

// sharedMySQL holds the lazily-booted container and the connection
// primitives every per-test helper composes a DSN from.
//
// host/port come from the container at boot time; dbName is the
// container's seed db (we ignore it and create per-test dbs below).
// container is kept for TestMain's terminate.
//
// containerDeadOnce + containerDead together implement the
// mid-shard-death sentinel (task #71, mirror of task #64's PG
// counterpart): if the shared container becomes unreachable after a
// successful boot — observed on the self-hosted CI runner pool when
// the docker engine restarts mid-shard, or when the mysqld process
// inside the container dies while Docker still considers the
// container running, see PR #73 run
// https://sluicesync.dev/sluice/actions/runs/26533999631/job/78157843061
// where ~80 tests each emitted "[mysql] packets.go:58 unexpected EOF"
// in resetSharedDB — every subsequent reset would otherwise fail
// individually with the same noisy stack. The sentinel collapses
// that into one loud log line via the sync.Once and a fast t.Fatalf
// for the rest of the shard so the failure reason is unmistakable
// in CI logs rather than buried in 80x repeated stack traces.
//
// The liveness probe used is a fast TCP dial against the mapped SQL
// port, not Container.IsRunning(). The PG counterpart (PR #72)
// demonstrated that Docker can report the container as running while
// the SQL port refuses connections — so IsRunning() is too coarse a
// signal. The MySQL failure mode in PR #73's run is the same shape
// (driver-level "unexpected EOF" instead of "connection refused"
// because go-sql-driver/mysql translates the dead-socket signal
// differently, but the underlying liveness issue is identical).
type sharedMySQLState struct {
	once    sync.Once
	bootErr error

	containerDeadOnce sync.Once
	containerDead     bool

	host      string
	port      string
	user      string
	password  string
	container *mysqltc.MySQLContainer
}

var sharedMySQL sharedMySQLState

// sharedMySQLBootTimeout: per-attempt budget for the testcontainers
// MySQL boot. Bumped from 2min to 4min in task #69 after CI logs on
// the self-hosted runner pool showed every failed attempt hitting
// the 2-minute `wait until ready: "port: 3306 ... matched 0 times"`
// deadline, while successful attempts in the same runs took ~50s.
// Root cause was disk-I/O contention when multiple integration
// shards boot MySQL containers concurrently against the same
// `/var/lib/docker` — MySQL init writes 50-100MB during cold boot
// and slow attempts could stretch past 2min under load. 4min absorbs
// load spikes without unbounded budget growth.
//
// TODO(#68-follow-up): with the pre-baked image (sharedMySQLImage
// below) the init step is already on disk, so cold-start drops to
// ~5s. Once several CI cycles confirm the pre-baked image is reliable
// this budget can revert to 2min (and possibly 1min); the retry-with-
// backoff scaffolding stays for defense in depth.
const sharedMySQLBootTimeout = 4 * time.Minute

// sharedMySQLBootAttempts is the total number of attempts the retry
// loop in ensureSharedMySQL will make before giving up. Bumped from
// 3 to 5 by task #12 Phase B: the v0.83.0 → ~v0.84.x session
// captured two CI runs (PR #54 first-run + PR #61 first-run) where
// the original 3-attempt schedule exhausted under runner load — the
// boot was hitting `wait until ready` every time, not quick-failing
// for ~2 minutes per attempt. 5 attempts buys two more chances at
// 120s + 240s backoff. Cumulative worst-case wall time:
//
//	5 * sharedMySQLBootTimeout (per-attempt budget)
//	  + 30s + 60s + 120s + 240s
//	= 5 * 4min + 7.5min
//	= ~27.5 min
//
// (task #69 bumped per-attempt budget 2min → 4min; cumulative
// worst-case grew from 17.5 → 27.5 min, still under the 45m outer
// integration-job timeout — the shared TestMain is single-boot per
// shard, so the budget growth doesn't multiply by test count.)
// Still under the CI shard timeout, and the marginal cost
// vs. 3 attempts is paid only when the runner is sick (a few
// minutes once a release cycle, vs. a full CI rerun without it).
//
// Task #60 history (the original 3-attempt landing): prior to that
// loop the boot was single-shot — any wait-until-ready timeout
// (e.g. `port: 3306 ... matched 0 times, expected 1` followed by
// `context deadline exceeded`) failed all ~62 tests in the
// engines-mysql shard with "shared mysql unavailable", costing 3-5
// CI reruns per release tag.
const sharedMySQLBootAttempts = 5

// sharedMySQLImage is the testcontainers image reference used by the
// shared TestMain boot and by per-test boots in this package.
//
// Task #68: this is the pre-baked image
// (ghcr.io/sluicesync/sluice-mysql:8.0-prebaked) — built nightly from
// upstream mysql:8.0 by .github/workflows/build-prebaked-images.yml.
// The pre-baked image already has the heavy first-boot init step
// (mysqld --initialize-insecure writes 50-100MB of system tables)
// completed, so cold-start drops from 30-60s — up to 2-3 minutes
// under self-hosted runner disk-I/O contention — to ~5s.
//
// Byte-equivalent to upstream mysql:8.0 except that
// /var/lib/mysql is pre-populated; ENTRYPOINT, CMD, EXPOSE, and the
// MySQL version are identical to the base. The Cmd args
// (--log-bin, --binlog-format, --binlog-row-image, --server-id) in
// bootSharedMySQLOnce are still applied at boot time the same way
// they would be against the upstream image; the pre-bake just removes
// the init-on-first-boot step from the critical path.
//
// History — why this constant exists:
//   - Tasks #60, #63, #64, and #69 walked the boot timeout / retry
//     budget upward (single-shot → 3 retries → 4-min timeout →
//     WithWaitStrategyAndDeadline) to absorb the init-time disk-I/O
//     contention on the self-hosted runner pool. Each round added
//     headroom without eliminating the contention.
//   - Task #68 cuts the root cause by baking the init step into the
//     image. The retry-with-backoff scaffolding above stays as defense
//     in depth — if ghcr.io is unreachable or the bake is broken, the
//     retry layer absorbs the boot failure rather than failing all
//     ~62 tests in the shard.
//
// See docs/dev/ci-images.md for how the pre-baked images are built and
// when to bump the base version (e.g. MySQL 8.0 → 8.4).
const sharedMySQLImage = "ghcr.io/sluicesync/sluice-mysql:8.0-prebaked"

// sharedMySQLBootBackoff returns the sleep duration to apply between
// a failed boot attempt and the next one. attempt is 1-indexed and
// refers to the attempt that JUST failed; the function is only
// consulted while attempt < sharedMySQLBootAttempts.
//
// Schedule: 30s, 60s, 120s, 240s (doubling). Spelled out as a
// switch so the actual numbers are visible at the call site rather
// than computed from a `30s << (attempt-1)` formula.
func sharedMySQLBootBackoff(attempt int) time.Duration {
	switch attempt {
	case 1:
		return 30 * time.Second
	case 2:
		return 60 * time.Second
	case 3:
		return 120 * time.Second
	case 4:
		return 240 * time.Second
	default:
		// Defensive: never hit today because the loop exits after
		// attempt 5 without sleeping further.
		return 480 * time.Second
	}
}

// bootSharedMySQLOnce runs a single boot attempt against a fresh
// container. On any error it terminates the half-state container
// (idempotent / safe to call even on a never-started instance) so
// the next attempt starts from clean slate. Returns the boot
// primitives on success or (zero-values, error) on any failure of
// the underlying testcontainers boot, Host lookup, or MappedPort
// lookup.
//
// Separated from the retry loop so the loop body is a clean
// "attempt → backoff → attempt" rhythm and so a unit test can
// exercise the retry path without booting a real container.
func bootSharedMySQLOnce(ctx context.Context) (host, port string, container *mysqltc.MySQLContainer, err error) {
	const (
		rootUser = "root"
		rootPass = "rootpw"
		seedDB   = "sluice_shared_seed"
	)

	container, err = mysqltc.Run(
		ctx,
		sharedMySQLImage,
		mysqltc.WithDatabase(seedDB),
		mysqltc.WithUsername(rootUser),
		mysqltc.WithPassword(rootPass),
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
		// Wait-strategy override LAST so it definitively replaces module
		// defaults. testcontainers' `WithWaitStrategy` hard-wraps any
		// strategy with a 60s deadline at the OUTER wait.ForAll, which
		// overrides any inner WithStartupTimeout — use the explicit
		// `WithWaitStrategyAndDeadline` form so both inner and outer
		// timeouts match our 4-minute boot budget. Without this, the
		// outer 60s deadline fires under self-hosted runner disk-I/O
		// contention (task #69 / PR #69-follow-up: CI evidence showed
		// each failed attempt at ~100s — the 60s deadline, not the
		// 4-minute inner timeout).
		testcontainers.WithWaitStrategyAndDeadline(
			sharedMySQLBootTimeout,
			wait.ForLog("port: 3306  MySQL Community Server").
				WithStartupTimeout(sharedMySQLBootTimeout),
		),
	)
	if err != nil {
		if container != nil {
			_ = container.Terminate(context.Background())
		}
		return "", "", nil, fmt.Errorf("mysqltc.Run: %w", err)
	}

	hostV, err := container.Host(ctx)
	if err != nil {
		_ = container.Terminate(context.Background())
		return "", "", nil, fmt.Errorf("container.Host: %w", err)
	}
	portV, err := container.MappedPort(ctx, "3306/tcp")
	if err != nil {
		_ = container.Terminate(context.Background())
		return "", "", nil, fmt.Errorf("container.MappedPort: %w", err)
	}
	return hostV, portV.Port(), container, nil
}

// ensureSharedMySQL boots the shared container the first time it's
// called and is a no-op on subsequent calls. Returns the connection
// primitives needed to build a DSN for an arbitrary database.
//
// Boot is retried up to sharedMySQLBootAttempts times with
// sharedMySQLBootBackoff between attempts; the testcontainers
// `wait until ready` window can spuriously time out under CI load
// and the single-shot boot from task #56 made that flake fail all
// ~62 tests in the shard. Each attempt's failure is logged so the
// CI logs surface the retry pattern.
//
// Boot failures are sticky: bootErr is captured and returned by
// every subsequent call, so the FIRST test to touch the shared
// container fails noisily and the rest skip via Fatalf without
// each attempting a fresh boot.
func ensureSharedMySQL(t *testing.T) (host, port, user, password string) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	sharedMySQL.once.Do(func() {
		const (
			rootUser = "root"
			rootPass = "rootpw"
		)

		var lastErr error
		for attempt := 1; attempt <= sharedMySQLBootAttempts; attempt++ {
			ctx, cancel := context.WithTimeout(context.Background(), sharedMySQLBootTimeout)
			hostV, portV, container, err := bootSharedMySQLOnce(ctx)
			cancel()
			if err == nil {
				sharedMySQL.host = hostV
				sharedMySQL.port = portV
				sharedMySQL.user = rootUser
				sharedMySQL.password = rootPass
				sharedMySQL.container = container
				if attempt > 1 {
					log.Printf("shared mysql boot attempt %d/%d succeeded", attempt, sharedMySQLBootAttempts)
				}
				return
			}
			lastErr = err
			if attempt < sharedMySQLBootAttempts {
				backoff := sharedMySQLBootBackoff(attempt)
				log.Printf("shared mysql boot attempt %d/%d failed: %v; retrying in %s",
					attempt, sharedMySQLBootAttempts, err, backoff)
				time.Sleep(backoff)
				continue
			}
			log.Printf("shared mysql boot attempt %d/%d failed: %v; giving up",
				attempt, sharedMySQLBootAttempts, err)
		}
		sharedMySQL.bootErr = fmt.Errorf("shared mysql boot: %d attempts exhausted: %w",
			sharedMySQLBootAttempts, lastErr)
	})

	if sharedMySQL.bootErr != nil {
		t.Fatalf("shared mysql unavailable: %v", sharedMySQL.bootErr)
	}
	return sharedMySQL.host, sharedMySQL.port, sharedMySQL.user, sharedMySQL.password
}

// checkSharedContainerAlive is the mid-shard-death sentinel (task #71,
// mirror of the PG counterpart in task #64). Probes the container's
// mapped SQL port via a fast TCP-dial — the liveness signal that
// actually matters to the SQL work the caller is about to do — and
// short-circuits the test if the port is no longer reachable.
//
// **Why TCP-dial, not Container.IsRunning():** the PG counterpart's
// first cut used IsRunning(), which queries Docker's view of the
// container. Its own CI rerun
// (https://sluicesync.dev/sluice/actions/runs/26527039528/job/78138790049)
// reproduced the exact cascade the sentinel was supposed to catch
// and the loud DOCKER-ENGINE-DEAD log line was NOT emitted, proving
// IsRunning() returned true while the SQL port was unreachable. The
// failure mode in practice is "container alive by Docker's lights
// but mysqld process dead inside (or port mapping broken)", not
// "Docker engine restarted entirely". A TCP-dial against host:port
// catches both failure modes; IsRunning() catches only the latter.
// Dial cost is ~1ms locally vs ~30ms for a SQL ping, and we call
// this from every test's reset, so the dial is the right cheap
// signal. If the dial succeeds but SQL still fails inside reset(),
// the test fails loudly anyway — no silent loss.
//
// On the FIRST unreachable-container observation in the shard, the
// sentinel records the failure via containerDeadOnce + log.Printf so
// the CI log shows a single loud "docker engine dead mid-shard"
// message. Every subsequent test sees containerDead == true and
// t.Fatalf's quickly against that flag, skipping the SQL work that
// would otherwise produce 80x "[mysql] packets.go:58 unexpected EOF"
// stack traces.
//
// Original MySQL cascade: PR #73 run
// https://sluicesync.dev/sluice/actions/runs/26533999631/job/78157843061
// where ~80 engines-mysql tests all reported the driver-level EOF in
// resetSharedDB after the shared mysqld died mid-shard. The boot
// retry above (sharedMySQLBootAttempts) only protects the initial
// boot; this sentinel covers the post-boot lifetime where the same
// container instance is shared across the whole shard.
//
// Caller contract: invoke before any SQL work that touches the
// shared container. Returns true on alive (caller proceeds); calls
// t.Fatalf and does not return on dead.
func checkSharedContainerAlive(t *testing.T) bool {
	t.Helper()
	if sharedMySQL.container == nil {
		// Boot never happened (or failed and bootErr was set). Caller
		// should not have reached this path; defensive Fatalf so the
		// failure mode is loud rather than a nil-deref.
		t.Fatalf("shared mysql container is nil; ensureSharedMySQL was not called or failed")
		return false
	}
	if sharedMySQL.containerDead {
		t.Fatalf("shared mysql container unreachable mid-shard; skipping (see prior 'DOCKER-ENGINE-DEAD' message)")
		return false
	}
	addr := net.JoinHostPort(sharedMySQL.host, sharedMySQL.port)
	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err == nil {
		_ = conn.Close()
		return true
	}
	sharedMySQL.containerDeadOnce.Do(func() {
		sharedMySQL.containerDead = true
		log.Printf("DOCKER-ENGINE-DEAD: shared mysql container %s is unreachable mid-shard "+
			"(TCP dial to %s failed: %v); every remaining test in engines/mysql will fail "+
			"fast with this sentinel. Root cause is almost always the docker daemon restarting "+
			"under the self-hosted runner — or the mysqld process dying inside an otherwise "+
			"alive container — NOT a sluice bug. See task #71 / PR #73 for the original "+
			"cascade and task #64 / PR #72 for the matching PG implementation.",
			sharedMySQL.container.GetContainerID(), addr, err)
	})
	t.Fatalf("shared mysql container unreachable mid-shard (TCP dial to %s failed); see prior 'DOCKER-ENGINE-DEAD' log line", addr)
	return false
}

// sharedDSN builds a DSN pointed at dbName on the shared container.
// parseTime=true matches every per-test helper's prior connection
// string so test code that compares time.Time values behaves the
// same as before the refactor.
func sharedDSN(host, port, user, password, dbName string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		user, password, host, port, dbName)
}

// resetSharedDB drops + recreates dbName on the shared container so
// the caller gets the "fresh container" semantics each per-test
// helper used to deliver via a brand-new container. utf8mb4 matches
// the prior `WithDatabase(...)` default character set; tests that
// CREATE TABLE with `DEFAULT CHARSET=utf8mb4` continue to round-trip
// identically.
//
// Schemas reset via plain DDL execute via the root credentials' DSN
// pointed at the shared seed db (which is itself never used by any
// test — exists only so the mysql module's wait-for-readiness probe
// has a default schema). The probe's session has FK-checks etc.
// enabled by default; the DROP/CREATE is unconditional and atomic.
func resetSharedDB(t *testing.T, dbName string) {
	t.Helper()

	// Task #71 sentinel: probe container liveness BEFORE opening the
	// SQL connection. If the shared container died mid-shard (docker
	// engine restart, OOM kill, etc.), this short-circuits with a
	// single loud log line instead of letting each of the ~80 tests
	// in the shard waste time hitting "[mysql] packets.go:58
	// unexpected EOF".
	checkSharedContainerAlive(t)

	host, port, user, password := sharedPrimitives()

	const sharedSeedDB = "sluice_shared_seed"
	rootDSN := sharedDSN(host, port, user, password, sharedSeedDB)
	rootDSN += "&multiStatements=true"

	db, err := sql.Open("mysql", rootDSN)
	if err != nil {
		t.Fatalf("reset %q: open: %v", dbName, err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ddl := fmt.Sprintf(
		"DROP DATABASE IF EXISTS `%s`; CREATE DATABASE `%s` CHARACTER SET utf8mb4;",
		dbName, dbName,
	)
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		t.Fatalf("reset %q: %v", dbName, err)
	}
}

// sharedPrimitives returns the captured host/port/user/password
// without re-running the sync.Once. Callers must have already called
// ensureSharedMySQL — resetSharedDB does, indirectly, through its
// caller chain. (Helper exists so the test-side helpers below can
// build many DSNs without each carrying four return values around.)
func sharedPrimitives() (host, port, user, password string) {
	return sharedMySQL.host, sharedMySQL.port, sharedMySQL.user, sharedMySQL.password
}

// newSharedDB resets a database on the shared container and returns
// a DSN pointed at it. The returned cleanup is a no-op (container
// teardown is owned by TestMain); kept in the signature so the 62
// call sites that wrote `dsn, cleanup := startMySQL...(t); defer
// cleanup()` continue to compile without modification.
func newSharedDB(t *testing.T, dbName string) (dsn string, cleanup func()) {
	t.Helper()
	host, port, user, password := ensureSharedMySQL(t)
	resetSharedDB(t, dbName)
	return sharedDSN(host, port, user, password, dbName), func() {}
}

// TestMain is the lifecycle hook: run the tests, then unconditionally
// terminate the shared container (if it was booted). os.Exit is used
// because TestMain must return the test exit code; the deferred-
// terminate pattern doesn't run under os.Exit, so we Terminate
// explicitly first.
//
// Build-tagged under //go:build integration so it does not affect
// the unit suite. The unit tests in this package compile without
// integration; their `go test` invocation never picks this file up.
//
// Note: the context for Terminate is NOT deferred-cancel — gocritic's
// exitAfterDefer rule notes that defers won't run under os.Exit, and
// the WithTimeout's cancel is a leak-prevention call only relevant if
// the function returned normally. Calling cancel before os.Exit is
// equivalent for the program-exit path; ignoring it (since the
// process is about to terminate) avoids the lint.
func TestMain(m *testing.M) {
	code := m.Run()

	// Task #71: if the mid-shard-death sentinel fired during the run,
	// surface that fact at the very end of TestMain so the operator
	// reading CI logs from the bottom up sees "docker engine died" as
	// the LAST log line — not buried among the cascading test
	// failures. Once-only log is also emitted by
	// checkSharedContainerAlive at the moment of detection; this is
	// the trailing summary so the cause is visible at both ends of
	// the failure region.
	if sharedMySQL.containerDead {
		log.Printf("DOCKER-ENGINE-DEAD: shared mysql container became unreachable mid-shard during this run; " +
			"all engines/mysql failures above with 'unreachable mid-shard' are downstream of this. " +
			"This is a runner-infrastructure issue (docker daemon restart or mysqld process death), " +
			"NOT a sluice code bug.")
	}

	if sharedMySQL.container != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_ = sharedMySQL.container.Terminate(ctx)
		cancel()
	}

	// Item 73: the lazily-booted shared MariaDB containers (11.4 /
	// 10.11) follow the same single-boot-per-shard lifecycle.
	terminateSharedMariaDBs()

	os.Exit(code)
}
