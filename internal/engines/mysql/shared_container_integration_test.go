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
	"os"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	mysqltc "github.com/testcontainers/testcontainers-go/modules/mysql"
)

// sharedMySQL holds the lazily-booted container and the connection
// primitives every per-test helper composes a DSN from.
//
// host/port come from the container at boot time; dbName is the
// container's seed db (we ignore it and create per-test dbs below).
// container is kept for TestMain's terminate.
type sharedMySQLState struct {
	once      sync.Once
	bootErr   error
	host      string
	port      string
	user      string
	password  string
	container *mysqltc.MySQLContainer
}

var sharedMySQL sharedMySQLState

// sharedMySQLBootTimeout is generous: cold-start of mysql:8.0 with
// binlog enabled occasionally takes ~30 s on CI, plus image-pull on
// the first run of a fresh runner can add another minute. The same
// 2-minute budget the per-test helpers used pre-refactor.
const sharedMySQLBootTimeout = 2 * time.Minute

// ensureSharedMySQL boots the shared container the first time it's
// called and is a no-op on subsequent calls. Returns the connection
// primitives needed to build a DSN for an arbitrary database.
//
// Boot failures are sticky: bootErr is captured and returned by
// every subsequent call, so the FIRST test to touch the shared
// container fails noisily and the rest skip via Fatalf without
// each attempting a fresh boot.
func ensureSharedMySQL(t *testing.T) (host, port, user, password string) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	sharedMySQL.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), sharedMySQLBootTimeout)
		defer cancel()

		const (
			rootUser = "root"
			rootPass = "rootpw"
			seedDB   = "sluice_shared_seed"
		)

		container, err := mysqltc.Run(
			ctx,
			"mysql:8.0",
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
		)
		if err != nil {
			sharedMySQL.bootErr = fmt.Errorf("shared mysql boot: %w", err)
			return
		}

		hostV, err := container.Host(ctx)
		if err != nil {
			sharedMySQL.bootErr = fmt.Errorf("shared mysql host: %w", err)
			_ = container.Terminate(context.Background())
			return
		}
		portV, err := container.MappedPort(ctx, "3306/tcp")
		if err != nil {
			sharedMySQL.bootErr = fmt.Errorf("shared mysql port: %w", err)
			_ = container.Terminate(context.Background())
			return
		}

		sharedMySQL.host = hostV
		sharedMySQL.port = portV.Port()
		sharedMySQL.user = rootUser
		sharedMySQL.password = rootPass
		sharedMySQL.container = container
	})

	if sharedMySQL.bootErr != nil {
		t.Fatalf("shared mysql unavailable: %v", sharedMySQL.bootErr)
	}
	return sharedMySQL.host, sharedMySQL.port, sharedMySQL.user, sharedMySQL.password
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

	if sharedMySQL.container != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_ = sharedMySQL.container.Terminate(ctx)
		cancel()
	}

	os.Exit(code)
}
