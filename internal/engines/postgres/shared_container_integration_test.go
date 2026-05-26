//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Shared Postgres testcontainer for the engine's integration suite.
//
// Backlog #59 / CI Option B (PG side, mirror of task #56's MySQL
// counterpart): prior to this file every integration test booted its
// own postgres:16 container via testcontainers-go, paying the
// ~10–15 s cold-start cost on every test in the engines-postgres
// shard. TestMain below lazily boots ONE container the first time
// any helper is called and terminates it once the shard finishes —
// paying the startup cost once and letting each test reset its own
// database for isolation.
//
// Isolation model (per-database, mirroring MySQL #56): every helper
// does
//
//	terminate backends on dbName;
//	DROP DATABASE IF EXISTS <name>;
//	DROP all non-default replication slots (cluster-global on PG);
//	DROP all non-default roles (cluster-global on PG);
//	CREATE DATABASE <name>;
//
// against the shared container before returning the DSN. Tests
// continue to see "fresh state" semantics — empty schema, no
// leftover slots, no leftover roles, no stale rows — exactly as if
// a new container had been booted. Per-database reset is sub-second
// on a warm container; the helper's interface (returning
// (dsn, cleanup)) is unchanged so existing call sites needed no
// modification.
//
// **Per-database vs per-schema:** chose per-database for v1 to
// mirror MySQL's isolation shape closely. Per-schema would be faster
// on PG (CREATE DATABASE is heavier than CREATE SCHEMA) but couples
// test data through cross-schema visibility unless every test
// remembers to scope its DDL to its own schema — a footgun this
// refactor is too mechanical to introduce. Follow-up could switch
// to per-schema after auditing call sites.
//
// **Replication slots are cluster-global, not per-database** on PG,
// so a slot left behind by one test would be visible to the next
// even after DROP DATABASE. The reset path explicitly enumerates
// pg_replication_slots and drops everything it finds. This is the
// PG-specific bite that MySQL #56 didn't have to deal with.
//
// **Roles are cluster-global too.** Several tests CREATE ROLE
// sluice_app / sluice_f7_user / noddl by hardcoded name; on a
// shared container the second test to run that DDL would fail with
// "role already exists". The reset path drops all non-default roles
// alongside the slot cleanup.
//
// Exceptions remain per-test-container — see the comment block at
// the foot of this file for the complete list and the reason each
// one needs its own server (conflicting GUCs, different PG version
// image, server-wide settings tested at boot time, etc.).
//
// The shared container's Cmd flag set is the union of every per-test
// CDC helper's prior flags: wal_level=logical, max_wal_senders=4,
// max_replication_slots=4. Non-CDC tests are unaffected by these
// (wal_level=logical is a superset of wal_level=replica's
// functionality; it doesn't disable any feature the non-CDC tests
// rely on). The one helper that conflicts on this point —
// startPostgresForCDCWithSmallDecodeMem, which sets
// logical_decoding_work_mem=64kB — keeps its own container.

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// sharedPostgres holds the lazily-booted container and the connection
// primitives every per-test helper composes a DSN from.
//
// host/port come from the container at boot time; user/password are
// the credentials the postgres testcontainer module seeds; container
// is kept for TestMain's terminate.
type sharedPostgresState struct {
	once      sync.Once
	bootErr   error
	host      string
	port      string
	user      string
	password  string
	container *pgtc.PostgresContainer
}

var sharedPostgres sharedPostgresState

// sharedPostgresBootTimeout is generous: cold-start of postgres:16
// with wal_level=logical occasionally takes ~30 s on CI, plus
// image-pull on the first run of a fresh runner can add another
// minute. Matches the 2-minute budget the per-test helpers used
// pre-refactor — applied per attempt of the retry loop (see
// sharedPostgresBootAttempts).
const sharedPostgresBootTimeout = 2 * time.Minute

// sharedPostgresBootAttempts is the total number of attempts the
// retry loop in ensureSharedPostgres will make before giving up.
// 5 attempts mirrors engines/mysql.sharedMySQLBootAttempts after
// task #12 raised it from 3 — single-boot site, the worst-case
// wall-time is bounded:
//
//	5 * sharedPostgresBootTimeout (per-attempt budget)
//	  + 30s + 60s + 120s + 240s
//	= 5 * 2min + 7.5min
//	= ~17.5 min
//
// still under the CI 30-minute shard timeout. Per-test PG container
// boots (the escape-hatch helpers below) use 3 attempts instead —
// the multiplier (one boot per test) would push cumulative
// wall-time over the 75-minute go-test-binary timeout on a sick
// runner. See user-memory `ci-retry-asymmetry.md` for the v0.84.0
// rationale.
const sharedPostgresBootAttempts = 5

// sharedPostgresBootBackoff returns the sleep duration to apply
// between a failed boot attempt and the next one. attempt is
// 1-indexed and refers to the attempt that JUST failed; the function
// is only consulted while attempt < sharedPostgresBootAttempts.
//
// Schedule: 30s, 60s, 120s, 240s (doubling). Spelled out as a switch
// so the actual numbers are visible at the call site rather than
// computed from a `30s << (attempt-1)` formula.
func sharedPostgresBootBackoff(attempt int) time.Duration {
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

const (
	sharedPGUser    = "test"
	sharedPGPass    = "test"
	sharedPGSeedDB  = "sluice_shared_seed"
	sharedPGImage   = "postgres:16"
	sharedPGAdminDB = "postgres"
)

// bootSharedPostgresOnce runs a single boot attempt against a fresh
// container. On any error it terminates the half-state container
// (idempotent / safe to call even on a never-started instance) so
// the next attempt starts from a clean slate. Returns the boot
// primitives on success or (zero-values, error) on any failure of
// the underlying testcontainers boot, Host lookup, or MappedPort
// lookup.
//
// Separated from the retry loop so the loop body is a clean
// "attempt → backoff → attempt" rhythm.
func bootSharedPostgresOnce(ctx context.Context) (host, port string, container *pgtc.PostgresContainer, err error) {
	container, err = pgtc.Run(
		ctx,
		sharedPGImage,
		pgtc.WithDatabase(sharedPGSeedDB),
		pgtc.WithUsername(sharedPGUser),
		pgtc.WithPassword(sharedPGPass),
		pgtc.BasicWaitStrategies(),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				// Union of every per-test CDC helper's GUC overrides.
				// wal_level=logical is a superset of replica for the
				// non-CDC tests' purposes. max_wal_senders /
				// max_replication_slots at 4 matches the highest
				// existing helper.
				Cmd: []string{
					"-c", "wal_level=logical",
					"-c", "max_wal_senders=4",
					"-c", "max_replication_slots=4",
				},
			},
		}),
	)
	if err != nil {
		if container != nil {
			_ = container.Terminate(context.Background())
		}
		return "", "", nil, fmt.Errorf("pgtc.Run: %w", err)
	}

	hostV, err := container.Host(ctx)
	if err != nil {
		_ = container.Terminate(context.Background())
		return "", "", nil, fmt.Errorf("container.Host: %w", err)
	}
	portV, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		_ = container.Terminate(context.Background())
		return "", "", nil, fmt.Errorf("container.MappedPort: %w", err)
	}
	return hostV, portV.Port(), container, nil
}

// ensureSharedPostgres boots the shared container the first time
// it's called and is a no-op on subsequent calls. Returns the
// connection primitives needed to build a DSN for an arbitrary
// database.
//
// Boot is retried up to sharedPostgresBootAttempts times with
// sharedPostgresBootBackoff between attempts; the testcontainers
// `wait until ready` window can spuriously time out under CI load
// and a single-shot boot would fail all tests in the shard.
//
// Boot failures are sticky: bootErr is captured and returned by
// every subsequent call, so the FIRST test to touch the shared
// container fails noisily and the rest skip via Fatalf without
// each attempting a fresh boot.
func ensureSharedPostgres(t *testing.T) (host, port, user, password string) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	sharedPostgres.once.Do(func() {
		var lastErr error
		for attempt := 1; attempt <= sharedPostgresBootAttempts; attempt++ {
			ctx, cancel := context.WithTimeout(context.Background(), sharedPostgresBootTimeout)
			hostV, portV, container, err := bootSharedPostgresOnce(ctx)
			cancel()
			if err == nil {
				sharedPostgres.host = hostV
				sharedPostgres.port = portV
				sharedPostgres.user = sharedPGUser
				sharedPostgres.password = sharedPGPass
				sharedPostgres.container = container
				if attempt > 1 {
					log.Printf("shared postgres boot attempt %d/%d succeeded",
						attempt, sharedPostgresBootAttempts)
				}
				return
			}
			lastErr = err
			if attempt < sharedPostgresBootAttempts {
				backoff := sharedPostgresBootBackoff(attempt)
				log.Printf("shared postgres boot attempt %d/%d failed: %v; retrying in %s",
					attempt, sharedPostgresBootAttempts, err, backoff)
				time.Sleep(backoff)
				continue
			}
			log.Printf("shared postgres boot attempt %d/%d failed: %v; giving up",
				attempt, sharedPostgresBootAttempts, err)
		}
		sharedPostgres.bootErr = fmt.Errorf("shared postgres boot: %d attempts exhausted: %w",
			sharedPostgresBootAttempts, lastErr)
	})

	if sharedPostgres.bootErr != nil {
		t.Fatalf("shared postgres unavailable: %v", sharedPostgres.bootErr)
	}
	return sharedPostgres.host, sharedPostgres.port, sharedPostgres.user, sharedPostgres.password
}

// sharedPGDSN builds a DSN pointed at dbName on the shared container.
// sslmode=disable matches every per-test helper's prior connection
// string so test code that relies on plain-TCP behaves the same as
// before the refactor.
func sharedPGDSN(host, port, user, password, dbName string) string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		user, password, host, port, dbName)
}

// sharedPrimitives returns the captured host/port/user/password
// without re-running the sync.Once. Callers must have already called
// ensureSharedPostgres — resetSharedDB does so via its callers.
func sharedPrimitives() (host, port, user, password string) {
	return sharedPostgres.host, sharedPostgres.port, sharedPostgres.user, sharedPostgres.password
}

// dropAllReplicationSlots removes every entry in pg_replication_slots.
// Replication slots are cluster-global on PG (not per-database), so a
// slot created by one test is visible to the next even after DROP
// DATABASE. The pg_terminate_backend call clears any active consumer
// so the subsequent pg_drop_replication_slot doesn't fail with
// "slot is active". Errors on individual slots are logged but not
// fatal — the goal is best-effort cleanup, and a failure here is
// almost always "slot was already dropped between SELECT and DROP"
// from concurrent test cleanup.
func dropAllReplicationSlots(ctx context.Context, db *sql.DB) error {
	type slot struct {
		name      string
		active    bool
		activePID int
	}
	slots, err := func() ([]slot, error) {
		rows, err := db.QueryContext(ctx,
			`SELECT slot_name, active, COALESCE(active_pid, 0) FROM pg_replication_slots`)
		if err != nil {
			return nil, fmt.Errorf("list slots: %w", err)
		}
		defer func() { _ = rows.Close() }()

		var out []slot
		for rows.Next() {
			var s slot
			if err := rows.Scan(&s.name, &s.active, &s.activePID); err != nil {
				return nil, fmt.Errorf("scan slot: %w", err)
			}
			out = append(out, s)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iter slots: %w", err)
		}
		return out, nil
	}()
	if err != nil {
		return err
	}

	for _, s := range slots {
		if s.active && s.activePID != 0 {
			// Terminate the walsender backend so the slot becomes
			// droppable. Best-effort; pg_drop_replication_slot below
			// will fail loudly if the slot is still active.
			_, _ = db.ExecContext(ctx, `SELECT pg_terminate_backend($1)`, s.activePID)
			// Brief poll for the slot to go inactive. PG marks the
			// slot inactive when the walsender backend observes the
			// signal; in practice this is sub-second.
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				var stillActive bool
				if err := db.QueryRowContext(
					ctx,
					`SELECT active FROM pg_replication_slots WHERE slot_name = $1`,
					s.name,
				).Scan(&stillActive); err != nil {
					// Slot already gone — fine.
					stillActive = false
				}
				if !stillActive {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
		if _, err := db.ExecContext(
			ctx,
			`SELECT pg_drop_replication_slot($1) FROM pg_replication_slots WHERE slot_name = $1`,
			s.name,
		); err != nil {
			log.Printf("dropAllReplicationSlots: drop %q: %v (continuing)", s.name, err)
		}
	}
	return nil
}

// dropAllNonDefaultRoles drops every role in pg_roles that wasn't
// installed by the postgres image or created at container boot
// (postgres, the seed `test` superuser, and pg_*/rdsadmin built-ins).
// Roles are cluster-global on PG; without this step the second test
// to run `CREATE ROLE sluice_app` would fail with "role already
// exists". Errors on individual roles are logged but not fatal.
//
// The skip list is conservative: anything not in it gets dropped.
// PG's built-in pg_* roles are skipped by name prefix; rolname='test'
// is the testcontainer seed and must stay; 'postgres' is the bootstrap
// superuser. Any test that creates a role with one of those names is
// in violation of the test convention and the conflict will surface
// loudly.
func dropAllNonDefaultRoles(ctx context.Context, host, port, user, password string) error {
	adminDSN := sharedPGDSN(host, port, user, password, sharedPGAdminDB)
	db, err := sql.Open("pgx", adminDSN)
	if err != nil {
		return fmt.Errorf("open admin db: %w", err)
	}
	defer func() { _ = db.Close() }()

	names, err := func() ([]string, error) {
		rows, err := db.QueryContext(ctx,
			`SELECT rolname FROM pg_roles
			 WHERE rolname NOT IN ('postgres', `+quoteSQLString(sharedPGUser)+`)
			   AND rolname NOT LIKE 'pg\_%' ESCAPE '\'
			   AND rolname NOT LIKE 'rdsadmin%'`)
		if err != nil {
			return nil, fmt.Errorf("list roles: %w", err)
		}
		defer func() { _ = rows.Close() }()

		var out []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return nil, fmt.Errorf("scan role: %w", err)
			}
			out = append(out, name)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iter roles: %w", err)
		}
		return out, nil
	}()
	if err != nil {
		return err
	}

	// Cross-database REASSIGN + DROP OWNED so DROP ROLE can succeed.
	// REASSIGN/DROP OWNED affect only the current database; the
	// per-test databases (target_db / source_db / sluice_test / etc.)
	// may still contain role-owned objects.
	reassignAndDropOwnedAcrossDatabases(ctx, host, port, user, password, names)

	for _, name := range names {
		// Per-cluster sweep — picks up role-default-privilege grants
		// that REASSIGN can't move (they live cluster-wide, not in a
		// specific db).
		_, _ = db.ExecContext(ctx, `DROP OWNED BY `+quoteIdent(name)+` CASCADE`)
		if _, err := db.ExecContext(ctx, `DROP ROLE IF EXISTS `+quoteIdent(name)); err != nil {
			log.Printf("dropAllNonDefaultRoles: drop %q: %v (continuing)", name, err)
		}
	}
	return nil
}

// terminateBackendsOnDB kills any backend currently connected to
// dbName. Required before DROP DATABASE: PG will refuse to drop a
// database that still has connections, even if those connections are
// the idle pgx pools from the previous test. Best-effort — failures
// are logged but not fatal.
func terminateBackendsOnDB(ctx context.Context, db *sql.DB, dbName string) {
	_, err := db.ExecContext(
		ctx,
		`SELECT pg_terminate_backend(pid)
		   FROM pg_stat_activity
		  WHERE datname = $1 AND pid <> pg_backend_pid()`,
		dbName,
	)
	if err != nil {
		log.Printf("terminateBackendsOnDB: %v (continuing)", err)
	}
}

// listNonDefaultDatabases returns the names of all databases in the
// cluster except the bootstrap ones (postgres, template0/1) and the
// testcontainer's seed db (sharedPGSeedDB). These are the databases
// where leftover test-created roles might own objects, so we connect
// to each and run REASSIGN OWNED / DROP OWNED before attempting
// DROP ROLE.
func listNonDefaultDatabases(ctx context.Context, db *sql.DB) ([]string, error) {
	q := `SELECT datname FROM pg_database
		 WHERE datistemplate = false
		   AND datname NOT IN ('postgres', ` + quoteSQLString(sharedPGSeedDB) + `)`
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("list databases: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan database: %w", err)
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iter databases: %w", err)
	}
	return out, nil
}

// reassignAndDropOwnedAcrossDatabases connects to each non-default
// test database in the cluster, runs REASSIGN OWNED BY each role to
// the bootstrap test superuser, then runs DROP OWNED BY (to clean up
// any role-default-privilege grants that REASSIGN doesn't touch).
//
// This is the cross-database half of role cleanup: REASSIGN OWNED
// only affects the database the session is connected to, so we must
// repeat per-database.
//
// Errors are logged but not fatal — best-effort, mirroring the
// philosophy of the other cleanup helpers.
func reassignAndDropOwnedAcrossDatabases(ctx context.Context, host, port, user, password string, roleNames []string) {
	if len(roleNames) == 0 {
		return
	}
	// Open the admin connection only to enumerate the databases; we
	// then dial each one individually.
	adminDSN := sharedPGDSN(host, port, user, password, sharedPGAdminDB)
	adminDB, err := sql.Open("pgx", adminDSN)
	if err != nil {
		log.Printf("reassignAndDropOwnedAcrossDatabases: open admin: %v (continuing)", err)
		return
	}
	defer func() { _ = adminDB.Close() }()

	dbs, err := listNonDefaultDatabases(ctx, adminDB)
	if err != nil {
		log.Printf("reassignAndDropOwnedAcrossDatabases: list dbs: %v (continuing)", err)
		return
	}

	for _, dbName := range dbs {
		dbDSN := sharedPGDSN(host, port, user, password, dbName)
		dbConn, err := sql.Open("pgx", dbDSN)
		if err != nil {
			log.Printf("reassignAndDropOwnedAcrossDatabases: open %q: %v (continuing)", dbName, err)
			continue
		}
		for _, role := range roleNames {
			// REASSIGN moves ownership of objects in this db to the
			// bootstrap user; DROP OWNED then sweeps default-privilege
			// grants. Both run in the current database only.
			_, _ = dbConn.ExecContext(ctx,
				`REASSIGN OWNED BY `+quoteIdent(role)+` TO `+quoteIdent(sharedPGUser))
			_, _ = dbConn.ExecContext(ctx,
				`DROP OWNED BY `+quoteIdent(role)+` CASCADE`)
		}
		_ = dbConn.Close()
	}
}

// resetSharedDB drops + recreates dbName on the shared container so
// the caller gets the "fresh container" semantics each per-test
// helper used to deliver via a brand-new container. The connection
// for the reset uses the admin database (the bootstrap `postgres`
// db, which cannot itself be dropped), not the seed `test` user's
// database — DROP DATABASE cannot be issued from a session attached
// to the database being dropped.
//
// Steps, in order:
//
//  1. Drop all replication slots (cluster-global cleanup). Slots
//     reference a database; dropping them before the target db
//     avoids orphan slots whose database column points at the
//     dropped db.
//  2. Drop all non-default roles (cluster-global cleanup). Uses
//     REASSIGN OWNED across every non-default database to release
//     ownership before DROP ROLE — necessary because a role may own
//     objects in a sibling test db (e.g. sluice_f7_user owning
//     tables in target_db while the current test uses source_db).
//  3. Drop dbName (only the target database — sibling per-test dbs
//     are left intact so two helpers in the same test that ask for
//     different db names don't destroy each other's setup).
//  4. Create dbName.
//
// Why only dbName in step 3: two helpers in the same test commonly
// produce two DSNs against two databases (e.g. cutover tests
// requesting src and tgt). If the second helper's reset dropped the
// first helper's database, the first DSN would dangle for the rest
// of the test. The role-cleanup in step 2 handles cross-db ownership
// before this point so DROP ROLE doesn't need the sibling db to
// vanish.
func resetSharedDB(t *testing.T, dbName string) {
	t.Helper()
	host, port, user, password := sharedPrimitives()

	adminDSN := sharedPGDSN(host, port, user, password, sharedPGAdminDB)

	db, err := sql.Open("pgx", adminDSN)
	if err != nil {
		t.Fatalf("reset %q: open admin db: %v", dbName, err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Step 1: drop replication slots (cluster-global).
	if err := dropAllReplicationSlots(ctx, db); err != nil {
		t.Fatalf("reset %q: drop slots: %v", dbName, err)
	}

	// Step 2: drop non-default roles (cluster-global). Cross-db
	// REASSIGN OWNED + per-cluster DROP OWNED clear the way for
	// DROP ROLE.
	if err := dropAllNonDefaultRoles(ctx, host, port, user, password); err != nil {
		t.Fatalf("reset %q: drop roles: %v", dbName, err)
	}

	// Step 3: drop only the target db. Backends on dbName must be
	// terminated first or DROP DATABASE fails with "database in use".
	terminateBackendsOnDB(ctx, db, dbName)
	if _, err := db.ExecContext(
		ctx,
		`DROP DATABASE IF EXISTS `+quoteIdent(dbName)+` WITH (FORCE)`,
	); err != nil {
		t.Fatalf("reset %q: drop db: %v", dbName, err)
	}

	// Step 4: create the fresh target database.
	if _, err := db.ExecContext(
		ctx,
		`CREATE DATABASE `+quoteIdent(dbName),
	); err != nil {
		t.Fatalf("reset %q: create db: %v", dbName, err)
	}
}

// newSharedPGDB resets a database on the shared container and returns
// a DSN pointed at it. The returned cleanup is a no-op (container
// teardown is owned by TestMain); kept in the signature so the
// existing call sites that wrote `dsn, cleanup := startPostgres...(t);
// defer cleanup()` continue to compile without modification.
func newSharedPGDB(t *testing.T, dbName string) (dsn string, cleanup func()) {
	t.Helper()
	host, port, user, password := ensureSharedPostgres(t)
	resetSharedDB(t, dbName)
	return sharedPGDSN(host, port, user, password, dbName), func() {}
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
// equivalent for the program-exit path.
func TestMain(m *testing.M) {
	code := m.Run()

	if sharedPostgres.container != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_ = sharedPostgres.container.Terminate(ctx)
		cancel()
	}

	os.Exit(code)
}

// --- Per-test escape hatches (NOT migrated to shared container) ---
//
// The following helpers retain their per-test container boot because
// the test's setup mutates global server state in a way that would
// corrupt other tests on the shared container:
//
//   - startPostgresForCDCWithSmallDecodeMem
//       Sets logical_decoding_work_mem=64kB at server-boot time. That
//       GUC fights with the shared container's default and would
//       affect every other CDC test's spill semantics. Lives in
//       cdc_reader_streaming_protocol_integration_test.go.
//
//   - startPostgres17ForCDC (and startPostgresForCDCImage with
//     "postgres:17")
//       Different container image (postgres:17) so semantically a
//       different server — sharing would defeat the purpose of the
//       PG 17–specific tests. Lives in cdc_reader_integration_test.go.
//
//   - TestCDCReader_RejectsWrongWALLevel (inlined boot in the test)
//       Asserts the reader's wal_level precondition fires when the
//       server has wal_level=replica (the PG default). The shared
//       container is wal_level=logical, so it can't test the
//       refusal path. Inlined boot lives in
//       cdc_reader_integration_test.go.
//
// Each of these per-test boots applies the 3-attempt retry pattern
// (mirroring pipeline/mysql_boot_retry_integration_test.go), NOT the
// 5-attempt pattern of the shared container. Rationale: per-test
// retries multiply by call-site frequency. See user-memory
// `ci-retry-asymmetry.md` for the v0.84.0 lesson that landed this
// asymmetry.

// pgPerTestBootAttempts is the per-test container boot retry cap.
// Lower than sharedPostgresBootAttempts (5) because per-test boots
// multiply by the test count in the shard; budgeting 5 attempts at
// each per-test site would push cumulative wall-time over the CI
// timeout on a sick runner. 3 attempts with 30s/60s backoff is the
// asymmetric ceiling task #12 / PR #62 landed for MySQL — applied
// here for parity.
const pgPerTestBootAttempts = 3

// pgPerTestBootTimeout is the per-attempt budget for per-test PG
// container boots. Matches sharedPostgresBootTimeout (2m).
const pgPerTestBootTimeout = 2 * time.Minute

// pgPerTestBootBackoff returns the sleep between failed boot attempts
// at a per-test site. Schedule: 30s, 60s. The default branch (120s)
// is defensive — never reached at 3 attempts but kept for the case
// where a future raise reuses this function.
func pgPerTestBootBackoff(attempt int) time.Duration {
	switch attempt {
	case 1:
		return 30 * time.Second
	case 2:
		return 60 * time.Second
	default:
		return 120 * time.Second
	}
}

// runPGWithRetry boots a postgres testcontainer with the given image
// + customisers, retrying on transient wait-until-ready failures.
// Mirrors pipeline.runMySQLWithRetry's shape. Calls
// testcontainers.SkipIfProviderIsNotHealthy internally so callers
// don't need to; t.Fatalf on final exhaustion mirrors the prior
// single-shot helpers' error path.
func runPGWithRetry(t *testing.T, image string, opts ...testcontainers.ContainerCustomizer) *pgtc.PostgresContainer {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	var lastErr error
	for attempt := 1; attempt <= pgPerTestBootAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), pgPerTestBootTimeout)
		container, err := pgtc.Run(ctx, image, opts...)
		cancel()
		if err == nil {
			if attempt > 1 {
				log.Printf("postgres per-test boot attempt %d/%d succeeded",
					attempt, pgPerTestBootAttempts)
			}
			return container
		}
		if container != nil {
			_ = container.Terminate(context.Background())
		}
		lastErr = err
		if attempt < pgPerTestBootAttempts {
			backoff := pgPerTestBootBackoff(attempt)
			log.Printf("postgres per-test boot attempt %d/%d failed: %v; retrying in %s",
				attempt, pgPerTestBootAttempts, err, backoff)
			time.Sleep(backoff)
			continue
		}
		log.Printf("postgres per-test boot attempt %d/%d failed: %v; giving up",
			attempt, pgPerTestBootAttempts, err)
	}
	t.Fatalf("start container: %d attempts exhausted: %v", pgPerTestBootAttempts, lastErr)
	return nil
}
