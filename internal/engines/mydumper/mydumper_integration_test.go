//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Real-dump end-to-end validation for the mydumper source engine
// (ADR-0161). The dumps are produced by the REAL mydumper tool (the
// mydumper/mydumper docker image) against a live seeded MySQL — not by
// hand-built fixtures — in every shape the reader supports: the default
// backslash-escaped binary (the pscale-dump escape class), --hex-blob,
// and gzip/zstd compression.
//
// The oracle is EQUIVALENCE WITH THE LIVE PATH: the same source database
// is migrated once via the live `mysql` engine and once via the dump, into
// separate fresh targets (MySQL and Postgres), and the two targets are
// compared row-by-row through the target engine's own reader. Any
// divergence — a mis-lexed escape, a rounded integer, a shifted instant —
// shows up as a target mismatch. A direct reader-level comparison
// (dump-reader rows vs live-reader rows) additionally pins the ir.Row
// contract per column for the family corpus.

package mydumper

import (
	"archive/tar"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline"
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	// Postgres must self-register for the cross-engine target leg. The
	// mysql engine is already registered via this package's non-test
	// imports.
	_ "sluicesync.dev/sluice/internal/engines/postgres"

	"github.com/testcontainers/testcontainers-go"
	mysqltc "github.com/testcontainers/testcontainers-go/modules/mysql"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	itMySQLImage = "ghcr.io/sluicesync/sluice-mysql:8.0-prebaked"
	itPGImage    = "ghcr.io/sluicesync/sluice-postgres:16-prebaked"
	// Pinned to the release this suite was ground-truthed against (the
	// `_binary "…"` introducers, double-quoted strings, and ini metadata
	// shapes are version-specific); bump deliberately, re-ground-truthing.
	itDumperImage  = "mydumper/mydumper:v1.0.3-1"
	itSourceDB     = "shop"
	itBootTimeout  = 4 * time.Minute
	itBootAttempts = 3
)

// seedDDL is the family corpus: every value family the engine maps (minus
// geometry — the plain PG target image has no PostGIS, and the geometry
// decode is unit-pinned), plus an FK pair and a filter-target table.
//
// FLOAT (single-precision) values are chosen float32-exact within 6
// significant digits (-2.5, 1.5) DELIBERATELY: mydumper's dump text
// renders FLOAT through mysqld's ~6-digit formatter (8388608 dumps as
// 8.38861e6 — ground-truthed against v1.0.3), so values beyond that
// precision diverge from the live read AT DUMP TIME. That wart is
// documented (ADR-0161 §4) and WARNed by the reader
// ([warnIfSingleFloatColumns]); the equivalence oracle here pins the
// faithful-within-the-format cases.
//
// DOUBLE values, by contrast, DELIBERATELY require MORE than 6
// significant digits (3.141592653589793 / 0.1 / 1.7976931348623157e308 —
// 16-17 digits, shortest-roundtrip) so the equivalence legs PROVE that
// DOUBLE is not display-rounded like its FLOAT sibling (the Bug-74
// family-vs-representative discipline: ground-truthed against v1.0.3,
// where all three dump at full precision).
const seedDDL = `
CREATE TABLE families (
  id    BIGINT NOT NULL,
  i8    TINYINT,
  i8u   TINYINT UNSIGNED,
  i16   SMALLINT,
  i24   MEDIUMINT,
  i32   INT,
  i64   BIGINT,
  u64   BIGINT UNSIGNED,
  flag  TINYINT(1),
  dec20 DECIMAL(20,4),
  f32   FLOAT,
  f64   DOUBLE,
  ch    CHAR(4),
  vc    VARCHAR(64),
  txt   TEXT,
  bfix  BINARY(3),
  vbin  VARBINARY(32),
  bl    BLOB,
  d     DATE,
  dt6   DATETIME(6),
  ts6   TIMESTAMP(6) NULL,
  tm3   TIME(3),
  yr    YEAR,
  js    JSON,
  en    ENUM('alpha','beta','it''s'),
  st    SET('x','y','z'),
  bt5   BIT(5),
  bt1   BIT(1),
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO families VALUES
(1, -128, 255, -32768, 8388607, -2147483648, 9007199254740993, 18446744073709551615, 1,
 '1234567890123456.7890', -2.5, 3.141592653589793, 'abcd',
 CONCAT('it''s', CHAR(10), ' a "q" 100% _x_ \\ ', 0xF09F8D8A), 'long text body',
 X'00FF1A', X'0022275C0A00', X'DEADBEEF00',
 '1999-12-31', '2026-01-02 03:04:05.123456', '2001-02-03 04:05:06.500000', '11:22:33.123', 2077,
 '{"a": 1, "b": [true, null]}', 'it''s', 'x,z', b'10101', b'1'),
(2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0,
 NULL, NULL, 0.1, NULL, 'NULL', NULL, NULL, NULL, NULL,
 NULL, NULL, NULL, NULL, NULL, NULL, NULL, '', NULL, NULL),
(3, 1, 2, 3, 4, 5, 6, 7, 1,
 '0.0001', 1.5, 1.7976931348623157e308, 'wxyz', 'plain', 'ascii only',
 X'414243', X'58', X'59',
 '2000-01-01', '2000-01-01 00:00:00.000000', '2000-01-01 00:00:01.000000', '00:00:00.000', 1901,
 '[]', 'alpha', 'y', b'1', b'0');

CREATE TABLE users (
  id    BIGINT NOT NULL AUTO_INCREMENT,
  email VARCHAR(190) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY users_email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE posts (
  id      BIGINT NOT NULL AUTO_INCREMENT,
  user_id BIGINT NOT NULL,
  body    TEXT,
  PRIMARY KEY (id),
  KEY posts_user (user_id),
  CONSTRAINT posts_fk FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO users (email) VALUES ('a@example.com'), ('b@example.com');
INSERT INTO posts (user_id, body) VALUES (1, 'first'), (1, 'second'), (2, NULL);

CREATE TABLE skipme (
  id BIGINT NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO skipme VALUES (1), (2);

CREATE TABLE geo (
  id BIGINT NOT NULL,
  pt POINT,
  g  GEOMETRY,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO geo VALUES
(1, ST_GeomFromText('POINT(1 2)'), ST_GeomFromText('LINESTRING(0 0,1 1,2 0)')),
(2, NULL, ST_GeomFromText('POINT(3 4)', 4326)),
(3, ST_GeomFromText('POINT(-5.5 6.25)'), NULL);
`

// TestMydumperIntegration_RealDumpEndToEnd is the ground-truth suite. One
// MySQL + one Postgres + one mydumper container serve every leg.
func TestMydumperIntegration_RealDumpEndToEnd(t *testing.T) {
	ctx := context.Background()

	mysqlC, mysqlRootDSN := startMySQLIT(t)
	seedSourceDB(t, mysqlRootDSN)
	srcDSN := mysqlRootDSN // already points at itSourceDB

	pgAdminDSN := startPostgresIT(t)

	dumperC := startDumperIT(t)
	sourceIP, err := mysqlC.ContainerIP(ctx)
	if err != nil {
		t.Fatalf("mysql container IP: %v", err)
	}

	// Produce the real dumps: every shape leg from the same seeded source.
	legs := []struct{ name, flags string }{
		{"escape", ""}, // default: backslash-escaped binary, the pscale escape class
		{"hex", "--hex-blob"},
		{"gzip", "-c gzip"},
		{"zstd", "-c zstd"},
	}
	dumpDirs := map[string]string{}
	for _, leg := range legs {
		dumpDirs[leg.name] = runRealMydumper(t, dumperC, sourceIP, leg.name, leg.flags)
	}

	mysqlEng := mustEngine(t, "mysql")
	pgEng := mustEngine(t, "postgres")
	dumpEng := mustEngine(t, "mydumper")

	// PG has no unsigned 64-bit integer: the live MySQL→PG path refuses
	// u64 values above MaxInt64 loudly, and the documented remedy is the
	// decimal(20,0) type override — applied identically on the oracle and
	// dump PG legs, which also exercises --type-override THROUGH the
	// mydumper reader (the override rewrites the IR type its decode
	// dispatches on).
	u64Override := config.Mapping{
		Table: "families", Column: "u64", TargetType: "decimal",
		TargetTypeOptions: map[string]any{"precision": 20, "scale": 0},
	}

	// The geo table rides only the MySQL legs and the reader-equivalence
	// compares: the plain PG target image has no PostGIS, so the PG legs
	// (oracle AND dump alike) exclude it. The SRID+WKB dump shape is still
	// validated against real mydumper output on the MySQL side.
	pgFilter, err := migcore.NewTableFilter(nil, []string{"geo"})
	if err != nil {
		t.Fatal(err)
	}

	// Oracle targets: the live-mysql-source migration into each engine.
	oracleMy := createMySQLDB(t, mysqlRootDSN, "oracle_my")
	runMigrate(t, mysqlEng, mysqlEng, srcDSN, oracleMy, migcore.TableFilter{})
	oraclePG := createPGDB(t, pgAdminDSN, "oracle_pg")
	runMigrate(t, mysqlEng, pgEng, srcDSN, oraclePG, pgFilter, u64Override)

	// Leg 1+2: full migrate of the escape-shape and hex-blob dumps into
	// BOTH targets, compared table-by-table against the live-path oracle.
	for _, leg := range []string{"escape", "hex"} {
		t.Run("migrate-"+leg+"-to-mysql", func(t *testing.T) {
			target := createMySQLDB(t, mysqlRootDSN, "dump_"+leg+"_my")
			runMigrate(t, dumpEng, mysqlEng, dumpDirs[leg], target, migcore.TableFilter{})
			compareTargets(t, mysqlEng, oracleMy, target)
		})
		t.Run("migrate-"+leg+"-to-postgres", func(t *testing.T) {
			target := createPGDB(t, pgAdminDSN, "dump_"+leg+"_pg")
			runMigrate(t, dumpEng, pgEng, dumpDirs[leg], target, pgFilter, u64Override)
			compareTargets(t, pgEng, oraclePG, target)
		})
	}

	// Leg 3: gzip-compressed dump → MySQL target (the decompression path
	// over a real compressed dump; value assertions identical).
	t.Run("migrate-gzip-to-mysql", func(t *testing.T) {
		target := createMySQLDB(t, mysqlRootDSN, "dump_gzip_my")
		runMigrate(t, dumpEng, mysqlEng, dumpDirs["gzip"], target, migcore.TableFilter{})
		compareTargets(t, mysqlEng, oracleMy, target)
	})

	// Leg 4: zstd-compressed dump — reader-level equivalence (the values
	// are identical to the gzip leg by construction; this pins the zstd
	// decompressor over a real `mydumper -c zstd` artifact).
	t.Run("zstd-reader-equivalence", func(t *testing.T) {
		compareDumpReaderToLive(t, dumpEng, mysqlEng, dumpDirs["zstd"], srcDSN)
	})

	// Reader-level ir.Row equivalence on the uncompressed legs too — the
	// per-column pin that the dump reader produces the SAME canonical
	// values as the live MySQL reader (docs/value-types.md).
	t.Run("escape-reader-equivalence", func(t *testing.T) {
		compareDumpReaderToLive(t, dumpEng, mysqlEng, dumpDirs["escape"], srcDSN)
	})
	t.Run("hex-reader-equivalence", func(t *testing.T) {
		compareDumpReaderToLive(t, dumpEng, mysqlEng, dumpDirs["hex"], srcDSN)
	})

	// TableFilter: --exclude-table skipme must keep the table off the target.
	t.Run("table-filter", func(t *testing.T) {
		filter, err := migcore.NewTableFilter(nil, []string{"skipme"})
		if err != nil {
			t.Fatal(err)
		}
		target := createMySQLDB(t, mysqlRootDSN, "dump_filtered_my")
		runMigrate(t, dumpEng, mysqlEng, dumpDirs["escape"], target, filter)
		schema := readSchemaIT(t, mysqlEng, target)
		for _, tbl := range schema.Tables {
			if tbl.Name == "skipme" {
				t.Fatal("skipme migrated despite --exclude-table")
			}
		}
		if len(schema.Tables) != 4 {
			t.Fatalf("filtered target tables = %d; want 4 (families, users, posts, geo)", len(schema.Tables))
		}
	})

	// Bug 188: one unsupported-charset table in a dump must not block
	// migrating the REST of the dump — the ADR-0161 §5 refusal is
	// deferred past the table filter, so --exclude-table routes around
	// it, while an INCLUDED violating table still refuses up front
	// (before any table copies — not mid-run).
	t.Run("charset-refusal-deferred-past-exclude", func(t *testing.T) {
		stained := t.TempDir()
		copyDumpDir(t, dumpDirs["escape"], stained)
		dbPrefix := dumpDatabasePrefix(t, stained)
		writeDumpFile(t, stained, dbPrefix+".legacy_latin1-schema.sql",
			"CREATE TABLE `legacy_latin1` (`id` bigint NOT NULL, `txt` varchar(20)) DEFAULT CHARSET=latin1;")
		writeDumpFile(t, stained, dbPrefix+".legacy_latin1.00000.sql",
			"INSERT INTO `legacy_latin1` VALUES (1,'a');")

		// (a) Included → loud up-front refusal naming column + remedy,
		// with ZERO tables landed (pre-DDL, not mid-migration).
		target := createMySQLDB(t, mysqlRootDSN, "dump_stained_refused")
		err := runMigrateErr(t, dumpEng, mysqlEng, stained, target, migcore.TableFilter{})
		if err == nil || !strings.Contains(err.Error(), "latin1") ||
			!strings.Contains(err.Error(), "--exclude-table") {
			t.Fatalf("stained dump migrate = %v; want the charset refusal naming latin1 + the exclude remedy", err)
		}
		if n := len(readSchemaIT(t, mysqlEng, target).Tables); n != 0 {
			t.Fatalf("refused migrate landed %d table(s); want 0 (refusal must fire before DDL)", n)
		}

		// (b) Excluded → the rest of the dump migrates cleanly.
		filter, ferr := migcore.NewTableFilter(nil, []string{"legacy_latin1"})
		if ferr != nil {
			t.Fatal(ferr)
		}
		target2 := createMySQLDB(t, mysqlRootDSN, "dump_stained_excluded")
		runMigrate(t, dumpEng, mysqlEng, stained, target2, filter)
		if n := len(readSchemaIT(t, mysqlEng, target2).Tables); n != 5 {
			t.Fatalf("excluded-run target tables = %d; want the 5 non-latin1 tables", n)
		}
	})

	// verify --depth count: the dump source vs a migrated target is clean.
	t.Run("verify-count-depth", func(t *testing.T) {
		verifier := &pipeline.Verifier{
			Source:    dumpEng,
			Target:    mysqlEng,
			SourceDSN: dumpDirs["escape"],
			TargetDSN: oracleMy,
			Depth:     pipeline.VerifyDepthCount,
			Format:    "text",
			Out:       io.Discard,
		}
		result, err := verifier.Run(ctx)
		if err != nil {
			t.Fatalf("verify: %v", err)
		}
		if result.HasMismatch() {
			t.Fatalf("verify reported mismatches: %+v", result.Summary)
		}
	})
}

// ---------------------------------------------------------------------------
// container plumbing
// ---------------------------------------------------------------------------

func startMySQLIT(t *testing.T) (*mysqltc.MySQLContainer, string) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	var lastErr error
	for attempt := 1; attempt <= itBootAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), itBootTimeout)
		container, err := mysqltc.Run(
			ctx,
			itMySQLImage,
			mysqltc.WithDatabase(itSourceDB),
			mysqltc.WithUsername("root"),
			mysqltc.WithPassword("rootpw"),
			testcontainers.WithWaitStrategyAndDeadline(
				itBootTimeout,
				wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(itBootTimeout),
			),
		)
		cancel()
		if err == nil {
			t.Cleanup(func() {
				shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
				defer c()
				_ = container.Terminate(shutdown)
			})
			dsn, err := container.ConnectionString(context.Background(), "parseTime=true", "multiStatements=true")
			if err != nil {
				t.Fatalf("mysql connection string: %v", err)
			}
			// The pre-baked image skips the first-boot init step, so the
			// module's MYSQL_DATABASE env is inert — create the source
			// database explicitly (the shared-container helpers do the same).
			adminDSN := replaceMySQLDBName(t, dsn, "mysql")
			db, err := sql.Open("mysql", adminDSN)
			if err != nil {
				t.Fatalf("open admin conn: %v", err)
			}
			if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS " + itSourceDB); err != nil {
				_ = db.Close()
				t.Fatalf("create %s: %v", itSourceDB, err)
			}
			_ = db.Close()
			return container, dsn
		}
		if container != nil {
			_ = container.Terminate(context.Background())
		}
		lastErr = err
		time.Sleep(time.Duration(attempt) * 20 * time.Second)
	}
	t.Fatalf("mysql boot failed after %d attempts: %v", itBootAttempts, lastErr)
	return nil, ""
}

func startPostgresIT(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), itBootTimeout)
	defer cancel()
	container, err := pgtc.Run(
		ctx,
		itPGImage,
		pgtc.WithDatabase("postgres"),
		pgtc.WithUsername("test"),
		pgtc.WithPassword("test"),
		// The pre-baked image skips the init phase, so the module's
		// default two-“ready to accept connections” wait never matches —
		// use the single-match strategy the pipeline's PG prebaked boots
		// use, and target the always-present `postgres` admin database
		// (POSTGRES_DB is inert without the init step).
		testcontainers.WithWaitStrategyAndDeadline(
			itBootTimeout,
			wait.ForAll(
				wait.ForLog("database system is ready to accept connections"),
				wait.ForListeningPort("5432/tcp"),
			),
		),
	)
	if err != nil {
		t.Fatalf("postgres boot: %v", err)
	}
	t.Cleanup(func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	})
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}
	return dsn
}

// startDumperIT boots the mydumper image parked on `sleep` so each leg
// runs via Exec against the same container.
func startDumperIT(t *testing.T) testcontainers.Container {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), itBootTimeout)
	defer cancel()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:      itDumperImage,
			Entrypoint: []string{"sleep"},
			Cmd:        []string{"1800"},
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("mydumper container boot: %v", err)
	}
	t.Cleanup(func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	})
	return container
}

// runRealMydumper executes one real `mydumper` run inside the dumper
// container against the seeded source, tars the output, copies the tar
// out, and extracts it into a local temp dir — the dump directory the
// engine under test reads.
func runRealMydumper(t *testing.T, dumper testcontainers.Container, sourceIP, name, extraFlags string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	outDir := "/dump-" + name
	tarPath := outDir + ".tar"
	script := fmt.Sprintf(
		"mydumper -h %s -P 3306 -u root -p rootpw -B %s -o %s %s && tar -cf %s -C %s .",
		sourceIP, itSourceDB, outDir, extraFlags, tarPath, outDir,
	)
	code, reader, err := dumper.Exec(ctx, []string{"sh", "-c", script})
	if err != nil {
		t.Fatalf("mydumper %s exec: %v", name, err)
	}
	out, _ := io.ReadAll(reader)
	if code != 0 {
		t.Fatalf("mydumper %s exited %d:\n%s", name, code, out)
	}

	tarReader, err := dumper.CopyFileFromContainer(ctx, tarPath)
	if err != nil {
		t.Fatalf("copy %s: %v", tarPath, err)
	}
	defer func() { _ = tarReader.Close() }()

	local := t.TempDir()
	tr := tar.NewReader(tarReader)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("untar %s: %v", name, err)
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		base := filepath.Base(filepath.Clean(hdr.Name))
		if base == "." || base == ".." {
			continue
		}
		f, err := os.Create(filepath.Join(local, base)) //nolint:gosec // basename-only, test temp dir
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.Copy(f, tr); err != nil { //nolint:gosec // trusted test artifact
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return local
}

// ---------------------------------------------------------------------------
// database plumbing
// ---------------------------------------------------------------------------

func seedSourceDB(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(seedDDL); err != nil {
		t.Fatalf("seed: %v", err)
	}
}

// createMySQLDB creates a fresh database on the shared MySQL container and
// returns its DSN.
func createMySQLDB(t *testing.T, rootDSN, name string) string {
	t.Helper()
	db, err := sql.Open("mysql", rootDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec("CREATE DATABASE " + name); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	return replaceMySQLDBName(t, rootDSN, name)
}

// replaceMySQLDBName swaps the database segment of a go-sql-driver DSN
// (`user:pass@tcp(host:port)/db?params`).
func replaceMySQLDBName(t *testing.T, dsn, name string) string {
	t.Helper()
	params := ""
	base := dsn
	if i := strings.IndexByte(dsn, '?'); i >= 0 {
		base, params = dsn[:i], dsn[i:]
	}
	slash := strings.LastIndexByte(base, '/')
	if slash < 0 {
		t.Fatalf("mysql DSN %q has no database segment", dsn)
	}
	return base[:slash+1] + name + params
}

// createPGDB creates a fresh database on the shared PG container and
// returns its DSN.
func createPGDB(t *testing.T, adminDSN, name string) string {
	t.Helper()
	db, err := sql.Open("pgx", adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec("CREATE DATABASE " + name); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	// The PG DSN is a URI; swap the path segment.
	i := strings.LastIndexByte(adminDSN, '/')
	rest := adminDSN[i+1:]
	if q := strings.IndexByte(rest, '?'); q >= 0 {
		return adminDSN[:i+1] + name + rest[q:]
	}
	return adminDSN[:i+1] + name
}

func mustEngine(t *testing.T, name string) ir.Engine {
	t.Helper()
	e, ok := engines.Get(name)
	if !ok {
		t.Fatalf("engine %q not registered", name)
	}
	return e
}

func runMigrate(t *testing.T, source, target ir.Engine, srcDSN, tgtDSN string, filter migcore.TableFilter,
	mappings ...config.Mapping,
) {
	t.Helper()
	mig := &pipeline.Migrator{
		Source:    source,
		Target:    target,
		SourceDSN: srcDSN,
		TargetDSN: tgtDSN,
		Filter:    filter,
		Mappings:  mappings,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run(%s → %s): %v", source.Name(), target.Name(), err)
	}
}

// runMigrateErr is runMigrate's expected-to-fail sibling: it returns
// the Migrator error instead of failing the test, for refusal pins.
func runMigrateErr(t *testing.T, source, target ir.Engine, srcDSN, tgtDSN string, filter migcore.TableFilter,
	mappings ...config.Mapping,
) error {
	t.Helper()
	mig := &pipeline.Migrator{
		Source:    source,
		Target:    target,
		SourceDSN: srcDSN,
		TargetDSN: tgtDSN,
		Filter:    filter,
		Mappings:  mappings,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	return mig.Run(ctx)
}

// copyDumpDir copies every regular file of a dump directory into dst
// (flat layout — mydumper dirs have no subdirectories).
func copyDumpDir(t *testing.T, src, dst string) {
	t.Helper()
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// dumpDatabasePrefix returns the `<database>` filename prefix of a dump
// dir, discovered from any table schema file.
func dumpDatabasePrefix(t *testing.T, dir string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		name, _ := stripCompressionSuffix(e.Name())
		if !strings.HasSuffix(name, "-schema.sql") || strings.HasSuffix(name, "-schema-create.sql") {
			continue
		}
		if dot := strings.IndexByte(name, '.'); dot > 0 {
			return name[:dot]
		}
	}
	t.Fatal("no table schema file found to derive the database prefix")
	return ""
}

func readSchemaIT(t *testing.T, eng ir.Engine, dsn string) *ir.Schema {
	t.Helper()
	ctx := context.Background()
	sr, err := eng.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer migcore.CloseIf(sr)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	return schema
}

// ---------------------------------------------------------------------------
// comparison
// ---------------------------------------------------------------------------

// compareTargets reads every table of two same-engine targets through the
// engine's own reader and requires identical canonical rows — the
// live-path-equivalence oracle.
func compareTargets(t *testing.T, eng ir.Engine, oracleDSN, dumpDSN string) {
	t.Helper()
	oracle := snapshotDatabase(t, eng, oracleDSN)
	got := snapshotDatabase(t, eng, dumpDSN)

	if len(oracle) != len(got) {
		t.Fatalf("table count: oracle %d vs dump-migrated %d", len(oracle), len(got))
	}
	for name, oracleRows := range oracle {
		gotRows, ok := got[name]
		if !ok {
			t.Errorf("table %s missing on the dump-migrated target", name)
			continue
		}
		if len(oracleRows) != len(gotRows) {
			t.Errorf("table %s: rows oracle=%d dump=%d", name, len(oracleRows), len(gotRows))
			continue
		}
		for i := range oracleRows {
			if oracleRows[i] != gotRows[i] {
				t.Errorf("table %s row %d diverged:\noracle: %s\ndump:   %s", name, i, oracleRows[i], gotRows[i])
			}
		}
	}
}

// snapshotDatabase renders every table's rows into sorted canonical
// strings via the engine's schema+row readers.
func snapshotDatabase(t *testing.T, eng ir.Engine, dsn string) map[string][]string {
	t.Helper()
	ctx := context.Background()
	schema := readSchemaIT(t, eng, dsn)
	rr, err := eng.OpenRowReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer migcore.CloseIf(rr)

	out := map[string][]string{}
	for _, table := range schema.Tables {
		rows := drainRowsIT(t, rr, table)
		rendered := make([]string, 0, len(rows))
		for _, row := range rows {
			rendered = append(rendered, canonicalRow(table, row))
		}
		sort.Strings(rendered)
		out[table.Name] = rendered
	}
	return out
}

func drainRowsIT(t *testing.T, rr ir.RowReader, table *ir.Table) []ir.Row {
	t.Helper()
	ch, err := rr.ReadRows(context.Background(), table)
	if err != nil {
		t.Fatalf("ReadRows(%s): %v", table.Name, err)
	}
	var rows []ir.Row
	for row := range ch {
		rows = append(rows, row)
	}
	if err := rr.Err(); err != nil {
		t.Fatalf("reader Err(%s): %v", table.Name, err)
	}
	return rows
}

// canonicalRow renders a row as `col=value|col=value|…` in column order
// with type-stable value formatting, so two readers' rows compare exactly.
func canonicalRow(table *ir.Table, row ir.Row) string {
	parts := make([]string, 0, len(table.Columns))
	for _, col := range table.Columns {
		if col.IsGenerated() {
			continue
		}
		parts = append(parts, col.Name+"="+canonicalValue(row[col.Name]))
	}
	return strings.Join(parts, "|")
}

// canonicalValue folds the ir.Row value shapes into comparison-stable
// text. int64-vs-uint64 (both legal for unsigned columns per the value
// contract) fold to decimal text; time.Time to RFC3339Nano UTC; []byte to
// hex; []string to a joined list.
func canonicalValue(v any) string {
	switch x := v.(type) {
	case nil:
		return "<NULL>"
	case int64:
		return fmt.Sprintf("int:%d", x)
	case uint64:
		return fmt.Sprintf("int:%d", x)
	case float64:
		return fmt.Sprintf("float:%g", x)
	case bool:
		return fmt.Sprintf("bool:%v", x)
	case time.Time:
		return "time:" + x.UTC().Format(time.RFC3339Nano)
	case []byte:
		return fmt.Sprintf("bytes:%x", x)
	case []string:
		return "set:" + strings.Join(x, ",")
	case string:
		return "str:" + x
	default:
		return fmt.Sprintf("%T:%v", v, v)
	}
}

// compareDumpReaderToLive pins reader-level ir.Row equivalence: the dump
// engine's rows for every table must canonicalize identically to the live
// MySQL engine's rows for the same source table.
func compareDumpReaderToLive(t *testing.T, dumpEng, liveEng ir.Engine, dumpDir, liveDSN string) {
	t.Helper()
	dump := snapshotDatabase(t, dumpEng, dumpDir)
	live := snapshotDatabase(t, liveEng, liveDSN)
	if len(dump) != len(live) {
		t.Fatalf("table count: dump %d vs live %d", len(dump), len(live))
	}
	for name, liveRows := range live {
		dumpRows, ok := dump[name]
		if !ok {
			t.Errorf("table %s missing from the dump", name)
			continue
		}
		if len(liveRows) != len(dumpRows) {
			t.Errorf("table %s: rows live=%d dump=%d", name, len(liveRows), len(dumpRows))
			continue
		}
		for i := range liveRows {
			if liveRows[i] != dumpRows[i] {
				t.Errorf("table %s row %d diverged:\nlive: %s\ndump: %s", name, i, liveRows[i], dumpRows[i])
			}
		}
	}
}
