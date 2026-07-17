//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// MariaDB-flavor integration suite (roadmap item 73 Phase 1), run
// against BOTH supported LTS lines — mariadb:11.4 and mariadb:10.11 —
// plus the shared MySQL 8 container as the parity anchor:
//
//   - the COLUMN_DEFAULT normalization matrix, re-derived from live
//     servers and pinned equal to what the SAME logical schema
//     produces via a real MySQL 8 read (the Bug-74 class discipline);
//   - full-corpus migrate mariadb → mysql8 and mysql8 → mariadb with
//     canonical row ground truth + `verify` in both role directions
//     (target-side schema reads were the probe's leg-5c wall);
//   - backup → restore round-trip on mariadb;
//   - the coded CDC refusal on `sync start`;
//   - the plain-mysql-driver steering WARN + the loud srs_id wall;
//   - the mariadb-flavor-on-MySQL-server refusal;
//   - the SEQUENCE / SYSTEM VERSIONED census refusal;
//   - the VALUES() upsert spelling executed live (applier single-row,
//     position write, migrate-state — the statements the row-alias
//     form would 1064 on).
package mysql

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/sluicecode"
)

const (
	mariadb114Image  = "mariadb:11.4"
	mariadb1011Image = "mariadb:10.11"
)

// sharedMariaDBState mirrors sharedMySQLState's lazily-booted shared-
// container model, one instance per MariaDB image. Tests reset a
// per-test database on the shared container instead of paying a boot
// per test; TestMain terminates whatever booted.
type sharedMariaDBState struct {
	once    sync.Once
	bootErr error

	host      string
	port      string
	container testcontainers.Container
}

var sharedMariaDBs = map[string]*sharedMariaDBState{
	mariadb114Image:  {},
	mariadb1011Image: {},
}

// terminateSharedMariaDBs is called from TestMain after the run.
func terminateSharedMariaDBs() {
	for _, s := range sharedMariaDBs {
		if s.container != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_ = s.container.Terminate(ctx)
			cancel()
		}
	}
}

// ensureSharedMariaDB boots the shared container for image on first
// use. The wait strategy is ForSQL against the mapped port: the
// MariaDB entrypoint's init phase starts a socket-only temp server, so
// log-line waits are ambiguous while a successful SQL round-trip on
// the TCP port is definitive.
func ensureSharedMariaDB(t *testing.T, image string) (host, port string) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	s, ok := sharedMariaDBs[image]
	if !ok {
		t.Fatalf("unregistered mariadb image %q", image)
	}
	s.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
		defer cancel()
		req := testcontainers.ContainerRequest{
			Image: image,
			Env: map[string]string{
				"MARIADB_ROOT_PASSWORD": "rootpw",
				"MARIADB_DATABASE":      "seed",
			},
			ExposedPorts: []string{"3306/tcp"},
			WaitingFor: wait.ForSQL("3306/tcp", "mysql", func(host string, port network.Port) string {
				return fmt.Sprintf("root:rootpw@tcp(%s:%s)/seed", host, port.Port())
			}).WithStartupTimeout(4 * time.Minute),
		}
		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			s.bootErr = fmt.Errorf("boot %s: %w", image, err)
			return
		}
		hostV, err := container.Host(ctx)
		if err != nil {
			s.bootErr = fmt.Errorf("%s host: %w", image, err)
			return
		}
		portV, err := container.MappedPort(ctx, "3306/tcp")
		if err != nil {
			s.bootErr = fmt.Errorf("%s port: %w", image, err)
			return
		}
		s.host, s.port, s.container = hostV, portV.Port(), container
		log.Printf("shared mariadb container booted: %s at %s:%s", image, s.host, s.port)
	})
	if s.bootErr != nil {
		t.Fatalf("shared mariadb (%s) unavailable: %v", image, s.bootErr)
	}
	return s.host, s.port
}

// newMariaDB resets dbName on image's shared container and returns a
// DSN pointed at it (fresh-database semantics, mirroring newSharedDB).
func newMariaDB(t *testing.T, image, dbName string) string {
	t.Helper()
	host, port := ensureSharedMariaDB(t, image)
	rootDSN := fmt.Sprintf("root:rootpw@tcp(%s:%s)/seed?parseTime=true&multiStatements=true", host, port)
	db, err := sql.Open("mysql", rootDSN)
	if err != nil {
		t.Fatalf("open %s root: %v", image, err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ddl := fmt.Sprintf("DROP DATABASE IF EXISTS `%s`; CREATE DATABASE `%s` CHARACTER SET utf8mb4;", dbName, dbName)
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		t.Fatalf("reset %s on %s: %v", dbName, image, err)
	}
	return fmt.Sprintf("root:rootpw@tcp(%s:%s)/%s?parseTime=true", host, port, dbName)
}

// execSQLScript runs a multi-statement script against dsn.
func execSQLScript(t *testing.T, dsn, script string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn+"&multiStatements=true")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, script); err != nil {
		t.Fatalf("apply script: %v", err)
	}
}

// defaultsMatrixDDL is the live counterpart of the unit parity matrix:
// one logical schema declaring every default shape × type family the
// probes cataloged, in spellings BOTH server families accept. Includes
// the NUL-bearing binary default (X'2700'), which information_schema
// C-truncates on MySQL (repaired via SHOW CREATE recovery) and
// escape-encodes on MariaDB — the two paths must converge on one IR.
const defaultsMatrixDDL = `
	CREATE TABLE t_defaults (
		id              INT          NOT NULL,
		i_pos           INT          DEFAULT 42,
		i_neg           INT          DEFAULT -7,
		d_dec           DECIMAL(10,2) DEFAULT 9.99,
		f_dbl           DOUBLE       DEFAULT 1.5,
		s_plain         VARCHAR(20)  DEFAULT 'abc',
		s_quote         VARCHAR(20)  DEFAULT 'it''s',
		s_null_str      VARCHAR(20)  DEFAULT 'NULL',
		s_empty         VARCHAR(20)  DEFAULT '',
		s_nullable      VARCHAR(20),
		s_expl_null     VARCHAR(20)  DEFAULT NULL,
		s_notnull_nodef VARCHAR(20)  NOT NULL,
		s_nl            VARCHAR(20)  DEFAULT 'a\nb',
		ts_cur          TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
		ts_upd          TIMESTAMP    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		dt3             DATETIME(3)  DEFAULT CURRENT_TIMESTAMP(3),
		b1              BIT(1)       DEFAULT b'1',
		b8              BIT(8)       DEFAULT b'10100101',
		bin2            BINARY(2)    DEFAULT X'4142',
		vb              VARBINARY(4) DEFAULT X'0102',
		vb_nul          VARBINARY(4) DEFAULT X'2700',
		en              ENUM('red','green') DEFAULT 'red',
		bo              TINYINT(1)   DEFAULT TRUE,
		yr              YEAR         DEFAULT 2024,
		u_expr          CHAR(36)     DEFAULT (uuid()),
		PRIMARY KEY (id)
	);`

// readColumnsViaEngine reads t_defaults' columns through the named
// registered engine (the full production read path, guards included).
func readColumnsViaEngine(t *testing.T, engineName, dsn string) map[string]*ir.Column {
	t.Helper()
	eng, ok := engines.Get(engineName)
	if !ok {
		t.Fatalf("engine %q not registered", engineName)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	sr, err := eng.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("%s: OpenSchemaReader: %v", engineName, err)
	}
	defer func() { _ = sr.(interface{ Close() error }).Close() }()
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("%s: ReadSchema: %v", engineName, err)
	}
	for _, tbl := range schema.Tables {
		if tbl.Name == "t_defaults" {
			out := map[string]*ir.Column{}
			for _, c := range tbl.Columns {
				out[c.Name] = c
			}
			return out
		}
	}
	t.Fatalf("%s: t_defaults not found", engineName)
	return nil
}

// TestMariaDB_DefaultsMatrix_LiveParityWithMySQL8 is the integration
// half of the Bug-74 defaults pin: the SAME logical schema is created
// on mariadb:11.4, mariadb:10.11, and real MySQL 8, read through the
// respective flavors, and every column's IR default must be IDENTICAL
// across all three — element-for-element over the full shape × family
// matrix, not one representative.
func TestMariaDB_DefaultsMatrix_LiveParityWithMySQL8(t *testing.T) {
	myDSN, _ := newSharedDB(t, "mdb_defaults_anchor")
	execSQLScript(t, myDSN, defaultsMatrixDDL)
	anchor := readColumnsViaEngine(t, "mysql", myDSN)

	for _, image := range []string{mariadb114Image, mariadb1011Image} {
		image := image
		t.Run(image, func(t *testing.T) {
			mdbDSN := newMariaDB(t, image, "mdb_defaults")
			execSQLScript(t, mdbDSN, defaultsMatrixDDL)
			got := readColumnsViaEngine(t, "mariadb", mdbDSN)

			if len(got) != len(anchor) {
				t.Fatalf("column count mismatch: mariadb %d vs mysql8 %d", len(got), len(anchor))
			}
			for name, want := range anchor {
				g, ok := got[name]
				if !ok {
					t.Errorf("column %q missing from mariadb read", name)
					continue
				}
				if g.Default != want.Default {
					t.Errorf("column %q: mariadb default = %#v; mysql8 anchor = %#v", name, g.Default, want.Default)
				}
				if g.Nullable != want.Nullable {
					t.Errorf("column %q: nullable = %v; anchor %v", name, g.Nullable, want.Nullable)
				}
				// Type identity minus charset/collation (server defaults
				// legitimately differ across families; the collation remap
				// pins that separately).
				if reflect.TypeOf(g.Type) != reflect.TypeOf(want.Type) {
					t.Errorf("column %q: type %T; anchor %T", name, g.Type, want.Type)
				}
			}
		})
	}
}

// corpusDDL + corpusRows: the migrate/backup/verify corpus — every
// MySQL-family type family the mariadb flavor declares, with multibyte
// text, extremes, NULLs, an FK child, and a JSON column (real JSON on
// MySQL 8, LONGTEXT + json_valid CHECK on MariaDB — both spellings are
// valid DDL on both families).
const corpusDDL = `
	CREATE TABLE corpus (
		id    BIGINT NOT NULL AUTO_INCREMENT,
		i8    TINYINT,
		i16   SMALLINT,
		i32   INT,
		i64   BIGINT,
		u64   BIGINT UNSIGNED,
		dec1  DECIMAL(20,5),
		f32   FLOAT,
		f64   DOUBLE,
		b1    BIT(1),
		b16   BIT(16),
		ch    CHAR(8),
		vc    VARCHAR(64) NOT NULL,
		txt   TEXT,
		bin4  BINARY(4),
		vbin  VARBINARY(16),
		blb   BLOB,
		d     DATE,
		tm    TIME(3),
		dt    DATETIME(6),
		ts    TIMESTAMP(3) NULL,
		yr    YEAR,
		en    ENUM('a','b','c'),
		st    SET('x','y','z'),
		bo    TINYINT(1),
		js    JSON,
		PRIMARY KEY (id),
		UNIQUE KEY corpus_vc_uniq (vc)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

	CREATE TABLE corpus_child (
		id        BIGINT NOT NULL AUTO_INCREMENT,
		corpus_id BIGINT NOT NULL,
		note      VARCHAR(32),
		PRIMARY KEY (id),
		KEY corpus_child_fk_idx (corpus_id),
		CONSTRAINT corpus_child_fk FOREIGN KEY (corpus_id)
			REFERENCES corpus (id) ON DELETE CASCADE
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

	INSERT INTO corpus
		(i8, i16, i32, i64, u64, dec1, f32, f64, b1, b16, ch, vc, txt, bin4, vbin, blb, d, tm, dt, ts, yr, en, st, bo, js)
	VALUES
		(-128, -32768, -2147483648, -9223372036854775808, 18446744073709551615,
		 '123456789012345.54321', 1.5, 2.25, b'1', b'1010101010101010',
		 'abc', 'row-one', 'héllo 世界', X'DEADBEEF', X'0102030405', X'00FF00FF',
		 '2024-02-29', '13:14:15.123', '2024-06-07 08:09:10.123456', '2024-06-07 08:09:10.123',
		 2024, 'b', 'x,z', 1, '{"k": "v", "n": 1}'),
		(127, 32767, 2147483647, 9223372036854775807, 0,
		 '-0.00001', -1.5, -2.25, b'0', b'0000000000000001',
		 '', 'row-two', '', X'00000000', X'', X'FF',
		 '1970-01-01', '00:00:00.000', '1000-01-01 00:00:00.000000', '1971-01-01 00:00:01.000',
		 1901, 'a', '', 0, '[1, 2, 3]'),
		(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		 NULL, 'row-three-nulls', NULL, NULL, NULL, NULL,
		 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

	INSERT INTO corpus_child (corpus_id, note) VALUES (1, 'child-of-one'), (1, 'second'), (2, 'child-of-two');`

// dumpCorpus reads the corpus rows over the PREPARED (binary) protocol
// — the dummy `? = 1` predicate forces it — so values arrive typed and
// server-formatting-independent, then canonicalizes each into a string
// (bytes → hex, times → RFC3339Nano UTC). Independent of sluice's own
// readers by design (the "verify must not ride the reader under test"
// rule).
func dumpCorpus(t *testing.T, dsn, table string) []string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, "SELECT * FROM `"+table+"` WHERE ? = 1 ORDER BY id", 1)
	if err != nil {
		t.Fatalf("dump %s: %v", table, err)
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	var out []string
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan: %v", err)
		}
		parts := make([]string, len(cols))
		for i, v := range vals {
			parts[i] = cols[i] + "=" + canonicalValue(v)
		}
		out = append(out, strings.Join(parts, "|"))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}

func canonicalValue(v any) string {
	switch x := v.(type) {
	case nil:
		return "<NULL>"
	case []byte:
		// Boolean fold: sluice's value contract maps BIT(1) ↔ TINYINT(1)
		// ↔ ir.Boolean, so a source BIT(1) legitimately lands as
		// TINYINT(1) on the target — the wire type changes (1-byte bit
		// string vs int64) while the value is identical. Canonicalize
		// the two spellings of a boolean to one form; both sides pass
		// through this same fold, so same-type columns stay exact.
		if len(x) == 1 && (x[0] == 0x00 || x[0] == 0x01) {
			return strconv.Itoa(int(x[0]))
		}
		return "0x" + hex.EncodeToString(x)
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano)
	case float32:
		return strconv.FormatFloat(float64(x), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case int64:
		return strconv.FormatInt(x, 10)
	case uint64:
		return strconv.FormatUint(x, 10)
	case string:
		return x
	default:
		return fmt.Sprintf("%v", x)
	}
}

func assertCorpusEqual(t *testing.T, srcDSN, dstDSN string) {
	t.Helper()
	for _, table := range []string{"corpus", "corpus_child"} {
		src := dumpCorpus(t, srcDSN, table)
		dst := dumpCorpus(t, dstDSN, table)
		if !reflect.DeepEqual(src, dst) {
			t.Errorf("table %s: row ground truth differs\nsource: %v\ntarget: %v", table, src, dst)
		}
	}
}

// runMigrate drives pipeline.Migrator between two registered engines.
func runMigrate(t *testing.T, srcEngine, srcDSN, dstEngine, dstDSN string) {
	t.Helper()
	src, ok := engines.Get(srcEngine)
	if !ok {
		t.Fatalf("engine %q not registered", srcEngine)
	}
	dst, ok := engines.Get(dstEngine)
	if !ok {
		t.Fatalf("engine %q not registered", dstEngine)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	mig := &pipeline.Migrator{Source: src, SourceDSN: srcDSN, Target: dst, TargetDSN: dstDSN}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("migrate %s → %s: %v", srcEngine, dstEngine, err)
	}
}

// runVerify drives pipeline.Verifier (count depth) and asserts clean.
func runVerify(t *testing.T, srcEngine, srcDSN, dstEngine, dstDSN string) {
	t.Helper()
	src, _ := engines.Get(srcEngine)
	dst, _ := engines.Get(dstEngine)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var buf strings.Builder
	v := &pipeline.Verifier{Source: src, Target: dst, SourceDSN: srcDSN, TargetDSN: dstDSN, Out: &buf}
	res, err := v.Run(ctx)
	if err != nil {
		t.Fatalf("verify %s → %s: %v\n%s", srcEngine, dstEngine, err, buf.String())
	}
	if res.HasMismatch() {
		t.Fatalf("verify %s → %s found mismatches:\n%s", srcEngine, dstEngine, buf.String())
	}
}

// TestMariaDB_MigrateCorpus_BothDirections_BothLTS is the full-corpus
// migrate + verify pin over {mariadb→mysql8, mysql8→mariadb} × {11.4,
// 10.11}. The mysql8→mariadb-10.11 leg additionally exercises the
// utf8mb4_0900_* → utf8mb4_uca1400_* collation remap live (10.11
// rejects the 0900 names outright, so a green CREATE is the pin) —
// every string column of the default-collation MySQL 8 source carries
// utf8mb4_0900_ai_ci.
func TestMariaDB_MigrateCorpus_BothDirections_BothLTS(t *testing.T) {
	for _, image := range []string{mariadb114Image, mariadb1011Image} {
		image := image
		t.Run(image, func(t *testing.T) {
			t.Run("mariadb_to_mysql8", func(t *testing.T) {
				srcDSN := newMariaDB(t, image, "mdb_corpus_src")
				execSQLScript(t, srcDSN, corpusDDL)
				dstDSN, _ := newSharedDB(t, "mdb_corpus_dst_my8")

				runMigrate(t, "mariadb", srcDSN, "mysql", dstDSN)
				assertCorpusEqual(t, srcDSN, dstDSN)
				runVerify(t, "mariadb", srcDSN, "mysql", dstDSN)
			})
			t.Run("mysql8_to_mariadb", func(t *testing.T) {
				srcDSN, _ := newSharedDB(t, "my8_corpus_src")
				execSQLScript(t, srcDSN, corpusDDL)
				dstDSN := newMariaDB(t, image, "mdb_corpus_dst")

				runMigrate(t, "mysql", srcDSN, "mariadb", dstDSN)
				assertCorpusEqual(t, srcDSN, dstDSN)
				runVerify(t, "mysql", srcDSN, "mariadb", dstDSN)

				// The collation-remap pin: the MySQL 8 source's string
				// columns carry utf8mb4_0900_ai_ci; the MariaDB target
				// must hold a uca1400 (10.11) or 0900/uca1400 (11.4)
				// collation — and on 10.11 the remap is the ONLY way the
				// CREATE succeeded at all.
				db, err := sql.Open("mysql", dstDSN)
				if err != nil {
					t.Fatalf("open target: %v", err)
				}
				defer func() { _ = db.Close() }()
				var coll string
				q := "SELECT collation_name FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'corpus' AND column_name = 'vc'"
				if err := db.QueryRow(q).Scan(&coll); err != nil {
					t.Fatalf("read target collation: %v", err)
				}
				if coll != "utf8mb4_uca1400_ai_ci" {
					t.Errorf("target vc collation = %q; want utf8mb4_uca1400_ai_ci (remapped from utf8mb4_0900_ai_ci)", coll)
				}
			})
		})
	}
}

// TestMariaDB_BackupRestoreRoundTrip pins backup full FROM mariadb →
// restore INTO a fresh mariadb database, corpus-ground-truthed.
func TestMariaDB_BackupRestoreRoundTrip(t *testing.T) {
	srcDSN := newMariaDB(t, mariadb114Image, "mdb_bkup_src")
	execSQLScript(t, srcDSN, corpusDDL)
	dstDSN := newMariaDB(t, mariadb114Image, "mdb_bkup_dst")

	eng, ok := engines.Get("mariadb")
	if !ok {
		t.Fatal("mariadb engine not registered")
	}
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	bk := &backup.Backup{Source: eng, SourceDSN: srcDSN, Store: store, SluiceVersion: "test-mariadb"}
	if err := bk.Run(ctx); err != nil {
		t.Fatalf("backup full from mariadb: %v", err)
	}
	rs := &backup.Restore{Target: eng, TargetDSN: dstDSN, Store: store}
	if err := rs.Run(ctx); err != nil {
		t.Fatalf("restore into mariadb: %v", err)
	}
	assertCorpusEqual(t, srcDSN, dstDSN)
}

// TestMariaDB_SyncStart_CodedRefusal pins the `sync start` shape: a
// real Streamer against a live mariadb source refuses with
// SLUICE-E-CDC-MARIADB-UNSUPPORTED before opening anything.
func TestMariaDB_SyncStart_CodedRefusal(t *testing.T) {
	srcDSN := newMariaDB(t, mariadb114Image, "mdb_sync_src")
	execSQLScript(t, srcDSN, "CREATE TABLE t1 (id INT PRIMARY KEY);")
	dstDSN, _ := newSharedDB(t, "mdb_sync_dst")

	src, _ := engines.Get("mariadb")
	dst, _ := engines.Get("mysql")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s := &pipeline.Streamer{Source: src, SourceDSN: srcDSN, Target: dst, TargetDSN: dstDSN}
	err := s.Run(ctx)
	if err == nil {
		t.Fatal("sync start from mariadb succeeded; want the coded CDC refusal")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeCDCMariaDBUnsupported {
		t.Fatalf("sync start error = %v; want code %s", err, sluicecode.CodeCDCMariaDBUnsupported)
	}
}

// TestMariaDB_DeclaredMySQL_WarnsThenFailsLoudly pins the steering
// shape when a MariaDB server is driven as plain `mysql`: the schema-
// reader open WARNs toward --source-driver/--target-driver mariadb,
// and the read then fails loudly on the srs_id wall (the probe's
// leg-1 error) — never silently mis-reads.
func TestMariaDB_DeclaredMySQL_WarnsThenFailsLoudly(t *testing.T) {
	dsn := newMariaDB(t, mariadb114Image, "mdb_as_mysql")
	execSQLScript(t, dsn, "CREATE TABLE t1 (id INT PRIMARY KEY, v VARCHAR(10));")

	buf := captureSlogWarn(t)
	eng, _ := engines.Get("mysql")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	sr, err := eng.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader (vanilla on mariadb) should open, WARN, and fail later: %v", err)
	}
	defer func() { _ = sr.(interface{ Close() error }).Close() }()

	if !strings.Contains(buf.String(), "mariadb") || !strings.Contains(buf.String(), "driver") {
		t.Errorf("expected the mariadb-driver steering WARN; got log: %s", buf.String())
	}

	_, err = sr.ReadSchema(ctx)
	if err == nil {
		t.Fatal("vanilla ReadSchema against mariadb succeeded; want the loud srs_id wall")
	}
	if !strings.Contains(err.Error(), "srs_id") {
		t.Errorf("vanilla ReadSchema error = %v; want the srs_id wall", err)
	}
}

// TestMariaDB_FlavorOnMySQLServer_Refused pins the reverse guard: the
// mariadb flavor pointed at a real MySQL 8 server refuses (coded) —
// its defaults shim would otherwise mis-read MySQL conventions.
func TestMariaDB_FlavorOnMySQLServer_Refused(t *testing.T) {
	dsn, _ := newSharedDB(t, "my8_as_mariadb")
	eng, _ := engines.Get("mariadb")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, err := eng.OpenSchemaReader(ctx, dsn)
	if err == nil {
		t.Fatal("mariadb flavor opened a MySQL 8 server; want the fingerprint refusal")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeDriverHostMismatch {
		t.Fatalf("error = %v; want code %s", err, sluicecode.CodeDriverHostMismatch)
	}
}

// TestMariaDB_InvisibleTables_RefusedLoudly pins the census guard:
// SEQUENCE and SYSTEM VERSIONED objects refuse the schema read by
// name instead of silently vanishing behind the BASE TABLE filter.
func TestMariaDB_InvisibleTables_RefusedLoudly(t *testing.T) {
	dsn := newMariaDB(t, mariadb114Image, "mdb_invisible")
	execSQLScript(t, dsn, `
		CREATE TABLE plain (id INT PRIMARY KEY);
		CREATE SEQUENCE seq1;
		CREATE TABLE versioned (id INT PRIMARY KEY, x INT) WITH SYSTEM VERSIONING;`)

	eng, _ := engines.Get("mariadb")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	sr, err := eng.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() { _ = sr.(interface{ Close() error }).Close() }()
	_, err = sr.ReadSchema(ctx)
	if err == nil {
		t.Fatal("ReadSchema succeeded despite SEQUENCE + SYSTEM VERSIONED objects; want the census refusal")
	}
	// The census names each object with its class and a per-class remedy
	// (item 73 Phase 2 kept both a loud refusal — see ADR-0169).
	for _, want := range []string{
		"seq1", "versioned", "SEQUENCE", "SYSTEM VERSIONED",
		"DROP SYSTEM VERSIONING", // versioned-table remedy
		"PG target",              // sequence remedy
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("census refusal missing %q: %v", want, err)
		}
	}
}

// TestMariaDB_GeometrySRID_ReadBack is the item-73 Phase-2 item-4
// ground-truth pin: a geometry column declared with a non-zero SRID must
// read back with the CORRECT SRID — recovered from
// information_schema.GEOMETRY_COLUMNS.SRID (NOT REF_SYSTEM_ID from SHOW
// CREATE, which MariaDB does not echo), not 0 — the exact silent-loss
// the Phase-1 geometry exclusion avoided. Pins the geometry FAMILY
// (POINT / LINESTRING / POLYGON) × {SRID 0, SRID 4326}, not one
// representative (Bug-74 discipline), on BOTH supported LTS lines
// (mariadb:11.4 and mariadb:10.11 — ADR-0169 §4 claims both verified).
func TestMariaDB_GeometrySRID_ReadBack(t *testing.T) {
	for _, image := range []string{mariadb114Image, mariadb1011Image} {
		image := image
		t.Run(image, func(t *testing.T) {
			dsn := newMariaDB(t, image, "mdb_geom_srid")
			execSQLScript(t, dsn, `
				CREATE TABLE geo (
					id          INT NOT NULL PRIMARY KEY,
					p_none      POINT,
					p_4326      POINT      REF_SYSTEM_ID=4326,
					ls_none     LINESTRING,
					ls_4326     LINESTRING REF_SYSTEM_ID=4326,
					poly_none   POLYGON,
					poly_4326   POLYGON    REF_SYSTEM_ID=4326
				);`)

			got := readGeoColumns(t, dsn)
			want := map[string]ir.Geometry{
				"p_none":    {Subtype: ir.GeometryPoint, SRID: 0},
				"p_4326":    {Subtype: ir.GeometryPoint, SRID: 4326},
				"ls_none":   {Subtype: ir.GeometryLineString, SRID: 0},
				"ls_4326":   {Subtype: ir.GeometryLineString, SRID: 4326},
				"poly_none": {Subtype: ir.GeometryPolygon, SRID: 0},
				"poly_4326": {Subtype: ir.GeometryPolygon, SRID: 4326},
			}
			for name, wantGeom := range want {
				col, ok := got[name]
				if !ok {
					t.Errorf("column %q missing from mariadb read", name)
					continue
				}
				geom, ok := col.Type.(ir.Geometry)
				if !ok {
					t.Errorf("column %q type = %#v; want ir.Geometry", name, col.Type)
					continue
				}
				if geom != wantGeom {
					t.Errorf("column %q geometry = %#v; want %#v (SRID from GEOMETRY_COLUMNS)", name, geom, wantGeom)
				}
			}
		})
	}
}

// readGeoColumns reads the `geo` table's columns through the mariadb
// engine's full production read path.
func readGeoColumns(t *testing.T, dsn string) map[string]*ir.Column {
	t.Helper()
	eng, ok := engines.Get("mariadb")
	if !ok {
		t.Fatal("mariadb engine not registered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	sr, err := eng.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() { _ = sr.(interface{ Close() error }).Close() }()
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	for _, tbl := range schema.Tables {
		if tbl.Name == "geo" {
			out := map[string]*ir.Column{}
			for _, c := range tbl.Columns {
				out[c.Name] = c
			}
			return out
		}
	}
	t.Fatal("geo table not found")
	return nil
}

// TestMariaDB_NativeIdentityToMySQL8 pins item-73 Phase-2 item-2 on the
// same-family cross-engine path: MariaDB's native uuid / inet6 / inet4
// columns migrate to a MySQL 8 target as CHAR(36) / VARCHAR(45) (the
// established auto-emit), values round-tripping losslessly — the exact
// "lossless carry" the capability comment claims. INET4 and INET6 both
// land as VARCHAR(45) (they collapse to ir.Inet). NULLs stay NULL.
func TestMariaDB_NativeIdentityToMySQL8(t *testing.T) {
	srcDSN := newMariaDB(t, mariadb114Image, "mdb_native_src")
	execSQLScript(t, srcDSN, `
		CREATE TABLE ids (
			id     INT NOT NULL PRIMARY KEY,
			uid    UUID   NOT NULL,
			uid_n  UUID,
			ip6    INET6,
			ip4    INET4
		);
		INSERT INTO ids (id, uid, uid_n, ip6, ip4) VALUES
			(1, '123e4567-e89b-12d3-a456-426614174000', 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee', '2001:db8::1', '192.168.1.10'),
			(2, '223e4567-e89b-12d3-a456-426614174000', NULL, NULL, NULL);`)
	dstDSN, _ := newSharedDB(t, "mdb_native_dst_my8")

	runMigrate(t, "mariadb", srcDSN, "mysql", dstDSN)

	db, err := sql.Open("mysql", dstDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Target column types: uuid → CHAR(36), inet6/inet4 → VARCHAR(45).
	wantType := map[string]string{
		"uid":   "char(36)",
		"uid_n": "char(36)",
		"ip6":   "varchar(45)",
		"ip4":   "varchar(45)",
	}
	for col, want := range wantType {
		var ct string
		if err := db.QueryRowContext(ctx, `SELECT column_type FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'ids' AND column_name = ?`, col).Scan(&ct); err != nil {
			t.Fatalf("read %s type: %v", col, err)
		}
		if ct != want {
			t.Errorf("target %s column_type = %q; want %q (MariaDB native type → MySQL-family auto-emit)", col, ct, want)
		}
	}
	// Values round-trip (and NULLs stay NULL).
	var uid, ip6, ip4 string
	if err := db.QueryRowContext(ctx, "SELECT uid, ip6, ip4 FROM ids WHERE id = 1").Scan(&uid, &ip6, &ip4); err != nil {
		t.Fatalf("read row 1: %v", err)
	}
	if uid != "123e4567-e89b-12d3-a456-426614174000" {
		t.Errorf("uid = %q; want the canonical uuid", uid)
	}
	if ip6 != "2001:db8::1" {
		t.Errorf("ip6 = %q; want 2001:db8::1", ip6)
	}
	if ip4 != "192.168.1.10" {
		t.Errorf("ip4 = %q; want 192.168.1.10", ip4)
	}
	var uidN, ip6N sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT uid_n, ip6 FROM ids WHERE id = 2").Scan(&uidN, &ip6N); err != nil {
		t.Fatalf("read row 2: %v", err)
	}
	if uidN.Valid || ip6N.Valid {
		t.Errorf("row 2 NULLs did not round-trip: uid_n=%v ip6=%v", uidN, ip6N)
	}
}

// TestMariaDB_JSONIdentity_ReadBack is the item-73 Phase-2 item-1 pin: a
// MariaDB `JSON` column (a LONGTEXT alias with an auto json_valid CHECK)
// reads back as ir.JSON{Binary:false} with that auto-CHECK STRIPPED from
// the IR, while a plain LONGTEXT column stays ir.Text and a user's own
// json_valid-bearing CHECK is preserved. Ground-truthed on mariadb:11.4.
func TestMariaDB_JSONIdentity_ReadBack(t *testing.T) {
	dsn := newMariaDB(t, mariadb114Image, "mdb_json_identity")
	execSQLScript(t, dsn, `
		CREATE TABLE jt (
			id    INT NOT NULL PRIMARY KEY,
			js    JSON,
			blurb LONGTEXT,
			doc   LONGTEXT,
			CONSTRAINT doc_ck CHECK (json_valid(doc) AND length(doc) > 2)
		);`)

	eng, _ := engines.Get("mariadb")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	sr, err := eng.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() { _ = sr.(interface{ Close() error }).Close() }()
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	var jt *ir.Table
	for _, tbl := range schema.Tables {
		if tbl.Name == "jt" {
			jt = tbl
		}
	}
	if jt == nil {
		t.Fatal("jt table not found")
	}
	byName := map[string]*ir.Column{}
	for _, c := range jt.Columns {
		byName[c.Name] = c
	}

	if got, ok := byName["js"].Type.(ir.JSON); !ok || got.Binary {
		t.Errorf("js type = %#v; want ir.JSON{Binary:false}", byName["js"].Type)
	}
	if txt, ok := byName["blurb"].Type.(ir.Text); !ok || txt.Size != ir.TextLong {
		t.Errorf("blurb type = %#v; want ir.Text{TextLong} (plain longtext)", byName["blurb"].Type)
	}
	if txt, ok := byName["doc"].Type.(ir.Text); !ok || txt.Size != ir.TextLong {
		t.Errorf("doc type = %#v; want ir.Text{TextLong} (complex user CHECK, not remapped)", byName["doc"].Type)
	}
	// The js auto json_valid CHECK must be stripped; the doc_ck user CHECK
	// must survive.
	var checkNames []string
	for _, cc := range jt.CheckConstraints {
		checkNames = append(checkNames, cc.Name)
		if strings.EqualFold(cc.Expr, "json_valid(js)") {
			t.Errorf("the js auto json_valid CHECK was not stripped: %+v", cc)
		}
	}
	foundDocCk := false
	for _, n := range checkNames {
		if n == "doc_ck" {
			foundDocCk = true
		}
	}
	if !foundDocCk {
		t.Errorf("user CHECK doc_ck was lost; remaining checks: %v", checkNames)
	}
}

// TestMariaDB_CheckConstraintFanout_Bug198 pins the check-constraint
// join fan-out fix on BOTH LTS lines. MariaDB constraint names are unique
// only per-table, so two tables sharing a check-constraint name fanned
// the check_constraints↔table_constraints join out — each table captured
// every same-named CHECK once per sharing table (and cross-contaminated:
// table a could capture table b's CHECK). The table_name-disambiguated
// join restores the 1:1.
//
// Two shapes: (1) two tables with a same-named NON-JSON user CHECK — the
// crisp fan-out pin: each captured EXACTLY ONCE (2 without the fix), so a
// same-engine migrate emits no within-table duplicate (Error 1826); (2)
// two tables sharing a JSON column named `meta` — MariaDB's json_valid
// auto-CHECK is named after the column, so this is the ubiquitous
// trigger; each meta is detected as JSON with its auto-CHECK stripped and
// no cross-contaminating leftover, and a cross-engine migrate to MySQL 8
// succeeds.
func TestMariaDB_CheckConstraintFanout_Bug198(t *testing.T) {
	for _, image := range []string{mariadb114Image, mariadb1011Image} {
		image := image
		t.Run(image, func(t *testing.T) {
			t.Run("nonjson_same_named_check", func(t *testing.T) {
				dsn := newMariaDB(t, image, "mdb_cc_nonjson")
				// Same constraint name `ck_pos` on BOTH tables (legal on
				// MariaDB — per-table names).
				execSQLScript(t, dsn, `
					CREATE TABLE c (id INT PRIMARY KEY, qty INT, CONSTRAINT ck_pos CHECK (qty > 0));
					CREATE TABLE d (id INT PRIMARY KEY, qty INT, CONSTRAINT ck_pos CHECK (qty > 0));`)

				tables := readTablesViaMariaDB(t, dsn)
				for _, name := range []string{"c", "d"} {
					tbl := tables[name]
					if tbl == nil {
						t.Fatalf("table %q not read", name)
					}
					if got := len(tbl.CheckConstraints); got != 1 {
						t.Errorf("table %q captured %d CHECKs; want exactly 1 (fan-out would give 2)", name, got)
					}
				}
				// Same-engine migrate: a within-table duplicate CHECK would
				// fail CREATE TABLE (Error 1826). Clean 1:1 → succeeds.
				dstDSN := newMariaDB(t, image, "mdb_cc_nonjson_dst")
				runMigrate(t, "mariadb", dsn, "mariadb", dstDSN)
			})

			t.Run("json_same_named_column", func(t *testing.T) {
				dsn := newMariaDB(t, image, "mdb_cc_json")
				execSQLScript(t, dsn, `
					CREATE TABLE a (id INT PRIMARY KEY, meta JSON);
					CREATE TABLE b (id INT PRIMARY KEY, meta JSON);`)

				tables := readTablesViaMariaDB(t, dsn)
				for _, name := range []string{"a", "b"} {
					tbl := tables[name]
					if tbl == nil {
						t.Fatalf("table %q not read", name)
					}
					var meta *ir.Column
					for _, col := range tbl.Columns {
						if col.Name == "meta" {
							meta = col
						}
					}
					if meta == nil {
						t.Fatalf("table %q missing meta column", name)
					}
					if _, ok := meta.Type.(ir.JSON); !ok {
						t.Errorf("table %q meta type = %#v; want ir.JSON", name, meta.Type)
					}
					if got := len(tbl.CheckConstraints); got != 0 {
						t.Errorf("table %q has %d CHECKs; want 0 (json_valid auto-CHECK stripped, no cross-contamination): %+v", name, got, tbl.CheckConstraints)
					}
				}
				// Cross-engine migrate to MySQL 8: the stripped json_valid
				// CHECKs mean no invalid CHECK emit; a clean run is the pin.
				dstDSN, _ := newSharedDB(t, "mdb_cc_json_dst_my8")
				runMigrate(t, "mariadb", dsn, "mysql", dstDSN)
			})
		})
	}
}

// readTablesViaMariaDB reads the full schema through the mariadb engine
// and returns its tables keyed by name.
func readTablesViaMariaDB(t *testing.T, dsn string) map[string]*ir.Table {
	t.Helper()
	eng, _ := engines.Get("mariadb")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	sr, err := eng.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() { _ = sr.(interface{ Close() error }).Close() }()
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	out := map[string]*ir.Table{}
	for _, tbl := range schema.Tables {
		out[tbl.Name] = tbl
	}
	return out
}

// TestMariaDB_UpsertSpelling_ExecutesLive executes the VALUES()-form
// statements — the applier single-row upsert, the position write, and
// the migrate-state upsert — against a real MariaDB server (the
// row-alias forms all 1064 there; a green exec is the pin) and
// asserts upsert semantics (second write updates, not duplicates).
func TestMariaDB_UpsertSpelling_ExecutesLive(t *testing.T) {
	dsn := newMariaDB(t, mariadb114Image, "mdb_upsert")
	execSQLScript(t, dsn, "CREATE TABLE t_ups (id INT PRIMARY KEY, v INT);")

	eng, _ := engines.Get("mariadb")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Applier + control table via the full production open path.
	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() { _ = applier.(interface{ Close() error }).Close() }()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Single-row applier upsert, twice with the same PK — the second
	// exec must UPDATE via ON DUPLICATE KEY UPDATE … VALUES(…).
	for _, v := range []int{1, 2} {
		stmt, args, err := buildInsertSQL("mdb_upsert", "t_ups", ir.Row{"id": 10, "v": v}, []string{"id"}, nil, upsertValuesFunc)
		if err != nil {
			t.Fatalf("buildInsertSQL: %v", err)
		}
		if _, err := db.ExecContext(ctx, stmt, args...); err != nil {
			t.Fatalf("exec applier upsert (v=%d): %v\nstmt: %s", v, err, stmt)
		}
	}
	var n, v int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*), MAX(v) FROM t_ups").Scan(&n, &v); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if n != 1 || v != 2 {
		t.Errorf("after double upsert: count=%d v=%d; want 1 row with v=2", n, v)
	}

	// Position write, twice (the second is the conflict path).
	for i, tok := range []string{"tok-1", "tok-2"} {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		if err := writePositionTx(ctx, tx, "", "mdb-stream", tok, "", "", "", int64(i), upsertValuesFunc); err != nil {
			t.Fatalf("writePositionTx (%s): %v", tok, err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}
	var tok string
	if err := db.QueryRowContext(ctx, "SELECT source_position FROM sluice_cdc_state WHERE stream_id = 'mdb-stream'").Scan(&tok); err != nil {
		t.Fatalf("read position: %v", err)
	}
	if tok != "tok-2" {
		t.Errorf("position after conflict-path write = %q; want tok-2", tok)
	}

	// Migrate-state store, twice (header + progress upserts).
	store, err := eng.(interface {
		OpenMigrationStateStore(context.Context, string) (ir.MigrationStateStore, error)
	}).OpenMigrationStateStore(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenMigrationStateStore: %v", err)
	}
	defer func() { _ = store.(interface{ Close() error }).Close() }()
	if err := store.EnsureControlTable(ctx); err != nil {
		t.Fatalf("migrate-state EnsureControlTable: %v", err)
	}
	for _, phase := range []ir.MigrationPhase{ir.MigrationPhaseBulkCopy, ir.MigrationPhaseIndexes} {
		st := ir.MigrationState{
			MigrationID: "mdb-mig",
			Phase:       phase,
			TableProgress: map[string]ir.TableProgress{
				"t_ups": {State: ir.TableProgressComplete},
			},
		}
		if err := store.Write(ctx, st); err != nil {
			t.Fatalf("migrate-state Write (%s): %v", phase, err)
		}
	}
	st, found, err := store.Read(ctx, "mdb-mig")
	if err != nil || !found {
		t.Fatalf("migrate-state Read: ok=%v err=%v", found, err)
	}
	if st.Phase != ir.MigrationPhaseIndexes {
		t.Errorf("migrate-state phase after conflict-path write = %q; want indexes", st.Phase)
	}
}
