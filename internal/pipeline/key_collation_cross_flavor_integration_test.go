//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// MySQL 8 `utf8mb4_0900_ai_ci` keys (NO PAD) streaming into MariaDB, end to
// end. The source keeps 'a' and 'a ' apart.
//
// KEY-COLLATION review F-A: through v0.159.0 the cold start created the
// target column as `utf8mb4_uca1400_ai_ci`, the emitter's closest
// equivalent — and PAD SPACE, so the target equated the two keys and the
// source's second row was an ON DUPLICATE KEY UPDATE onto the first. The
// applier judged the two collations "the same" by the emitter's remap and
// left the key unguarded: measured on MariaDB 11.4.13, the row silently
// overwrote row 1. The guard now compares under the NO PAD uca1400 forms.
//
// Bug 305: the guard's refusal then told the operator to re-copy, and the
// re-copy rebuilt the same PAD SPACE column — a duplicate-key failure with
// the target left empty (PK) or without its unique index (UNIQUE). The cold
// start now creates the NO PAD form, so a stream sluice creates never meets
// the refusal, and the re-copy remedy converges for a target an older
// sluice created.
//
// The independent expected value is the source's own rows read by
// id:HEX(k) — never the applier's errors. Either the target equals the
// source key for key, or the stream refused and every row the target holds
// is the source's row with the same bytes.

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"sluicesync.dev/sluice/internal/ir"
)

// kcxBootMariaDB boots a MariaDB target of image and returns its root DSN.
// A boot is retried (the failed container terminated): on a Windows host
// Docker can publish the port inside an excluded TCP range, and the SQL
// wait then never connects; a fresh container gets a fresh port.
func kcxBootMariaDB(t *testing.T, image string) (dsn string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	var (
		container testcontainers.Container
		err       error
	)
	for attempt := 1; attempt <= 3; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		container, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        image,
				Env:          map[string]string{"MARIADB_ROOT_PASSWORD": "rootpw", "MARIADB_DATABASE": "target_db"},
				ExposedPorts: []string{"3306/tcp"},
				WaitingFor: wait.ForSQL("3306/tcp", "mysql", func(host string, port network.Port) string {
					return fmt.Sprintf("root:rootpw@tcp(%s:%s)/target_db", host, port.Port())
				}).WithStartupTimeout(2 * time.Minute),
			},
			Started: true,
		})
		cancel()
		if err == nil {
			break
		}
		if container != nil {
			_ = container.Terminate(context.Background())
		}
		t.Logf("boot %s attempt %d: %v", image, attempt, err)
	}
	if err != nil {
		t.Fatalf("boot %s: %v", image, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("host: %v", err)
	}
	port, err := container.MappedPort(ctx, "3306/tcp")
	if err != nil {
		t.Fatalf("port: %v", err)
	}
	return fmt.Sprintf("root:rootpw@tcp(%s:%s)/target_db?parseTime=true", host, port.Port()),
		func() { _ = container.Terminate(context.Background()) }
}

// kcxShape is one key layout of the table: a primary key on k, or k as a
// UNIQUE key beside an id primary key, created on the MySQL source.
type kcxShape struct {
	name, sourceDDL string
}

func kcxShapes() []kcxShape {
	const k = "k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL"
	return []kcxShape{
		{"pk", "CREATE TABLE kd (id INT NOT NULL, " + k + ", d INT, PRIMARY KEY (k))"},
		{"uq", "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, " + k + ", d INT, UNIQUE KEY uk (k))"},
	}
}

// kcxLegacyKey gives the target key the PAD SPACE collation every sluice
// through v0.159.0 created a MySQL 8 `0900_ai_ci` column with.
const kcxLegacyKey = "ALTER TABLE kd MODIFY k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_uca1400_ai_ci NOT NULL"

// kcxSeed is ids 1..3; 'a ' (row 99) is distinct from row 1 only under NO
// PAD.
const kcxSeed = "INSERT INTO kd (id,k,d) VALUES (1,'a',1),(2,'b',2),(3,'c',3)"

// TestKeyCollationFirstBoundary_MySQL84ToMariaDB_PadSpace is the F-A pin,
// and since Bug 305 the remedy pin too: MySQL 8.4 (binlog_row_metadata
// MINIMAL and FULL) into MariaDB 10.11 and 11.4, × a PK and a UNIQUE key,
// onto a target table holding k as PAD SPACE — what every sluice through
// v0.159.0 created, and what an upgraded stream still streams into. 'a'
// then 'a ': the stream must refuse KEY-COLLATION-MERGE with nothing
// merged, its remedy must name the NO PAD collation, and the remedy it
// leads with (--restart-from-scratch) must then converge exactly, the key
// recreated NO PAD.
func TestKeyCollationFirstBoundary_MySQL84ToMariaDB_PadSpace(t *testing.T) {
	srcRoot, srcCleanup := rsfBootMySQL(t, "mysql:8.4")
	defer srcCleanup()
	idx := 0
	for _, image := range []string{"mariadb:10.11", "mariadb:11.4"} {
		tgtRoot, tgtCleanup := kcxBootMariaDB(t, image)
		srcSrv := rsfMySQLFamily(t, "mysql-8.4", "mysql", srcRoot, buildMySQLDSN)
		tgtSrv := rsfMySQLFamily(t, image, "mariadb", tgtRoot, buildMySQLDSN)
		for _, full := range []bool{false, true} {
			meta := "MINIMAL"
			if full {
				meta = "FULL"
			}
			setMeta(t, srcRoot, meta)
			for _, shape := range kcxShapes() {
				idx++
				t.Run(image+"/"+strings.ToLower(meta)+"/"+shape.name, func(t *testing.T) {
					kcxRunLegacyCell(t, srcSrv, tgtSrv, shape, idx)
				})
			}
		}
		tgtCleanup()
	}
}

// TestKeyCollation_MySQL84ToMariaDB_NoPadColdStart is Bug 305's forward
// pin: a stream sluice creates from scratch. MySQL 8.4 into MariaDB 11.4,
// × a PK and a UNIQUE key: the cold start creates k as
// `utf8mb4_uca1400_nopad_ai_ci`, a source that already holds 'a' and 'a '
// copies exactly (this arm failed the cold copy on every release since the
// MariaDB flavor's remap), and an 'a ' arriving over CDC lands as a second
// row, nothing refused.
func TestKeyCollation_MySQL84ToMariaDB_NoPadColdStart(t *testing.T) {
	srcRoot, srcCleanup := rsfBootMySQL(t, "mysql:8.4")
	defer srcCleanup()
	tgtRoot, tgtCleanup := kcxBootMariaDB(t, "mariadb:11.4")
	defer tgtCleanup()
	srcSrv := rsfMySQLFamily(t, "mysql-8.4", "mysql", srcRoot, buildMySQLDSN)
	tgtSrv := rsfMySQLFamily(t, "mariadb:11.4", "mariadb", tgtRoot, buildMySQLDSN)
	idx := 100
	for _, shape := range kcxShapes() {
		for _, preexisting := range []bool{false, true} {
			idx++
			name := shape.name + "/cdc"
			if preexisting {
				name = shape.name + "/copy"
			}
			t.Run(name, func(t *testing.T) {
				tag := fmt.Sprintf("kcx%d", idx)
				src, tgt := srcSrv.newDB(t, "s_"+tag), tgtSrv.newDB(t, "t_"+tag)
				cell := twfbCell{src: src, tgt: tgt, streamID: "kcx-" + tag, slotName: "kcx_" + tag}
				src.exec(t, shape.sourceDDL+"; "+kcxSeed)
				if preexisting {
					src.exec(t, "INSERT INTO kd (id,k,d) VALUES (99,'a ',99)")
				}
				run := startTWFBRun(cell.streamer())
				if !tgt.waitRow(t, "kd", 3, run, 180*time.Second) {
					t.Fatalf("the cold start never delivered the seed (%v)", run.stop(t))
				}
				cell.waitStreaming(t, run)
				if got := kcxKeyCollation(t, tgt); got != "utf8mb4_uca1400_nopad_ai_ci" {
					t.Errorf("the cold start created k as %q; want utf8mb4_uca1400_nopad_ai_ci (NO PAD, as the source's 0900)", got)
				}
				if !preexisting {
					src.exec(t, "INSERT INTO kd (id,k,d) VALUES (99,'a ',99)")
				}
				src.exec(t, "INSERT INTO kd (id,k,d) VALUES (100,'z',100)")
				_ = tgt.waitRow(t, "kd", 100, run, 60*time.Second)
				kcfbWaitConverged(t, src, tgt, run, 20*time.Second)
				if run.exited() {
					t.Fatalf("the stream exited: %v", run.err)
				}
				if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
					t.Errorf("the stream stopped with %v", err)
				}
				kcfbGrade(t, tag, src, tgt, nil)
			})
		}
	}
}

func setMeta(t *testing.T, rootDSN, meta string) {
	t.Helper()
	root := twfbDB{engine: "mysql", dsn: rootDSN}
	root.exec(t, "SET GLOBAL binlog_row_metadata = "+meta)
}

// kcxKeyCollation reads the target key column's collation off the catalog.
func kcxKeyCollation(t *testing.T, db twfbDB) string {
	t.Helper()
	return db.scalar(t, `SELECT COLLATION_NAME FROM information_schema.columns
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'kd' AND COLUMN_NAME = 'k'`)
}

func kcxRunLegacyCell(t *testing.T, srcSrv, tgtSrv rsfServer, shape kcxShape, idx int) {
	tag := fmt.Sprintf("kcx%d", idx)
	src, tgt := srcSrv.newDB(t, "s_"+tag), tgtSrv.newDB(t, "t_"+tag)
	cell := twfbCell{src: src, tgt: tgt, streamID: "kcx-" + tag, slotName: "kcx_" + tag}
	src.exec(t, shape.sourceDDL+"; "+kcxSeed)
	run := startTWFBRun(cell.streamer())
	if !tgt.waitRow(t, "kd", 3, run, 180*time.Second) {
		t.Fatalf("the cold start never delivered the seed (%v)", run.stop(t))
	}
	cell.waitStreaming(t, run)
	// The stream an upgrade inherits: stopped, its target key on the PAD
	// SPACE collation an older sluice created, resumed by this one. (A
	// pre-created PAD SPACE table does not get this far: the cold start's
	// pre-existing-table shape check refuses it.)
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("the clean stop returned %v", err)
	}
	tgt.exec(t, kcxLegacyKey)
	run = startTWFBRun(cell.streamer())
	if got := kcxKeyCollation(t, tgt); got != "utf8mb4_uca1400_ai_ci" {
		t.Fatalf("the legacy target's k is %q; want the PAD SPACE utf8mb4_uca1400_ai_ci — the cell is not exercising the guard", got)
	}
	// The source's NO PAD key keeps these apart; the target's PAD SPACE key
	// does not.
	src.exec(t, "INSERT INTO kd (id,k,d) VALUES (99,'a ',99)")
	src.exec(t, "INSERT INTO kd (id,k,d) VALUES (100,'z',100)")
	_ = tgt.waitRow(t, "kd", 100, run, 60*time.Second)
	kcfbWaitConverged(t, src, tgt, run, 20*time.Second)
	if !run.exited() {
		_ = run.stop(t)
		t.Fatalf("the stream did not refuse the PAD SPACE merge\nsource: %s\ntarget: %s",
			kcfbRender(kcfbRows(t, src)), kcfbRender(kcfbRows(t, tgt)))
	}
	refusal := run.err
	if !errors.Is(refusal, ir.ErrKeyCollationMerge) {
		t.Fatalf("the stream exited with %v; want KEY-COLLATION-MERGE", refusal)
	}
	for _, want := range []string{"COLLATE utf8mb4_uca1400_nopad_ai_ci", "--restart-from-scratch"} {
		if !strings.Contains(refusal.Error(), want) {
			t.Errorf("the refusal's remedy lacks %q: %v", want, refusal)
		}
	}
	kcfbGrade(t, tag, src, tgt, refusal)

	// The remedy the refusal leads with. It must converge exactly, the key
	// recreated NO PAD.
	remedy := cell.streamer()
	remedy.RestartFromScratch = true
	run = startTWFBRun(remedy)
	_ = tgt.waitRow(t, "kd", 100, run, 180*time.Second)
	kcfbWaitConverged(t, src, tgt, run, 60*time.Second)
	if run.exited() {
		t.Fatalf("the --restart-from-scratch remedy failed: %v\nsource: %s\ntarget: %s",
			run.err, kcfbRender(kcfbRows(t, src)), kcfbRender(kcfbRows(t, tgt)))
	}
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("the remedy's stream stopped with %v", err)
	}
	kcfbGrade(t, tag+" (after --restart-from-scratch)", src, tgt, nil)
	if got := kcxKeyCollation(t, tgt); got != "utf8mb4_uca1400_nopad_ai_ci" {
		t.Errorf("after the re-copy k is %q; want utf8mb4_uca1400_nopad_ai_ci", got)
	}
}
