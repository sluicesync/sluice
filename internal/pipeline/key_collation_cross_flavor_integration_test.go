//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// KEY-COLLATION review F-A, end to end: a MySQL 8.4 source whose UNIQUE key
// is `utf8mb4_0900_ai_ci` (NO PAD) streams into MariaDB, where the cold
// start creates the column as `utf8mb4_uca1400_ai_ci` — the emitter's
// closest equivalent, and PAD SPACE. The source keeps 'a' and 'a ' apart;
// the target equates them, so the source's second row is an
// ON DUPLICATE KEY UPDATE onto the first. Before the fix the applier judged
// the two collations "the same" by the emitter's remap and left the key
// unguarded: measured on MariaDB 11.4.13, the row silently overwrote row 1.
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
)

// kcxBootMariaDB boots a MariaDB target of image and returns its root DSN.
func kcxBootMariaDB(t *testing.T, image string) (dsn string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        image,
			Env:          map[string]string{"MARIADB_ROOT_PASSWORD": "rootpw", "MARIADB_DATABASE": "target_db"},
			ExposedPorts: []string{"3306/tcp"},
			WaitingFor: wait.ForSQL("3306/tcp", "mysql", func(host string, port network.Port) string {
				return fmt.Sprintf("root:rootpw@tcp(%s:%s)/target_db", host, port.Port())
			}).WithStartupTimeout(4 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("boot %s: %v", image, err)
	}
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

// TestKeyCollationFirstBoundary_MySQL84ToMariaDB_PadSpace is the F-A pin:
// MySQL 8.4 (binlog_row_metadata MINIMAL and FULL) into MariaDB 10.11 and
// 11.4, a `utf8mb4_0900_ai_ci` VARCHAR UNIQUE key, 'a' then 'a '. It must
// refuse or stay exact; a merge is the defect.
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
			idx++
			setMeta(t, srcRoot, meta)
			t.Run(image+"/"+strings.ToLower(meta), func(t *testing.T) {
				kcxRunCell(t, srcSrv, tgtSrv, idx)
			})
		}
		tgtCleanup()
	}
}

func setMeta(t *testing.T, rootDSN, meta string) {
	t.Helper()
	root := twfbDB{engine: "mysql", dsn: rootDSN}
	root.exec(t, "SET GLOBAL binlog_row_metadata = "+meta)
}

func kcxRunCell(t *testing.T, srcSrv, tgtSrv rsfServer, idx int) {
	tag := fmt.Sprintf("kcx%d", idx)
	src, tgt := srcSrv.newDB(t, "s_"+tag), tgtSrv.newDB(t, "t_"+tag)
	cell := twfbCell{src: src, tgt: tgt, streamID: "kcx-" + tag, slotName: "kcx_" + tag}
	src.exec(t, "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, "+
		"k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL, d INT, UNIQUE KEY uk (k)); "+
		"INSERT INTO kd VALUES (1,'a',1),(2,'b',2),(3,'c',3)")
	run := startTWFBRun(cell.streamer())
	if !tgt.waitRow(t, "kd", 3, run, 180*time.Second) {
		t.Fatalf("the cold start never delivered the seed (%v)", run.stop(t))
	}
	cell.waitStreaming(t, run)
	// The source's NO PAD key keeps these apart; the target's PAD SPACE key
	// does not.
	src.exec(t, "INSERT INTO kd VALUES (99,'a ',99)")
	src.exec(t, "INSERT INTO kd VALUES (100,'z',100)")
	_ = tgt.waitRow(t, "kd", 100, run, 60*time.Second)
	kcfbWaitConverged(t, src, tgt, run, 20*time.Second)
	var refusal error
	if run.exited() {
		refusal = run.err
		if refusal == nil || errors.Is(refusal, context.Canceled) {
			t.Fatal("the stream exited without an error")
		}
		t.Logf("refused: %v", refusal)
	} else if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("the stream stopped with %v", err)
	}
	kcfbGrade(t, tag, src, tgt, refusal)
}
