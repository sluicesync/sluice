//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// KEY-COLLATION-PG-DEFAULT-TO-MYSQL, operator's choice (b), end to end: a
// Postgres source whose UNIQUE key (beside a primary key) is on the database
// DEFAULT collation — case-sensitive — streams into MySQL, where the cold
// start creates the column under the target's default `utf8mb4_0900_ai_ci`.
// The source keeps 'A' and 'a' apart; the target's UNIQUE key equates them,
// so the source's second row was an ON DUPLICATE KEY UPDATE onto the first.
// The guard is armed for exactly this shape (a UNIQUE key beside a primary
// key); the primary key itself stays unarmed, documented as not covered.
//
// The independent expected value is the Postgres source's own rows read by
// id:HEX(k).

package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestKeyCollationFirstBoundary_PostgresDefaultUniqueToMySQL(t *testing.T) {
	srcRoot, _, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()
	_, myRoot, myCleanup := startMySQL(t)
	defer myCleanup()
	twfbDB{engine: "postgres", dsn: srcRoot}.exec(t, "CREATE DATABASE kcpd_s")
	srcDSN, err := buildPGDSN(srcRoot, "kcpd_s")
	if err != nil {
		t.Fatal(err)
	}
	twfbDB{engine: "mysql", dsn: myRoot}.exec(t, "CREATE DATABASE kcpd_t")
	tgtDSN, err := buildMySQLDSN(myRoot, "kcpd_t")
	if err != nil {
		t.Fatal(err)
	}
	src, tgt := twfbDB{engine: "postgres", dsn: srcDSN}, twfbDB{engine: "mysql", dsn: tgtDSN}
	cell := twfbCell{src: src, tgt: tgt, streamID: "kcpd", slotName: "kcpd"}
	src.exec(t, "CREATE TABLE kd (id int NOT NULL PRIMARY KEY, k varchar(20) NOT NULL UNIQUE, d int); "+
		"ALTER TABLE kd REPLICA IDENTITY FULL; INSERT INTO kd VALUES (1,'a',1),(2,'b',2),(3,'c',3)")
	run := startTWFBRun(cell.streamer())
	if !tgt.waitRow(t, "kd", 3, run, 180*time.Second) {
		t.Fatalf("the cold start never delivered the seed (%v)", run.stop(t))
	}
	cell.waitStreaming(t, run)
	src.exec(t, "INSERT INTO kd VALUES (99,'A',99)")
	src.exec(t, "INSERT INTO kd VALUES (100,'zz',100)")
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
	if refusal == nil {
		t.Fatalf("nothing refused: the target's UNIQUE key equates 'A' with 'a', so row 99 cannot land exactly "+
			"(target %s)", kcfbRender(kcfbRows(t, tgt)))
	}
	kcfbGrade(t, "pg-default-unique-to-mysql", src, tgt, refusal)
}
