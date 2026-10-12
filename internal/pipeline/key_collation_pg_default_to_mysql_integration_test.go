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
// The same policy covers a NONDETERMINISTIC Postgres key (review 2026-10-12,
// finding 1): an ICU `ks-level2` key keeps 'e' and 'é' apart, a level-3 one
// 'a' and 'A', and the MySQL emitter — which cannot declare an ICU collation
// — creates both under `utf8mb4_0900_ai_ci`, which equates each pair.
//
// The independent expected value is the Postgres source's own rows read by
// id:HEX(k).

package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestKeyCollationFirstBoundary_PostgresDefaultUniqueToMySQL(t *testing.T) {
	kcpdRunCell(t, "kcpd", "", "varchar(20)", "a", "A")
}

// TestKeyCollationFirstBoundary_PostgresNondeterministicUniqueToMySQL is
// finding 1's apply half: before the fix the second source row merged into
// the first at exit 0.
func TestKeyCollationFirstBoundary_PostgresNondeterministicUniqueToMySQL(t *testing.T) {
	t.Run("level2 e/é", func(t *testing.T) {
		kcpdRunCell(t, "kcn2", "CREATE COLLATION l2 (provider = icu, locale = 'und-u-ks-level2', deterministic = false)",
			"varchar(20) COLLATE l2", "e", "é")
	})
	t.Run("level3 a/A", func(t *testing.T) {
		kcpdRunCell(t, "kcn3", "CREATE COLLATION l3 (provider = icu, locale = 'und-u-ks-level3', deterministic = false)",
			"varchar(20) COLLATE l3", "a", "A")
	})
}

// kcpdRunCell streams a Postgres table whose UNIQUE key (beside a primary
// key) is keyType into MySQL, seeds seedK as row 1 and then inserts the
// source-distinct collidingK as row 99, and requires KEY-COLLATION-MERGE
// with the target exact by HEX(k).
func kcpdRunCell(t *testing.T, tag, collationDDL, keyType, seedK, collidingK string) {
	srcRoot, _, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()
	_, myRoot, myCleanup := startMySQL(t)
	defer myCleanup()
	twfbDB{engine: "postgres", dsn: srcRoot}.exec(t, "CREATE DATABASE "+tag+"_s")
	srcDSN, err := buildPGDSN(srcRoot, tag+"_s")
	if err != nil {
		t.Fatal(err)
	}
	twfbDB{engine: "mysql", dsn: myRoot}.exec(t, "CREATE DATABASE "+tag+"_t")
	tgtDSN, err := buildMySQLDSN(myRoot, tag+"_t")
	if err != nil {
		t.Fatal(err)
	}
	src, tgt := twfbDB{engine: "postgres", dsn: srcDSN}, twfbDB{engine: "mysql", dsn: tgtDSN}
	cell := twfbCell{src: src, tgt: tgt, streamID: tag, slotName: tag}
	if collationDDL != "" {
		src.exec(t, collationDDL)
	}
	src.exec(t, "CREATE TABLE kd (id int NOT NULL PRIMARY KEY, k "+keyType+" NOT NULL UNIQUE, d int); "+
		"ALTER TABLE kd REPLICA IDENTITY FULL; INSERT INTO kd VALUES (1,'"+seedK+"',1),(2,'b',2),(3,'c',3)")
	run := startTWFBRun(cell.streamer())
	if !tgt.waitRow(t, "kd", 3, run, 180*time.Second) {
		t.Fatalf("the cold start never delivered the seed (%v)", run.stop(t))
	}
	cell.waitStreaming(t, run)
	src.exec(t, "INSERT INTO kd VALUES (99,'"+collidingK+"',99)")
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
		t.Fatalf("nothing refused: the target's UNIQUE key equates %q with %q, so row 99 cannot land exactly "+
			"(target %s)", seedK, collidingK, kcfbRender(kcfbRows(t, tgt)))
	}
	if !errors.Is(refusal, ir.ErrKeyCollationMerge) {
		t.Errorf("refused with %v; want %v", refusal, ir.ErrKeyCollationMerge)
	}
	kcfbGrade(t, tag, src, tgt, refusal)
}
