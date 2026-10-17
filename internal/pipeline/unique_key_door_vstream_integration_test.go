//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// UNIQUE-KEY-STOPPED-WINDOW-DOOR on a VStream (PlanetScale / Vitess flavor)
// source, on a real vttestserver. VStream decodes against FIELD events and
// is exempt from the binlog's unforwarded-class door, so a source DROP
// INDEX reaches the stream as nothing it acts on (VSTREAM-UNIQUE-KEY-DDL).
// Two cells: the stopped window, refused at stream open; and a mid-stream
// DROP, refused by the periodic re-check (period shortened to one second)
// before the duplicate it precedes is applied. Graded on the source's rows
// against the target's, through ukdRows.
//
// Usage (Windows; see CLAUDE.md for the Rancher Desktop env):
//
//	go test -tags='integration vstream' -count=1 -timeout=30m \
//	  -run 'TestStreamer_UniqueKeyDoor_VStream' ./internal/pipeline/

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestStreamer_UniqueKeyDoor_VStream(t *testing.T) {
	const keyspace = "commerce"
	mysqlDSN, grpcEndpoint, _, cleanupSrc := startShardedVTTestServer(t, keyspace, 1)
	defer cleanupSrc()
	targetDSN, cleanupTgt := startPGTarget(t)
	defer cleanupTgt()

	// src reads and writes through vtgate; the stream's DSN carries the
	// VStream endpoint (twfbDB.exec strips it for statements).
	src := twfbDB{engine: "planetscale", dsn: fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0", mysqlDSN, grpcEndpoint,
	)}
	srcRows := twfbDB{engine: "mysql", dsn: mysqlDSN}
	tgt := twfbDB{engine: "postgres", dsn: targetDSN}

	src.exec(t, "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) NOT NULL, v VARCHAR(20), UNIQUE KEY kd_k (k))")
	src.exec(t, "INSERT INTO kd VALUES (1,'a','one'),(2,'b','two')")
	time.Sleep(3 * time.Second) // the schema tracker

	cell := twfbCell{src: src, tgt: tgt, streamID: "ukdv"}
	run := startTWFBRun(cell.streamer())
	if !tgt.waitRow(t, "kd", 2, run, 180*time.Second) {
		t.Fatalf("the cold start never delivered the seed (%v)", run.stop(t))
	}
	cell.waitStreaming(t, run)
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("the clean stop returned %v", err)
	}

	t.Run("stopped window refused at open", func(t *testing.T) {
		before := ukdRows(t, tgt)
		src.exec(t, "ALTER TABLE kd DROP INDEX kd_k")
		src.exec(t, "INSERT INTO kd VALUES (99,'a','ninetynine')")
		time.Sleep(3 * time.Second)
		run := startTWFBRun(cell.streamer())
		ukdAwaitExitOrConverge(t, run, srcRows, tgt, 120*time.Second)
		refusal := ukdRequireRefusal(t, run, tgt, before, "kd.kd_k", ir.UniqueKeyDoorAtOpen)
		if refusal.SourceSeesDDLLive {
			t.Errorf("a VStream source was classified as seeing unique-key DDL live")
		}
		// Stopped across the DROP: drop the key on the target, start again.
		tgt.exec(t, "DROP INDEX kd_k")
		ukdConvergeAndResume(t, cell.streamer(), srcRows, tgt, "INSERT INTO kd VALUES (5,'e','five')")
	})

	t.Run("mid-stream DROP refused by the re-check before the duplicate", func(t *testing.T) {
		// A second key, live on both sides, then dropped mid-stream.
		src.exec(t, "ALTER TABLE kd ADD UNIQUE KEY kd_v (v)")
		tgt.exec(t, "CREATE UNIQUE INDEX kd_v ON kd (v)")
		time.Sleep(3 * time.Second)
		s := cell.streamer()
		s.uniqueKeyRecheckEvery = time.Second
		run := startTWFBRun(s)
		src.exec(t, "INSERT INTO kd VALUES (50,'p','fifty')")
		if !tgt.waitRow(t, "kd", 50, run, 90*time.Second) {
			t.Fatalf("the priming row never landed (%v)", run.stop(t))
		}
		before := ukdRows(t, tgt)
		src.exec(t, "ALTER TABLE kd DROP INDEX kd_v")
		time.Sleep(4 * time.Second)
		src.exec(t, "INSERT INTO kd VALUES (98,'z','fifty')")
		ukdAwaitExitOrConverge(t, run, srcRows, tgt, 90*time.Second)
		refusal := ukdRequireRefusal(t, run, tgt, before, "kd.kd_v", ir.UniqueKeyDoorMidStream)
		if !strings.Contains(refusal.Error(), "--reset-target-data --yes") {
			t.Errorf("the mid-stream refusal does not print the clean re-copy: %v", refusal)
		}
	})
}
