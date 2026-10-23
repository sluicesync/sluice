//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"fmt"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/logcapture"
)

// TestStreamer_VStreamSlotNameKeepsTheCopyScope pins
// VSTREAM-SLOT-NAME-UNSCOPED-COPY on a real vttestserver: a PlanetScale
// cold start given --slot-name (which VStream has no use for) must copy
// exactly what it copies without it. Before the fix the name sent the open
// down the no-table path, one keyspace-wide stream, so the copy skipped the
// --where push-down (rows outside the filter landed) and, with a table
// copied after the in-scope ones (sn_x, outside --include-table), the
// catch-up before it replayed the in-copy deletes as copy-phase rows and
// dropped them.
//
// The verdict compares the target with the source rows the filter admits,
// after the hand-off and a marker written behind the changes; it never
// consults sluice's own state.
func TestStreamer_VStreamSlotNameKeepsTheCopyScope(t *testing.T) {
	var logs logcapture.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	mysqlDSN, grpcEndpoint, _, cleanupSrc := startShardedVTTestServer(t, "commerce", 1)
	defer cleanupSrc()
	tgtDSN, cleanupTgt := startPGTarget(t)
	defer cleanupTgt()
	r := vcrRig{
		mysqlDSN: mysqlDSN,
		streamDSN: fmt.Sprintf("%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
			mysqlDSN, grpcEndpoint),
		tgtDSN: tgtDSN,
		logs:   &logs,
	}

	// sn_a then sn_b in scope (copied in name order), sn_x after them and
	// out of scope.
	const a, b, x = "sn_a", "sn_b", "sn_x"
	const floor = 100 // the --where: id > 100
	vcrCreate(t, r, a, b, x)
	vcrSeed(t, r.mysqlDSN, a, "v1", vcrRange(1, 1, 1000))
	vcrSeed(t, r.mysqlDSN, b, "v1", vcrRange(1, 1, vrsBigRows))
	vcrSeed(t, r.mysqlDSN, x, "v1", vcrRange(1, 1, 1000))
	time.Sleep(3 * time.Second) // the schema tracker

	s := r.streamer("vsn", []string{a, b}, "")
	s.SlotName = "sluice_vsn"
	s.RowFilters = map[string]string{a: fmt.Sprintf("id > %d", floor), b: fmt.Sprintf("id > %d", floor)}
	logStart := r.logs.Len()
	run := startTWFBRun(s)
	defer func() { _ = run.stop(t) }()

	// The changes land once sn_a is on the target, while sn_b copies.
	deadline := time.Now().Add(8 * time.Minute)
	for {
		if run.exited() || time.Now().After(deadline) {
			vcrLogTail(t, r, logStart)
			t.Fatalf("RIG: the copy exited or timed out before %s was on the target (%v)", a, run.err)
		}
		if n, _ := strconv.Atoi(r.tgt().scalar(t, "SELECT count(*) FROM "+a)); n >= 1000-floor {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	nb, _ := strconv.Atoi(r.tgt().scalar(t, "SELECT count(*) FROM "+b))
	t.Logf("changes start with %s on the target and %d rows of %s", a, nb, b)
	for _, stmt := range []string{
		"DELETE FROM " + a + " WHERE id BETWEEN 201 AND 220",
		"UPDATE " + a + " SET payload = CONCAT('upd-', id) WHERE id BETWEEN 301 AND 310",
		"DELETE FROM " + b + " WHERE id BETWEEN 101 AND 120",
		"DELETE FROM " + b + " WHERE id BETWEEN 150001 AND 150020",
		"UPDATE " + b + " SET payload = CONCAT('upd-', id) WHERE id BETWEEN 160001 AND 160010",
	} {
		r.src().exec(t, stmt)
	}

	cell := twfbCell{tgt: r.tgt(), streamID: s.StreamID}
	for cell.persistedPosition(t) == "" {
		if run.exited() || time.Now().After(deadline) {
			vcrLogTail(t, r, logStart)
			t.Fatalf("the cold start never handed off to CDC (%v)", run.err)
		}
		time.Sleep(250 * time.Millisecond)
	}
	for _, tbl := range []string{a, b} {
		vcrSeed(t, r.mysqlDSN, tbl, "marker", []int64{9_000_001})
	}
	for _, tbl := range []string{a, b} {
		if !r.tgt().waitRow(t, tbl, 9_000_001, run, 120*time.Second) {
			vcrLogTail(t, r, logStart)
			t.Fatalf("%s's marker never reached the target (%v)", tbl, run.err)
		}
	}
	time.Sleep(2 * time.Second)

	for _, tbl := range []string{a, b} {
		src, tgt := vrsRows(t, r.src(), tbl), vrsRows(t, r.tgt(), tbl)
		var missing, stale, extra, belowFloor int
		for id, sv := range src {
			if id <= floor {
				continue
			}
			if tv, ok := tgt[id]; !ok {
				missing++
			} else if tv != sv {
				stale++
			}
		}
		for id := range tgt {
			if id <= floor {
				belowFloor++
			} else if _, ok := src[id]; !ok {
				extra++
			}
		}
		verdict := fmt.Sprintf("%s: target %d rows; missing=%d stale-values=%d deleted-but-present=%d outside-the-where=%d",
			tbl, len(tgt), missing, stale, extra, belowFloor)
		if missing+stale+extra+belowFloor != 0 {
			t.Errorf("SILENT DIVERGENCE with --slot-name: %s", verdict)
		} else {
			t.Logf("EXACT: %s", verdict)
		}
	}
	if rows := vrsRows(t, r.tgt(), x); rows != nil {
		t.Errorf("%s, outside --include-table, exists on the target with %d rows", x, len(rows))
	}
}
