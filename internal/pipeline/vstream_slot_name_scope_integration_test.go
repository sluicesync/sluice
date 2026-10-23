//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
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
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
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

	// The changes land once sn_a is on the target and sn_b's copy has
	// committed at least one batch there, so they fall inside sn_b's copy.
	deadline := time.Now().Add(8 * time.Minute)
	var nb int
	for {
		if run.exited() || time.Now().After(deadline) {
			vcrLogTail(t, r, logStart)
			t.Fatalf("RIG: the copy exited or timed out before %s was on the target and %s had a row there (%v)", a, b, run.err)
		}
		na, _ := strconv.Atoi(r.tgt().scalar(t, "SELECT count(*) FROM "+a))
		nb, _ = strconv.Atoi(r.tgt().scalar(t, "SELECT count(*) FROM "+b))
		if na >= 1000-floor && nb >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if nb < 1 {
		t.Fatalf("RIG: the changes would start before %s had a copied batch on the target", b)
	}
	t.Logf("changes start with %s on the target and %d of %d rows of %s (still copying: %v)",
		a, nb, vrsBigRows-floor, b, nb < vrsBigRows-floor)
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
	// The open named exactly sn_a and sn_b: the per-table copy logs its
	// table count when it starts (sn_a's stream opens with it) and each
	// later table as it advances. The keyspace-wide open logs neither; it
	// streams sn_x too, which the target cannot show, since a table outside
	// the schema scope is never created there.
	opened := r.logs.String()[logStart:]
	if !strings.Contains(opened, "auto-shard-by-table COPY") || !strings.Contains(opened, "tables=2") {
		t.Errorf("the copy did not open per table over the two in-scope tables (no auto-shard line with tables=2): " +
			"the --slot-name open was not scoped to --include-table")
	}
	var advanced []string
	for _, line := range strings.Split(opened, "\n") {
		if !strings.Contains(line, "auto-shard advanced to next table COPY") {
			continue
		}
		if i := strings.Index(line, "table="); i >= 0 {
			advanced = append(advanced, strings.Fields(line[i+len("table="):])[0])
		}
	}
	if !slices.Equal(advanced, []string{b}) {
		t.Errorf("the per-table copy advanced to %v; want exactly [%s] (after %s), never %s", advanced, b, a, x)
	}
}
