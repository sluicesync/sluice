//go:build integration && vitessreshard

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0120 live A/B: does relaxing vtgate's MinimizeSkew
// (--vstream-relax-skew / source-DSN vstream_relax_skew=true) let BOTH
// shards of a multi-shard VStream drain concurrently under an
// apply-deficit backlog, instead of vtgate holding the ahead shard back?
//
// This is the A/B the ADR's Validation section gates the default flip on
// ("Live A/B (REQUIRED before flipping the default)"). It is a LOGGED
// measurement, NOT a hard timing assertion — the only HARD assertions are
// the exactly-once correctness oracles in BOTH runs (relaxing skew must
// not lose or duplicate anything). The throughput delta is reported.
//
// TOPOLOGY / HARNESS: reuses the multi-process vitess/lite reshard cluster
// + helpers from vitess_cluster_reshard_integration_test.go
// (startVitessReshardCluster, addTargetShards, waitReshardRunning,
// vtctldExec, vrApplySQL, vrShowShards, vrAsInt64). Source = a 2-shard
// (-80 / 80-, hash VINDEX) keyspace produced by resharding 1 -> 2 during
// setup. The A/B then runs on the steady-state 2-shard cluster — the
// reshard is setup, NOT mid-stream (ADR-0120 measures the steady-state CDC
// stream, scope §1: "only the steady-state CDC stream").
//
// MEASUREMENT DESIGN (why this shape):
//
//   - MinimizeSkew is a vtgate DELIVERY-side flag, so the cleanest place to
//     observe it is the reader's delivery, not a downstream target. This
//     drives the PRODUCTION reader (eng.OpenCDCReader → *vstreamCDCReader)
//     directly from "current" (CDC-only, no cold-copy), exactly as the
//     sibling TestVitessReshard_ChaosExactlyOnce does, and the A/B is the
//     source DSN WITHOUT vs WITH vstream_relax_skew=true. The reader reads
//     that param at open (vstreamRelaxSkewFromDSN); we assert the toggle
//     took effect (reader.relaxSkew + buildVStreamRequest MinimizeSkew).
//
//   - The apply-deficit is induced WITHOUT toxiproxy or a throttled target:
//     a fast continuous cross-shard writer (thousands/s) feeds vtgate while
//     a RATE-THROTTLED consumer (the test collector, ~400 rows/s) drains.
//     Consumer << producer ⇒ the reader's bounded channel fills ⇒ gRPC recv
//     backpressures ⇒ vtgate's MinimizeSkew governs how the merged stream
//     is delivered. This is the in-process equivalent of the ADR's
//     "toxiproxy bandwidth toxic OR --apply-concurrency 1 plus a fast
//     writer" backlog recipe.
//
//   - PER-SHARD progress is measured by bucketing each delivered id to its
//     shard. The emitted ir.Insert carries NO shard (ADR-0120 §2: "ir.Change
//     carries no shard"), and the reader's per-shard currentVgtid is mutated
//     by the pump goroutine (reading it from the test goroutine would race —
//     and CI runs this with -race). So shard membership is derived race-free
//     AFTER the run via vtgate "keyspace:shard" targeting (range-sharding
//     fixes each (table,PK) to exactly one shard, so membership is stable).
//     The per-shard delivered-count time series over the throttled window is
//     the ADR's "per-shard advancement" signal: run A (skew on) is expected
//     ASYMMETRIC (one shard frozen while its peer drains — the hold), run B
//     (skew off) is expected to advance BOTH shards concurrently.
//
//   - CORRECTNESS (load-bearing, hard-asserted in BOTH runs): the delivered
//     stream == the committed source set, exactly-once — every committed id
//     delivered (no gap), at most a small boundary dup (no dup), and every
//     delivered payload matches its id (no value corruption). This is the
//     same delivered-vs-committed oracle ChaosExactlyOnce uses; here "dst"
//     is the delivered stream rather than a materialized target, which keeps
//     the two A/B runs cleanly independent on one cluster (no cross-run
//     cold-copy pollution) while still proving the relaxation is lossless.
//
// WHY TIMING IS NOT HARD-ASSERTED: the hold is a timing-sensitive vtgate
// behaviour on a single-host local cluster; per ADR-0120 Consequences item
// 27 is a "catch-up-latency win, not a steady-state one", and the prompt
// mandates a logged measurement plus (if robust) a generous directional
// check — not a brittle timing gate. The directional signal logged is the
// fraction of 1s windows in which BOTH shards advanced (expected higher
// under skew relaxed) and the longest per-shard frozen streak (expected
// longer under skew on). The exactly-once oracles are the hard gate.

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// relaxAB collects the measured outcome of one A/B run.
type relaxAB struct {
	relax bool

	shardNames []string // discovered shard layout (expected 2)

	committed         int // ids the writer COMMITTed to the source
	deliveredDistinct int // distinct ids the stream delivered
	dupExcess         int // sum over committed of (deliveredCount-1)
	missing           int // committed ids never delivered (no gap ⇒ 0)
	valueMismatch     int // delivered payload != p-<id> (no corruption ⇒ 0)

	// Per-shard delivered counts DURING the throttled measure window.
	perShardWindow map[string]int
	// 1s-window directional signal over the throttled measure window.
	bothAdvancedWindows int
	totalWindows        int
	// Longest run of consecutive 1s windows in which a shard delivered 0
	// while its peer delivered >0 (the "hold" signature, per shard).
	maxFrozenStreak map[string]int

	// Per-shard COMMITTED counts (membership, whole run) — the 2-shard
	// exercise guard.
	perShardCommitted map[string]int

	convergeDur time.Duration // writer-stop → delivered==committed

	// backlog is committed-minus-delivered at the moment the writer stops
	// (the apply-deficit the full-speed drain then has to absorb). Populated
	// by the magnitude-sweep runner; the legacy runRelaxSkewScenario leaves
	// it zero.
	backlog int

	// noConverge is set by the magnitude-sweep runner when a HELD
	// (MinimizeSkew ON) run could not drain to delivered==committed within the
	// budget because vtgate's skew buffer held the ahead shard's tail once the
	// writer went quiet (the measured hold pathology — the stream stays alive
	// with heartbeats and the rows remain on the source; it is NOT sluice
	// loss). Relaxed runs never set this (they must converge — the win).
	noConverge bool
}

// TestVitessReshard_RelaxSkewConcurrentDrainAB is the ADR-0120 live A/B.
// One resharded 2-shard cluster; run A (skew on) then run B (skew off),
// each from "current" on a disjoint id range so the two runs are
// independent. Logs the per-shard A/B numbers; hard-asserts exactly-once
// in both runs.
func TestVitessReshard_RelaxSkewConcurrentDrainAB(t *testing.T) {
	c := startVitessReshardCluster(t, "-")
	defer c.terminate()

	// --- source schema: hash-vindexed table on the 1-shard keyspace ---
	vrApplySQL(t, c.mysqlDSN, `CREATE TABLE acct (
		id      BIGINT      NOT NULL,
		payload VARCHAR(64) NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	vrApplySQL(t, c.mysqlDSN, `ALTER VSCHEMA ON `+vrKeyspace+`.acct ADD VINDEX hash(id) USING hash`)
	time.Sleep(3 * time.Second)

	// Seed a few baseline rows so the reshard copy phase has data (matches
	// the ProofOfReshardability shape). ids 1..8 are pre-stream and in a
	// range disjoint from both run id-bases, so neither A/B stream (opened
	// at "current") ever delivers them.
	vrApplySQL(t, c.mysqlDSN+"&multiStatements=true",
		`INSERT INTO acct (id, payload) VALUES `+
			`(1,'p-1'),(2,'p-2'),(3,'p-3'),(4,'p-4'),(5,'p-5'),(6,'p-6'),(7,'p-7'),(8,'p-8');`)
	time.Sleep(2 * time.Second)

	// --- RESHARD 1 -> 2 (setup; the A/B runs on the steady-state result) ---
	c.addTargetShards(t, "-80", "80-")
	if out, rerr := c.vtctldExec(t, "Reshard", "create",
		"--workflow", "relaxab", "--target-keyspace", vrKeyspace,
		"--source-shards", "-", "--target-shards", "-80,80-"); rerr != nil {
		t.Fatalf("Reshard create: %v\n%s", rerr, out)
	}
	c.waitReshardRunning(t, "relaxab")
	if _, rerr := c.vtctldExec(t, "Reshard", "SwitchTraffic",
		"--workflow", "relaxab", "--target-keyspace", vrKeyspace); rerr != nil {
		t.Fatalf("Reshard SwitchTraffic: %v", rerr)
	}
	shards := vrShowShards(t, c.mysqlDSN)
	assertReshardTargetShardsPresent(t, "SETUP", shards)
	t.Logf("SETUP: resharded 1 -> 2; vtgate shards now %v", shards)

	// Wait through the post-SwitchTraffic "no healthy tablet for PRIMARY"
	// window before the A/B opens a CDC reader / burst-writes to `acct`
	// (else the first op races the cutover and fails 1105 — the CI-only flake).
	c.waitReshardPrimariesRoutable(t, "acct")

	// --- the A/B: same scenario, skew OFF then skew ON ---
	runA := runRelaxSkewScenario(t, c, false, 100_000_000)
	runB := runRelaxSkewScenario(t, c, true, 200_000_000)

	logRelaxABComparison(t, runA, runB)
}

// reshardTargetShards are the two serving shards a 1->2 reshard of the single
// "-" source keyspace produces.
var reshardTargetShards = []string{"-80", "80-"}

// assertReshardTargetShardsPresent asserts both post-reshard serving shards
// (-80, 80-) are in the discovered set, WITHOUT asserting an exact count.
//
// Why not `== 2`: after SwitchTraffic, sluice's shard auto-discovery is the
// v0.99.195 union of SHOW VITESS_SHARDS and SHOW VITESS_TABLETS(SERVING). The
// DRAINED pre-reshard source shard "-" is dropped from SHOW VITESS_SHARDS but
// its tablets are STILL SERVING in the immediate post-SwitchTraffic window
// (traffic switches at routing; the source tablets aren't torn down until the
// workflow completes), so the tablet cross-check legitimately re-includes "-" —
// the discovered set is [- -80 80-]. Discovery deliberately keeps it: it cannot
// distinguish a drained source shard from a genuinely-serving shard that SHOW
// VITESS_SHARDS wrongly omitted (the exact under-reporting bug v0.99.195 fixed),
// and dropping the latter is a silent-partial-copy risk. The reshard-follow
// oracles (TestVitessReshard_StreamerFollowsReshardEndToEnd + the sweep ORACLE
// block) prove streaming this 3-shard set is exactly-once — no gap, no dup — in
// this window. So the A/B precondition is "both new serving shards present", not
// "exactly two shards"; the harmless drained "-" is tolerated.
func assertReshardTargetShardsPresent(t *testing.T, label string, shards []string) {
	t.Helper()
	set := make(map[string]struct{}, len(shards))
	for _, s := range shards {
		set[s] = struct{}{}
	}
	for _, want := range reshardTargetShards {
		if _, ok := set[want]; !ok {
			t.Fatalf("%s: discovered shards %v missing post-reshard serving shard %q — "+
				"A/B needs both new shards streamed", label, shards, want)
		}
	}
}

// runRelaxSkewScenario executes one half of the A/B and hard-asserts
// exactly-once. relax selects the source-DSN vstream_relax_skew param;
// idBase is the disjoint id range this run's writer uses.
func runRelaxSkewScenario(t *testing.T, c *vitessReshardCluster, relax bool, idBase int64) relaxAB {
	t.Helper()
	label := "A(skew ON / relax=false)"
	if relax {
		label = "B(skew OFF / relax=true)"
	}
	t.Logf("=== RUN %s starting (idBase=%d) ===", label, idBase)

	const (
		measureWindow   = 30 * time.Second // throttled-drain measurement window
		throttleTick    = 50 * time.Millisecond
		perTick         = 20                // ⇒ ~400 delivered rows/s consumer rate
		writerBatch     = 25                // rows per multi-row INSERT
		numWriters      = 3                 // concurrent cross-shard writers
		drainTimeout    = 150 * time.Second // full-speed tail drain budget
		boundaryDupTol  = 50                // generous; no reshard here ⇒ expect ~0
		minCommitted    = 1000              // below this the test isn't exercising the deficit
		minPerShardSeed = 50                // each shard must hold a non-trivial share
	)

	// Source DSN: CDC from "current" with shard auto-discovery. Relaxed skew is
	// now the DEFAULT (ADR-0120 flipped); the preserve-skew opt-out param is set
	// only on the non-relaxed arm. The reader reads vstream_preserve_skew at open.
	// vstream_progress_timeout=300s (> the throttled drainTimeout): this A/B skew
	// test throttles the CONSUMER to measure per-shard skew, backpressuring the
	// source past the 45s default liveness window — without this the reader
	// correctly reconnects mid-measurement and the test flags an unclean teardown
	// (test-only; a genuine >300s hang is still caught).
	sourceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_auto_discover_shards=true&vstream_progress_timeout=300s",
		c.mysqlDSN, c.grpcAddr,
	)
	if !relax {
		sourceDSN += "&vstream_preserve_skew=true"
	}

	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, sourceDSN)
	if err != nil {
		t.Fatalf("%s: OpenCDCReader: %v", label, err)
	}
	cdcRdr, ok := rdr.(*vstreamCDCReader)
	if !ok {
		t.Fatalf("%s: OpenCDCReader returned %T; want *vstreamCDCReader", label, rdr)
	}
	defer func() { _ = cdcRdr.Close() }()

	// Guard: the toggle actually took effect (the load-bearing mechanism).
	// This is a pure reader-field check (independent of shard discovery), so it
	// stays before StreamChanges.
	if cdcRdr.relaxSkew != relax {
		t.Fatalf("%s: reader.relaxSkew = %v; want %v (DSN param did not take effect)", label, cdcRdr.relaxSkew, relax)
	}

	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("%s: StreamChanges: %v", label, err)
	}
	time.Sleep(3 * time.Second) // let the stream register at "current"

	// Shard auto-discovery is deferred to stream-open (product commit 8f82b30e
	// moved it out of OpenCDCReader so reader construction stays
	// connection-free), so cdcRdr.shards is empty until StreamChanges runs.
	// Assert the post-reshard serving shards are discovered and pin the
	// MinimizeSkew request AFTER StreamChanges has populated r.shards.
	assertReshardTargetShardsPresent(t, label, cdcRdr.shards)
	// Pin that the request carries MinimizeSkew == !relax for this run.
	if req, berr := cdcRdr.buildVStreamRequest(fromNowVStreamPos(cdcRdr.keyspace, cdcRdr.shards)); berr != nil {
		t.Fatalf("%s: buildVStreamRequest: %v", label, berr)
	} else if got := req.GetFlags().GetMinimizeSkew(); got != !relax {
		t.Fatalf("%s: request MinimizeSkew = %v; want %v", label, got, !relax)
	}

	// ---- collector state (mutex-guarded; read after the collector exits) ----
	type tlEntry struct {
		elapsed float64
		id      int64
	}
	var (
		mu            sync.Mutex
		delivered     = make(map[int64]int)
		distinct      int
		valueMismatch int
		timeline      []tlEntry
	)
	var phase atomic.Int32 // 0 = throttle (measure), 1 = full (drain tail)
	var closedEarly atomic.Bool
	measureStart := time.Now()

	distinctNow := func() int { mu.Lock(); defer mu.Unlock(); return distinct }

	recordEvent := func(ev ir.Change) {
		ins, isIns := ev.(ir.Insert)
		if !isIns {
			return
		}
		id, ok := vrAsInt64(ins.Row["id"])
		if !ok {
			return
		}
		mu.Lock()
		prev := delivered[id]
		delivered[id] = prev + 1
		if prev == 0 {
			distinct++
			if pv, _ := ins.Row["payload"].(string); pv != fmt.Sprintf("p-%d", id) {
				valueMismatch++
			}
		}
		if phase.Load() == 0 {
			timeline = append(timeline, tlEntry{elapsed: time.Since(measureStart).Seconds(), id: id})
		}
		mu.Unlock()
	}

	collCtx, collCancel := context.WithCancel(ctx)
	collectorDone := make(chan struct{})
	go func() {
		defer close(collectorDone)
		ticker := time.NewTicker(throttleTick)
		defer ticker.Stop()
		for {
			if phase.Load() == 0 {
				// THROTTLE: at most perTick reads per tick (consumer rate cap).
				select {
				case <-collCtx.Done():
					return
				case <-ticker.C:
				}
				for n := 0; n < perTick; {
					select {
					case <-collCtx.Done():
						return
					case ev, alive := <-changes:
						if !alive {
							closedEarly.Store(true)
							return
						}
						recordEvent(ev)
						n++
					default:
						n = perTick // channel momentarily empty; wait for next tick
					}
				}
			} else {
				// FULL: drain the tail as fast as it arrives.
				select {
				case <-collCtx.Done():
					return
				case ev, alive := <-changes:
					if !alive {
						closedEarly.Store(true)
						return
					}
					recordEvent(ev)
				}
			}
		}
	}()

	// ---- fast continuous cross-shard writer (the deficit's producer) ----
	// The writer is ONLY a producer; it does NOT record what it wrote. The
	// oracle's ground truth is the SOURCE itself (queried after the writer
	// stops): a stopWriter()-cancelled INSERT can commit server-side yet
	// return context.Canceled to the client, so the writer's own tally would
	// undercount the rows that actually landed in the binlog (and which the
	// stream therefore delivers). Reading the committed set back from the
	// source eliminates that accounting race entirely. ids are striped
	// (idBase+w, +numWriters, …) so the writers never collide on the PK.
	writerCtx, stopWriter := context.WithCancel(ctx)
	var writerWG sync.WaitGroup
	for w := 0; w < numWriters; w++ {
		writerWG.Add(1)
		go func(w int) {
			defer writerWG.Done()
			db, derr := sql.Open("mysql", c.mysqlDSN)
			if derr != nil {
				t.Errorf("%s: writer %d open: %v", label, w, derr)
				return
			}
			defer func() { _ = db.Close() }()
			id := idBase + int64(w)
			for {
				select {
				case <-writerCtx.Done():
					return
				default:
				}
				var sb strings.Builder
				sb.WriteString("INSERT INTO acct (id, payload) VALUES ")
				for b := 0; b < writerBatch; b++ {
					if b > 0 {
						sb.WriteByte(',')
					}
					fmt.Fprintf(&sb, "(%d,'p-%d')", id, id)
					id += int64(numWriters)
				}
				if _, e := db.ExecContext(writerCtx, sb.String()); e != nil {
					if errors.Is(e, context.Canceled) {
						return
					}
					// Transient (cutover settle, deadlock): keep going.
					continue
				}
			}
		}(w)
	}

	// THROTTLED MEASURE WINDOW: writer fast, consumer capped ⇒ growing
	// deficit; vtgate's MinimizeSkew (on/off) governs per-shard delivery.
	time.Sleep(measureWindow)

	// Stop the writer, then read the AUTHORITATIVE committed set back from the
	// source (every row the stream must deliver exactly-once).
	stopWriter()
	writerWG.Wait()
	time.Sleep(1 * time.Second) // let any in-flight commit settle on the source

	maxID := sourceMaxID(t, c, idBase)
	hi := maxID + 1
	srcCount := sourceRangeCount(t, c, idBase, hi)
	if srcCount < minCommitted {
		collCancel()
		<-collectorDone
		t.Fatalf("%s: source committed only %d rows in %s; deficit not exercised (need >= %d) — local cluster too slow",
			label, srcCount, measureWindow, minCommitted)
	}
	t.Logf("%s: source committed=%d during measure window; delivered-so-far(distinct)=%d (backlog≈%d)",
		label, srcCount, distinctNow(), srcCount-distinctNow())

	// FULL-SPEED TAIL DRAIN to convergence (the oracle needs the whole source
	// set delivered — drain target is the source count, not a writer tally).
	phase.Store(1)
	convergeStart := time.Now()
	deadline := time.Now().Add(drainTimeout)
	for distinctNow() < srcCount && !closedEarly.Load() && time.Now().Before(deadline) {
		time.Sleep(250 * time.Millisecond)
	}
	convergeDur := time.Since(convergeStart)
	collCancel()
	<-collectorDone

	if closedEarly.Load() {
		if e := cdcRdr.Err(); e != nil && !errors.Is(e, context.Canceled) && !errors.Is(e, context.DeadlineExceeded) {
			t.Fatalf("%s: stream closed early with error (not a clean teardown): %v", label, e)
		}
	}

	// ---- derive the committed set + per-shard membership from the SOURCE ----
	// (race-free; range-sharding ⇒ each id is on exactly one shard.)
	committed := make(map[int64]struct{}, srcCount)
	idToShard := make(map[int64]string)
	perShardCommitted := make(map[string]int)
	for _, sh := range cdcRdr.shards {
		ids := shardScopedIDs(t, c, sh, idBase, hi)
		for _, id := range ids {
			committed[id] = struct{}{}
			idToShard[id] = sh
			perShardCommitted[sh]++
		}
	}

	// ---- compute the per-shard windowed signal ----
	perShardWindow := make(map[string]int)
	// bins[shard][secondBin] = delivered count in that 1s window.
	bins := make(map[string]map[int]int)
	for _, sh := range cdcRdr.shards {
		bins[sh] = make(map[int]int)
	}
	maxBin := 0
	mu.Lock()
	tl := append([]tlEntry(nil), timeline...)
	mu.Unlock()
	for _, e := range tl {
		sh, ok := idToShard[e.id]
		if !ok {
			continue // id not yet visible on a shard query (rare race) — skip
		}
		perShardWindow[sh]++
		bin := int(e.elapsed)
		bins[sh][bin]++
		if bin > maxBin {
			maxBin = bin
		}
	}

	bothAdvanced, totalWindows := 0, 0
	maxFrozenStreak := make(map[string]int)
	curStreak := make(map[string]int)
	for bin := 0; bin <= maxBin; bin++ {
		anyActivity := false
		allAdvanced := true
		for _, sh := range cdcRdr.shards {
			if bins[sh][bin] > 0 {
				anyActivity = true
			} else {
				allAdvanced = false
			}
		}
		if !anyActivity {
			continue // idle window (e.g. pre-roll) — not counted
		}
		totalWindows++
		if allAdvanced {
			bothAdvanced++
		}
		// Frozen-streak per shard: this shard 0 while a peer >0.
		for _, sh := range cdcRdr.shards {
			peerActive := false
			for _, other := range cdcRdr.shards {
				if other != sh && bins[other][bin] > 0 {
					peerActive = true
				}
			}
			if bins[sh][bin] == 0 && peerActive {
				curStreak[sh]++
				if curStreak[sh] > maxFrozenStreak[sh] {
					maxFrozenStreak[sh] = curStreak[sh]
				}
			} else {
				curStreak[sh] = 0
			}
		}
	}

	// ---- exactly-once oracle (HARD — relaxing skew must not lose/dup) ----
	mu.Lock()
	missing := 0
	dupExcess := 0
	for id := range committed {
		switch cnt := delivered[id]; {
		case cnt == 0:
			missing++
		case cnt > 1:
			dupExcess += cnt - 1
		}
	}
	distinctFinal := distinct
	valueMismatchFinal := valueMismatch
	mu.Unlock()

	res := relaxAB{
		relax:               relax,
		shardNames:          append([]string(nil), cdcRdr.shards...),
		committed:           len(committed),
		deliveredDistinct:   distinctFinal,
		dupExcess:           dupExcess,
		missing:             missing,
		valueMismatch:       valueMismatchFinal,
		perShardWindow:      perShardWindow,
		bothAdvancedWindows: bothAdvanced,
		totalWindows:        totalWindows,
		maxFrozenStreak:     maxFrozenStreak,
		perShardCommitted:   perShardCommitted,
		convergeDur:         convergeDur,
	}

	// 2-shard exercise guard: both shards must hold a non-trivial committed share.
	for _, sh := range cdcRdr.shards {
		if perShardCommitted[sh] < minPerShardSeed {
			t.Fatalf("%s: shard %q holds only %d committed ids (< %d) — run not exercising both shards (per-shard committed: %v)",
				label, sh, perShardCommitted[sh], minPerShardSeed, perShardCommitted)
		}
	}

	// HARD assertions (the load-bearing correctness check, both runs):
	if missing > 0 {
		t.Fatalf("%s: EXACTLY-ONCE GAP: %d/%d committed ids were NEVER delivered (relaxSkew=%v lost rows)",
			label, missing, len(committed), relax)
	}
	if valueMismatchFinal > 0 {
		t.Fatalf("%s: VALUE CORRUPTION: %d delivered payloads != p-<id> (relaxSkew=%v corrupted values)",
			label, valueMismatchFinal, relax)
	}
	if dupExcess > boundaryDupTol {
		t.Fatalf("%s: EXACTLY-ONCE DUP: %d duplicate deliveries (> %d tolerance) with relaxSkew=%v",
			label, dupExcess, boundaryDupTol, relax)
	}
	if srcCount != len(committed) {
		t.Fatalf("%s: source range COUNT=%d != committed=%d — source lost/gained rows (test-setup issue, not a streamer verdict)",
			label, srcCount, len(committed))
	}

	t.Logf("%s: EXACTLY-ONCE PASSED: committed=%d delivered-distinct=%d dupExcess=%d (<=%d) valueMismatch=0 srcCount=%d — no gap, no dup, no corruption with relaxSkew=%v",
		label, len(committed), distinctFinal, dupExcess, boundaryDupTol, srcCount, relax)
	t.Logf("%s: PER-SHARD committed=%v  windowDelivered=%v  bothAdvancedWindows=%d/%d  maxFrozenStreak=%v  convergeAfterStop=%s",
		label, perShardCommitted, perShardWindow, bothAdvanced, totalWindows, maxFrozenStreak, convergeDur)

	return res
}

// shardScopedIDs returns the ids in [lo,hi) that live on shard sh, read via
// vtgate "keyspace:shard" targeting (range-sharding ⇒ each id is on exactly
// one shard, so this is a stable membership query).
func shardScopedIDs(t *testing.T, c *vitessReshardCluster, sh string, lo, hi int64) []int64 {
	t.Helper()
	dsn := strings.Replace(c.mysqlDSN, "/"+vrKeyspace+"?", "/"+vrKeyspace+":"+sh+"?", 1)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open shard %q: %v", sh, err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	// A run commits tens of thousands of rows per shard, which exceeds
	// vttablet's default 10000-row result cap (Error 1317 "Row count
	// exceeded 10000"). `set workload='olap'` switches the session to the
	// streaming executor (no row cap) — the pscale-dumper trick noted in
	// CLAUDE.md — so the membership scan returns the full set. Pinned to one
	// *sql.Conn so the directive and the SELECT share a session.
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("shard %q conn: %v", sh, err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, "set workload='olap'"); err != nil {
		t.Fatalf("shard %q set workload=olap: %v", sh, err)
	}
	rows, err := conn.QueryContext(ctx, "SELECT id FROM acct WHERE id >= ? AND id < ?", lo, hi)
	if err != nil {
		t.Fatalf("shard %q select ids: %v", sh, err)
	}
	defer func() { _ = rows.Close() }()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("shard %q scan: %v", sh, err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("shard %q rows: %v", sh, err)
	}
	return out
}

// sourceMaxID returns MAX(id) of acct rows with id >= lo via vtgate (scatter).
// Used to bound the committed range after the writer stops; returns lo-1 when
// the range is empty so [lo, maxID+1) collapses to empty.
func sourceMaxID(t *testing.T, c *vitessReshardCluster, lo int64) int64 {
	t.Helper()
	db, err := sql.Open("mysql", c.mysqlDSN)
	if err != nil {
		t.Fatalf("open source for max id: %v", err)
	}
	defer func() { _ = db.Close() }()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		var maxID sql.NullInt64
		qerr := db.QueryRowContext(ctx, "SELECT MAX(id) FROM acct WHERE id >= ?", lo).Scan(&maxID)
		cancel()
		if qerr != nil {
			time.Sleep(2 * time.Second)
			continue
		}
		if !maxID.Valid {
			return lo - 1
		}
		return maxID.Int64
	}
	t.Fatalf("sourceMaxID: timed out querying MAX(id)")
	return 0
}

// sourceRangeCount returns the scatter COUNT(*) of acct rows in [lo,hi)
// across all shards via vtgate.
func sourceRangeCount(t *testing.T, c *vitessReshardCluster, lo, hi int64) int {
	t.Helper()
	db, err := sql.Open("mysql", c.mysqlDSN)
	if err != nil {
		t.Fatalf("open source for range count: %v", err)
	}
	defer func() { _ = db.Close() }()
	deadline := time.Now().Add(60 * time.Second)
	last := -1
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		var n int
		qerr := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM acct WHERE id >= ? AND id < ?", lo, hi).Scan(&n)
		cancel()
		if qerr != nil {
			time.Sleep(2 * time.Second)
			continue
		}
		last = n
		return last
	}
	return last
}

// logRelaxABComparison emits the side-by-side A/B summary the ADR-0120 A/B
// gate reads. Directional only — no hard assertion on timing.
func logRelaxABComparison(t *testing.T, a, b relaxAB) {
	t.Helper()
	fracA := windowFraction(a.bothAdvancedWindows, a.totalWindows)
	fracB := windowFraction(b.bothAdvancedWindows, b.totalWindows)
	t.Logf("================ ADR-0120 A/B SUMMARY ================")
	t.Logf("RUN A (MinimizeSkew ON,  relax=false): committed=%d delivered=%d perShardWindow=%v bothAdvanced=%d/%d (%.0f%%) maxFrozenStreak=%v convergeAfterStop=%s",
		a.committed, a.deliveredDistinct, a.perShardWindow, a.bothAdvancedWindows, a.totalWindows, fracA*100, a.maxFrozenStreak, a.convergeDur)
	t.Logf("RUN B (MinimizeSkew OFF, relax=true ): committed=%d delivered=%d perShardWindow=%v bothAdvanced=%d/%d (%.0f%%) maxFrozenStreak=%v convergeAfterStop=%s",
		b.committed, b.deliveredDistinct, b.perShardWindow, b.bothAdvancedWindows, b.totalWindows, fracB*100, b.maxFrozenStreak, b.convergeDur)
	t.Logf("EXPECTATION: run B both-shards-advanced fraction >= run A (relaxed skew lets both shards drain concurrently);")
	t.Logf("            run A max frozen streak >= run B (MinimizeSkew holds the ahead shard ⇒ longer freezes).")
	t.Logf("OBSERVED   : both-advanced A=%.0f%% B=%.0f%% (Δ=%+.0f pts);  maxFrozenStreak A=%v B=%v",
		fracA*100, fracB*100, (fracB-fracA)*100, a.maxFrozenStreak, b.maxFrozenStreak)
	if fracB >= fracA {
		t.Logf("DIRECTION  : CONSISTENT with ADR-0120 (relaxed >= held for concurrent drain).")
	} else {
		t.Logf("DIRECTION  : NOT reproduced on this local single-host cluster — the hold is timing-sensitive; see test header. Correctness held in BOTH runs regardless.")
	}
	t.Logf("=====================================================")
}

func windowFraction(num, den int) float64 {
	if den == 0 {
		return 0
	}
	return float64(num) / float64(den)
}
