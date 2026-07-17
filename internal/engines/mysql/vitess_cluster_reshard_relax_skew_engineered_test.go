//go:build integration && vitessreshard

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0120 ENGINEERED-SKEW A/B: manufacture genuine cross-shard temporal
// skew so vtgate's MinimizeSkew hold actually FORMS on a single-host
// vitess/lite cluster, then measure the throughput delta when the hold is
// relieved by --vstream-relax-skew (source-DSN vstream_relax_skew=true).
//
// WHY THIS EXISTS (the gap the sibling A/B left open):
//
// TestVitessReshard_RelaxSkewConcurrentDrainAB (same build tags) confirmed
// EXACTLY-ONCE holds under both skew on/off, but the per-shard HOLD did NOT
// reproduce: a fast continuous cross-shard writer commits on BOTH shard
// tablets with near-identical timestamps, so MinimizeSkew has no cross-shard
// temporal skew to act on and both shards drain concurrently regardless of
// the flag. ADR-0120's Validation section records that the documented hold
// (item 23: shard 80- frozen for minutes) is a real cross-region/scale
// phenomenon, and that the local throughput delta needs "either an
// engineered-skew harness or a cross-region source." THIS test is that
// engineered-skew harness.
//
// THE MECHANISM WE EXPLOIT (how MinimizeSkew forms the hold):
//
// vtgate with MinimizeSkew delivers the merged multi-shard stream in
// ~commit-timestamp order, holding back the shard whose next event is NEWER
// until the other shard catches up to that time. So if shard -80's backlog
// is ENTIRELY OLDER (earlier commit timestamps) than shard 80-'s, vtgate
// delivers all of -80 first and FREEZES 80- until -80 drains. A throttled
// consumer widens that freeze window. With MinimizeSkew OFF, both shards
// deliver concurrently regardless of timestamp.
//
// THE ENGINEERED-SKEW PROCEDURE (per A/B run):
//
//  1. (setup, once) Reshard 1 -> 2 (-80, 80-, hash VINDEX), like the
//     sibling A/B. The A/B then runs on the steady-state 2-shard cluster.
//  2. PARTITION ids by shard: insert a probe range, read each shard's
//     tablet directly (keyspace:-80@... / keyspace:80-@..., workload=olap
//     to dodge vttablet's 10k row cap) to learn idsMinus80 / ids80Minus,
//     then DELETE the probe range so the measured run starts clean.
//  3. Open the production reader from "current" (CDC-only). Start a THROTTLE
//     consumer (slow enough the bursts accumulate as an undelivered backlog
//     before they drain).
//  4. Burst-write idsMinus80 (all land -80) at t0 — the OLD backlog. Sleep a
//     real skewGap so the next burst's commit timestamps are distinctly
//     later. Then burst-write ids80Minus (all land 80-) — the NEW backlog.
//  5. Stop writing; let the throttled consumer drain. Sample per-shard
//     delivered progress (bucket each delivered id to its shard via the
//     membership sets from step 2 — race-free, no live read of the reader's
//     per-shard vgtid which the pump goroutine mutates).
//
// MEASURED, A/B (run A: vstream_relax_skew unset = MinimizeSkew ON; run B:
// =true = MinimizeSkew OFF):
//   - 80- FROZEN duration: how long shard 80- stays at ~0 delivered while
//     -80 actively delivers (= first-80- delivery minus first-(-80)
//     delivery). Expect ON >> OFF (the hold).
//   - Both-shard CONVERGENCE: t0 -> both shards delivered == committed.
//   - The throughput-relevant win is the ON/OFF ratio on the freeze: under
//     ON, 80-'s rows are unavailable to apply for the whole -80 drain (the
//     item-23 catch-up-latency pathology); under OFF they're available
//     immediately.
//
// CORRECTNESS (load-bearing, HARD-asserted in BOTH runs): after drain, the
// delivered set == the source-committed set, exactly-once — 0 gap, 0 dup
// (beyond a small boundary tolerance), 0 value mismatch. The SOURCE is the
// oracle (re-read per shard after the bursts settle), NOT the writer's tally
// — a bounded burst that fully completes has no cancellation race, but
// reading the source back is the same belt-and-suspenders the sibling A/B
// uses and keeps the oracle honest.
//
// HARD GATES vs DIRECTIONAL FINDING: the HARD gates are (1) exactly-once in
// BOTH runs and (2) the ON run REPRODUCING the hold (80- frozen past an
// absolute floor) — that reproduction is this harness's load-bearing
// achievement: the first LOCAL reproduction of the item-23 "80- frozen while
// -80 drains" pathology, and proof the engineered skew is real. Whether
// relaxing skew RELIEVES the hold is a LOGGED directional finding, NOT a hard
// gate.
//
// GROUND-TRUTH FINDING (measured 2026-06-25, two independent setups —
// drain-during-burst AND pause-then-drain-both-pre-queued — gave byte-identical
// ON/OFF delivery, 80- frozen ~30s in BOTH):
//
//   The hold REPRODUCES robustly under ON, but relaxing MinimizeSkew does NOT
//   relieve it on a single-host vitess/lite cluster. Root cause is a
//   single-host invariant: forming the ON hold needs cross-shard TEMPORAL skew
//   (the -80 backlog committed strictly before 80-), but that same temporal
//   separation makes -80's events ARRIVE at vtgate first, so with a fast
//   backlog read + a throttled consumer the -80 substream saturates the merged
//   send loop and drains first EVEN with MinimizeSkew off. Showing OFF
//   interleave instead needs CONCURRENT same-timestamp arrival on both shards
//   — which leaves no skew for ON to hold on. The two requirements are
//   mutually exclusive on one wall-clock host; the relief requires concurrent
//   ongoing writes under a SUSTAINED cross-shard clock skew, which only arises
//   at cross-region/scale — exactly as ADR-0120's Validation section
//   anticipated ("cross-region/scale phenomenon"). The relaxation is proven
//   SAFE here (exactly-once held, request verified MinimizeSkew=false); only
//   its throughput BENEFIT is unobservable on single-host infra.

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// engTLEntry is one delivered-id sample on the engineered-skew timeline:
// the elapsed seconds since t0 (first burst start) and the shard the id
// belongs to (race-free membership from the probe step).
type engTLEntry struct {
	elapsed float64
	shard   string
}

// engSkewAB collects one A/B run's measured outcome.
type engSkewAB struct {
	relax bool
	label string

	committed         int
	deliveredDistinct int
	dupExcess         int
	missing           int
	valueMismatch     int

	perShardCommitted map[string]int // membership (oldShard=-80, newShard=80-)

	// Engineered-skew timing signals (seconds from t0 = first burst start).
	oldShardFirstDelivery float64 // -80 (older backlog) first delivered
	newShardFirstDelivery float64 // 80- (newer backlog) first delivered
	newShardFrozenDur     float64 // newShardFirst - oldShardFirst, clamped >=0 (the HOLD)
	oldShardConverge      float64 // -80 delivered==committed
	newShardConverge      float64 // 80- delivered==committed
	bothConverge          float64 // max(old,new) — both fully delivered

	// Corroborating 1s-window signal over the whole drain.
	bothAdvancedWindows int
	totalWindows        int
	maxFrozenStreak     map[string]int
}

// TestVitessReshard_RelaxSkewEngineeredSkewHoldAB is the ADR-0120
// engineered-skew A/B. One resharded 2-shard cluster; run A (skew ON) then
// run B (skew OFF), each on a disjoint id range. Manufactures cross-shard
// temporal skew (old backlog on -80, new backlog on 80-) so the MinimizeSkew
// hold forms under ON and is relieved under OFF. Logs the A/B numbers;
// hard-asserts exactly-once in both runs.
func TestVitessReshard_RelaxSkewEngineeredSkewHoldAB(t *testing.T) {
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

	// Seed a few baseline rows so the reshard copy phase has data. ids 1..8
	// are pre-stream and in a range disjoint from both run id-bases, so
	// neither A/B stream (opened at "current") ever delivers them.
	vrApplySQL(t, c.mysqlDSN+"&multiStatements=true",
		`INSERT INTO acct (id, payload) VALUES `+
			`(1,'p-1'),(2,'p-2'),(3,'p-3'),(4,'p-4'),(5,'p-5'),(6,'p-6'),(7,'p-7'),(8,'p-8');`)
	time.Sleep(2 * time.Second)

	// --- RESHARD 1 -> 2 (setup; the A/B runs on the steady-state result) ---
	c.addTargetShards(t, "-80", "80-")
	if out, rerr := c.vtctldExec(t, "Reshard", "create",
		"--workflow", "relaxeng", "--target-keyspace", vrKeyspace,
		"--source-shards", "-", "--target-shards", "-80,80-"); rerr != nil {
		t.Fatalf("Reshard create: %v\n%s", rerr, out)
	}
	c.waitReshardRunning(t, "relaxeng")
	if _, rerr := c.vtctldExec(t, "Reshard", "SwitchTraffic",
		"--workflow", "relaxeng", "--target-keyspace", vrKeyspace); rerr != nil {
		t.Fatalf("Reshard SwitchTraffic: %v", rerr)
	}
	shards := vrShowShards(t, c.mysqlDSN)
	assertReshardTargetShardsPresent(t, "SETUP", shards)
	t.Logf("SETUP: resharded 1 -> 2; vtgate shards now %v", shards)

	// Wait through the post-SwitchTraffic "no healthy tablet for PRIMARY"
	// window before the A/B burst-writes to `acct` (else the first burst
	// races the cutover and fails 1105 — the CI-only flake).
	c.waitReshardPrimariesRoutable(t, "acct")

	// --- the A/B: same engineered-skew scenario, skew ON then skew OFF ---
	runA := runEngineeredSkewScenario(t, c, false, 100_000_000)
	runB := runEngineeredSkewScenario(t, c, true, 200_000_000)

	logEngSkewABComparison(t, runA, runB)

	// HARD: the engineered skew must genuinely REPRODUCE the MinimizeSkew hold
	// under ON — that is the harness's load-bearing achievement (the first
	// LOCAL reproduction of the item-23 "80- frozen for minutes while -80
	// drains" pathology) and a regression guard that the skew was really
	// manufactured. If this floor is not cleared, the harness failed to create
	// cross-shard temporal skew and the whole A/B is meaningless. (Observed
	// ~30s with burstPerShard=3000 at ~100/s; floor is generous.) Whether
	// relaxing skew RELIEVES the hold is reported by logEngSkewABComparison as
	// a directional finding, NOT gated here — see that verdict for why the
	// relief is a cross-region/scale phenomenon not observable on one host.
	const holdReproducedFloorSec = 8.0
	if runA.newShardFrozenDur < holdReproducedFloorSec {
		t.Fatalf("engineered skew did NOT reproduce the hold under ON: 80- frozen only %.1fs (< %.1fs floor) — the harness failed to manufacture cross-shard temporal skew, so the A/B is inconclusive",
			runA.newShardFrozenDur, holdReproducedFloorSec)
	}
}

// runEngineeredSkewScenario executes one half of the A/B and hard-asserts
// exactly-once. relax selects the source-DSN vstream_relax_skew param;
// idBase is the disjoint id range this run uses (probe + bursts).
func runEngineeredSkewScenario(t *testing.T, c *vitessReshardCluster, relax bool, idBase int64) engSkewAB {
	t.Helper()
	label := "A(skew ON / relax=false)"
	if relax {
		label = "B(skew OFF / relax=true)"
	}
	t.Logf("=== RUN %s starting (idBase=%d) ===", label, idBase)

	const (
		// Engineered-skew knobs. Tuned so -80's throttled drain clearly
		// outlasts the skew gap (so 80- stays frozen well past its burst).
		probeRange     = 12000                 // ids probed to learn shard membership
		burstPerShard  = 3000                  // ids burst per shard in the measured run
		skewGap        = 6 * time.Second       // real wall gap between the two bursts
		throttleTick   = 50 * time.Millisecond // consumer tick
		perTick        = 5                     // => ~100 delivered rows/s consumer rate
		writerBatch    = 200                   // rows per multi-row INSERT (fast burst)
		drainTimeout   = 240 * time.Second     // throttled tail-drain budget
		boundaryDupTol = 50                    // generous; no reshard mid-run => expect ~0
		minPerShard    = 200                   // each shard must hold a non-trivial share
	)

	hi := idBase + int64(probeRange)

	// --- STEP 2: partition ids by shard via a probe, then clean. ---
	// Insert the probe range, read each shard tablet directly to bucket ids,
	// then DELETE the probe range so the measured bursts deliver fresh.
	probeInsertRange(t, c, idBase, hi, writerBatch)
	oldShard, newShard := "-80", "80-"
	idsOld := shardScopedIDs(t, c, oldShard, idBase, hi) // -80 = OLD backlog
	idsNew := shardScopedIDs(t, c, newShard, idBase, hi) // 80- = NEW backlog
	if len(idsOld) < burstPerShard || len(idsNew) < burstPerShard {
		t.Fatalf("%s: probe bucketed -80=%d 80-=%d ids; need >=%d each (hash split too uneven / probe range too small)",
			label, len(idsOld), len(idsNew), burstPerShard)
	}
	// Trim each bucket to an equal burst size so the two backlogs are
	// symmetric (only the timestamp ordering differs between them).
	sort.Slice(idsOld, func(i, j int) bool { return idsOld[i] < idsOld[j] })
	sort.Slice(idsNew, func(i, j int) bool { return idsNew[i] < idsNew[j] })
	idsOld = idsOld[:burstPerShard]
	idsNew = idsNew[:burstPerShard]
	// Clean the whole probe range so the measured run starts empty (the
	// bursts re-insert the SAME ids, so probe rows must be gone first).
	shardScopedDeleteRange(t, c, oldShard, idBase, hi)
	shardScopedDeleteRange(t, c, newShard, idBase, hi)
	t.Logf("%s: partitioned ids by shard: -80(OLD)=%d 80-(NEW)=%d (burstPerShard=%d); probe range cleaned",
		label, len(idsOld), len(idsNew), burstPerShard)

	// id -> shard membership (race-free; computed before any concurrency).
	idToShard := make(map[int64]string, 2*burstPerShard)
	for _, id := range idsOld {
		idToShard[id] = oldShard
	}
	for _, id := range idsNew {
		idToShard[id] = newShard
	}

	// --- STEP 3: open the production reader (CDC-only, from "current") ---
	// vstream_progress_timeout=300s (> the 240s throttled drainTimeout below):
	// this A/B skew test deliberately throttles the CONSUMER for up to 240s to
	// measure per-shard skew, which backpressures the source stream past the 45s
	// default liveness window — without this the reader correctly reconnects
	// mid-measurement and the test flags an unclean teardown. A real slow applier
	// would likewise reconnect (harmless, from last position); the raised window
	// is test-only and still catches a genuine >300s hang.
	sourceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_auto_discover_shards=true&vstream_progress_timeout=300s",
		c.mysqlDSN, c.grpcAddr,
	)
	if !relax { // relaxed is the default (ADR-0120 flipped); opt out to preserve skew
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
	if req, berr := cdcRdr.buildVStreamRequest(fromNowVStreamPos(cdcRdr.keyspace, cdcRdr.shards)); berr != nil {
		t.Fatalf("%s: buildVStreamRequest: %v", label, berr)
	} else if got := req.GetFlags().GetMinimizeSkew(); got != !relax {
		t.Fatalf("%s: request MinimizeSkew = %v; want %v", label, got, !relax)
	}

	// ---- throttled collector (mutex-guarded; read after the collector exits) ----
	var (
		mu            sync.Mutex
		delivered     = make(map[int64]int)
		distinct      int
		valueMismatch int
		timeline      []engTLEntry
	)
	var closedEarly atomic.Bool
	var t0 atomic.Int64 // unix-nanos of first burst start; set just before bursting

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
		sh, known := idToShard[id]
		if !known {
			return // not one of our engineered ids (defensive)
		}
		mu.Lock()
		prev := delivered[id]
		delivered[id] = prev + 1
		if prev == 0 {
			distinct++
			if pv, _ := ins.Row["payload"].(string); pv != fmt.Sprintf("p-%d", id) {
				valueMismatch++
			}
			if base := t0.Load(); base != 0 {
				timeline = append(timeline, engTLEntry{
					elapsed: time.Since(time.Unix(0, base)).Seconds(),
					shard:   sh,
				})
			}
		}
		mu.Unlock()
	}

	collCtx, collCancel := context.WithCancel(ctx)
	collectorDone := make(chan struct{})
	startCollector := func() {
		go func() {
			defer close(collectorDone)
			ticker := time.NewTicker(throttleTick)
			defer ticker.Stop()
			for {
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
			}
		}()
	}

	// --- STEP 4: engineered bursts with the consumer PAUSED. ---
	// CRITICAL ORDERING (the run-1 ground-truth fix): the consumer does NOT
	// drain during the bursts. If it did, the OLD shard (-80) — written first
	// — would get a multi-second pipe head-start, its vtgate vstream substream
	// would saturate the merged send loop, and the OLD backlog would drain
	// first EVEN with MinimizeSkew off (observed: run-1 gave byte-identical
	// ON/OFF delivery, 80- frozen 30s in BOTH). By committing BOTH backlogs
	// (-80 OLD, then after a real skewGap, 80- NEW) while nothing is consuming,
	// both substreams are queued at vtgate with -80 strictly older when drain
	// begins — so MinimizeSkew has genuine temporal skew to act on (ON holds
	// 80- until -80 drains) and, with it off, vtgate's per-shard substreams
	// compete fairly on the send loop (OFF interleaves). Only the reader's
	// small bounded channel (256) pre-fills with OLD events during the pause;
	// that is a ~256/rate prefix, not a 6s head-start.
	burstWrite(t, c.mysqlDSN, idsOld, writerBatch) // OLD: all land -80
	t.Logf("%s: burst OLD backlog -80 done (%d ids); sleeping skewGap=%s before NEW backlog (consumer paused)", label, len(idsOld), skewGap)
	time.Sleep(skewGap)
	burstWrite(t, c.mysqlDSN, idsNew, writerBatch) // NEW: all land 80-
	time.Sleep(2 * time.Second)                    // let 80- commit + register at vtgate before drain

	// Both backlogs queued with -80 older. NOW start the throttled drain; t0
	// is the drain start so first-delivery times are measured from here.
	t0.Store(time.Now().UnixNano())
	startCollector()
	t.Logf("%s: burst NEW backlog 80- done (%d ids); draining (throttled ~%d/s, both backlogs pre-queued)", label, len(idsNew), perTick*int(time.Second/throttleTick))

	// --- STEP 5: throttled drain to convergence ---
	srcCount := len(idsOld) + len(idsNew)
	deadline := time.Now().Add(drainTimeout)
	for distinctNow() < srcCount && !closedEarly.Load() && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}
	collCancel()
	<-collectorDone

	if closedEarly.Load() {
		if e := cdcRdr.Err(); e != nil && !isCleanTeardownErr(e) {
			t.Fatalf("%s: stream closed early with error (not a clean teardown): %v", label, e)
		}
	}

	// --- oracle ground truth: re-read the committed set per shard (SOURCE) ---
	committed := make(map[int64]struct{}, srcCount)
	perShardCommitted := make(map[string]int)
	for _, sh := range []string{oldShard, newShard} {
		ids := shardScopedIDs(t, c, sh, idBase, hi)
		for _, id := range ids {
			committed[id] = struct{}{}
			perShardCommitted[sh]++
		}
	}

	// --- per-shard timing signals from the timeline ---
	mu.Lock()
	tl := append([]engTLEntry(nil), timeline...)
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

	res := engSkewAB{
		relax:             relax,
		label:             label,
		committed:         len(committed),
		deliveredDistinct: distinctFinal,
		dupExcess:         dupExcess,
		missing:           missing,
		valueMismatch:     valueMismatchFinal,
		perShardCommitted: perShardCommitted,
		maxFrozenStreak:   map[string]int{},
	}
	computeEngSkewTimings(&res, tl, oldShard, newShard, perShardCommitted)

	// 2-shard exercise guard.
	for _, sh := range []string{oldShard, newShard} {
		if perShardCommitted[sh] < minPerShard {
			t.Fatalf("%s: shard %q holds only %d committed ids (< %d) — run not exercising both shards (per-shard: %v)",
				label, sh, perShardCommitted[sh], minPerShard, perShardCommitted)
		}
	}

	// HARD exactly-once assertions (the load-bearing correctness gate).
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

	t.Logf("%s: EXACTLY-ONCE PASSED: committed=%d delivered-distinct=%d dupExcess=%d (<=%d) valueMismatch=0 — no gap, no dup, no corruption with relaxSkew=%v",
		label, len(committed), distinctFinal, dupExcess, boundaryDupTol, relax)
	t.Logf("%s: TIMINGS  -80(OLD)first=%.1fs 80-(NEW)first=%.1fs 80-frozen=%.1fs | converge -80=%.1fs 80-=%.1fs both=%.1fs | bothAdvanced=%d/%d maxFrozenStreak=%v perShardCommitted=%v",
		label, res.oldShardFirstDelivery, res.newShardFirstDelivery, res.newShardFrozenDur,
		res.oldShardConverge, res.newShardConverge, res.bothConverge,
		res.bothAdvancedWindows, res.totalWindows, res.maxFrozenStreak, perShardCommitted)

	return res
}

// computeEngSkewTimings derives the per-shard first-delivery / convergence
// timings and the 1s-window corroborating signal from the delivery timeline.
func computeEngSkewTimings(res *engSkewAB, tl []engTLEntry, oldShard, newShard string, perShardCommitted map[string]int) {
	sort.Slice(tl, func(i, j int) bool { return tl[i].elapsed < tl[j].elapsed })

	firstSeen := map[string]float64{}
	seenCount := map[string]int{}
	convergeAt := map[string]float64{}
	bins := map[string]map[int]int{oldShard: {}, newShard: {}}
	maxBin := 0
	for _, e := range tl {
		if _, ok := firstSeen[e.shard]; !ok {
			firstSeen[e.shard] = e.elapsed
		}
		seenCount[e.shard]++
		if seenCount[e.shard] == perShardCommitted[e.shard] {
			convergeAt[e.shard] = e.elapsed
		}
		b := int(e.elapsed)
		if bins[e.shard] != nil {
			bins[e.shard][b]++
		}
		if b > maxBin {
			maxBin = b
		}
	}

	res.oldShardFirstDelivery = firstSeen[oldShard]
	res.newShardFirstDelivery = firstSeen[newShard]
	frozen := res.newShardFirstDelivery - res.oldShardFirstDelivery
	if frozen < 0 {
		frozen = 0
	}
	res.newShardFrozenDur = frozen
	res.oldShardConverge = convergeAt[oldShard]
	res.newShardConverge = convergeAt[newShard]
	res.bothConverge = res.oldShardConverge
	if res.newShardConverge > res.bothConverge {
		res.bothConverge = res.newShardConverge
	}

	// 1s-window signal: bothAdvanced fraction + per-shard max frozen streak.
	bothAdvanced, total := 0, 0
	maxStreak := map[string]int{oldShard: 0, newShard: 0}
	curStreak := map[string]int{oldShard: 0, newShard: 0}
	for b := 0; b <= maxBin; b++ {
		oldA := bins[oldShard][b] > 0
		newA := bins[newShard][b] > 0
		if !oldA && !newA {
			continue
		}
		total++
		if oldA && newA {
			bothAdvanced++
		}
		// frozen streak: this shard 0 while peer >0.
		if !oldA && newA {
			curStreak[oldShard]++
			if curStreak[oldShard] > maxStreak[oldShard] {
				maxStreak[oldShard] = curStreak[oldShard]
			}
		} else {
			curStreak[oldShard] = 0
		}
		if !newA && oldA {
			curStreak[newShard]++
			if curStreak[newShard] > maxStreak[newShard] {
				maxStreak[newShard] = curStreak[newShard]
			}
		} else {
			curStreak[newShard] = 0
		}
	}
	res.bothAdvancedWindows = bothAdvanced
	res.totalWindows = total
	res.maxFrozenStreak = maxStreak
}

// probeInsertRange inserts every id in [lo,hi) with payload p-<id> via vtgate
// (each row routes to its shard by hash). Used to learn shard membership.
func probeInsertRange(t *testing.T, c *vitessReshardCluster, lo, hi int64, batch int) {
	t.Helper()
	ids := make([]int64, 0, hi-lo)
	for id := lo; id < hi; id++ {
		ids = append(ids, id)
	}
	burstWrite(t, c.mysqlDSN, ids, batch)
	time.Sleep(2 * time.Second) // let the inserts settle on both shards
}

// burstWrite inserts the given ids (payload p-<id>) as fast as possible via
// multi-row INSERTs on a dedicated connection. Bounded and synchronous (no
// cancellation), so every id either commits or fails loudly — no
// commit-but-Canceled accounting race. A duplicate-key on re-insert is fatal
// (means the caller didn't clean the range first).
func burstWrite(t *testing.T, dsn string, ids []int64, batch int) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("burstWrite open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	for start := 0; start < len(ids); start += batch {
		end := start + batch
		if end > len(ids) {
			end = len(ids)
		}
		var sb strings.Builder
		sb.WriteString("INSERT INTO acct (id, payload) VALUES ")
		for i := start; i < end; i++ {
			if i > start {
				sb.WriteByte(',')
			}
			fmt.Fprintf(&sb, "(%d,'p-%d')", ids[i], ids[i])
		}
		if _, e := db.ExecContext(ctx, sb.String()); e != nil {
			t.Fatalf("burstWrite exec [%d,%d): %v", ids[start], ids[end-1], e)
		}
	}
}

// shardScopedDeleteRange deletes acct rows in [lo,hi) on shard sh via vtgate
// "keyspace:shard" targeting — a single-shard DML (always allowed, no scatter
// surprise). Used to clean the probe range before the measured bursts.
func shardScopedDeleteRange(t *testing.T, c *vitessReshardCluster, sh string, lo, hi int64) {
	t.Helper()
	dsn := strings.Replace(c.mysqlDSN, "/"+vrKeyspace+"?", "/"+vrKeyspace+":"+sh+"?", 1)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open shard %q for delete: %v", sh, err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, "DELETE FROM acct WHERE id >= ? AND id < ?", lo, hi); err != nil {
		t.Fatalf("shard %q delete range [%d,%d): %v", sh, lo, hi, err)
	}
}

// isCleanTeardownErr reports whether a stream error is an expected teardown
// signal (context cancel/deadline or a connection-closing transport error),
// not a real oracle failure.
func isCleanTeardownErr(e error) bool {
	if e == nil {
		return true
	}
	if errors.Is(e, context.Canceled) || errors.Is(e, context.DeadlineExceeded) {
		return true
	}
	s := e.Error()
	return strings.Contains(s, "connection is closing") ||
		strings.Contains(s, "context canceled") ||
		strings.Contains(s, "use of closed network connection")
}

// logEngSkewABComparison emits the side-by-side A/B summary + the directional
// verdict on the engineered-skew hold. Exactly-once is already hard-asserted
// per run; this is the throughput-delta report.
func logEngSkewABComparison(t *testing.T, a, b engSkewAB) {
	t.Helper()
	fracA := windowFraction(a.bothAdvancedWindows, a.totalWindows)
	fracB := windowFraction(b.bothAdvancedWindows, b.totalWindows)

	frozenRatio := 0.0
	if b.newShardFrozenDur > 0.05 {
		frozenRatio = a.newShardFrozenDur / b.newShardFrozenDur
	}

	t.Logf("================ ADR-0120 ENGINEERED-SKEW A/B SUMMARY ================")
	t.Logf("RUN A (MinimizeSkew ON,  relax=false): 80-(NEW) frozen=%.1fs first(-80)=%.1fs first(80-)=%.1fs converge both=%.1fs bothAdvanced=%d/%d (%.0f%%) maxFrozenStreak=%v committed=%d delivered=%d",
		a.newShardFrozenDur, a.oldShardFirstDelivery, a.newShardFirstDelivery, a.bothConverge, a.bothAdvancedWindows, a.totalWindows, fracA*100, a.maxFrozenStreak, a.committed, a.deliveredDistinct)
	t.Logf("RUN B (MinimizeSkew OFF, relax=true ): 80-(NEW) frozen=%.1fs first(-80)=%.1fs first(80-)=%.1fs converge both=%.1fs bothAdvanced=%d/%d (%.0f%%) maxFrozenStreak=%v committed=%d delivered=%d",
		b.newShardFrozenDur, b.oldShardFirstDelivery, b.newShardFirstDelivery, b.bothConverge, b.bothAdvancedWindows, b.totalWindows, fracB*100, b.maxFrozenStreak, b.committed, b.deliveredDistinct)
	t.Logf("EXPECTATION: ON freezes 80- (newer backlog) until -80 (older) drains; OFF delivers both concurrently => 80- available immediately.")
	t.Logf("OBSERVED   : 80- frozen ON=%.1fs OFF=%.1fs (ON/OFF ratio=%.1fx);  both-advanced ON=%.0f%% OFF=%.0f%% (Δ=%+.0f pts);  bothConverge ON=%.1fs OFF=%.1fs",
		a.newShardFrozenDur, b.newShardFrozenDur, frozenRatio, fracA*100, fracB*100, (fracB-fracA)*100, a.bothConverge, b.bothConverge)

	// Directional verdict on the OFF-relief. This is a LOGGED finding, NOT a
	// hard gate: the hard gates are exactly-once (per run) + the ON-hold
	// reproduction (asserted by the caller). Whether relaxing skew RELIEVES
	// the hold on a single-host cluster is the open question this harness was
	// built to probe, and a non-relief here is a legitimate, expected result
	// (not a test failure) — see the GROUND-TRUTH note below.
	const minRatio = 3.0
	if frozenRatio >= minRatio {
		t.Logf("VERDICT    : HOLD REPRODUCED under ON (80- frozen %.1fs) and RELIEVED under OFF (ratio %.1fx >= %.1fx). ADR-0120 concurrent-drain win demonstrated locally.",
			a.newShardFrozenDur, frozenRatio, minRatio)
	} else {
		t.Logf("VERDICT    : HOLD REPRODUCED under ON (80- frozen %.1fs) but NOT relieved under OFF on this single-host cluster (OFF frozen %.1fs, ratio %.1fx). This is a VALID, EXPECTED finding, not a failure:",
			a.newShardFrozenDur, b.newShardFrozenDur, frozenRatio)
		t.Logf("             on a single wall-clock host the two conditions are MUTUALLY EXCLUSIVE. Forming the ON hold needs cross-shard TEMPORAL skew (the -80 backlog committed strictly BEFORE 80-); but that same temporal separation makes -80's events ARRIVE at vtgate first, so with a fast backlog read + a throttled consumer the -80 shard's vstream goroutine saturates the merged send loop and drains first EVEN with MinimizeSkew off. Showing OFF interleave instead needs CONCURRENT same-timestamp arrival on both shards — which leaves NO skew for ON to hold on. The relief therefore requires genuinely concurrent ongoing writes under a SUSTAINED cross-shard clock skew, which only arises at cross-region / scale (ADR-0120 Validation: 'cross-region/scale phenomenon').")
		t.Logf("             Correctness (exactly-once) held in BOTH runs, and the relaxed-skew request was verified to carry MinimizeSkew=false — so the relaxation is proven safe; only its throughput BENEFIT is unobservable on single-host infra.")
	}
	t.Logf("=====================================================================")
}
