//go:build integration && vitessreshard

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0120 PER-SHARD-LATENCY A/B: inject a SUSTAINED network latency on
// ONE shard's vttablet->vtgate (VStream delivery) leg, drive a STEADY
// concurrent writer to BOTH shards, and measure whether relaxing vtgate's
// MinimizeSkew (--vstream-relax-skew / source-DSN vstream_relax_skew=true)
// lets the lagged shard's peer drain concurrently instead of vtgate holding
// it back to keep the merged stream commit-time ordered.
//
// WHY THIS EXISTS (the gap the two sibling A/Bs left open):
//
//   - TestVitessReshard_RelaxSkewConcurrentDrainAB (steady writer + slow
//     consumer) confirmed EXACTLY-ONCE under skew on/off but NO hold formed:
//     both tablets commit with near-identical timestamps on one host, so
//     MinimizeSkew has no cross-shard skew to act on.
//   - TestVitessReshard_RelaxSkewEngineeredSkewHoldAB (old backlog on -80,
//     new on 80-, real wall gap) REPRODUCED the hold under ON but could NOT
//     show the relief under OFF: forming the ON hold needs the -80 backlog
//     committed strictly BEFORE 80-, but that same temporal separation makes
//     -80 ARRIVE first, so -80 drains first even with MinimizeSkew off. The
//     two are mutually exclusive on one wall-clock host.
//
// THE NEW INGREDIENT a single host lacks is a SUSTAINED per-shard DELIVERY
// skew under ONGOING concurrent writes — the cross-region phenomenon. This
// test manufactures it WITHOUT clock skew: a toxiproxy sidecar adds a fixed
// latency to shard 80-'s vttablet->vtgate gRPC leg, so 80-'s received-event
// frontier lags -80's continuously while BOTH shards are written at the same
// commit rate.
//
// HYPOTHESIS (UNCERTAIN — either outcome is a reportable finding):
//   If vtgate's MinimizeSkew keys on RECEIPT of events, the delivery lag
//   makes 80- the perpetually-behind shard, so under ON vtgate holds the
//   AHEAD shard (-80) waiting for 80- (a sustained hold) and under OFF -80
//   drains concurrently (the relief). If instead MinimizeSkew keys on the
//   source COMMIT timestamp (clock-shared in one container), delivery latency
//   adds only a uniform tail delay and forms NO hold — which would CLOSE the
//   question: even per-shard latency can't reproduce the win locally, only a
//   genuinely geo-distributed source can.
//
// INJECTION MECHANISM: toxiproxy sidecar + tablet-hostname indirection.
//   - The vitess/lite container has the stable network alias "vitess"
//     (vrVitessAlias). The 80- shard's REPLICA tablet (the one sluice's
//     VStream streams from, by design TabletType_REPLICA) is brought up with
//     --tablet-hostname=toxiproxy and its normal grpc-port 16251.
//   - A toxiproxy container (alias "toxiproxy") runs a proxy LISTENing on
//     0.0.0.0:16251 forwarding UPSTREAM to vitess:16251 (the real replica),
//     with a latency toxic in BOTH directions.
//   - So vtgate (inside the vitess container) dials toxiproxy:16251 ->
//     proxy(+latency) -> real replica, while -80's replica advertises the
//     vitess alias directly (no latency). Net: 80-'s VStream delivery is
//     sustained-lagged; -80's is not.
//   - The PRIMARY of 80- is NOT rerouted, so reparent/admin RPCs and the
//     membership/oracle reads (which target the primary) stay direct/fast.
//
// REUSE: the steady-writer + throttled-consumer + per-shard-frontier +
// exactly-once machinery is identical to the sibling concurrent-drain A/B, so
// this test reuses runRelaxSkewScenario / logRelaxABComparison verbatim. The
// ONLY new thing is the latency injection at cluster setup.
//
// HARD GATES: exactly-once in BOTH runs + the flag-flip assertions (both done
// inside runRelaxSkewScenario). The hold/relief result is a LOGGED
// measurement with a generous directional read — NOT a brittle timing assert.

package mysql

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// Target-shard uids for the A/B reshard (disjoint; -80 gets the lower
	// block, 80- the next). bringUpTargetShard derives the REPLICA at
	// uid+50 and its grpc-port at 16000+(uid+50).
	vrLatNonLaggyShard    = "-80"
	vrLatNonLaggyShardUID = 200
	vrLatLaggyShard       = "80-"
	vrLatLaggyShardUID    = 201

	// 80-'s REPLICA grpc-port = 16000 + (uid+50) = 16251. This is the leg
	// the toxiproxy latency proxy listens on / forwards to.
	vrLatLaggyReplicaGRPC = 16000 + vrLatLaggyShardUID + 50 // 16251

	// Toxiproxy sidecar.
	vrToxiproxyImage     = "ghcr.io/shopify/toxiproxy:2.9.0"
	vrToxiproxyAlias     = "toxiproxy"
	vrToxiproxyAdminPort = "8474/tcp"

	// Per-direction latency added to 80-'s VStream delivery leg. Large
	// enough to make 80-'s received frontier visibly lag -80's, small
	// enough that vtgate's streaming healthcheck still marks the tablet
	// serving (RTT ~2x this; default healthcheck tolerances are seconds).
	vrLatLatencyMs = 250
	vrLatJitterMs  = 30
)

// toxiproxySidecar owns the toxiproxy container + its host-mapped admin API.
type toxiproxySidecar struct {
	container testcontainers.Container
	adminBase string // http://host:mappedAdminPort
	terminate func()
}

// startToxiproxy boots a toxiproxy container on the given cluster's network
// with the alias vtgate/vtctld can dial, and publishes only the admin API
// (the per-tablet listen ports are reached over the Docker network, not the
// host). The proxy itself is configured by the caller via createLatencyProxy.
func startToxiproxy(t *testing.T, c *vitessReshardCluster) *toxiproxySidecar {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	req := testcontainers.ContainerRequest{
		Image:    vrToxiproxyImage,
		Networks: []string{c.net.Name},
		NetworkAliases: map[string][]string{
			c.net.Name: {vrToxiproxyAlias},
		},
		ExposedPorts: []string{vrToxiproxyAdminPort},
		// The image's default entrypoint already runs the server bound to
		// 0.0.0.0:8474 (CMD=-host=0.0.0.0), so no override is needed.
		WaitingFor: wait.ForListeningPort(vrToxiproxyAdminPort).WithStartupTimeout(2 * time.Minute),
	}
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start toxiproxy: %v", err)
	}
	host, err := ctr.Host(ctx)
	if err != nil {
		_ = ctr.Terminate(ctx)
		t.Fatalf("toxiproxy host: %v", err)
	}
	port, err := ctr.MappedPort(ctx, vrToxiproxyAdminPort)
	if err != nil {
		_ = ctr.Terminate(ctx)
		t.Fatalf("toxiproxy mapped admin port: %v", err)
	}
	return &toxiproxySidecar{
		container: ctr,
		adminBase: fmt.Sprintf("http://%s:%d", host, port.Num()),
		terminate: func() {
			tc, cc := context.WithTimeout(context.Background(), 30*time.Second)
			defer cc()
			_ = ctr.Terminate(tc)
		},
	}
}

// createLatencyProxy creates a toxiproxy proxy that LISTENs on
// 0.0.0.0:<port> (reachable on the network as toxiproxy:<port>) and forwards
// UPSTREAM to <upstreamAlias>:<port>, then adds a latency toxic in BOTH
// directions. Called BEFORE the lagged shard's replica is brought up so the
// listener is ready when vtgate first dials it.
func (tp *toxiproxySidecar) createLatencyProxy(t *testing.T, name, upstreamAlias string, port, latencyMs, jitterMs int) {
	t.Helper()
	tp.apiPost(t, "/proxies", map[string]any{
		"name":     name,
		"listen":   fmt.Sprintf("0.0.0.0:%d", port),
		"upstream": fmt.Sprintf("%s:%d", upstreamAlias, port),
		"enabled":  true,
	})
	for _, stream := range []string{"downstream", "upstream"} {
		tp.apiPost(t, "/proxies/"+name+"/toxics", map[string]any{
			"name":     "lat_" + stream,
			"type":     "latency",
			"stream":   stream,
			"toxicity": 1.0,
			"attributes": map[string]any{
				"latency": latencyMs,
				"jitter":  jitterMs,
			},
		})
	}
	t.Logf("toxiproxy: proxy %q listening 0.0.0.0:%d -> %s:%d with +%dms (jitter %dms) latency both directions",
		name, port, upstreamAlias, port, latencyMs, jitterMs)
}

// apiPost POSTs a JSON body to the toxiproxy admin API and fatals on any
// non-2xx response (the latency injection is load-bearing; a silent failure
// would invalidate the whole A/B).
func (tp *toxiproxySidecar) apiPost(t *testing.T, path string, body map[string]any) {
	t.Helper()
	buf, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("toxiproxy marshal %s: %v", path, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tp.adminBase+path, bytes.NewReader(buf))
	if err != nil {
		t.Fatalf("toxiproxy new request %s: %v", path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("toxiproxy POST %s: %v", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	rb, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("toxiproxy POST %s -> %d: %s", path, resp.StatusCode, string(rb))
	}
}

// TestVitessReshard_RelaxSkewPerShardLatencyAB is the ADR-0120
// per-shard-latency A/B. One resharded 2-shard cluster with a toxiproxy
// latency injected on shard 80-'s VStream replica leg; run A (skew ON) then
// run B (skew OFF), each on a disjoint id range. Logs the per-shard A/B
// numbers; hard-asserts exactly-once + the flag flip in both runs (inside
// runRelaxSkewScenario).
func TestVitessReshard_RelaxSkewPerShardLatencyAB(t *testing.T) {
	skipReshardSkewABQuarantine(t)

	c := startVitessReshardCluster(t, "-")
	defer c.terminate()

	// --- toxiproxy sidecar on the cluster network ---
	tp := startToxiproxy(t, c)
	defer tp.terminate()

	// --- source schema: hash-vindexed table on the 1-shard keyspace ---
	vrApplySQL(t, c.mysqlDSN, `CREATE TABLE acct (
		id      BIGINT      NOT NULL,
		payload VARCHAR(64) NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	vrApplySQL(t, c.mysqlDSN, `ALTER VSCHEMA ON `+vrKeyspace+`.acct ADD VINDEX hash(id) USING hash`)
	time.Sleep(3 * time.Second)

	// Seed baseline rows for the reshard copy phase. ids 1..8 are disjoint
	// from both run id-bases, so neither A/B stream (opened at "current")
	// delivers them.
	vrApplySQL(t, c.mysqlDSN+"&multiStatements=true",
		`INSERT INTO acct (id, payload) VALUES `+
			`(1,'p-1'),(2,'p-2'),(3,'p-3'),(4,'p-4'),(5,'p-5'),(6,'p-6'),(7,'p-7'),(8,'p-8');`)
	time.Sleep(2 * time.Second)

	// --- configure the latency proxy BEFORE bringing up 80-'s replica ---
	// so the listener is ready when vtgate first dials toxiproxy:16251.
	tp.createLatencyProxy(t, "tablet80replica", vrVitessAlias, vrLatLaggyReplicaGRPC, vrLatLatencyMs, vrLatJitterMs)

	// --- RESHARD 1 -> 2 (setup); route 80-'s REPLICA through the proxy ---
	// -80: direct (no latency). 80-: replica advertises the toxiproxy alias.
	c.bringUpTargetShard(t, vrLatNonLaggyShard, vrLatNonLaggyShardUID, "")
	c.bringUpTargetShard(t, vrLatLaggyShard, vrLatLaggyShardUID, vrToxiproxyAlias)

	if out, rerr := c.vtctldExec(t, "Reshard", "create",
		"--workflow", "relaxlat", "--target-keyspace", vrKeyspace,
		"--source-shards", "-", "--target-shards", "-80,80-"); rerr != nil {
		t.Fatalf("Reshard create: %v\n%s", rerr, out)
	}
	c.waitReshardRunning(t, "relaxlat")
	if _, rerr := c.vtctldExec(t, "Reshard", "SwitchTraffic",
		"--workflow", "relaxlat", "--target-keyspace", vrKeyspace); rerr != nil {
		t.Fatalf("Reshard SwitchTraffic: %v", rerr)
	}
	shards := vrShowShards(t, c.mysqlDSN)
	if len(shards) != 2 {
		t.Fatalf("post-reshard shards = %v; want 2 (-80, 80-) — cluster not 2-shard, A/B cannot run", shards)
	}
	t.Logf("SETUP: resharded 1 -> 2; vtgate shards now %v; 80- replica routed through +%dms toxiproxy latency", shards, vrLatLatencyMs)

	// Wait through the post-SwitchTraffic "no healthy tablet for PRIMARY"
	// window before the A/B opens a CDC reader / burst-writes to `acct`. The
	// scatter probe routes to PRIMARIES (never rerouted through toxiproxy), so
	// the +latency on the 80- replica does not affect this gate.
	c.waitReshardPrimariesRoutable(t, "acct")

	// --- the A/B: same scenario, skew ON then skew OFF ---
	runA := runRelaxSkewScenario(t, c, false, 100_000_000)
	runB := runRelaxSkewScenario(t, c, true, 200_000_000)

	logRelaxABComparison(t, runA, runB)
	logPerShardLatencyVerdict(t, runA, runB)
}

// logPerShardLatencyVerdict interprets the A/B specifically for the
// sustained-per-shard-latency mechanism. The lagged shard is 80-; under the
// receipt-keyed hypothesis vtgate (ON) holds the AHEAD shard -80 waiting for
// the lagged 80-, so -80 shows longer frozen streaks / fewer both-advanced
// windows under ON than OFF. Directional + logged only — exactly-once is the
// hard gate (already asserted per run).
func logPerShardLatencyVerdict(t *testing.T, a, b relaxAB) {
	t.Helper()
	const laggy = vrLatLaggyShard    // 80- : delivery-lagged
	const ahead = vrLatNonLaggyShard // -80 : the shard MinimizeSkew would hold

	fracA := windowFraction(a.bothAdvancedWindows, a.totalWindows)
	fracB := windowFraction(b.bothAdvancedWindows, b.totalWindows)

	t.Logf("============ ADR-0120 PER-SHARD-LATENCY VERDICT ============")
	t.Logf("MECHANISM  : 80- VStream replica leg lagged +%dms; both shards written at equal commit rate.", vrLatLatencyMs)
	t.Logf("RUN A (ON)  perShardWindow=%v maxFrozenStreak=%v bothAdvanced=%d/%d (%.0f%%) converge=%s",
		a.perShardWindow, a.maxFrozenStreak, a.bothAdvancedWindows, a.totalWindows, fracA*100, a.convergeDur)
	t.Logf("RUN B (OFF) perShardWindow=%v maxFrozenStreak=%v bothAdvanced=%d/%d (%.0f%%) converge=%s",
		b.perShardWindow, b.maxFrozenStreak, b.bothAdvancedWindows, b.totalWindows, fracB*100, b.convergeDur)

	aheadFrozenA := a.maxFrozenStreak[ahead]
	aheadFrozenB := b.maxFrozenStreak[ahead]
	t.Logf("HYPOTHESIS : receipt-keyed MinimizeSkew => ON holds the AHEAD shard %q (longer frozen streak) waiting for the lagged %q; OFF lets %q drain concurrently.",
		ahead, laggy, ahead)
	t.Logf("OBSERVED   : ahead-shard(%s) maxFrozenStreak ON=%d OFF=%d ; both-advanced ON=%.0f%% OFF=%.0f%% (Δ=%+.0f pts) ; converge ON=%s OFF=%s",
		ahead, aheadFrozenA, aheadFrozenB, fracA*100, fracB*100, (fracB-fracA)*100, a.convergeDur, b.convergeDur)

	switch {
	case aheadFrozenA >= 3 && aheadFrozenA > aheadFrozenB && fracB >= fracA:
		t.Logf("VERDICT    : SUSTAINED per-shard latency REPRODUCED the hold under ON (ahead shard %q frozen %ds) and the relief under OFF (both-advanced %.0f%% -> %.0f%%). ADR-0120 concurrent-drain WIN demonstrated locally.",
			ahead, aheadFrozenA, fracA*100, fracB*100)
	case fracB > fracA || aheadFrozenA > aheadFrozenB:
		t.Logf("VERDICT    : DIRECTIONAL signal toward the ADR-0120 win (OFF drains both shards more concurrently than ON), but the hold was weak on this single-host cluster. See numbers above; treat as suggestive, not conclusive.")
	default:
		t.Logf("VERDICT    : NO hold formed even with sustained per-shard delivery latency (ON and OFF behaved alike). This supports the COMMIT-TIMESTAMP hypothesis: vtgate's MinimizeSkew keys on the source commit time (clock-shared in one container), so delivery latency alone cannot reproduce the win locally — only a genuinely geo-distributed (cross-region) source can. The relaxation is proven SAFE here (exactly-once held in BOTH runs); only its throughput BENEFIT remains unobservable on single-host infra.")
	}
	t.Logf("===========================================================")
}
