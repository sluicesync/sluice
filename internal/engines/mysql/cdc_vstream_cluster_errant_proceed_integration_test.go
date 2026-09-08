//go:build integration && vitesscluster && chaos

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// EVIDENCE-GATHERING probe (decision support, not a product gate).
//
// THE QUESTION. 400fbe90 keeps the errant-GTID arm REFUSING, on the
// strength of audit SLM-2's claim that vttablet's "GTIDSet Mismatch" does
// not reliably reach sluice -- vtgate marks the refusing tablet ignorable
// and BLOCKS waiting for another, so a `backup incremental` window
// deadline expires into a clean close and a link with an empty
// end_position. That claim was reasoned, not measured. This probe measures
// it, by BYPASSING the pre-flight and letting the resume actually happen.
//
// THE BYPASS IS TEST-SIDE AND EXACT. bypassedStreamChanges below replays
// [vstreamCDCReader.StreamChanges] verbatim -- shard resolution, start
// position, request build, client.VStream, startPump -- with the single
// pre-flight block (cdc_vstream.go, the verifyVStreamPositionReachable
// call) omitted. No production file is modified.
//
// THE RIG. The base compose is primary + ONE replica, so the REPLICA pool
// is a single tablet and a @replica probe can only ever land on the tablet
// that produced the errant GTID. docker-compose.errant3.yml adds a second
// replica (uid 102), which makes both scenarios reachable:
//
//	A (PERMANENT): the errant-carrying tablet is DELETED from the shard.
//	   No tablet holds the UUID. This is the shape that recurs forever.
//	B (TRANSIENT): the errant-carrying tablet is still serving, but the
//	   probe/stream may land on the sibling that lacks the UUID.
//
// The test RECORDS. It fails only if the evidence could not be gathered.

package mysql

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/proto/binlogdata"

	"sluicesync.dev/sluice/internal/ir"
)

// Distinct from the chaos harness ports so this stack never collides.
const (
	errant3MySQLPort = 15507
	errant3GRPCPort  = 15792

	svcTabletReplica2   = "vttablet-replica2"
	tabletAliasReplica2 = "zone1-0000000102"
	uidPrimary          = "0000000100"
	uidReplica          = "0000000101"
	uidReplica2         = "0000000102"
)

// errant3Cluster is the three-tablet stack. It embeds the chaos handle so
// the existing helpers (tabletScalarOn, vtctldclient, ...) work unchanged;
// runBoth layers the override file on for commands that must see the
// third tablet's service definition.
type errant3Cluster struct {
	*chaosCluster
	overrideFile string
}

func errant3OverridePath(t *testing.T) string {
	t.Helper()
	base := composeFilePath(t)
	return strings.Replace(base, "docker-compose.yml", "docker-compose.errant3.yml", 1)
}

// runBoth runs a compose subcommand with BOTH files layered, so services
// defined only in the override are addressable.
func (ec *errant3Cluster) runBoth(ctx context.Context, args ...string) ([]byte, error) {
	full := append([]string{
		"compose", "-f", ec.composeFile, "-f", ec.overrideFile, "-p", ec.project,
	}, args...)
	cmd := exec.CommandContext(ctx, ec.dockerBin, full...)
	cmd.Env = ec.baseEnv
	return cmd.CombinedOutput()
}

// startErrant3Cluster boots primary(100) + replica(101) + replica2(102).
func startErrant3Cluster(t *testing.T) *errant3Cluster {
	t.Helper()

	dockerBin := findDocker(t)
	composeFile := composeFilePath(t)
	project := fmt.Sprintf("sluice-errant3-%d", os.Getpid())

	baseEnv := append(
		os.Environ(),
		"COMPOSE_PROJECT="+project,
		fmt.Sprintf("VTGATE_MYSQL_PORT=%d", errant3MySQLPort),
		fmt.Sprintf("VTGATE_GRPC_PORT=%d", errant3GRPCPort),
	)

	cc := &chaosCluster{
		dockerBin:   dockerBin,
		composeFile: composeFile,
		project:     project,
		baseEnv:     baseEnv,
	}
	ec := &errant3Cluster{chaosCluster: cc, overrideFile: errant3OverridePath(t)}

	cc.cleanup = func() {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()
		if out, err := ec.runBoth(ctx, "down", "-v", "--remove-orphans"); err != nil {
			t.Logf("errant3 teardown: %v\n%s", err, out)
		}
	}

	upCtx, upCancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer upCancel()
	if out, err := ec.runBoth(upCtx, "up", "-d"); err != nil {
		cc.cleanup()
		t.Fatalf("errant3 compose up: %v\n%s", err, out)
	}

	cc.mysqlDSN = fmt.Sprintf(
		"root@tcp(127.0.0.1:%d)/%s?parseTime=true&interpolateParams=true",
		errant3MySQLPort, chaosKeyspace,
	)
	cc.grpcEndpoint = fmt.Sprintf("127.0.0.1:%d", errant3GRPCPort)
	cc.keyspace = chaosKeyspace

	if err := waitForWritablePrimary(t, cc.mysqlDSN, 6*time.Minute); err != nil {
		out, _ := ec.runBoth(context.Background(), "logs", "--tail", "60")
		cc.cleanup()
		t.Fatalf("errant3 cluster never reached writable PRIMARY: %v\nrecent logs:\n%s", err, out)
	}
	ec.waitForTabletCount(t, 3, 5*time.Minute)
	return ec
}

// waitForTabletCount blocks until the shard reports n registered tablets.
func (ec *errant3Cluster) waitForTabletCount(t *testing.T, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		out, err := ec.vtctldclient(ctx, "GetTablets", "--keyspace", chaosKeyspace)
		cancel()
		if err == nil && strings.Count(string(out), "zone1-") >= n {
			t.Logf("shard has %d tablets registered:\n%s", n, strings.TrimSpace(string(out)))
			return
		}
		time.Sleep(3 * time.Second)
	}
	t.Fatalf("shard never reached %d registered tablets within %s", n, timeout)
}

func (ec *errant3Cluster) tablets(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	out, err := ec.vtctldclient(ctx, "GetTablets", "--keyspace", chaosKeyspace)
	if err != nil {
		return fmt.Sprintf("(GetTablets failed: %v)", err)
	}
	return strings.TrimSpace(string(out))
}

// bypassedStreamChanges is StreamChanges with the pre-flight block omitted
// and NOTHING else changed. Test-side; production is untouched.
func bypassedStreamChanges(ctx context.Context, r *vstreamCDCReader, from ir.Position) (<-chan ir.Change, error) {
	r.mu.Lock()
	if r.streamStarted {
		r.mu.Unlock()
		return nil, fmt.Errorf("mysql/vstream: StreamChanges already called")
	}
	r.streamStarted = true
	r.err = nil
	r.mu.Unlock()

	if len(r.shards) == 0 {
		shards, err := resolveVStreamShards(ctx, r.cfg)
		if err != nil {
			return nil, err
		}
		r.shards = shards
	}
	startPos, err := r.resolveStartPosition(from)
	if err != nil {
		return nil, err
	}
	// --- verifyVStreamPositionReachable(ctx, decoded) DELIBERATELY OMITTED ---
	r.currentVgtid = startPos
	req, err := r.buildVStreamRequest(startPos)
	if err != nil {
		return nil, err
	}
	loopCtx, cancel := context.WithCancel(ctx)
	r.streamerCancel = cancel
	stream, err := r.client.VStream(loopCtx, req)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("mysql/vstream: open stream: %w", err)
	}
	out := make(chan ir.Change, vstreamChannelBuffer)
	r.startPump(loopCtx, cancel, req.GetTabletType(), stream, out)
	return out, nil
}

// rawVStreamOutcome is what a RAW VStream (no pump, no watchdog, no
// classifier) actually did with an unservable resume position.
type rawVStreamOutcome struct {
	messages   int
	heartbeats int
	nonHB      int
	firstErr   error
	elapsed    time.Duration
	eventKinds map[string]int
}

// rawVStreamProbe opens a VStream at the given position and dumps what
// comes back for up to d -- the Phase-A style ground truth: does vtgate
// ERROR, does it emit only heartbeats (the SLM-2 "blocks" claim), or does
// Recv block with nothing at all?
func rawVStreamProbe(t *testing.T, ec *errant3Cluster, pos ir.Position, tabletType string, d time.Duration, label string) rawVStreamOutcome {
	t.Helper()
	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), d+90*time.Second)
	defer cancel()

	reader, err := eng.OpenCDCReader(ctx, errant3DSN(ec, tabletType))
	if err != nil {
		t.Fatalf("rawVStreamProbe %s: OpenCDCReader: %v", label, err)
	}
	r, ok := reader.(*vstreamCDCReader)
	if !ok {
		t.Fatalf("rawVStreamProbe %s: reader is %T, want *vstreamCDCReader", label, reader)
	}
	defer func() { _ = r.Close() }()

	if len(r.shards) == 0 {
		shards, serr := resolveVStreamShards(ctx, r.cfg)
		if serr != nil {
			t.Fatalf("rawVStreamProbe %s: resolveVStreamShards: %v", label, serr)
		}
		r.shards = shards
	}
	startPos, err := r.resolveStartPosition(pos)
	if err != nil {
		t.Fatalf("rawVStreamProbe %s: resolveStartPosition: %v", label, err)
	}
	req, err := r.buildVStreamRequest(startPos)
	if err != nil {
		t.Fatalf("rawVStreamProbe %s: buildVStreamRequest: %v", label, err)
	}

	streamCtx, streamCancel := context.WithTimeout(ctx, d)
	defer streamCancel()
	start := time.Now()
	st, err := r.client.VStream(streamCtx, req)
	out := rawVStreamOutcome{eventKinds: map[string]int{}}
	if err != nil {
		out.firstErr = err
		out.elapsed = time.Since(start)
		t.Logf("==== RAW VSTREAM %s: VStream() returned SYNCHRONOUSLY: %v", label, err)
		return out
	}
	for {
		resp, rerr := st.Recv()
		if rerr != nil {
			out.firstErr = rerr
			break
		}
		out.messages++
		allHB := true
		for _, ev := range resp.GetEvents() {
			out.eventKinds[ev.GetType().String()]++
			if ev.GetType() != binlogdata.VEventType_HEARTBEAT {
				allHB = false
			}
		}
		if allHB {
			out.heartbeats++
		} else {
			out.nonHB++
		}
	}
	out.elapsed = time.Since(start)

	t.Logf("==== RAW VSTREAM %s (budget %s, tabletType=%s) ====", label, d, tabletType)
	t.Logf("  elapsed            = %s", out.elapsed.Round(time.Second))
	t.Logf("  messages received  = %d (heartbeat-only %d, carrying real events %d)",
		out.messages, out.heartbeats, out.nonHB)
	t.Logf("  event kinds        = %v", out.eventKinds)
	t.Logf("  terminal error     = %v", out.firstErr)
	switch {
	case out.firstErr != nil && !isDeadlineish(out.firstErr):
		t.Logf("  ==> LOUD: vtgate/tablet surfaced a real error")
	case out.nonHB > 0:
		t.Logf("  ==> SERVED: real events flowed; the position WAS servable")
	case out.messages > 0:
		t.Logf("  ==> BLOCKED-WITH-HEARTBEATS: no error, no data — the SLM-2 shape")
	default:
		t.Logf("  ==> SILENT BLOCK: not one message before the deadline")
	}
	return out
}

func isDeadlineish(err error) bool {
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "deadlineexceeded") ||
		strings.Contains(s, "deadline exceeded") ||
		strings.Contains(s, "context canceled")
}

func errant3DSN(ec *errant3Cluster, tabletType string) string {
	return fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0"+
			"&vstream_tablet_type=%s&vstream_progress_timeout=45s&vstream_copy_progress_timeout=60s",
		ec.mysqlDSN, ec.grpcEndpoint, tabletType,
	)
}

// pumpedResume runs the BYPASSED resume through sluice's real pump +
// liveness watchdog + error classifier and reports what an operator would
// see: did it deliver, error loudly, or sit there.
func pumpedResume(t *testing.T, ec *errant3Cluster, pos ir.Position, tabletType string, d time.Duration, label string) {
	t.Helper()
	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), d+2*time.Minute)
	defer cancel()

	reader, err := eng.OpenCDCReader(ctx, errant3DSN(ec, tabletType))
	if err != nil {
		t.Fatalf("pumpedResume %s: OpenCDCReader: %v", label, err)
	}
	r := reader.(*vstreamCDCReader)
	defer func() { _ = r.Close() }()

	start := time.Now()
	changes, serr := bypassedStreamChanges(ctx, r, pos)
	t.Logf("==== PUMPED RESUME (pre-flight bypassed) %s, tabletType=%s ====", label, tabletType)
	if serr != nil {
		t.Logf("  open error (synchronous) = %v", serr)
		return
	}
	deadline := time.After(d)
	n := 0
	closed := false
loop:
	for {
		select {
		case _, ok := <-changes:
			if !ok {
				closed = true
				break loop
			}
			n++
		case <-deadline:
			break loop
		}
	}
	t.Logf("  elapsed          = %s", time.Since(start).Round(time.Second))
	t.Logf("  changes received = %d", n)
	t.Logf("  channel closed   = %v", closed)
	t.Logf("  reader Err()     = %v", r.Err())
	switch {
	case r.Err() != nil:
		t.Logf("  ==> LOUD: the reader surfaced a terminal error an operator would see")
	case n > 0:
		t.Logf("  ==> SERVED: changes flowed")
	default:
		t.Logf("  ==> SILENT: no changes, no error within the budget")
	}
}

func TestVitessErrant_ProceedOrRefuse(t *testing.T) {
	ec := startErrant3Cluster(t)
	defer ec.cleanup()

	const table = "proceed_t"
	chaosSeedTable(t, ec.mysqlDSN, table)
	chaosInsertBatch(t, ec.mysqlDSN, table, 1, 50)
	time.Sleep(3 * time.Second)
	t.Logf("---- initial topology ----\n%s", ec.tablets(t))

	for _, tb := range []struct{ svc, uid string }{
		{svcTabletPrimary, uidPrimary}, {svcTabletReplica, uidReplica}, {svcTabletReplica2, uidReplica2},
	} {
		t.Logf("tablet %s server_uuid = %s", tb.uid,
			tabletScalarOn(t, ec.chaosCluster, tb.svc, tb.uid, "SELECT @@global.server_uuid"))
	}

	// ---- Pin the stream to replica 101 so the position it produces is the
	// one that carries the errant UUID: stop 102 while capturing. ----
	t.Log("### stopping replica2 (102) so the CDC tail must bind to replica 101")
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 90*time.Second)
	if out, err := ec.runBoth(stopCtx, "stop", svcTabletReplica2); err != nil {
		stopCancel()
		t.Fatalf("stop replica2: %v\n%s", err, out)
	}
	stopCancel()
	time.Sleep(10 * time.Second)

	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	reader, err := eng.OpenCDCReader(ctx, errant3DSN(ec, "replica"))
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	changes, err := reader.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	chaosInsertBatch(t, ec.mysqlDSN, table, 100, 30)
	_, n0 := latestPos(t, changes, 40*time.Second)
	t.Logf("baseline drained %d changes", n0)

	t.Log("### creating the ERRANT transaction directly on replica 101 (outside the synced keyspace)")
	if out, ok := tabletExecOn(t, ec.chaosCluster, svcTabletReplica, uidReplica,
		"SET GLOBAL super_read_only=0; SET GLOBAL read_only=0; "+
			"CREATE DATABASE IF NOT EXISTS errantdb; "+
			"CREATE TABLE IF NOT EXISTS errantdb.e (id INT PRIMARY KEY); "+
			"INSERT INTO errantdb.e VALUES (1); "+
			"SET GLOBAL read_only=1; SET GLOBAL super_read_only=1;"); !ok {
		t.Logf("%s", out)
		t.Fatal("could not create the errant transaction")
	}
	errantUUID := strings.TrimSpace(tabletScalarOn(t, ec.chaosCluster, svcTabletReplica, uidReplica,
		"SELECT @@global.server_uuid"))
	t.Logf("errant UUID (tablet 101) = %s", errantUUID)

	chaosInsertBatch(t, ec.mysqlDSN, table, 500, 40)
	pos, n1 := latestPos(t, changes, 60*time.Second)
	if c, ok := reader.(interface{ Close() error }); ok {
		_ = c.Close()
	}
	if pos.Token == "" {
		t.Fatal("no position captured; evidence not gathered")
	}
	bare := resumeBareGTID(t, pos)
	t.Logf("---- position after %d further changes ----", n1)
	t.Logf("  bare  = %s", bare)
	t.Logf("  uuids = %v", gtidUUIDs(bare))
	if !strings.Contains(strings.ToLower(bare), strings.ToLower(errantUUID)) {
		t.Fatalf("the captured position does NOT carry the errant UUID %s; the stream must have "+
			"bound elsewhere and the experiment cannot proceed", errantUUID)
	}
	t.Logf("CONFIRMED: the persisted position carries the errant UUID %s", errantUUID)

	// ---- bring replica2 back; it replicates from the PRIMARY so it never
	// sees the errant transaction. ----
	t.Log("### restarting replica2 (102)")
	startCtx, startCancel := context.WithTimeout(context.Background(), 120*time.Second)
	if out, err := ec.runBoth(startCtx, "start", svcTabletReplica2); err != nil {
		startCancel()
		t.Fatalf("start replica2: %v\n%s", err, out)
	}
	startCancel()
	ec.waitForTabletCount(t, 3, 4*time.Minute)
	time.Sleep(20 * time.Second)
	for _, tb := range []struct{ svc, uid string }{
		{svcTabletPrimary, uidPrimary}, {svcTabletReplica, uidReplica}, {svcTabletReplica2, uidReplica2},
	} {
		t.Logf("tablet %s gtid_executed = %s", tb.uid, strings.ReplaceAll(
			tabletScalarOn(t, ec.chaosCluster, tb.svc, tb.uid, "SELECT @@global.gtid_executed"), "\n", " ",
		))
	}

	// ================= SCENARIO B — TRANSIENT =================
	t.Log("################ SCENARIO B: the errant-carrying replica is STILL SERVING ################")
	for i := 1; i <= 3; i++ {
		rawVStreamProbe(t, ec, pos, "replica", 45*time.Second,
			fmt.Sprintf("B/attempt-%d (pool = {101 has UUID, 102 lacks it})", i))
	}
	pumpedResume(t, ec, pos, "replica", 60*time.Second, "B (pool has a tablet that can serve)")

	// ================= SCENARIO A — PERMANENT =================
	t.Log("################ SCENARIO A: DELETE the errant-carrying tablet from the shard ################")
	delCtx, delCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	if out, err := ec.runBoth(delCtx, "stop", svcTabletReplica); err != nil {
		t.Logf("stop replica 101: %v\n%s", err, out)
	}
	delCancel()
	dctx, dcancel := context.WithTimeout(context.Background(), 2*time.Minute)
	out, derr := ec.vtctldclient(dctx, "DeleteTablets", tabletAliasReplica)
	dcancel()
	t.Logf("DeleteTablets %s: err=%v out=%s", tabletAliasReplica, derr, strings.TrimSpace(string(out)))
	time.Sleep(15 * time.Second)
	t.Logf("---- topology after deletion ----\n%s", ec.tablets(t))

	rawVStreamProbe(t, ec, pos, "replica", 3*time.Minute, "A (NO tablet holds the errant UUID)")
	pumpedResume(t, ec, pos, "replica", 3*time.Minute, "A (NO tablet holds the errant UUID)")

	// vtgate's own account of what it did.
	lctx, lcancel := context.WithTimeout(context.Background(), 90*time.Second)
	vlogs, _ := ec.runBoth(lctx, "logs", "--tail", "400", svcVtgate)
	lcancel()
	t.Logf("==== vtgate log tail ====\n%s", lastLines(string(vlogs), 60))
	tctx, tcancel := context.WithTimeout(context.Background(), 90*time.Second)
	tlogs, _ := ec.runBoth(tctx, "logs", "--tail", "300", svcTabletReplica2)
	tcancel()
	t.Logf("==== replica2 (102) tablet log tail ====\n%s", lastLines(string(tlogs), 40))
}

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
