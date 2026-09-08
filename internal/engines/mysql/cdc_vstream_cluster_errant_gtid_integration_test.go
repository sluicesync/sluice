//go:build integration && vitesscluster && chaos

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// EVIDENCE-GATHERING probe (decision support, not a product gate).
//
// ERRANT GTIDs vs the VStream lineage pre-flight.
//
// An errant GTID is a transaction executed DIRECTLY on a replica, so that
// replica's gtid_executed carries a UUID the primary never executed. The
// concern under test:
//
//	sluice does not compute its own position -- vgtidToShardGtidSlice
//	copies sg.GetGtid() verbatim out of the VGTID event, so the STREAMING
//	TABLET's position is what gets persisted. On PlanetScale the CDC tail
//	streams from REPLICA. If sluice streams from replica R and R carries
//	errant uuid-R:1, that UUID lands in the persisted position. On resume,
//	verifyVStreamPositionReachable probes keyspace:shard@<tablettype>
//	through vtgate, which may land on a DIFFERENT tablet whose
//	gtid_executed has no uuid-R at all -- gtidSetUUIDsSubset returns false
//	and the pre-flight refuses as "a different lineage", on the SAME
//	database.
//
// Three questions, in the operator's priority order:
//
//  1. Does an errant GTID actually REACH sluice's persisted position?
//     (Load-bearing. If no, the concern dissolves.)
//  2. If it does: routed at a tablet LACKING that UUID, does the
//     pre-flight refuse with the different-lineage message?
//  3. Does an errant transaction on something sluice does NOT sync still
//     land its GTID in the position? (Separates "the target actually
//     diverged" from "only the bookkeeping mentions a UUID".)
//
// The errant transaction this probe creates is deliberately OUTSIDE the
// synced keyspace entirely (its own `errantdb` database on the replica's
// mysqld), which is the strongest form of question 3: nothing sluice
// syncs is touched, so any UUID that shows up in the position is pure
// bookkeeping.
//
// The test RECORDS; it fails only if the evidence could not be gathered.

package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// errantVStreamDSN is chaosVStreamDSN with an explicit tablet type, so the
// probe can stream from REPLICA (the PlanetScale default) and later resume
// with the pre-flight routed at a DIFFERENT tablet type.
func errantVStreamDSN(cc *chaosCluster, tabletType string) string {
	return fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0"+
			"&vstream_tablet_type=%s&vstream_progress_timeout=60s&vstream_copy_progress_timeout=60s",
		cc.mysqlDSN, cc.grpcEndpoint, tabletType,
	)
}

// tabletExecOn runs (possibly multiple, ;-separated) statements against a
// specific tablet's mysqld through its mysqlctl socket. Returns the raw
// output and whether it succeeded.
func tabletExecOn(t *testing.T, cc *chaosCluster, service, uid, sqlText string) (string, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	out, err := cc.runCompose(ctx, "exec", "-T", service, "sh", "-c",
		fmt.Sprintf("mysql -u root --socket=%s -e %q", tabletSocketFor(uid), sqlText))
	if err != nil {
		t.Logf("  tablet exec on %s failed: %v\n%s", uid, err, out)
		return string(out), false
	}
	return string(out), true
}

// latestPos drains changes for up to d and returns the most recent
// position any change carried, plus how many changes were seen. This is
// exactly the value the pipeline persists (Change.Pos()).
func latestPos(t *testing.T, changes <-chan ir.Change, d time.Duration) (ir.Position, int) {
	t.Helper()
	deadline := time.After(d)
	var last ir.Position
	n := 0
	for {
		select {
		case c, ok := <-changes:
			if !ok {
				return last, n
			}
			if p := c.Pos(); p.Token != "" {
				last = p
			}
			n++
		case <-deadline:
			return last, n
		}
	}
}

// probeAt runs the production pre-flight's own SQL through a chosen
// vtgate target and reports the raw answers -- which arm the real
// pre-flight would take.
func probeAt(t *testing.T, cc *chaosCluster, pos ir.Position, tabletType, label string) {
	t.Helper()
	bare := resumeBareGTID(t, pos)
	dsn := strings.Replace(cc.mysqlDSN, "/"+chaosKeyspace+"?",
		"/"+chaosKeyspace+":"+chaosShard+"@"+tabletType+"?", 1)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Logf("probeAt %s: open: %v", label, err)
		return
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var contained int
	var executed string
	if err := db.QueryRowContext(ctx,
		"SELECT GTID_SUBSET(?, @@global.gtid_executed), @@global.gtid_executed", bare).Scan(&contained, &executed); err != nil {
		t.Logf("probeAt %s: query: %v", label, err)
		return
	}
	uuidsOK := gtidSetUUIDsSubset(bare, executed)
	t.Logf("==== PRE-FLIGHT SQL @ %s (target %s:%s@%s) ====", label, chaosKeyspace, chaosShard, tabletType)
	t.Logf("  resume (bare)                   = %s", bare)
	t.Logf("  that tablet's gtid_executed     = %s", strings.ReplaceAll(executed, "\n", " "))
	t.Logf("  GTID_SUBSET(resume, executed)   = %d", contained)
	t.Logf("  gtidSetUUIDsSubset(resume,exec) = %v", uuidsOK)
	switch {
	case contained == 1:
		t.Logf("  ==> ARM: no refusal path entered (position contained)")
	case uuidsOK:
		t.Logf("  ==> ARM: 'treating as replica lag' INFO carve-out (all UUIDs present, lower seqnos)")
	default:
		t.Logf("  ==> ARM: LINEAGE REFUSAL ('names a source UUID the shard has never executed')")
	}
}

// resumeAt opens a FRESH reader at tabletType and resumes from pos, with
// slog captured at DEBUG. Returns the StreamChanges error and the logs.
func resumeAt(t *testing.T, cc *chaosCluster, pos ir.Position, tabletType string) (error, string) {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(prev)

	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	reader, err := eng.OpenCDCReader(ctx, errantVStreamDSN(cc, tabletType))
	if err != nil {
		return err, buf.String()
	}
	_, serr := reader.StreamChanges(ctx, pos)
	if c, ok := reader.(interface{ Close() error }); ok {
		_ = c.Close()
	}
	return serr, buf.String()
}

func TestVitessChaos_ErrantGTID_PositionAndLineage(t *testing.T) {
	cc := startChaosCluster(t)
	defer cc.cleanup()

	const table = "errant_t"
	chaosSeedTable(t, cc.mysqlDSN, table)
	chaosInsertBatch(t, cc.mysqlDSN, table, 1, 50)
	time.Sleep(3 * time.Second)

	uuid100 := tabletScalarOn(t, cc, svcTabletPrimary, "0000000100", "SELECT @@global.server_uuid")
	uuid101 := tabletScalarOn(t, cc, svcTabletReplica, "0000000101", "SELECT @@global.server_uuid")
	t.Logf("PRIMARY  tablet 100 server_uuid = %s", uuid100)
	t.Logf("REPLICA  tablet 101 server_uuid = %s  (never a primary in this run)", uuid101)
	t.Logf("tablet 100 gtid_executed = %s",
		tabletScalarOn(t, cc, svcTabletPrimary, "0000000100", "SELECT @@global.gtid_executed"))
	t.Logf("tablet 101 gtid_executed = %s",
		tabletScalarOn(t, cc, svcTabletReplica, "0000000101", "SELECT @@global.gtid_executed"))

	// ---------------- Q1/Q3: does an errant GTID reach the position? -----
	// Stream the CDC tail from REPLICA -- the PlanetScale default and the
	// tablet type the concern is about.
	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	reader, err := eng.OpenCDCReader(ctx, errantVStreamDSN(cc, "replica"))
	if err != nil {
		t.Fatalf("OpenCDCReader(replica): %v", err)
	}
	changes, err := reader.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges(current): %v", err)
	}

	// Drive some primary traffic so the replica's stream has events, and
	// capture a BASELINE position (before any errant transaction).
	chaosInsertBatch(t, cc.mysqlDSN, table, 100, 30)
	posBefore, nBefore := latestPos(t, changes, 45*time.Second)
	t.Logf("---- BASELINE position after %d changes ----", nBefore)
	t.Logf("  token = %s", posBefore.Token)
	if posBefore.Token == "" {
		if c, ok := reader.(interface{ Close() error }); ok {
			_ = c.Close()
		}
		t.Fatal("no position was carried by any change; cannot gather the evidence")
	}
	t.Logf("  uuids = %v", gtidUUIDs(resumeBareGTID(t, posBefore)))

	// ---- create the ERRANT transaction directly on the REPLICA tablet ----
	// Deliberately outside the synced keyspace: its own database on the
	// replica's mysqld. Nothing sluice syncs is touched, so any UUID that
	// reaches the position is pure bookkeeping (question 3).
	t.Log("### creating an ERRANT transaction directly on replica tablet 101 (outside the synced keyspace)")
	if out, ok := tabletExecOn(t, cc, svcTabletReplica, "0000000101",
		"SET GLOBAL super_read_only=0; SET GLOBAL read_only=0; "+
			"CREATE DATABASE IF NOT EXISTS errantdb; "+
			"CREATE TABLE IF NOT EXISTS errantdb.e (id INT PRIMARY KEY); "+
			"INSERT INTO errantdb.e VALUES (1); "+
			"SET GLOBAL read_only=1; SET GLOBAL super_read_only=1;"); !ok {
		t.Logf("errant-transaction creation output:\n%s", out)
		t.Fatal("could not create the errant transaction on the replica; evidence not reachable on this rig")
	}
	execAfterErrant := tabletScalarOn(t, cc, svcTabletReplica, "0000000101", "SELECT @@global.gtid_executed")
	t.Logf("tablet 101 gtid_executed AFTER the errant transaction = %s", strings.ReplaceAll(execAfterErrant, "\n", " "))
	t.Logf("  uuids on 101 = %v", gtidUUIDs(execAfterErrant))
	t.Logf("tablet 100 gtid_executed (unchanged by an errant txn) = %s",
		tabletScalarOn(t, cc, svcTabletPrimary, "0000000100", "SELECT @@global.gtid_executed"))

	// Drive more primary traffic so the replica's stream advances PAST the
	// errant transaction, then re-read the position sluice would persist.
	chaosInsertBatch(t, cc.mysqlDSN, table, 500, 40)
	posAfter, nAfter := latestPos(t, changes, 60*time.Second)
	if c, ok := reader.(interface{ Close() error }); ok {
		_ = c.Close()
	}
	t.Logf("---- POST-ERRANT position after %d further changes ----", nAfter)
	t.Logf("  token = %s", posAfter.Token)
	if posAfter.Token == "" {
		t.Fatal("no position after the errant transaction; evidence not gathered")
	}
	bareAfter := resumeBareGTID(t, posAfter)
	uuidsAfter := gtidUUIDs(bareAfter)
	t.Logf("  bare  = %s", bareAfter)
	t.Logf("  uuids = %v", uuidsAfter)

	errantInPos := strings.Contains(strings.ToLower(bareAfter), strings.ToLower(strings.TrimSpace(uuid101)))
	t.Logf("==== Q1 ANSWER: errant replica UUID %s present in the persisted position = %v ====",
		uuid101, errantInPos)
	t.Logf("==== Q3 ANSWER: the errant txn touched ONLY errantdb (nothing sluice syncs); "+
		"its UUID in the position = %v ====", errantInPos)

	// ---------------- Q2: what does the pre-flight do on resume? ---------
	// The pre-flight probes keyspace:shard@<tablettype>. On this two-tablet
	// rig the REPLICA pool is a single tablet (101 -- the very tablet that
	// carries the errant UUID), so to land the probe on a tablet that LACKS
	// the UUID we route it at PRIMARY (tablet 100). On a real PlanetScale
	// shard with several replicas, a @replica probe landing on a sibling
	// replica produces the identical shape. Both routings are recorded.
	probeAt(t, cc, posAfter, "replica", "resume probe landing on the SAME tablet that produced the errant GTID")
	probeAt(t, cc, posAfter, "primary", "resume probe landing on a tablet WITHOUT the errant GTID")

	serrReplica, logsReplica := resumeAt(t, cc, posAfter, "replica")
	reportResume(t, "Q2 control: resume routed at REPLICA (the tablet holding the errant UUID)", serrReplica, logsReplica)

	serrPrimary, logsPrimary := resumeAt(t, cc, posAfter, "primary")
	reportResume(t, "Q2 experiment: resume routed at a tablet LACKING the errant UUID", serrPrimary, logsPrimary)

	t.Logf("==== Q2 ANSWER ====")
	t.Logf("  routed at the errant-carrying tablet : err=%v", serrReplica)
	t.Logf("  routed at a tablet lacking the UUID  : err=%v", serrPrimary)
	if serrPrimary != nil {
		t.Logf("  errors.Is(ErrPositionInvalid) = %v  (true today means auto-re-snapshot: DROP + re-copy)",
			errors.Is(serrPrimary, ir.ErrPositionInvalid))
		t.Logf("  message contains the different-lineage refusal = %v",
			strings.Contains(serrPrimary.Error(), "names a source UUID the shard has never"))
	}
}
