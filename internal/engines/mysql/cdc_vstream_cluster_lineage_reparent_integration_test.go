//go:build integration && vitesscluster && chaos

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// EVIDENCE-GATHERING probe (decision support, not a product gate).
//
// Question: would making the VStream lineage pre-flight refusal
// (verifyVStreamPositionReachable's "names a source UUID the shard has
// never executed" arm) TERMINAL ever halt a healthy sync on a ROUTINE
// Vitess/PlanetScale event -- specifically a PlannedReparentShard or an
// EmergencyReparentShard?
//
// The four TestVitessChaos_* scenarios cannot answer this: each opens
// StreamChanges ONCE, BEFORE the fault, and the in-process recovery path
// (Reopen / the pump's reconnect) does NOT re-run the pre-flight. The
// pre-flight only fires on a FRESH StreamChanges carrying a decoded
// position -- i.e. a warm resume by a NEW sluice process. So this probe
// does exactly that, on both sides of each reparent.
//
// It records, and never asserts on, the four things asked for:
//  1. whether the lineage refusal fired,
//  2. whether the "treating as replica lag" carve-out fired,
//  3. whether a resume returned ir.ErrPositionInvalid (which is what
//     auto-re-snapshots today),
//  4. the shard's @@global.gtid_executed BEFORE and AFTER each reparent,
//     with the UUID sets compared -- the mechanical claim the whole answer
//     rests on.
//
// It FAILS only if the harness could not gather the evidence.

package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	topodata "vitess.io/vitess/go/vt/proto/topodata"
)

// tabletSocketFor is primaryTabletSocket generalised to any tablet uid.
func tabletSocketFor(uid string) string {
	return "/vt/vtdataroot/vt_" + uid + "/mysql.sock"
}

// tabletScalarOn runs a single-column query against ANY tablet's mysqld
// (via its mysqlctl socket inside that tablet's container) and returns the
// value row. tabletScalar in the purged test is hardwired to the uid-100
// service; this probe must read BOTH tablets.
func tabletScalarOn(t *testing.T, cc *chaosCluster, service, uid, query string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	out, err := cc.runCompose(ctx, "exec", "-T", service, "sh", "-c",
		fmt.Sprintf("mysql -u root --socket=%s -e %q", tabletSocketFor(uid), query))
	if err != nil {
		t.Logf("  (tablet %s/%s unreachable for %q: %v)", service, uid, query, err)
		return ""
	}
	text := strings.TrimRight(string(out), "\n")
	lines := strings.Split(text, "\n")
	if len(lines) < 2 {
		return ""
	}
	return strings.TrimSpace(lines[len(lines)-1])
}

// vtgateShardScalar reads a scalar through vtgate at the SAME
// shard-scoped, tablet-type-routed target the production pre-flight uses
// (shardScopedTarget), so the value observed here is the value the
// pre-flight would compare against.
func vtgateShardScalar(t *testing.T, cc *chaosCluster, query string) string {
	t.Helper()
	dsn := strings.Replace(cc.mysqlDSN, "/"+chaosKeyspace+"?",
		"/"+chaosKeyspace+":"+chaosShard+"@primary?", 1)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Logf("  (vtgate probe open failed: %v)", err)
		return ""
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var v string
	if err := db.QueryRowContext(ctx, query).Scan(&v); err != nil {
		t.Logf("  (vtgate probe %q failed: %v)", query, err)
		return ""
	}
	return v
}

// gtidUUIDs extracts the sorted, lowercased source-UUID set of a GTID set.
func gtidUUIDs(set string) []string {
	seen := map[string]bool{}
	for _, part := range strings.Split(set, ",") {
		if i := strings.IndexByte(part, ':'); i > 0 {
			seen[strings.ToLower(strings.TrimSpace(part[:i]))] = true
		}
	}
	out := make([]string, 0, len(seen))
	for u := range seen {
		out = append(out, u)
	}
	sort.Strings(out)
	return out
}

// snapshotShardState records everything interesting about the shard at one
// instant, from three vantage points, and logs it.
func snapshotShardState(t *testing.T, cc *chaosCluster, label string) (viaVtgate string) {
	t.Helper()
	t.Logf("---- SHARD STATE @ %s ----", label)
	for _, tb := range []struct{ svc, uid string }{
		{svcTabletPrimary, "0000000100"},
		{svcTabletReplica, "0000000101"},
	} {
		uuid := tabletScalarOn(t, cc, tb.svc, tb.uid, "SELECT @@global.server_uuid")
		exec := tabletScalarOn(t, cc, tb.svc, tb.uid, "SELECT @@global.gtid_executed")
		ro := tabletScalarOn(t, cc, tb.svc, tb.uid, "SELECT @@global.super_read_only")
		t.Logf("  tablet %s (%s): server_uuid=%s super_read_only=%s", tb.uid, tb.svc, uuid, ro)
		t.Logf("    gtid_executed = %s", strings.ReplaceAll(exec, "\n", " "))
		t.Logf("    uuid set      = %v", gtidUUIDs(exec))
	}
	viaVtgate = vtgateShardScalar(t, cc, "SELECT @@global.gtid_executed")
	t.Logf("  via vtgate %s: gtid_executed = %s",
		shardScopedTarget(chaosKeyspace, chaosShard, topodata.TabletType_PRIMARY),
		strings.ReplaceAll(viaVtgate, "\n", " "))
	t.Logf("    uuid set      = %v", gtidUUIDs(viaVtgate))
	return viaVtgate
}

// capturePosition cold-starts a snapshot stream and returns the VStream
// resume position it ends at -- exactly the token a sluice sync persists.
func capturePosition(t *testing.T, cc *chaosCluster, table string, wantRows int) ir.Position {
	t.Helper()
	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	stream, err := eng.OpenSnapshotStream(ctx, chaosVStreamDSN(cc))
	if err != nil {
		t.Fatalf("capturePosition: OpenSnapshotStream: %v", err)
	}
	rowsCh, err := stream.Rows.ReadRows(ctx, chaosTable(table))
	if err != nil {
		_ = stream.Close()
		t.Fatalf("capturePosition: ReadRows: %v", err)
	}
	n := 0
	for range rowsCh {
		n++
	}
	if rerr := stream.Rows.Err(); rerr != nil {
		_ = stream.Close()
		t.Fatalf("capturePosition: Rows.Err = %v", rerr)
	}
	pos := stream.Position
	_ = stream.Close()
	t.Logf("captured resume position after copying %d rows (wanted >=%d): %s", n, wantRows, pos.Token)
	if pos.Token == "" {
		t.Fatal("capturePosition: empty position token")
	}
	return pos
}

// warmResume is the actual experiment: a FRESH reader resuming from pos,
// with slog captured at DEBUG so every pre-flight line is visible. It
// returns the StreamChanges error (nil = the resume was accepted) and the
// captured log text.
func warmResume(t *testing.T, cc *chaosCluster, pos ir.Position, label string) (error, string) {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(prev)

	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	reader, err := eng.OpenCDCReader(ctx, chaosVStreamDSN(cc))
	if err != nil {
		return err, buf.String()
	}
	_, serr := reader.StreamChanges(ctx, pos)
	if c, ok := reader.(interface{ Close() error }); ok {
		_ = c.Close()
	}
	return serr, buf.String()
}

// reportResume prints the answers for one warm-resume attempt.
func reportResume(t *testing.T, label string, serr error, logs string) {
	t.Helper()
	t.Logf("==== WARM RESUME: %s ====", label)
	if serr == nil {
		t.Logf("  StreamChanges error: <nil>  (the resume was ACCEPTED)")
	} else {
		t.Logf("  StreamChanges error: %v", serr)
		t.Logf("  errors.Is(ErrPositionInvalid) = %v  (true today means auto-re-snapshot: DROP + re-copy)",
			errors.Is(serr, ir.ErrPositionInvalid))
	}
	for _, probe := range []struct{ name, needle string }{
		{"LINEAGE REFUSAL fired", "names a source UUID the shard has never"},
		{"'different lineage' text", "different lineage"},
		{"UNVERIFIED-INSTANCE-IDENTITY", unverifiedInstanceIdentityMarker},
		{"'treating as replica lag' carve-out", "treating as replica lag"},
		{"purged pre-flight refusal", "has purged GTIDs not present"},
		{"probe could not open", "could not open probe connection"},
		{"GTID_SUBSET probe failed", "GTID_SUBSET probe failed"},
	} {
		hit := strings.Contains(logs, probe.needle) ||
			(serr != nil && strings.Contains(serr.Error(), probe.needle))
		t.Logf("  %-38s : %v", probe.name, hit)
	}
	if strings.TrimSpace(logs) != "" {
		t.Logf("  --- captured log ---\n%s", logs)
	}
}

// resumeBareGTID decodes the persisted position the way PRODUCTION does
// (decodeVStreamPos -> per-shard Gtid -> stripGTIDFlavor) and returns the
// bare GTID set for the shard under test. Parsing the raw token as if it
// were a GTID set is wrong -- the token is a JSON array -- and reporting
// that would be a harness artifact, not evidence.
func resumeBareGTID(t *testing.T, pos ir.Position) string {
	t.Helper()
	decoded, ok, err := decodeVStreamPos(pos)
	if err != nil || !ok || len(decoded) == 0 {
		t.Fatalf("resumeBareGTID: decodeVStreamPos(ok=%v, err=%v) returned %d shards", ok, err, len(decoded))
	}
	return stripGTIDFlavor(decoded[0].Gtid)
}

// preflightProbe re-runs, verbatim, the two SQL questions the production
// pre-flight asks (verifyVStreamPositionReachable), through the SAME
// shard-scoped target, and reports the raw answers.
func preflightProbe(t *testing.T, cc *chaosCluster, pos ir.Position, label string) {
	t.Helper()
	bare := resumeBareGTID(t, pos)
	dsn := strings.Replace(cc.mysqlDSN, "/"+chaosKeyspace+"?",
		"/"+chaosKeyspace+":"+chaosShard+"@primary?", 1)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Logf("preflightProbe %s: open: %v", label, err)
		return
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var contained int
	var executed string
	if err := db.QueryRowContext(ctx,
		"SELECT GTID_SUBSET(?, @@global.gtid_executed), @@global.gtid_executed", bare).Scan(&contained, &executed); err != nil {
		t.Logf("preflightProbe %s: query: %v", label, err)
		return
	}
	t.Logf("==== PRE-FLIGHT PROBE (production SQL, verbatim): %s ====", label)
	t.Logf("  resume (bare)                   = %s", bare)
	t.Logf("  shard @@global.gtid_executed    = %s", executed)
	t.Logf("  GTID_SUBSET(resume, executed)   = %d   (1 = no refusal path entered at all)", contained)
	t.Logf("  resume uuids                    = %v", gtidUUIDs(bare))
	t.Logf("  executed uuids                  = %v", gtidUUIDs(executed))
	t.Logf("  gtidSetUUIDsSubset(resume,exec) = %v   (false AND contained=0 means LINEAGE REFUSAL)",
		gtidSetUUIDsSubset(bare, executed))
}

// TestVitessChaos_LineagePreflight_AcrossReparents is the probe.
func TestVitessChaos_LineagePreflight_AcrossReparents(t *testing.T) {
	cc := startChaosCluster(t)
	defer cc.cleanup()

	const table = "lineage_t"
	const seedRows = 100
	chaosSeedTable(t, cc.mysqlDSN, table)
	chaosInsertBatch(t, cc.mysqlDSN, table, 1, seedRows)
	time.Sleep(3 * time.Second)

	// ---------- BEFORE ANY FAULT ----------
	before := snapshotShardState(t, cc, "BEFORE any reparent")
	pos0 := capturePosition(t, cc, table, seedRows)
	t.Logf("pos0 resume uuid set = %v", gtidUUIDs(resumeBareGTID(t, pos0)))
	preflightProbe(t, cc, pos0, "pos0, BEFORE any reparent")

	serr, logs := warmResume(t, cc, pos0, "baseline (no fault yet)")
	reportResume(t, "baseline (no fault yet)", serr, logs)

	// ---------- FAULT A: PlannedReparentShard 100 -> 101 ----------
	chaosInsertBatch(t, cc.mysqlDSN, table, seedRows+1, 50)
	t.Log("### injecting PlannedReparentShard -> zone1-0000000101")
	cc.plannedReparent(t, tabletAliasReplica)
	cc.waitForPrimaryAlias(t, tabletAliasReplica, 3*time.Minute)
	cc.waitForWritablePrimaryHandle(t, 2*time.Minute)

	afterPRS := snapshotShardState(t, cc, "AFTER PlannedReparentShard")
	reportUUIDPreservation(t, "PRS", before, afterPRS, resumeBareGTID(t, pos0))
	preflightProbe(t, cc, pos0, "pos0, AFTER PRS")

	serr, logs = warmResume(t, cc, pos0, "resume from PRE-PRS position, AFTER PRS")
	reportResume(t, "resume from PRE-PRS position, AFTER PRS", serr, logs)

	// A position captured under the NEW primary -- so the ERS below is
	// asked to accept a position naming the uid-101 UUID.
	chaosInsertBatch(t, cc.mysqlDSN, table, seedRows+200, 50)
	time.Sleep(2 * time.Second)
	pos1 := capturePosition(t, cc, table, seedRows)
	t.Logf("pos1 (post-PRS) resume uuid set = %v", gtidUUIDs(resumeBareGTID(t, pos1)))

	// ---------- FAULT B: kill the primary, EmergencyReparentShard 101 -> 100 ----------
	t.Log("### killing current primary (zone1-0000000101) and EmergencyReparentShard -> zone1-0000000100")
	cc.killContainer(t, svcTabletReplica, "SIGKILL")
	cc.emergencyReparent(t, tabletAliasPrim)
	cc.waitForPrimaryAlias(t, tabletAliasPrim, 4*time.Minute)
	cc.waitForWritablePrimaryHandle(t, 3*time.Minute)

	afterERS := snapshotShardState(t, cc, "AFTER EmergencyReparentShard")
	reportUUIDPreservation(t, "ERS", afterPRS, afterERS, resumeBareGTID(t, pos1))
	preflightProbe(t, cc, pos0, "pos0, AFTER PRS+ERS")
	preflightProbe(t, cc, pos1, "pos1, AFTER ERS")

	serr, logs = warmResume(t, cc, pos0, "resume from ORIGINAL (pre-PRS) position, AFTER PRS+ERS")
	reportResume(t, "resume from ORIGINAL (pre-PRS) position, AFTER PRS+ERS", serr, logs)

	serr, logs = warmResume(t, cc, pos1, "resume from POST-PRS position, AFTER ERS")
	reportResume(t, "resume from POST-PRS position, AFTER ERS", serr, logs)
}

// reportUUIDPreservation answers item 4 directly: does the promoted
// primary's gtid_executed still carry every UUID the earlier state (and the
// resume position) named?
func reportUUIDPreservation(t *testing.T, event, beforeSet, afterSet, resumeBare string) {
	t.Helper()
	b, a := gtidUUIDs(beforeSet), gtidUUIDs(afterSet)
	t.Logf("==== UUID PRESERVATION ACROSS %s ====", event)
	t.Logf("  before uuids = %v", b)
	t.Logf("  after  uuids = %v", a)
	t.Logf("  after CONTAINS-ALL before (every pre-%s UUID still present) = %v", event,
		gtidSetUUIDsSubset(beforeSet, afterSet))
	bare := resumeBare
	t.Logf("  resume position uuids = %v", gtidUUIDs(bare))
	t.Logf("  after CONTAINS-ALL resume-position UUIDs = %v  <-- FALSE would trip the lineage refusal",
		gtidSetUUIDsSubset(bare, afterSet))
}
