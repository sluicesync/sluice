//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// VStream integration tests using vitess/vttestserver — Vitess's
// all-in-one test harness that runs vtcombo (vtgate + vttablet +
// etcd in a single binary) plus an embedded MySQL with binlogs
// enabled. Gated behind a separate `vstream` build tag because the
// vttestserver image is heavier (~700 MB) than the plain mysql:8.0
// the default integration suite uses, so the standard `make
// test-it` shouldn't pull it on every run.
//
// These tests prove the FlavorPlanetScale engine works against
// vanilla Vitess deployments — not just PlanetScale's hosted
// service. Real PlanetScale verification lives in
// cdc_vstream_psverify_test.go (psverify build tag).
//
// Usage from a shell:
//
//	go test -tags='integration vstream' -v -count=1 -timeout=15m \
//	  -run 'TestVStream_VTTestServer' ./internal/engines/mysql/...

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"sluicesync.dev/sluice/internal/ir"
)

// startVTTestServer boots a vitess/vttestserver container with one
// keyspace, one shard, returns the MySQL DSN (for SQL setup) and
// the gRPC endpoint (for VStream).
//
// The container takes a noticeably long time to start — ~20–40
// seconds for vtcombo to wire up the underlying MySQL, register
// the keyspace, and start serving on gRPC. The wait strategy looks
// for the canonical "ready" log line vttestserver emits.
func startVTTestServer(t *testing.T) (mysqlDSN, grpcEndpoint, keyspace string, cleanup func()) {
	return startVTTestServerWithShards(t, 1)
}

// startVTTestServerWithShards is the multi-shard variant. With
// numShards>1 vttestserver creates a sharded keyspace using the
// default split-on-high-bit vschema, producing shards "-80" and
// "80-" for numShards=2 (and proportionally more for higher
// values; sluice's tests stop at 2, which is enough to validate
// the multi-shard receive path without ballooning startup time).
//
// Boot time scales roughly linearly with shard count: 2 shards
// adds ~10–15s on top of the single-shard baseline. Set test
// timeouts accordingly.
func startVTTestServerWithShards(t *testing.T, numShards int) (mysqlDSN, grpcEndpoint, keyspace string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	const (
		basePort      = 33574 // vttestserver default base; MySQL is base+3, gRPC is base+1
		mysqlPortBase = "33577/tcp"
		grpcPortBase  = "33575/tcp"
	)

	keyspace = "test"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	req := testcontainers.ContainerRequest{
		Image:        "vitess/vttestserver:mysql80",
		ExposedPorts: []string{mysqlPortBase, grpcPortBase},
		Env: map[string]string{
			"PORT":       fmt.Sprintf("%d", basePort),
			"KEYSPACES":  keyspace,
			"NUM_SHARDS": fmt.Sprintf("%d", numShards),
			// Without an override, vttestserver binds the MySQL
			// listener to 127.0.0.1 (container-local), which makes
			// the host-side port mapping useless. 0.0.0.0 binds on
			// all interfaces so the published port reaches us.
			"MYSQL_BIND_HOST": "0.0.0.0",
			// ENABLE_ONLINE_DDL makes vttestserver accept online
			// schema-change strategies (ddl_strategy='vitess'),
			// which build a shadow table, VReplication-copy into it,
			// then atomically cut over — emitting the Vitess-internal
			// `_vt_vrp_*` artifacts ADR-0073 (c) must exclude. It
			// defaults to true in vttestserver's run.sh, but we set
			// it explicitly so the online-DDL test surface doesn't
			// depend on an image default that could change. The
			// sibling knobs (FOREIGN_KEY_MODE, KEYSPACES, NUM_SHARDS)
			// are already covered above / left at their image
			// defaults; noted here so a future FK-cutover test knows
			// where to set FOREIGN_KEY_MODE.
			"ENABLE_ONLINE_DDL": "true",
		},
		// vttestserver logs "Local cluster started." once vtcombo
		// has finished bringing up the embedded MySQL + vtgate +
		// vttablet stack and the gRPC listener is up. Pair with a
		// port check so the test doesn't race against the listener
		// finishing its bind.
		WaitingFor: wait.ForAll(
			wait.ForLog("Local cluster started."),
			wait.ForListeningPort(grpcPortBase),
			wait.ForListeningPort(mysqlPortBase),
		).WithStartupTimeoutDefault(4 * time.Minute),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start vttestserver: %v", err)
	}

	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}

	host, err := container.Host(ctx)
	if err != nil {
		terminate()
		t.Fatalf("container host: %v", err)
	}
	mysqlPort, err := container.MappedPort(ctx, mysqlPortBase)
	if err != nil {
		terminate()
		t.Fatalf("mapped mysql port: %v", err)
	}
	grpcPort, err := container.MappedPort(ctx, grpcPortBase)
	if err != nil {
		terminate()
		t.Fatalf("mapped grpc port: %v", err)
	}

	mysqlDSN = fmt.Sprintf(
		// vttestserver doesn't require auth on its embedded MySQL;
		// the user/passwd are arbitrary. parseTime=true keeps the
		// driver decoding TIMESTAMP into time.Time so the IR
		// contract holds.
		"root@tcp(%s:%d)/%s?parseTime=true&interpolateParams=true",
		host, mysqlPort.Num(), keyspace,
	)
	grpcEndpoint = fmt.Sprintf("%s:%d", host, grpcPort.Num())
	return mysqlDSN, grpcEndpoint, keyspace, terminate
}

// TestVStream_VTTestServer_BasicChangeStream is the spine
// integration test for the VStream reader against vanilla Vitess:
// open against vttestserver with vstream_transport=plaintext +
// vstream_auth=none, perform DML on the SQL side, and assert the
// resulting ir.Insert / ir.Update / ir.Delete events arrive on the
// CDC channel.
//
// Mirrors the binlog reader's TestCDCReader_BasicChangeStream
// shape so the diff between the two CDC paths is purely the
// transport.
func TestVStream_VTTestServer_BasicChangeStream(t *testing.T) {
	mysqlDSN, grpcEndpoint, keyspace, cleanup := startVTTestServer(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE users (
			id     BIGINT       NOT NULL AUTO_INCREMENT,
			email  VARCHAR(255) NOT NULL,
			active TINYINT(1)   NOT NULL DEFAULT 1,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
	`
	applyVTTestSQL(t, mysqlDSN, seedDDL)

	// Build a sluice DSN with the VStream knobs set for
	// vttestserver: gRPC endpoint override, plaintext transport,
	// no auth, and the vttestserver shard convention ("0" instead
	// of PlanetScale's "-" default).
	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
		mysqlDSN, grpcEndpoint,
	)

	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	// Stream from "current" — head of the binlog at request time.
	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	// Settle window: vtgate's stream takes a moment to register at
	// "current"; events generated too quickly can land just before
	// the boundary and get dropped.
	time.Sleep(2 * time.Second)

	const dml = `
		INSERT INTO users (email, active) VALUES ('alice@example.com', 1);
		INSERT INTO users (email, active) VALUES ('bob@example.com', 0);
		UPDATE users SET active = 0 WHERE email = 'alice@example.com';
		DELETE FROM users WHERE email = 'bob@example.com';
	`
	applyVTTestSQL(t, mysqlDSN+"&multiStatements=true", dml)

	got := drainVTTestChanges(t, ctx, changes, 4, 60*time.Second)
	if len(got) != 4 {
		if cdcRdr, ok := rdr.(*vstreamCDCReader); ok {
			if streamErr := cdcRdr.Err(); streamErr != nil {
				t.Fatalf("got %d changes; want 4 (stream error: %v)", len(got), streamErr)
			}
		}
		t.Fatalf("got %d changes; want 4", len(got))
	}

	// Verify the four-event sequence: insert alice, insert bob,
	// update alice, delete bob.
	insAlice, ok := got[0].(ir.Insert)
	if !ok {
		t.Fatalf("got[0] = %T; want ir.Insert", got[0])
	}
	if email, _ := insAlice.Row["email"].(string); email != "alice@example.com" {
		t.Errorf("got[0].Row[email] = %#v; want alice@example.com", insAlice.Row["email"])
	}
	// Cross-engine bool path: TINYINT(1)=1 must surface as Go bool
	// true through the VStream decoder, not int64(1).
	if active, ok := insAlice.Row["active"].(bool); !ok || !active {
		t.Errorf("got[0].Row[active] = %#v (%T); want bool(true) (TINYINT(1)→bool)", insAlice.Row["active"], insAlice.Row["active"])
	}

	insBob, ok := got[1].(ir.Insert)
	if !ok {
		t.Fatalf("got[1] = %T; want ir.Insert", got[1])
	}
	if email, _ := insBob.Row["email"].(string); email != "bob@example.com" {
		t.Errorf("got[1].Row[email] = %#v; want bob@example.com", insBob.Row["email"])
	}
	// And TINYINT(1)=0 must surface as bool false.
	if active, ok := insBob.Row["active"].(bool); !ok || active {
		t.Errorf("got[1].Row[active] = %#v (%T); want bool(false)", insBob.Row["active"], insBob.Row["active"])
	}

	upd, ok := got[2].(ir.Update)
	if !ok {
		t.Fatalf("got[2] = %T; want ir.Update", got[2])
	}
	if email, _ := upd.After["email"].(string); email != "alice@example.com" {
		t.Errorf("got[2].After[email] = %#v; want alice@example.com", upd.After["email"])
	}
	// Update flips active true → false; both halves are bool.
	if before, ok := upd.Before["active"].(bool); !ok || !before {
		t.Errorf("got[2].Before[active] = %#v (%T); want bool(true)", upd.Before["active"], upd.Before["active"])
	}
	if after, ok := upd.After["active"].(bool); !ok || after {
		t.Errorf("got[2].After[active] = %#v (%T); want bool(false)", upd.After["active"], upd.After["active"])
	}

	del, ok := got[3].(ir.Delete)
	if !ok {
		t.Fatalf("got[3] = %T; want ir.Delete", got[3])
	}
	if email, _ := del.Before["email"].(string); email != "bob@example.com" {
		t.Errorf("got[3].Before[email] = %#v; want bob@example.com", del.Before["email"])
	}

	// Position bookkeeping: every emitted change must carry a
	// non-empty position the engine can decode.
	for i, c := range got {
		if c.Pos().Engine != engineNameVStream {
			t.Errorf("got[%d].Pos.Engine = %q; want %q", i, c.Pos().Engine, engineNameVStream)
		}
		if c.Pos().Token == "" {
			t.Errorf("got[%d].Pos.Token is empty", i)
		}
		if shards, ok, err := decodeVStreamPos(c.Pos()); err != nil || !ok || len(shards) == 0 {
			t.Errorf("got[%d].Pos failed to decode: ok=%v err=%v shards=%v", i, ok, err, shards)
		}
	}

	// Quiet the unused-import warning for keyspace; surfacing it
	// in a log line keeps the value useful when the test logs are
	// inspected.
	t.Logf("keyspace = %q", keyspace)
}

// TestVStream_VTTestServer_Truncate covers the Phase C TRUNCATE
// path end-to-end: insert a row, TRUNCATE the table, assert both
// events arrive in order with the expected shapes. Mirrors the
// binlog reader's TestCDCReader_Truncate against the VStream path.
func TestVStream_VTTestServer_Truncate(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
	`
	applyVTTestSQL(t, mysqlDSN, seedDDL)

	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
		mysqlDSN, grpcEndpoint,
	)

	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	time.Sleep(2 * time.Second)

	const dml = `
		INSERT INTO users (email) VALUES ('alice@example.com');
		TRUNCATE TABLE users;
	`
	applyVTTestSQL(t, mysqlDSN+"&multiStatements=true", dml)

	got := drainVTTestChanges(t, ctx, changes, 2, 60*time.Second)
	if len(got) != 2 {
		if cdcRdr, ok := rdr.(*vstreamCDCReader); ok {
			if streamErr := cdcRdr.Err(); streamErr != nil {
				t.Fatalf("got %d changes; want 2 (stream error: %v)", len(got), streamErr)
			}
		}
		t.Fatalf("got %d changes; want 2 (1 Insert + 1 Truncate)", len(got))
	}

	if _, ok := got[0].(ir.Insert); !ok {
		t.Errorf("got[0] = %T; want ir.Insert", got[0])
	}
	tr, ok := got[1].(ir.Truncate)
	if !ok {
		t.Fatalf("got[1] = %T; want ir.Truncate", got[1])
	}
	if tr.Table != "users" {
		t.Errorf("truncate.Table = %q; want users", tr.Table)
	}
	if tr.Pos().Engine != engineNameVStream || tr.Pos().Token == "" {
		t.Errorf("truncate.Pos = %+v; want non-empty %s position", tr.Pos(), engineNameVStream)
	}
}

// TestVStream_VTTestServer_SnapshotStream exercises the
// FlavorPlanetScale snapshot+CDC handoff via VStream's built-in
// COPY mode against vanilla Vitess. Mirrors the binlog reader's
// TestSnapshotStream_NoGapNoOverlap shape (in
// cdc_snapshot_integration_test.go) so the diff between the two
// snapshot paths is purely the underlying mechanism.
//
// Sequence:
//
//  1. Seed R1..R5 (committed BEFORE OpenSnapshotStream).
//  2. Open SnapshotStream — drains the COPY phase, captures VGTID.
//  3. INSERT R6 on a SEPARATE connection (commits AFTER COPY).
//  4. Drain stream.Rows.ReadRows for users → expect exactly R1..R5.
//  5. Drain stream.Changes.StreamChanges → expect exactly the R6 insert.
//
// Properties under test:
//
//   - If R6 appears in step 4: the COPY phase didn't have a clean
//     boundary (snapshot included a post-COPY change).
//   - If R6 doesn't appear in step 5: there's a gap (CDC missed
//     events between snapshot-capture and stream resume).
func TestVStream_VTTestServer_SnapshotStream(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
	`
	applyVTTestSQL(t, mysqlDSN, seedDDL)

	const seedRows = `
		INSERT INTO users (email) VALUES ('r1@example.com');
		INSERT INTO users (email) VALUES ('r2@example.com');
		INSERT INTO users (email) VALUES ('r3@example.com');
		INSERT INTO users (email) VALUES ('r4@example.com');
		INSERT INTO users (email) VALUES ('r5@example.com');
	`
	applyVTTestSQL(t, mysqlDSN+"&multiStatements=true", seedRows)

	// vttestserver's schema tracker watches the binlog for DDL
	// events and refreshes vttablet's schema engine accordingly —
	// vstream's COPY phase needs the table visible there
	// (`uvstreamer.buildTablePlan`) or it errors with "stream needs
	// a position or a table to copy". The tracker is async so we
	// give it a couple of seconds to catch up before opening the
	// snapshot stream.
	time.Sleep(3 * time.Second)

	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
		mysqlDSN, grpcEndpoint,
	)

	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	stream, err := eng.OpenSnapshotStream(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenSnapshotStream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// ADR-0071 streaming contract: OpenSnapshotStream returns immediately
	// with a ZERO Position; the COPY drains in a background pump and the
	// Position is finalised at the global COPY_COMPLETED. So the snapshot
	// rows AND the captured Position are only valid AFTER ReadRows is
	// fully drained — checking them right after open is a race (the pump
	// wins on a fast box but loses under -race on CI). Drain first, then
	// assert; insert the post-snapshot row only after the drain so it is
	// deterministically a CDC event, not a COPY-vs-insert race.

	// Step 3 — drain bulk rows (the snapshot is complete when the channel
	// closes).
	usersTable := &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
			{Name: "email", Type: ir.Varchar{Length: 255}},
		},
	}
	rowsCh, err := stream.Rows.ReadRows(ctx, usersTable)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	bulkEmails := make([]string, 0, 5)
	for r := range rowsCh {
		s, _ := r["email"].(string)
		bulkEmails = append(bulkEmails, s)
	}
	sort.Strings(bulkEmails)
	want := []string{"r1@example.com", "r2@example.com", "r3@example.com", "r4@example.com", "r5@example.com"}
	if !equalSorted(bulkEmails, want) {
		t.Fatalf("bulk rows = %v; want exactly %v (overlap or missing rows)", bulkEmails, want)
	}

	// Step 4 — the Position is finalised now that the COPY has drained.
	if stream.Position.Engine != engineNameVStream {
		t.Errorf("Position.Engine = %q; want %q", stream.Position.Engine, engineNameVStream)
	}
	if stream.Position.Token == "" {
		t.Error("Position.Token is empty after COPY_COMPLETED")
	}

	// Step 5 — insert R6 AFTER the snapshot completed so it surfaces via
	// CDC, not the COPY.
	applyVTTestSQL(t, mysqlDSN, "INSERT INTO users (email) VALUES ('r6@example.com')")

	// Step 6 — start CDC from the captured position. The R6 insert
	// committed after COPY_COMPLETED so it must surface here.
	changes, err := stream.Changes.StreamChanges(ctx, stream.Position)
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	got := drainVTTestChanges(t, ctx, changes, 1, 90*time.Second)
	if len(got) != 1 {
		t.Fatalf("got %d changes; want 1 (R6 insert)", len(got))
	}
	insR6, ok := got[0].(ir.Insert)
	if !ok {
		t.Fatalf("change[0] = %T; want ir.Insert", got[0])
	}
	if email, _ := insR6.Row["email"].(string); email != "r6@example.com" {
		t.Errorf("R6 insert email = %#v; want r6@example.com", insR6.Row["email"])
	}
}

// TestVStream_VTTestServer_MultiShardSnapshot exercises the
// multi-shard snapshot+CDC handoff: the snapshot path's COPY phase
// must fan out to BOTH shards (-80 and 80-), buffer rows from each
// into a unified per-table slice, and only terminate on the *global*
// COPY_COMPLETED event (not the per-scope events from each shard).
// After the handoff, post-COPY inserts must surface via CDC from
// the persisted multi-shard position.
//
// This is the multi-shard counterpart to
// TestVStream_VTTestServer_SnapshotStream. The single-shard test
// must continue to pass alongside this one.
func TestVStream_VTTestServer_MultiShardSnapshot(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServerWithShards(t, 2)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT       NOT NULL,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
	`
	applyVTTestSQL(t, mysqlDSN, seedDDL)

	// Sharded keyspace: declare the primary vindex so vtgate routes
	// inserts to a specific shard. Same approach as the multi-shard
	// CDC integration test.
	const vindexDDL = `ALTER VSCHEMA ON test.users ADD VINDEX hash(id) USING hash`
	applyVTTestSQL(t, mysqlDSN, vindexDDL)

	// Schema tracker is async; wait for the vschema change to
	// propagate before opening the snapshot stream (the COPY phase
	// enumerates tables via the tablet's schema engine).
	time.Sleep(3 * time.Second)

	// Seed eight rows with mixed ids — Vitess's hash vindex
	// distributes them across both shards. Eight is enough for both
	// shards to host at least one row in practice; the test
	// asserts on the count and content, not on a specific shard
	// assignment.
	const seedRows = `
		INSERT INTO users (id, email) VALUES (1, 'r1@example.com');
		INSERT INTO users (id, email) VALUES (2, 'r2@example.com');
		INSERT INTO users (id, email) VALUES (3, 'r3@example.com');
		INSERT INTO users (id, email) VALUES (4, 'r4@example.com');
		INSERT INTO users (id, email) VALUES (5, 'r5@example.com');
		INSERT INTO users (id, email) VALUES (6, 'r6@example.com');
		INSERT INTO users (id, email) VALUES (7, 'r7@example.com');
		INSERT INTO users (id, email) VALUES (8, 'r8@example.com');
	`
	applyVTTestSQL(t, mysqlDSN+"&multiStatements=true", seedRows)

	// Same beat as the single-shard test: schema-tracker time before
	// the COPY phase opens.
	time.Sleep(3 * time.Second)

	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_auto_discover_shards=true",
		mysqlDSN, grpcEndpoint,
	)

	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	stream, err := eng.OpenSnapshotStream(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenSnapshotStream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// ADR-0071 streaming contract: OpenSnapshotStream returns with a ZERO
	// Position and the COPY drains in a background pump, so the captured
	// Position (and the snapshot-complete guarantee) are valid only AFTER
	// ReadRows is fully drained. Drain first; assert the position and do
	// the post-COPY inserts afterwards so they are deterministically CDC
	// events, not COPY-vs-insert races.

	// Drain bulk rows. Multi-shard COPY merges rows from both
	// shards into the same unqualified-table slice; ReadRows
	// surfaces all eight regardless of shard origin.
	usersTable := &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "email", Type: ir.Varchar{Length: 255}},
		},
	}
	rowsCh, err := stream.Rows.ReadRows(ctx, usersTable)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	bulkEmails := make([]string, 0, 8)
	for r := range rowsCh {
		s, _ := r["email"].(string)
		bulkEmails = append(bulkEmails, s)
	}
	sort.Strings(bulkEmails)
	want := []string{
		"r1@example.com", "r2@example.com", "r3@example.com", "r4@example.com",
		"r5@example.com", "r6@example.com", "r7@example.com", "r8@example.com",
	}
	if !equalSorted(bulkEmails, want) {
		t.Fatalf("bulk rows = %v; want exactly %v (overlap or missing rows across shards)", bulkEmails, want)
	}

	// The captured position is finalised now that the COPY drained; it
	// must carry TWO shardGtid entries (one per shard) — the multi-shard
	// snapshot path's defining difference from the single-shard path.
	shards, ok, err := decodeVStreamPos(stream.Position)
	if err != nil || !ok {
		t.Fatalf("decodeVStreamPos(stream.Position) ok=%v err=%v", ok, err)
	}
	if len(shards) != 2 {
		t.Fatalf("captured position has %d shardGtid entries (%v); want 2", len(shards), shards)
	}
	for i, s := range shards {
		if s.Gtid == "" || s.Gtid == "current" {
			t.Errorf("position shards[%d].Gtid = %q; want concrete GTID after COPY_COMPLETED", i, s.Gtid)
		}
	}

	// Post-COPY inserts on a separate connection — they commit after
	// COPY_COMPLETED so they must surface via CDC, not the snapshot rows.
	applyVTTestSQL(t, mysqlDSN, "INSERT INTO users (id, email) VALUES (1001, 'after-copy-1@example.com')")
	applyVTTestSQL(t, mysqlDSN, "INSERT INTO users (id, email) VALUES (1002, 'after-copy-2@example.com')")

	// Start CDC from the captured position. The two after-copy
	// inserts committed after COPY_COMPLETED so they must surface
	// here.
	changes, err := stream.Changes.StreamChanges(ctx, stream.Position)
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	got := drainVTTestChanges(t, ctx, changes, 2, 90*time.Second)
	postCopy := 0
	for _, c := range got {
		ins, ok := c.(ir.Insert)
		if !ok {
			continue
		}
		if email, _ := ins.Row["email"].(string); strings.HasPrefix(email, "after-copy-") {
			postCopy++
		}
	}
	if postCopy < 2 {
		t.Fatalf("post-COPY CDC: got %d after-copy inserts; want 2", postCopy)
	}
}

// equalSorted reports whether two pre-sorted slices have the same
// contents in the same order.
func equalSorted(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// applyVTTestSQL runs DDL/DML against vttestserver's embedded
// MySQL. The connection is short-lived; multiStatements=true is
// caller-controlled (DML batches typically need it; the seed DDL
// is one statement and doesn't).
func applyVTTestSQL(t *testing.T, dsn, sqlText string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, sqlText); err != nil {
		t.Fatalf("apply sql: %v", err)
	}
}

// TestVStream_VTTestServer_MultiShard exercises the multi-shard
// receive path against a vttestserver booted with NUM_SHARDS=2
// (default vschema produces shards "-80" and "80-"). It validates:
//
//  1. Auto-discovery (vstream_auto_discover_shards=true) returns
//     both shards from `SHOW VITESS_SHARDS` so the reader doesn't
//     need an explicit shard list.
//  2. The reader streams events from BOTH shards through the
//     single gRPC stream (vtgate's per-shard fan-out).
//  3. Persisting the position and reopening with it resumes from
//     each shard's individual cursor (no gap, no duplicates).
//
// Vitess's default hash vindex distributes integer ids across
// shards by their hashed keyspace-id; alternating ids 1,2,3,4 is
// enough to land rows on both shards in practice. The test
// asserts on the multi-shard nature of the position rather than a
// specific shard:row assignment, which would couple the test to
// the hash function's internals.
func TestVStream_VTTestServer_MultiShard(t *testing.T) {
	mysqlDSN, grpcEndpoint, keyspace, cleanup := startVTTestServerWithShards(t, 2)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT       NOT NULL,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
	`
	applyVTTestSQL(t, mysqlDSN, seedDDL)

	// On a sharded keyspace the table needs a primary vindex
	// declared via the vschema before vtgate will route INSERTs to
	// a specific shard. The default `hash` vindex on the integer
	// id column distributes rows across shards by hashed keyspace
	// id. vttestserver enables vschema_ddl_authorized_users=% in
	// its run.sh so this is callable from any user.
	const vindexDDL = `ALTER VSCHEMA ON test.users ADD VINDEX hash(id) USING hash`
	applyVTTestSQL(t, mysqlDSN, vindexDDL)

	// vttestserver's schema tracker is async; give it a beat to
	// pick up the vschema change before opening the stream.
	time.Sleep(3 * time.Second)

	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_auto_discover_shards=true",
		mysqlDSN, grpcEndpoint,
	)

	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	cdcRdr, ok := rdr.(*vstreamCDCReader)
	if !ok {
		t.Fatalf("OpenCDCReader returned %T; want *vstreamCDCReader", rdr)
	}

	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	// Confirm auto-discovery populated both shards. Discovery is deferred to
	// StreamChanges (reader construction is deliberately connection-free), so
	// assert the shard list AFTER opening the stream — the reader exposes it
	// as the unexported `shards` field, read via the typed pointer.
	if len(cdcRdr.shards) != 2 {
		t.Fatalf("auto-discovered shards = %v; want 2 entries (-80, 80-)", cdcRdr.shards)
	}
	t.Logf("auto-discovered shards for keyspace %q: %v", keyspace, cdcRdr.shards)

	time.Sleep(3 * time.Second)

	// Insert eight rows with mixed ids — Vitess's hash vindex
	// hashes integer ids fairly evenly, so eight ids gives a high
	// probability of hitting both shards. The test asserts that
	// SOME row landed on each shard (via the position decoded
	// from the change events), not on a specific id-to-shard
	// assignment.
	const dml = `
		INSERT INTO users (id, email) VALUES (1, 'r1@example.com');
		INSERT INTO users (id, email) VALUES (2, 'r2@example.com');
		INSERT INTO users (id, email) VALUES (3, 'r3@example.com');
		INSERT INTO users (id, email) VALUES (4, 'r4@example.com');
		INSERT INTO users (id, email) VALUES (5, 'r5@example.com');
		INSERT INTO users (id, email) VALUES (6, 'r6@example.com');
		INSERT INTO users (id, email) VALUES (7, 'r7@example.com');
		INSERT INTO users (id, email) VALUES (8, 'r8@example.com');
	`
	applyVTTestSQL(t, mysqlDSN+"&multiStatements=true", dml)

	got := drainVTTestChanges(t, ctx, changes, 8, 90*time.Second)
	if len(got) < 1 {
		if streamErr := cdcRdr.Err(); streamErr != nil {
			t.Fatalf("got %d changes; want >=1 (stream error: %v)", len(got), streamErr)
		}
		t.Fatalf("got %d changes; want >=1", len(got))
	}

	// The stream is multiplexed — events from both shards arrive
	// on the same channel. We confirm multi-shard delivery by
	// checking that the LAST change's position carries TWO
	// shardGtid entries (one per shard). The position is the
	// reader's currentVgtid encoded; vtgate emits a VGTID after
	// every transaction with both shards' positions advanced.
	last := got[len(got)-1]
	pos := last.Pos()
	shards, ok2, err := decodeVStreamPos(pos)
	if err != nil || !ok2 {
		t.Fatalf("decodeVStreamPos(last) ok=%v err=%v", ok2, err)
	}
	if len(shards) != 2 {
		t.Errorf("last change position has %d shards (%v); want 2 (per-shard cursor tracking)", len(shards), shards)
	}
	for i, s := range shards {
		if s.Gtid == "" || s.Gtid == "current" {
			t.Errorf("position shards[%d].Gtid = %q; want concrete GTID after streaming", i, s.Gtid)
		}
	}

	// Persist position, close, reopen, insert more rows, confirm
	// the new inserts surface from the persisted position.
	persistedPos := last.Pos()
	_ = cdcRdr.Close()

	rdr2, err := eng.OpenCDCReader(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenCDCReader (resume): %v", err)
	}
	defer func() {
		if c, ok := rdr2.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	changes2, err := rdr2.StreamChanges(ctx, persistedPos)
	if err != nil {
		t.Fatalf("StreamChanges (resume): %v", err)
	}

	time.Sleep(3 * time.Second)

	const dml2 = `
		INSERT INTO users (id, email) VALUES (101, 'after-resume-1@example.com');
		INSERT INTO users (id, email) VALUES (102, 'after-resume-2@example.com');
		INSERT INTO users (id, email) VALUES (103, 'after-resume-3@example.com');
		INSERT INTO users (id, email) VALUES (104, 'after-resume-4@example.com');
	`
	applyVTTestSQL(t, mysqlDSN+"&multiStatements=true", dml2)

	// Drain enough events to cover the four post-resume inserts.
	// VStream's VGTID semantics permit the last pre-resume
	// transaction to reappear (the captured position is "before
	// this txn commits", not "after"), so we may see one or two
	// rN replays before the after-resume-* set arrives. Drain up
	// to 8 events and assert all FOUR post-resume rows showed up.
	got2 := drainVTTestChanges(t, ctx, changes2, 8, 90*time.Second)
	postResume := 0
	for _, c := range got2 {
		ins, ok := c.(ir.Insert)
		if !ok {
			continue
		}
		if email, _ := ins.Row["email"].(string); strings.HasPrefix(email, "after-resume-") {
			postResume++
		}
	}
	if postResume < 4 {
		if cdc2, ok := rdr2.(*vstreamCDCReader); ok {
			if streamErr := cdc2.Err(); streamErr != nil {
				t.Fatalf("resume: got %d after-resume rows of 4 (stream error: %v)", postResume, streamErr)
			}
		}
		t.Fatalf("resume: got %d after-resume rows of 4; want all 4", postResume)
	}
}

// drainVTTestChanges drains up to want events with a timeout.
// Returned slice may be shorter than want if the stream closed
// early; caller asserts.
func drainVTTestChanges(
	t *testing.T,
	ctx context.Context,
	changes <-chan ir.Change,
	want int,
	timeout time.Duration,
) []ir.Change {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	got := make([]ir.Change, 0, want)
	for len(got) < want {
		select {
		case c, ok := <-changes:
			if !ok {
				return got
			}
			// ir.SchemaSnapshot is the ADR-0049 schema-history
			// boundary event (a VStream reader emits one at
			// first-touch + each true FIELD-delta). It is orthogonal
			// infra, not a VTTest row change — skip it so the
			// data-shape assertions count only row events. Chunk B2's
			// own snapshot pins use drainSnapshots, not this helper.
			if _, ok := c.(ir.SchemaSnapshot); ok {
				continue
			}
			got = append(got, c)
		case <-deadline.C:
			t.Logf("timed out after %v with %d/%d changes", timeout, len(got), want)
			return got
		case <-ctx.Done():
			return got
		}
	}
	return got
}
