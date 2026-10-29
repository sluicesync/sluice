//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The pins for CROSS-SHARD-VINDEX-NOT-IN-PK (Bugs 337 and 338), on a real
// two-shard vttestserver keyspace (`ks2`, -80 / 80-) with the bug's table:
// xt(id PRIMARY KEY, tenant, v), sharded by hash(tenant). Tenant 1 hashes to
// -80 and tenant 4 to 80- (measured on the image).
//
//   - Refuses: --allow-cross-shard-merge refuses the table before anything
//     is written to the target, on a cold start (MySQL and Postgres targets)
//     and on the warm resume of a stream established under the assertion.
//   - ShardRoutingShapes: the engine's VSchema read (ir.ShardRoutingReader)
//     and the judge, over a vindex inside a composite key, a multi-column
//     vindex partly outside / wholly inside the key, a secondary lookup
//     vindex (ignored: only the primary places a row), and a table the
//     VSchema does not know.
//   - Remedy: the refusal's remedy, one stream per shard (vstream_shards
//     pinned) with its own --inject-shard-column value, lands the move cells
//     EXACT on both targets, with live DDL coordination and without: a
//     forward move (DELETE on -80, INSERT on 80-), an insert-first move in
//     each direction and a move-and-back, all committed milliseconds apart —
//     the shapes that lost rows in the bug's rig.
//   - Control: the same cells through the merged stream, let through by the
//     assertion, must lose rows on the MySQL target (Bug 337's
//     insert-first arm), or the EXACT verdicts measured nothing. The streams
//     run the CLI's batched apply (--apply-batch-size=auto): the serial path
//     deletes by full before image and does not reproduce the bug.
//
// The verdict compares the SOURCE's (id, tenant, v) rows read through vtgate
// with the TARGET's own read; it never consults sluice's state.
//
// Usage (Windows; see CLAUDE.md for the Rancher Desktop env):
//
//	go test -tags='integration vstream' -count=1 -timeout=40m -v \
//	  -run 'TestStreamer_CrossShardRowMoveDoor_VStream' ./internal/pipeline/

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

const (
	xsdKeyspace = "ks2"
	xsdTenantA  = 1 // hashes to -80
	xsdTenantB  = 4 // hashes to 80-
)

type xsdRig struct {
	mysqlDSN string // vtgate, no VStream params
	grpc     string
}

// streamDSN is the vtgate DSN with VStream params, pinned to shards.
func (r xsdRig) streamDSN(shards string) string {
	return fmt.Sprintf("%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=%s",
		r.mysqlDSN, r.grpc, shards)
}

func (r xsdRig) exec(t *testing.T, stmts ...string) {
	t.Helper()
	db, err := sql.Open("mysql", r.mysqlDSN)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer func() { _ = db.Close() }()
	for _, q := range stmts {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_, err := db.ExecContext(ctx, q)
		cancel()
		if err != nil {
			t.Fatalf("source %q: %v", q, err)
		}
	}
}

// xsdRows reads every (id -> "tenant|v") of xt, failing on a duplicate id
// (a target that kept one key twice is a divergence, not a map overwrite).
func xsdRows(t *testing.T, db twfbDB) map[int64]string {
	t.Helper()
	dsn := db.dsn
	conn, err := sql.Open(db.driver(), dsn)
	if err != nil {
		t.Fatalf("open %s: %v", db.engine, err)
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rows, err := conn.QueryContext(ctx, "SELECT id, tenant, v FROM xt ORDER BY id")
	if err != nil {
		t.Fatalf("read %s xt: %v", db.engine, err)
	}
	defer func() { _ = rows.Close() }()
	out := map[int64]string{}
	for rows.Next() {
		var id, tenant int64
		var v string
		if err := rows.Scan(&id, &tenant, &v); err != nil {
			t.Fatalf("scan %s xt: %v", db.engine, err)
		}
		if prev, dup := out[id]; dup {
			t.Errorf("%s holds id %d twice (%s and %d|%s)", db.engine, id, prev, tenant, v)
		}
		out[id] = fmt.Sprintf("%d|%s", tenant, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows %s xt: %v", db.engine, err)
	}
	return out
}

// xsdDiff renders every id whose row differs between src and dst.
func xsdDiff(src, dst map[int64]string) []string {
	ids := map[int64]bool{}
	for id := range src {
		ids[id] = true
	}
	for id := range dst {
		ids[id] = true
	}
	var out []string
	for id := range ids {
		if src[id] != dst[id] {
			out = append(out, fmt.Sprintf("id %d: source=%q target=%q", id, src[id], dst[id]))
		}
	}
	sort.Strings(out)
	return out
}

func xsdTableExists(t *testing.T, db twfbDB) bool {
	t.Helper()
	q := "SELECT count(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'xt'"
	if db.engine == "postgres" {
		q = "SELECT count(*) FROM pg_tables WHERE tablename = 'xt'"
	}
	return db.scalar(t, q) == "1"
}

func xsdStreamer(t *testing.T, rig xsdRig, tgt twfbDB, streamID, shards string) *Streamer {
	t.Helper()
	srcEng, _ := engines.Get("vitess")
	tgtEng, _ := engines.Get(tgt.engine)
	filter, err := migcore.NewTableFilter([]string{"xt"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return &Streamer{
		Source: srcEng, Target: tgtEng,
		SourceDSN: rig.streamDSN(shards), TargetDSN: tgt.dsn,
		StreamID: streamID, Filter: filter, ApplyRetryAttempts: 4,
		// The CLI's --apply-batch-size=auto ceiling: Bug 337 lives on the
		// batched apply path (PK-keyed DELETE … IN, PK upsert); the serial
		// path deletes by full before image.
		ApplyBatchSize: 1000, AutoTune: true,
	}
}

func TestStreamer_CrossShardRowMoveDoor_VStream(t *testing.T) {
	mysqlDSN, grpc, _, cleanupSrc := startShardedVTTestServer(t, xsdKeyspace, 2)
	defer cleanupSrc()
	rig := xsdRig{mysqlDSN: mysqlDSN, grpc: grpc}
	rig.exec(
		t,
		"CREATE TABLE xt (id BIGINT NOT NULL, tenant BIGINT NOT NULL, v VARCHAR(64) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB",
		"ALTER VSCHEMA ON ks2.xt ADD VINDEX hash(tenant) USING hash",
	)
	time.Sleep(3 * time.Second) // the schema tracker
	var seed []string
	for _, base := range []int{0, 100, 200} { // one id range per move cell below
		for id := base + 1; id <= base+40; id++ {
			tenant := xsdTenantA
			if id > base+20 {
				tenant = xsdTenantB
			}
			seed = append(seed, fmt.Sprintf("(%d,%d,'v1-%d')", id, tenant, id))
		}
	}
	rig.exec(t, "INSERT INTO xt (id, tenant, v) VALUES "+strings.Join(seed, ","))

	pgDSN, cleanupPG := startPGTarget(t)
	defer cleanupPG()
	myDSN, cleanupMy := startMySQLTarget(t)
	defer cleanupMy()
	targets := []twfbDB{{engine: "postgres", dsn: pgDSN}, {engine: "mysql", dsn: myDSN}}

	t.Run("ColdStartRefusesBeforeWriting", func(t *testing.T) {
		// With --allow-cross-shard-merge on each target, and without it on
		// Postgres: the door keys on the topology, not the flag.
		cells := []struct {
			tgt   twfbDB
			allow bool
		}{{targets[0], true}, {targets[1], true}, {targets[0], false}}
		for _, c := range cells {
			tgt := c.tgt
			s := xsdStreamer(t, rig, tgt, fmt.Sprintf("xsd_refuse_%s_%v", tgt.engine, c.allow), "-80,80-")
			s.AllowCrossShardMerge = c.allow
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			err := s.Run(ctx)
			cancel()
			if !errors.Is(err, errCrossShardRowMoveRefused) {
				t.Fatalf("%s: Run = %v; want the CROSS-SHARD-VINDEX-NOT-IN-PK refusal", tgt.engine, err)
			}
			for _, want := range []string{crossShardVindexNotInPKMarker, "xt: primary vindex (tenant)", "primary key (id)"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("%s: refusal lacks %q: %v", tgt.engine, want, err)
				}
			}
			if xsdTableExists(t, tgt) {
				t.Errorf("%s: the refusal left xt created on the target; it must refuse before anything is written", tgt.engine)
			}
		}
	})

	// The engine's VSchema read across the routing shapes vtgate can answer
	// on a sharded keyspace, judged by the door: what reaches the judge is
	// the PRIMARY vindex only, multi-column vindexes whole, and a table the
	// VSchema does not know as Unknown.
	t.Run("ShardRoutingShapes", func(t *testing.T) {
		rig.exec(
			t,
			"CREATE TABLE rs_comp (tenant BIGINT NOT NULL, id BIGINT NOT NULL, PRIMARY KEY (tenant, id))",
			"ALTER VSCHEMA ON ks2.rs_comp ADD VINDEX hash(tenant) USING hash",
			"CREATE TABLE rs_multi (a BIGINT NOT NULL, b BIGINT NOT NULL, c BIGINT NOT NULL, PRIMARY KEY (a, c))",
			"ALTER VSCHEMA ON ks2.rs_multi ADD VINDEX rs_mc(a, b) USING multicol WITH column_count=2",
			"CREATE TABLE rs_multi_ok (a BIGINT NOT NULL, b BIGINT NOT NULL, PRIMARY KEY (b, a))",
			"ALTER VSCHEMA ON ks2.rs_multi_ok ADD VINDEX rs_mc2(a, b) USING multicol WITH column_count=2",
			"CREATE TABLE rs_lookup (id BIGINT NOT NULL, tenant BIGINT NOT NULL, email VARCHAR(64), PRIMARY KEY (tenant, id))",
			"ALTER VSCHEMA ON ks2.rs_lookup ADD VINDEX hash(tenant) USING hash",
			"ALTER VSCHEMA ON ks2.rs_lookup ADD VINDEX rs_em(email) USING unicode_loose_md5",
			"CREATE TABLE rs_novs (id BIGINT NOT NULL PRIMARY KEY)",
			// A lookup_unique PRIMARY vindex on the key column: the key
			// contains the vindex column, but the lookup table, not the
			// value, places the row.
			"CREATE TABLE rs_lkp (id BIGINT NOT NULL PRIMARY KEY, email VARCHAR(64) NOT NULL)",
			"CREATE TABLE rs_lkp_idx (id BIGINT NOT NULL PRIMARY KEY, keyspace_id VARBINARY(16))",
			"ALTER VSCHEMA ON ks2.rs_lkp_idx ADD VINDEX hash(id) USING hash",
			"ALTER VSCHEMA ON ks2.rs_lkp ADD VINDEX rs_lk(id) USING lookup_unique WITH table=ks2.rs_lkp_idx, from=id, to=keyspace_id",
		)
		time.Sleep(3 * time.Second)
		srcEng, _ := engines.Get("vitess")
		reader, ok := srcEng.(ir.ShardRoutingReader)
		if !ok {
			t.Fatal("the vitess engine does not implement ir.ShardRoutingReader")
		}
		names := []string{"rs_comp", "rs_multi", "rs_multi_ok", "rs_lookup", "rs_novs", "rs_lkp", "xt"}
		routes, err := reader.ShardRouting(context.Background(), rig.streamDSN("-80,80-"), names)
		if err != nil {
			t.Fatalf("ShardRouting: %v", err)
		}
		cases := []struct {
			table   string
			pk      []string
			want    ir.ShardRouting
			refused bool
		}{
			{"rs_comp", []string{"tenant", "id"}, ir.ShardRouting{Kind: ir.ShardRoutingVindex, PrimaryVindex: []string{"tenant"}}, false},
			{"rs_multi", []string{"a", "c"}, ir.ShardRouting{Kind: ir.ShardRoutingVindex, PrimaryVindex: []string{"a", "b"}}, true},
			{"rs_multi_ok", []string{"b", "a"}, ir.ShardRouting{Kind: ir.ShardRoutingVindex, PrimaryVindex: []string{"a", "b"}}, false},
			{"rs_lookup", []string{"tenant", "id"}, ir.ShardRouting{Kind: ir.ShardRoutingVindex, PrimaryVindex: []string{"tenant"}}, false},
			{"rs_novs", []string{"id"}, ir.ShardRouting{Kind: ir.ShardRoutingUnknown}, true},
			{"rs_lkp", []string{"id"}, ir.ShardRouting{Kind: ir.ShardRoutingNonFunctional, PrimaryVindex: []string{"id"}}, true},
			{"xt", []string{"id"}, ir.ShardRouting{Kind: ir.ShardRoutingVindex, PrimaryVindex: []string{"tenant"}}, true},
		}
		for _, tc := range cases {
			got := routes[tc.table]
			if got.Kind != tc.want.Kind || strings.Join(got.PrimaryVindex, ",") != strings.Join(tc.want.PrimaryVindex, ",") {
				t.Errorf("%s: routing = %+v; want %+v", tc.table, got, tc.want)
			}
			if refused := judgeKeyResidency(tc.table, tc.pk, got) != nil; refused != tc.refused {
				t.Errorf("%s: judged refused=%v; want %v", tc.table, refused, tc.refused)
			}
		}
		// The door itself passes the configurations that must keep working:
		// a vindex inside the key on two shards, and a stream pinned to one
		// shard (which merges nothing), with no flag set.
		pg := targets[0]
		for _, c := range []struct {
			name, table, shards string
		}{{"vindex inside the key, two shards", "rs_comp", "-80,80-"}, {"stream pinned to one shard", "xt", "-80"}} {
			s := xsdStreamer(t, rig, pg, "xsd_pass", c.shards)
			filter, err := migcore.NewTableFilter([]string{c.table}, nil)
			if err != nil {
				t.Fatal(err)
			}
			s.Filter = filter
			if err := s.preflightCrossShardRowMoves(context.Background()); err != nil {
				t.Errorf("%s: the door refused a working configuration: %v", c.name, err)
			}
		}
		for _, tbl := range []string{"rs_comp", "rs_multi", "rs_multi_ok", "rs_lookup", "rs_novs", "rs_lkp", "rs_lkp_idx"} {
			rig.exec(t, "DROP TABLE "+tbl)
		}
	})

	t.Run("WarmResumeRefuses", func(t *testing.T) {
		tgt := twfbDB{engine: "postgres", dsn: pgDSN}
		const streamID = "xsd_warm"
		cell := twfbCell{tgt: tgt, streamID: streamID}
		s := xsdStreamer(t, rig, tgt, streamID, "-80,80-")
		s.AllowCrossShardMerge, s.AssertNoCrossShardRowMoves = true, true
		run := startTWFBRun(s)
		if !vrsAwaitHandoff(t, cell, run, "", 4*time.Minute) {
			t.Fatalf("RIG: the asserted stream never handed off (exited=%v err=%v)", run.exited(), run.err)
		}
		_ = run.stop(t)
		if cell.persistedPosition(t) == "" {
			t.Fatal("RIG: no persisted position, so the next start would not be a warm resume")
		}
		// The restart passes neither flag: --allow-cross-shard-merge is not
		// persisted, and this warm resume merges the shards all the same.
		s = xsdStreamer(t, rig, tgt, streamID, "-80,80-")
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := s.Run(ctx); !errors.Is(err, errCrossShardRowMoveRefused) {
			t.Fatalf("warm resume without either flag: Run = %v; want the refusal", err)
		}

		// A fresh cold start under --schema-already-applied, which Bug 152's
		// collision door does not run on, into the xt the stream created:
		// refused by this door, without either flag.
		saa := xsdStreamer(t, rig, tgt, "xsd_saa", "-80,80-")
		saa.SchemaAlreadyApplied = true
		before := tgt.scalar(t, "SELECT count(*) FROM xt")
		if err := saa.Run(ctx); !errors.Is(err, errCrossShardRowMoveRefused) {
			t.Fatalf("--schema-already-applied cold start: Run = %v; want the refusal", err)
		}
		if after := tgt.scalar(t, "SELECT count(*) FROM xt"); after != before {
			t.Errorf("--schema-already-applied refusal wrote to the target: xt had %s rows, now %s", before, after)
		}

		// `schema add-table` into the stopped stream: refused before any copy
		// (the table never reaches the target), then let through by the
		// assertion with its WARN.
		rig.exec(
			t,
			"CREATE TABLE yt (id BIGINT NOT NULL, tenant BIGINT NOT NULL, v VARCHAR(64) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB",
			"ALTER VSCHEMA ON ks2.yt ADD VINDEX hash(tenant) USING hash",
		)
		time.Sleep(3 * time.Second)
		rig.exec(t, fmt.Sprintf("INSERT INTO yt (id, tenant, v) VALUES (1,%d,'a'),(2,%d,'b')", xsdTenantA, xsdTenantB))
		srcEng, _ := engines.Get("vitess")
		pgEng, _ := engines.Get("postgres")
		add := &AddTable{Source: srcEng, Target: pgEng, SourceDSN: rig.streamDSN("-80,80-"), TargetDSN: tgt.dsn, StreamID: streamID, TableName: "yt"}
		err := add.Run(ctx)
		if !errors.Is(err, errCrossShardRowMoveRefused) || !strings.Contains(err.Error(), "schema add-table") {
			t.Fatalf("add-table: Run = %v; want the add-table refusal", err)
		}
		if tgt.scalar(t, "SELECT count(*) FROM pg_tables WHERE tablename = 'yt'") != "0" {
			t.Errorf("add-table refusal left yt on the target; it must refuse before any copy")
		}
		logs := captureSlog(t)
		add.AssertNoCrossShardRowMoves = true
		if err := add.Run(ctx); err != nil {
			t.Fatalf("asserted add-table: Run = %v; want the add to proceed", err)
		}
		if !strings.Contains(logs.String(), crossShardRowMovesAssertedMarker) || !strings.Contains(logs.String(), "tables=yt") {
			t.Errorf("asserted add-table logged no %s WARN naming yt", crossShardRowMovesAssertedMarker)
		}
		if n := tgt.scalar(t, "SELECT count(*) FROM yt"); n != "2" {
			t.Errorf("asserted add-table copied %s rows of yt; want 2", n)
		}

		// The target and source are cleared for the remedy cells below.
		tgt.exec(t, "DROP TABLE xt")
		tgt.exec(t, "DROP TABLE yt")
		rig.exec(t, "DROP TABLE yt")
	})

	// The remedy: one stream per shard, each pinned and each with its own
	// discriminator, on each target, under live cross-shard DDL coordination
	// (the --inject-shard-column default) and under the drained model. Then
	// the move cells, committed milliseconds apart through vtgate (which
	// routes by tenant).
	t.Run("PerShardInjectRemedyIsExact/Coordinated", func(t *testing.T) { xsdMoveCell(t, rig, targets, 0, xsdPerShardCoordinated) })
	t.Run("PerShardInjectRemedyIsExact/Drained", func(t *testing.T) { xsdMoveCell(t, rig, targets, 1, xsdPerShardDrained) })
	// The anti-vacuity control: the same cells through the merged stream the
	// door refuses (here let through by the assertion) must lose rows on the
	// MySQL target, or the EXACT verdicts above measured nothing.
	t.Run("MergedAssertedControlLosesOnMySQL", func(t *testing.T) { xsdMoveCell(t, rig, targets, 2, xsdMergedAsserted) })
}

// xsdTopology is how a move cell streams the two shards to each target.
type xsdTopology int

const (
	xsdPerShardCoordinated xsdTopology = iota // the remedy, live DDL coordination (the default)
	xsdPerShardDrained                        // the remedy, --no-coordinate-live-ddl
	xsdMergedAsserted                         // one merged stream, --assert-no-cross-shard-row-moves
)

// xsdMoveCell streams every target in the given topology, commits the move
// cells, and grades each target against the source. mode selects the cell's
// id range (seeded 100*mode + 1..40) and its markers.
func xsdMoveCell(t *testing.T, rig xsdRig, targets []twfbDB, mode int, topo xsdTopology) {
	t.Helper()
	for _, tgt := range targets {
		if xsdTableExists(t, tgt) {
			tgt.exec(t, "DROP TABLE xt")
		}
	}
	var runs []*twfbRun
	defer func() {
		for _, r := range runs {
			_ = r.stop(t)
		}
	}()
	shards := []string{"-80", "80-"}
	if topo == xsdMergedAsserted {
		shards = []string{"-80,80-"}
	}
	for _, tgt := range targets {
		for i, shard := range shards {
			streamID := fmt.Sprintf("xsd_mv%d_%s_%d", mode, tgt.engine, i+1)
			s := xsdStreamer(t, rig, tgt, streamID, shard)
			if topo == xsdMergedAsserted {
				s.AllowCrossShardMerge, s.AssertNoCrossShardRowMoves = true, true
			} else {
				s.InjectShardColumn = ShardColumnSpec{Name: "src_shard", Value: fmt.Sprint(i + 1)}
				s.NoCoordinateLiveDDL = topo == xsdPerShardDrained
			}
			run := startTWFBRun(s)
			runs = append(runs, run)
			if !vrsAwaitHandoff(t, twfbCell{tgt: tgt, streamID: streamID}, run, "", 4*time.Minute) {
				t.Fatalf("RIG: %s stream %s never handed off (exited=%v err=%v)", tgt.engine, streamID, run.exited(), run.err)
			}
		}
	}
	base := 100 * mode
	del := func(id, tenant int) string {
		return fmt.Sprintf("DELETE FROM xt WHERE id = %d AND tenant = %d", id, tenant)
	}
	ins := func(id, tenant int, v string) string {
		return fmt.Sprintf("INSERT INTO xt (id, tenant, v) VALUES (%d, %d, '%s-%d')", id, tenant, v, id)
	}
	var stmts []string
	for id := base + 1; id <= base+4; id++ { // forward: delete on -80, insert on 80-
		stmts = append(stmts, del(id, xsdTenantA), ins(id, xsdTenantB, "mv"))
	}
	for id := base + 5; id <= base+8; id++ { // reverse (copy-then-delete): insert on 80- first
		stmts = append(stmts, ins(id, xsdTenantB, "rv"), del(id, xsdTenantA))
	}
	for id := base + 9; id <= base+12; id++ { // move and back
		stmts = append(stmts, del(id, xsdTenantA), ins(id, xsdTenantB, "mv"), del(id, xsdTenantB), ins(id, xsdTenantA, "back"))
	}
	for id := base + 21; id <= base+24; id++ { // the other direction, 80- to -80, insert first
		stmts = append(stmts, ins(id, xsdTenantA, "rv"), del(id, xsdTenantB))
	}
	rig.exec(t, stmts...)
	markA, markB := 9000001+10*mode, 9000002+10*mode
	rig.exec(t, ins(markA, xsdTenantA, "mk"), ins(markB, xsdTenantB, "mk"))
	want := xsdRows(t, twfbDB{engine: "mysql", dsn: rig.mysqlDSN})
	for _, tgt := range targets {
		deadline := time.Now().Add(3 * time.Minute)
		for !tgt.hasRow(t, "xt", markA) || !tgt.hasRow(t, "xt", markB) {
			if time.Now().After(deadline) {
				for _, r := range runs {
					if r.exited() {
						t.Errorf("a remedy stream exited: %v", r.err)
					}
				}
				t.Fatalf("%s: the markers never arrived", tgt.engine)
			}
			time.Sleep(250 * time.Millisecond)
		}
		time.Sleep(2 * time.Second)
		diff := xsdDiff(want, xsdRows(t, tgt))
		switch {
		case topo == xsdMergedAsserted:
			// Bug 337's deterministic arm: an INSERT on the new shard
			// applied before the old shard's DELETE, which a MySQL target
			// then applies by primary key alone.
			t.Logf("%s, merged stream: %d id(s) diverged:\n  %s", tgt.engine, len(diff), strings.Join(diff, "\n  "))
			if tgt.engine == "mysql" && len(diff) == 0 {
				t.Errorf("CONTROL VACUOUS: the merged stream lost nothing on MySQL, so these cells do not reach Bug 337")
			}
		case len(diff) > 0:
			t.Errorf("SILENT DIVERGENCE on %s under the per-shard --inject-shard-column remedy (%d ids):\n  %s",
				tgt.engine, len(diff), strings.Join(diff, "\n  "))
		default:
			t.Logf("%s: EXACT (%d rows) under the per-shard remedy", tgt.engine, len(want))
		}
	}
	for _, r := range runs {
		if r.exited() {
			t.Errorf("a remedy stream exited: %v", r.err)
		}
	}
}
