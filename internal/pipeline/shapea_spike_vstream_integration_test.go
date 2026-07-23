//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test — Multi-source aggregation Shape A (ADR-0048).
//
// LANDED 2026-05-21. ADR-0048 was originally accepted design-only
// with implementation demand-gated per roadmap §4; the operator-
// direction lifted that gate and the implementation landed. This
// file was the design-first spike that surfaced the actual design
// pain; with the real APIs in place (translate.InjectShardColumn,
// pipeline.shardStampRows, pipeline.preflightShardConsolidation,
// ir.ShardColumnSetter), the spike's throwaway helpers have been
// REPLACED with calls to those real APIs. The harness now serves
// as the permanent Shape-A integration artifact under the
// `integration vstream` tag — exercising sharded Vitess → both
// consolidated PG and same-engine MySQL targets end-to-end.
//
// Design evidence (kept for archaeology):
//
//   - docs/dev/notes/prep-multi-source-shape-a.md (prep/research)
//   - docs/adr/adr-0048-multi-source-aggregation-shape-a.md (Accepted)
//
// Build tag rationale: reuses the existing `vstream` tag (not a new
// one). The spike's defining cost is the vitess/vttestserver image
// (~700 MB) — exactly the cost `vstream` already gates. A separate
// `shapea` tag would fragment the heavy-image gate for no benefit;
// the existing tag's image-cost contract already fits. The harness is
// shaped (table-driven over {MySQL target, PG target}) so it can
// become a permanent Shape-A integration artifact under this same tag
// if the design is accepted.
//
// Usage (Windows; see CLAUDE.md for the Rancher Desktop env):
//
//	$env:PATH += ";C:\Program Files\Rancher Desktop\resources\resources\win32\bin"
//	$env:TESTCONTAINERS_RYUK_DISABLED = "true"
//	go test -tags='integration vstream' -v -count=1 -timeout=30m \
//	  -run 'TestSpikeShapeA' ./internal/pipeline/...

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	mobycontainer "github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	mysqltc "github.com/testcontainers/testcontainers-go/modules/mysql"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/translate"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// ---------------------------------------------------------------------
// Source: sharded vttestserver (keyspace `commerce`, shards -80 / 80-,
// table `customer` sharded by `customer_id`). Mirrors the proven
// pattern in internal/engines/mysql/cdc_vstream_integration_test.go
// (re-declared here because that helper is package-private to the
// mysql engine package and the spike drives the pipeline package).
// ---------------------------------------------------------------------

// startShardedVTTestServer boots a sharded vttestserver and returns its
// vtgate MySQL DSN, its VStream gRPC endpoint, a restartSource closure,
// and a cleanup func.
//
// restartSource stops then re-starts the SAME container in place and
// re-waits for the original readiness signal before returning, so a
// chaos test can disrupt an in-flight source connection mid-copy and be
// guaranteed the source is serving again by the time the closure
// returns. Contrary to what this comment used to claim, an EPHEMERAL
// (empty-HostPort) mapped port is NOT reliably stable across a Docker
// stop/start — the daemon may re-allocate it (observed under Rancher
// Desktop by the chaos-lite work; see
// streamer_chaoslite_restart_integration_test.go's file header). Both
// exposed ports are therefore bound to explicitly-reserved FIXED host
// ports at create time ([chaosFixedPortModifier]), which DO survive a
// stop/start — so the previously-returned mysqlDSN / grpcEndpoint stay
// valid after a restart and callers do not need to re-read them.
// (testcontainers' Stop/Start preserve the container; only Terminate
// removes it.)
func startShardedVTTestServer(t *testing.T, keyspace string, numShards int) (mysqlDSN, grpcEndpoint string, restartSource func(t *testing.T), cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	const (
		basePort      = 33574
		mysqlPortBase = "33577/tcp"
		grpcPortBase  = "33575/tcp"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	// readiness is the same gate the initial bring-up uses; reused by
	// restartSource so a restarted source is serving before we assert.
	readiness := wait.ForAll(
		wait.ForLog("Local cluster started."),
		wait.ForListeningPort(grpcPortBase),
		wait.ForListeningPort(mysqlPortBase),
	).WithStartupTimeoutDefault(5 * time.Minute)

	// Fixed host-port bindings for both exposed ports, so the DSN and
	// gRPC endpoint returned below survive restartSource's stop/start
	// (see the function comment; the ephemeral-binding re-allocation is
	// real, not theoretical).
	fixedMySQL, _ := chaosFixedPortModifier(t, mysqlPortBase)
	fixedGRPC, _ := chaosFixedPortModifier(t, grpcPortBase)

	req := testcontainers.ContainerRequest{
		Image:        "vitess/vttestserver:mysql80",
		ExposedPorts: []string{mysqlPortBase, grpcPortBase},
		Env: map[string]string{
			"PORT":            fmt.Sprintf("%d", basePort),
			"KEYSPACES":       keyspace,
			"NUM_SHARDS":      fmt.Sprintf("%d", numShards),
			"MYSQL_BIND_HOST": "0.0.0.0",
		},
		WaitingFor: readiness,
		HostConfigModifier: func(hc *mobycontainer.HostConfig) {
			fixedMySQL(hc)
			fixedGRPC(hc)
		},
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
		"root@tcp(%s:%d)/%s?parseTime=true&interpolateParams=true",
		host, mysqlPort.Num(), keyspace,
	)
	grpcEndpoint = fmt.Sprintf("%s:%d", host, grpcPort.Num())

	// restartSource: stop + start the SAME container, then re-wait for
	// the same readiness signal the bring-up used. Generous timeout
	// (3 min) covers the vttestserver re-boot. This is the mid-copy
	// disruption used by the cross-engine chaos test; for the
	// non-chaos callers it is simply ignored (`_`).
	restartSource = func(t *testing.T) {
		t.Helper()
		rctx, rcancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer rcancel()
		stopTimeout := 30 * time.Second
		if err := container.Stop(rctx, &stopTimeout); err != nil {
			t.Fatalf("restartSource: stop vttestserver: %v", err)
		}
		if err := container.Start(rctx); err != nil {
			t.Fatalf("restartSource: start vttestserver: %v", err)
		}
		// Re-wait for readiness so the source is serving again before
		// the test asserts the post-fault invariant.
		if err := readiness.WaitUntilReady(rctx, container); err != nil {
			t.Fatalf("restartSource: vttestserver not ready after restart: %v", err)
		}
	}

	return mysqlDSN, grpcEndpoint, restartSource, terminate
}

func applySQL(t *testing.T, dsn, sqlText string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, sqlText); err != nil {
		t.Fatalf("apply sql %q: %v", truncate(sqlText, 60), err)
	}
}

func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// ---------------------------------------------------------------------
// Targets: stock testcontainers MySQL + PG (the standard pipeline
// integration pattern). Cross-engine consolidation (Vitess → PG) is
// the most informative case; same-engine (Vitess → MySQL) is the
// sanity baseline.
// ---------------------------------------------------------------------

func startMySQLTarget(t *testing.T) (dsn string, cleanup func()) {
	t.Helper()
	c := runMySQLWithRetry(
		t,
		mysqltc.WithDatabase("warehouse"),
		mysqltc.WithUsername("root"),
		mysqltc.WithPassword("rootpw"),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	term := func() {
		sd, cc := context.WithTimeout(context.Background(), 30*time.Second)
		defer cc()
		_ = c.Terminate(sd)
	}
	conn, err := c.ConnectionString(ctx)
	if err != nil {
		term()
		t.Fatalf("mysql conn string: %v", err)
	}
	// testcontainers' mysql module hands a go-sql-driver DSN; ensure
	// parseTime so the IR value contract holds on the target side too.
	if !strings.Contains(conn, "parseTime") {
		if strings.Contains(conn, "?") {
			conn += "&parseTime=true"
		} else {
			conn += "?parseTime=true"
		}
	}
	return conn, term
}

func startPGTarget(t *testing.T) (dsn string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c, err := pgtc.Run(
		// Task #68: pre-baked PG image. The bake includes a
		// pre-created `warehouse` database matching this test's
		// WithDatabase value. See pg_prebaked_integration_test.go.
		ctx, pgPrebakedImage,
		pgtc.WithDatabase("warehouse"),
		pgtc.WithUsername("test"),
		pgtc.WithPassword("test"),
		pgtc.BasicWaitStrategies(),
		pgPrebakedWaitStrategy(),
	)
	if err != nil {
		t.Fatalf("start pg target: %v", err)
	}
	term := func() {
		sd, cc := context.WithTimeout(context.Background(), 30*time.Second)
		defer cc()
		_ = c.Terminate(sd)
	}
	conn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		term()
		t.Fatalf("pg conn string: %v", err)
	}
	return conn, term
}

// pgQueryConn / mysqlQueryConn open a throwaway *sql.DB for the
// read-back assertions. The spike normalizes the PG URI driver name
// (pgx) the way the rest of the suite does.
func pgRowCount(t *testing.T, dsn, query string, args ...any) int {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("pg open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("pg query %q: %v", query, err)
	}
	return n
}

func pgStrings(t *testing.T, dsn, query string) []string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("pg open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Fatalf("pg query %q: %v", query, err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("pg scan: %v", err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("pg rows iteration: %v", err)
	}
	sort.Strings(out)
	return out
}

// (Phase 7 migration 2026-05-21) The previous throwaway helpers
// — `injectShardColumnIntoSchema` + `shardValueRowReader` — were
// REPLACED by direct calls to the shipped APIs:
//
//   - schema rewrite → translate.InjectShardColumn (pure IR pass)
//   - per-row stamp  → pipeline.shardStampRows (orchestrator value wrap)
//   - preflight      → pipeline.preflightShardConsolidation
//
// See runShardConsolidation below for the wiring. The integration
// surface for the CDC half is exercised separately by the engine-
// level applier tests + the `ir.ShardColumnSetter` compile-time
// witness in internal/engines/{mysql,postgres}/change_applier_shape_a_test.go.

// =====================================================================
// The spike test. Table-driven over the two target engines so the
// cross-engine (Vitess → PG) and same-engine (Vitess → MySQL) cases
// share the topology and assertions.
// =====================================================================

func TestSpikeShapeA_ShardedToConsolidated(t *testing.T) {
	const keyspace = "commerce"
	const shardCol = "source_shard_id"

	cases := []struct {
		name       string
		targetKind string // "pg" | "mysql"
	}{
		{"VitessShardsToPostgres", "pg"},
		{"VitessShardsToMySQL", "mysql"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			// restartSource is unused here (Shape-A consolidation spike,
			// no source-disruption); ignore it.
			mysqlDSN, grpcEndpoint, _, vtCleanup := startShardedVTTestServer(t, keyspace, 2)
			defer vtCleanup()

			// Sharded table: customer keyed by customer_id, hash vindex
			// so vtgate distributes rows across both shards (-80, 80-).
			applySQL(t, mysqlDSN, `
				CREATE TABLE customer (
					customer_id BIGINT       NOT NULL,
					email       VARCHAR(255) NOT NULL,
					region      VARCHAR(64)  NOT NULL,
					PRIMARY KEY (customer_id)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
			applySQL(t, mysqlDSN, `ALTER VSCHEMA ON commerce.customer ADD VINDEX hash(customer_id) USING hash`)
			time.Sleep(3 * time.Second) // schema-tracker settle

			// Seed disjoint key ranges. With the hash vindex Vitess
			// scatters these; the spike asserts on consolidated totals
			// + per-shard discriminator values, not a fixed id→shard
			// map (mirrors the existing vstream multi-shard test).
			applySQL(t, mysqlDSN+"&multiStatements=true", `
				INSERT INTO customer (customer_id, email, region) VALUES
					(1,'c1@ex.com','us-east'),(2,'c2@ex.com','us-east'),
					(3,'c3@ex.com','us-west'),(4,'c4@ex.com','us-west'),
					(5,'c5@ex.com','eu'),(6,'c6@ex.com','eu'),
					(7,'c7@ex.com','ap'),(8,'c8@ex.com','ap');`)
			time.Sleep(2 * time.Second)

			var targetDSN string
			var targetEngineName string
			switch tc.targetKind {
			case "pg":
				dsn, cl := startPGTarget(t)
				defer cl()
				targetDSN = dsn
				targetEngineName = "postgres"
			case "mysql":
				dsn, cl := startMySQLTarget(t)
				defer cl()
				targetDSN = dsn
				targetEngineName = "mysql"
			}

			// vttestserver is vanilla Vitess; the FlavorPlanetScale
			// engine (registered name "planetscale") drives VStream
			// against it — proven by the existing vstream suite.
			srcEng, ok := engines.Get("planetscale")
			if !ok {
				t.Fatalf("source engine %q not registered", "planetscale")
			}
			tgtEng, ok := engines.Get(targetEngineName)
			if !ok {
				t.Fatalf("target engine %q not registered", targetEngineName)
			}

			// Per the proto-design's N-process model: ONE per-shard
			// stream-equivalent per shard, both consolidating into the
			// single `customer` target table. The spike runs them as
			// two sequential Migrator passes (bulk-copy only — CDC
			// handoff is a documented spike gap, see prep-doc) because
			// vttestserver's per-shard VStream COPY is already covered
			// by the engine-level suite; the Shape-A-specific pain is
			// in the *consolidation* (shared target, discriminator,
			// populated-target preflight), which the Migrator path
			// surfaces directly.
			//
			// Shard "1" and shard "2" here are logical labels for the
			// two per-shard streams. vttestserver scatters rows by
			// hash; the spike drives a single source DSN per pass but
			// stamps a distinct discriminator per pass to *simulate*
			// two physically-distinct shard sources landing in one
			// target. This is a deliberate spike simplification — the
			// real topology has two physical shard DSNs (documented in
			// the prep-doc's "harness fidelity" caveat).

			sluiceSrcDSN := fmt.Sprintf(
				"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_auto_discover_shards=true",
				mysqlDSN, grpcEndpoint,
			)

			// --- Shard 1 stream: cold-start into a FRESH target. ---
			runShardConsolidation(t, shardConsolidationParams{
				srcEng: srcEng, tgtEng: tgtEng,
				srcDSN: sluiceSrcDSN, tgtDSN: targetDSN,
				shardCol: shardCol, shardVal: 1,
				expectPopulatedTarget: false,
			})

			// --- Shard 2 stream: must land ALONGSIDE shard 1's data.
			// This is the populated-target bulk-copy bypass (roadmap
			// §4 piece 2 / Bug 9). The spike OBSERVES the refusal here
			// without the bypass, then re-runs with --force-cold-start
			// to demonstrate the *silent-corruption hazard* the loud
			// preflight must replace. See prep-doc Piece 2.
			runShardConsolidation(t, shardConsolidationParams{
				srcEng: srcEng, tgtEng: tgtEng,
				srcDSN: sluiceSrcDSN, tgtDSN: targetDSN,
				shardCol: shardCol, shardVal: 2,
				expectPopulatedTarget: true,
			})

			// --- Consolidated assertions ---
			if tc.targetKind == "pg" {
				pgDSN := targetDSN
				total := pgRowCount(t, pgDSN, `SELECT count(*) FROM customer`)
				// 8 source rows stamped shard 1 + 8 stamped shard 2 =
				// 16 (the simulated two-physical-shard topology). The
				// composite PK (source_shard_id, customer_id) is what
				// prevents the second pass colliding with the first.
				if total != 16 {
					t.Fatalf("consolidated customer count = %d; want 16 (8 per simulated shard, composite PK keeps them disjoint)", total)
				}
				shards := pgStrings(t, pgDSN, `SELECT DISTINCT source_shard_id::text FROM customer ORDER BY 1`)
				if len(shards) != 2 || shards[0] != "1" || shards[1] != "2" {
					t.Fatalf("distinct source_shard_id = %v; want [1 2]", shards)
				}
				// Loud-fail observation: every consolidated row MUST
				// have a NON-NULL discriminator. A NULL here would be
				// exactly the silent-corruption shape the §4 preflight
				// must prevent.
				nulls := pgRowCount(t, pgDSN, `SELECT count(*) FROM customer WHERE source_shard_id IS NULL`)
				if nulls != 0 {
					t.Fatalf("found %d rows with NULL source_shard_id — silent cross-shard corruption hazard (roadmap §4 gotcha 2)", nulls)
				}
			}
			t.Logf("SPIKE OBSERVATION (%s): consolidation completed; design pain recorded in docs/dev/notes/prep-multi-source-shape-a.md", tc.name)
		})
	}
}

type shardConsolidationParams struct {
	srcEng, tgtEng        ir.Engine
	srcDSN, tgtDSN        string
	shardCol              string
	shardVal              int64
	expectPopulatedTarget bool
}

// runShardConsolidation drives one per-shard stream into the shared
// consolidated target. Post-Phase-7 (2026-05-21) it uses the SHIPPED
// APIs end-to-end:
//   - translate.InjectShardColumn for the IR pass,
//   - pipeline.preflightShardConsolidation for the populated-target
//     three-point loud refusal (Decision 3 / DP-2),
//   - pipeline.shardStampRows for the orchestrator-side per-row
//     value stamp (DP-1 option (a) — the bulk-copy half).
func runShardConsolidation(t *testing.T, p shardConsolidationParams) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	// Read source schema once via the engine and apply the SHIPPED
	// IR-stage injection. The pass is pure / copy-on-write; the
	// per-shard value is supplied via ShardColumnSpec to the value
	// wrap below (DP-1's two-surface split).
	sr, err := p.srcEng.OpenSchemaReader(ctx, p.srcDSN)
	if err != nil {
		t.Fatalf("open source schema reader: %v", err)
	}
	schema, err := sr.ReadSchema(ctx)
	migcore.CloseIf(sr)
	if err != nil {
		t.Fatalf("read source schema: %v", err)
	}
	injected, err := translate.InjectShardColumn(schema, p.shardCol, ir.Varchar{Length: 64})
	if err != nil {
		t.Fatalf("translate.InjectShardColumn: %v", err)
	}

	sw, err := p.tgtEng.OpenSchemaWriter(ctx, p.tgtDSN)
	if err != nil {
		t.Fatalf("open target schema writer: %v", err)
	}
	defer migcore.CloseIf(sw)
	rw, err := p.tgtEng.OpenRowWriter(ctx, p.tgtDSN)
	if err != nil {
		t.Fatalf("open target row writer: %v", err)
	}
	defer migcore.CloseIf(rw)
	rr, err := p.srcEng.OpenRowReader(ctx, p.srcDSN)
	if err != nil {
		t.Fatalf("open source row reader: %v", err)
	}
	defer migcore.CloseIf(rr)

	// Shape-A populated-target preflight: the LOUD replacement for
	// --force-cold-start's silent skip. On the first pass (target
	// empty) every per-table check is short-circuited by the
	// IsTableEmpty probe; on the second pass the three-point
	// assertion must pass before any row moves.
	shardValue := fmt.Sprintf("%d", p.shardVal)
	if err := preflightShardConsolidation(ctx, injected, rw, p.shardCol, shardValue); err != nil {
		t.Fatalf("shard preflight refused (shard %d): %v", p.shardVal, err)
	}

	if err := sw.CreateTablesWithoutConstraints(ctx, injected); err != nil {
		t.Fatalf("create consolidated table: %v", err)
	}

	shard := ShardColumnSpec{Name: p.shardCol, Value: shardValue}
	for _, tbl := range injected.Tables {
		if err := copyTable(ctx, rr, rw, tbl, nil /*redactor*/, shard); err != nil {
			t.Fatalf("copy table %q (shard %d): %v", tbl.Name, p.shardVal, err)
		}
	}
	if err := sw.CreateIndexes(ctx, injected); err != nil {
		t.Fatalf("create indexes: %v", err)
	}
}
