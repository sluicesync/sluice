//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// SPIKE harness — Multi-source aggregation Shape A (roadmap §4).
//
// THIS IS A DESIGN-FIRST SPIKE, NOT A SHIPPED FEATURE. It exists to
// surface the *real* design pain of Shape A's three pieces by running
// a sharded Vitess source into a single consolidated target, rather
// than theorizing. The accompanying design evidence lives in:
//
//   - docs/dev/notes/prep-multi-source-shape-a.md (the prep/research doc)
//   - docs/adr/adr-0048-multi-source-aggregation-shape-a.md (Proposed ADR)
//
// Nothing in this file is wired into production code. The
// `injectShardColumn*` helpers below are THROWAWAY exploratory
// prototypes that simulate, at the test level, what an IR-stage
// discriminator-injection transform *would* do — they are clearly
// marked and deliberately live only in this build-tagged test file so
// the spike can observe behaviour end-to-end without committing a
// production surface. If Shape A is greenlit, the real transform lands
// in internal/translate/ (see the ADR's "Decision" section) and this
// file becomes the permanent integration artifact (drop the
// throwaway helpers, point at the real transform).
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

	"github.com/testcontainers/testcontainers-go"
	mysqltc "github.com/testcontainers/testcontainers-go/modules/mysql"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/orware/sluice/internal/engines"
	"github.com/orware/sluice/internal/ir"

	_ "github.com/orware/sluice/internal/engines/mysql"
	_ "github.com/orware/sluice/internal/engines/postgres"
)

// ---------------------------------------------------------------------
// Source: sharded vttestserver (keyspace `commerce`, shards -80 / 80-,
// table `customer` sharded by `customer_id`). Mirrors the proven
// pattern in internal/engines/mysql/cdc_vstream_integration_test.go
// (re-declared here because that helper is package-private to the
// mysql engine package and the spike drives the pipeline package).
// ---------------------------------------------------------------------

func startShardedVTTestServer(t *testing.T, keyspace string, numShards int) (mysqlDSN, grpcEndpoint string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	const (
		basePort      = 33574
		mysqlPortBase = "33577/tcp"
		grpcPortBase  = "33575/tcp"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	req := testcontainers.ContainerRequest{
		Image:        "vitess/vttestserver:mysql80",
		ExposedPorts: []string{mysqlPortBase, grpcPortBase},
		Env: map[string]string{
			"PORT":            fmt.Sprintf("%d", basePort),
			"KEYSPACES":       keyspace,
			"NUM_SHARDS":      fmt.Sprintf("%d", numShards),
			"MYSQL_BIND_HOST": "0.0.0.0",
		},
		WaitingFor: wait.ForAll(
			wait.ForLog("Local cluster started."),
			wait.ForListeningPort(grpcPortBase),
			wait.ForListeningPort(mysqlPortBase),
		).WithStartupTimeoutDefault(5 * time.Minute),
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
	return mysqlDSN, grpcEndpoint, terminate
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
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c, err := mysqltc.Run(
		ctx, "mysql:8.0",
		mysqltc.WithDatabase("warehouse"),
		mysqltc.WithUsername("root"),
		mysqltc.WithPassword("rootpw"),
	)
	if err != nil {
		t.Fatalf("start mysql target: %v", err)
	}
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
		ctx, "postgres:16",
		pgtc.WithDatabase("warehouse"),
		pgtc.WithUsername("test"),
		pgtc.WithPassword("test"),
		pgtc.BasicWaitStrategies(),
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

// =====================================================================
// THROWAWAY EXPLORATORY PROTOTYPE — discriminator-column injection.
//
// This simulates, at the test level, an IR-stage transform that adds
// a sluice-injected `source_shard_id` column to a table and rewrites
// the PK to be composite (shard, source_pk). The REAL design must live
// in internal/translate/ as a pure IR pass (see ADR-0048 Decision 1).
// It is reproduced here ONLY so the spike can run end-to-end and the
// prep-doc can record the OBSERVED pain (PK rewrite, value population,
// diff/verify drift). DO NOT promote this code; it is intentionally
// crude (no column-origin marker, no IdempotentRowWriter PK plumbing,
// no CDC-path coverage) precisely so the gaps are visible.
// =====================================================================

// injectShardColumnIntoSchema returns a deep-ish copy of schema with
// `colName` (BIGINT NOT NULL) appended to every table and folded into
// the PK as the leading column. Crude on purpose — see banner.
func injectShardColumnIntoSchema(schema *ir.Schema, colName string) *ir.Schema {
	out := &ir.Schema{Views: schema.Views}
	for _, tbl := range schema.Tables {
		nt := *tbl
		nt.Columns = append(append([]*ir.Column{}, tbl.Columns...), &ir.Column{
			Name:     colName,
			Type:     ir.Integer{Width: 64},
			Nullable: false,
			// NOTE (spike finding): there is no IR field to mark this
			// column as sluice-injected vs source-derived. The real
			// design needs one (ADR-0048 Decision 1) or diff/verify
			// flags it as drift — observed below in
			// assertConsolidatedSchema's commentary.
		})
		if tbl.PrimaryKey != nil {
			npk := *tbl.PrimaryKey
			npk.Columns = append([]ir.IndexColumn{{Column: colName}}, tbl.PrimaryKey.Columns...)
			nt.PrimaryKey = &npk
		}
		out.Tables = append(out.Tables, &nt)
	}
	return out
}

// shardValueRowReader wraps an ir.RowReader and stamps every row with
// the shard's discriminator value. This is the THROWAWAY analogue of
// what the real value-population step would do; it deliberately sits
// outside the IR transform to make the "where does population belong?"
// question concrete in the prep-doc.
type shardValueRowReader struct {
	inner    ir.RowReader
	colName  string
	shardVal int64
}

func (r *shardValueRowReader) ReadRows(ctx context.Context, table *ir.Table) (<-chan ir.Row, error) {
	src, err := r.inner.ReadRows(ctx, table)
	if err != nil {
		return nil, err
	}
	out := make(chan ir.Row)
	go func() {
		defer close(out)
		for row := range src {
			row[r.colName] = r.shardVal
			select {
			case out <- row:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// Err delegates to the wrapped reader: this decorator only mutates
// rows in flight, it has no failure surface of its own.
func (r *shardValueRowReader) Err() error { return r.inner.Err() }

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
			mysqlDSN, grpcEndpoint, vtCleanup := startShardedVTTestServer(t, keyspace, 2)
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
// consolidated target. It uses the Migrator directly with the
// THROWAWAY shard-injection wrappers so the spike can observe exactly
// where the production design has to intervene.
func runShardConsolidation(t *testing.T, p shardConsolidationParams) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	// Read source schema once via the engine so the spike can apply
	// the throwaway IR-stage injection BEFORE handing it to the
	// Migrator-equivalent path. NOTE: the production design routes
	// this through internal/translate (a pure IR pass) — see
	// ADR-0048. The spike does it inline to keep the harness small.
	sr, err := p.srcEng.OpenSchemaReader(ctx, p.srcDSN)
	if err != nil {
		t.Fatalf("open source schema reader: %v", err)
	}
	schema, err := sr.ReadSchema(ctx)
	closeIf(sr)
	if err != nil {
		t.Fatalf("read source schema: %v", err)
	}
	injected := injectShardColumnIntoSchema(schema, p.shardCol)

	sw, err := p.tgtEng.OpenSchemaWriter(ctx, p.tgtDSN)
	if err != nil {
		t.Fatalf("open target schema writer: %v", err)
	}
	defer closeIf(sw)
	rw, err := p.tgtEng.OpenRowWriter(ctx, p.tgtDSN)
	if err != nil {
		t.Fatalf("open target row writer: %v", err)
	}
	defer closeIf(rw)
	rr, err := p.srcEng.OpenRowReader(ctx, p.srcDSN)
	if err != nil {
		t.Fatalf("open source row reader: %v", err)
	}
	defer closeIf(rr)

	// SPIKE OBSERVATION (Piece 2): on the populated-target pass, run
	// the existing cold-start preflight to capture its behaviour. We
	// EXPECT it to refuse (Bug 9), which is the design evidence: the
	// real Shape-A bypass must replace this with a discriminator-aware
	// loud preflight, NOT --force-cold-start (which is silent).
	if p.expectPopulatedTarget {
		err := preflightColdStart(ctx, injected, rw, false /*force*/, preflightModeMigrate)
		if err == nil {
			t.Logf("SPIKE OBSERVATION: populated-target preflight did NOT refuse — unexpected; record in prep-doc Piece 2")
		} else {
			t.Logf("SPIKE OBSERVATION (Piece 2): cold-start preflight refused as designed: %v", err)
			t.Logf("SPIKE OBSERVATION (Piece 2): the production Shape-A path must NOT route around this with --force-cold-start (silent PK-collision corruption). It needs a discriminator-aware loud preflight — see ADR-0048 Decision 2.")
		}
		// Proceed past the refusal the way --force-cold-start would,
		// to demonstrate that the composite PK (shard, source_pk) is
		// what actually makes the second shard's rows land cleanly —
		// i.e. the bypass is *correct* IF AND ONLY IF the discriminator
		// guarantees PK disjointness. That conditional is the entire
		// design point of Piece 2.
	}

	if err := sw.CreateTablesWithoutConstraints(ctx, injected); err != nil {
		t.Fatalf("create consolidated table: %v", err)
	}

	stamped := &shardValueRowReader{inner: rr, colName: p.shardCol, shardVal: p.shardVal}
	for _, tbl := range injected.Tables {
		if err := copyTable(ctx, stamped, rw, tbl, nil /*redactor*/); err != nil {
			t.Fatalf("copy table %q (shard %d): %v", tbl.Name, p.shardVal, err)
		}
	}
	if err := sw.CreateIndexes(ctx, injected); err != nil {
		t.Fatalf("create indexes: %v", err)
	}
}
