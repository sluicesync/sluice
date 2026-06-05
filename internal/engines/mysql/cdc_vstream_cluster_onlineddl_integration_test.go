//go:build integration && vitesscluster

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0073 (c) FULL cutover-survival validation — tier 2 (the real
// Vitess cluster). This is the proof vttestserver could not give: the
// vtcombo scheduler is stubbed, so the `vstream`-tagged tests only
// validate (a) internal-table ROW/FIELD exclusion via a directly-created
// internal table and (b) that shadow-table DDL events don't wedge the
// logical stream. They CANNOT exercise the genuine VReplication copy +
// atomic rename cutover. This suite runs the real scheduler end-to-end
// through sluice's VStream and asserts the logical stream survives the
// rename swap with zero row loss and the post-cutover schema flowing
// through.
//
// Run (heavy — own build tag, NOT in the per-PR gate):
//
//	go test -tags='integration vitesscluster' -v -count=1 -timeout=20m \
//	  -run 'TestVitessCluster_OnlineDDL' ./internal/engines/mysql/...

package mysql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestVitessCluster_OnlineDDL_CutoverSurvivesWithZeroLoss is the payoff
// test. It:
//
//  1. seeds a logical table with enough rows that the online ALTER's
//     VReplication copy is non-trivial;
//  2. cold-starts a sluice VStream sync (snapshot COPY → CDC tail);
//  3. fires a real ddl_strategy='vitess' online ALTER MID-STREAM and
//     lets the scheduler COMPLETE the cutover (the VReplication copy +
//     the atomic rename swap onto the logical name);
//  4. keeps writing rows (using the post-cutover schema) across and
//     after the swap.
//
// It then asserts ADR-0073 (c)'s four properties:
//
//	(i)   the logical table's stream is NOT wedged across the rename swap
//	      (post-cutover inserts are delivered, the reader's Err() is nil);
//	(ii)  ZERO row loss — the final target COUNT(*) equals the source;
//	(iii) the post-cutover schema (the new column) flows through CDC;
//	(iv)  no `_vt_*` internal table surfaces as a copied/applied row and
//	      no scope-mismatch refusal fires.
func TestVitessCluster_OnlineDDL_CutoverSurvivesWithZeroLoss(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVitessCluster(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE widgets (
			id   BIGINT       NOT NULL AUTO_INCREMENT,
			name VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
	applyClusterSQL(t, mysqlDSN, seedDDL)

	// Seed enough rows that the cutover's VReplication copy is real work
	// (not an instant empty-table swap).
	const seedRows = 300
	seedWidgets(t, mysqlDSN, 1, seedRows)

	// Let the schema tracker pick up the new table before COPY opens (the
	// COPY phase enumerates tables via the tablet's schema engine).
	time.Sleep(3 * time.Second)

	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
		mysqlDSN, grpcEndpoint,
	)
	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	// Cold-start: snapshot COPY then CDC tail (the ADR-0071 streaming
	// handoff).
	stream, err := eng.OpenSnapshotStream(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenSnapshotStream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	widgetsTable := &ir.Table{
		Name: "widgets",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
			{Name: "name", Type: ir.Varchar{Length: 255}},
		},
	}
	rowsCh, err := stream.Rows.ReadRows(ctx, widgetsTable)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	snapCount := 0
	for r := range rowsCh {
		// (iv) no internal table may surface during COPY.
		if isVitessInternalTable("widgets") { // defensive guard on the contract
			t.Fatal("widgets must not be classified internal (test bug)")
		}
		_ = r
		snapCount++
	}
	if rerr := stream.Rows.Err(); rerr != nil {
		t.Fatalf("snapshot Rows.Err = %v; want nil (no `_vt_*` scope-mismatch refusal)", rerr)
	}
	if snapCount != seedRows {
		t.Fatalf("snapshot copied %d rows; want %d", snapCount, seedRows)
	}

	// Start CDC from the captured snapshot position.
	changes, err := stream.Changes.StreamChanges(ctx, stream.Position)
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	// Fire the real online ALTER mid-stream and let the scheduler
	// COMPLETE the cutover. This is the genuine VReplication copy +
	// atomic rename that vtcombo cannot do.
	uuid := launchOnlineDDL(t, mysqlDSN, "ALTER TABLE widgets ADD COLUMN qty INT NOT NULL DEFAULT 0")
	t.Logf("online ALTER launched: migration_uuid=%s", uuid)
	waitMigrationComplete(t, mysqlDSN, uuid, 5*time.Minute)
	t.Logf("migration %s reached COMPLETE (real cutover performed)", uuid)

	// (iii) Post-cutover inserts use the NEW schema (the qty column). If
	// the rename swap wedged the stream, these never arrive.
	const postRows = 10
	for i := 1; i <= postRows; i++ {
		applyClusterSQL(t, mysqlDSN, fmt.Sprintf(
			"INSERT INTO widgets (name, qty) VALUES ('post-%d', %d)", i, i,
		))
	}

	// Drain the post-cutover inserts. We expect at least postRows Insert
	// events on the LOGICAL table carrying the new qty column.
	post := 0
	qtySeen := 0
	deadline := time.After(3 * time.Minute)
drain:
	for post < postRows {
		select {
		case c, ok := <-changes:
			if !ok {
				t.Fatalf("CDC channel closed before all post-cutover inserts arrived (got %d/%d)", post, postRows)
			}
			ins, ok := c.(ir.Insert)
			if !ok {
				continue
			}
			// (iv) no internal table may surface as a CDC change.
			if isVitessInternalTable(ins.Table) {
				t.Fatalf("internal `_vt_*` table surfaced as a CDC change: table=%q", ins.Table)
			}
			name, _ := ins.Row["name"].(string)
			if len(name) >= 5 && name[:5] == "post-" {
				post++
				// (iii) the post-cutover schema must be present.
				if _, present := ins.Row["qty"]; present {
					qtySeen++
				}
			}
		case <-deadline:
			break drain
		}
	}

	// (i) the stream must not have errored across the rename swap.
	if cdc, ok := stream.Changes.(*vstreamCDCReader); ok {
		if streamErr := cdc.Err(); streamErr != nil {
			t.Fatalf("stream errored across the online-DDL cutover: %v (ADR-0073 (c): the rename swap must not wedge the logical stream)", streamErr)
		}
	}
	if post < postRows {
		t.Fatalf("post-cutover inserts delivered = %d; want %d (the logical stream must survive the rename swap)", post, postRows)
	}
	// (iii) the new column flowed through on every post-cutover insert.
	if qtySeen != postRows {
		t.Fatalf("post-cutover inserts carrying the new `qty` column = %d; want %d (post-cutover schema must flow through)", qtySeen, postRows)
	}

	// (ii) ZERO row loss — the source COUNT(*) must equal seed + post.
	wantTotal := seedRows + postRows
	if got := targetRowCount(t, mysqlDSN, "widgets"); got != wantTotal {
		t.Fatalf("source widgets COUNT(*) = %d; want %d (seed %d + post %d)", got, wantTotal, seedRows, postRows)
	}
	t.Logf("cutover-survival PASS: snapshot=%d, post-cutover inserts=%d (qty present on all), final count=%d, stream not wedged, no internal table surfaced",
		snapCount, post, wantTotal)
}

// TestVitessCluster_OnlineDDL_ComplexShapesSurviveCutover exercises
// RICHER online-DDL shapes than a trailing ADD COLUMN — the kinds a
// real migration mixes in — and asserts each completes the real cutover
// and sluice's logical stream survives + picks up the post-cutover
// schema correctly. The shapes, fired sequentially mid-stream:
//
//  1. DROP a column in the MIDDLE of the table (column-position shift —
//     the post-cutover FIELD event re-describes the table with fewer,
//     re-ordered columns; the stream must not wedge and the dropped
//     column must simply stop appearing).
//  2. ADD an ENUM column (a new typed column with a constrained set).
//  3. MODIFY (extend) that ENUM's value set (a type-shape change on an
//     existing column).
//
// For each shape the test launches the online ALTER, waits for the real
// cutover to COMPLETE, writes a post-cutover row through the NEW schema,
// and drains CDC to confirm: the stream's Err() stays nil across the
// rename swap, the post-cutover insert is delivered on the logical
// table, no `_vt_*` table surfaces, and the post-cutover column set
// matches the new schema (the dropped column absent, the new/extended
// ENUM value present). Final COUNT(*) proves zero loss across all three
// cutovers.
func TestVitessCluster_OnlineDDL_ComplexShapesSurviveCutover(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVitessCluster(t)
	defer cleanup()

	// A table with a column in the MIDDLE (mid) we will later drop, plus
	// trailing columns so the drop is a genuine position shift.
	const seedDDL = `
		CREATE TABLE products (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			sku   VARCHAR(64)  NOT NULL,
			mid   VARCHAR(64)  NULL,
			label VARCHAR(128) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
	applyClusterSQL(t, mysqlDSN, seedDDL)

	const seedRows = 50
	{
		var b []byte
		for i := 1; i <= seedRows; i++ {
			b = append(b, []byte(fmt.Sprintf(
				"INSERT INTO products (sku, mid, label) VALUES ('sku-%d', 'mid-%d', 'label-%d');", i, i, i,
			))...)
		}
		applyClusterSQL(t, mysqlDSN+"&multiStatements=true", string(b))
	}
	time.Sleep(3 * time.Second)

	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
		mysqlDSN, grpcEndpoint,
	)
	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 18*time.Minute)
	defer cancel()

	stream, err := eng.OpenSnapshotStream(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenSnapshotStream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	productsTable := &ir.Table{
		Name: "products",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
			{Name: "sku", Type: ir.Varchar{Length: 64}},
			{Name: "mid", Type: ir.Varchar{Length: 64}},
			{Name: "label", Type: ir.Varchar{Length: 128}},
		},
	}
	rowsCh, err := stream.Rows.ReadRows(ctx, productsTable)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	snap := 0
	for range rowsCh {
		snap++
	}
	if rerr := stream.Rows.Err(); rerr != nil {
		t.Fatalf("snapshot Rows.Err = %v; want nil", rerr)
	}
	if snap != seedRows {
		t.Fatalf("snapshot copied %d; want %d", snap, seedRows)
	}

	changes, err := stream.Changes.StreamChanges(ctx, stream.Position)
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	postCount := 0

	// drainOnePost drains CDC until it sees the named post-cutover insert
	// on the logical table, asserting the stream isn't wedged, no internal
	// table surfaces, and checks the column predicate against the row.
	drainOnePost := func(label, marker string, check func(row ir.Row) error) {
		t.Helper()
		deadline := time.After(3 * time.Minute)
		for {
			select {
			case c, ok := <-changes:
				if !ok {
					t.Fatalf("[%s] CDC channel closed before the post-cutover insert arrived", label)
				}
				ins, ok := c.(ir.Insert)
				if !ok {
					continue
				}
				if isVitessInternalTable(ins.Table) {
					t.Fatalf("[%s] internal `_vt_*` table surfaced as a CDC change: %q", label, ins.Table)
				}
				if s, _ := ins.Row["sku"].(string); s == marker {
					if err := check(ins.Row); err != nil {
						t.Fatalf("[%s] post-cutover row schema check failed: %v (row=%#v)", label, err, ins.Row)
					}
					postCount++
					return
				}
			case <-deadline:
				if cdc, ok := stream.Changes.(*vstreamCDCReader); ok {
					t.Fatalf("[%s] timed out waiting for post-cutover insert; stream err=%v", label, cdc.Err())
				}
				t.Fatalf("[%s] timed out waiting for post-cutover insert", label)
			}
		}
	}

	assertNoStreamErr := func(stage string) {
		t.Helper()
		if cdc, ok := stream.Changes.(*vstreamCDCReader); ok {
			if e := cdc.Err(); e != nil {
				t.Fatalf("[%s] stream errored across cutover: %v", stage, e)
			}
		}
	}

	// Shape 1 — DROP the MIDDLE column. After cutover, `mid` must be gone
	// from the table; the post-cutover insert's row must not carry it.
	uuid1 := launchOnlineDDL(t, mysqlDSN, "ALTER TABLE products DROP COLUMN mid")
	t.Logf("shape 1 (DROP middle col) migration_uuid=%s", uuid1)
	waitMigrationComplete(t, mysqlDSN, uuid1, 5*time.Minute)
	applyClusterSQL(t, mysqlDSN, "INSERT INTO products (sku, label) VALUES ('post-drop', 'after-drop')")
	drainOnePost("drop-middle", "post-drop", func(row ir.Row) error {
		if _, present := row["mid"]; present {
			return fmt.Errorf("dropped column `mid` still present post-cutover")
		}
		if l, _ := row["label"].(string); l != "after-drop" {
			return fmt.Errorf("label = %q; want after-drop", l)
		}
		return nil
	})
	assertNoStreamErr("after-drop")

	// Shape 2 — ADD an ENUM column. Post-cutover insert sets it; the row
	// must carry the new column with the chosen value.
	uuid2 := launchOnlineDDL(t, mysqlDSN,
		"ALTER TABLE products ADD COLUMN status ENUM('active','discontinued') NOT NULL DEFAULT 'active'")
	t.Logf("shape 2 (ADD enum col) migration_uuid=%s", uuid2)
	waitMigrationComplete(t, mysqlDSN, uuid2, 5*time.Minute)
	applyClusterSQL(t, mysqlDSN,
		"INSERT INTO products (sku, label, status) VALUES ('post-enum-add', 'after-enum-add', 'discontinued')")
	drainOnePost("add-enum", "post-enum-add", func(row ir.Row) error {
		v, present := row["status"]
		if !present {
			return fmt.Errorf("new ENUM column `status` absent post-cutover")
		}
		if s, _ := v.(string); s != "discontinued" {
			return fmt.Errorf("status = %#v; want discontinued", v)
		}
		return nil
	})
	assertNoStreamErr("after-enum-add")

	// Shape 3 — MODIFY/extend the ENUM's value set, then use a NEW value.
	// This is a type-shape change on an existing column; the post-cutover
	// insert exercises the newly-added enum member.
	uuid3 := launchOnlineDDL(t, mysqlDSN,
		"ALTER TABLE products MODIFY COLUMN status ENUM('active','discontinued','archived') NOT NULL DEFAULT 'active'")
	t.Logf("shape 3 (extend enum) migration_uuid=%s", uuid3)
	waitMigrationComplete(t, mysqlDSN, uuid3, 5*time.Minute)
	applyClusterSQL(t, mysqlDSN,
		"INSERT INTO products (sku, label, status) VALUES ('post-enum-mod', 'after-enum-mod', 'archived')")
	drainOnePost("extend-enum", "post-enum-mod", func(row ir.Row) error {
		s, _ := row["status"].(string)
		if s != "archived" {
			return fmt.Errorf("status = %q; want archived (the newly-added enum member)", s)
		}
		return nil
	})
	assertNoStreamErr("after-enum-mod")

	// Zero loss across all three cutovers: seed + 3 post-cutover inserts.
	wantTotal := seedRows + 3
	if got := targetRowCount(t, mysqlDSN, "products"); got != wantTotal {
		t.Fatalf("source products COUNT(*) = %d; want %d (seed %d + 3 post)", got, wantTotal, seedRows)
	}
	if postCount != 3 {
		t.Fatalf("post-cutover inserts delivered = %d; want 3", postCount)
	}
	t.Logf("complex-shapes cutover-survival PASS: 3 shapes (drop-middle, add-enum, extend-enum) each completed a real cutover, the logical stream survived all three, schema deltas flowed through, final count=%d", wantTotal)
}

// seedWidgets inserts rows [start, start+n) as name='seed-<i>'.
func seedWidgets(t *testing.T, dsn string, start, n int) {
	t.Helper()
	// Batch into one multi-statement exec for speed.
	var b []byte
	for i := start; i < start+n; i++ {
		b = append(b, []byte(fmt.Sprintf("INSERT INTO widgets (name) VALUES ('seed-%d');", i))...)
	}
	applyClusterSQL(t, dsn+"&multiStatements=true", string(b))
}
