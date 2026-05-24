//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for the MySQL CDC reader. Boots a MySQL container
// with binlog enabled (the default on 8.0 + an explicit log-bin name
// for clarity), seeds a table, opens the reader at "from now", then
// performs INSERT/UPDATE/DELETE and asserts the expected sequence of
// ir.Change events arrives.

package mysql

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"

	"github.com/testcontainers/testcontainers-go"
	mysqltc "github.com/testcontainers/testcontainers-go/modules/mysql"
)

// startMySQLForCDC boots a MySQL container with binlog enabled. We
// can't reuse the existing startMySQL helper from the pipeline package
// because that one uses default container args; the reproducible thing
// to do is to spell out the binlog flags here even though MySQL 8.0
// happens to default to ROW-formatted binlogs.
func startMySQLForCDC(t *testing.T) (dsn string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := mysqltc.Run(
		ctx,
		"mysql:8.0",
		mysqltc.WithDatabase("source_db"),
		mysqltc.WithUsername("root"),
		mysqltc.WithPassword("rootpw"),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Cmd: []string{
					"mysqld",
					"--server-id=1",
					"--log-bin=mysql-bin",
					"--binlog-format=ROW",
					"--binlog-row-image=FULL",
				},
			},
		}),
	)
	if err != nil {
		t.Fatalf("start container: %v", err)
	}

	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}

	conn, err := container.ConnectionString(ctx, "parseTime=true")
	if err != nil {
		terminate()
		t.Fatalf("connection string: %v", err)
	}
	return conn, terminate
}

// applyMySQL runs a possibly-multi-statement DDL/DML script against a
// MySQL DSN. Mirrors the helper in the pipeline package so this file
// stays self-contained.
func applyMySQL(t *testing.T, dsn, sqlText string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn+"&multiStatements=true")
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

// TestCDCReader_BasicChangeStream is the spine test for the binlog
// reader: write some rows after StreamChanges starts and assert each
// one comes back as the expected ir.Change variant with the right
// table, position, and decoded values.
func TestCDCReader_BasicChangeStream(t *testing.T) {
	dsn, cleanup := startMySQLForCDC(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE users (
			id     BIGINT       NOT NULL AUTO_INCREMENT,
			email  VARCHAR(255) NOT NULL,
			active TINYINT(1)   NOT NULL DEFAULT 1,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`
	applyMySQL(t, dsn, seedDDL)

	eng := Engine{Flavor: FlavorVanilla}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	// Empty position = "from now". Anything done before this point
	// (the seed DDL) is excluded from the stream.
	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	// The binlog syncer registers asynchronously; give it a moment to
	// catch up to "now" before we generate events. Otherwise the very
	// first INSERT can be slightly ahead of the registration boundary
	// and get dropped. 200ms is conservative — local docker is faster.
	time.Sleep(200 * time.Millisecond)

	const dml = `
		INSERT INTO users (email, active) VALUES
			('alice@example.com', 1),
			('bob@example.com',   0);
		UPDATE users SET active = 0 WHERE email = 'alice@example.com';
		DELETE FROM users WHERE email = 'bob@example.com';
	`
	applyMySQL(t, dsn, dml)

	// Drain four events: 2 inserts, 1 update, 1 delete.
	got := drainChanges(t, ctx, changes, 4, 30*time.Second)

	if len(got) != 4 {
		// Surface the streaming-side error if pump captured one;
		// otherwise the channel just hadn't filled yet.
		if cdcRdr, ok := rdr.(*CDCReader); ok {
			if streamErr := cdcRdr.Err(); streamErr != nil {
				t.Fatalf("got %d changes; want 4 (stream error: %v)", len(got), streamErr)
			}
		}
		t.Fatalf("got %d changes; want 4", len(got))
	}

	insAlice, ok := got[0].(ir.Insert)
	if !ok {
		t.Fatalf("change[0] = %T; want ir.Insert", got[0])
	}
	if insAlice.Table != "users" {
		t.Errorf("change[0].Table = %q; want users", insAlice.Table)
	}
	if email, _ := insAlice.Row["email"].(string); email != "alice@example.com" {
		t.Errorf("change[0].Row[email] = %#v; want alice@example.com", insAlice.Row["email"])
	}
	if active, _ := insAlice.Row["active"].(bool); !active {
		t.Errorf("change[0].Row[active] = %#v; want true", insAlice.Row["active"])
	}

	insBob, ok := got[1].(ir.Insert)
	if !ok {
		t.Fatalf("change[1] = %T; want ir.Insert", got[1])
	}
	if active, _ := insBob.Row["active"].(bool); active {
		t.Errorf("change[1].Row[active] = %#v; want false", insBob.Row["active"])
	}

	upd, ok := got[2].(ir.Update)
	if !ok {
		t.Fatalf("change[2] = %T; want ir.Update", got[2])
	}
	if upd.Before == nil || upd.After == nil {
		t.Fatalf("update missing Before/After: %+v", upd)
	}
	if before, _ := upd.Before["active"].(bool); !before {
		t.Errorf("update.Before[active] = %#v; want true", upd.Before["active"])
	}
	if after, _ := upd.After["active"].(bool); after {
		t.Errorf("update.After[active] = %#v; want false", upd.After["active"])
	}

	del, ok := got[3].(ir.Delete)
	if !ok {
		t.Fatalf("change[3] = %T; want ir.Delete", got[3])
	}
	// Bug 88: the CDC reader narrows the DELETE Before-image to PK
	// columns before emit (see filterDeleteBefore in cdc_reader.go).
	// Under the seed DDL above (PRIMARY KEY id), the Before should
	// carry exactly {id} — non-PK columns (email, active) are
	// excluded so the applier doesn't construct WHERE clauses with
	// nil-IS-NULL predicates that fail to match under MINIMAL /
	// NOBLOB binlog_row_image. Bob is row id=2 from the INSERT
	// ordering above.
	if _, ok := del.Before["id"]; !ok {
		t.Errorf("delete.Before missing PK column id: %+v", del.Before)
	}
	if id, _ := del.Before["id"].(int64); id != 2 {
		t.Errorf("delete.Before[id] = %#v; want int64(2) (bob's row)", del.Before["id"])
	}
	if _, present := del.Before["email"]; present {
		t.Errorf("delete.Before unexpectedly carries non-PK email column (Bug 88 narrowing regressed?): %+v", del.Before)
	}
	if _, present := del.Before["active"]; present {
		t.Errorf("delete.Before unexpectedly carries non-PK active column (Bug 88 narrowing regressed?): %+v", del.Before)
	}

	// Position bookkeeping: every emitted change must carry a non-empty
	// position the engine can decode. Also: positions should be
	// monotonically non-decreasing in their canonical comparison form.
	for i, c := range got {
		if c.Pos().Engine != "mysql" {
			t.Errorf("change[%d].Pos.Engine = %q; want mysql", i, c.Pos().Engine)
		}
		if c.Pos().Token == "" {
			t.Errorf("change[%d].Pos.Token is empty", i)
		}
		if _, ok, err := decodeBinlogPos(c.Pos()); !ok || err != nil {
			t.Errorf("change[%d].Pos failed to decode: ok=%v err=%v", i, ok, err)
		}
	}
}

// TestCDCReader_PlanetScaleReturnsVStreamReader is a unit-style
// guard inside the integration suite (no docker dependency in the
// assertion path) that confirms FlavorPlanetScale's OpenCDCReader
// returns the VStream-backed reader rather than the binlog one.
// The flavor used to declare CDC=None and short-circuit; with the
// VStream phase B work, it now declares CDCVStream and the engine
// dispatches on flavor.
//
// We don't open the actual stream here — that needs real PS
// credentials and is covered by the psverify suite. This test
// verifies only that the dispatch produces the right reader type.
func TestCDCReader_PlanetScaleReturnsVStreamReader(t *testing.T) {
	eng := Engine{Flavor: FlavorPlanetScale}
	rdr, err := eng.OpenCDCReader(context.Background(), "user:pw@tcp(127.0.0.1:3306)/db")
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	if _, ok := rdr.(*vstreamCDCReader); !ok {
		t.Errorf("OpenCDCReader returned %T; want *vstreamCDCReader", rdr)
	}
}

// TestCDCReader_Truncate verifies that MySQL's TRUNCATE TABLE
// surfaces as ir.Truncate on the change channel, not as a silent
// schema-cache invalidation. PG's pgoutput emits typed truncate
// messages natively; on MySQL we recognise TRUNCATE by parsing the
// query text inside QUERY_EVENT.
func TestCDCReader_Truncate(t *testing.T) {
	dsn, cleanup := startMySQLForCDC(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`
	applyMySQL(t, dsn, seedDDL)

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, dsn)
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

	// Brief settle so the binlog syncer is positioned at "now"
	// before the DML lands.
	time.Sleep(200 * time.Millisecond)

	// Issue an INSERT, then a TRUNCATE. We expect two events on
	// the channel: ir.Insert for the row, then ir.Truncate for
	// the table.
	const dml = `
		INSERT INTO users (email) VALUES ('alice@example.com');
		TRUNCATE TABLE users;
	`
	applyMySQL(t, dsn, dml)

	got := drainChanges(t, ctx, changes, 2, 30*time.Second)
	if len(got) != 2 {
		t.Fatalf("got %d changes; want 2 (1 Insert + 1 Truncate)", len(got))
	}
	if _, ok := got[0].(ir.Insert); !ok {
		t.Errorf("change[0] = %T; want ir.Insert", got[0])
	}
	trunc, ok := got[1].(ir.Truncate)
	if !ok {
		t.Fatalf("change[1] = %T; want ir.Truncate", got[1])
	}
	if trunc.Table != "users" {
		t.Errorf("truncate.Table = %q; want \"users\"", trunc.Table)
	}
	// The Position should be decodable — the same shape every
	// other emitted change carries.
	if trunc.Pos().Engine != "mysql" || trunc.Pos().Token == "" {
		t.Errorf("truncate.Pos = %+v; want non-empty mysql position", trunc.Pos())
	}
}

// drainChanges reads up to want row-level events from changes, with
// an overall timeout. The returned slice may be shorter than want
// if the stream closed early or the timeout fired — caller asserts.
// Source-tx boundary events (TxBegin / TxCommit, ADR-0027) are
// silently consumed without counting toward want — the assertions
// in this test file target row-level events; boundary coverage
// lives in the applier integration tests.
func drainChanges(
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
			// TxBegin/TxCommit are applier-internal tx-boundary
			// signals (ADR-0027); ir.SchemaSnapshot is the ADR-0049
			// schema-history boundary event (a reader emits one at
			// first-touch + on each true DDL delta). Both are
			// orthogonal infra on the change stream, not DML — the
			// data-flow tests that use this helper count row/tx
			// changes, so skip them here. Chunk B's own schema-history
			// pins use dedicated collectors (drainSnapshots), not this
			// shared helper, so this does not weaken them.
			switch c.(type) {
			case ir.TxBegin, ir.TxCommit, ir.SchemaSnapshot:
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

// _ ensures reflect is referenced when the assertions move; keeps the
// import set honest if the test grows.
var _ = reflect.DeepEqual
