//go:build integration && vitesscluster

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 27 end-to-end verification (roadmap item 1(e)) on the local
// self-hosted Vitess cluster.
//
// Bug 27 (closed v0.28.0, ADR-0035): VStream delivers spatial values in
// MySQL's on-wire geometry format — `<srid uint32 LE><wkb>` — and the
// decoder must strip the 4-byte SRID prefix so downstream consumers see
// the raw OGC WKB the IR contract (docs/value-types.md) promises. The fix
// lives in decodeVStreamCell (cdc_vstream.go, Type_GEOMETRY) and is
// unit-pinned; this is the END-TO-END leg that was historically deferred
// to the credentials-gated `psverify` real-PlanetScale harness. The local
// cluster IS self-hosted Vitess, so it is a sufficient — and far cheaper —
// end-to-end target.
//
// It doubles as FlavorVitess's first real-cluster exercise: the DSN
// deliberately OMITS vstream_transport / vstream_auth so the flavor's
// self-hosted defaults (plaintext / none, applied in
// applyVStreamFlavorDefaults) are what actually connect. The other cluster
// tests use FlavorPlanetScale and hand-set those params.
//
// Run (heavy — own build tag, NOT in the per-PR gate):
//
//	go test -tags='integration vitesscluster' -v -count=1 -timeout=20m \
//	  -run 'TestVitessCluster_Bug27' ./internal/engines/mysql/...

package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestVitessCluster_Bug27_VStreamPointSRIDStripped seeds a POINT column,
// opens VStream CDC at "current" via FlavorVitess, INSERTs a point, and
// asserts the decoded ir.Geometry value is the OGC WKB with MySQL's
// internal 4-byte SRID prefix stripped — ground-truthed against the
// source's own ST_AsWKB(geom), and cross-checked that it is exactly 4
// bytes shorter than the raw internal storage (HEX(geom)). That 4-byte
// delta is precisely the prefix Bug 27 was failing to strip.
func TestVitessCluster_Bug27_VStreamPointSRIDStripped(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVitessCluster(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE geo_points (
			id    BIGINT      NOT NULL AUTO_INCREMENT,
			label VARCHAR(64) NOT NULL,
			geom  POINT       NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
	applyClusterSQL(t, mysqlDSN, seedDDL)

	// Let the tablet's schema engine register the new table before the
	// VStream FieldEvent (and thus the column-type metadata) is needed.
	time.Sleep(3 * time.Second)

	// FlavorVitess DSN: only the endpoint + shard layout are supplied.
	// vstream_transport / vstream_auth are OMITTED on purpose so the
	// flavor's self-hosted defaults (plaintext / none) are what connect —
	// this is the flavor's first real-cluster exercise.
	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_shards=0",
		mysqlDSN, grpcEndpoint,
	)
	eng := Engine{Flavor: FlavorVitess}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenCDCReader (FlavorVitess): %v", err)
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

	// Settle window: vtgate's stream takes a moment to register at
	// "current"; an INSERT too early lands before the boundary.
	time.Sleep(3 * time.Second)

	const wkt = "POINT(1 2)"
	applyClusterSQL(t, mysqlDSN,
		"INSERT INTO geo_points (label, geom) VALUES ('p1', ST_GeomFromText('"+wkt+"'))")

	// Ground truth from the source: ST_AsWKB returns the OGC WKB WITHOUT
	// the SRID prefix (exactly the IR ir.Geometry contract); HEX(geom)
	// returns MySQL's internal storage WITH the 4-byte prefix.
	wantWKB := queryClusterHexBytes(t, mysqlDSN,
		"SELECT HEX(ST_AsWKB(geom)) FROM geo_points WHERE label='p1'")
	rawInternal := queryClusterHexBytes(t, mysqlDSN,
		"SELECT HEX(geom) FROM geo_points WHERE label='p1'")
	if len(rawInternal) != len(wantWKB)+4 {
		t.Fatalf("sanity: internal storage is %d bytes, ST_AsWKB is %d bytes; expected a 4-byte SRID-prefix delta",
			len(rawInternal), len(wantWKB))
	}

	ins := drainClusterForInsert(t, ctx, changes, "geo_points", 90*time.Second)

	gotVal, ok := ins.Row["geom"]
	if !ok {
		t.Fatalf("ir.Insert.Row has no geom column; row=%#v", ins.Row)
	}
	gotWKB, ok := gotVal.([]byte)
	if !ok {
		t.Fatalf("geom value = %T; want []byte (raw WKB)", gotVal)
	}

	if !bytes.Equal(gotWKB, wantWKB) {
		t.Fatalf("Bug 27: decoded geom WKB = %s; want %s (source ST_AsWKB)\n  raw internal (with SRID prefix) = %s",
			hex.EncodeToString(gotWKB), hex.EncodeToString(wantWKB), hex.EncodeToString(rawInternal))
	}
	// Belt-and-suspenders: the decoded value must NOT still carry the
	// 4-byte SRID prefix that Bug 27 failed to strip.
	if bytes.Equal(gotWKB, rawInternal) {
		t.Fatal("Bug 27 regression: decoded geom still carries the 4-byte SRID prefix (== raw internal storage)")
	}
	t.Logf("Bug 27 verified on FlavorVitess: decoded WKB (%d bytes) == ST_AsWKB, SRID prefix stripped", len(gotWKB))
}

// queryClusterHexBytes runs a single-column HEX(...) query through the
// vtgate MySQL port and decodes the hex string into bytes.
func queryClusterHexBytes(t *testing.T, dsn, q string) []byte {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var h string
	if err := db.QueryRowContext(ctx, q).Scan(&h); err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	b, err := hex.DecodeString(h)
	if err != nil {
		t.Fatalf("decode hex %q: %v", h, err)
	}
	return b
}

// drainClusterForInsert pumps the changes channel until the first
// ir.Insert for the named table arrives (skipping schema/tx metadata
// events), or fails the test on close/timeout/context-cancel.
func drainClusterForInsert(
	t *testing.T,
	ctx context.Context,
	changes <-chan ir.Change,
	table string,
	timeout time.Duration,
) ir.Insert {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		select {
		case c, ok := <-changes:
			if !ok {
				t.Fatalf("changes channel closed before an ir.Insert for %q arrived", table)
			}
			if ins, isInsert := c.(ir.Insert); isInsert && ins.Table == table {
				return ins
			}
		case <-deadline.C:
			t.Fatalf("timed out after %v waiting for an ir.Insert for %q", timeout, table)
		case <-ctx.Done():
			t.Fatalf("context done waiting for ir.Insert: %v", ctx.Err())
		}
	}
}
