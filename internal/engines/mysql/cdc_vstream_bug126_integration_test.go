//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 126 regression: the FlavorPlanetScale engine's non-CDC Open*
// paths (schema-reader, row-reader, …) must not leak sluice's
// vstream_* DSN extensions into the underlying MySQL session.
//
// Before the fix, OpenSchemaReader → parseDSN → openDB handed the
// *gomysql.Config straight to go-sql-driver/mysql with vstream_endpoint
// / vstream_transport / vstream_auth / vstream_shards still in
// cfg.Params; the driver's session init emits each unknown param as a
// `SET vstream_endpoint = …` after the handshake, which self-hosted
// Vitess / vttestserver REJECTS (Error 1105 for the IP-bearing endpoint,
// VT05006 unknown system variable for the rest). A planetscale-source
// cold-start therefore died at "open source schema reader" before any
// data moved. The CDC reader was unaffected — it reads vstream_* from
// cfg.Params first, then dials vtgate over gRPC, never reaching a MySQL
// session — which is why every existing vstream test (all CDC-path)
// passed while this whole class was broken.
//
// This is the FIRST test to drive the planetscale Open* (non-CDC) path
// against real Vitess: it builds a planetscale-shaped DSN that INCLUDES
// the vstream_* params exactly as a real `--source-driver=planetscale`
// CLI invocation would, then exercises OpenSchemaReader + OpenRowReader
// and asserts they SUCCEED. It fails before the openDB strip and passes
// after.
//
// Usage from a shell:
//
//	go test -tags='integration vstream' -v -count=1 -timeout=15m \
//	  -run 'TestVStream_Bug126' ./internal/engines/mysql/...

package mysql

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestVStream_Bug126_PlanetScaleOpenPathsNoVStreamLeak boots
// vttestserver, seeds a table, and opens the planetscale-flavored
// schema reader AND row reader against a DSN carrying the vstream_*
// extensions. Both must succeed: any leaked `SET vstream_* = …` would
// surface as an Error 1105 / VT05006 from vtgate at open time.
func TestVStream_Bug126_PlanetScaleOpenPathsNoVStreamLeak(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
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
	applyVTTestSQL(t, mysqlDSN+"&multiStatements=true", `
		INSERT INTO users (email, active) VALUES ('alice@example.com', 1);
		INSERT INTO users (email, active) VALUES ('bob@example.com', 0);
	`)

	// vttestserver's schema tracker is async; give it a beat to pick
	// up the new table before reading information_schema through vtgate.
	time.Sleep(3 * time.Second)

	// The planetscale-shaped DSN a real `--source-driver=planetscale`
	// invocation produces: the embedded-MySQL DSN PLUS sluice's
	// vstream_* extensions. vstream_endpoint deliberately carries the
	// host:port (an IP-bearing endpoint on CI) — that is the param
	// whose leak produced the `Error 1105 … near '.0'` syntax error.
	sluiceDSN := mysqlDSN +
		"&vstream_endpoint=" + grpcEndpoint +
		"&vstream_transport=plaintext" +
		"&vstream_auth=none" +
		"&vstream_shards=0"

	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// --- Schema-reader Open* path (the exact path Bug 126 broke). ---
	sr, err := eng.OpenSchemaReader(ctx, sluiceDSN)
	if err != nil {
		// Pre-fix this is where the bug fires: a leaked
		// `SET vstream_endpoint = …` makes the ping/first query fail
		// with Error 1105 / VT05006.
		if isVStreamLeakError(err) {
			t.Fatalf("Bug 126 regression: vstream_* param leaked into the MySQL session at OpenSchemaReader: %v", err)
		}
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIfCloser(sr)

	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		if isVStreamLeakError(err) {
			t.Fatalf("Bug 126 regression: vstream_* leak surfaced during ReadSchema: %v", err)
		}
		t.Fatalf("ReadSchema: %v", err)
	}

	var users *ir.Table
	for _, tb := range schema.Tables {
		if tb.Name == "users" {
			users = tb
			break
		}
	}
	if users == nil {
		t.Fatalf("ReadSchema returned no `users` table; got %d tables", len(schema.Tables))
	}
	gotCols := make([]string, 0, len(users.Columns))
	for _, c := range users.Columns {
		gotCols = append(gotCols, c.Name)
	}
	for _, want := range []string{"id", "email", "active"} {
		if !contains(gotCols, want) {
			t.Errorf("users schema missing column %q; got %v", want, gotCols)
		}
	}

	// --- Row-reader Open* path (also routes through openDB). ---
	rr, err := eng.OpenRowReader(ctx, sluiceDSN)
	if err != nil {
		if isVStreamLeakError(err) {
			t.Fatalf("Bug 126 regression: vstream_* leak surfaced at OpenRowReader: %v", err)
		}
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer closeIfCloser(rr)

	rowsCh, err := rr.ReadRows(ctx, users)
	if err != nil {
		if isVStreamLeakError(err) {
			t.Fatalf("Bug 126 regression: vstream_* leak surfaced at ReadRows: %v", err)
		}
		t.Fatalf("ReadRows: %v", err)
	}
	emails := make([]string, 0, 2)
	for r := range rowsCh {
		if s, ok := r["email"].(string); ok {
			emails = append(emails, s)
		}
	}
	if len(emails) != 2 {
		t.Fatalf("ReadRows returned %d rows (%v); want 2 seeded rows", len(emails), emails)
	}
	if !contains(emails, "alice@example.com") || !contains(emails, "bob@example.com") {
		t.Errorf("ReadRows emails = %v; want alice + bob", emails)
	}
}

// isVStreamLeakError reports whether err looks like vtgate rejecting a
// leaked `SET vstream_* = …` session variable — the Bug 126 signature.
// Both vtgate error shapes are covered: Error 1105 (the IP-bearing
// vstream_endpoint trips the parser near the `.0` of the address) and
// VT05006 (unknown system variable, for the non-IP vstream_* names).
func isVStreamLeakError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "vstream_") ||
		strings.Contains(msg, "VT05006") ||
		strings.Contains(msg, "1105")
}

// closeIfCloser closes v if its concrete type exposes a Close() error
// method. The ir.SchemaReader / ir.RowReader interfaces don't surface
// Close (the concrete *SchemaReader / *RowReader do), so the test
// releases the pool via a type assertion — mirroring how the existing
// vstream CDC tests close their readers.
func closeIfCloser(v any) {
	if c, ok := v.(interface{ Close() error }); ok {
		_ = c.Close()
	}
}

// contains reports whether s appears in xs.
func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
