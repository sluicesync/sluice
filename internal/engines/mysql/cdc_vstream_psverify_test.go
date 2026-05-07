//go:build psverify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// PlanetScale-MySQL VStream verification tests. Gated behind the
// psverify build tag — same shape as the PS-PG verification suite
// in internal/engines/{postgres,pipeline}.
//
// Usage from a shell with credentials available (env vars or
// PLANETSCALE_CREDENTIALS.env at the repo root):
//
//	go test -tags=psverify -v -count=1 -timeout=10m \
//	  -run 'TestPSVStream' ./internal/engines/mysql/...
//
// The suite exercises the FlavorPlanetScale CDC path against a real
// PlanetScale MySQL database. Tests skip cleanly when credentials
// aren't available so a CI run without secrets is a no-op rather
// than a failure.

package mysql

import (
	"bufio"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// psMySQLDSN returns the PS-MySQL source DSN. Same env-then-file
// pattern as the PS-PG verification suite.
func psMySQLDSN(t *testing.T) string {
	t.Helper()
	dsn := lookupPSMySQLCred(t, "SLUICE_MYSQL_SOURCE")
	if dsn == "" {
		t.Skip("SLUICE_MYSQL_SOURCE not found in env or PLANETSCALE_CREDENTIALS.env")
	}
	return dsn
}

func lookupPSMySQLCred(t *testing.T, key string) string {
	t.Helper()
	if v := os.Getenv(key); v != "" {
		return v
	}
	path, ok := findPSMySQLCredsFile()
	if !ok {
		return ""
	}
	f, err := os.Open(path)
	if err != nil {
		t.Logf("open %s: %v", path, err)
		return ""
	}
	defer func() { _ = f.Close() }()
	prefix := key + "="
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || !strings.HasPrefix(line, prefix) {
			continue
		}
		val := strings.TrimSpace(strings.TrimPrefix(line, prefix))
		if (strings.HasPrefix(val, `"`) && strings.HasSuffix(val, `"`)) ||
			(strings.HasPrefix(val, "'") && strings.HasSuffix(val, "'")) {
			val = val[1 : len(val)-1]
		}
		return val
	}
	return ""
}

func findPSMySQLCredsFile() (string, bool) {
	dir, err := os.Getwd()
	if err != nil {
		return "", false
	}
	for i := 0; i < 8; i++ {
		candidate := filepath.Join(dir, "PLANETSCALE_CREDENTIALS.env")
		if _, err := os.Stat(candidate); err == nil {
			return candidate, true
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", false
		}
		dir = parent
	}
	return "", false
}

// TestPSVStream_Connectivity proves the FlavorPlanetScale engine
// can build a VStream reader against a real PS-MySQL DSN — i.e.,
// the gRPC dial succeeds and the reader's resources are
// constructed. Doesn't open the actual stream (that's the next
// test); errors here would point at TLS, auth, or DSN-parsing
// problems.
func TestPSVStream_Connectivity(t *testing.T) {
	dsn := psMySQLDSN(t)

	eng := Engine{Flavor: FlavorPlanetScale}
	rdr, err := eng.OpenCDCReader(context.Background(), dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	// Probe the underlying SQL endpoint (separate connection from
	// VStream) to confirm the DSN is otherwise sane and the schema
	// reader will work for any orchestrator path that needs it.
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	pingCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	var version string
	if err := db.QueryRowContext(pingCtx, "SELECT VERSION()").Scan(&version); err != nil {
		t.Fatalf("VERSION(): %v", err)
	}
	t.Logf("source: VERSION() = %q", version)
}

// TestPSVStream_BasicChangeStream is the spine integration test:
// open the VStream reader at "current", insert a row on the SQL
// side, and assert the resulting ir.Insert event arrives on the
// CDC channel with the expected shape.
//
// The test creates and drops a sluice-prefixed test table so it
// doesn't disturb operator-owned tables. PlanetScale's vtgate
// proxies most DDL transparently — the test should clean up after
// itself even on failure.
func TestPSVStream_BasicChangeStream(t *testing.T) {
	dsn := psMySQLDSN(t)

	const tableName = "sluice_psvs_users"

	// Pre-clean any leftover table from a previous run.
	if err := psMySQLExec(t, dsn, "DROP TABLE IF EXISTS "+tableName); err != nil {
		t.Fatalf("pre-clean drop: %v", err)
	}

	const seedDDL = `
		CREATE TABLE ` + tableName + ` (
			id     BIGINT       NOT NULL AUTO_INCREMENT,
			email  VARCHAR(255) NOT NULL,
			active TINYINT(1)   NOT NULL DEFAULT 1,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
	`
	if err := psMySQLExec(t, dsn, seedDDL); err != nil {
		t.Fatalf("seed DDL: %v", err)
	}
	defer func() {
		_ = psMySQLExec(t, dsn, "DROP TABLE IF EXISTS "+tableName)
	}()

	eng := Engine{Flavor: FlavorPlanetScale}
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

	// Empty position → "current": stream from head, no copy.
	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	// Settle window — vtgate's stream takes a moment to
	// register at "current". Inserts happening too early can
	// land just before the registration boundary.
	time.Sleep(2 * time.Second)

	if err := psMySQLExec(t, dsn,
		"INSERT INTO "+tableName+" (email, active) VALUES ('alice@psverify.example.com', 1)",
	); err != nil {
		t.Fatalf("DML: %v", err)
	}

	got := drainPSVStreamChanges(t, ctx, changes, 1, 60*time.Second)
	if len(got) != 1 {
		// Surface any reader-side error — the channel may have
		// closed early due to a gRPC failure that's more
		// informative than just "didn't see an event".
		if streamErr := vstreamReaderErr(rdr); streamErr != nil {
			t.Fatalf("got %d changes; want 1 (stream error: %v)", len(got), streamErr)
		}
		t.Fatalf("got %d changes; want 1", len(got))
	}

	ins, ok := got[0].(ir.Insert)
	if !ok {
		t.Fatalf("change[0] = %T; want ir.Insert", got[0])
	}
	if ins.Table != tableName {
		t.Errorf("change[0].Table = %q; want %q", ins.Table, tableName)
	}
	if email, _ := ins.Row["email"].(string); email != "alice@psverify.example.com" {
		t.Errorf("change[0].Row[email] = %#v; want alice@psverify.example.com", ins.Row["email"])
	}
	// VStream advertises positions per-event; the position should
	// decode back to a non-empty shardGtid slice.
	if ins.Position.Engine != engineNameVStream {
		t.Errorf("change[0].Position.Engine = %q; want %q", ins.Position.Engine, engineNameVStream)
	}
	if shards, ok, err := decodeVStreamPos(ins.Position); err != nil || !ok || len(shards) == 0 {
		t.Errorf("change[0].Position decode failed: ok=%v err=%v shards=%v", ok, err, shards)
	}
}

// vstreamReaderErr is a tiny shim that retrieves the Err() value
// from a CDCReader without forcing the test file to import the
// concrete *vstreamCDCReader type into its identifier scope (the
// reader is unexported).
func vstreamReaderErr(rdr ir.CDCReader) error {
	if r, ok := rdr.(*vstreamCDCReader); ok {
		return r.Err()
	}
	return nil
}

// psMySQLExec is the per-test SQL helper. Opens a fresh
// connection (cheap) and runs one statement. Caller-friendly
// errors include the SQL text on failure so the operator can
// figure out which step exploded.
func psMySQLExec(t *testing.T, dsn, sqlText string) error {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, sqlText); err != nil {
		return err
	}
	return nil
}

// drainPSVStreamChanges reads up to want events with a timeout.
// The returned slice may be shorter than want if the stream closed
// early or the timeout fired — caller asserts.
func drainPSVStreamChanges(
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
