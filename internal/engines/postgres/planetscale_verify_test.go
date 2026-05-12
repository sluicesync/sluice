//go:build psverify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Verification tests for PlanetScale Postgres (PS-PG). Gated behind
// the psverify build tag because they need real PS-PG credentials
// supplied via env vars and consequently can't run in CI's hermetic
// container model.
//
// Usage (from repo root, on a shell with PLANETSCALE_CREDENTIALS.env
// already sourced):
//
//	go test -tags=psverify -v -count=1 -run 'TestPSPG' \
//	  ./internal/engines/postgres/...
//
// Each test phase that creates objects on PS-PG cleans them up before
// returning so the same database can host repeated runs.

package postgres

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// dsnPair returns the source/destination PS-PG DSNs. Tries env vars
// first, then falls back to reading the repo-root
// PLANETSCALE_CREDENTIALS.env file so callers don't have to wrestle
// with shell quoting around the MySQL DSN's parentheses. Skips the
// test cleanly when neither path turns up the values.
func dsnPair(t *testing.T) (sourceDSN, destDSN string) {
	t.Helper()
	sourceDSN = lookupCred(t, "SLUICE_POSTGRES_SOURCE")
	destDSN = lookupCred(t, "SLUICE_POSTGRES_DESTINATION")
	if sourceDSN == "" || destDSN == "" {
		t.Skip("SLUICE_POSTGRES_SOURCE/DESTINATION not found in env or PLANETSCALE_CREDENTIALS.env")
	}
	return sourceDSN, destDSN
}

// lookupCred returns the named credential. Reads os.Getenv first
// (cheapest, plays well with normal CI patterns), then walks up from
// the test's working directory looking for a PLANETSCALE_CREDENTIALS.env
// file and parses KEY=VALUE pairs out of it. The .env file is the
// dev-loop path; env vars are the CI path.
func lookupCred(t *testing.T, key string) string {
	t.Helper()
	if v := os.Getenv(key); v != "" {
		return v
	}
	path, ok := findCredsFile()
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
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		val := strings.TrimPrefix(line, prefix)
		// Strip a single layer of surrounding quotes if present —
		// the PG DSNs in the file are double-quoted so URI special
		// characters survive shell sourcing.
		val = strings.TrimSpace(val)
		if (strings.HasPrefix(val, `"`) && strings.HasSuffix(val, `"`)) ||
			(strings.HasPrefix(val, "'") && strings.HasSuffix(val, "'")) {
			val = val[1 : len(val)-1]
		}
		return val
	}
	if err := scanner.Err(); err != nil {
		t.Logf("scan %s: %v", path, err)
	}
	return ""
}

// findCredsFile walks upward from the current working directory
// looking for PLANETSCALE_CREDENTIALS.env. Tests run with their
// package as the working directory; the file lives at the repo
// root, so a small upward walk is enough.
func findCredsFile() (string, bool) {
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

// TestPSPG_Connectivity is Phase 1 of the verification plan: prove
// pgx can open a connection to each PS-PG instance, ping it, and
// run a trivial SELECT 1. Anything that goes wrong here is a config
// problem (TLS, credentials, hostname) — we want a clear failure
// before the more elaborate phases below.
func TestPSPG_Connectivity(t *testing.T) {
	sourceDSN, destDSN := dsnPair(t)

	for _, c := range []struct {
		role string
		dsn  string
	}{
		{"source", sourceDSN},
		{"destination", destDSN},
	} {
		t.Run(c.role, func(t *testing.T) {
			db, err := sql.Open("pgx", c.dsn)
			if err != nil {
				t.Fatalf("sql.Open: %v", err)
			}
			defer func() { _ = db.Close() }()

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			if err := db.PingContext(ctx); err != nil {
				t.Fatalf("ping: %v", err)
			}

			var one int
			if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
				t.Fatalf("SELECT 1: %v", err)
			}
			if one != 1 {
				t.Errorf("SELECT 1 = %d; want 1", one)
			}

			// Server version — useful in the test log when something
			// surprises us later. PS-PG advertises a Postgres version
			// string; capture it for the writeup.
			var version string
			if err := db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
				t.Fatalf("version(): %v", err)
			}
			t.Logf("%s: version() = %q", c.role, version)

			// Capture wal_level and a few other settings up-front. The
			// CDC test downstream needs wal_level=logical; surface it
			// here so the operator sees it in the connectivity log
			// before the CDC test fails.
			settings := []string{"wal_level", "max_wal_senders", "max_replication_slots"}
			for _, s := range settings {
				var v string
				err := db.QueryRowContext(ctx,
					"SELECT setting FROM pg_settings WHERE name = $1", s,
				).Scan(&v)
				if err != nil {
					t.Logf("%s: pg_settings %s: %v", c.role, s, err)
					continue
				}
				t.Logf("%s: %s = %q", c.role, s, v)
			}

			// Check whether the connecting role has the REPLICATION
			// attribute. CDC needs it; calling it out here lets us
			// document the requirement clearly.
			var canReplicate bool
			err = db.QueryRowContext(ctx,
				"SELECT rolreplication FROM pg_roles WHERE rolname = current_user",
			).Scan(&canReplicate)
			if err != nil {
				t.Logf("%s: pg_roles rolreplication: %v", c.role, err)
			} else {
				t.Logf("%s: current_user has REPLICATION attribute = %v", c.role, canReplicate)
			}

			// PostGIS detection — informational; the user noted it
			// isn't enabled today but can be turned on.
			var hasPostGIS bool
			err = db.QueryRowContext(ctx,
				"SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'postgis')",
			).Scan(&hasPostGIS)
			if err != nil {
				t.Logf("%s: pg_extension lookup: %v", c.role, err)
			} else {
				t.Logf("%s: postgis extension installed = %v", c.role, hasPostGIS)
			}
		})
	}
}

// TestPSPG_SchemaReaderRoundTrip is Phase 2: seed a small schema on
// the source, run sluice's SchemaReader, assert the IR shape matches
// the seed. Cleanup happens via DROP TABLE in a deferred call so the
// test is idempotent across runs.
func TestPSPG_SchemaReaderRoundTrip(t *testing.T) {
	sourceDSN, _ := dsnPair(t)

	db, err := sql.Open("pgx", sourceDSN)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Use a sluice-prefixed schema so the verification doesn't trip
	// over operator-owned tables and cleanup is unambiguous.
	const schemaName = "sluice_psverify"
	if _, err := db.ExecContext(ctx,
		"DROP SCHEMA IF EXISTS "+schemaName+" CASCADE",
	); err != nil {
		t.Fatalf("pre-clean drop schema: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		"CREATE SCHEMA "+schemaName,
	); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	defer func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dropCancel()
		if _, err := db.ExecContext(dropCtx,
			"DROP SCHEMA IF EXISTS "+schemaName+" CASCADE",
		); err != nil {
			t.Logf("post-clean drop schema: %v", err)
		}
	}()

	const seedDDL = `
		CREATE TABLE sluice_psverify.users (
			id          BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			email       VARCHAR(255) NOT NULL UNIQUE,
			active      BOOLEAN      NOT NULL DEFAULT TRUE,
			created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
		);
		CREATE TABLE sluice_psverify.posts (
			id      BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			user_id BIGINT NOT NULL REFERENCES sluice_psverify.users(id) ON DELETE CASCADE,
			body    TEXT   NOT NULL
		);
		CREATE INDEX posts_user_id_idx ON sluice_psverify.posts (user_id);
	`
	if _, err := db.ExecContext(ctx, seedDDL); err != nil {
		t.Fatalf("seed DDL: %v", err)
	}

	// Schema reader needs the schema name on the DSN. Append it.
	psDSN := withSchemaParam(sourceDSN, schemaName)

	eng := Engine{}
	sr, err := eng.OpenSchemaReader(ctx, psDSN)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(sr)

	got, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}

	if len(got.Tables) != 2 {
		var names []string
		for _, tb := range got.Tables {
			names = append(names, tb.Name)
		}
		t.Fatalf("tables = %v; want exactly 2 (users, posts)", names)
	}

	users := findTablePS(got, "users")
	posts := findTablePS(got, "posts")
	if users == nil || posts == nil {
		t.Fatalf("missing users or posts; have %+v", got.Tables)
	}

	// Spot-check the IR shape — we're not retesting the entire schema
	// reader, just confirming PS-PG's pg_catalog answers match what
	// the reader expects.
	if c := findColumnByName(users, "id"); c == nil {
		t.Errorf("users.id missing")
	} else if _, ok := c.Type.(ir.Integer); !ok {
		t.Errorf("users.id type = %T; want ir.Integer", c.Type)
	}
	if c := findColumnByName(users, "active"); c == nil {
		t.Errorf("users.active missing")
	} else if _, ok := c.Type.(ir.Boolean); !ok {
		t.Errorf("users.active type = %T; want ir.Boolean", c.Type)
	}
	if users.PrimaryKey == nil || len(users.PrimaryKey.Columns) != 1 ||
		users.PrimaryKey.Columns[0].Column != "id" {
		t.Errorf("users PK = %+v; want PK on id", users.PrimaryKey)
	}
	if len(posts.ForeignKeys) != 1 {
		t.Errorf("posts FKs = %d; want 1", len(posts.ForeignKeys))
	} else if posts.ForeignKeys[0].ReferencedTable != "users" ||
		posts.ForeignKeys[0].OnDelete != ir.FKActionCascade {
		t.Errorf("posts FK = %+v; want users on-delete cascade", posts.ForeignKeys[0])
	}
}

// TestPSPG_CDCReaderBasic is Phase 4 of the verification plan: open
// a CDCReader against PS-PG, perform INSERT/UPDATE/DELETE on a test
// table, and assert the events arrive on the change channel with
// the expected shape.
//
// Skips when wal_level != logical or the connecting role lacks
// REPLICATION. Phase 1's connectivity test logs both so the operator
// can see why this skips before re-running with elevated config.
func TestPSPG_CDCReaderBasic(t *testing.T) {
	sourceDSN, _ := dsnPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", sourceDSN)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Pre-flight: skip cleanly when the database isn't configured
	// for logical replication. Failing here would be misleading —
	// it's an operator config decision, not a sluice bug.
	var walLevel string
	if err := db.QueryRowContext(ctx,
		"SELECT setting FROM pg_settings WHERE name = 'wal_level'",
	).Scan(&walLevel); err != nil {
		t.Fatalf("read wal_level: %v", err)
	}
	if walLevel != "logical" {
		t.Skipf("wal_level = %q on PS-PG source; need 'logical' for CDC", walLevel)
	}
	var canReplicate bool
	if err := db.QueryRowContext(ctx,
		"SELECT rolreplication FROM pg_roles WHERE rolname = current_user",
	).Scan(&canReplicate); err == nil && !canReplicate {
		t.Skipf("current_user lacks REPLICATION attribute; CDC will fail without it")
	}

	const schemaName = "sluice_psverify_cdc"
	if _, err := db.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+schemaName+" CASCADE"); err != nil {
		t.Fatalf("pre-clean: %v", err)
	}
	if _, err := db.ExecContext(ctx, "CREATE SCHEMA "+schemaName); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	defer func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dropCancel()
		if _, err := db.ExecContext(dropCtx, "DROP SCHEMA IF EXISTS "+schemaName+" CASCADE"); err != nil {
			t.Logf("post-clean: %v", err)
		}
	}()

	const seedDDL = `
		CREATE TABLE sluice_psverify_cdc.users (
			id    BIGINT       PRIMARY KEY,
			email VARCHAR(255) NOT NULL,
			active BOOLEAN     NOT NULL DEFAULT TRUE
		);
		ALTER TABLE sluice_psverify_cdc.users REPLICA IDENTITY FULL;
	`
	if _, err := db.ExecContext(ctx, seedDDL); err != nil {
		t.Fatalf("seed DDL: %v", err)
	}

	psDSN := withSchemaParam(sourceDSN, schemaName)

	eng := Engine{}
	rdr, err := eng.OpenCDCReader(ctx, psDSN)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer closeIf(rdr)

	// Empty position = "from now".
	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	// Settle window — let the slot fully attach to the publication
	// before we generate events.
	time.Sleep(500 * time.Millisecond)

	const dml = `
		INSERT INTO sluice_psverify_cdc.users (id, email, active) VALUES
			(1, 'alice@example.com', TRUE),
			(2, 'bob@example.com',   FALSE);
		UPDATE sluice_psverify_cdc.users SET active = FALSE WHERE id = 1;
		DELETE FROM sluice_psverify_cdc.users WHERE id = 2;
	`
	if _, err := db.ExecContext(ctx, dml); err != nil {
		t.Fatalf("DML: %v", err)
	}

	// Drain four events: 2 inserts, 1 update, 1 delete.
	got := drainPSChanges(t, ctx, changes, 4, 30*time.Second)
	if len(got) != 4 {
		t.Fatalf("got %d changes; want 4", len(got))
	}
	if _, ok := got[0].(ir.Insert); !ok {
		t.Errorf("got[0] = %T; want ir.Insert", got[0])
	}
	if _, ok := got[2].(ir.Update); !ok {
		t.Errorf("got[2] = %T; want ir.Update", got[2])
	}
	if _, ok := got[3].(ir.Delete); !ok {
		t.Errorf("got[3] = %T; want ir.Delete", got[3])
	}
}

// TestPSPG_CDCReader_FailoverFlag is the PlanetScale Postgres
// counterpart to the integration TestCDCReader_FailoverFlag_PG17:
// cold-start a CDC reader, query pg_replication_slots.failover,
// assert it's true. This is the load-bearing verification for
// roadmap item #7 — the operational story is that PS-PG slots
// silently disappear on failover unless created with FAILOVER true
// AND added to the cluster's permanent-slots config.
//
// Cleans up the slot afterwards so repeated runs don't leak.
func TestPSPG_CDCReader_FailoverFlag(t *testing.T) {
	sourceDSN, _ := dsnPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", sourceDSN)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Skip when wal_level isn't logical or REPLICATION attr is
	// missing — same gates as TestPSPG_CDCReaderBasic.
	var walLevel string
	if err := db.QueryRowContext(ctx,
		"SELECT setting FROM pg_settings WHERE name = 'wal_level'",
	).Scan(&walLevel); err != nil {
		t.Fatalf("read wal_level: %v", err)
	}
	if walLevel != "logical" {
		t.Skipf("wal_level = %q; need 'logical'", walLevel)
	}
	var canReplicate bool
	if err := db.QueryRowContext(ctx,
		"SELECT rolreplication FROM pg_roles WHERE rolname = current_user",
	).Scan(&canReplicate); err == nil && !canReplicate {
		t.Skipf("current_user lacks REPLICATION attribute")
	}

	// Confirm the server is PG 17+; the FAILOVER flag exists only
	// on those versions, and the test below would always assert
	// false on older servers. Skip cleanly with the version logged.
	var versionNum int
	if err := db.QueryRowContext(ctx, "SHOW server_version_num").Scan(&versionNum); err != nil {
		t.Fatalf("server_version_num: %v", err)
	}
	t.Logf("PS-PG server_version_num = %d", versionNum)
	if versionNum < 170000 {
		t.Skipf("PS-PG is PG <17 (%d); FAILOVER flag is unsupported", versionNum)
	}

	// Drop any leftover slot from a previous run so this test is
	// idempotent. Errors here are informational only.
	if _, err := db.ExecContext(ctx,
		"SELECT pg_drop_replication_slot('sluice_slot') FROM pg_replication_slots WHERE slot_name = 'sluice_slot'",
	); err != nil {
		t.Logf("pre-clean drop slot: %v", err)
	}

	const schemaName = "sluice_psverify_failover"
	if _, err := db.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+schemaName+" CASCADE"); err != nil {
		t.Fatalf("pre-clean schema: %v", err)
	}
	if _, err := db.ExecContext(ctx, "CREATE SCHEMA "+schemaName); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	defer func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dropCancel()
		// Always try to drop the slot; pg_drop_replication_slot is
		// safe to call when the slot doesn't exist.
		if _, err := db.ExecContext(dropCtx,
			"SELECT pg_drop_replication_slot('sluice_slot') FROM pg_replication_slots WHERE slot_name = 'sluice_slot'",
		); err != nil {
			t.Logf("post-clean drop slot: %v", err)
		}
		if _, err := db.ExecContext(dropCtx, "DROP SCHEMA IF EXISTS "+schemaName+" CASCADE"); err != nil {
			t.Logf("post-clean schema: %v", err)
		}
	}()

	const seedDDL = `
		CREATE TABLE sluice_psverify_failover.users (
			id BIGINT PRIMARY KEY
		);
		ALTER TABLE sluice_psverify_failover.users REPLICA IDENTITY FULL;
	`
	if _, err := db.ExecContext(ctx, seedDDL); err != nil {
		t.Fatalf("seed: %v", err)
	}

	psDSN := withSchemaParam(sourceDSN, schemaName)

	eng := Engine{}
	rdr, err := eng.OpenCDCReader(ctx, psDSN)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer closeIf(rdr)

	if _, err := rdr.StreamChanges(ctx, ir.Position{}); err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	// The slot should now exist with failover=true. Use a separate
	// connection (not the replication conn) to query the view.
	var failover bool
	err = db.QueryRowContext(ctx,
		"SELECT failover FROM pg_replication_slots WHERE slot_name = $1",
		"sluice_slot",
	).Scan(&failover)
	if err != nil {
		t.Fatalf("query failover: %v", err)
	}
	if !failover {
		t.Errorf("pg_replication_slots.failover = false on PS-PG; want true (slot will be lost on failover)")
	}
}

// drainPSChanges drains up to want events from the channel with an
// overall timeout. Local copy of the helper used in other CDC tests.
//
// Source-tx boundary events ([ir.TxBegin] / [ir.TxCommit], ADR-0027)
// are silently consumed without counting toward the `want` budget.
// Bug 55: pre-fix the helper accepted every event including the
// boundary markers; after ADR-0027 (which added TxBegin/TxCommit as
// first-class IR change types) that meant the test would drain
// `[TxBegin, Insert, Insert, Update]` and miss the trailing Delete.
// Mirrors the integration-suite drainChanges helper's filter.
func drainPSChanges(
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
			switch c.(type) {
			case ir.TxBegin, ir.TxCommit:
				continue
			}
			got = append(got, c)
		case <-deadline.C:
			return got
		case <-ctx.Done():
			return got
		}
	}
	return got
}

// withSchemaParam returns dsn with `schema=<name>` appended/replaced
// in the URI's query string. PS-PG DSNs come as URIs in the
// credentials file.
func withSchemaParam(dsn, schema string) string {
	if strings.Contains(dsn, "schema=") {
		// Conservative: we don't expect this in the credentials file,
		// but if it surfaces, refuse loudly so we notice rather than
		// silently double-set.
		panic("dsn already has schema= param; refusing to mutate")
	}
	if strings.Contains(dsn, "?") {
		return dsn + "&schema=" + schema
	}
	return dsn + "?schema=" + schema
}

// closeIf calls Close on x if it implements an io.Closer-shaped
// interface. Lifted from the regular integration tests so this file
// stays self-contained.
func closeIf(x any) {
	type closer interface{ Close() error }
	if c, ok := x.(closer); ok {
		_ = c.Close()
	}
}

// findColumnByName is the local copy used here so the file builds
// independently of any unrelated package state.
func findColumnByName(t *ir.Table, name string) *ir.Column {
	for _, c := range t.Columns {
		if c.Name == name {
			return c
		}
	}
	return nil
}

// findTablePS is the psverify-tagged sibling of findTable in
// schema_reader_test.go (which lives under the integration tag and
// therefore isn't visible from this file).
func findTablePS(s *ir.Schema, name string) *ir.Table {
	for _, t := range s.Tables {
		if t.Name == name {
			return t
		}
	}
	return nil
}

// (avoid lint about unused error helpers when phases short-circuit)
var _ = errors.New
