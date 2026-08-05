// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite" // pure-Go driver — no cgo, no container needed

	"sluicesync.dev/sluice/internal/engines/internal/triggercdc"
)

// Real-SQLite-file pins for the item-115 SOURCE-SIDE CONSUMER REGISTRY. These
// exercise the ACTUAL SQL (modernc is pure Go, so no Docker): the upsert, the
// registry read with its source-clock age, the schema-version gate, and the
// prune's cut. The d1-trigger transport shares this CDCReader and this SQL — its
// HTTP decode is pinned separately over the mock in d1_cdc_test.go — and
// pgtrigger's own SQL is pinned in its integration test.

// seedRegisteredSource writes a temp SQLite file carrying the v2 change-log
// shape: rows id=1..n, the meta row at the current schema version, and the
// consumer registry. It is what `sluice trigger setup` produces at this version.
func seedRegisteredSource(t *testing.T, n int64) string {
	t.Helper()
	path := seedChangeLog(t, n)
	db := openSeed(t, path)
	defer func() { _ = db.Close() }()
	ctx := context.Background()
	for _, stmt := range []string{
		`CREATE TABLE "` + ChangeLogMetaTable + `" (singleton_pk INTEGER PRIMARY KEY CHECK (singleton_pk = 1), ` +
			`schema_version INTEGER NOT NULL, installed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')))`,
		`INSERT INTO "` + ChangeLogMetaTable + `" (singleton_pk, schema_version) VALUES (1, 2)`,
		`CREATE TABLE "` + ChangeLogConsumersTable + `" (consumer_id TEXT PRIMARY KEY, applied_id INTEGER NOT NULL, ` +
			`updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')))`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("seed %q: %v", stmt, err)
		}
	}
	return path
}

func openSeed(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	return db
}

// execSeed runs one statement against the seeded file (used to hand-write the
// fixtures an OLDER sluice binary would have left behind).
func execSeed(t *testing.T, path, stmt string, args ...any) {
	t.Helper()
	db := openSeed(t, path)
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(context.Background(), stmt, args...); err != nil {
		t.Fatalf("exec %q: %v", stmt, err)
	}
}

// registeredFrontier reads one consumer's recorded frontier back.
func registeredFrontier(t *testing.T, path, consumerID string) (int64, bool) {
	t.Helper()
	db := openSeed(t, path)
	defer func() { _ = db.Close() }()
	var applied int64
	err := db.QueryRowContext(context.Background(),
		`SELECT applied_id FROM "`+ChangeLogConsumersTable+`" WHERE consumer_id = ?`, consumerID).Scan(&applied)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false
	}
	if err != nil {
		t.Fatalf("read registry: %v", err)
	}
	return applied, true
}

// TestItem115_SlowPeersUnreadRowsSurviveAFastPeersPrune is THE gate for roadmap
// item 115, and the test that fails at HEAD.
//
// Two syncs read one source change log (the staged-wave shape the docs
// recommend). The fast one has durably applied through id 100 and runs the
// auto-prune; the slow one has only applied through 20. Before the fix the cut
// was the FAST stream's frontier alone, so ids 21..100 — rows the slow sync had
// not read — were deleted before it ever saw them, silently and permanently.
//
// The independent expected value here is the SLOW peer's registered frontier
// (20), which comes from the slow stream's own target and is not derived from
// anything the fast stream's prune path computes.
func TestItem115_SlowPeersUnreadRowsSurviveAFastPeersPrune(t *testing.T) {
	path := seedRegisteredSource(t, 100)
	fast := &CDCReader{b: localBackend(path)}
	slow := &CDCReader{b: localBackend(path)}
	ctx := context.Background()

	if err := slow.RegisterChangeLogConsumer(ctx, "slow-sync", `{"last_id":20}`); err != nil {
		t.Fatalf("register slow: %v", err)
	}
	if err := fast.RegisterChangeLogConsumer(ctx, "fast-sync", `{"last_id":100}`); err != nil {
		t.Fatalf("register fast: %v", err)
	}

	deleted, err := fast.PruneConsumedChangeLogToRegisteredMin(ctx, "fast-sync", `{"last_id":100}`, 0)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if deleted != 20 {
		t.Errorf("deleted = %d; want 20 (cut at the SLOW peer's frontier, not the fast peer's)", deleted)
	}
	got := remainingIDs(t, path)
	if len(got) != 80 || got[0] != 21 || got[len(got)-1] != 100 {
		t.Fatalf("remaining = %v (len %d); want ids 21..100 — every row the slow sync has not read must survive",
			got, len(got))
	}
}

// TestItem115_PruneAdvancesAsTheSlowPeerCatchesUp pins the other half: the
// registry is not a permanent brake. Once the slow peer's frontier moves, the
// next prune reaps up to the new MIN.
func TestItem115_PruneAdvancesAsTheSlowPeerCatchesUp(t *testing.T) {
	path := seedRegisteredSource(t, 100)
	r := &CDCReader{b: localBackend(path)}
	ctx := context.Background()

	if err := r.RegisterChangeLogConsumer(ctx, "slow-sync", `{"last_id":20}`); err != nil {
		t.Fatalf("register slow: %v", err)
	}
	if err := r.RegisterChangeLogConsumer(ctx, "fast-sync", `{"last_id":100}`); err != nil {
		t.Fatalf("register fast: %v", err)
	}
	if _, err := r.PruneConsumedChangeLogToRegisteredMin(ctx, "fast-sync", `{"last_id":100}`, 0); err != nil {
		t.Fatalf("first prune: %v", err)
	}
	// The slow peer catches up to 60 and re-registers.
	if err := r.RegisterChangeLogConsumer(ctx, "slow-sync", `{"last_id":60}`); err != nil {
		t.Fatalf("re-register slow: %v", err)
	}
	deleted, err := r.PruneConsumedChangeLogToRegisteredMin(ctx, "fast-sync", `{"last_id":100}`, 0)
	if err != nil {
		t.Fatalf("second prune: %v", err)
	}
	if deleted != 40 {
		t.Errorf("second prune deleted = %d; want 40 (ids 21..60, now that the slow peer has applied them)", deleted)
	}
	if got := remainingIDs(t, path); len(got) != 40 || got[0] != 61 {
		t.Errorf("remaining = %v; want ids 61..100", got)
	}
}

// TestItem115_ColdCopyingPeerBlocksThePruneEntirely pins the cold-start window:
// a peer that has registered but applied nothing (frontier 0 — it is still bulk
// copying, which can take hours) holds the whole change log. Its CDC anchor is
// already taken, so every row now in the log is one it must eventually apply.
func TestItem115_ColdCopyingPeerBlocksThePruneEntirely(t *testing.T) {
	path := seedRegisteredSource(t, 50)
	r := &CDCReader{b: localBackend(path)}
	ctx := context.Background()

	// An EMPTY token is what the registration sidecar publishes before the
	// first apply.
	if err := r.RegisterChangeLogConsumer(ctx, "copying-sync", ""); err != nil {
		t.Fatalf("register copying: %v", err)
	}
	if err := r.RegisterChangeLogConsumer(ctx, "live-sync", `{"last_id":50}`); err != nil {
		t.Fatalf("register live: %v", err)
	}
	deleted, err := r.PruneConsumedChangeLogToRegisteredMin(ctx, "live-sync", `{"last_id":50}`, 0)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if deleted != 0 {
		t.Errorf("deleted = %d; want 0 while a peer is still cold-copying", deleted)
	}
	if got := remainingIDs(t, path); len(got) != 50 {
		t.Errorf("remaining = %d rows; want all 50", len(got))
	}
}

// TestItem115_OwnStaleRegistrationDoesNotBlockOurOwnPrune pins the regression
// the pgtrigger auto-prune integration test caught. This stream registered 0 at
// cold start and the registry refresh is a one-minute cadence, so for the first
// minute its own row understates its frontier. Taking that row at face value
// would make a fresh sync's auto-prune a no-op for its first minute; the fresh
// token it just read from its own target is the authority for itself.
func TestItem115_OwnStaleRegistrationDoesNotBlockOurOwnPrune(t *testing.T) {
	path := seedRegisteredSource(t, 50)
	r := &CDCReader{b: localBackend(path)}
	ctx := context.Background()
	if err := r.RegisterChangeLogConsumer(ctx, "self", ""); err != nil { // cold-start registration
		t.Fatalf("register self: %v", err)
	}

	deleted, err := r.PruneConsumedChangeLogToRegisteredMin(ctx, "self", `{"last_id":40}`, 0)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if deleted != 40 {
		t.Errorf("deleted = %d; want 40 — our own stale registry row must not block our own prune", deleted)
	}
}

// TestItem115_EmptyRegistryRefuses is the primary-hazard pin at the SQL layer:
// the registry table exists and is empty. Nothing may be deleted.
func TestItem115_EmptyRegistryRefuses(t *testing.T) {
	path := seedRegisteredSource(t, 10)
	r := &CDCReader{b: localBackend(path)}

	deleted, err := r.PruneConsumedChangeLogToRegisteredMin(context.Background(), "self", `{"last_id":10}`, 0)
	if err == nil {
		t.Fatal("prune against an EMPTY registry returned nil; want a loud refusal")
	}
	if deleted != 0 || len(remainingIDs(t, path)) != 10 {
		t.Errorf("deleted = %d, remaining = %v; a refused prune must delete nothing",
			deleted, remainingIDs(t, path))
	}
}

// TestItem115_UnregisteredCallerRefuses: peers are registered but WE are not
// (our registration has been failing). A cut derived from peers alone is not a
// safe bound for us.
func TestItem115_UnregisteredCallerRefuses(t *testing.T) {
	path := seedRegisteredSource(t, 10)
	r := &CDCReader{b: localBackend(path)}
	ctx := context.Background()
	if err := r.RegisterChangeLogConsumer(ctx, "someone-else", `{"last_id":9}`); err != nil {
		t.Fatalf("register peer: %v", err)
	}

	if _, err := r.PruneConsumedChangeLogToRegisteredMin(ctx, "me", `{"last_id":10}`, 0); err == nil {
		t.Fatal("prune by an unregistered caller returned nil; want a loud refusal")
	}
	if len(remainingIDs(t, path)) != 10 {
		t.Error("a refused prune must delete nothing")
	}
}

// TestItem115_UnmigratedChangeLogFailsClosed is the cross-version gate for a NEW
// binary meeting an OLD source: the change log has no registry table and its
// meta says schema_version 1.
//
// The fixture is hand-written to be exactly what a PRE-registry sluice produces
// — no registry table, version 1 — deliberately NOT generated by this version's
// installer, which would make the gate self-referential (the item-104 trap).
func TestItem115_UnmigratedChangeLogFailsClosed(t *testing.T) {
	path := seedChangeLog(t, 10) // v1 shape: change-log only
	execSeed(t, path, `CREATE TABLE "`+ChangeLogMetaTable+`" (singleton_pk INTEGER PRIMARY KEY, schema_version INTEGER NOT NULL)`)
	execSeed(t, path, `INSERT INTO "`+ChangeLogMetaTable+`" (singleton_pk, schema_version) VALUES (1, 1)`)
	r := &CDCReader{b: localBackend(path)}

	_, err := r.PruneConsumedChangeLogToRegisteredMin(context.Background(), "self", `{"last_id":10}`, 0)
	if err == nil {
		t.Fatal("prune against an un-migrated change log returned nil; want a loud refusal")
	}
	if !errors.Is(err, triggercdc.ErrConsumerRegistryUnavailable) {
		t.Errorf("error %v does not wrap ErrConsumerRegistryUnavailable", err)
	}
	if !strings.Contains(err.Error(), "trigger setup") {
		t.Errorf("error %q does not name the migration action", err)
	}
	if len(remainingIDs(t, path)) != 10 {
		t.Error("a refused prune must delete nothing")
	}
}

// TestItem115_OldBinaryReranSetupFailsClosed is the other cross-version
// direction, and the reason the gate checks the VERSION and not just the table.
// An older sluice sharing this source re-runs its own `trigger setup`: that
// leaves the registry table in place (it does not know to drop it) but rewrites
// schema_version back to 1. That binary streams WITHOUT registering, so its sync
// is invisible — and its trace on the source is the only evidence we get.
//
// The fixture is again what the OLD binary writes (version 1), not something
// this version can produce.
func TestItem115_OldBinaryReranSetupFailsClosed(t *testing.T) {
	path := seedRegisteredSource(t, 10)
	r := &CDCReader{b: localBackend(path)}
	ctx := context.Background()
	if err := r.RegisterChangeLogConsumer(ctx, "self", `{"last_id":10}`); err != nil {
		t.Fatalf("register: %v", err)
	}
	// The old binary's setup: same idempotent meta upsert, its own version.
	execSeed(t, path, `UPDATE "`+ChangeLogMetaTable+`" SET schema_version = 1`)

	_, err := r.PruneConsumedChangeLogToRegisteredMin(ctx, "self", `{"last_id":10}`, 0)
	if err == nil {
		t.Fatal("prune with schema_version below the registry floor returned nil; want a loud refusal")
	}
	if !errors.Is(err, triggercdc.ErrConsumerRegistryUnavailable) {
		t.Errorf("error %v does not wrap ErrConsumerRegistryUnavailable", err)
	}
	if len(remainingIDs(t, path)) != 10 {
		t.Error("a refused prune must delete nothing")
	}
}

// TestItem115_RegistrationOverwritesTheFrontierInBothDirections pins the upsert
// contract. Forward is the ordinary case; BACKWARD must also land — a target
// restored to an earlier point resumes with a lower frontier, and a registry
// that only ratcheted forward would let the next prune cut above what that
// target has actually applied.
func TestItem115_RegistrationOverwritesTheFrontierInBothDirections(t *testing.T) {
	path := seedRegisteredSource(t, 10)
	r := &CDCReader{b: localBackend(path)}
	ctx := context.Background()

	if err := r.RegisterChangeLogConsumer(ctx, "s", `{"last_id":8}`); err != nil {
		t.Fatalf("register: %v", err)
	}
	if got, ok := registeredFrontier(t, path, "s"); !ok || got != 8 {
		t.Fatalf("frontier = %d (found %v); want 8", got, ok)
	}
	if err := r.RegisterChangeLogConsumer(ctx, "s", `{"last_id":3}`); err != nil {
		t.Fatalf("re-register lower: %v", err)
	}
	if got, _ := registeredFrontier(t, path, "s"); got != 3 {
		t.Errorf("frontier = %d after a BACKWARD registration; want 3 (a rolled-back target must be able to "+
			"lower its own frontier)", got)
	}
}

// TestItem115_RegistrationRefusesAForeignToken pins that the registry inherits
// the position codec's loud refusal: a non-trigger-CDC token cannot be recorded
// as a change-log frontier.
func TestItem115_RegistrationRefusesAForeignToken(t *testing.T) {
	path := seedRegisteredSource(t, 10)
	r := &CDCReader{b: localBackend(path)}

	if err := r.RegisterChangeLogConsumer(context.Background(), "s", `{"slot":"s","lsn":"0/16B3748"}`); err == nil {
		t.Error("RegisterChangeLogConsumer(foreign token) returned nil; want a loud refuse")
	}
	if _, ok := registeredFrontier(t, path, "s"); ok {
		t.Error("a refused registration must not write a row")
	}
}

// TestItem115_OperatorPruneIsClampedToTheRegistry pins the sibling path: the
// operator-run `sluice trigger prune` (engine entry point [Prune]) reaps to the
// cut the CLI computed from ONE stream's frontier, and is clamped the same way.
// It deliberately does NOT fail closed without registry evidence — see the
// comment at the clamp — but it must not out-delete a registered peer.
func TestItem115_OperatorPruneIsClampedToTheRegistry(t *testing.T) {
	path := seedRegisteredSource(t, 100)
	r := &CDCReader{b: localBackend(path)}
	if err := r.RegisterChangeLogConsumer(context.Background(), "slow-sync", `{"last_id":20}`); err != nil {
		t.Fatalf("register slow: %v", err)
	}

	res, err := Prune(context.Background(), path, PruneOptions{Cut: 100})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if res.ClampedTo != 20 {
		t.Errorf("ClampedTo = %d; want 20 (the operator's cut of 100 lowered to the slowest consumer)", res.ClampedTo)
	}
	if res.Deleted != 20 {
		t.Errorf("deleted = %d; want 20", res.Deleted)
	}
	if got := remainingIDs(t, path); len(got) != 80 {
		t.Errorf("remaining = %d rows; want 80 (ids 21..100 survive)", len(got))
	}
}

// TestItem115_OperatorPruneWithoutARegistryIsUnchanged pins the deliberate
// non-regression: a v1 source (no registry table) keeps the exact ADR-0137
// Phase-A behaviour for the explicit operator command, rather than refusing an
// action that has always been safe for a single stream.
func TestItem115_OperatorPruneWithoutARegistryIsUnchanged(t *testing.T) {
	path := seedChangeLog(t, 10)
	res, err := Prune(context.Background(), path, PruneOptions{Cut: 6})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if res.ClampedTo != 0 || res.Deleted != 6 {
		t.Errorf("ClampedTo = %d, deleted = %d; want 0 and 6 (no registry ⇒ unchanged behaviour)",
			res.ClampedTo, res.Deleted)
	}
}

// TestItem115_SetupInstallsTheRegistryOnARealFile is the migration pin: running
// the real installer against a v1 file adds the registry and lifts the version,
// so a pre-registry install is migrated by re-running `sluice trigger setup`
// with no data movement and no manual DDL.
func TestItem115_SetupInstallsTheRegistryOnARealFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "src.db")
	db := openSeed(t, path)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatalf("create t: %v", err)
	}
	// The v1 shape an older sluice left behind: change log + meta at version 1,
	// no registry. Hand-written, not produced by this version's installer.
	for _, stmt := range []string{
		`CREATE TABLE "` + ChangeLogTable + `" (id INTEGER PRIMARY KEY AUTOINCREMENT, op TEXT, tbl TEXT, before TEXT, after TEXT, captured_at TEXT)`,
		`CREATE TABLE "` + ChangeLogMetaTable + `" (singleton_pk INTEGER PRIMARY KEY CHECK (singleton_pk = 1), schema_version INTEGER NOT NULL, installed_at TEXT NOT NULL DEFAULT '')`,
		`INSERT INTO "` + ChangeLogMetaTable + `" (singleton_pk, schema_version) VALUES (1, 1)`,
		`CREATE TABLE "` + ChangeLogColumnsTable + `" (tbl TEXT PRIMARY KEY, columns TEXT NOT NULL)`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("seed v1 %q: %v", stmt, err)
		}
	}
	_ = db.Close()

	if _, err := Setup(ctx, path, SetupOptions{Tables: []string{"t"}}); err != nil {
		t.Fatalf("Setup (the migration): %v", err)
	}

	r := &CDCReader{b: localBackend(path)}
	if err := r.RegisterChangeLogConsumer(ctx, "s", `{"last_id":0}`); err != nil {
		t.Fatalf("register after migration: %v", err)
	}
	exec, err := localBackend(path).openExec(ctx, false)
	if err != nil {
		t.Fatalf("open exec: %v", err)
	}
	defer func() { _ = exec.close() }()
	exists, ver, err := exec.consumerRegistryState(ctx)
	if err != nil {
		t.Fatalf("registry state: %v", err)
	}
	if !exists || ver < triggercdc.ConsumerRegistrySchemaVer {
		t.Errorf("after migration: registry exists = %v, schema_version = %d; want true and >= %d",
			exists, ver, triggercdc.ConsumerRegistrySchemaVer)
	}
}

// TestItem115_TeardownRemovesTheRegistry pins that the engine's promise to
// remove every trace covers the new table too — a leftover registry on a source
// whose triggers are gone would hold a future prune back forever.
func TestItem115_TeardownRemovesTheRegistry(t *testing.T) {
	stmts := renderTeardownDDL(nil, false)
	var found bool
	for _, s := range stmts {
		if strings.Contains(s, ChangeLogConsumersTable) {
			found = true
		}
	}
	if !found {
		t.Errorf("teardown DDL %v does not drop %q", stmts, ChangeLogConsumersTable)
	}
}
