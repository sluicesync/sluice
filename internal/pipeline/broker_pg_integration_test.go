//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for Phase 4.5 — backup-as-broker. Two-process
// pattern: an in-test goroutine drives `BackupStream` writing to a
// local-FS chain; another in-test goroutine drives `SyncFromBackup`
// reading from the same destination + applying into a separate target
// database. Both run in the same Go process via goroutines, not as
// separate OS processes — Go's testing framework can drive both with
// shared lifecycle control.
//
// Acceptance criteria covered (per design-logical-backups-phase-4-5.md):
//
//   1. End-to-end PG happy path (TestSyncFromBackup_Postgres_HappyPath).
//   3. Schema evolution (TestSyncFromBackup_SchemaEvolution).
//   4. Cooperative stop (TestSyncFromBackup_StopCommand).
//   5. Restart resumes (TestSyncFromBackup_RestartResumes).
//   6. Cold-start refusal (TestSyncFromBackup_ColdStartRefusal).
//   7. Cold-start with --reset-target-data (TestSyncFromBackup_ColdStartWithReset).
//
// MySQL coverage in broker_mysql_integration_test.go (criterion 2).
// Fan-out (criterion 9) lives in broker_fanout_integration_test.go.

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"
	"github.com/orware/sluice/internal/ir"

	_ "github.com/orware/sluice/internal/engines/postgres"
)

// brokerTestStreamSetup is the boilerplate that every Phase 4.5 PG
// integration test runs through: boot two PG containers (source +
// broker target), seed schema on source, start a publication + slot,
// take a full backup into a local-FS chain. Returns the source DSN,
// the broker target DSN, the chain store, the full's BackupID, and a
// teardown closure that callers must defer.
//
// The full's EndPosition is patched in via createPGLogicalSlotReturningLSN
// so the stream that follows captures every change committed AFTER
// the slot was created.
func brokerTestStreamSetup(t *testing.T, seedDDL string) (
	sourceDSN, brokerTargetDSN string,
	store *LocalStore,
	fullBackupID string,
	teardown func(),
) {
	t.Helper()
	sourceDSN, _, ctn1Teardown := startPostgresLogical(t)
	brokerSourceDSN, brokerTargetDSN, ctn2Teardown := startPostgresLogical(t)
	// brokerSourceDSN is unused — startPostgresLogical's "source"
	// would receive the broker's writes, but in our shape the broker
	// reads from the chain, not from a live source. Just use its
	// targetDSN as the broker's destination DB.
	_ = brokerSourceDSN

	teardown = func() {
		ctn1Teardown()
		ctn2Teardown()
	}

	applyDDL(t, sourceDSN, seedDDL)
	pgEng, _ := engines.Get("postgres")

	dir := t.TempDir()
	var err error
	store, err = NewLocalStore(dir)
	if err != nil {
		teardown()
		t.Fatalf("NewLocalStore: %v", err)
	}

	applyDDL(t, sourceDSN, `CREATE PUBLICATION sluice_pub FOR ALL TABLES`)
	slotLSN, err := createPGLogicalSlotReturningLSN(t, sourceDSN, "sluice_slot")
	if err != nil {
		teardown()
		t.Fatalf("create slot: %v", err)
	}
	t.Cleanup(func() { dropPGLogicalSlot(t, sourceDSN, "sluice_slot") })

	if err := (&Backup{
		Source: pgEng, SourceDSN: sourceDSN, Store: store, SluiceVersion: "test",
	}).Run(context.Background()); err != nil {
		teardown()
		t.Fatalf("Backup.Run: %v", err)
	}
	full, _ := readManifest(context.Background(), store)
	full.Kind = ir.BackupKindFull
	full.EndPosition = ir.Position{
		Engine: "postgres",
		Token:  fmt.Sprintf(`{"slot":"sluice_slot","lsn":%q}`, slotLSN),
	}
	full.BackupID = ir.ComputeBackupID(full)
	if err := writeManifestAt(context.Background(), store, ManifestFileName, full); err != nil {
		teardown()
		t.Fatalf("rewrite full manifest: %v", err)
	}
	fullBackupID = full.BackupID
	return sourceDSN, brokerTargetDSN, store, fullBackupID, teardown
}

// runStreamInGoroutine launches a BackupStream against the supplied
// store + source DSN with small bounds suitable for a 30-60s test.
// Returns a cancel func + a channel that fires when stream.Run
// returns. Callers wait on the channel after cancelling to confirm a
// clean exit.
func runStreamInGoroutine(t *testing.T, sourceDSN, parentRef string, store *LocalStore) (cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	pgEng, _ := engines.Get("postgres")
	stream := &BackupStream{
		Source:                pgEng,
		SourceDSN:             sourceDSN,
		Store:                 store,
		ParentRef:             parentRef,
		RolloverWindow:        2 * time.Second,
		RolloverMaxChanges:    5,
		RolloverMaxBytes:      1 << 30,
		ChunkChanges:          5,
		IncludeEmptyRollovers: false,
		SluiceVersion:         "test",
	}
	ctx, c := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- stream.Run(ctx) }()
	return c, errCh
}

// runBrokerInGoroutine launches a SyncFromBackup against the supplied
// store + broker target DSN. Returns a cancel func + a channel that
// fires when broker.Run returns.
func runBrokerInGoroutine(t *testing.T, brokerTargetDSN, streamID string, store *LocalStore, opts brokerOpts) (cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	pgEng, _ := engines.Get("postgres")
	broker := &SyncFromBackup{
		Target:          pgEng,
		TargetDSN:       brokerTargetDSN,
		Store:           store,
		ChainURL:        "test://" + streamID,
		StreamID:        streamID,
		PollInterval:    opts.PollInterval,
		ApplyBatchSize:  opts.ApplyBatchSize,
		ResetTargetData: opts.ResetTargetData,
		AtChainID:       opts.AtChainID,
		SluiceVersion:   "test",
		brokerStatePath: opts.StatePath,
	}
	ctx, c := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- broker.Run(ctx) }()
	return c, errCh
}

// brokerOpts groups the few SyncFromBackup options tests tune. The
// remaining fields take their defaults.
type brokerOpts struct {
	PollInterval    time.Duration
	ApplyBatchSize  int
	ResetTargetData bool
	AtChainID       string

	// StatePath, when non-empty, sets the broker's `brokerStatePath`
	// override. Used by multi-broker fan-out tests that need distinct
	// state files per broker — without this, two brokers running in
	// the same process against the same chain root race on the
	// shared `manifests/broker_state.json` write (writeBrokerState's
	// `LocalStore.Put` is not goroutine-safe against same-path
	// concurrent writes; one of the brokers occasionally hangs at
	// startup). Production is unaffected — one broker process per
	// chain — so the fix lives in the test surface, not the engine.
	// Empty means "use the default" (`DefaultBrokerStateFilename`).
	StatePath string
}

// TestSyncFromBackup_Postgres_HappyPath exercises Acceptance Criterion 1:
// drive INSERTs on source, observe target catch up via the broker
// within 2 × poll-interval.
func TestSyncFromBackup_Postgres_HappyPath(t *testing.T) {
	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			email VARCHAR(255) NOT NULL
		);
		ALTER TABLE users REPLICA IDENTITY FULL;
		INSERT INTO users (email) VALUES ('alice@example.com');
	`
	sourceDSN, brokerTargetDSN, store, fullBackupID, teardown := brokerTestStreamSetup(t, seedDDL)
	defer teardown()

	// Pre-restore the full into the broker's target so the broker
	// has the schema + base rows before live polling begins. The
	// alternative — operator passes --reset-target-data — is covered
	// by TestSyncFromBackup_ColdStartWithReset.
	pgEng, _ := engines.Get("postgres")
	if err := (&Restore{
		Target: pgEng, TargetDSN: brokerTargetDSN, Store: store,
	}).Run(context.Background()); err != nil {
		t.Fatalf("seed restore: %v", err)
	}

	// Start the source-side stream.
	streamCancel, streamDone := runStreamInGoroutine(t, sourceDSN, fullBackupID, store)
	defer streamCancel()

	// Drive 5 INSERTs on the source so the stream can roll them up.
	for i := 0; i < 5; i++ {
		applyDDL(t, sourceDSN, fmt.Sprintf(
			`INSERT INTO users (email) VALUES ('user%d@example.com');`, i))
	}

	// Wait for at least 1 incremental to land in the chain.
	waitForIncrementals(t, store, 1, 30*time.Second)

	// Start the broker. Use --at-chain-id so the broker treats the
	// pre-restored target as already up to the full's BackupID.
	brokerCancel, brokerDone := runBrokerInGoroutine(t, brokerTargetDSN, "test-broker", store, brokerOpts{
		PollInterval: 2 * time.Second,
		AtChainID:    fullBackupID,
	})
	defer brokerCancel()

	// Wait for the broker to catch up (target has ≥ 6 emails: alice
	// + 5 users).
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		got := pgQueryEmails(t, brokerTargetDSN)
		if len(got) >= 6 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	got := pgQueryEmails(t, brokerTargetDSN)
	if len(got) < 6 {
		t.Fatalf("broker did not catch up: target emails = %d (got %v); want >= 6", len(got), got)
	}

	// Stop both, verify clean exit.
	streamCancel()
	select {
	case err := <-streamDone:
		if err != nil {
			t.Errorf("stream.Run = %v; want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("stream did not exit within 10s of cancel")
	}
	brokerCancel()
	select {
	case err := <-brokerDone:
		if err != nil {
			t.Errorf("broker.Run = %v; want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("broker did not exit within 10s of cancel")
	}
}

// TestSyncFromBackup_SchemaEvolution exercises Acceptance Criterion 3:
// ALTER TABLE ADD COLUMN on source while the BackupStream is running,
// then the broker applies the delta + the rows referencing the new
// column.
//
// Bug 38 fix (v0.20.1): pre-fix, the stream baked the parent's schema
// snapshot into every rollover's manifest without ever refreshing —
// so an ALTER on the source mid-stream produced manifests with stale
// schema, and the broker's apply hit "column does not exist". Fix:
// stream re-reads source schema at each rollover boundary and emits
// SchemaDelta entries for any diff.
func TestSyncFromBackup_SchemaEvolution(t *testing.T) {
	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			email VARCHAR(255) NOT NULL
		);
		ALTER TABLE users REPLICA IDENTITY FULL;
		INSERT INTO users (email) VALUES ('alice@example.com');
	`
	sourceDSN, brokerTargetDSN, store, fullBackupID, teardown := brokerTestStreamSetup(t, seedDDL)
	defer teardown()

	pgEng, _ := engines.Get("postgres")
	if err := (&Restore{
		Target: pgEng, TargetDSN: brokerTargetDSN, Store: store,
	}).Run(context.Background()); err != nil {
		t.Fatalf("seed restore: %v", err)
	}

	// Bug 38 repro shape: drive the source via the long-running
	// BackupStream (the path the bug lived on), NOT a one-shot
	// IncrementalBackup. The stream must capture the post-ALTER
	// schema in its next rollover for the broker to apply the row.
	streamCancel, streamDone := runStreamInGoroutine(t, sourceDSN, fullBackupID, store)
	defer streamCancel()

	// First write so the stream produces an initial pre-ALTER rollover.
	applyDDL(t, sourceDSN, `INSERT INTO users (email) VALUES ('pre@example.com')`)
	waitForIncrementals(t, store, 1, 30*time.Second)

	// Now: ALTER on source while stream is running, then a row referencing
	// the new column.
	applyDDL(t, sourceDSN, `ALTER TABLE users ADD COLUMN nickname VARCHAR(100)`)
	applyDDL(t, sourceDSN, `INSERT INTO users (email, nickname) VALUES ('bob@example.com', 'bobby')`)

	// Wait for at least one more rollover that should now carry the
	// schema delta. Stream's RolloverWindow is 2s in the test helper.
	waitForIncrementals(t, store, 2, 30*time.Second)

	// Run the broker.
	brokerCancel, brokerDone := runBrokerInGoroutine(t, brokerTargetDSN, "schema-evolve", store, brokerOpts{
		PollInterval: 2 * time.Second,
		AtChainID:    fullBackupID,
	})
	defer brokerCancel()

	// Wait for the broker to catch up: target should have 3 rows
	// (alice + pre + bob) AND a `nickname` column.
	deadline := time.Now().Add(60 * time.Second)
	caught := false
	for time.Now().Before(deadline) {
		emails := pgQueryEmails(t, brokerTargetDSN)
		if len(emails) >= 3 && pgColumnExists(t, brokerTargetDSN, "users", "nickname") {
			caught = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !caught {
		t.Fatalf("broker did not apply schema delta + new row within deadline; emails=%v has_nickname=%v",
			pgQueryEmails(t, brokerTargetDSN),
			pgColumnExists(t, brokerTargetDSN, "users", "nickname"))
	}

	brokerCancel()
	select {
	case err := <-brokerDone:
		if err != nil {
			t.Errorf("broker.Run = %v; want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("broker did not exit within 10s")
	}
	streamCancel()
	select {
	case <-streamDone:
	case <-time.After(10 * time.Second):
		t.Fatal("stream did not exit within 10s")
	}
}

// TestSyncFromBackup_StopCommand exercises Acceptance Criterion 4:
// cooperative stop via RequestSyncFromBackupStop drains the broker
// within 2 × poll-interval.
func TestSyncFromBackup_StopCommand(t *testing.T) {
	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			email VARCHAR(255) NOT NULL
		);
		ALTER TABLE users REPLICA IDENTITY FULL;
		INSERT INTO users (email) VALUES ('alice@example.com');
	`
	_, brokerTargetDSN, store, fullBackupID, teardown := brokerTestStreamSetup(t, seedDDL)
	defer teardown()

	pgEng, _ := engines.Get("postgres")
	if err := (&Restore{
		Target: pgEng, TargetDSN: brokerTargetDSN, Store: store,
	}).Run(context.Background()); err != nil {
		t.Fatalf("seed restore: %v", err)
	}

	brokerCancel, brokerDone := runBrokerInGoroutine(t, brokerTargetDSN, "stop-test", store, brokerOpts{
		PollInterval: 2 * time.Second,
		AtChainID:    fullBackupID,
	})
	defer brokerCancel()

	// Wait for the broker to write its initial state file (first
	// tick + heartbeat).
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		exists, _ := store.Exists(context.Background(), DefaultBrokerStateFilename)
		if exists {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Stop via the public helper.
	if _, err := RequestSyncFromBackupStop(context.Background(), store, time.Now()); err != nil {
		t.Fatalf("RequestSyncFromBackupStop: %v", err)
	}

	select {
	case err := <-brokerDone:
		if err != nil {
			t.Errorf("broker.Run after stop = %v; want nil", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("broker did not exit within 20s of stop request")
	}
}

// TestSyncFromBackup_RestartResumes exercises Acceptance Criterion 5:
// kill broker mid-stream, restart, observe no duplicate applies.
// ADR-0010 idempotent applier guarantees safety; the test confirms
// the broker correctly resumes at last_applied_backup_id.
func TestSyncFromBackup_RestartResumes(t *testing.T) {
	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			email VARCHAR(255) NOT NULL
		);
		ALTER TABLE users REPLICA IDENTITY FULL;
		INSERT INTO users (email) VALUES ('alice@example.com');
	`
	sourceDSN, brokerTargetDSN, store, fullBackupID, teardown := brokerTestStreamSetup(t, seedDDL)
	defer teardown()

	pgEng, _ := engines.Get("postgres")
	if err := (&Restore{
		Target: pgEng, TargetDSN: brokerTargetDSN, Store: store,
	}).Run(context.Background()); err != nil {
		t.Fatalf("seed restore: %v", err)
	}

	// Start a stream that produces 2-3 incrementals. We let the
	// stream keep running through the test; its done-channel is not
	// load-bearing for the assertions.
	streamCancel, _ := runStreamInGoroutine(t, sourceDSN, fullBackupID, store)
	defer streamCancel()
	for i := 0; i < 10; i++ {
		applyDDL(t, sourceDSN, fmt.Sprintf(
			`INSERT INTO users (email) VALUES ('round1-%d@example.com');`, i))
	}
	waitForIncrementals(t, store, 2, 30*time.Second)

	// First broker run: stop after observing at least 1 row applied.
	brokerCancel, brokerDone := runBrokerInGoroutine(t, brokerTargetDSN, "resume-test", store, brokerOpts{
		PollInterval: 2 * time.Second,
		AtChainID:    fullBackupID,
	})
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		emails := pgQueryEmails(t, brokerTargetDSN)
		if len(emails) >= 5 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	emailsAfterFirst := pgQueryEmails(t, brokerTargetDSN)

	// Kill the first broker.
	brokerCancel()
	select {
	case <-brokerDone:
	case <-time.After(15 * time.Second):
		t.Fatal("first broker did not exit")
	}

	// Drive a few more inserts so the stream produces another
	// incremental.
	for i := 0; i < 5; i++ {
		applyDDL(t, sourceDSN, fmt.Sprintf(
			`INSERT INTO users (email) VALUES ('round2-%d@example.com');`, i))
	}
	waitForIncrementals(t, store, 3, 30*time.Second)

	// Restart the broker (no AtChainID this time — warm resume from
	// the persisted state).
	brokerCancel2, brokerDone2 := runBrokerInGoroutine(t, brokerTargetDSN, "resume-test", store, brokerOpts{
		PollInterval: 2 * time.Second,
	})
	defer brokerCancel2()

	// Wait for the broker to catch up to all rows.
	deadline = time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		emails := pgQueryEmails(t, brokerTargetDSN)
		if len(emails) >= 16 { // alice + 10 round1 + 5 round2
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	emailsAfterSecond := pgQueryEmails(t, brokerTargetDSN)

	// Verify no duplicates: idempotent applier should not double up
	// any of the round-1 emails.
	gotMap := map[string]int{}
	for _, e := range emailsAfterSecond {
		gotMap[e]++
	}
	for e, n := range gotMap {
		if n > 1 {
			t.Errorf("duplicate apply of %q: count=%d (after-first=%d, after-second=%d)",
				e, n, len(emailsAfterFirst), len(emailsAfterSecond))
		}
	}

	brokerCancel2()
	select {
	case <-brokerDone2:
	case <-time.After(10 * time.Second):
		t.Fatal("second broker did not exit")
	}
}

// TestSyncFromBackup_ColdStartRefusal exercises Acceptance Criterion 6:
// run broker against a populated target with no override flag → refuse.
func TestSyncFromBackup_ColdStartRefusal(t *testing.T) {
	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			email VARCHAR(255) NOT NULL
		);
		ALTER TABLE users REPLICA IDENTITY FULL;
		INSERT INTO users (email) VALUES ('alice@example.com');
	`
	_, brokerTargetDSN, store, _, teardown := brokerTestStreamSetup(t, seedDDL)
	defer teardown()

	// Don't seed the target at all; an empty target with no
	// sluice_cdc_state row should still refuse.
	pgEng, _ := engines.Get("postgres")
	broker := &SyncFromBackup{
		Target:        pgEng,
		TargetDSN:     brokerTargetDSN,
		Store:         store,
		StreamID:      "refusal-test",
		PollInterval:  2 * time.Second,
		SluiceVersion: "test",
	}
	err := broker.Run(context.Background())
	if err == nil {
		t.Fatal("err = nil; want cold-start refusal")
	}
	if !strings.Contains(err.Error(), "no `sluice_cdc_state` row") {
		t.Errorf("err = %v; want 'no sluice_cdc_state row' guidance", err)
	}
	if !strings.Contains(err.Error(), "--reset-target-data") || !strings.Contains(err.Error(), "--at-chain-id") {
		t.Errorf("err = %v; want both override flags named", err)
	}
}

// TestSyncFromBackup_ColdStartWithReset exercises Acceptance Criterion 7:
// non-empty (well, here, empty-but-no-row) target + --reset-target-data
// → broker drops + restores the chain + transitions to live polling.
func TestSyncFromBackup_ColdStartWithReset(t *testing.T) {
	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			email VARCHAR(255) NOT NULL
		);
		ALTER TABLE users REPLICA IDENTITY FULL;
		INSERT INTO users (email) VALUES ('alice@example.com'), ('bob@example.com');
	`
	sourceDSN, brokerTargetDSN, store, fullBackupID, teardown := brokerTestStreamSetup(t, seedDDL)
	defer teardown()

	// Drive a couple of inserts + take a single incremental so the
	// chain has at least one incremental beyond the full.
	applyDDL(t, sourceDSN, `INSERT INTO users (email) VALUES ('carol@example.com')`)
	pgEng, _ := engines.Get("postgres")
	incrCtx, incrCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer incrCancel()
	if err := (&IncrementalBackup{
		Source: pgEng, SourceDSN: sourceDSN, Store: store,
		ParentRef: fullBackupID, Window: 10 * time.Second, MaxChanges: 5,
		ChunkChanges: 5, SluiceVersion: "test",
	}).Run(incrCtx); err != nil {
		t.Fatalf("IncrementalBackup.Run: %v", err)
	}

	// Run the broker with --reset-target-data. After it exits we
	// should see all 3 emails on the target.
	brokerCancel, brokerDone := runBrokerInGoroutine(t, brokerTargetDSN, "reset-test", store, brokerOpts{
		PollInterval:    2 * time.Second,
		ResetTargetData: true,
	})
	defer brokerCancel()

	// Wait for the broker to land all 3 rows. The target table doesn't
	// exist yet when the broker hasn't finished its inline ChainRestore;
	// pgQueryEmails fatals on any query error, so use the tolerant
	// variant that returns an empty slice when "users" hasn't been
	// recreated yet by the reset path.
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		got := pgQueryEmailsTolerant(t, brokerTargetDSN)
		if len(got) >= 3 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	got := pgQueryEmailsTolerant(t, brokerTargetDSN)
	if len(got) < 3 {
		t.Fatalf("after --reset-target-data, target emails = %v; want 3 (alice, bob, carol)", got)
	}

	brokerCancel()
	select {
	case <-brokerDone:
	case <-time.After(10 * time.Second):
		t.Fatal("broker did not exit")
	}
}

// TestSyncFromBackup_ColdStartWithReset_StaleSchema exercises Bug 40a:
// pre-seed the target with a `users` table whose shape differs from
// the chain's schema, then run --reset-target-data and confirm the
// broker drops the stale table before the inline ChainRestore.
//
// Pre-fix shape: ChainRestore's CREATE TABLE IF NOT EXISTS no-op'd
// against the stale-schema table, then the bulk-copy COPY referenced
// columns the table didn't have, and the producer goroutine deadlocked
// waiting on rowCh — broker hung indefinitely with idle PG connections
// in ClientRead state. v0.20.1 fix: drop the table first; surface
// COPY errors loudly instead of swallowing.
func TestSyncFromBackup_ColdStartWithReset_StaleSchema(t *testing.T) {
	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			email VARCHAR(255) NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
		ALTER TABLE users REPLICA IDENTITY FULL;
		INSERT INTO users (email) VALUES ('alice@example.com'), ('bob@example.com');
	`
	sourceDSN, brokerTargetDSN, store, fullBackupID, teardown := brokerTestStreamSetup(t, seedDDL)
	defer teardown()

	// Take a single incremental so the chain isn't full-only.
	applyDDL(t, sourceDSN, `INSERT INTO users (email) VALUES ('carol@example.com')`)
	pgEng, _ := engines.Get("postgres")
	incrCtx, incrCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer incrCancel()
	if err := (&IncrementalBackup{
		Source: pgEng, SourceDSN: sourceDSN, Store: store,
		ParentRef: fullBackupID, Window: 10 * time.Second, MaxChanges: 5,
		ChunkChanges: 5, SluiceVersion: "test",
	}).Run(incrCtx); err != nil {
		t.Fatalf("IncrementalBackup.Run: %v", err)
	}

	// Pre-seed the broker target with a STALE-SCHEMA `users` table
	// (only id + email — no created_at) and a row in it. The pre-fix
	// behaviour was to leave this table intact and hang on COPY.
	applyDDL(t, brokerTargetDSN, `
		CREATE TABLE users (
			id    BIGINT PRIMARY KEY,
			email VARCHAR(255) NOT NULL
		);
		INSERT INTO users (id, email) VALUES (999, 'stale@example.com');
	`)

	// Run the broker with --reset-target-data + a generous timeout so
	// the pre-fix deadlock (if regressed) surfaces as a test timeout
	// rather than hanging the suite.
	runCtx, runCancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer runCancel()
	broker := &SyncFromBackup{
		Target:          pgEng,
		TargetDSN:       brokerTargetDSN,
		Store:           store,
		ChainURL:        "test://reset-stale",
		StreamID:        "reset-stale-test",
		PollInterval:    2 * time.Second,
		ResetTargetData: true,
		SluiceVersion:   "test",
	}
	errCh := make(chan error, 1)
	go func() { errCh <- broker.Run(runCtx) }()

	// Wait for the broker to land the 3 chain rows. The 999/stale row
	// should be GONE — that's the proof the table was dropped.
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		got := pgQueryEmailsTolerant(t, brokerTargetDSN)
		if len(got) >= 3 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	got := pgQueryEmailsTolerant(t, brokerTargetDSN)
	if len(got) < 3 {
		t.Fatalf("after --reset-target-data with stale schema, target emails = %v; want 3 chain rows", got)
	}
	for _, e := range got {
		if e == "stale@example.com" {
			t.Errorf("stale row survived --reset-target-data; got %v", got)
		}
	}

	runCancel()
	select {
	case <-errCh:
	case <-time.After(10 * time.Second):
		t.Fatal("broker did not exit within 10s of cancel")
	}
}

// waitForIncrementals blocks until at least minCount incremental
// manifests are in the chain or the deadline fires. Returns silently
// in either case; assertions are the caller's job.
func waitForIncrementals(t *testing.T, store *LocalStore, minCount int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		records, _ := listAllManifests(context.Background(), store)
		var n int
		for _, r := range records {
			if r.manifest.Kind == ir.BackupKindIncremental {
				n++
			}
		}
		if n >= minCount {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// pgColumnExists returns true when the named column is present on
// the named table in the public schema. Used for schema-evolution
// assertions.
func pgColumnExists(t *testing.T, dsn, table, column string) bool {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	row := db.QueryRowContext(ctx,
		`SELECT 1 FROM information_schema.columns
		 WHERE table_schema='public' AND table_name=$1 AND column_name=$2`,
		table, column)
	var dummy int
	err = row.Scan(&dummy)
	if errors.Is(err, sql.ErrNoRows) {
		return false
	}
	if err != nil {
		t.Fatalf("pgColumnExists: %v", err)
	}
	return true
}

// Ensure the sync package is picked up so MySQL-only test files don't
// accidentally drop the wg helper used elsewhere; just a small
// import-keepalive.
var _ = sync.WaitGroup{}
