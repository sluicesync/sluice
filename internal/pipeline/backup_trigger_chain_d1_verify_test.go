//go:build d1verify && integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Roadmap item 163 on LIVE Cloudflare D1 — the twin of
// [TestBackupChain_SQLiteTrigger_PostFullRowsReachTheRestore].
//
// # Why this exists
//
// Item 163 gave every trigger-CDC engine a snapshot opener that records the
// change log's anchor at `backup full` time, so a chain resumes where the
// full's sweep ended instead of at the change log's CURRENT MAX(id). The
// `sqlite-trigger` and `postgres-trigger` halves were pinned end to end
// against real servers. The D1 half was NOT: `d1-trigger` runs the same
// `openBackupSnapshot` through the backend seam, and its author labelled the
// live behaviour DERIVED-NOT-VERIFIED, pinned only against the D1 mock
// (audit A0915-D1VERIFY-DEFERRED).
//
// The existing `d1verify` suite does not close that gap, and it is worth
// saying why rather than assuming a green suite covers it: its twelve tests
// exercise the D1 READER lane (value matrix, keyset pagination, mangled
// text, wide rows) and the trigger STREAMING lane (`OpenD1CDCReader` +
// `StreamChanges` from an empty position). Not one of them opens a snapshot
// or builds a chain, so none can reach the capturer. A suite that passes
// without touching the thing under question is the "gate narrower than its
// name" shape CLAUDE.md warns about.
//
// # What it grades, and with which independent expected value
//
// Full → 100 rows written AFTER the full → incremental → chain-restore into
// a fresh Postgres. The restored count and SUM must equal the count and SUM
// read back FROM D1 after those writes — the source's own numbers, never
// anything the chain reports (the 2026-08-01 rule). Before item 163 a
// trigger full recorded no position at all, so the incremental anchored
// "from now" and the restore was short by every post-full row: 100 of 100 in
// the v0.153.1 regression cycle, which is the exact shape asserted here.
//
// [runTriggerChainFull] additionally asserts the full's EndPosition carries
// the tag this engine's CODEC WRITES — `sqlite-trigger`, not `d1-trigger` —
// and that it decodes through the engine's own resume decoder, so the
// capturer's OUTPUT is graded and not merely its existence.
//
// That distinction is deliberate and is the one thing about this engine
// worth knowing before reading the assertions: `d1-trigger` is the REGISTRY
// name, while the shared trigger-CDC codec writes `sqlite-trigger` and
// accepts both (`Codec.Accept`, enforced by `Codec.Decode`). An anchor
// tagged for the sibling is therefore correct here, not foreign — which is
// the opposite of the pgtrigger case item 163 fixed, where a genuinely
// foreign pgoutput position was refused by the poller.
//
// The load-bearing assertion is not the tag in any case. It is that the
// restored row COUNT and SUM equal D1's own, read back after the post-full
// writes: a tag can look right while the anchor is a default, but the count
// cannot.
//
// # Cost and cleanup
//
// One throwaway D1 database per run, created and deleted through
// [d1AdvCreateThrowaway], which registers its deletion on test cleanup and
// fails loudly if the delete does not land. Plus one Postgres container.
// The post-full rows go in batched multi-row INSERTs — one change-log entry
// per row either way, so the assertion shape is unchanged, but a handful of
// HTTP round trips instead of a hundred.

package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	_ "sluicesync.dev/sluice/internal/engines/d1-trigger" // self-registers d1-trigger
	sqlitetrigger "sluicesync.dev/sluice/internal/engines/sqlite-trigger"
)

func TestBackupChain_D1Trigger_PostFullRowsReachTheRestore(t *testing.T) {
	account, token := d1AdvCreds(t)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	dbID, dbName := d1AdvCreateThrowaway(ctx, t, account, token)
	dsn := "d1://" + account + "/" + dbID
	t.Logf("throwaway D1 database %s (%s)", dbName, dbID)

	// The same table and seed rows seedSQLiteTriggerSource builds, so the
	// two engines' pins grade an identical shape rather than diverging by
	// accident. (No journal_mode pragma: D1 has no such knob over HTTP.)
	d1AdvQuery(ctx, t, account, dbID, token, `CREATE TABLE events (
		id   INTEGER PRIMARY KEY,
		big  INTEGER NOT NULL,
		blb  BLOB,
		note TEXT
	)`)
	d1AdvQuery(ctx, t, account, dbID, token,
		`INSERT INTO events (id, big, blb, note) VALUES (1, 100, x'cafe', 'seed-1')`)
	d1AdvQuery(ctx, t, account, dbID, token,
		`INSERT INTO events (id, big, blb, note) VALUES (2, 200, NULL, 'seed-2')`)

	if _, err := sqlitetrigger.SetupD1(ctx, dsn, sqlitetrigger.SetupOptions{Tables: []string{"events"}}); err != nil {
		t.Fatalf("SetupD1: %v", err)
	}

	src, ok := engines.Get(sqlitetrigger.EngineNameD1)
	if !ok {
		t.Fatalf("%s engine not registered", sqlitetrigger.EngineNameD1)
	}
	_, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	// The full. This is where the capturer runs, and where a pre-item-163
	// build recorded nothing at all.
	//
	// NOTE the expected tag is [sqlitetrigger.EngineName], NOT EngineNameD1,
	// and the difference is the whole subtlety of this engine. The helper
	// grades the tag the engine's CODEC WRITES, not the name it registers
	// under. `d1-trigger` shares the trigger-CDC family codec, whose
	// `WriteEngine` is `sqlite-trigger` and whose `Accept` list is BOTH names
	// — and `Codec.Decode` enforces that list, refusing anything outside it
	// loudly. So a D1 full legitimately records a `sqlite-trigger`-tagged
	// anchor and resumes fine.
	//
	// This cost a red run to learn: passing EngineNameD1 here fails with
	// "want a \"d1-trigger\"-tagged anchor", which reads exactly like the
	// pgtrigger half of item 163 (a position tagged with a FOREIGN engine
	// that the poller then refuses). It is not that. For `sqlite-trigger`
	// and `pgtrigger` the registry name and the write tag coincide, so the
	// original pin never had to tell them apart.
	store := runTriggerChainFull(ctx, t, src, dsn, sqlitetrigger.EngineName, sqlitetrigger.AppliedLastID)

	// Rows written AFTER the full — the window that used to vanish.
	const perStatement = 25
	for lo := 1; lo <= triggerChainPostFullRows; lo += perStatement {
		var vals []string
		for i := lo; i < lo+perStatement && i <= triggerChainPostFullRows; i++ {
			vals = append(vals, fmt.Sprintf("(%d, %d, NULL, 'post-full-%d')", 100+i, int64(1)<<40+int64(i), i))
		}
		d1AdvQuery(ctx, t, account, dbID, token,
			`INSERT INTO events (id, big, blb, note) VALUES `+strings.Join(vals, ", "))
	}

	// The INDEPENDENT expected value: D1's own numbers, read after the
	// writes. Never the chain's, and never a count derived from the same
	// read path the chain used.
	wantRows, wantSum := d1CountAndSumEvents(ctx, t, account, dbID, token)
	if wantRows != 2+triggerChainPostFullRows {
		t.Fatalf("source rows = %d; want %d — the test premise is broken before the chain is graded", wantRows, 2+triggerChainPostFullRows)
	}

	runTriggerChainIncrementalAndRestore(ctx, t, src, dsn, store, targetDSN)

	gotRows := pgQueryOne[int64](t, targetDSN, "SELECT COUNT(*) FROM events")
	gotSum := pgQueryOne[int64](t, targetDSN, "SELECT COALESCE(SUM(big), 0) FROM events")
	if gotRows != wantRows || gotSum != wantSum {
		t.Fatalf("chain restore: target rows/sum(big) = %d/%d; LIVE D1 source (read AFTER the post-full writes) = %d/%d — "+
			"the window between the full's sweep and the incremental's anchor is missing from the chain, which is "+
			"roadmap item 163's defect surviving on the D1 backend", gotRows, gotSum, wantRows, wantSum)
	}
}

// d1CountAndSumEvents reads the source's own COUNT and SUM straight from D1.
//
// Deliberately its own round trip rather than a reuse of anything the backup
// path touched: the point of an independent expected value is that it does
// not share evidence with the thing it grades.
func d1CountAndSumEvents(ctx context.Context, t *testing.T, account, dbID, token string) (rows, sum int64) {
	t.Helper()
	res := d1AdvQuery(ctx, t, account, dbID, token,
		`SELECT COUNT(*) AS c, COALESCE(SUM(big), 0) AS s FROM events`)
	if len(res) != 1 {
		t.Fatalf("count/sum query returned %d rows; want 1", len(res))
	}
	if err := json.Unmarshal(res[0]["c"], &rows); err != nil {
		t.Fatalf("decode COUNT(*) %s: %v", res[0]["c"], err)
	}
	if err := json.Unmarshal(res[0]["s"], &sum); err != nil {
		t.Fatalf("decode SUM(big) %s: %v", res[0]["s"], err)
	}
	return rows, sum
}
