//go:build nekiverify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"os"
	"runtime/debug"
	"sort"
	"strings"
	"testing"
	"time"
)

// Tier-2 coverage: `sluice restore` INTO a sharded Neki, and `sluice backup
// full` FROM one.
//
// # Why these are worth a live cluster
//
// Filed 2026-09-14, deliberately blocked on the NK306 control-table fix and
// unblocked by it (ADR-0187). The argument for the arm is a COMMENT — the only
// Neki reference the whole backup package contains, in restore_table_pool.go,
// recording a defect code review caught rather than a run: passing a literal
// `0` to the axis resolver dropped `CopyConcurrencyCeiling`, which *"on a
// PlanetScale Neki target let a restore open table × chunk concurrent COPYs
// against a router that admits four."*
//
// Restore runs the same phases migrate does — schema-apply, bulk copy,
// indexes, constraints — so it meets every Neki hazard catalogued this week.
// Migrate's versions of those are now measured. Restore's were assumed, and an
// assumption recorded as a comment is a hypothesis until a test fails when it
// breaks.
//
// # What each arm's evidence is, and what it is not
//
// Everything that is not about the router lives in
// neki_backup_restore_core_test.go and runs on every PR against vanilla
// PostgreSQL with the flavor forced. What is HERE is only what a router can
// show: routing across shards, the placement of sluice's shard-key-less
// control tables, and the SQLSTATEs.
//
// # Ordering
//
// The restore arm runs FIRST and the backup arm second, so the backup arm can
// include the table the restore created. That is one more sharded table to
// scatter-read across, at no cost. Both run after the CDC arm — which has
// already created and placed the applier's control tables — and that ordering
// is load-bearing for how the placement check below is WORDED: it is a
// statement about the target's state, not about what this restore caused. The
// causation question is answered separately, by measuring which control tables
// the restore itself created.

// nekiRestoreIntoShardedTarget restores a full backup into the sharded
// fixture through the real `sluice restore` door and grades it on content.
func nekiRestoreIntoShardedTarget(ctx context.Context, t *testing.T, db *sql.DB, fx *nekiFixture, tenantA, tenantB int) {
	t.Helper()

	t.Run("restore INTO a sharded Neki target lands byte-exact content across both shards", func(t *testing.T) {
		nekiArmIdentity(t, "restore-into-sharded-neki", fx.name)

		spec := defaultNekiBackupRestoreSpec(int64(tenantA), int64(tenantB))

		// A clean slate for this arm's own tables. `DROP TABLE IF EXISTS`
		// rather than a filter, because a leftover from a previous failed
		// run inside the same fixture would make the restore's CREATE TABLE
		// fail for a reason that has nothing to do with the platform.
		for _, tbl := range []string{spec.table, spec.tableSmall} {
			if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS `+tbl); err != nil {
				t.Fatalf("drop %s before the restore: %v", tbl, err)
			}
		}

		// The control-table census BEFORE, so the delta afterwards answers
		// which door this restore actually reached. See the placement
		// sub-check below.
		before := nekiControlTablesPresent(ctx, t, db)

		res := nekiRestoreCoreIntoTarget(ctx, t, fx.dsn, spec)

		// ---- the router-only half ----

		// (a) Both shards must hold rows, or the content check above passed
		// without exercising a sharded target at all.
		census := nekiPerShardCensus(ctx, t, db, fx.shards, spec.table)
		if populated := nekiCensusPopulated(census); populated < 2 {
			t.Errorf("every restored row landed on ONE shard (%v). The content assertions passed, and they "+
				"proved nothing about a SHARDED target: no row crossed a shard boundary, so the router's "+
				"routing of a restore was never exercised. tenant_id values used were %d and %d, which "+
				"tenantsOnDistinctShards reported as landing on different shards — if that is still true, the "+
				"restore is not routing on the shard key.", census, tenantA, tenantB)
		} else {
			t.Logf("restore scattered %d rows across %d shards: %v", res.gotRows, populated, census)
		}

		// (b) The concurrent-COPY ceiling. This is the comment the whole item
		// was filed from, turned into an assertion: restore resolved its
		// cross-table axis with default (auto) concurrency against a Neki
		// target, so the resolved value must sit at or under the ceiling
		// sluice paces a Neki to.
		switch {
		case res.tableParallelism == 0:
			t.Errorf("restore's cross-table dispatch observer never fired, so this arm cannot say what "+
				"concurrency the restore resolved against the router — and the concurrent-COPY ceiling, which "+
				"is the defect class this arm was filed for, is UNCHECKED. RestoreDispatchObserver is set in "+
				"nekiRestoreCoreIntoTarget; if the seam was renamed or moved, this arm needs rewiring. "+
				"(reason reported: %q)", res.dispatchReason)
		case res.tableParallelism > nekiConcurrentCopyLimit:
			t.Errorf("restore resolved table-parallelism %d against a Neki target; nekiConcurrentCopyLimit is "+
				"%d. The ceiling did not reach this door. On a router that admits four concurrent COPYs this "+
				"is the shape restore_table_pool.go's comment describes — table × chunk concurrent COPYs "+
				"against a router that admits four — and it surfaces as SQLSTATE 53300 mid-copy under load, "+
				"not here. (dispatch reason: %q)", res.tableParallelism, nekiConcurrentCopyLimit, res.dispatchReason)
		default:
			t.Logf("restore resolved table-parallelism %d against the router (ceiling %d, reason %q)",
				res.tableParallelism, nekiConcurrentCopyLimit, res.dispatchReason)
		}

		// (c) The ADR-0187 placement premise, for the restore door.
		nekiControlTablePlacementHolds(ctx, t, db, fx, before)
	})
}

// nekiBackupFromShardedSource takes a full backup FROM the sharded fixture and
// reads it back independently.
//
// The operator's expectation is that this "should already work" — the read
// side is the better-covered direction. The arm's value is that nothing has
// ever measured it against a SHARDED read: every row the backup streams comes
// back through the router's scatter-gather, on the IR copy path rather than
// the raw-COPY lane (the reader declines that one on Neki, because the router
// refuses `COPY (SELECT …) TO` with NK013).
func nekiBackupFromShardedSource(ctx context.Context, t *testing.T, db *sql.DB, fx *nekiFixture, tenantA, tenantB int) {
	t.Helper()

	t.Run("backup FROM a sharded Neki source reads every row back byte-exact", func(t *testing.T) {
		nekiArmIdentity(t, "backup-from-sharded-neki", fx.name)

		// sk_good is the fixture's own sharded table and is always present.
		// The restore arm's tables join it when that arm got far enough to
		// create them — more sharded tables is more scatter-gather, and
		// making it conditional keeps THIS arm interpretable on a run where
		// the restore arm failed.
		tables := []string{"sk_good"}
		for _, candidate := range []string{"nk_restored", "nk_restored_small"} {
			if nekiRelationExists(ctx, t, db, candidate) {
				tables = append(tables, candidate)
			}
		}
		t.Logf("backing up %v from the sharded fixture (tenants %d and %d route to different shards)",
			tables, tenantA, tenantB)

		// Anti-vacuity, BEFORE the backup: a scattering read is what is being
		// measured, so every table in the set must genuinely span both
		// shards. A table sitting entirely on one shard would be backed up by
		// a read that never gathered.
		for _, tbl := range tables {
			census := nekiPerShardCensus(ctx, t, db, fx.shards, tbl)
			populated := nekiCensusPopulated(census)
			if populated < 2 {
				t.Errorf("%s occupies only %d shard(s) (%v) — backing it up would not measure a SCATTERING "+
					"read, which is the one thing this arm exists to exercise", tbl, populated, census)
				continue
			}
			t.Logf("source census for %s: %v (%d shards populated)", tbl, census, populated)
		}

		// chunkRows small enough that sk_good — a few dozen rows from the
		// refusal arms — still rolls more than one chunk file, so the
		// chunked read path is reached on the smallest table too.
		res := nekiBackupCoreFromPG(ctx, t, fx.dsn, tables, 16)

		t.Logf("manifest facts an operator would want: format_version=%d source_engine=%q tables=%d chunks=%d "+
			"end_position={engine=%q token=%q} partial_state=%q; %s",
			res.manifest.FormatVersion, res.manifest.SourceEngine, len(res.manifest.Tables),
			nekiChunkCount(res.manifest), res.manifest.EndPosition.Engine, res.manifest.EndPosition.Token,
			res.manifest.PartialState, res.rawCopyNote)

		if !strings.Contains(res.rawCopyNote, "declined=true") {
			t.Errorf("the Postgres row reader did NOT decline the raw-copy lane against this Neki source (%s). "+
				"The router refuses `COPY (SELECT …) TO` with NK013, so a backup taking that lane would die "+
				"mid-stream; if the decline stopped firing, every Neki backup is on a path the router refuses.",
				res.rawCopyNote)
		}
	})
}

// nekiControlTableNames are the control tables a Neki target can end up
// holding, in the order a report should list them.
//
// Derived by hand from the ADR-0187 sibling-sweep table rather than from the
// package's constants, because the gate that owns that enumeration
// (TestEveryPostgresControlTableIsPlacedOnNeki) is an untagged unit test and
// this file must not fight it for ownership. If a control table is added, that
// gate fails first, which is the order that matters.
var nekiControlTableNames = []string{
	"sluice_cdc_state",
	"sluice_cdc_schema_history",
	"sluice_shard_consolidation_lease",
	"sluice_cdc_skipped_tables",
	"sluice_target_metrics_history",
	"sluice_migrate_state",
	"sluice_migrate_table_progress",
	"sluice_keysets",
}

// nekiRelationExists reports whether a relation exists in the target's search
// path.
func nekiRelationExists(ctx context.Context, t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var reg sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT to_regclass($1)::text`, name).Scan(&reg); err != nil {
		t.Fatalf("probe for relation %q: %v", name, err)
	}
	return reg.Valid
}

// nekiControlTablesPresent returns the set of sluice control tables currently
// on the target.
func nekiControlTablesPresent(ctx context.Context, t *testing.T, db *sql.DB) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for _, name := range nekiControlTableNames {
		if nekiRelationExists(ctx, t, db, name) {
			out[name] = true
		}
	}
	return out
}

// nekiTableShardGroup reads the shard group the topology assigns to one table,
// or "" when the topology does not name it.
func nekiTableShardGroup(ctx context.Context, t *testing.T, db *sql.DB, table string) string {
	t.Helper()
	var group sql.NullString
	const q = `SELECT ((__neki.get_data_topology())::jsonb)
	             #>> ARRAY['databases', current_database(), 'schemas', 'public', 'tables', $1, 'shard_group']`
	if err := db.QueryRowContext(ctx, q, table).Scan(&group); err != nil {
		t.Fatalf("read the topology's shard_group for %q: %v\n%s", table, err,
			nekiFunctionSignature(ctx, db, "get_data_topology"))
	}
	return group.String
}

// nekiControlTablePlacementHolds grades ADR-0187's premise on the target the
// restore just wrote to, and separately reports WHICH control tables this
// restore created.
//
// # The two questions, kept apart on purpose
//
// **State.** Every sluice control table present on a sharded Neki must sit in
// the authoritative shard group. That is what ADR-0187 makes true, and it is
// what a shard-key-less INSERT into one of them depends on; a table that has
// drifted out of the group takes NK306 on the next write. This is asserted.
//
// **Causation.** Whether THIS restore created any of them is a different
// question, and the honest answer is measured rather than assumed. The filed
// item and `docs/dev/neki-readiness.md` both say restore writes
// `sluice_migrate_state`; the ADR-0187 sibling sweep corrects that in place —
// *"a chain restore never opens this store, it reaches the applier door"* — and
// reading `internal/pipeline/backup/restore.go` says something narrower still:
// only `ChainRestore.Run` opens a change applier and calls `EnsureControlTable`
// (chain_restore.go, step 2.9). A SINGLE-FULL restore, which is what this arm
// takes, goes down `Restore.Run`'s single-manifest path and opens no applier at
// all.
//
// So a delta of zero here is not a failure — it is the answer to "which door
// did this restore reach", and it is printed rather than asserted, because
// asserting it would be asserting a claim about sluice's own structure that the
// code, not the platform, decides.
func nekiControlTablePlacementHolds(ctx context.Context, t *testing.T, db *sql.DB, fx *nekiFixture, before map[string]bool) {
	t.Helper()

	after := nekiControlTablesPresent(ctx, t, db)
	var created []string
	for name := range after {
		if !before[name] {
			created = append(created, name)
		}
	}
	sort.Strings(created)

	switch len(created) {
	case 0:
		t.Logf("DOOR: this restore created NO control tables. That matches the code — only ChainRestore.Run "+
			"opens a change applier and calls EnsureControlTable (internal/pipeline/backup/chain_restore.go "+
			"step 2.9), and a single-full restore takes Restore.Run's single-manifest path, which opens none. "+
			"The placement assertion below is therefore about the TARGET'S STATE (the CDC arm placed these "+
			"earlier in this run), not about anything this restore caused. A multi-segment chain restore is "+
			"the shape that would reach the applier door, and this arm does not build one — NOT COVERED, and "+
			"filed as such rather than implied. Control tables present: %v", nekiSortedKeys(after))
	default:
		t.Logf("DOOR: this restore CREATED %v. So the single-full restore path does reach a control-table door "+
			"after all — which contradicts the reading of restore.go recorded in this function's doc, and is "+
			"the more interesting outcome of the two. The placement assertion below now grades tables this "+
			"restore is responsible for.", created)
	}

	auth := readAuthoritativeShardGroup(ctx, t, fx)
	if auth == "" {
		t.Fatal("the topology names no authoritative_shard_group, so there is nothing to grade placement against")
	}

	graded := 0
	for _, name := range nekiControlTableNames {
		if !after[name] {
			continue
		}
		graded++
		group := nekiTableShardGroup(ctx, t, db, name)
		switch {
		case group == "":
			t.Errorf("control table %q exists on this sharded target and the data topology does NOT name it, so "+
				"it falls to the database's default shard group and is routed by a shard index. It carries no "+
				"shard key, so the next write to it is refused with SQLSTATE NK306 — the exact class ADR-0187 "+
				"closes by placing it in the authoritative group %q. Either the placement never ran on the door "+
				"that created it, or something removed the entry.", name, auth)
		case group != auth:
			t.Errorf("control table %q is placed in shard group %q, not the authoritative group %q. A "+
				"non-authoritative group carries a default shard index, so a shard-key-less write to this "+
				"table is refused with NK306 exactly as if it were unplaced.", name, group, auth)
		default:
			t.Logf("control table %q is in the authoritative shard group %q", name, auth)
		}
	}
	if graded == 0 {
		t.Errorf("NO sluice control table exists on this target, so the ADR-0187 placement premise was graded "+
			"against nothing. The CDC arm runs before this one and calls EnsureControlTable; if it no longer "+
			"does, this check is vacuous and so is that arm's. Names looked for: %v", nekiControlTableNames)
	}

	// The corroborating half: a shard-key-less write into a placed control
	// table must be ACCEPTED. Placement in the topology is a declaration;
	// this is the behaviour the declaration is supposed to buy, and the two
	// can diverge (a router that has not converged on the revision yet is
	// exactly that state).
	if after["sluice_cdc_state"] {
		nekiControlTableTakesAShardKeylessWrite(ctx, t, db)
	}
}

// nekiControlTableTakesAShardKeylessWrite confirms the placement actually
// buys what it is for.
//
// The write is an idempotent no-op UPDATE against a stream name nothing else
// uses, so it cannot disturb the CDC arm's row: the point is whether the
// router ADMITS a statement carrying no routing column, and an UPDATE that
// matches nothing still has to be routed to be refused.
func nekiControlTableTakesAShardKeylessWrite(ctx context.Context, t *testing.T, db *sql.DB) {
	t.Helper()
	const probeStream = "nekiverify-restore-placement-probe"
	_, err := db.ExecContext(ctx,
		`UPDATE sluice_cdc_state SET updated_at = updated_at WHERE stream_name = $1`, probeStream)
	if err == nil {
		t.Logf("a shard-key-less write into sluice_cdc_state was accepted — the placement buys what it is for")
		return
	}
	if isPGCode(err, "NK306") {
		t.Errorf("sluice_cdc_state is placed in the authoritative shard group and a shard-key-less write into "+
			"it was STILL refused with NK306: %v\n\nPlacement and behaviour disagree. Candidates, in the order "+
			"worth checking: the router had not converged on the topology revision (ensureNekiControlTablePlacement "+
			"waits on __neki.wait_for_data_topology — if that wait is not reaching this door, the window is real "+
			"and racy); or the authoritative group has acquired a default shard index, which planNekiControlPlacement "+
			"refuses to write into but an EXISTING placement would not re-check.", err)
		return
	}
	if isPGCode(err, "42703") {
		t.Logf("the probe UPDATE named a column sluice_cdc_state does not have (%v) — the control table's "+
			"shape changed and this probe needs updating; it proves nothing as written", err)
		return
	}
	t.Errorf("a shard-key-less write into the placed sluice_cdc_state failed for an unexpected reason: %v", err)
}

// nekiSortedKeys renders a set deterministically for a log line.
func nekiSortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// nekiPerShardCensus counts a table's rows on each shard by pinning the
// session to one shard at a time.
//
// This is the anti-vacuity foundation for every content assertion in this
// family: a run where every row landed on ONE shard exercised no routing at
// all, and would grade green while proving nothing about a SHARDED target.
func nekiPerShardCensus(ctx context.Context, t *testing.T, db *sql.DB, shards []string, table string) map[string]int {
	t.Helper()
	census := map[string]int{}
	for _, uid := range shards {
		if _, err := db.ExecContext(ctx, `SET __neki.shard = '`+uid+`'`); err != nil {
			t.Fatalf("per-shard census: pin to shard %s: %v", uid, err)
		}
		var n int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM `+table).Scan(&n); err != nil {
			t.Fatalf("per-shard census: count %s on shard %s: %v", table, uid, err)
		}
		census[uid] = n
	}
	// Unconditional: a session left pinned would silently make every later
	// read in this run single-shard, and a later arm's failure would be
	// someone else's mystery.
	if _, err := db.ExecContext(ctx, `RESET __neki.shard`); err != nil {
		t.Fatalf("per-shard census: reset the shard pin: %v", err)
	}
	return census
}

// nekiCensusPopulated counts how many shards hold at least one row.
func nekiCensusPopulated(census map[string]int) int {
	n := 0
	for _, c := range census {
		if c > 0 {
			n++
		}
	}
	return n
}

// nekiBuildIdentity names the sluice build under test, for quoting in a
// finding report.
//
// GITHUB_SHA first because that is the one that is actually populated in the
// workflow this suite runs in; `go test` does not reliably stamp VCS info into
// a test binary, so the build-info path is the fallback and its absence is
// stated rather than rendered as an empty string.
func nekiBuildIdentity() string {
	if sha := os.Getenv("GITHUB_SHA"); sha != "" {
		return sha
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			if s.Key == "vcs.revision" && s.Value != "" {
				return s.Value
			}
		}
	}
	return "(unstamped — go test does not always record vcs info; quote the workflow run id instead)"
}

// nekiArmIdentity is the line a finding report quotes, printed while the
// cluster still exists — these databases are deleted when the run ends, so
// anything not recorded now cannot be looked up afterward.
func nekiArmIdentity(t *testing.T, arm, fixture string) {
	t.Helper()
	t.Logf("nekiverify ARM %q — fixture database %q, sluice build %s, %s UTC. Quote this line in any finding.",
		arm, fixture, nekiBuildIdentity(), time.Now().UTC().Format(time.RFC3339))
}
