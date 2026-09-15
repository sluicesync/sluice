//go:build nekiverify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/appliershared"
)

// nekiShardTargetedSessionProbe measures what a SHARD-TARGETED session can
// and cannot do on a live Neki, and it is a PROBE rather than a premise
// check: sluice has no behaviour here yet, and every outcome is useful.
//
// # What a targeted session is, and why it might matter
//
// PlanetScale's query-planning page documents `SET __neki.shard = '<uid>'`
// as a per-session switch that forwards DML — SELECT/INSERT/UPDATE/DELETE/
// MERGE — to ONE shard, bypassing shard-key routing. COPY, DO and schema DDL
// are rejected under it; the router-managed functions (`current_setting`,
// `set_config`, `nextval`) are unavailable; it cannot change mid-transaction;
// `RESET __neki.shard` clears it.
//
// The same page states the fact the whole Neki arc rests on: an ordinary
// multi-shard read does NOT establish one shared Postgres snapshot across its
// destinations, and Neki commits each shard separately with no two-phase
// commit. So today a `migrate` or `backup` FROM a sharded Neki reads through
// the router's scatter-gather, which is not consistent even within one table.
//
// If a targeted session is a real Postgres session on a real shard, two
// things sluice has been told it cannot have come back into reach:
//
//	PREMISE 1 (per-shard consistent reads). A REPEATABLE READ transaction on
//	a targeted session would be a genuine snapshot of ONE shard. Reading a
//	table shard-by-shard, N-way parallel, each slice internally consistent, is
//	the platform's own consistency model and strictly better than the scatter
//	sluice does now. Measured by (b), (c) and (d).
//
//	PREMISE 2 (per-shard positions). `pg_current_wal_lsn()` is not a
//	router-managed function; under targeting it may answer PER SHARD. N
//	positions plus a shard-targeted replication door would make an incremental
//	FROM a Neki the same composite-position shape sluice already ships for
//	Vitess. Measured by (a) and (g).
//
// And one thing it would make WORSE, which is why (f) exists:
//
//	THE HAZARD. A targeted INSERT bypasses routing, so a row lands wherever
//	the session points rather than where its shard key says. That is exactly
//	the placement mismatch `ShardPlacementMismatch` refuses, and it is
//	invisible to an equality-routed read. sluice must never WRITE under
//	targeting; (f) measures the harm precisely so that rule has a number
//	behind it rather than a caution.
//
// # The probe contract, inherited from the control-table probes
//
// It passes on any CONCLUSIVE answer and fails only when a measurement could
// not RUN. A refusal is an answer; a third outcome is an answer recorded
// verbatim; "the connection died" is not. Every subtest prints an `ANSWER:`
// line an engineer can act on without re-running the suite, because the
// database is deleted when the run ends.
//
// # Residue
//
// Every row this arm inserts is deleted in a `t.Cleanup`, and every session
// it opens is closed there too. A targeted session must not leak into the
// suite's shared pool: each pin is one connection taken from a POOL OF ITS
// OWN, so the pin cannot be handed to a later arm even if `RESET` were to
// fail. See [nekiPinShardConn].
func nekiShardTargetedSessionProbe(
	ctx context.Context,
	t *testing.T,
	db *sql.DB,
	fx *nekiFixture,
	tenantA, tenantB int,
	authShard string,
) {
	t.Helper()

	t.Run("PROBE: what can a shard-targeted session (SET __neki.shard) do?", func(t *testing.T) {
		nekiArmIdentity(t, "shard-targeted-sessions", fx.name)

		if len(fx.shards) < 2 {
			t.Fatalf("INCONCLUSIVE: the fixture reports %d shard(s). Every measurement here compares two "+
				"shards against each other, and against the router's unpinned view; with one shard the "+
				"three views are the same view.", len(fx.shards))
		}

		pins := make(map[string]*sql.Conn, len(fx.shards))
		for _, uid := range fx.shards {
			conn, err := nekiPinShardConn(ctx, t, fx, uid)
			if err != nil {
				// Not INCONCLUSIVE — this is the most conclusive answer the
				// arm can produce, and it invalidates the vendor page rather
				// than the measurement.
				t.Logf("ANSWER: `SET __neki.shard` is REFUSED on this build — %v (SQLSTATE %s).\n\n"+
					"Shard targeting is the mechanism both premises rest on, so PREMISE 1 (per-shard "+
					"consistent reads) and PREMISE 2 (composite positions) are dead as described, and "+
					"the hazard in (f) cannot occur. Re-read PlanetScale's query-planning page: the "+
					"feature this arm was built against may have been withdrawn or renamed.",
					err, nekiSQLState(err))
				return
			}
			pins[uid] = conn
		}

		// WHICH tenant lives on WHICH shard, discovered the same way
		// tenantsOnDistinctShards discovers the tenants themselves — routing
		// is xxhash against key ranges and is not predictable from outside.
		//
		// This loop is also the PIN CHECK, and it is the anti-vacuity
		// foundation for everything below. If `SET __neki.shard` silently did
		// nothing, both pinned reads would return the router's scatter-gather
		// result and EVERY tenant would appear to have rows on BOTH shards.
		// A measurement that reads the router while believing it reads a
		// shard is this arm's version of the adjacent-question defect the
		// suite has hit three times.
		home := make(map[int]string, 2)
		for _, tenant := range []int{tenantA, tenantB} {
			for _, uid := range fx.shards {
				var n int
				if err := pins[uid].QueryRowContext(ctx,
					`SELECT count(*) FROM sk_good WHERE tenant_id = $1`, tenant).Scan(&n); err != nil {
					t.Fatalf("INCONCLUSIVE: a pinned read on shard %s could not count tenant %d: %v", uid, tenant, err)
				}
				if n == 0 {
					continue
				}
				if prev, ok := home[tenant]; ok {
					t.Fatalf("INCONCLUSIVE: tenant %d has rows on BOTH %s and %s under targeting.\n\n"+
						"Either the pin is not taking effect — in which case every pinned read below is "+
						"reading the ROUTER and nothing here measures a shard — or a row for one tenant "+
						"genuinely exists on two shards, which would be a far larger finding than this "+
						"arm was built to make. Do not report either reading without checking the other.",
						tenant, prev, uid)
				}
				home[tenant] = uid
			}
		}
		if home[tenantA] == "" || home[tenantB] == "" {
			t.Fatalf("INCONCLUSIVE: could not place both probe tenants on a shard (tenant %d → %q, tenant "+
				"%d → %q). tenantsOnDistinctShards seeds sk_good with 40 rows, so an empty placement means "+
				"the pinned reads are not seeing them.", tenantA, home[tenantA], tenantB, home[tenantB])
		}
		if home[tenantA] == home[tenantB] {
			t.Fatalf("INCONCLUSIVE: tenants %d and %d both resolve to shard %s, but they were chosen "+
				"BECAUSE they route differently. The hazard measurement in (f) needs a tenant that routes "+
				"somewhere the session is not pointing.", tenantA, tenantB, home[tenantA])
		}
		t.Logf("pinned sessions are live and distinct: tenant %d → %s, tenant %d → %s",
			tenantA, home[tenantA], tenantB, home[tenantB])

		nekiShardTargetWALPosition(ctx, t, db, fx, pins)
		nekiShardTargetExportSnapshot(ctx, t, fx, pins)
		nekiShardTargetRepeatableRead(ctx, t, db, pins, home[tenantA], tenantA)
		nekiShardTargetUnionReads(ctx, t, db, fx, pins, authShard)
		nekiShardTargetCopyRejection(ctx, t, db, pins, home[tenantA], tenantA)
		nekiShardTargetWriteHazard(ctx, t, db, fx, pins, home[tenantA], home[tenantB], tenantB)
		nekiShardTargetReplicationConn(ctx, t, fx)
	})
}

// nekiPinShardConn opens a pool of its own, takes ONE connection from it, and
// pins that connection to shard uid.
//
// Both halves matter. A `*sql.Conn` is guaranteed to be the same underlying
// connection for its whole life, which is what makes a session-scoped `SET`
// stick — pinning through a *pool* would leave the SET on whichever
// connection happened to serve it. And the pool is the arm's OWN (capped at
// one connection) so that a pinned session can never be recycled back into
// the suite's shared pool and handed to a later arm: `RESET __neki.shard` in
// the cleanup is the courtesy, the private pool is the guarantee.
//
// A CONNECT failure is fatal — the measurement could not run. A refused SET
// is returned instead, because "the platform does not support targeting" is
// the single most conclusive answer this arm can produce and must not be
// reported as a broken probe.
func nekiPinShardConn(ctx context.Context, t *testing.T, fx *nekiFixture, uid string) (*sql.Conn, error) {
	t.Helper()

	db, err := sql.Open("pgx", fx.dsn)
	if err != nil {
		t.Fatalf("INCONCLUSIVE: open a pool for shard %s: %v", uid, err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)

	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		t.Fatalf("INCONCLUSIVE: take a connection for shard %s: %v", uid, err)
	}
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := conn.ExecContext(cctx, `RESET __neki.shard`); err != nil {
			t.Logf("nekiverify teardown: RESET __neki.shard on %s: %v (the pool is closed below, so the "+
				"session dies with it either way)", uid, err)
		}
		_ = conn.Close()
		_ = db.Close()
	})

	// The uid comes from `__neki.list_shards()`, not from the test, and SET
	// takes no bind parameters — the same interpolation tenantsOnDistinctShards
	// already does.
	if _, err := conn.ExecContext(ctx, `SET __neki.shard = '`+uid+`'`); err != nil {
		return nil, err
	}
	return conn, nil
}

// nekiShardTargetWALPosition is measurement (a): does a targeted session
// answer `pg_current_wal_lsn()`, and does each shard answer its OWN?
//
// This is the whole of PREMISE 2's first half. The router refuses the
// function outright — `not implemented: opcode not implemented:
// pg_current_wal_lsn (NK013)` is what killed `backup full` FROM a Neki at the
// finalize phase — so a per-shard answer would turn "a Neki has no position"
// into "a Neki has N positions", which is a composite-position design rather
// than a dead end.
//
// The router's refusal runs first as the CONTROL. Without it a per-shard
// success proves nothing about targeting: it could simply mean the router
// started answering the function for everyone.
func nekiShardTargetWALPosition(ctx context.Context, t *testing.T, db *sql.DB, fx *nekiFixture, pins map[string]*sql.Conn) {
	t.Run("(a) pg_current_wal_lsn() under targeting", func(t *testing.T) {
		const q = `SELECT pg_catalog.pg_current_wal_lsn()::text`

		var routerLSN string
		switch routerErr := db.QueryRowContext(ctx, q).Scan(&routerLSN); {
		case routerErr == nil:
			t.Logf("CONTROL CHANGED: the UNPINNED router answered pg_current_wal_lsn() with %q. The "+
				"backup capturer's Neki branch (irbackup.ErrPositionUnavailable) is decided from the "+
				"flavor probe rather than from this refusal, so nothing breaks — but a per-shard answer "+
				"below would no longer be evidence about targeting.", routerLSN)
		case isPGCode(routerErr, "NK013"):
			t.Logf("control holds: the unpinned router refuses pg_current_wal_lsn() — %v", routerErr)
		default:
			t.Logf("control (third outcome, record verbatim): the unpinned router failed with neither an "+
				"LSN nor NK013: %v (SQLSTATE %s)", routerErr, nekiSQLState(routerErr))
		}

		lsns := make(map[string]string, len(fx.shards))
		for _, uid := range fx.shards {
			var lsn string
			err := pins[uid].QueryRowContext(ctx, q).Scan(&lsn)
			switch {
			case err == nil:
				lsns[uid] = lsn
				t.Logf("shard %s: pg_current_wal_lsn() = %s", uid, lsn)
			case isPGCode(err, "NK013"):
				t.Logf("shard %s: REFUSED with NK013 under targeting too — %v", uid, err)
			default:
				t.Logf("shard %s (third outcome, record verbatim): %v (SQLSTATE %s)", uid, err, nekiSQLState(err))
			}
		}

		switch {
		case len(lsns) == 0:
			t.Logf("ANSWER: NO — a targeted session does not expose a WAL position on any shard.\n\n" +
				"PREMISE 2 has no source: with no per-shard position there is nothing to record N of, and " +
				"`backup incremental` FROM a Neki stays impossible for the reason ADR-0186 R-1 already " +
				"gives. The replication measurement in (g) is then the only remaining door.")
		case len(lsns) < len(fx.shards):
			t.Logf("ANSWER (partial, and the asymmetry IS the finding): %d of %d shards answered "+
				"pg_current_wal_lsn() under targeting: %v.\n\n"+
				"A composite position needs EVERY shard to have one — a position vector with a hole in it "+
				"cannot bound an incremental. Record which shard refused and why; an answer that varies by "+
				"shard suggests the capability follows some shard property (authoritative role, sidecar "+
				"version) rather than targeting itself.", len(lsns), len(fx.shards), lsns)
		default:
			distinct := make(map[string]struct{}, len(lsns))
			for _, lsn := range lsns {
				distinct[lsn] = struct{}{}
			}
			if len(distinct) == 1 {
				t.Logf("ANSWER (and this is the outcome to distrust): all %d shards reported the SAME "+
					"LSN %v.\n\nTwo independent Postgres primaries do not share a WAL position, so either "+
					"the value is synthesised by the router — in which case it is not a per-shard position "+
					"and PREMISE 2 does not follow from it — or the pins are not taking effect. The "+
					"tenant-placement check at the top of this arm says they are, which leaves the first "+
					"reading. Do not build a composite position on this number until something explains it.",
					len(lsns), lsns)
				return
			}
			t.Logf("ANSWER: YES — every shard answers pg_current_wal_lsn() under targeting, and the "+
				"positions DIFFER (%v).\n\n"+
				"PREMISE 2's first half holds: a Neki has N positions, one per shard, readable over plain "+
				"SQL by a session that targets each shard in turn. That is the VGTID shape sluice already "+
				"carries for Vitess. It is not a green light on its own — the pipeline's single-position "+
				"model is the chunk (ADR-0186), and (g) decides whether the STREAMING half exists — but "+
				"'no mechanism' is no longer the right description.", lsns)
		}
	})
}

// nekiShardTargetExportSnapshot is measurement (b): can a targeted session
// export a snapshot?
//
// This is the second half of the per-shard-consistent-read question, and it
// is the half that decides whether N-way PARALLEL reads of one shard are
// possible. A single targeted REPEATABLE READ transaction gives one reader a
// consistent slice; an exported snapshot is what lets several connections
// share that slice, which is how `backup` and the cold copy get their
// throughput on an ordinary Postgres.
//
// Both call forms are measured. Bare (autocommit) is the cheap probe of
// whether the function exists at all on a targeted session; inside a
// REPEATABLE READ transaction is the form sluice would actually use, and the
// only one whose success would mean anything.
func nekiShardTargetExportSnapshot(ctx context.Context, t *testing.T, fx *nekiFixture, pins map[string]*sql.Conn) {
	t.Run("(b) pg_export_snapshot() under targeting", func(t *testing.T) {
		const q = `SELECT pg_catalog.pg_export_snapshot()`
		uid := fx.shards[0]

		var bare string
		if err := pins[uid].QueryRowContext(ctx, q).Scan(&bare); err != nil {
			t.Logf("shard %s, autocommit: REFUSED — %v (SQLSTATE %s)", uid, err, nekiSQLState(err))
		} else {
			t.Logf("shard %s, autocommit: exported %q (an autocommit snapshot dies at the end of the "+
				"statement, so this proves the function is reachable and nothing more)", uid, bare)
		}

		tx, err := pins[uid].BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
		if err != nil {
			t.Logf("ANSWER: a targeted session cannot open a REPEATABLE READ transaction at all: %v "+
				"(SQLSTATE %s).\n\nThat answers (c) as well and kills PREMISE 1 outright — without a "+
				"transaction there is no snapshot to share and no consistent slice to read.",
				err, nekiSQLState(err))
			return
		}
		defer func() { _ = tx.Rollback() }()

		var snap string
		if err := tx.QueryRowContext(ctx, q).Scan(&snap); err != nil {
			t.Logf("ANSWER: NO — a targeted REPEATABLE READ transaction cannot export a snapshot: %v "+
				"(SQLSTATE %s).\n\n"+
				"PREMISE 1 survives in its single-reader form — (c) decides that — but a per-shard read "+
				"would be SERIAL within each shard: one connection holding the transaction, with no way "+
				"to hand its snapshot to a second. The parallelism would be across shards only, which for "+
				"a two-shard cluster is a factor of two and not the table × chunk fan-out the copy path "+
				"is built around.", err, nekiSQLState(err))
			return
		}
		t.Logf("ANSWER: YES — a targeted REPEATABLE READ transaction exported snapshot %q on shard %s.\n\n"+
			"This is the strong form of PREMISE 1: several connections could each target the same shard, "+
			"SET TRANSACTION SNAPSHOT to this id, and read one internally-consistent slice in parallel — "+
			"the shape sluice's copy path already has for an ordinary Postgres. NOT yet verified here: "+
			"that a SECOND targeted session can actually IMPORT it. Measure that before designing on it; "+
			"an exported id that nothing can import is a string, not a snapshot.", snap, uid)
	})
}

// nekiShardTargetRepeatableRead is measurement (c): is a targeted REPEATABLE
// READ transaction a real Postgres snapshot of that shard?
//
// The measurement is an open transaction on shard S, a concurrent INSERT from
// the ROUTER whose shard key routes to S, and a re-count inside the still-open
// transaction. A snapshot must not see it.
//
// The anti-vacuity half runs after the commit and matters as much as the
// measurement: the row must be VISIBLE to a fresh pinned read on S. Without
// that check, "the count did not change" is equally consistent with the row
// having landed on the other shard, where the transaction would not have seen
// it regardless of isolation — the adjacent-question shape this suite has
// filed three times.
func nekiShardTargetRepeatableRead(
	ctx context.Context,
	t *testing.T,
	db *sql.DB,
	pins map[string]*sql.Conn,
	uid string,
	tenant int,
) {
	t.Run("(c) REPEATABLE READ on a targeted session", func(t *testing.T) {
		const probeID = 970101

		tx, err := pins[uid].BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
		if err != nil {
			t.Logf("ANSWER: NO — a targeted session cannot open a REPEATABLE READ transaction: %v "+
				"(SQLSTATE %s). PREMISE 1 is dead: there is no per-shard consistent read to build a "+
				"shard-by-shard copy on.", err, nekiSQLState(err))
			return
		}
		committed := false
		defer func() {
			if !committed {
				_ = tx.Rollback()
			}
		}()

		var before int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM sk_good`).Scan(&before); err != nil {
			t.Fatalf("INCONCLUSIVE: the first in-transaction count on shard %s failed: %v", uid, err)
		}

		t.Cleanup(func() {
			nekiDropResidue(t, db, fmt.Sprintf(`DELETE FROM sk_good WHERE tenant_id = %d AND id = %d`, tenant, probeID))
		})
		if _, err := db.ExecContext(ctx,
			`INSERT INTO sk_good (tenant_id, id, v) VALUES ($1, $2, 'rr-probe')`, tenant, probeID); err != nil {
			t.Fatalf("INCONCLUSIVE: the concurrent router INSERT failed: %v\n\nWith no concurrent write "+
				"there is nothing for the snapshot to hide, and an unchanged count would mean nothing.", err)
		}

		var after int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM sk_good`).Scan(&after); err != nil {
			t.Fatalf("INCONCLUSIVE: the second in-transaction count on shard %s failed: %v", uid, err)
		}
		if err := tx.Commit(); err != nil {
			t.Logf("the probe transaction failed to commit: %v (it wrote nothing, so this affects only the "+
				"visibility check below)", err)
		}
		committed = true

		// ANTI-VACUITY: the row has to be ON this shard and visible now that
		// the snapshot is gone.
		var visible int
		if err := pins[uid].QueryRowContext(ctx,
			`SELECT count(*) FROM sk_good WHERE id = $1`, probeID).Scan(&visible); err != nil {
			t.Fatalf("INCONCLUSIVE: could not re-read the probe row on shard %s after the transaction "+
				"closed: %v", uid, err)
		}
		if visible != 1 {
			t.Fatalf("INCONCLUSIVE: after the transaction closed, a pinned read on shard %s sees %d copies "+
				"of the probe row, not 1.\n\nThe row did not land where the snapshot was taken, so the "+
				"counts below measure routing rather than isolation. tenant %d was placed on %s by this "+
				"arm's own pin check, which makes this a contradiction worth chasing before anything else "+
				"here is believed.", uid, visible, tenant, uid)
		}

		switch after {
		case before:
			t.Logf("ANSWER: YES — a REPEATABLE READ transaction on a targeted session is a real per-shard "+
				"snapshot. The count held at %d across a concurrent router INSERT that demonstrably landed "+
				"on this shard (it is visible at %d row after the commit).\n\n"+
				"PREMISE 1 HOLDS in its single-reader form: `migrate` and `backup` FROM a sharded Neki "+
				"could read each table shard-by-shard, each slice internally consistent — which is "+
				"strictly better than today's router scatter-gather, consistent nowhere. What is still "+
				"missing is the CROSS-shard cut: Neki commits each shard separately with no two-phase "+
				"commit, so N consistent slices are still N snapshots and not one. (b) says whether the "+
				"slices can be read in parallel; (d) says whether they can be summed.", before, visible)
		case before + 1:
			t.Logf("ANSWER: NO — the open REPEATABLE READ transaction SAW the concurrent insert (%d → %d).\n\n"+
				"A targeted session's transaction does not carry a Postgres snapshot of the shard, so "+
				"PREMISE 1 is false as stated and a shard-by-shard read would be no more consistent than "+
				"the scatter-gather sluice does today. Before acting on this, rule out the obvious "+
				"alternative: that the router is not honouring ISOLATION LEVEL on a targeted session at "+
				"all, which presents identically and is a different defect to report.", before, after)
		default:
			t.Fatalf("INCONCLUSIVE: the in-transaction count moved from %d to %d, which is neither "+
				"unchanged nor exactly one more.\n\nSomething else wrote to sk_good on shard %s during "+
				"this measurement. The arms in this file run sequentially, so record what else was "+
				"running rather than forcing this into one of the two readings.", before, after, uid)
		}
	})
}

// nekiShardTargetUnionReads is measurement (d): does reading every shard and
// summing give the router's answer — for a SHARDED table, and for an
// UNSHARDED one?
//
// Both halves are needed before a per-shard copy could replace the router
// scatter. A sharded table's slices live on every shard of its group, so the
// sum is the table. An authoritative-group table lives on ONE shard, and what
// the OTHER shards say about it is the whole question: 0 rows means a naive
// union is correct; rows mean it would double-count; an error means the union
// has to know which tables are unsharded before it reads them.
//
// The unsharded half rides `sluice_cdc_state`, placed in the authoritative
// group by ADR-0187 when the CDC arm ran earlier in this sequence. If that arm
// did not get that far the table is absent — logged as INCONCLUSIVE for this
// sub-measurement only, never fatal, because the sharded half above it is a
// complete answer on its own.
func nekiShardTargetUnionReads(
	ctx context.Context,
	t *testing.T,
	db *sql.DB,
	fx *nekiFixture,
	pins map[string]*sql.Conn,
	authShard string,
) {
	t.Run("(d) per-shard union reads vs the router's unpinned view", func(t *testing.T) {
		var routerTotal int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM sk_good`).Scan(&routerTotal); err != nil {
			t.Fatalf("INCONCLUSIVE: the router's unpinned count of sk_good failed: %v", err)
		}

		sum, empty := 0, 0
		for _, uid := range fx.shards {
			var n int
			if err := pins[uid].QueryRowContext(ctx, `SELECT count(*) FROM sk_good`).Scan(&n); err != nil {
				t.Fatalf("INCONCLUSIVE: the pinned count of sk_good on shard %s failed: %v", uid, err)
			}
			t.Logf("shard %s holds %d rows of sk_good", uid, n)
			if n == 0 {
				empty++
			}
			sum += n
		}
		if empty > 0 {
			t.Logf("%d shard(s) hold NO rows of sk_good. The comparison below still runs, but a sum that "+
				"matches while one shard is empty is a weaker result than it looks — it would also match "+
				"if the pins were reading the router and one of them were broken.", empty)
		}

		if sum == routerTotal {
			t.Logf("ANSWER (sharded table): YES — the per-shard sum equals the router's unpinned count "+
				"(%d). Each row is visible on exactly one shard, so a shard-by-shard read of a SHARDED "+
				"table is complete and non-duplicating.", routerTotal)
		} else {
			t.Logf("ANSWER (sharded table): NO — the per-shard sum is %d and the router reports %d.\n\n"+
				"This is the finding that would stop a per-shard copy dead, and its direction says which "+
				"failure it is: a sum ABOVE the router's count means a row is visible on more than one "+
				"shard (a union would duplicate); BELOW means the union misses rows the router can see, "+
				"which is worse — those rows live somewhere targeting cannot reach. Nothing else in this "+
				"file wrote to sk_good while this ran.", sum, routerTotal)
		}

		// ---- the UNSHARDED half ----------------------------------------
		ctl := appliershared.ControlTableName
		var routerCtl int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM public.`+ctl).Scan(&routerCtl); err != nil {
			t.Logf("INCONCLUSIVE (this sub-measurement only): the router cannot read public.%s: %v "+
				"(SQLSTATE %s).\n\nThat table is created and placed in the authoritative group by the CDC "+
				"arm earlier in this sequence; if that arm did not reach its control-table door there is "+
				"no unsharded table here to ask about. The sharded half above is unaffected.",
				ctl, err, nekiSQLState(err))
			return
		}
		t.Logf("the router's unpinned count of public.%s is %d", ctl, routerCtl)

		nonAuth := ""
		for _, uid := range fx.shards {
			if uid != authShard {
				nonAuth = uid
			}
		}
		if authShard == "" || nonAuth == "" || pins[authShard] == nil {
			t.Logf("INCONCLUSIVE (this sub-measurement only): the authoritative shard group is %q, which "+
				"is not one of this fixture's shards (%v).\n\nThe fixture's own notes say the "+
				"auto-created authoritative GROUP's uid IS its shard's uid, so this is either a platform "+
				"change or a fixture whose authoritative group was declared differently. Without knowing "+
				"which shard is authoritative, 'the NON-authoritative shard' is not a thing this arm can "+
				"point at.", authShard, fx.shards)
			return
		}

		var onAuth int
		if err := pins[authShard].QueryRowContext(ctx, `SELECT count(*) FROM public.`+ctl).Scan(&onAuth); err != nil {
			t.Logf("the AUTHORITATIVE shard %s errors reading public.%s: %v (SQLSTATE %s)",
				authShard, ctl, err, nekiSQLState(err))
		} else {
			t.Logf("the AUTHORITATIVE shard %s reports %d row(s) of public.%s", authShard, onAuth, ctl)
		}

		var onOther int
		err := pins[nonAuth].QueryRowContext(ctx, `SELECT count(*) FROM public.`+ctl).Scan(&onOther)
		switch {
		case err != nil:
			t.Logf("ANSWER (unsharded table): the NON-authoritative shard %s ERRORS on public.%s: %v "+
				"(SQLSTATE %s).\n\nA per-shard union read must therefore know which tables are unsharded "+
				"BEFORE it reads them — it cannot discover it by trying. That makes the topology document "+
				"a required input to any shard-by-shard copy, not an optimisation, and it is the cheapest "+
				"of the three outcomes to get wrong silently: a reader that treats this error as 'empty "+
				"shard' would drop the table.", nonAuth, ctl, err, nekiSQLState(err))
		case onOther == 0:
			t.Logf("ANSWER (unsharded table): the NON-authoritative shard %s reports 0 rows of public.%s "+
				"while the router reports %d.\n\nA naive per-shard union is CORRECT for unsharded tables "+
				"too: the table exists everywhere and holds rows only where it is placed, so summing "+
				"cannot duplicate. That is the outcome that makes a shard-by-shard copy uniform across "+
				"both table kinds.", nonAuth, ctl, routerCtl)
		default:
			t.Logf("ANSWER (unsharded table): the NON-authoritative shard %s reports %d row(s) of "+
				"public.%s while the router reports %d.\n\nA naive per-shard union would DOUBLE-COUNT an "+
				"authoritative-group table — the same rows are readable from more than one shard. Any "+
				"shard-by-shard copy must read unsharded tables exactly once, from the authoritative "+
				"shard, and must decide that from the topology rather than from what it can see.",
				nonAuth, onOther, ctl, routerCtl)
		}
	})
}

// nekiShardTargetCopyRejection is measurement (e): is `COPY … FROM STDIN`
// rejected under targeting, and with what?
//
// The vendor page says COPY, DO and schema DDL are rejected on a targeted
// session. The SQLSTATE is what matters to sluice: a copy path that met this
// refusal would need to classify it, and a code shared with something else
// (a bare 0A000, say) would make the operator-facing message generic at the
// exact moment it needs to be specific.
//
// A SUCCESS is also an answer, and the more consequential one — it would mean
// the hazard in (f) is reachable through the bulk-copy path and not only
// through single-row DML.
//
// The COPY error is captured and `nil` returned from `conn.Raw`, deliberately:
// returning it would mark the driver connection bad and destroy the pin that
// (f) still needs.
func nekiShardTargetCopyRejection(
	ctx context.Context,
	t *testing.T,
	db *sql.DB,
	pins map[string]*sql.Conn,
	uid string,
	tenant int,
) {
	t.Run("(e) COPY FROM STDIN under targeting", func(t *testing.T) {
		const probeID = 970201

		var copyErr error
		if err := pins[uid].Raw(func(driverConn any) error {
			pgConn, perr := pgConnFromDriver(driverConn)
			if perr != nil {
				return perr
			}
			row := strings.NewReader(fmt.Sprintf("%d\t%d\tcopy-under-targeting\n", tenant, probeID))
			_, copyErr = pgConn.CopyFrom(ctx, row, `COPY sk_good (tenant_id, id, v) FROM STDIN`)
			return nil
		}); err != nil {
			t.Fatalf("INCONCLUSIVE: could not reach the raw pgconn behind the pinned session on %s: %v", uid, err)
		}

		if copyErr != nil {
			t.Logf("ANSWER: REFUSED, as the vendor page says — %v (SQLSTATE %s).\n\n"+
				"Record the code: a copy path that ever targeted a shard would have to classify it, and "+
				"sluice grades NK013/NK205/NK213/NK306 today. This also closes the bulk half of the "+
				"hazard in (f): a misrouted write cannot arrive through COPY, only through DML.",
				copyErr, nekiSQLState(copyErr))
			return
		}

		t.Cleanup(func() {
			nekiDropResidue(t, db, fmt.Sprintf(`DELETE FROM sk_good WHERE tenant_id = %d AND id = %d`, tenant, probeID))
		})
		var onShard int
		if err := pins[uid].QueryRowContext(ctx,
			`SELECT count(*) FROM sk_good WHERE id = $1`, probeID).Scan(&onShard); err != nil {
			t.Logf("the copied row could not be counted back on shard %s: %v", uid, err)
		}
		t.Logf("ANSWER: ACCEPTED — `COPY … FROM STDIN` was NOT rejected on a targeted session, and shard "+
			"%s now sees %d copy of the probe row.\n\n"+
			"That contradicts PlanetScale's query-planning page, which lists COPY among the statements a "+
			"targeted session rejects. Two consequences, and the second is the dangerous one: the "+
			"per-shard READ side gains a fast path it was not expected to have, and the WRITE hazard in "+
			"(f) becomes reachable in BULK — a whole COPY stream could land on the wrong shard, silently, "+
			"at full speed. The probe's own row is deleted below; the finding is not.", uid, onShard)
	})
}

// nekiShardTargetWriteHazard is measurement (f), THE HAZARD: what happens
// when a targeted session INSERTs a row whose shard key routes somewhere
// else?
//
// The vendor page says targeting bypasses shard-key routing for DML. If that
// is literally true then a targeted INSERT can put a row on a shard its key
// does not belong to — and an equality-routed read, which is what an
// application does, would look on the shard the key names and find nothing.
// The row is not lost (a scattering read can still see it) and it is not
// visible (the read anyone would write cannot), which is the worst pairing:
// a row that exists, is unreachable by its own key, and would be duplicated
// the moment anything re-inserted it.
//
// This is measured rather than reasoned about because it is the evidence
// behind a rule sluice needs to keep: never write under targeting. The row is
// removed three ways afterwards — targeted, equality-routed, and scattering —
// and a residue that survives all three is itself reported.
func nekiShardTargetWriteHazard(
	ctx context.Context,
	t *testing.T,
	db *sql.DB,
	fx *nekiFixture,
	pins map[string]*sql.Conn,
	targetShard, homeShard string,
	foreignTenant int,
) {
	t.Run("(f) THE HAZARD: an INSERT under targeting bypasses shard-key routing", func(t *testing.T) {
		const probeID = 970301

		// The pin must still be the pin. (e) ran a COPY on this connection,
		// and a session that lost its targeting would make everything below
		// read as "routing was honoured" — the benign answer, for the wrong
		// reason.
		var foreignHere int
		if err := pins[targetShard].QueryRowContext(ctx,
			`SELECT count(*) FROM sk_good WHERE tenant_id = $1`, foreignTenant).Scan(&foreignHere); err != nil {
			t.Fatalf("INCONCLUSIVE: could not re-verify the pin on shard %s: %v", targetShard, err)
		}
		if foreignHere != 0 {
			t.Fatalf("INCONCLUSIVE: shard %s can see %d row(s) of tenant %d, which routes to %s.\n\n"+
				"The pin is no longer pointing where this measurement believes — most likely the session "+
				"lost its targeting during (e) — so an insert here would not be the cross-shard write the "+
				"hazard is about.", targetShard, foreignHere, foreignTenant, homeShard)
		}

		t.Cleanup(func() {
			nekiDropResidue(t, db,
				fmt.Sprintf(`DELETE FROM sk_good WHERE tenant_id = %d AND id = %d`, foreignTenant, probeID))
		})

		if _, err := pins[targetShard].ExecContext(ctx,
			`INSERT INTO sk_good (tenant_id, id, v) VALUES ($1, $2, 'targeting-hazard')`,
			foreignTenant, probeID); err != nil {
			t.Logf("ANSWER: the platform REFUSED a targeted INSERT whose shard key routes elsewhere: %v "+
				"(SQLSTATE %s).\n\nThe hazard does not exist on this build — the router validates the key "+
				"against the target shard even though targeting is documented as bypassing routing. That "+
				"is the safe outcome and it is a premise sluice must not come to depend on quietly: a "+
				"preview platform that validates today may stop, and the never-write-under-targeting rule "+
				"should stand on its own.", err, nekiSQLState(err))
			return
		}

		var eq, scatter int
		eqErr := db.QueryRowContext(ctx,
			`SELECT count(*) FROM sk_good WHERE tenant_id = $1 AND id = $2`, foreignTenant, probeID).Scan(&eq)
		scatterErr := db.QueryRowContext(ctx,
			`SELECT count(*) FROM sk_good WHERE id = $1`, probeID).Scan(&scatter)
		if eqErr != nil {
			t.Logf("the router's EQUALITY-routed read failed: %v (SQLSTATE %s)", eqErr, nekiSQLState(eqErr))
		}
		if scatterErr != nil {
			t.Logf("the router's SCATTERING read failed: %v (SQLSTATE %s)", scatterErr, nekiSQLState(scatterErr))
		}

		perShard := make(map[string]int, len(fx.shards))
		for _, uid := range fx.shards {
			var n int
			if err := pins[uid].QueryRowContext(ctx,
				`SELECT count(*) FROM sk_good WHERE id = $1`, probeID).Scan(&n); err != nil {
				t.Logf("the targeted read on shard %s failed: %v (SQLSTATE %s)", uid, err, nekiSQLState(err))
				continue
			}
			perShard[uid] = n
		}
		t.Logf("the row was written by a session targeting %s, carrying tenant %d which routes to %s. "+
			"router equality-routed read: %d (err=%v); router scattering read: %d (err=%v); targeted "+
			"reads: %v", targetShard, foreignTenant, homeShard, eq, eqErr, scatter, scatterErr, perShard)

		switch {
		case perShard[targetShard] == 1 && perShard[homeShard] == 0 && eq == 0:
			t.Logf("ANSWER: THE HAZARD IS REAL, and it is the silent shape.\n\n"+
				"The row landed on the shard the SESSION pointed at (%s), not the one its shard key "+
				"routes to (%s). The router's equality-routed read — the read an application writes, and "+
				"the read sluice's own verify and upsert paths emit — finds NOTHING, while a scattering "+
				"read sees %d. So a write made under targeting is present, unreachable by its own key, "+
				"and would be duplicated by the next upsert on that key.\n\n"+
				"This is the evidence for the rule: SLUICE MUST NEVER WRITE UNDER TARGETING. Any "+
				"per-shard design that comes out of (a)-(d) is READ-ONLY, and the restore/apply path "+
				"stays on the router where ShardPlacementMismatch can refuse a misplacement instead of "+
				"creating one.", targetShard, homeShard, scatter)
		case eq == 1 && perShard[homeShard] == 1:
			t.Logf("ANSWER: routing was HONOURED despite targeting — the row landed on %s, the shard its "+
				"key names, and the equality-routed read finds it.\n\n"+
				"So targeting redirects reads but not the placement of writes on this build. Benign, and "+
				"NOT something to design on: the vendor page documents the opposite, so this is either "+
				"undocumented behaviour or a bug in the direction that happens to be safe.", homeShard)
		default:
			t.Logf("ANSWER (third shape, record verbatim): equality read %d, scattering read %d, per-shard "+
				"%v. This is neither 'the row went where the session pointed' nor 'routing was honoured'. "+
				"A count above 1 anywhere means one INSERT produced more than one row, which would "+
				"outrank everything else in this file.", eq, scatter, perShard)
		}

		// Undo it, three ways, then prove it is gone. A row that no read can
		// route to is also a row no DELETE can route to, so the targeted
		// delete is the one most likely to be the effective one.
		if _, err := pins[targetShard].ExecContext(ctx,
			`DELETE FROM sk_good WHERE id = $1`, probeID); err != nil {
			t.Logf("the TARGETED delete on %s failed: %v (SQLSTATE %s)", targetShard, err, nekiSQLState(err))
		}
		if _, err := db.ExecContext(ctx,
			`DELETE FROM sk_good WHERE tenant_id = $1 AND id = $2`, foreignTenant, probeID); err != nil {
			t.Logf("the router's equality-routed delete failed: %v (SQLSTATE %s)", err, nekiSQLState(err))
		}
		if _, err := db.ExecContext(ctx, `DELETE FROM sk_good WHERE id = $1`, probeID); err != nil {
			t.Logf("the router's scattering delete failed: %v (SQLSTATE %s)", err, nekiSQLState(err))
		}

		var left int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM sk_good WHERE id = $1`, probeID).Scan(&left); err != nil {
			t.Errorf("could not confirm the hazard row is gone: %v — treat id=%d in sk_good as residue a "+
				"later arm may see", err, probeID)
			return
		}
		if left != 0 {
			t.Errorf("the hazard row SURVIVED all three deletes (%d row(s) still visible to a scattering "+
				"read).\n\nThat is a finding in its own right — a row written under targeting that cannot "+
				"be removed through the router is unreachable state on a live cluster — and it is "+
				"residue: every later arm in this suite now reads a sk_good with an extra row in it.", left)
		}
	})
}

// nekiShardTargetReplicationConn is measurement (g): can a REPLICATION
// connection be pointed at one shard?
//
// This is PREMISE 2's second half, and it needs a startup parameter rather
// than a `SET`: a replication connection completes its handshake before any
// SQL can run, and the router's own refusal ("replication connections must
// target a specific shard", 0A000) is what suggests the parameter exists at
// all. Two spellings are tried — `options=-c __neki.shard=<uid>`, the
// standard libpq way to push a GUC through the startup packet, and the bare
// runtime parameter `__neki.shard=<uid>`.
//
// Each spelling is attempted TWICE, and the pair is the point. A bare
// `pgconn` connect answers whether the HANDSHAKE accepts the parameter;
// [openReplicationConn] — the production door, which sluice's CDC reader and
// both snapshot openers go through — also runs two `SET`s after connecting,
// so a failure there with the handshake accepted means the parameter is fine
// and the production path is not. Those are different findings with different
// fixes, and one attempt cannot tell them apart.
//
// Read-only by construction: `IDENTIFY_SYSTEM` and nothing else. No slot is
// created. The database is deleted at the end of the run, so a slot would
// leave no residue — but a probe that creates one is no longer a probe, and
// the next question (does a targeted slot actually decode that shard's WAL?)
// deserves its own arm rather than riding in on this one.
func nekiShardTargetReplicationConn(ctx context.Context, t *testing.T, fx *nekiFixture) {
	t.Run("(g) a replication connection targeted at one shard", func(t *testing.T) {
		uid := fx.shards[0]

		for _, attempt := range []struct {
			name  string
			key   string
			value string
		}{
			{"options=-c __neki.shard", "options", "-c __neki.shard=" + uid},
			{"bare runtime parameter", "__neki.shard", uid},
		} {
			dsn, err := nekiDSNWithParam(fx.dsn, attempt.key, attempt.value)
			if err != nil {
				t.Fatalf("INCONCLUSIVE: could not build the %s DSN: %v", attempt.name, err)
			}

			// The handshake, bare. pgconn puts any setting it does not
			// recognise into RuntimeParams and sends it in the startup
			// packet, which is exactly the mechanism under test.
			replDSN, err := withReplicationParam(dsn)
			if err != nil {
				t.Fatalf("INCONCLUSIVE: could not add replication=database to the %s DSN: %v", attempt.name, err)
			}
			cfg, err := pgconn.ParseConfig(replDSN)
			if err != nil {
				t.Logf("ANSWER (%s, shard %s): the DSN itself is REJECTED BY THE CLIENT before any "+
					"connection is made: %v.\n\nThat is a pgconn result, not a platform one — the "+
					"parameter cannot be expressed this way at all.", attempt.name, uid, err)
				continue
			}
			bare, bareErr := pgconn.ConnectConfig(ctx, cfg)
			if bareErr != nil {
				t.Logf("ANSWER (%s, shard %s): the replication HANDSHAKE was REFUSED — %v (SQLSTATE %s).\n\n"+
					"If this is 0A000 with the router's 'replication connections must target a specific "+
					"shard' wording, the parameter spelling is wrong rather than the idea; if it names "+
					"__neki.shard, the router rejects the parameter itself and this door is closed for "+
					"this spelling.", attempt.name, uid, bareErr, nekiSQLState(bareErr))
				continue
			}

			sys, idErr := pglogrepl.IdentifySystem(ctx, bare)
			if idErr != nil {
				t.Logf("ANSWER (%s, shard %s): the handshake was ACCEPTED and IDENTIFY_SYSTEM then failed: "+
					"%v (SQLSTATE %s). A replication connection that connects and cannot identify itself "+
					"is not yet a usable door.", attempt.name, uid, idErr, nekiSQLState(idErr))
			} else {
				t.Logf("ANSWER (%s, shard %s): ACCEPTED — IDENTIFY_SYSTEM reports systemid=%s timeline=%d "+
					"xlogpos=%s dbname=%s.\n\n"+
					"PREMISE 2's streaming half exists: a replication connection can be pointed at one "+
					"shard through the startup packet. With (a)'s per-shard positions that is the whole "+
					"composite-position shape — N slots, N positions, the VGTID pattern sluice already "+
					"carries for Vitess. Still a chunk (the pipeline's single-position model, ADR-0186) "+
					"and still demand-gated; no longer 'no mechanism'. NOT measured here: whether a slot "+
					"created on this connection decodes only that shard's WAL, which is the next arm.",
					attempt.name, uid, sys.SystemID, sys.Timeline, sys.XLogPos, sys.DBName)
			}
			closeReplConnGraceful(bare)

			// And the PRODUCTION door, which adds application_name and runs
			// SET extra_float_digits / SET bytea_output after connecting.
			prod, prodErr := openReplicationConn(ctx, dsn, "nekiverify-shard-target")
			if prodErr != nil {
				t.Logf("ANSWER (%s, shard %s): the handshake above was accepted but sluice's OWN "+
					"replication opener failed: %v (SQLSTATE %s).\n\n"+
					"openReplicationConn runs `SET extra_float_digits = 3` and `SET bytea_output = hex` "+
					"on the walsender session after connecting (Bug 194's CDC face and audit 2026-08-05 "+
					"B-1). A targeted session that refuses those SETs would need the pins moved into the "+
					"startup packet too — a small, known change, and a different finding from the "+
					"parameter being rejected.", attempt.name, uid, prodErr, nekiSQLState(prodErr))
				continue
			}
			t.Logf("sluice's own openReplicationConn also accepted the %s spelling on shard %s, post-connect "+
				"SETs included", attempt.name, uid)
			closeReplConnGraceful(prod)
		}
	})
}

// nekiDSNWithParam returns dsn with one query parameter set, preserving
// everything already there. URI form only, which is what the fixture mints.
func nekiDSNWithParam(dsn, key, value string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set(key, value)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// nekiSQLState renders an error's SQLSTATE for a log line, or says there is
// none. A probe reports codes it does not recognise rather than matching on
// the ones it does, so this returns a string instead of a bool.
func nekiSQLState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return "no SQLSTATE"
}
