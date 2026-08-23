// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"
)

// Gap-freedom for the trigger change log — the ONE correctness argument
// the cold-start anchor ([readChangeLogAnchor]) and the steady-state
// poll ([CDCReader.poll]) both have to satisfy. They used to encode two
// different arguments, and only the anchor's was sound; this file is
// where the shared argument and its premises live.
//
// THE INVARIANT. A change-log id may be CONSUMED (emitted, and the
// durable watermark advanced past it) only once every lower id is
// either already consumed or PROVEN never to arrive. The id is a
// BIGSERIAL allocated at trigger time, not at commit time, so
//
//	id order ≠ commit order
//
// and the two diverge under ordinary multi-statement OLTP, because the
// id is allocated per statement while the txid is assigned at the
// transaction's FIRST write:
//
//	T_A: BEGIN; INSERT a;   -- xid 100, change-log id 1
//	T_B: BEGIN; INSERT c;   -- xid 101, change-log id 2  (stays open)
//	T_A: INSERT b; COMMIT;  -- xid 100, change-log id 3
//
// A poll taken while T_B is open sees ids 1 and 3 (committed, and both
// with txid < the snapshot's xmin = 101, so a per-ROW `txid < xmin`
// hold-back passes them) and cannot see id 2 (MVCC — T_B has not
// committed). Emitting 1 and 3 advances the watermark to 3; when T_B
// commits, id 2 is behind the durable position and is never emitted, on
// any poll or any restart. Exit 0, `sync status` green, one committed
// change silently gone. That was the shipped steady-state behaviour
// until this file existed — ADR-0066 recorded the hole as "bounded by
// the §2 safety-lag hold-back", which it was not.
//
// THE RULE. Consume only the CONTIGUOUS run of ids starting at
// lastSeen+1. An id missing from the poll's snapshot is a HOLE, and a
// hole is exactly "a row whose allocating transaction has not committed
// — yet, or ever", so stopping at the first hole IS the invariant
// restated. [CDCReader.poll] walks its window in id order and stops
// there. [settledCeilingSQL] additionally truncates the tail at the
// first not-provably-settled VISIBLE row; that is the conservative arm
// the anchor shares, and holding back too much is always safe while
// holding back too little is silent loss.
//
// PREMISES. Each is load-bearing and none is self-evident:
//
//   - The id sequence allocates strictly ASCENDING ids above zero, one
//     at a time, and never re-issues one. That is what makes nextval
//     globally monotonic in allocation order, so no transaction can
//     commit an id BELOW one already consumed. FOUR sequence settings
//     each break it on their own, and nothing in the schema enforces
//     any of them, so [verifyChangeLogSequence] preflights all four at
//     every CDC open and refuses loudly:
//     CACHE > 1 (concurrent sessions draw from disjoint pre-allocated
//     ranges, so a session can commit a low id arbitrarily long after a
//     higher one was consumed); a MINVALUE below 1 (an id of 0 or less
//     sits at or under the watermark floor — [readChangeLogMaxID]
//     returns 0 for an empty log and [readChangeLogAnchor] clamps a
//     negative anchor to 0 — so the change is skipped on the first
//     poll); a negative INCREMENT (every id after the first lands below
//     the last); and CYCLE (at MAXVALUE the sequence wraps back to
//     MINVALUE and re-issues the whole id space). Each breaks the
//     contiguity rule and the hole-permanence proof outright.
//     These are CONFIGURATION checks read at open. A mid-stream
//     `ALTER SEQUENCE … RESTART WITH <low>` re-introduces low ids
//     against any configuration and is outside what an open-time
//     preflight can see — the same stated limit the CACHE check has
//     always had.
//   - A hole is PERMANENT (its allocating transaction aborted) only
//     once every transaction that could still fill it has settled.
//     [holeGuard] proves that with the same freshly-ASSIGNED xid bound
//     + settle probe the cold-start handoff uses ([captureTxidUpperBound],
//     [settleWaitQuery]). Skipping an unproven hole is the silent loss
//     above; never skipping one wedges the stream on the first
//     rolled-back write to a captured table. The proof is what makes
//     both outcomes loud.
//   - Nothing DELETES a change-log row the stream has not consumed. A
//     hole is read as "never committed, or aborted" — a deleted row
//     would be read the same way and skipped, since its allocating
//     transaction has long since settled. The auto-prune holds to this
//     ([PruneConsumedChangeLog] cuts at the TARGET's durably-applied
//     frontier minus a margin, which is at or below this watermark, and
//     its doc says why it must never key off the reader's cursor). A
//     future reaper that trims from the head on age or size instead
//     MUST stay below the consumed watermark, or it turns a prune into
//     silent loss here.

// changeLogWindow is the CTE alias the poll's ceiling is computed over.
// The poll bounds its ceiling to the batch WINDOW (not the whole table)
// so a multi-million-row catch-up backlog cannot turn each poll into a
// full tail scan; the anchor, which runs once per cold start and must
// consider every row, passes the table reference itself.
const changeLogWindow = "w"

// changeLogHoleWarnEvery paces the "the stream is blocked at a hole"
// WARN. A blocked stream is correct (it is the invariant doing its job)
// but it is indistinguishable from a healthy idle one in the logs, and
// this fix trades a silent GAP for a silent STALL if the operator never
// hears about it. Same cadence as the cold-start settle wait.
const changeLogHoleWarnEvery = 30 * time.Second

// holeProofPolls is how many consecutive polls must observe the SAME
// hole before the guard spends an xid proving it permanent. A hole from
// an in-flight transaction normally fills within a poll or two, so the
// delay keeps the steady state free of xid churn and confines the
// assignment to holes that are actually blocking progress.
const holeProofPolls = 2

// settledCeilingSQL renders the shared "(first not-provably-settled id)
// − 1, else MAX(id)" ceiling over rel — the change-log table for the
// cold-start anchor, the batch-window CTE for the poll. Sharing one
// builder is deliberate: the anchor and the poll drifting into two
// hand-written encodings of two different rules is how the steady-state
// gap shipped (see this file's header). Pinned by
// TestPollAndAnchorShareTheSettledCeiling.
//
// BOTH sides of the comparison live in the 64-bit epoch-carrying xid8
// domain: `txid` is the allocating transaction's
// `pg_current_xact_id()::text::bigint`, recorded by the capture trigger
// at insert time (ADR-0066 §2 — NOT NULL since the engine's first
// release, so there are no legacy rows to special-case), and the right
// side is the poll/anchor snapshot's xmin through the same cast. The
// text → bigint cast keeps both values on the driver's int64 path (a
// JSON-number float64 would lose precision above 2^53).
//
// The row's system `xmin` column MUST NOT be used on the left: xmin is
// the 32-bit epoch-LESS xid while pg_snapshot_xmin() is epoch-carrying,
// so once the cluster's lifetime txid count crosses 2^32 (routine on
// long-lived busy PG) the arm never matches, COALESCE falls through to
// MAX(id), and the ceiling silently stops holding anything back. That
// regression shipped once (live-confirmed on a pg_resetwal-epoch-bumped
// PG 16, 2026-07-08). Pinned by TestSettledCeilingSQL_ComparesInXID8Domain
// and TestCDCReader_XIDEpochBump.
//
// SCOPE of the stall WARN, stated so [changeLogHoleWarnEvery]'s rationale
// cannot be read as broader than the code (2026-08-22 invariant sweep):
// the "blocked at a hole" WARN reaches ONLY the hole arm. A stall induced
// by THIS ceiling is named by its own arm — [ceilingStallGuard] (batch C,
// 2026-08-23): a long-open WRITE transaction anywhere on the source (any
// table, captured or not; it only needs an assigned xid) pins
// pg_snapshot_xmin, the ceiling prefix-cuts every committed row with
// txid ≥ xmin out of the window, the poll returns zero rows, and the pump
// takes the no-hole path (holes.clear(), no warnStuck). Correctness
// holds — over-holding is always safe, and consumption resumes when the
// transaction settles — but before the guard existed the operator saw a
// healthy-idle stream while sync lag grew, exactly the silent-STALL shape
// the hole WARN was built to name. The guard probes with
// [ceilingStallProbeSQL] — the SAME not-settled predicate as this
// ceiling, so the two cannot drift ([notSettledPredicate], pinned by
// TestCeilingStallProbe_SharesTheCeilingPredicate) — at WARN cadence, not
// per poll, only after the stream has produced nothing for a full cadence.
// End-to-end: TestCDCReader_CeilingStall_WarnsWhileHeldAndResumes.
func settledCeilingSQL(rel string) string {
	return "COALESCE(\n" +
		"    (SELECT MIN(id) - 1 FROM " + rel + "\n" +
		"      WHERE " + notSettledPredicate + "),\n" +
		"    (SELECT COALESCE(MAX(id), 0) FROM " + rel + ")\n" +
		"  )"
}

// notSettledPredicate is the ceiling's held-row test: the row is visible
// (committed — MVCC hides uncommitted rows from both the poll and the
// probe) but its allocating transaction is not provably settled before
// the current snapshot. It is a single shared constant because TWO
// consumers must agree on it byte-for-byte: [settledCeilingSQL] uses it
// to hold rows back, and [ceilingStallProbeSQL] uses it to tell the
// operator that rows ARE being held back. Two hand-written copies of
// this predicate drifting apart is exactly the anchor-vs-poll drift this
// file's header records; the probe reporting "idle" while the ceiling
// holds rows would be the same shape one layer up.
const notSettledPredicate = "txid >= pg_snapshot_xmin(pg_current_snapshot())::text::bigint"

// ceilingStallProbeSQL renders the held-row probe the stall guard runs
// when the stream has been unproductive for a full WARN cadence: how
// many VISIBLE change-log rows above the watermark the settled ceiling
// is currently holding back, the first such id, and the pinned xmin
// (for the log line). Zero held rows means the stream is genuinely
// idle; a non-zero count is a ceiling stall — committed changes exist
// that no poll will return until the pinning transaction settles.
func ceilingStallProbeSQL(tableRef string) string {
	return "SELECT COUNT(*), COALESCE(MIN(id), 0), pg_snapshot_xmin(pg_current_snapshot())::text::bigint\n" +
		"  FROM " + tableRef + "\n" +
		" WHERE id > $1 AND " + notSettledPredicate
}

// ceilingStallGuard names the OTHER stall — the one [holeGuard.warnStuck]
// cannot see. A hole stall has visible rows above a missing id; a
// CEILING stall has zero visible rows at all (the prefix cut removes the
// whole window), so from the pump's point of view it is byte-identical
// to a healthy idle source. The guard's job is to make that
// distinction observable without touching the hot path: it acts only
// after warnEvery of no progress, and then at most once per warnEvery.
// Owned by the single pump goroutine; no locking.
type ceilingStallGuard struct {
	warnEvery    time.Duration // pacing; changeLogHoleWarnEvery unless a test tightens it
	lastProgress time.Time     // last time the pump consumed events or advanced the watermark
	lastProbe    time.Time     // last probe issued (pacing the probe IS pacing the WARN)
}

// noteProgress records that the stream moved: events were emitted or the
// watermark advanced (including a proven hole skip). Progress resets the
// probe pacing so the next stall is timed from scratch.
func (g *ceilingStallGuard) noteProgress(now time.Time) {
	g.lastProgress = now
	g.lastProbe = time.Time{}
}

// probeDue reports whether the guard should spend a probe now: a full
// cadence of no progress, and a full cadence since the last probe. The
// probe cadence bounds the WARN cadence, and both are off the hot path.
func (g *ceilingStallGuard) probeDue(now time.Time) bool {
	if g.lastProgress.IsZero() || now.Sub(g.lastProgress) < g.warnEvery {
		return false
	}
	return g.lastProbe.IsZero() || now.Sub(g.lastProbe) >= g.warnEvery
}

// holeGuard turns "id h is missing from this poll's snapshot" into one
// of two answers: WAIT (a transaction may still commit it) or SKIP
// (proven aborted, so it can never arrive). Owned by the single pump
// goroutine; no locking.
//
// The proof obligation, in order — the ORDER is the proof:
//
//  1. a poll observes h missing, with ids up to `seenTo` visible above
//     it (so every id ≤ seenTo has already been allocated);
//  2. AFTER that observation the guard ASSIGNS an xid bound U. Every
//     transaction that could hold an id ≤ seenTo was assigned before
//     that id was allocated, hence before the observation, hence < U;
//  3. a LATER settle probe finds no running transaction below U — so
//     all of them settled. Monotone: once true, true forever;
//  4. a LATER poll STILL does not see h. A transaction that committed
//     would be visible by now, so h's allocator aborted.
//
// Step 4 must come after step 3. A probe run after the observation
// would admit a transaction that committed in between, and skipping
// there is exactly the silent loss this file exists to prevent.
type holeGuard struct {
	at     int64     // the hole the stream is currently blocked on (0 = none)
	seen   int       // consecutive polls that observed `at` missing
	since  time.Time // when `at` was first observed (WARN cadence)
	warned time.Time // last WARN emitted for `at`

	bound   int64 // assigned xid upper bound (0 = unarmed)
	ceiling int64 // highest id observed above the hole when `bound` was assigned
	proven  bool  // every transaction with xid < bound has settled
}

// observe records that this poll found the stream blocked at `hole`.
// A hole id that differs from the last one restarts the persistence
// count, but deliberately does NOT discard an existing bound: a proof
// covering ids ≤ ceiling covers every hole in that range, whichever one
// the stream is currently sitting on.
func (g *holeGuard) observe(hole int64, now time.Time) {
	if g.at != hole {
		g.at = hole
		g.seen = 1
		g.since = now
		g.warned = time.Time{}
		return
	}
	g.seen++
}

// clear forgets everything: the stream is no longer blocked, so the
// next hole starts a fresh proof (its allocator may well be a
// transaction that started after the current bound).
func (g *holeGuard) clear() {
	*g = holeGuard{}
}

// needsBound reports whether the guard should spend an xid now: the
// hole has persisted, and no existing proof-in-progress covers it.
func (g *holeGuard) needsBound() bool {
	return g.seen >= holeProofPolls && (g.bound == 0 || g.at > g.ceiling)
}

// arm records the freshly-assigned xid bound and the highest id the
// observing poll saw. The caller MUST assign the bound after that
// observation (step 2 of the proof).
func (g *holeGuard) arm(bound, seenTo int64) {
	g.bound = bound
	g.ceiling = seenTo
	g.proven = false
}

// refresh runs the settle probe (step 3) when one is outstanding. It
// must run BEFORE the poll whose observation it will license. A probe
// error leaves the guard unproven — the stream waits rather than
// skipping on an unverified proof.
func (g *holeGuard) refresh(ctx context.Context, q settleQuerier) error {
	if g.bound == 0 || g.proven {
		return nil
	}
	remaining, err := querySettleTxids(ctx, q, settleWaitQuery, g.bound)
	if err != nil {
		return err
	}
	if len(remaining) == 0 {
		g.proven = true
	}
	return nil
}

// skipTo reports the watermark the stream may advance to across a
// PROVEN-permanent hole, and false when the hole is not proven. The
// skip covers the whole missing run below the next visible id
// (holeEnd), so a rolled-back million-row batch collapses in one step
// instead of one poll per id — but never past `ceiling`, because ids
// above it may have been allocated by transactions that started after
// the bound and are therefore outside the proof.
func (g *holeGuard) skipTo(hole, holeEnd int64) (int64, bool) {
	if !g.proven || g.bound == 0 || hole > g.ceiling {
		return 0, false
	}
	to := holeEnd - 1
	if to > g.ceiling {
		to = g.ceiling
	}
	return to, true
}

// warnStuck emits the paced "blocked at a hole" WARN. Correct behaviour,
// but an operator watching a flat sync-lag graph deserves to know which
// change-log id the stream is waiting on and why.
func (g *holeGuard) warnStuck(ctx context.Context, now time.Time) {
	if now.Sub(g.since) < changeLogHoleWarnEvery {
		return
	}
	if !g.warned.IsZero() && now.Sub(g.warned) < changeLogHoleWarnEvery {
		return
	}
	g.warned = now
	slog.WarnContext(
		ctx, "pgtrigger: CDC stream is holding at a change-log gap (gap-freedom): the transaction that allocated this id has neither committed nor aborted, and emitting later ids would strand it permanently",
		slog.Int64("change_log_id", g.at),
		slog.Duration("blocked_for", now.Sub(g.since).Round(time.Second)),
		slog.Bool("abort_proof_armed", g.bound != 0),
		slog.String("hint", "find the long-running writer: SELECT pid, state, xact_start, query FROM pg_stat_activity WHERE xact_start IS NOT NULL ORDER BY xact_start"),
	)
}

// changeLogSequenceQuery reads the change-log id sequence's name and the
// four allocation settings this file's first premise rests on.
// pg_get_serial_sequence resolves the sequence a BIGSERIAL (or identity)
// column is attached to, so the probe survives a renamed table; a NULL
// means the id column is backed by no sequence at all, which is itself a
// refusal (see [verifyChangeLogSequence]).
//
// The LEFT JOIN (rather than the obvious FROM pg_sequence WHERE …) is
// load-bearing: it returns exactly one all-NULL row when the column has
// no sequence, so that case reaches the named refusal below instead of
// surfacing as sql.ErrNoRows from a query that found nothing.
const changeLogSequenceQuery = `
SELECT q.seq, s.seqcache, s.seqmin, s.seqincrement, s.seqcycle
  FROM (SELECT pg_get_serial_sequence($1, 'id') AS seq) q
  LEFT JOIN pg_sequence s ON s.seqrelid = q.seq::regclass`

// verifyChangeLogSequence preflights the ascending-monotone-allocation
// premise this file's header states, across all four sequence settings
// that can break it. `sluice trigger setup` creates the column as
// BIGSERIAL — CACHE 1, MINVALUE 1, INCREMENT 1, NO CYCLE — so any other
// value means someone ALTERed it; refusing is right and costs a
// correctly-configured source nothing.
//
// SCOPE, stated so the name cannot be read as broader than the truth:
// this grades the sequence's CONFIGURATION at CDC open. It does not and
// cannot catch a mid-stream `ALTER SEQUENCE … RESTART WITH <low>`, which
// re-issues low ids under a perfectly legal configuration. It is also
// pgtrigger-only — the sibling sqlite-trigger engines allocate ids from
// SQLite's own rowid/AUTOINCREMENT machinery, which has no pg_sequence
// row to read and needs its own separate argument.
func verifyChangeLogSequence(ctx context.Context, db *sql.DB, schema string) error {
	tableRef := quoteIdent(schema) + "." + quoteIdent(ChangeLogTable)
	var (
		seqName   sql.NullString
		cache     sql.NullInt64
		minValue  sql.NullInt64
		increment sql.NullInt64
		cycle     sql.NullBool
	)
	row := db.QueryRowContext(ctx, changeLogSequenceQuery, tableRef)
	if err := row.Scan(&seqName, &cache, &minValue, &increment, &cycle); err != nil {
		return fmt.Errorf("pgtrigger: preflight: read %s id sequence settings: %w", tableRef, err)
	}
	if !seqName.Valid || !cache.Valid {
		return fmt.Errorf(
			"pgtrigger: %s.id is not backed by a sequence — sluice's gap-free CDC ordering requires the BIGSERIAL identity `sluice trigger setup` installs (ids allocated monotonically across sessions); drop the change-log table and re-run `sluice trigger setup --dsn=...`",
			tableRef,
		)
	}
	if cache.Int64 != 1 {
		return fmt.Errorf(
			"pgtrigger: change-log id sequence %s has CACHE %d, not 1 — with a cached range each session pre-allocates a disjoint block of ids, so a session can commit a LOW id after the stream has already consumed a HIGHER one and that change would be dropped silently; run `ALTER SEQUENCE %s CACHE 1` on the source, then restart the stream",
			seqName.String, cache.Int64, seqName.String,
		)
	}
	if minValue.Int64 < 1 {
		return fmt.Errorf(
			"pgtrigger: change-log id sequence %s has MINVALUE %d, not 1 — the poll only ever consumes ids ABOVE its watermark and that watermark floors at 0 (an empty change log starts the stream at 0, and a negative cold-start anchor is clamped to 0), so a change captured with id %d or lower is never emitted and is dropped silently; run `ALTER SEQUENCE %s MINVALUE 1 RESTART WITH <a value above every id already in the change log>` on the source, then restart the stream",
			seqName.String, minValue.Int64, minValue.Int64, seqName.String,
		)
	}
	if increment.Int64 < 1 {
		return fmt.Errorf(
			"pgtrigger: change-log id sequence %s has INCREMENT %d, not a positive step — a descending sequence allocates each id BELOW the previous one, so every change after the first lands under the stream's watermark and is dropped silently; run `ALTER SEQUENCE %s INCREMENT BY 1 RESTART WITH <a value above every id already in the change log>` on the source, then restart the stream",
			seqName.String, increment.Int64, seqName.String,
		)
	}
	if cycle.Bool {
		return fmt.Errorf(
			"pgtrigger: change-log id sequence %s is declared CYCLE — on reaching MAXVALUE it wraps back to MINVALUE and re-issues ids the stream has already consumed, and every re-issued change would be dropped silently; run `ALTER SEQUENCE %s NO CYCLE` on the source, then restart the stream",
			seqName.String, seqName.String,
		)
	}
	return nil
}
