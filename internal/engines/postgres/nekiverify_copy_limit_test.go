//go:build nekiverify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// nekiConcurrentCopyLimitHoldsOnTheCluster MEASURES the platform's concurrent
// `COPY … FROM STDIN` limit and grades it against the constant sluice ships.
//
// # Why this is worth a live cluster
//
// [nekiConcurrentCopyLimit] is 4, and it is not a preference — it is a claim
// about somebody else's platform, compiled into shipped tuning. When a Neki
// target is detected, `ResolveCopyAxes` folds it into
// `ir.ConnectionBudget.CopyConcurrencyCeiling`, which collapses the copy's
// table × chunk fan-out so the PRODUCT stays at or below 4. Every migrate into
// a Neki target is paced by that number.
//
// Its own doc comment says, in those words, "The premise this rests on, named"
// — and then names it. What the repository had was `TestNekiCopyBudgetCap`, a
// unit test proving sluice's arithmetic agrees with sluice's constant. That is
// worth having and it cannot fail when the PLATFORM moves, because no unit test
// has ever asked the platform anything. This is the gap CLAUDE.md's
// premise-naming rule describes: a safety argument citing an environmental fact
// owes that fact a runtime check, and a weekly live suite is the only place one
// can exist.
//
// # The two directions fail differently, and both fail
//
// Observed limit BELOW the constant is the dangerous direction: sluice would
// pace itself to 4, the platform would admit fewer, and a real migration meets
// `53300` mid-copy — the loud, actionable failure the constant's comment says
// must stay loud precisely because the cap cannot prevent contention.
//
// Observed limit ABOVE the constant is benign for correctness — sluice is
// merely conservative and copies slower than it could. It still fails here, on
// purpose. A weekly that tolerates drift stops being evidence of anything, the
// fix is a one-line constant change, and the constant's own comment already
// nominates where to make it. Silently leaving throughput on the table for
// however long nobody looks is not the better outcome.
//
// # What "held open" means, and why the probe is shaped this way
//
// A `COPY … FROM STDIN` occupies its slot for as long as the client has not
// finished sending. So each probe session starts a CopyFrom whose reader
// BLOCKS, and the session stays open until this test releases it. An accepted
// COPY is therefore one whose CopyFrom has not returned; a refused one returns
// `53300` promptly. That distinction is the whole measurement.
func nekiConcurrentCopyLimitHoldsOnTheCluster(ctx context.Context, t *testing.T, fx *nekiFixture, probeTenants []int) {
	t.Helper()

	t.Run("PREMISE: the concurrent-COPY limit sluice paces itself to is still the platform's", func(t *testing.T) {
		// The ceiling is 12, raised from nekiConcurrentCopyLimit+2 after the
		// first live run accepted every one of its six sessions and could only
		// report a floor.
		//
		// 12 is chosen against the cluster rather than picked: a fresh PS-10
		// reports max_connections = 30, of which the sidecar reserves 10 for
		// the replicator, backups and metrics, so ~20 are available to an
		// application. Twelve concurrent holders leaves real headroom for the
		// fixture's own connection and anything the router keeps for itself,
		// while sitting comfortably above both the constant and the floor the
		// first run established.
		//
		// THE HYPOTHESIS IT IS SIZED TO TEST: this fixture has two shards, so
		// a limit enforced PER SHARD at 4 would present as 8 here — and
		// nekiConcurrentCopyLimit would then be right as written, with only
		// this arm's comparison being wrong. Twelve can distinguish that from
		// a genuinely raised cluster-wide limit; six could not.
		maxProbe := 12

		type session struct {
			conn    *sql.DB
			release chan struct{}
			done    chan error
		}
		var held []*session

		// A connection of the arm's own, for cleaning the probe's rows up. The
		// per-session ones are each pinned to a held COPY and cannot run a
		// DELETE while they are holding one.
		probeDB, err := sql.Open("pgx", fx.dsn)
		if err != nil {
			t.Fatalf("open the probe's cleanup connection: %v", err)
		}
		defer func() { _ = probeDB.Close() }()

		// Release every held COPY before leaving, whatever happens. A COPY
		// left open would occupy a slot for the rest of the suite and make
		// every later subtest's failure someone else's mystery.
		//
		// Released CONCURRENTLY, and that is a fix rather than a flourish. The
		// first live run released them one at a time with a 30s budget each,
		// two sessions did not come back, and the arm spent 87s — most of it
		// waiting serially on timeouts that a parallel release overlaps into
		// one. It also reports the ERROR from a session that fails to finish,
		// where the first cut only said "did not finish" and left the reason
		// in the platform rather than in the log.
		defer func() {
			type outcome struct {
				i   int
				err error
				ok  bool
			}
			results := make(chan outcome, len(held))
			for i, s := range held {
				close(s.release)
				go func() {
					select {
					case err := <-s.done:
						results <- outcome{i: i, err: err, ok: true}
					case <-time.After(45 * time.Second):
						results <- outcome{i: i, ok: false}
					}
				}()
			}
			for range held {
				r := <-results
				switch {
				case !r.ok:
					t.Errorf("held COPY %d did not finish within 45s of release — the fixture may still "+
						"be occupying a copy slot for whatever runs next", r.i+1)
				case r.err != nil:
					t.Errorf("held COPY %d failed on release: %v", r.i+1, r.err)
				}
			}
			for _, s := range held {
				_ = s.conn.Close()
			}

			// PROVE the sessions are gone rather than asserting it in a
			// comment.
			//
			// The Errorf above says "the fixture MAY still be occupying a copy
			// slot for whatever runs next", and that sentence was the entire
			// state of knowledge: a client-side `sql.DB.Close` returns without
			// waiting for anything the SERVER is still doing (it closes free
			// connections and marks the pool closed; in-use ones close on
			// return), so nothing here had ever established that the probe's
			// twelve COPY sessions were actually gone. A written invariant
			// nobody checks is indistinguishable from one that holds, and this
			// one gates every arm that follows — run 34932058458 spent 1008 s
			// inside the NEXT arm's first single-row INSERT.
			//
			// That stall is NOT attributed to these sessions (the same release
			// outcomes appear in two runs that had no stall, and this teardown
			// measured 0.55 s in the stalled run itself). It is the reason the
			// question must stop being unanswerable: the next occurrence now
			// has a census either way.
			nekiReapProbeCopySessions(t, probeDB, copyProbeTable)

			// The probe's own rows, removed so the fixture is as it was found.
			cctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			if _, err := probeDB.ExecContext(cctx,
				fmt.Sprintf("DELETE FROM %s WHERE v = 'copy-limit-probe'", copyProbeTable)); err != nil {
				t.Errorf("could not remove the probe's rows from %s: %v", copyProbeTable, err)
			}
		}()

		observed := 0
		var refusal error

		for i := 1; i <= maxProbe; i++ {
			db, err := sql.Open("pgx", fx.dsn)
			if err != nil {
				t.Fatalf("open connection %d: %v", i, err)
			}
			db.SetMaxOpenConns(1)

			// CONNECT FIRST, as its own step, and this is not ceremony.
			//
			// Measured while building this probe: with a wrong password every
			// session returned early, exactly as a refusal does, and the loop
			// read that as "the platform admits 0 concurrent COPYs". A
			// transient connect failure at session 3 would likewise have been
			// reported as a measured limit of 2 — a confident, alarming, wrong
			// finding about somebody else's platform. Establishing the
			// connection separately means a connection problem can only ever
			// fail as a connection problem.
			if err := db.PingContext(ctx); err != nil {
				_ = db.Close()
				t.Fatalf("probe session %d could not CONNECT (%v).\n\n"+
					"This is not a concurrency limit and must not be reported as one — %d session(s) "+
					"were holding a COPY at the time, but that number measures nothing while a "+
					"connection cannot be established.", i, err, len(held))
			}

			s := &session{conn: db, release: make(chan struct{}), done: make(chan error, 1)}

			go func() {
				// A distinct id per session, and a tenant that alternates, so
				// the sessions do not all route to the same shard — a limit
				// enforced per shard would otherwise be invisible to a probe
				// whose rows all land in one place.
				s.done <- holdOneCopy(ctx, db, s.release, probeTenants[i%len(probeTenants)], 91000+i)
			}()

			// Give the COPY long enough to be accepted or refused. A refusal
			// comes back fast; an acceptance never comes back until released.
			select {
			case err := <-s.done:
				// This session did NOT get a slot. Whether that is THE LIMIT
				// or merely a failure depends on what came back, and the probe
				// must not assume.
				refusal = err
				_ = db.Close()

				var pgErr *pgconn.PgError
				if !errors.As(err, &pgErr) || pgErr.Code != "53300" {
					t.Fatalf("probe session %d's COPY failed with something that is NOT the "+
						"concurrency refusal: %v\n\n"+
						"%d session(s) were holding a COPY, but that is not a measured limit — only a "+
						"53300 means 'the platform would not admit another COPY'. Reporting this as a "+
						"limit would turn an unrelated failure into a finding about the platform.",
						i, err, len(held))
				}
				observed = i - 1
			case <-time.After(4 * time.Second):
				// Still running ⇒ the COPY is open and holding a slot.
				held = append(held, s)
				continue
			}
			break
		}

		// Nothing was refused within the probe's own ceiling. That is a FLOOR,
		// not a measurement, and the distinction is load-bearing.
		//
		// The first live run hit exactly this and reported "measured 6" —
		// then told the reader to raise nekiConcurrentCopyLimit "to the
		// measured value". Six was never measured; six is where the probe
		// stopped asking, and the true limit could be 8, 40, or absent. Acting
		// on that advice would have replaced one unverified constant with
		// another, which is worse than the original because it would look
		// freshly confirmed.
		unbounded := observed == 0 && refusal == nil
		if unbounded {
			observed = maxProbe
		}

		// ANTI-VACUITY, and it matters more than usual here: a probe where
		// nothing was ever accepted is not measuring a limit of zero, it is
		// measuring a broken fixture — wrong DSN, missing table, revoked
		// INSERT. Reporting that as "the platform admits 0 concurrent COPYs"
		// would be a confident lie.
		if len(held) == 0 {
			t.Fatalf("not a single COPY was accepted, so no limit was measured. The first failure was: "+
				"%v\n\nThis is a broken probe rather than a platform of zero — check that %s exists on "+
				"the fixture and that the role may INSERT into it.", refusal, copyProbeTable)
		}

		t.Logf("measured concurrent-COPY limit: %d accepted, then %v", len(held), refusal)

		switch {
		case observed == nekiConcurrentCopyLimit:
			// The premise holds. Say the number out loud so the run's log is
			// evidence rather than a silent pass.
			t.Logf("premise holds: the platform admits %d concurrent COPYs, matching "+
				"nekiConcurrentCopyLimit", observed)

		case observed < nekiConcurrentCopyLimit:
			t.Fatalf("THE PLATFORM NOW ADMITS FEWER CONCURRENT COPYs THAN SLUICE PACES ITSELF TO: "+
				"measured %d, nekiConcurrentCopyLimit is %d.\n\n"+
				"This is the dangerous direction. ResolveCopyAxes folds that constant into "+
				"CopyConcurrencyCeiling and collapses the table × chunk fan-out to stay at or below it, "+
				"so every Neki migrate now opens more concurrent COPYs than the cluster will take and "+
				"meets 53300 mid-copy.\n\nFirst refusal: %v\n\n"+
				"Lower nekiConcurrentCopyLimit (internal/engines/postgres/connection_budget.go) to the "+
				"measured value.", observed, nekiConcurrentCopyLimit, refusal)

		case unbounded:
			t.Fatalf("the platform admits AT LEAST %d concurrent COPYs — every session this probe "+
				"opened was accepted — while nekiConcurrentCopyLimit is %d.\n\n"+
				"AT LEAST, not exactly: %d is where this probe stopped asking, so the real limit may be "+
				"higher or may not exist. Do NOT raise the constant to %d on the strength of this "+
				"message; that would swap one unverified number for another that merely looks freshly "+
				"confirmed.\n\n"+
				"This is the benign direction for correctness — sluice is conservative, not wrong, and "+
				"no migration breaks. It fails anyway, deliberately: a weekly premise check that "+
				"tolerated drift would stop being evidence, and the cost is throughput left on the "+
				"table for as long as nobody looks.\n\n"+
				"To act on it: raise maxProbe here until a 53300 actually appears, confirm the number "+
				"is stable across runs and shard counts, and only then change "+
				"nekiConcurrentCopyLimit (internal/engines/postgres/connection_budget.go). Worth ruling "+
				"out first that the limit is per-SHARD rather than per-cluster — this fixture has two "+
				"shards, so a per-shard limit of 4 would present as 8 here and the constant would be "+
				"right as written.",
				observed, nekiConcurrentCopyLimit, observed, observed)

		default:
			t.Fatalf("the platform refused the %dth concurrent COPY, so it admits %d — more than the %d "+
				"sluice paces itself to.\n\n"+
				"This is the benign direction for correctness — sluice is conservative, not wrong. It "+
				"fails anyway, deliberately: a weekly that tolerated drift would stop being evidence, "+
				"and the fix is a one-line constant change its own comment nominates a home for.\n\n"+
				"Refusal: %v\n\n"+
				"Before raising nekiConcurrentCopyLimit "+
				"(internal/engines/postgres/connection_budget.go), rule out that the limit is per-SHARD "+
				"rather than per-cluster — this fixture has two shards.",
				observed+1, observed, nekiConcurrentCopyLimit, refusal)
		}

		// The SQLSTATE is already guaranteed to be 53300 — the loop refuses to
		// treat anything else as a limit, and re-asserting it here would be a
		// check that cannot fail.
		//
		// What is still worth grading is the MESSAGE. sluice's operator-facing
		// remedy for this condition quotes the platform's own wording, so a
		// reworded refusal leaves sluice explaining a sentence the operator is
		// not reading.
		if refusal != nil {
			var pgErr *pgconn.PgError
			_ = errors.As(refusal, &pgErr) // guaranteed by the loop above
			t.Logf("the concurrency refusal reads: %s (SQLSTATE %s)", pgErr.Message, pgErr.Code)

			lower := strings.ToLower(pgErr.Message)
			if !strings.Contains(lower, "copy") && !strings.Contains(lower, "concurrent") {
				t.Errorf("the 53300 refusal no longer mentions COPY or concurrency (%q).\n\n"+
					"The code is what sluice classifies on, so nothing breaks — but the operator-facing "+
					"remedy paraphrases this wording, and a refusal about something else arriving under "+
					"53300 would make that remedy misleading rather than merely stale.", pgErr.Message)
			}
		}
	})
}

// copyProbeTable is the fixture table the probe streams into. sk_good is the
// fixture's well-formed sharded table — it has a shard key the probe supplies,
// so a refusal can only be about concurrency and never about NK306.
const copyProbeTable = "sk_good"

// holdOneCopy opens a COPY … FROM STDIN and keeps it open until release is
// closed, then finishes it with zero rows.
//
// The blocking reader is the mechanism: `CopyFrom` does not return until the
// reader reports EOF, so the COPY — and its slot on the cluster — is held for
// exactly as long as this test wants it. Returning io.EOF with no bytes ends
// the COPY cleanly and inserts nothing, so the probe leaves the fixture's data
// exactly as it found it.
func holdOneCopy(ctx context.Context, db *sql.DB, release <-chan struct{}, tenant, id int) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer func() { _ = conn.Close() }()

	row := []byte(fmt.Sprintf("%d\t%d\tcopy-limit-probe\n", tenant, id))

	return conn.Raw(func(driverConn any) error {
		pgConn, perr := pgConnFromDriver(driverConn)
		if perr != nil {
			return perr
		}
		_, cerr := pgConn.CopyFrom(
			ctx,
			&blockingReader{release: release, row: row},
			fmt.Sprintf("COPY %s (tenant_id, id, v) FROM STDIN", copyProbeTable),
		)
		if cerr != nil {
			return fmt.Errorf("COPY FROM STDIN: %w", cerr)
		}
		return nil
	})
}

// blockingReader emits ONE row, then blocks until release is closed, then
// reports EOF.
//
// # Why it emits a row rather than nothing
//
// The first cut sent no bytes at all, which held the COPY open and measured
// six acceptances where sluice assumes four. That number was not trustworthy,
// and the reason is the shard key.
//
// `sk_good` is sharded on `tenant_id`. A COPY that never sends a row never
// routes anything, so it may occupy a protocol slot on the router while never
// engaging the per-shard machinery the limit is actually about — in which case
// the probe was counting open COPY commands rather than concurrent copies into
// the cluster, and comparing that to a constant about the latter.
//
// Sending one real row first makes each session a copy that has demonstrably
// reached a shard before it parks. The row is deleted by the caller's cleanup.
type blockingReader struct {
	release <-chan struct{}
	row     []byte
	sent    bool
	done    bool
}

func (r *blockingReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	if !r.sent {
		r.sent = true
		n := copy(p, r.row)
		if n < len(r.row) {
			// The caller's buffer is smaller than one row. Not expected for a
			// ~40-byte row, and silently truncating would corrupt the COPY
			// stream into a confusing parse error attributed to the platform.
			return 0, fmt.Errorf("probe row (%d bytes) does not fit the COPY buffer (%d bytes)",
				len(r.row), len(p))
		}
		return n, nil
	}
	<-r.release
	r.done = true
	return 0, io.EOF
}

// nekiShardKeyRequiredOnInsert pins NK306 — the refusal a sharded Neki target
// issues for an INSERT that does not name the shard key.
//
// # Why it is worth pinning even though nothing is fixed yet
//
// The suite's own CDC arm currently FAILS on exactly this code, and the
// investigation it is filed under has two candidate causes with different
// fixes: sluice's batched applier dropping the column, or the platform being
// unable to derive the shard key from the statement SHAPE that lane emits.
//
// This arm settles the half that is about the platform rather than about
// sluice, and it settles it every week: a plain INSERT naming the shard key
// succeeds, and the same INSERT omitting it is refused with NK306. With that
// pinned, a future CDC failure cannot be explained away as "the platform
// changed its mind about shard keys" without this arm failing first.
//
// It also documents a code sluice does NOT classify. NK306 appears in this
// repository only as prose and as one hand-built string in a unit test, while
// the engine grades NK013, NK205 and NK213 and nothing else in the NK3xx
// range — so the shape is known to be real and still carries no verdict.
func nekiShardKeyRequiredOnInsert(ctx context.Context, t *testing.T, db *sql.DB, tenant int) {
	t.Helper()

	t.Run("PREMISE: an INSERT omitting the shard key is refused with NK306", func(t *testing.T) {
		// Anti-vacuity FIRST, and in this direction deliberately: if the
		// well-formed INSERT does not work, the refusal below proves nothing
		// about shard keys — it would just mean the table is unusable.
		if _, err := db.ExecContext(
			ctx,
			`INSERT INTO sk_good (tenant_id, id, v) VALUES ($1, 90001, 'shard-key-present')`,
			tenant,
		); err != nil {
			t.Fatalf("the CONTROL insert — which names the shard key — failed: %v\n\n"+
				"Nothing below is evidence about shard keys while this does not work", err)
		}
		t.Cleanup(func() {
			cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_, _ = db.ExecContext(cctx, `DELETE FROM sk_good WHERE id = 90001`)
		})

		// The premise: omit the shard key and the router refuses.
		_, err := db.ExecContext(ctx,
			`INSERT INTO sk_good (id, v) VALUES (90002, 'shard-key-absent')`)
		if err == nil {
			t.Fatalf("an INSERT omitting the shard key SUCCEEDED. That is a platform behaviour change: " +
				"sluice's upsert-key preflight and the CDC applier's shard-key handling are both built " +
				"on the shard key being mandatory, and a router that infers or defaults it would route " +
				"rows somewhere nothing predicts")
		}

		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) {
			t.Fatalf("the shard-key refusal carries no *pgconn.PgError: %v", err)
		}
		if pgErr.Code != "NK306" {
			t.Fatalf("a shard-key-less INSERT is now refused with SQLSTATE %q (%s), not NK306.\n\n"+
				"The code is load-bearing for the open CDC investigation, which turns on distinguishing "+
				"a platform refusal from a sluice defect", pgErr.Code, pgErr.Message)
		}
		if !strings.Contains(strings.ToLower(pgErr.Message), "shard-key") {
			t.Errorf("NK306's message no longer names the shard key (%q) — sluice's operator-facing "+
				"text quotes this wording", pgErr.Message)
		}
		t.Logf("premise holds: %s (SQLSTATE %s)", pgErr.Message, pgErr.Code)
	})
}
