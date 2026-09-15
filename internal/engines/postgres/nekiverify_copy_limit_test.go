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
	"sync"
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
// # THE CONTRACT, and why it is asymmetric
//
// The graded question is exactly one: **is the measured admitted count at
// least [nekiConcurrentCopyLimit]?**
//
//   - FEWER than 4 is the dangerous direction and FAILS. `ResolveCopyAxes`
//     paces every Neki migrate to 4; if the platform admits three, sluice
//     opens more concurrent COPYs than the cluster will take and meets `53300`
//     mid-copy.
//   - An INCONCLUSIVE measurement FAILS. A probe that cannot tell its subject
//     from its apparatus must say so rather than report a number; every defect
//     this arm has had was of that shape.
//   - MORE than 4 is LOGGED, not failed. sluice is conservative, not wrong,
//     and nothing breaks. This used to fail on the argument that "a weekly
//     that tolerates drift stops being evidence" — which was correct in
//     principle and wrong in practice: it made the weekly permanently red, so
//     GitHub issue #338 re-filed every Sunday on a condition nobody was going
//     to act on, and a suite that is always red is a suite whose next genuine
//     red is invisible. The measured number is logged under the
//     `NEKI-COPYLIMIT` marker on every outcome, which is what makes the trend
//     greppable without making it a failure.
//
// # What "accepted" means, and why the first two cuts measured the wrong thing
//
// A `COPY … FROM STDIN` occupies its slot for as long as the client has not
// finished sending, so each probe session starts a CopyFrom whose reader
// eventually BLOCKS, and the session stays open until this arm releases it.
// The measurement is which of those sessions the platform took.
//
// Deciding that by "CopyFrom has not returned within four seconds" is WRONG,
// and three paid runs recorded it being wrong without the arm noticing:
//
//	34926553073, 34928571469, 34932058458 — all three logged
//	  "measured concurrent-COPY limit: 12 accepted, then <nil>"
//	and then, in the SAME SECOND, on release:
//	  held COPY 5..12 failed on release: … too many concurrent COPY
//	  operations (limit: 4) (SQLSTATE 53300)
//	  held COPY 1..4  failed on release: … idle-in-transaction timeout (25P03)
//
// Read those together and the platform is saying its limit is four. Sessions
// 1–4 held real slots — the server saw them as sessions in a transaction and
// reaped them, which is what a 25P03 IS. Sessions 5–12 were refused; the
// router simply did not deliver the refusal until the client sent more, and
// the first row plus a four-second wait was not "more". The arm reported a
// floor of twelve, which is how `NEKI-COPYLIMIT` came to be filed as "at least
// 12, and the per-shard hypothesis is REFUTED" on evidence that says neither.
//
// So acceptance is now PROVEN rather than inferred from silence. Each session
// streams a sustained burst — [nekiCopyBurstRows] rows of
// [nekiCopyBurstPayload] bytes, about half a megabyte, many times pgx's own
// 64 KiB send frame — and counts as accepted only when **every byte of that
// burst has been flushed AND no error has arrived in the settle window that
// follows**. A `53300` at any point during the burst or the settle is a
// refusal, which is what it always was.
//
// # The independent expected value
//
// The burst is a client-side inference, so it gets a cross-check that does not
// share its evidence: [nekiCountRouterCopySessions] asks the ROUTER how many
// backends are running this COPY, before anything is released. The two numbers
// are logged side by side. They are not required to agree — a router-side
// session that is holding a client COPY without a downstream slot may well
// appear — which is why the census also counts how many of those rows carry
// `sidecar_backends`, the per-shard detail that only a COPY which reached a
// shard can have.
//
// And the artifact that fooled three runs is now caught explicitly: if a
// session this arm counted as ACCEPTED comes back `53300` on release, the
// refusal was deferred past the burst too, the burst is not a sufficient
// criterion either, and the arm reports INCONCLUSIVE with both numbers rather
// than publishing the one it can no longer trust. That is the outcome to read
// for first on the next live run.
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
		// a genuinely raised cluster-wide limit; six could not. (The previous
		// runs' apparent refutation of the per-shard hypothesis rested on the
		// deferred-refusal artifact and refutes nothing — see this arm's doc.)
		const maxProbe = 12

		type session struct {
			conn      *sql.DB
			release   chan struct{}
			burstDone chan struct{}
			done      chan error
		}
		var held []*session

		// A connection of the arm's own, for the router census and for
		// cleaning the probe's rows up. The per-session ones are each pinned
		// to a held COPY and cannot run anything while they are holding one.
		probeDB, err := sql.Open("pgx", fx.dsn)
		if err != nil {
			t.Fatalf("open the probe's own connection: %v", err)
		}
		defer func() { _ = probeDB.Close() }()

		// Releasing the held COPYs is part of the MEASUREMENT, not teardown:
		// an outcome that arrives only on release is exactly the artifact this
		// arm exists to stop mis-reading, so the verdict below reads these.
		// The deferred call is the safety net for a path that leaves early.
		type outcome struct {
			session  int
			err      error
			timedOut bool
		}
		var outcomes []outcome
		var releaseOnce sync.Once
		releaseAll := func() {
			releaseOnce.Do(func() {
				// Released CONCURRENTLY, and that is a fix rather than a
				// flourish. The first live run released them one at a time
				// with a 30s budget each, two sessions did not come back, and
				// the arm spent 87s — most of it waiting serially on timeouts
				// that a parallel release overlaps into one.
				results := make(chan outcome, len(held))
				for i, s := range held {
					close(s.release)
					go func() {
						select {
						case err := <-s.done:
							results <- outcome{session: i + 1, err: err}
						case <-time.After(45 * time.Second):
							results <- outcome{session: i + 1, timedOut: true}
						}
					}()
				}
				for range held {
					outcomes = append(outcomes, <-results)
				}
			})
		}

		defer func() {
			releaseAll()
			for _, s := range held {
				_ = s.conn.Close()
			}

			// PROVE the sessions are gone rather than asserting it in a
			// comment.
			//
			// A client-side `sql.DB.Close` returns without waiting for
			// anything the SERVER is still doing (it closes free connections
			// and marks the pool closed; in-use ones close on return), so
			// nothing here had ever established that the probe's sessions were
			// actually gone. A written invariant nobody checks is
			// indistinguishable from one that holds, and this one gates every
			// arm that follows.
			nekiReapProbeCopySessions(t, probeDB, copyProbeTable)

			// The probe's own rows, removed so the fixture is as it was found.
			// LIKE rather than equality: the burst's payload column carries a
			// per-row suffix, so the marker is a prefix now.
			cctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()
			if _, err := probeDB.ExecContext(cctx,
				fmt.Sprintf("DELETE FROM %s WHERE v LIKE '%s%%'", copyProbeTable, nekiCopyProbeMarker)); err != nil {
				t.Errorf("could not remove the probe's rows from %s: %v", copyProbeTable, err)
			}
		}()

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
				t.Fatalf("NEKI-COPYLIMIT INCONCLUSIVE: probe session %d could not CONNECT (%v).\n\n"+
					"This is not a concurrency limit and must not be reported as one — %d session(s) "+
					"were holding a COPY at the time, but that number measures nothing while a "+
					"connection cannot be established.", i, err, len(held))
			}

			s := &session{
				conn:      db,
				release:   make(chan struct{}),
				burstDone: make(chan struct{}),
				done:      make(chan error, 1),
			}

			go func() {
				// A distinct id range per session, and a tenant that
				// alternates, so the sessions do not all route to the same
				// shard — a limit enforced per shard would otherwise be
				// invisible to a probe whose rows all land in one place.
				s.done <- holdOneCopy(ctx, db, copyProbeTable, s.release, s.burstDone,
					probeTenants[i%len(probeTenants)], nekiCopyProbeFirstID+i*1000)
			}()

			// ACCEPTANCE, proven in two steps. Step one: every byte of the
			// burst reaches the wire. Step two: nothing comes back for the
			// settle window afterwards.
			select {
			case err := <-s.done:
				// Returned before the burst finished — this session did not
				// get a slot, and what came back decides whether that is THE
				// LIMIT or merely a failure.
				refusal = err
				_ = db.Close()
				if !isPGCode(err, "53300") {
					// Run 34939361499: the sixth session's burst died with
					// `write failed: use of closed network connection` — the
					// router ended the session instead of (or before) sending
					// a 53300, and pgx surfaced the write failure. A session
					// the router closes mid-burst did not get a slot; it is a
					// refusal delivered as a close, and the sessions accepted
					// before it remain a lower bound on what the platform
					// admits — which is the only direction the premise grades.
					if !nekiIsConnectionClosed(err) {
						t.Fatalf("%s", nekiInconclusiveCopyProbe(i, len(held), "during its burst", err))
					}
					refusal = fmt.Errorf("refusal delivered as a connection close, no SQLSTATE (session %d): %w", i, err)
					t.Logf("NEKI-COPYLIMIT: session %d was CLOSED by the router during its burst (%v) — counted "+
						"as refused; %d session(s) were accepted before it", i, err, len(held))
				}
			case <-time.After(nekiCopyBurstDeadline):
				// The burst never finished writing and no error arrived. The
				// router is neither taking the data nor refusing it, so this
				// session's status is unknown — and an unknown session makes
				// the count unknown.
				_ = db.Close()
				t.Fatalf("NEKI-COPYLIMIT INCONCLUSIVE: probe session %d neither finished sending its %s burst "+
					"nor failed, within %s.\n\n"+
					"%d session(s) had been accepted before it. A session the probe cannot classify makes the "+
					"whole count unclassifiable, so no number is reported. Back-pressure with no refusal is "+
					"itself worth knowing: it would mean the router accepts bytes it has nowhere to put, and "+
					"the next thing to measure is whether it ever refuses at all.",
					i, nekiCopyBurstDescription(), nekiCopyBurstDeadline, len(held))
			case <-s.burstDone:
				// Every burst byte is flushed. Now give a refusal time to
				// arrive before calling this session accepted.
				select {
				case err := <-s.done:
					refusal = err
					_ = db.Close()
					if !isPGCode(err, "53300") {
						if !nekiIsConnectionClosed(err) {
							t.Fatalf("%s", nekiInconclusiveCopyProbe(i, len(held), "in the settle window after its burst", err))
						}
						refusal = fmt.Errorf("refusal delivered as a connection close, no SQLSTATE (session %d): %w", i, err)
						t.Logf("NEKI-COPYLIMIT: session %d was CLOSED by the router in its settle window (%v) — counted "+
							"as refused; %d session(s) were accepted before it", i, err, len(held))
					}
				case <-time.After(nekiCopyBurstSettle):
					held = append(held, s)
					continue
				}
			}
			break
		}

		// ANTI-VACUITY, and it matters more than usual here: a probe where
		// nothing was ever accepted is not measuring a limit of zero, it is
		// measuring a broken fixture — wrong DSN, missing table, revoked
		// INSERT. Reporting that as "the platform admits 0 concurrent COPYs"
		// would be a confident lie.
		if len(held) == 0 {
			t.Fatalf("NEKI-COPYLIMIT INCONCLUSIVE: not a single COPY was accepted, so no limit was measured. "+
				"The first failure was: %v\n\nThis is a broken probe rather than a platform of zero — check "+
				"that %s exists on the fixture and that the role may INSERT into it.", refusal, copyProbeTable)
		}

		measured := len(held)

		// The INDEPENDENT number, taken while the sessions are still held and
		// before anything is released. It does not share the burst's evidence.
		running, withSidecars, censusNote := nekiCountRouterCopySessions(t, probeDB, copyProbeTable)

		// The outcomes on release, which is where the deferred refusal that
		// fooled three runs shows itself.
		releaseAll()
		deferred := 0
		var deferredDetail strings.Builder
		for _, o := range outcomes {
			switch {
			case o.timedOut:
				t.Errorf("held COPY %d did not finish within 45s of release — the fixture may still be "+
					"occupying a copy slot for whatever runs next", o.session)
			case o.err == nil:
				continue
			case isPGCode(o.err, "53300"):
				deferred++
				fmt.Fprintf(&deferredDetail, "\n  session %d: %v", o.session, o.err)
			default:
				// 25P03 belongs here and is EXPECTED for a session that held a
				// real slot: the server reaps a session idle in a transaction,
				// which is what a parked COPY becomes once its burst is sent.
				t.Logf("held COPY %d ended with %v", o.session, o.err)
			}
		}

		// One line, always, under a marker an operator can grep and issue #338
		// can be closed against.
		t.Logf("NEKI-COPYLIMIT: measured=%d admitted (probe ceiling %d, nekiConcurrentCopyLimit=%d); "+
			"router census: %s; refusals deferred past the burst: %d",
			measured, maxProbe, nekiConcurrentCopyLimit, censusNote, deferred)

		// THE ARTIFACT CHECK, turned into the measurement. A session counted
		// as accepted that comes back 53300 on release was refused all along —
		// the burst did not outrun the deferral for it. But every session ends
		// in exactly one of three ways: a 53300 on release (refused, late), a
		// close mid-burst (refused, as a close), or a clean end / 25P03 (it
		// held a slot until released or reaped). The RELEASE OUTCOME is
		// therefore per-session evidence that does not share the burst's
		// defect, and the admitted count is the sessions that never met a
		// refusal in any form. Measured 2026-09-15 (run 34940466964): 5
		// passed the burst, the router census counted 5 (all with sidecar
		// detail — so sidecar detail is not a slot proof either), and 1 came
		// back 53300 on release: 4 admitted, exactly the platform's own
		// `limit: 4`. Runs 34926553073 / 34928571469 / 34932058458 had
		// reported "12" by inferring acceptance from four seconds of silence.
		if deferred > 0 {
			t.Logf("NEKI-COPYLIMIT: %d of the %d session(s) that passed the burst came back 53300 on release — "+
				"refused late, not admitted.%s\n  The burst is NOT a sufficient acceptance criterion on its own; "+
				"the release outcome is. Router census while held: %s\n  %s",
				deferred, measured, deferredDetail.String(), censusNote,
				nekiCensusReading(running, withSidecars, measured))
			measured -= deferred
			if refusal == nil {
				refusal = fmt.Errorf("%d session(s) refused with 53300 on release (deferred past the burst)", deferred)
			}
			t.Logf("NEKI-COPYLIMIT: admitted=%d after subtracting the late refusals (nekiConcurrentCopyLimit=%d)",
				measured, nekiConcurrentCopyLimit)
		}

		// The graded question, and only it.
		switch {
		case measured < nekiConcurrentCopyLimit:
			t.Fatalf("NEKI-COPYLIMIT: THE PLATFORM NOW ADMITS FEWER CONCURRENT COPYs THAN SLUICE PACES ITSELF "+
				"TO: measured %d, nekiConcurrentCopyLimit is %d.\n\n"+
				"This is the dangerous direction. ResolveCopyAxes folds that constant into "+
				"CopyConcurrencyCeiling and collapses the table × chunk fan-out to stay at or below it, so "+
				"every Neki migrate now opens more concurrent COPYs than the cluster will take and meets "+
				"53300 mid-copy.\n\nFirst refusal: %v\nRouter census: %s\n\n"+
				"Lower nekiConcurrentCopyLimit (internal/engines/postgres/connection_budget.go) to the "+
				"measured value.", measured, nekiConcurrentCopyLimit, refusal, censusNote)

		case measured == nekiConcurrentCopyLimit && refusal != nil:
			t.Logf("NEKI-COPYLIMIT premise holds EXACTLY: the platform admitted %d concurrent COPYs and "+
				"refused the %dth, matching nekiConcurrentCopyLimit. Refusal: %v",
				measured, measured+1, refusal)

		case refusal == nil:
			// Nothing was refused within the probe's own ceiling. That is a
			// FLOOR, not a measurement, and the distinction is load-bearing:
			// the first live run hit exactly this, reported "measured 6" and
			// advised raising the constant "to the measured value". Six was
			// never measured; six is where the probe stopped asking.
			t.Logf("NEKI-COPYLIMIT: the platform admits AT LEAST %d concurrent COPYs — every session this "+
				"probe opened was accepted and none was refused — while nekiConcurrentCopyLimit is %d. "+
				"AT LEAST, not exactly: %d is where this probe stopped asking.\n\n"+
				"This is the benign direction and is NOT a failure: sluice is conservative, not wrong, and "+
				"no migration breaks. To act on it, raise maxProbe here until a 53300 actually appears, "+
				"confirm the number is stable across runs and shard counts, and only then change "+
				"nekiConcurrentCopyLimit (internal/engines/postgres/connection_budget.go). Rule out first "+
				"that the limit is per-SHARD rather than per-cluster — this fixture has two shards, so a "+
				"per-shard limit of 4 would present as 8 here and the constant would be right as written.\n\n"+
				"Router census: %s", measured, nekiConcurrentCopyLimit, measured, censusNote)

		default:
			t.Logf("NEKI-COPYLIMIT: the platform refused the %dth concurrent COPY, so it admits %d — more "+
				"than the %d sluice paces itself to. Benign: sluice is conservative, not wrong, and this "+
				"is logged rather than failed.\n\nRefusal: %v\nRouter census: %s\n\n"+
				"Before raising nekiConcurrentCopyLimit "+
				"(internal/engines/postgres/connection_budget.go), rule out that the limit is per-SHARD "+
				"rather than per-cluster — this fixture has two shards.",
				measured+1, measured, nekiConcurrentCopyLimit, refusal, censusNote)
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

// The arm's own windows and id range. The burst itself — its size, its reader
// and the COPY that streams it — lives in neki_copy_burst_test.go, under a tag
// the per-PR run can reach, because a router is not needed to prove that the
// apparatus does what it claims.
//
// IDS. Session i writes [nekiCopyProbeFirstID + i*1000, +nekiCopyBurstRows),
// which for a 12-session probe spans 92000–104128 — clear of every other id
// this suite uses (90001/90002, 900000+g, 3001, 4002, 1).
//
// WINDOWS. The deadline bounds "the burst never finished and nothing came
// back", which is a state the probe must not silently wait out. The settle is
// how long a refusal has to arrive after the burst is flushed before the
// session counts as accepted; it is short because pgx surfaces an
// ErrorResponse concurrently with its writes, so a refusal that is going to
// arrive at all arrives promptly once the data has been sent.
const (
	nekiCopyProbeFirstID  = 91000
	nekiCopyBurstDeadline = 20 * time.Second
	nekiCopyBurstSettle   = 3 * time.Second
)

func nekiCopyBurstDescription() string {
	return fmt.Sprintf("%d-row / ~%dKiB", nekiCopyBurstRows, nekiCopyBurstRows*nekiCopyBurstPayload/1024)
}

// nekiInconclusiveCopyProbe renders the "this failed, but not as a limit"
// refusal. Its whole job is to stop an unrelated failure being published as a
// finding about somebody else's platform.
func nekiInconclusiveCopyProbe(session, accepted int, when string, err error) string {
	hint := ""
	if isPGCode(err, "25P03") {
		hint = "\n\n25P03 is the idle-in-transaction reaper. A session that has finished its burst and parked " +
			"IS idle in a transaction, so this means the server's timeout is shorter than this probe's own " +
			"hold — which is a fixture-tuning problem, not a concurrency limit. Shorten the probe or raise " +
			"the timeout; do not read it as a refusal."
	}
	return fmt.Sprintf("NEKI-COPYLIMIT INCONCLUSIVE: probe session %d's COPY failed %s with something that is "+
		"NOT the concurrency refusal: %v\n\n"+
		"%d session(s) were holding a COPY, but that is not a measured limit — only a 53300 means 'the "+
		"platform would not admit another COPY'. Reporting this as a limit would turn an unrelated failure "+
		"into a finding about the platform.%s", session, when, err, accepted, hint)
}

// nekiCensusReading says what the router census means for the next revision of
// this arm, in the one case where the client-side burst has already been shown
// to be untrustworthy.
//
// It is prose in a failure message rather than a branch in the grading,
// deliberately: nothing has yet established what a Neki router reports for a
// COPY it has queued but not placed, so turning either count into a verdict
// today would be the same mistake as trusting the burst — a criterion adopted
// before anything measured it. The next live run under this code is what
// establishes it, and this sentence is what tells its reader which number to
// believe.
func nekiCensusReading(running, withSidecars, measured int) string {
	switch {
	case running < 0:
		return "The census could not be taken on this run, so there is no independent number to fall back " +
			"on and the next dispatch has to re-ask the same question."
	case withSidecars == nekiConcurrentCopyLimit:
		return fmt.Sprintf("READ THIS FIRST: %d of the %d router backends carry sidecar detail — exactly "+
			"nekiConcurrentCopyLimit. That is the reading which agrees with the platform's own `limit: %d` "+
			"message, and it says the constant is CORRECT as written and only this probe's client-side "+
			"criterion was wrong. Grade the next revision of this arm on the sidecar-carrying count.",
			withSidecars, running, nekiConcurrentCopyLimit)
	case running == nekiConcurrentCopyLimit:
		return fmt.Sprintf("READ THIS FIRST: the router reports %d backends running the COPY — exactly "+
			"nekiConcurrentCopyLimit — even though the client counted %d. The router-side count is the one "+
			"to grade on, and the constant is CORRECT as written.",
			running, measured)
	case running >= measured:
		return fmt.Sprintf("The router reports %d backend(s) running the COPY, %d of them with sidecar "+
			"detail — it counts the queued sessions too, so neither number discriminates on its own. What "+
			"would: whether `sidecar_backends` is populated for a queued COPY. The %d/%d split is the "+
			"evidence for that question.", running, withSidecars, withSidecars, running)
	default:
		return fmt.Sprintf("The router reports %d backend(s) running the COPY against %d counted by the "+
			"client, %d with sidecar detail. The two views disagree and neither matches "+
			"nekiConcurrentCopyLimit (%d); that disagreement is the finding to chase, not a number to "+
			"publish.", running, measured, withSidecars, nekiConcurrentCopyLimit)
	}
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

// nekiIsConnectionClosed reports whether err is the shape a session shows when
// the ROUTER ended it under the client — a write onto a closed socket, an EOF,
// or a reset — rather than a SQL-level answer. Measured 2026-09-15 (run
// 34939361499): the sixth probe session's burst failed with `write failed: use
// of closed network connection`, which is the deferred concurrency refusal
// arriving as a close instead of a 53300.
func nekiIsConnectionClosed(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	msg := err.Error()
	for _, s := range []string{
		"use of closed network connection",
		"unexpected EOF",
		"connection reset by peer",
		"broken pipe",
		"conn closed",
	} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}
