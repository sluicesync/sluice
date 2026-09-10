//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// A0909-STOP-1 end to end on real PostgreSQL: a cold start STOPPED
// after its copy committed but before its CDC anchor was written must
// be resumable, and must refuse in every case it cannot prove.
//
// # The stop is deterministic, and that is the point
//
// The pin this replaces polled the target's row count every 1 ms and
// cancelled when the copy landed, which is how it spent two releases
// green by race: whenever the cancel happened to land in the anchor
// window it graded a path that was already protected, and the gap it
// existed to cover went untested (audit A0909-STOP-1, "how it was
// missed"). Here the stop is injected at a known point — the engine's
// per-table index-build-start seam — so every run lands in the window
// under test: the copy has committed, the index has NOT been built,
// and no anchor exists.
//
// # What the resume has to prove, and how each is witnessed
//
//   - the copy is not re-run → a row DELETED from the target during the
//     stop stays deleted (a re-copy would put it back);
//   - the indexes are built → read back from pg_indexes on the target;
//   - the anchor is written → the sluice_cdc_state row exists;
//   - nothing was lost → a row committed on the SOURCE while sluice was
//     stopped arrives on the target. Without that last one the test
//     proves only that something ran.

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/postgres"
)

// resumeFixtureRows is the copied row count. Small on purpose: the
// stop's landing point comes from the injected failpoint, not from the
// copy being slow, so there is nothing to buy with a big table.
const resumeFixtureRows = 40

// stoppedColdStart is the state every pin in this file starts from: a
// cold start whose copy committed and whose index build was
// interrupted, leaving the slot alive and no CDC anchor.
type stoppedColdStart struct {
	src, tgt string
	streamID string
	newRun   func() *Streamer
}

// stopColdStartInIndexBuild runs a cold start and cancels it the
// instant the target's index build starts, then asserts the state that
// leaves behind. Every pin in this file begins here.
func stopColdStartInIndexBuild(t *testing.T, streamID string) stoppedColdStart {
	t.Helper()
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	src, tgt, cleanup := startPostgresLogical(t)
	t.Cleanup(cleanup)

	// REPLICA IDENTITY FULL because one case below re-runs WITH a
	// `--where`, and the filtered-sync preflight refuses a predicate on
	// a table without full before-images — it runs BEFORE the resume
	// gate, so without this the copy-shape arm would never execute and
	// the case would grade a different refusal. (The first cut did
	// exactly that: it passed the run-refused check while asserting
	// nothing about the gate it exists for.)
	applyDDL(t, src, `CREATE TABLE resume_t (id BIGINT PRIMARY KEY, v INT NOT NULL);
		ALTER TABLE resume_t REPLICA IDENTITY FULL;
		CREATE INDEX resume_t_v_idx ON resume_t (v);
		INSERT INTO resume_t (id, v) SELECT g, g FROM generate_series(1, 40) g;`)

	newRun := func() *Streamer {
		return &Streamer{
			Source:    pgEng,
			Target:    pgEng,
			SourceDSN: src,
			TargetDSN: tgt,
			StreamID:  streamID,
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// THE FAILPOINT: fire once, at the moment the first index build
	// starts on the target. By then the copy pool has marked its table
	// complete (the table reaches the index builder only after its copy
	// returns), and coldStartBeginCDC is several phases away.
	fired := make(chan struct{})
	var once bool
	restore := postgres.SetIndexBuildStartObserverForTest(func(string) {
		if once {
			return
		}
		once = true
		close(fired)
		cancel()
	})

	// POSITIVE CONTROL for the no-re-copy witness the headline pin
	// uses. That witness asserts the copy seam fires ZERO times during
	// a resume, which proves nothing unless the seam fires when a copy
	// DOES run. Record run 1's copies here and require at least one.
	var run1Copied []string
	prevCopyObserver := onTableCopiedObserver
	onTableCopiedObserver = func(table string) { run1Copied = append(run1Copied, table) }

	errCh := make(chan error, 1)
	go func() { errCh <- newRun().Run(ctx) }()

	select {
	case <-fired:
	case err := <-errCh:
		restore()
		t.Fatalf("the cold start returned (%v) without ever starting an index build; the pin never entered "+
			"the window it exists to cover", err)
	case <-time.After(3 * time.Minute):
		restore()
		t.Fatal("no index build started within 3m")
	}
	var runErr error
	select {
	case runErr = <-errCh:
	case <-time.After(2 * time.Minute):
		restore()
		t.Fatal("the cold start did not return after its index build was cancelled")
	}
	// Restore BEFORE any resume: leaving the failpoint armed would cancel
	// the resume's own index build too.
	restore()
	onTableCopiedObserver = prevCopyObserver
	if runErr == nil {
		t.Fatal("the cancelled cold start returned nil; the stop did not interrupt it")
	}
	if len(run1Copied) == 0 {
		t.Fatal("the copy-completion seam did not fire during the INTERRUPTED cold start, so the headline " +
			"pin's 'the copy did not run' witness would be satisfied by a dead seam rather than by a resume")
	}

	// The window this pin requires, asserted rather than assumed.
	if got := pollRowCount(tgt, "resume_t"); got != resumeFixtureRows {
		t.Fatalf("the copy had not committed all %d rows when the index build started (target has %d); "+
			"the failpoint is in the wrong place", resumeFixtureRows, got)
	}
	if pgCDCStateRowExists(t, tgt, streamID) {
		t.Fatal("a CDC anchor row already exists; the stop landed AFTER the anchor write, which is the " +
			"already-protected window rather than the one under test")
	}
	if !pgSlotExists(t, src, "sluice_slot") {
		t.Fatalf("the stop dropped sluice_slot; the resume has nothing to anchor on. Run error: %v", runErr)
	}
	if anchor := recordedSnapshotAnchor(t, tgt, streamID); anchor == "" {
		t.Fatal("the interrupted cold start recorded NO snapshot anchor, so nothing could resume from it")
	}
	t.Logf("stopped in the index-build window: run err = %v", runErr)
	return stoppedColdStart{src: src, tgt: tgt, streamID: streamID, newRun: newRun}
}

// recordedSnapshotAnchor reads the anchor the cold start persisted.
func recordedSnapshotAnchor(t *testing.T, dsn, streamID string) string {
	t.Helper()
	if !pgQueryOne[bool](t, dsn, "SELECT to_regclass('sluice_migrate_state') IS NOT NULL") {
		return ""
	}
	return pgQueryOne[string](t, dsn,
		"SELECT COALESCE(snapshot_anchor, '') FROM sluice_migrate_state WHERE migration_id = $1",
		"sync-"+streamID)
}

func recordedPhase(t *testing.T, dsn, streamID string) string {
	t.Helper()
	return pgQueryOne[string](t, dsn,
		"SELECT phase FROM sluice_migrate_state WHERE migration_id = $1", "sync-"+streamID)
}

func execOnTarget(t *testing.T, dsn, stmt string, args ...any) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, stmt, args...); err != nil {
		t.Fatalf("exec %q: %v", stmt, err)
	}
}

func indexExists(t *testing.T, dsn, index string) bool {
	t.Helper()
	return pgQueryOne[bool](t, dsn,
		"SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = $1)", index)
}

// TestStreamer_ColdStartStoppedInIndexBuild_PG_ResumesWithoutRecopy is
// the headline pin.
func TestStreamer_ColdStartStoppedInIndexBuild_PG_ResumesWithoutRecopy(t *testing.T) {
	st := stopColdStartInIndexBuild(t, "coldstart-resume-happy")

	if indexExists(t, st.tgt, "resume_t_v_idx") {
		t.Fatal("the secondary index already exists; the stop landed after the build, so the resume's " +
			"index assertion would be vacuous")
	}
	if phase := recordedPhase(t, st.tgt, st.streamID); phase != "indexes" {
		t.Logf("recorded phase after the interrupted index build: %q", phase)
	}

	// Witness 1 (no re-copy): the copy pool's own per-table completion
	// seam must not fire at all during the resumed run.
	//
	// The first cut of this witness DELETEd a row from the target and
	// asserted it stayed deleted — which works only because the
	// target-row floor is a non-empty check rather than an exact count,
	// so the witness depended on a gate being loose, and would break the
	// day that floor is tightened. Observing the copy directly says the
	// same thing without tampering with the target.
	var copied []string
	prevObserver := onTableCopiedObserver
	onTableCopiedObserver = func(table string) { copied = append(copied, table) }
	t.Cleanup(func() { onTableCopiedObserver = prevObserver })

	// Witness 2 (losslessness): a row committed on the SOURCE while
	// sluice is stopped. It is after the snapshot's consistent point, so
	// the resumed stream must deliver it.
	applyDDL(t, st.src, `INSERT INTO resume_t (id, v) VALUES (1001, 1001);`)

	logs := captureSlog(t)
	resumeCtx, resumeCancel := context.WithCancel(context.Background())
	defer resumeCancel()
	resumeErr := make(chan error, 1)
	go func() { resumeErr <- st.newRun().Run(resumeCtx) }()

	// The live change proves CDC is running from the recorded anchor:
	// the copied rows plus the one committed during the stop.
	const wantRows = resumeFixtureRows + 1
	if !waitForExactRowCount(st.tgt, "resume_t", wantRows, 3*time.Minute) {
		select {
		case err := <-resumeErr:
			t.Fatalf("the resumed run exited instead of streaming: %v\nlogs:\n%s", err, logs.String())
		default:
			t.Fatalf("the resumed stream never reached %d rows (target has %d)\nlogs:\n%s",
				wantRows, pollRowCount(st.tgt, "resume_t"), logs.String())
		}
	}

	if !pgQueryOne[bool](t, st.tgt, "SELECT EXISTS (SELECT 1 FROM resume_t WHERE id = 1001)") {
		t.Error("the row committed on the SOURCE during the stop never arrived; the resume is not lossless, " +
			"which is the whole reason it may skip the copy")
	}
	if len(copied) != 0 {
		t.Errorf("the resumed run COPIED %v; the copy pool must not run at all — the resume did not resume, "+
			"it repeated the work it exists to skip", copied)
	}
	if !indexExists(t, st.tgt, "resume_t_v_idx") {
		t.Error("the secondary index is still missing after the resume; the interrupted index build was " +
			"never finished, so the resume left the target unindexed at exit 0")
	}
	if !pgCDCStateRowExists(t, st.tgt, st.streamID) {
		t.Error("the resumed run wrote no CDC anchor row; a stop now would be unresumable all over again")
	}
	if got := logs.String(); !strings.Contains(got, coldStartResumedMarker) {
		t.Errorf("the resumed run never logged %s, so an operator has no way to tell a resume from a fresh "+
			"copy:\n%s", coldStartResumedMarker, got)
	}

	resumeCancel()
	select {
	case <-resumeErr:
	case <-time.After(60 * time.Second):
		t.Error("the resumed Run did not return after ctx cancel")
	}
}

// TestStreamer_ColdStartResume_PG_RefusesWithoutProof pins the three
// NEGATIVES: each removes exactly one piece of the evidence the resume
// requires and demands the old behaviour back.
//
// Two of them edit the recorded state directly rather than racing a
// second stop into the right window. That is deliberate: the states
// they describe (a copy that stopped mid-table, a header written by a
// binary with no anchor column) are precisely defined by their rows,
// and reproducing them by timing would put the pin back in the
// green-by-race class this file exists to leave. The rows are real,
// written by a real run, read back through the real store.
func TestStreamer_ColdStartResume_PG_RefusesWithoutProof(t *testing.T) {
	t.Run("a copy that did not finish", func(t *testing.T) {
		st := stopColdStartInIndexBuild(t, "coldstart-resume-partial")
		// The shape a stop DURING the bulk copy leaves: the table's
		// progress row is not complete.
		execOnTarget(t, st.tgt,
			"UPDATE sluice_migrate_table_progress SET progress = '\"in_progress\"' WHERE migration_id = $1",
			"sync-"+st.streamID)
		requireResumeRefused(t, st, "an unfinished copy")
	})

	t.Run("a header written before the anchor column existed", func(t *testing.T) {
		st := stopColdStartInIndexBuild(t, "coldstart-resume-noanchor")
		execOnTarget(t, st.tgt,
			"UPDATE sluice_migrate_state SET snapshot_anchor = NULL WHERE migration_id = $1",
			"sync-"+st.streamID)
		requireResumeRefused(t, st, "a row carrying no anchor")
	})

	t.Run("the re-run WIDENS --where", func(t *testing.T) {
		// The C-1 silent-loss path: run 1 copied the rows its predicate
		// selected; a re-run without the predicate would inherit them as
		// if they were everything, and CDC can never backfill the rest.
		// Refused rather than declined, so this asserts the marker too.
		st := stopColdStartInIndexBuild(t, "coldstart-resume-widened")
		widened := st.newRun
		st.newRun = func() *Streamer {
			s := widened()
			s.RowFilters = map[string]string{"resume_t": "v > 0"}
			return s
		}
		err := runResumeAndCaptureRefusal(t, st)
		if !strings.Contains(err.Error(), coldStartShapeChangedMarker) {
			t.Errorf("a changed --where did not refuse under %s: %v", coldStartShapeChangedMarker, err)
		}
		if !strings.Contains(err.Error(), "where") {
			t.Errorf("the refusal does not name which input changed: %v", err)
		}
	})

	t.Run("the re-run changes --type-override", func(t *testing.T) {
		// A second aspect, chosen because NO preflight stands in front
		// of it: if the `--where` case above ever starts being caught by
		// something earlier again, this one still exercises the gate.
		// Run 1 created the target column from the source type; a re-run
		// declaring a different one describes a table it will not
		// re-create.
		st := stopColdStartInIndexBuild(t, "coldstart-resume-retyped")
		base := st.newRun
		st.newRun = func() *Streamer {
			s := base()
			s.Mappings = []config.Mapping{{Table: "resume_t", Column: "v", TargetType: "TEXT"}}
			return s
		}
		err := runResumeAndCaptureRefusal(t, st)
		if !strings.Contains(err.Error(), coldStartShapeChangedMarker) {
			t.Errorf("a changed --type-override did not refuse under %s: %v", coldStartShapeChangedMarker, err)
		}
		if !strings.Contains(err.Error(), "types") {
			t.Errorf("the refusal does not name which input changed: %v", err)
		}
	})

	t.Run("the target was TRUNCATEd", func(t *testing.T) {
		// The C-2 path, and the one every bookkeeping check passes: the
		// control rows, the phase, the anchor and even the indexes all
		// survive a TRUNCATE untouched.
		st := stopColdStartInIndexBuild(t, "coldstart-resume-truncated")
		execOnTarget(t, st.tgt, "TRUNCATE TABLE resume_t")
		err := runResumeAndCaptureRefusal(t, st)
		if !strings.Contains(err.Error(), coldStartTargetEmptiedMarker) {
			t.Errorf("an emptied target did not refuse under %s: %v", coldStartTargetEmptiedMarker, err)
		}
		if !strings.Contains(err.Error(), "resume_t") {
			t.Errorf("the refusal does not name the emptied table: %v", err)
		}
		if got := pollRowCount(st.tgt, "resume_t"); got != 0 {
			t.Errorf("the refused run put %d rows back on the target; a refusal must change nothing", got)
		}
	})

	t.Run("a phase from before the copy", func(t *testing.T) {
		st := stopColdStartInIndexBuild(t, "coldstart-resume-earlyphase")
		// NOT bulk_copy: that is what a real stop in the index window
		// records (measured), so it is an ALLOWED floor and the per-table
		// rows decide. `tables` is a phase from which no copy can have
		// finished, so however the rows read, the resume must refuse.
		execOnTarget(t, st.tgt,
			"UPDATE sluice_migrate_state SET phase = 'tables' WHERE migration_id = $1",
			"sync-"+st.streamID)
		requireResumeRefused(t, st, "phase tables")
	})
}

// TestStreamer_ColdStartResume_PG_RefusesWhenTheSlotMoved is the
// silent-loss negative and the one the gate exists for: something
// consumed the slot between the stop and the re-run, so the changes
// between the anchor and the slot's position are gone. Resuming would
// skip them without a word.
func TestStreamer_ColdStartResume_PG_RefusesWhenTheSlotMoved(t *testing.T) {
	st := stopColdStartInIndexBuild(t, "coldstart-resume-moved")
	recorded := recordedSnapshotAnchor(t, st.tgt, st.streamID)

	// Someone else drains the slot — a peer consumer, or an operator's
	// pg_logical_slot_get_changes.
	applyDDL(t, st.src, `INSERT INTO resume_t (id, v) VALUES (2001, 2001);`)
	db, err := sql.Open("pgx", st.src)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(
		// The BINARY variant: pgoutput is a binary output plugin and the
		// textual function refuses it outright (SQLSTATE 0A000).
		`SELECT 1 FROM pg_logical_slot_get_binary_changes('sluice_slot', NULL, NULL,
			'proto_version', '1', 'publication_names', 'sluice_pub')`,
	); err != nil {
		t.Fatalf("consume the slot: %v", err)
	}
	moved := pgQueryOne[string](t, st.src,
		"SELECT COALESCE(confirmed_flush_lsn::text, '') FROM pg_replication_slots WHERE slot_name = 'sluice_slot'")
	if strings.Contains(recorded, moved) {
		t.Skipf("consuming the slot left confirmed_flush_lsn at %s, so the moved-anchor arm cannot be "+
			"exercised on this server", moved)
	}

	logs := captureSlog(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	err = st.newRun().Run(ctx)
	if err == nil {
		t.Fatalf("the re-run SUCCEEDED after the slot was consumed past the recorded anchor — the changes "+
			"in between are gone and it did not say so\nlogs:\n%s", logs.String())
	}
	if !strings.Contains(err.Error(), coldStartAnchorMovedMarker) {
		t.Errorf("the refusal does not carry %s: %v", coldStartAnchorMovedMarker, err)
	}
	if !strings.Contains(err.Error(), moved) {
		t.Errorf("the refusal does not name the slot's CURRENT position (%s), which is half the evidence an "+
			"operator needs: %v", moved, err)
	}
	// The recorded anchor is a JSON token; the refusal names its LSN.
	if lsn := lsnFromToken(t, recorded); !strings.Contains(err.Error(), lsn) {
		t.Errorf("the refusal does not name the RECORDED anchor (%s): %v", lsn, err)
	}
	if pgCDCStateRowExists(t, st.tgt, st.streamID) {
		t.Error("the refused run wrote a CDC anchor row; a refusal must change nothing")
	}
}

// lsnFromToken extracts the LSN from a Postgres position token without
// importing the engine's private decoder.
func lsnFromToken(t *testing.T, token string) string {
	t.Helper()
	const key = `"lsn":"`
	i := strings.Index(token, key)
	if i < 0 {
		t.Fatalf("position token %q carries no lsn field", token)
	}
	rest := token[i+len(key):]
	j := strings.Index(rest, `"`)
	if j < 0 {
		t.Fatalf("position token %q has an unterminated lsn field", token)
	}
	return rest[:j]
}

// runResumeAndCaptureRefusal re-runs the stream and requires it to
// FAIL, returning the error so the caller can grade its marker. Used
// by the cases that are a deliberate refusal (a changed copy shape, an
// emptied target) rather than a fall-through to the cold start.
func runResumeAndCaptureRefusal(t *testing.T, st stoppedColdStart) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	logs := captureSlog(t)
	err := st.newRun().Run(ctx)
	if err == nil {
		t.Fatalf("the re-run SUCCEEDED where it had to refuse\nlogs:\n%s", logs.String())
	}
	if strings.Contains(logs.String(), coldStartResumedMarker) {
		t.Errorf("the run announced %s before refusing", coldStartResumedMarker)
	}
	if pgCDCStateRowExists(t, st.tgt, st.streamID) {
		t.Error("the refused run wrote a CDC anchor row; a refusal must change nothing")
	}
	return err
}

// requireResumeRefused re-runs the stream and requires the pre-resume
// behaviour: a loud refusal naming the slot, no CDC anchor, and no
// silent re-copy over the target.
func requireResumeRefused(t *testing.T, st stoppedColdStart, why string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	logs := captureSlog(t)
	err := st.newRun().Run(ctx)
	if err == nil {
		t.Fatalf("with %s, the re-run SUCCEEDED; it must fall back to the pre-resume refusal rather than "+
			"resume on evidence it does not have\nlogs:\n%s", why, logs.String())
	}
	if !strings.Contains(err.Error(), "sluice_slot") {
		t.Errorf("with %s, the re-run failed without naming the slot the operator has to deal with: %v", why, err)
	}
	if strings.Contains(logs.String(), coldStartResumedMarker) {
		t.Errorf("with %s, the run announced %s before failing", why, coldStartResumedMarker)
	}
	if pgCDCStateRowExists(t, st.tgt, st.streamID) {
		t.Errorf("with %s, the refused run wrote a CDC anchor row", why)
	}
}
