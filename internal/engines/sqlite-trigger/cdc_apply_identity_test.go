// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"database/sql"
	"strconv"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/applymarks"
	"sluicesync.dev/sluice/internal/ir"
)

// capturedAtOf reads a change-log row's captured_at straight from the file —
// the stamp's independent expected value, not the reader's rendering of it.
func capturedAtOf(t *testing.T, path string, id int64) string {
	t.Helper()
	db := openWritable(t, path)
	var s string
	if err := db.QueryRowContext(bg(), `SELECT captured_at FROM `+ChangeLogTable+` WHERE id = ?`, id).Scan(&s); err != nil {
		t.Fatalf("read captured_at of change-log id %d: %v", id, err)
	}
	return s
}

// TestCapture_ApplyIdentityIsTheChangeLogID pins ADR-0190 phase 5 on the
// sqlite-trigger reader (and so the d1-trigger engine that rides it): every
// change carries `sqlite-trigger:<change-log id>:<captured_at as stored>`,
// ordinal 1, and a re-delivery from any position reproduces exactly the
// identities the first delivery gave those changes. The independent expected
// value is each change's own position — the change-log id the reader resumes
// from — and that row's captured_at read directly from the file.
func TestCapture_ApplyIdentityIsTheChangeLogID(t *testing.T) {
	path := newSourceFile(t, `CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)`)
	if _, err := Setup(bg(), path, SetupOptions{Tables: []string{"t"}}); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	for i := 1; i <= 4; i++ {
		exec(t, path, `INSERT INTO t (id, n) VALUES (?, ?)`, i, i*10)
	}
	exec(t, path, `UPDATE t SET n = 99 WHERE id = 2`)
	exec(t, path, `DELETE FROM t WHERE id = 3`)

	deliver := func(from ir.Position, want int) map[string]ir.ApplyID {
		r, err := openCDCReader(bg(), path)
		if err != nil {
			t.Fatalf("openCDCReader: %v", err)
		}
		defer func() { _ = r.(interface{ Close() error }).Close() }()
		out := map[string]ir.ApplyID{}
		for _, c := range collect(t, r, from, want) {
			p, _, err := decodePos(c.Pos())
			if err != nil {
				t.Fatalf("decodePos: %v", err)
			}
			id := ir.ApplyIDOf(c)
			txID := EngineName + ":" + strconv.FormatInt(p.LastID, 10) + ":" + capturedAtOf(t, path, p.LastID)
			if want := (ir.ApplyID{TxID: txID, Seq: 1}); id != want {
				t.Errorf("change at change-log id %d carries identity %v; want %v", p.LastID, id, want)
			}
			out[c.Pos().Token] = id
		}
		return out
	}
	first := deliver(pos0(t), 6)
	if len(first) != 6 {
		t.Fatalf("the first delivery holds %d changes; want 6 (4 inserts, an update, a delete)", len(first))
	}
	resume, err := encodePos(sqliteTriggerPos{LastID: 3})
	if err != nil {
		t.Fatalf("encodePos: %v", err)
	}
	again := deliver(resume, 3)
	if len(again) != 3 {
		t.Fatalf("the re-delivery holds %d changes; want 3", len(again))
	}
	for tok, id := range again {
		if first[tok] != id {
			t.Errorf("the change at %s carried %v first and %v on re-delivery", tok, first[tok], id)
		}
	}
}

// TestApplyIdentity_LoweredSequenceCannotReissueAMarkedName pins the
// captured_at stamp in [triggercdc.ChangeApplyID] against the state a
// change-log id alone cannot survive: `sqlite_sequence` set to exactly the
// resume watermark — which [verifyChangeLogWatermark] accepts, and which its
// remedy once suggested — re-issues the id an ADR-0190 apply mark still
// names once the prune drains the log.
//
// The shape is the worst case the stamp exists for: a KEYLESS target (the
// capture requires a source key, the target need not have one; its mark is
// per table and no key collision is loud), a re-issued change
// byte-identical to the marked one (the digest tripwire agrees), and the
// watermark at the marked change's predecessor (where a crash on the fenced
// lane leaves it). Without the stamp both changes are `sqlite-trigger:1`,
// ordinal 1, and the tracker skips the new row at exit 0.
//
// The independent evidence is the file's: both changes sit at change-log id 1
// (the premise that the name was re-issued), and the control cell proves the
// tracker WOULD skip a change carrying the old name, so a no-skip verdict is
// the stamp's doing and not a tracker that never skips.
func TestApplyIdentity_LoweredSequenceCannotReissueAMarkedName(t *testing.T) {
	path := newSourceFile(t, `CREATE TABLE k (id INTEGER PRIMARY KEY, label TEXT)`)
	if _, err := Setup(bg(), path, SetupOptions{Tables: []string{"k"}}); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	deliverAtOne := func() ir.Change {
		r, err := openCDCReader(bg(), path)
		if err != nil {
			t.Fatalf("openCDCReader (watermark 0): %v", err)
		}
		defer func() { _ = r.(interface{ Close() error }).Close() }()
		got := collect(t, r, pos0(t), 1)
		if len(got) != 1 {
			t.Fatalf("delivered %d changes; want 1", len(got))
		}
		p, _, err := decodePos(got[0].Pos())
		if err != nil {
			t.Fatalf("decodePos: %v", err)
		}
		if p.LastID != 1 {
			t.Fatalf("the change sits at change-log id %d; want 1 — this test's premise is that the "+
				"lowered sequence re-issued the marked change's id, and it did not", p.LastID)
		}
		return got[0]
	}

	exec(t, path, `INSERT INTO k (id, label) VALUES (7, 'same')`)
	marked := deliverAtOne()
	stamp := capturedAtOf(t, path, 1)

	// The operator's reset: the prune drains the log, and the sequence is set
	// to the watermark (0). The re-capture must land in a later millisecond —
	// the premise the stamp rests on, which a real reset (an operator between
	// a crash and a restart) cannot miss; the test waits for it rather than
	// assume it.
	db := openWritable(t, path)
	for _, stmt := range []string{
		`DELETE FROM k`,
		`DELETE FROM ` + ChangeLogTable,
		`UPDATE sqlite_sequence SET seq = 0 WHERE name = '` + ChangeLogTable + `'`,
	} {
		if _, err := db.ExecContext(bg(), stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	waitPastMillisecond(t, db, stamp)
	exec(t, path, `INSERT INTO k (id, label) VALUES (7, 'same')`)
	reissued := deliverAtOne()

	const table = "k"
	keyless := applymarks.Subject{Table: table}
	if applymarks.ChangeDigest(marked, table, nil) != applymarks.ChangeDigest(reissued, table, nil) {
		t.Fatal("the two changes differ in content; this test needs them identical so only the identity can tell them apart")
	}
	var writer applymarks.Tracker
	writer.Load("s", "", nil)
	d, err := writer.Decide(marked, keyless)
	if err != nil || len(d.Marks) == 0 {
		t.Fatalf("the marked change wrote no mark on a keyless target (marks %d, err %v)", len(d.Marks), err)
	}
	decide := func(c ir.Change) (applymarks.Decision, error) {
		var resumed applymarks.Tracker
		resumed.Load("s", "", d.Marks)
		return resumed.Decide(c, keyless)
	}

	// Control: the tracker skips a change carrying the mark's own name.
	sameName := reissued.(ir.Insert)
	sameName.ApplyID = ir.ApplyIDOf(marked)
	if got, err := decide(sameName); err != nil || !got.Skip {
		t.Fatalf("control: the tracker did not skip a change carrying the mark's own name (skip %v, err %v)", got.Skip, err)
	}

	if ir.ApplyIDOf(marked) == ir.ApplyIDOf(reissued) {
		t.Errorf("the re-issued change carries the marked change's identity %v; a lowered "+
			"sqlite_sequence can re-issue a name a durable mark still holds", ir.ApplyIDOf(reissued))
	}
	if got, _ := decide(reissued); got.Skip {
		t.Fatalf("SILENT SKIP: the change re-issued at the marked change's id was skipped on a keyless "+
			"target (identity %v); it must apply or refuse", ir.ApplyIDOf(reissued))
	}
}

// waitPastMillisecond blocks until SQLite's own clock — the one the capture
// trigger stamps captured_at with — reads later than stamp.
func waitPastMillisecond(t *testing.T, db *sql.DB, stamp string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var now string
		if err := db.QueryRowContext(bg(), `SELECT strftime('%Y-%m-%d %H:%M:%f', 'now')`).Scan(&now); err != nil {
			t.Fatalf("read the SQLite clock: %v", err)
		}
		if now > stamp {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the SQLite clock did not pass %s within 5s (it reads %s)", stamp, now)
		}
		time.Sleep(time.Millisecond)
	}
}
