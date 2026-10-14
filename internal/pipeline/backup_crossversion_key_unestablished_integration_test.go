//go:build integration && crossversion

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The cross-version gate for FormatVersionKeyCollationUnestablished
// (KEY-COLLATION-PG-LOOSENED-REPLAY), run by the same extended-suites.yml
// `crossversion` leg as TestBackup_CrossVersionChainCompat.
//
// WHY A FIFTH SUITE, NOT A CELL OF THE KEY-COLLATION-DELTA ONE. The two tiers
// need different old binaries. Version 13's harm needs a release whose diff
// cannot see a collation change at all (v0.158.x); version 14's needs the
// newest release below 14 (v0.159.3), whose diff CAN see a change but whose
// guard arms nothing for a key recorded under its parent's own collation name
// with no determinism — the shape the v0.160.0 Postgres reader stamps when a
// key column's pg_attribute row is newer than the rows it describes. The
// segment carries no schema delta and no collation-name change, so neither
// 13 stamp fires: only the history-based 14 stamp keeps v0.159.x out.
//
// THE FIXTURE IS BEHAVIOURAL. OLD is CROSSVER_BELOW_KEY_UNESTABLISHED_* —
// the newest release below the tier (see THE FOURTH AXIS in
// scripts/crossversion-build.sh), built from its tag. Every assertion is
// what a binary DOES with a store, graded by the SOURCE's own rows read by
// id:hex(key):v — never by a manifest field alone.
//
//	cell A  refusal      NEW writes a chain whose `backup incremental`,
//	                       captured AFTER the window, replays a key tightened
//	                       ci → "C", 'A' inserted beside 'a', moved to 'A2',
//	                       and the key loosened back to ci → the reader
//	                       records {ci, not established} and the segment is
//	                       stamped 14
//	                       → OLD `restore` and `backup verify` REFUSE at the
//	                         version ceiling, target untouched
//	                       → NEW `restore` refuses KEY-COLLATION-MERGE with
//	                         nothing merged onto the target
//	cell B  control      NEW writes a chain whose window is plain DML → no
//	                       segment above OLD's ceiling → OLD restores it
//	                       exactly (the stamp is proportional)
//	cell C  premise      OLD writes cell A's chain itself and restores it at
//	                       exit 0 with row 1 MERGED away — the silent loss
//	                       the tier refuses older binaries over

package pipeline

import (
	"os"
	"strconv"
	"strings"
	"testing"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// kuxvLoadBinaries reads this suite's slice of the build script's contract.
// Every failure path is a t.Fatalf, never a skip.
func kuxvLoadBinaries(t *testing.T) kcxvBinaries {
	t.Helper()
	get := func(k string) string {
		v := os.Getenv(k)
		if v == "" {
			t.Fatalf("%s is unset — run `bash scripts/crossversion-build.sh` and export the KEY=VALUE lines it prints, "+
				"then re-run with -tags='integration crossversion'. (A FAILURE, not a skip, on purpose.)", k)
		}
		return v
	}
	getInt := func(k string) int {
		n, err := strconv.Atoi(get(k))
		if err != nil {
			t.Fatalf("%s=%q is not an integer: %v", k, os.Getenv(k), err)
		}
		return n
	}
	b := kcxvBinaries{
		oldBin:    get("CROSSVER_BELOW_KEY_UNESTABLISHED_BIN"),
		newBin:    get("CROSSVER_NEW_BIN"),
		oldTag:    get("CROSSVER_BELOW_KEY_UNESTABLISHED_TAG"),
		oldFormat: getInt("CROSSVER_BELOW_KEY_UNESTABLISHED_FORMAT"),
		newFormat: getInt("CROSSVER_NEW_FORMAT"),
		tier:      getInt("CROSSVER_BELOW_KEY_UNESTABLISHED_TIER"),
	}
	if b.tier != irbackup.FormatVersionKeyCollationUnestablished {
		t.Fatalf("CROSSVER_BELOW_KEY_UNESTABLISHED_TIER=%d but this tree's FormatVersionKeyCollationUnestablished is %d",
			b.tier, irbackup.FormatVersionKeyCollationUnestablished)
	}
	if b.oldFormat >= b.tier {
		t.Fatalf("OLD %s stamps BackupFormatVersion=%d, at or above the not-established-key tier %d — it would READ "+
			"the stamped segment and cell A would be vacuous", b.oldTag, b.oldFormat, b.tier)
	}
	if b.newFormat < b.tier {
		t.Fatalf("NEW stamps BackupFormatVersion=%d, below the tier %d it is supposed to write", b.newFormat, b.tier)
	}
	for _, p := range []string{b.oldBin, b.newBin} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("binary %q: %v", p, err)
		}
	}
	t.Logf("not-established-key gate: OLD=%s (%s, BackupFormatVersion=%d)  NEW=%s (BackupFormatVersion=%d)  tier=%d",
		b.oldBin, b.oldTag, b.oldFormat, b.newBin, b.newFormat, b.tier)
	return b
}

// kuxvLoosenedWindow is KEY-COLLATION-PG-LOOSENED-REPLAY: the key tightens,
// a row only the tighter collation keeps apart from 'a' arrives and moves,
// and the key loosens back. Each statement is its own transaction, so every
// row event is older than the loosening's pg_attribute row — the catalog a
// later `backup incremental` reads says ci, and the reader must record it
// as not established for those rows.
var kuxvLoosenedWindow = []string{
	`ALTER TABLE kd ALTER COLUMN k TYPE text COLLATE "C"`,
	`INSERT INTO kd VALUES ('A', 99, 99, 'z')`,
	`UPDATE kd SET k = 'A2' WHERE id = 99`,
	`ALTER TABLE kd ALTER COLUMN k TYPE text COLLATE ci`,
}

// kuxvPlainWindow is DML only: no catalog row of kd is newer than its rows.
var kuxvPlainWindow = []string{
	`INSERT INTO kd VALUES ('c', 3, 3, 'w')`,
	`UPDATE kd SET v = v + 10 WHERE id = 1`,
}

// kuxvChain writes a chain with bin: a full, then window applied statement by
// statement, then ONE `backup incremental` that replays it after the fact.
func kuxvChain(t *testing.T, bin, srcDSN, dir, slot string, window []string) {
	t.Helper()
	xvMustExec(t, "backup full", bin, "backup", "full",
		"--source-driver", "postgres", "--source", srcDSN, "--output-dir", dir,
		"--chain-slot", "--slot-name", slot)
	for _, s := range window {
		applyDDL(t, srcDSN, s)
	}
	xvMustExec(t, "backup incremental", bin, "backup", "incremental",
		"--source-driver", "postgres", "--source", srcDSN, "--output-dir", dir,
		"--slot-name", slot, "--window", xvIncrementalWindow, "--max-changes", "0")
}

// kuxvLineWith is the first line of out containing marker, for the log.
func kuxvLineWith(out, marker string) string {
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, marker) {
			return strings.TrimSpace(line)
		}
	}
	return ""
}

func TestBackup_CrossVersionKeyCollationUnestablished(t *testing.T) {
	bins := kuxvLoadBinaries(t)
	adminDSN, _, cleanup := startPostgresLogical(t)
	defer cleanup()
	ledger := newXVLedger(t, "A-refusal", "B-control", "C-old-merges")

	t.Run("cellA_unestablished_key_segment_refused_by_OLD", func(t *testing.T) {
		src := xvCreateDB(t, adminDSN, "kuxv_a_src")
		applyDDL(t, src, kcxvDDL)
		dir := t.TempDir()
		kuxvChain(t, bins.newBin, src, dir, "kuxv_a", kuxvLoosenedWindow)

		// OLD first, so a missing stamp shows as what it costs (a restore at
		// exit 0 with the merge) rather than only as a version number.
		oldTgt := kcxvTarget(t, adminDSN, "kuxv_a_old_tgt")
		out, err := xvExec(t, bins.oldBin, "restore", "--from-dir", dir, "--target-driver", "postgres", "--target", oldTgt)
		if err == nil {
			t.Fatalf("cell A (OLD restore): OLD %s RESTORED a chain whose window replays a key collation that was not "+
				"established for its rows, at exit 0\nsource: %s\ntarget: %s", bins.oldTag, kcxvRows(t, src), kcxvRows(t, oldTgt))
		}
		if !strings.Contains(out, xvFormatVersionRefusal) {
			t.Fatalf("cell A (OLD restore): want the %q refusal; err=%v\n--- output ---\n%s", xvFormatVersionRefusal, err, out)
		}
		_, segs := xvChainVersions(t, dir)
		if len(segs) != 1 || segs[0] != bins.tier {
			t.Fatalf("NEW stamped the segments %v; want one, at the tier %d", segs, bins.tier)
		}
		if n := xvPublicRelationCount(t, oldTgt); n != 0 {
			t.Fatalf("cell A (OLD restore): refused, but the target already carries %d relation(s) — the refusal must sit at the manifest preflight", n)
		}
		t.Logf("cell A: segment versions %v; OLD restore refused: %s", segs, kuxvLineWith(out, xvFormatVersionRefusal))
		out, err = xvExec(t, bins.oldBin, "backup", "verify", "--from-dir", dir)
		if err == nil || !strings.Contains(out, xvFormatVersionRefusal) {
			t.Fatalf("cell A (OLD backup verify): want the %q refusal; err=%v\n--- output ---\n%s", xvFormatVersionRefusal, err, out)
		}

		// NEW reads the segment and refuses the merge itself: 'A' was written
		// under "C", the restored key compares under ci, and the history says
		// ci was not established for that row.
		newTgt := kcxvTarget(t, adminDSN, "kuxv_a_new_tgt")
		out, err = xvExec(t, bins.newBin, "restore", "--from-dir", dir, "--target-driver", "postgres", "--target", newTgt)
		t.Logf("cell A (NEW restore): err=%v\nsource: %s\ntarget: %s", err, kcxvRows(t, src), kcxvRows(t, newTgt))
		if err == nil || !strings.Contains(out, "KEY-COLLATION-MERGE") {
			t.Fatalf("cell A (NEW restore): want a KEY-COLLATION-MERGE refusal; err=%v\n--- output ---\n%s", err, out)
		}
		t.Logf("cell A (NEW restore) refused: %s", kuxvLineWith(out, "KEY-COLLATION-MERGE"))
		kcfbGrade(t, "cell A (NEW restore)", twfbDB{engine: "postgres", dsn: src}, twfbDB{engine: "postgres", dsn: newTgt}, err)
		ledger.report(t, "A-refusal")
	})

	t.Run("cellB_plain_window_stays_readable_by_OLD", func(t *testing.T) {
		src := xvCreateDB(t, adminDSN, "kuxv_b_src")
		applyDDL(t, src, kcxvDDL)
		dir := t.TempDir()
		kuxvChain(t, bins.newBin, src, dir, "kuxv_b", kuxvPlainWindow)

		root, segs := xvChainVersions(t, dir)
		for _, v := range append([]int{root}, segs...) {
			if v > bins.oldFormat {
				t.Fatalf("NEW stamped a plain DML chain root=%d segments=%v, above OLD's %d — the stamp is not "+
					"proportional", root, segs, bins.oldFormat)
			}
		}
		t.Logf("cell B: root=%d segments=%v", root, segs)
		tgt := kcxvTarget(t, adminDSN, "kuxv_b_tgt")
		xvMustExec(t, "cell B (OLD restore)", bins.oldBin, "restore", "--from-dir", dir, "--target-driver", "postgres", "--target", tgt)
		if want, got := kcxvRows(t, src), kcxvRows(t, tgt); want != got {
			t.Fatalf("cell B (OLD restore): the restore is not the source\nsource: %s\ntarget: %s", want, got)
		}
		ledger.report(t, "B-control")
	})

	// Cell C — the premise the tier rests on, measured rather than assumed:
	// OLD restores its own loosened-replay chain at EXIT 0 and loses row 1.
	// If OLD ever restores this exactly, cell A's refusal protects against
	// nothing and the tier's cost — older binaries locked out of every such
	// segment — needs re-deciding.
	t.Run("cellC_OLD_silently_merges_its_own_chain", func(t *testing.T) {
		src := xvCreateDB(t, adminDSN, "kuxv_c_src")
		applyDDL(t, src, kcxvDDL)
		dir := t.TempDir()
		kuxvChain(t, bins.oldBin, src, dir, "kuxv_c", kuxvLoosenedWindow)
		tgt := kcxvTarget(t, adminDSN, "kuxv_c_tgt")
		xvMustExec(t, "cell C (OLD restore)", bins.oldBin, "restore", "--from-dir", dir, "--target-driver", "postgres", "--target", tgt)
		want, got := kcxvRows(t, src), kcxvRows(t, tgt)
		if want == got {
			t.Fatalf("cell C: OLD %s restored its own loosened-replay chain EXACTLY at exit 0 — the silent merge the "+
				"not-established-key tier refuses older binaries over is not present in it\nrows: %s", bins.oldTag, got)
		}
		t.Logf("cell C: OLD %s restored its own chain at exit 0, silently different\nsource: %s\ntarget: %s", bins.oldTag, want, got)
		ledger.report(t, "C-old-merges")
	})
}
