//go:build integration && crossversion

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The cross-version gate for FormatVersionExactNumbers (2026-09-25), run
// by the same extended-suites.yml `crossversion` leg as
// TestBackup_CrossVersionChainCompat.
//
// WHY A THIRD SUITE. Version 12 is reachable by no `backup full` flag: it
// is stamped on a postgres-trigger CDC SEGMENT whose change chunks carried
// an exact-text number (an unconstrained numeric, a jsonb number, a float
// or an element of those). Through v0.156.3 every binary decoded such a
// number out of the chunk as a float64 — 10.50 restored as 10.5, a long
// numeric lost its tail, 1e-400 restored as 0 — and the chunk bytes did
// not change with the fix, so without the stamp an older binary would
// restore a new chain with the same silent rounding.
//
// THE FIXTURE IS BEHAVIOURAL. The OLD binary is the newest release below
// the tier (CROSSVER_BELOW_EXACT_NUMBERS_*, see THE FOURTH AXIS in
// scripts/crossversion-build.sh), built from its tag, and every assertion
// is what a binary DOES with a store, graded against the SOURCE's own
// ::text rendering.
//
//	cell A  refusal      NEW writes a numeric trigger chain → the segment is stamped 12
//	                       → OLD `restore` and `backup verify` REFUSE at the version ceiling
//	                       → NEW restores it exactly
//	cell B  control      NEW writes an integer/text-only trigger chain → no segment above OLD
//	                       → OLD restores it (the stamp is proportional)
//	cell C  old chains   OLD writes a numeric trigger chain (stamped at most OLD's ceiling)
//	                       → NEW restores it EXACTLY (exact decoding keys on the source
//	                         engine, not on the version)
//	cell D  premise      OLD restores its OWN scalar numeric chain at exit 0, WRONG —
//	                       the silent rounding the tier exists to refuse

package pipeline

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

type xnBinaries struct {
	oldBin, newBin       string
	oldTag               string
	oldFormat, newFormat int
	tier                 int
}

// xnLoadBinaries reads this suite's slice of the build script's contract.
// Every failure path is a t.Fatalf, never a skip.
func xnLoadBinaries(t *testing.T) xnBinaries {
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
	b := xnBinaries{
		oldBin:    get("CROSSVER_BELOW_EXACT_NUMBERS_BIN"),
		newBin:    get("CROSSVER_NEW_BIN"),
		oldTag:    get("CROSSVER_BELOW_EXACT_NUMBERS_TAG"),
		oldFormat: getInt("CROSSVER_BELOW_EXACT_NUMBERS_FORMAT"),
		newFormat: getInt("CROSSVER_NEW_FORMAT"),
		tier:      getInt("CROSSVER_BELOW_EXACT_NUMBERS_TIER"),
	}
	if b.tier != irbackup.FormatVersionExactNumbers {
		t.Fatalf("CROSSVER_BELOW_EXACT_NUMBERS_TIER=%d but this tree's FormatVersionExactNumbers is %d", b.tier, irbackup.FormatVersionExactNumbers)
	}
	if b.oldFormat >= b.tier {
		t.Fatalf("OLD %s stamps BackupFormatVersion=%d, at or above the exact-numbers tier %d — it would READ the "+
			"stamped segment and cell A would be vacuous", b.oldTag, b.oldFormat, b.tier)
	}
	if b.newFormat < b.tier {
		t.Fatalf("NEW stamps BackupFormatVersion=%d, below the tier %d it is supposed to write", b.newFormat, b.tier)
	}
	for _, p := range []string{b.oldBin, b.newBin} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("binary %q: %v", p, err)
		}
	}
	t.Logf("exact-numbers gate: OLD=%s (%s, BackupFormatVersion=%d)  NEW=%s (BackupFormatVersion=%d)  tier=%d",
		b.oldBin, b.oldTag, b.oldFormat, b.newBin, b.newFormat, b.tier)
	return b
}

// xnNumsDDL is the numeric test table (see pgNumsText), with one row
// written before the full so the full's data path is graded too.
const xnNumsDDL = `
	CREATE TABLE nums (
		id BIGINT PRIMARY KEY,
		nu NUMERIC, np NUMERIC(38,12), f8 DOUBLE PRECISION, f4 REAL,
		na NUMERIC[], js JSONB, fa DOUBLE PRECISION[]
	);
	INSERT INTO nums (id, nu, np, f8, f4, na, js, fa) VALUES (1, 1, 1, 1, 1, '{1}', '{"seed": 1}', '{1}');
`

// xnNumericDelta inserts every pgTriggerNumericCells row — the values an
// older reader rounds.
func xnNumericDelta() string {
	s := ""
	for _, c := range pgTriggerNumericCells {
		s += fmt.Sprintf("INSERT INTO nums (id, nu, np, f8, f4, na, js, fa) VALUES (%d, '%s', '%s', '%s', '%s', '%s', '%s', '%s');\n",
			c.id, c.nu, c.np, c.f8, c.f4, c.na, c.js, c.fa)
	}
	return s
}

// xnIntegerDelta carries only integral numbers and NULLs — nothing the
// capture holds as exact text (the UPDATE targets row 500, whose np is
// NULL: row 1's NUMERIC(38,12) value is captured as 1.000000000000,
// which DOES carry, and would stamp the segment) — so it must not raise
// the segment. The jsonb leaves are integers up to 15 digits, the
// widest the stamp exempts.
const xnIntegerDelta = `
	INSERT INTO nums (id, nu, np, f8, f4, na, js, fa) VALUES (500, 42, NULL, 7, 3, '{1,2}', '{"n": 5, "s": "x", "big": 999999999999999, "neg": -123456789012345, "z": 0, "l": [1, [2, 3]]}', '{4}');
	UPDATE nums SET nu = 43 WHERE id = 500;
`

// xnChain writes a trigger chain with bin: setup, a full, delta, one
// incremental window.
func xnChain(t *testing.T, bin, srcDSN, dir, delta string) {
	t.Helper()
	xvMustExec(t, "trigger setup", bin, "trigger", "setup", "--dsn", srcDSN, "--tables", "nums")
	xvMustExec(t, "backup full", bin, "backup", "full",
		"--source-driver", "postgres-trigger", "--source", srcDSN, "--output-dir", dir)
	applyDDL(t, srcDSN, delta)
	xvMustExec(t, "backup incremental", bin, "backup", "incremental",
		"--source-driver", "postgres-trigger", "--source", srcDSN, "--output-dir", dir,
		"--window", xvIncrementalWindow, "--max-changes", "0")
}

func xnRestore(t *testing.T, bin, dir, targetDSN string) (string, error) {
	t.Helper()
	return xvExec(t, bin, "restore", "--from-dir", dir, "--target-driver", "postgres", "--target", targetDSN)
}

// xnDiff returns every (row, column) where got differs from the source.
func xnDiff(want, got map[int64]map[string]string) []string {
	var d []string
	for id, w := range want {
		g, ok := got[id]
		if !ok {
			d = append(d, fmt.Sprintf("row %d missing", id))
			continue
		}
		for col, wv := range w {
			if g[col] != wv {
				d = append(d, fmt.Sprintf("row %d %s: restored %q, source %q", id, col, g[col], wv))
			}
		}
	}
	if len(got) != len(want) {
		d = append(d, fmt.Sprintf("restored %d rows, source has %d", len(got), len(want)))
	}
	return d
}

func xnAssertExact(t *testing.T, what, srcDSN, tgtDSN string) {
	t.Helper()
	if d := xnDiff(pgNumsText(t, srcDSN), pgNumsText(t, tgtDSN)); len(d) != 0 {
		t.Fatalf("%s: the restore is not the source:\n%v", what, d)
	}
}

func TestBackup_CrossVersionExactNumbers(t *testing.T) {
	bins := xnLoadBinaries(t)
	adminDSN, _, cleanup := startPostgresLogical(t)
	defer cleanup()
	ledger := newXVLedger(t, "A-refusal", "B-control", "C-old-chain-exact", "D-old-rounds")

	t.Run("cellA_numeric_segment_refused_by_OLD", func(t *testing.T) {
		src := xvCreateDB(t, adminDSN, "xn_a_src")
		applyDDL(t, src, xnNumsDDL)
		dir := t.TempDir()
		xnChain(t, bins.newBin, src, dir, xnNumericDelta())

		_, segs := xvChainVersions(t, dir)
		if len(segs) != 1 || segs[0] != bins.tier {
			t.Fatalf("NEW stamped the numeric segment(s) %v; want one at the exact-numbers tier %d", segs, bins.tier)
		}
		oldTgt := xvCreateDB(t, adminDSN, "xn_a_old_tgt")
		out, err := xnRestore(t, bins.oldBin, dir, oldTgt)
		xvAssertVersionRefusal(t, "cell A (OLD restore)", out, err, oldTgt)
		out, err = xvExec(t, bins.oldBin, "backup", "verify", "--from-dir", dir)
		if err == nil || !strings.Contains(out, xvFormatVersionRefusal) {
			t.Fatalf("cell A (OLD backup verify): want the %q refusal; err=%v\n--- output ---\n%s", xvFormatVersionRefusal, err, out)
		}

		newTgt := xvCreateDB(t, adminDSN, "xn_a_new_tgt")
		xvMustExec(t, "cell A (NEW restore)", bins.newBin, "restore", "--from-dir", dir, "--target-driver", "postgres", "--target", newTgt)
		xnAssertExact(t, "cell A (NEW restore)", src, newTgt)
		ledger.report(t, "A-refusal")
	})

	t.Run("cellB_integer_segment_stays_readable_by_OLD", func(t *testing.T) {
		src := xvCreateDB(t, adminDSN, "xn_b_src")
		applyDDL(t, src, xnNumsDDL)
		dir := t.TempDir()
		xnChain(t, bins.newBin, src, dir, xnIntegerDelta)

		root, segs := xvChainVersions(t, dir)
		for _, v := range append([]int{root}, segs...) {
			if v > bins.oldFormat {
				t.Fatalf("NEW stamped an integer-only trigger chain root=%d segments=%v, above OLD's %d — the stamp is not "+
					"proportional, and every trigger chain just became unreadable by older binaries", root, segs, bins.oldFormat)
			}
		}
		tgt := xvCreateDB(t, adminDSN, "xn_b_tgt")
		xvMustExec(t, "cell B (OLD restore)", bins.oldBin, "restore", "--from-dir", dir, "--target-driver", "postgres", "--target", tgt)
		xnAssertExact(t, "cell B (OLD restore)", src, tgt)
		ledger.report(t, "B-control")
	})

	t.Run("cellC_OLD_numeric_chain_restored_exactly_by_NEW", func(t *testing.T) {
		src := xvCreateDB(t, adminDSN, "xn_c_src")
		applyDDL(t, src, xnNumsDDL)
		dir := t.TempDir()
		xnChain(t, bins.oldBin, src, dir, xnNumericDelta())

		root, segs := xvChainVersions(t, dir)
		for _, v := range append([]int{root}, segs...) {
			if v > bins.oldFormat {
				t.Fatalf("OLD wrote root=%d segments=%v, above its own ceiling %d", root, segs, bins.oldFormat)
			}
		}
		newTgt := xvCreateDB(t, adminDSN, "xn_c_new_tgt")
		xvMustExec(t, "cell C (NEW restore)", bins.newBin, "restore", "--from-dir", dir, "--target-driver", "postgres", "--target", newTgt)
		xnAssertExact(t, "cell C (NEW restore of an OLD chain)", src, newTgt)

		ledger.report(t, "C-old-chain-exact")
	})

	// Cell D — the premise the tier rests on, measured rather than
	// assumed: OLD restores its own scalar numeric chain at EXIT 0 and
	// gets it wrong. (Array elements are not used here: OLD refuses a
	// numeric[] element loudly — "expected string, got float64" — so the
	// silent arm is the scalar and jsonb one.) If OLD ever restores this
	// exactly, cell A's refusal protects against nothing and the tier's
	// cost — older binaries locked out — needs re-deciding.
	t.Run("cellD_OLD_silently_rounds_its_own_chain", func(t *testing.T) {
		src := xvCreateDB(t, adminDSN, "xn_d_src")
		applyDDL(t, src, xnNumsDDL)
		dir := t.TempDir()
		xnChain(t, bins.oldBin, src, dir, `
			INSERT INTO nums (id, nu, js) VALUES (600, '10.50', '{"t": 1.500}');
			INSERT INTO nums (id, nu, js) VALUES (601, '12345678901234567890.123', '{"u": 1e-400}');
		`)
		tgt := xvCreateDB(t, adminDSN, "xn_d_tgt")
		xvMustExec(t, "cell D (OLD restore)", bins.oldBin, "restore", "--from-dir", dir, "--target-driver", "postgres", "--target", tgt)
		d := xnDiff(pgNumsText(t, src), pgNumsText(t, tgt))
		if len(d) == 0 {
			t.Fatalf("cell D: OLD %s restored its own numeric trigger chain EXACTLY at exit 0 — the silent rounding the "+
				"exact-numbers tier refuses older binaries over is not present in it", bins.oldTag)
		}
		t.Logf("cell D: OLD %s restored its own chain at exit 0 with %d silent difference(s): %v", bins.oldTag, len(d), d)
		ledger.report(t, "D-old-rounds")
	})
}
