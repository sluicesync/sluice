//go:build integration && crossversion

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The cross-version gate for FormatVersionHiddenCollation
// (FUNCTIONAL-CAST-HIDDEN-COLLATION; sixth review F3/F4), run by the same
// extended-suites.yml `crossversion` leg as TestBackup_CrossVersionChainCompat.
//
// WHY A SIXTH SUITE. Version 15 is stamped on any manifest whose schema
// carries a MySQL functional key part's hidden-column collation — a full
// reaches it with no flag at all, from a vanilla MySQL source with such a
// key. The tier exists because the field is folded into the schema
// fingerprint and decides how a restored key compares; an older binary
// drops it. The old binary is CROSSVER_BELOW_HIDDEN_COLLATION_* — the
// newest release below 15 (see THE FOURTH AXIS in
// scripts/crossversion-build.sh), v0.160.4 when this was written.
//
// THE FIXTURE IS BEHAVIOURAL, graded by the SOURCE's own rows read as
// id:hex(k):v — never by a manifest field alone. The source key
// `cast(k as char(10))` is created from a `utf8mb4_bin` session, so its
// hidden column compares under `utf8mb4_bin` while the expression text says
// nothing of it: 'a' and 'A' are two keys on the source and one under the
// target's default.
//
//	cell A  refusal      NEW writes a chain: a full holding 'a', then one
//	                       window inserting 'A' → the root is stamped 15
//	                       → OLD `restore` and `backup verify` REFUSE at the
//	                         version ceiling, target untouched
//	                       → NEW restores it exactly (the key pinned
//	                         `COLLATE utf8mb4_bin`)
//	cell B  control      NEW writes a chain over a table with no functional
//	                       key → nothing above OLD's ceiling → OLD restores
//	                       it exactly (the stamp is proportional)
//	cell C  premise      OLD writes cell A's chain itself and restores it:
//	                       NOT the source (its key is rebuilt from the text,
//	                       under the target default) — the harm the tier
//	                       refuses older binaries over. Exit 0 with a row
//	                       merged away is the silent shape; a loud refusal is
//	                       logged as the weaker premise it is

package pipeline

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// hcxvLoadBinaries reads this suite's slice of the build script's contract.
// Every failure path is a t.Fatalf, never a skip.
func hcxvLoadBinaries(t *testing.T) kcxvBinaries {
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
		oldBin:    get("CROSSVER_BELOW_HIDDEN_COLLATION_BIN"),
		newBin:    get("CROSSVER_NEW_BIN"),
		oldTag:    get("CROSSVER_BELOW_HIDDEN_COLLATION_TAG"),
		oldFormat: getInt("CROSSVER_BELOW_HIDDEN_COLLATION_FORMAT"),
		newFormat: getInt("CROSSVER_NEW_FORMAT"),
		tier:      getInt("CROSSVER_BELOW_HIDDEN_COLLATION_TIER"),
	}
	if b.tier != irbackup.FormatVersionHiddenCollation {
		t.Fatalf("CROSSVER_BELOW_HIDDEN_COLLATION_TIER=%d but this tree's FormatVersionHiddenCollation is %d",
			b.tier, irbackup.FormatVersionHiddenCollation)
	}
	if b.oldFormat >= b.tier {
		t.Fatalf("OLD %s stamps BackupFormatVersion=%d, at or above the hidden-collation tier %d — it would READ the "+
			"stamped root and cell A would be vacuous", b.oldTag, b.oldFormat, b.tier)
	}
	if b.newFormat < b.tier {
		t.Fatalf("NEW stamps BackupFormatVersion=%d, below the tier %d it is supposed to write", b.newFormat, b.tier)
	}
	for _, p := range []string{b.oldBin, b.newBin} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("binary %q: %v", p, err)
		}
	}
	t.Logf("hidden-collation gate: OLD=%s (%s, BackupFormatVersion=%d)  NEW=%s (BackupFormatVersion=%d)  tier=%d",
		b.oldBin, b.oldTag, b.oldFormat, b.newBin, b.newFormat, b.tier)
	return b
}

// hcxvFunctionalKey is created from a utf8mb4_bin session: the hidden
// column of `fk` compares under utf8mb4_bin, the column under 0900_ai_ci.
const hcxvFunctionalKey = "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, " +
	"k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL, v INT, " +
	"UNIQUE KEY fk ((cast(k as char(10)))))"

// hcxvPlainKey is the control: the same rows under a plain unique key.
const hcxvPlainKey = "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, " +
	"k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, v INT, UNIQUE KEY uk (k))"

// hcxvSource creates a source database holding create and row 1 ('a'),
// creating the table from a utf8mb4_bin session.
func hcxvSource(t *testing.T, root twfbDB, name, create string) twfbDB {
	t.Helper()
	root.exec(t, "CREATE DATABASE "+name)
	dsn, err := buildMySQLDSN(root.dsn, name)
	if err != nil {
		t.Fatal(err)
	}
	twfbDB{engine: "mysql", dsn: dsn + "&collation=utf8mb4_bin"}.exec(t, create)
	src := twfbDB{engine: "mysql", dsn: dsn}
	src.exec(t, "INSERT INTO kd VALUES (1, 'a', 1)")
	return src
}

// hcxvTarget creates an empty restore target database.
func hcxvTarget(t *testing.T, root twfbDB, name string) twfbDB {
	t.Helper()
	root.exec(t, "CREATE DATABASE "+name)
	dsn, err := buildMySQLDSN(root.dsn, name)
	if err != nil {
		t.Fatal(err)
	}
	return twfbDB{engine: "mysql", dsn: dsn}
}

// hcxvChain writes a chain with bin: a full of src (row 1), then 'A' as row
// 2 in one incremental window.
func hcxvChain(t *testing.T, bin string, src twfbDB, dir string) {
	t.Helper()
	xvMustExec(t, "backup full", bin, "backup", "full",
		"--source-driver", "mysql", "--source", src.dsn, "--output-dir", dir)
	src.exec(t, "INSERT INTO kd VALUES (2, 'A', 2)")
	xvMustExec(t, "backup incremental", bin, "backup", "incremental",
		"--source-driver", "mysql", "--source", src.dsn, "--output-dir", dir,
		"--window", xvIncrementalWindow, "--max-changes", "0")
}

// hcxvRows renders kd as "id:hex(k):v" sorted by id — the key's BYTES, so a
// merge under the target default cannot read as a match.
func hcxvRows(t *testing.T, d twfbDB) string {
	t.Helper()
	n := d.scalar(t, "SELECT COUNT(*) FROM kd")
	var out []string
	for _, id := range []int{1, 2} {
		if r := d.scalar(t, "SELECT CONCAT(id, ':', HEX(k), ':', IFNULL(v, 'NULL')) FROM kd WHERE id = ?", id); !strings.HasPrefix(r, "<") {
			out = append(out, r)
		}
	}
	sort.Strings(out)
	return fmt.Sprintf("[%s rows] %s", n, strings.Join(out, " "))
}

func TestBackup_CrossVersionHiddenCollation(t *testing.T) {
	bins := hcxvLoadBinaries(t)
	_, myRoot, cleanup := startMySQL(t)
	defer cleanup()
	root := twfbDB{engine: "mysql", dsn: myRoot}
	ledger := newXVLedger(t, "A-refusal", "B-control", "C-old-diverges")

	t.Run("cellA_hidden_collation_root_refused_by_OLD", func(t *testing.T) {
		src := hcxvSource(t, root, "hcxv_a_src", hcxvFunctionalKey)
		dir := t.TempDir()
		hcxvChain(t, bins.newBin, src, dir)
		want := hcxvRows(t, src)
		if !strings.HasPrefix(want, "[2 rows]") {
			t.Fatalf("the source key did not keep 'a' and 'A' apart (%s); the cell measures nothing", want)
		}

		// OLD first, so a missing stamp shows as what it costs.
		oldTgt := hcxvTarget(t, root, "hcxv_a_old_tgt")
		out, err := xvExec(t, bins.oldBin, "restore", "--from-dir", dir, "--target-driver", "mysql", "--target", oldTgt.dsn)
		if err == nil {
			t.Fatalf("cell A (OLD restore): OLD %s RESTORED a chain whose functional key compares under a hidden "+
				"collation it cannot read, at exit 0\nsource: %s\ntarget: %s", bins.oldTag, want, hcxvRows(t, oldTgt))
		}
		if !strings.Contains(out, xvFormatVersionRefusal) {
			t.Fatalf("cell A (OLD restore): want the %q refusal; err=%v\n--- output ---\n%s", xvFormatVersionRefusal, err, out)
		}
		rootV, segs := xvChainVersions(t, dir)
		if rootV != bins.tier {
			t.Fatalf("NEW stamped the root %d (segments %v); want the tier %d", rootV, segs, bins.tier)
		}
		if n := oldTgt.scalar(t, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE()"); n != "0" {
			t.Fatalf("cell A (OLD restore): refused, but the target already carries %s table(s) — the refusal must sit at the manifest preflight", n)
		}
		out, err = xvExec(t, bins.oldBin, "backup", "verify", "--from-dir", dir)
		if err == nil || !strings.Contains(out, xvFormatVersionRefusal) {
			t.Fatalf("cell A (OLD backup verify): want the %q refusal; err=%v\n--- output ---\n%s", xvFormatVersionRefusal, err, out)
		}

		newTgt := hcxvTarget(t, root, "hcxv_a_new_tgt")
		xvMustExec(t, "cell A (NEW restore)", bins.newBin, "restore", "--from-dir", dir, "--target-driver", "mysql", "--target", newTgt.dsn)
		if got := hcxvRows(t, newTgt); got != want {
			t.Fatalf("cell A (NEW restore): the restore is not the source\nsource: %s\ntarget: %s", want, got)
		}
		t.Logf("cell A: root=%d segments=%v; OLD refused; NEW restored %s", rootV, segs, want)
		ledger.report(t, "A-refusal")
	})

	t.Run("cellB_no_functional_key_stays_readable_by_OLD", func(t *testing.T) {
		src := hcxvSource(t, root, "hcxv_b_src", hcxvPlainKey)
		dir := t.TempDir()
		hcxvChain(t, bins.newBin, src, dir)
		rootV, segs := xvChainVersions(t, dir)
		for _, v := range append([]int{rootV}, segs...) {
			if v > bins.oldFormat {
				t.Fatalf("NEW stamped a chain with no functional key root=%d segments=%v, above OLD's %d — the stamp "+
					"is not proportional", rootV, segs, bins.oldFormat)
			}
		}
		tgt := hcxvTarget(t, root, "hcxv_b_tgt")
		xvMustExec(t, "cell B (OLD restore)", bins.oldBin, "restore", "--from-dir", dir, "--target-driver", "mysql", "--target", tgt.dsn)
		if want, got := hcxvRows(t, src), hcxvRows(t, tgt); want != got {
			t.Fatalf("cell B (OLD restore): the restore is not the source\nsource: %s\ntarget: %s", want, got)
		}
		t.Logf("cell B: root=%d segments=%v", rootV, segs)
		ledger.report(t, "B-control")
	})

	// Cell C — the premise, measured rather than assumed: OLD cannot restore
	// its own chain of this source faithfully. If it ever restores it
	// EXACTLY at exit 0, the tier's cost — older binaries locked out of
	// every MySQL backup with a collated functional key — needs re-deciding.
	t.Run("cellC_OLD_cannot_restore_its_own_chain", func(t *testing.T) {
		src := hcxvSource(t, root, "hcxv_c_src", hcxvFunctionalKey)
		dir := t.TempDir()
		hcxvChain(t, bins.oldBin, src, dir)
		tgt := hcxvTarget(t, root, "hcxv_c_tgt")
		out, err := xvExec(t, bins.oldBin, "restore", "--from-dir", dir, "--target-driver", "mysql", "--target", tgt.dsn)
		want, got := hcxvRows(t, src), hcxvRows(t, tgt)
		switch {
		case err == nil && want == got:
			t.Fatalf("cell C: OLD %s restored its own chain EXACTLY at exit 0 — the harm the hidden-collation tier "+
				"refuses older binaries over is not present\nrows: %s", bins.oldTag, got)
		case err == nil:
			t.Logf("cell C: OLD %s restored its own chain at exit 0, SILENTLY different\nsource: %s\ntarget: %s", bins.oldTag, want, got)
		default:
			t.Logf("cell C: OLD %s refused its own chain loudly (the weaker premise): %v\nsource: %s\ntarget: %s\n%s",
				bins.oldTag, err, want, got, out)
		}
		ledger.report(t, "C-old-diverges")
	})
}
