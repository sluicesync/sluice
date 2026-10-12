//go:build integration && crossversion

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The cross-version gate for FormatVersionKeyCollationDelta (Bug 304), run
// by the same extended-suites.yml `crossversion` leg as
// TestBackup_CrossVersionChainCompat.
//
// WHY A FOURTH SUITE. Version 13 is reachable by no `backup full` flag: it
// is stamped on a CDC SEGMENT whose schema delta changes a key column's
// collation. The delta entry is an ordinary alter_table; the collation
// aspect is derived from it at read time, and a reader built before
// v0.159.0 compares no collation, reads "no change", restores the table
// under its old collation and replays the window onto it — two rows the
// source keeps apart become one, at exit 0. v0.159.0 recorded such deltas
// without the stamp, which is the bug.
//
// THE FIXTURE IS BEHAVIOURAL. OLD is CROSSVER_BELOW_KEY_COLLATION_* — the
// newest release below the tier whose diff cannot see a collation change
// at all (see THE FOURTH AXIS in scripts/crossversion-build.sh), built from
// its tag. Every assertion is what a binary DOES with a store, graded by
// the SOURCE's own rows read by id:hex(key):v — never by a manifest field
// alone.
//
//	cell A  refusal      NEW writes a chain whose window changes the PK's
//	                       collation (nondeterministic ICU "ci" → "C") and adds
//	                       'A' beside 'a' → that segment is stamped 13
//	                       → OLD `restore` and `backup verify` REFUSE at the
//	                         version ceiling, target untouched
//	                       → NEW restores it exactly (a second segment after
//	                         the stamped one proves the chain still links)
//	cell B  control      NEW writes a chain whose window changes a NON-key
//	                       column's collation → no segment above OLD's ceiling
//	                       → OLD restores it exactly (the stamp is proportional)
//	cell C  premise      OLD writes cell A's chain itself and restores it at
//	                       exit 0 with row 99 MERGED away — the silent loss the
//	                       tier refuses older binaries over

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

type kcxvBinaries struct {
	oldBin, newBin       string
	oldTag               string
	oldFormat, newFormat int
	tier                 int
}

// kcxvLoadBinaries reads this suite's slice of the build script's contract.
// Every failure path is a t.Fatalf, never a skip.
func kcxvLoadBinaries(t *testing.T) kcxvBinaries {
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
		oldBin:    get("CROSSVER_BELOW_KEY_COLLATION_BIN"),
		newBin:    get("CROSSVER_NEW_BIN"),
		oldTag:    get("CROSSVER_BELOW_KEY_COLLATION_TAG"),
		oldFormat: getInt("CROSSVER_BELOW_KEY_COLLATION_FORMAT"),
		newFormat: getInt("CROSSVER_NEW_FORMAT"),
		tier:      getInt("CROSSVER_BELOW_KEY_COLLATION_TIER"),
	}
	if b.tier != irbackup.FormatVersionKeyCollationDelta {
		t.Fatalf("CROSSVER_BELOW_KEY_COLLATION_TIER=%d but this tree's FormatVersionKeyCollationDelta is %d",
			b.tier, irbackup.FormatVersionKeyCollationDelta)
	}
	if b.oldFormat >= b.tier {
		t.Fatalf("OLD %s stamps BackupFormatVersion=%d, at or above the key-collation tier %d — it would READ the "+
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
	t.Logf("key-collation gate: OLD=%s (%s, BackupFormatVersion=%d)  NEW=%s (BackupFormatVersion=%d)  tier=%d",
		b.oldBin, b.oldTag, b.oldFormat, b.newBin, b.newFormat, b.tier)
	return b
}

// kcxvCollation is the nondeterministic ICU collation the source key starts
// on: 'a' and 'A' are one key under it, two under "C". A restore target
// needs it too — the full's shape names it and sluice carries no
// CREATE COLLATION.
const kcxvCollation = `CREATE COLLATION ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false);`

const kcxvDDL = kcxvCollation + `
	CREATE TABLE kd (k text COLLATE ci PRIMARY KEY, id int NOT NULL, v int, note text COLLATE ci);
	INSERT INTO kd VALUES ('a', 1, 1, 'x'), ('b', 2, 2, 'y');
`

// kcxvKeyDelta is the Bug 304 window: the KEY's collation tightens, then a
// row only the new collation keeps apart arrives, then an ordinary update.
const kcxvKeyDelta = `
	ALTER TABLE kd ALTER COLUMN k TYPE text COLLATE "C";
	INSERT INTO kd VALUES ('A', 99, 99, 'z');
	UPDATE kd SET v = v + 10 WHERE id = 1;
`

// kcxvNonKeyDelta changes only a non-key column's collation — no reader
// treats that as a delta, so nothing may be stamped.
const kcxvNonKeyDelta = `
	ALTER TABLE kd ALTER COLUMN note TYPE text COLLATE "C";
	INSERT INTO kd VALUES ('c', 3, 3, 'w');
	UPDATE kd SET v = v + 10 WHERE id = 1;
`

// kcxvChain writes a chain with bin: a full, delta, one incremental window,
// and — when after is non-empty — a second window over after.
func kcxvChain(t *testing.T, bin, srcDSN, dir, slot, delta, after string) {
	t.Helper()
	xvMustExec(t, "backup full", bin, "backup", "full",
		"--source-driver", "postgres", "--source", srcDSN, "--output-dir", dir,
		"--chain-slot", "--slot-name", slot)
	for _, window := range []string{delta, after} {
		if window == "" {
			continue
		}
		applyDDL(t, srcDSN, window)
		xvMustExec(t, "backup incremental", bin, "backup", "incremental",
			"--source-driver", "postgres", "--source", srcDSN, "--output-dir", dir,
			"--slot-name", slot, "--window", xvIncrementalWindow, "--max-changes", "0")
	}
}

// kcxvTarget creates a restore target carrying the source's collation.
func kcxvTarget(t *testing.T, adminDSN, name string) string {
	t.Helper()
	dsn := xvCreateDB(t, adminDSN, name)
	applyDDL(t, dsn, kcxvCollation)
	return dsn
}

// kcxvRows renders kd as "id:hex(k):v" sorted by id — the key's BYTES, so a
// merge under the old collation cannot read as a match.
func kcxvRows(t *testing.T, dsn string) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.QueryContext(context.Background(),
		`SELECT id, encode(convert_to(k, 'UTF8'), 'hex'), coalesce(v::text, 'NULL') FROM kd`)
	if err != nil {
		t.Fatalf("read kd: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var id int
		var k, v string
		if err := rows.Scan(&id, &k, &v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, fmt.Sprintf("%05d:%s:%s", id, k, v))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	sort.Strings(out)
	return strings.Join(out, " ")
}

func TestBackup_CrossVersionKeyCollationDelta(t *testing.T) {
	bins := kcxvLoadBinaries(t)
	adminDSN, _, cleanup := startPostgresLogical(t)
	defer cleanup()
	ledger := newXVLedger(t, "A-refusal", "B-control", "C-old-merges")

	t.Run("cellA_key_collation_segment_refused_by_OLD", func(t *testing.T) {
		src := xvCreateDB(t, adminDSN, "kcxv_a_src")
		applyDDL(t, src, kcxvDDL)
		dir := t.TempDir()
		kcxvChain(t, bins.newBin, src, dir, "kcxv_a", kcxvKeyDelta, `INSERT INTO kd VALUES ('d', 100, 100, 'q');`)

		// OLD first, so a missing stamp shows as what it costs (a restore at
		// exit 0 with the merge) rather than only as a version number.
		oldTgt := kcxvTarget(t, adminDSN, "kcxv_a_old_tgt")
		out, err := xvExec(t, bins.oldBin, "restore", "--from-dir", dir, "--target-driver", "postgres", "--target", oldTgt)
		if err == nil {
			t.Fatalf("cell A (OLD restore): OLD %s RESTORED a chain whose window changed a key's collation, at exit 0\n"+
				"source: %s\ntarget: %s", bins.oldTag, kcxvRows(t, src), kcxvRows(t, oldTgt))
		}
		if !strings.Contains(out, xvFormatVersionRefusal) {
			t.Fatalf("cell A (OLD restore): want the %q refusal; err=%v\n--- output ---\n%s", xvFormatVersionRefusal, err, out)
		}
		_, segs := xvChainVersions(t, dir)
		if len(segs) != 2 || segs[0] != bins.tier {
			t.Fatalf("NEW stamped the segments %v; want two, the first (the key-collation window) at the tier %d", segs, bins.tier)
		}
		if n := xvPublicRelationCount(t, oldTgt); n != 0 {
			t.Fatalf("cell A (OLD restore): refused, but the target already carries %d relation(s) — the refusal must sit at the manifest preflight", n)
		}
		out, err = xvExec(t, bins.oldBin, "backup", "verify", "--from-dir", dir)
		if err == nil || !strings.Contains(out, xvFormatVersionRefusal) {
			t.Fatalf("cell A (OLD backup verify): want the %q refusal; err=%v\n--- output ---\n%s", xvFormatVersionRefusal, err, out)
		}

		newTgt := kcxvTarget(t, adminDSN, "kcxv_a_new_tgt")
		xvMustExec(t, "cell A (NEW restore)", bins.newBin, "restore", "--from-dir", dir, "--target-driver", "postgres", "--target", newTgt)
		if want, got := kcxvRows(t, src), kcxvRows(t, newTgt); want != got {
			t.Fatalf("cell A (NEW restore): the restore is not the source\nsource: %s\ntarget: %s", want, got)
		}
		ledger.report(t, "A-refusal")
	})

	t.Run("cellB_non_key_collation_stays_readable_by_OLD", func(t *testing.T) {
		src := xvCreateDB(t, adminDSN, "kcxv_b_src")
		applyDDL(t, src, kcxvDDL)
		dir := t.TempDir()
		kcxvChain(t, bins.newBin, src, dir, "kcxv_b", kcxvNonKeyDelta, "")

		root, segs := xvChainVersions(t, dir)
		for _, v := range append([]int{root}, segs...) {
			if v > bins.oldFormat {
				t.Fatalf("NEW stamped a non-key collation chain root=%d segments=%v, above OLD's %d — the stamp is not "+
					"proportional", root, segs, bins.oldFormat)
			}
		}
		tgt := kcxvTarget(t, adminDSN, "kcxv_b_tgt")
		xvMustExec(t, "cell B (OLD restore)", bins.oldBin, "restore", "--from-dir", dir, "--target-driver", "postgres", "--target", tgt)
		if want, got := kcxvRows(t, src), kcxvRows(t, tgt); want != got {
			t.Fatalf("cell B (OLD restore): the restore is not the source\nsource: %s\ntarget: %s", want, got)
		}
		ledger.report(t, "B-control")
	})

	// Cell C — the premise the tier rests on, measured rather than assumed:
	// OLD restores its own key-collation chain at EXIT 0 and loses row 99.
	// If OLD ever restores this exactly, cell A's refusal protects against
	// nothing and the tier's cost — older binaries locked out of every chain
	// that changes a key's collation — needs re-deciding.
	t.Run("cellC_OLD_silently_merges_its_own_chain", func(t *testing.T) {
		src := xvCreateDB(t, adminDSN, "kcxv_c_src")
		applyDDL(t, src, kcxvDDL)
		dir := t.TempDir()
		kcxvChain(t, bins.oldBin, src, dir, "kcxv_c", kcxvKeyDelta, "")
		tgt := kcxvTarget(t, adminDSN, "kcxv_c_tgt")
		xvMustExec(t, "cell C (OLD restore)", bins.oldBin, "restore", "--from-dir", dir, "--target-driver", "postgres", "--target", tgt)
		want, got := kcxvRows(t, src), kcxvRows(t, tgt)
		if want == got {
			t.Fatalf("cell C: OLD %s restored its own key-collation chain EXACTLY at exit 0 — the silent merge the "+
				"key-collation tier refuses older binaries over is not present in it\nrows: %s", bins.oldTag, got)
		}
		t.Logf("cell C: OLD %s restored its own chain at exit 0, silently different\nsource: %s\ntarget: %s", bins.oldTag, want, got)
		ledger.report(t, "C-old-merges")
	})
}
