//go:build integration && crossversion

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The CROSS-VERSION backup-compatibility gate (roadmap item 90 / Bug 212).
//
// Every other test in this repo runs ONE sluice binary against its own
// output. That is exactly why the defect this file exists for reached a
// release: it is invisible to a single binary by construction.
//
// The contract. A manifest records the format version its own chunks were
// sealed under, and every reader recomputes at the version the artifact
// records (ADR-0154 / ADR-0181's dual-version rule). That rule is what
// makes newer-sluice-reads-older true for the FORMAT VERSION, and it is
// deliberately PER-LINK. The consequence is not per-link: a restore walks
// the WHOLE chain, so a binary that refuses ANY one manifest restores
// NOTHING from that chain — including the segments it wrote itself, under
// a version it understands perfectly well.
//
// This paragraph used to end "true forever" (corrected 2026-08-07,
// invariant sweep). It is not forever and it is not unconditional, and the
// counterexample is two screens down in this very file: CELL 5 asserts a
// newer binary REFUSING an older chain across a schema-fingerprint epoch.
// The dual-version rule covers what the artifact RECORDS a version for;
// the schema fingerprint records none. Both halves are true and the
// unqualified sentence was the problem.
//
// forward-compat-caveats: schema-fingerprint-epoch
//
// So when a v0.104.0 binary (BackupFormatVersion 9) takes `backup
// incremental` against a chain rooted by a v0.103.2 binary (at most 8),
// the new segment is stamped 9 and the v0.103.2 binary loses the entire
// chain. That is the KNOWN, ACCEPTED behaviour — the alternative, sealing
// fresh chunks under the encoding a format bump exists to retire, is
// worse. This gate's job is to make it VISIBLE and to fail loudly if its
// SHAPE ever changes, not to assert it is fixed.
//
// The suite runs three real binaries against one real backup store:
//
//	cell 1  forward read      OLD writes chain → NEW restores        PASS
//	cell 2  backward refusal  NEW writes chain → OLD restores      REFUSE
//	cell 3  the Bug-212 cell  OLD writes, NEW extends →
//	                            (a) NEW restores                      PASS
//	                            (b) OLD restores                     FAIL
//	cell 4  control           OLD writes, OLD extends, OLD restores  PASS
//	cell 5  epoch partition   EPOCH writes chain → NEW restores    REFUSE
//	cell 6  fingerprint fwd   NEW writes chain → PEER restores       PASS
//
// Cell 4 is not padding: it is what proves cell 3b's failure is caused by
// the version raise and by nothing else in the harness.
//
// CELL 5 IS A SECOND AXIS, and it exists because cells 1–4 structurally
// cannot see it (roadmap item 102). OLD is derived as "the newest tag
// whose BackupFormatVersion is strictly lower than the working tree's" —
// the rule that makes this gate non-vacuous, and the rule that made it
// blind to the SCHEMA-FINGERPRINT partition: sluice's manifest schema
// fingerprint moved twice (v0.100.0, v0.103.0) from untagged field
// additions to ir.Index, and BOTH boundaries sit INSIDE format version 8,
// so the "strictly lower" walk steps right over them. Cell 5 pins a pair
// that straddles one, and asserts the CURRENT, ACCEPTED behaviour: the
// chain is refused, with the honest two-cause schema-fingerprint refusal
// rather than the accusation of tampering it used to carry.
//
// So cell 5 documents the partition as known-and-accepted. If someone
// later builds the compatibility shim item 102 describes, this cell goes
// red — which is the correct trigger to come back and update it.
//
// CELL 6 IS THE THIRD AXIS, and it exists because cells 1–5 all read
// chains written by an OLDER binary, or assert a refusal. None of them
// asks the question that caught the FOURTH epoch (roadmap item 104 / Bug
// 216): does a chain THIS build writes still restore on the previous
// release? v0.104.3 added an `omitempty` field to ir.Index believing the
// zero value would keep the fingerprint still — but the Postgres reader
// sets it on every primary-keyed table, so every real PG chain moved, and
// no gate saw it. The frozen golden could not: its fixture carried the
// new field at its new value, so it only ever proved the current binary
// agrees with itself. What found it was a regression cycle asking the
// boring question, and cell 6 is that question made permanent. Its peer
// binary is derived at the SAME BackupFormatVersion precisely so that a
// refusal here can only be the fingerprint's.
//
// Encryption is not optional here. FormatVersion 9 is an ENCRYPTED-manifest
// tier — a plaintext chain keeps its schema-derived version (1/2/4) and
// would reproduce nothing.
//
// Binaries come from scripts/crossversion-build.sh via the environment.
// Absent them this test FAILS rather than skipping: a skip is the vacuous
// green this whole file exists to prevent, and the `crossversion` build tag
// means nothing else compiles it by accident.

package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// xvPassphraseEnv / xvPassphraseValue: the passphrase reaches both binaries
// through the environment (`--encryption-passphrase-env`), never on the
// command line, so a failing run's logged argv carries no secret.
//
// The variable name deliberately carries NO `SLUICE_` prefix and NO
// "passphrase" substring. Both were learned the hard way while building
// this: koanf reads every `SLUICE_*` variable as config and WARNs about
// the ones it doesn't recognise, which put the literal string "passphrase"
// into the output of a refusal that had nothing to do with the passphrase
// — and tripped this suite's own you-must-not-blame-the-passphrase
// assertion. A test-fixture name that can appear in the artifact under
// test is a name that can lie about it.
const (
	xvPassphraseEnv   = "CROSSVER_BACKUP_KEY"
	xvPassphraseValue = "crossversion-gate-key-not-a-real-secret"
)

// xvCmdTimeout bounds any single binary invocation. Generous: a `backup
// incremental` sits on its CDC window, and the window budget below is well
// inside this.
const xvCmdTimeout = 5 * time.Minute

// xvIncrementalWindow is how long each incremental streams. Every delta
// this suite writes is COMMITTED before the window opens, so decoding it
// takes milliseconds and the rest is the window running out — 12s is slack
// for a loaded runner, not a wait for anything. Six segments, so this is
// roughly 75s of the suite's total.
const xvIncrementalWindow = "12s"

// xvFormatVersionRefusal is the operator-facing substring the older binary
// MUST print when it meets a manifest from the future. Pinned as a literal
// (not imported) on purpose — this is a cross-VERSION contract, and the
// wording that matters is the one the OLD binary already shipped with, so
// importing the new build's constant would assert the wrong side.
const xvFormatVersionRefusal = "newer than this build supports"

// The cell-5 refusal contract, pinned as literals for the same reason as
// xvFormatVersionRefusal: this is a cross-VERSION assertion about what an
// operator reads, so it must not ride a constant the build under test can
// move.
//
// xvSchemaFingerprintRefusal is the refusal itself. The two "must say"
// fragments are the halves that make it honest: the mismatch has TWO
// causes and this check cannot tell them apart, so it must name the
// release-skew one (the chain is intact, only unreadable here) alongside
// corruption. xvTamperAccusation is the wording it USED to carry —
// asserted absent, because telling an operator holding a byte-perfect
// chain that their manifest is corrupt, during a recovery, sends them
// hunting a storage fault that does not exist.
const (
	xvSchemaFingerprintRefusal = "schema hash mismatch"
	xvSchemaFingerprintIntact  = "the chain is intact"
	xvSchemaFingerprintCorrupt = "genuinely corrupt or tampered"
	xvTamperAccusation         = "corrupted or partially-rewritten"
)

// xvMisleadingRefusalTerms are words the version refusal must NOT contain.
// A v0.103.x binary once told an operator holding the CORRECT passphrase
// that the passphrase was wrong; an operator handed the wrong reason does
// the wrong thing next (rotates keys, re-derives, escalates a security
// incident) instead of the one right thing (upgrade the binary). A refusal
// that names the wrong cause is a defect even though the refusal itself is
// correct, so it is asserted, not merely hoped for.
var xvMisleadingRefusalTerms = []string{
	"passphrase",
	"message authentication failed",
	"wrong key",
	"decrypt",
}

// ---------------------------------------------------------------------
// binaries + the non-vacuity contract
// ---------------------------------------------------------------------

type xvBinaries struct {
	oldBin, newBin       string
	oldTag               string
	oldFormat, newFormat int

	// The cell-5 axis: a binary from a DIFFERENT schema-fingerprint epoch
	// (see the file header). Its BackupFormatVersion is unconstrained
	// relative to oldFormat — the two axes are independent.
	epochBin    string
	epochTag    string
	epochFormat int

	// The cell-6 axis: the newest release at the SAME format version as
	// this build (roadmap item 104 / Bug 216). Equal-format is what makes
	// the cell about the schema fingerprint rather than the version gate.
	peerBin    string
	peerTag    string
	peerFormat int
}

// xvEpochVersion is the version string the epoch binary stamps into every
// manifest it writes — the tag without its leading "v", exactly as
// goreleaser's `-X main.version={{.Version}}` renders a real release (and
// as scripts/crossversion-build.sh reproduces for the tag builds). The
// fingerprint refusal ECHOES this back to the operator, so cell 5 can
// assert the refusal names the release that actually wrote the chain.
func (b xvBinaries) xvEpochVersion() string { return strings.TrimPrefix(b.epochTag, "v") }

// xvLoadBinaries reads the contract scripts/crossversion-build.sh emits.
// Every failure path here is a t.Fatalf, never a t.Skip.
func xvLoadBinaries(t *testing.T) xvBinaries {
	t.Helper()
	get := func(k string) string {
		v := os.Getenv(k)
		if v == "" {
			t.Fatalf("%s is unset — this suite needs BOTH binaries. Run `bash scripts/crossversion-build.sh` "+
				"and export the KEY=VALUE lines it prints, then re-run with -tags='integration crossversion'. "+
				"(Deliberately a FAILURE and not a skip: a skipped cross-version gate is the vacuous green it exists to prevent.)", k)
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
	b := xvBinaries{
		oldBin:      get("CROSSVER_OLD_BIN"),
		newBin:      get("CROSSVER_NEW_BIN"),
		oldTag:      get("CROSSVER_OLD_TAG"),
		oldFormat:   getInt("CROSSVER_OLD_FORMAT"),
		newFormat:   getInt("CROSSVER_NEW_FORMAT"),
		epochBin:    get("CROSSVER_EPOCH_BIN"),
		epochTag:    get("CROSSVER_EPOCH_TAG"),
		epochFormat: getInt("CROSSVER_EPOCH_FORMAT"),
		peerBin:     get("CROSSVER_PEER_BIN"),
		peerTag:     get("CROSSVER_PEER_TAG"),
		peerFormat:  getInt("CROSSVER_PEER_FORMAT"),
	}
	// The FIRST non-vacuity guard: a gate whose two binaries understand the
	// same manifest versions asserts nothing at all, and would report a
	// confident green while covering none of the four cells' intent. The
	// build script refuses to produce such a pair; re-assert it here so a
	// hand-set environment cannot route around it.
	if b.oldFormat >= b.newFormat {
		t.Fatalf("OLD %s stamps BackupFormatVersion=%d and NEW stamps %d — the gate needs old < new, "+
			"otherwise every cell below is vacuous. Re-run scripts/crossversion-build.sh.",
			b.oldTag, b.oldFormat, b.newFormat)
	}
	// Cell 5's format-version precondition. The epoch cell asserts a
	// SCHEMA-FINGERPRINT refusal; if the epoch binary wrote manifests NEW
	// cannot read at all, the version gate preempts and the cell would
	// assert the wrong contract entirely. (This is the harness half; the
	// cell re-checks the chain it actually got.)
	if b.epochFormat > b.newFormat {
		t.Fatalf("EPOCH %s stamps BackupFormatVersion=%d, above NEW's %d — NEW would refuse it on the VERSION gate "+
			"before reaching the fingerprint check, so cell 5 would assert the wrong refusal. Pick an older epoch tag "+
			"in scripts/crossversion-build.sh.", b.epochTag, b.epochFormat, b.newFormat)
	}
	// Cell 6's precondition, and the reason the peer is derived on EQUALITY.
	// A peer at a lower format refuses a NEW-written chain on the version
	// gate, which cell 2 already covers — cell 6 would then report green
	// about a refusal instead of red about a fingerprint it cannot
	// reproduce, which is the exact failure mode the whole file is built to
	// avoid.
	if b.peerFormat != b.newFormat {
		t.Fatalf("PEER %s stamps BackupFormatVersion=%d and NEW stamps %d — cell 6 needs them EQUAL so the only thing "+
			"that can refuse the restore is the schema fingerprint. Re-run scripts/crossversion-build.sh, or repoint "+
			"CROSSVER_PEER_TAG at a same-format release.", b.peerTag, b.peerFormat, b.newFormat)
	}
	for _, p := range []string{b.oldBin, b.newBin, b.epochBin, b.peerBin} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("binary %q: %v", p, err)
		}
	}
	t.Logf("cross-version gate: OLD=%s (%s, BackupFormatVersion=%d)  NEW=%s (BackupFormatVersion=%d)  EPOCH=%s (%s, BackupFormatVersion=%d)  PEER=%s (%s, BackupFormatVersion=%d)",
		b.oldBin, b.oldTag, b.oldFormat, b.newBin, b.newFormat, b.epochBin, b.epochTag, b.epochFormat,
		b.peerBin, b.peerTag, b.peerFormat)
	return b
}

// ---------------------------------------------------------------------
// the cell ledger (the second non-vacuity guard)
// ---------------------------------------------------------------------

// xvLedger records which cells actually reached their assertions. The
// workflow leg has its own PASS-count guard, but that one watches from
// outside and can only see what `go test` chose to print; this one lives
// where the truth is. A cell that never ran — filtered out, panicked past,
// quietly returned early — fails the parent test by name.
type xvLedger struct {
	expected []string
	done     map[string]bool
}

func newXVLedger(t *testing.T, cells ...string) *xvLedger {
	t.Helper()
	l := &xvLedger{expected: cells, done: make(map[string]bool, len(cells))}
	t.Cleanup(func() {
		var missing []string
		for _, c := range l.expected {
			if !l.done[c] {
				missing = append(missing, c)
			}
		}
		if len(missing) > 0 {
			t.Errorf("cross-version gate did not run every cell — missing: %s. "+
				"All %d cells must report; a partial run is a vacuous green, not a pass.",
				strings.Join(missing, ", "), len(l.expected))
		}
	})
	return l
}

func (l *xvLedger) report(t *testing.T, cell string) {
	t.Helper()
	if !l.done[cell] {
		l.done[cell] = true
		t.Logf("cross-version gate: cell %s reported", cell)
	}
}

// ---------------------------------------------------------------------
// invocation helpers
// ---------------------------------------------------------------------

// xvExec runs one sluice invocation and returns its combined output. The
// combined stream is what an operator sees, which is what the refusal
// assertions are about.
//
// The child's environment is the caller's with every `SLUICE_*` variable
// REMOVED. That prefix is sluice's own koanf config namespace, so a stray
// SLUICE_SOURCE / SLUICE_TARGET in the shell that launched `go test` would
// silently retarget a cell — and any other SLUICE_* variable produces a
// config-typo WARN that pollutes the very output the refusal assertions
// read. The cells pass everything they mean on the command line.
func xvExec(t *testing.T, bin string, args ...string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), xvCmdTimeout)
	defer cancel()
	env := make([]string, 0, len(os.Environ())+1)
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "SLUICE_") {
			continue
		}
		env = append(env, kv)
	}
	env = append(env, xvPassphraseEnv+"="+xvPassphraseValue)
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func xvMustExec(t *testing.T, what, bin string, args ...string) string {
	t.Helper()
	out, err := xvExec(t, bin, args...)
	if err != nil {
		t.Fatalf("%s: %v\n--- %s %s ---\n%s", what, err, filepath.Base(bin), strings.Join(args, " "), out)
	}
	return out
}

// xvEncryptFlags is the passphrase-encryption flag set every write and read
// side shares. Defined once so no cell can accidentally take a plaintext
// path (which would keep the schema-derived version and reproduce nothing).
func xvEncryptFlags() []string {
	return []string{"--encrypt", "--encryption-passphrase-env", xvPassphraseEnv}
}

func xvBackupFull(t *testing.T, bin, srcDSN, dir, slot string) {
	t.Helper()
	args := append([]string{
		"backup", "full",
		"--source-driver", "postgres", "--source", srcDSN,
		"--output-dir", dir,
		"--chain-slot", "--slot-name", slot,
	}, xvEncryptFlags()...)
	xvMustExec(t, "backup full", bin, args...)
}

// xvBackupIncremental extends the chain in dir with one TIME-BOUND CDC
// window.
//
// `--max-changes` is deliberately NOT used, and the reason is worth
// keeping. The count it bounds is CDC EVENTS, not row changes — a window
// budgeted at "the 2 rows I just wrote" closes on transaction framing
// before the rows are decoded, and the segment lands with
// start_position == end_position: a real, chain-linked, correctly-stamped
// manifest carrying none of the delta. Ground-truthed while building this
// gate; every restore assertion below silently lost its extension delta
// until the bound came out. A count bound here would make the harness, not
// the product, decide whether the cells mean anything.
//
// xvIncrementalWindow is the whole cost of a segment (the run sits out its
// window), so it is the suite's dominant wall-clock term.
func xvBackupIncremental(t *testing.T, bin, srcDSN, dir, slot string) {
	t.Helper()
	args := append([]string{
		"backup", "incremental",
		"--source-driver", "postgres", "--source", srcDSN,
		"--output-dir", dir,
		"--slot-name", slot,
		"--window", xvIncrementalWindow,
		"--max-changes", "0",
	}, xvEncryptFlags()...)
	xvMustExec(t, "backup incremental", bin, args...)
}

// xvVerify runs `backup verify` over the same directory a cell restores,
// so a cell can assert the two AGREE. Bug 217: verify skipped the
// schema-fingerprint preflight restore runs, so it returned rc=0 over
// epoch-crossing chains restore refuses — the trust signal an operator
// runs on a schedule saying healthy about a chain that cannot be
// recovered. No target: verify reads the store and nothing else.
func xvVerify(t *testing.T, bin, dir string) (string, error) {
	t.Helper()
	args := append([]string{"backup", "verify", "--from-dir", dir}, xvEncryptFlags()...)
	return xvExec(t, bin, args...)
}

func xvRestore(t *testing.T, bin, dir, targetDSN string) (string, error) {
	t.Helper()
	args := append([]string{
		"restore",
		"--from-dir", dir,
		"--target-driver", "postgres", "--target", targetDSN,
	}, xvEncryptFlags()...)
	return xvExec(t, bin, args...)
}

// ---------------------------------------------------------------------
// store + database inspection
// ---------------------------------------------------------------------

// xvManifestVersion reads the format_version off a manifest JSON file.
// Decoded through an anonymous struct rather than irbackup.Manifest on
// purpose: this is a WIRE assertion about bytes on disk written by a
// binary that is not this build, so it must not ride the current build's
// type to interpret them.
func xvManifestVersion(t *testing.T, path string) int {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read manifest %s: %v", path, err)
	}
	var m struct {
		FormatVersion int `json:"format_version"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("decode manifest %s: %v", path, err)
	}
	return m.FormatVersion
}

// xvSchemaFingerprint decodes a manifest written by ANOTHER binary and
// returns both the fingerprint that binary recorded and the fingerprint
// THIS build computes for the very same carried schema. Equal means the
// two binaries share a schema-fingerprint epoch; unequal means they do
// not, and that is the entire precondition cell 5 rests on.
//
// This is the ONE place in the suite that deliberately reads a foreign
// binary's bytes through the CURRENT build's types, and the deliberation
// matters. Everywhere else that would be a bug (an assertion about bytes
// on disk must not ride the type interpreting them — see
// xvManifestVersion). Here the question IS "what does this build make of
// those bytes", so the current build's decoder is the instrument, not a
// contaminant.
//
// It measures the right thing, which an obvious alternative does not: a
// second full taken by the NEW binary and its recorded hash compared
// would conflate a fingerprint-epoch boundary with ordinary SCHEMA-READER
// drift (a newer reader populating a field the older one left empty
// changes the fingerprint of a fresh read while old manifests still
// reproduce perfectly). Ground-truthed while building this cell: v0.103.2
// and the working tree disagree about a fresh read of one database and
// still restore each other's chains. The recompute-what-was-recorded form
// is the predicate the refusal actually keys on.
func xvSchemaFingerprint(t *testing.T, path string) (recorded, recomputed string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read manifest %s: %v", path, err)
	}
	var m irbackup.Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("decode manifest %s: %v", path, err)
	}
	if m.SchemaHash == "" {
		t.Fatalf("manifest %s records no schema_hash — the fingerprint check skips such links, so cell 5 would be vacuous", path)
	}
	got, err := irbackup.ComputeSchemaHash(m.Schema)
	if err != nil {
		t.Fatalf("recompute schema hash for %s: %v", path, err)
	}
	return m.SchemaHash, got
}

// xvChainCarriesNamedConstraint reports whether the chain's root manifest
// records at least one index whose name came from a real source constraint
// — [ir.Index.ConstraintNamed], the field whose fingerprint treatment is
// cell 6's whole subject.
//
// Decoding through the CURRENT build's type is correct here, unlike in
// xvManifestVersion: the manifest under inspection was written by THIS
// build, so there is no foreign shape to misread. The question is simply
// whether the writer produced the shape the cell needs.
func xvChainCarriesNamedConstraint(t *testing.T, dir string) bool {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var m irbackup.Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if m.Schema == nil {
		t.Fatalf("manifest carries no schema — cell 6 has nothing to inspect")
	}
	for _, tbl := range m.Schema.Tables {
		if tbl == nil {
			continue
		}
		if tbl.PrimaryKey != nil && tbl.PrimaryKey.ConstraintNamed {
			return true
		}
		for _, idx := range tbl.Indexes {
			if idx != nil && idx.ConstraintNamed {
				return true
			}
		}
	}
	return false
}

// xvChainVersions returns the root full's format version and the
// incremental segments' versions in path order.
func xvChainVersions(t *testing.T, dir string) (root int, segments []int) {
	t.Helper()
	root = xvManifestVersion(t, filepath.Join(dir, "manifest.json"))
	paths, err := filepath.Glob(filepath.Join(dir, "manifests", "incr-*.json"))
	if err != nil {
		t.Fatalf("glob incremental manifests: %v", err)
	}
	sort.Strings(paths)
	for _, p := range paths {
		segments = append(segments, xvManifestVersion(t, p))
	}
	if len(segments) == 0 {
		t.Fatalf("chain at %s has no incremental segments — the cell wrote a bare full, "+
			"so it is not exercising the chain contract at all", dir)
	}
	return root, segments
}

// xvUser is one row of the shared chainSeedDDL `users` table.
type xvUser struct {
	ID     int64
	Email  string
	Active bool
}

func xvUsers(t *testing.T, dsn string) []xvUser {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open %s: %v", dsn, err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, "SELECT id, email, active FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("select users: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var out []xvUser
	for rows.Next() {
		var u xvUser
		if err := rows.Scan(&u.ID, &u.Email, &u.Active); err != nil {
			t.Fatalf("scan users: %v", err)
		}
		out = append(out, u)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate users: %v", err)
	}
	return out
}

// xvAssertUsers pins the restored CONTENT, not just the exit code. A
// restore that succeeds while landing the wrong rows is the silent-loss
// shape; an exit-code assertion would sail straight past it.
func xvAssertUsers(t *testing.T, dsn, what string, want []xvUser) {
	t.Helper()
	got := xvUsers(t, dsn)
	if len(got) != len(want) {
		t.Fatalf("%s: restored %d rows, want %d\n got: %+v\nwant: %+v", what, len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: row %d = %+v, want %+v\n got: %+v\nwant: %+v", what, i, got[i], want[i], got, want)
		}
	}
}

// xvPublicRelationCount counts the relations a restore would have created
// in the target's public schema. A refused restore must leave this at zero:
// the format-version gate sits at the manifest preflight, ahead of anything
// that touches the target, and "refused loudly" is only half the contract
// if the target is left half-built.
func xvPublicRelationCount(t *testing.T, dsn string) int64 {
	t.Helper()
	return pgQueryOne[int64](t, dsn,
		`SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE n.nspname = 'public' AND c.relkind IN ('r','p','v','m','S')`)
}

// xvAssertVersionRefusal pins the whole refusal contract: it failed, it
// said why in the operator's words, it did NOT blame the passphrase, and
// the target is untouched.
func xvAssertVersionRefusal(t *testing.T, what, out string, err error, targetDSN string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: the older binary RESTORED a chain stamped above its BackupFormatVersion — "+
			"the forward-compat refusal is gone, which means older binaries now read manifests they "+
			"cannot correctly interpret.\n--- output ---\n%s", what, out)
	}
	if !strings.Contains(out, xvFormatVersionRefusal) {
		t.Fatalf("%s: refused (%v) but never said %q — the operator cannot tell a version gap from corruption.\n--- output ---\n%s",
			what, err, xvFormatVersionRefusal, out)
	}
	lower := strings.ToLower(out)
	for _, term := range xvMisleadingRefusalTerms {
		if strings.Contains(lower, term) {
			t.Fatalf("%s: the refusal blames %q. The passphrase is CORRECT here; this is a format-version gap. "+
				"An operator handed the wrong cause rotates keys instead of upgrading the binary.\n--- output ---\n%s",
				what, term, out)
		}
	}
	if n := xvPublicRelationCount(t, targetDSN); n != 0 {
		t.Fatalf("%s: refused, but the target already carries %d relation(s) in public — the version gate must "+
			"sit at the manifest preflight, ahead of anything that touches the target", what, n)
	}
}

// xvAssertSchemaFingerprintRefusal pins cell 5's whole contract: the
// restore failed, it failed on the FINGERPRINT (not the format version —
// a version refusal here would mean the cell asserted a different
// mechanism), the message names both causes and the writing release, it
// does NOT accuse the operator of tampering, it does not blame the
// passphrase, and the target is untouched.
func xvAssertSchemaFingerprintRefusal(t *testing.T, what, out string, err error, targetDSN, writerVersion string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: the chain RESTORED across a schema-fingerprint epoch. That is not a failure of this cell — it "+
			"means the partition roadmap item 102 documents has been closed (a compatibility shim, or a re-derivation "+
			"of the fingerprint at the writing release). Update this cell to assert the NEW contract.\n--- output ---\n%s",
			what, out)
	}
	if strings.Contains(out, xvFormatVersionRefusal) {
		t.Fatalf("%s: refused on the FORMAT VERSION, not the schema fingerprint — this cell asserts the fingerprint "+
			"partition and the version gate preempted it. Pick an epoch tag whose BackupFormatVersion this build "+
			"reads.\n--- output ---\n%s", what, out)
	}
	for _, want := range []string{xvSchemaFingerprintRefusal, xvSchemaFingerprintIntact, xvSchemaFingerprintCorrupt} {
		if !strings.Contains(out, want) {
			t.Fatalf("%s: refused (%v) but never said %q — the refusal must name BOTH causes, because it cannot tell "+
				"them apart.\n--- output ---\n%s", what, err, want, out)
		}
	}
	if !strings.Contains(out, "written by sluice "+writerVersion) {
		t.Fatalf("%s: the refusal does not name the release that WROTE the chain (%s). That version is the only datum "+
			"an operator can act on — it is what tells them the chain is probably intact and which binary reads "+
			"it.\n--- output ---\n%s", what, writerVersion, out)
	}
	if strings.Contains(out, xvTamperAccusation) {
		t.Fatalf("%s: the refusal still says %q about a chain this harness wrote and never touched. An operator told "+
			"their backup is corrupt, mid-recovery, goes looking for a storage fault that is not there.\n--- output ---\n%s",
			what, xvTamperAccusation, out)
	}
	lower := strings.ToLower(out)
	for _, term := range xvMisleadingRefusalTerms {
		if strings.Contains(lower, term) {
			t.Fatalf("%s: the refusal blames %q. The passphrase is CORRECT here; this is a fingerprint-epoch "+
				"gap.\n--- output ---\n%s", what, term, out)
		}
	}
	if n := xvPublicRelationCount(t, targetDSN); n != 0 {
		t.Fatalf("%s: refused, but the target already carries %d relation(s) in public — the fingerprint check must sit "+
			"at the manifest preflight, ahead of anything that touches the target", what, n)
	}
}

// ---------------------------------------------------------------------
// fixture
// ---------------------------------------------------------------------

// The post-full deltas. Each is a single multi-statement Exec, so it lands
// as ONE transaction and is fully committed before the incremental that
// captures it opens its window.
const (
	// xvDelta1: insert, update, delete — all three DML shapes, so a
	// change-replay that mishandles one is caught by the row assertion.
	xvDelta1 = `
		INSERT INTO users (id, email, active) VALUES (4, 'dave@example.com', true);
		UPDATE users SET active = false WHERE id = 1;
		DELETE FROM users WHERE id = 3;
	`

	// xvDelta2: the EXTENSION window's delta. Distinct rows from xvDelta1
	// (a new id, and an update to a row xvDelta1 left alone) so "the
	// extension's changes landed" is decidable from the restored rows
	// alone — a replay of xvDelta1 in xvDelta2's place is idempotent and
	// would otherwise be indistinguishable from success.
	xvDelta2 = `
		INSERT INTO users (id, email, active) VALUES (5, 'erin@example.com', true);
		UPDATE users SET active = false WHERE id = 2;
	`
)

// xvAfterDelta1 / xvAfterDelta2 are the exact row sets a correct restore
// lands, seeded rows folded through each delta.
var (
	xvAfterDelta1 = []xvUser{
		{1, "alice@example.com", false},
		{2, "bob@example.com", true},
		{4, "dave@example.com", true},
	}
	xvAfterDelta2 = []xvUser{
		{1, "alice@example.com", false},
		{2, "bob@example.com", false},
		{4, "dave@example.com", true},
		{5, "erin@example.com", true},
	}
)

// xvCreateDB creates a database in the running container and returns its
// DSN. Every cell gets its own source (so it can own a replication slot)
// and its own target(s) (so an "untouched" assertion means what it says).
func xvCreateDB(t *testing.T, adminDSN, name string) string {
	t.Helper()
	db, err := sql.Open("pgx", adminDSN)
	if err != nil {
		t.Fatalf("open admin: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	dsn, err := buildPGDSN(adminDSN, name)
	if err != nil {
		t.Fatalf("build DSN for %s: %v", name, err)
	}
	return dsn
}

// xvSeededSource creates a source database, applies the shared seed, and
// returns its DSN.
func xvSeededSource(t *testing.T, adminDSN, name string) string {
	t.Helper()
	dsn := xvCreateDB(t, adminDSN, name)
	applyDDL(t, dsn, chainSeedDDL)
	return dsn
}

// ---------------------------------------------------------------------
// the gate
// ---------------------------------------------------------------------

func TestBackup_CrossVersionChainCompat(t *testing.T) {
	bins := xvLoadBinaries(t)

	// One container, one database per role. Slots are cluster-wide, so each
	// cell's slot name is distinct; publications are per-database, so the
	// default name is fine.
	adminDSN, _, cleanup := startPostgresLogical(t)
	defer cleanup()

	ledger := newXVLedger(t, "1-forward-read", "2-backward-refusal", "3a-new-reads-extended", "3b-old-loses-extended", "4-control", "5-epoch-partition", "6-fingerprint-forward-compat")

	t.Run("cell1_forward_read_OLD_writes_NEW_restores", func(t *testing.T) {
		src := xvSeededSource(t, adminDSN, "xv1_src")
		tgt := xvCreateDB(t, adminDSN, "xv1_tgt")
		dir := t.TempDir()

		xvBackupFull(t, bins.oldBin, src, dir, "xv_slot_1")
		applyDDL(t, src, xvDelta1)
		xvBackupIncremental(t, bins.oldBin, src, dir, "xv_slot_1")

		root, segs := xvChainVersions(t, dir)
		if root > bins.oldFormat {
			t.Fatalf("OLD %s wrote a root manifest stamped %d, above its own BackupFormatVersion %d — impossible; the harness is not running the binary it thinks it is",
				bins.oldTag, root, bins.oldFormat)
		}
		t.Logf("cell 1: OLD chain stamped root=%d segments=%v", root, segs)

		out, err := xvRestore(t, bins.newBin, dir, tgt)
		if err != nil {
			t.Fatalf("NEW binary failed to restore an OLD chain — the always-reads-older half of the "+
				"forward-compat contract is broken: %v\n--- output ---\n%s", err, out)
		}
		xvAssertUsers(t, tgt, "cell 1 (NEW restores OLD chain)", xvAfterDelta1)
		ledger.report(t, "1-forward-read")
	})

	t.Run("cell2_backward_refusal_NEW_writes_OLD_refuses", func(t *testing.T) {
		src := xvSeededSource(t, adminDSN, "xv2_src")
		tgt := xvCreateDB(t, adminDSN, "xv2_tgt")
		dir := t.TempDir()

		xvBackupFull(t, bins.newBin, src, dir, "xv_slot_2")
		applyDDL(t, src, xvDelta1)
		xvBackupIncremental(t, bins.newBin, src, dir, "xv_slot_2")

		root, segs := xvChainVersions(t, dir)
		if root <= bins.oldFormat {
			t.Fatalf("NEW wrote a root manifest stamped %d, which OLD (%d) can read — this cell cannot "+
				"assert a refusal it will never see. An encrypted full must stamp the current tier; "+
				"check that --encrypt actually engaged.", root, bins.oldFormat)
		}
		t.Logf("cell 2: NEW chain stamped root=%d segments=%v", root, segs)

		out, err := xvRestore(t, bins.oldBin, dir, tgt)
		xvAssertVersionRefusal(t, "cell 2 (OLD restores NEW chain)", out, err, tgt)
		ledger.report(t, "2-backward-refusal")
	})

	// Cell 3 — the reason this gate exists.
	//
	// OLD roots the chain and takes the first incremental; NEW then extends
	// it with an ordinary `backup incremental`, which is the single most
	// ordinary thing an operator does to a chain. The extension is stamped
	// at NEW's tier (an encrypted incremental's chunks are freshly SEALED,
	// so they carry the current AAD encoding — see incremental.go's stamp),
	// which raises the chain's readability floor for EVERY link.
	//
	// 3b is a KNOWN, ACCEPTED consequence (roadmap item 90 / Bug 212), NOT
	// a bug this gate is waiting to see fixed. Sealing the extension under
	// the retired encoding to keep OLD happy would defeat the bump. What was
	// missing was visibility, and this is it: if 3b ever starts PASSING, or
	// starts failing in a different SHAPE (a mid-restore failure instead of
	// a preflight refusal, a half-built target, a refusal blaming the
	// passphrase), this cell goes red and someone looks.
	t.Run("cell3_bug212_OLD_chain_extended_by_NEW", func(t *testing.T) {
		src := xvSeededSource(t, adminDSN, "xv3_src")
		tgtNew := xvCreateDB(t, adminDSN, "xv3_tgt_new")
		tgtOld := xvCreateDB(t, adminDSN, "xv3_tgt_old")
		dir := t.TempDir()

		xvBackupFull(t, bins.oldBin, src, dir, "xv_slot_3")
		applyDDL(t, src, xvDelta1)
		xvBackupIncremental(t, bins.oldBin, src, dir, "xv_slot_3")

		// The extension, by the NEW binary.
		applyDDL(t, src, xvDelta2)
		xvBackupIncremental(t, bins.newBin, src, dir, "xv_slot_3")

		// The THIRD and sharpest non-vacuity guard, and the mechanism stated
		// as an assertion: one chain, an OLD-written root and a NEW-written
		// extension, and the extension must record a strictly higher version.
		// If it does not, cells 3a/3b below are asserting nothing.
		root, segs := xvChainVersions(t, dir)
		if len(segs) < 2 {
			t.Fatalf("cell 3: chain carries %d incremental segment(s), want 2 (OLD's, then NEW's extension)", len(segs))
		}
		ext := segs[len(segs)-1]
		if ext <= root {
			t.Fatalf("cell 3: the NEW binary's extension is stamped %d and the OLD root is stamped %d — "+
				"the extension did not raise the chain's floor, so this cell is vacuous. Either the two "+
				"binaries agree on the format version (check CROSSVER_OLD_TAG=%s) or --encrypt did not engage.",
				ext, root, bins.oldTag)
		}
		if ext <= bins.oldFormat {
			t.Fatalf("cell 3: the extension is stamped %d, which OLD (%d) can still read — cell 3b would "+
				"assert a refusal that cannot happen", ext, bins.oldFormat)
		}
		t.Logf("cell 3: chain floor raised — root=%d segments=%v (OLD reads up to %d)", root, segs, bins.oldFormat)

		t.Run("a_NEW_restores_the_extended_chain", func(t *testing.T) {
			out, err := xvRestore(t, bins.newBin, dir, tgtNew)
			if err != nil {
				t.Fatalf("NEW binary failed to restore the chain it extended: %v\n--- output ---\n%s", err, out)
			}
			xvAssertUsers(t, tgtNew, "cell 3a (NEW restores the extended chain)", xvAfterDelta2)
			ledger.report(t, "3a-new-reads-extended")
		})

		t.Run("b_OLD_loses_the_whole_chain", func(t *testing.T) {
			out, err := xvRestore(t, bins.oldBin, dir, tgtOld)
			// KNOWN + ACCEPTED (item 90 / Bug 212): OLD wrote the root and the
			// first segment itself, understands both perfectly, and still
			// restores NOTHING — because the walk refuses at the newer
			// extension. Cell 4 is the control that proves the version raise,
			// and not the harness, is what does this.
			xvAssertVersionRefusal(t, "cell 3b (OLD restores the chain NEW extended)", out, err, tgtOld)
			ledger.report(t, "3b-old-loses-extended")
		})
	})

	// Cell 4 — the control. Byte-for-byte the same shape as cell 3, with the
	// extension taken by OLD instead of NEW. It must PASS. Without it, cell
	// 3b's failure is only evidence that SOMETHING in a two-incremental
	// chain restore breaks; with it, the version raise is the only variable
	// left standing.
	t.Run("cell4_control_OLD_writes_extends_restores", func(t *testing.T) {
		src := xvSeededSource(t, adminDSN, "xv4_src")
		tgt := xvCreateDB(t, adminDSN, "xv4_tgt")
		dir := t.TempDir()

		xvBackupFull(t, bins.oldBin, src, dir, "xv_slot_4")
		applyDDL(t, src, xvDelta1)
		xvBackupIncremental(t, bins.oldBin, src, dir, "xv_slot_4")
		applyDDL(t, src, xvDelta2)
		xvBackupIncremental(t, bins.oldBin, src, dir, "xv_slot_4")

		root, segs := xvChainVersions(t, dir)
		if len(segs) < 2 {
			t.Fatalf("cell 4: chain carries %d incremental segment(s), want 2", len(segs))
		}
		for _, v := range append([]int{root}, segs...) {
			if v > bins.oldFormat {
				t.Fatalf("cell 4: OLD %s stamped %d, above its own BackupFormatVersion %d — impossible; "+
					"the control is not running the OLD binary", bins.oldTag, v, bins.oldFormat)
			}
		}
		t.Logf("cell 4: OLD-only chain stamped root=%d segments=%v", root, segs)

		out, err := xvRestore(t, bins.oldBin, dir, tgt)
		if err != nil {
			t.Fatalf("CONTROL FAILED: OLD could not restore a chain it wrote entirely by itself. "+
				"Cell 3b's failure can no longer be attributed to the format-version raise — fix the "+
				"harness before reading anything into cell 3.\n%v\n--- output ---\n%s", err, out)
		}
		xvAssertUsers(t, tgt, "cell 4 (OLD writes, extends, restores)", xvAfterDelta2)
		ledger.report(t, "4-control")
	})

	// Cell 5 — the epoch axis (roadmap item 102).
	//
	// A chain written by a binary from a DIFFERENT schema-fingerprint
	// epoch, restored by the working tree. The known, accepted outcome is
	// a REFUSAL: the manifest's recorded schema fingerprint does not
	// reproduce under this binary's IR field set, and there is no
	// re-derivation path, so the chain — byte-for-byte intact — is
	// unreadable here. Cell 5 asserts that refusal and, just as
	// importantly, its WORDING: it must name the release-skew cause and
	// the writing release, and must not tell the operator their store is
	// corrupt.
	//
	// A BARE FULL crosses epochs fine (the single-manifest restore path
	// never walks a chain and never runs the check), which is why this
	// cell takes an incremental — the shape that actually partitions.
	t.Run("cell5_epoch_partition_EPOCH_writes_NEW_refuses", func(t *testing.T) {
		src := xvSeededSource(t, adminDSN, "xv5_src")
		tgt := xvCreateDB(t, adminDSN, "xv5_tgt")
		dir := t.TempDir()

		xvBackupFull(t, bins.epochBin, src, dir, "xv_slot_5")
		applyDDL(t, src, xvDelta1)
		xvBackupIncremental(t, bins.epochBin, src, dir, "xv_slot_5")

		// THE NON-VACUITY GUARD, and the reason this cell can be trusted
		// without trusting a tag NAME. The two binaries are in different
		// fingerprint epochs exactly when this build fails to reproduce the
		// fingerprint the epoch binary recorded with its own schema. Equal
		// means one epoch: the refusal below could not fire for the reason
		// this cell claims, so it is a FAILURE, not a pass. Same class of
		// guard as cell 3's "the extension must raise the floor".
		recorded, recomputed := xvSchemaFingerprint(t, filepath.Join(dir, "manifest.json"))
		if recorded == recomputed {
			t.Fatalf("cell 5: this build reproduces EPOCH %s's recorded fingerprint (%s) — the two are in the SAME "+
				"schema-fingerprint epoch, so this cell asserts nothing and a green here would be vacuous. Either the "+
				"epoch tag in scripts/crossversion-build.sh needs repointing at a tag on the far side of a boundary, "+
				"or the epochs have been unified (in which case this cell needs rewriting, not repointing).",
				bins.epochTag, recorded)
		}
		t.Logf("cell 5: epochs confirmed distinct — EPOCH %s recorded %s, this build recomputes %s",
			bins.epochTag, recorded, recomputed)

		// The chain must be a CHAIN (a bare full legitimately crosses), and
		// every link must be readable by NEW's format-version gate so the
		// refusal below is the fingerprint's and not the version's.
		root, segs := xvChainVersions(t, dir)
		for _, v := range append([]int{root}, segs...) {
			if v > bins.newFormat {
				t.Fatalf("cell 5: EPOCH %s stamped a manifest at %d, above NEW's BackupFormatVersion %d — the version "+
					"gate would preempt the fingerprint check", bins.epochTag, v, bins.newFormat)
			}
		}
		t.Logf("cell 5: EPOCH chain stamped root=%d segments=%v (NEW reads up to %d)", root, segs, bins.newFormat)

		out, err := xvRestore(t, bins.newBin, dir, tgt)
		xvAssertSchemaFingerprintRefusal(t, "cell 5 (NEW restores an EPOCH-crossing chain)", out, err, tgt, bins.xvEpochVersion())

		// AND verify must say the same thing (Bug 217). This is the one
		// place in the suite where a chain restore refuses is already
		// built, so it is the cheapest place to hold verify to it: for
		// as long as verify skipped the fingerprint preflight, it
		// returned rc=0 `all chunks OK` right here — every chunk really
		// was intact, and the chain really was unrestorable, which is
		// precisely the disagreement v0.104.3's headline existed to
		// close one preflight over.
		vout, verr := xvVerify(t, bins.newBin, dir)
		if verr == nil {
			t.Fatalf("cell 5: `backup verify` returned CLEAN over the same epoch-crossing chain restore just refused. "+
				"An operator who runs verify on a schedule would be told this chain is healthy and discover otherwise "+
				"during a recovery — see Bug 217.\n--- verify output ---\n%s", vout)
		}
		if !strings.Contains(vout, "schema") {
			t.Errorf("cell 5: verify refused, but its output does not name the schema fingerprint, so an operator cannot "+
				"tell this refusal from chunk corruption:\n%s", vout)
		}
		ledger.report(t, "5-epoch-partition")
	})

	// Cell 6 — the fingerprint-forward axis (roadmap item 104 / Bug 216).
	//
	// THIS build writes a chain from a primary-keyed Postgres source; the
	// newest release at the same format version restores it. That must
	// PASS, and unlike every cell above it, it is a cell whose failure is a
	// BUG rather than a documented consequence: the two binaries agree
	// about the manifest format, so the only thing left that can refuse is
	// a schema fingerprint this build moved.
	//
	// The source is the shared seed, unmodified, because the shape that
	// partitions is not exotic — `id BIGINT PRIMARY KEY` is enough. Postgres
	// auto-names that constraint, the reader records the name as
	// constraint-owned, and that is the field v0.104.3 folded into the
	// fingerprint. An operator did not have to do anything unusual to be
	// stranded.
	t.Run("cell6_fingerprint_forward_NEW_writes_PEER_restores", func(t *testing.T) {
		src := xvSeededSource(t, adminDSN, "xv6_src")
		tgt := xvCreateDB(t, adminDSN, "xv6_tgt")
		dir := t.TempDir()

		xvBackupFull(t, bins.newBin, src, dir, "xv_slot_6")
		applyDDL(t, src, xvDelta1)
		xvBackupIncremental(t, bins.newBin, src, dir, "xv_slot_6")

		// THE NON-VACUITY GUARD, and it is the whole lesson of Bug 216 in
		// one assertion. The pre-release verification of the omitempty fix
		// hashed a schema in which the field was ABSENT, measured
		// byte-identical, and shipped a partition. So: prove the chain this
		// cell just wrote actually CARRIES the constraint-named shape before
		// reading anything into the restore below. If a future reader change
		// stops setting it, this cell would keep passing while covering
		// nothing — that is the failure this guard exists to convert into a
		// red run.
		if !xvChainCarriesNamedConstraint(t, dir) {
			t.Fatalf("cell 6: NEW wrote a chain whose schema records no constraint-owned index name — the shape that " +
				"partitioned the fingerprint in v0.104.3 is absent, so a green here would prove nothing. The seed's " +
				"`id BIGINT PRIMARY KEY` should make the PG reader set ir.Index.ConstraintNamed; check the reader " +
				"before relaxing this guard.")
		}

		// And the chain must be a chain the PEER can reach the fingerprint
		// check on at all — cell 2's refusal shape must not be what we end
		// up measuring.
		root, segs := xvChainVersions(t, dir)
		for _, v := range append([]int{root}, segs...) {
			if v > bins.peerFormat {
				t.Fatalf("cell 6: NEW stamped a manifest at %d, above PEER %s's BackupFormatVersion %d — PEER would "+
					"refuse on the VERSION gate and this cell would be measuring cell 2. The peer derivation is "+
					"supposed to make that impossible; see scripts/crossversion-build.sh.", v, bins.peerTag, bins.peerFormat)
			}
		}
		t.Logf("cell 6: NEW chain stamped root=%d segments=%v (PEER %s reads up to %d)", root, segs, bins.peerTag, bins.peerFormat)

		out, err := xvRestore(t, bins.peerBin, dir, tgt)
		if err != nil {
			t.Fatalf("cell 6: PEER %s could not restore a chain THIS build wrote from an ordinary primary-keyed "+
				"Postgres source. That is a fingerprint partition shipping — every chain written by this build "+
				"becomes unrestorable by the release an operator is most likely to still be running, and it surfaces "+
				"to them as a refused (not degraded) restore. See roadmap item 104 / Bug 216, and the three answers "+
				"in the block comment on goldenSchemaHash (internal/ir/backup/schema_hash_golden_test.go).\n"+
				"%v\n--- output ---\n%s", bins.peerTag, err, out)
		}
		xvAssertUsers(t, tgt, "cell 6 (PEER restores a NEW-written chain)", xvAfterDelta1)
		ledger.report(t, "6-fingerprint-forward-compat")
	})

	// Belt for the ledger's braces: a summary line naming what ran, so a
	// human reading the CI log sees the four cells rather than inferring
	// them from subtest names.
	t.Logf("cross-version gate: %d/%d cells reported", len(ledger.done), len(ledger.expected))
}
