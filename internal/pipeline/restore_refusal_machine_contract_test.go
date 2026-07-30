// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The machine contract of a chain-restore refusal (Bug 219): for every
// shape that refuses, `restore` and `backup verify` must report the SAME
// (exit status, SLUICE-E-*) pair over the SAME directory.
//
// The bug: the FORKED-lineage refusal — two overlapping `backup
// incremental` crons each committing off one parent, the shape that
// actually happens in the field — was the one shape of
// SLUICE-E-BACKUP-MANIFEST-INVALID that `restore` raised with exit 1 and
// NO code at all, while `backup verify` on the identical directory exited
// 3 with the code and `restore` itself exited 3 with that same code for
// the code's OTHER two shapes (schema-hash mismatch, BackupID recompute).
// One code, two machine contracts on one command, keyed on which shape
// raised it. docs/operator/error-codes.md classes that code as a refusal,
// documents `3 | Named refusal`, and tells operators in as many words to
// stop checking `exit == 1` — so a DR script that followed the docs could
// not detect the exact refusal the release existed to surface. Loud to a
// human (the prose is excellent and is deliberately untouched), invisible
// to a script, zero rows landed.
//
// Why a table over shapes rather than a test for the fork: the fork was
// one of EIGHT refusals the lineage walk can raise, all siblings of one
// another, and a single-shape test is what let this sit through three
// releases — every other shape of the same code passed. So the pin is the
// family: each structural shape the walk refuses, × both commands, on the
// (status, code) PAIR. The status half is the half a DR script keys on and
// the half that was wrong, so it is asserted, not inferred from the code.

package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// exitStatusLikeKong mirrors kong's exitCodeFromError — the mapping
// cmd/sluice/main.go's ctx.FatalIfErrorf actually runs at the process exit
// boundary: the outermost error in the chain implementing
// `interface{ ExitCode() int }` (kong.ExitCoder, structurally) wins; no
// ExitCoder means the traditional 1. Reimplemented rather than imported
// because kong's version is unexported and the pipeline package has no
// business importing the CLI parser; cmd/sluice/exitcode_test.go carries
// the same mirror against kong.ExitCoder itself.
func exitStatusLikeKong(err error) int {
	var ec interface{ ExitCode() int }
	if errors.As(err, &ec) {
		return ec.ExitCode()
	}
	if err == nil {
		return sluicecode.ExitSuccess
	}
	return sluicecode.ExitFailure
}

// chainFixture is one on-disk backup directory: a healthy full +
// incremental chain that the shape function may mutate before the catalog
// is written.
type chainFixture struct {
	full  *irbackup.Manifest
	incrs []*irbackup.Manifest
	cat   *lineage.Catalog
}

// buildChainFixture writes a full + the supplied incrementals into a fresh
// local store and commits a lineage catalog describing them. mutate runs
// after the manifests are built and BEFORE anything is written, so a shape
// can tamper with a manifest, an incremental list, or the catalog itself.
//
// The chain deliberately carries no chunks: every shape here refuses (or
// passes) on manifest STRUCTURE, before a byte of data is read, which is
// also why `backup verify` can score the identical directory with no
// target and no key material.
func buildChainFixture(t *testing.T, incrCount int, mutate func(f *chainFixture)) irbackup.Store {
	t.Helper()
	ctx := context.Background()
	store, _ := blobcodec.NewLocalStore(t.TempDir())

	f := &chainFixture{full: makeManifest(t, irbackup.BackupKindFull, nil, "0/100")}
	f.full.SchemaHash = mustSchemaHash(t, f.full.Schema)
	f.full.BackupID = irbackup.ComputeBackupID(f.full)
	parent := f.full
	for i := 0; i < incrCount; i++ {
		// A no-chunk incremental must NOT advance its EndPosition — the
		// real writer records the last change's position and a 0-change
		// window never advances it (Bug 183 refuses the advanced shape).
		incr := makeManifest(t, irbackup.BackupKindIncremental, parent, "0/100")
		incr.CreatedAt = incr.CreatedAt.Add(time.Duration(i) * time.Minute)
		incr.SchemaHash = mustSchemaHash(t, incr.Schema)
		incr.BackupID = irbackup.ComputeBackupID(incr)
		f.incrs = append(f.incrs, incr)
		parent = incr
	}
	if mutate != nil {
		mutate(f)
	}
	seg := seedSegment(t, store, "", f.full, f.incrs, blobcodec.CodecGzip)
	if f.cat == nil {
		f.cat = &lineage.Catalog{FormatVersion: 1, SourceEngine: "postgres"}
	}
	f.cat.Segments = []lineage.Segment{seg}
	if err := lineage.WriteLineageCatalog(ctx, store, f.cat); err != nil {
		t.Fatalf("write lineage catalog: %v", err)
	}
	return store
}

func mustSchemaHash(t *testing.T, schema *ir.Schema) string {
	t.Helper()
	h, err := irbackup.ComputeSchemaHash(schema)
	if err != nil {
		t.Fatalf("ComputeSchemaHash: %v", err)
	}
	return h
}

// TestRestoreRefusalMachineContract_MatchesVerify is the Bug-219 gate.
//
// Every row drives the REAL restore path (backup.ChainRestore.Run, which
// `sluice restore` dispatches into) — not `backup verify`, and not the
// lineage walk in isolation. A gate that only grepped for the code string
// would have passed on the verify path while restore stayed broken, which
// is precisely the asymmetry the bug is.
func TestRestoreRefusalMachineContract_MatchesVerify(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name string
		// store builds the directory both commands are scored against.
		store func(t *testing.T) irbackup.Store
		// want is the (code, exit) pair BOTH commands must report. There is
		// deliberately no per-command override: "the two answers are the
		// same pair" IS the contract this gate exists to hold, so a shape
		// that needs one is a finding, not a table entry.
		want refusalPair
	}{
		{
			// Non-vacuity: the coded rows below must be shape-driven, so a
			// well-formed chain has to restore CLEAN through the same call.
			name:  "healthy chain restores, no code, exit 0",
			store: func(t *testing.T) irbackup.Store { return buildChainFixture(t, 1, nil) },
			want:  refusalPair{exit: sluicecode.ExitSuccess},
		},
		{
			// THE BUG. Two incrementals both parented on the full: the
			// on-disk footprint of two concurrent chain writers.
			name: "forked lineage (two incrementals off one parent)",
			store: func(t *testing.T) irbackup.Store {
				return buildChainFixture(t, 2, func(f *chainFixture) {
					f.incrs[1].ParentBackupID = f.full.BackupID
					f.incrs[1].BackupID = irbackup.ComputeBackupID(f.incrs[1])
				})
			},
			want: manifestInvalidRefusal,
		},
		{
			// Regression pin: this shape was already correct. It is here so
			// the three shapes of ONE code are pinned to behave alike —
			// the property the bug violated.
			name: "schema_hash mismatch",
			store: func(t *testing.T) irbackup.Store {
				return buildChainFixture(t, 1, func(f *chainFixture) {
					f.incrs[0].SchemaHash = "0000000000000000000000000000000000000000000000000000000000000000"
					f.incrs[0].BackupID = irbackup.ComputeBackupID(f.incrs[0])
				})
			},
			want: manifestInvalidRefusal,
		},
		{
			// Regression pin (the other already-correct shape): a
			// BackupID-covered field edited without recomputing the id.
			name: "BackupID-covered field edited without recompute",
			store: func(t *testing.T) irbackup.Store {
				return buildChainFixture(t, 1, func(f *chainFixture) {
					f.incrs[0].CreatedAt = f.incrs[0].CreatedAt.Add(time.Hour)
				})
			},
			want: manifestInvalidRefusal,
		},
		{
			// The remaining siblings of the walk's refusal family. Each was
			// uncoded exit-1 on restore for the same reason the fork was.
			name: "duplicate incremental BackupID",
			store: func(t *testing.T) irbackup.Store {
				return buildChainFixture(t, 2, func(f *chainFixture) {
					f.incrs[1].CreatedAt = f.incrs[0].CreatedAt
					f.incrs[1].ParentBackupID = f.incrs[0].BackupID
					f.incrs[1].BackupID = irbackup.ComputeBackupID(f.incrs[1])
				})
			},
			want: manifestInvalidRefusal,
		},
		{
			name: "incremental manifest labelled full",
			store: func(t *testing.T) irbackup.Store {
				return buildChainFixture(t, 1, func(f *chainFixture) {
					f.incrs[0].Kind = irbackup.BackupKindFull
					f.incrs[0].BackupID = irbackup.ComputeBackupID(f.incrs[0])
				})
			},
			want: manifestInvalidRefusal,
		},
		{
			name: "boundary mismatch (tampered StartPosition)",
			store: func(t *testing.T) irbackup.Store {
				return buildChainFixture(t, 1, func(f *chainFixture) {
					f.incrs[0].StartPosition = ir.Position{Engine: "postgres", Token: `{"slot":"x","lsn":"WRONG"}`}
					f.incrs[0].BackupID = irbackup.ComputeBackupID(f.incrs[0])
				})
			},
			want: manifestInvalidRefusal,
		},
		{
			name: "restorable_from_segment out of range",
			store: func(t *testing.T) irbackup.Store {
				return buildChainFixture(t, 1, func(f *chainFixture) {
					f.cat = &lineage.Catalog{FormatVersion: 1, SourceEngine: "postgres", RestorableFromSegment: 7}
				})
			},
			want: manifestInvalidRefusal,
		},
		{
			// CARVE-OUT 1 of 2 (see [lineage.invalidLineage]): a manifest
			// OBJECT the catalog names is unreadable. A store GET can fail
			// transiently (an S3 5xx, a timeout) and exit 3 promises "a
			// re-run will not help", so this stays an uncoded exit 1 — on
			// BOTH commands, which is what keeps them in agreement. If
			// someone wraps the read site by reflex, this row fails and
			// forces the argument.
			name: "unreadable manifest object — transient-capable, uncoded on BOTH",
			store: func(t *testing.T) irbackup.Store {
				store := buildChainFixture(t, 1, nil)
				if err := store.Delete(context.Background(), "manifests/incr-0000.json"); err != nil {
					t.Fatalf("delete incremental manifest: %v", err)
				}
				return store
			},
			want: refusalPair{exit: sluicecode.ExitFailure},
		},
		{
			// CARVE-OUT 2 of 2: an unknown recorded compression codec. This
			// one is uncoded not because it is transient — it is a corrupt
			// or tampered lineage — but because `backup verify` reaches the
			// same check through ListAllSegmentManifests, its own PRE-walk
			// listing, which returns it uncoded. Coding it inside the walk
			// alone would put restore at 3 and verify at 1: Bug 219 inverted,
			// not fixed. This row is the pin that says so.
			name: "unknown recorded codec — uncoded on BOTH (verify reaches it pre-walk)",
			store: func(t *testing.T) irbackup.Store {
				store := buildChainFixture(t, 1, func(f *chainFixture) {
					f.cat = &lineage.Catalog{FormatVersion: 1, SourceEngine: "postgres"}
				})
				cat, err := lineage.ResolveLineage(context.Background(), store)
				if err != nil {
					t.Fatalf("ResolveLineage: %v", err)
				}
				cat.Segments[0].Codec = "brotli-9000"
				if err := lineage.WriteLineageCatalog(context.Background(), store, cat); err != nil {
					t.Fatalf("rewrite catalog: %v", err)
				}
				return store
			},
			want: refusalPair{exit: sluicecode.ExitFailure},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			store := c.store(t)

			// RESTORE — the half that was wrong.
			tgt := &chainRestoreRecorderEngine{restoreRecorderEngine: newRestoreRecorderEngine("postgres")}
			rerr := (&backup.ChainRestore{Target: tgt, TargetDSN: "tgt", Store: store}).Run(ctx)
			assertRefusalPair(t, "restore", rerr, c.want)

			// VERIFY on the IDENTICAL directory, through the same entry
			// point the CLI uses (VerifyBackupCodedReport). Scored against
			// the SAME pair: one code, one exit status, one answer.
			_, verr := backup.VerifyBackupCodedReport(ctx, store, backup.VerifyOptions{})
			assertRefusalPair(t, "backup verify", verr, c.want)
		})
	}
}

// refusalPair is what a command reports for one directory: the code an
// operator's script branches on, and the process exit status it keys on.
// An empty code means "no coded refusal" — at exit 0 that is a clean run,
// at exit 1 an uncoded failure.
type refusalPair struct {
	code sluicecode.Code
	exit int
}

// manifestInvalidRefusal is the pair every structural-lineage shape must
// report from BOTH commands.
var manifestInvalidRefusal = refusalPair{
	code: sluicecode.CodeBackupManifestInvalid,
	exit: sluicecode.ExitRefusal,
}

// assertRefusalPair asserts BOTH halves. Asserting only the code is what a
// vacuous version of this gate would do — the exit status was the broken
// half.
func assertRefusalPair(t *testing.T, what string, err error, want refusalPair) {
	t.Helper()
	if got := exitStatusLikeKong(err); got != want.exit {
		t.Errorf("%s: exit status = %d; want %d (err: %v)", what, got, want.exit, err)
	}
	if want.code == "" {
		if want.exit == sluicecode.ExitSuccess && err != nil {
			t.Errorf("%s: err = %v; want a clean run", what, err)
		}
		if want.exit != sluicecode.ExitSuccess && err == nil {
			t.Fatalf("%s: err = nil; want a loud failure", what)
		}
		if ce, ok := sluicecode.FromError(err); ok {
			t.Errorf("%s: unexpected code %s; this shape is deliberately uncoded", what, ce.Code)
		}
		return
	}
	if err == nil {
		t.Fatalf("%s: no refusal; want %s", what, want.code)
	}
	ce, ok := sluicecode.FromError(err)
	if !ok {
		t.Fatalf("%s: refusal carries NO SLUICE-E-* code (invisible to a script); want %s. err: %v",
			what, want.code, err)
	}
	if ce.Code != want.code {
		t.Errorf("%s: code = %s; want %s (err: %v)", what, ce.Code, want.code, err)
	}
	if ce.Hint == "" {
		t.Errorf("%s: coded refusal carries no remedy hint", what)
	}
}
