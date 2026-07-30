// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// writeForkedChain builds, in a fresh temp directory, the on-disk
// footprint of a FORKED backup lineage: a full plus TWO incrementals that
// both chain off it — what two overlapping `backup incremental` crons (or
// an incremental racing a `backup stream`) leave behind. Returns the
// directory `restore --from-dir` is pointed at.
func writeForkedChain(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	store, err := blobcodec.NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}
	pos := ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/100"}`}
	newManifest := func(kind string, createdAt time.Time) *irbackup.Manifest {
		m := &irbackup.Manifest{
			FormatVersion: irbackup.BackupFormatVersion,
			CreatedAt:     createdAt,
			SourceEngine:  "postgres",
			Kind:          kind,
			Schema:        schema,
			EndPosition:   pos,
		}
		m.BackupID = irbackup.ComputeBackupID(m)
		return m
	}
	base := time.Date(2026, 7, 29, 10, 0, 0, 0, time.UTC)
	full := newManifest(irbackup.BackupKindFull, base)
	seg := lineage.Segment{
		SegmentID:        full.BackupID,
		FullManifestPath: lineage.ManifestFileName,
		StartPosition:    pos,
		EndPosition:      pos,
		Codec:            blobcodec.CodecGzip,
	}
	if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("write full: %v", err)
	}
	// Both siblings record the FULL as their parent — the fork. Distinct
	// CreatedAt so they get distinct BackupIDs (identical ones would trip
	// the duplicate-link refusal instead, one shape earlier).
	for i := 0; i < 2; i++ {
		incr := newManifest(irbackup.BackupKindIncremental, base.Add(time.Duration(i+1)*time.Minute))
		incr.ParentBackupID = full.BackupID
		incr.StartPosition = pos
		incr.BackupID = irbackup.ComputeBackupID(incr)
		p := fmt.Sprintf("manifests/incr-%04d.json", i)
		if err := lineage.WriteManifestAt(ctx, store, p, incr); err != nil {
			t.Fatalf("write incr %d: %v", i, err)
		}
		seg.Incrementals = append(seg.Incrementals, p)
	}
	cat := &lineage.Catalog{FormatVersion: 1, SourceEngine: "postgres", Segments: []lineage.Segment{seg}}
	if err := lineage.WriteLineageCatalog(ctx, store, cat); err != nil {
		t.Fatalf("write lineage catalog: %v", err)
	}
	return dir
}

// TestRestoreForkedChain_ExitsRefusalWithCode drives the forked-chain
// refusal through the REAL `sluice restore` command path — RestoreCmd.run,
// the same function kong dispatches, including its --format envelope
// wrapper — and asserts both halves of the machine contract at the two
// places the process actually implements them: the kong exit boundary
// (exitCodeLikeKong, exactly what ctx.FatalIfErrorf runs) and the
// exit-boundary slog record (logCodedError, the `code` attr an agent
// branches on under --log-format json).
//
// Bug 219: this refusal reached the operator as exit 1 with ZERO
// occurrences of `SLUICE-E-` in its whole output, while `backup verify`
// scored the same directory as exit 3 with the code and restore itself
// scored the sibling shapes of that same code as exit 3. The class pin
// (every shape × both commands) lives in
// internal/pipeline/restore_refusal_machine_contract_test.go; this is the
// one row driven end to end through the CLI layer, because the exit status
// and the code emission are CLI-layer properties and a pipeline-level
// assertion cannot prove either reached a process.
//
// No target database is involved: the lineage walk is restore's FIRST
// step, so the refusal fires before the change applier is opened. That is
// the same reason zero rows land.
func TestRestoreForkedChain_ExitsRefusalWithCode(t *testing.T) {
	r := &RestoreCmd{
		FromDir:      writeForkedChain(t),
		TargetDriver: "postgres",
		Target:       "postgres://u:p@127.0.0.1:1/app",
		Format:       "text",
	}
	err := r.run(testFleetGlobals(), newEnvelopeRun("restore", r.Format))
	if err == nil {
		t.Fatal("restore of a FORKED lineage returned nil; want a loud refusal")
	}

	// The half a DR script keys on, and the half that was wrong.
	if got := exitCodeLikeKong(err); got != sluicecode.ExitRefusal {
		t.Errorf("exit status = %d; want %d (named refusal). err: %v", got, sluicecode.ExitRefusal, err)
	}
	ce, ok := sluicecode.FromError(err)
	if !ok {
		t.Fatalf("refusal carries no SLUICE-E-* code — invisible to a script. err: %v", err)
	}
	if ce.Code != sluicecode.CodeBackupManifestInvalid {
		t.Errorf("code = %s; want %s", ce.Code, sluicecode.CodeBackupManifestInvalid)
	}

	// The code must actually be EMITTED at the exit boundary, not merely
	// present in the error chain.
	var (
		mu      sync.Mutex
		records []slog.Record
	)
	prev := slog.Default()
	slog.SetDefault(slog.New(captureHandler{mu: &mu, records: &records}))
	logCodedError(err)
	slog.SetDefault(prev)
	if len(records) != 1 {
		t.Fatalf("exit boundary emitted %d records; want 1 carrying the code", len(records))
	}
	var emitted string
	records[0].Attrs(func(a slog.Attr) bool {
		if a.Key == "code" {
			emitted = a.Value.String()
		}
		return true
	})
	if emitted != string(sluicecode.CodeBackupManifestInvalid) {
		t.Errorf("emitted code attr = %q; want %q", emitted, sluicecode.CodeBackupManifestInvalid)
	}

	// The human-facing prose is untouched by the coding — this fix was
	// purely the machine contract, and the message it rides on (which names
	// the fork, blames overlapping writers, and inlines the repair) is the
	// part that already worked.
	for _, want := range []string{
		"branching/mis-stitched lineage",
		"Most likely a FORK",
		"REPAIR (lossless for the fork case",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal prose lost %q; got:\n%s", want, err)
		}
	}
}
