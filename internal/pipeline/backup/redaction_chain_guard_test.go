// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/redact"
	"sluicesync.dev/sluice/internal/sluicecode"
)

func redactedMarker(fp string) *irbackup.RedactionInfo {
	return &irbackup.RedactionInfo{RuleCount: 1, Fingerprint: fp}
}

func wantRedactedChainCode(t *testing.T, what string, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: want the redacted-chain refusal, got nil", what)
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeBackupRedactedChain {
		t.Fatalf("%s: want %s, got %v", what, sluicecode.CodeBackupRedactedChain, err)
	}
	if got := ce.ExitCode(); got != sluicecode.ExitRefusal {
		t.Errorf("%s: ExitCode() = %d; want %d (refusal class — a DR script branches on it)", what, got, sluicecode.ExitRefusal)
	}
}

// TestRefuseRedactedChainExtension covers both directions of the write
// door. The refusal arm is the leak; the pass arms are every operator
// running an ordinary unredacted chain on a cron, which must not start
// failing.
func TestRefuseRedactedChainExtension(t *testing.T) {
	t.Run("redacted parent refuses", func(t *testing.T) {
		parent := &irbackup.Manifest{Kind: irbackup.BackupKindFull, Redaction: redactedMarker("abcdef0123456789")}
		err := RefuseRedactedChainExtension(parent, "manifest.json", "backup incremental")
		wantRedactedChainCode(t, "redacted parent", err)
		// The message must name the policy without disclosing the rules.
		if !strings.Contains(err.Error(), "abcdef0123456789") {
			t.Errorf("message does not name the recorded policy fingerprint: %v", err)
		}
	})

	t.Run("unredacted parent passes", func(t *testing.T) {
		parent := &irbackup.Manifest{Kind: irbackup.BackupKindFull}
		if err := RefuseRedactedChainExtension(parent, "manifest.json", "backup incremental"); err != nil {
			t.Errorf("unredacted parent refused: %v", err)
		}
	})

	t.Run("nil parent passes", func(t *testing.T) {
		if err := RefuseRedactedChainExtension(nil, "manifest.json", "backup stream"); err != nil {
			t.Errorf("nil parent refused: %v", err)
		}
	})
}

// TestRefuseResumeUnderDifferentRedaction is the same-manifest door. The
// "policy dropped" cell is the one that happens by accident — a shorter
// re-run of the command — and is the worst outcome, because the manifest
// would still claim the archive is PII-clean.
func TestRefuseResumeUnderDifferentRedaction(t *testing.T) {
	for _, c := range []struct {
		name       string
		prior, cur *irbackup.RedactionInfo
		wantRefuse bool
	}{
		{"both unredacted", nil, nil, false},
		{"same policy", redactedMarker("aaaa000000000000"), redactedMarker("aaaa000000000000"), false},
		{"policy dropped on the resume", redactedMarker("aaaa000000000000"), nil, true},
		{"policy added on the resume", nil, redactedMarker("aaaa000000000000"), true},
		{"different policy", redactedMarker("aaaa000000000000"), redactedMarker("bbbb000000000000"), true},
		{
			"same fingerprint, different rule count",
			&irbackup.RedactionInfo{RuleCount: 1, Fingerprint: "aaaa000000000000"},
			&irbackup.RedactionInfo{RuleCount: 2, Fingerprint: "aaaa000000000000"},
			true,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			prior := &irbackup.Manifest{PartialState: irbackup.BackupStateInProgress, Redaction: c.prior}
			err := refuseResumeUnderDifferentRedaction(prior, c.cur, "manifest.json")
			if c.wantRefuse {
				wantRedactedChainCode(t, c.name, err)
				return
			}
			if err != nil {
				t.Errorf("%s: refused a resume that keeps one policy: %v", c.name, err)
			}
		})
	}

	// No prior attempt at all is a fresh backup, never a resume.
	if err := refuseResumeUnderDifferentRedaction(nil, redactedMarker("aaaa000000000000"), "manifest.json"); err != nil {
		t.Errorf("fresh run (no prior) refused: %v", err)
	}
}

// TestRefuseMixedRedactionChain is the read door. The pass arms matter
// as much as the refusal: a chain of consistently unredacted links is
// every backup anyone has ever taken with this tool.
func TestRefuseMixedRedactionChain(t *testing.T) {
	link := func(path string, r *irbackup.RedactionInfo) lineage.SegmentRecord {
		return lineage.SegmentRecord{
			ManifestRecord: lineage.ManifestRecord{
				Path:     path,
				Manifest: &irbackup.Manifest{Kind: irbackup.BackupKindFull, Redaction: r},
			},
		}
	}
	fp := redactedMarker("aaaa000000000000")

	for _, c := range []struct {
		name       string
		links      []lineage.SegmentRecord
		wantRefuse bool
	}{
		{"no links", nil, false},
		{"single unredacted full", []lineage.SegmentRecord{link("manifest.json", nil)}, false},
		{"single redacted full", []lineage.SegmentRecord{link("manifest.json", fp)}, false},
		{
			"uniformly unredacted chain",
			[]lineage.SegmentRecord{link("manifest.json", nil), link("manifests/incr-1.json", nil)},
			false,
		},
		{
			"uniformly redacted chain",
			[]lineage.SegmentRecord{link("manifest.json", fp), link("manifests/incr-1.json", fp)},
			false,
		},
		{
			"redacted root, unredacted incremental",
			[]lineage.SegmentRecord{link("manifest.json", fp), link("manifests/incr-1.json", nil)},
			true,
		},
		{
			"unredacted root, redacted later link",
			[]lineage.SegmentRecord{link("manifest.json", nil), link("manifests/incr-1.json", fp)},
			true,
		},
		{
			"two different policies",
			[]lineage.SegmentRecord{link("manifest.json", fp), link("manifests/incr-1.json", redactedMarker("bbbb000000000000"))},
			true,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := refuseMixedRedactionChain(c.links)
			if c.wantRefuse {
				wantRedactedChainCode(t, c.name, err)
				return
			}
			if err != nil {
				t.Errorf("%s: refused a coherent chain: %v", c.name, err)
			}
		})
	}
}

// TestBackupFullRecordsTheRedactionMarker is the end-to-end write pin:
// a real `backup full` with rules records the marker, stamps the format
// version that makes an older binary refuse rather than ignore it, and
// folds the fingerprint into the recorded BackupID. Its control is the
// byte-level compatibility claim — an unredacted run of the SAME backup
// writes no marker and stays on its schema-derived version.
func TestBackupFullRecordsTheRedactionMarker(t *testing.T) {
	ctx := context.Background()
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "email", Type: ir.Varchar{Length: 200}},
		},
	}}}
	rows := map[string][]ir.Row{"users": {{"id": int64(1), "email": "alice@example.com"}}}

	runBackup := func(t *testing.T, reg *redact.Registry) *irbackup.Manifest {
		t.Helper()
		store, err := blobcodec.NewLocalStore(t.TempDir())
		if err != nil {
			t.Fatalf("NewLocalStore: %v", err)
		}
		b := &Backup{
			Source:    newBackupRecorderEngine("postgres", schema, rows),
			SourceDSN: "src",
			Store:     store,
			ChunkRows: 100,
			Redactor:  reg,
		}
		if err := b.Run(ctx); err != nil {
			t.Fatalf("Backup.Run: %v", err)
		}
		m, err := lineage.ReadManifest(ctx, store)
		if err != nil {
			t.Fatalf("ReadManifest: %v", err)
		}
		return m
	}

	reg := redact.New()
	reg.Set("", "users", "email", redact.Hash{Algo: "sha256"})

	redacted := runBackup(t, reg)
	if redacted.Redaction == nil {
		t.Fatal("a `backup full --redact` recorded NO redaction marker — the incremental door has nothing to refuse on")
	}
	if redacted.Redaction.RuleCount != 1 || redacted.Redaction.Fingerprint != reg.Fingerprint() {
		t.Errorf("recorded marker = %+v; want RuleCount 1 and fingerprint %q", *redacted.Redaction, reg.Fingerprint())
	}
	if redacted.FormatVersion < irbackup.FormatVersionRedaction {
		t.Errorf("redacted manifest FormatVersion = %d; want >= %d so an older binary refuses it rather than "+
			"ignoring the marker and extending the chain", redacted.FormatVersion, irbackup.FormatVersionRedaction)
	}
	if got := irbackup.ComputeBackupID(redacted); got != redacted.BackupID {
		t.Errorf("recorded BackupID %s does not recompute (%s) — restore's own id preflight would refuse the backup "+
			"we just wrote", redacted.BackupID, got)
	}

	// Control / mutation arm: same source, same store shape, no rules.
	plain := runBackup(t, nil)
	if plain.Redaction != nil {
		t.Errorf("an unredacted backup recorded a marker: %+v", *plain.Redaction)
	}
	if plain.FormatVersion != irbackup.FormatVersionFor(schema) {
		t.Errorf("unredacted manifest FormatVersion = %d; want the schema-derived %d — an unredacted backup must "+
			"stay readable by older binaries", plain.FormatVersion, irbackup.FormatVersionFor(schema))
	}
	if got := irbackup.ComputeBackupID(plain); got != plain.BackupID {
		t.Errorf("recorded BackupID %s does not recompute (%s)", plain.BackupID, got)
	}
}

// redactionResumeFixture leaves an INTERRUPTED redacted `backup full` in a
// fresh store and returns the store plus the schema/rows/registry it was
// taken with, asserting the fixture's own shape first.
func redactionResumeFixture(t *testing.T) (*blobcodec.LocalStore, *ir.Schema, map[string][]ir.Row, *redact.Registry) {
	t.Helper()
	ctx := context.Background()
	schema := &ir.Schema{Tables: []*ir.Table{
		{Name: "users", Columns: []*ir.Column{{Name: "email", Type: ir.Varchar{Length: 200}}}},
		{Name: "posts", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
		{Name: "tags", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
	}}
	rows := map[string][]ir.Row{
		"users": {{"email": "alice@example.com"}},
		"posts": {{"id": int64(10)}},
		"tags":  {{"id": int64(100)}},
	}
	reg := redact.New()
	reg.Set("", "users", "email", redact.Hash{Algo: "sha256"})

	inner, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	interrupted := &Backup{
		Source:    newBackupRecorderEngine("postgres", schema, rows),
		SourceDSN: "src",
		Store:     newFailOnNthPutStore(inner, 3),
		ChunkRows: 100,
		Redactor:  reg,
	}
	if err := interrupted.Run(ctx); err == nil {
		t.Fatal("fixture: want the injected mid-sweep failure, got nil")
	}
	prior, err := lineage.ReadManifest(ctx, inner)
	if err != nil {
		t.Fatalf("ReadManifest after the interrupted run: %v", err)
	}
	if prior.PartialState != irbackup.BackupStateInProgress || prior.Redaction == nil {
		t.Fatalf("fixture is not an interrupted REDACTED run (state=%q marker=%v); every assertion using it would be vacuous",
			prior.PartialState, prior.Redaction)
	}
	return inner, schema, rows, reg
}

// TestBackupFullRefusesAResumeUnderDifferentRules drives the resume door
// through the real orchestrator: an interrupted redacted run, re-run with
// --redact dropped, must refuse before it reads anything.
func TestBackupFullRefusesAResumeUnderDifferentRules(t *testing.T) {
	ctx := context.Background()
	inner, schema, rows, reg := redactionResumeFixture(t)

	// The accident: the same command re-run without --redact.
	resumed := &Backup{
		Source:    newBackupRecorderEngine("postgres", schema, rows),
		SourceDSN: "src",
		Store:     inner,
		ChunkRows: 100,
	}
	wantRedactedChainCode(t, "resume without --redact", resumed.Run(ctx))

	// Mutation arm: the SAME resume carrying the SAME rules completes.
	// Without it the refusal above could be any resume failure at all.
	resumedSameRules := &Backup{
		Source:    newBackupRecorderEngine("postgres", schema, rows),
		SourceDSN: "src",
		Store:     inner,
		ChunkRows: 100,
		Redactor:  reg,
	}
	if err := resumedSameRules.Run(ctx); err != nil {
		t.Fatalf("mutation arm: a resume under the SAME --redact rules failed: %v", err)
	}
	final, err := lineage.ReadManifest(ctx, inner)
	if err != nil {
		t.Fatalf("ReadManifest after the resume: %v", err)
	}
	if final.PartialState != irbackup.BackupStateComplete {
		t.Errorf("mutation arm: resumed backup left partial_state=%q", final.PartialState)
	}
	if final.Redaction == nil || final.Redaction.Fingerprint != reg.Fingerprint() {
		t.Errorf("mutation arm: finalized manifest lost the marker: %v", final.Redaction)
	}
}

// TestBackupFullForceOverwriteStartsFreshUnderNewRules pins the REMEDY the
// resume refusal prints. "Discard that attempt with --force-overwrite and
// start again under the policy you want" is only advice if the guard also
// fires on the discard — and it must not, because --force-overwrite
// resolves the prior attempt away entirely rather than resuming it.
//
// An unrunnable remedy is the shape this repo has been bitten by before
// (Bug 249), so the sentence gets a test rather than a reading.
func TestBackupFullForceOverwriteStartsFreshUnderNewRules(t *testing.T) {
	ctx := context.Background()
	inner, schema, rows, _ := redactionResumeFixture(t)

	fresh := &Backup{
		Source:         newBackupRecorderEngine("postgres", schema, rows),
		SourceDSN:      "src",
		Store:          inner,
		ChunkRows:      100,
		ForceOverwrite: true,
		// Deliberately NO redactor: the discard-and-restart case where
		// the operator has decided the new chain is unredacted.
	}
	if err := fresh.Run(ctx); err != nil {
		t.Fatalf("--force-overwrite over an interrupted REDACTED attempt failed: %v — the refusal's own remedy does not run", err)
	}
	m, err := lineage.ReadManifest(ctx, inner)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	if m.PartialState != irbackup.BackupStateComplete {
		t.Errorf("force-overwrite left partial_state=%q", m.PartialState)
	}
	if m.Redaction != nil {
		t.Errorf("the fresh unredacted backup inherited a marker from the discarded attempt: %+v", *m.Redaction)
	}
}
