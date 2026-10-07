// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// Bug 297's write side, through the real Backup orchestrator: a
// rotation-born segment full refuses a keyless table before anything is
// written, and an ordinary `backup full` of the same source is untouched.

func rotationKeylessSource() *backupRecorderEngine {
	schema := &ir.Schema{Tables: []*ir.Table{r297Keyed("kd"), r297Keyless("kl")}}
	return newBackupRecorderEngine("postgres", schema, map[string][]ir.Row{
		"kd": {{"id": int64(1)}},
		"kl": {{"a": int64(1)}},
	})
}

func assertRotationKeylessRefusal(t *testing.T, err error, mentions ...string) {
	t.Helper()
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeBackupRotatedKeylessTable {
		t.Fatalf("err = %v; want %s", err, sluicecode.CodeBackupRotatedKeylessTable)
	}
	for _, m := range mentions {
		if !strings.Contains(err.Error(), m) {
			t.Errorf("refusal does not mention %q:\n%v", m, err)
		}
	}
	if !strings.Contains(ce.Hint, "--retain-rotate-at") {
		t.Errorf("hint does not name the rotation flags: %q", ce.Hint)
	}
}

func TestBackup_RotationSegmentRefusesAKeylessTable(t *testing.T) {
	ctx := context.Background()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	err = (&Backup{Source: rotationKeylessSource(), SourceDSN: "src", Store: store, RotationSegment: true}).Run(ctx)
	assertRotationKeylessRefusal(t, err, `"kl"`, "stays on its open segment")
	if _, rerr := lineage.ReadManifest(ctx, store); rerr == nil {
		t.Error("a refused rotation full wrote a manifest; the door is before anything is written")
	}

	// The same source as an ordinary `backup full` (the zero value): the
	// first segment is applied as a plain restore and needs no key.
	plain, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := (&Backup{Source: rotationKeylessSource(), SourceDSN: "src", Store: plain}).Run(ctx); err != nil {
		t.Fatalf("ordinary backup full of a source with a keyless table: %v", err)
	}
}

func TestPreflightRotationKeyless(t *testing.T) {
	ctx := context.Background()
	assertRotationKeylessRefusal(t, PreflightRotationKeyless(ctx, rotationKeylessSource(), "src"), `"kl"`, "Nothing has been written")

	keyed := newBackupRecorderEngine("postgres", &ir.Schema{Tables: []*ir.Table{r297Keyed("kd")}}, nil)
	if err := PreflightRotationKeyless(ctx, keyed, "src"); err != nil {
		t.Errorf("keyed-only source: %v", err)
	}
}
