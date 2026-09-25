// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestBackupCaptureReaderErr_ReplayMismatchNamesAFreshFull pins the GC-37
// (j) third-review item 4 remedy: a replay-mismatch refusal reaching a
// backup capture lane keeps its code and its reader prose, and its hint —
// what an operator is told to do — is a fresh full backup, not a sync flag.
// Any other reader error passes through with only the old wrapping.
func TestBackupCaptureReaderErr_ReplayMismatchNamesAFreshFull(t *testing.T) {
	refusal := sluicecode.Wrap(sluicecode.CodeCDCSchemaReplayMismatch, "re-snapshot the table (sync --restart-from-scratch)",
		errors.New("mysql: cdc: d.t: replaying history"))
	got := backupCaptureReaderErr(refusal)
	ce, ok := sluicecode.FromError(got)
	if !ok || ce.Code != sluicecode.CodeCDCSchemaReplayMismatch {
		t.Fatalf("code = %v; want %s kept", got, sluicecode.CodeCDCSchemaReplayMismatch)
	}
	if ce.Hint != backupReplayMismatchHint {
		t.Errorf("hint = %q; want the backup remedy %q", ce.Hint, backupReplayMismatchHint)
	}
	if !errors.Is(got, refusal) {
		t.Error("the reader's own refusal is no longer in the chain")
	}

	other := sluicecode.Wrap(sluicecode.CodeBackupManifestInvalid, "h", errors.New("x"))
	if ce, _ := sluicecode.FromError(backupCaptureReaderErr(other)); ce.Hint != "h" {
		t.Errorf("an unrelated coded error was re-hinted: %q", ce.Hint)
	}
	if backupCaptureReaderErr(errors.New("plain")).Error() != "cdc reader: plain" {
		t.Error("a plain reader error is not wrapped as before")
	}
}

// shapeReporter is a CDC reader stub that reports decoded shapes.
type shapeReporter map[string]map[string][2]string

func (s shapeReporter) CharsetUnrecordedShapes() map[string]map[string][2]string { return s }

// TestRefuseUnrecordedCharsetReplay pins the capture lanes' rule both ways:
// a column whose charset changed during the window, decoded by the NEW
// charset without the stream crossing the ALTER, refuses; decoded by the OLD
// one (a live reader) passes; so do a collation-only change, a change into
// UTF-8, a reader that reports nothing, and a table the reader left out.
func TestRefuseUnrecordedCharsetReplay(t *testing.T) {
	schema := func(cs, coll string) *ir.Schema {
		return &ir.Schema{Tables: []*ir.Table{{Name: "cr", Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
			{Name: "v", Type: ir.Varchar{Length: 16, Charset: cs, Collation: coll}},
		}}}}
	}
	utf8 := schema("utf8mb4", "utf8mb4_uca1400_ai_ci")
	latin1 := schema("latin1", "latin1_swedish_ci")
	latin1Bin := schema("latin1", "latin1_bin")
	decodedBy := func(cs, coll string) shapeReporter {
		return shapeReporter{"cr": {"v": {cs, coll}}}
	}

	err := refuseUnrecordedCharsetReplay(decodedBy("latin1", "latin1_swedish_ci"), utf8, latin1)
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeCDCSchemaReplayMismatch || ce.Hint != backupReplayMismatchHint {
		t.Fatalf("rows decoded by the post-change latin1 with the ALTER not crossed: err = %v; want %s with the fresh-full hint",
			err, sluicecode.CodeCDCSchemaReplayMismatch)
	}
	if !strings.Contains(err.Error(), "cr.v (utf8mb4 → latin1)") || !strings.Contains(err.Error(), "CHARSET-HISTORY-UNRECORDED") {
		t.Errorf("the refusal must name the column, both charsets and the marker: %v", err)
	}
	for name, tc := range map[string]struct {
		cdc           any
		before, after *ir.Schema
	}{
		"a live reader (decoded by the pre-change charset)": {decodedBy("utf8mb4", "utf8mb4_uca1400_ai_ci"), utf8, latin1},
		"a collation-only change":                           {decodedBy("latin1", "latin1_bin"), latin1, latin1Bin},
		"a change into UTF-8":                               {decodedBy("utf8mb4", "utf8mb4_uca1400_ai_ci"), latin1, utf8},
		"no charset change":                                 {decodedBy("latin1", "latin1_swedish_ci"), latin1, latin1},
		"a reader that reports no shapes":                   {struct{}{}, utf8, latin1},
		"the table left out (crossed an ALTER)":             {shapeReporter{}, utf8, latin1},
	} {
		if err := refuseUnrecordedCharsetReplay(tc.cdc, tc.before, tc.after); err != nil {
			t.Errorf("%s: err = %v; want nil", name, err)
		}
	}
}
