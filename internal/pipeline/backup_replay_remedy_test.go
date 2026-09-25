// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"errors"
	"testing"

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
