// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"fmt"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// backupReplayMismatchHint is the remedy a backup-chain capture lane gives a
// SLUICE-E-CDC-SCHEMA-REPLAY-MISMATCH refusal (GC-37 (j) third review).
//
// The MySQL reader's own hint is `sync --restart-from-scratch`, which does
// not exist for a backup and would not help if it did: the charset-DDL
// guard refuses AFTER the rows it replayed were emitted, and a
// `backup stream` rollover or an earlier `backup incremental` window that
// closed between those rows and the DDL event has already COMMITTED them to
// the chain, decoded by the post-DDL charset (the failing window itself is
// not committed — a non-transient capture error ends the stream before
// commitRollover, and the one-shot lane returns before writing its
// manifest). No restore of such a chain gives the values back; only a new
// full does.
const backupReplayMismatchHint = "take a fresh full backup of this source (sluice backup full), and start the chain from it; " +
	"windows this chain committed before the refusal may hold rows decoded by the wrong charset, and no restore of them recovers the values"

// backupCaptureReaderErr is how both backup capture lanes surface the CDC
// reader's terminal error: wrapped as before, and a replay-mismatch refusal
// re-hinted with [backupReplayMismatchHint] so the operator is not sent to a
// sync flag.
func backupCaptureReaderErr(e error) error {
	err := fmt.Errorf("cdc reader: %w", e)
	if ce, ok := sluicecode.FromError(e); ok && ce.Code == sluicecode.CodeCDCSchemaReplayMismatch {
		return sluicecode.Wrap(ce.Code, backupReplayMismatchHint, err)
	}
	return err
}
