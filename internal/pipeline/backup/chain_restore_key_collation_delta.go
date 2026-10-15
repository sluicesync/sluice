// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// # A key-collation delta the target refuses (KEY-COLLATION-ROLLOVER-SPLIT-DELTA-DUP)
//
// Chain restore applies a window's schema delta ahead of its rows. A delta
// that changes a key column's collation can therefore reach the target
// while rows the source had already reconciled are both still there: the
// source tightened a key, wrote 'A' beside 'a', and in a LATER window moved
// 'A' away and loosened the key back. That later window's delta (`_bin` →
// `_ci`) runs before its own UPDATE, and the target's ALTER fails on the
// duplicate the UPDATE would have removed (MySQL `Error 1062 Duplicate entry
// 'A' for key ...`). Nothing merged — the ALTER refused — but the operator
// read a raw duplicate-key error with no remedy, where every other
// key-collation refusal of the window names one. An hourly `backup
// incremental` splits a change from its revert this way by construction,
// not only under a rollover's timing.
//
// So the failure of such a delta is named KEY-COLLATION-MERGE (it is the
// same class: the window changes a key's collation and its rows cannot be
// placed under it), marked KEY-COLLATION-DELTA-FAILED, and given the window
// remedy. Applying a loosening key delta at the window's END instead would
// let this shape converge; that is a design change, not this wrapper
// (stated in the backlog entry).
//
// Scope: only a delta [migcore.DeltaChangesKeyCollation] holds for, and only
// a failure of the target's own DDL — a refusal sluice raised before any DDL
// (the delta's coded SLUICE-E-BACKUP-SCHEMA-DELTA-UNSUPPORTED), an error
// already classed KEY-COLLATION-MERGE and a cancellation keep their own
// names. The engine-neutral pipeline cannot tell a duplicate key from any
// other DDL failure, so the text says the ALTER failed and why that is the
// likely shape, and keeps the target's error.
//
// Sibling: `sync from-backup`'s own applySchemaDeltas never meets such a
// delta — [RefuseUnreplayableKeyCollationHistory] refuses every window whose
// delta changes a key collation (KEY-COLLATION-HISTORY-UNORDERED) before
// its deltas are applied. Its --reset-target-data cold start runs THIS
// chain restore, and gets the broker wording from [ChainRestore.Command].

// keyCollationDeltaFailedMarker names the refusal.
const keyCollationDeltaFailedMarker = "KEY-COLLATION-DELTA-FAILED"

// keyCollationDeltaRefusal is err, named KEY-COLLATION-MERGE with the window
// remedy when d changes a key column's collation and err is the target's
// failure to apply it; any other err is returned as is.
func keyCollationDeltaRefusal(link *lineage.SegmentRecord, cmd RestoreCommand, d *irbackup.SchemaDeltaEntry, err error) error {
	if err == nil || d == nil || link == nil || link.Manifest == nil {
		return err
	}
	if !migcore.DeltaChangesKeyCollation([]*irbackup.SchemaDeltaEntry{d}) {
		return err
	}
	if errors.Is(err, ir.ErrKeyCollationMerge) || errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if coded, ok := sluicecode.FromError(err); ok && coded.Code == sluicecode.CodeBackupSchemaDeltaUnsupported {
		return err
	}
	return fmt.Errorf("%w: %s: chain restore: incremental %s changes the collation of a key column of %q, and the "+
		"target refused that change (%w). A window's schema delta is applied ahead of its rows, so a key change "+
		"the source made after rows the target has not replayed yet (a row that moved away a value the new "+
		"collation equates with another) meets those values still side by side; nothing was merged. %s",
		ir.ErrKeyCollationMerge, keyCollationDeltaFailedMarker, lineage.ManifestBackupID(link.Manifest), d.Table,
		err, windowKeyCollationRemedy(link, cmd))
}
