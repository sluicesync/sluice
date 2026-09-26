// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
)

// stampExactNumbersOnSeal raises the segment's format version to
// [irbackup.FormatVersionExactNumbers] when the change chunk just sealed
// carried an exact-text number on a source whose chains are read back
// exactly (postgres-trigger). Called at every capture seal site — the
// one-shot `backup incremental` window's flush and [changeChunkBuffer]'s
// flushTo, which serves both `backup stream` rollovers and the ADD COLUMN
// fill — so a segment that encoded such a number cannot be restored,
// brokered or smart-compacted by a pre-v0.156.4 binary, whose reader would
// round the number through a float64 at exit 0.
//
// Safe mid-capture: the stamp can only move 11→12 for an encrypted
// segment (which is already at the injective-AAD tier before its first
// chunk), and a plaintext chunk has no AAD at all, so no sealed chunk's
// binding changes. Every capture lane stamps before [irbackup.ComputeBackupID].
//
// Smart compaction deliberately does NOT stamp: it rewrites a manifest in
// place under its recorded BackupID, and raising a pre-8 segment's version
// would change which fields that id folds. A segment it compacts keeps the
// version its capture recorded — 12 when a v0.156.4+ capture carried such a
// number, and unstamped for a chain an older binary wrote (which the older
// binary could already read, and round, before any compaction).
func stampExactNumbersOnSeal(m *irbackup.Manifest, w *blobcodec.ChangeChunkWriter) {
	irbackup.StampExactNumbers(m, w.CarriedExactNumber() && blobcodec.NumbersArePreserved(m.SourceEngine))
}
