// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

// absorbSmartGroup folds one merge group's smart-compaction result into the
// run's result: the group's own plan entry (located by MergedSegmentID — the
// plan was appended to during planning) and the run totals. Lifted out of
// CompactChain unchanged.
func (res *CompactResult) absorbSmartGroup(pg *plannedGroup, groupRes *smartCompactResult, tablesWithoutPK, tablesUnmatched map[string]struct{}) {
	for pi := range res.Plan {
		if res.Plan[pi].MergedSegmentID != pg.plan.MergedSegmentID {
			continue
		}
		res.Plan[pi].EventsBefore = groupRes.eventsBefore
		res.Plan[pi].EventsAfter = groupRes.eventsAfter
		res.Plan[pi].EventsCollapsed = groupRes.eventsBefore - groupRes.eventsAfter
		res.Plan[pi].RowsCollapsed = groupRes.rowsCollapsed
		res.Plan[pi].TablesWithoutPK = groupRes.tablesWithoutPKList()
		res.Plan[pi].TablesUnmatched = groupRes.tablesUnmatchedList()
		res.Plan[pi].ChainsEvicted = groupRes.chainsEvicted
		res.Plan[pi].PeakChunkBufferBytes = groupRes.peakChunkBufferBytes
		break
	}
	res.EventsBefore += groupRes.eventsBefore
	res.EventsAfter += groupRes.eventsAfter
	res.RowsCollapsed += groupRes.rowsCollapsed
	res.ChainsEvicted += groupRes.chainsEvicted
	// A peak is a MAXIMUM at every level: two groups compacted one
	// after the other never hold their buffers at the same time. This
	// mirrors [smartCompactResult.merge], which does the same for the
	// incrementals within a group.
	if groupRes.peakChunkBufferBytes > res.PeakChunkBufferBytes {
		res.PeakChunkBufferBytes = groupRes.peakChunkBufferBytes
	}
	// Re-derive BytesAfter for this group: the chunks have been
	// rewritten with possibly-fewer events, so the merged
	// segment's actual byte total is groupRes.bytesAfter (chunk
	// data only; manifest bytes are negligible and the naive
	// BytesEstimate was chunk-byte sums).
	res.BytesAfter += groupRes.bytesAfter - pg.plan.BytesEstimate
	for k := range groupRes.tablesWithoutPK {
		tablesWithoutPK[k] = struct{}{}
	}
	for k := range groupRes.tablesUnmatched {
		tablesUnmatched[k] = struct{}{}
	}
}
