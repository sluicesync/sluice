// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Progress-sidecar contract for in-progress full backups (ADR-0086,
// the task-#54 fix for the O(N²) manifest-checkpoint wall).
//
// Through v0.99.38 every per-chunk / per-table checkpoint re-marshaled
// the ENTIRE manifest — embedded schema included — and re-Put the whole
// `manifest.json`. The manifest grows with table count, so total
// checkpoint cost was quadratic in N (the #38 scale probe measured
// ≈ 0.018·N + 2.77e-5·N² seconds: ~78 h of pure manifest rewriting at
// 100k tables). This file defines the replacement layout:
//
//   - The BASE manifest is written ONCE up front (schema, anchor,
//     pre-staged table entries) and stamped
//     [FormatVersionProgressSidecar] + a [ProgressSidecarRef].
//   - Each checkpoint APPENDS one [ProgressEvent] JSON line to the
//     sidecar — O(1) per event, independent of table count.
//   - The truth about an in-progress backup is base + [ReplayProgress]
//     of the sidecar's matching-attempt events.
//   - The FINAL manifest folds everything back into one self-contained
//     `manifest.json` (re-stamped via [FormatVersionFor], sidecar
//     reference cleared) so finalized backups keep the pre-ADR shape.
//
// Crash semantics worth naming: appends are whole lines, but a crash
// mid-append can tear the FINAL line. Replay tolerates exactly that —
// a torn tail is reported (loudly logged by callers), never fatal; the
// event it carried is lost and the affected table simply re-streams
// (the Bug-135 table-granular resume already pays that shape). Any
// OTHER malformed line is corruption and fails loudly.

package backup

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// Appender is an OPTIONAL [Store] capability: append the contents of r
// to the blob at path, creating it when absent. The backup writer uses
// it for the O(1) progress-sidecar checkpoints; stores that cannot
// append atomically-enough (object stores without an append primitive)
// simply don't implement it and the writer falls back to the legacy
// full-manifest checkpoint rewrites.
//
// Contract: callers append WHOLE lines (newline-terminated) in a
// single call; implementations write the payload in one operation so a
// crash tears at most the final line — the [ReplayProgress] tolerance.
// Appends to the same path are serialized by the caller.
type Appender interface {
	Append(ctx context.Context, path string, r io.Reader) error
}

// ProgressSidecarRef is the base manifest's pointer to its progress
// sidecar. String fields are part of the on-disk format.
type ProgressSidecarRef struct {
	// File is the sidecar's store path, relative to the same store
	// root as the manifest (conventionally `manifest.progress.jsonl`,
	// next to `manifest.json`).
	File string `json:"file"`

	// AttemptID is a random token minted per backup attempt and
	// stamped on every sidecar line that attempt writes. Replay applies
	// only matching-attempt events, so a stale sidecar surviving a
	// crash window between a new attempt's base-manifest write and its
	// sidecar reset can never corrupt the reconstructed state.
	AttemptID string `json:"attempt_id"`
}

// Progress-event kinds. String literals are part of the on-disk
// format; a new kind requires a [BackupFormatVersion] bump (an older
// replay refuses unknown kinds loudly).
const (
	// ProgressEventChunk records one finished chunk upload: Chunk is
	// appended to the named table's chunk list.
	ProgressEventChunk = "chunk"

	// ProgressEventTableComplete records a table's natural row-stream
	// EOF: the named table flips Partial=false with RowCount rows.
	ProgressEventTableComplete = "table_complete"
)

// ProgressEvent is one line of the progress sidecar.
type ProgressEvent struct {
	// AttemptID matches [ProgressSidecarRef.AttemptID] of the base
	// manifest this event belongs to; replay skips mismatches.
	AttemptID string `json:"attempt_id"`

	// Event is one of ProgressEventChunk / ProgressEventTableComplete.
	Event string `json:"event"`

	// Schema/Table identify the manifest table entry the event applies
	// to (Schema empty for flat-scope engines, mirroring
	// [TableManifest.Schema]).
	Schema string `json:"schema,omitempty"`
	Table  string `json:"table"`

	// Chunk carries the finished chunk's metadata. Non-nil exactly for
	// ProgressEventChunk.
	Chunk *ChunkInfo `json:"chunk,omitempty"`

	// RowCount is the table's final row count. Meaningful only for
	// ProgressEventTableComplete.
	RowCount int64 `json:"row_count,omitempty"`
}

// MarshalLine serializes the event as one compact JSON line including
// the trailing newline — the unit the writer hands to
// [Appender.Append] so a crash tears at most this one line.
func (e *ProgressEvent) MarshalLine() ([]byte, error) {
	b, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("backup: marshal progress event: %w", err)
	}
	return append(b, '\n'), nil
}

// ProgressReplayStats reports what [ReplayProgress] did, so callers
// can log the reconstruction loudly (torn tails and stale lines are
// expected crash debris, but operators deserve to see them named).
type ProgressReplayStats struct {
	// ChunksApplied / TablesCompleted count the matching-attempt events
	// applied to the manifest.
	ChunksApplied   int
	TablesCompleted int

	// StaleLines counts parseable lines whose AttemptID did not match
	// the base manifest's — debris from a previous attempt's crash
	// window, skipped by design.
	StaleLines int

	// TornTail reports that the sidecar's FINAL line failed to parse —
	// the expected shape of a crash mid-append. The event it carried is
	// lost; the affected table re-streams on resume.
	TornTail bool
}

// maxProgressLineBytes bounds one sidecar line during replay. Real
// events are a few hundred bytes; the bound only matters for refusing
// pathological garbage loudly instead of buffering it unbounded.
const maxProgressLineBytes = 1 << 20

// ReplayProgress applies the sidecar events read from r onto m's table
// entries, reconstructing the in-progress state a crashed sidecar-mode
// backup left behind. m must be an in-progress manifest carrying a
// [ProgressSidecarRef]; events are matched to entries by (schema,
// table) and applied in order.
//
// Loud-failure ladder:
//
//   - a torn FINAL line (crash mid-append) is tolerated: replay stops,
//     TornTail is set, no error — the caller logs it loudly;
//   - a malformed line FOLLOWED by more data is corruption → error;
//   - an event naming an unknown table, an unknown event kind, or a
//     table already complete is corruption → error (with matching
//     attempt IDs none of these can occur in a sound sidecar).
//
// Stale lines (mismatched AttemptID) are skipped and counted — the
// documented crash-window debris, see [ProgressSidecarRef.AttemptID].
func ReplayProgress(m *Manifest, r io.Reader) (ProgressReplayStats, error) {
	var stats ProgressReplayStats
	if m == nil || m.ProgressSidecar == nil {
		return stats, errors.New("backup: replay progress: manifest carries no progress-sidecar reference")
	}
	byKey := make(map[string]*TableManifest, len(m.Tables))
	for _, t := range m.Tables {
		byKey[progressTableKey(t.Schema, t.Name)] = t
	}

	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), maxProgressLineBytes)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var ev ProgressEvent
		if err := json.Unmarshal(line, &ev); err != nil {
			if sc.Scan() {
				// More data follows the malformed line — that is not a
				// torn tail, it is mid-file corruption.
				return stats, fmt.Errorf("backup: progress sidecar line %d is malformed mid-file (not a torn tail): %w", lineNo, err)
			}
			stats.TornTail = true
			return stats, nil
		}
		if ev.AttemptID != m.ProgressSidecar.AttemptID {
			stats.StaleLines++
			continue
		}
		entry, ok := byKey[progressTableKey(ev.Schema, ev.Table)]
		if !ok {
			return stats, fmt.Errorf("backup: progress sidecar line %d names table %q not present in the base manifest", lineNo, qualifiedProgressTable(ev.Schema, ev.Table))
		}
		switch ev.Event {
		case ProgressEventChunk:
			if ev.Chunk == nil {
				return stats, fmt.Errorf("backup: progress sidecar line %d: chunk event without chunk metadata", lineNo)
			}
			if !entry.Partial {
				return stats, fmt.Errorf("backup: progress sidecar line %d: chunk event for table %q which is already complete", lineNo, qualifiedProgressTable(ev.Schema, ev.Table))
			}
			entry.Chunks = append(entry.Chunks, ev.Chunk)
			stats.ChunksApplied++
		case ProgressEventTableComplete:
			if !entry.Partial {
				return stats, fmt.Errorf("backup: progress sidecar line %d: duplicate completion for table %q", lineNo, qualifiedProgressTable(ev.Schema, ev.Table))
			}
			entry.RowCount = ev.RowCount
			entry.Partial = false
			stats.TablesCompleted++
		default:
			return stats, fmt.Errorf("backup: progress sidecar line %d carries unknown event kind %q (written by a newer sluice?); refusing to reconstruct partial state from it", lineNo, ev.Event)
		}
	}
	if err := sc.Err(); err != nil {
		return stats, fmt.Errorf("backup: read progress sidecar: %w", err)
	}
	return stats, nil
}

// progressTableKey keys manifest table entries the way replay matches
// events to them. NUL-separated so a schema/table pair can never
// collide with a differently-split pair.
func progressTableKey(schema, name string) string {
	return schema + "\x00" + name
}

// qualifiedProgressTable renders a (schema, table) pair for error
// messages.
func qualifiedProgressTable(schema, name string) string {
	if schema == "" {
		return name
	}
	return schema + "." + name
}
