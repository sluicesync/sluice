// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// AddedTableCopyPositionsColumn records each added table's replay window
// ([ir.AddedTableReplayWindowStore], Bugs 333 and 336): the source position
// its copy is complete at, the key its replay addresses rows by and the
// columns a complete after-image carries. NULL means nothing recorded. Only
// its own writes ensure it.
const AddedTableCopyPositionsColumn = "added_table_copy_positions"

// The entry tags. An entry is NUL-separated parts, the table first:
//
//	<table> \x00 copy \x00 <engine> \x00 <token> \x00 <n> \x00 <key column>×n \x00 <column>...
//	<table> \x00 none
//
// where n is the key's column count in decimal and the columns after the key
// are the table's non-generated columns. It rides the table-set codec
// ([EncodeTableSet]), which quotes each entry to plain ASCII, so a token or
// name round-trips byte for byte through any column character set. A NUL
// cannot occur in a table or column name on either engine, and a position
// token is the engine's own text encoding (JSON, a GTID set, an LSN), which
// never holds one; the encoder refuses a part that does rather than write an
// entry that would split differently on the way back.
const (
	replayWindowTagCopy = "copy"
	replayWindowTagNone = "none"
)

// recoveryCopyPositions is the recovery an undecodable copy-position value
// names. Clearing it by hand would drop a window still owed to a replay, so
// the recovery is the re-copy, whose cold start overwrites the value.
const recoveryCopyPositions = "re-copy the stream with `sluice sync start --restart-from-scratch`, whose cold start " +
	"overwrites the record without reading it; clearing the column by hand instead would let a restart replay an " +
	"added table's changes onto its copy unguarded (Bug 333)"

// EncodeReplayWindowEntry renders one window as its entry, refusing a window
// whose parts could not come back as they went in.
func EncodeReplayWindowEntry(w ir.AddedTableReplayWindow) (string, error) {
	if w.Table == "" {
		return "", errors.New("added-table replay window: empty table name")
	}
	parts := []string{w.Table}
	if !w.HasWindow() {
		if len(w.Key) != 0 || len(w.Columns) != 0 || w.CopyPosition.Engine != "" {
			return "", fmt.Errorf("added-table replay window for %q: a window with no copy position carries no key, columns or engine", w.Table)
		}
		parts = append(parts, replayWindowTagNone)
	} else {
		if w.CopyPosition.Engine == "" {
			return "", fmt.Errorf("added-table replay window for %q: copy position names no engine", w.Table)
		}
		if len(w.Key) == 0 {
			return "", fmt.Errorf("added-table replay window for %q: no key columns", w.Table)
		}
		if len(w.Columns) == 0 {
			return "", fmt.Errorf("added-table replay window for %q: no columns", w.Table)
		}
		parts = append(parts, replayWindowTagCopy, w.CopyPosition.Engine, w.CopyPosition.Token, strconv.Itoa(len(w.Key)))
		for _, c := range append(append([]string(nil), w.Key...), w.Columns...) {
			if c == "" {
				return "", fmt.Errorf("added-table replay window for %q: empty column name", w.Table)
			}
			parts = append(parts, c)
		}
	}
	for _, p := range parts {
		if strings.IndexByte(p, 0) >= 0 {
			return "", fmt.Errorf("added-table replay window for %q: a part holds a NUL byte, which the entry cannot carry", w.Table)
		}
	}
	return strings.Join(parts, "\x00"), nil
}

// DecodeReplayWindowEntry parses an entry [EncodeReplayWindowEntry] wrote. A
// malformed entry is an error, never a skipped one: a window that cannot be
// read must refuse the start rather than let the replay run unguarded.
func DecodeReplayWindowEntry(entry string) (ir.AddedTableReplayWindow, error) {
	parts := strings.Split(entry, "\x00")
	if len(parts) < 2 || parts[0] == "" {
		return ir.AddedTableReplayWindow{}, fmt.Errorf("added-table replay window entry %q is not <table>\\x00<tag>…", entry)
	}
	w := ir.AddedTableReplayWindow{Table: parts[0]}
	switch parts[1] {
	case replayWindowTagNone:
		if len(parts) != 2 {
			return ir.AddedTableReplayWindow{}, fmt.Errorf("added-table replay window entry %q: a none entry carries nothing after its tag", entry)
		}
		return w, nil
	case replayWindowTagCopy:
		if len(parts) < 7 || parts[2] == "" || parts[3] == "" {
			return ir.AddedTableReplayWindow{}, fmt.Errorf("added-table replay window entry %q is not <table>\\x00copy\\x00<engine>\\x00<token>\\x00<n>\\x00<key>…\\x00<column>…", entry)
		}
		n, err := strconv.Atoi(parts[4])
		if err != nil || n < 1 || strconv.Itoa(n) != parts[4] || len(parts) < 5+n+1 {
			return ir.AddedTableReplayWindow{}, fmt.Errorf("added-table replay window entry %q: key count %q is not a decimal count its parts can hold", entry, parts[4])
		}
		for _, c := range parts[5:] {
			if c == "" {
				return ir.AddedTableReplayWindow{}, fmt.Errorf("added-table replay window entry %q names an empty column", entry)
			}
		}
		w.CopyPosition = ir.Position{Engine: parts[2], Token: parts[3]}
		w.Key = append([]string(nil), parts[5:5+n]...)
		w.Columns = append([]string(nil), parts[5+n:]...)
		return w, nil
	}
	return ir.AddedTableReplayWindow{}, fmt.Errorf("added-table replay window entry %q has unknown tag %q", entry, parts[1])
}

// replayWindowEntryTable is an entry's table, well-formed or not: the text
// before its first NUL.
func replayWindowEntryTable(entry string) string { return pendingEntryTable(entry) }

// ReplayWindowsRecord is the [TableSetEdit] that writes windows, replacing
// any entry for the same tables. An entry of another table is kept as it is,
// even one this binary cannot decode: it is not this write's to judge.
func ReplayWindowsRecord(windows []ir.AddedTableReplayWindow) (TableSetEdit, error) {
	entries, err := encodeReplayWindows(windows)
	if err != nil {
		return TableSetEdit{}, err
	}
	return TableSetEdit{Apply: func(set map[string]struct{}, _ bool, _ time.Time) (map[string]struct{}, bool) {
		for entry := range set {
			if _, replaced := entries[replayWindowEntryTable(entry)]; replaced {
				delete(set, entry)
			}
		}
		for _, e := range entries {
			set[e] = struct{}{}
		}
		return set, true
	}}, nil
}

// ReplayWindowsReset is the [TableSetEdit] that replaces every entry with
// windows (NULL when there are none). A replace: an undecodable value is
// overwritten without being read.
func ReplayWindowsReset(windows []ir.AddedTableReplayWindow) (TableSetEdit, error) {
	entries, err := encodeReplayWindows(windows)
	if err != nil {
		return TableSetEdit{}, err
	}
	return TableSetEdit{Replace: true, Apply: func(map[string]struct{}, bool, time.Time) (map[string]struct{}, bool) {
		if len(entries) == 0 {
			return nil, true
		}
		next := make(map[string]struct{}, len(entries))
		for _, e := range entries {
			next[e] = struct{}{}
		}
		return next, true
	}}, nil
}

// encodeReplayWindows encodes windows keyed by table, refusing two windows
// for one table (which would make the stored value depend on map order).
func encodeReplayWindows(windows []ir.AddedTableReplayWindow) (map[string]string, error) {
	out := make(map[string]string, len(windows))
	for _, w := range windows {
		if _, dup := out[w.Table]; dup {
			return nil, fmt.Errorf("added-table replay windows: two windows for table %q", w.Table)
		}
		e, err := EncodeReplayWindowEntry(w)
		if err != nil {
			return nil, err
		}
		out[w.Table] = e
	}
	return out, nil
}

// ReadReplayWindows reads streamID's windows, sorted by table. No row, no
// control table, no column and NULL read as none; a value that does not
// decode, as a whole or entry by entry, is an [ir.UndecodableTableSetError].
func ReadReplayWindows(ctx context.Context, db *sql.DB, cfg *ControlTableConfig, q TableSetQueries, streamID string) ([]ir.AddedTableReplayWindow, error) {
	stored, _, err := readRow(ctx, db, cfg, q, streamID)
	if err != nil || !stored.Valid {
		return nil, err
	}
	windows, err := DecodeReplayWindows(stored.String)
	if err != nil {
		return nil, fmt.Errorf("%s: read %s of stream %q: %w", cfg.EngineName, q.Column, streamID, undecodable(q.Column, stored.String, err))
	}
	return windows, nil
}

// DecodeReplayWindows decodes a stored value into its windows, sorted by
// table, refusing a malformed line or entry and a table named twice.
func DecodeReplayWindows(stored string) ([]ir.AddedTableReplayWindow, error) {
	entries, err := DecodeTableSet(stored)
	if err != nil {
		return nil, err
	}
	out := make([]ir.AddedTableReplayWindow, 0, len(entries))
	seen := make(map[string]bool, len(entries))
	for _, e := range entries {
		w, err := DecodeReplayWindowEntry(e)
		if err != nil {
			return nil, err
		}
		if seen[w.Table] {
			return nil, fmt.Errorf("added-table replay windows: table %q is recorded twice", w.Table)
		}
		seen[w.Table] = true
		out = append(out, w)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Table < out[j].Table })
	return out, nil
}
