// JSON marshalling for the per-table progress entries within
// [MigrationState.TableProgress].
//
// The wire shape supports two forms inside the table_progress JSON
// map: a bare string ("complete", "no_pk_truncate_and_redo", or the
// legacy v0.3.0 "in_progress") and an object form
// ({"state":"in_progress","last_pk":[...],"rows_copied":N}).
//
// Two reasons the bare-string form survives:
//
//  1. **Backward compatibility with v0.3.0 state rows.** A v0.3.0
//     row stored just `"in_progress"` / `"complete"`. v0.4.0 has to
//     read those rows and decide what to do — see
//     [TableProgress] for the upgrade path. Without
//     accept-string-or-object on the unmarshal side, a mid-flight
//     v0.3.0 → v0.4.0 binary swap would explode on the first
//     resume.
//  2. **Operators do `psql -c "SELECT table_progress FROM
//     sluice_migrate_state"`** to debug. Compact "complete" entries
//     keep the JSON readable; only the cursor-bearing entries pay
//     the cost of the object form.
//
// MarshalJSON emits the bare string for `complete` and
// `no_pk_truncate_and_redo` (no cursor data to preserve), and the
// object form for `in_progress` (the cursor and row count are the
// load-bearing fields). UnmarshalJSON accepts either shape on input.

package ir

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

// tableProgressObject is the on-wire representation of the object form.
// Field tags lock the wire keys so Go-side renames don't break
// deployed state rows.
type tableProgressObject struct {
	State      TableProgressState `json:"state"`
	LastPK     []any              `json:"last_pk,omitempty"`
	RowsCopied int64              `json:"rows_copied,omitempty"`
}

// MarshalJSON emits the compact bare-string form for terminal states
// (`complete`, `no_pk_truncate_and_redo`) and the object form for
// `in_progress` (where the cursor and row count are the load-bearing
// fields). An empty State marshals as the bare string "" so a
// zero-value TableProgress round-trips unchanged.
func (p TableProgress) MarshalJSON() ([]byte, error) {
	switch p.State {
	case TableProgressComplete, TableProgressNoPKTruncateAndRedo:
		// Terminal-style states: nothing useful to carry beyond the
		// label. Emit the bare string for compact wire output.
		return json.Marshal(string(p.State))
	case TableProgressInProgress:
		// In-progress entries carry cursor data; emit the object form
		// so resume can pick the cursor up. omitempty on LastPK and
		// RowsCopied keeps a v0.3.0-style "in_progress with no cursor"
		// degenerate-case write compact, even though the orchestrator
		// shouldn't be producing those — defensive against future
		// callers that pass a zero LastPK.
		return json.Marshal(tableProgressObject(p))
	case "":
		// Zero value. Emit an empty bare string so the round-trip is
		// stable; callers should populate State before writing in
		// practice.
		return json.Marshal("")
	default:
		// Unknown future state. Round-trip via the object form so the
		// information isn't lost.
		return json.Marshal(tableProgressObject(p))
	}
}

// UnmarshalJSON accepts either the bare-string form (v0.3.0 wire
// shape, plus the v0.4.0 compact form for terminal states) or the
// object form (v0.4.0 wire shape with cursor data). Empty input
// returns a zero-valued TableProgress.
//
// A v0.3.0 row with `"in_progress"` decodes into State=in_progress
// with a nil LastPK; the orchestrator interprets the missing cursor
// as "fall back to truncate-and-redo for this table".
func (p *TableProgress) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if len(b) == 0 || bytes.Equal(b, []byte("null")) {
		*p = TableProgress{}
		return nil
	}

	switch b[0] {
	case '"':
		// Bare string form.
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return fmt.Errorf("table progress: decode string form: %w", err)
		}
		*p = TableProgress{State: TableProgressState(s)}
		return nil
	case '{':
		// Object form. Decode strictly enough that an unknown field
		// surfaces during development (and not a silent loss of data
		// on disk) but lenient enough that a future-version row with
		// an extra field doesn't fail the read.
		var obj tableProgressObject
		dec := json.NewDecoder(bytes.NewReader(b))
		// DisallowUnknownFields would be too strict for forward-
		// compat; an older binary reading a newer row should keep
		// going, just without the new field. The contract is
		// documented in the file-level comment.
		if err := dec.Decode(&obj); err != nil {
			return fmt.Errorf("table progress: decode object form: %w", err)
		}
		*p = TableProgress(obj)
		return nil
	}

	return errors.New("table progress: expected JSON string or object")
}
