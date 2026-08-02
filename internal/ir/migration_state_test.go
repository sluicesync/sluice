// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
)

// TestTableProgressMarshalCompactStrings confirms the bare-string
// wire form is used for terminal states (complete and
// no_pk_truncate_and_redo). Compact strings keep the JSON readable
// in psql output and match the v0.3.0 wire shape for `complete`.
func TestTableProgressMarshalCompactStrings(t *testing.T) {
	cases := []struct {
		name string
		in   TableProgress
		want string
	}{
		{"complete", TableProgress{State: TableProgressComplete}, `"complete"`},
		{"no_pk_truncate_and_redo", TableProgress{State: TableProgressNoPKTruncateAndRedo}, `"no_pk_truncate_and_redo"`},
		{"zero", TableProgress{}, `""`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b, err := json.Marshal(c.in)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			if string(b) != c.want {
				t.Errorf("Marshal: got %s; want %s", b, c.want)
			}
		})
	}
}

// TestTableProgressIndexesBuiltWire pins the ADR-0077 IndexesBuilt wire
// contract: a `complete` table with the flag UNSET stays the compact bare
// string (false is the wire default), a `complete` table with the flag SET
// promotes to the object form carrying indexes_built:true, an old
// bare-string `"complete"` row decodes to IndexesBuilt=false (the safe
// "re-feed to the index pool" interpretation), and the object form
// round-trips.
func TestTableProgressIndexesBuiltWire(t *testing.T) {
	// (a) complete + not-yet-indexed → compact bare string.
	b, err := json.Marshal(TableProgress{State: TableProgressComplete, IndexesBuilt: false})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if string(b) != `"complete"` {
		t.Errorf("complete+!built: got %s; want \"complete\"", b)
	}

	// (b) complete + indexed → object form with the flag.
	b, err = json.Marshal(TableProgress{State: TableProgressComplete, IndexesBuilt: true})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if string(b) != `{"state":"complete","indexes_built":true}` {
		t.Errorf("complete+built: got %s; want object form with indexes_built:true", b)
	}

	// (c) old bare-string complete row → IndexesBuilt false (re-feed).
	var old TableProgress
	if err := json.Unmarshal([]byte(`"complete"`), &old); err != nil {
		t.Fatalf("Unmarshal legacy: %v", err)
	}
	if old.State != TableProgressComplete || old.IndexesBuilt {
		t.Errorf("legacy decode: got state=%q built=%v; want complete,false", old.State, old.IndexesBuilt)
	}

	// (d) object form round-trips the flag.
	var rt TableProgress
	if err := json.Unmarshal([]byte(`{"state":"complete","indexes_built":true}`), &rt); err != nil {
		t.Fatalf("Unmarshal object: %v", err)
	}
	if rt.State != TableProgressComplete || !rt.IndexesBuilt {
		t.Errorf("object decode: got state=%q built=%v; want complete,true", rt.State, rt.IndexesBuilt)
	}
}

// TestTableProgressMarshalInProgressObject confirms the in-progress
// state emits the object form with cursor and row count. Round-trips
// through Unmarshal preserve all fields.
func TestTableProgressMarshalInProgressObject(t *testing.T) {
	in := TableProgress{
		State:      TableProgressInProgress,
		LastPK:     []any{int64(42), "abc"},
		RowsCopied: 100,
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	// Object form must contain the load-bearing fields; integer cursor
	// values wear the i64 envelope so they never pass through float64
	// (audit 2026-07-15 HIGH-1), clean strings stay bare.
	want := `{"state":"in_progress","last_pk":[{"_t":"i64","v":42},"abc"],"rows_copied":100}`
	if string(b) != want {
		t.Errorf("Marshal: got %s; want %s", b, want)
	}

	var out TableProgress
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if out.State != in.State {
		t.Errorf("State: got %q; want %q", out.State, in.State)
	}
	if out.RowsCopied != in.RowsCopied {
		t.Errorf("RowsCopied: got %d; want %d", out.RowsCopied, in.RowsCopied)
	}
	// The envelope round-trips the cursor TYPE-exact: int64 in, int64
	// out (a plain decode would have collapsed it to float64).
	if got, want := len(out.LastPK), len(in.LastPK); got != want {
		t.Fatalf("LastPK len: got %d; want %d", got, want)
	}
	if got := out.LastPK[0]; got != int64(42) {
		t.Errorf("LastPK[0]: got %v (%T); want 42 (int64)", got, got)
	}
	if got := out.LastPK[1]; got != "abc" {
		t.Errorf("LastPK[1]: got %v; want \"abc\"", got)
	}
}

// TestTableProgressUnmarshalLegacyString confirms a v0.3.0-shape state
// row decodes correctly into a v0.4.0 TableProgress. The bare strings
// "complete" and "in_progress" are the load-bearing values; the
// post-decode state row has a nil LastPK that the orchestrator
// interprets as "truncate-and-redo on resume" for in-progress, "skip"
// for complete.
func TestTableProgressUnmarshalLegacyString(t *testing.T) {
	cases := []struct {
		raw  string
		want TableProgress
	}{
		{`"complete"`, TableProgress{State: TableProgressComplete}},
		{`"in_progress"`, TableProgress{State: TableProgressInProgress}},
		{`"no_pk_truncate_and_redo"`, TableProgress{State: TableProgressNoPKTruncateAndRedo}},
		{`""`, TableProgress{}},
	}
	for _, c := range cases {
		t.Run(c.raw, func(t *testing.T) {
			var got TableProgress
			if err := json.Unmarshal([]byte(c.raw), &got); err != nil {
				t.Fatalf("Unmarshal %s: %v", c.raw, err)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("Unmarshal %s: got %+v; want %+v", c.raw, got, c.want)
			}
		})
	}
}

// TestTableProgressUnmarshalNullAndEmpty confirms degenerate inputs
// decode to zero values without error.
func TestTableProgressUnmarshalNullAndEmpty(t *testing.T) {
	cases := []string{`null`, ``}
	for _, raw := range cases {
		var got TableProgress
		err := json.Unmarshal([]byte(raw), &got)
		if raw == "" {
			// json.Unmarshal of empty input is itself an error; we
			// only validate that *if* the unmarshaler is invoked it
			// doesn't blow up. Skip the empty case here.
			_ = err
			continue
		}
		if err != nil {
			t.Errorf("Unmarshal %q: %v", raw, err)
		}
		if !reflect.DeepEqual(got, TableProgress{}) {
			t.Errorf("Unmarshal %q: got %+v; want zero value", raw, got)
		}
	}
}

// TestTableProgressMapBackwardCompat exercises the realistic
// upgrade scenario: a state row written by v0.3.0 with bare strings,
// decoded by v0.4.0 into the new map[string]TableProgress shape.
func TestTableProgressMapBackwardCompat(t *testing.T) {
	v03Wire := `{"users":"complete","orders":"in_progress","events":"no_pk_truncate_and_redo"}`
	out := map[string]TableProgress{}
	if err := json.Unmarshal([]byte(v03Wire), &out); err != nil {
		t.Fatalf("Unmarshal v0.3.0 wire: %v", err)
	}
	if out["users"].State != TableProgressComplete {
		t.Errorf("users.State = %q; want complete", out["users"].State)
	}
	if out["orders"].State != TableProgressInProgress {
		t.Errorf("orders.State = %q; want in_progress", out["orders"].State)
	}
	if out["orders"].LastPK != nil {
		t.Errorf("orders.LastPK should be nil for v0.3.0 row; got %v", out["orders"].LastPK)
	}
	if out["events"].State != TableProgressNoPKTruncateAndRedo {
		t.Errorf("events.State = %q; want no_pk_truncate_and_redo", out["events"].State)
	}
}

// TestTableProgressMapMixedShapes covers the realistic v0.4.0 wire
// shape: a mix of bare strings and objects in one map.
func TestTableProgressMapMixedShapes(t *testing.T) {
	wire := `{"users":"complete","orders":{"state":"in_progress","last_pk":[12345],"rows_copied":12345},"products":{"state":"in_progress","last_pk":["a",7],"rows_copied":8000},"events_log":"no_pk_truncate_and_redo"}`
	out := map[string]TableProgress{}
	if err := json.Unmarshal([]byte(wire), &out); err != nil {
		t.Fatalf("Unmarshal mixed wire: %v", err)
	}
	if out["users"].State != TableProgressComplete {
		t.Errorf("users wrong: %+v", out["users"])
	}
	if got := out["orders"]; got.State != TableProgressInProgress || got.RowsCopied != 12345 || len(got.LastPK) != 1 {
		t.Errorf("orders wrong: %+v", got)
	}
	if got := out["products"]; got.State != TableProgressInProgress || got.RowsCopied != 8000 || len(got.LastPK) != 2 {
		t.Errorf("products wrong: %+v", got)
	}
	if out["events_log"].State != TableProgressNoPKTruncateAndRedo {
		t.Errorf("events_log wrong: %+v", out["events_log"])
	}

	// Round-trip back to JSON and confirm it decodes equivalent.
	rb, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("Marshal round-trip: %v", err)
	}
	out2 := map[string]TableProgress{}
	if err := json.Unmarshal(rb, &out2); err != nil {
		t.Fatalf("Unmarshal round-trip: %v", err)
	}
	if out["users"].State != out2["users"].State {
		t.Errorf("round-trip users: %+v vs %+v", out["users"], out2["users"])
	}
	if out["orders"].RowsCopied != out2["orders"].RowsCopied {
		t.Errorf("round-trip orders.RowsCopied: %d vs %d", out["orders"].RowsCopied, out2["orders"].RowsCopied)
	}
}

// TestTableProgressUnmarshalRejectsArray covers the failure shape
// for an obviously-wrong JSON document. Operators inspecting state
// rows manually shouldn't see silent zero-value fallback when they
// typo the JSON.
func TestTableProgressUnmarshalRejectsArray(t *testing.T) {
	var got TableProgress
	err := json.Unmarshal([]byte(`[1,2,3]`), &got)
	if err == nil {
		t.Fatal("Unmarshal succeeded on invalid input; want error")
	}
}

// TestTableProgressJSONBytesAfterMarshal is a tiny smoke test that
// confirms the marshaller doesn't accidentally include trailing
// whitespace or null bytes — sometimes a stringbuilder bug elsewhere
// can pollute the output.
func TestTableProgressJSONBytesAfterMarshal(t *testing.T) {
	in := TableProgress{State: TableProgressComplete}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !bytes.Equal(b, []byte(`"complete"`)) {
		t.Errorf("Marshal output not byte-exact: %q", string(b))
	}
}

// TestTableProgressMarshalChunks confirms the v0.5.0 parallel-copy wire
// shape: an in-progress TableProgress with a populated Chunks slice
// emits both the legacy single-chunk fields (omitted when empty) and
// the chunks array. Round-trips through Unmarshal preserve every chunk.
func TestTableProgressMarshalChunks(t *testing.T) {
	in := TableProgress{
		State: TableProgressInProgress,
		Chunks: []TableChunkProgress{
			{
				ChunkIndex: 0,
				UpperPK:    []any{int64(100)},
				LastPK:     []any{int64(42)},
				RowsCopied: 42,
				State:      TableProgressInProgress,
			},
			{
				ChunkIndex: 1,
				LowerPK:    []any{int64(100)},
				UpperPK:    []any{int64(200)},
				State:      TableProgressComplete,
				RowsCopied: 100,
			},
		},
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	// State on top, chunks array follows. Numeric values land as JSON
	// numbers and decode back as float64 — see the LastPK assertion in
	// TestTableProgressMarshalInProgressObject for the round-trip
	// caveat.
	got := string(b)
	if !bytes.Contains(b, []byte(`"state":"in_progress"`)) {
		t.Errorf("Marshal: missing state=in_progress; got %s", got)
	}
	if !bytes.Contains(b, []byte(`"chunk_index":0`)) {
		t.Errorf("Marshal: missing chunk_index=0; got %s", got)
	}
	if !bytes.Contains(b, []byte(`"chunk_index":1`)) {
		t.Errorf("Marshal: missing chunk_index=1; got %s", got)
	}

	var out TableProgress
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if out.State != TableProgressInProgress {
		t.Errorf("State: got %q; want in_progress", out.State)
	}
	if len(out.Chunks) != 2 {
		t.Fatalf("Chunks: got %d; want 2", len(out.Chunks))
	}
	if out.Chunks[0].ChunkIndex != 0 || out.Chunks[1].ChunkIndex != 1 {
		t.Errorf("Chunks indices: got %d/%d; want 0/1", out.Chunks[0].ChunkIndex, out.Chunks[1].ChunkIndex)
	}
	if out.Chunks[1].State != TableProgressComplete {
		t.Errorf("Chunks[1].State: got %q; want complete", out.Chunks[1].State)
	}
	if out.Chunks[0].RowsCopied != 42 || out.Chunks[1].RowsCopied != 100 {
		t.Errorf("Chunks rows_copied: got %d/%d; want 42/100", out.Chunks[0].RowsCopied, out.Chunks[1].RowsCopied)
	}
}

// TestTableProgressUnmarshalV04Compat confirms that a v0.4.0-shape
// object form (no Chunks field) decodes correctly into a v0.5.0
// TableProgress with Chunks = nil. The orchestrator's classifier
// reads Chunks==nil as the single-chunk path and falls back to the
// v0.4.0 cursor-bearing resume.
func TestTableProgressUnmarshalV04Compat(t *testing.T) {
	v04 := `{"state":"in_progress","last_pk":[12345],"rows_copied":12345}`
	var got TableProgress
	if err := json.Unmarshal([]byte(v04), &got); err != nil {
		t.Fatalf("Unmarshal v0.4.0 wire: %v", err)
	}
	if got.State != TableProgressInProgress {
		t.Errorf("State: got %q; want in_progress", got.State)
	}
	if got.RowsCopied != 12345 {
		t.Errorf("RowsCopied: got %d; want 12345", got.RowsCopied)
	}
	if got.Chunks != nil {
		t.Errorf("Chunks: got %v; want nil for v0.4.0 row", got.Chunks)
	}
}

// TestTableProgressBareStringNeverDropsDetail is the Bug 221 class pin.
//
// The compact bare-string wire form carries EXACTLY one field — State —
// so any other populated field is discarded by it, and discarded
// silently: a reader cannot tell a dropped RowsCopied from a recorded
// zero. `sluice backfill` persists its completed walk as
// {State: complete, RowsCopied: n}; the bare string dropped n on the
// way to the store, and the re-run's --verify evidence gate then
// refused to authorize the contract step, asserting that no run of the
// spec had ever moved a row.
//
// Pin the CLASS, not the representative (the Bug 74 lesson): the matrix
// is every bare-string-ELIGIBLE state × every non-State field, and the
// field axis is derived by reflection so a field ADDED to TableProgress
// fails this test until carriesDetail enumerates it. A field type with
// no sample value below is a failure, never a skip — an unenumerated
// field is precisely what this guard exists to catch.
func TestTableProgressBareStringNeverDropsDetail(t *testing.T) {
	// The states MarshalJSON may render as a bare string. Any state NOT
	// listed here already takes the object form unconditionally.
	bareStringStates := []TableProgressState{
		TableProgressComplete,
		TableProgressNoPKTruncateAndRedo,
		"", // the zero value
	}

	typ := reflect.TypeOf(TableProgress{})
	fields := 0
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.Name == "State" {
			continue
		}
		fields++
		for _, state := range bareStringStates {
			t.Run(string(state)+"/"+field.Name, func(t *testing.T) {
				in := TableProgress{State: state}
				setNonZeroProgressField(t, &in, field.Name)

				b, err := json.Marshal(in)
				if err != nil {
					t.Fatalf("Marshal: %v", err)
				}
				if b[0] == '"' {
					t.Fatalf("%s with %s set marshalled to the bare string %s — the %s is silently dropped",
						state, field.Name, b, field.Name)
				}
				var got TableProgress
				if err := json.Unmarshal(b, &got); err != nil {
					t.Fatalf("Unmarshal %s: %v", b, err)
				}
				if !reflect.DeepEqual(got, in) {
					t.Errorf("round-trip through %s: got %#v; want %#v", b, got, in)
				}
			})
		}
	}
	if fields == 0 {
		t.Fatal("derived zero non-State fields; the reflection walk is vacuous")
	}
}

// setNonZeroProgressField populates ONE named field of p with a
// non-zero value. It fails the test on a field it has no sample for,
// which is what makes TestTableProgressBareStringNeverDropsDetail a
// completeness guard rather than a fixed list.
func setNonZeroProgressField(t *testing.T, p *TableProgress, name string) {
	t.Helper()
	switch name {
	case "LastPK":
		p.LastPK = []any{int64(9007199254740995)}
	case "RowsCopied":
		p.RowsCopied = 500
	case "Chunks":
		p.Chunks = []TableChunkProgress{{
			ChunkIndex: 0,
			UpperPK:    []any{int64(100)},
			LastPK:     []any{int64(42)},
			RowsCopied: 42,
			State:      TableProgressInProgress,
		}}
	case "IndexesBuilt":
		p.IndexesBuilt = true
	default:
		t.Fatalf("TableProgress grew field %q with no sample value here — add one, and make sure "+
			"TableProgress.carriesDetail enumerates the field so the bare-string form can't drop it", name)
	}
}

// TestTableProgressBareStringStillCompactsWhenEmpty is the other half
// of the pin above: an entry with nothing but its label MUST keep the
// compact bare-string form. The promotion is a loss guard, not a
// blanket switch to the object form — operators read these rows in
// psql, and the v0.3.0 wire shape stays byte-identical.
func TestTableProgressBareStringStillCompactsWhenEmpty(t *testing.T) {
	cases := []struct {
		in   TableProgress
		want string
	}{
		{TableProgress{State: TableProgressComplete}, `"complete"`},
		{TableProgress{State: TableProgressNoPKTruncateAndRedo}, `"no_pk_truncate_and_redo"`},
		{TableProgress{}, `""`},
	}
	for _, c := range cases {
		b, err := json.Marshal(c.in)
		if err != nil {
			t.Fatalf("Marshal %#v: %v", c.in, err)
		}
		if string(b) != c.want {
			t.Errorf("Marshal %#v: got %s; want %s", c.in, b, c.want)
		}
	}
}

// TestTableProgressBackfillCompletionRoundTrip pins the exact entry
// `sluice backfill` writes when its walk finishes, at the exact
// boundary that mattered: a completed spec that moved rows keeps its
// count, and a completed spec that genuinely moved none stays the
// compact bare string (so the refusal it produces is honest).
func TestTableProgressBackfillCompletionRoundTrip(t *testing.T) {
	moved := TableProgress{State: TableProgressComplete, RowsCopied: 500}
	b, err := json.Marshal(moved)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if string(b) != `{"state":"complete","rows_copied":500}` {
		t.Errorf("completed-with-work wire shape: got %s", b)
	}
	var got TableProgress
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.RowsCopied != 500 {
		t.Errorf("RowsCopied after round-trip: got %d; want 500", got.RowsCopied)
	}

	none := TableProgress{State: TableProgressComplete}
	if b, err = json.Marshal(none); err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if string(b) != `"complete"` {
		t.Errorf("completed-no-work wire shape: got %s; want \"complete\"", b)
	}
}
