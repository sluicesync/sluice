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
	// Object form must contain the load-bearing fields.
	want := `{"state":"in_progress","last_pk":[42,"abc"],"rows_copied":100}`
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
	// JSON numbers come back as float64; compare element-wise via
	// reflect after coercing the int64 source values to the float64
	// shape the decoder produces.
	if got, want := len(out.LastPK), len(in.LastPK); got != want {
		t.Fatalf("LastPK len: got %d; want %d", got, want)
	}
	if got := out.LastPK[0]; got != float64(42) {
		t.Errorf("LastPK[0]: got %v (%T); want 42 (float64)", got, got)
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
