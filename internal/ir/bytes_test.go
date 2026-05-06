package ir

import (
	"testing"
	"time"
)

// TestApproximateRowBytes covers each arm of the byte-walk type
// switch so a new IR type added in the future without an arm here
// is caught when the metric drops to zero.
//
// Mirrors the pipeline-package test that exercised the same shape
// before v0.7.0 hoisted the helper into ir for reuse on the
// memory-bounded streaming path (ADR-0028).
func TestApproximateRowBytes(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		row  Row
		want int64
	}{
		{"nil row", nil, 0},
		{"empty row", Row{}, 0},
		{"string", Row{"s": "hello"}, 5},
		{"bytes", Row{"b": []byte{1, 2, 3}}, 3},
		{"bool", Row{"b": true}, 1},
		{"int8", Row{"i": int8(7)}, 1},
		{"int16", Row{"i": int16(7)}, 2},
		{"int32 + float32", Row{"i": int32(1), "f": float32(2.0)}, 8},
		{"int64 + float64", Row{"i": int64(1), "f": float64(2.0)}, 16},
		{"int + uint", Row{"i": 1, "u": uint(2)}, 16},
		{"time.Time", Row{"t": now}, 24},
		{"nil value contributes nothing", Row{"n": nil, "s": "x"}, 1},
		{"[]any of strings", Row{"a": []any{"foo", "bar"}}, 6},
		{"[]string", Row{"a": []string{"abc", "de"}}, 5},
		{"unknown type contributes zero", Row{"x": struct{ A int }{}}, 0},
		{
			"mixed row",
			Row{
				"id":    int64(123),
				"email": "alice@example.com",
				"flag":  true,
				"ts":    now,
				"data":  []byte{1, 2, 3, 4},
			},
			8 + 17 + 1 + 24 + 4,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := ApproximateRowBytes(c.row)
			if got != c.want {
				t.Errorf("ApproximateRowBytes(%#v) = %d; want %d", c.row, got, c.want)
			}
		})
	}
}

// TestApproximateChangeBytes pins each [Change] variant's byte
// contribution so an Update mistakenly counted as a single row's
// worth (or a Truncate accidentally non-zero) shows up here.
func TestApproximateChangeBytes(t *testing.T) {
	cases := []struct {
		name string
		c    Change
		want int64
	}{
		{
			"Insert sums Row",
			Insert{Row: Row{"id": int64(1), "name": "alice"}},
			8 + 5,
		},
		{
			"Update sums Before+After",
			Update{
				Before: Row{"id": int64(1), "name": "alice"},
				After:  Row{"id": int64(1), "name": "alicia"},
			},
			(8 + 5) + (8 + 6),
		},
		{
			"Delete sums Before",
			Delete{Before: Row{"id": int64(1), "name": "alice"}},
			8 + 5,
		},
		{"Truncate carries no row data", Truncate{}, 0},
		{"TxBegin carries no row data", TxBegin{}, 0},
		{"TxCommit carries no row data", TxCommit{}, 0},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := ApproximateChangeBytes(c.c)
			if got != c.want {
				t.Errorf("ApproximateChangeBytes(%T) = %d; want %d", c.c, got, c.want)
			}
		})
	}
}
