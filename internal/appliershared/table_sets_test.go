// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

import (
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"sluicesync.dev/sluice/internal/ir"
)

// TestTableSetCodecRoundTrip pins the table-set codec (pending_added_tables, established_tables)
// over every shape a table name can take on the engines that keep the
// record: the column stores what an add-table recorded, and the warm-resume
// door compares it byte for byte with the names the source reports, so a
// lossy round trip is either a false refusal or a missed one.
func TestTableSetCodecRoundTrip(t *testing.T) {
	names := []string{
		"orders",            // plain
		"Orders",            // case is significant (Postgres)
		"naïve_表",           // non-ASCII UTF-8
		"a,b",               // the separator MySQL's live_added_tables uses
		"line\nbreak",       // this codec's own line separator
		`quo"te`,            // a quote
		`back\slash`,        // an escape character
		" padded ",          // significant whitespace
		"",                  // the empty name
		"\xff\xferaw-bytes", // not UTF-8 (a SQL_ASCII Postgres database)
		"app\x00orders",     // a multi-database entry: namespace NUL table
	}
	for _, n := range names {
		got, err := DecodeTableSet(EncodeTableSet([]string{n}))
		if err != nil {
			t.Fatalf("%q: decode: %v", n, err)
		}
		if len(got) != 1 || got[0] != n {
			t.Fatalf("%q round-tripped to %q", n, got)
		}
	}

	t.Run("the whole set, deduplicated, in any order", func(t *testing.T) {
		in := append(append([]string(nil), names...), names[0], names[3])
		enc := EncodeTableSet(in)
		for i := 0; i < len(enc); i++ {
			if enc[i] >= utf8.RuneSelf {
				t.Fatalf("encoded value is not ASCII at byte %d: %q", i, enc)
			}
		}
		got, err := DecodeTableSet(enc)
		if err != nil {
			t.Fatal(err)
		}
		want := append([]string(nil), names...)
		sort.Strings(want)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %q, want %q", got, want)
		}
		reversed := append([]string(nil), in...)
		for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
			reversed[i], reversed[j] = reversed[j], reversed[i]
		}
		if EncodeTableSet(reversed) != enc {
			t.Fatal("the encoding depends on input order")
		}
	})

	t.Run("the empty set is the empty string", func(t *testing.T) {
		if enc := EncodeTableSet(nil); enc != "" {
			t.Fatalf("got %q", enc)
		}
		if got, err := DecodeTableSet(""); err != nil || len(got) != 0 {
			t.Fatalf("got %q, %v", got, err)
		}
		// A set holding only the empty name is NOT the empty set.
		if enc := EncodeTableSet([]string{""}); enc == "" {
			t.Fatal(`{""} encoded as the empty set`)
		}
	})

	t.Run("an unreadable record is an error, never empty", func(t *testing.T) {
		for _, bad := range []string{"orders", `"unterminated`, "\"ok\"\nbare", "\"ok\"\n"} {
			if got, err := DecodeTableSet(bad); err == nil {
				t.Errorf("%q decoded to %q with no error", bad, got)
			} else if !strings.Contains(err.Error(), "not a quoted table name") {
				t.Errorf("%q: %v", bad, err)
			}
		}
	})
}

// TestPendingEntryCodec pins a pending add-table entry's own codec (S-B): the
// table, both stamps to the millisecond and the token come back exact, and an
// entry missing its token — the round-3 shape, which would let any run
// compare-and-set onto it — or carrying a malformed stamp is an error, never
// a skipped entry. It also pins the cold-start sentinel's disjointness from
// every real entry.
func TestPendingEntryCodec(t *testing.T) {
	started := time.UnixMilli(1_800_000_000_123).UTC()
	for _, table := range []string{"orders", "naïve_表", " padded ", "\xff\xferaw"} {
		in := ir.PendingAddedTable{Table: table, Token: "20261018T050000.000Z-0011223344556677", StartedAt: started, HeartbeatAt: started.Add(90 * time.Second)}
		got, err := DecodePendingEntry(EncodePendingEntry(in))
		if err != nil || got != in {
			t.Fatalf("%q: round trip %+v, %v; want %+v", table, got, err, in)
		}
	}
	for _, bad := range []string{
		"orders\x001\x002",            // no token
		"orders\x001\x002\x00",        // empty token
		"orders\x00x\x002\x00tok",     // malformed start
		"orders\x001\x00y\x00tok",     // malformed heartbeat
		"orders\x001\x002\x00tok\x00", // a fifth field
	} {
		if _, err := DecodePendingEntry(bad); err == nil {
			t.Errorf("%q decoded; want an error", bad)
		}
	}
	if _, err := DecodePendingEntry(ColdStartInProgressEntry); err == nil {
		t.Error("the cold-start sentinel decodes as a pending entry")
	}
	if strings.IndexByte(ColdStartInProgressEntry, 0) != 0 {
		t.Error("the sentinel must lead with NUL: no bare table name or namespace\\x00table key does")
	}
}

// TestNarrowRecord is R4-4's arithmetic. The door read established {a, b}
// and decided the scope is {a, c}; by the time the narrowing locks the row,
// a concurrent `schema add-table c` has completed (established {a, b, c},
// dropped without c). Computed from the locked values, c stays established
// and only b moves; computed from the door's read — the round-4 replace —
// c was overwritten away and its completed copy forgotten.
func TestNarrowRecord(t *testing.T) {
	est, drop, leaving := NarrowRecord([]string{"a", "b", "c"}, []string{"gone", "x"}, []string{"a", "c"}, []string{"gone"}, nil)
	if !reflect.DeepEqual(est, []string{"a", "c"}) || !reflect.DeepEqual(drop, []string{"b", "x"}) || !reflect.DeepEqual(leaving, []string{"b"}) {
		t.Fatalf("established %q dropped %q leaving %q; want [a c] [b x] [b]", est, drop, leaving)
	}
	// A leaving table the source no longer has is forgotten, not dropped.
	est, drop, leaving = NarrowRecord([]string{"a", "b"}, nil, []string{"a"}, []string{"b"}, nil)
	if !reflect.DeepEqual(est, []string{"a"}) || len(drop) != 0 || !reflect.DeepEqual(leaving, []string{"b"}) {
		t.Fatalf("established %q dropped %q leaving %q", est, drop, leaving)
	}
	// Narrowing to nothing records the empty set, not "no record".
	if est, _, _ := NarrowRecord([]string{"a"}, nil, nil, nil, nil); est == nil || len(est) != 0 {
		t.Fatalf("established %#v; want the empty, non-nil set", est)
	}
}

// TestDecodePendingSet is R6 L1's tolerant pending decode: every line comes
// back — well-formed, an entry that does not decode (Table readable), or a
// line that is not even quoted (Table "") — and none is skipped.
func TestDecodePendingSet(t *testing.T) {
	good := ir.PendingAddedTable{Table: "kd", Token: "tok", StartedAt: time.UnixMilli(1).UTC(), HeartbeatAt: time.UnixMilli(2).UTC()}
	stored := EncodeTableSet([]string{EncodePendingEntry(good), "nt\x00broken"}) + "\nnot quoted"
	got := DecodePendingSet(stored)
	if len(got) != 3 {
		t.Fatalf("decoded %d entries, want 3: %+v", len(got), got)
	}
	byRaw := map[string]ir.PendingAddedTable{}
	for _, p := range got {
		byRaw[p.Malformed] = p
	}
	if byRaw[""] != good {
		t.Fatalf("the well-formed entry: %+v", byRaw[""])
	}
	if p := byRaw["nt\x00broken"]; p.Table != "nt" {
		t.Fatalf("an undecodable entry: %+v; want Table nt", p)
	}
	if p, ok := byRaw["not quoted"]; !ok || p.Table != "" {
		t.Fatalf("an unquoted line: %+v; want reported with no table", p)
	}
}

// TestTableSetIDCodec pins the id beside an established entry (R6 rename):
// a name holding a TAB, a NUL key, the sentinel and an entry without an id
// all round-trip, the id-less decoder still reads the value, and an id that
// is not decimal refuses.
func TestTableSetIDCodec(t *testing.T) {
	names := []string{"t8", "tab\tname", "app\x00orders", ColdStartInProgressEntry, "no_id"}
	ids := map[string]string{"t8": "16391", "tab\tname": "7", "app\x00orders": "123456789012"}
	enc := EncodeTableSetIDs(names, ids)
	got, gotIDs, err := DecodeTableSetIDs(enc)
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(got)
	want := append([]string(nil), names...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(gotIDs, ids) {
		t.Fatalf("round trip: %q %v; want %q %v", got, gotIDs, want, ids)
	}
	if plain, err := DecodeTableSet(enc); err != nil || len(plain) != len(names) {
		t.Fatalf("the id-less decoder: %q, %v", plain, err)
	}
	for _, bad := range []string{`"t8"` + "\t", `"t8"` + "\tx1", `"t8"` + "\t1\t2"} {
		if _, _, err := DecodeTableSetIDs(bad); err == nil {
			t.Errorf("%q decoded; want an error", bad)
		}
	}
	// NarrowRecord follows a rename before it narrows.
	est, drop, leaving := NarrowRecord([]string{"kd", "t8"}, nil, []string{"kd", "t8b"}, []string{"t8"}, map[string]string{"t8": "t8b"})
	if !reflect.DeepEqual(est, []string{"kd", "t8b"}) || len(drop) != 0 || len(leaving) != 0 {
		t.Fatalf("a rename: established %q dropped %q leaving %q", est, drop, leaving)
	}
}
