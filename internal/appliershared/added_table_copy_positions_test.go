// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

import (
	"errors"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"sluicesync.dev/sluice/internal/ir"
)

// TestReplayWindowCodecRoundTrip pins added_table_copy_positions as the codec
// it is (Bug 333; the new-surface checklist): every position family the
// stores carry, every table and key-name shape, comes back byte for byte
// through the stored text, and the stored text is plain ASCII, so no column
// character set can touch it. The engine-produced tokens of each family are
// also round-tripped from their real encoders in the mysql package
// (TestAddedTableReplayWindow_CodecRoundTripsEveryPositionFamily); this one
// covers the shapes no encoder emits today but the codec must not mangle.
func TestReplayWindowCodecRoundTrip(t *testing.T) {
	long := strings.Repeat("3E11FA47-71CA-11E1-9E33-C80AA9429562:1-99999,", 3000) // > 64 KB
	positions := []ir.Position{
		{Engine: "mysql", Token: `{"mode":"gtid","gtid_set":"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5,4e11fa47-71ca-11e1-9e33-c80aa9429562:1-12"}`},
		{Engine: "mysql", Token: `{"mode":"file_pos","file":"binlog.000003","pos":4,"server_uuid":"3e11fa47-71ca-11e1-9e33-c80aa9429562"}`},
		{Engine: "mariadb", Token: `{"mode":"gtid","gtid_set":"0-1-5,1-2-9"}`},
		{Engine: "planetscale", Token: `[{"keyspace":"ks1","shard":"-80","gtid":"MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-50"},{"keyspace":"ks1","shard":"80-","gtid":"MySQL56/4e11fa47-71ca-11e1-9e33-c80aa9429562:1-7","table_p_ks":[{"table_name":"t","lastpk":{"fields":[{"name":"id","type":265}],"rows":[{"lengths":[3],"values":"MTAw"}]}}]}]`},
		{Engine: "postgres", Token: "0/16B3748"},
		{Engine: "mysql", Token: long},
		{Engine: "e", Token: "unicode-é-表-\t-\n-\"-\\-\xff\xfe"},
	}
	tables := []string{"orders", "Orders", "naïve_表", "a,b", "line\nbreak", `quo"te`, " padded ", "\xff\xferaw", "t\tab"}
	keys := [][]string{{"id"}, {"ID"}, {"tenant_id", "id"}, {"naïve col", "x\ny", `"q"`, "\xffk"}}

	all := make([]ir.AddedTableReplayWindow, 0, (len(keys)*len(positions)+1)*len(tables))
	i := 0
	for _, tbl := range tables {
		for _, pos := range positions {
			for _, key := range keys {
				i++
				w := ir.AddedTableReplayWindow{Table: tbl + "_" + strconv.Itoa(i), CopyPosition: pos, Key: key, Columns: append(append([]string(nil), key...), "big", "naïve")}
				all = append(all, w)
				entry, err := EncodeReplayWindowEntry(w)
				if err != nil {
					t.Fatalf("encode %+v: %v", w, err)
				}
				got, err := DecodeReplayWindowEntry(entry)
				if err != nil {
					t.Fatalf("decode %q: %v", entry, err)
				}
				if !reflect.DeepEqual(got, w) {
					t.Fatalf("round trip:\n got %+v\nwant %+v", got, w)
				}
			}
		}
		all = append(all, ir.AddedTableReplayWindow{Table: tbl}) // a "none" entry
	}

	t.Run("the whole stored value, through the table-set codec", func(t *testing.T) {
		edit, err := ReplayWindowsReset(all)
		if err != nil {
			t.Fatal(err)
		}
		next, write := edit.Apply(nil, false, time.Time{})
		if !write {
			t.Fatal("reset wrote nothing")
		}
		stored, _ := encodeNext(next).(string)
		for j := 0; j < len(stored); j++ {
			if stored[j] >= utf8.RuneSelf {
				t.Fatalf("stored value is not ASCII at byte %d", j)
			}
		}
		got, err := DecodeReplayWindows(stored)
		if err != nil {
			t.Fatal(err)
		}
		want := sortedWindows(all)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("stored value round-tripped to %d windows, want %d (first mismatch hidden)", len(got), len(want))
		}
	})
}

// TestReplayWindowCodecRefuses pins the encoder's and decoder's refusals: a
// window that would not come back as it went in is never written, and a
// stored entry that does not decode is an error, never a skipped window
// (skipping it would let a restart replay the table unguarded).
func TestReplayWindowCodecRefuses(t *testing.T) {
	pos := ir.Position{Engine: "mysql", Token: `{"mode":"gtid","gtid_set":"x:1"}`}
	id, cols := []string{"id"}, []string{"id", "v"}
	for name, w := range map[string]ir.AddedTableReplayWindow{
		"empty table":           {CopyPosition: pos, Key: id, Columns: cols},
		"NUL in table":          {Table: "a\x00b", CopyPosition: pos, Key: id, Columns: cols},
		"NUL in token":          {Table: "t", CopyPosition: ir.Position{Engine: "mysql", Token: "a\x00b"}, Key: id, Columns: cols},
		"NUL in engine":         {Table: "t", CopyPosition: ir.Position{Engine: "my\x00sql", Token: "x"}, Key: id, Columns: cols},
		"NUL in key":            {Table: "t", CopyPosition: pos, Key: []string{"i\x00d"}, Columns: cols},
		"NUL in a column":       {Table: "t", CopyPosition: pos, Key: id, Columns: []string{"id", "v\x00"}},
		"no engine":             {Table: "t", CopyPosition: ir.Position{Token: "x"}, Key: id, Columns: cols},
		"no key":                {Table: "t", CopyPosition: pos, Columns: cols},
		"no columns":            {Table: "t", CopyPosition: pos, Key: id},
		"empty key name":        {Table: "t", CopyPosition: pos, Key: []string{"id", ""}, Columns: cols},
		"empty column name":     {Table: "t", CopyPosition: pos, Key: id, Columns: []string{"id", ""}},
		"none carrying a key":   {Table: "t", Key: id},
		"none carrying columns": {Table: "t", Columns: cols},
		"none naming engine":    {Table: "t", CopyPosition: ir.Position{Engine: "mysql"}},
	} {
		if e, err := EncodeReplayWindowEntry(w); err == nil {
			t.Errorf("%s: encoded to %q with no error", name, e)
		}
	}
	if _, err := ReplayWindowsRecord([]ir.AddedTableReplayWindow{{Table: "t"}, {Table: "t"}}); err == nil {
		t.Error("two windows for one table: no error")
	}
	for _, bad := range []string{
		"t",                                     // no tag
		"\x00none",                              // no table
		"t\x00none\x00extra",                    // none with a payload
		"t\x00copy\x00mysql\x00tok\x001\x00id",  // copy without columns
		"t\x00copy\x00\x00tok\x001\x00id\x00id", // copy without an engine
		"t\x00copy\x00mysql\x00\x001\x00id\x00id",    // copy without a token
		"t\x00copy\x00mysql\x00tok\x00x\x00id\x00v",  // a key count that is not a number
		"t\x00copy\x00mysql\x00tok\x000\x00id\x00v",  // a zero key count
		"t\x00copy\x00mysql\x00tok\x0001\x00id\x00v", // a key count not in canonical form
		"t\x00copy\x00mysql\x00tok\x002\x00id\x00v",  // a key count leaving no column
		"t\x00copy\x00mysql\x00tok\x001\x00\x00v",    // an empty key name
		"t\x00copy\x00mysql\x00tok\x001\x00id\x00",   // an empty column name
		"t\x00later\x00mysql\x00tok\x001\x00k\x00k",  // an unknown tag
	} {
		if w, err := DecodeReplayWindowEntry(bad); err == nil {
			t.Errorf("%q decoded to %+v with no error", bad, w)
		}
	}
	dup := EncodeTableSet([]string{"t\x00none", "t\x00copy\x00mysql\x00tok\x001\x00id\x00id"})
	if _, err := DecodeReplayWindows(dup); err == nil {
		t.Error("a table recorded twice decoded with no error")
	}
	if _, err := DecodeReplayWindows("bare"); err == nil {
		t.Error("an unquoted line decoded with no error")
	}
	err := undecodable(AddedTableCopyPositionsColumn, "x", errors.New("boom"))
	if !strings.Contains(err.Error(), "--restart-from-scratch") {
		t.Errorf("the undecodable value names no re-copy recovery: %v", err)
	}
}

// TestReplayWindowEdits pins the two writes: a record replaces only its own
// tables' entries (keeping every other entry exactly, one this binary cannot
// decode included), and a reset replaces everything, NULL when empty.
func TestReplayWindowEdits(t *testing.T) {
	pos := ir.Position{Engine: "mysql", Token: "tok"}
	keep := "other\x00future-tag\x00whatever"
	set := map[string]struct{}{"a\x00none": {}, keep: {}, "b\x00copy\x00mysql\x00old\x001\x00id\x00id": {}}
	edit, err := ReplayWindowsRecord([]ir.AddedTableReplayWindow{{Table: "b", CopyPosition: pos, Key: []string{"id"}, Columns: []string{"id", "v"}}})
	if err != nil {
		t.Fatal(err)
	}
	next, write := edit.Apply(set, true, time.Time{})
	want := map[string]struct{}{"a\x00none": {}, keep: {}, "b\x00copy\x00mysql\x00tok\x001\x00id\x00id\x00v": {}}
	if !write || !reflect.DeepEqual(next, want) {
		t.Fatalf("record: got %q (write=%v), want %q", next, write, want)
	}
	reset, err := ReplayWindowsReset(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reset.Replace {
		t.Fatal("a reset must overwrite an undecodable value without reading it")
	}
	if next, write := reset.Apply(want, true, time.Time{}); !write || next != nil {
		t.Fatalf("an empty reset: got %q (write=%v), want NULL", next, write)
	}
}

func sortedWindows(ws []ir.AddedTableReplayWindow) []ir.AddedTableReplayWindow {
	out := append([]ir.AddedTableReplayWindow(nil), ws...)
	sort.Slice(out, func(i, j int) bool { return out[i].Table < out[j].Table })
	return out
}
