// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The binding half of the laneapply decode-type-stability premise
// (2026-08-07 invariant sweep). Two facts were each pinned and nothing
// bound them: this package's change-chunk round trip WIDENS a sized
// integer to its 64-bit form, and internal/laneapply tags a key value by
// its Go kind. Chain-restore's incremental replay and the from-backup
// broker feed chunk-decoded changes into the concurrent lane router at a
// default concurrency of 4, so if the two sides ever disagreed about a
// kind, one row's INSERT and its later DELETE would land on two lanes and
// commit out of source order — silently.

package blobcodec

import (
	"bytes"
	"hash/fnv"
	"io"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/laneapply"
)

// canonicalKeyBytes is what the lane hash actually consumes for one key
// value — [laneapply.WriteCanonicalKeyValue]'s output, captured verbatim so
// a divergence is reported as bytes rather than as a lane index that
// happens to collide.
func canonicalKeyBytes(v any) string {
	var b bytes.Buffer
	laneapply.WriteCanonicalKeyValue(&b, v)
	return b.String()
}

// roundTripKeyValue sends v through the REAL change-chunk writer and reader
// as a single-column primary key on an Insert, and returns the value the
// reader handed back. Deliberately the whole codec rather than
// encodeValue/decodeValue directly: the JSON boundary between them is where
// a number's Go kind is actually decided.
func roundTripKeyValue(t *testing.T, v any) any {
	t.Helper()
	buf := &bytes.Buffer{}
	w, err := NewChangeChunkWriter(buf, nil, CodecGzip, nil)
	if err != nil {
		t.Fatalf("NewChangeChunkWriter: %v", err)
	}
	in := ir.Insert{
		Position: ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"0/100"}`},
		Schema:   "public",
		Table:    "t",
		Row:      ir.Row{"id": v},
	}
	if err := w.WriteChange(in); err != nil {
		t.Fatalf("WriteChange: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	r, err := NewChangeChunkReader(nopReadCloserFromBytes(buf.Bytes()), w.Hash(), nil, CodecGzip, nil)
	if err != nil {
		t.Fatalf("NewChangeChunkReader: %v", err)
	}
	defer func() { _ = r.Close() }()
	c, err := r.ReadChange()
	if err != nil {
		t.Fatalf("ReadChange: %v", err)
	}
	got, ok := c.(ir.Insert)
	if !ok {
		t.Fatalf("ReadChange returned %T; want ir.Insert", c)
	}
	return got.Row["id"]
}

// TestCanonicalKeyValue_SurvivesTheBackupRoundTrip is the gate that binds
// this package's type fold to the lane router's type tags.
//
// WHAT IT ASSERTS, and why it is bytes rather than lanes: for every Go kind
// a CDC reader can place in a primary-key column, the canonical key encoding
// of the value BEFORE it enters a backup chunk must equal the encoding of
// the value the chunk reader hands back. Comparing lane indices would pass
// on a collision; comparing the hashed bytes cannot.
//
// SCOPE, stated at the gate. It reaches the FAMILY-and-WIDTH axis of the
// fold, which is the one both sides can disagree about silently. It does not
// reach a change to the on-disk tag SET (a new `_t` a reader does not know
// is a loud decode error, which `TestChangeChunk_UnknownTag` covers), and it
// does not reach the live readers themselves — that is
// TestCDCDecodeTypeStableAcrossChangeKinds in the engine packages.
func TestCanonicalKeyValue_SurvivesTheBackupRoundTrip(t *testing.T) {
	utc := time.Date(2026, 8, 7, 12, 34, 56, 123456789, time.UTC)
	for _, tc := range []struct {
		name string
		v    any
		// wantStable is false only where the fold is KNOWN to move the
		// canonical bytes. Recording those cells is the point: an
		// exemption with a reason beats a matrix that quietly omits them.
		wantStable bool
		why        string
	}{
		{name: "int64", v: int64(42), wantStable: true},
		{name: "int", v: 42, wantStable: true},
		{name: "int32", v: int32(42), wantStable: true},
		{name: "int16", v: int16(42), wantStable: true},
		{name: "int8", v: int8(42), wantStable: true},
		{name: "uint64", v: uint64(42), wantStable: true},
		{name: "uint", v: uint(42), wantStable: true},
		{name: "uint32", v: uint32(42), wantStable: true},
		{name: "uint16", v: uint16(42), wantStable: true},
		{name: "uint8", v: uint8(42), wantStable: true},
		{name: "string", v: "k-42", wantStable: true},
		{name: "bytes", v: []byte{0x01, 0x02, 0xff}, wantStable: true},
		{name: "bool", v: true, wantStable: true},
		{name: "nil", v: nil, wantStable: true},
		{name: "float64", v: float64(1.5), wantStable: true},
		{
			name: "float32", v: float32(0.1), wantStable: true,
			why: "the fold widens to float64, and %v renders the shortest form of each — equal for a float32-representable value",
		},
		{
			name: "time.Time UTC", v: utc, wantStable: true,
			why: "already UTC, so the fold's UTC normalisation is a no-op",
		},
		{
			name: "time.Time with an offset", v: utc.In(time.FixedZone("plus2", 2*3600)), wantStable: false,
			why: "KNOWN RESIDUAL: the fold normalises to UTC and the '?' fallback renders the offset, so a TIMESTAMPTZ primary key " +
				"routes differently after a chunk round trip. Reachable only by keying a table on a timestamptz; recorded so the " +
				"exposure is measured, and it is the reason the router doc names time.Time explicitly",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := canonicalKeyBytes(tc.v)
			after := canonicalKeyBytes(roundTripKeyValue(t, tc.v))
			stable := before == after
			if stable != tc.wantStable {
				if tc.wantStable {
					t.Errorf("canonical key bytes changed across the backup round trip: %q -> %q.\n"+
						"A chunk-replayed change and a live-read change for the SAME row would hash to different lanes, "+
						"so their apply order is no longer guaranteed. Either restore the fold or add the kind to "+
						"laneapply.WriteCanonicalKeyValue's alias set.", before, after)
					return
				}
				t.Errorf("canonical key bytes are now STABLE (%q) for a cell recorded as a known residual (%s) — "+
					"if the residual was fixed, flip wantStable and delete the reason", before, tc.why)
			}
		})
	}
}

// TestCanonicalKeyValue_FamiliesStillDoNotAlias is the other direction of
// the widening change: aliasing the integer WIDTHS onto one tag must not
// have aliased the FAMILIES. int64(1), uint64(1), "1", true and nil are five
// different keys and must stay five different hashes.
func TestCanonicalKeyValue_FamiliesStillDoNotAlias(t *testing.T) {
	seen := map[string]string{}
	for _, tc := range []struct {
		name string
		v    any
	}{
		{"int64", int64(1)},
		{"uint64", uint64(1)},
		{"string", "1"},
		{"bytes", []byte("1")},
		{"bool", true},
		{"nil", nil},
		{"float64", float64(1)},
	} {
		got := canonicalKeyBytes(tc.v)
		if prev, dup := seen[got]; dup {
			t.Errorf("%s and %s both canonicalise to %q — two different keys would share a lane slot and, worse, "+
				"a routing decision would depend on which one arrived", tc.name, prev, got)
		}
		seen[got] = tc.name
	}
	// Anti-vacuity: the encoder must actually have written something for
	// every case, or "all distinct" is a statement about empty strings.
	if len(seen) != 7 {
		t.Fatalf("distinct canonical encodings = %d; want 7", len(seen))
	}
}

// TestCanonicalKeyValue_WidthsAliasWithinAFamily states the property the
// widening buys, separately from the round trip, so a reader can see it
// without running the codec.
func TestCanonicalKeyValue_WidthsAliasWithinAFamily(t *testing.T) {
	signed := []any{int8(7), int16(7), int32(7), int(7), int64(7)}
	want := canonicalKeyBytes(int64(7))
	for _, v := range signed {
		if got := canonicalKeyBytes(v); got != want {
			t.Errorf("%T(7) canonicalises to %q; want %q (every signed width must share the int64 encoding)", v, got, want)
		}
	}
	unsigned := []any{uint8(7), uint16(7), uint32(7), uint(7), uint64(7)}
	wantU := canonicalKeyBytes(uint64(7))
	for _, v := range unsigned {
		if got := canonicalKeyBytes(v); got != wantU {
			t.Errorf("%T(7) canonicalises to %q; want %q (every unsigned width must share the uint64 encoding)", v, got, wantU)
		}
	}
	if want == wantU {
		t.Fatal("the signed and unsigned families collapsed onto one encoding — the width aliasing went one step too far")
	}
}

// TestCanonicalKeyBytesFeedTheLaneHash is the anti-vacuity floor for the
// three tests above: it proves canonicalKeyBytes is what the router hashes,
// so an assertion about those bytes is an assertion about lanes.
func TestCanonicalKeyBytesFeedTheLaneHash(t *testing.T) {
	r := laneapply.NewRouter(8)
	// Reproduce LaneFor's own composition: table name, separator, then
	// each value's canonical bytes followed by a separator.
	want := r.LaneFor("public.t", []any{int64(42)})
	h := fnv.New64a()
	_, _ = h.Write([]byte("public.t"))
	_, _ = h.Write([]byte{0})
	_, _ = io.WriteString(h, canonicalKeyBytes(int64(42)))
	_, _ = h.Write([]byte{0})
	if got := int(h.Sum64() % 8); got != want {
		t.Fatalf("recomposed lane = %d; router said %d — canonicalKeyBytes is not what LaneFor hashes, "+
			"so the round-trip assertions above prove nothing about routing", got, want)
	}
}
