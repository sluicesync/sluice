// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package applymarks

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"unicode/utf8"

	"sluicesync.dev/sluice/internal/ir"
)

// longVStreamTxID is a VStream transaction identity on a shard that has seen
// six primaries: "vstream:<ks>/<shard>:" plus a six-UUID executed GTID set,
// well past the MySQL mark table's tx_id VARCHAR(255).
func longVStreamTxID(seq int) string {
	parts := make([]string, 0, 6)
	for i := range 6 {
		n := 1000
		if i == 5 {
			n += seq // only the last server UUID differs between fixtures
		}
		parts = append(parts, fmt.Sprintf("%08x-1111-2222-3333-%012x:1-%d", i, i, n))
	}
	return "vstream:commerce/-80:MySQL56/" + strings.Join(parts, ",")
}

// TestMarkTxKey pins the key's contract: a TxID that fits is its own key; a
// longer one becomes a bounded key that is deterministic, idempotent, valid
// UTF-8, and distinct for TxIDs that share their whole kept prefix (the
// digest covers the WHOLE TxID).
func TestMarkTxKey(t *testing.T) {
	for _, short := range []string{"", "pg:7:1:0/1A2B", "gtid:aaaa:1", strings.Repeat("x", MarkTxIDMaxLen)} {
		if got := MarkTxKey(short); got != short {
			t.Errorf("MarkTxKey(%d bytes) = %q; a TxID that fits must be its own key", len(short), got)
		}
	}
	a, b := longVStreamTxID(1), longVStreamTxID(2)
	if len(a) <= MarkTxIDMaxLen || a[:markTxDigestPrefixLen] != b[:markTxDigestPrefixLen] {
		t.Fatalf("fixture: want two TxIDs over %d bytes sharing their first %d (got %d bytes)", MarkTxIDMaxLen, markTxDigestPrefixLen, len(a))
	}
	ka, kb := MarkTxKey(a), MarkTxKey(b)
	switch {
	case len(ka) > MarkTxIDMaxLen:
		t.Errorf("key of a %d-byte TxID is %d bytes; want at most %d", len(a), len(ka), MarkTxIDMaxLen)
	case ka != MarkTxKey(a):
		t.Error("the key is not deterministic")
	case MarkTxKey(ka) != ka:
		t.Error("the key is not its own key: a value keyed twice (a loaded mark, a re-keyed fence id) would stop matching")
	case ka == kb:
		t.Error("two TxIDs that differ past the kept prefix share a key")
	case !strings.HasPrefix(ka, a[:markTxDigestPrefixLen]):
		t.Error("the key does not keep the TxID's readable prefix")
	}
	multi := strings.Repeat("é", 200) // 400 bytes; byte 128 is a rune boundary, 127 is not
	if k := MarkTxKey("x" + multi); !utf8.ValidString(k) || len(k) > MarkTxIDMaxLen {
		t.Errorf("a key cut inside a multi-byte character: valid %v, %d bytes", utf8.ValidString(k), len(k))
	}
}

// TestTracker_LongTxIDRoundTrip is the v0.157.0 review's HIGH at the
// Tracker: a change whose TxID is longer than the MySQL mark column writes a
// mark that fits it, and the same change re-delivered after a crash finds
// that mark (reloaded from the target exactly as written) and is SKIPPED — not
// re-applied, which on a keyless table is a duplicate. Every TxID entry point
// keys the same way: the lane fence's transaction and fold ticket, and the
// checkpoint's CloseTxs, which must retire the stored mark. And a Postgres mark
// written raw before the rule (tx_id TEXT) still matches after Load keys it.
func TestTracker_LongTxIDRoundTrip(t *testing.T) {
	tx := longVStreamTxID(7)
	c := ins("keyless", 1, tx, ir.Row{"v": int64(1)})

	d, err := loaded(t).Decide(c, keyless)
	if err != nil || len(d.Marks) != 1 {
		t.Fatalf("Decide = %+v, %v; want one mark", d, err)
	}
	m := d.Marks[0]
	if len(m.TxID) > MarkTxIDMaxLen {
		t.Fatalf("the mark's tx_id is %d bytes; the MySQL column holds %d", len(m.TxID), MarkTxIDMaxLen)
	}

	raw := m
	raw.TxID = tx
	for _, tc := range []struct {
		name        string
		stored      Mark
		wantDeletes []string
	}{
		{"stored as keyed (this binary)", m, []string{MarkTxKey(tx)}},
		// A Postgres mark written before the rule holds the TxID raw: it
		// still vouches for its change, and the close deletes it under the
		// raw value too, so the row does not linger.
		{"stored raw (a Postgres mark written before the rule)", raw, []string{MarkTxKey(tx), tx}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := loaded(t, tc.stored)
			d, err := tr.Decide(c, keyless)
			if err != nil || !d.Skip {
				t.Fatalf("the re-delivered change: Decide = %+v, %v; want it SKIPPED by its own mark", d, err)
			}
			tr.CloseTxs([]string{tx})
			pl := tr.Plan(nil, true)
			got := append([]string(nil), pl.Deletes...)
			sort.Strings(got)
			want := append([]string(nil), tc.wantDeletes...)
			sort.Strings(want)
			if !reflect.DeepEqual(got, want) {
				t.Errorf("closing the raw TxID must retire the stored mark: plan deletes %q; want %q", pl.Deletes, tc.wantDeletes)
			}
			tr.Committed(pl)
			if len(tr.storedAs) != 0 {
				t.Errorf("the alias outlived its transaction: %v", tr.storedAs)
			}
		})
	}

	var f LaneFence
	f.Open(tx, false)
	if !f.Admits([]Mark{m}, tx) {
		t.Error("the fence opened with the raw TxID does not admit its own keyed mark on its fold")
	}
	if f.Admits([]Mark{m}, "") {
		t.Error("an unanchored fence admitted a non-fold batch")
	}
	f.Anchor(tx)
	if !f.Admits([]Mark{m}, "") {
		t.Error("anchoring with the raw TxID did not anchor the fence")
	}
}
