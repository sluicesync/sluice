// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"strconv"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestCapture_ApplyIdentityIsTheChangeLogID pins ADR-0190 phase 5 on the
// sqlite-trigger reader (and so the d1-trigger engine that rides it): every
// change carries `sqlite-trigger:<change-log id>`, ordinal 1, and a
// re-delivery from any position reproduces exactly the identities the first
// delivery gave those changes. The independent expected value is each
// change's own position — the change-log id the reader resumes from.
func TestCapture_ApplyIdentityIsTheChangeLogID(t *testing.T) {
	path := newSourceFile(t, `CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)`)
	if _, err := Setup(bg(), path, SetupOptions{Tables: []string{"t"}}); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	for i := 1; i <= 4; i++ {
		exec(t, path, `INSERT INTO t (id, n) VALUES (?, ?)`, i, i*10)
	}
	exec(t, path, `UPDATE t SET n = 99 WHERE id = 2`)
	exec(t, path, `DELETE FROM t WHERE id = 3`)

	deliver := func(from ir.Position, want int) map[string]ir.ApplyID {
		r, err := openCDCReader(bg(), path)
		if err != nil {
			t.Fatalf("openCDCReader: %v", err)
		}
		defer func() { _ = r.(interface{ Close() error }).Close() }()
		out := map[string]ir.ApplyID{}
		for _, c := range collect(t, r, from, want) {
			p, _, err := decodePos(c.Pos())
			if err != nil {
				t.Fatalf("decodePos: %v", err)
			}
			id := ir.ApplyIDOf(c)
			if want := (ir.ApplyID{TxID: EngineName + ":" + strconv.FormatInt(p.LastID, 10), Seq: 1}); id != want {
				t.Errorf("change at change-log id %d carries identity %v; want %v", p.LastID, id, want)
			}
			out[c.Pos().Token] = id
		}
		return out
	}
	first := deliver(pos0(t), 6)
	if len(first) != 6 {
		t.Fatalf("the first delivery holds %d changes; want 6 (4 inserts, an update, a delete)", len(first))
	}
	resume, err := encodePos(sqliteTriggerPos{LastID: 3})
	if err != nil {
		t.Fatalf("encodePos: %v", err)
	}
	again := deliver(resume, 3)
	if len(again) != 3 {
		t.Fatalf("the re-delivery holds %d changes; want 3", len(again))
	}
	for tok, id := range again {
		if first[tok] != id {
			t.Errorf("the change at %s carried %v first and %v on re-delivery", tok, first[tok], id)
		}
	}
}
