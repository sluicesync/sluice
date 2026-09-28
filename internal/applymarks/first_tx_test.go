// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package applymarks

import (
	"log/slog"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/logcapture"
)

// TestConsult_TrustsOnlyTheFirstDeliveredTransaction pins amendment B as a
// runtime check. The 2026-09-28 CRITICAL: a persisted position that regressed
// behind transaction T-1 replays T-1 first — its upsert re-creates row k —
// and then reaches T's delete of k, whose own durable mark would skip it.
// Marks only ever exist for the first transaction after the persisted
// position, so a mark of T is evidence only when T is the first transaction
// the run delivers; otherwise the change applies and the run WARNs once
// with [UntrustedMarker].
func TestConsult_TrustsOnlyTheFirstDeliveredTransaction(t *testing.T) {
	k := ir.Row{"id": int64(7), "u": "x"}
	deleteK := del("uniq", 1, txB, k)
	mark := markFor(t, deleteK, uniq)

	t.Run("the mark's transaction is delivered first: trusted", func(t *testing.T) {
		tr := loaded(t, mark)
		if d, err := tr.Decide(deleteK, uniq); err != nil || !d.Skip {
			t.Fatalf("Decide = %+v, %v; want a skip", d, err)
		}
	})

	t.Run("an earlier transaction is delivered first: applied, not skipped", func(t *testing.T) {
		logs := &logcapture.Buffer{}
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
		t.Cleanup(func() { slog.SetDefault(prev) })

		tr := loaded(t, mark)
		// T-1 (txA) re-creates k: a mark of another transaction proves
		// nothing about it, so it applies.
		if d, err := tr.Decide(ins("uniq", 1, txA, k), uniq); err != nil || d.Skip {
			t.Fatalf("T-1's insert: Decide = %+v, %v; want it applied", d, err)
		}
		if tr.Skips(deleteK, uniq) {
			t.Fatal("Skips trusts a mark of a transaction that was not delivered first")
		}
		d, err := tr.Decide(deleteK, uniq)
		if err != nil || d.Skip {
			t.Fatalf("T's delete: Decide = %+v, %v; want it APPLIED — its mark is not evidence once T-1 replayed first", d, err)
		}
		if got := strings.Count(logs.String(), UntrustedMarker); got != 1 {
			t.Fatalf("%s logged %d times; want once per run:\n%s", UntrustedMarker, got, logs.String())
		}
	})
}
