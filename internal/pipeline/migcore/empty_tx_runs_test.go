// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// runsScript builds a change stream from a compact script, one letter per
// event: B TxBegin, C TxCommit, r a row (Insert), t a Truncate, s a schema
// snapshot. Every event's position token is its index in the script, so an
// output names exactly which input events survived.
func runsScript(script string) []ir.Change {
	out := make([]ir.Change, 0, len(script))
	for i, ch := range script {
		p := ir.Position{Engine: "test", Token: strconv.Itoa(i)}
		switch ch {
		case 'B':
			out = append(out, ir.TxBegin{Position: p})
		case 'C':
			out = append(out, ir.TxCommit{Position: p})
		case 'r':
			out = append(out, ir.Insert{Position: p, Table: "t", Row: ir.Row{"id": int64(i)}})
		case 't':
			out = append(out, ir.Truncate{Position: p, Table: "t"})
		case 's':
			out = append(out, ir.SchemaSnapshot{Position: p, Table: "t"})
		default:
			panic("runsScript: unknown event " + string(ch))
		}
	}
	return out
}

// runThrough pushes every change of in through r, flushes, and returns the
// indices (position tokens) of what came out, in order.
func runThrough(t *testing.T, r *EmptyTxRuns, in []ir.Change) []int {
	t.Helper()
	var got []int
	emit := func(c ir.Change) error {
		n, err := strconv.Atoi(c.Pos().Token)
		if err != nil {
			t.Fatalf("emitted a change with no index position: %#v", c)
		}
		got = append(got, n)
		return nil
	}
	for _, c := range in {
		if err := r.Push(c, emit); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Flush(emit); err != nil {
		t.Fatal(err)
	}
	if r.Pending() {
		t.Fatal("Pending() after Flush")
	}
	return got
}

// survivors is the independent expected value: an offline restatement of the
// rule over the WHOLE stream, written without the streaming state machine. An
// empty transaction is a TxBegin opened outside any transaction and followed
// immediately by a TxCommit; one is dropped when the very next two events are
// another empty transaction. Everything else survives, in order.
func survivors(in []ir.Change) []int {
	empty := func(i int, open bool) bool {
		if open || i+1 >= len(in) {
			return false
		}
		_, b := in[i].(ir.TxBegin)
		_, c := in[i+1].(ir.TxCommit)
		return b && c
	}
	var keep []int
	open := false
	for i := 0; i < len(in); i++ {
		if empty(i, open) {
			// The next transaction starts outside any transaction too.
			if empty(i+2, false) {
				i++ // drop this pair
				continue
			}
			keep = append(keep, i, i+1)
			i++
			continue
		}
		switch in[i].(type) {
		case ir.TxBegin:
			open = true
		case ir.TxCommit:
			open = false
		}
		keep = append(keep, i)
	}
	return keep
}

func ints(s string) []int {
	if s == "" {
		return nil
	}
	var out []int
	for _, f := range strings.Fields(s) {
		n, _ := strconv.Atoi(f)
		out = append(out, n)
	}
	return out
}

// TestEmptyTxRuns_Shapes pins the Bug 300 rule shape by shape, with the
// survivors written out by hand: only an empty transaction followed by
// another empty one is dropped, and nothing else moves.
func TestEmptyTxRuns_Shapes(t *testing.T) {
	for _, tc := range []struct {
		name, script, want string
		elided             int64
	}{
		{"one empty transaction", "BC", "0 1", 0},
		{"a run keeps its last", "BCBCBC", "4 5", 2},
		{"a run before a real transaction", "BCBCBrC", "2 3 4 5 6", 1},
		{"a real transaction before a run", "BrCBCBC", "0 1 2 5 6", 1},
		{"runs around a real transaction", "BCBCBrrCBCBCBC", "2 3 4 5 6 7 12 13", 3},
		{"real transactions only", "BrCBrrC", "0 1 2 3 4 5 6", 0},
		{"a change outside a transaction ends the run", "BCBCtBCBC", "2 3 4 7 8", 2},
		{"a snapshot outside a transaction ends the run", "BCBCsBC", "2 3 4 5 6", 1},
		{"a snapshot inside a transaction is content", "BCBsC", "0 1 2 3 4", 0},
		{"a begin inside an open transaction is verbatim", "BrBCC", "0 1 2 3 4", 0},
		{"two begins: the first is not empty", "BBCC", "0 1 2 3", 0},
		{"a stray commit", "CBCBC", "0 3 4", 1},
		{"the stream ends inside a transaction with nothing yet", "BCBCB", "2 3 4", 1},
		{"the stream ends inside a transaction with a row", "BCBCBr", "2 3 4 5", 1},
		{"a lone row", "r", "0", 0},
		{"empty stream", "", "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := runsScript(tc.script)
			var r EmptyTxRuns
			got := runThrough(t, &r, in)
			if fmt.Sprint(got) != fmt.Sprint(ints(tc.want)) {
				t.Errorf("%q emitted %v; want %v", tc.script, got, ints(tc.want))
			}
			if fmt.Sprint(survivors(in)) != fmt.Sprint(ints(tc.want)) {
				t.Errorf("the offline oracle disagrees with the hand-written expectation on %q: %v", tc.script, survivors(in))
			}
			if r.Elided() != tc.elided {
				t.Errorf("Elided() = %d; want %d", r.Elided(), tc.elided)
			}
		})
	}
}

// TestEmptyTxRuns_MatchesTheOfflineRule drives random streams over every
// event kind through the streaming runs and compares with [survivors], the
// rule restated over the whole stream at once.
func TestEmptyTxRuns_MatchesTheOfflineRule(t *testing.T) {
	rng := rand.New(rand.NewSource(300))
	alphabet := "BBBBCCCCrrts"
	dropped := 0
	for n := 0; n < 5000; n++ {
		var sb strings.Builder
		for i := rng.Intn(40); i > 0; i-- {
			sb.WriteByte(alphabet[rng.Intn(len(alphabet))])
		}
		in := runsScript(sb.String())
		var r EmptyTxRuns
		got, want := runThrough(t, &r, in), survivors(in)
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("%q: streaming emitted %v; the rule keeps %v", sb.String(), got, want)
		}
		if int(r.Elided())*2 != len(in)-len(got) {
			t.Fatalf("%q: Elided() = %d, but %d events were dropped", sb.String(), r.Elided(), len(in)-len(got))
		}
		dropped += len(in) - len(got)
	}
	// Anti-vacuity: the generator must actually produce runs to collapse.
	if dropped == 0 {
		t.Fatal("no random stream dropped anything: the comparison graded only pass-through")
	}
}

// TestEmptyTxRuns_MaxRunBoundsARun pins the live stream's bound: once MaxRun
// empty transactions of a run have been dropped, the held one is emitted, so
// a source that is busy only elsewhere still persists a position every
// MaxRun+1 transactions.
func TestEmptyTxRuns_MaxRunBoundsARun(t *testing.T) {
	const pairs, maxRun = 25, 10
	in := runsScript(strings.Repeat("BC", pairs))
	r := EmptyTxRuns{MaxRun: maxRun}
	got := runThrough(t, &r, in)
	// Pairs 10 and 21 are emitted by the bound, pair 24 by the flush.
	want := []int{20, 21, 42, 43, 48, 49}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("emitted %v; want %v", got, want)
	}
}

// TestEmptyTxRuns_EmitErrorStops pins that an emit failure (a cancelled
// consumer) is returned rather than swallowed.
func TestEmptyTxRuns_EmitErrorStops(t *testing.T) {
	boom := errors.New("consumer gone")
	var r EmptyTxRuns
	emit := func(ir.Change) error { return boom }
	in := runsScript("BCBr")
	if err := r.Push(in[0], emit); err != nil {
		t.Fatal(err)
	}
	if err := r.Push(in[1], emit); err != nil {
		t.Fatal(err) // held, nothing emitted yet
	}
	if err := r.Push(in[2], emit); err != nil {
		t.Fatal(err) // withheld begin
	}
	if err := r.Push(in[3], emit); !errors.Is(err, boom) {
		t.Fatalf("Push returned %v; want the emit error", err)
	}
}
