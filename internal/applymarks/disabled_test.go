// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package applymarks

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestTracker_DisabledIsInertOnEveryMethod is Bug 293's pin (GC-41 (d)): a
// tracker that was never loaded — the zero value, or one the engine
// Disable()d because the mark table is unusable (APPLY-MARKS-UNAVAILABLE) —
// must survive EVERY method with no panic and no bookkeeping, because the
// apply paths call them all unconditionally. v0.156.5's CloseTxs wrote into
// the nil closed map at the default lane path's first checkpoint.
//
// The roster is the method set itself, walked by reflection over *Tracker
// and *TxMarks (whose methods take the tracker), so a method added later is
// called here without anyone listing it. Every argument is drawn from a
// table of NON-EMPTY samples — an empty slice would short-circuit CloseTxs
// before the defect, which is exactly how a zero-argument roster would pass
// on v0.156.5 — and an argument type the table does not know fails the test
// rather than being skipped. Load is the one exclusion: it is what enables
// the tracker. noteOpen, the one unexported map writer the exported methods
// reach only on an enabled tracker, is called by name.
func TestTracker_DisabledIsInertOnEveryMethod(t *testing.T) {
	arms := map[string]func() *Tracker{
		"zero value": func() *Tracker { return &Tracker{} },
		"Disable()d, never loaded": func() *Tracker {
			tr := &Tracker{}
			tr.Disable()
			return tr
		},
	}
	for name, fresh := range arms {
		t.Run(name, func(t *testing.T) {
			tr := fresh()
			called := callEveryMethod(t, reflect.ValueOf(tr), tr)
			called += callEveryMethod(t, reflect.ValueOf(&TxMarks{}), tr)
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("noteOpen panicked on a tracker that was never loaded: %v", r)
					}
				}()
				tr.noteOpen(txA)
			}()
			// Anti-vacuity floor: the Tracker's exported set minus Load is 12
			// methods today and TxMarks' is 3; a walk that reached fewer is not
			// walking the method set.
			if called < 15 {
				t.Fatalf("the roster called %d methods; want at least 15 — the reflection walk is not reaching the method set", called)
			}
			if tr.Enabled() {
				t.Fatal("a disabled tracker enabled itself")
			}
			if tr.open != nil || tr.closed != nil || tr.dirty != nil || tr.swept {
				t.Fatalf("a disabled tracker kept bookkeeping: open %v, closed %v, dirty %v, swept %v", tr.open, tr.closed, tr.dirty, tr.swept)
			}
			var p Pending
			p.Add([]Mark{{Table: uniq.Table, KeyDigest: "k", TxID: txA, Seq: 1}})
			if pl := tr.Plan(&p, true); !pl.Empty() {
				t.Fatalf("a disabled tracker planned mark writes: %+v", pl)
			}
		})
	}
}

// callEveryMethod calls every exported method of recv (but Load) with every
// combination of the sample arguments, failing on a panic, and returns how
// many methods it called.
func callEveryMethod(t *testing.T, recv reflect.Value, tr *Tracker) int {
	t.Helper()
	samples := disabledSamples(tr)
	called := 0
	for i := range recv.NumMethod() {
		m := recv.Type().Method(i)
		if m.Name == "Load" {
			continue
		}
		ft := recv.Method(i).Type()
		choices := make([][]reflect.Value, ft.NumIn())
		for a := range ft.NumIn() {
			vs, ok := samples[ft.In(a)]
			if !ok {
				t.Fatalf("%s.%s takes a %s the roster has no sample for; add non-empty samples to disabledSamples so the method is exercised",
					recv.Type(), m.Name, ft.In(a))
			}
			choices[a] = vs
		}
		for _, args := range combinations(choices) {
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("%s.%s%v panicked on a tracker that was never loaded: %v", recv.Type(), m.Name, describe(args), r)
					}
				}()
				recv.Method(i).Call(args)
			}()
		}
		called++
	}
	return called
}

// disabledSamples is the argument table: every value non-empty, and every
// change class the tracker distinguishes (a secondary-unique insert, a
// primary-key change, a keyless delete) against every subject class.
func disabledSamples(tr *Tracker) map[reflect.Type][]reflect.Value {
	of := func(vs ...any) []reflect.Value {
		out := make([]reflect.Value, len(vs))
		for i, v := range vs {
			out[i] = reflect.ValueOf(v)
		}
		return out
	}
	changes := []ir.Change{
		ins("uniq", 1, txA, ir.Row{"id": int64(1), "u": "x"}),
		upd("pk_only", 2, txA, ir.Row{"id": int64(1)}, ir.Row{"id": int64(2)}),
		del("keyless", 3, txB, ir.Row{"v": "y"}),
	}
	changeVals := make([]reflect.Value, len(changes))
	for i, c := range changes {
		v := reflect.New(reflect.TypeFor[ir.Change]()).Elem()
		v.Set(reflect.ValueOf(c))
		changeVals[i] = v
	}
	mark := Mark{Table: uniq.Table, KeyDigest: "k", TxID: txA, Seq: 1, ScopeDigest: testScope}
	var pending Pending
	pending.Add([]Mark{mark})
	ctx := reflect.New(reflect.TypeFor[context.Context]()).Elem()
	ctx.Set(reflect.ValueOf(context.Background()))
	return map[reflect.Type][]reflect.Value{
		reflect.TypeFor[context.Context](): {ctx},
		reflect.TypeFor[ir.Change]():       changeVals,
		reflect.TypeFor[Subject]():         of(pkOnly, uniq, keyless),
		reflect.TypeFor[string]():          of("mysql"),
		reflect.TypeFor[[]string]():        of([]string{txA, txB}),
		reflect.TypeFor[bool]():            of(true, false),
		reflect.TypeFor[*Pending]():        of(&pending),
		reflect.TypeFor[[]Mark]():          of([]Mark{mark}),
		reflect.TypeFor[*Tracker]():        of(tr),
		reflect.TypeFor[Plan]():            of(Plan{Upserts: []Mark{mark}, Deletes: []string{txB}, closing: []string{txA, txB}}),
	}
}

// combinations is the cartesian product of choices (one empty call for a
// method with no arguments).
func combinations(choices [][]reflect.Value) [][]reflect.Value {
	out := [][]reflect.Value{nil}
	for _, vs := range choices {
		var next [][]reflect.Value
		for _, prefix := range out {
			for _, v := range vs {
				next = append(next, append(append([]reflect.Value{}, prefix...), v))
			}
		}
		out = next
	}
	return out
}

func describe(args []reflect.Value) string {
	s := "("
	for i, a := range args {
		if i > 0 {
			s += ", "
		}
		s += fmt.Sprintf("%T", a.Interface())
	}
	return s + ")"
}
