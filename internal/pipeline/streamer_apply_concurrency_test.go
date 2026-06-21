// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// --- ADR-0106 (item 31): fast-by-default apply-concurrency resolution ---
//
// These pins cover the load-bearing contract: the unset/0 case resolves to
// auto:N (NOT serial — the whole point of the ADR), an explicit 1 stays
// serial byte-identically, an explicit N>1 is honored, and auto:N is
// connection-budget-bounded (PG) / fixed-ceiling (MySQL). The zero-value
// trap is the thing this ADR exists NOT to re-introduce, so the "unset →
// auto" pin is the keystone.

// TestResolveApplyConcurrency_ContractMapping pins the
// `--table-parallelism`-style mapping for the explicit (non-auto) cases on a
// target WITHOUT a budget prober (MySQL-shaped), where auto:N is the fixed
// default. This is the pure-contract table: < 0 → 1, 1 → 1, N>1 → N, and
// 0 → the fixed auto default.
func TestResolveApplyConcurrency_ContractMapping(t *testing.T) {
	cases := []struct {
		name string
		in   int
		want int
	}{
		{"negative clamps to serial", -5, 1},
		{"explicit serial opt-out", 1, 1},
		{"explicit two honored", 2, 2},
		{"explicit eight honored above default", 8, 8},
		{"unset resolves to auto default", 0, defaultApplyConcurrency},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := &Streamer{
				Source:           noProberEngine{},
				Target:           noProberEngine{}, // MySQL-shaped: no slot probe
				ApplyConcurrency: c.in,
			}
			if got := s.resolveApplyConcurrency(context.Background()); got != c.want {
				t.Errorf("resolveApplyConcurrency(%d) = %d; want %d", c.in, got, c.want)
			}
		})
	}
}

// TestResolveApplyConcurrency_UnsetIsNotSerial is the keystone pin: the
// DEFAULT (unset, ApplyConcurrency == 0) must resolve to MORE than one lane,
// i.e. concurrency engages by default. A regression here would silently
// revert the ADR to serial-by-default (the v0.99.51 zero-value trap shape).
func TestResolveApplyConcurrency_UnsetIsNotSerial(t *testing.T) {
	s := &Streamer{Source: noProberEngine{}, Target: noProberEngine{}} // ApplyConcurrency == 0
	got := s.resolveApplyConcurrency(context.Background())
	if got <= 1 {
		t.Fatalf("unset --apply-concurrency resolved to %d (serial) — the ADR-0106 default must be auto:N > 1", got)
	}
	if got != defaultApplyConcurrency {
		t.Errorf("unset auto default = %d; want %d", got, defaultApplyConcurrency)
	}
}

// TestResolveApplyConcurrency_MySQLFixedCeiling pins that a MySQL-shaped
// target (no [ir.TargetConnectionBudgetProber]) gets the fixed conservative
// ceiling for the unset case — there is no slot probe to bound it, and
// --max-target-connections is documented inert there.
func TestResolveApplyConcurrency_MySQLFixedCeiling(t *testing.T) {
	s := &Streamer{
		Source:               noProberEngine{},
		Target:               noProberEngine{},
		MaxTargetConnections: 2, // inert on a non-prober target — must NOT clamp
	}
	if got := s.resolveApplyConcurrency(context.Background()); got != defaultApplyConcurrency {
		t.Errorf("MySQL-shaped target unset auto = %d; want fixed ceiling %d (--max-target-connections inert)", got, defaultApplyConcurrency)
	}
}

// TestResolveApplyConcurrency_PGBudgetBoundsLanes pins that a constrained PG
// target yields FEWER lanes: the probe's EffectiveParallelism (clamped to the
// live slot budget) caps the auto value below the fixed default.
func TestResolveApplyConcurrency_PGBudgetBoundsLanes(t *testing.T) {
	eng := &budgetProberEngine{report: ir.ConnectionBudget{
		EffectiveParallelism: 2, // small instance: only 2 lanes fit
		CopyBudget:           2,
		Capped:               true,
	}}
	s := &Streamer{Source: noProberEngine{}, Target: eng, TargetDSN: "dsn"}
	got := s.resolveApplyConcurrency(context.Background())
	if got != 2 {
		t.Fatalf("constrained PG target auto = %d; want 2 (budget-bounded)", got)
	}
	// The probe must be asked for the desired default, bounded by the ceiling.
	if eng.gotReq != defaultApplyConcurrency {
		t.Errorf("probe requested = %d; want %d (the desired lane count)", eng.gotReq, defaultApplyConcurrency)
	}
	if eng.probeCall != 1 {
		t.Errorf("probe called %d times; want exactly 1", eng.probeCall)
	}
}

// TestResolveApplyConcurrency_PGBudgetCapsToDefault pins the upper bound: a
// generous PG instance whose probe would allow more than the default lanes is
// still capped to defaultApplyConcurrency (the auto value is conservative; an
// operator raises it explicitly).
func TestResolveApplyConcurrency_PGBudgetCapsToDefault(t *testing.T) {
	eng := &budgetProberEngine{report: ir.ConnectionBudget{
		EffectiveParallelism: defaultApplyConcurrency, // probe was asked for 4 and granted 4
		CopyBudget:           200,
	}}
	s := &Streamer{Source: noProberEngine{}, Target: eng, TargetDSN: "dsn"}
	if got := s.resolveApplyConcurrency(context.Background()); got != defaultApplyConcurrency {
		t.Errorf("generous PG target auto = %d; want capped at %d", got, defaultApplyConcurrency)
	}
}

// TestResolveApplyConcurrency_PGProbeRefuseDegradesToSerial pins that an
// exhausted-budget refusal does NOT crash the resolution: it degrades to
// serial (1). The cold-start connection-budget preflight owns the loud
// refusal; a warm-resume into a tight target should still run, serially.
func TestResolveApplyConcurrency_PGProbeRefuseDegradesToSerial(t *testing.T) {
	eng := &budgetProberEngine{report: ir.ConnectionBudget{
		Refuse:       true,
		RefusalError: errors.New("budget exhausted"),
	}}
	s := &Streamer{Source: noProberEngine{}, Target: eng, TargetDSN: "dsn"}
	if got := s.resolveApplyConcurrency(context.Background()); got != 1 {
		t.Errorf("refused-budget auto = %d; want 1 (degrade to serial, not crash)", got)
	}
}

// TestResolveApplyConcurrency_PGProbeFailedDegradesToSerial pins that a
// degraded probe (catalog quirk / permission gap on a managed engine) also
// degrades to serial rather than guessing a lane count off a meaningless
// report.
func TestResolveApplyConcurrency_PGProbeFailedDegradesToSerial(t *testing.T) {
	eng := &budgetProberEngine{report: ir.ConnectionBudget{
		ProbeFailed: true,
		Warning:     "catalog quirk",
	}}
	s := &Streamer{Source: noProberEngine{}, Target: eng, TargetDSN: "dsn"}
	if got := s.resolveApplyConcurrency(context.Background()); got != 1 {
		t.Errorf("probe-failed auto = %d; want 1 (degrade to serial)", got)
	}
}

// TestResolveApplyConcurrency_PGProbeOpenErrorDegradesToSerial pins that even
// a connection-open error from the probe degrades to serial here (the broken
// DSN surfaces loudly at the applier open immediately after — the resolver
// must not panic or propagate it as a lane count).
func TestResolveApplyConcurrency_PGProbeOpenErrorDegradesToSerial(t *testing.T) {
	eng := &budgetProberEngine{openErr: errors.New("bad dsn")}
	s := &Streamer{Source: noProberEngine{}, Target: eng, TargetDSN: "dsn"}
	if got := s.resolveApplyConcurrency(context.Background()); got != 1 {
		t.Errorf("probe-open-error auto = %d; want 1 (degrade to serial)", got)
	}
}

// TestResolveApplyConcurrency_ExplicitValueSkipsProbe pins that an explicit
// operator value (1 or N>1) is honored WITHOUT probing — the operator owns
// their target's budget when they set the flag, exactly as before this ADR.
func TestResolveApplyConcurrency_ExplicitValueSkipsProbe(t *testing.T) {
	for _, w := range []int{1, 3, 16} {
		eng := &budgetProberEngine{report: ir.ConnectionBudget{EffectiveParallelism: 2}}
		s := &Streamer{Source: noProberEngine{}, Target: eng, TargetDSN: "dsn", ApplyConcurrency: w}
		if got := s.resolveApplyConcurrency(context.Background()); got != w {
			t.Errorf("explicit W=%d resolved to %d; want it honored verbatim", w, got)
		}
		if eng.probeCall != 0 {
			t.Errorf("explicit W=%d probed %d times; want 0 (no probe on explicit value)", w, eng.probeCall)
		}
	}
}

// recordingConcurrencySetter is a fake applier that implements ONLY
// [ir.ApplyConcurrencySetter] so we can assert whether the streamer plumb
// engaged it.
type recordingConcurrencySetter struct {
	calls int
	lanes int
}

func (r *recordingConcurrencySetter) SetApplyConcurrency(lanes int) {
	r.calls++
	r.lanes = lanes
}

// plainApplierStub implements no optional setters — models a programmatic /
// test applier with no dedicated lane pool. applyApplyConcurrency must be a
// no-op on it (it stays serial), bounding the blast radius of the new default.
type plainApplierStub struct{}

// TestApplyApplyConcurrency_NoSetterStaysSerial pins the blast-radius bound:
// a caller that wires an applier WITHOUT the ApplyConcurrencySetter surface
// (no dedicated lane pool) never engages concurrency, even when the resolved
// lane count is the auto default. This is the zero-value-safe guarantee for
// programmatic/broker/test callers.
func TestApplyApplyConcurrency_NoSetterStaysSerial(t *testing.T) {
	// Must not panic / must be a clean no-op on a non-setter applier.
	applyApplyConcurrency(plainApplierStub{}, defaultApplyConcurrency)

	// And the setter-bearing applier only engages for the resolved W > 1.
	rec := &recordingConcurrencySetter{}
	applyApplyConcurrency(rec, 1) // explicit serial → no-op
	if rec.calls != 0 {
		t.Errorf("lanes=1 engaged the setter %d times; want 0 (serial)", rec.calls)
	}
	applyApplyConcurrency(rec, defaultApplyConcurrency) // auto:N → engage
	if rec.calls != 1 || rec.lanes != defaultApplyConcurrency {
		t.Errorf("lanes=%d: setter calls=%d lanes=%d; want calls=1 lanes=%d", defaultApplyConcurrency, rec.calls, rec.lanes, defaultApplyConcurrency)
	}
}

// TestResolveApplyConcurrency_DryRunSkipsProbe pins that a dry-run resolves to
// the fixed default WITHOUT opening a probe connection (dry-run mutates
// nothing and the lanes never engage).
func TestResolveApplyConcurrency_DryRunSkipsProbe(t *testing.T) {
	eng := &budgetProberEngine{report: ir.ConnectionBudget{EffectiveParallelism: 2}}
	s := &Streamer{Source: noProberEngine{}, Target: eng, TargetDSN: "dsn", DryRun: true}
	if got := s.resolveApplyConcurrency(context.Background()); got != defaultApplyConcurrency {
		t.Errorf("dry-run auto = %d; want fixed default %d", got, defaultApplyConcurrency)
	}
	if eng.probeCall != 0 {
		t.Errorf("dry-run probed %d times; want 0", eng.probeCall)
	}
}
