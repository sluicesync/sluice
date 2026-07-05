// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"testing"

	"sluicesync.dev/sluice/internal/appliercontrol"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
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
		{"unset resolves to auto default", 0, migcore.DefaultApplyConcurrency},
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
	if got != migcore.DefaultApplyConcurrency {
		t.Errorf("unset auto default = %d; want %d", got, migcore.DefaultApplyConcurrency)
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
	if got := s.resolveApplyConcurrency(context.Background()); got != migcore.DefaultApplyConcurrency {
		t.Errorf("MySQL-shaped target unset auto = %d; want fixed ceiling %d (--max-target-connections inert)", got, migcore.DefaultApplyConcurrency)
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
	if eng.gotReq != migcore.DefaultApplyConcurrency {
		t.Errorf("probe requested = %d; want %d (the desired lane count)", eng.gotReq, migcore.DefaultApplyConcurrency)
	}
	if eng.probeCall != 1 {
		t.Errorf("probe called %d times; want exactly 1", eng.probeCall)
	}
}

// TestResolveApplyConcurrency_PGBudgetCapsToDefault pins the upper bound: a
// generous PG instance whose probe would allow more than the default lanes is
// still capped to migcore.DefaultApplyConcurrency (the auto value is conservative; an
// operator raises it explicitly).
func TestResolveApplyConcurrency_PGBudgetCapsToDefault(t *testing.T) {
	eng := &budgetProberEngine{report: ir.ConnectionBudget{
		EffectiveParallelism: migcore.DefaultApplyConcurrency, // probe was asked for 4 and granted 4
		CopyBudget:           200,
	}}
	s := &Streamer{Source: noProberEngine{}, Target: eng, TargetDSN: "dsn"}
	if got := s.resolveApplyConcurrency(context.Background()); got != migcore.DefaultApplyConcurrency {
		t.Errorf("generous PG target auto = %d; want capped at %d", got, migcore.DefaultApplyConcurrency)
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

// --- ADR-0107 Phase 3 (a): headroom-clamped auto:N ---

// TestClampConcurrencyByHeadroom pins the startup headroom bias on the auto:N
// base (MySQL-shaped target, base = migcore.DefaultApplyConcurrency = 4). Healthy
// headroom keeps the full base; approaching the mark halves it; at/above the
// high-water mark quarters it; and every degrade path (no provider, no fresh
// signal, neither CPU nor mem observed) returns the base UNCHANGED — the
// advisory contract. The clamp never RAISES the base.
func TestClampConcurrencyByHeadroom(t *testing.T) {
	hw := appliercontrol.DefaultTelemetryHighWater
	base := migcore.DefaultApplyConcurrency // 4
	cases := []struct {
		name string
		prov ir.TargetTelemetry
		want int
	}{
		{"nil provider → base", nil, base},
		{
			"healthy cpu → base",
			&fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{SampledAt: freshNow(), CPUUtil: 0.10, CPUKnown: true}},
			base,
		},
		{
			"approaching (0.72) → halved",
			&fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{SampledAt: freshNow(), CPUUtil: 0.72, CPUKnown: true}},
			base / 2,
		},
		{
			"saturated cpu (0.90) → quartered",
			&fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{SampledAt: freshNow(), CPUUtil: 0.90, CPUKnown: true}},
			base / 4,
		},
		{
			"exactly at high-water → quartered",
			&fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{SampledAt: freshNow(), CPUUtil: hw, CPUKnown: true}},
			base / 4,
		},
		{
			"mem drives when cpu unknown (saturated)",
			&fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{SampledAt: freshNow(), MemUtil: 0.95, MemKnown: true}},
			base / 4,
		},
		{
			"busiest of cpu/mem wins (mem hotter)",
			&fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{SampledAt: freshNow(), CPUUtil: 0.10, CPUKnown: true, MemUtil: 0.95, MemKnown: true}},
			base / 4,
		},
		{
			"no fresh signal (ok=false) → base",
			&fakeTelemetry{ok: false, snap: ir.TargetHealthSnapshot{SampledAt: freshNow(), CPUUtil: 0.99, CPUKnown: true}},
			base,
		},
		{
			"neither cpu nor mem observed → base",
			&fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{SampledAt: freshNow(), StorageUtil: 0.99, StorageKnown: true}},
			base,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := &Streamer{Source: noProberEngine{}, Target: noProberEngine{}, TargetTelemetry: c.prov}
			if got := s.clampConcurrencyByHeadroom(context.Background(), base); got != c.want {
				t.Errorf("clampConcurrencyByHeadroom(base=%d) = %d; want %d", base, got, c.want)
			}
		})
	}
}

// TestClampConcurrencyByHeadroom_NeverRaisesOrGoesBelowOne pins the bounds: a
// base of 1 stays 1 even on a saturated target (the clamp never produces 0 /
// serial-below-serial), and a saturated target never RAISES the base.
func TestClampConcurrencyByHeadroom_NeverRaisesOrGoesBelowOne(t *testing.T) {
	sat := &fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{SampledAt: freshNow(), CPUUtil: 0.99, CPUKnown: true}}
	healthy := &fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{SampledAt: freshNow(), CPUUtil: 0.05, CPUKnown: true}}

	s := &Streamer{Source: noProberEngine{}, Target: noProberEngine{}, TargetTelemetry: sat}
	if got := s.clampConcurrencyByHeadroom(context.Background(), 1); got != 1 {
		t.Errorf("base=1 saturated = %d; want 1 (never below serial)", got)
	}
	// A small base of 2, saturated: 2/4 = 0 → floored to 1.
	if got := s.clampConcurrencyByHeadroom(context.Background(), 2); got != 1 {
		t.Errorf("base=2 saturated = %d; want 1 (floored)", got)
	}
	s.TargetTelemetry = healthy
	if got := s.clampConcurrencyByHeadroom(context.Background(), 2); got != 2 {
		t.Errorf("base=2 healthy = %d; want 2 (never raised)", got)
	}
}

// TestAutoApplyConcurrency_HeadroomClampWiredIn pins that the clamp is actually
// on the resolve path: an unset --apply-concurrency on a saturated target
// resolves BELOW the fixed default (the budget base is quartered).
func TestAutoApplyConcurrency_HeadroomClampWiredIn(t *testing.T) {
	sat := &fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{SampledAt: freshNow(), CPUUtil: 0.92, CPUKnown: true}}
	s := &Streamer{Source: noProberEngine{}, Target: noProberEngine{}, TargetTelemetry: sat} // unset → auto:N
	got := s.resolveApplyConcurrency(context.Background())
	if got != migcore.DefaultApplyConcurrency/4 {
		t.Fatalf("saturated-target auto = %d; want %d (default %d quartered)", got, migcore.DefaultApplyConcurrency/4, migcore.DefaultApplyConcurrency)
	}
}

// TestAutoApplyConcurrency_DryRunSkipsHeadroomClamp pins that a dry-run reports
// the policy default even with a saturated telemetry snapshot wired — the plan
// reflects the policy, not a transient load, and does no I/O.
func TestAutoApplyConcurrency_DryRunSkipsHeadroomClamp(t *testing.T) {
	sat := &fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{SampledAt: freshNow(), CPUUtil: 0.99, CPUKnown: true}}
	s := &Streamer{Source: noProberEngine{}, Target: noProberEngine{}, TargetTelemetry: sat, DryRun: true}
	if got := s.resolveApplyConcurrency(context.Background()); got != migcore.DefaultApplyConcurrency {
		t.Errorf("dry-run saturated auto = %d; want fixed default %d (clamp skipped)", got, migcore.DefaultApplyConcurrency)
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
// test applier with no dedicated lane pool. migcore.ApplyApplyConcurrency must be a
// no-op on it (it stays serial), bounding the blast radius of the new default.
type plainApplierStub struct{}

// TestApplyApplyConcurrency_NoSetterStaysSerial pins the blast-radius bound:
// a caller that wires an applier WITHOUT the ApplyConcurrencySetter surface
// (no dedicated lane pool) never engages concurrency, even when the resolved
// lane count is the auto default. This is the zero-value-safe guarantee for
// programmatic/broker/test callers.
func TestApplyApplyConcurrency_NoSetterStaysSerial(t *testing.T) {
	// Must not panic / must be a clean no-op on a non-setter applier.
	migcore.ApplyApplyConcurrency(plainApplierStub{}, migcore.DefaultApplyConcurrency)

	// And the setter-bearing applier only engages for the resolved W > 1.
	rec := &recordingConcurrencySetter{}
	migcore.ApplyApplyConcurrency(rec, 1) // explicit serial → no-op
	if rec.calls != 0 {
		t.Errorf("lanes=1 engaged the setter %d times; want 0 (serial)", rec.calls)
	}
	migcore.ApplyApplyConcurrency(rec, migcore.DefaultApplyConcurrency) // auto:N → engage
	if rec.calls != 1 || rec.lanes != migcore.DefaultApplyConcurrency {
		t.Errorf("lanes=%d: setter calls=%d lanes=%d; want calls=1 lanes=%d", migcore.DefaultApplyConcurrency, rec.calls, rec.lanes, migcore.DefaultApplyConcurrency)
	}
}

// TestResolveApplyConcurrency_DryRunSkipsProbe pins that a dry-run resolves to
// the fixed default WITHOUT opening a probe connection (dry-run mutates
// nothing and the lanes never engage).
func TestResolveApplyConcurrency_DryRunSkipsProbe(t *testing.T) {
	eng := &budgetProberEngine{report: ir.ConnectionBudget{EffectiveParallelism: 2}}
	s := &Streamer{Source: noProberEngine{}, Target: eng, TargetDSN: "dsn", DryRun: true}
	if got := s.resolveApplyConcurrency(context.Background()); got != migcore.DefaultApplyConcurrency {
		t.Errorf("dry-run auto = %d; want fixed default %d", got, migcore.DefaultApplyConcurrency)
	}
	if eng.probeCall != 0 {
		t.Errorf("dry-run probed %d times; want 0", eng.probeCall)
	}
}
