// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// stubXIDWraparoundProber is a fake [xidWraparoundProber] returning a
// canned (age, datname) pair plus an optional error. callCount lets
// tests assert the engine-name gate short-circuits BEFORE the prober is
// consulted (the MySQL exclusion).
type stubXIDWraparoundProber struct {
	age       int64
	datname   string
	err       error
	callCount int
}

func (s *stubXIDWraparoundProber) SourceXIDWraparoundHorizon(_ context.Context) (age int64, datname string, err error) {
	s.callCount++
	if s.err != nil {
		return 0, "", s.err
	}
	return s.age, s.datname, nil
}

// TestPreflightSourceXIDWraparound_GateExcludesMySQL: MySQL has no
// XID semantics — the gate must skip even when (defensively) a prober
// is present.
func TestPreflightSourceXIDWraparound_GateExcludesMySQL(t *testing.T) {
	prober := &stubXIDWraparoundProber{age: xidWraparoundRefuseThreshold + 1, datname: "mysql_db"}
	if err := preflightSourceXIDWraparound(context.Background(), prober, "mysql"); err != nil {
		t.Errorf("mysql source must not refuse on XID preflight; got %v", err)
	}
	if prober.callCount != 0 {
		t.Errorf("expected mysql to short-circuit at the gate; got %d prober calls", prober.callCount)
	}
}

// TestPreflightSourceXIDWraparound_GateExcludesEmptyEngine: defensive —
// an empty sourceEngine must not fire.
func TestPreflightSourceXIDWraparound_GateExcludesEmptyEngine(t *testing.T) {
	prober := &stubXIDWraparoundProber{age: xidWraparoundRefuseThreshold + 1, datname: "anywhere"}
	if err := preflightSourceXIDWraparound(context.Background(), prober, ""); err != nil {
		t.Errorf("empty sourceEngine must not refuse; got %v", err)
	}
	if prober.callCount != 0 {
		t.Errorf("expected empty engine to short-circuit at the gate; got %d prober calls", prober.callCount)
	}
}

// TestPreflightSourceXIDWraparound_GateIncludesPostgresTrigger is the
// key behaviour difference vs the REPLICATION preflight: the XID risk
// applies to ANY PG-flavoured source, including the slot-less
// postgres-trigger engine. A near-wraparound source remains dangerous
// regardless of the CDC mechanism (trigger-engine's xmin safety-lag
// query holds back autovacuum the same way a logical-slot xmin does).
func TestPreflightSourceXIDWraparound_GateIncludesPostgresTrigger(t *testing.T) {
	prober := &stubXIDWraparoundProber{age: xidWraparoundRefuseThreshold + 1, datname: "near_wrap_db"}
	err := preflightSourceXIDWraparound(context.Background(), prober, "postgres-trigger")
	if err == nil {
		t.Fatal("expected refusal for postgres-trigger near wraparound; got nil")
	}
	if !errors.Is(err, errXIDWraparoundRefused) {
		t.Errorf("expected errors.Is(errXIDWraparoundRefused); got %v", err)
	}
	if prober.callCount != 1 {
		t.Errorf("expected exactly one prober call; got %d", prober.callCount)
	}
}

// TestPreflightSourceXIDWraparound_NonProberHandleSkips: a postgres
// handle that doesn't implement the prober skips silently (matches the
// REPLICATION preflight's opportunistic-skip posture).
func TestPreflightSourceXIDWraparound_NonProberHandleSkips(t *testing.T) {
	if err := preflightSourceXIDWraparound(context.Background(), stubWriterNoChecker{}, "postgres"); err != nil {
		t.Errorf("expected nil when handle lacks xidWraparoundProber; got %v", err)
	}
}

// TestPreflightSourceXIDWraparound_HealthyAgePasses: the common case —
// a database with healthy autovacuum and age well below the threshold
// passes silently. The bench is at 0 (fresh container would be tens of
// thousands max).
func TestPreflightSourceXIDWraparound_HealthyAgePasses(t *testing.T) {
	prober := &stubXIDWraparoundProber{age: 1_000_000, datname: "healthy_db"}
	if err := preflightSourceXIDWraparound(context.Background(), prober, "postgres"); err != nil {
		t.Errorf("expected nil for healthy age; got %v", err)
	}
	if prober.callCount != 1 {
		t.Errorf("expected exactly one prober call; got %d", prober.callCount)
	}
}

// TestPreflightSourceXIDWraparound_BelowThresholdPasses: at exactly
// threshold - 1 the preflight must still pass — the threshold is the
// refusal floor, not a "near" warning.
func TestPreflightSourceXIDWraparound_BelowThresholdPasses(t *testing.T) {
	prober := &stubXIDWraparoundProber{age: xidWraparoundRefuseThreshold - 1, datname: "just_under"}
	if err := preflightSourceXIDWraparound(context.Background(), prober, "postgres"); err != nil {
		t.Errorf("expected nil for age = threshold-1; got %v", err)
	}
}

// TestPreflightSourceXIDWraparound_AtOrAboveThresholdRefuses is the core
// behaviour: at or above the threshold the preflight refuses loudly with
// errXIDWraparoundRefused and a message naming the database, observed
// age, and the canonical recovery action.
func TestPreflightSourceXIDWraparound_AtOrAboveThresholdRefuses(t *testing.T) {
	prober := &stubXIDWraparoundProber{age: xidWraparoundRefuseThreshold, datname: "danger_db"}
	err := preflightSourceXIDWraparound(context.Background(), prober, "postgres")
	if err == nil {
		t.Fatal("expected refusal at threshold; got nil")
	}
	if !errors.Is(err, errXIDWraparoundRefused) {
		t.Errorf("expected errors.Is(errXIDWraparoundRefused); got %v", err)
	}
	msg := err.Error()
	for _, want := range []string{
		`"danger_db"`,       // database named
		"age(datfrozenxid)", // mechanism named
		"VACUUM FREEZE",     // recovery (a)
		"pg_stat_activity",  // recovery (b) — long-lived txn diagnosis
		"autovacuum",        // recovery (c) — wait-for-autovac
		"2,147,483,647",     // wraparound horizon named
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("error message missing %q\nfull: %s", want, msg)
		}
	}
}

// TestPreflightSourceXIDWraparound_ProbeErrorPropagates: a transient
// probe failure must surface — silently treating it as "healthy" would
// defer to the mid-migration write-block error this preflight exists to
// replace.
func TestPreflightSourceXIDWraparound_ProbeErrorPropagates(t *testing.T) {
	prober := &stubXIDWraparoundProber{err: errors.New("connection reset probing pg_database")}
	err := preflightSourceXIDWraparound(context.Background(), prober, "postgres")
	if err == nil {
		t.Fatal("expected error; got nil")
	}
	if !strings.Contains(err.Error(), "connection reset probing pg_database") {
		t.Errorf("expected probe error wrapped verbatim; got %v", err)
	}
	if errors.Is(err, errXIDWraparoundRefused) {
		t.Errorf("probe-error must NOT masquerade as a clean refusal; got %v", err)
	}
}

// TestFormatXIDWraparoundRefusal_NamesDBAndHints pins the message shape
// directly (independent of the gate plumbing).
func TestFormatXIDWraparoundRefusal_NamesDBAndHints(t *testing.T) {
	msg := formatXIDWraparoundRefusal("prod_main", 1_800_000_000)
	if !strings.Contains(msg, `"prod_main"`) {
		t.Errorf("expected database named; got %q", msg)
	}
	if !strings.Contains(msg, "1800000000") && !strings.Contains(msg, "1_800_000_000") && !strings.Contains(msg, "1800000000)") {
		// fmt.Sprintf uses no separator; assert presence of the
		// concrete number in some readable form.
		if !strings.Contains(msg, "1800000000") {
			t.Errorf("expected observed age in message; got %q", msg)
		}
	}
	if !strings.Contains(msg, "VACUUM FREEZE") {
		t.Errorf("expected VACUUM FREEZE recovery; got %q", msg)
	}
}
