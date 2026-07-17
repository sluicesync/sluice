// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// stubReplicationProber is a fake [replicationCapabilityProber]
// returning a canned (canReplicate, role) pair plus an optional error.
// Used by the unit tests to exercise the gate + refusal-formatting paths
// without booting a real PG container. callCount lets tests assert the
// capability gate short-circuits BEFORE the prober is consulted (the
// postgres-trigger exclusion).
type stubReplicationProber struct {
	canReplicate bool
	role         string
	err          error
	callCount    int
}

func (s *stubReplicationProber) SourceReplicationCapability(_ context.Context) (canReplicate bool, role string, err error) {
	s.callCount++
	if s.err != nil {
		return false, "", s.err
	}
	return s.canReplicate, s.role, nil
}

// TestPreflightSourceReplication_GateExcludesPostgresTrigger is the KEY
// exclusion test: postgres-trigger is the slot-less RECOMMENDED FIX. Its
// SchemaReader delegates to the composed postgres.Engine, so it DOES
// satisfy replicationCapabilityProber — its declared CDCTriggers
// capability is the only thing that excludes it. A prober that WOULD refuse (cannot replicate)
// must never be consulted for postgres-trigger.
func TestPreflightSourceReplication_GateExcludesPostgresTrigger(t *testing.T) {
	prober := &stubReplicationProber{canReplicate: false, role: "heroku_like"}
	if err := preflightSourceReplication(context.Background(), prober, capsTriggerPG); err != nil {
		t.Errorf("postgres-trigger must never refuse on replication preflight; got %v", err)
	}
	if prober.callCount != 0 {
		t.Errorf("expected the capability gate to short-circuit BEFORE the prober is consulted; got %d calls",
			prober.callCount)
	}
}

// TestPreflightSourceReplication_GateExcludesMySQL: MySQL sources never
// create a slot — the gate must skip them even when (defensively) a
// prober is present.
func TestPreflightSourceReplication_GateExcludesMySQL(t *testing.T) {
	prober := &stubReplicationProber{canReplicate: false, role: "mysql_role"}
	if err := preflightSourceReplication(context.Background(), prober, capsMySQL); err != nil {
		t.Errorf("mysql source must not refuse on replication preflight; got %v", err)
	}
	if prober.callCount != 0 {
		t.Errorf("expected mysql to short-circuit at the gate; got %d prober calls", prober.callCount)
	}
}

// TestPreflightSourceReplication_GateExcludesEmptyCaps: zero
// Capabilities (defensive — an unset test stub) must not fire — only
// a declared CDCLogicalReplication capability does.
func TestPreflightSourceReplication_GateExcludesEmptyCaps(t *testing.T) {
	prober := &stubReplicationProber{canReplicate: false, role: "anyone"}
	if err := preflightSourceReplication(context.Background(), prober, ir.Capabilities{}); err != nil {
		t.Errorf("zero-capability source must not refuse; got %v", err)
	}
	if prober.callCount != 0 {
		t.Errorf("expected zero-capability source to short-circuit at the gate; got %d prober calls", prober.callCount)
	}
}

// TestPreflightSourceReplication_NonProberHandleSkips: a `postgres`
// source whose handle doesn't implement the prober surface skips
// silently — the opportunistic-skip posture matches preflightRLS.
func TestPreflightSourceReplication_NonProberHandleSkips(t *testing.T) {
	// stubWriterNoChecker (from preflight_test.go) doesn't implement
	// replicationCapabilityProber.
	if err := preflightSourceReplication(context.Background(), stubWriterNoChecker{}, capsSlotPG); err != nil {
		t.Errorf("expected nil when handle lacks replicationCapabilityProber; got %v", err)
	}
}

// TestPreflightSourceReplication_CapableRolePasses: a superuser or
// REPLICATION-enabled role on a `postgres` source passes (the positive
// control's unit-test counterpart).
func TestPreflightSourceReplication_CapableRolePasses(t *testing.T) {
	prober := &stubReplicationProber{canReplicate: true, role: "sluice_super"}
	if err := preflightSourceReplication(context.Background(), prober, capsSlotPG); err != nil {
		t.Errorf("expected nil for a replication-capable role; got %v", err)
	}
	if prober.callCount != 1 {
		t.Errorf("expected exactly one prober call for a `postgres` source; got %d", prober.callCount)
	}
}

// TestPreflightSourceReplication_IncapableRoleRefuses is the core
// behaviour: a `postgres` source whose role cannot create a slot refuses
// loudly with errReplicationRefused and a message naming the role plus
// the postgres-trigger recovery hint.
func TestPreflightSourceReplication_IncapableRoleRefuses(t *testing.T) {
	prober := &stubReplicationProber{canReplicate: false, role: "heroku_like"}
	err := preflightSourceReplication(context.Background(), prober, capsSlotPG)
	if err == nil {
		t.Fatal("expected refusal; got nil")
	}
	if !errors.Is(err, errReplicationRefused) {
		t.Errorf("expected errors.Is(errReplicationRefused); got %v", err)
	}
	msg := err.Error()
	for _, want := range []string{
		`"heroku_like"`,                        // connecting role named
		"REPLICATION",                          // mechanism named
		"ALTER ROLE",                           // grant-attribute recovery (a)
		"Google Cloud SQL",                     // (a) works verbatim there (live-validated 2026-07-16)
		"superuser",                            // alternative-role recovery (b)
		"rds_replication",                      // RDS/Aurora membership recovery (c) — the F1 provider-aware hint
		"GRANT rds_replication TO heroku_like", // the concrete custom-role remedy, naming the role
		"postgres-trigger",                     // slot-less recovery (d) — the key hint
		"--source-driver",                      // how to engage it
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("error message missing %q\nfull: %s", want, msg)
		}
	}
}

// TestPreflightSourceReplication_ProbeErrorPropagates: a transient probe
// failure (network, catalog-mismatch) must surface — silently treating
// it as "can replicate" would defer to the raw mid-cold-start permission
// error this preflight exists to replace.
func TestPreflightSourceReplication_ProbeErrorPropagates(t *testing.T) {
	prober := &stubReplicationProber{err: errors.New("connection reset probing pg_roles")}
	err := preflightSourceReplication(context.Background(), prober, capsSlotPG)
	if err == nil {
		t.Fatal("expected error; got nil")
	}
	if !strings.Contains(err.Error(), "connection reset probing pg_roles") {
		t.Errorf("expected probe error wrapped verbatim; got %v", err)
	}
	if errors.Is(err, errReplicationRefused) {
		t.Errorf("probe-error must NOT masquerade as a clean refusal; got %v", err)
	}
}

// TestFormatReplicationRefusal_NamesRoleAndHints pins the message shape
// directly (independent of the gate plumbing).
func TestFormatReplicationRefusal_NamesRoleAndHints(t *testing.T) {
	msg := formatReplicationRefusal("essential_db_user")
	if !strings.Contains(msg, `"essential_db_user"`) {
		t.Errorf("expected role named; got %q", msg)
	}
	if !strings.Contains(msg, "postgres-trigger") {
		t.Errorf("expected postgres-trigger hint; got %q", msg)
	}
	if !strings.Contains(msg, "logical replication slot") {
		t.Errorf("expected the slot mechanism explained; got %q", msg)
	}
	if !strings.Contains(msg, "GRANT rds_replication TO essential_db_user") {
		t.Errorf("expected the RDS/Aurora membership remedy naming the role; got %q", msg)
	}
	if !strings.Contains(msg, "cloudsqlsuperuser") {
		t.Errorf("expected the Cloud SQL self-service note on recovery (a); got %q", msg)
	}
}
