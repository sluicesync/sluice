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

// stubRLSProber is a fake [rlsPreflightProber] returning canned per-
// table RLS state plus a canned role attribute. Used by the unit tests
// to exercise every cell of {RLS on/off} × {role BYPASSRLS yes/no}
// plus the FORCE variant without booting a real PG container.
type stubRLSProber struct {
	role         string
	roleBypass   bool
	roleErr      error
	enabled      map[string]bool
	forced       map[string]bool
	tableErr     error
	tableCalls   []string
	roleCallsCnt int
}

func newStubRLSProber(role string, bypass bool) *stubRLSProber {
	return &stubRLSProber{
		role:       role,
		roleBypass: bypass,
		enabled:    map[string]bool{},
		forced:     map[string]bool{},
	}
}

func (s *stubRLSProber) CurrentRoleBypassesRLS(_ context.Context) (bypass bool, role string, err error) {
	s.roleCallsCnt++
	if s.roleErr != nil {
		return false, "", s.roleErr
	}
	return s.roleBypass, s.role, nil
}

func (s *stubRLSProber) TableRLSStatus(_ context.Context, table *ir.Table) (enabled, forced bool, err error) {
	s.tableCalls = append(s.tableCalls, table.Name)
	if s.tableErr != nil {
		return false, false, s.tableErr
	}
	return s.enabled[table.Name], s.forced[table.Name], nil
}

func twoTableRLSSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{
		{Name: "orders"},
		{Name: "users"},
	}}
}

// TestPreflightRLS_NoRLSAndNoBypass is the lowercase-control case: no
// table has RLS enabled and the role lacks BYPASSRLS — the preflight
// must still pass (RLS-disabled tables migrate identically before and
// after this preflight was added, the load-bearing no-op guarantee).
func TestPreflightRLS_NoRLSAndNoBypass(t *testing.T) {
	schema := twoTableRLSSchema()
	prober := newStubRLSProber("sluice_app", false)
	// enabled map is empty → no table has RLS.
	if err := preflightRLS(context.Background(), schema, prober, rlsSideTarget); err != nil {
		t.Errorf("expected nil on RLS-off / role-without-bypass; got %v", err)
	}
	if prober.roleCallsCnt != 1 {
		t.Errorf("expected exactly one role probe; got %d", prober.roleCallsCnt)
	}
	if len(prober.tableCalls) != 2 {
		t.Errorf("expected both tables probed when role lacks BYPASSRLS; got %v", prober.tableCalls)
	}
}

// TestPreflightRLS_RLSOnAndBypass: role HAS BYPASSRLS so RLS state is
// moot — the preflight passes regardless of how many tables have RLS
// enabled, and per-table probing short-circuits.
func TestPreflightRLS_RLSOnAndBypass(t *testing.T) {
	schema := twoTableRLSSchema()
	prober := newStubRLSProber("sluice_super", true)
	prober.enabled["orders"] = true
	prober.enabled["users"] = true
	prober.forced["orders"] = true
	if err := preflightRLS(context.Background(), schema, prober, rlsSideTarget); err != nil {
		t.Errorf("expected nil when role has BYPASSRLS; got %v", err)
	}
	if len(prober.tableCalls) != 0 {
		t.Errorf("expected zero table probes when role has BYPASSRLS (short-circuit); got %v",
			prober.tableCalls)
	}
}

// TestPreflightRLS_NoRLSAndBypass: trivial no-op, both axes pass.
func TestPreflightRLS_NoRLSAndBypass(t *testing.T) {
	schema := twoTableRLSSchema()
	prober := newStubRLSProber("sluice_super", true)
	if err := preflightRLS(context.Background(), schema, prober, rlsSideSource); err != nil {
		t.Errorf("expected nil; got %v", err)
	}
}

// TestPreflightRLS_RLSOnAndNoBypassRefuses is the silent-loss class
// the preflight closes: RLS enabled on one table + role lacks BYPASSRLS
// → refuse loudly with structured error naming the table, the role,
// the mechanism, and every operator-actionable recovery path.
func TestPreflightRLS_RLSOnAndNoBypassRefuses(t *testing.T) {
	schema := twoTableRLSSchema()
	prober := newStubRLSProber("sluice_app", false)
	prober.enabled["orders"] = true
	err := preflightRLS(context.Background(), schema, prober, rlsSideTarget)
	if err == nil {
		t.Fatal("expected refusal; got nil")
	}
	if !errors.Is(err, errRLSRefused) {
		t.Errorf("expected errors.Is(errRLSRefused); got %v", err)
	}
	msg := err.Error()
	for _, want := range []string{
		`"orders"`,        // offending table named
		`"sluice_app"`,    // connecting role named
		"BYPASSRLS",       // mechanism named
		"ALTER ROLE",      // grant-BYPASSRLS recovery (a)
		"superuser",       // alternative-role recovery (b)
		"--exclude-table", // scope-out recovery (c)
		"target",          // side label
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("error message missing %q\nfull: %s", want, msg)
		}
	}
	// The non-offending table must not appear (sorted offender list
	// only mentions tables that actually triggered the refusal).
	if strings.Contains(msg, `"users"`) {
		t.Errorf("error names a non-offending table; got %q", msg)
	}
}

// TestPreflightRLS_ForceRLSCallsItOut: a FORCE-RLS table needs
// BYPASSRLS regardless of ownership — the message must explicitly
// surface that ("even the table owner is RLS-checked under FORCE")
// so an operator who tried the table-owner workaround knows it won't
// help.
func TestPreflightRLS_ForceRLSCallsItOut(t *testing.T) {
	schema := twoTableRLSSchema()
	prober := newStubRLSProber("sluice_app", false)
	prober.enabled["orders"] = true
	prober.forced["orders"] = true
	err := preflightRLS(context.Background(), schema, prober, rlsSideTarget)
	if err == nil {
		t.Fatal("expected refusal; got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "FORCE RLS") {
		t.Errorf("expected per-table FORCE RLS annotation; got %q", msg)
	}
	if !strings.Contains(msg, "even the table owner is RLS-checked") {
		t.Errorf("expected FORCE-specific explanation; got %q", msg)
	}
}

// TestPreflightRLS_SourceSideHintNamesSilentFiltering: source-side
// refusals must explain the silent-snapshot-filter loss mode (the
// worst class — silent row loss). Target-side refusals explain the
// WITH CHECK INSERT-rejection mode. The wording differs because the
// operator-actionable diagnosis differs.
func TestPreflightRLS_SourceSideHintNamesSilentFiltering(t *testing.T) {
	schema := twoTableRLSSchema()
	prober := newStubRLSProber("sluice_app", false)
	prober.enabled["orders"] = true
	err := preflightRLS(context.Background(), schema, prober, rlsSideSource)
	if err == nil {
		t.Fatal("expected refusal; got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "source") {
		t.Errorf("expected source-side label; got %q", msg)
	}
	if !strings.Contains(msg, "silently filtered") {
		t.Errorf("expected source-side message to name silent-filtering mode; got %q", msg)
	}
	if strings.Contains(msg, "WITH CHECK") {
		t.Errorf("source-side message must not name the target-side WITH CHECK mode; got %q", msg)
	}
}

// TestPreflightRLS_TargetSideHintNamesWithCheck: complement of the
// previous test — target-side wording.
func TestPreflightRLS_TargetSideHintNamesWithCheck(t *testing.T) {
	schema := twoTableRLSSchema()
	prober := newStubRLSProber("sluice_app", false)
	prober.enabled["orders"] = true
	err := preflightRLS(context.Background(), schema, prober, rlsSideTarget)
	if err == nil {
		t.Fatal("expected refusal; got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "target") {
		t.Errorf("expected target-side label; got %q", msg)
	}
	if !strings.Contains(msg, "WITH CHECK") {
		t.Errorf("expected target-side message to name WITH CHECK mode; got %q", msg)
	}
	if strings.Contains(msg, "silently filtered") {
		t.Errorf("target-side message must not name the source-side silent-filter mode; got %q", msg)
	}
}

// TestPreflightRLS_NoProberSurfaceSkips: non-PG engines (or PG handles
// that don't surface the probes) skip silently — matches the
// opportunistic-skip posture of preflightColdStart for engines without
// [ir.TableEmptyChecker].
func TestPreflightRLS_NoProberSurfaceSkips(t *testing.T) {
	schema := twoTableRLSSchema()
	// stubWriterNoChecker (from preflight_test.go) doesn't implement
	// rlsPreflightProber — engines without the surface must not error.
	if err := preflightRLS(context.Background(), schema, stubWriterNoChecker{}, rlsSideTarget); err != nil {
		t.Errorf("expected nil when handle lacks rlsPreflightProber; got %v", err)
	}
}

// TestPreflightRLS_RoleProbeErrorPropagates: a transient probe failure
// (network, permission denied on pg_roles) must surface — silently
// treating it as "no bypass" would be acceptable, silently treating it
// as "bypass" would be a silent-loss class. We surface the error so the
// operator sees it.
func TestPreflightRLS_RoleProbeErrorPropagates(t *testing.T) {
	schema := twoTableRLSSchema()
	prober := newStubRLSProber("sluice_app", false)
	prober.roleErr = errors.New("permission denied on pg_roles")
	err := preflightRLS(context.Background(), schema, prober, rlsSideTarget)
	if err == nil {
		t.Fatal("expected error; got nil")
	}
	if !strings.Contains(err.Error(), "permission denied on pg_roles") {
		t.Errorf("expected probe error wrapped verbatim; got %v", err)
	}
}

// TestPreflightRLS_TableProbeErrorPropagates: same shape for the per-
// table probe.
func TestPreflightRLS_TableProbeErrorPropagates(t *testing.T) {
	schema := twoTableRLSSchema()
	prober := newStubRLSProber("sluice_app", false)
	prober.tableErr = errors.New("connection reset probing pg_class")
	err := preflightRLS(context.Background(), schema, prober, rlsSideSource)
	if err == nil {
		t.Fatal("expected error; got nil")
	}
	if !strings.Contains(err.Error(), "connection reset probing pg_class") {
		t.Errorf("expected probe error wrapped verbatim; got %v", err)
	}
}

// TestPreflightRLS_MultipleOffendersListedSorted: when multiple tables
// would refuse, the message names every one — deterministically sorted
// for stable operator-facing output.
func TestPreflightRLS_MultipleOffendersListedSorted(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{
		{Name: "zeta"}, {Name: "alpha"}, {Name: "mu"},
	}}
	prober := newStubRLSProber("sluice_app", false)
	prober.enabled["zeta"] = true
	prober.enabled["alpha"] = true
	prober.enabled["mu"] = true
	err := preflightRLS(context.Background(), schema, prober, rlsSideTarget)
	if err == nil {
		t.Fatal("expected refusal; got nil")
	}
	msg := err.Error()
	// All three named.
	for _, name := range []string{`"alpha"`, `"mu"`, `"zeta"`} {
		if !strings.Contains(msg, name) {
			t.Errorf("expected %s in error; got %q", name, msg)
		}
	}
	// Sorted: alpha before mu before zeta in the rendered list.
	idxAlpha := strings.Index(msg, `"alpha"`)
	idxMu := strings.Index(msg, `"mu"`)
	idxZeta := strings.Index(msg, `"zeta"`)
	if idxAlpha >= idxMu || idxMu >= idxZeta {
		t.Errorf("expected sorted offender list (alpha < mu < zeta); got positions a=%d m=%d z=%d in %q",
			idxAlpha, idxMu, idxZeta, msg)
	}
}

// TestPreflightRLS_EmptySchemaIsNoOp: zero tables → no probes, no
// refusal. The orchestrator's earlier no-tables-to-migrate
// short-circuit already covers this, but the preflight is defensive.
func TestPreflightRLS_EmptySchemaIsNoOp(t *testing.T) {
	prober := newStubRLSProber("anything", false)
	if err := preflightRLS(context.Background(), &ir.Schema{}, prober, rlsSideTarget); err != nil {
		t.Errorf("expected nil on empty schema; got %v", err)
	}
	if prober.roleCallsCnt != 0 {
		t.Errorf("expected no probes on empty schema; got %d role calls", prober.roleCallsCnt)
	}
}
