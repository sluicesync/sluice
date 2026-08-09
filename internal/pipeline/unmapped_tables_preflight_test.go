// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// probeApplier is a ChangeApplier that answers the target-table probe from a
// fixed set, so the preflight's DECISION is under test rather than any engine's
// catalog.
type probeApplier struct {
	ir.ChangeApplier
	present map[string]bool
	err     error
	asked   []string
}

func (p *probeApplier) TargetTableExists(_ context.Context, schema, table string) (bool, error) {
	qn := schema + "." + table
	p.asked = append(p.asked, qn)
	if p.err != nil {
		return false, p.err
	}
	return p.present[qn], nil
}

// noProbeApplier deliberately does NOT implement ir.TargetTableProbe.
type noProbeApplier struct{ ir.ChangeApplier }

// preflightStreamer builds a Streamer whose source-schema read is stubbed to
// the given tables, so the phase can be driven without a database.
func preflightStreamer(t *testing.T, tables []sourceTableRef, allow bool) *Streamer {
	t.Helper()
	s := &Streamer{AllowUnmappedTables: allow}
	s.testInScopeTables = func(context.Context) ([]sourceTableRef, error) { return tables, nil }
	return s
}

// TestUnmappedTablePreflight_RefusesAndNamesEveryMissingTable pins the refusal
// and, as much, its CONTENT: an operator who cannot act on the message is no
// better off than one who got the old silent skip.
func TestUnmappedTablePreflight_RefusesAndNamesEveryMissingTable(t *testing.T) {
	s := preflightStreamer(t, []sourceTableRef{
		{Schema: "public", Name: "orders"},
		{Schema: "public", Name: "audit_log"},
		{Schema: "public", Name: "sessions"},
	}, false)
	applier := &probeApplier{present: map[string]bool{"public.orders": true}}

	err := s.phasePreflightUnmappedTables(context.Background(), applier, "wave-a")
	if err == nil {
		t.Fatal("expected a refusal when in-scope tables are absent on the target")
	}

	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeSyncUnmappedTables {
		t.Errorf("refusal should carry CodeSyncUnmappedTables; got %+v", ce)
	}

	msg := err.Error()
	// EVERY missing table, not just the first — the whole point over MySQL's
	// old mid-run halt is that one run tells you the complete list.
	for _, want := range []string{"public.audit_log", "public.sessions"} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal does not name %q:\n%s", want, msg)
		}
	}
	if strings.Contains(msg, "public.orders") {
		t.Errorf("refusal names a table that DOES exist on the target:\n%s", msg)
	}
	// The remedy has to be the command that copies existing rows; a bare
	// CREATE would leave the table holding only future changes.
	if !strings.Contains(msg, "sluice schema add-table public.audit_log") {
		t.Errorf("refusal does not give the per-table add-table command:\n%s", msg)
	}
	if !strings.Contains(msg, "--allow-unmapped-tables") {
		t.Errorf("refusal does not mention the opt-out:\n%s", msg)
	}
}

// TestUnmappedTablePreflight_AllMappedIsSilent is the over-refusal control, and
// it is the half that would have caught the Bug 236 shape in this area: a check
// that refuses everything satisfies the refusal test above perfectly.
func TestUnmappedTablePreflight_AllMappedIsSilent(t *testing.T) {
	s := preflightStreamer(t, []sourceTableRef{
		{Schema: "public", Name: "orders"},
		{Schema: "public", Name: "items"},
	}, false)
	applier := &probeApplier{present: map[string]bool{
		"public.orders": true,
		"public.items":  true,
	}}

	if err := s.phasePreflightUnmappedTables(context.Background(), applier, "wave-a"); err != nil {
		t.Fatalf("a fully-mapped stream must start silently; got %v", err)
	}
	if len(applier.asked) != 2 {
		t.Errorf("probed %d tables; want 2 — the preflight must actually ask", len(applier.asked))
	}
}

// TestUnmappedTablePreflight_OptOutStarts pins the escape hatch, including that
// it is the FLAG doing the work rather than the check having quietly stopped.
func TestUnmappedTablePreflight_OptOutStarts(t *testing.T) {
	tables := []sourceTableRef{{Schema: "public", Name: "audit_log"}}
	applier := &probeApplier{present: map[string]bool{}}

	if err := preflightStreamer(t, tables, true).
		phasePreflightUnmappedTables(context.Background(), applier, "wave-a"); err != nil {
		t.Fatalf("--allow-unmapped-tables must start; got %v", err)
	}
	// The same input WITHOUT the flag must refuse, or the test above proves
	// nothing about the flag.
	if err := preflightStreamer(t, tables, false).
		phasePreflightUnmappedTables(context.Background(), &probeApplier{present: map[string]bool{}}, "wave-a"); err == nil {
		t.Fatal("the identical stream without the opt-out must refuse")
	}
}

// TestUnmappedTablePreflight_ZeroValueRefuses pins the v0.99.51 zero-value
// trap directly: a Streamer built anywhere other than the CLI — a test, the
// broker, a future caller — must inherit the LOUD behaviour.
func TestUnmappedTablePreflight_ZeroValueRefuses(t *testing.T) {
	var s Streamer
	if s.AllowUnmappedTables {
		t.Fatal("the zero value must be refuse, not allow — a field named for the " +
			"on-behaviour silently inverts to off for every non-CLI construction")
	}
}

// TestUnmappedTablePreflight_SkipsWithoutProbe pins the optional-surface
// posture, and names it as a GAP rather than a pass.
func TestUnmappedTablePreflight_SkipsWithoutProbe(t *testing.T) {
	s := preflightStreamer(t, []sourceTableRef{{Schema: "public", Name: "nope"}}, false)
	if err := s.phasePreflightUnmappedTables(context.Background(), &noProbeApplier{}, "wave-a"); err != nil {
		t.Fatalf("an applier without the probe must be skipped, not refused; got %v", err)
	}
}

// TestUnmappedTablePreflight_ProbeErrorPropagates keeps a broken probe from
// reading as "everything is mapped" — the failure direction that would make
// this check silently inert.
func TestUnmappedTablePreflight_ProbeErrorPropagates(t *testing.T) {
	s := preflightStreamer(t, []sourceTableRef{{Schema: "public", Name: "orders"}}, false)
	sentinel := errors.New("catalog unreachable")
	err := s.phasePreflightUnmappedTables(context.Background(), &probeApplier{err: sentinel}, "wave-a")
	if !errors.Is(err, sentinel) {
		t.Fatalf("a probe failure must propagate, not read as mapped; got %v", err)
	}
}

// TestUnmappedTablePreflight_IsActuallyWiredIntoRunOnce closes the gap every
// other test in this file shares: they call the phase directly, so all six
// would still pass if nothing ever invoked it.
//
// A preflight that is never called is indistinguishable from one that always
// passes, and that failure has a track record here — it is the same shape as a
// gate whose walker stopped matching. So this reads runOnce's source and
// requires the call, with the position check that it happens BEFORE the change
// stream opens (refusing after the slot exists costs the operator a slot, a
// position, and possibly applied work).
func TestUnmappedTablePreflight_IsActuallyWiredIntoRunOnce(t *testing.T) {
	src, err := os.ReadFile("streamer.go")
	if err != nil {
		t.Fatalf("read streamer.go: %v", err)
	}
	body := string(src)

	start := strings.Index(body, "func (s *Streamer) runOnce(")
	if start < 0 {
		t.Fatal("runOnce not found in streamer.go — this gate has stopped seeing the code it checks")
	}
	runOnce := body[start:]

	callIdx := strings.Index(runOnce, "s.phasePreflightUnmappedTables(")
	if callIdx < 0 {
		t.Fatal("runOnce never calls phasePreflightUnmappedTables — the unmapped-table refusal " +
			"is dead code and every test above passes anyway")
	}

	openIdx := strings.Index(runOnce, "s.phaseOpenChangeStream(")
	if openIdx < 0 {
		t.Fatal("phaseOpenChangeStream not found in runOnce — the ordering half of this gate " +
			"cannot be checked; re-derive it rather than deleting the assertion")
	}
	if callIdx > openIdx {
		t.Error("the unmapped-table preflight runs AFTER the change stream opens; it must refuse " +
			"before a slot, a position, or any applied work exists")
	}
}
