// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// stubLOProber is a fake [largeObjectPreflightProber] with a canned
// census. callCount asserts the capability gate short-circuits before
// the prober is consulted.
type stubLOProber struct {
	loCount   int64
	suspects  map[string][]string
	err       error
	callCount int
}

func (s *stubLOProber) LargeObjectCensus(_ context.Context) (loCount int64, suspects map[string][]string, err error) {
	s.callCount++
	if s.err != nil {
		return 0, nil, s.err
	}
	return s.loCount, s.suspects, nil
}

// loExclude builds the deny-list filter the WARN scoping rides (Bug 205:
// the census runs before ReadSchema, so scoping is filter-based, not
// parsed-schema-based — the same in-scope notion as warnForeignTables).
func loExclude(t *testing.T, tables ...string) migcore.TableFilter {
	t.Helper()
	f, err := migcore.NewTableFilter(nil, tables)
	if err != nil {
		t.Fatalf("NewTableFilter: %v", err)
	}
	return f
}

// TestWarnLargeObjects_GateExcludesNonPG: pg_largeobject is a
// PG-server concept — MySQL/zero-caps sources never consult the prober.
func TestWarnLargeObjects_GateExcludesNonPG(t *testing.T) {
	for _, caps := range []ir.Capabilities{capsMySQL, {}} {
		prober := &stubLOProber{loCount: 5}
		warnLargeObjects(context.Background(), prober, caps, migcore.TableFilter{})
		if prober.callCount != 0 {
			t.Errorf("caps %+v: expected the gate to short-circuit; got %d prober calls", caps, prober.callCount)
		}
	}
}

// TestWarnLargeObjects_NonProberHandleSkips: a handle without the
// census surface skips silently.
func TestWarnLargeObjects_NonProberHandleSkips(t *testing.T) {
	logs := captureLogs(t)
	warnLargeObjects(context.Background(), stubWriterNoChecker{}, capsSlotPG, migcore.TableFilter{})
	if strings.Contains(logs.String(), "large object") {
		t.Errorf("non-prober handle must stay silent:\n%s", logs.String())
	}
}

// TestWarnLargeObjects_NoLargeObjectsStaysSilent: an empty
// pg_largeobject_metadata makes the whole census a no-op — no WARN on
// the ordinary source.
func TestWarnLargeObjects_NoLargeObjectsStaysSilent(t *testing.T) {
	logs := captureLogs(t)
	prober := &stubLOProber{loCount: 0, suspects: map[string][]string{"docs": {"blob_ref"}}}
	warnLargeObjects(context.Background(), prober, capsSlotPG, migcore.TableFilter{})
	if strings.Contains(logs.String(), "WARN") || strings.Contains(logs.String(), "large object") {
		t.Errorf("no-lo source must stay silent:\n%s", logs.String())
	}
}

// TestWarnLargeObjects_InScopeSuspectsNamed is the core advisory: los
// exist AND in-scope oid/lo columns exist → the full WARN naming every
// suspect table.column, the coming unsupported-type refusal (Bug 205:
// an in-scope oid/lo column refuses at the schema read, so the WARN is
// the refusal's context, not a copies-as-integers claim), and the
// recovery pointers.
func TestWarnLargeObjects_InScopeSuspectsNamed(t *testing.T) {
	logs := captureLogs(t)
	prober := &stubLOProber{
		loCount: 3,
		suspects: map[string][]string{
			"docs":     {"blob_ref", "thumb_ref"},
			"excluded": {"other_ref"}, // out of scope — must NOT be named
		},
	}
	warnLargeObjects(context.Background(), prober, capsSlotPG, loExclude(t, "excluded"))
	out := logs.String()
	for _, want := range []string{
		"3 large object(s)",
		"docs.blob_ref", "docs.thumb_ref", // in-scope suspects named
		"schema read will refuse", // the coming refusal, contextualized
		"--exclude-table",         // proceed-without-them recovery
		"docs/type-mapping.md",    // the carry-recipes pointer
	} {
		if !strings.Contains(out, want) {
			t.Errorf("WARN missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "excluded.other_ref") {
		t.Errorf("out-of-scope suspect must not be named:\n%s", out)
	}
}

// TestWarnLargeObjects_NoInScopeSuspectsQuieterWarn: los exist but no
// in-scope column could reference them → the single quieter WARN.
func TestWarnLargeObjects_NoInScopeSuspectsQuieterWarn(t *testing.T) {
	logs := captureLogs(t)
	prober := &stubLOProber{
		loCount:  7,
		suspects: map[string][]string{"excluded": {"other_ref"}},
	}
	warnLargeObjects(context.Background(), prober, capsSlotPG, loExclude(t, "excluded"))
	out := logs.String()
	for _, want := range []string{"7 large object(s)", "no in-scope column is typed oid/lo"} {
		if !strings.Contains(out, want) {
			t.Errorf("quiet WARN missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "excluded.other_ref") {
		t.Errorf("quiet WARN must not name out-of-scope suspects:\n%s", out)
	}
}

// TestWarnLargeObjects_ProbeFailureSkipsSilently is the KEY advisory
// posture: a failed census (managed-PG permission variance) skips with
// a DEBUG note only — never a WARN, never an error, no new failure
// mode on a path that worked before.
func TestWarnLargeObjects_ProbeFailureSkipsSilently(t *testing.T) {
	logs := captureLogs(t)
	prober := &stubLOProber{err: errors.New("permission denied for pg_largeobject_metadata")}
	warnLargeObjects(context.Background(), prober, capsSlotPG, migcore.TableFilter{})
	out := logs.String()
	if strings.Contains(out, "level=WARN") {
		t.Errorf("probe failure must not WARN (advisory census):\n%s", out)
	}
	if !strings.Contains(out, "census probe failed") {
		t.Errorf("expected the DEBUG breadcrumb:\n%s", out)
	}
}
