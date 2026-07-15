// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// stubForeignProber implements [foreignTablePreflightProber] for the
// unit-test surface.
type stubForeignProber struct {
	tables map[string]string
	err    error
}

func (s stubForeignProber) ForeignTables(_ context.Context) (map[string]string, error) {
	return s.tables, s.err
}

// TestWarnForeignTables_NamesTablesAndServers pins the item-68a WARN
// shape: every skipped foreign table is named WITH its foreign server
// (where the data actually lives), the mechanism is explained, and
// both recovery paths (migrate the foreign server; --exclude-table to
// silence) are surfaced. Before this preflight the skip was silent —
// the schema read's BASE-TABLE filter simply dropped them.
func TestWarnForeignTables_NamesTablesAndServers(t *testing.T) {
	logs := captureSlog(t)
	p := stubForeignProber{tables: map[string]string{
		"remote_orders": "erp_server",
		"remote_users":  "crm_server",
	}}
	if err := warnForeignTables(context.Background(), p, capsSlotPG, migcore.TableFilter{}); err != nil {
		t.Fatalf("got %v; want nil (WARN-and-proceed)", err)
	}
	out := logs.String()
	if !strings.Contains(out, "level=WARN") {
		t.Errorf("expected a WARN record; got: %q", out)
	}
	for _, want := range []string{
		"remote_orders", "erp_server",
		"remote_users", "crm_server",
		"foreign server", "--exclude-table",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("WARN should mention %q; got: %q", want, out)
		}
	}
}

// TestWarnForeignTables_ExcludedTablesSilenced pins the documented
// silencer: an operator's `--exclude-table` of a foreign table is an
// acknowledged skip, so it neither WARNs nor errors. With every
// foreign table excluded, no record is emitted at all.
func TestWarnForeignTables_ExcludedTablesSilenced(t *testing.T) {
	logs := captureSlog(t)
	p := stubForeignProber{tables: map[string]string{"remote_orders": "erp_server"}}
	filter, err := migcore.NewTableFilter(nil, []string{"remote_orders"})
	if err != nil {
		t.Fatalf("NewTableFilter: %v", err)
	}
	if err := warnForeignTables(context.Background(), p, capsSlotPG, filter); err != nil {
		t.Fatalf("got %v; want nil", err)
	}
	if out := logs.String(); strings.Contains(out, "remote_orders") {
		t.Errorf("excluded foreign table must not WARN; got: %q", out)
	}
}

// TestWarnForeignTables_Skips pins the three silent no-op gates
// (mirroring the partition preflight): non-PG source, a handle
// without the prober surface, and an empty census.
func TestWarnForeignTables_Skips(t *testing.T) {
	logs := captureSlog(t)
	// Non-PG source short-circuits even with a would-fire prober.
	p := stubForeignProber{tables: map[string]string{"ft": "srv"}}
	if err := warnForeignTables(context.Background(), p, capsMySQL, migcore.TableFilter{}); err != nil {
		t.Errorf("non-PG: got %v; want nil", err)
	}
	// Handle without the prober surface skips silently.
	if err := warnForeignTables(context.Background(), struct{}{}, capsSlotPG, migcore.TableFilter{}); err != nil {
		t.Errorf("no prober: got %v; want nil", err)
	}
	// Empty census is the happy-path no-op.
	if err := warnForeignTables(context.Background(), stubForeignProber{}, capsSlotPG, migcore.TableFilter{}); err != nil {
		t.Errorf("empty census: got %v; want nil", err)
	}
	if out := logs.String(); strings.Contains(out, "foreign") {
		t.Errorf("no gate should have warned; got: %q", out)
	}
}

// TestWarnForeignTables_ProberErrorPropagates pins the fail-loudly
// posture on a probe failure — a census that can't run is a source-
// connection problem, not a reason to silently skip detection.
func TestWarnForeignTables_ProberErrorPropagates(t *testing.T) {
	p := stubForeignProber{err: errors.New("source connection refused")}
	err := warnForeignTables(context.Background(), p, capsSlotPG, migcore.TableFilter{})
	if err == nil {
		t.Fatal("got nil; want prober error propagated")
	}
	if !strings.Contains(err.Error(), "source connection refused") {
		t.Errorf("err should preserve the prober failure; got: %v", err)
	}
}
