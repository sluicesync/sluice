package main

import (
	"errors"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// TestDriftError_ExitCode confirms that the drift sentinel reports
// kong's ExitCoder contract for exit code 1 — the load-bearing CI
// gating signal.
func TestDriftError_ExitCode(t *testing.T) {
	d := driftError{summary: "1 missing table"}
	if got := d.ExitCode(); got != 1 {
		t.Errorf("ExitCode() = %d; want 1", got)
	}
	if got := d.Error(); got != "drift detected: 1 missing table" {
		t.Errorf("Error() = %q; want 'drift detected: 1 missing table'", got)
	}
	// Empty summary still produces a meaningful error string.
	d2 := driftError{}
	if got := d2.Error(); got != "drift detected" {
		t.Errorf("empty-summary Error() = %q; want 'drift detected'", got)
	}
}

// TestOperationalError_ExitCode confirms that the operational sentinel
// reports exit code 2 (distinct from drift) and unwraps to the
// underlying error so callers can errors.Is / errors.As against it.
func TestOperationalError_ExitCode(t *testing.T) {
	inner := errors.New("connection refused")
	o := operationalError{err: inner}
	if got := o.ExitCode(); got != 2 {
		t.Errorf("ExitCode() = %d; want 2", got)
	}
	if got := o.Error(); got != "connection refused" {
		t.Errorf("Error() = %q; want 'connection refused'", got)
	}
	if !errors.Is(o, inner) {
		t.Errorf("errors.Is(operationalError, inner) = false; want true")
	}
}

// TestSchemaDiff_SummaryMatchesCLI exercises the ir.SchemaDiff.Summary
// path the CLI's drift sentinel embeds. A regression on the IR-side
// rendering would bubble up here before users see it.
func TestSchemaDiff_SummaryMatchesCLI(t *testing.T) {
	d := ir.SchemaDiff{
		TablesMissing: []string{"orders"},
		TablesMismatched: []ir.TableDiff{
			{Name: "users", ColumnsMissing: []string{"created_at"}},
		},
	}
	want := "1 missing table, 1 missing column"
	if got := d.Summary(); got != want {
		t.Errorf("Summary() = %q; want %q", got, want)
	}
}
