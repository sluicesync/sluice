// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// TestExtractPGLSN_BareLSN pins the source-current-position case:
// SourceCurrentPosition emits a bare LSN string via
// pg_current_wal_lsn()::text. The extractor passes it through
// unchanged.
func TestExtractPGLSN_BareLSN(t *testing.T) {
	got, err := extractPGLSN(ir.Position{Engine: "postgres", Token: "0/1A2B3C4D"})
	if err != nil {
		t.Fatalf("extractPGLSN: %v", err)
	}
	if got != "0/1A2B3C4D" {
		t.Errorf("got %q; want 0/1A2B3C4D", got)
	}
}

// TestExtractPGLSN_JSONEnvelope pins the v0.15.1 / Bug 32 fix: the
// persisted-state Position from sluice_cdc_state is a JSON envelope
// {"slot":"...","lsn":"X/Y"}, NOT a bare LSN. Pre-fix, sync-health's
// LagBytes passed the JSON verbatim into pg_wal_lsn_diff() which
// errored with SQLSTATE 22P02. Post-fix, the extractor pulls the
// "lsn" field.
func TestExtractPGLSN_JSONEnvelope(t *testing.T) {
	got, err := extractPGLSN(ir.Position{
		Engine: "postgres",
		Token:  `{"slot":"sluice_slot","lsn":"0/1A2B3C4D"}`,
	})
	if err != nil {
		t.Fatalf("extractPGLSN: %v", err)
	}
	if got != "0/1A2B3C4D" {
		t.Errorf("got %q; want 0/1A2B3C4D (extracted from JSON envelope)", got)
	}
}

// TestExtractPGLSN_JSONWithLeadingWhitespace covers the defensive
// trim — the envelope detector tolerates leading whitespace before
// the opening `{`.
func TestExtractPGLSN_JSONWithLeadingWhitespace(t *testing.T) {
	got, err := extractPGLSN(ir.Position{
		Engine: "postgres",
		Token:  "  \n\t" + `{"slot":"sluice_slot","lsn":"0/ABCD"}`,
	})
	if err != nil {
		t.Fatalf("extractPGLSN: %v", err)
	}
	if got != "0/ABCD" {
		t.Errorf("got %q; want 0/ABCD", got)
	}
}

// TestExtractPGLSN_EmptyToken pins the validation: the engine
// surfaces a clear error for empty tokens rather than passing them
// through to PG and getting an opaque pg_lsn parse error.
func TestExtractPGLSN_EmptyToken(t *testing.T) {
	_, err := extractPGLSN(ir.Position{Engine: "postgres", Token: ""})
	if err == nil {
		t.Fatal("expected error on empty token")
	}
	if !strings.Contains(err.Error(), "empty Token") {
		t.Errorf("error should mention empty Token; got %v", err)
	}
}

// TestExtractPGLSN_MalformedJSON pins the JSON-envelope parse-error
// path: a token starting with `{` that isn't valid JSON should
// surface a clear "decode JSON-envelope position" error.
func TestExtractPGLSN_MalformedJSON(t *testing.T) {
	_, err := extractPGLSN(ir.Position{
		Engine: "postgres",
		Token:  `{"slot":"x"`, // missing closing brace
	})
	if err == nil {
		t.Fatal("expected error on malformed JSON envelope")
	}
	if !strings.Contains(err.Error(), "decode JSON-envelope") {
		t.Errorf("error should mention decode JSON-envelope; got %v", err)
	}
}
