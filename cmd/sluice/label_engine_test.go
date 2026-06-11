// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
)

// TestLabelEngineLabelsConnectionLabeler pins labelEngine's dispatch on
// the [ir.ConnectionLabeler] surface: an engine that implements it
// (postgres) is swapped for a labeled copy, and the copy keeps the same
// engine identity for the rest of the run.
func TestLabelEngineLabelsConnectionLabeler(t *testing.T) {
	e, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	if _, ok := e.(ir.ConnectionLabeler); !ok {
		t.Fatal("postgres engine should implement ir.ConnectionLabeler")
	}
	got := labelEngine(e, "mystream")
	if got == e {
		t.Error("labelEngine returned the registry's engine value unchanged; want a labeled copy")
	}
	if got.Name() != "postgres" {
		t.Errorf("labeled engine Name() = %q, want %q", got.Name(), "postgres")
	}
}

// TestLabelEnginePassthrough pins the passthrough arm: an engine
// without a per-connection label concept (mysql) flows through
// labelEngine untouched.
func TestLabelEnginePassthrough(t *testing.T) {
	e, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	if _, ok := e.(ir.ConnectionLabeler); ok {
		t.Fatal("mysql engine unexpectedly implements ir.ConnectionLabeler; this test needs a non-labeling engine")
	}
	if got := labelEngine(e, "mystream"); got != e {
		t.Errorf("labelEngine should pass a non-labeling engine through unchanged; got %T", got)
	}
}
