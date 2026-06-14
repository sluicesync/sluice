// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"reflect"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestApplyBatchSizeDefaultIsAuto pins the ADR-0089 decision: the
// sync-start --apply-batch-size default must be "auto", not "1". A
// default of "1" makes the ADR-0052 AIMD controller's cap equal its
// floor, leaving the controller dormant and the slow single-row apply
// as the out-of-box behaviour (the PlanetScale soak measured >10x
// throughput loss). This reflection guard fails loudly if the default
// is ever flipped back.
func TestApplyBatchSizeDefaultIsAuto(t *testing.T) {
	field, ok := reflect.TypeOf(SyncStartCmd{}).FieldByName("ApplyBatchSize")
	if !ok {
		t.Fatal("SyncStartCmd has no ApplyBatchSize field")
	}
	if got := field.Tag.Get("default"); got != "auto" {
		t.Errorf("--apply-batch-size default = %q; want %q (ADR-0089)", got, "auto")
	}
}

// TestResolveApplyBatchSize_AutoAndStatic pins the resolver: "auto"
// maps to the engine-default ceiling (ADR-0052/0089), and the explicit
// static forms preserve the conservative single-row escape hatch.
func TestResolveApplyBatchSize_AutoAndStatic(t *testing.T) {
	pg, err := resolveEngine("postgres")
	if err != nil {
		t.Fatalf("resolveEngine(postgres): %v", err)
	}
	ps, err := resolveEngine("planetscale")
	if err != nil {
		t.Fatalf("resolveEngine(planetscale): %v", err)
	}

	cases := []struct {
		name   string
		raw    string
		engine ir.Engine
		want   int
	}{
		{"auto on postgres → 1000 ceiling", "auto", pg, 1000},
		{"auto on planetscale → 100 ceiling", "auto", ps, 100},
		{"auto is case-insensitive", "AUTO", pg, 1000},
		{"explicit 1 stays 1 (conservative escape)", "1", pg, 1},
		{"explicit N passes through", "250", pg, 250},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := resolveApplyBatchSize(c.raw, c.engine)
			if err != nil {
				t.Fatalf("resolveApplyBatchSize(%q): %v", c.raw, err)
			}
			if got != c.want {
				t.Errorf("resolveApplyBatchSize(%q) = %d; want %d", c.raw, got, c.want)
			}
		})
	}
}
