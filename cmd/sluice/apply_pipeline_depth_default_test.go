// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"reflect"
	"testing"
)

// TestApplyPipelineDepthDefaultIsSerial pins the ADR-0104 zero-value-safe
// decision (the v0.99.51 trap): the sync-start --apply-pipeline-depth
// default must be "0" (serial), not a value > 1. A default that engaged
// the pipeline for every stream would open W dedicated backends on every
// MySQL target unbidden and change the apply path's behaviour out of the
// box; pipelining must be strictly opt-in. This reflection guard fails
// loudly if the default is ever flipped to an engaging value.
func TestApplyPipelineDepthDefaultIsSerial(t *testing.T) {
	field, ok := reflect.TypeOf(SyncStartCmd{}).FieldByName("ApplyPipelineDepth")
	if !ok {
		t.Fatal("SyncStartCmd has no ApplyPipelineDepth field")
	}
	if got := field.Tag.Get("default"); got != "0" {
		t.Errorf("--apply-pipeline-depth default = %q; want %q (ADR-0104 zero-value-safe: 0/1 = serial)", got, "0")
	}
}
