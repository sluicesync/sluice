// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"reflect"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/pipeline"
)

// TestSchemaAddTableHelpStatesTheLivenessDoor holds `schema add-table`'s help
// to the drained-add liveness door it describes: the marker it names, the rule
// (a recorded stop, nothing less), and the crashed stream's remedy (start it
// once, then `sync stop --wait`).
func TestSchemaAddTableHelpStatesTheLivenessDoor(t *testing.T) {
	f, ok := reflect.TypeOf(SchemaCmd{}).FieldByName("AddTable")
	if !ok {
		t.Fatal("SchemaCmd has no AddTable field")
	}
	help := f.Tag.Get("help")
	for _, want := range []string{
		pipeline.DrainedAddTableStreamNotStoppedMarker,
		"records that it stopped",
		"start it once and stop it with 'sync stop --wait'",
		"position moved while it copied",
	} {
		if !strings.Contains(help, want) {
			t.Errorf("the add-table help does not state %q: %q", want, help)
		}
	}
}
