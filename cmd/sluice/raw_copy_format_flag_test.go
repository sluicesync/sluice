// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"reflect"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestParseRawCopyFormat_CLIMapping pins the --raw-copy-format flag →
// IR request mapping THROUGH the CLI parser (the Bug-180 lesson: a fix
// gated on a CLI value must be pinned at the layer that produces it).
// This is the ground truth behind the Bug 194 "auto chose text"
// investigation: 'auto' REQUESTS binary (the orchestrator's version
// probe then decides), so a run that took the text lane under 'auto'
// with matching server majors is not reproducible from this mapping —
// the observed text lane came from the flag DEFAULT ('text'), not from
// a broken auto preference.
func TestParseRawCopyFormat_CLIMapping(t *testing.T) {
	cases := []struct {
		raw  string
		want ir.RawCopyFormat
	}{
		{"text", ir.RawCopyText},
		{"binary", ir.RawCopyBinary},
		{"auto", ir.RawCopyBinary}, // auto = request binary, let the probe decide
		{"AUTO", ir.RawCopyBinary},
		{"", ir.RawCopyText}, // zero value stays the always-safe floor
	}
	for _, c := range cases {
		if got := parseRawCopyFormat(c.raw); got != c.want {
			t.Errorf("parseRawCopyFormat(%q) = %v; want %v", c.raw, got, c.want)
		}
	}
}

// TestRawCopyFormatDefaults documents (and pins) that BOTH raw-copy
// surfaces default to 'text' — the cross-major-safe floor. Recorded as
// part of the Bug 194 closure: the text lane is float-exact by the
// extra_float_digits export pin, so the default no longer depends on
// the source server's extra_float_digits posture. If this default is
// ever flipped to 'auto' (binary-when-negotiable), delete this pin
// deliberately, with the negotiation-failure story re-reviewed.
func TestRawCopyFormatDefaults(t *testing.T) {
	for _, c := range []struct {
		name string
		typ  reflect.Type
	}{
		{"migrate", reflect.TypeOf(MigrateCmd{})},
		{"sync start", reflect.TypeOf(SyncStartCmd{})},
	} {
		field, ok := c.typ.FieldByName("RawCopyFormat")
		if !ok {
			t.Fatalf("%s: no RawCopyFormat field", c.name)
		}
		if got := field.Tag.Get("default"); got != "text" {
			t.Errorf("%s --raw-copy-format default = %q; want %q", c.name, got, "text")
		}
	}
}
