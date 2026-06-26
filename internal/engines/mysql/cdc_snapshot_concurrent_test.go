// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// --- ADR-0101 native-MySQL concurrent cold-copy: unit pins ---

// TestNativeCopyTableParallelismFromDSN pins the knob parse: absent → 1
// (serial), a valid value passes through, a malformed value is a LOUD error
// (the loud-failure tenet — an operator who set the knob deserves to know it
// didn't parse), NOT a silent fallback to serial.
func TestNativeCopyTableParallelismFromDSN(t *testing.T) {
	t.Run("absent defaults to 1 (serial)", func(t *testing.T) {
		cfg, err := parseDSN("u:p@tcp(h:3306)/db")
		if err != nil {
			t.Fatalf("parseDSN: %v", err)
		}
		n, err := nativeCopyTableParallelismFromDSN(cfg)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if n != 1 {
			t.Fatalf("absent param = %d; want 1 (the zero-value-safe serial default)", n)
		}
	})
	t.Run("valid value passes through", func(t *testing.T) {
		cfg, err := parseDSN("u:p@tcp(h:3306)/db?copy_table_parallelism=4")
		if err != nil {
			t.Fatalf("parseDSN: %v", err)
		}
		n, err := nativeCopyTableParallelismFromDSN(cfg)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if n != 4 {
			t.Fatalf("copy_table_parallelism=4 → %d; want 4", n)
		}
	})
	t.Run("malformed is a loud error", func(t *testing.T) {
		cfg, err := parseDSN("u:p@tcp(h:3306)/db?copy_table_parallelism=lots")
		if err != nil {
			t.Fatalf("parseDSN: %v", err)
		}
		if _, err := nativeCopyTableParallelismFromDSN(cfg); err == nil {
			t.Fatal("malformed copy_table_parallelism parsed without error; want a loud parse error")
		}
	})
}

// TestStripVStreamParams_StripsNativeKnob pins that copy_table_parallelism is
// stripped before a MySQL session (Bug 126 class): it is a sluice-internal
// snapshot-opener knob, not a MySQL system variable, so openDB must not emit
// SET copy_table_parallelism=… (MySQL would reject the unknown variable).
func TestStripVStreamParams_StripsNativeKnob(t *testing.T) {
	cfg, err := parseDSN("u:p@tcp(h:3306)/db?copy_table_parallelism=4")
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	stripped := stripVStreamParams(cfg)
	if _, ok := stripped.Params["copy_table_parallelism"]; ok {
		t.Error("stripped.Params still contains copy_table_parallelism; openDB would emit SET copy_table_parallelism and MySQL would reject it (Bug 126 class)")
	}
	// Non-mutation: the caller's cfg keeps the param (the snapshot opener
	// reads it out of its own cfg before openDB runs).
	if _, ok := cfg.Params["copy_table_parallelism"]; !ok {
		t.Error("stripVStreamParams mutated the caller's cfg (lost copy_table_parallelism); it must Clone()")
	}
}

// TestConcurrentBinlogRows_DispatchByGroup pins the multi-snapshot router's
// table→connection dispatch (the only new reader code, ADR-0101 §6): each
// table routes to the inner reader owning its group, and a table not in any
// group is refused LOUDLY (never silently read from a wrong/zero connection).
//
// We build the router directly with nil connections — the dispatch happens
// before any query, so we exercise the routing (the byTable map) without a
// DB. A table present in a group routes to a non-nil inner reader (and would
// query its connection); a table absent from all groups is the loud-refuse
// path, which returns before touching any connection.
func TestConcurrentBinlogRows_DispatchByGroup(t *testing.T) {
	groups := [][]string{{"a", "c"}, {"b"}}
	// Build the router with nil conns: the constructor only reads len(conns)
	// and indexes the groups, and the paths we assert (absent/nil table)
	// return BEFORE dereferencing any connection. The present-table query
	// path is covered by the integration test against a real DB.
	rr := newConcurrentBinlogRows(nil, groups, "db", nil, zeroDateInherit)

	if got := rr.ConcurrentCopyGroups(); len(got) != 2 {
		t.Fatalf("ConcurrentCopyGroups len = %d; want 2", len(got))
	}

	// A table NOT in any group → loud refuse (no reader owns it). This is the
	// silent-loss guard: never silently read from a wrong/zero connection.
	_, err := rr.ReadRows(context.Background(), &ir.Table{
		Name:    "ghost",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	})
	if err == nil {
		t.Fatal("ReadRows for a table absent from the partition returned nil error; want a loud partition/scope-mismatch refusal (silent-loss guard)")
	}

	// A nil table is refused too.
	if _, err := rr.ReadRows(context.Background(), nil); err == nil {
		t.Fatal("ReadRows(nil table) returned nil error; want a loud refusal")
	}
}
