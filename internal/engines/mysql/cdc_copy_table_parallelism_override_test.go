// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import "testing"

// ADR-0118 finding 4 precedence pins (task 2.5 per-instance form): the cold-copy
// READ-axis resolvers honor explicit CLI flag > DSN param > engine default for
// BOTH axes (VStream: 1 = serial, deliberate; native MySQL: auto 4 since the
// perf-parity gap-3 chunk). These pin that the CLI override WINS over a DSN
// value, that an unset (0) override falls back to the DSN, and that absent both
// resolves to the engine default — and that the override is zero-value-safe (a 0
// cliOverride never overrides a DSN value).
//
// The override is now the per-instance engineOptions.{vstream,}CopyTableParallelism
// (set via the [Engine.With*] builders, task 2.5), passed to the resolver as an
// explicit argument — no process-global state, so no reset/cleanup dance.

func TestADR0118_VStreamCopyTableParallelism_Precedence(t *testing.T) {
	cases := []struct {
		name    string
		dsn     string
		cliFlag int
		want    int
	}{
		{name: "CLI wins over DSN", dsn: "u:p@tcp(h:3306)/db?vstream_copy_table_parallelism=2", cliFlag: 7, want: 7},
		{name: "absent CLI falls back to DSN", dsn: "u:p@tcp(h:3306)/db?vstream_copy_table_parallelism=3", cliFlag: 0, want: 3},
		{name: "absent both → serial default", dsn: "u:p@tcp(h:3306)/db", cliFlag: 0, want: 1},
		// Zero-value-safety: a 0 override must NOT override the DSN value.
		{name: "override 0 does not override DSN", dsn: "u:p@tcp(h:3306)/db?vstream_copy_table_parallelism=5", cliFlag: 0, want: 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Drive the override through the public builder so the CLI's exact
			// path (WithVStreamCopyTableParallelism, write-only on >0) is pinned.
			e := Engine{Flavor: FlavorPlanetScale}.WithVStreamCopyTableParallelism(tc.cliFlag)
			cfg, err := parseDSN(tc.dsn)
			if err != nil {
				t.Fatalf("parseDSN: %v", err)
			}
			got, err := vstreamCopyTableParallelismFromDSN(cfg, e.opts.vstreamCopyTableParallelism)
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			if got != tc.want {
				t.Errorf("got %d; want %d (CLI=%d, DSN=%q)", got, tc.want, tc.cliFlag, tc.dsn)
			}
		})
	}
}

func TestADR0118_NativeCopyTableParallelism_Precedence(t *testing.T) {
	cases := []struct {
		name    string
		dsn     string
		cliFlag int
		want    int
	}{
		{name: "CLI wins over DSN", dsn: "u:p@tcp(h:3306)/db?copy_table_parallelism=2", cliFlag: 6, want: 6},
		{name: "absent CLI falls back to DSN", dsn: "u:p@tcp(h:3306)/db?copy_table_parallelism=4", cliFlag: 0, want: 4},
		{name: "absent both → auto default (gap 3)", dsn: "u:p@tcp(h:3306)/db", cliFlag: 0, want: defaultNativeCopyTableParallelism},
		{name: "explicit CLI 1 is the serial opt-out", dsn: "u:p@tcp(h:3306)/db", cliFlag: 1, want: 1},
		{name: "override 0 does not override DSN", dsn: "u:p@tcp(h:3306)/db?copy_table_parallelism=5", cliFlag: 0, want: 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := Engine{}.WithCopyTableParallelism(tc.cliFlag)
			cfg, err := parseDSN(tc.dsn)
			if err != nil {
				t.Fatalf("parseDSN: %v", err)
			}
			got, err := nativeCopyTableParallelismFromDSN(cfg, e.opts.copyTableParallelism)
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			if got != tc.want {
				t.Errorf("got %d; want %d (CLI=%d, DSN=%q)", got, tc.want, tc.cliFlag, tc.dsn)
			}
		})
	}
}

// TestADR0118_Override_AxesIndependent pins that the two axes are independent:
// the VStream override lives in a different engineOptions field than the native
// one, so setting one never affects the other resolver (a self-managed MySQL
// source has no VStream knob, so they must not cross-contaminate).
func TestADR0118_Override_AxesIndependent(t *testing.T) {
	e := Engine{Flavor: FlavorPlanetScale}.WithVStreamCopyTableParallelism(9)
	if e.opts.copyTableParallelism != 0 {
		t.Fatalf("setting the VStream override touched the native field: %d; want 0", e.opts.copyTableParallelism)
	}
	cfg, err := parseDSN("u:p@tcp(h:3306)/db")
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	n, err := nativeCopyTableParallelismFromDSN(cfg, e.opts.copyTableParallelism)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if n != defaultNativeCopyTableParallelism {
		t.Errorf("native resolver = %d after setting ONLY the VStream override; want the native default %d (axes must be independent)", n, defaultNativeCopyTableParallelism)
	}
}
