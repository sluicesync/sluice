// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import "testing"

// ADR-0118 finding 4 precedence pins: the cold-copy READ-axis resolvers honor
// explicit CLI flag > DSN param > engine default for BOTH axes (VStream:
// 1 = serial, deliberate; native MySQL: auto 4 since the perf-parity gap-3
// chunk). These pin that the CLI override WINS over a DSN value, that an
// unset (0) override falls back to the DSN, and that absent both resolves to
// the engine default — and that the override is zero-value-safe (Set(0)
// never overrides a DSN value).
//
// The override is package-level process state (the --mysql-sql-mode idiom), so
// each subtest resets it via the setters' "0 = unset" reset path with a direct
// Store, restored in Cleanup, to stay independent of test ordering.

func resetCopyTableParallelismOverrides(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		vstreamCopyTableParallelismOverride.Store(0)
		nativeCopyTableParallelismOverride.Store(0)
	})
	vstreamCopyTableParallelismOverride.Store(0)
	nativeCopyTableParallelismOverride.Store(0)
}

func TestADR0118_VStreamCopyTableParallelism_Precedence(t *testing.T) {
	cases := []struct {
		name    string
		dsn     string
		cliFlag int // 0 = unset (not passed); <0 = call Set(0) explicitly
		want    int
	}{
		{name: "CLI wins over DSN", dsn: "u:p@tcp(h:3306)/db?vstream_copy_table_parallelism=2", cliFlag: 7, want: 7},
		{name: "absent CLI falls back to DSN", dsn: "u:p@tcp(h:3306)/db?vstream_copy_table_parallelism=3", cliFlag: 0, want: 3},
		{name: "absent both → serial default", dsn: "u:p@tcp(h:3306)/db", cliFlag: 0, want: 1},
		// Zero-value-safety: SetVStreamCopyTableParallelismOverride(0) must NOT
		// override the DSN value (0 means "unset").
		{name: "Set(0) does not override DSN", dsn: "u:p@tcp(h:3306)/db?vstream_copy_table_parallelism=5", cliFlag: -1 /* call Set(0) */, want: 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetCopyTableParallelismOverrides(t)
			switch {
			case tc.cliFlag > 0:
				SetVStreamCopyTableParallelismOverride(tc.cliFlag)
			case tc.cliFlag < 0:
				SetVStreamCopyTableParallelismOverride(0) // explicit zero-value call
			}
			cfg, err := parseDSN(tc.dsn)
			if err != nil {
				t.Fatalf("parseDSN: %v", err)
			}
			got, err := vstreamCopyTableParallelismFromDSN(cfg)
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
		{name: "Set(0) does not override DSN", dsn: "u:p@tcp(h:3306)/db?copy_table_parallelism=5", cliFlag: -1, want: 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetCopyTableParallelismOverrides(t)
			switch {
			case tc.cliFlag > 0:
				SetNativeCopyTableParallelismOverride(tc.cliFlag)
			case tc.cliFlag < 0:
				SetNativeCopyTableParallelismOverride(0)
			}
			cfg, err := parseDSN(tc.dsn)
			if err != nil {
				t.Fatalf("parseDSN: %v", err)
			}
			got, err := nativeCopyTableParallelismFromDSN(cfg)
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
// setting the VStream override does not affect the native resolver and vice
// versa (a self-managed MySQL source has no VStream knob, so they must not
// cross-contaminate).
func TestADR0118_Override_AxesIndependent(t *testing.T) {
	resetCopyTableParallelismOverrides(t)
	SetVStreamCopyTableParallelismOverride(9)
	cfg, err := parseDSN("u:p@tcp(h:3306)/db")
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	n, err := nativeCopyTableParallelismFromDSN(cfg)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if n != defaultNativeCopyTableParallelism {
		t.Errorf("native resolver = %d after setting ONLY the VStream override; want the native default %d (axes must be independent)", n, defaultNativeCopyTableParallelism)
	}
}
