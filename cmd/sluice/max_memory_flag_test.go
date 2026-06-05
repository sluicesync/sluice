// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"strings"
	"testing"
)

// TestParseMaxMemory pins the --max-memory flag parse: "" / "off" → 0
// (the OFF sentinel — SetMemoryLimit is not called, GOMEMLIMIT is left
// to Go), human sizes ("2GiB", "512MiB", "2GB") and raw byte counts →
// bytes (power-of-two units via units.RAMInBytes), and unparseable /
// zero / negative input → a loud error. The 0 sentinel and the size
// path are the load-bearing contract main()'s applyMaxMemory depends on.
func TestParseMaxMemory(t *testing.T) {
	const (
		mib = int64(1024 * 1024)
		gib = 1024 * mib
	)
	cases := []struct {
		name    string
		raw     string
		want    int64
		wantErr string // expected error substring; empty means accept
	}{
		{name: "empty is off", raw: "", want: 0},
		{name: "whitespace is off", raw: "  ", want: 0},
		{name: "off sentinel", raw: "off", want: 0},
		{name: "off case-insensitive", raw: "OFF", want: 0},
		{name: "2GiB power-of-two", raw: "2GiB", want: 2 * gib},
		{name: "512MiB power-of-two", raw: "512MiB", want: 512 * mib},
		{name: "2GB alias", raw: "2GB", want: 2 * gib},
		{name: "lowercase unit", raw: "256mib", want: 256 * mib},
		{name: "raw byte count", raw: "2147483648", want: 2 * gib},
		{name: "unparseable", raw: "lots", wantErr: "max-memory"},
		{name: "zero is rejected", raw: "0", wantErr: "max-memory"},
		{name: "negative", raw: "-1", wantErr: "max-memory"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseMaxMemory(tc.raw)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("parseMaxMemory(%q) = (%d, nil), want error containing %q", tc.raw, got, tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("parseMaxMemory(%q) error = %q, want substring %q", tc.raw, err.Error(), tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseMaxMemory(%q) unexpected error: %v", tc.raw, err)
			}
			if got != tc.want {
				t.Errorf("parseMaxMemory(%q) = %d bytes, want %d", tc.raw, got, tc.want)
			}
		})
	}
}
