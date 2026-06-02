// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"strings"
	"testing"
)

// TestParseIndexBuildMem pins the --index-build-mem flag parse: "auto"
// / "" → 0 (the auto sentinel), human sizes ("512MB", "2GB") and raw
// byte counts → bytes (power-of-two units via units.RAMInBytes), and
// unparseable / negative input → a loud error. The 0 sentinel and the
// size path are the load-bearing contract the PG tuner depends on.
func TestParseIndexBuildMem(t *testing.T) {
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
		{name: "auto sentinel", raw: "auto", want: 0},
		{name: "auto case-insensitive", raw: "AUTO", want: 0},
		{name: "empty is auto", raw: "", want: 0},
		{name: "whitespace is auto", raw: "  ", want: 0},
		{name: "512MB power-of-two", raw: "512MB", want: 512 * mib},
		{name: "2GB power-of-two", raw: "2GB", want: 2 * gib},
		{name: "lowercase unit", raw: "256mb", want: 256 * mib},
		{name: "raw byte count", raw: "67108864", want: 64 * mib},
		{name: "unparseable", raw: "lots", wantErr: "index-build-mem"},
		{name: "negative", raw: "-1", wantErr: "index-build-mem"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseIndexBuildMem(tc.raw)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("parseIndexBuildMem(%q) = (%d, nil), want error containing %q", tc.raw, got, tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("parseIndexBuildMem(%q) error = %q, want substring %q", tc.raw, err.Error(), tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseIndexBuildMem(%q) unexpected error: %v", tc.raw, err)
			}
			if got != tc.want {
				t.Errorf("parseIndexBuildMem(%q) = %d bytes, want %d", tc.raw, got, tc.want)
			}
		})
	}
}
