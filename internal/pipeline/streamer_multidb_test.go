// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"strings"
	"testing"
)

// TestStreamer_MultiDatabaseMode pins the back-compat discriminator: the
// fan-out path engages ONLY when a database-scope flag is set, and the
// zero value is single-database mode (byte-identical to pre-ADR-0074).
func TestStreamer_MultiDatabaseMode(t *testing.T) {
	cases := []struct {
		name string
		s    *Streamer
		want bool
	}{
		{"default zero value -> single-database", &Streamer{}, false},
		{"all-databases -> multi", &Streamer{AllDatabases: true}, true},
		{"include filter -> multi", &Streamer{DatabaseFilter: DatabaseFilter{Include: []string{"app_*"}}}, true},
		{"exclude filter -> multi", &Streamer{DatabaseFilter: DatabaseFilter{Exclude: []string{"tmp_*"}}}, true},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := c.s.multiDatabaseMode(); got != c.want {
				t.Errorf("multiDatabaseMode() = %v; want %v", got, c.want)
			}
		})
	}
}

// TestStreamer_ValidateMultiDatabaseStream pins the multi-database
// flag-combo refusals (loud-failure tenet — fail before any I/O).
func TestStreamer_ValidateMultiDatabaseStream(t *testing.T) {
	cases := []struct {
		name    string
		s       *Streamer
		wantErr string // substring; "" means expect nil
	}{
		{
			name: "valid include filter",
			s:    &Streamer{DatabaseFilter: DatabaseFilter{Include: []string{"app_db"}}},
		},
		{
			name:    "all-databases + include is mutually exclusive",
			s:       &Streamer{AllDatabases: true, DatabaseFilter: DatabaseFilter{Include: []string{"app_db"}}},
			wantErr: "mutually exclusive",
		},
		{
			name:    "target-schema incompatible",
			s:       &Streamer{AllDatabases: true, TargetSchema: "analytics"},
			wantErr: "--target-schema is incompatible",
		},
		{
			name:    "inject-shard-column unsupported",
			s:       &Streamer{AllDatabases: true, InjectShardColumn: ShardColumnSpec{Name: "shard", Value: "a"}},
			wantErr: "--inject-shard-column is not supported",
		},
		{
			name:    "schema-already-applied unsupported",
			s:       &Streamer{AllDatabases: true, SchemaAlreadyApplied: true},
			wantErr: "--schema-already-applied is not supported",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			err := c.s.validateMultiDatabaseStream()
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("validateMultiDatabaseStream() = %v; want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("validateMultiDatabaseStream() = %v; want error containing %q", err, c.wantErr)
			}
		})
	}
}
