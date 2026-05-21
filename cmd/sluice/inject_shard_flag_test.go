// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"strings"
	"testing"
)

func TestParseInjectShardColumn_EmptyDisengaged(t *testing.T) {
	spec, err := parseInjectShardColumn("")
	if err != nil {
		t.Fatalf("expected nil for empty raw; got %v", err)
	}
	if spec.Engaged() {
		t.Errorf("empty raw should not engage; got %+v", spec)
	}
}

func TestParseInjectShardColumn_Engaged(t *testing.T) {
	spec, err := parseInjectShardColumn("source_shard_id=us-east-1")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !spec.Engaged() {
		t.Fatalf("expected engaged; got %+v", spec)
	}
	if spec.Name != "source_shard_id" {
		t.Errorf("Name = %q; want source_shard_id", spec.Name)
	}
	if spec.Value != "us-east-1" {
		t.Errorf("Value = %v; want us-east-1", spec.Value)
	}
}

func TestParseInjectShardColumn_TrimsWhitespace(t *testing.T) {
	spec, err := parseInjectShardColumn("  shard = value  ")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if spec.Name != "shard" || spec.Value != "value" {
		t.Errorf("got %+v; want {shard,value}", spec)
	}
}

func TestParseInjectShardColumn_Refusals(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		wantSub string
	}{
		{"missing equals", "shard", "missing '='"},
		{"empty name", "=value", "NAME is empty"},
		{"empty value", "shard=", "VALUE is empty"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseInjectShardColumn(tc.raw)
			if err == nil {
				t.Fatalf("expected refusal for %q; got nil", tc.raw)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error %q missing %q", err.Error(), tc.wantSub)
			}
		})
	}
}

func TestParseInjectShardColumn_AcceptsEqualsInValue(t *testing.T) {
	// Values may contain '=' (e.g. URL-shaped), so only the first
	// '=' is the separator.
	spec, err := parseInjectShardColumn("shard=a=b=c")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if spec.Value != "a=b=c" {
		t.Errorf("Value = %v; want a=b=c", spec.Value)
	}
}
