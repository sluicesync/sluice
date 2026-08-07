// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/config"
)

// TestPreflightBinaryTypeOverrideOnCDC_RefusesEveryBinaryAlias walks the
// `targetTypeRegistry` aliases that resolve to a binary IR type and
// requires each to be refused.
//
// It is written as an ALIAS matrix rather than one representative,
// because the registry is what an operator types and a new binary alias
// added there is exactly how this refusal would silently stop covering
// its own class (`bytea` was the obvious one; `binary_uuid` resolves to
// ir.Binary and is just as ambiguous, and nothing about the name says
// so).
func TestPreflightBinaryTypeOverrideOnCDC_RefusesEveryBinaryAlias(t *testing.T) {
	binaryAliases := []string{"bytea", "binary_uuid"}
	for _, alias := range binaryAliases {
		alias := alias
		t.Run(alias, func(t *testing.T) {
			err := preflightBinaryTypeOverrideOnCDC([]config.Mapping{
				{Table: "docs", Column: "body", TargetType: alias},
			})
			if err == nil {
				t.Fatalf("--type-override docs.body=%s was accepted on a sync run; "+
					"the CDC lane cannot disambiguate a string value on a binary column", alias)
			}
			if !errors.Is(err, errBinaryOverrideOnCDC) {
				t.Errorf("err = %v; want the errBinaryOverrideOnCDC sentinel", err)
			}
			if !strings.Contains(err.Error(), "docs.body="+alias) {
				t.Errorf("err = %v; want it to name the offending override", err)
			}
		})
	}
}

// TestPreflightBinaryTypeOverrideOnCDC_AllowsNonBinaryAliases is the
// anti-over-refusal half. Every non-binary alias must pass, or the
// refusal has quietly become "no type overrides on sync at all".
func TestPreflightBinaryTypeOverrideOnCDC_AllowsNonBinaryAliases(t *testing.T) {
	nonBinary := []string{
		"text", "mediumtext", "text_array", "jsonb", "json",
		"timestamptz", "datetime", "smallint", "integer", "int", "interval",
		"varchar", "postgis_point",
	}
	for _, alias := range nonBinary {
		alias := alias
		t.Run(alias, func(t *testing.T) {
			if err := preflightBinaryTypeOverrideOnCDC([]config.Mapping{
				{Table: "t", Column: "c", TargetType: alias},
			}); err != nil {
				t.Errorf("--type-override t.c=%s was refused on a sync run: %v", alias, err)
			}
		})
	}
}

// TestPreflightBinaryTypeOverrideOnCDC_NoOpCases pins the two shapes that
// must never refuse: no mappings at all (the overwhelmingly common run),
// and an unresolvable alias — whose diagnostic belongs to ApplyMappings,
// which names the recognised set.
func TestPreflightBinaryTypeOverrideOnCDC_NoOpCases(t *testing.T) {
	if err := preflightBinaryTypeOverrideOnCDC(nil); err != nil {
		t.Errorf("empty mappings refused: %v", err)
	}
	if err := preflightBinaryTypeOverrideOnCDC([]config.Mapping{
		{Table: "t", Column: "c", TargetType: "not_a_real_alias"},
	}); err != nil {
		t.Errorf("unknown alias refused HERE; ApplyMappings owns that error: %v", err)
	}
}

// TestPreflightBinaryTypeOverrideOnCDC_NamesEveryOffender pins that a
// multi-override config reports ALL offenders, not the first. An
// operator who fixes one and re-runs to find another is being made to
// pay per round-trip for a check that already knew the answer.
func TestPreflightBinaryTypeOverrideOnCDC_NamesEveryOffender(t *testing.T) {
	err := preflightBinaryTypeOverrideOnCDC([]config.Mapping{
		{Table: "a", Column: "x", TargetType: "bytea"},
		{Table: "b", Column: "y", TargetType: "text"},
		{Table: "c", Column: "z", TargetType: "binary_uuid"},
	})
	if err == nil {
		t.Fatal("no refusal for two binary overrides")
	}
	for _, want := range []string{"a.x=bytea", "c.z=binary_uuid"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("err = %v; want it to name %s", err, want)
		}
	}
	if strings.Contains(err.Error(), "b.y") {
		t.Errorf("err = %v; b.y=text is not an offender and must not be named", err)
	}
}
