// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"
)

// TestSplitQualifiedSequence pins the qualified-name parser used by
// the cutover sequence primer (F10 / ADR-0062). pg_get_serial_sequence
// returns names in `schema.name` form, sometimes with one or both
// components double-quoted; pg_sequences expects the unquoted form
// in its `schemaname` / `sequencename` columns.
func TestSplitQualifiedSequence(t *testing.T) {
	cases := []struct {
		name        string
		input       string
		wantSchema  string
		wantName    string
		wantErr     bool
		errContains string
	}{
		{
			name:       "bare lowercase",
			input:      "public.users_id_seq",
			wantSchema: "public",
			wantName:   "users_id_seq",
		},
		{
			name:       "quoted schema and name",
			input:      `"public"."users_id_seq"`,
			wantSchema: "public",
			wantName:   "users_id_seq",
		},
		{
			name:       "quoted with mixed case",
			input:      `"Public"."Widgets_id_seq"`,
			wantSchema: "Public",
			wantName:   "Widgets_id_seq",
		},
		{
			name:       "quoted name with dot inside",
			input:      `"weird.schema"."seq.name"`,
			wantSchema: "weird.schema",
			wantName:   "seq.name",
		},
		{
			name:       "schema-qualified with one side quoted",
			input:      `public."Widgets_id_seq"`,
			wantSchema: "public",
			wantName:   "Widgets_id_seq",
		},
		{
			name:        "no qualifier",
			input:       "bare_seq",
			wantErr:     true,
			errContains: "not qualified",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotSchema, gotName, err := splitQualifiedSequence(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("err = nil; want error containing %q", tc.errContains)
				}
				if tc.errContains != "" && !strings.Contains(err.Error(), tc.errContains) {
					t.Errorf("err = %q; want substring %q", err.Error(), tc.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("err = %v; want nil", err)
			}
			if gotSchema != tc.wantSchema {
				t.Errorf("schema = %q; want %q", gotSchema, tc.wantSchema)
			}
			if gotName != tc.wantName {
				t.Errorf("name = %q; want %q", gotName, tc.wantName)
			}
		})
	}
}

// TestUnquoteIdent pins the identifier-unquoting helper, including
// PG's interior `""` escape sequence.
func TestUnquoteIdent(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"foo", "foo"},
		{`"foo"`, "foo"},
		{`"Foo"`, "Foo"},
		{`"foo""bar"`, `foo"bar`},
		{`""`, ""},
		{`"weird.id"`, "weird.id"},
	}
	for _, tc := range cases {
		got := unquoteIdent(tc.in)
		if got != tc.want {
			t.Errorf("unquoteIdent(%q) = %q; want %q", tc.in, got, tc.want)
		}
	}
}
