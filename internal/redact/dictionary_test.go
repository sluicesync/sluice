// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package redact

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/config"
)

// TestLoadDictionaries_NilEmpty pins the no-op default: nil / empty
// declarations produce nil result + no error.
func TestLoadDictionaries_NilEmpty(t *testing.T) {
	got, err := LoadDictionaries(nil)
	if err != nil {
		t.Fatalf("nil: unexpected error %v", err)
	}
	if got != nil {
		t.Errorf("nil: want nil map; got %+v", got)
	}
	got, err = LoadDictionaries(map[string]config.Dictionary{})
	if err != nil {
		t.Fatalf("empty: unexpected error %v", err)
	}
	if got != nil {
		t.Errorf("empty: want nil map; got %+v", got)
	}
}

// TestLoadDictionaries_Inline covers the inline-entries form: small
// dicts declared directly in YAML.
func TestLoadDictionaries_Inline(t *testing.T) {
	decls := map[string]config.Dictionary{
		"first_names": {Entries: []string{"Alice", "Bob", "Carol"}},
		"cities":      {Entries: []string{"Boston", "Denver"}},
	}
	got, err := LoadDictionaries(decls)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d dicts; want 2", len(got))
	}
	if got["first_names"][0] != "Alice" {
		t.Errorf("first_names[0] = %q; want Alice", got["first_names"][0])
	}
	if len(got["cities"]) != 2 {
		t.Errorf("cities len = %d; want 2", len(got["cities"]))
	}
}

// TestLoadDictionaries_InlineTrimsAndDropsEmpty pins the whitespace
// trim + empty-entry drop behaviour for inline form.
func TestLoadDictionaries_InlineTrimsAndDropsEmpty(t *testing.T) {
	decls := map[string]config.Dictionary{
		"first_names": {Entries: []string{"  Alice  ", "Bob", "", "   ", "Carol"}},
	}
	got, err := LoadDictionaries(decls)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	want := []string{"Alice", "Bob", "Carol"}
	if len(got["first_names"]) != len(want) {
		t.Fatalf("got %d entries; want %d (entries: %v)", len(got["first_names"]), len(want), got["first_names"])
	}
	for i, w := range want {
		if got["first_names"][i] != w {
			t.Errorf("first_names[%d] = %q; want %q", i, got["first_names"][i], w)
		}
	}
}

// TestLoadDictionaries_File covers the file-form loader: one entry
// per line, with comment + blank handling.
func TestLoadDictionaries_File(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cities.txt")
	content := "# Test fixture\nBoston\n   Denver  \n\n# Skip me\nMiami\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	decls := map[string]config.Dictionary{
		"cities": {File: path},
	}
	got, err := LoadDictionaries(decls)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	want := []string{"Boston", "Denver", "Miami"}
	if len(got["cities"]) != len(want) {
		t.Fatalf("got %d entries; want %d (entries: %v)", len(got["cities"]), len(want), got["cities"])
	}
	for i, w := range want {
		if got["cities"][i] != w {
			t.Errorf("cities[%d] = %q; want %q", i, got["cities"][i], w)
		}
	}
}

// TestLoadDictionaries_RefusalPaths covers every documented refusal.
func TestLoadDictionaries_RefusalPaths(t *testing.T) {
	dir := t.TempDir()
	emptyPath := filepath.Join(dir, "empty.txt")
	if err := os.WriteFile(emptyPath, []byte(""), 0o600); err != nil {
		t.Fatalf("write empty: %v", err)
	}
	allCommentPath := filepath.Join(dir, "comments.txt")
	if err := os.WriteFile(allCommentPath, []byte("# nothing useful\n# more\n"), 0o600); err != nil {
		t.Fatalf("write comments: %v", err)
	}
	cases := []struct {
		name          string
		decls         map[string]config.Dictionary
		wantSubstring string
	}{
		{
			name:          "empty inline",
			decls:         map[string]config.Dictionary{"x": {Entries: []string{}}},
			wantSubstring: "has 0 entries",
		},
		{
			name:          "all-whitespace inline",
			decls:         map[string]config.Dictionary{"x": {Entries: []string{"  ", "\t"}}},
			wantSubstring: "has 0 entries",
		},
		{
			name:          "both file and entries",
			decls:         map[string]config.Dictionary{"x": {File: "/some/path", Entries: []string{"a"}}},
			wantSubstring: "declares both 'file:' and inline 'entries:'",
		},
		{
			name:          "missing file",
			decls:         map[string]config.Dictionary{"x": {File: filepath.Join(dir, "no-such-file.txt")}},
			wantSubstring: "dictionary \"x\"",
		},
		{
			name:          "empty file",
			decls:         map[string]config.Dictionary{"x": {File: emptyPath}},
			wantSubstring: "has 0 entries",
		},
		{
			name:          "all-comments file",
			decls:         map[string]config.Dictionary{"x": {File: allCommentPath}},
			wantSubstring: "has 0 entries",
		},
		{
			name:          "empty name",
			decls:         map[string]config.Dictionary{"": {Entries: []string{"a"}}},
			wantSubstring: "name is empty",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			_, err := LoadDictionaries(c.decls)
			if err == nil {
				t.Fatal("expected error; got nil")
			}
			if !strings.Contains(err.Error(), c.wantSubstring) {
				t.Errorf("error %q should contain %q", err.Error(), c.wantSubstring)
			}
		})
	}
}

// TestResolveDictEntries covers the lookup helper: hit, miss-with-
// available, miss-with-empty-loaded-map.
func TestResolveDictEntries(t *testing.T) {
	loaded := map[string][]string{
		"first_names": {"Alice", "Bob"},
		"cities":      {"Boston"},
	}
	got, err := ResolveDictEntries(loaded, "first_names")
	if err != nil {
		t.Fatalf("hit: unexpected error %v", err)
	}
	if len(got) != 2 || got[0] != "Alice" {
		t.Errorf("hit: got %v; want [Alice Bob]", got)
	}
	// Defensive copy: mutating the result must not affect the source.
	got[0] = "MUTATED"
	if loaded["first_names"][0] != "Alice" {
		t.Errorf("ResolveDictEntries did not defensive-copy; source got mutated")
	}

	if _, err := ResolveDictEntries(loaded, ""); err == nil {
		t.Error("empty name: expected error")
	}

	_, err = ResolveDictEntries(loaded, "missing")
	if err == nil {
		t.Fatal("missing: expected error")
	}
	if !strings.Contains(err.Error(), "available dictionaries") {
		t.Errorf("missing: error %q should list available dictionaries", err.Error())
	}

	_, err = ResolveDictEntries(nil, "first_names")
	if err == nil {
		t.Fatal("nil-loaded: expected error")
	}
	if !strings.Contains(err.Error(), "no dictionaries are loaded") {
		t.Errorf("nil-loaded: error %q should say no dictionaries loaded", err.Error())
	}
}
