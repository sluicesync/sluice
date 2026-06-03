// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package redact

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/config"
)

// PII Phase 3 — dictionary loading.
//
// Operators declare named dictionaries in YAML's `dictionaries:` block
// (see [config.Config.Dictionaries]). Each dictionary is either an
// inline list of entries or a pointer to a file with one entry per
// line. [LoadDictionaries] resolves both shapes into a flat
// `map[string][]string` (dict name → entries) that callers pass to
// `parseRedactFlags` / `mergeYAMLRedactions` so the per-rule parsers
// can materialise [RandomizeDict] / [TokenizeDict] strategies with
// the resolved entries embedded.
//
// File-form policy:
//
//   - Read with `os.ReadFile`. Refused on read error.
//   - Split on `\n`; each line is trimmed of surrounding whitespace.
//   - Lines starting with `#` (after trimming) are comments — skipped.
//   - Blank lines are skipped.
//
// Empty dictionaries (0 effective entries) are refused at load time —
// a dict that maps every input to an empty string is operator error,
// not a valid config. Refusing loudly here beats producing
// inscrutable mod-by-zero panics deeper in [RandomizeDict] / [TokenizeDict].
//
// Caching: callers should invoke [LoadDictionaries] once per command
// invocation (typically at the top of a Run method) and pass the
// resulting map to every parser that needs it. The result is
// effectively a small in-memory cache — re-resolving on every call
// would be wasted work but harmless.

// LoadDictionaries resolves the operator's YAML `dictionaries:` block
// into a flat name → entries map. Each entry's Entries OR File field
// is exclusive: declaring both is operator error and refused with a
// clear message. Inline empty list / empty file is also refused.
//
// Returns nil (no error) when the input map is empty — operators
// without dictionaries get a zero-cost no-op path.
func LoadDictionaries(decls map[string]config.Dictionary) (map[string][]string, error) {
	if len(decls) == 0 {
		return nil, nil
	}
	// Iterate in deterministic order so the first refusal naming a
	// particular dictionary is reproducible across runs.
	names := make([]string, 0, len(decls))
	for k := range decls {
		names = append(names, k)
	}
	sort.Strings(names)

	out := make(map[string][]string, len(decls))
	for _, name := range names {
		decl := decls[name]
		if name == "" {
			return nil, errors.New("redact: dictionary name is empty (every entry under 'dictionaries:' must have a non-empty key)")
		}
		if decl.File != "" && len(decl.Entries) > 0 {
			return nil, fmt.Errorf("redact: dictionary %q declares both 'file:' and inline 'entries:'; choose one form", name)
		}
		var entries []string
		var err error
		switch {
		case decl.File != "":
			entries, err = readDictionaryFile(decl.File)
			if err != nil {
				return nil, fmt.Errorf("redact: dictionary %q (file %q): %w", name, decl.File, err)
			}
		default:
			entries = trimDictionaryEntries(decl.Entries)
		}
		if len(entries) == 0 {
			return nil, fmt.Errorf("redact: dictionary %q has 0 entries; populate the 'entries:' list or supply a 'file:' with at least one non-empty, non-comment line", name)
		}
		out[name] = entries
	}
	return out, nil
}

// readDictionaryFile loads a file-form dictionary. See package-level
// notes for the line-handling policy (trim, skip blank, skip
// #-prefixed comment lines).
func readDictionaryFile(path string) ([]string, error) {
	data, err := os.ReadFile(path) //nolint:gosec // operator-supplied dictionary path
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(data), "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		t := strings.TrimSpace(line)
		if t == "" {
			continue
		}
		if strings.HasPrefix(t, "#") {
			continue
		}
		out = append(out, t)
	}
	return out, nil
}

// trimDictionaryEntries strips surrounding whitespace from every
// inline entry and drops empties. Mirrors the file-form's trimming
// behaviour so YAML inline + file-loaded dictionaries see identical
// semantics. Comment lines (# prefix) are NOT honoured for inline
// entries — operators using inline YAML can just omit the entry.
func trimDictionaryEntries(in []string) []string {
	out := make([]string, 0, len(in))
	for _, e := range in {
		t := strings.TrimSpace(e)
		if t == "" {
			continue
		}
		out = append(out, t)
	}
	return out
}
