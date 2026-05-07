// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Add SPDX license headers to .go files under internal/.
//
// Idempotent: skips files that already carry a SPDX-License-Identifier
// line. Handles //go:build build constraints by inserting the license
// AFTER the constraint block (constraints must remain the first thing
// in the file, per Go's build-tag rules).
//
// Run from the repo root:
//
//	go run ./scripts/add-license-headers
//
// Output is a count of files modified vs skipped.
package main

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

const (
	header = "// Copyright 2026 Omar Ramos\n" +
		"// SPDX-License-Identifier: Apache-2.0\n\n"
	sentinel = "SPDX-License-Identifier"
)

func hasHeader(b []byte) bool {
	if len(b) > 600 {
		b = b[:600]
	}
	return bytes.Contains(b, []byte(sentinel))
}

// splitBuildConstraint splits the input into (constraintBlock, rest).
// The constraint block is the leading run of `//go:build` / `// +build`
// lines plus the blank line that terminates them. Returns ("", text)
// when the file has no build constraints.
func splitBuildConstraint(text string) (constraint, rest string) {
	lines := strings.Split(text, "\n")
	cut := 0
	sawConstraint := false
	for i, line := range lines {
		stripped := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(stripped, "//go:build"), strings.HasPrefix(stripped, "// +build"):
			sawConstraint = true
			cut = i + 1
		case sawConstraint && stripped == "":
			cut = i + 1
			return strings.Join(lines[:cut], "\n"), strings.Join(lines[cut:], "\n")
		case sawConstraint:
			// Constraint not followed by blank line — be conservative
			// and don't claim the constraint block; the file probably
			// has malformed constraints. Caller treats as "no constraint."
			return "", text
		case stripped != "":
			// First non-blank, non-constraint line: no constraint block.
			return "", text
		}
	}
	if sawConstraint {
		return strings.Join(lines[:cut], "\n"), strings.Join(lines[cut:], "\n")
	}
	return "", text
}

func addHeader(path string) (bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return false, err
	}
	if hasHeader(data) {
		return false, nil
	}
	constraint, rest := splitBuildConstraint(string(data))
	// strings.Join packs N elements with N-1 separators; when the
	// constraint slice ends with a "" sentinel for the trailing blank
	// line, Join still produces only one trailing newline. Pad with
	// an extra "\n" so the constraint block ends with a blank line
	// before the license — gofumpt rejects the no-blank-line shape.
	if constraint != "" && !strings.HasSuffix(constraint, "\n\n") {
		constraint += "\n"
	}
	out := constraint + header + rest
	if err := os.WriteFile(path, []byte(out), 0o644); err != nil {
		return false, err
	}
	return true, nil
}

func main() {
	root := "internal"
	if _, err := os.Stat(root); err != nil {
		fmt.Fprintf(os.Stderr, "error: %s not found (run from repo root)\n", root)
		os.Exit(1)
	}
	modified, skipped := 0, 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		changed, err := addHeader(path)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		if changed {
			modified++
		} else {
			skipped++
		}
		return nil
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("modified: %d  skipped (already had header): %d\n", modified, skipped)
}
