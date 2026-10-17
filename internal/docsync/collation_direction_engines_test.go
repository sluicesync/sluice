// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package docsync

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
)

// TestCollationDirectionEngineListMatchesTheCode holds the operator doc's
// list of engines whose key-column collation direction the unique-key door
// judges (UNIQUE-KEY-STOPPED-WINDOW-DOOR) to the engines that implement
// ir.CollationLooseningResolver. Round-5 review MEDIUM-2: the prose implied
// a SQLite source into Postgres was judged while sqlite-trigger had no
// resolver, so a citext target merged 'a' and 'A' silently. The set is the
// registry's, by runtime assertion on every registered engine value.
//
// What it derives, stated so it cannot be read as broader: the resolver
// IMPLEMENTORS only — the engines whose collations are judged by direction.
// The engines judged by the narrow fail-closed rule instead are derived by
// [TestCollationNarrowRuleEngineListMatchesTheCode] below; neither list is
// hand-held.
func TestCollationDirectionEngineListMatchesTheCode(t *testing.T) {
	var fromCode []string
	for _, name := range engines.Names() {
		e, ok := engines.Get(name)
		if !ok {
			t.Fatalf("engines.Names() listed %q but engines.Get(%q) missed", name, name)
		}
		if _, ok := e.(ir.CollationLooseningResolver); ok {
			fromCode = append(fromCode, name)
		}
	}
	sort.Strings(fromCode)
	if len(fromCode) < 2 {
		t.Fatalf("only %d registered engine(s) implement ir.CollationLooseningResolver (%v); the scan broke or the "+
			"interface was renamed — this gate cannot pass on a near-empty set", len(fromCode), fromCode)
	}

	docPath := filepath.Join("..", "..", "docs", "operator", "cdc-streaming.md")
	raw, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("read %s: %v", docPath, err)
	}
	m := regexp.MustCompile(`<!--\s*collation-direction-engines:\s*([^>]*?)\s*-->`).FindSubmatch(raw)
	if m == nil {
		t.Fatalf("docs/operator/cdc-streaming.md carries no `<!-- collation-direction-engines: … -->` marker; add it "+
			"listing: %s", strings.Join(fromCode, ", "))
	}
	if fromDoc := splitList(string(m[1])); !equalStringSets(fromCode, fromDoc) {
		t.Errorf("the operator doc's collation-direction engine list disagrees with the code.\n"+
			"  code (engines implementing ir.CollationLooseningResolver): %s\n"+
			"  doc  (marker):                                             %s\n\n"+
			"Update the marker AND the prose beside it: an engine without a resolver is judged only by the narrow "+
			"fail-closed rule (a target column known to fold), never by direction.",
			strings.Join(fromCode, ", "), strings.Join(fromDoc, ", "))
	}
}

// TestCollationNarrowRuleEngineListMatchesTheCode holds the doc's list of
// source engines judged by the unique-key door's NARROW collation rule (a
// Postgres-family target key column known to fold refuses) to the
// registered engines that can stream changes (Capabilities().CDC is not
// none) but implement no ir.CollationLooseningResolver: those are the
// sources keyColumnWeakening routes to foldingTargetColumn (round-6 review
// LOW-D).
func TestCollationNarrowRuleEngineListMatchesTheCode(t *testing.T) {
	var fromCode []string
	for _, name := range engines.Names() {
		e, ok := engines.Get(name)
		if !ok {
			t.Fatalf("engines.Names() listed %q but engines.Get(%q) missed", name, name)
		}
		if _, resolves := e.(ir.CollationLooseningResolver); !resolves && e.Capabilities().CDC != ir.CDCNone {
			fromCode = append(fromCode, name)
		}
	}
	sort.Strings(fromCode)
	if len(fromCode) == 0 {
		t.Fatal("no registered CDC source lacks a collation resolver; the narrow rule has no engine, so the doc's " +
			"marker and the prose about it should go, or the scan broke")
	}
	raw, err := os.ReadFile(filepath.Join("..", "..", "docs", "operator", "cdc-streaming.md"))
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`<!--\s*collation-narrow-rule-engines:\s*([^>]*?)\s*-->`).FindSubmatch(raw)
	if m == nil {
		t.Fatalf("docs/operator/cdc-streaming.md carries no `<!-- collation-narrow-rule-engines: … -->` marker; add it "+
			"listing: %s", strings.Join(fromCode, ", "))
	}
	if fromDoc := splitList(string(m[1])); !equalStringSets(fromCode, fromDoc) {
		t.Errorf("the doc's narrow-rule engine list disagrees with the code.\n  code: %s\n  doc:  %s",
			strings.Join(fromCode, ", "), strings.Join(fromDoc, ", "))
	}
}
