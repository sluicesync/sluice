// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package docsync

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The stopped-cold-start resume (A0909-STOP-1) is a claim about an
// ENGINE SET: "re-running resumes on a PostgreSQL source; every other
// source still refuses". This project has shipped a wrong sentence of
// exactly that shape three releases running, and the standing rule is
// that such a claim rides a marker derived from the code rather than
// prose someone maintains by hand.
//
// The capability IS the answer here: the resume's first gate is a type
// assertion for ir.SnapshotAnchorVerifier, so an engine that pins it
// can be resumed and an engine that does not cannot — there is no
// second condition to reason about, which is why this marker can be a
// mechanical derivation rather than a summary.
//
// Sibling to TestFilteredSyncEngineListMatchesTheCode; same helpers,
// same shape.
func TestColdStartResumeEngineListMatchesTheCode(t *testing.T) {
	const capability = "SnapshotAnchorVerifier"

	fromCode := registeredEnginesImplementing(t, capability)
	if len(fromCode) == 0 {
		t.Fatalf("no engine package declares a `var _ ir.%s` pin; either the capability was renamed or the "+
			"scan broke — and an empty set would make the resume unreachable while this gate reported "+
			"agreement", capability)
	}

	docPath := filepath.Join("..", "..", "docs", "operator", "cdc-streaming.md")
	raw, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("read %s: %v", docPath, err)
	}

	marker := regexp.MustCompile(`<!--\s*coldstart-resume-engines:\s*([^>]*?)\s*-->`)
	m := marker.FindSubmatch(raw)
	if m == nil {
		t.Fatalf("docs/operator/cdc-streaming.md carries no `<!-- coldstart-resume-engines: … -->` marker "+
			"beside the stopped-cold-start resume section; add it listing: %s", strings.Join(fromCode, ", "))
	}

	fromDoc := splitList(string(m[1]))
	if !equalStringSets(fromCode, fromDoc) {
		t.Errorf("the operator doc's stopped-cold-start-resume engine list disagrees with the code.\n"+
			"  code (engines pinning ir.%s): %s\n"+
			"  doc  (marker):                %s\n\n"+
			"A source without that capability cannot prove where its snapshot anchor stands, so its cold "+
			"start after a stop still refuses and must not be described as resumable. Update the marker AND "+
			"the prose beside it — and if you are about to write this in release notes, use this list rather "+
			"than reasoning about which engines lack the capability.",
			capability, strings.Join(fromCode, ", "), strings.Join(fromDoc, ", "))
	}
}
