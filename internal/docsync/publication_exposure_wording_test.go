// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package docsync

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// exposureMarker is the grep-stable marker every publication-exposure warning
// carries. It is the UNIVERSE this gate derives itself from — deliberately not
// a hand-listed set of files, and deliberately not a helper's identifier.
const exposureMarker = "UNSELECTED-NAMESPACE-EXPOSURE"

// TestPublicationExposureWordingAcrossEveryEmitter holds every emitter of the
// exposure warning to the same claim rules, wherever it lives.
//
// WHY IT IS KEYED ON THE MARKER. v0.141.2's first cut put this check in
// internal/engines/postgres, over the two methods that render the engine-local
// message. There are THREE emitters: the backup door, the stream-recreates-a-
// missing-publication door (those two share the engine-local message), and the
// multi-schema `sync start` warning, which lives in internal/pipeline and was
// the headline feature of v0.141.0. A gate scoped to one package could not see
// the third, so the Bug 270 fork correction reached two of three messages and
// the release notes claimed "both messages now state it as a fork" — a false
// sentence in the release whose subject is false sentences. Caught by the
// pre-tag value-fidelity review, which is the third finding of this class in
// three releases.
//
// The rule this encodes: when a claim has N emitters, key the gate on the
// thing they all carry (the marker) rather than on the helper one of them
// happens to use.
//
// WHAT IT REACHES: the source text of every non-test file containing the
// marker. It cannot tell which branch of a file emits what, so it asserts
// file-level presence and absence of specific claim shapes. That is coarse,
// and it is enough for the failure it exists to catch — a sibling emitter left
// behind when a claim is corrected.
func TestPublicationExposureWordingAcrossEveryEmitter(t *testing.T) {
	root := repoRootFromDocsync(t)

	var emitters []string
	for _, dir := range []string{"internal", "cmd"} {
		err := filepath.Walk(filepath.Join(root, dir), func(path string, info os.FileInfo, werr error) error {
			if werr != nil || info.IsDir() {
				return werr
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			b, rerr := os.ReadFile(path)
			if rerr != nil {
				return rerr
			}
			if strings.Contains(string(b), exposureMarker) {
				emitters = append(emitters, path)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
	}

	// Anti-vacuity. Two is the floor because the two known homes are in
	// different packages; if the marker is renamed or the walk breaks, this
	// must fail rather than silently grade nothing.
	if len(emitters) < 2 {
		t.Fatalf("found %d file(s) emitting %s; the gate derives its universe from that marker, so a "+
			"rename makes it vacuous rather than failing. Re-anchor it.", len(emitters), exposureMarker)
	}

	// Claims every emitter must carry. Each is a sentence a previous release
	// got wrong, so each costs a real release to re-learn.
	required := []struct{ substr, why string }{
		{
			substr: "after the DROP",
			why: "Bug 270: dropping the publication is a FORK, not a certainty. Whether the stream wedges " +
				"or resumes having silently widened the publication depends on whether it must decode " +
				"anything written after the DROP. Stating only the loud branch hides the quiet one, which " +
				"is the branch that comes back green with a database-wide publication",
		},
		{
			substr: "never dropped",
			why: "the shared default publication is NOT removed by `sluice sync decommission` " +
				"(dropOwnPublicationIfPerStream returns early for it), so a remedy that names decommission " +
				"must say the shared default survives and has to be dropped by hand. v0.141.1 shipped the " +
				"opposite claim and v0.141.2's first cut copied it onto a second door",
		},
	}
	// Claim shapes no emitter may carry — the exact wordings that were wrong.
	forbidden := []struct{ substr, why string }{
		{
			substr: "can then never resume",
			why:    "Bug 270's absolute. It is one branch of a fork, stated as the outcome",
		},
		{
			substr: "which drops the slot and the publication together",
			why: "false for the shared default publication, which is the dominant configuration at " +
				"these doors — decommission drops the slot and leaves `sluice_pub` in place",
		},
	}

	for _, f := range emitters {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		body := string(b)
		rel, _ := filepath.Rel(root, f)
		for _, r := range required {
			if !strings.Contains(body, r.substr) {
				t.Errorf("%s emits %s but does not contain %q.\n\n%s", rel, exposureMarker, r.substr, r.why)
			}
		}
		for _, fb := range forbidden {
			if strings.Contains(body, fb.substr) {
				t.Errorf("%s emits %s and still contains %q.\n\n%s", rel, exposureMarker, fb.substr, fb.why)
			}
		}
	}
}
