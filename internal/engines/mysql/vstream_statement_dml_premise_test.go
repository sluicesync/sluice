// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestVStreamStatementDMLPremise_UpstreamStillEmitsRowDMLVEvents pins the
// environmental fact the entire statement-DML refusal class rests on.
//
// WHY. Four dispatchers refuse statement-format DML on the VStream lane, and
// every one of them is justified by the same sentence in
// `cdc_vstream_statement_dml.go`: the vendored vstreamer's QueryEvent switch
// has explicit arms for the four row-DML categories, so those events REACH
// sluice rather than being absorbed upstream. That sentence is load-bearing
// in both directions — if upstream absorbed them, the refusals would be dead
// code; because upstream forwards them, a dispatcher without an arm drops
// rows silently, which is exactly the defect found on the concurrent COPY
// pump in the 2026-09-06 audit.
//
// The matrix previously recorded the OPPOSITE ("absorbed upstream: the
// vendored vstreamer kills the stream on statement-format DML"). It was
// wrong, and being wrong is what let two dispatchers ship with a silent
// `default:` arm. A premise that has already been wrong once, and that four
// refusals depend on, is exactly the kind CLAUDE.md says owes a check rather
// than a comment.
//
// WHAT THIS CAN AND CANNOT SHOW. It reads the pinned dependency's SOURCE and
// asserts the four arms still exist. That is strictly weaker than measuring a
// real cluster — no such measurement exists for any of the four arms — but it
// is strictly stronger than prose, and it fails the moment a Vitess bump
// changes the upstream behaviour, which is the realistic way this premise
// would silently stop being true.
func TestVStreamStatementDMLPremise_UpstreamStillEmitsRowDMLVEvents(t *testing.T) {
	out, err := exec.CommandContext(t.Context(), "go", "list", "-m", "-f", "{{.Dir}}", "vitess.io/vitess").Output()
	if err != nil {
		t.Skipf("cannot locate the vitess module (%v) — this pin needs the module cache populated", err)
	}
	dir := strings.TrimSpace(string(out))
	if dir == "" {
		t.Skip("vitess module dir empty")
	}
	src := filepath.Join(dir, "go", "vt", "vttablet", "tabletserver", "vstreamer", "vstreamer.go")
	b, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read vendored vstreamer source: %v\n\nThe premise behind FOUR refusals is unreadable, which is "+
			"not a skip: if upstream moved this file the premise needs re-checking by hand.", err)
	}
	body := string(b)

	// The four row-DML statement categories the refusal class names. Sourced
	// from the production verb table so a fifth category added there forces
	// this premise to be re-checked rather than silently outgrowing it.
	for _, verb := range vstreamStatementDMLVerbs {
		// "INSERT" -> "StmtInsert", the sqlparser constant upstream switches
		// on. Derived from the production verb table rather than listed, so a
		// fifth category forces this premise to be re-checked.
		arm := "Stmt" + strings.ToUpper(verb[:1]) + strings.ToLower(verb[1:])
		if !strings.Contains(body, arm) {
			t.Errorf("vendored vstreamer no longer names %q.\n\n"+
				"sluice refuses statement-format %s on FOUR VStream dispatchers because upstream FORWARDS it as a "+
				"VEvent rather than absorbing it. If that arm is gone, either the refusals are now dead code or "+
				"the event arrives under a different type and the dispatchers drop it SILENTLY — the exact defect "+
				"the concurrent COPY pump shipped with. Re-read %s and re-derive the class before changing anything.",
				arm, verb, src)
		}
	}

	// Anti-vacuity: a file that stopped containing ANY of the markers would
	// fail every assertion above for the wrong reason (wrong file, moved
	// upstream), and a file we matched by accident would pass them all.
	// The anchor is the sqlparser switch itself. The first cut of this check
	// looked for "QueryEvent" — the name the sluice-side comment uses for the
	// upstream construct — and that string does not appear in the file at all.
	// It failed, correctly, and is worth recording: an anti-vacuity check whose
	// own anchor is guessed from prose is the same defect it exists to catch.
	if !strings.Contains(body, "sqlparser.StmtOther") {
		t.Fatalf("%s does not contain the sqlparser statement-category switch — this is not the file the "+
			"premise describes (or upstream restructured it), so the assertions above prove nothing", src)
	}
	if len(vstreamStatementDMLVerbs) < 4 {
		t.Fatalf("the production verb table holds %d entries; the premise names FOUR row-DML categories. "+
			"A shrunken table makes this pin trivially true.", len(vstreamStatementDMLVerbs))
	}
}
