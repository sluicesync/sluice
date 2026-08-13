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
)

// The PlanetScale-only command list, kept honest against the command
// sources (audit 2026-08-11 D-3).
//
// production-readiness.md claimed "the online schema-change family
// (`backfill`, `expand-contract`, `deploy-ddl`) covers MySQL-family +
// Postgres in place" — but `expand-contract` hardcodes
// `resolveEngine("planetscale")` and `deploy-ddl` drives PlanetScale
// deploy requests through the PlanetScale API client; both refuse
// without a PlanetScale service token. Only `backfill` (an open
// `--driver` flag) covers the wider set, which
// docs/schema-change-runbook.md's command table states correctly.
//
//	<!-- planetscale-only-commands: deploy-ddl, expand-contract -->
//
// The derivation is source-anchored: each command named in the marker
// must still carry its PlanetScale hardcode, and `backfill` must still
// carry an open `--driver` flag (the anti-vacuity direction — if
// backfill ever became PlanetScale-only too, or a hardcode was lifted,
// the marker and prose need re-deriving, and this gate says so).
func TestPlanetScaleOnlyCommandListMatchesTheCode(t *testing.T) {
	cmdDir := filepath.Join("..", "..", "cmd", "sluice")
	read := func(name string) string {
		raw, err := os.ReadFile(filepath.Join(cmdDir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		return string(raw)
	}

	fromCode := map[string]bool{}
	if strings.Contains(read("expand_contract.go"), `resolveEngine("planetscale")`) {
		fromCode["expand-contract"] = true
	}
	deployDDL := read("deploy_ddl.go")
	if strings.Contains(deployDDL, "PlanetScale service token is required") &&
		strings.Contains(deployDDL, "api.New(") {
		fromCode["deploy-ddl"] = true
	}
	if len(fromCode) != 2 {
		t.Fatalf("expected both expand-contract and deploy-ddl to carry their PlanetScale hardcodes; found %v — "+
			"a hardcode moved or was lifted; re-derive the doc claim and update this gate's anchors", fromCode)
	}
	// The anti-vacuity direction: backfill is the family member that is
	// deliberately NOT PlanetScale-only (open --driver flag, no
	// hardcoded engine). If this flips, the prose contrast is wrong.
	backfill := read("backfill.go")
	if !strings.Contains(backfill, "Driver string") || strings.Contains(backfill, `resolveEngine("planetscale")`) {
		t.Fatal("backfill no longer looks like the open-driver member of the schema-change family — " +
			"the production-readiness contrast (backfill wide, the other two PlanetScale-only) needs re-deriving")
	}

	docPath := filepath.Join("..", "..", "docs", "production-readiness.md")
	raw, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("read %s: %v", docPath, err)
	}
	marker := regexp.MustCompile(`<!--\s*planetscale-only-commands:\s*([^>]*?)\s*-->`)
	m := marker.FindSubmatch(raw)
	if m == nil {
		t.Fatal("docs/production-readiness.md carries no `<!-- planetscale-only-commands: … -->` marker; " +
			"add it listing: deploy-ddl, expand-contract")
	}
	fromCodeList := make([]string, 0, len(fromCode))
	for name := range fromCode {
		fromCodeList = append(fromCodeList, name)
	}
	sort.Strings(fromCodeList)
	fromDoc := splitList(string(m[1]))
	if !equalStringSets(fromCodeList, fromDoc) {
		t.Errorf("the PlanetScale-only command list disagrees with the command sources.\n"+
			"  code: %s\n  doc:  %s",
			strings.Join(fromCodeList, ", "), strings.Join(fromDoc, ", "))
	}
}
