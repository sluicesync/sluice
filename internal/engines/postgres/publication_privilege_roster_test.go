// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// publicationDDLLine matches a line that EXECUTES publication DDL. Keyed on
// the SQL verb rather than on a helper's identifier: the queries are built by
// fmt.Sprintf into local variables with several different names
// (createQuery, dropQuery, alterQuery), so a roster keyed on a name would
// miss the next one someone invents.
var publicationDDLQuery = regexp.MustCompile(`(CREATE|DROP|ALTER) PUBLICATION`)

// TestPublicationPrivilegeRoster_EveryDDLSiteIsClassified enumerates the
// publication DDL sites and requires each to route its failure through
// classifyPublicationPermission.
//
// WHY THIS EXISTS (UPR-4, from pgcopydb fork PR #59). sluice ran every one of
// these statements with nothing between the operator and the server, so a role
// without the right grant got a raw uncoded `SQLSTATE 42501` at cold start
// with no remedy — and no hint matched it, because migcore/hints.go carries
// only "permission denied for schema" and "permission denied for replication",
// neither of which is a substring of any of the three real failures (measured
// on PG 16: permission denied for database / must be owner of table / must be
// superuser to create FOR ALL TABLES publication).
//
// The gate is the enumeration, not the fix. CLAUDE.md's most expensive
// recurring shape is a refusal that reaches one path and silently misses a
// sibling, and this file holds NINE execution sites across create, drop,
// alter, the multi-schema FOR ALL TABLES ensure, and two rolled-back probe
// transactions. Wiring six of eight would look finished and read finished.
//
// WHAT IT REACHES, stated so the name cannot be read as broader than the
// truth: lines in publication.go that both execute (`ExecContext`) and name
// publication DDL. It does NOT reach publication DDL executed from another
// file, nor a query assembled so the verb never appears on the exec line — if
// either becomes possible, this roster needs widening and will not tell you
// so. The anti-vacuity floor below is what catches the first of those.
func TestPublicationPrivilegeRoster_EveryDDLSiteIsClassified(t *testing.T) {
	const file = "publication.go"
	b, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("read %s: %v", file, err)
	}
	lines := strings.Split(string(b), "\n")

	// Find the DDL query assignments, then the exec + error-return that
	// follows each. Walking forward from the query keeps the pairing local
	// and avoids matching an unrelated ExecContext elsewhere in the file.
	var sites, classified int
	var unclassified []string
	for i, ln := range lines {
		if strings.HasPrefix(strings.TrimSpace(ln), "//") || !publicationDDLQuery.MatchString(ln) {
			continue
		}
		// Look ahead a short window for the exec of this query and the
		// error branch it returns from.
		window := lines[i:min(i+12, len(lines))]
		joined := strings.Join(window, "\n")
		if !strings.Contains(joined, "ExecContext(ctx,") {
			continue // a query built here but executed elsewhere; not a site
		}
		sites++
		if strings.Contains(joined, "classifyPublicationPermission") {
			classified++
			continue
		}
		// isDuplicatePublication guards the create-race path, which returns
		// nil rather than an error — that arm is not a failure to classify.
		if strings.Contains(joined, "isDuplicatePublication") && strings.Contains(joined, "classifyPublicationPermission") {
			classified++
			continue
		}
		unclassified = append(unclassified, strings.TrimSpace(ln))
	}

	// Anti-vacuity. The regex stopped matching, the file moved, or the DDL
	// is now assembled somewhere this walk cannot see. Any of those makes
	// "every site is classified" trivially true.
	if sites < 6 {
		t.Fatalf("found only %d publication-DDL exec site(s); this file carried NINE when the roster was "+
			"written, so the walk has broken rather than the code having shrunk. A roster that finds "+
			"nothing passes silently, which is the failure this floor exists to prevent.", sites)
	}

	for _, u := range unclassified {
		t.Errorf("publication DDL executed without classifyPublicationPermission:\n    %s\n\n"+
			"A raw SQLSTATE 42501 here reaches the operator uncoded and unsteered — the exact defect "+
			"UPR-4 closed. Wrap the returned error, or add an exemption here naming why this site "+
			"cannot produce a permission error.", u)
	}
	if len(unclassified) > 0 {
		t.Logf("%d of %d sites classified", classified, sites)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
