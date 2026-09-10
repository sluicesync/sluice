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

// queryAssignment captures the identifier on the left of a `:=` or `=`
// whose right-hand side is an fmt.Sprintf — how every publication query in
// this file is built.
var queryAssignment = regexp.MustCompile(`^\s*(\w+)\s*:?=\s*fmt\.Sprintf\(`)

// execCall captures the QUERY ARGUMENT of an ExecContext, on either a `db`
// or a `tx` receiver. The probe transactions use `tx`, and a roster that
// only knew `db.` would miss both of them.
var execCall = regexp.MustCompile(`\.ExecContext\(ctx,\s*([^,)]+)`)

// queryVarName matches the codebase's own naming convention for a variable
// holding publication DDL, and it covers a query received as a PARAMETER:
// `rescopeRowFilteredPublication(ctx, db, name, alterQuery, …)` executes a
// query that has no assignment anywhere in its own function.
//
// **It is REDUNDANT today, and saying so is the point.** The first draft of
// this comment claimed it was "what catches" that parameter site. It is
// not: the parameter is also named `alterQuery`, and this file assigns
// `alterQuery` elsewhere, so ddlQueryVars already holds that name and
// catches the site on its own. Mutation-run to check rather than assumed —
// deleting this clause changes nothing today. It earns its place only if
// the assignments are renamed while a parameter keeps one of these names,
// so treat it as defence, not as the load-bearing clause. A brand-new gate
// is exactly where an unverified premise is easiest to write down and
// hardest to notice.
var queryVarName = regexp.MustCompile(`^(create|drop|alter)Query$`)

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
// sibling, and this file holds TEN execution sites across create, drop,
// alter, the multi-schema FOR ALL TABLES ensure, and two rolled-back probe
// transactions. Wiring eight of ten would look finished and read finished.
//
// # This roster graded 8 of 10, and its own prose said 9 (audit A0909-TCI-H-3)
//
// The first cut keyed on the QUERY-ASSIGNMENT line and looked forward a
// bounded window for the exec. That is structurally wrong for counting
// EXECUTIONS, in two ways that both showed up here:
//
//   - The `ALTER PUBLICATION … SET TABLE` assigned at publication.go:296
//     is executed twelve lines later, one line past the window, because a
//     row-filter guard sits between them. The site was not merely
//     unclassified, it was not COUNTED — the walk saw no `ExecContext` in
//     the window and skipped the line entirely.
//   - `rescopeRowFilteredPublication` receives that same query as a
//     PARAMETER and executes it in a probe transaction. There is no
//     assignment line anywhere near that exec, so nothing anchored it.
//
// Both of those sites happen to be classified correctly, so nothing was
// broken — but a roster that cannot see a site cannot report it, and the
// doc-comment above asserted "NINE execution sites" while the walk found
// eight and the file held ten. Three numbers, no two agreeing, and only
// the wrong one was written down.
//
// So the universe is now the EXECUTIONS, keyed on `ExecContext`: a site
// is an exec whose argument is a publication-DDL query, and the check
// looks forward from the EXEC — where the error branch always is —
// rather than from wherever the string was built.
//
// WHAT IT REACHES, stated so the name cannot be read as broader than the
// truth: `ExecContext` calls in publication.go whose argument is either a
// string literal naming publication DDL, or an identifier this file
// assigns such a query to, or an identifier matching the codebase's
// `<verb>Query` naming convention (which is how the parameter case above
// is caught). It does NOT reach publication DDL executed from another
// file, nor a query passed under a name that fits none of those. The
// floors below are what catch the first of those.
func TestPublicationPrivilegeRoster_EveryDDLSiteIsClassified(t *testing.T) {
	const file = "publication.go"
	b, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("read %s: %v", file, err)
	}
	lines := strings.Split(string(b), "\n")

	// Pass 1: identifiers this file assigns a publication-DDL query to.
	// `fmt.Sprintf` calls wrap, so the verb can sit on the line after the
	// assignment; the lookahead here is two lines and is bounded by the
	// closing paren, which is a different thing from the site-pairing
	// window that went wrong before — it only decides what a NAME holds.
	ddlQueryVars := map[string]bool{}
	for i, ln := range lines {
		m := queryAssignment.FindStringSubmatch(ln)
		if m == nil {
			continue
		}
		joined := strings.Join(lines[i:min(i+3, len(lines))], "\n")
		if publicationDDLQuery.MatchString(joined) {
			ddlQueryVars[m[1]] = true
		}
	}

	// Pass 2: every EXEC of one of those, or of a DDL string literal. The
	// site is the exec line, and the error branch is always immediately
	// below it, so the window is short and cannot reach a sibling's
	// evidence — the 2026-09-06 defect where an unclassified site was
	// satisfied by its NEIGHBOUR's classifyPublicationPermission.
	isDDLExec := func(ln string) bool {
		if strings.HasPrefix(strings.TrimSpace(ln), "//") {
			return false
		}
		m := execCall.FindStringSubmatch(ln)
		if m == nil {
			return false
		}
		arg := strings.TrimSpace(m[1])
		return publicationDDLQuery.MatchString(ln) || ddlQueryVars[arg] || queryVarName.MatchString(arg)
	}

	var sites, classified int
	var unclassified []string
	for i, ln := range lines {
		if !isDDLExec(ln) {
			continue
		}
		sites++
		// The window runs to the NEXT DDL exec, capped at 20 lines. The cap
		// alone is not the safety property — the next-exec bound is. An
		// error branch here can be long (site 1013's carries a nine-line
		// comment explaining the create-race arm before it classifies), so
		// a tight cap produces false positives, and a generous cap with no
		// structural bound is how a site gets satisfied by its NEIGHBOUR's
		// evidence, which is the 2026-09-06 defect. Both bounds together
		// give a window that is as long as the site's own code and stops
		// exactly where the next site begins.
		end := min(i+20, len(lines))
		for j := i + 1; j < end; j++ {
			if isDDLExec(lines[j]) {
				end = j
				break
			}
		}
		if strings.Contains(strings.Join(lines[i:end], "\n"), "classifyPublicationPermission") {
			classified++
			continue
		}
		unclassified = append(unclassified, strings.TrimSpace(ln))
	}

	// Anti-vacuity. The regex stopped matching, the file moved, or the DDL
	// is now assembled somewhere this walk cannot see. Any of those makes
	// "every site is classified" trivially true.
	//
	// The floor is TEN, the number the file actually holds, not a slack
	// figure below it. Audit A0909-TCI-H-3 is what a slack floor buys: the
	// previous floor was six over a real ten, so a walk that had silently
	// dropped to eight sat comfortably above it and reported success. A
	// floor funded below the true count is a floor that cannot fire.
	if sites < 10 {
		t.Fatalf("found only %d publication-DDL exec site(s); this file carries TEN. The walk has broken "+
			"rather than the code having shrunk — re-point it rather than lowering this floor, which is "+
			"exactly what let this roster grade 8 of 10 while passing (audit A0909-TCI-H-3). Sites are "+
			"keyed on ExecContext, with the DDL-carrying argument recognised as a literal, an "+
			"identifier assigned a DDL query in this file, or a <verb>Query name.", sites)
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
