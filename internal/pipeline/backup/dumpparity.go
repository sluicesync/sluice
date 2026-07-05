// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Restore-parity oracle comparator (roadmap item 51).
//
// The oracle migrates the same Postgres source twice — once through
// sluice's Migrator, once through `pg_dump | psql` (the reference
// implementation of "everything a PG schema can contain") — then
// compares `pg_dump --schema-only` of the two targets. Raw text-diff
// of dump output is ordering-fragile, so the comparison works on
// statement *sets* keyed by object identity (verb + object kind +
// schema-qualified name): any statement present on one side only, or
// present on both sides with different bodies, is a divergence. Each
// divergence is either a documented degradation (matched by
// DumpParityAllowlist, every entry citing the doc/ADR/code that
// declares it) or a latent bug — there is no third category.
//
// Everything in this file is a pure function over dump text so the
// splitting / normalization / keying / diff / allowlist logic gets
// unit pins without Docker; the integration harness that produces
// the dumps lives in migrate_dump_parity_integration_test.go.
//
// v1 limitations (deliberate, documented):
//   - The statement splitter understands single quotes, double
//     quotes, line comments, nested block comments, and
//     dollar-quoting — but not `E'...'` backslash-escape strings.
//     pg_dump emits standard_conforming_strings output, so E-strings
//     don't appear in the dumps this comparator consumes; the
//     kitchen-sink seed also avoids function/procedure bodies so a
//     splitter bug in dollar-quote handling can't silently eat a
//     statement (user triggers are roadmap item 50).
//   - Allowlist granularity is the statement KEY. Allowlisting a
//     body mismatch on `CREATE TABLE public.orders` masks every
//     divergence in that table's body, not just the cited one — the
//     harness logs both bodies for every allowlisted mismatch so the
//     full delta stays visible in CI logs.

package backup

import (
	"path"
	"sort"
	"strings"
)

// dumpStatement is one normalized, identity-keyed statement extracted
// from a `pg_dump --schema-only` text dump.
type dumpStatement struct {
	// Key is the object-identity key, e.g.
	// "CREATE TABLE public.orders" or
	// "ALTER TABLE public.orders ADD CONSTRAINT orders_customer_fk".
	Key string

	// Body is the whitespace-collapsed statement text (terminator
	// stripped).
	Body string
}

// dumpParityMismatch is a same-key-different-body divergence: both
// targets have the object, with different definitions.
type dumpParityMismatch struct {
	Key    string
	Sluice string // body in the sluice-migrated target's dump
	Oracle string // body in the pg_dump|psql-restored target's dump
}

// dumpParityDiff is the full divergence set between the two targets.
type dumpParityDiff struct {
	OnlyInSluice []dumpStatement
	OnlyInOracle []dumpStatement
	Mismatched   []dumpParityMismatch
}

// Empty reports whether the two dumps are in full parity.
func (d dumpParityDiff) Empty() bool {
	return len(d.OnlyInSluice) == 0 && len(d.OnlyInOracle) == 0 && len(d.Mismatched) == 0
}

// dumpParityAllowlistEntry declares one KNOWN divergence class. The
// pattern matches statement keys under stdlib path.Match semantics
// (same glob dialect as migcore.TableFilter); every entry must carry a
// human-readable reason plus a citation — the doc/ADR/source file
// that declares the degradation, or the literal
// DumpParityTriageCitation for a latent gap under investigation.
type dumpParityAllowlistEntry struct {
	Pattern  string
	Reason   string
	Citation string
}

// DumpParityTriageCitation marks an allowlist entry whose divergence
// is NOT a documented degradation: a real fidelity gap found by the
// oracle, allowlisted only to keep the harness landable while the
// gap is triaged into an audit finding/bug. Entries carrying this
// citation are reported with a distinct TRIAGE banner by the harness
// so they cannot quietly become permanent.
const DumpParityTriageCitation = "TRIAGE"

// MatchDumpParityAllowlist returns the first entry whose pattern
// matches key, or nil. First-match-wins keeps the list reviewable:
// order entries most-specific-first.
func MatchDumpParityAllowlist(key string, allow []dumpParityAllowlistEntry) *dumpParityAllowlistEntry {
	for i := range allow {
		if ok, err := path.Match(allow[i].Pattern, key); err == nil && ok {
			return &allow[i]
		}
	}
	return nil
}

// ParseSchemaDump splits dump text into statements, drops the session
// preamble, normalizes whitespace, and keys each statement by object
// identity.
func ParseSchemaDump(dump string) []dumpStatement {
	var out []dumpStatement
	for _, raw := range splitDumpStatements(dump) {
		body := normalizeDumpStatement(raw)
		if body == "" {
			continue
		}
		out = append(out, dumpStatement{Key: dumpStatementKey(body), Body: body})
	}
	return out
}

// CountCreateStatements returns how many parsed statements are CREATE
// statements. The vacuous-pass guard: the seed declares how many
// objects each side must yield at minimum, and a comparator/normalizer
// bug that eats statements trips the floor instead of reading as
// parity (the check-shard-coverage.sh lesson applied here).
func CountCreateStatements(stmts []dumpStatement) int {
	n := 0
	for _, s := range stmts {
		if strings.HasPrefix(s.Key, "CREATE ") {
			n++
		}
	}
	return n
}

// splitDumpStatements splits SQL text into top-level statements on
// semicolons, respecting single-quoted literals, double-quoted
// identifiers, line comments, nested block comments, and
// dollar-quoted bodies. Comments are elided from the returned text.
func splitDumpStatements(text string) []string {
	var out []string
	var cur strings.Builder
	flush := func() {
		if s := strings.TrimSpace(cur.String()); s != "" {
			out = append(out, s)
		}
		cur.Reset()
	}
	i, n := 0, len(text)
	for i < n {
		c := text[i]
		switch {
		case c == '-' && i+1 < n && text[i+1] == '-':
			// Line comment: elide to end-of-line (the newline itself
			// survives as ordinary whitespace).
			for i < n && text[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < n && text[i+1] == '*':
			// Block comment; PG block comments nest.
			depth := 1
			i += 2
			for i < n && depth > 0 {
				switch {
				case text[i] == '*' && i+1 < n && text[i+1] == '/':
					depth--
					i += 2
				case text[i] == '/' && i+1 < n && text[i+1] == '*':
					depth++
					i += 2
				default:
					i++
				}
			}
			cur.WriteByte(' ')
		case c == '\'' || c == '"':
			// Quoted literal/identifier. A doubled quote ('' or "")
			// re-enters the quoted state on the next loop pass, so it
			// needs no special handling here.
			quote := c
			cur.WriteByte(c)
			i++
			for i < n {
				cur.WriteByte(text[i])
				if text[i] == quote {
					i++
					break
				}
				i++
			}
		case c == '$':
			if tag, ok := dollarQuoteTag(text[i:]); ok {
				rest := text[i+len(tag):]
				end := strings.Index(rest, tag)
				if end < 0 {
					// Unterminated dollar-quote: consume the rest so a
					// malformed dump can't smuggle statements past the
					// scanner.
					cur.WriteString(text[i:])
					i = n
				} else {
					stop := i + len(tag) + end + len(tag)
					cur.WriteString(text[i:stop])
					i = stop
				}
			} else {
				cur.WriteByte(c)
				i++
			}
		case c == ';':
			flush()
			i++
		default:
			cur.WriteByte(c)
			i++
		}
	}
	flush()
	return out
}

// dollarQuoteTag reports whether s begins a dollar-quote opener
// ($$, $tag$) and returns the full tag including both dollar signs.
// A tag is an optional identifier (letter/underscore first, then
// letters/digits/underscores) between the dollars — `$1` positional
// parameters don't qualify.
func dollarQuoteTag(s string) (string, bool) {
	if len(s) < 2 || s[0] != '$' {
		return "", false
	}
	j := 1
	if isDollarTagStart(s[j]) {
		j++
		for j < len(s) && isDollarTagChar(s[j]) {
			j++
		}
	}
	if j < len(s) && s[j] == '$' {
		return s[:j+1], true
	}
	return "", false
}

func isDollarTagStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isDollarTagChar(c byte) bool {
	return isDollarTagStart(c) || (c >= '0' && c <= '9')
}

// normalizeDumpStatement collapses all whitespace runs to single
// spaces and returns "" for statements that are session preamble
// rather than schema objects (SET, the search_path set_config SELECT,
// psql backslash metacommands).
func normalizeDumpStatement(stmt string) string {
	body := strings.Join(strings.Fields(stmt), " ")
	if body == "" {
		return ""
	}
	upper := strings.ToUpper(body)
	switch {
	case strings.HasPrefix(upper, "SET "),
		strings.HasPrefix(upper, "SELECT PG_CATALOG.SET_CONFIG"),
		strings.HasPrefix(body, "\\"):
		return ""
	}
	return body
}

// dumpStatementKey derives the object-identity key for a normalized
// statement: verb + object kind + schema-qualified name, plus the
// sub-action for ALTERs (ADD CONSTRAINT x / ALTER COLUMN y / ATTACH
// PARTITION z). Statements the keyer doesn't recognize fall back to
// their first three tokens — a fallback key still diffs correctly,
// it's just less precise about what changed.
func dumpStatementKey(body string) string {
	toks := strings.Fields(body)
	tok := func(i int) string {
		if i >= len(toks) {
			return ""
		}
		return strings.TrimRight(toks[i], "(,;")
	}
	up := func(i int) string { return strings.ToUpper(tok(i)) }

	switch up(0) {
	case "CREATE":
		// Skip modifiers that don't change object identity.
		i := 1
	modifiers:
		for {
			switch up(i) {
			case "UNIQUE", "OR", "REPLACE", "MATERIALIZED", "UNLOGGED",
				"GLOBAL", "LOCAL", "TEMPORARY", "TEMP", "RECURSIVE":
				i++
			default:
				break modifiers
			}
		}
		kind := up(i)
		i++
		if up(i) == "IF" { // IF NOT EXISTS
			i += 3
		}
		return "CREATE " + kind + " " + tok(i)

	case "ALTER":
		kind := up(1)
		i := 2
		if up(i) == "ONLY" {
			i++
		}
		if up(i) == "IF" { // IF EXISTS
			i += 2
		}
		name := tok(i)
		action := up(i + 1)
		switch action {
		case "ADD", "ALTER", "ATTACH", "DETACH", "DROP", "RENAME", "VALIDATE":
			// e.g. ADD CONSTRAINT c / ALTER COLUMN col / ATTACH
			// PARTITION p — include the sub-object kind and name.
			return "ALTER " + kind + " " + name + " " + action + " " + up(i+2) + " " + tok(i+3)
		default:
			return "ALTER " + kind + " " + name + " " + action
		}

	case "COMMENT":
		// COMMENT ON <kind> <name> IS ...; kind may be two words
		// (MATERIALIZED VIEW, FOREIGN TABLE).
		i := 2
		kind := up(i)
		i++
		if kind == "MATERIALIZED" || kind == "FOREIGN" {
			kind += " " + up(i)
			i++
		}
		return "COMMENT ON " + kind + " " + tok(i)
	}

	return strings.TrimSpace(tok(0) + " " + tok(1) + " " + tok(2))
}

// DiffDumpStatements computes the divergence set between the
// sluice-target dump and the oracle-target dump. Statements are
// grouped by key; within a key, identical bodies cancel (multiset
// intersection), remaining bodies pair up as mismatches, and unpaired
// leftovers surface as only-in-one-side. All three result slices are
// sorted by key for a deterministic ledger.
func DiffDumpStatements(sluice, oracle []dumpStatement) dumpParityDiff {
	group := func(stmts []dumpStatement) map[string][]string {
		m := make(map[string][]string)
		for _, s := range stmts {
			m[s.Key] = append(m[s.Key], s.Body)
		}
		return m
	}
	a, b := group(sluice), group(oracle)

	keySet := make(map[string]struct{}, len(a)+len(b))
	for k := range a {
		keySet[k] = struct{}{}
	}
	for k := range b {
		keySet[k] = struct{}{}
	}
	keys := make([]string, 0, len(keySet))
	for k := range keySet {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var d dumpParityDiff
	for _, k := range keys {
		as, bs := removeCommonBodies(a[k], b[k])
		pairs := min(len(as), len(bs))
		for i := 0; i < pairs; i++ {
			d.Mismatched = append(d.Mismatched, dumpParityMismatch{Key: k, Sluice: as[i], Oracle: bs[i]})
		}
		for _, body := range as[pairs:] {
			d.OnlyInSluice = append(d.OnlyInSluice, dumpStatement{Key: k, Body: body})
		}
		for _, body := range bs[pairs:] {
			d.OnlyInOracle = append(d.OnlyInOracle, dumpStatement{Key: k, Body: body})
		}
	}
	return d
}

// removeCommonBodies drops the multiset intersection from both sides
// and returns the sorted leftovers.
func removeCommonBodies(a, b []string) (restA, restB []string) {
	counts := make(map[string]int, len(b))
	for _, s := range b {
		counts[s]++
	}
	for _, s := range a {
		if counts[s] > 0 {
			counts[s]--
			continue
		}
		restA = append(restA, s)
	}
	for _, s := range b {
		if counts[s] > 0 {
			counts[s]--
			restB = append(restB, s)
		}
	}
	sort.Strings(restA)
	sort.Strings(restB)
	return restA, restB
}
