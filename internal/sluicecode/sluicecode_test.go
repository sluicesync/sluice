// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sluicecode

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestWrapAndFromError pins the wrapping contract: FromError extracts
// the CodedError through further fmt.Errorf wrapping, the message is
// byte-identical to the wrapped error's (presentation-free), and the
// chain below stays traversable for errors.Is.
func TestWrapAndFromError(t *testing.T) {
	sentinel := errors.New("sentinel: zero date")
	coded := Wrap(CodeValueZeroDate, "pass --zero-date=null", fmt.Errorf("mysql: column %q: %w", "d", sentinel))
	outer := fmt.Errorf("pipeline: copy table %q: %w", "t", coded)

	ce, ok := FromError(outer)
	if !ok {
		t.Fatal("FromError did not find the CodedError through the outer wrap")
	}
	if ce.Code != CodeValueZeroDate {
		t.Errorf("Code = %q; want %q", ce.Code, CodeValueZeroDate)
	}
	if ce.Hint != "pass --zero-date=null" {
		t.Errorf("Hint = %q; want the construction-site hint", ce.Hint)
	}
	if coded.Error() != ce.Err.Error() {
		t.Errorf("Error() must delegate to the wrapped error: %q vs %q", coded.Error(), ce.Err.Error())
	}
	if !errors.Is(outer, sentinel) {
		t.Error("errors.Is must traverse through the CodedError to the sentinel")
	}
}

// TestWrapNil ensures nil-in-nil-out so construction sites can wrap
// inline without a guard.
func TestWrapNil(t *testing.T) {
	if got := Wrap(CodeValueNULByte, "hint", nil); got != nil {
		t.Errorf("Wrap(_, _, nil) = %v; want nil", got)
	}
}

// TestFromErrorUncoded confirms a plain error chain yields no code.
func TestFromErrorUncoded(t *testing.T) {
	if _, ok := FromError(fmt.Errorf("outer: %w", errors.New("inner"))); ok {
		t.Error("FromError found a CodedError in an uncoded chain")
	}
	if _, ok := FromError(nil); ok {
		t.Error("FromError(nil) reported a CodedError")
	}
}

// TestExitCodeByClass pins the exit-code mapping: refusal-class codes
// exit ExitRefusal, runtime-class codes keep the traditional
// ExitFailure, and an unregistered code degrades to ExitFailure. Every
// REGISTERED code is exercised (not one representative per class) so a
// registry edit that flips a class shows up here.
func TestExitCodeByClass(t *testing.T) {
	for _, c := range All() {
		info, _ := Describe(c)
		want := ExitFailure
		if info.Class == ClassRefusal {
			want = ExitRefusal
		}
		ce := &CodedError{Code: c, Err: errors.New("x")}
		if got := ce.ExitCode(); got != want {
			t.Errorf("%s: ExitCode() = %d; want %d (class %d)", c, got, want, info.Class)
		}
	}
	unregistered := &CodedError{Code: Code("SLUICE-E-NOT-A-CODE"), Err: errors.New("x")}
	if got := unregistered.ExitCode(); got != ExitFailure {
		t.Errorf("unregistered code ExitCode() = %d; want %d", got, ExitFailure)
	}
}

// TestConfigErrorExit pins the config-error exit shape: exit 2, message
// delegation, and Unwrap traversal.
func TestConfigErrorExit(t *testing.T) {
	inner := errors.New("config: load \"x.yaml\": no such file")
	ce := &ConfigError{Err: inner}
	if got := ce.ExitCode(); got != ExitConfig {
		t.Errorf("ExitCode() = %d; want %d", got, ExitConfig)
	}
	if ce.Error() != inner.Error() {
		t.Errorf("Error() = %q; want delegation to %q", ce.Error(), inner.Error())
	}
	if !errors.Is(ce, inner) {
		t.Error("errors.Is must traverse ConfigError.Unwrap")
	}
}

// TestAttrs pins the slog-attr helper: a coded chain yields exactly the
// code and hint attrs, an uncoded chain yields nil (so call sites can
// append unconditionally).
func TestAttrs(t *testing.T) {
	coded := fmt.Errorf("outer: %w", Wrap(CodeValueNULByte, "use --type-override", errors.New("NUL byte")))
	attrs := Attrs(coded)
	if len(attrs) != 2 {
		t.Fatalf("Attrs = %d entries; want 2", len(attrs))
	}
	code, ok := attrs[0].(slog.Attr)
	if !ok || code.Key != "code" || code.Value.String() != string(CodeValueNULByte) {
		t.Errorf("attrs[0] = %v; want code=%s", attrs[0], CodeValueNULByte)
	}
	hint, ok := attrs[1].(slog.Attr)
	if !ok || hint.Key != "hint" || hint.Value.String() != "use --type-override" {
		t.Errorf("attrs[1] = %v; want hint=%q", attrs[1], "use --type-override")
	}
	if got := Attrs(errors.New("plain")); got != nil {
		t.Errorf("Attrs(uncoded) = %v; want nil", got)
	}
}

// TestRegistryDocSync_TableIsContiguous is the RENDERING gate (audit
// 2026-08-05 C-15). A single blank line inside a GitHub-flavoured Markdown
// table ends the table: every row after it renders as raw pipe-delimited text.
// One had crept in after row 28, so 48 of the 74 documented codes were
// unreadable on the published docs — and BOTH existing doc gates stayed green,
// because each parses the file LINE BY LINE and never asks whether the lines
// form one table.
//
// That is the "gate narrower than its name" shape: two checks named for
// doc/registry synchronisation, neither of which could see that the document
// was broken. This asserts the property the other two assume — every
// registered code's row lives in the SAME uninterrupted block as the header.
func TestRegistryDocSync_TableIsContiguous(t *testing.T) {
	docPath := filepath.Join("..", "..", "docs", "operator", "error-codes.md")
	raw, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("read %s: %v", docPath, err)
	}
	lines := strings.Split(strings.ReplaceAll(string(raw), "\r\n", "\n"), "\n")

	start := -1
	for i, l := range lines {
		if strings.HasPrefix(l, "| Code | Class | Meaning | Remedy |") {
			start = i
			break
		}
	}
	if start < 0 {
		t.Fatal("could not find the error-code table header — this gate is vacuous; fix the locator")
	}
	end := start
	for end+1 < len(lines) && strings.HasPrefix(lines[end+1], "|") {
		end++
	}
	inTable := map[Code]bool{}
	codeRe := regexp.MustCompile(`^\| ` + "`" + `(SLUICE-E-[A-Z0-9-]+)` + "`")
	for _, l := range lines[start : end+1] {
		if m := codeRe.FindStringSubmatch(l); m != nil {
			inTable[Code(m[1])] = true
		}
	}

	if len(inTable) == 0 {
		t.Fatal("found no code rows in the table block — the locator no longer matches the doc")
	}
	var stranded []string
	for _, c := range All() {
		if !inTable[c] {
			stranded = append(stranded, string(c))
		}
	}
	if len(stranded) > 0 {
		t.Errorf("%d of %d registered codes sit OUTSIDE the contiguous table block that begins at "+
			"line %d and ends at line %d: %v\n\n"+
			"A blank line inside a GFM table ends it, so every row past the break renders as raw "+
			"text on the published docs. The other doc gates parse line-by-line and cannot see this.",
			len(stranded), len(All()), start+1, end+1, stranded)
	}
}

// TestRegistryDocSync enforces the docs/operator/error-codes.md table
// against the registry IN BOTH DIRECTIONS: every registered code has a
// doc row, and every SLUICE-E-… token in the doc is a registered code.
// This is the run-filter-guard lesson — a doc that must stay in sync
// with code gets a test, not a convention.
func TestRegistryDocSync(t *testing.T) {
	docPath := filepath.Join("..", "..", "docs", "operator", "error-codes.md")
	raw, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("read %s: %v", docPath, err)
	}
	doc := string(raw)

	documented := map[Code]bool{}
	for _, m := range regexp.MustCompile(`SLUICE-E-[A-Z0-9-]+`).FindAllString(doc, -1) {
		documented[Code(m)] = true
	}

	for _, c := range All() {
		if !documented[c] {
			t.Errorf("registered code %s has no row in %s", c, docPath)
		}
	}
	for c := range documented {
		if _, ok := Describe(c); !ok {
			t.Errorf("%s documents %s, which is not a registered code", docPath, c)
		}
	}
}

// splitDocCells splits a markdown table row on unescaped pipes (the doc
// escapes in-cell pipes as `\|`; the escape is KEPT in the cell text,
// matching how docRows stores it).
func splitDocCells(line string) []string {
	var cells []string
	var cur strings.Builder
	escaped := false
	for _, r := range line {
		switch {
		case escaped:
			cur.WriteRune('\\')
			cur.WriteRune(r)
			escaped = false
		case r == '\\':
			escaped = true
		case r == '|':
			cells = append(cells, cur.String())
			cur.Reset()
		default:
			cur.WriteRune(r)
		}
	}
	return append(cells, cur.String())
}

// normalizeDocCell collapses runs of whitespace so incidental reflow in
// either home never fails the equality gate — only content changes do.
func normalizeDocCell(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// TestRegistryDocSync_ContentEquality is the G-15 gate (audit 2026-07-23
// DEVEX-2): the token-presence check above passes forever on a row whose
// PROSE lags a semantics change — error-codes row 29 kept prescribing the
// scope guard's pre-v0.99.289 "drain the other stream" remedy one release
// after the existence-semantics ratchet made it insufficient. This test
// pins the doc table's Meaning/Remedy cells byte-for-byte (whitespace-
// normalized) against the in-package docRows mirror, and the Class cell
// against the registry Class — so editing the doc, the mirror, or a
// code's class alone fails CI until the two homes agree again.
func TestRegistryDocSync_ContentEquality(t *testing.T) {
	docPath := filepath.Join("..", "..", "docs", "operator", "error-codes.md")
	raw, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("read %s: %v", docPath, err)
	}

	type docCells struct{ class, meaning, remedy string }
	fromDoc := map[Code]docCells{}
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, "| `SLUICE-E-") {
			continue
		}
		cells := splitDocCells(line)
		if len(cells) < 5 {
			t.Errorf("table row has %d cells, want >= 5: %s", len(cells), line)
			continue
		}
		code := Code(strings.Trim(strings.TrimSpace(cells[1]), "`"))
		if _, dup := fromDoc[code]; dup {
			t.Errorf("%s has two table rows in %s", code, docPath)
		}
		fromDoc[code] = docCells{
			class:   strings.TrimSpace(cells[2]),
			meaning: strings.TrimSpace(cells[3]),
			remedy:  strings.TrimSpace(cells[4]),
		}
	}
	// Vacuity guard: the table has ~58 rows; near-empty parse = broken parse.
	if len(fromDoc) < 40 {
		t.Fatalf("parsed only %d code rows from %s — the table shape changed; fix the parser", len(fromDoc), docPath)
	}

	classWord := func(c Class) string {
		if c == ClassRefusal {
			return "refusal"
		}
		return "runtime"
	}

	for _, c := range All() {
		info, _ := Describe(c)
		doc, inDoc := fromDoc[c]
		mirror, inMirror := docRows[c]
		if !inDoc {
			t.Errorf("registered code %s has no table row in %s", c, docPath)
			continue
		}
		if !inMirror {
			t.Errorf("registered code %s has no docRows mirror entry (internal/sluicecode/docrows.go) — add the row's Meaning/Remedy there", c)
			continue
		}
		if doc.class != classWord(info.Class) {
			t.Errorf("%s: doc Class cell says %q but the registry class is %q — reconcile", c, doc.class, classWord(info.Class))
		}
		if got, want := normalizeDocCell(doc.meaning), normalizeDocCell(mirror.Meaning); got != want {
			t.Errorf("%s: doc Meaning cell diverged from the docRows mirror — update BOTH homes together (audit 2026-07-23 G-15)\n  doc:    %s\n  mirror: %s", c, got, want)
		}
		if got, want := normalizeDocCell(doc.remedy), normalizeDocCell(mirror.Remedy); got != want {
			t.Errorf("%s: doc Remedy cell diverged from the docRows mirror — update BOTH homes together (audit 2026-07-23 G-15)\n  doc:    %s\n  mirror: %s", c, got, want)
		}
	}

	// Parity in the remaining directions: no orphan doc rows (already
	// covered token-wise above, but keep this loop self-contained) and no
	// orphan mirror entries.
	for c := range fromDoc {
		if _, ok := Describe(c); !ok {
			t.Errorf("%s has a table row but is not a registered code", c)
		}
	}
	for c := range docRows {
		if _, ok := Describe(c); !ok {
			t.Errorf("docRows mirrors %s, which is not a registered code — drop the stale entry", c)
		}
	}
}

// remedyOpenerVerbs is the closed VOCABULARY of imperative verbs this
// registry's remedies open with when the fix is genuinely prose ("Add a
// primary key", "Upgrade sluice") rather than an artifact worth quoting.
// It is a vocabulary, not a taxonomy: extending it because an honest new
// remedy opens with a verb it lacks is the intended maintenance —
// suppressing the gate that consults it is not.
var remedyOpenerVerbs = map[string]bool{
	"add": true, "alter": true, "apply": true, "bootstrap": true, "check": true,
	"choose": true, "correct": true, "declare": true, "delete": true, "disable": true,
	"drop": true, "enable": true, "exclude": true, "filter": true, "fix": true,
	"free": true, "give": true, "grant": true, "inspect": true, "install": true,
	"normalize": true, "pass": true, "point": true, "raise": true, "rebuild": true,
	"recreate": true, "re-create": true, "re-run": true, "remove": true, "rename": true,
	"rerun": true, "restore": true, "rewrite": true, "run": true, "set": true,
	"supply": true, "take": true, "update": true, "upgrade": true, "use": true,
	"verify": true, "wait": true, "widen": true, "write": true,
}

// remedyOpener returns a remedy's first word, lowercased and stripped of
// the markdown a doc cell carries (backticks, emphasis, punctuation), so
// the opener test reads the WORD rather than its formatting.
func remedyOpener(remedy string) string {
	fields := strings.Fields(remedy)
	if len(fields) == 0 {
		return ""
	}
	return strings.ToLower(strings.Trim(fields[0], "`*_\"'(,.:;"))
}

// remedyIsActionable is what "actionable" means MECHANICALLY here, and
// the bar is deliberately a floor rather than a judgement:
//
//   - non-empty and at least one sentence long — a bare "None." or a
//     "TBD" placeholder is not a remedy; and
//   - it names a concrete next step, approximated as EITHER a backticked
//     artifact (a flag, a command, a SQL statement, a setting — which is
//     how 46 of the 50 refusal rows say what to do) OR an imperative
//     opener from [remedyOpenerVerbs] (which is how the other four say
//     it, e.g. "Add a primary key to the table").
//
// What it cannot check is whether the advice is CORRECT — that stays a
// human review. What it makes impossible is the class items 91, 96 and
// 100 hit three times: shipping a refusal whose human-readable half is
// right and whose actionable half is absent.
//
// The opener test rather than an anywhere-match is the deliberate part:
// explanatory prose ("The chain cannot be restored; this is a known
// limitation") mentions actions constantly but rarely OPENS with an
// imperative, so matching the opener keeps the gate from going vacuous.
func remedyIsActionable(remedy string) bool {
	trimmed := strings.TrimSpace(remedy)
	if len(trimmed) < 30 {
		return false
	}
	if strings.Count(trimmed, "`") >= 2 {
		return true
	}
	return remedyOpenerVerbs[remedyOpener(trimmed)]
}

// TestRefusalCodesCarryAnActionableRemedy is the item-100 RATCHET, and it
// exists because that item is the THIRD instance of one class: an
// operator-facing refusal whose human-readable half is right and whose
// ACTIONABLE half is missing. Item 91 (Bug 213) reported the wrong code
// and exit status, item 96 had no code at all, and item 100 had both and
// no remedy — a correct, coded, exit-3 refusal on the most ordinary
// `backup prune` invocation there is, under prose that told the operator
// neither whether their backups were broken nor what to run instead.
//
// A refusal is sluice's loud-failure tenet made machine-readable, and the
// tenet is only half-kept if the loud failure has no way out. So: every
// ClassRefusal code must mirror a remedy in [docRows] (which
// TestRegistryDocSync_ContentEquality holds equal to the operator table,
// so this gates BOTH homes at once) and that remedy must be actionable by
// [remedyIsActionable]'s definition.
//
// Runtime-class codes are deliberately out of scope: they name a failure
// the operator did not ask sluice to prevent, and several are honestly
// "the database said no" — the refusal contract is what promises a way
// forward.
//
// One exemption, and it is mechanical rather than a suppression: a code
// whose registry summary carries [retainedButUnemittedMarker] is a
// LIFTED refusal kept registered only because removing a published code
// is breaking. It cannot fire, so it has no remedy to give, and the
// marker is already the registry's own machine-readable statement of
// that.
func TestRefusalCodesCarryAnActionableRemedy(t *testing.T) {
	checked := 0
	for _, c := range All() {
		info, _ := Describe(c)
		if info.Class != ClassRefusal {
			continue
		}
		if strings.Contains(info.Summary, retainedButUnemittedMarker) {
			t.Logf("%s: skipped — %s (a lifted refusal has no remedy to give)", c, retainedButUnemittedMarker)
			continue
		}
		checked++
		row, ok := docRows[c]
		if !ok {
			t.Errorf("refusal code %s has no docRows entry — a refusal with no remedy is half a loud failure", c)
			continue
		}
		if !remedyIsActionable(row.Remedy) {
			t.Errorf("refusal code %s has no ACTIONABLE remedy (roadmap items 91/96/100 are this class three times over).\n"+
				"  A refusal must name a next step: a backticked flag/command/statement, or an imperative opener from remedyOpenerVerbs.\n"+
				"  got: %q", c, row.Remedy)
		}
	}
	// Vacuity guard: the registry carries ~50 refusal codes, so a
	// near-empty sweep means the class filter (or All()) broke, not that
	// the codebase got tidy.
	if checked < 40 {
		t.Fatalf("checked only %d refusal codes; the sweep is not reaching the registry", checked)
	}
}

// TestRemedyActionablePredicateHasTeeth pins [remedyIsActionable] against
// synthetic cases rather than against the corpus, because a predicate
// validated only by "every existing row passes" is indistinguishable from
// one that returns true. The negatives are the shapes the three filed
// items actually produced or would have.
func TestRemedyActionablePredicateHasTeeth(t *testing.T) {
	cases := []struct {
		name   string
		remedy string
		want   bool
	}{
		{"empty", "", false},
		{"placeholder", "TBD", false},
		{"bare none", "None.", false},
		{
			"explanation with no next step",
			"The chain cannot be restored in this state; this is a known limitation of the retention model and is tracked on the roadmap.",
			false,
		},
		{
			"names the shape but not the way out (the item-100 shape)",
			"A within-segment incremental trim severs the chain, so the readability gate refuses the prune before anything is deleted.",
			false,
		},
		{"backticked flag", "Re-run the migration with `--resume` once the target has room.", true},
		{"backticked command", "Free a leftover slot with `sluice slot drop <name>`, then re-run the sync.", true},
		{"imperative opener, no markdown", "Add a primary key to the table (or fix the non-orderable key), then re-run.", true},
		{"imperative opener with emphasis", "**Upgrade** sluice to a build that supports the backup's signature scheme, then retry.", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := remedyIsActionable(tc.remedy); got != tc.want {
				t.Errorf("remedyIsActionable(%q) = %v; want %v", tc.remedy, got, tc.want)
			}
		})
	}
}

// retainedButUnemittedMarker is the sentinel a registry summary carries
// when a code's refusal has been LIFTED but the string is kept registered
// (removing a published catalog code is breaking). It couples the registry
// prose to the doc prose in TestRegistryDocSync_RetainedProse.
const retainedButUnemittedMarker = "RETAINED-BUT-UNEMITTED"

// TestRegistryDocSync_RetainedProse extends the token-only sync check
// (TestRegistryDocSync) to compare row PROSE against the registry — the F7
// (audit 2026-07-17) gate. TestRegistryDocSync passes forever on a row that
// still describes an UNEMITTED code as an active refusal, because it only
// checks the SLUICE-E-… token is present, never that the prose matches the
// shipped status — exactly the drift error-codes.md rows 29-30 exhibited
// (MariaDB CDC "not supported yet" long after CDC shipped). This test pins
// the retained-but-unemitted class: any code whose registry summary carries
// the [retainedButUnemittedMarker] must have a doc row that also flags it as
// retained/no-longer-emitted, so stale ACTIVE-refusal prose fails CI.
func TestRegistryDocSync_RetainedProse(t *testing.T) {
	docPath := filepath.Join("..", "..", "docs", "operator", "error-codes.md")
	raw, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("read %s: %v", docPath, err)
	}
	// Index each doc row by the first code token on its line (each code
	// occupies exactly one table row).
	codeRe := regexp.MustCompile(`SLUICE-E-[A-Z0-9-]+`)
	rowFor := map[Code]string{}
	for _, line := range strings.Split(string(raw), "\n") {
		if m := codeRe.FindString(line); m != "" {
			if _, seen := rowFor[Code(m)]; !seen {
				rowFor[Code(m)] = line
			}
		}
	}

	sawRetained := false
	for c, info := range registry {
		if !strings.Contains(info.Summary, retainedButUnemittedMarker) {
			continue
		}
		sawRetained = true
		row, ok := rowFor[c]
		if !ok {
			t.Errorf("%s is %s in the registry but has no doc row", c, retainedButUnemittedMarker)
			continue
		}
		up := strings.ToUpper(row)
		if !strings.Contains(up, "RETAINED") && !strings.Contains(up, "NO LONGER EMITTED") {
			t.Errorf("%s summary is %s but its error-codes.md row does not flag it retained/no-longer-emitted "+
				"(prose lags the shipped status — the F7 stale-active-refusal class): %s",
				c, retainedButUnemittedMarker, row)
		}
	}
	if !sawRetained {
		t.Logf("no %s codes in the registry (nothing to cross-check) — fine, the guard is a no-op", retainedButUnemittedMarker)
	}
}
