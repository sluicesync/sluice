// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package refusalgate holds the checkable property behind Bug 235: a
// refusal's SHORT field must not prescribe a remedy its own LONG field
// rules out.
//
// # Why this is a gate and not a fixed string
//
// sluice's coded refusals carry two operator-facing texts — a `hint=`
// naming what to do, and an `err=` body explaining what happened. The
// body is where the reasoning goes, so the body is where a shape's
// exceptions get written; the hint is short, shared across shapes, and
// is what log pipelines and terse readers surface. That asymmetry has a
// standing failure mode: a NEW shape is routed through an EXISTING code,
// its body is written correctly for the new shape, and the shared hint
// is never re-derived. The operator then reads an actionable sentence
// the accurate one denies.
//
// It has happened twice. Bug 231's adoption refusal printed
// `deployment_state "in_progress"` beside "Nothing is deploying, so the
// branch is safe to delete"; its fix built a gate rather than editing
// the string, and that gate — a claim graded against the state that
// would establish it — is the shape this package generalises. Bug 235
// is the same defect at the hint layer: the generated-identity replica
// -identity refusal prescribed `ALTER TABLE … REPLICA IDENTITY FULL`
// while its body said, twice, that FULL is not a fix for that shape.
//
// # What it checks, stated narrowly
//
// [Contradictions] extracts the REMEDIES a hint prescribes — SQL-shaped
// ALL-CAPS phrases and `--flag` tokens, the two vocabularies sluice's
// hints actually use — and reports the ones the body DENIES within a
// short window. It is deliberately conservative in both directions:
//
//   - It does not parse prose. A body that rules a remedy out in some
//     other wording is not caught, which is why [DenialCues] is a named,
//     extendable list rather than a heuristic.
//   - It is scoped to a window, so a denial of some OTHER remedy that
//     merely appears later in the same paragraph is not attributed here.
//
// So a clean result is not proof that a message is coherent; it is proof
// that it does not contain the specific self-contradiction that has now
// shipped twice. That is worth having and it is not worth overstating.
//
// # Who applies it (the roster this package does NOT enumerate for you)
//
// This package is a pure function; it grades only what a caller hands
// it. The callers today:
//
//   - internal/engines/postgres —
//     TestReplicaIdentityRefusalHintNeverContradictsItsBody, over every
//     shape [errUnusableReplicaIdentity] can render.
//
// Deliberately NOT wired, with reasons, so the absence is a decision and
// not an oversight:
//
//   - internal/pipeline/blobcodec's ErrChunkLineTooLong ("splitting the
//     table does not help") carries no sluicecode hint at all — there is
//     no short field to contradict. If one is added, wire it.
//   - internal/sluicecode's static `Remedy` rows (e.g. the float code's
//     "a target-side --type-override to DOUBLE does NOT help") state a
//     denial INSIDE the remedy text itself, which is the coherent
//     shape — a hint may rule a remedy out; it may not prescribe one the
//     body rules out.
package refusalgate

import "strings"

// DenialCues are the phrasings a sluice refusal body uses to rule a
// remedy out. Extend this list when a new one is written — an
// unrecognised denial is a gate that silently stops grading, which is
// exactly the failure mode this package exists to prevent, so prefer
// reusing one of these spellings in new prose.
//
// Matched case-insensitively; the shouty and quiet spellings of the same
// sentence are the same denial.
var DenialCues = []string{
	"is not a fix",
	"are not a fix",
	"not a fix",
	"does not fix",
	"do not fix",
	"is not sufficient",
	"not sufficient",
	"does not help",
	"do not help",
	"will not help",
	"is not a remedy",
	"not a remedy",
	"cannot fix",
	"has no effect",
}

// denialWindow is how far after a remedy phrase a denial cue may start
// and still be attributed to it, and clauseBreaks end the window early.
//
// A denial belongs to the CLAUSE it is written in, so the window stops
// at a sentence or clause boundary regardless of how much of the byte
// budget is left. Both halves were forced by measured text:
//
//   - the byte cap separates `REPLICA IDENTITY FULL is NOT a fix here`
//     (a 1-byte gap — caught) from an `immediate NOT NULL UNIQUE index
//     "…"` mentioned ~120 bytes before an unrelated denial in the same
//     sentence (not attributed);
//   - the clause break separates `… take it out of scope with
//     --exclude-table. REPLICA IDENTITY FULL is NOT a fix for these
//     tables` — where the byte cap alone attributed the FULL denial to
//     the flag one sentence earlier. The first run of the postgres gate
//     produced exactly that false positive, which is the argument for
//     writing the denial in the same clause as the remedy it denies.
//
// A false positive on a gate like this gets the gate suppressed, so the
// bias is toward missing a denial rather than inventing one.
const denialWindow = 48

// clauseBreaks end a denial window early. Written as the punctuation
// followed by a space so a decimal point or an abbreviation inside a
// clause does not split it.
var clauseBreaks = []string{". ", "; ", "\n"}

// Contradictions returns each remedy the hint prescribes that the body
// denies, in the order they appear in the hint. Empty means clean.
func Contradictions(body, hint string) []string {
	lowerBody := strings.ToLower(body)
	var out []string
	seen := make(map[string]bool)
	for _, phrase := range Remedies(hint) {
		if seen[phrase] {
			continue
		}
		seen[phrase] = true
		if deniedIn(lowerBody, strings.ToLower(phrase)) {
			out = append(out, phrase)
		}
	}
	return out
}

// Remedies extracts the remedy phrases a hint prescribes: maximal runs
// of two or more consecutive SQL-shaped ALL-CAPS words (`REPLICA
// IDENTITY FULL`, `ALTER TABLE`, `WITHOUT DEFERRABLE`) plus `--flag`
// tokens.
//
// Two words minimum on the caps side, because a lone `NOT` or `INDEX`
// carries no remedy and would match half the corpus. Flags are single
// tokens because that is how they are written.
func Remedies(hint string) []string {
	var (
		out []string
		run []string
	)
	flush := func() {
		if len(run) >= 2 {
			out = append(out, strings.Join(run, " "))
		}
		run = run[:0]
	}
	for _, field := range strings.Fields(hint) {
		word := strings.Trim(field, "`'\"“”‘’(),.;:—–…?!")
		switch {
		case isFlagToken(word):
			flush()
			out = append(out, word)
		case isSQLCapsWord(word):
			run = append(run, word)
		default:
			flush()
		}
	}
	flush()
	return out
}

// isFlagToken reports whether word is a `--long-flag`.
func isFlagToken(word string) bool {
	if !strings.HasPrefix(word, "--") || len(word) < 4 {
		return false
	}
	for _, r := range word[2:] {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-':
		default:
			return false
		}
	}
	return true
}

// isSQLCapsWord reports whether word is an ALL-CAPS SQL keyword shape:
// at least two characters, at least one uppercase letter, and nothing
// but uppercase letters, digits, underscores and hyphens.
func isSQLCapsWord(word string) bool {
	if len(word) < 2 {
		return false
	}
	letters := 0
	for _, r := range word {
		switch {
		case r >= 'A' && r <= 'Z':
			letters++
		case (r >= '0' && r <= '9') || r == '_' || r == '-':
		default:
			return false
		}
	}
	return letters > 0
}

// deniedIn reports whether body denies phrase — a denial cue starting
// within [denialWindow] bytes of some occurrence of it. Both arguments
// are already lowercased by the caller.
func deniedIn(body, phrase string) bool {
	from := 0
	for {
		i := strings.Index(body[from:], phrase)
		if i < 0 {
			return false
		}
		start := from + i + len(phrase)
		window := body[start:min(start+denialWindow, len(body))]
		for _, brk := range clauseBreaks {
			if j := strings.Index(window, brk); j >= 0 {
				window = window[:j]
			}
		}
		for _, cue := range DenialCues {
			if strings.Contains(window, cue) {
				return true
			}
		}
		from = start
	}
}
