// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

// Engine-supplied collation resolution (audit 2026-07-18 M2.1 / M-A1).
//
// A continuous FILTERED sync (`sync --where`, ADR-0173 Phase 2) evaluates the
// predicate CLIENT-SIDE per CDC change, so a string comparison must reproduce
// the SOURCE's own `=` — which is collation-dependent. Reproducing MySQL's
// case/accent folding needs the Vitess collation library; a byte-exact compare
// is right only for a deterministic collation. Rather than hard-wire a MySQL
// collation library into the engine-neutral evaluator (the M-A1 leak: the
// evaluator was the ONLY importer of vitess.io/vitess outside
// internal/engines/mysql), the SOURCE ENGINE supplies a [CollationResolver]:
// MySQL backs it with Vitess's comparator, Postgres/SQLite resolve
// byte-exact-or-refuse through their own determinism lens. The evaluator
// consumes only this interface, so the orchestrator carries no compile-time
// edge to any engine's collation library.

// CollationResolver decides how a string column under a given collation must be
// compared client-side to faithfully reproduce the source engine's own `=`. It
// is supplied by the source engine (see [CollationResolverProvider]) and
// consumed by the row-predicate evaluator; the evaluator never reasons about
// collation names itself.
type CollationResolver interface {
	// ResolveStringEquality returns the client-side comparison policy for a
	// string column with the given collation. determinism is the IR's
	// collation-determinism signal (meaningful for Postgres named collations;
	// engines that don't use it ignore it). strict is the operator's
	// --where-strict-collation opt-out: when set, an engine that would
	// otherwise reproduce a case/accent-INSENSITIVE `=` via a folding
	// comparator must instead refuse it (byte-exact and default remain
	// allowed). fixedChar is true for a fixed-length CHAR/bpchar column, whose
	// `=` can have PAD-SPACE semantics distinct from a variable-length
	// text/varchar of the same collation (Postgres bpchar `=` ignores trailing
	// spaces regardless of collation — audit 2026-07-19 A2); a resolver whose
	// char `=` matches its varchar `=` ignores it. A non-faithful result
	// ([StringEquality.Faithful] == false) means the comparison must be REFUSED
	// loudly at compile time rather than evaluated under a guessed collation.
	ResolveStringEquality(collation string, determinism CollationDeterminism, strict, fixedChar bool) StringEquality
}

// StringEquality is the resolved client-side string-comparison policy for one
// column, returned by a [CollationResolver].
type StringEquality struct {
	// Faithful reports whether the source's `=` can be reproduced client-side
	// at all. When false the caller REFUSES the comparison (an unresolvable /
	// non-UTF-8 / non-deterministic collation, or --where-strict-collation on a
	// fold path); the other fields are then meaningless.
	Faithful bool
	// PadSpace, when true, tells the evaluator to right-trim trailing ASCII
	// spaces from BOTH operands before comparing — reproducing the MySQL legacy
	// PAD SPACE `=` (trailing spaces ignored). Vitess's comparator is NO-PAD
	// regardless of the collation's real pad attribute (audit F0-1/F0-2), so
	// the trim is applied by the evaluator around whatever [Compare] does.
	// False on NO-PAD collations and on engines (Postgres) whose `=` is
	// trailing-space-significant.
	PadSpace bool
	// Compare reproduces the source's `=` for two non-NULL string values. A nil
	// Compare means the comparison is BYTE-EXACT (`a == b`, after any PadSpace
	// trim); a non-nil Compare is a collation-aware comparator (MySQL ci/ai via
	// Vitess). Consulted only when Faithful.
	Compare func(a, b string) bool
}

// CollationResolverProvider is the optional Engine surface a source engine
// implements to supply its [CollationResolver] to the filtered-sync
// orchestrator. MySQL provides a Vitess-backed resolver; Postgres provides
// [ByteExactCollationResolver]. An engine that does NOT implement it cannot be
// the source of a continuous filtered sync (the pipeline refuses loudly rather
// than compare under a guessed collation).
type CollationResolverProvider interface {
	CollationResolver() CollationResolver
}

// ByteExactCollationResolver is the [CollationResolver] for engines whose
// string `=` is BYTE-EXACT for the default collation and for any DETERMINISTIC
// named collation, and collation-aware (NOT client-reproducible) for a
// NON-deterministic or unknown-determinism named collation — the Postgres /
// SQLite family lens. It reproduces the pre-refactor non-MySQL branch of the
// row-predicate evaluator exactly: a deterministic collation's `=` is byte
// equality (libc C/POSIX/en_US, deterministic ICU — collisdeterministic=true),
// so a byte compare reproduces it; a non-deterministic ICU collation
// (collisdeterministic=false) or an unknown-determinism named collation (the
// safe zero value) has a collation-aware `=` sluice cannot reproduce, so it
// refuses (audit F0-3). It carries no PAD SPACE handling: text/varchar `=` is
// trailing-space significant, and this GENERIC byte-exact lens stays
// pad-agnostic so a family whose fixed-length CHAR does NOT pad (e.g. SQLite)
// is correct. An engine whose CHAR/bpchar DOES pad (Postgres) layers that on in
// its own resolver (see engines/postgres) rather than here (audit 2026-07-19 A2).
type ByteExactCollationResolver struct{}

// ResolveStringEquality implements [CollationResolver] for the byte-exact
// (Postgres/SQLite) family. strict is ignored (byte-exact IS the strict
// guarantee, and there is no fold path to opt out of); fixedChar is ignored
// here (the generic byte-exact lens is pad-agnostic — a padding CHAR is an
// engine-specific override layered by the engine's own resolver).
func (ByteExactCollationResolver) ResolveStringEquality(collation string, determinism CollationDeterminism, _, _ bool) StringEquality {
	if collation == "" || determinism == CollationDeterministic {
		return StringEquality{Faithful: true} // byte-exact, no pad-space
	}
	return StringEquality{} // non-deterministic / unknown named collation -> refuse
}
