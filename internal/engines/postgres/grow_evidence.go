// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # What a grow-gate trip actually observed (roadmap item 143), PG side
//
// The Postgres twin of internal/engines/mysql/grow_evidence.go; that file
// carries the full reasoning and it applies here unchanged. In short:
// [classifyApplierError] decides retriability and is deliberately broad;
// this file decides what the ADR-0110 grow-gate trip may CLAIM to have seen,
// and it under-claims by rule, because asserting a cause it had no evidence
// for is the defect item 143 exists to remove.
//
// PG-specific notes on the split, since the SQLSTATE space is not MySQL's:
//
//   - 53100 disk_full IS the storage-grow boundary being hit — "could not
//     extend file … No space left on device" mid-COPY is the live finding #94
//     face and it names the target's storage directly.
//   - 53000 (insufficient_resources, the class root) and 53200 (out_of_memory)
//     are retriable siblings of 53100 but are NOT storage evidence: 53200 is
//     memory, and 53000 is the generic class code. Grouped with 53100 for
//     RETRY by shape; separated here because the label is about what was said.
//   - The XX000 `cluster is read-only` / `pg_readonly` wording is the PG twin
//     of MySQL's 1290 read-only face: a serving transition, in the target's
//     own words.
//   - 57P01/57P02/57P03 (admin_shutdown / crash_shutdown / cannot_connect_now)
//     are server-STATE, not transport — the server answered and said it is
//     going away or not accepting connections yet, which is what a PG
//     promotion looks like from the client. Counted as evidence, with the
//     honest caveat that an ordinary restart produces them too; that is the
//     weakest cell in this file and it is stated rather than hidden.
//   - Class 08 (connection_exception) is NOT evidence. It is the generic
//     "the connection broke" class and it is exactly the population item 143
//     is about. The shared [nettransient.IsConnectionAvailabilitySQLState]
//     predicate covers 57P0x AND class 08 together — correct for retry,
//     too coarse for a claim — so this file reads the codes it means.
//   - Everything reaching the transport legs ([nettransient.IsTransientShape],
//     bad-conn / EOF sentinels, pgconn.SafeToRetry) is [ir.GrowEvidenceNone]
//     by construction. So are 40001/40P01 (contention) and 42703/42P01
//     (schema drift).

package postgres

import (
	"errors"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/ir"
)

// pgReadOnlyClusterSubstrings is the lower-cased wording that marks a generic
// XX000 as the PlanetScale-PG serving-transition read-only window. Declared
// once and read by BOTH [classifyApplierError] (where it decides retriability)
// and [growEvidenceOf] (where it decides the claim), so the two cannot drift —
// [TestGrowEvidence_SharesTheReadOnlyPredicateWithTheClassifier] is what fails
// if a second spelling appears in one of them.
var pgReadOnlyClusterSubstrings = []string{
	"cluster is read-only",
	"pg_readonly",
}

// isPGReadOnlyClusterMessage reports whether msg carries the read-only
// serving-transition wording. Case-insensitive.
func isPGReadOnlyClusterMessage(msg string) bool {
	lower := strings.ToLower(msg)
	for _, sub := range pgReadOnlyClusterSubstrings {
		if strings.Contains(lower, sub) {
			return true
		}
	}
	return false
}

// pgServingTransitionSQLStates are the SQLSTATEs whose meaning is the SERVER's
// own state rather than the connection's. See the file header for why class 08
// is deliberately absent.
var pgServingTransitionSQLStates = []string{"57P01", "57P02", "57P03"}

// growEvidenceOf reports whether err carries positive storage-grow /
// serving-transition evidence, for the ADR-0110 grow-gate trip's structured
// log. A pure verdict: it never decides retriability and never changes what
// the gate does. Safe on a terminal error (reads as the zero value).
func growEvidenceOf(err error) ir.GrowEvidence {
	if err == nil {
		return ir.GrowEvidenceNone
	}

	// Structured server response ⇒ the SQLSTATE decides, and only XX000 (a
	// generic catch-all whose semantics are message-dependent) may consult the
	// message — the same shield discipline [classifyApplierError] applies.
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if pgErr.Code == "53100" {
			return ir.GrowEvidenceTargetFace
		}
		if pgErr.Code == "XX000" && isPGReadOnlyClusterMessage(pgErr.Message) {
			return ir.GrowEvidenceTargetFace
		}
		for _, code := range pgServingTransitionSQLStates {
			if pgErr.Code == code {
				return ir.GrowEvidenceTargetFace
			}
		}
		return ir.GrowEvidenceNone
	}

	// No structured error: the PG server-lifecycle wordings are the
	// unstructured face of 57P01/57P03 and carry the same meaning.
	if msg := err.Error(); msg != "" {
		if strings.Contains(msg, "the database system is starting up") ||
			strings.Contains(msg, "the database system is shutting down") {
			return ir.GrowEvidenceTargetFace
		}
		if isPGReadOnlyClusterMessage(msg) {
			return ir.GrowEvidenceTargetFace
		}
	}
	return ir.GrowEvidenceNone
}
