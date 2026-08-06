// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # What a grow-gate trip actually observed (roadmap item 143)
//
// [classifyApplierError] answers ONE question — "should this lane retry?" —
// and its answer is deliberately broad: the whole transient class, transport
// losses included, because a lane that cannot retry a dropped connection dies
// on a target that was merely momentarily unreachable. That breadth is right
// and item 143 does not touch it (2,687 drops absorbed with zero unhandled in
// the field is the evidence that it works).
//
// This file answers a SECOND, narrower question that was previously answered
// by assertion: does the error the lane is tripping the ADR-0110 grow gate on
// carry any actual storage-grow / serving-transition evidence? The shipped
// WARN said "likely a primary reparent / 'not serving'" for every trip. Two
// independent datasets say that is almost always false — see [ir.GrowEvidence]
// for both.
//
// # The rule this file follows
//
// UNDER-claim. A face lands in [ir.GrowEvidenceTargetFace] only when the
// target itself said something that names its own storage or serving state:
// it is out of disk, it is read-only, no primary is serving, a reparent or
// resharding is in progress. Everything a grow merely CAUSES — a killed
// query, a lock-wait timeout, a throttler rejection, a dropped connection —
// stays [ir.GrowEvidenceNone], because each of those is produced just as
// readily by an ordinary busy target and a label that fires on both tells the
// next reader nothing. Over-claiming is the exact defect item 143 exists to
// remove, so the ambiguous cells resolve toward "no evidence".
//
// # Derived from the retry classifier's own predicates, not re-spelled
//
// Every branch below reads a named predicate or a named literal slice that
// [classifyApplierError] also reads. Re-spelling the face list here would
// create precisely the drift the 2026-07-28 lesson is about: two lists that
// agree the day they are written and quietly stop agreeing.
// [TestGrowEvidence_IsDerivedFromTheRetriableClassifier] is what fails if a
// face becomes evidence-carrying without being retriable, and
// [TestVtgateSubstringHalves_PartitionTheTransientSet] is what fails if the
// availability/transport split of [vtgateTransientSubstrings] drifts from the
// union the classifier matches on.
//
// # It honours the TERMINAL-CODE SHIELD, for the same reason the classifier does
//
// When a structured *MySQLError is present the server RESPONDED and the code
// decides; the loose [reparentRetriableSubstrings] tokens are NOT consulted.
// A 1105 whose message echoes `Sql: "insert into reparent_history ..."` must
// not read as reparent evidence any more than it may read as retriable (audit
// 2026-07-23 D0-3). Here the consequence of getting it wrong is "only" a false
// label — which is the whole subject of this item, so it gets the same care.

package mysql

import (
	"errors"
	"strings"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

// growEvidenceOf reports whether err carries positive storage-grow /
// serving-transition evidence, for the ADR-0110 grow-gate trip's structured
// log. It is a PURE VERDICT over the error: it never decides retriability,
// never changes what the gate does, and is safe to call on a terminal error
// (which reads as [ir.GrowEvidenceNone], the zero value).
func growEvidenceOf(err error) ir.GrowEvidence {
	if err == nil {
		return ir.GrowEvidenceNone
	}

	// Structured server response ⇒ the code decides, and only a code whose
	// semantics are message-dependent may consult the message (the shield).
	var mysqlErr *gomysql.MySQLError
	if errors.As(err, &mysqlErr) {
		switch mysqlErr.Number {
		case 1021, 1114:
			// ER_DISK_FULL / ER_RECORD_FILE_FULL. The target said it is out
			// of space, which IS the storage-grow boundary being hit.
			return ir.GrowEvidenceTargetFace
		case 3:
			// Generic "Error writing file" — evidence only with the ENOSPC
			// wording, same AND-gate the classifier applies.
			if isDiskFullSignal(err) {
				return ir.GrowEvidenceTargetFace
			}
		case 1290:
			// ER_OPTION_PREVENTS_STATEMENT is generic; only the read-only
			// variant is the serving-transition face (PS-320-v10).
			if isReadOnlyTargetSignal(err) {
				return ir.GrowEvidenceTargetFace
			}
		case 1105:
			// The Vitess envelope carries both classes of message, which is
			// exactly why the two literal sets below are separate.
			if isVtgateServingTransitionMessage(mysqlErr.Message) ||
				isVittabletServingTransitionMessage(mysqlErr.Message) {
				return ir.GrowEvidenceTargetFace
			}
		}
		return ir.GrowEvidenceNone
	}

	// No structured error: the text is transport/driver framing, so the loose
	// server-state wordings are safe to consult (the classifier's own
	// reasoning, one leg down).
	if isDiskFullSignal(err) || isReadOnlyTargetSignal(err) {
		return ir.GrowEvidenceTargetFace
	}
	msg := err.Error()
	if isVtgateServingTransitionMessage(msg) {
		return ir.GrowEvidenceTargetFace
	}
	lower := strings.ToLower(msg)
	for _, sub := range reparentRetriableSubstrings {
		if strings.Contains(lower, sub) {
			return ir.GrowEvidenceTargetFace
		}
	}
	return ir.GrowEvidenceNone
}

// vtgateServingTransitionSubstrings and vtgateTransportSubstrings are the two
// halves of [vtgateTransientSubstrings]. The split is not new information —
// that slice's own comments already grouped its entries as "(1)/(2) vtgate
// availability sentences" and "(3) vtgate's own TRANSPORT-loss framing" — it
// is the same distinction made STRUCTURAL so item 143's evidence verdict can
// read it instead of re-deriving it from prose.
//
// The union is what the retry classifier matches on and it is unchanged, so
// retriability is byte-identical to before this file existed.
// [TestVtgateSubstringHalves_PartitionTheTransientSet] pins the partition:
// every literal belongs to exactly one half and the concatenation is the whole
// set, in order.
//
// Serving-transition half: vtgate is telling the client about the CLUSTER's
// state — no primary is serving, a reparent or resharding is under way, the
// failover buffer is engaged. That is a statement about the target's serving
// state and it is what ADR-0110's quiesce argument is actually about.
var vtgateServingTransitionSubstrings = []string{
	"primary is not serving",
	"reparent operation in progress",
	"current keyspace is being resharded",
	"inconsistent state detected, primary is serving",
	"no healthy tablet available for",
	"no connection for tablet",
	"primary buffer is full",
	"buffer full: request evicted for newer request",
	"destination shard is missing after a resharding operation",
}

// vtgateTransportSubstrings is the TRANSPORT half — a connection between the
// client and vtgate (or PlanetScale's edge proxy in front of it) died. It says
// nothing about whether a tablet is serving.
//
// This one literal is the single most consequential entry in the whole file:
// it is what matched 244 of the 246 gate windows in the 2026-08-05 field log
// (`vtgate connection error: no endpoints`) and every one of the ~870 absorbed
// drops in the 2026-08-06 real-PlanetScale validation (`no endpoints` and
// `no healthy endpoints` both, which is why the anchor is the framing and not
// the tail). Its own definition site already records the doubt —
// "It is probably PlanetScale's edge proxy rather than vtgate itself" — and a
// proxy that cannot find an endpoint has observed nothing about the tablet
// behind it.
var vtgateTransportSubstrings = []string{
	"vtgate connection error",
}

// vittabletServingTransitionSubstrings is the vttablet-framed half of the same
// distinction over [vitessRetriableSubstrings].
//
// # What is IN, and the four that are deliberately OUT
//
// IN: `code = Unavailable` — vttablet's own "I am not ready to serve", the
// canonical in-flight-failover framing.
//
// OUT, each because a busy healthy target produces it just as readily as a
// growing one, so a label that fired on them would be uninformative:
//
//   - `QueryList.TerminateAll` — vttablet's QUERY killer. It was observed
//     during a real grow (v0.99.93 PS-320-v3) and roadmap item 143's own
//     filing lists it among the "reparent-evidenced faces", so this is a
//     deliberate DIVERGENCE from that filing, recorded here rather than left
//     implied: the killer fires on any query that outruns the query timeout,
//     which is a statement about the query's duration, not about the target's
//     serving state. Grow-CONSISTENT, not grow-evidencing.
//   - `code = Aborted` — covers both the tx-killer rollback (a timeout) and a
//     primary stepping down (a reparent). Two causes, one string; unusable as
//     evidence.
//   - `code = ResourceExhausted` — the throttler, or a full pool. Load.
//   - `code = Unknown` — vttablet's catch-all for several internal transients.
//     A catch-all cannot be evidence for anything specific.
//
// The InnoDB shapes the classifier also retries (1213 deadlock, 1205
// lock-wait-timeout) are OUT for the same reason and never reach this
// predicate: contention. ADR-0110's own context notes the 1205s during the
// grow arc were "a CONSEQUENCE of the hammering, not an independent fault".
var vittabletServingTransitionSubstrings = []string{
	"code = Unavailable",
}

// isVtgateServingTransitionMessage reports whether msg carries one of the
// vtgate CLUSTER-state sentences. Case-insensitive, matching the retry
// classifier's own treatment of these literals.
func isVtgateServingTransitionMessage(msg string) bool {
	lower := strings.ToLower(msg)
	for _, sub := range vtgateServingTransitionSubstrings {
		if strings.Contains(lower, sub) {
			return true
		}
	}
	return false
}

// isVittabletServingTransitionMessage reports whether msg is vttablet-framed
// AND carries a serving-transition gRPC code. The `vttablet` tag requirement
// mirrors [classifyVitessMessage] — a bare `code = Unavailable` with no tablet
// framing is not vttablet speaking.
func isVittabletServingTransitionMessage(msg string) bool {
	if !strings.Contains(msg, "vttablet") {
		return false
	}
	for _, sub := range vittabletServingTransitionSubstrings {
		if strings.Contains(msg, sub) {
			return true
		}
	}
	return false
}
