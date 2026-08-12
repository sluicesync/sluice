// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

// CDCScopePredicateSetter is the optional CDCReader surface the pipeline
// uses to hand the reader the sync's EFFECTIVE table scope — the operator's
// --include-table/--exclude-table filter merged with the live-added set
// (Bug 246). The pipeline applies that filter one stage DOWNSTREAM of the
// reader (the dispatch-side filter goroutine), which is fine for dropping
// events but wrong for reader-side POLICY decisions: the MySQL reader's XA
// refusal fired for tables the sync excludes — a configuration that worked
// before the refusal existed — because the reader could only see schema
// scope, not table scope. Readers consult the predicate ONLY for such
// policy checks; event filtering stays downstream, so wiring this changes
// no delivery semantics.
//
// The predicate must be safe for concurrent calls from the reader's pump
// goroutine (the pipeline's closure reads an atomic pointer). A reader
// that never receives one treats every in-schema table as in scope — the
// conservative direction for a policy refusal.
type CDCScopePredicateSetter interface {
	SetCDCScopePredicate(allowed func(schema, table string) bool)
}
