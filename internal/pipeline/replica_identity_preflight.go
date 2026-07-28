// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Source-side replica-identity preflight (roadmap item 93).
//
// A publication-scoped CDC cold start does something to the SOURCE that
// no other sluice phase does: it adds the stream's tables to a
// publication with `pubupdate=t pubdelete=t`. Postgres then requires
// each of those tables to have a replica identity — and it will not use
// a DEFERRABLE key's index as one. So a source table whose only key is a
// deferrable PRIMARY KEY starts refusing the OPERATOR'S OWN
// UPDATE/DELETE the moment sluice scopes the publication:
//
//	ERROR: cannot update table "dpk" because it does not have a replica
//	       identity and publishes updates
//
// INSERT keeps working, so the table looks healthy until the first
// update; and because the failure lands in the operator's application
// rather than in sluice's output, there is nothing to connect it back to
// the migration tool. Before this preflight sluice logged nothing about
// it at cold start.
//
// # Where it runs (the load-bearing part)
//
// At both doors into a publication, in each case BEFORE the door opens —
// putting the source into the broken state and only then complaining IS
// the defect, not the fix, so a refusal leaves the publication (and the
// source) exactly as it found them:
//
//   - [Streamer.coldStartReadSourceSchema], alongside the other
//     source-side preflights and therefore ahead of
//     [Streamer.coldStart]'s `EnsurePublication`.
//   - [AddTable.Run], right after the source schema read and ahead of
//     `extendPublicationScope`'s `ALTER PUBLICATION … ADD TABLE`. A
//     live-added table joins the same publication and breaks the same
//     way; unlike the target-side sibling (Bug 211) there is no
//     defence-in-depth floor to fall back on here, because the source's
//     refusal happens in the operator's application and never reaches
//     sluice at all.
//
// # Gating
//
// Fires only for a source whose declared CDC mechanism is
// [ir.CDCLogicalReplication] — the capability that MEANS "cold start
// scopes a publication". It must NOT fire for:
//
//   - `postgres-trigger`, which captures via triggers and creates no
//     publication, so none of its tables can reach this failure. Its
//     SchemaReader delegates to the composed [postgres.Engine] and DOES
//     satisfy the preflighter interface, so — exactly as in
//     [preflightSourceReplication] — the capability gate runs FIRST and
//     is the only thing that excludes it.
//   - MySQL sources (binlog / VStream capabilities), which have no
//     replica-identity concept at all.
//   - One-shot `migrate`, which never scopes a publication. This
//     preflight is wired only into the two paths that do.
//
// Warm resume never reaches it either: the publication was scoped at
// cold start and warm resume does not re-assert it. The interrupted-COPY
// resume DOES run it, deliberately — that path re-ensures the
// publication, and a source already in the broken state should be named,
// not resumed past.

import (
	"context"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// preflightSourceReplicaIdentity runs the source-side replica-identity
// preflight against the still-open source schema reader. Returns nil
// when:
//
//   - The source's declared CDC capability is not
//     [ir.CDCLogicalReplication] (the capability gate — see the file
//     comment; it runs FIRST because postgres-trigger's delegated
//     SchemaReader would otherwise satisfy the interface assert).
//   - The handle doesn't implement [ir.ReplicaIdentityPreflighter] (the
//     opportunistic-skip posture shared with [preflightRLS]).
//   - tables is empty.
//   - Every named table can publish an UPDATE/DELETE.
//
// Otherwise it returns the engine's coded refusal
// ([sluicecode.CodeSourceReplicaIdentity]) naming every offending table.
//
// tables is what the caller is about to hand the publication — the
// cold-start caller passes [migcore.TableNamesForPublication] of the
// POST-filter schema, the identical call it feeds to
// `EnsurePublication`; the add-table caller passes the single table it
// is about to `ALTER PUBLICATION … ADD`. Sourcing the list that way is
// what makes the preflight's scope and the publication's scope the same
// question, and what makes `--exclude-table` (one of the remedies the
// refusal names) genuinely short-circuit it.
func preflightSourceReplicaIdentity(ctx context.Context, handle any, sourceCaps ir.Capabilities, tables []string) error {
	if sourceCaps.CDC != ir.CDCLogicalReplication {
		return nil
	}
	pf, ok := handle.(ir.ReplicaIdentityPreflighter)
	if !ok {
		return nil
	}
	if len(tables) == 0 {
		return nil
	}
	if err := pf.PreflightReplicaIdentity(ctx, tables); err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, err)
	}
	return nil
}
