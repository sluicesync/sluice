// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Pre-copy gate for foreign keys the TARGET ALREADY CARRIES (roadmap
// item 140, field report 2026-08-05).
//
// sluice's deferred-constraint discipline — create the tables bare, copy
// the rows, then add indexes and constraints — governs only the
// constraints sluice CREATES. A reporter branched their target from an
// existing database, so the schema arrived with its foreign keys intact;
// the bulk copy is not parent-first ordered (tables are copied in
// parallel), a child row landed before its parent, and the run died ~20
// seconds in on `Error 1452 (23000): Cannot add or update a child row`.
// Their own read was right: "wouldn't have been an issue on a totally
// new db." That is exactly the problem — the failure is invisible until
// it happens, and the remedy is obvious only to someone who already
// understands the deferred-constraint design.
//
// It is a PREFLIGHT gap, not a copy bug: the target's foreign keys are
// readable before a single row moves, through the very SchemaReader the
// ADR-0166 shape gate already opens. This check rides that one read, so
// it costs no extra round trip.
//
// # Refuse vs warn — split on evidence, not on posture
//
//   - the constraint's PARENT table is IN this run's scope → REFUSE
//     (coded). The parent's rows arrive during this same copy and
//     nothing orders them first, so the constraint will reject a child.
//     A self-referencing foreign key lands here too — the parent is the
//     table itself.
//   - the parent is NOT in scope → WARN. Whatever that table holds on
//     the target predates this run and may well satisfy every child row
//     (the "users is already loaded, now copy orders" shape). sluice
//     cannot know, so it must not refuse a configuration that works —
//     but it must not be silent about one that might not.
//   - the target's copy path bypasses FK enforcement
//     ([ir.Capabilities.BulkCopyBypassesForeignKeys]) → nothing to
//     refuse; that copy genuinely cannot fail this way.
//
// # --skip-foreign-keys is NOT an exemption, deliberately
//
// It is the obvious flag to reach for and it does not work on its own:
// [applySkipForeignKeys] strips foreign keys from the IR so the
// constraints phase creates none. It issues no DDL against the target,
// so a constraint that was ALREADY there stays there and still rejects
// the copy. Exempting the flag would wave a run through to the identical
// 20-second failure this gate exists to pre-empt. The refusal names it
// as PART of a remedy — so sluice does not re-add what you just dropped
// — never as the remedy. Pinned by
// TestMigrateFKGate_SkipForeignKeysIsNotAnExemption and, against a real
// server, by TestMigrate_PreExistingTargetForeignKeys_MySQL's skip-flag
// leg, which reads the constraint back off the target after the refusal.
//
// # Which paths this reaches, stated so it cannot be read as broader
//
// The check lives on [existingTablesGate], so it reaches exactly that
// gate's two callers — migrate's non-resume cold start and the sync
// cold-start default branch — and inherits ADR-0166's skip rules
// verbatim. Deliberately NOT covered, each for the reason the branch
// already carries: --reset-target-data and --restart-from-scratch (the
// in-scope tables are dropped and recreated constraint-free, so nothing
// pre-existing survives to find), --schema-already-applied (the operator
// has explicitly opted out of the target round-trips this check needs),
// and --resume / the interrupted-COPY resume (a resumed run may have
// already finished its FK-bearing tables, so a refusal there could be a
// false positive; the resume contract is unchanged).

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// preExistingFKsShown caps how many constraints one message spells out;
// the remainder is summarized as a count so a wide schema doesn't
// produce a page of error.
const preExistingFKsShown = 5

// checkPreExistingForeignKeys refuses (coded) when the target already
// carries a foreign key on an in-scope table whose parent this same run
// copies, WARNs when the parent is out of scope, and is a no-op on a
// target whose copy path bypasses FK enforcement. actual is the target's
// catalog as read by [existingTablesGate.readTargetTablesForShapeGate];
// schema is the post-filter in-scope set.
func (g *existingTablesGate) checkPreExistingForeignKeys(ctx context.Context, schema *ir.Schema, actual map[string]*ir.Table) error {
	if g.Target.Capabilities().BulkCopyBypassesForeignKeys {
		return nil
	}

	inScope := make(map[string]struct{}, len(schema.Tables))
	for _, t := range schema.Tables {
		if t != nil {
			inScope[t.Name] = struct{}{}
		}
	}

	var blocking, advisory []string
	for _, t := range schema.Tables {
		if t == nil {
			continue
		}
		act, exists := actual[t.Name]
		if !exists || act == nil {
			continue
		}
		for _, fk := range act.ForeignKeys {
			if fk == nil {
				continue
			}
			// The parent is matched on BARE TABLE NAME, deliberately: the
			// gate's reader is scoped to one target schema and the IR's
			// in-scope set carries bare names too. A cross-schema parent
			// whose name collides with an in-scope table is therefore
			// counted as in-scope — the conservative direction (refuse),
			// and the message names the constraint so the operator can see
			// what was matched.
			if _, parentInScope := inScope[fk.ReferencedTable]; parentInScope {
				blocking = append(blocking, renderPreExistingFK(t.Name, fk))
				continue
			}
			advisory = append(advisory, renderPreExistingFK(t.Name, fk))
		}
	}

	if len(advisory) > 0 {
		slog.WarnContext(ctx,
			g.Mode+": the target already carries foreign key(s) on table(s) this run copies into, whose PARENT tables are NOT in scope — tolerated (the parent rows already on the target may satisfy every child row), but a child row whose parent is missing fails loudly mid-copy (MySQL Error 1452 / Postgres SQLSTATE 23503); drop the constraint for the duration of the copy if that is not what you want",
			slog.String("foreign_keys", renderPreExistingFKList(advisory)))
	}
	if len(blocking) == 0 {
		return nil
	}

	// The hint leads with the non-destructive remedy and names
	// --reset-target-data last with its blast radius spelled out, the
	// same ordering the shape gate's hint settled on. The closing
	// sentence about --skip-foreign-keys is load-bearing, not
	// decoration: it is the flag every operator reaches for first, and
	// on a pre-existing constraint it does nothing at all.
	return sluicecode.Wrap(
		sluicecode.CodeTargetPreexistingForeignKey,
		"drop the named foreign keys on the target for the duration of the copy and re-add them afterwards "+
			"(ALTER TABLE ... DROP FOREIGN KEY ... on MySQL, ALTER TABLE ... DROP CONSTRAINT ... on Postgres), "+
			"adding --skip-foreign-keys if you do not want sluice to re-create them at the end; or take the child "+
			"tables out of scope with --exclude-table; or, only if the ENTIRE target dataset is disposable, "+
			"--reset-target-data drops every in-scope target table and re-creates it WITHOUT constraints, so the "+
			"copy runs unconstrained and the foreign keys are added back after it. Note that --skip-foreign-keys "+
			"ALONE does not clear this: it governs only the constraints sluice would create, never one the target "+
			"already has, so a run passing it would still die on the same rejection",
		fmt.Errorf(
			"pipeline: the target already carries %d foreign key(s) on table(s) this %s copies into, and their "+
				"parent tables are copied by this SAME run — refusing before any data moves (the copy is not "+
				"parent-first ordered, so a child row reaches the target before its parent and the constraint "+
				"rejects it: MySQL Error 1452 / Postgres SQLSTATE 23503): %s",
			len(blocking), g.Mode, renderPreExistingFKList(blocking),
		),
	)
}

// renderPreExistingFK renders one constraint the way an operator asks
// about it: which child table, which constraint, which parent. An
// unnamed source constraint (the IR allows it) renders as "(unnamed)"
// rather than an empty pair of quotes.
func renderPreExistingFK(child string, fk *ir.ForeignKey) string {
	name := fk.Name
	if name == "" {
		name = "(unnamed)"
	}
	return fmt.Sprintf("table %q constraint %q -> parent %q", child, name, fk.ReferencedTable)
}

// renderPreExistingFKList joins the rendered constraints, truncating
// past [preExistingFKsShown] with a count of the remainder.
func renderPreExistingFKList(entries []string) string {
	if len(entries) <= preExistingFKsShown {
		return strings.Join(entries, "; ")
	}
	return strings.Join(entries[:preExistingFKsShown], "; ") +
		fmt.Sprintf("; and %d more", len(entries)-preExistingFKsShown)
}
