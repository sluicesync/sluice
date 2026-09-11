// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// # Finding out whether direct DDL is accepted, BEFORE the schema phase
//
// An operator migrating to PlanetScale MySQL reported five stumbling
// blocks; two of them were one problem. They hit the Safe Migrations
// refusal partway into the run, disabled Safe Migrations, re-ran, and hit
// it AGAIN — then, because the first attempt had already created part of
// the schema, a plain re-run was wrong too and they needed `--resume`. A
// whole cycle, twice, to learn a fact that is knowable in 200 ms.
//
// # Why this has to be behavioural
//
// There is no server-side flag to read. The operator went looking for one
// and the answer came back: the only way to know is to issue a DDL and see
// what happens. The PlanetScale API does expose the branch's
// `safe_migrations` boolean, and sluice already reads it where credentials
// are configured ([ir.BranchStatus]) — but that value is NOT a substitute
// here, and the reason is the whole point:
//
//	Disabling Safe Migrations in the UI flips the flag immediately. The
//	change then propagates ASYNCHRONOUSLY through the cluster, reaching
//	each VTGate in its own time. Between those two moments the API says
//	"disabled" and the cluster still refuses.
//
// That window is exactly what cost the operator their second cycle. An
// API-derived preflight would have cleared them to run, and the run would
// have failed. The flag and the behaviour are two different facts, and the
// one that decides whether a DDL succeeds is the behaviour.
//
// # The answers are ASYMMETRIC, and the doc says so because the code cannot
//
//	REFUSED  conclusive. Direct DDL is not being accepted right now.
//	         Whether that is because Safe Migrations is on, or because it
//	         was just turned off and has not propagated, does not change
//	         what the operator should do: wait, or use deploy requests.
//	ACCEPTED NOT conclusive. It means this gateway, on this connection,
//	         at this moment. Another VTGate mid-propagation may still
//	         refuse. sluice therefore never reports "Safe Migrations is
//	         off" — there is no independent evidence for that claim, and
//	         stating it would be the kind of all-clear that is worse than
//	         no check at all.
//
// So this is a fail-fast, not a clearance. It converts a failure that
// arrives after partial schema creation into one that arrives before
// anything is touched, which is the whole of its value.
//
// # The probe SHAPE, and the premise deliberately not relied on
//
// The probe creates a throwaway table and drops it. A cheaper shape was
// considered and rejected: `ALTER TABLE <a table that does not exist>` has
// no side effects at all, and on a branch accepting DDL it answers 1146
// (unknown table), which is a perfectly good "accepted" signal.
//
// It was rejected because it is only sound if Vitess evaluates the
// safe-migrations gate BEFORE resolving the table. If resolution wins,
// a branch that refuses every DDL answers 1146 to this probe and the
// preflight reports a FALSE ALL-CLEAR — silently, on exactly the
// configuration it exists to catch. That premise could not be measured
// here (PlanetScale refuses to enable Safe Migrations on the test branch:
// "Safe migrations requires schema errors to be fixed before it may be
// enabled", and the torture fixture's keyless tables are those errors), so
// rather than ship a safety argument resting on an unverified premise, the
// shape was chosen so the premise is not load-bearing. A CREATE that
// succeeds is proof the DDL path is open, whatever the gate's ordering.
//
// Residue: if the CREATE succeeds and the DROP fails, one empty table named
// below is left behind. That is logged, not swallowed, and the name says
// what it is.
// Named with the sluice_ prefix every other control object uses, and
// registered on appliershared.ControlTableNames(), so a residue table is
// classified as sluice bookkeeping rather than enumerated as USER data by
// every schema reader -- which would show it as drift in schema diff /
// verify and copy it onward from a promoted ex-target.
const directDDLProbeTable = "sluice_direct_ddl_probe"

// ProbeDirectDDL issues a real DDL against the target and reports whether
// it was accepted. A nil return means "accepted on this connection, now" —
// see the asymmetry note above; it is NOT a statement about the branch.
//
// The PlanetScale/Vitess safe-migrations refusal comes back as the coded
// [sluicecode.CodePSDirectDDLBlocked]. Every other failure is returned as
// a probe error, never as a verdict: a preflight that cannot RUN must not
// be reported as a preflight that FAILED.
func (w *RowWriter) ProbeDirectDDL(ctx context.Context) error {
	// Vanilla MySQL and MariaDB have no mode that refuses direct DDL, so
	// there is nothing for the probe to discover and no reason to issue a
	// CREATE+DROP against every such target on every run. Scoped at the
	// flavor rather than at the call site so the pipeline stays
	// engine-neutral (the preflight only knows it holds a prober).
	if !w.safeMigrationsPossible {
		return nil
	}
	create := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id INT NOT NULL PRIMARY KEY)",
		quoteIdent(directDDLProbeTable))
	if _, err := w.db.ExecContext(ctx, create); err != nil {
		if isDirectDDLDisabledErr(err) {
			return directDDLBlockedRefusal(err)
		}
		return fmt.Errorf("mysql: direct-DDL preflight probe could not run: %w", err)
	}
	drop := "DROP TABLE IF EXISTS " + quoteIdent(directDDLProbeTable)
	if _, err := w.db.ExecContext(ctx, drop); err != nil {
		// The question the preflight asked has been answered — DDL is
		// accepted — so this is not a refusal. It is residue, and it is
		// named so the operator can recognise and remove it.
		return fmt.Errorf("mysql: direct DDL is accepted, but the preflight's throwaway table %q could not be "+
			"dropped and has been left on the target; drop it by hand: %w", directDDLProbeTable, err)
	}
	return nil
}

// directDDLBlockedRefusal is the operator-facing form of the safe-migrations
// refusal when it is caught at PREFLIGHT rather than mid-schema.
//
// The hint says the two things the operator's report shows are missing from
// every other spelling of this message: that disabling is not instant, and
// that a re-run after a partial attempt is `--resume` rather than a plain
// re-run. Both cost that operator a cycle each.
func directDDLBlockedRefusal(err error) error {
	return sluicecode.Wrap(
		sluicecode.CodePSDirectDDLBlocked,
		"either (a) disable safe migrations on the target branch for the migration window and re-run — but note "+
			"that disabling is NOT instant: the setting propagates asynchronously to the cluster, so the branch can "+
			"read as disabled while DDL is still refused; re-run only once this preflight passes, which is what it "+
			"is for; or (b) pre-create the schema via deploy requests (`sluice schema preview` prints the target "+
			"DDL, `sluice deploy-ddl --ddl '<statement>'` ships each statement) and re-run. Because this refusal "+
			"fires BEFORE the schema phase, no table of yours exists to resume onto — so re-run with a fresh "+
			"`--migration-id` (or clear the recorded state) rather than with `--resume`. `--resume` is the right "+
			"answer only when an earlier attempt got far enough to create part of the schema, which is a different "+
			"refusal raised from the schema phase itself",
		fmt.Errorf("%w: %w | "+
			"refused BEFORE the schema phase: sluice probed the target with a throwaway CREATE TABLE and the "+
			"branch refused it, so every DDL this run needs would be refused too. No TABLE was created and no "+
			"data moved — but this run did record its own migration-state row, so a plain re-run of the same "+
			"--migration-id will be refused as a partial migration once the branch accepts DDL again. Re-run with "+
			"a fresh --migration-id, or clear the recorded state, rather than reaching for --resume",
			ErrSafeMigrationsBlocked, err),
	)
}

// ErrDirectDDLProbeUnavailable marks a probe that could not run for a reason
// that is not a verdict. Callers treat it as "no information", never as a
// pass and never as a refusal.
var ErrDirectDDLProbeUnavailable = errors.New("mysql: direct-DDL preflight probe unavailable")
