//go:build nekiverify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

// nekiControlTableSchemaProbe answers the one question that decides how NK306's
// control-table half gets fixed, and it is deliberately a PROBE rather than a
// premise check.
//
// # The question
//
// sluice's control tables — `sluice_cdc_state`, `sluice_migrate_state`,
// `sluice_cdc_skipped_tables` — live in `public` and carry no shard key,
// because on every other engine in the world they have no reason to. On a
// sharded Neki the default shard group covers `public`, so every INSERT into
// them is refused with `NK306`. That blocks BOTH apply lanes: the bisect arm
// shows the serial lane reaching its position write, meaning its DATA insert
// succeeded and only sluice's own bookkeeping failed.
//
// sluice cannot fix that by creating the table differently. Neki's topology
// declares `shard_group` PER TABLE in an operator-declared document, and
// `CREATE TABLE` does not enrol anything into it — the MoveTables arm proved
// that with `NK604` on a table that plainly existed and held rows. So a table
// sluice creates is absent from the document by construction.
//
// Which leaves the question this arm exists to answer: **does a table in a
// schema the topology does not mention fall OUTSIDE the shard group, or into
// the default one?**
//
//	outside  -> sluice puts its control tables in their own schema. No DDL
//	            shape change, no target-dependent columns, by far the cleanest
//	            of the three candidate fixes.
//	default  -> that option is dead, and the fix has to be a discovered
//	            shard-key column on the control tables, whose TYPE varies per
//	            database — control-table DDL becomes target-dependent, which it
//	            has never been on any engine.
//
// # Why it passes on either answer
//
// A premise check asserts the platform still behaves as sluice assumes. This
// asserts nothing of the kind: sluice has no behaviour here yet, and both
// answers are useful. So the arm's contract is to produce a CONCLUSIVE answer
// and record it — it fails only when it cannot, which is the outcome that
// would otherwise be mistaken for one of the two real ones.
//
// Once the fix lands this converts into a premise check for whichever approach
// was chosen. Until then, a weekly run that reports the same conclusive answer
// costs nothing and guards against the platform changing it underneath a
// design that has not been written yet.
func nekiControlTableSchemaProbe(ctx context.Context, t *testing.T, db *sql.DB) {
	t.Helper()

	t.Run("PROBE: can sluice's control tables live in a schema outside the shard group?", func(t *testing.T) {
		const (
			ctlSchema = "sluice_ctl_probe"
			ctlTable  = "probe_cdc_state"
			pubTable  = "probe_cdc_state_public"
		)

		t.Cleanup(func() {
			cctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			_, _ = db.ExecContext(cctx, `DROP TABLE IF EXISTS public.`+pubTable)
			_, _ = db.ExecContext(cctx, `DROP SCHEMA IF EXISTS `+ctlSchema+` CASCADE`)
		})

		// The real shape, not a simplification: sluice_cdc_state is
		// (stream_id VARCHAR(255) PK, source_position TEXT, updated_at
		// TIMESTAMP). What matters is that it carries no shard key and has no
		// sensible place to put one — a CDC position is not per-tenant data.
		const ddl = `(
			stream_id       VARCHAR(255) NOT NULL,
			source_position TEXT         NOT NULL,
			updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (stream_id)
		)`

		// ---- THE CONTROL, and it runs first -----------------------------
		//
		// The same table in `public` must be REFUSED. Without this the probe
		// cannot tell "the separate schema is outside the shard group" from
		// "this database is not enforcing shard keys at all today", and those
		// produce identical success in the arm below while meaning opposite
		// things.
		if _, err := db.ExecContext(ctx, `CREATE TABLE public.`+pubTable+ddl); err != nil {
			t.Fatalf("could not create the control table in public: %v", err)
		}
		_, ctlErr := db.ExecContext(ctx,
			`INSERT INTO public.`+pubTable+` (stream_id, source_position) VALUES ('probe','tok')`)

		if ctlErr == nil {
			t.Fatalf("INCONCLUSIVE: the control INSERT into public.%s SUCCEEDED.\n\n"+
				"A shard-key-less control table in `public` is supposed to be refused with NK306 — that "+
				"refusal is the entire problem this probe is scoping. If it now succeeds, either this "+
				"database is not sharded the way the fixture believes, or the platform's behaviour has "+
				"changed and NK306's control-table half may no longer exist. Either way the arm below "+
				"would succeed for a reason that has nothing to do with schemas, so it is not run.",
				pubTable)
		}
		if !isPGCode(ctlErr, "NK306") {
			t.Fatalf("INCONCLUSIVE: the control INSERT into public.%s failed with something OTHER than "+
				"NK306: %v\n\nThe probe needs the known refusal as its baseline; an unrelated failure "+
				"means the comparison below is not measuring the shard group.", pubTable, ctlErr)
		}
		t.Logf("control holds: a shard-key-less table in `public` is refused — %v", ctlErr)

		// ---- THE QUESTION -----------------------------------------------
		if _, err := db.ExecContext(ctx, `CREATE SCHEMA `+ctlSchema); err != nil {
			t.Fatalf("INCONCLUSIVE: could not CREATE SCHEMA %s through the router: %v\n\n"+
				"If a sharded Neki refuses new schemas outright, the separate-schema option is dead for "+
				"a different reason than the one this probe was testing — and that is itself the answer, "+
				"but it needs recording as such rather than as a shard-group result.", ctlSchema, err)
		}
		if _, err := db.ExecContext(ctx, `CREATE TABLE `+ctlSchema+`.`+ctlTable+ddl); err != nil {
			t.Fatalf("INCONCLUSIVE: the schema was created but the table in it was refused: %v\n\n"+
				"Record this verbatim — it constrains the fix as much as a shard-key refusal would.", err)
		}

		_, probeErr := db.ExecContext(ctx,
			`INSERT INTO `+ctlSchema+`.`+ctlTable+` (stream_id, source_position) VALUES ('probe','tok')`)

		switch {
		case probeErr == nil:
			// Verify it is READABLE too. An INSERT that is accepted and a row
			// that cannot be read back is the worse outcome, and the whole
			// point of a control table is reading it on resume.
			var n int
			if err := db.QueryRowContext(ctx,
				`SELECT count(*) FROM `+ctlSchema+`.`+ctlTable).Scan(&n); err != nil {
				t.Fatalf("ANSWER (partial): the INSERT into %s.%s succeeded but the read back failed: "+
					"%v\n\nA control table that accepts writes and cannot be read is useless to --resume; "+
					"this is not the clean outcome it first appeared to be.", ctlSchema, ctlTable, err)
			}
			if n != 1 {
				t.Fatalf("ANSWER (partial): the INSERT into %s.%s succeeded but the table holds %d rows, "+
					"not 1 — the row went somewhere an unpinned read cannot see it, which for a control "+
					"table is indistinguishable from losing it", ctlSchema, ctlTable, n)
			}
			t.Logf("ANSWER: YES — a shard-key-less control table in schema %q accepts writes and reads "+
				"them back, while the identical table in `public` is refused with NK306.\n\n"+
				"This is the CLEAN fix for NK306's control-table half: move sluice's three control "+
				"tables into their own schema. No DDL shape change, no discovered shard-key column, no "+
				"target-dependent control-table definition.\n\n"+
				"It is still a STATE-FORMAT change — every --resume and warm CDC start reads these "+
				"tables, so a binary must still FIND state written to the old location, on every engine "+
				"and not just Neki. See the backlog entry for the sibling sweep (three tables, four "+
				"writers).", ctlSchema)

		case isPGCode(probeErr, "NK306"):
			t.Logf("ANSWER: NO — a table in schema %q is refused with NK306 exactly as one in `public` "+
				"is, so the default shard group reaches unlisted schemas too.\n\n"+
				"The separate-schema option is DEAD. The remaining candidate is a discovered shard-key "+
				"column on the control tables: the refusal names the column, and get_data_topology "+
				"confirms it — but its TYPE varies per database, so control-table DDL becomes "+
				"target-dependent, which it has never been on any engine. The open question then "+
				"becomes whether a CONSTANT shard-key value is accepted.\n\nrefusal: %v",
				ctlSchema, probeErr)

		default:
			t.Fatalf("INCONCLUSIVE: the INSERT into %s.%s failed with neither success nor NK306: %v\n\n"+
				"Record it verbatim rather than forcing it into one of the two readings — an "+
				"unexplained third outcome is the finding.", ctlSchema, ctlTable, probeErr)
		}
	})
}
