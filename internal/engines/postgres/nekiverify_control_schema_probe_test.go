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

// nekiControlTableShardKeyProbe is the follow-on, and since 2026-09-14 it is
// the DECIDING question for NK306's control-table half.
//
// The schema probe above answered NO: a shard-key-less control table in its own
// schema is refused with NK306 exactly as one in `public` is, so the default
// shard group reaches unlisted schemas too. That kills the clean fix and leaves
// one candidate — give the control tables a shard-key column — whose viability
// turns on questions nobody has asked a live router.
//
// It probes the FULL control-table lifecycle rather than an INSERT, because
// that is what sluice actually does with these tables and each step can fail
// differently:
//
//	INSERT  — the position row is created once per stream
//	UPDATE  — every committed batch rewrites source_position. This is the one
//	          most likely to break: the suite separately proves a sharded Neki
//	          refuses an UPDATE that NAMES the shard key in its SET list, even
//	          assigned its own value, so a control-table update must be written
//	          to leave the routing column alone.
//	SELECT  — resume reads it back, unpinned. A row that landed somewhere an
//	          ordinary read cannot see is indistinguishable from a lost one.
//
// A CONSTANT shard-key value is used deliberately. sluice has no per-tenant
// meaning to put in a CDC position row, so whatever it writes there is
// arbitrary — which means every control row routes to ONE shard. Whether the
// router accepts that, and whether the rows stay readable, is precisely what
// has to be known before the fix is designed rather than after.
func nekiControlTableShardKeyProbe(ctx context.Context, t *testing.T, db *sql.DB, shardKey string) {
	t.Helper()

	t.Run("PROBE: does a control table with a CONSTANT shard key survive its lifecycle?", func(t *testing.T) {
		const tbl = "probe_ctl_shardkey"

		t.Cleanup(func() {
			cctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			_, _ = db.ExecContext(cctx, `DROP TABLE IF EXISTS public.`+tbl)
		})

		// sluice_cdc_state's real shape plus the routing column. The shard key
		// is NOT in the primary key: stream_id is what identifies a row, and
		// making the routing column part of the identity would change what the
		// table MEANS on every engine, not just this one.
		if _, err := db.ExecContext(ctx, `CREATE TABLE public.`+tbl+` (
			`+shardKey+`      int          NOT NULL,
			stream_id       VARCHAR(255) NOT NULL,
			source_position TEXT         NOT NULL,
			updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (stream_id)
		)`); err != nil {
			t.Fatalf("ANSWER: the control table cannot even be CREATED with the shard key outside its "+
				"primary key: %v\n\nThat constrains the fix hard — the routing column would have to join "+
				"the PRIMARY KEY, which changes what the table identifies on every engine and not just "+
				"this one.", err)
		}

		// INSERT — several rows, all on the same constant key, as sluice would.
		for i, stream := range []string{"stream-a", "stream-b", "stream-c"} {
			if _, err := db.ExecContext(ctx,
				`INSERT INTO public.`+tbl+` (`+shardKey+`, stream_id, source_position) VALUES (0, $1, $2)`,
				stream, "tok-"+stream); err != nil {
				t.Fatalf("ANSWER: INSERT %d with a CONSTANT shard key of 0 was refused: %v\n\n"+
					"The remaining NK306 candidate depends on this working. If a constant is refused, "+
					"sluice would have to invent a meaningful per-row routing value for bookkeeping that "+
					"has no per-tenant meaning — and there is no honest one to invent.", i, err)
			}
		}

		// UPDATE — the position write, the step most likely to break. The SET
		// list deliberately does NOT name the shard key.
		res, err := db.ExecContext(ctx,
			`UPDATE public.`+tbl+` SET source_position = $1, updated_at = CURRENT_TIMESTAMP
			 WHERE stream_id = $2`, "tok-advanced", "stream-b")
		if err != nil {
			t.Fatalf("ANSWER: the POSITION WRITE was refused: %v\n\n"+
				"This is the step every committed CDC batch performs, so a refusal here kills the "+
				"candidate outright even though the INSERT worked. Note the SET list does not name the "+
				"shard key — the suite separately proves naming it is refused — so this is the "+
				"best-case form of the statement.", err)
		}
		if n, _ := res.RowsAffected(); n != 1 {
			t.Fatalf("ANSWER: the position write reported %d rows affected, not 1.\n\n"+
				"A control-table UPDATE that matches nothing is the silent half of this problem: sluice "+
				"would believe it had checkpointed while the stored position never moved, and a resume "+
				"would replay from an older token.", n)
		}

		// SELECT — unpinned, the way resume reads it.
		var got string
		if err := db.QueryRowContext(ctx,
			`SELECT source_position FROM public.`+tbl+` WHERE stream_id = $1`, "stream-b").Scan(&got); err != nil {
			t.Fatalf("ANSWER: the row could not be read back unpinned: %v\n\n"+
				"Resume reads these tables without a shard pin. A row that is written and not readable "+
				"is worse than a refusal.", err)
		}
		if got != "tok-advanced" {
			t.Fatalf("ANSWER: the position read back as %q, not the value just written.\n\n"+
				"The update landed somewhere the read does not see — which for a checkpoint is silent "+
				"divergence rather than a loud failure.", got)
		}

		var n int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM public.`+tbl).Scan(&n); err != nil {
			t.Fatalf("count: %v", err)
		}
		if n != 3 {
			t.Fatalf("ANSWER: an unpinned count returned %d of 3 control rows — some are not visible "+
				"without a shard pin, and resume does not pin", n)
		}

		t.Logf("ANSWER: YES — a control table carrying a CONSTANT shard key (outside its primary key) "+
			"takes INSERTs, accepts the position UPDATE without naming the routing column, and reads "+
			"back unpinned with all %d rows visible.\n\n"+
			"So the remaining NK306 candidate is viable: add the routing column, discovered from the "+
			"topology, with an arbitrary constant. The cost is that control-table DDL becomes "+
			"TARGET-DEPENDENT — the column's name and type come from the database — which it has never "+
			"been on any engine, and it remains a STATE-FORMAT change that a binary written against the "+
			"old shape must still be able to read.", n)
	})
}
