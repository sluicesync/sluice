// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// # A Neki workflow can take a table away from a live stream
//
// PlanetScale Neki's MoveTables moves a set of tables from one database
// into another. Its write cutover does not redirect a connection that
// named the OLD database — it BLOCKS the table there, on every shard
// primary, and every statement against it then fails with
//
//	ERROR: access to table "public.mv_src" is blocked (SQLSTATE NK213)
//
// Measured on a live 3-shard cluster (2026-09-10) with a sluice stream
// applying underneath the workflow. The sequence and what a stream sees:
//
//	move_tables_create        — transparent. The per-shard copy+stream
//	                            runs alongside sluice's; both sides stay
//	                            byte-identical while it does.
//	move_tables_switch_reads  — transparent. In Postgres the client picks
//	                            its database at connect time, so a read
//	                            switch cannot reach a connection that
//	                            named the source database; sluice kept
//	                            reading and writing the old copy with no
//	                            divergence. (Proven non-vacuously: a
//	                            sentinel row written only into the target
//	                            database stayed invisible through the
//	                            source connection.)
//	move_tables_switch_writes — the table is BLOCKED on the source
//	                            database. Every sluice statement against
//	                            it fails; the stream halts.
//
// The halt is the correct outcome and it is what already happened: NK213
// is not in the retriable set, so [classifyApplierError]'s terminal-code
// shield returned it verbatim and the stream exited non-zero having
// applied nothing further. Nothing was lost — the persisted CDC position
// had last advanced ~1s BEFORE the block, and reversing the workflow and
// restarting the stream replayed the gap to exact parity.
//
// What was missing is the sentence an operator needs. The raw text names
// a SQLSTATE nobody has memorised and does not mention workflows,
// databases, or cutovers; an operator watching a migration die on it has
// no way to reach "a MoveTables workflow just took this table" from what
// sluice printed. That is the same defect an operator reported against
// the MySQL lane in different words ("the errors are pretty dense and
// hard to parse"), and the remedy is the same: say what happened and name
// the command that shows it.
//
// Deliberately still TERMINAL, not retriable. The block carries a
// one-YEAR expiry and clears only when the workflow completes or is
// reversed, so a retry budget would burn out against a wall; and the
// resolution is a decision sluice must not make on the operator's behalf
// — after a completed move the stream has to be repointed at the NEW
// database, which changes where the data lives.
//
// SIBLING SWEEP — which paths reach this classification, since NK213's
// block is `scope=any` and so refuses reads as well as writes:
//
//   - CDC apply (change_applier*.go), raw-copy export (raw_copy.go:137),
//     row reads (row_reader.go:350) and the CDC reader
//     (classifyReaderError delegates here): REACHED. All propagate the
//     classified error.
//   - The grow gate (row_writer_grow_gate.go), the schema writer's retry
//     predicate (schema_writer.go:440, schema_writer_index_overlap.go:289)
//     and raw_copy.go:242: these call [classifyApplierError] only to ask
//     "is this retriable?" and DISCARD the returned error. They see the
//     right answer (no) and are unchanged, but the hint does not reach
//     their own wraps. NOT covered, deliberately — annotating them means
//     changing what they return, and each already fails loudly with the
//     underlying pgconn text.
//   - The bulk-copy `COPY` writer does not route through this classifier
//     at all. A cold start into a table blocked by a workflow therefore
//     still surfaces the bare NK213. Filed rather than guessed at: the
//     COPY path's error handling is its own shape and folding a coded
//     refusal into it is a separate change.

// nekiBlockedTableCode is the SQLSTATE Neki's shard primaries return for
// a statement against a table a workflow holds a block on.
const nekiBlockedTableCode = "NK213"

// isNekiTableBlocked reports whether err is Neki's blocked-table refusal.
func isNekiTableBlocked(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == nekiBlockedTableCode
}

// annotateNekiBlockedTable wraps Neki's blocked-table refusal in the
// coded refusal an operator can act on. Returns err unchanged for every
// other shape, so it is safe to call on any error.
//
// The hint names `__neki.list_blocked_tables()` because that is the one
// query that answers the operator's actual question — it returns the
// database, the table, the action and the holding shard, which is enough
// to identify the workflow without guessing — and `move_tables_status()`
// because the traffic state says whether the move is mid-cutover or done.
func annotateNekiBlockedTable(err error) error {
	if err == nil || !isNekiTableBlocked(err) {
		return err
	}
	return sluicecode.Wrap(
		sluicecode.CodeTargetTableBlockedByWorkflow,
		"run `SELECT * FROM __neki.list_blocked_tables()` on the Neki cluster to see which database and table are blocked, "+
			"and `SELECT workflow, traffic_state FROM __neki.move_tables_status()` to see the workflow holding it; "+
			"then either finish the move and restart sluice against the NEW database (the stream resumes from its "+
			"persisted position — nothing was lost), or reverse the cutover "+
			"(`SELECT __neki.move_tables_reverse_traffic('<workflow>')`) to hand the table back to this database",
		fmt.Errorf("a PlanetScale Neki workflow has blocked this table on the database sluice is connected to — "+
			"a MoveTables cutover blocks the table on the OLD database rather than redirecting connections that "+
			"named it, so every statement sluice makes against it is refused until the move completes or is "+
			"reversed: %w", err),
	)
}
