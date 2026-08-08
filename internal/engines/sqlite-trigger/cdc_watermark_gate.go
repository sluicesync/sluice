// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"context"
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// The premise under this engine's whole gap-freedom argument, and the one
// thing that can break it.
//
// `cdc_snapshot.go` and `cdc_reader.go`'s pump both reason from the same two
// sentences: SQLite serialises writers, so the change-log id is allocated in
// COMMIT order; and the id is `INTEGER PRIMARY KEY AUTOINCREMENT` — the
// "monotonic, never-reused watermark" (setup.go). Together they license a
// plain `id > lastSeen` poll with no safety-lag predicate, which is the
// load-bearing simplification over pgtrigger's contiguous-committed-prefix
// walk. The FIRST sentence is a property of SQLite's locking and is now
// ground-truthed under real concurrency by
// TestChangeLogIDsAreCommitOrderedUnderConcurrentWriters.
//
// The SECOND is not a property of SQLite at all — it is a property of two
// pieces of ordinary, writable state:
//
//   - the change-log table's own DDL. `renderSetupDDL` emits
//     `CREATE TABLE IF NOT EXISTS`, and its own comment records that a
//     pre-existing table of the same name is accepted and used as-is
//     (roadmap item 149b). A shadow table declared `INTEGER PRIMARY KEY`
//     WITHOUT `AUTOINCREMENT` reuses rowids: SQLite allocates
//     `max(rowid)+1`, so once the auto-prune has emptied the log the next
//     capture gets id 1 — at or below every watermark the stream has ever
//     persisted, and `id > lastSeen` never matches it again.
//   - `sqlite_sequence`, which is an ORDINARY WRITABLE TABLE. With
//     AUTOINCREMENT the next id is `max(max(rowid), seq) + 1`, so
//     `UPDATE sqlite_sequence SET seq = 0 WHERE name = 'sluice_change_log'`
//     is inert while rows remain and becomes an id-reissue the moment the
//     prune drains the log — which the engine does on a cadence, by design.
//
// Either one strands every subsequent change silently: captured, never
// polled, position advancing. This is the sqlite sibling of the pgtrigger
// sequence hazard (`verifyChangeLogSequence`, ADR-0066 / the 2026-08-07
// sweep), which was filed as ungated on this engine precisely because
// `pg_sequence` has no counterpart here. It does have a counterpart; it is
// just spelled differently.
//
// SCOPE, stated so the name cannot be read as broader than the truth: this
// grades the allocation state at the two CDC DOORS — [CDCReader.StreamChanges]
// and [openSnapshotStream] — for both transports (local SQLite file and D1
// over HTTP, since `sqlite_master` and `sqlite_sequence` are ordinary tables
// on D1 too). Like its pgtrigger twin it cannot catch a tamper performed
// mid-stream, after the door has been passed.

// changeLogAllocation is the change log's id-allocation state: the floor
// below which the table can never issue a new id, and whether the table is
// declared AUTOINCREMENT at all.
//
// floor is `max(MAX(id), sqlite_sequence.seq)` — exactly the value SQLite's
// OP_NewRowid adds one to. A missing `sqlite_sequence` row (or a missing
// `sqlite_sequence` table, on a database that has never held an AUTOINCREMENT
// table) contributes 0, which is the honest reading: SQLite treats it the
// same way.
type changeLogAllocation struct {
	floor         int64
	autoincrement bool
}

// verifyChangeLogWatermark refuses loudly when the change log could allocate
// an id at or below watermark — i.e. when a change captured after this point
// would be invisible to a `id > watermark` poll forever.
//
// Both arms are checked, because they fail on disjoint inputs: a shadow table
// with no AUTOINCREMENT is caught by the DDL arm even at a zero watermark (an
// empty log has nothing for the floor arm to see), and a tampered
// `sqlite_sequence` under a correctly-declared table is caught only by the
// floor arm.
func verifyChangeLogWatermark(ctx context.Context, exec executor, driver string, watermark int64) error {
	st, err := exec.changeLogAllocation(ctx)
	if err != nil {
		return fmt.Errorf("%s: preflight: read change-log id allocation state: %w", driver, err)
	}
	if !st.autoincrement {
		return sluicecode.Wrap(
			sluicecode.CodeCDCChangeLogIDReuse,
			"Re-create the change log with `sluice trigger setup --source-driver "+driver+" --dsn=...` after "+
				"dropping the existing "+ChangeLogTable+" table (its rows are already-applied history; drop it "+
				"only when the stream is caught up), so the id column is declared INTEGER PRIMARY KEY "+
				"AUTOINCREMENT.",
			fmt.Errorf(
				"%s: the %q change log's id column is not declared AUTOINCREMENT, so SQLite reuses rowids: "+
					"after the auto-prune empties the log the next captured change is allocated id 1, at or "+
					"below the stream's persisted watermark, and the `id > watermark` poll never emits it "+
					"again. Every change from that point is captured and silently never replayed. (A table "+
					"of this name that already existed when `sluice trigger setup` ran is accepted as-is by "+
					"CREATE TABLE IF NOT EXISTS, which is how a non-AUTOINCREMENT change log gets here.)",
				driver, ChangeLogTable,
			),
		)
	}
	if st.floor < watermark {
		return sluicecode.Wrap(
			sluicecode.CodeCDCChangeLogIDReuse,
			"Restore the sequence above every id the stream has consumed: `UPDATE sqlite_sequence SET seq = "+
				"<the stream's watermark or higher> WHERE name = '"+ChangeLogTable+"'`, then restart the "+
				"stream. If the watermark is not trustworthy, re-cold-start the sync instead.",
			fmt.Errorf(
				"%s: the %q change log can re-issue ids the stream has already consumed: the next id will be "+
					"%d (SQLite allocates max(MAX(id), sqlite_sequence.seq) + 1) but the resume watermark is "+
					"%d. Every change captured until the counter climbs back past %d would be written with an "+
					"id at or below the watermark, and the `id > watermark` poll would never emit it — "+
					"captured, never replayed, position advancing. This state is reached by lowering "+
					"sqlite_sequence.seq (an ordinary writable table) and then letting the auto-prune drain "+
					"the log",
				driver, ChangeLogTable, st.floor+1, watermark, watermark,
			),
		)
	}
	return nil
}

// changeLogDeclaresAutoincrement reports whether a `CREATE TABLE` statement
// read out of `sqlite_master` declares AUTOINCREMENT. SQLite stores the
// statement verbatim, so this is a token search rather than a parse; the
// keyword is legal in exactly one place in a table definition (on an INTEGER
// PRIMARY KEY column), so a match cannot come from anywhere else — except a
// column or table NAME containing the word, which the boundary check below
// rules out.
func changeLogDeclaresAutoincrement(createSQL string) bool {
	upper := strings.ToUpper(createSQL)
	const kw = "AUTOINCREMENT"
	for i := 0; ; {
		j := strings.Index(upper[i:], kw)
		if j < 0 {
			return false
		}
		at := i + j
		before := byte(' ')
		if at > 0 {
			before = upper[at-1]
		}
		after := byte(' ')
		if at+len(kw) < len(upper) {
			after = upper[at+len(kw)]
		}
		if !isIdentByte(before) && !isIdentByte(after) {
			return true
		}
		i = at + len(kw)
	}
}

func isIdentByte(b byte) bool {
	return b == '_' || (b >= '0' && b <= '9') || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}
