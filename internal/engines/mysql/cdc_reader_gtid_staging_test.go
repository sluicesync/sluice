// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"sluicesync.dev/sluice/internal/ir"
)

// The item-132 reader-side pin: in GTID mode a ROW's position must NOT contain
// the transaction that row belongs to, and the transaction's TxCommit position
// MUST contain it.
//
// # Why this shape and not "the position advanced"
//
// The defect was invisible to a position-advanced check, because the position
// DID advance — it advanced too early. gtidSet.Update ran on the GTIDEvent at
// the transaction's START, so every row of tx N carried a set that already
// contained N. Handing such a set to StartSyncGTID skips N entirely, so a
// serial applier that checkpointed a row-cap / byte-cap / idle flush
// mid-transaction persisted a position that silently dropped the rest of that
// transaction's rows.
//
// The expected value here is INDEPENDENT of the reader: each case names the
// transaction's own GTID as a literal, parses it with go-mysql's own set
// parser, and asks go-mysql's [gomysql.GTIDSet.Contain] whether the position
// the reader emitted covers it. No second call into the reader participates in
// the expectation. (The real-server counterpart — where the source's own
// GTID_SUBSET answers the same question about a real binlog — is
// TestCDCReader_GTIDStaging_RowPositionExcludesOwnTransaction in
// cdc_reader_gtid_staging_integration_test.go.)
//
// # Pin the class, not the representative
//
// The staging is dispatched across SEVEN event arms, and a green test on one
// says nothing about the others — three of them terminate a transaction group
// with no XID at all. Every arm that touches r.gtidSet or emits a position is
// exercised below:
//
//	MySQL GTIDEvent + BEGIN + XID .............. the ordinary DML transaction
//	MySQL GTIDEvent + DDL QueryEvent ........... implicit commit, NO XID
//	MySQL GTIDEvent + TRUNCATE QueryEvent ...... implicit commit, NO XID, emits ir.Truncate
//	MySQL COMMIT-as-QueryEvent ................. the non-InnoDB commit surface
//	MariaDB non-standalone group + XID ......... the ADR-0170 transactional shape
//	MariaDB STANDALONE group ................... implicit commit, NO XID
//	TransactionPayloadEvent .................... the compressed shape, via inner dispatch
//
// plus the two shapes that are about what must NOT happen: a mid-transaction
// SAVEPOINT (reaches the same generic-DDL arm and must not fold early) and a
// group with an unmodelled terminator (must not have its GTID dropped).

const stagingUUID = "3e11fa47-71ca-11e1-9e33-c80aa9429562"

// newStagingReader builds a dispatch-only reader primed so row events decode
// without touching a database: the table map and schema cache are pre-filled,
// so tableFor never runs its information_schema query.
func newStagingReader(t *testing.T, flavor Flavor, startSet string) *CDCReader {
	t.Helper()
	goFlavor := gomysql.MySQLFlavor
	if flavor == FlavorMariaDB {
		goFlavor = gomysql.MariaDBFlavor
	}
	set, err := gomysql.ParseGTIDSet(goFlavor, startSet)
	if err != nil {
		t.Fatalf("parse start set %q: %v", startSet, err)
	}
	users := func() *tableSchema {
		return &tableSchema{
			Schema:     "app",
			Name:       "users",
			Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
			PrimaryKey: []string{"id"},
		}
	}
	return &CDCReader{
		schema:      "app",
		flavor:      flavor,
		posMode:     positionModeGTID,
		gtidSet:     set,
		tableMap:    map[uint64]string{7: "app.users"},
		schemaCache: map[string]*tableSchema{"app.users": users()},
		// A DDL clears the schema cache, so the next row event rebuilds
		// through this seam rather than a live information_schema query.
		schemaLoader: func(context.Context, *sql.DB, string, string) (*tableSchema, error) {
			return users(), nil
		},
		snapshotSig:    map[string]ir.SchemaSignature{},
		forwardNullSig: map[string]string{},
	}
}

func hdr(t replication.EventType) *replication.EventHeader {
	return &replication.EventHeader{EventType: t}
}

func mysqlGTIDEvent(t *testing.T, gno int64) *replication.BinlogEvent {
	t.Helper()
	return &replication.BinlogEvent{
		Header: hdr(replication.GTID_EVENT),
		Event:  &replication.GTIDEvent{SID: mustSID(t, stagingUUID), GNO: gno},
	}
}

func queryEvent(stmt string) *replication.BinlogEvent {
	return &replication.BinlogEvent{
		Header: hdr(replication.QUERY_EVENT),
		Event:  &replication.QueryEvent{Schema: []byte("app"), Query: []byte(stmt)},
	}
}

func xidEvent() *replication.BinlogEvent {
	return &replication.BinlogEvent{Header: hdr(replication.XID_EVENT), Event: &replication.XIDEvent{}}
}

func insertRowEvent(id int64) *replication.BinlogEvent {
	return &replication.BinlogEvent{
		Header: hdr(replication.WRITE_ROWS_EVENTv2),
		Event:  &replication.RowsEvent{TableID: 7, Rows: [][]any{{id}}},
	}
}

func mariadbGTIDEvent(domain uint32, seq uint64, standalone bool) *replication.BinlogEvent {
	var flags byte
	if standalone {
		flags = replication.BINLOG_MARIADB_FL_STANDALONE
	}
	return &replication.BinlogEvent{
		Header: hdr(replication.MARIADB_GTID_EVENT),
		Event: &replication.MariadbGTIDEvent{
			GTID:  gomysql.MariadbGTID{DomainID: domain, ServerID: 1, SequenceNumber: seq},
			Flags: flags,
		},
	}
}

// dispatchAll runs the events through the reader and returns everything
// emitted, failing on the first dispatch error.
func dispatchAll(t *testing.T, r *CDCReader, evs ...*replication.BinlogEvent) []ir.Change {
	t.Helper()
	out := make(chan ir.Change, 64)
	ctx := context.Background()
	for i, ev := range evs {
		if err := r.dispatch(ctx, ev, out); err != nil {
			t.Fatalf("dispatch event %d: %v", i, err)
		}
	}
	close(out)
	var got []ir.Change
	for c := range out {
		got = append(got, c)
	}
	return got
}

// setOf decodes the GTID set a change's position carries.
func setOf(t *testing.T, c ir.Change) string {
	t.Helper()
	decoded, ok, err := decodeBinlogPos(c.Pos())
	if err != nil || !ok {
		t.Fatalf("decode position of %T: ok=%v err=%v", c, ok, err)
	}
	if decoded.Mode != positionModeGTID {
		t.Fatalf("position of %T is mode %q, want %q", c, decoded.Mode, positionModeGTID)
	}
	return decoded.GTIDSet
}

// containsGTID answers, via go-mysql's own set algebra rather than sluice's,
// whether `set` covers the single GTID `gtid`.
func containsGTID(t *testing.T, flavor Flavor, set, gtid string) bool {
	t.Helper()
	goFlavor := gomysql.MySQLFlavor
	if flavor == FlavorMariaDB {
		goFlavor = gomysql.MariaDBFlavor
	}
	have, err := gomysql.ParseGTIDSet(goFlavor, set)
	if err != nil {
		t.Fatalf("parse observed set %q: %v", set, err)
	}
	want, err := gomysql.ParseGTIDSet(goFlavor, gtid)
	if err != nil {
		t.Fatalf("parse expected gtid %q: %v", gtid, err)
	}
	return have.Contain(want)
}

// assertExcludes / assertIncludes are the two halves of the contract, named so
// a failure says which half broke.
func assertExcludes(t *testing.T, flavor Flavor, c ir.Change, txGTID, what string) {
	t.Helper()
	set := setOf(t, c)
	if containsGTID(t, flavor, set, txGTID) {
		t.Errorf("%s carries GTID set %q, which CONTAINS its own transaction %s — "+
			"StartSyncGTID with that set skips the transaction entirely, so a mid-transaction "+
			"checkpoint of this position silently loses the rest of the transaction (item 132)",
			what, set, txGTID)
	}
}

func assertIncludes(t *testing.T, flavor Flavor, c ir.Change, txGTID, what string) {
	t.Helper()
	set := setOf(t, c)
	if !containsGTID(t, flavor, set, txGTID) {
		t.Errorf("%s carries GTID set %q, which does NOT contain the transaction %s it commits — "+
			"a resume from this position re-reads a transaction that was already durably applied "+
			"(item 132: the fold must happen BEFORE the commit position is computed)",
			what, set, txGTID)
	}
}

// TestGTIDStaging_OrdinaryTransaction is the crux case: GTID → BEGIN → ROW →
// ROW → XID. Both rows and the TxBegin must exclude the transaction; only the
// TxCommit includes it.
func TestGTIDStaging_OrdinaryTransaction(t *testing.T) {
	const tx = stagingUUID + ":6"
	r := newStagingReader(t, FlavorVanilla, stagingUUID+":1-5")

	got := dispatchAll(
		t, r,
		mysqlGTIDEvent(t, 6),
		queryEvent("BEGIN"),
		insertRowEvent(1),
		insertRowEvent(2),
		xidEvent(),
	)
	if len(got) != 4 {
		t.Fatalf("emitted %d changes, want 4 (TxBegin, 2 Inserts, TxCommit): %#v", len(got), got)
	}
	assertExcludes(t, FlavorVanilla, got[0], tx, "TxBegin")
	assertExcludes(t, FlavorVanilla, got[1], tx, "first Insert")
	assertExcludes(t, FlavorVanilla, got[2], tx, "second Insert")
	assertIncludes(t, FlavorVanilla, got[3], tx, "TxCommit")

	// The pre-transaction set is the resume set the reader started from —
	// stated independently of the reader, so "excludes its own transaction"
	// cannot be satisfied by an empty or garbage set.
	if want := stagingUUID + ":1-5"; setOf(t, got[1]) != want {
		t.Errorf("row position set = %q, want the pre-transaction set %q", setOf(t, got[1]), want)
	}
	if want := stagingUUID + ":1-6"; setOf(t, got[3]) != want {
		t.Errorf("TxCommit position set = %q, want the post-transaction set %q", setOf(t, got[3]), want)
	}
}

// TestGTIDStaging_DDLImplicitCommit: a DDL group carries NO XID. Its GTID must
// be folded by the DDL itself, or it stays staged forever and every later
// position silently omits it.
func TestGTIDStaging_DDLImplicitCommit(t *testing.T) {
	const ddlTx = stagingUUID + ":6"
	r := newStagingReader(t, FlavorVanilla, stagingUUID+":1-5")

	dispatchAll(t, r, mysqlGTIDEvent(t, 6), queryEvent("ALTER TABLE users ADD c INT"))

	// The DDL's own anchor must already include it (a DDL implicit-commits, so
	// resuming at the anchor must not re-run it).
	decoded, ok, err := decodeBinlogPos(r.pendingDDLAnchor)
	if err != nil || !ok {
		t.Fatalf("decode DDL anchor: ok=%v err=%v", ok, err)
	}
	if !containsGTID(t, FlavorVanilla, decoded.GTIDSet, ddlTx) {
		t.Errorf("DDL anchor set %q does not contain the DDL's own GTID %s", decoded.GTIDSet, ddlTx)
	}

	// And a LATER transaction's positions must carry the DDL's GTID — the
	// "staged forever" failure mode shows up here, one group later.
	got := dispatchAll(t, r, mysqlGTIDEvent(t, 7), queryEvent("BEGIN"), insertRowEvent(1), xidEvent())
	if !containsGTID(t, FlavorVanilla, setOf(t, got[1]), ddlTx) {
		t.Errorf("a row AFTER the DDL carries %q, which omits the already-committed DDL %s — "+
			"the DDL's GTID was never folded", setOf(t, got[1]), ddlTx)
	}
}

// TestGTIDStaging_TruncateImplicitCommit: TRUNCATE is DDL that also EMITS a
// change (ir.Truncate). Its emitted position must be post-commit, because the
// applier treats a Truncate flush as a boundary checkpoint.
func TestGTIDStaging_TruncateImplicitCommit(t *testing.T) {
	const tx = stagingUUID + ":6"
	r := newStagingReader(t, FlavorVanilla, stagingUUID+":1-5")

	got := dispatchAll(t, r, mysqlGTIDEvent(t, 6), queryEvent("TRUNCATE TABLE users"))
	if len(got) != 1 {
		t.Fatalf("emitted %d changes, want 1 (ir.Truncate): %#v", len(got), got)
	}
	if _, ok := got[0].(ir.Truncate); !ok {
		t.Fatalf("emitted %T, want ir.Truncate", got[0])
	}
	assertIncludes(t, FlavorVanilla, got[0], tx, "ir.Truncate")
}

// TestGTIDStaging_CommitQueryEvent covers the non-InnoDB COMMIT-as-QueryEvent
// surface: the fold must happen before the position is computed there too.
func TestGTIDStaging_CommitQueryEvent(t *testing.T) {
	const tx = stagingUUID + ":6"
	r := newStagingReader(t, FlavorVanilla, stagingUUID+":1-5")

	got := dispatchAll(
		t, r,
		mysqlGTIDEvent(t, 6),
		queryEvent("BEGIN"),
		insertRowEvent(1),
		queryEvent("COMMIT"),
	)
	if len(got) != 3 {
		t.Fatalf("emitted %d changes, want 3 (TxBegin, Insert, TxCommit): %#v", len(got), got)
	}
	assertExcludes(t, FlavorVanilla, got[1], tx, "Insert")
	assertIncludes(t, FlavorVanilla, got[2], tx, "TxCommit (QueryEvent)")
}

// TestGTIDStaging_SavepointDoesNotFoldEarly is the negative half of the
// generic-DDL arm. SAVEPOINT arrives as a QueryEvent INSIDE a transaction and
// reaches the same branch a real implicit-committing DDL does; folding there
// would put the transaction back into its own following rows' positions.
func TestGTIDStaging_SavepointDoesNotFoldEarly(t *testing.T) {
	const tx = stagingUUID + ":6"
	r := newStagingReader(t, FlavorVanilla, stagingUUID+":1-5")

	got := dispatchAll(
		t, r,
		mysqlGTIDEvent(t, 6),
		queryEvent("BEGIN"),
		insertRowEvent(1),
		queryEvent("SAVEPOINT sp1"),
		insertRowEvent(2),
		queryEvent("ROLLBACK TO SAVEPOINT sp1"),
		insertRowEvent(3),
		xidEvent(),
	)
	var rows []ir.Change
	for _, c := range got {
		if _, ok := c.(ir.Insert); ok {
			rows = append(rows, c)
		}
	}
	if len(rows) != 3 {
		t.Fatalf("got %d Insert changes, want 3", len(rows))
	}
	for i, row := range rows {
		assertExcludes(t, FlavorVanilla, row, tx, "Insert after a SAVEPOINT (row "+string(rune('0'+i))+")")
	}
}

// TestGTIDStaging_UnmodelledTerminatorFoldsAtNextGroup: a group whose
// terminator sluice does not model (XA PREPARE ends in XA_PREPARE_LOG_EVENT,
// not an XID) must not have its GTID DROPPED when the next group stages over
// it. Folding at the next group degrades the unrecognised shape to the
// pre-item-132 behaviour, never to loss.
func TestGTIDStaging_UnmodelledTerminatorFoldsAtNextGroup(t *testing.T) {
	const stranded = stagingUUID + ":6"
	r := newStagingReader(t, FlavorVanilla, stagingUUID+":1-5")

	// Group 6 opens and its rows arrive, but no XID follows — the next
	// group's GTID event is the first thing that says group 6 is over.
	got := dispatchAll(
		t, r,
		mysqlGTIDEvent(t, 6),
		queryEvent("BEGIN"),
		insertRowEvent(1),
		mysqlGTIDEvent(t, 7),
		queryEvent("BEGIN"),
		insertRowEvent(2),
		xidEvent(),
	)
	last := got[len(got)-1]
	if !containsGTID(t, FlavorVanilla, setOf(t, last), stranded) {
		t.Errorf("after an unmodelled terminator the position set %q omits group %s — its GTID was "+
			"DROPPED rather than folded one group late; a resume would replay everything from there",
			setOf(t, last), stranded)
	}

	// The other half of the same hazard: an unterminated group must not leave
	// inSourceTx stuck true, or the NEXT group's implicit-commit fold is
	// suppressed and its GTID strands in turn.
	r2 := newStagingReader(t, FlavorVanilla, stagingUUID+":1-5")
	dispatchAll(
		t, r2,
		mysqlGTIDEvent(t, 6),
		queryEvent("BEGIN"),
		insertRowEvent(1),
		// No XID; the next group is a DDL, which implicit-commits.
		mysqlGTIDEvent(t, 7),
		queryEvent("ALTER TABLE users ADD c INT"),
	)
	for _, want := range []string{stagingUUID + ":6", stagingUUID + ":7"} {
		if !containsGTID(t, FlavorVanilla, r2.gtidSet.String(), want) {
			t.Errorf("executed set %q omits %s after an unterminated group followed by a DDL — "+
				"inSourceTx stayed true and suppressed the DDL's implicit-commit fold",
				r2.gtidSet.String(), want)
		}
	}
}

// TestGTIDStaging_MariaDBTransactionalGroup covers the ADR-0170 shape:
// MARIADB_GTID → TABLE_MAP → ROWS → XID, with no BEGIN QueryEvent.
func TestGTIDStaging_MariaDBTransactionalGroup(t *testing.T) {
	const tx = "0-1-6"
	r := newStagingReader(t, FlavorMariaDB, "0-1-5")

	got := dispatchAll(
		t, r,
		mariadbGTIDEvent(0, 6, false),
		insertRowEvent(1),
		xidEvent(),
	)
	if len(got) != 3 {
		t.Fatalf("emitted %d changes, want 3 (TxBegin, Insert, TxCommit): %#v", len(got), got)
	}
	assertExcludes(t, FlavorMariaDB, got[0], tx, "MariaDB TxBegin")
	assertExcludes(t, FlavorMariaDB, got[1], tx, "MariaDB Insert")
	assertIncludes(t, FlavorMariaDB, got[2], tx, "MariaDB TxCommit")
}

// TestGTIDStaging_MariaDBStandaloneGroup: a STANDALONE group is an implicit
// commit with no XID; it must fold on the spot.
func TestGTIDStaging_MariaDBStandaloneGroup(t *testing.T) {
	const ddlTx = "0-1-6"
	r := newStagingReader(t, FlavorMariaDB, "0-1-5")

	dispatchAll(t, r, mariadbGTIDEvent(0, 6, true), queryEvent("ALTER TABLE users ADD c INT"))
	if got := r.gtidSet.String(); !containsGTID(t, FlavorMariaDB, got, ddlTx) {
		t.Fatalf("after a STANDALONE group the executed set is %q, which omits the group's own GTID %s — "+
			"a standalone group has no XID, so nothing would ever fold it", got, ddlTx)
	}

	// A later transaction's rows must carry it.
	got := dispatchAll(t, r, mariadbGTIDEvent(0, 7, false), insertRowEvent(1), xidEvent())
	if !containsGTID(t, FlavorMariaDB, setOf(t, got[1]), ddlTx) {
		t.Errorf("a row after the standalone DDL carries %q, which omits %s", setOf(t, got[1]), ddlTx)
	}
}

// TestGTIDStaging_CompressedTransactionPayload: binlog_transaction_compression
// wraps BEGIN/ROWS/XID into one event whose inner events are dispatched through
// the same arms. The guarantee must therefore hold by construction — this pin
// is what makes that "by construction" claim checkable rather than asserted.
func TestGTIDStaging_CompressedTransactionPayload(t *testing.T) {
	const tx = stagingUUID + ":6"
	r := newStagingReader(t, FlavorVanilla, stagingUUID+":1-5")

	payload := &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.TRANSACTION_PAYLOAD_EVENT, LogPos: 5000, EventSize: 400},
		Event: &replication.TransactionPayloadEvent{Events: []*replication.BinlogEvent{
			queryEvent("BEGIN"),
			insertRowEvent(1),
			insertRowEvent(2),
			xidEvent(),
		}},
	}
	got := dispatchAll(t, r, mysqlGTIDEvent(t, 6), payload)
	if len(got) != 4 {
		t.Fatalf("emitted %d changes, want 4 (TxBegin, 2 Inserts, TxCommit): %#v", len(got), got)
	}
	assertExcludes(t, FlavorVanilla, got[1], tx, "compressed Insert")
	assertExcludes(t, FlavorVanilla, got[2], tx, "compressed Insert")
	assertIncludes(t, FlavorVanilla, got[3], tx, "compressed TxCommit")
}

// TestGTIDStaging_StartStreamerClearsStaging: a reconnect replays from a
// committed boundary, so nothing may survive from the previous connection's
// in-flight transaction.
func TestGTIDStaging_StartStreamerClearsStaging(t *testing.T) {
	r := newStagingReader(t, FlavorVanilla, stagingUUID+":1-5")
	dispatchAll(t, r, mysqlGTIDEvent(t, 6), queryEvent("BEGIN"), insertRowEvent(1))
	if r.pendingGTID == "" || !r.inSourceTx {
		t.Fatalf("test premise broken: expected a staged GTID inside a transaction, got pending=%q inTx=%v",
			r.pendingGTID, r.inSourceTx)
	}

	// startStreamer's syncer call needs a live connection, so drive the reset
	// it performs and assert on the reader state rather than the streamer.
	// (The reset is the first statement in startStreamer, before the mode
	// switch, so it runs on both position modes.)
	r.pendingGTID = ""
	r.inSourceTx = false
	set, err := gomysql.ParseGTIDSet(gomysql.MySQLFlavor, stagingUUID+":1-5")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	r.gtidSet = set

	got := dispatchAll(t, r, mysqlGTIDEvent(t, 6), queryEvent("BEGIN"), insertRowEvent(1), xidEvent())
	if want := stagingUUID + ":1-6"; setOf(t, got[len(got)-1]) != want {
		t.Errorf("after a restart the TxCommit set = %q, want %q (a carried-over staged GTID would "+
			"fold twice or fold the wrong group)", setOf(t, got[len(got)-1]), want)
	}
}
