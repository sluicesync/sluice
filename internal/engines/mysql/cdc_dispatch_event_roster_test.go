// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The binlog dispatch roster (audit 2026-09-01 SLM-3 gate b).
//
// Statement-format LOAD DATA was dropped silently because
// [CDCReader.dispatch] had no arm for EXECUTE_LOAD_QUERY and its default
// arm returns nil — "transient or unknown event types are quietly
// ignored". That default is the right shape for bookkeeping and the
// wrong shape for a write, and nothing asked which of go-mysql's event
// types was which. This gate does: the universe is every event type
// go-mysql names (derived from [replication.EventType.String], so a new
// constant in a go-mysql bump fails here until it is classified), and
// each one is APPLIED, REFUSED, or EXEMPT with a reason. The write-
// bearing types — rows events, query events, the LOAD DATA family, the
// compressed payload — must be applied or refused, and for each of those
// the test drives the real dispatcher with a synthesized event and also
// checks the type switch in cdc_reader.go names the Go type, so a removed
// arm fails twice.
//
// WHAT THIS GATE REACHES: [CDCReader.dispatch]'s type switch and its
// behaviour on one representative event per write-bearing Go type. It
// does not reach the VStream lane (a different event vocabulary; see
// TestVStreamStatementDML) and it does not grade what an APPLIED arm
// does with the event's contents — the per-arm tests own that.

package mysql

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

type dispatchClass int

const (
	dispatchApplied dispatchClass = iota // the event's write reaches the target (or its boundary/state is consumed)
	dispatchRefused                      // the event is a write sluice cannot apply and stops the stream loudly
	dispatchExempt                       // bookkeeping, or a write-bearing type whose refusal site is elsewhere (reason required)
)

type dispatchEntry struct {
	class  dispatchClass
	goType string // the *replication.X the type switch must name (applied/refused only)
	reason string
}

// dispatchEventRoster classifies every go-mysql event type. Keyed by the
// constant, not its String() — go-mysql's name for EXECUTE_LOAD_QUERY is
// misspelled ("ExectueLoadQueryEvent"), which is one more reason a
// name-derived roster would be the wrong instrument.
var dispatchEventRoster = map[replication.EventType]dispatchEntry{
	// Row events: the write path.
	replication.WRITE_ROWS_EVENTv0:                      {dispatchApplied, "RowsEvent", ""},
	replication.UPDATE_ROWS_EVENTv0:                     {dispatchApplied, "RowsEvent", ""},
	replication.DELETE_ROWS_EVENTv0:                     {dispatchApplied, "RowsEvent", ""},
	replication.WRITE_ROWS_EVENTv1:                      {dispatchApplied, "RowsEvent", ""},
	replication.UPDATE_ROWS_EVENTv1:                     {dispatchApplied, "RowsEvent", ""},
	replication.DELETE_ROWS_EVENTv1:                     {dispatchApplied, "RowsEvent", ""},
	replication.WRITE_ROWS_EVENTv2:                      {dispatchApplied, "RowsEvent", ""},
	replication.UPDATE_ROWS_EVENTv2:                     {dispatchApplied, "RowsEvent", ""},
	replication.DELETE_ROWS_EVENTv2:                     {dispatchApplied, "RowsEvent", ""},
	replication.PARTIAL_UPDATE_ROWS_EVENT:               {dispatchApplied, "RowsEvent", ""},
	replication.MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1:  {dispatchApplied, "RowsEvent", ""},
	replication.MARIADB_UPDATE_ROWS_COMPRESSED_EVENT_V1: {dispatchApplied, "RowsEvent", ""},
	replication.MARIADB_DELETE_ROWS_COMPRESSED_EVENT_V1: {dispatchApplied, "RowsEvent", ""},

	// Query events: DDL applied as boundary/cache state; statement DML refused.
	replication.QUERY_EVENT:                    {dispatchApplied, "QueryEvent", ""},
	replication.MARIADB_QUERY_COMPRESSED_EVENT: {dispatchApplied, "QueryEvent", "decompressed into a QueryEvent by go-mysql"},

	// Statement-format LOAD DATA (SLM-3 shape 1).
	replication.EXECUTE_LOAD_QUERY_EVENT: {dispatchRefused, "ExecuteLoadQueryEvent", ""},
	replication.BEGIN_LOAD_QUERY_EVENT: {
		dispatchExempt, "BeginLoadQueryEvent",
		"a block of the loaded file, with no schema and no statement; the EXECUTE_LOAD_QUERY that follows in the " +
			"same group is the refusal site, and a BEGIN with no EXECUTE is a load the server rolled back",
	},
	// The pre-5.0.3 LOAD DATA wire shapes, undecoded by go-mysql: refused rather than exempt-on-a-promise.
	replication.LOAD_EVENT:         {dispatchRefused, "GenericEvent", ""},
	replication.NEW_LOAD_EVENT:     {dispatchRefused, "GenericEvent", ""},
	replication.CREATE_FILE_EVENT:  {dispatchRefused, "GenericEvent", ""},
	replication.APPEND_BLOCK_EVENT: {dispatchRefused, "GenericEvent", ""},
	replication.EXEC_LOAD_EVENT:    {dispatchRefused, "GenericEvent", ""},
	replication.DELETE_FILE_EVENT:  {dispatchRefused, "GenericEvent", ""},

	// A compressed transaction: unpacked, inner events re-dispatched.
	replication.TRANSACTION_PAYLOAD_EVENT: {dispatchApplied, "TransactionPayloadEvent", ""},

	// Statement-format companions: each precedes the Query event it
	// parameterises and carries no write of its own; the Query event is
	// the refusal site.
	replication.INTVAR_EVENT:   {dispatchExempt, "", "statement-format companion (LAST_INSERT_ID / auto-increment seed); the Query event it precedes refuses"},
	replication.RAND_EVENT:     {dispatchExempt, "", "statement-format companion (RAND seed); the Query event it precedes refuses"},
	replication.USER_VAR_EVENT: {dispatchExempt, "", "statement-format companion (user variable); the Query event it precedes refuses"},

	// Row-event annotations: the rows follow as their own events.
	replication.ROWS_QUERY_EVENT:            {dispatchExempt, "", "the SQL text annotating a row event (binlog_rows_query_log_events); the rows follow"},
	replication.MARIADB_ANNOTATE_ROWS_EVENT: {dispatchExempt, "", "MariaDB's row-event annotation; the rows follow (and sluice's syncer does not request it)"},

	// Transaction / position bookkeeping.
	replication.XID_EVENT:                       {dispatchApplied, "XIDEvent", "commit boundary"},
	replication.GTID_EVENT:                      {dispatchApplied, "GTIDEvent", "position staging"},
	replication.ANONYMOUS_GTID_EVENT:            {dispatchApplied, "GTIDEvent", "position staging (anonymous)"},
	replication.MARIADB_GTID_EVENT:              {dispatchApplied, "MariadbGTIDEvent", "position staging + tx boundary"},
	replication.TABLE_MAP_EVENT:                 {dispatchApplied, "TableMapEvent", "table-id scope map"},
	replication.ROTATE_EVENT:                    {dispatchApplied, "RotateEvent", "current file"},
	replication.FORMAT_DESCRIPTION_EVENT:        {dispatchApplied, "FormatDescriptionEvent", "checksum algorithm for the trailer"},
	replication.PREVIOUS_GTIDS_EVENT:            {dispatchExempt, "", "file-open bookkeeping"},
	replication.MARIADB_GTID_LIST_EVENT:         {dispatchExempt, "", "file-open bookkeeping"},
	replication.MARIADB_BINLOG_CHECKPOINT_EVENT: {dispatchExempt, "", "MariaDB checkpoint bookkeeping"},
	replication.XA_PREPARE_LOG_EVENT:            {dispatchExempt, "", "XA prepare marker; the XA verb dispatch (CDCPOS-1) owns the refusal window at the XA START QueryEvent"},
	replication.TRANSACTION_CONTEXT_EVENT:       {dispatchExempt, "", "group replication bookkeeping"},
	replication.VIEW_CHANGE_EVENT:               {dispatchExempt, "", "group replication bookkeeping"},
	replication.GTID_TAGGED_LOG_EVENT:           {dispatchExempt, "", "tagged GTID (8.3+) — carries no write; position handling for tagged sets is outside this roster"},
	replication.HEARTBEAT_EVENT:                 {dispatchExempt, "", "liveness"},
	replication.HEARTBEAT_LOG_EVENT_V2:          {dispatchExempt, "", "liveness"},
	replication.STOP_EVENT:                      {dispatchExempt, "", "server shutdown marker"},
	replication.START_EVENT_V3:                  {dispatchExempt, "", "pre-5.0 file header"},
	replication.SLAVE_EVENT:                     {dispatchExempt, "", "never written by any server"},
	replication.IGNORABLE_EVENT:                 {dispatchExempt, "", "by definition"},
	replication.INCIDENT_EVENT:                  {dispatchExempt, "", "not a write: the source reports it LOST events (a real replica stops here); sluice ignores it today — filed as a follow-up by the SLM-3 sweep, out of this roster's scope"},
	replication.MARIADB_START_ENCRYPTION_EVENT:  {dispatchExempt, "", "encrypted-binlog header (go-mysql refuses encrypted streams upstream)"},
}

// writeBearingEventName reports whether an event type's go-mysql name
// says it can carry a write: rows, query, the LOAD DATA family, or the
// compressed payload. Derived from the name so a NEW write-bearing type
// that lands in go-mysql under a recognisable name cannot be classified
// exempt without a reason.
func writeBearingEventName(name string) bool {
	// The two ANNOTATION types carry the TEXT of a row event whose rows
	// follow as their own events; they are not writes, and their names
	// happen to contain the markers below.
	if strings.Contains(name, "RowsQuery") || strings.Contains(name, "AnnotateRows") {
		return false
	}
	for _, marker := range []string{
		"RowsEvent", "QueryEvent", "LoadQueryEvent", "LoadEvent", "CreateFileEvent",
		"AppendBlockEvent", "ExecLoadEvent", "DeleteFileEvent", "TransactionPayloadEvent",
	} {
		if strings.Contains(name, marker) {
			return true
		}
	}
	return false
}

// dispatchEventUniverse is every event type go-mysql can name, derived
// from the String() table rather than hand-listed.
func dispatchEventUniverse() []replication.EventType {
	var out []replication.EventType
	for v := 1; v < 256; v++ {
		et := replication.EventType(v)
		if et.String() != "UnknownEvent" {
			out = append(out, et)
		}
	}
	return out
}

func TestDispatchEventRoster_EveryWriteBearingType(t *testing.T) {
	t.Parallel()

	universe := dispatchEventUniverse()
	if len(universe) < 50 {
		t.Fatalf("derived %d event types from replication.EventType.String(); the derivation is broken (expected 50+)", len(universe))
	}

	// 1. Every named type is classified; every write-bearing one is
	//    applied or refused, or exempt WITH a reason; every exempt entry
	//    carries a reason.
	for _, et := range universe {
		entry, ok := dispatchEventRoster[et]
		if !ok {
			t.Errorf("event type %v (%d) is not classified in dispatchEventRoster — a go-mysql bump added it; decide "+
				"whether dispatch applies, refuses, or may ignore it, and say why", et, et)
			continue
		}
		if entry.class == dispatchExempt && entry.reason == "" {
			t.Errorf("event type %v is exempt with no reason", et)
		}
		if writeBearingEventName(et.String()) && entry.class == dispatchExempt && entry.goType == "" {
			t.Errorf("event type %v can carry a write by name and is exempt without naming the Go type whose arm "+
				"handles its class", et)
		}
	}
	for et := range dispatchEventRoster {
		if et.String() == "UnknownEvent" {
			t.Errorf("roster entry %d names an event type go-mysql no longer has", et)
		}
	}

	// 2. Anti-vacuity: the types this gate exists for, by name.
	for _, must := range []replication.EventType{
		replication.EXECUTE_LOAD_QUERY_EVENT, replication.BEGIN_LOAD_QUERY_EVENT, replication.QUERY_EVENT,
		replication.WRITE_ROWS_EVENTv2, replication.TRANSACTION_PAYLOAD_EVENT, replication.LOAD_EVENT,
	} {
		if e, ok := dispatchEventRoster[must]; !ok || (e.class == dispatchExempt && e.goType == "") {
			t.Errorf("%v must be rostered as applied/refused (or exempt naming its arm)", must)
		}
	}

	// 3. The type switch in cdc_reader.go names every Go type the roster
	//    relies on — a removed arm fails here before it fails behaviourally.
	arms := dispatchTypeSwitchArms(t)
	for et, entry := range dispatchEventRoster {
		if entry.goType == "" {
			continue
		}
		if !arms[entry.goType] {
			t.Errorf("%v is rostered on *replication.%s but CDCReader.dispatch has no case for that type", et, entry.goType)
		}
	}
	if len(arms) < 8 {
		t.Fatalf("found %d case arms in CDCReader.dispatch's type switch; the AST walk is not finding it", len(arms))
	}
}

// dispatchTypeSwitchArms returns the *replication.X type names cased in
// CDCReader.dispatch's type switch.
func dispatchTypeSwitchArms(t *testing.T) map[string]bool {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "cdc_reader.go", nil, 0)
	if err != nil {
		t.Fatalf("parse cdc_reader.go: %v", err)
	}
	arms := map[string]bool{}
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "dispatch" || fn.Recv == nil {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			cc, ok := n.(*ast.CaseClause)
			if !ok {
				return true
			}
			for _, expr := range cc.List {
				star, ok := expr.(*ast.StarExpr)
				if !ok {
					continue
				}
				if sel, ok := star.X.(*ast.SelectorExpr); ok {
					if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "replication" {
						arms[sel.Sel.Name] = true
					}
				}
			}
			return true
		})
	}
	return arms
}

// TestDispatchEventRoster_WriteBearingTypesBehave drives the real
// dispatcher with one event per write-bearing Go type and checks the
// roster's verdict is what happens: applied types produce a change or
// consume state without error; refused types return the coded refusal.
func TestDispatchEventRoster_WriteBearingTypesBehave(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const load = "LOAD DATA INFILE '/x' INTO TABLE users"

	probes := map[string]struct {
		ev   *replication.BinlogEvent
		want dispatchClass
	}{
		"RowsEvent":                {insertRowEvent(1), dispatchApplied},
		"QueryEvent/ddl":           {queryEvent("ALTER TABLE users ADD COLUMN x INT"), dispatchApplied},
		"QueryEvent/statement-dml": {queryEvent("INSERT INTO users VALUES (1)"), dispatchRefused},
		"ExecuteLoadQueryEvent":    {executeLoadQueryEvent(t, "app", load, nil, 0), dispatchRefused},
		"TransactionPayloadEvent/rows": {&replication.BinlogEvent{
			Header: &replication.EventHeader{EventType: replication.TRANSACTION_PAYLOAD_EVENT, LogPos: 900, EventSize: 100},
			Event:  &replication.TransactionPayloadEvent{Events: []*replication.BinlogEvent{insertRowEvent(2)}},
		}, dispatchApplied},
		"TransactionPayloadEvent/statement-dml": {&replication.BinlogEvent{
			Header: &replication.EventHeader{EventType: replication.TRANSACTION_PAYLOAD_EVENT, LogPos: 900, EventSize: 100},
			Event:  &replication.TransactionPayloadEvent{Events: []*replication.BinlogEvent{queryEvent("DELETE FROM users")}},
		}, dispatchRefused},
		"GenericEvent/legacy-load": {&replication.BinlogEvent{Header: hdr(replication.LOAD_EVENT), Event: &replication.GenericEvent{}}, dispatchRefused},
		"BeginLoadQueryEvent":      {&replication.BinlogEvent{Header: hdr(replication.BEGIN_LOAD_QUERY_EVENT), Event: &replication.BeginLoadQueryEvent{}}, dispatchExempt},
	}
	for name, p := range probes {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			r := newStagingReader(t, FlavorVanilla, "6a3175a8-0000-0000-0000-000000000000:1-4")
			out := make(chan ir.Change, 8)
			err := r.dispatch(ctx, p.ev, out)
			close(out)
			switch p.want {
			case dispatchRefused:
				ce, ok := sluicecode.FromError(err)
				if !ok || ce.Code != sluicecode.CodeCDCStatementDML {
					t.Fatalf("want %s; got %T: %v", sluicecode.CodeCDCStatementDML, err, err)
				}
				if n := len(drainChannel(out)); n != 0 {
					t.Fatalf("refused event emitted %d changes; want 0", n)
				}
			case dispatchApplied, dispatchExempt:
				if err != nil {
					t.Fatalf("dispatch = %v; want nil", err)
				}
				if strings.HasPrefix(name, "RowsEvent") || name == "TransactionPayloadEvent/rows" {
					if n := len(drainChannel(out)); n != 1 {
						t.Fatalf("applied row event emitted %d changes; want 1", n)
					}
				}
			}
		})
	}
}
