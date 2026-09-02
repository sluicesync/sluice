// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"strings"
	"testing"

	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestVStreamStatementDML pins the VStream lane's half of the
// STATEMENT-DML belt (audit 2026-09-01 SLM-3 sibling sweep) on every
// dispatcher a statement-DML VEvent can reach: the standalone reader,
// the snapshot stream's post-COPY CDC tail, and the snapshot stream's
// COPY phase. Each of the four VEvent types the vendored vstreamer emits
// for statement-format DML must refuse with the coded error, the SQL's
// values withheld; the bookkeeping types around them must keep flowing.
//
// WHAT THIS REACHES: sluice's dispatchers, fed synthesized VEvents. It
// does not prove the vendored vstreamer emits these types — that is
// read from vstreamer.go:613-656 (v0.24.2) and recorded in
// cdc_vstream_statement_dml.go; a Vitess bump that changed it would
// change the population, not the correctness of refusing what arrives.
// Statement-format LOAD DATA never arrives (vstreamer's IsQuery() is
// QUERY_EVENT-only) and is exempt upstream — stated here, not pinned.
func TestVStreamStatementDML(t *testing.T) {
	ctx := context.Background()
	fields := map[string][]*query.Field{
		fieldCacheKey("-", "users"): {{Name: "id", Type: query.Type_INT64}},
	}
	statementEv := func(typ binlogdata.VEventType, dml string) *binlogdata.VEvent {
		return &binlogdata.VEvent{Type: typ, Keyspace: "main", Shard: "-", Dml: dml}
	}
	cells := map[binlogdata.VEventType]string{
		binlogdata.VEventType_INSERT:  "INSERT INTO users VALUES (1,'alice@example.com')",
		binlogdata.VEventType_REPLACE: "REPLACE INTO users VALUES (1,'alice@example.com')",
		binlogdata.VEventType_UPDATE:  "UPDATE users SET email = 'alice@example.com' WHERE id = 1",
		binlogdata.VEventType_DELETE:  "DELETE FROM users WHERE email = 'alice@example.com'",
	}
	wantRefusal := func(t *testing.T, err error, verb string) {
		t.Helper()
		if err == nil {
			t.Fatal("dispatch of a statement-DML VEvent = nil; want the coded refusal (the old default arm's silent drop)")
		}
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeCDCStatementDML {
			t.Fatalf("want %s; got %T: %v", sluicecode.CodeCDCStatementDML, err, err)
		}
		msg := err.Error()
		for _, want := range []string{"a " + verb + " statement", `"main"`, "vgtid "} {
			if !strings.Contains(msg, want) {
				t.Errorf("refusal missing %q: %v", want, err)
			}
		}
		if strings.Contains(msg, "alice@example.com") || strings.Contains(ce.Hint, "alice@example.com") {
			t.Errorf("refusal leaked the statement's value: %v", err)
		}
	}
	vgtid := []shardGtid{{Keyspace: "main", Shard: "-", Gtid: "MySQL56/6a3175a8-0000-0000-0000-000000000000:1-4"}}

	dispatchers := map[string]func(ev *binlogdata.VEvent) error{
		"vstreamCDCReader.dispatch": func(ev *binlogdata.VEvent) error {
			r := &vstreamCDCReader{keyspace: "main", shards: []string{"-"}, fields: fields, currentVgtid: vgtid}
			return r.dispatch(ctx, ev, make(chan ir.Change, 4))
		},
		"vstreamSnapshotStream.dispatchCDCEvent": func(ev *binlogdata.VEvent) error {
			s := &vstreamSnapshotStream{keyspace: "main", fields: fields, currentVgtid: vgtid}
			return s.dispatchCDCEvent(ctx, ev, make(chan ir.Change, 4))
		},
		"vstreamSnapshotStream.dispatchCopyEvent": func(ev *binlogdata.VEvent) error {
			s := &vstreamSnapshotStream{keyspace: "main", fields: fields, currentVgtid: vgtid}
			_, err := s.dispatchCopyEvent(ev)
			return err
		},
	}
	for dname, dispatch := range dispatchers {
		for typ, dml := range cells {
			t.Run(dname+"/"+typ.String(), func(t *testing.T) {
				wantRefusal(t, dispatch(statementEv(typ, dml)), vstreamStatementDMLVerbs[typ])
			})
		}
		t.Run(dname+"/bookkeeping_keeps_flowing", func(t *testing.T) {
			for _, typ := range []binlogdata.VEventType{
				binlogdata.VEventType_BEGIN, binlogdata.VEventType_COMMIT, binlogdata.VEventType_OTHER,
				binlogdata.VEventType_HEARTBEAT, binlogdata.VEventType_SET, binlogdata.VEventType_GTID,
			} {
				if err := dispatch(&binlogdata.VEvent{Type: typ, Keyspace: "main", Shard: "-"}); err != nil {
					t.Fatalf("%v: dispatch = %v; want nil", typ, err)
				}
			}
		})
	}

	// Anti-vacuity: the verb table names every statement type the
	// vstreamer emits, and only those.
	if len(vstreamStatementDMLVerbs) != 4 {
		t.Fatalf("vstreamStatementDMLVerbs has %d entries; want the vstreamer's four (INSERT/REPLACE/UPDATE/DELETE)", len(vstreamStatementDMLVerbs))
	}
	// The no-position shape says so rather than inventing a coordinate.
	err := (&vstreamCDCReader{keyspace: "main", fields: fields}).dispatch(ctx, statementEv(binlogdata.VEventType_INSERT, "INSERT INTO users VALUES (1)"), make(chan ir.Change, 1))
	if err == nil || !strings.Contains(err.Error(), "no VGTID observed yet") {
		t.Errorf("refusal before any VGTID = %v; want it to say no coordinate is available", err)
	}
}
