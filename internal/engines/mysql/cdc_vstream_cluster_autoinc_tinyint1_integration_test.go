//go:build integration && vitesscluster

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// PREM-VSTREAM-AUTOINC — the LIVE-WIRE half of the auto-increment
// TINYINT(1) premise (audit-2026-08-19 value-fidelity follow-up),
// ground-truthed against a REAL multi-process Vitess cluster.
//
// The premise, named UNVERIFIED at [mysqlFlagAutoIncrement]: sluice
// classifies a MySQL/Vitess `tinyint(1)` column as ir.Boolean UNLESS the
// column carries the AUTO_INCREMENT flag — an auto-increment `tinyint(1)`
// PK is an integer key (values 1, 2, 3, …), not a bool, so collapsing it
// to bool would corrupt every value past 1. On the VStream lane sluice
// reads that distinction from the FIELD event's MySQL flag bit 512
// ([mysqlFlagAutoIncrement], AUTO_INCREMENT_FLAG). The code-scoped binding
// (TestMySQLFlagAutoIncrementBoundToVitessCarrier) proves sluice READS bit
// 512 and that the carrier DEFINES bit 512 — but NOT that a live vtgate
// POPULATES bit 512 on the wire for an auto-increment column. That is the
// half only a real cluster can settle, and this is that test.
//
// Why the round-trip IS the ground truth (no synthetic flag anywhere): the
// auto-inc PK is seeded with values 2 and 3 — past bool's {0,1}. Both the
// COPY and the CDC value decode run through the SAME authority
// ([vstreamTinyint1IsBool] → the FIELD event's live Flags):
//
//   - premise HOLDS (vtgate sets bit 512): the auto-inc column classifies
//     as ir.Integer, so 2 and 3 decode as their numbers and round-trip.
//   - premise FAILS (bit 512 absent): the SAME column classifies as bool,
//     value 2 decodes to `true`, and the decode-time range guard REFUSES
//     it LOUDLY (SLUICE-E-VALUE-TINYINT1-RANGE) — never a silent collapse
//     (the SOFT-dependency shape the premise comment predicts). So a
//     failing premise turns THIS test's COPY drain into a coded refusal,
//     not a wrong-but-green pass.
//
// The anti-vacuity control is a PLAIN (non-auto-inc) `tinyint(1)` column in
// the same table seeded with {0,1}: it MUST classify as bool. If it decoded
// as an integer the classifier would be ignoring the flag entirely (blanket
// integer), and the auto-inc cell would prove nothing — so the control is
// what makes the auto-inc assertion load-bearing.
//
// Run (heavy — own build tag, NOT in the per-PR gate; weekly cluster job's
// `-run 'TestVitessCluster'` covers it):
//
//	$env:PATH += ";C:\Program Files\Rancher Desktop\resources\resources\win32\bin"
//	$env:TESTCONTAINERS_RYUK_DISABLED = "true"
//	go test -tags='integration vitesscluster' -v -count=1 -timeout=20m \
//	  -run 'TestVitessClusterAutoIncTinyint1' ./internal/engines/mysql/...

package mysql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// autoIncTinyint1DDL seeds a table with BOTH tinyint(1) shapes the premise
// distinguishes: an AUTO_INCREMENT PK (`id` — an integer key, values grow
// 1,2,3,…) and a PLAIN tinyint(1) (`flag_col` — MySQL's canonical bool).
const autoIncTinyint1DDL = `
	CREATE TABLE ai_tinyint1 (
		id       TINYINT(1)  NOT NULL AUTO_INCREMENT,
		flag_col TINYINT(1)  NOT NULL DEFAULT 0,
		label    VARCHAR(32) NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

// autoIncTinyint1Table describes ai_tinyint1 for the cold-start COPY drain.
// The declared column types do NOT drive decoding (the VStream FIELD event's
// wire type + Flags do — see [decodeVStreamCell]); only table.Name is
// load-bearing here. The types are declared honestly for documentation: the
// auto-inc PK is an integer, the plain column a bool.
func autoIncTinyint1Table() *ir.Table {
	return &ir.Table{
		Name: "ai_tinyint1",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 8}},
			{Name: "flag_col", Type: ir.Boolean{}},
			{Name: "label", Type: ir.Varchar{Length: 32}},
		},
	}
}

// TestVitessClusterAutoIncTinyint1PreservesInteger is the PREM-VSTREAM-AUTOINC
// live-wire gate. It settles whether a live vtgate populates AUTO_INCREMENT_FLAG
// (bit 512) on the VStream FIELD event, by proving the value-fidelity behavior
// that depends on it: an auto-increment tinyint(1) PK round-trips as an INTEGER
// (values past 1 preserved), while a plain tinyint(1) classifies as bool.
func TestVitessClusterAutoIncTinyint1PreservesInteger(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVitessCluster(t)
	defer cleanup()

	applyClusterSQL(t, mysqlDSN, autoIncTinyint1DDL)
	// Let the tablet's schema engine register the table before the VStream
	// FieldEvent (column-type metadata + Flags) is needed.
	time.Sleep(3 * time.Second)

	// Seed BEFORE the snapshot so the auto-inc PK grows past bool's {0,1}:
	//   label=row-1 -> id=1, flag_col=1 (true)
	//   label=row-2 -> id=2, flag_col=0 (false)   <- id past 1
	//   label=row-3 -> id=3, flag_col=1 (true)    <- id past 1
	// flag_col carries BOTH {0,1} so the bool control is genuine, not vacuous.
	applyClusterSQL(t, mysqlDSN+"&multiStatements=true", `
		INSERT INTO ai_tinyint1 (flag_col, label) VALUES
			(1, 'row-1'),
			(0, 'row-2'),
			(1, 'row-3')`)
	time.Sleep(2 * time.Second)

	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0&vstream_tablet_type=primary",
		mysqlDSN, grpcEndpoint,
	)
	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	fo, ok := any(eng).(ir.FilteredSnapshotOpener)
	if !ok {
		t.Fatal("Engine{Flavor: FlavorPlanetScale} must implement ir.FilteredSnapshotOpener")
	}
	// nil filters == an unfiltered snapshot+CDC open (the plain sync path).
	stream, err := fo.OpenSnapshotStreamForTablesFiltered(ctx, sluiceDSN, []string{"ai_tinyint1"}, nil)
	if err != nil {
		t.Fatalf("OpenSnapshotStreamForTablesFiltered: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// (1) COLD-START COPY. If the premise FAILS, the auto-inc id=2/id=3 rows
	// classify as bool and the range guard turns THIS drain into a coded
	// SLUICE-E-VALUE-TINYINT1-RANGE refusal — surfaced on stream.Rows.Err().
	rowsCh, err := stream.Rows.ReadRows(ctx, autoIncTinyint1Table())
	if err != nil {
		t.Fatalf("ReadRows(ai_tinyint1): %v", err)
	}
	type decoded struct {
		id     int64
		idKind string // concrete Go type of the id cell
		flag   any
	}
	copied := map[string]decoded{}
	for row := range rowsCh {
		label := asStringVal(row["label"])
		id, isInt := asInt64Val(row["id"])
		if !isInt {
			// id decoded as something other than an integer — the premise-
			// failure shape (a bool) lands here if the range guard did not
			// already refuse (e.g. a value of exactly 1).
			t.Errorf("COPY row label=%q: auto-inc `id` decoded as %T (%#v), not an integer — "+
				"the live FIELD event did NOT carry AUTO_INCREMENT_FLAG (bit 512), so sluice collapsed the "+
				"integer PK to bool (PREM-VSTREAM-AUTOINC FAILS)", label, row["id"], row["id"])
		}
		copied[label] = decoded{id: id, idKind: fmt.Sprintf("%T", row["id"]), flag: row["flag_col"]}
	}
	if err := stream.Rows.Err(); err != nil {
		if ce, isCoded := sluicecode.FromError(err); isCoded && ce.Code == sluicecode.CodeValueTinyint1Range {
			t.Fatalf("COPY drain REFUSED with %s — the live vtgate did NOT populate AUTO_INCREMENT_FLAG (bit 512) "+
				"on the FIELD event, so the auto-inc tinyint(1) PK (values 2,3) was classified as bool and the "+
				"decode-time range guard refused it. PREM-VSTREAM-AUTOINC FAILS on this cluster: %v", ce.Code, err)
		}
		t.Fatalf("snapshot COPY error after drain: %v", err)
	}

	// The auto-inc PK values must be preserved EXACTLY, past 1.
	for label, want := range map[string]int64{"row-1": 1, "row-2": 2, "row-3": 3} {
		got, ok := copied[label]
		if !ok {
			t.Fatalf("COPY missing row label=%q; copied=%v", label, copied)
		}
		if got.id != want {
			t.Errorf("COPY row label=%q: id=%d (kind %s); want %d — auto-inc value not preserved", label, got.id, got.idKind, want)
		}
	}

	// (2) ANTI-VACUITY CONTROL: the PLAIN tinyint(1) `flag_col` MUST decode as
	// a Go bool. If it decoded as an integer, the classifier would be ignoring
	// the flag entirely and the auto-inc assertion above would be vacuous.
	wantFlag := map[string]bool{"row-1": true, "row-2": false, "row-3": true}
	for label, want := range wantFlag {
		got := copied[label]
		b, isBool := got.flag.(bool)
		if !isBool {
			t.Errorf("ANTI-VACUITY: plain tinyint(1) `flag_col` for label=%q decoded as %T (%#v), not a bool — "+
				"the classifier is not treating a plain tinyint(1) as MySQL's canonical bool, so the auto-inc "+
				"integer cell proves nothing", label, got.flag, got.flag)
			continue
		}
		if b != want {
			t.Errorf("plain tinyint(1) `flag_col` for label=%q = %v; want %v", label, b, want)
		}
	}
	t.Logf("COPY PASS: auto-inc tinyint(1) PK preserved as integer past 1 (%v); plain tinyint(1) classified as bool",
		map[string]int64{"row-1": copied["row-1"].id, "row-2": copied["row-2"].id, "row-3": copied["row-3"].id})

	// (3) CDC: resume from the COPY_COMPLETED position and drive an INSERT
	// (id=4) plus an UPDATE (of id=2 — a value past 1). Both exercise the CDC
	// value decode of the auto-inc column through the SAME live-flag authority.
	catchup, err := stream.Changes.StreamChanges(ctx, stream.Position)
	if err != nil {
		t.Fatalf("Changes.StreamChanges: %v", err)
	}
	time.Sleep(3 * time.Second)

	applyClusterSQL(t, mysqlDSN, "INSERT INTO ai_tinyint1 (flag_col, label) VALUES (0, 'row-4')")               // id=4
	applyClusterSQL(t, mysqlDSN, "UPDATE ai_tinyint1 SET flag_col = 1, label = 'row-2b' WHERE label = 'row-2'") // id=2 update

	got := drainAiTinyint1Changes(t, ctx, catchup, 2, 90*time.Second)

	sawInsert4, sawUpdate2 := false, false
	for _, ch := range got {
		switch e := ch.(type) {
		case ir.Insert:
			if e.Table != "ai_tinyint1" {
				continue
			}
			id, isInt := asInt64Val(e.Row["id"])
			if !isInt {
				t.Errorf("CDC INSERT: auto-inc `id` decoded as %T (%#v), not an integer — bit 512 absent on the CDC FIELD event", e.Row["id"], e.Row["id"])
				continue
			}
			if id == 4 {
				sawInsert4 = true
				if _, isBool := e.Row["flag_col"].(bool); !isBool {
					t.Errorf("CDC INSERT id=4: plain tinyint(1) flag_col decoded as %T, not bool", e.Row["flag_col"])
				}
			}
		case ir.Update:
			if e.Table != "ai_tinyint1" {
				continue
			}
			id, isInt := asInt64Val(e.After["id"])
			if !isInt {
				t.Errorf("CDC UPDATE: auto-inc after `id` decoded as %T (%#v), not an integer", e.After["id"], e.After["id"])
				continue
			}
			if id == 2 {
				sawUpdate2 = true // an auto-inc value past 1 survived the CDC decode
			}
		}
	}
	if !sawInsert4 {
		t.Errorf("CDC INSERT of auto-inc id=4 never arrived among %d changes (%s)", len(got), changeKinds(got))
	}
	if !sawUpdate2 {
		t.Errorf("CDC UPDATE of auto-inc id=2 never arrived among %d changes (%s)", len(got), changeKinds(got))
	}

	if err := stream.Changes.(interface{ Err() error }).Err(); err != nil {
		t.Fatalf("CDC stream errored: %v", err)
	}
	t.Log("CDC PASS: auto-inc tinyint(1) INSERT (id=4) and UPDATE (id=2, value past 1) decoded as integers; " +
		"PREM-VSTREAM-AUTOINC CONFIRMED — the live vtgate populates AUTO_INCREMENT_FLAG on the VStream wire")
}

// drainAiTinyint1Changes collects `want` row-bearing changes on ai_tinyint1
// (Insert/Update/Delete), ignoring Tx boundaries and schema snapshots, or
// fails on close/timeout/cancel.
func drainAiTinyint1Changes(t *testing.T, ctx context.Context, changes <-chan ir.Change, want int, timeout time.Duration) []ir.Change {
	t.Helper()
	var got []ir.Change
	rowBearing := 0
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for rowBearing < want {
		select {
		case ch, ok := <-changes:
			if !ok {
				t.Fatalf("CDC stream closed early after %d row-bearing changes; want %d (%s)", rowBearing, want, changeKinds(got))
			}
			switch ch.(type) {
			case ir.Insert, ir.Update, ir.Delete:
				got = append(got, ch)
				rowBearing++
			default:
				// Tx boundaries / schema snapshots — not counted.
			}
		case <-deadline.C:
			t.Fatalf("timed out after %v with %d/%d row-bearing changes (%s)", timeout, rowBearing, want, changeKinds(got))
		case <-ctx.Done():
			t.Fatalf("context done draining changes (%d/%d): %v", rowBearing, want, ctx.Err())
		}
	}
	return got
}
