// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"sluicesync.dev/sluice/internal/ir"
)

const (
	collIDBin    = 46  // utf8mb4_bin (PAD SPACE)
	collIDCI     = 255 // utf8mb4_0900_ai_ci
	collID0900B  = 309 // utf8mb4_0900_bin (NO PAD)
	collIDASC    = 65  // ascii_bin
	collIDLatin1 = 8   // latin1_swedish_ci
)

func recordedCollationReader() *CDCReader {
	return &CDCReader{
		schema: "d",
		serverCollationIDNames: map[uint64]string{
			collIDBin: "utf8mb4_bin", collIDCI: "utf8mb4_0900_ai_ci", collID0900B: "utf8mb4_0900_bin",
			collIDASC: "ascii_bin", collIDLatin1: "latin1_swedish_ci",
		},
		serverCollations: map[uint64]string{
			collIDBin: "utf8mb4", collIDCI: "utf8mb4", collID0900B: "utf8mb4", collIDASC: "ascii", collIDLatin1: "latin1",
		},
	}
}

// kdShapeOf is d.kd (id, k) with k of type kType and k the primary key.
func kdShapeOf(kType ir.Type) *tableSchema {
	return &tableSchema{Schema: "d", Name: "kd", PrimaryKey: []string{"k"}, Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}},
		{Name: "k", Type: kType},
	}}
}

func kdShape(collation string) *tableSchema {
	return kdShapeOf(ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: collation})
}

// kdTableMapOf is a TABLE_MAP for (INT, k) with k's column type code.
func kdTableMapOf(code byte, meta uint16, kCollationID uint64) *replication.TableMapEvent {
	return &replication.TableMapEvent{
		ColumnCount:   2,
		ColumnType:    []byte{gomysql.MYSQL_TYPE_LONG, code},
		ColumnMeta:    []uint16{0, meta},
		ColumnCharset: []uint64{kCollationID},
	}
}

func kdTableMap(kCollationID uint64) *replication.TableMapEvent {
	return kdTableMapOf(gomysql.MYSQL_TYPE_VARCHAR, 80, kCollationID)
}

func kCollationOf(tbl *tableSchema) (charset, collation string) {
	switch v := tbl.Columns[1].Type.(type) {
	case ir.Varchar:
		return v.Charset, v.Collation
	case ir.Char:
		return v.Charset, v.Collation
	case ir.Text:
		return v.Charset, v.Collation
	}
	return "", ""
}

// TestStampRecordedCollations pins KEY-COLLATION-REVERTED-WINDOW-RESTORE and
// KEY-COLLATION-LOOSENED-WINDOW at the reader: a rows event read behind the
// head under a catalog that has since changed the key's collation is
// decoded and reported under the collation it RECORDED. The family matrix of
// [withCollation] (the Bug 74 rule): VARCHAR, CHAR and TEXT, a character-set
// change (utf8mb4 → ascii, utf8mb4 → latin1) and PAD SPACE against NO PAD.
func TestStampRecordedCollations(t *testing.T) {
	ctx := context.Background()
	r := recordedCollationReader()
	type kType func(collation, charset string) ir.Type
	varchar := func(coll, cs string) ir.Type { return ir.Varchar{Length: 20, Charset: cs, Collation: coll} }
	char := func(coll, cs string) ir.Type { return ir.Char{Length: 20, Charset: cs, Collation: coll} }
	text := func(coll, cs string) ir.Type { return ir.Text{Charset: cs, Collation: coll} }
	families := []struct {
		name string
		typ  kType
		code byte
		meta uint16
	}{
		{"varchar", varchar, gomysql.MYSQL_TYPE_VARCHAR, 80},
		{"char", char, gomysql.MYSQL_TYPE_STRING, uint16(gomysql.MYSQL_TYPE_STRING)<<8 | 80},
		{"text (a prefix key)", text, gomysql.MYSQL_TYPE_BLOB, 2},
	}
	cases := []struct {
		name                  string
		declared, declaredCS  string
		recorded              uint64
		wantCharset, wantColl string
		same                  bool
	}{
		{"reverted or loosened: written under _bin, catalog on _ci", "utf8mb4_0900_ai_ci", "utf8mb4", collIDBin, "utf8mb4", "utf8mb4_bin", false},
		{"tightened: written under _ci, catalog now _bin", "utf8mb4_bin", "utf8mb4", collIDCI, "utf8mb4", "utf8mb4_0900_ai_ci", false},
		{"PAD SPACE written, NO PAD declared", "utf8mb4_0900_bin", "utf8mb4", collIDBin, "utf8mb4", "utf8mb4_bin", false},
		{"NO PAD written, PAD SPACE declared", "utf8mb4_bin", "utf8mb4", collID0900B, "utf8mb4", "utf8mb4_0900_bin", false},
		{"character set: ascii written", "utf8mb4_bin", "utf8mb4", collIDASC, "ascii", "ascii_bin", false},
		{"character set: latin1 written", "utf8mb4_0900_ai_ci", "utf8mb4", collIDLatin1, "latin1", "latin1_swedish_ci", false},
		{"at the head: recorded equals the catalog", "utf8mb4_0900_ai_ci", "utf8mb4", collIDCI, "utf8mb4", "utf8mb4_0900_ai_ci", true},
		{"binary column", "utf8mb4_0900_ai_ci", "utf8mb4", binaryCollationID, "utf8mb4", "utf8mb4_0900_ai_ci", true},
		{"unnamed ID, the name table loaded", "utf8mb4_0900_ai_ci", "utf8mb4", 999, "utf8mb4", "utf8mb4_0900_ai_ci", true},
	}
	for _, fam := range families {
		for _, tc := range cases {
			t.Run(fam.name+"/"+tc.name, func(t *testing.T) {
				in := kdShapeOf(fam.typ(tc.declared, tc.declaredCS))
				got := r.stampRecordedCollations(ctx, in, kdTableMapOf(fam.code, fam.meta, tc.recorded))
				if (got == in) != tc.same {
					t.Fatalf("returned the input shape = %v; want %v", got == in, tc.same)
				}
				if cs, coll := kCollationOf(got); cs != tc.wantCharset || coll != tc.wantColl {
					t.Fatalf("k = %s/%s; want %s/%s", cs, coll, tc.wantCharset, tc.wantColl)
				}
				if _, coll := kCollationOf(in); coll != tc.declared {
					t.Fatalf("the input shape was modified: k = %s", coll)
				}
			})
		}
	}
	// No TABLE_MAP collations at all (MariaDB NO_LOG): the shape is the catalog's.
	in := kdShape("utf8mb4_bin")
	noLog := &replication.TableMapEvent{ColumnCount: 2, ColumnType: []byte{gomysql.MYSQL_TYPE_LONG, gomysql.MYSQL_TYPE_VARCHAR}, ColumnMeta: []uint16{0, 80}}
	if got := r.stampRecordedCollations(ctx, in, noLog); got != in {
		t.Fatal("a TABLE_MAP recording no collation changed the shape")
	}
	// ENUM and SET are exempt: their values are label indexes.
	enum := kdShapeOf(ir.Enum{Values: []string{"a", "b"}})
	if got := r.stampRecordedCollations(ctx, enum, kdTableMapOf(gomysql.MYSQL_TYPE_STRING, uint16(gomysql.MYSQL_TYPE_ENUM)<<8|1, collIDBin)); got != enum {
		t.Fatal("an ENUM column was stamped")
	}
}

// TestHoldUnannouncedCollation (review of d6ecb939, finding 2): a decoded
// collation that differs from the stream's last decoded shape with no
// logged DDL of that table read since and no first touch pending — a DDL run
// with sql_log_bin=0 — refuses, including after another table's DDL (which
// owes every table a boundary, so an owed boundary is no evidence); a DDL of
// the table, an unattributable DDL, a pending first touch, an excluded table
// and a table with no basis do not.
func TestHoldUnannouncedCollation(t *testing.T) {
	const qn = "d.kd"
	seededReader := func(collation string) *CDCReader {
		r := recordedCollationReader()
		r.SetDecodedShapeBaseline([]*ir.Table{{Schema: "d", Name: "kd", Columns: kdShape(collation).Columns}})
		r.seededBoundaries.start(ir.Position{}, true)
		return r
	}
	// newReader's basis is OBSERVED: the stream decoded a `_ci` row of kd.
	newReader := func() *CDCReader {
		r := seededReader("utf8mb4_0900_ai_ci")
		if err := r.holdDecodedOrder(qn, kdShape("utf8mb4_0900_ai_ci")); err != nil {
			t.Fatal(err)
		}
		return r
	}
	shape := kdShape("utf8mb4_bin")

	r := newReader()
	err := r.holdUnannouncedCollation(qn, shape)
	if !errors.Is(err, ir.ErrDeferredSchemaChange) || !strings.Contains(err.Error(), unannouncedCollationMarker) {
		t.Fatalf("unannounced change: err = %v; want %v naming %s", err, ir.ErrDeferredSchemaChange, unannouncedCollationMarker)
	}
	// A SEEDED basis is a catalog read, which a parent manifest takes at
	// window close — past its end position, so it can already show a LOGGED
	// change the stream has not reached. A row recorded before that DDL
	// differs from it and must not refuse (third review of a0878c3c; this
	// case was pinned as a refusal until then): it owes a boundary at the
	// stream's start instead, so the history carries the recorded shape.
	// The basis the row's decode leaves is observed, and checked from there.
	r = seededReader("utf8mb4_bin")
	if err := r.holdUnannouncedCollation(qn, kdShape("utf8mb4_0900_ai_ci")); err != nil {
		t.Fatalf("a row recorded before a logged change the seeded basis already shows: err = %v", err)
	}
	if !r.seededBoundaries.take(qn) {
		t.Fatal("a row differing from the seeded basis owes no boundary; its recorded shape would reach no history")
	}
	if err := r.holdDecodedOrder(qn, kdShape("utf8mb4_0900_ai_ci")); err != nil {
		t.Fatal(err)
	}
	if err := r.holdUnannouncedCollation(qn, shape); !errors.Is(err, ir.ErrDeferredSchemaChange) {
		t.Fatalf("an unlogged change after the first observed row: err = %v; want %v", err, ir.ErrDeferredSchemaChange)
	}
	// No start anchor, or a shape that is never a boundary: no boundary
	// can be owed, so it refuses as before.
	r = seededReader("utf8mb4_bin")
	r.seededBoundaries = seededCollationBoundaries{}
	if err := r.holdUnannouncedCollation(qn, kdShape("utf8mb4_0900_ai_ci")); !errors.Is(err, ir.ErrDeferredSchemaChange) {
		t.Fatalf("seeded mismatch with no anchor: err = %v; want %v", err, ir.ErrDeferredSchemaChange)
	}
	r = seededReader("utf8mb4_bin")
	synth := kdShape("utf8mb4_0900_ai_ci")
	synth.synthesized = true
	if err := r.holdUnannouncedCollation(qn, synth); !errors.Is(err, ir.ErrDeferredSchemaChange) {
		t.Fatalf("seeded mismatch on a deferred shape: err = %v; want %v", err, ir.ErrDeferredSchemaChange)
	}
	// An owed boundary alone is no evidence: every logged DDL owes one for
	// every table the reader knows.
	r = newReader()
	r.owedSchemaBoundary = map[string]struct{}{qn: {}}
	if err := r.holdUnannouncedCollation(qn, shape); !errors.Is(err, ir.ErrDeferredSchemaChange) {
		t.Fatalf("owed boundary from another table's DDL: err = %v; want %v", err, ir.ErrDeferredSchemaChange)
	}
	r = newReader()
	r.firstTouch.armed = true
	r.firstTouch.start(ir.Position{})
	if err := r.holdUnannouncedCollation(qn, shape); err != nil {
		t.Fatalf("first touch pending: err = %v", err)
	}
	// A logged DDL of THIS table read since the basis (a table not yet
	// cached owes no boundary by name) announces the change.
	r = newReader()
	r.ddlGen.note(classifyDDLReach("ALTER TABLE kd MODIFY k VARCHAR(20) COLLATE utf8mb4_bin NOT NULL", "d"))
	if err := r.holdUnannouncedCollation(qn, shape); err != nil {
		t.Fatalf("a DDL of the table read since the basis: err = %v", err)
	}
	// A DDL the reader cannot bound to named tables counts for every table.
	r = newReader()
	r.ddlGen.note(ddlReach{all: true})
	if err := r.holdUnannouncedCollation(qn, shape); err != nil {
		t.Fatalf("an unattributable DDL read since the basis: err = %v", err)
	}
	// A routine ALTER of ANOTHER table does not (review of the DDL-since-
	// basis fix): an unlogged change of kd after it still refuses.
	r = newReader()
	r.ddlGen.note(classifyDDLReach("ALTER TABLE other ADD COLUMN x INT", "d"))
	if err := r.holdUnannouncedCollation(qn, shape); !errors.Is(err, ir.ErrDeferredSchemaChange) {
		t.Fatalf("after another table's DDL: err = %v; want %v", err, ir.ErrDeferredSchemaChange)
	}
	r = newReader()
	r.scopeAllowed = func(_, _ string) bool { return false }
	if err := r.holdUnannouncedCollation(qn, shape); err != nil {
		t.Fatalf("excluded table: err = %v", err)
	}
	if err := recordedCollationReader().holdUnannouncedCollation(qn, shape); err != nil {
		t.Fatalf("no basis: err = %v", err)
	}
	if err := newReader().holdUnannouncedCollation(qn, kdShape("utf8mb4_0900_ai_ci")); err != nil {
		t.Fatalf("no change: err = %v", err)
	}
}

// TestRefuseUnnamedCollations (review of d6ecb939, finding 3; third review of
// a0878c3c, finding 2): a recorded collation ID the reader cannot name — the
// source's collation-name table unreadable, or read and lacking the ID —
// refuses (retryably) on ANY character column, not only the primary key:
// the reader does not know the table's UNIQUE keys, so a key-only check
// left a UNIQUE key's reverted window to merge.
func TestRefuseUnnamedCollations(t *testing.T) {
	db, err := sql.Open("mysql", "u:p@tcp(127.0.0.1:1)/d")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	r := &CDCReader{schema: "d", db: db}
	err = r.refuseUnnamedCollations(context.Background(), "d.kd", kdShape("utf8mb4_0900_ai_ci"), kdTableMap(collIDBin))
	if err == nil || !strings.Contains(err.Error(), collationNamesUnavailableMarker) {
		t.Fatalf("err = %v; want %s", err, collationNamesUnavailableMarker)
	}
	if errors.Is(err, ir.ErrDeferredSchemaChange) {
		t.Fatal("the refusal is terminal; it must be retryable (the read is retried)")
	}
	// A column outside the primary key (a UNIQUE key's, which the reader
	// cannot see) refuses the same way.
	nonPK := kdShape("utf8mb4_0900_ai_ci")
	nonPK.PrimaryKey = []string{"id"}
	if err := r.refuseUnnamedCollations(context.Background(), "d.kd", nonPK, kdTableMap(collIDBin)); err == nil || !strings.Contains(err.Error(), collationNamesUnavailableMarker) {
		t.Fatalf("a column outside the primary key: err = %v; want %s", err, collationNamesUnavailableMarker)
	}
	r.serverCollationIDNames = map[uint64]string{collIDBin: "utf8mb4_bin"}
	if err := r.refuseUnnamedCollations(context.Background(), "d.kd", kdShape("utf8mb4_0900_ai_ci"), kdTableMap(collIDBin)); err != nil {
		t.Fatalf("names loaded: err = %v", err)
	}
	// The table loaded but lacking the recorded ID: nothing would be stamped.
	err = r.refuseUnnamedCollations(context.Background(), "d.kd", nonPK, kdTableMap(999))
	if err == nil || !strings.Contains(err.Error(), collationNamesUnavailableMarker) || !strings.Contains(err.Error(), "does not name it") {
		t.Fatalf("an ID the loaded table lacks: err = %v; want %s", err, collationNamesUnavailableMarker)
	}
	// Exempt: a binary column, and a column type with no collation.
	if err := r.refuseUnnamedCollations(context.Background(), "d.kd", nonPK, kdTableMap(binaryCollationID)); err != nil {
		t.Fatalf("binary: err = %v", err)
	}
	// A reader with no handle and no table loaded is a test seam.
	if err := (&CDCReader{schema: "d"}).refuseUnnamedCollations(context.Background(), "d.kd", nonPK, kdTableMap(999)); err != nil {
		t.Fatalf("test seam: err = %v", err)
	}
}
