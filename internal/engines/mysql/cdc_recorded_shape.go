// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # Decoding a row event by the shape it was RECORDED under (Bug 301)
//
// With binlog_row_metadata=FULL every TABLE_MAP event names its columns, in
// the order the row image carries them. That is the server's own statement
// of the shape a row was written under, at the row's position — exactly
// what the catalog cannot give a reader that is behind the binlog head (see
// cdc_ddl_lookahead.go). So under FULL the reader stops trusting the
// catalog's column LIST and takes it from the event:
//
//   - each recorded column is matched BY NAME to a column sluice has read
//     for this table — the current catalog first, then every earlier shape
//     this stream decoded with — and accepted only if its type decodes the
//     recorded value exactly as the recorded type does
//     ([recordedColumnAccepts]);
//   - a recorded column no known shape can type (dropped since, or retyped
//     since) is typed from the TABLE_MAP itself through the same
//     [translateType] the catalog path uses ([recordedColumnMeta]); a
//     family the TABLE_MAP cannot pin down (a signed TINYINT — TINYINT(1)
//     or not; a spatial column — its SRID; a MariaDB binary column — native
//     UUID/INET or not) refuses loudly, naming the column;
//   - the primary key is the event's own (FULL records it).
//
// The decoded row carries the RECORDED column names, so it means at its
// position what the source meant: a reorder or a rename swap decodes every
// value under the name it was written under, a pre-ADD COLUMN row lacks the
// column, a pre-DROP row still has it.
//
// # Schema boundaries
//
// A shape built here is the table's shape at the event, so it is also what
// the ADR-0049 boundary should carry — EXCEPT when a column of it was typed
// from the TABLE_MAP: such a column's IR carries only what decoding needs
// (no default, no generated expression, an approximate length), and a
// boundary built from it would forward a phantom change. Those shapes
// decode rows but emit no boundary; the boundary stays owed and is emitted
// once the stream reaches a shape built from read definitions.

package mysql

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// knownShapesLimit bounds the earlier shapes a reader keeps per table as
// decode dictionaries. A new one is retained only when a cache rebuild
// produces a different shape, so this is a count of DDLs, not of events.
const knownShapesLimit = 32

// recordedView memoises the shape built for one TABLE_MAP table_id, so a
// transaction's worth of rows events pays the build once.
type recordedView struct {
	catalog *tableSchema
	names   []string
	shape   *tableSchema
}

// recordedColumnNames returns the column names a TABLE_MAP event records,
// or nil when the source does not write them (binlog_row_metadata is not
// FULL).
func recordedColumnNames(tm *replication.TableMapEvent) []string {
	if tm == nil || tm.ColumnCount == 0 {
		return nil
	}
	names := tm.ColumnNameString()
	if len(names) != int(tm.ColumnCount) || len(tm.ColumnType) != len(names) || len(tm.ColumnMeta) != len(names) {
		return nil
	}
	return names
}

// shapeAtEvent returns the shape ev was recorded under: built from the
// event's own column names when it carries them, otherwise the catalog
// shape once [CDCReader.proveCatalogShape] has proven it holds at the
// reader's position. It refuses rather than return a shape it cannot stand
// behind.
func (r *CDCReader) shapeAtEvent(ctx context.Context, qn string, tbl *tableSchema, ev *replication.RowsEvent) (*tableSchema, error) {
	if names := recordedColumnNames(ev.Table); names != nil {
		shape, err := r.recordedShape(ctx, qn, tbl, ev.Table, names)
		if err != nil {
			return nil, err
		}
		if err := r.proveUnrecordedProperties(ctx, tbl, shape); err != nil {
			return nil, err
		}
		return shape, nil
	}
	if err := r.proveCatalogShape(ctx, tbl); err != nil {
		return nil, err
	}
	if err := r.recheckRetypedColumns(tbl, ev.Table); err != nil {
		return nil, err
	}
	return tbl, nil
}

// proveUnrecordedProperties covers what FULL metadata does NOT record for a
// column the shape took from a definition sluice read: whether it is
// GENERATED (the decoder drops a generated column's value, so a column made
// generated after the event would lose the value it was written with), and,
// for a type the TABLE_MAP family table cannot classify (a MariaDB native
// UUID/INET4/INET6, VECTOR), what it was recorded as. For a shape holding
// such a column the catalog definition must be proven to hold at the
// event's position, by the same look-ahead MINIMAL uses: a reshaping DDL of
// the table in between refuses, and so does an in-place one that redefined
// that column. A shape without one needs no proof.
func (r *CDCReader) proveUnrecordedProperties(ctx context.Context, catalog, shape *tableSchema) error {
	var unrecorded []string
	for j, col := range shape.Columns {
		_, classifiable := binlogTypeFamilyForDataType(dataTypeAt(shape, j))
		if col.IsGenerated() || !classifiable {
			unrecorded = append(unrecorded, col.Name)
		}
	}
	if len(unrecorded) == 0 {
		return nil
	}
	d, ahead, err := r.catalogShapeAhead(ctx, catalog)
	if err != nil {
		return err
	}
	if !ahead && catalog.retypedAhead.first != nil {
		for _, name := range unrecorded {
			if catalog.retypedAhead.covers(name) {
				d, ahead = *catalog.retypedAhead.first, true
				break
			}
		}
	}
	if ahead {
		return errCDCSchemaReplayMismatch(catalog, fmt.Sprintf(
			"column(s) %s are generated or of a type the binlog's metadata does not describe, and a schema change "+
				"logged later, at %s (%q), may have changed them; the event cannot show what they were when it was written",
			strings.Join(unrecorded, ", "), d.at, d.stmt,
		))
	}
	return nil
}

// recordedShape is shapeAtEvent's FULL-metadata arm, memoised per table_id.
func (r *CDCReader) recordedShape(ctx context.Context, qn string, tbl *tableSchema, tm *replication.TableMapEvent, names []string) (*tableSchema, error) {
	if v, ok := r.recordedViews[tm.TableID]; ok && v.catalog == tbl && equalStrings(v.names, names) {
		return v.shape, nil
	}
	shape, err := r.buildRecordedShape(ctx, qn, tbl, tm, names)
	if err != nil {
		return nil, err
	}
	if r.recordedViews == nil {
		r.recordedViews = map[uint64]recordedView{}
	}
	r.recordedViews[tm.TableID] = recordedView{catalog: tbl, names: names, shape: shape}
	return shape, nil
}

// buildRecordedShape builds the shape a FULL-metadata TABLE_MAP describes.
// It returns tbl itself when the event agrees with it column for column —
// the steady state — so nothing downstream sees a new pointer.
func (r *CDCReader) buildRecordedShape(ctx context.Context, qn string, tbl *tableSchema, tm *replication.TableMapEvent, names []string) (*tableSchema, error) {
	if sameShape(tbl, tm, names) {
		return tbl, nil
	}
	dictionaries := append([]*tableSchema{tbl}, r.knownShapes[qn]...)
	shape := &tableSchema{
		Schema:      tbl.Schema,
		Name:        tbl.Name,
		Columns:     make([]*ir.Column, len(names)),
		NativeKinds: make([]mariadbNativeKind, len(names)),
		DataTypes:   make([]string, len(names)),
		LostLabels:  make([][]bool, len(names)),
		proven:      true,
	}
	for i, name := range names {
		col, dt, native, lost, ok := pickRecordedColumn(dictionaries, tm, i, name)
		if !ok {
			meta, typ, err := recordedColumnMeta(tm, i, r.flavor, r.collationCharsets(ctx))
			if err != nil {
				return nil, errRecordedColumnUntypable(tbl, name, err)
			}
			typ = withRecordedCollation(typ, tm, i, r.collationIDNames(ctx))
			col, dt, native = &ir.Column{Name: name, Type: typ, Nullable: true}, meta.DataType, mariadbNativeNone
			shape.synthesized = true
			shape.tableMapTyped = append(shape.tableMapTyped, name)
		}
		shape.Columns[i], shape.DataTypes[i], shape.NativeKinds[i], shape.LostLabels[i] = col, dt, native, lost
	}
	if !anyLostLabel(shape.LostLabels) {
		shape.LostLabels = nil
	}
	for _, k := range tm.PrimaryKey {
		if int(k) >= len(names) {
			return nil, fmt.Errorf("mysql: cdc: %s: TABLE_MAP primary key names column %d of %d", qn, k, len(names))
		}
		shape.PrimaryKey = append(shape.PrimaryKey, names[k])
	}
	return shape, nil
}

// sameShape reports whether the event's names, in order, are tbl's and every
// column of tbl accepts its recorded type.
func sameShape(tbl *tableSchema, tm *replication.TableMapEvent, names []string) bool {
	if len(names) != len(tbl.Columns) {
		return false
	}
	for i, name := range names {
		if tbl.Columns[i].Name != name || !recordedColumnAccepts(tbl, i, tm, i) {
			return false
		}
	}
	// The event's primary key, where recorded, must be the catalog's too:
	// it decides the before-image narrowing.
	if len(tm.PrimaryKey) > 0 {
		if len(tm.PrimaryKey) != len(tbl.PrimaryKey) {
			return false
		}
		for j, k := range tm.PrimaryKey {
			if int(k) >= len(names) || !strings.EqualFold(names[k], tbl.PrimaryKey[j]) {
				return false
			}
		}
	}
	return true
}

// pickRecordedColumn finds, in dictionary order, a known column named name
// whose type decodes recorded column i faithfully. The column is copied with
// the RECORDED spelling of its name.
func pickRecordedColumn(dictionaries []*tableSchema, tm *replication.TableMapEvent, i int, name string) (col *ir.Column, dataType string, native mariadbNativeKind, lost []bool, ok bool) {
	for _, d := range dictionaries {
		for j, c := range d.Columns {
			if !strings.EqualFold(c.Name, name) || !recordedColumnAccepts(d, j, tm, i) {
				continue
			}
			cp := *c
			cp.Name = name
			if j < len(d.NativeKinds) {
				native = d.NativeKinds[j]
			}
			if j < len(d.LostLabels) {
				lost = d.LostLabels[j]
			}
			return &cp, d.DataTypes[j], native, lost, true
		}
	}
	return nil, "", mariadbNativeNone, nil, false
}

// recordedColumnAccepts reports whether column j of shape d, whose catalog
// data_type is d.DataTypes[j], decodes recorded column i of tm exactly as
// the recorded type would. The family must match (the CDC-4 table); within
// a family, the refinements are the properties the decoder reads from the
// IR type rather than from the row image:
//
//   - integer: signedness (an unsigned value read as signed changes);
//   - string: binary-ness, and a BINARY(n)'s width (the CDC decoder
//     re-pads to it);
//   - bit: the width (the decoded bit-string has exactly that many digits);
//   - float: single vs double;
//   - enum/set: the label list, where the event records it (an index means
//     the label at that index; a catalog label lost to '?' is not a
//     disagreement — enum_label_loss.go recovers it).
//
// An unclassifiable type on either side is accepted, as the CDC-4 guard
// does: it cannot be shown to differ, and a refusal on ignorance would
// stop every stream over a type this table does not know.
func recordedColumnAccepts(d *tableSchema, j int, tm *replication.TableMapEvent, i int) bool {
	if j >= len(d.DataTypes) || i >= len(tm.ColumnType) || i >= len(tm.ColumnMeta) {
		return false
	}
	code, meta := tm.ColumnType[i], tm.ColumnMeta[i]
	want, wantOK := binlogTypeFamilyForDataType(d.DataTypes[j])
	got, gotOK := binlogTypeFamilyForCode(code, meta)
	if !wantOK || !gotOK {
		return true
	}
	if want != got {
		return false
	}
	typ := d.Columns[j].Type
	switch want {
	case famInt:
		if unsigned, known := tm.UnsignedMap()[i]; known {
			isUnsigned := false
			if it, ok := typ.(ir.Integer); ok {
				isUnsigned = it.Unsigned
			}
			if unsigned != isUnsigned {
				return false
			}
		}
	case famString:
		if id, known := tm.CollationMap()[i]; known {
			recordedBinary := id == binaryCollationID
			_, isBinary := typ.(ir.Binary)
			_, isVarbinary := typ.(ir.Varbinary)
			_, isBlob := typ.(ir.Blob)
			if recordedBinary != (isBinary || isVarbinary || isBlob) {
				return false
			}
		}
		if b, ok := typ.(ir.Binary); ok {
			length, isFixed := fixedStringLength(code, meta)
			if !isFixed || length != b.Length {
				return false
			}
		}
	case famBit:
		width := 1
		if b, ok := typ.(ir.Bit); ok {
			width = b.Length
		}
		if bitWidthFromMeta(meta) != width {
			return false
		}
	case famFloat:
		f, ok := typ.(ir.Float)
		if !ok || (code == mysql.MYSQL_TYPE_FLOAT) != (f.Precision == ir.FloatSingle) {
			return false
		}
	case famEnum, famSet:
		return recordedLabelsAgree(d, j, tm, i)
	}
	return true
}

// recordedLabelsAgree compares an ENUM/SET column's label list with the
// event's, where the event records one.
func recordedLabelsAgree(d *tableSchema, j int, tm *replication.TableMapEvent, i int) bool {
	var recorded []string
	var values []string
	switch t := d.Columns[j].Type.(type) {
	case ir.Enum:
		recorded, values = tm.EnumStrValueMap()[i], t.Values
	case ir.Set:
		recorded, values = tm.SetStrValueMap()[i], t.Values
	default:
		return false
	}
	if recorded == nil {
		return true
	}
	if len(recorded) != len(values) {
		return false
	}
	var lost []bool
	if j < len(d.LostLabels) {
		lost = d.LostLabels[j]
	}
	for k := range values {
		if values[k] == recorded[k] || (k < len(lost) && lost[k]) {
			continue
		}
		return false
	}
	return true
}

// binaryCollationID is MySQL's `binary` collation: a BINARY/VARBINARY/BLOB
// column records it, a character column never does.
const binaryCollationID = 63

// withRecordedCollation stamps the collation recorded for column i onto a
// character type typed from the TABLE_MAP alone. The TABLE_MAP records a
// character column's collation ID, not only its character set, and the
// collation decides which values are one key: without it a deferred shape
// could not see `_ci` → `_bin` on a key column folded behind a later retype
// of that column, and rows distinct under the new collation merged on a
// target whose key still compared under the old one (DEFERRED-KEY-COLLATION,
// measured on MySQL 8.4 FULL). An ID the source's collation table does not
// name is left unstamped, and the hold then cannot compare it.
func withRecordedCollation(typ ir.Type, tm *replication.TableMapEvent, i int, nameOf func(uint64) string) ir.Type {
	id, ok := tm.CollationMap()[i]
	if !ok || id == binaryCollationID {
		return typ
	}
	name := strings.ToLower(nameOf(id))
	if name == "" {
		return typ
	}
	switch v := typ.(type) {
	case ir.Varchar:
		v.Collation = name
		return v
	case ir.Char:
		v.Collation = name
		return v
	case ir.Text:
		v.Collation = name
		return v
	}
	return typ
}

// bitWidthFromMeta decodes a BIT column's TABLE_MAP metadata: the low byte
// is the bits beyond whole bytes, the high byte the whole bytes.
func bitWidthFromMeta(meta uint16) int {
	return int(meta>>8)*8 + int(meta&0xff)
}

// fixedStringLength decodes a MYSQL_TYPE_STRING column's byte length from
// its TABLE_MAP metadata (the server folds the high bits of a length over
// 255 into the real-type byte). isFixed is false for an ENUM/SET, which
// shares the code.
func fixedStringLength(code byte, meta uint16) (length int, isFixed bool) {
	if code != mysql.MYSQL_TYPE_STRING {
		return 0, false
	}
	b0, b1 := byte(meta>>8), int(meta&0xff)
	if b0&0x30 != 0x30 {
		return b1 | int((b0&0x30)^0x30)<<4, true
	}
	if b0 == mysql.MYSQL_TYPE_ENUM || b0 == mysql.MYSQL_TYPE_SET {
		return 0, false
	}
	return b1, true
}

// recordedColumnMeta types recorded column i from the TABLE_MAP alone, for a
// column no shape sluice has read can type (dropped or retyped since). It
// builds the information_schema row the column had and runs it through
// [translateType], so a recorded column gets the IR type the catalog path
// would have given it — except for the properties the TABLE_MAP does not
// record, which are refused rather than guessed:
//
//   - a SIGNED TINYINT: TINYINT(1) is sluice's boolean and other widths
//     are integers, and the display width is not in the binlog;
//   - a spatial column: its declared SRID decides how a value decodes;
//   - a binary string on MariaDB: a native UUID/INET column is stored as
//     one and decodes differently.
//
// The lengths it reconstructs for character columns are byte lengths; the
// decoder does not read them, and a shape holding a column typed here never
// becomes a schema boundary (see the file comment).
func recordedColumnMeta(tm *replication.TableMapEvent, i int, flavor Flavor, charsetOf func(uint64) string) (columnMeta, ir.Type, error) {
	code, meta := tm.ColumnType[i], tm.ColumnMeta[i]
	unsigned, signKnown := tm.UnsignedMap()[i]
	if isBinlogIntegerCode(code) && !signKnown {
		// No server sluice has met writes names (FULL) without signedness,
		// but an integer typed here must not default to signed on that
		// assumption (MARIADB-NOLOG-UNSIGNED).
		return columnMeta{}, nil, errors.New("it is an integer, and the event does not record whether it was signed")
	}
	sign := ""
	if unsigned {
		sign = " unsigned"
	}
	n64 := func(v int) *int64 { x := int64(v); return &x }
	var m columnMeta
	switch code {
	case mysql.MYSQL_TYPE_TINY:
		if !unsigned {
			return m, nil, fmt.Errorf("it is a signed TINYINT, and the binlog does not record whether it was TINYINT(1), which sluice reads as a boolean")
		}
		m = columnMeta{DataType: "tinyint", ColumnType: "tinyint" + sign}
	case mysql.MYSQL_TYPE_SHORT:
		m = columnMeta{DataType: "smallint", ColumnType: "smallint" + sign}
	case mysql.MYSQL_TYPE_INT24:
		m = columnMeta{DataType: "mediumint", ColumnType: "mediumint" + sign}
	case mysql.MYSQL_TYPE_LONG:
		m = columnMeta{DataType: "int", ColumnType: "int" + sign}
	case mysql.MYSQL_TYPE_LONGLONG:
		m = columnMeta{DataType: "bigint", ColumnType: "bigint" + sign}
	case mysql.MYSQL_TYPE_YEAR:
		m = columnMeta{DataType: "year", ColumnType: "year"}
	case mysql.MYSQL_TYPE_NEWDECIMAL:
		p, s := int(meta>>8), int(meta&0xff)
		m = columnMeta{DataType: "decimal", ColumnType: fmt.Sprintf("decimal(%d,%d)%s", p, s, sign), NumPrec: n64(p), NumScale: n64(s)}
	case mysql.MYSQL_TYPE_FLOAT:
		m = columnMeta{DataType: "float", ColumnType: "float" + sign}
	case mysql.MYSQL_TYPE_DOUBLE:
		m = columnMeta{DataType: "double", ColumnType: "double" + sign}
	case mysql.MYSQL_TYPE_BIT:
		w := bitWidthFromMeta(meta)
		m = columnMeta{DataType: "bit", ColumnType: fmt.Sprintf("bit(%d)", w)}
	case mysql.MYSQL_TYPE_DATE, mysql.MYSQL_TYPE_NEWDATE:
		m = columnMeta{DataType: "date", ColumnType: "date"}
	case mysql.MYSQL_TYPE_TIME, mysql.MYSQL_TYPE_TIME2:
		m = temporalMeta("time", code, meta)
	case mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_DATETIME2:
		m = temporalMeta("datetime", code, meta)
	case mysql.MYSQL_TYPE_TIMESTAMP, mysql.MYSQL_TYPE_TIMESTAMP2:
		m = temporalMeta("timestamp", code, meta)
	case mysql.MYSQL_TYPE_JSON:
		m = columnMeta{DataType: "json", ColumnType: "json"}
	case mysql.MYSQL_TYPE_GEOMETRY:
		return m, nil, fmt.Errorf("it is a spatial column, and the binlog does not record its SRID")
	case mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_VAR_STRING, mysql.MYSQL_TYPE_BLOB, mysql.MYSQL_TYPE_STRING:
		return recordedStringMeta(tm, i, flavor, charsetOf)
	default:
		return m, nil, fmt.Errorf("its binlog type code %d is not one sluice can type from the binlog alone", code)
	}
	typ, err := translateType(m)
	return m, typ, err
}

// temporalMeta builds a temporal column's metadata; only the fractional-
// second (TIME2/DATETIME2/TIMESTAMP2) codes carry a precision.
func temporalMeta(dataType string, code byte, meta uint16) columnMeta {
	fsp := 0
	switch code {
	case mysql.MYSQL_TYPE_TIME2, mysql.MYSQL_TYPE_DATETIME2, mysql.MYSQL_TYPE_TIMESTAMP2:
		fsp = int(meta)
	}
	p := int64(fsp)
	ct := dataType
	if fsp > 0 {
		ct = fmt.Sprintf("%s(%d)", dataType, fsp)
	}
	return columnMeta{DataType: dataType, ColumnType: ct, DTPrec: &p}
}

// recordedStringMeta is recordedColumnMeta for the string codes: character
// vs binary from the recorded collation, ENUM/SET from the recorded labels.
func recordedStringMeta(tm *replication.TableMapEvent, i int, flavor Flavor, charsetOf func(uint64) string) (columnMeta, ir.Type, error) {
	code, meta := tm.ColumnType[i], tm.ColumnMeta[i]
	if code == mysql.MYSQL_TYPE_STRING {
		switch byte(meta >> 8) {
		case mysql.MYSQL_TYPE_ENUM:
			labels, ok := tm.EnumStrValueMap()[i]
			if !ok {
				return columnMeta{}, nil, fmt.Errorf("it is an ENUM and the binlog does not record its labels")
			}
			return columnMeta{DataType: "enum"}, ir.Enum{Values: labels}, nil
		case mysql.MYSQL_TYPE_SET:
			labels, ok := tm.SetStrValueMap()[i]
			if !ok {
				return columnMeta{}, nil, fmt.Errorf("it is a SET and the binlog does not record its labels")
			}
			return columnMeta{DataType: "set"}, ir.Set{Values: labels}, nil
		}
	}
	id, ok := tm.CollationMap()[i]
	if !ok {
		return columnMeta{}, nil, fmt.Errorf("the binlog does not record whether it is a character or a binary column")
	}
	binary := id == binaryCollationID
	if binary && flavor == FlavorMariaDB {
		return columnMeta{}, nil, fmt.Errorf("it is a binary column on MariaDB, and the binlog does not record whether it was a native UUID/INET type")
	}
	charset := ""
	if !binary {
		if charset = canonicalCharsetName(charsetOf(id)); charset == "" {
			return columnMeta{}, nil, fmt.Errorf("it was written under collation ID %d, which sluice cannot name", id)
		}
	}
	pick := func(text, bin string) string {
		if binary {
			return bin
		}
		return text
	}
	n64 := func(v int) *int64 { x := int64(v); return &x }
	var m columnMeta
	switch code {
	case mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_VAR_STRING:
		dt := pick("varchar", "varbinary")
		m = columnMeta{DataType: dt, ColumnType: fmt.Sprintf("%s(%d)", dt, meta), CharMaxLen: n64(int(meta)), Charset: charset}
	case mysql.MYSQL_TYPE_STRING:
		length, _ := fixedStringLength(code, meta)
		dt := pick("char", "binary")
		m = columnMeta{DataType: dt, ColumnType: fmt.Sprintf("%s(%d)", dt, length), CharMaxLen: n64(length), Charset: charset}
	case mysql.MYSQL_TYPE_BLOB:
		sizes := map[uint16][2]string{1: {"tinytext", "tinyblob"}, 2: {"text", "blob"}, 3: {"mediumtext", "mediumblob"}, 4: {"longtext", "longblob"}}
		s, ok := sizes[meta]
		if !ok {
			return columnMeta{}, nil, fmt.Errorf("its BLOB length-prefix width %d is not one MySQL writes", meta)
		}
		dt := pick(s[0], s[1])
		m = columnMeta{DataType: dt, ColumnType: dt, Charset: charset}
	}
	typ, err := translateType(m)
	return m, typ, err
}

// errRecordedColumnUntypable refuses a FULL-metadata row whose recorded
// column sluice can type neither from a definition it has read nor from the
// TABLE_MAP.
func errRecordedColumnUntypable(tbl *tableSchema, column string, why error) error {
	return sluicecode.Wrap(
		sluicecode.CodeCDCSchemaReplayMismatch,
		"re-snapshot the table (`sync start --restart-from-scratch` for a sync, a fresh `backup full` for a backup chain); "+
			"the buffered binlog history predates a schema change sluice cannot replay for this column",
		fmt.Errorf(
			"mysql: cdc: %s.%s: a row event recorded column %q, which the table no longer has in that form (a later "+
				"schema change dropped or retyped it), and sluice cannot type it from the binlog alone: %v. Refusing "+
				"before decoding the row",
			tbl.Schema, tbl.Name, column, why,
		),
	)
}

// retainKnownShape records a freshly loaded catalog shape as a decode
// dictionary for later FULL-metadata events of the same table.
func (r *CDCReader) retainKnownShape(qn string, tbl *tableSchema) {
	if r.knownShapes == nil {
		r.knownShapes = map[string][]*tableSchema{}
	}
	shapes := append([]*tableSchema{tbl}, r.knownShapes[qn]...)
	if len(shapes) > knownShapesLimit {
		shapes = shapes[:knownShapesLimit]
	}
	r.knownShapes[qn] = shapes
}

// equalStrings reports whether two string slices are identical.
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
