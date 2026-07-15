// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mydumper

import (
	"strconv"
	"strings"

	"sluicesync.dev/sluice/internal/engines/mysql"
	"sluicesync.dev/sluice/internal/ir"
)

// pendingDefault carries a parsed DEFAULT clause until the column's IR
// type is known — the live engine's [mysql.translateDefault] dispatches on
// the type (BIT vs BOOLEAN bit-literals, BINARY hex-literals), so the
// conversion happens after [mysql.TranslateColumnType].
type pendingDefault struct {
	kind   defaultKind
	text   string // number text / bit digits / 0x… spelling / expression body / CURRENT_TIMESTAMP spelling
	strVal []byte // decoded string-literal bytes
}

type defaultKind uint8

const (
	defNone defaultKind = iota
	defNull
	defString
	defNumber
	defBitLit
	defHexLit
	defExpr
	defCurrentTS
)

// parseColumnDef parses one column definition (`name type … attributes…`)
// and appends the resulting IR column. Every attribute outside the
// recognised set is a loud refusal — the bounded-parse commitment
// (ADR-0161 §3) is that this parser never skips a token it does not
// understand inside a column definition.
func (b *tableBuilder) parseColumnDef() error {
	p := b.p
	nameTok, err := p.expectIdent()
	if err != nil {
		return err
	}
	name := nameTok.text

	spec, err := b.parseColumnType()
	if err != nil {
		return err
	}

	col := &ir.Column{Name: name, Nullable: true}
	attrs, err := b.parseColumnAttributes(col, name)
	if err != nil {
		return err
	}

	meta := mysql.ColumnMeta{
		DataType:   spec.base,
		ColumnType: spec.columnTypeSpelling(),
		Charset:    attrs.charset,
		Collation:  attrs.collation,
		Extra:      attrs.extra,
		SrsID:      attrs.srid,
	}
	fillTypeDimensions(&meta, spec.base, spec.args)

	irType, err := mysql.TranslateColumnType(meta)
	if err != nil {
		return p.errAt(nameTok, "column %s: %v", name, err)
	}
	col.Type = irType
	col.Default = attrs.def.toIR(irType)

	// Effective charset defaults to the table's; record the explicit one
	// for the checkCharsets gate.
	if attrs.charset != "" {
		if b.columnCharsets == nil {
			b.columnCharsets = map[string]string{}
		}
		b.columnCharsets[name] = attrs.charset
	}

	b.table.Columns = append(b.table.Columns, col)
	return nil
}

// columnAttrs collects the side-band results of the column attribute loop
// — everything that feeds the [mysql.ColumnMeta] rather than the IR column
// directly.
type columnAttrs struct {
	def       pendingDefault
	charset   string
	collation string
	extra     string
	srid      int
}

// parseColumnAttributes consumes the attribute tail of a column definition
// up to (but not including) the `,`/`)` that terminates it. Nullability,
// comment, and generated-column facts land on col; type-translation inputs
// come back as [columnAttrs].
func (b *tableBuilder) parseColumnAttributes(col *ir.Column, name string) (columnAttrs, error) {
	p := b.p
	var attrs columnAttrs
	for {
		t := p.peek()
		switch {
		case t.kind == tokEOF:
			return attrs, p.errAt(t, "unterminated column definition for %s", name)
		case t.kind == tokPunct && (t.text == "," || t.text == ")"):
			return attrs, nil

		case p.acceptKeyword("NOT"):
			if err := p.expectKeyword("NULL"); err != nil {
				return attrs, err
			}
			col.Nullable = false
		case p.acceptKeyword("NULL"):
			col.Nullable = true

		case p.acceptKeyword("DEFAULT"):
			def, err := b.parseDefaultClause(name)
			if err != nil {
				return attrs, err
			}
			attrs.def = def

		case p.acceptKeyword("ON"):
			if err := b.skipOnUpdateClause(); err != nil {
				return attrs, err
			}

		case p.acceptKeyword("AUTO_INCREMENT"):
			attrs.extra = "auto_increment"

		case p.acceptKeyword("UNIQUE"):
			_ = p.acceptKeyword("KEY")
			// MySQL auto-names an inline unique key after the column.
			b.table.Indexes = append(b.table.Indexes, &ir.Index{
				Name:    name,
				Unique:  true,
				Kind:    ir.IndexKindBTree,
				Columns: []ir.IndexColumn{{Column: name}},
			})

		case p.acceptKeyword("PRIMARY"):
			if err := p.expectKeyword("KEY"); err != nil {
				return attrs, err
			}
			b.setInlinePrimaryKey(name)
		case p.acceptKeyword("KEY"):
			// `col INT KEY` is MySQL shorthand for PRIMARY KEY.
			b.setInlinePrimaryKey(name)

		case p.acceptKeyword("COMMENT"):
			ct := p.next()
			if ct.kind != tokString {
				return attrs, p.errAt(ct, "expected a string after COMMENT")
			}
			col.Comment = string(ct.val)

		case p.acceptKeyword("COLLATE"):
			cv, err := p.expectIdent()
			if err != nil {
				return attrs, err
			}
			attrs.collation = strings.ToLower(cv.text)

		case p.acceptKeyword("CHARACTER"):
			if err := p.expectKeyword("SET"); err != nil {
				return attrs, err
			}
			cv, err := p.expectIdent()
			if err != nil {
				return attrs, err
			}
			attrs.charset = strings.ToLower(cv.text)
		case p.acceptKeyword("CHARSET"):
			cv, err := p.expectIdent()
			if err != nil {
				return attrs, err
			}
			attrs.charset = strings.ToLower(cv.text)

		case p.acceptKeyword("GENERATED"):
			if err := p.expectKeyword("ALWAYS"); err != nil {
				return attrs, err
			}
			if err := p.expectKeyword("AS"); err != nil {
				return attrs, err
			}
			if err := b.parseGeneratedBody(col); err != nil {
				return attrs, err
			}
		case p.acceptKeyword("AS"):
			if err := b.parseGeneratedBody(col); err != nil {
				return attrs, err
			}
		case p.acceptKeyword("VIRTUAL"):
			col.GeneratedStored = false
		case p.acceptKeyword("STORED"):
			col.GeneratedStored = true

		case p.acceptKeyword("SRID"):
			nt := p.next()
			if nt.kind != tokNumber {
				return attrs, p.errAt(nt, "expected a number after SRID")
			}
			srid, err := strconv.Atoi(nt.text)
			if err != nil {
				return attrs, p.errAt(nt, "bad SRID: %v", err)
			}
			attrs.srid = srid

		case p.acceptKeyword("CHECK"):
			if err := b.parseCheck(""); err != nil {
				return attrs, err
			}

		case p.acceptKeyword("VISIBLE"), p.acceptKeyword("INVISIBLE"):
			// column visibility is an optimizer/SELECT-* toggle; not carried
		case p.acceptKeyword("COLUMN_FORMAT"), p.acceptKeyword("STORAGE"):
			p.next() // physical option value

		default:
			return attrs, p.errAt(t, "unsupported column attribute in definition of %s — outside the bounded "+
				"CREATE TABLE shape this reader supports (ADR-0161)", name)
		}
	}
}

// skipOnUpdateClause consumes `ON UPDATE CURRENT_TIMESTAMP[(n)]` (the ON
// keyword already consumed) — MySQL-only auto-update semantics with no IR
// slot, skipped exactly as the live information_schema reader skips the
// EXTRA "on update" token.
func (b *tableBuilder) skipOnUpdateClause() error {
	p := b.p
	if err := p.expectKeyword("UPDATE"); err != nil {
		return err
	}
	if err := p.expectKeyword("CURRENT_TIMESTAMP"); err != nil {
		return err
	}
	if p.acceptPunct("(") {
		p.next()
		if err := p.expectPunct(")"); err != nil {
			return err
		}
	}
	return nil
}

// setInlinePrimaryKey installs a single-column PRIMARY KEY declared inline
// on a column definition.
func (b *tableBuilder) setInlinePrimaryKey(colName string) {
	b.table.PrimaryKey = &ir.Index{
		Name:    "PRIMARY",
		Unique:  true,
		Kind:    ir.IndexKindBTree,
		Columns: []ir.IndexColumn{{Column: colName}},
	}
}

// parseGeneratedBody parses `(expr) [VIRTUAL|STORED]` after GENERATED
// ALWAYS AS / AS. The VIRTUAL/STORED keyword may also arrive via the outer
// attribute loop; MySQL's default is VIRTUAL, so GeneratedStored stays
// false unless STORED appears.
func (b *tableBuilder) parseGeneratedBody(col *ir.Column) error {
	expr, err := b.p.captureParenGroup()
	if err != nil {
		return err
	}
	col.GeneratedExpr = mysql.NormalizeExpressionText(expr)
	col.GeneratedExprDialect = "mysql"
	return nil
}

// columnTypeSpec is a parsed column type: the lowercased base name, the
// parenthesized argument text reassembled from the raw token spellings
// (load-bearing for ENUM/SET — the labels' source escaping must reach
// [mysql.TranslateColumnType] verbatim), the parsed numeric arguments for
// the dimension fields, and the UNSIGNED/ZEROFILL flags.
type columnTypeSpec struct {
	base     string
	rawArgs  string
	args     []int64
	unsigned bool
	zerofill bool
}

// columnTypeSpelling reconstructs the information_schema COLUMN_TYPE
// spelling ("tinyint(1)", "bigint unsigned", "enum('a','b')") that
// [mysql.TranslateColumnType] inspects for display widths, unsignedness,
// and ENUM/SET value lists.
func (s columnTypeSpec) columnTypeSpelling() string {
	ct := s.base + s.rawArgs
	if s.unsigned {
		ct += " unsigned"
	}
	if s.zerofill {
		ct += " zerofill"
	}
	return ct
}

// parseColumnType parses the type name, its optional argument list, and
// the UNSIGNED/ZEROFILL flags.
func (b *tableBuilder) parseColumnType() (columnTypeSpec, error) {
	p := b.p
	typeTok, err := p.expectIdent()
	if err != nil {
		return columnTypeSpec{}, err
	}
	spec := columnTypeSpec{base: strings.ToLower(typeTok.text)}
	switch spec.base {
	case "double":
		_ = p.acceptKeyword("PRECISION") // DOUBLE PRECISION ≡ DOUBLE
	case "bool", "boolean":
		// BOOLEAN is sugar for TINYINT(1); SHOW CREATE never emits it, but a
		// hand-maintained schema file may.
		return columnTypeSpec{base: "tinyint", rawArgs: "(1)", args: []int64{1}}, nil
	}

	if p.acceptPunct("(") {
		var rawParts []string
		for {
			t := p.next()
			switch t.kind {
			case tokNumber:
				n, perr := strconv.ParseInt(t.text, 10, 64)
				if perr != nil {
					return columnTypeSpec{}, p.errAt(t, "bad type argument: %v", perr)
				}
				spec.args = append(spec.args, n)
				rawParts = append(rawParts, t.text)
			case tokString:
				// ENUM/SET labels: keep the raw source spelling so the escape
				// discipline survives into the reconstructed column_type.
				rawParts = append(rawParts, t.text)
			default:
				return columnTypeSpec{}, p.errAt(t, "unexpected type argument")
			}
			if p.acceptPunct(",") {
				continue
			}
			if p.acceptPunct(")") {
				break
			}
			return columnTypeSpec{}, p.errAt(p.peek(), "expected ',' or ')' in type arguments")
		}
		spec.rawArgs = "(" + strings.Join(rawParts, ",") + ")"
	}

	for {
		switch {
		case p.acceptKeyword("UNSIGNED"):
			spec.unsigned = true
		case p.acceptKeyword("SIGNED"):
			// explicit default
		case p.acceptKeyword("ZEROFILL"):
			spec.zerofill = true
		default:
			return spec, nil
		}
	}
}

// fillTypeDimensions populates the nullable dimension fields of the column
// meta from the parsed type arguments, mirroring what information_schema
// reports for each family (including MySQL's defaults when the argument is
// omitted: CHAR/BINARY default to length 1, DECIMAL to (10,0)).
func fillTypeDimensions(meta *mysql.ColumnMeta, base string, args []int64) {
	argOr := func(i int, dflt int64) int64 {
		if i < len(args) {
			return args[i]
		}
		return dflt
	}
	switch base {
	case "char", "binary":
		v := argOr(0, 1)
		meta.CharMaxLen = &v
	case "varchar", "varbinary":
		v := argOr(0, 0)
		meta.CharMaxLen = &v
	case "decimal", "numeric":
		p, s := argOr(0, 10), argOr(1, 0)
		meta.NumPrec, meta.NumScale = &p, &s
	case "time", "datetime", "timestamp":
		v := argOr(0, 0)
		meta.DTPrec = &v
	}
}

// toIR converts a pending DEFAULT clause into the IR default for the now-
// known column type, mirroring the live reader's translateDefault shapes:
// bit-literals dispatch on BIT-vs-BOOLEAN, hex-literals on the binary
// family, expressions carry the "mysql" dialect tag after stored-form
// normalization.
func (d pendingDefault) toIR(t ir.Type) ir.DefaultValue {
	switch d.kind {
	case defString:
		return ir.DefaultLiteral{Value: string(d.strVal)}
	case defNumber:
		return ir.DefaultLiteral{Value: d.text}
	case defCurrentTS:
		return ir.DefaultExpression{Expr: d.text, Dialect: "mysql"}
	case defExpr:
		return ir.DefaultExpression{Expr: mysql.NormalizeExpressionText(d.text), Dialect: "mysql"}
	case defBitLit:
		if _, isBit := t.(ir.Bit); isBit {
			return ir.DefaultExpression{Expr: "b'" + d.text + "'", Dialect: mysql.BitDefaultDialect}
		}
		return ir.DefaultLiteral{Value: bitsToDecimal(d.text)}
	case defHexLit:
		switch t.(type) {
		case ir.Binary, ir.Varbinary:
			return ir.DefaultExpression{Expr: d.text, Dialect: mysql.HexDefaultDialect}
		}
		return ir.DefaultLiteral{Value: d.text}
	default: // defNone, defNull — information_schema reports both as NULL
		return ir.DefaultNone{}
	}
}

// bitsToDecimal renders a '0'/'1' digit string as its decimal value,
// mirroring the live reader's BIT(1)-default collapse. MySQL BIT holds at
// most 64 bits, so uint64 arithmetic is exact.
func bitsToDecimal(bits string) string {
	var v uint64
	for i := 0; i < len(bits); i++ {
		v = v<<1 | uint64(bits[i]-'0')
	}
	return strconv.FormatUint(v, 10)
}

// parseDefaultClause parses the value of a DEFAULT attribute. colName is
// used only in errors.
func (b *tableBuilder) parseDefaultClause(colName string) (pendingDefault, error) {
	p := b.p
	t := p.peek()
	switch {
	case t.kind == tokString:
		p.next()
		return pendingDefault{kind: defString, strVal: t.val}, nil
	case t.kind == tokNumber:
		p.next()
		return pendingDefault{kind: defNumber, text: t.text}, nil
	case t.kind == tokPunct && (t.text == "-" || t.text == "+"):
		p.next()
		nt := p.next()
		if nt.kind != tokNumber {
			return pendingDefault{}, p.errAt(nt, "expected a number after the sign in DEFAULT of %s", colName)
		}
		text := nt.text
		if t.text == "-" {
			text = "-" + text
		}
		return pendingDefault{kind: defNumber, text: text}, nil
	case t.kind == tokBitLit:
		p.next()
		return pendingDefault{kind: defBitLit, text: t.text}, nil
	case t.kind == tokHexLit:
		p.next()
		return pendingDefault{kind: defHexLit, text: t.text}, nil
	case p.acceptKeyword("NULL"):
		return pendingDefault{kind: defNull}, nil
	case p.acceptKeyword("CURRENT_TIMESTAMP"):
		spelling := "CURRENT_TIMESTAMP"
		if p.acceptPunct("(") {
			nt := p.next()
			if nt.kind != tokNumber {
				return pendingDefault{}, p.errAt(nt, "expected a precision in DEFAULT CURRENT_TIMESTAMP of %s", colName)
			}
			if err := p.expectPunct(")"); err != nil {
				return pendingDefault{}, err
			}
			spelling += "(" + nt.text + ")"
		}
		return pendingDefault{kind: defCurrentTS, text: spelling}, nil
	case t.kind == tokPunct && t.text == "(":
		expr, err := p.captureParenGroup()
		if err != nil {
			return pendingDefault{}, err
		}
		return pendingDefault{kind: defExpr, text: expr}, nil
	default:
		return pendingDefault{}, p.errAt(t, "unsupported DEFAULT value for column %s — outside the bounded "+
			"CREATE TABLE shape this reader supports (ADR-0161)", colName)
	}
}
