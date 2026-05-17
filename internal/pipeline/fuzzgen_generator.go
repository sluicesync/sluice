// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Schema+data generator for the generative round-trip fuzz harness
// (Track 2, Phase 1). One seeded master RNG (design decision #4):
// every generated case is a pure function of (seed, caseIndex), so a
// failure prints the seed and the harness can deterministically replay
// the exact failing schema+data from a dumped fixture.
//
// The generator emits RAW source-dialect DDL/DML — it never goes
// through sluice's IR or writers (design decision #1, the independent
// oracle). A generator/source mismatch can therefore never be masked
// by a writer bug, exactly as the battle-test fixtures
// (migrate_bug7374/75/69_integration_test.go) do it.
//
// No build tag: pure logic, unit-tested by fuzzgen_generator_test.go.

package pipeline

import (
	"fmt"
	"math/rand"
	"strings"
)

// genColumn is one generated column: a family at a shape, with its
// per-row literals already rendered in the source dialect.
type genColumn struct {
	name   string
	fam    *family
	shp    shape
	ddl    string   // column type spelling, source dialect
	values []string // one source-dialect literal per row ("NULL" sentinel handled)
}

// genCase is a fully-rendered, replayable test case: the source-dialect
// CREATE TABLE + INSERT script plus the metadata the oracle needs.
type genCase struct {
	seed     int64
	caseIdx  int
	dir      direction
	tableNm  string
	enumType string // non-empty when an enum column needs a CREATE TYPE (PG src)
	columns  []genColumn
	rowCount int

	// ddl is the full replayable script applied directly to the source
	// container (CREATE TYPE? + CREATE TABLE + INSERTs).
	ddl string
}

// arrayLeafScalars is how many scalar leaves a 1-D array holds; the
// multi-dim shape is a square matrix of side multiDimSide. Small fixed
// sizes keep the INSERT readable in a dumped fixture while still
// exercising NULL-element and ≥2-D nesting (the Bug 73/74 class).
const (
	arrayLeafScalars = 3
	multiDimSide     = 2
)

// generateCase builds case #idx for the given direction from the master
// seed. Deterministic: (seed, idx, dir) fully determines the output.
func generateCase(seed int64, idx int, dir direction) genCase {
	// Derive a per-case RNG so cases are independent yet reproducible.
	r := rand.New(rand.NewSource(seed + int64(idx)*1_000_003 + int64(dir.src)*7 + int64(dir.dst)*131))

	reg := registry()

	table := fmt.Sprintf("fuzz_%s_%d", strings.ReplaceAll(dir.String(), "->", "_"), idx)
	// The enum type name must be unique per case: a CREATE TYPE persists
	// in the target DB across the direction's reused-container
	// iterations, so a fixed name collides on case >0 (SQLSTATE 42710).
	enumTypeName := table + "_enum"

	// Pick a random non-empty subset of families that can be a SOURCE
	// in dir.src, each at a randomly chosen supported shape.
	var cols []genColumn
	colIdx := 0
	enumType := ""
	for _, f := range reg {
		// Bias: include each family ~70% of the time so a table is a
		// realistic mix, but over many cases every family is hit.
		if r.Intn(10) < 3 {
			continue
		}
		s := pickShape(r, f, dir.src)
		if s < 0 {
			continue
		}
		c := genColumn{
			name: fmt.Sprintf("c%02d_%s_%s", colIdx, f.name, shapeTag(shape(s))),
			fam:  f,
			shp:  shape(s),
			ddl:  f.columnDDL(dir.src, shape(s)),
		}
		if f.name == "enum" && dir.src == enginePG {
			enumType = enumTypeName
			c.ddl = enumType
		}
		cols = append(cols, c)
		colIdx++
	}

	// Guarantee at least one column (a degenerate empty table is
	// uninteresting and some engines reject it).
	if len(cols) == 0 {
		f := reg[0] // int8 — always source-capable everywhere
		cols = append(cols, genColumn{
			name: "c00_int8_s", fam: f, shp: shapeScalar,
			ddl: f.columnDDL(dir.src, shapeScalar),
		})
	}

	rowCount := 3 + r.Intn(4) // 3..6 rows
	for ci := range cols {
		cols[ci].values = make([]string, rowCount)
		for ri := 0; ri < rowCount; ri++ {
			cols[ci].values[ri] = renderCell(r, &cols[ci], dir.src)
		}
	}

	gc := genCase{
		seed: seed, caseIdx: idx, dir: dir,
		tableNm: table, enumType: enumType,
		columns: cols, rowCount: rowCount,
	}
	gc.ddl = renderScript(&gc, dir.src)
	return gc
}

// pickShape chooses a supported shape for f as a source in src, or -1.
func pickShape(r *rand.Rand, f *family, src engineKind) int {
	var ok []shape
	for s := shapeScalar; s <= shapeShapeLast; s++ {
		if f.canSource(src, s) {
			ok = append(ok, s)
		}
	}
	if len(ok) == 0 {
		return -1
	}
	return int(ok[r.Intn(len(ok))])
}

func shapeTag(s shape) string {
	switch s {
	case shape1DArray:
		return "a1"
	case shapeMultiDim:
		return "a2"
	default:
		return "s"
	}
}

// renderCell renders one cell. For arrays it assembles scalar leaves
// (driven by the family's gen) into a source-dialect array literal,
// folding in NULL-element and ≥2-D nesting — the Bug 73/74 axes.
func renderCell(r *rand.Rand, c *genColumn, src engineKind) string {
	scalar := func() string {
		lit, isNull := c.fam.gen(r, src)
		if isNull {
			return "NULL"
		}
		return lit
	}

	switch c.shp {
	case shapeScalar:
		v := scalar()
		if v == "NULL" {
			return "NULL"
		}
		return v

	case shape1DArray:
		// 1-in-7: the whole array column is SQL NULL.
		if r.Intn(7) == 0 {
			return "NULL"
		}
		elems := make([]string, arrayLeafScalars)
		for i := range elems {
			elems[i] = scalar() // may be the literal "NULL" → NULL element
		}
		return pgArrayLiteral(c, elems)

	case shapeMultiDim:
		if r.Intn(7) == 0 {
			return "NULL"
		}
		rows := make([]string, multiDimSide)
		for i := 0; i < multiDimSide; i++ {
			inner := make([]string, multiDimSide)
			for j := range inner {
				inner[j] = scalar()
			}
			rows[i] = "[" + strings.Join(inner, ",") + "]"
		}
		return pgArrayWrap(c, "ARRAY["+strings.Join(rows, ",")+"]")
	}
	return "NULL"
}

// pgArrayLiteral builds a 1-D PG ARRAY[...] with an explicit element
// cast so heterogeneous-looking literals (uuid/inet/cidr/macaddr/
// numeric/temporal) type-check — mirroring the battle-test fixtures'
// `ARRAY[...]::T[]` form.
func pgArrayLiteral(c *genColumn, elems []string) string {
	return pgArrayWrap(c, "ARRAY["+strings.Join(elems, ",")+"]")
}

// pgArrayWrap appends the per-family `::T[]` / `::T[][]` cast used by
// the battle-test array fixtures so PG accepts the literal.
func pgArrayWrap(c *genColumn, arr string) string {
	base := c.fam.pgType
	switch c.shp {
	case shape1DArray:
		return arr + "::" + base + "[]"
	case shapeMultiDim:
		return arr + "::" + base + "[][]"
	default:
		return arr
	}
}

// renderScript assembles the full replayable source-dialect script.
func renderScript(gc *genCase, src engineKind) string {
	var b strings.Builder
	if gc.enumType != "" && src == enginePG {
		fmt.Fprintf(&b, "DROP TYPE IF EXISTS %s CASCADE;\n", gc.enumType)
		fmt.Fprintf(&b, "CREATE TYPE %s AS ENUM ('red','green','blue');\n", gc.enumType)
	}
	fmt.Fprintf(&b, "DROP TABLE IF EXISTS %s;\n", gc.tableNm)

	// id PK + each generated column.
	b.WriteString("CREATE TABLE " + gc.tableNm + " (\n")
	if src == engineMySQL {
		b.WriteString("  id INT NOT NULL PRIMARY KEY")
	} else {
		b.WriteString("  id INT PRIMARY KEY")
	}
	for _, c := range gc.columns {
		fmt.Fprintf(&b, ",\n  %s %s", c.name, c.ddl)
	}
	if src == engineMySQL {
		b.WriteString("\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n")
	} else {
		b.WriteString("\n);\n")
	}

	colNames := make([]string, len(gc.columns))
	for i, c := range gc.columns {
		colNames[i] = c.name
	}
	for ri := 0; ri < gc.rowCount; ri++ {
		fmt.Fprintf(&b, "INSERT INTO %s (id", gc.tableNm)
		for _, n := range colNames {
			b.WriteString(", " + n)
		}
		fmt.Fprintf(&b, ") VALUES (%d", ri+1)
		for _, c := range gc.columns {
			b.WriteString(", " + c.values[ri])
		}
		b.WriteString(");\n")
	}
	return b.String()
}
