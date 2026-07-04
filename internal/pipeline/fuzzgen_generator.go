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

	// forceNonNullLeaf guarantees ≥1 non-NULL element in every rendered
	// cell of this column (and never a whole-column NULL). Set for an
	// array-shape column whose family loud-refuses for this direction:
	// the loud-refuse oracle is keyed on (family,shape,dir) with no
	// all-NULL carve-out, so an all-NULL array would push no element
	// through the unsupported codec, migrate would legitimately exit 0,
	// and the loud-refuse assertion would be *vacuous* — a guaranteed
	// false-positive (Findings 1 & 2). Forcing a real element makes the
	// refusal path actually fire, so the assertion is meaningful. This
	// is the generator-side discipline the prep-doc contract mandates:
	// "a loud-refuse pin must force the refused path."
	forceNonNullLeaf bool
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
		// SQLite-target refusal down-weighting: into a SQLite target,
		// EVERY array shape and several whole families are documented
		// loud-refuses (ADR-0134 §1). Unbiased draws would put a refused
		// column in nearly every case, so the whole-case loud-refuse
		// expectation would swallow the direction — the sqlite WRITE path
		// would never be value-fuzzed. Keep the refusal class pinned (a
		// 1-in-20 draw keeps it) but usually swap an array down to its
		// scalar value path, and drop the always-refused families.
		if dir.dst == engineSQLite && expectedOutcome(f, dir, shape(s)) == outcomeLoudRefuse && r.Intn(20) > 0 {
			s = int(shapeScalar)
			if !f.canSource(dir.src, shapeScalar) ||
				expectedOutcome(f, dir, shapeScalar) == outcomeLoudRefuse {
				continue
			}
		}
		c := genColumn{
			name: fmt.Sprintf("c%02d_%s_%s", colIdx, f.name, shapeTag(shape(s))),
			fam:  f,
			shp:  shape(s),
			ddl:  f.columnDDL(dir.src, shape(s)),
			// An array-shape loud-refuse column must force the refused
			// path; an all-NULL array would make the assertion vacuous
			// (Findings 1 & 2).
			forceNonNullLeaf: shape(s) != shapeScalar &&
				expectedOutcome(f, dir, shape(s)) == outcomeLoudRefuse,
		}
		if f.name == "enum" && dir.src == enginePG {
			enumType = enumTypeName
			c.ddl = enumType
		}
		cols = append(cols, c)
		colIdx++
	}

	// Guarantee at least one column (a degenerate empty table is
	// uninteresting and some engines reject it). The fallback is the
	// first family the SOURCE can actually spell (int8 for PG/MySQL;
	// int64 for SQLite, whose registry scope is narrower).
	if len(cols) == 0 {
		for _, f := range reg {
			if !f.canSource(dir.src, shapeScalar) {
				continue
			}
			cols = append(cols, genColumn{
				name: "c00_" + f.name + "_s", fam: f, shp: shapeScalar,
				ddl: f.columnDDL(dir.src, shapeScalar),
			})
			break
		}
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
	// nonNullScalar forces a real (non-NULL) leaf for a loud-refuse
	// array column so the unsupported-codec path actually fires (see
	// genColumn.forceNonNullLeaf). gen's NULL probability is ~1/6, so
	// the bounded retry effectively never exhausts; the final fallback
	// keeps generation total even in the impossible all-NULL case.
	nonNullScalar := func() string {
		for try := 0; try < 32; try++ {
			if v := scalar(); v != "NULL" {
				return v
			}
		}
		return scalar()
	}

	switch c.shp {
	case shapeScalar:
		v := scalar()
		if v == "NULL" {
			return "NULL"
		}
		return v

	case shape1DArray:
		// 1-in-7: the whole array column is SQL NULL — unless this is a
		// loud-refuse column, where a NULL/all-NULL value would make
		// the refusal assertion vacuous (Findings 1 & 2).
		if !c.forceNonNullLeaf && r.Intn(7) == 0 {
			return "NULL"
		}
		elems := make([]string, arrayLeafScalars)
		for i := range elems {
			elems[i] = scalar() // may be the literal "NULL" → NULL element
		}
		if c.forceNonNullLeaf {
			elems[0] = nonNullScalar() // guarantee ≥1 real element
		}
		return pgArrayLiteral(c, elems)

	case shapeMultiDim:
		if !c.forceNonNullLeaf && r.Intn(7) == 0 {
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
		if c.forceNonNullLeaf {
			// Re-render the [0][0] leaf as a guaranteed real element.
			firstInner := make([]string, multiDimSide)
			for j := range firstInner {
				firstInner[j] = scalar()
			}
			firstInner[0] = nonNullScalar()
			rows[0] = "[" + strings.Join(firstInner, ",") + "]"
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
