// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Random-op sync-convergence property core (docs/testing.md Layer 4,
// repo-audit task M3.12). This file is the pure, container-free half
// of the harness: the op representation, the rapid generators, the
// in-memory expected-state model, and the replayable-script renderer.
// The live half — real source + target databases with a Streamer in
// between — lives in sync_converge_integration_test.go behind the
// `integration` build tag.
//
// THE PROPERTY: for a random table shape and a random sequence of
// transactions of INSERT / UPDATE / PK-changing-UPDATE / DELETE /
// TRUNCATE applied to a live source while `sluice sync` streams, the
// target must converge to EXACTLY the source's final ordered content.
// The generator deliberately constructs the historically-nasty
// interleavings that hand-written CDC tests never cover:
//
//   - update-then-delete of the same row inside one tx (the classic
//     lost-update collapse shape);
//   - insert-then-update and insert-then-delete of the same row
//     inside one tx (a row born — and possibly gone — mid-tx);
//   - PK-changing UPDATEs (the row relocates under the applier);
//   - multi-row transactions, empty transactions, TRUNCATE (both
//     engines' apply paths support ir.Truncate);
//   - PK reuse: the keyspace is deliberately tiny (convPKSpace), so
//     an INSERT routinely resurrects a previously-deleted PK.
//
// Design mirrors the migrate fuzz harness (fuzzgen_*.go): the
// generator emits RAW source-dialect SQL applied directly to the
// source (never through sluice's writers — the independent oracle),
// every case renders as a replayable script, and the in-memory model
// is the validity oracle for the generated op sequence. Generation is
// driven by pgregory.net/rapid so a live failure SHRINKS to a minimal
// failing op sequence; determinism (same seed → byte-identical
// script) is pinned by TestConvergeGen_SameSeedRegeneratesByteIdenticalScript.
//
// Deliberate v1 scope limits (extensions, not gaps): one table per
// case (cross-TABLE tx interleaving is a follow-up axis), no
// mid-stream DDL (the schema-forward surface has its own suite), and
// no binary-float columns (FLOAT/DOUBLE before-image equality is an
// engine-semantics rabbit hole orthogonal to op interleaving; NUMERIC
// /DECIMAL covers the numeric family exactly).
//
// ---- Cross-engine convergence: a named wart ----
//
// The property runs in four directions: PG→PG, MySQL→MySQL (same
// engine) and PG→MySQL, MySQL→PG (cross engine). A convDirection
// carries the source AND target engine separately: the source engine
// drives the DDL/literal/CDC dialect; the target engine drives the
// canonical dump on the convergence side.
//
// Same-engine convergence compares source-dump == target-dump for
// EXACT byte equality, because both sides render through the identical
// server codec. Cross-engine CANNOT use byte equality: PG and MySQL
// render the same logical value to different text (PG `boolean`→"true"
// vs MySQL `tinyint(1)`→"1"; PG drops trailing-zero fractional seconds
// while MySQL DATETIME(6) keeps them; bytea hex vs raw; array→JSON;
// etc.). Asserting raw text-equality across engines would FAIL a
// faithful sync — the v0.69.0 #16 false-positive-refusal class the
// project forbids (see fuzzgen_oracle's Phase-1 scope note).
//
// Two design decisions contain this (both deliberately per-DIRECTION
// so the same-engine directions keep their richer surface unchanged):
//
//  1. SAFE-TYPE SUBSET (cross-engine only). The cross-engine column
//     type set is restricted to the families whose value round-trips
//     and CANONICALISES identically on both engines per
//     docs/value-types.md: int (BIGINT↔BIGINT, int64), text
//     (VARCHAR↔VARCHAR, string), numeric (NUMERIC(12,4)↔DECIMAL(12,4),
//     fixed-scale decimal string) and timestamp without time zone
//     (TIMESTAMP(6)↔DATETIME(6), wall-clock, no tz ambiguity). BOOL is
//     EXCLUDED cross-engine: PG `boolean::text` is "true"/"false" but
//     MySQL `tinyint(1)` is "1"/"0" — a representational, not lossy,
//     difference that no value-preserving canonicaliser should have to
//     paper over. Binary-float stays excluded everywhere (as above).
//     Same-engine keeps the full set INCLUDING bool. The subset lives
//     on convDirection.familySet so the generator picks it per
//     direction; convCrossEngineSafe is the single source of truth.
//
//  2. VALUE-SEMANTIC (not byte) COMPARE (cross-engine only). Each side
//     is rendered to per-engine canonical text on the SERVER (the
//     existing ::text / CAST AS CHAR dump), then a per-family Go-side
//     normaliser (convCanonField) folds the two engines' residual
//     spelling differences inside the safe set to ONE common form:
//     numeric "-0.0000"→"0.0000" (PG keeps the sign bit on a zero
//     magnitude; MySQL does not) and timestamp trailing-zero-fraction
//     stripping ("…05.000000"→"…05", "…05.100000"→"…05.1", since PG
//     renders the minimal fraction in ::text — all-zero disappears,
//     partial drops trailing zeros — while MySQL DATETIME(6) always
//     renders the full six digits). int and text need no normalisation — their
//     canonical text is already byte-identical across engines. The
//     normalised dumps are then compared for EXACT equality, so the
//     property is still "target == source's final ordered content",
//     just measured in a cross-engine-canonical space rather than raw
//     server text. Why normalise rather than widen the safe set with a
//     richer SQL projection: the residual differences are few and
//     value-preserving, and a Go-side normaliser is auditable in one
//     place (convCanonField) instead of scattered across two dialect
//     SELECTs.
//
// No build tag: pure logic, unit-tested by converge_gen_test.go.

package pipeline

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"pgregory.net/rapid"
)

// engineKind is the converge harness's engine axis — PG and MySQL,
// the two engines the four convDirections span. It was originally
// shared with the migrate fuzz harness's registry; when that machinery
// moved to internal/pipeline/internal/fuzzgen (repo-audit 2026-07-03
// finding A-3), this harness kept a minimal local copy rather than
// importing the fuzz package back into the shipped pipeline compile
// unit. (fuzzgen.EngineKind additionally carries SQLite, which the
// converge property deliberately does not cover — no CDC source there.)
type engineKind int

const (
	enginePG engineKind = iota
	engineMySQL
)

// String renders the engine name convDirection.String() embeds in
// case labels and replayable-script headers ("postgres->mysql").
func (e engineKind) String() string {
	if e == enginePG {
		return "postgres"
	}
	return "mysql"
}

// convPKSpace is the PK keyspace (1..convPKSpace). Deliberately tiny
// so deletes free PKs that later inserts reuse — the PK-reuse
// interleaving falls out of the generator naturally instead of
// needing a special case.
const (
	convPKSpace     = 40
	convMaxInitRows = 5
	convMaxOpsPerTx = 4
)

// ---- column families ----

// convFamily is the column-type axis. Every generated table carries
// at least one column of EVERY family (see convColsGen) — the
// pin-the-class discipline: the appliers bind parameters per column
// type, so a sequence that converges on int columns proves nothing
// about temporal or text columns.
type convFamily int

const (
	convFamInt convFamily = iota
	convFamText
	convFamBool
	convFamNumeric
	convFamTimestamp
	convFamCount // sentinel: number of families, not a family
)

func (f convFamily) String() string {
	switch f {
	case convFamInt:
		return "int"
	case convFamText:
		return "text"
	case convFamBool:
		return "bool"
	case convFamNumeric:
		return "numeric"
	case convFamTimestamp:
		return "timestamp"
	default:
		return "unknown-family"
	}
}

// columnDDL is the column type spelling in the source dialect.
func (f convFamily) columnDDL(eng engineKind) string {
	if eng == enginePG {
		switch f {
		case convFamInt:
			return "BIGINT"
		case convFamText:
			return "VARCHAR(255)"
		case convFamBool:
			return "BOOLEAN"
		case convFamNumeric:
			return "NUMERIC(12,4)"
		case convFamTimestamp:
			return "TIMESTAMP(6)"
		}
	}
	switch f {
	case convFamInt:
		return "BIGINT"
	case convFamText:
		return "VARCHAR(255)"
	case convFamBool:
		return "TINYINT(1)"
	case convFamNumeric:
		return "DECIMAL(12,4)"
	case convFamTimestamp:
		return "DATETIME(6)"
	}
	return ""
}

// convCrossEngineSafe reports whether a family canonicalises
// identically across PG and MySQL and so may appear in a CROSS-engine
// case (design decision #1 — see the header). The same-engine
// directions use every family regardless. bool is the only family the
// same-engine set carries that the cross-engine set drops (PG "true"
// vs MySQL "1"); binary-float is excluded everywhere upstream of this.
func convCrossEngineSafe(f convFamily) bool {
	switch f {
	case convFamInt, convFamText, convFamNumeric, convFamTimestamp:
		return true
	default:
		return false
	}
}

// convDirection is an ordered (source, target) engine pair. The source
// engine drives DDL/literal/CDC dialect; the target engine drives the
// convergence-side canonical dump. crossEngine() being true selects
// the safe-type subset and the value-semantic (normalised) compare.
type convDirection struct {
	src, dst engineKind
}

func (d convDirection) String() string { return d.src.String() + "->" + d.dst.String() }

func (d convDirection) crossEngine() bool { return d.src != d.dst }

// families lists, in family order, the families this direction may
// generate: the full set for same-engine, the cross-engine-safe subset
// for cross-engine (design decision #1).
func (d convDirection) families() []convFamily {
	out := make([]convFamily, 0, convFamCount)
	for f := convFamily(0); f < convFamCount; f++ {
		if d.crossEngine() && !convCrossEngineSafe(f) {
			continue
		}
		out = append(out, f)
	}
	return out
}

// convDirPGToPG and friends name the four directions the property
// exercises.
var (
	convDirPGToPG       = convDirection{enginePG, enginePG}
	convDirMySQLToMySQL = convDirection{engineMySQL, engineMySQL}
	convDirPGToMySQL    = convDirection{enginePG, engineMySQL}
	convDirMySQLToPG    = convDirection{engineMySQL, enginePG}
)

// convColumn is one generated non-PK column. The id BIGINT PK is
// implicit on every table.
type convColumn struct {
	name string
	fam  convFamily
}

// convValue is one cell: NULL, or a dialect-neutral payload rendered
// to a SQL literal per dialect at script-render time. The payload is
// never compared against database output — the live property compares
// source-dump vs target-dump (both server-rendered), so the model
// only needs payloads to be valid, not canonically formatted.
type convValue struct {
	null    bool
	payload string
}

// literal renders the value as a source-dialect SQL literal.
func (v convValue) literal(f convFamily, eng engineKind) string {
	if v.null {
		return "NULL"
	}
	switch f {
	case convFamInt, convFamNumeric:
		return v.payload
	case convFamBool:
		if eng == enginePG {
			if v.payload == "1" {
				return "TRUE"
			}
			return "FALSE"
		}
		return v.payload // TINYINT(1): 1 / 0
	case convFamTimestamp:
		return "'" + v.payload + "'"
	case convFamText:
		return quoteConvString(v.payload, eng)
	default:
		return "NULL"
	}
}

// quoteConvString quotes a text payload per dialect. Single quotes
// double in both dialects; MySQL's default sql_mode additionally
// treats backslash as an escape character inside string literals,
// while PG (standard_conforming_strings=on) does not.
func quoteConvString(s string, eng engineKind) string {
	s = strings.ReplaceAll(s, "'", "''")
	if eng == engineMySQL {
		s = strings.ReplaceAll(s, `\`, `\\`)
	}
	return "'" + s + "'"
}

// ---- ops, transactions, cases ----

type convOpKind int

const (
	convOpInsert convOpKind = iota
	convOpUpdate
	convOpPKUpdate
	convOpDelete
	convOpTruncate
)

// convOp is one statement. Field usage by kind:
//
//	insert:   pk + row (one value per table column)
//	update:   pk + set (column indexes) + row (one value per set entry)
//	pkUpdate: pk (old) + newPK
//	delete:   pk
//	truncate: no fields
type convOp struct {
	kind  convOpKind
	pk    int64
	newPK int64
	row   []convValue
	set   []int
}

// convTxPattern names how a transaction was constructed, so the
// dumped script reads as intent ("tx 3 (update-then-delete)") and the
// generator-rot pin (TestConvergeGen_NastyInterleavingsGenerated) can
// assert every nasty shape actually gets generated.
type convTxPattern int

const (
	convTxPlain convTxPattern = iota
	convTxEmpty
	convTxUpdateThenDelete
	convTxInsertThenUpdate
	convTxInsertThenDelete
	convTxTruncate
)

func (p convTxPattern) String() string {
	switch p {
	case convTxPlain:
		return "plain"
	case convTxEmpty:
		return "empty"
	case convTxUpdateThenDelete:
		return "update-then-delete"
	case convTxInsertThenUpdate:
		return "insert-then-update"
	case convTxInsertThenDelete:
		return "insert-then-delete"
	case convTxTruncate:
		return "truncate"
	default:
		return "unknown-pattern"
	}
}

type convTx struct {
	pattern convTxPattern
	ops     []convOp
}

// convCase is one fully-drawn property case: the direction, the table
// shape, the rows present before the stream attaches (delivered by
// bulk-copy), the mid-stream transactions (delivered by CDC), and the
// applier batch size the stream runs with. The case carries no table
// name — rendering takes one as a parameter so the live harness can use
// a per-check unique name while the determinism pin renders against a
// fixed placeholder.
//
// eng is the SOURCE engine (dir.src): all literal/DDL rendering is
// source-side. dir additionally carries the target engine and the
// cross-engine flag, which the live harness uses to pick the
// convergence-side dump dialect and the value-semantic compare.
type convCase struct {
	dir        convDirection
	eng        engineKind
	cols       []convColumn
	initial    []convOp // insert ops applied before the stream starts
	txs        []convTx
	applyBatch int
}

// ---- the expected-state model ----

// convModel is the pure in-memory model of the source table: pk →
// row values. It is both the generator's validity oracle (ops are
// drawn against the model's live/free PK sets, so every generated —
// and every SHRUNK — sequence is executable) and the live harness's
// independent check that the script really produced the state the
// generator intended.
type convModel struct {
	rows map[int64][]convValue
}

func newConvModel() *convModel {
	return &convModel{rows: map[int64][]convValue{}}
}

// apply mutates the model by one op, refusing loudly when the op is
// invalid against the current state (generated ops are valid by
// construction; the error path exists so hand-built or corrupted
// sequences fail loudly instead of silently diverging the model).
func (m *convModel) apply(op convOp, nCols int) error {
	switch op.kind {
	case convOpInsert:
		if _, ok := m.rows[op.pk]; ok {
			return fmt.Errorf("insert: pk %d already live", op.pk)
		}
		if len(op.row) != nCols {
			return fmt.Errorf("insert: row has %d values, table has %d columns", len(op.row), nCols)
		}
		m.rows[op.pk] = append([]convValue(nil), op.row...)
	case convOpUpdate:
		row, ok := m.rows[op.pk]
		if !ok {
			return fmt.Errorf("update: pk %d not live", op.pk)
		}
		if len(op.set) == 0 || len(op.set) != len(op.row) {
			return fmt.Errorf("update: set has %d columns, row has %d values", len(op.set), len(op.row))
		}
		for i, ci := range op.set {
			if ci < 0 || ci >= nCols {
				return fmt.Errorf("update: set column index %d out of range [0,%d)", ci, nCols)
			}
			row[ci] = op.row[i]
		}
	case convOpPKUpdate:
		row, ok := m.rows[op.pk]
		if !ok {
			return fmt.Errorf("pk-update: pk %d not live", op.pk)
		}
		if _, clash := m.rows[op.newPK]; clash {
			return fmt.Errorf("pk-update: new pk %d already live", op.newPK)
		}
		delete(m.rows, op.pk)
		m.rows[op.newPK] = row
	case convOpDelete:
		if _, ok := m.rows[op.pk]; !ok {
			return fmt.Errorf("delete: pk %d not live", op.pk)
		}
		delete(m.rows, op.pk)
	case convOpTruncate:
		m.rows = map[int64][]convValue{}
	default:
		return fmt.Errorf("unknown op kind %d", op.kind)
	}
	return nil
}

// livePKs returns the live PKs in ascending order.
func (m *convModel) livePKs() []int64 {
	out := make([]int64, 0, len(m.rows))
	for pk := range m.rows {
		out = append(out, pk)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// freePKs returns the unused PKs in 1..convPKSpace, ascending.
func (m *convModel) freePKs() []int64 {
	out := make([]int64, 0, convPKSpace)
	for pk := int64(1); pk <= convPKSpace; pk++ {
		if _, live := m.rows[pk]; !live {
			out = append(out, pk)
		}
	}
	return out
}

// convMustApply applies a generator-built op to the tracking model.
// Generated ops are valid by construction (each draw consults the
// model), so a failure here is a generator bug — pinned separately by
// TestConvergeGen_GeneratedOpsAreModelValid.
func convMustApply(m *convModel, op convOp, nCols int) {
	if err := m.apply(op, nCols); err != nil {
		panic(fmt.Sprintf("converge generator drew a model-invalid op: %v", err))
	}
}

// finalModel replays initial+txs into a fresh model — the expected
// source state after the whole script has been applied. The error
// path is unreachable for a generator-built case; it exists so a
// hand-built invalid case fails loudly.
func (c *convCase) finalModel() (*convModel, error) {
	m := newConvModel()
	for _, op := range c.initial {
		if err := m.apply(op, len(c.cols)); err != nil {
			return nil, fmt.Errorf("initial: %w", err)
		}
	}
	for ti, tx := range c.txs {
		for _, op := range tx.ops {
			if err := m.apply(op, len(c.cols)); err != nil {
				return nil, fmt.Errorf("tx %d (%s): %w", ti, tx.pattern, err)
			}
		}
	}
	return m, nil
}

// ---- rapid generators ----

// convPayloadGen draws a dialect-neutral payload for one family,
// folding in the catalogued hazards: int boundary values, quote/
// backslash/multi-byte/emoji/whitespace text, negative-zero-ish and
// max-scale numerics, microsecond-precision timestamps.
func convPayloadGen(f convFamily) *rapid.Generator[string] {
	switch f {
	case convFamInt:
		return rapid.OneOf(
			rapid.SampledFrom([]string{"0", "1", "-1", "9223372036854775807", "-9223372036854775808"}),
			rapid.Map(rapid.Int64Range(-1_000_000, 1_000_000), func(n int64) string {
				return strconv.FormatInt(n, 10)
			}),
		)
	case convFamBool:
		return rapid.SampledFrom([]string{"0", "1"})
	case convFamNumeric:
		return rapid.Custom(func(t *rapid.T) string {
			whole := rapid.Int64Range(-99_999_999, 99_999_999).Draw(t, "whole")
			frac := rapid.Int64Range(0, 9_999).Draw(t, "frac")
			sign := ""
			if whole == 0 && rapid.Bool().Draw(t, "negzero") {
				sign = "-"
			}
			return fmt.Sprintf("%s%d.%04d", sign, whole, frac)
		})
	case convFamTimestamp:
		return rapid.Custom(func(t *rapid.T) string {
			return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d",
				rapid.IntRange(1990, 2035).Draw(t, "y"),
				rapid.IntRange(1, 12).Draw(t, "mo"),
				rapid.IntRange(1, 28).Draw(t, "d"),
				rapid.IntRange(0, 23).Draw(t, "h"),
				rapid.IntRange(0, 59).Draw(t, "mi"),
				rapid.IntRange(0, 59).Draw(t, "s"),
				rapid.IntRange(0, 999_999).Draw(t, "us"))
		})
	case convFamText:
		// Fragments cover the escaping hazards (quote, backslash),
		// charset hazards (accents, CJK, emoji — 4-byte utf8mb4), and
		// trim hazards (empty, padded). NUL is deliberately excluded
		// (PG rejects it in text values).
		frags := []string{
			"", "a", "Zz9", "it's", `c:\tmp`, "émü", "漢字", "🙂🚀",
			"two words", " padded ", "%_wild_%",
		}
		return rapid.Custom(func(t *rapid.T) string {
			n := rapid.IntRange(0, 3).Draw(t, "nfrags")
			parts := make([]string, n)
			for i := range parts {
				parts[i] = rapid.SampledFrom(frags).Draw(t, "frag")
			}
			return strings.Join(parts, "")
		})
	default:
		return rapid.Just("")
	}
}

// convValueGen draws one cell: NULL ~1/6 of the time, else a family
// payload.
func convValueGen(f convFamily) *rapid.Generator[convValue] {
	return rapid.Custom(func(t *rapid.T) convValue {
		if rapid.IntRange(0, 5).Draw(t, "null") == 0 {
			return convValue{null: true}
		}
		return convValue{payload: convPayloadGen(f).Draw(t, "payload")}
	})
}

// convColsGen draws the table shape for one direction. Every family in
// the direction's family set is ALWAYS present at least once — pin the
// class, not the representative: the smoke budget runs only a few
// checks, so per-family coverage must hold in every check, not just in
// expectation. For a cross-engine direction the set is the safe subset
// (no bool); same-engine carries the full set. The randomness is in
// the per-family column count (1..2) and the column order.
func convColsGen(dir convDirection) *rapid.Generator[[]convColumn] {
	fams := dir.families()
	return rapid.Custom(func(t *rapid.T) []convColumn {
		var cols []convColumn
		for _, f := range fams {
			n := rapid.IntRange(1, 2).Draw(t, "ncols_"+f.String())
			for i := 0; i < n; i++ {
				cols = append(cols, convColumn{fam: f})
			}
		}
		cols = rapid.Permutation(cols).Draw(t, "order")
		for i := range cols {
			cols[i].name = fmt.Sprintf("c%02d_%s", i, cols[i].fam)
		}
		return cols
	})
}

func convColIndexes(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}

// convRowGen draws a full row (one value per column).
func convRowGen(t *rapid.T, cols []convColumn) []convValue {
	row := make([]convValue, len(cols))
	for i, c := range cols {
		row[i] = convValueGen(c.fam).Draw(t, "v_"+c.name)
	}
	return row
}

// convDrawInsert draws an INSERT at a free PK, or reports false when
// the keyspace is full.
func convDrawInsert(t *rapid.T, m *convModel, cols []convColumn) (convOp, bool) {
	free := m.freePKs()
	if len(free) == 0 {
		return convOp{}, false
	}
	return convOp{
		kind: convOpInsert,
		pk:   rapid.SampledFrom(free).Draw(t, "ins_pk"),
		row:  convRowGen(t, cols),
	}, true
}

// convDrawUpdateOf draws an UPDATE of the given PK assigning a random
// non-empty subset of columns.
func convDrawUpdateOf(t *rapid.T, pk int64, cols []convColumn) convOp {
	nSet := rapid.IntRange(1, len(cols)).Draw(t, "upd_nset")
	perm := rapid.Permutation(convColIndexes(len(cols))).Draw(t, "upd_setcols")
	idxs := append([]int(nil), perm[:nSet]...)
	sort.Ints(idxs)
	op := convOp{kind: convOpUpdate, pk: pk, set: idxs}
	for _, ci := range idxs {
		op.row = append(op.row, convValueGen(cols[ci].fam).Draw(t, "v_"+cols[ci].name))
	}
	return op
}

// convDrawUpdate draws an UPDATE of a random live row, or reports
// false when no row is live.
func convDrawUpdate(t *rapid.T, m *convModel, cols []convColumn) (convOp, bool) {
	live := m.livePKs()
	if len(live) == 0 {
		return convOp{}, false
	}
	return convDrawUpdateOf(t, rapid.SampledFrom(live).Draw(t, "upd_pk"), cols), true
}

// convDrawPKUpdate draws a PK-changing UPDATE (live PK → free PK), or
// reports false when either pool is empty.
func convDrawPKUpdate(t *rapid.T, m *convModel) (convOp, bool) {
	live, free := m.livePKs(), m.freePKs()
	if len(live) == 0 || len(free) == 0 {
		return convOp{}, false
	}
	return convOp{
		kind:  convOpPKUpdate,
		pk:    rapid.SampledFrom(live).Draw(t, "pkupd_old"),
		newPK: rapid.SampledFrom(free).Draw(t, "pkupd_new"),
	}, true
}

// convDrawPlainOp draws one op for a plain tx, weighted, with kinds
// whose preconditions fail excluded. The pool is never empty: an
// empty table always admits insert, a full keyspace always admits
// update.
func convDrawPlainOp(t *rapid.T, label string, m *convModel, cols []convColumn) convOp {
	var kinds []convOpKind
	add := func(k convOpKind, weight int) {
		for i := 0; i < weight; i++ {
			kinds = append(kinds, k)
		}
	}
	hasFree, hasLive := len(m.freePKs()) > 0, len(m.livePKs()) > 0
	if hasFree {
		add(convOpInsert, 3)
	}
	if hasLive {
		add(convOpUpdate, 3)
		add(convOpDelete, 2)
		if hasFree {
			add(convOpPKUpdate, 1)
		}
	}
	switch rapid.SampledFrom(kinds).Draw(t, label+"_kind") {
	case convOpInsert:
		op, _ := convDrawInsert(t, m, cols)
		return op
	case convOpUpdate:
		op, _ := convDrawUpdate(t, m, cols)
		return op
	case convOpPKUpdate:
		op, _ := convDrawPKUpdate(t, m)
		return op
	default:
		live := m.livePKs()
		return convOp{kind: convOpDelete, pk: rapid.SampledFrom(live).Draw(t, label+"_del_pk")}
	}
}

// convTxGen draws one transaction, mutating the model as it goes so
// later draws stay valid. Nasty patterns are generated EXPLICITLY
// (not hoped for); a pattern whose precondition isn't met at this
// point in the sequence falls back to a plain tx.
func convTxGen(t *rapid.T, label string, m *convModel, cols []convColumn) convTx {
	weighted := []convTxPattern{
		convTxPlain, convTxPlain, convTxPlain, convTxPlain,
		convTxEmpty,
		convTxUpdateThenDelete,
		convTxInsertThenUpdate,
		convTxInsertThenDelete,
		convTxTruncate,
	}
	nCols := len(cols)
	switch rapid.SampledFrom(weighted).Draw(t, label+"_pattern") {
	case convTxEmpty:
		return convTx{pattern: convTxEmpty}

	case convTxTruncate:
		op := convOp{kind: convOpTruncate}
		convMustApply(m, op, nCols)
		return convTx{pattern: convTxTruncate, ops: []convOp{op}}

	case convTxUpdateThenDelete:
		// The classic lost-update collapse shape: an UPDATE and an
		// immediately-following DELETE of the same row in one tx.
		if upd, ok := convDrawUpdate(t, m, cols); ok {
			convMustApply(m, upd, nCols)
			del := convOp{kind: convOpDelete, pk: upd.pk}
			convMustApply(m, del, nCols)
			return convTx{pattern: convTxUpdateThenDelete, ops: []convOp{upd, del}}
		}

	case convTxInsertThenUpdate:
		if ins, ok := convDrawInsert(t, m, cols); ok {
			convMustApply(m, ins, nCols)
			upd := convDrawUpdateOf(t, ins.pk, cols)
			convMustApply(m, upd, nCols)
			return convTx{pattern: convTxInsertThenUpdate, ops: []convOp{ins, upd}}
		}

	case convTxInsertThenDelete:
		// A row born and gone inside one tx — invisible in the final
		// state, but its events still cross the wire.
		if ins, ok := convDrawInsert(t, m, cols); ok {
			convMustApply(m, ins, nCols)
			del := convOp{kind: convOpDelete, pk: ins.pk}
			convMustApply(m, del, nCols)
			return convTx{pattern: convTxInsertThenDelete, ops: []convOp{ins, del}}
		}
	}

	// Plain multi-op tx — also the fallback when a pattern's
	// precondition (live row / free PK) isn't met.
	tx := convTx{pattern: convTxPlain}
	nOps := rapid.IntRange(1, convMaxOpsPerTx).Draw(t, label+"_nops")
	for i := 0; i < nOps; i++ {
		op := convDrawPlainOp(t, fmt.Sprintf("%s_op%d", label, i), m, cols)
		convMustApply(m, op, nCols)
		tx.ops = append(tx.ops, op)
	}
	return tx
}

// convCaseGen builds the full random case for one direction. The
// direction selects the source dialect (rendering) AND the column
// family set (the cross-engine-safe subset vs the full set). maxTxs is
// the op budget (SLUICE_CONVERGE_OPS at the live harness; fixed small
// constants in the unit pins — note a replay must use the SAME budget,
// since it shapes the draw sequence). At least one initial row is
// always drawn so the live harness can observe bulk-copy completion
// via the target row count before it starts the finite op burst.
func convCaseGen(dir convDirection, maxTxs int) *rapid.Generator[convCase] {
	return rapid.Custom(func(t *rapid.T) convCase {
		cols := convColsGen(dir).Draw(t, "cols")
		m := newConvModel()
		c := convCase{dir: dir, eng: dir.src, cols: cols}

		nInit := rapid.IntRange(1, convMaxInitRows).Draw(t, "ninit")
		for i := 0; i < nInit; i++ {
			op, ok := convDrawInsert(t, m, cols)
			if !ok {
				break // unreachable: convPKSpace > convMaxInitRows
			}
			convMustApply(m, op, len(cols))
			c.initial = append(c.initial, op)
		}

		nTx := rapid.IntRange(1, maxTxs).Draw(t, "ntx")
		for i := 0; i < nTx; i++ {
			c.txs = append(c.txs, convTxGen(t, fmt.Sprintf("tx%d", i), m, cols))
		}

		// The applier batch size is part of the interleaving surface:
		// 0 = per-change apply; >1 = the batched applier (idle-flush
		// grace + AIMD — a historical bug source).
		c.applyBatch = rapid.SampledFrom([]int{0, 8, 64}).Draw(t, "applybatch")
		return c
	})
}

// ---- script rendering ----

// renderSetup renders the pre-stream part of the replayable script:
// DROP/CREATE TABLE (+ REPLICA IDENTITY FULL on PG — full UPDATE/
// DELETE before-images, what every streamer integration test sets and
// what the applier's Before-image WHERE needs to locate rows, incl.
// PK-changing UPDATEs) and the initial INSERTs bulk-copy delivers.
func (c *convCase) renderSetup(table string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "DROP TABLE IF EXISTS %s;\n", table)
	b.WriteString("CREATE TABLE " + table + " (\n  id BIGINT NOT NULL PRIMARY KEY")
	for _, col := range c.cols {
		fmt.Fprintf(&b, ",\n  %s %s", col.name, col.fam.columnDDL(c.eng))
	}
	if c.eng == engineMySQL {
		b.WriteString("\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n")
	} else {
		b.WriteString("\n);\n")
		fmt.Fprintf(&b, "ALTER TABLE %s REPLICA IDENTITY FULL;\n", table)
	}
	for _, op := range c.initial {
		b.WriteString(c.renderOp(op, table) + "\n")
	}
	return b.String()
}

// renderTx renders one mid-stream transaction block. The live harness
// executes each block as its OWN driver call so transaction
// boundaries are unambiguous on both engines (a PG simple-protocol
// multi-statement string runs under one implicit transaction unless
// explicit control statements force otherwise — per-block execution
// sidesteps that subtlety entirely).
func (c *convCase) renderTx(tx convTx, idx int, table string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "-- tx %d (%s)\n", idx, tx.pattern)
	switch tx.pattern {
	case convTxEmpty:
		b.WriteString("BEGIN;\nCOMMIT;\n")
	case convTxTruncate:
		// Outside BEGIN/COMMIT: MySQL TRUNCATE is DDL with an implicit
		// commit, so wrapping it would render dialect-divergent
		// semantics. Standalone autocommit is identical on both.
		b.WriteString(c.renderOp(tx.ops[0], table) + "\n")
	default:
		b.WriteString("BEGIN;\n")
		for _, op := range tx.ops {
			b.WriteString(c.renderOp(op, table) + "\n")
		}
		b.WriteString("COMMIT;\n")
	}
	return b.String()
}

// renderOps renders every mid-stream transaction block.
func (c *convCase) renderOps(table string) string {
	var b strings.Builder
	for i, tx := range c.txs {
		b.WriteString(c.renderTx(tx, i, table))
	}
	return b.String()
}

// renderOp renders one statement in the source dialect.
func (c *convCase) renderOp(op convOp, table string) string {
	switch op.kind {
	case convOpInsert:
		names := make([]string, 0, len(c.cols)+1)
		vals := make([]string, 0, len(c.cols)+1)
		names = append(names, "id")
		vals = append(vals, strconv.FormatInt(op.pk, 10))
		for i, col := range c.cols {
			names = append(names, col.name)
			vals = append(vals, op.row[i].literal(col.fam, c.eng))
		}
		return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",
			table, strings.Join(names, ", "), strings.Join(vals, ", "))
	case convOpUpdate:
		sets := make([]string, len(op.set))
		for i, ci := range op.set {
			sets[i] = fmt.Sprintf("%s = %s", c.cols[ci].name, op.row[i].literal(c.cols[ci].fam, c.eng))
		}
		return fmt.Sprintf("UPDATE %s SET %s WHERE id = %d;", table, strings.Join(sets, ", "), op.pk)
	case convOpPKUpdate:
		return fmt.Sprintf("UPDATE %s SET id = %d WHERE id = %d;", table, op.newPK, op.pk)
	case convOpDelete:
		return fmt.Sprintf("DELETE FROM %s WHERE id = %d;", table, op.pk)
	case convOpTruncate:
		return fmt.Sprintf("TRUNCATE TABLE %s;", table)
	default:
		return ""
	}
}

// renderScript is the full replayable script (the dumped-fixture
// form): setup, a marker where the sync stream attaches, then the
// mid-stream transactions.
func (c *convCase) renderScript(table string) string {
	return c.renderSetup(table) +
		"-- >>> sluice sync stream attaches here <<<\n" +
		c.renderOps(table)
}

// ---- cross-engine value-semantic canonicalisation (design #2) ----
//
// convCanonField folds the residual per-engine spelling differences of
// ONE field (already server-rendered to canonical text via ::text /
// CAST AS CHAR) inside the cross-engine-safe family set down to a
// single common form, so two engines' dumps of the same logical value
// compare EXACTLY equal. It is applied to BOTH the source and target
// dump cross-engine; for same-engine the dumps already match byte for
// byte and this is not used (the live harness keeps the exact-equality
// path there). NULL is signalled by the caller (valid=false) and never
// reaches here.
//
// Per family:
//
//   - int / text: no normalisation. BIGINT renders "123" on both
//     engines; VARCHAR text content is identical.
//   - numeric: strip a leading "-" when the magnitude is all zeros. PG
//     `numeric` preserves the sign bit of a negative zero ("-0.0000")
//     while MySQL `decimal` renders "0.0000"; the values are equal.
//     Both engines already pad to the declared scale (4), so no scale
//     normalisation is needed.
//   - timestamp: strip ALL trailing zeros from the fractional part
//     (and the lone "." when the whole fraction was zero), folding both
//     engines to PG's form. PG `timestamp(6)::text` renders the minimal
//     fraction — an all-zero fraction disappears ("…05"), and a partial
//     one drops its trailing zeros (".100000"→".1", ".123000"→".123") —
//     whereas MySQL `DATETIME(6)` CAST always renders the full six
//     digits ("…05.000000", "…05.100000"). Stripping trailing zeros on
//     both sides makes them coincide. (The earlier "all-zero only" form
//     was wrong: it left ".100000" vs ".1" divergent, so a faithful
//     cross-engine sync of a sub-second value ending in a zero — e.g.
//     microsecond 100000 — would spuriously fail to "converge".)
func convCanonField(f convFamily, text string) string {
	switch f {
	case convFamNumeric:
		return convCanonNumeric(text)
	case convFamTimestamp:
		return convCanonTimestamp(text)
	default:
		return text
	}
}

// convCanonNumeric normalises "-0", "-0.0000" → "0", "0.0000": a
// leading minus on an all-zero magnitude is dropped. Any non-zero
// digit leaves the value untouched.
func convCanonNumeric(text string) string {
	if !strings.HasPrefix(text, "-") {
		return text
	}
	if strings.ContainsAny(text[1:], "123456789") {
		return text // genuinely non-zero negative
	}
	return text[1:] // all-zero magnitude — drop the sign
}

// convCanonTimestamp strips ALL trailing zeros from the fractional-
// second part, folding both engines to PG's minimal rendering: PG's
// "…05" / "…05.1" and MySQL's "…05.000000" / "…05.100000" coincide.
// MySQL `DATETIME(6)` always renders six fractional digits; PG
// `timestamp(6)::text` trims trailing zeros (and the whole fraction
// when it is all zero), so the fold must match PG, not merely drop the
// all-zero case.
func convCanonTimestamp(text string) string {
	dot := strings.LastIndexByte(text, '.')
	if dot < 0 {
		return text // no fractional part (PG integer-second form)
	}
	frac := strings.TrimRight(text[dot+1:], "0")
	if frac == "" {
		return text[:dot] // all-zero fraction — drop it and the dot
	}
	return text[:dot+1] + frac // partial fraction — trailing zeros trimmed
}
