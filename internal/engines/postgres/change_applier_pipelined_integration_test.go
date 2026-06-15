//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pins for the ADR-0092 pipelined CDC apply path.
//
// The pipelined path (a single pgx.Batch flush per batch instead of N
// serial execs) reuses the SAME build{Insert,Update,Delete}SQL builders
// and prepareApplierValue codec path as the serial *sql.Tx path, so value
// encoding is meant to be byte-identical. But pipelining is exactly the
// class of change CLAUDE.md's "pin the class, not the representative"
// mandate targets: pgx encodes each queued statement's parameters against
// the target column OID during SendBatch, and the array codec in
// particular (Bug 74) dispatches per element OID — a green pin on one
// family does NOT cover the others. So:
//
//   - TestPipelined_ValueFidelity_Matrix drives the full type-family ×
//     shape matrix (native / string-leaf / temporal element families ×
//     {1-D, 2-D multidim, NULL-element}, plus the scalar rich families)
//     THROUGH the pipelined batch, src==dst ground-truthed on the real
//     target (PG array_dims + element ::text).
//   - TestPipelined_EquivalenceWithSerial replays one mixed I/U/D +
//     Truncate + SchemaSnapshot stream through BOTH paths and asserts
//     byte-identical target state.
//   - TestPipelined_AtomicityMidBatchError pins that a mid-batch exec
//     error rolls back BOTH data and position, and that replay from the
//     prior boundary reproduces the batch idempotently.
//   - TestPipelined_GAP3_AlterTypeWiden pins that a forwarded ALTER TYPE
//     int4→bigint followed by an out-of-old-range value applies through
//     the pipelined batch (no stale-OID encode failure — the DescribeExec
//     pool re-describes each distinct statement fresh, caching nothing).

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// openPipelinedApplier opens a ChangeApplier and asserts that the
// pipelined path is actually engaged (pipelineCfg wired), so a silent
// fall-back to the serial path can't make these pins pass for the wrong
// reason. The applier's first batch lazily opens the Exec-mode pool.
func openPipelinedApplier(t *testing.T, ctx context.Context, dsn string) *ChangeApplier {
	t.Helper()
	eng := Engine{}
	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	ca, ok := applier.(*ChangeApplier)
	if !ok {
		t.Fatalf("OpenChangeApplier returned %T; want *ChangeApplier", applier)
	}
	if ca.pipelineCfg == nil {
		t.Fatal("pipelineCfg is nil — pipelined path would silently fall back to serial (ADR-0092)")
	}
	return ca
}

// arrayCol is one (column, PG type, value) entry in the pipelined array
// fidelity matrix. The applier derives the column's IR type from the live
// target catalog (loadColumnTypes), so the matrix only needs the DDL type
// and the canonical IR []any value (nested for multi-dim, nil slots for
// NULL elements) — the IR element family is selected by the target OID,
// which is exactly the per-OID dispatch the Bug-74 pin exists to cover.
type arrayCol struct {
	name     string
	pgType   string
	value    []any
	wantDims string // expected PG array_dims(col)
	wantText string // expected col::text (the wire round-trip ground truth)
}

// TestPipelined_ValueFidelity_Matrix is the Bug-74 corollary pin for the
// pipelined path: every array element family × shape, plus the scalar rich
// families, applied through ApplyBatch (which uses the pipelined batch),
// then ground-truthed on the real target via array_dims + ::text. A
// per-OID array-codec regression (the Bug-74 multi-dim flatten) would show
// up here as a wrong array_dims for that family's 2-D row.
func TestPipelined_ValueFidelity_Matrix(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	tz := time.FixedZone("UTC+5:30", int((5*time.Hour+30*time.Minute)/time.Second))

	cols := []arrayCol{
		// --- native element families (int / float / bool) ---
		{"a_int1", "INTEGER[]", []any{int64(1), int64(2), int64(3)}, "[1:3]", "{1,2,3}"},
		{"a_int2", "INTEGER[]", []any{[]any{int64(1), int64(2)}, []any{int64(3), int64(4)}}, "[1:2][1:2]", "{{1,2},{3,4}}"},
		{"a_intn", "INTEGER[]", []any{int64(1), nil, int64(3)}, "[1:3]", "{1,NULL,3}"},
		{"a_intnn", "INTEGER[]", []any{[]any{int64(1), nil}, []any{nil, int64(4)}}, "[1:2][1:2]", "{{1,NULL},{NULL,4}}"},
		{"a_flt1", "DOUBLE PRECISION[]", []any{1.5, 2.5}, "[1:2]", "{1.5,2.5}"},
		{"a_flt2", "DOUBLE PRECISION[]", []any{[]any{1.5, 2.5}, []any{3.5, 4.5}}, "[1:2][1:2]", "{{1.5,2.5},{3.5,4.5}}"},
		{"a_fltn", "DOUBLE PRECISION[]", []any{1.5, nil, 3.5}, "[1:3]", "{1.5,NULL,3.5}"},
		{"a_bool1", "BOOLEAN[]", []any{true, false}, "[1:2]", "{t,f}"},
		{"a_bool2", "BOOLEAN[]", []any{[]any{true, false}, []any{false, true}}, "[1:2][1:2]", "{{t,f},{f,t}}"},
		{"a_booln", "BOOLEAN[]", []any{true, nil, false}, "[1:3]", "{t,NULL,f}"},

		// --- string-leaf element families ---
		{"a_txt1", "TEXT[]", []any{"a", "b"}, "[1:2]", "{a,b}"},
		{"a_txt2", "TEXT[]", []any{[]any{"a", "b"}, []any{"c", "d"}}, "[1:2][1:2]", "{{a,b},{c,d}}"},
		{"a_txtn", "TEXT[]", []any{"a", nil, "c"}, "[1:3]", "{a,NULL,c}"},
		{"a_vc1", "VARCHAR(16)[]", []any{"x", "y"}, "[1:2]", "{x,y}"},
		{"a_uuid1", "UUID[]", []any{"00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"}, "[1:2]", "{00000000-0000-0000-0000-000000000001,00000000-0000-0000-0000-000000000002}"},
		{"a_uuid2", "UUID[]", []any{[]any{"00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"}, []any{"00000000-0000-0000-0000-000000000003", "00000000-0000-0000-0000-000000000004"}}, "[1:2][1:2]", "{{00000000-0000-0000-0000-000000000001,00000000-0000-0000-0000-000000000002},{00000000-0000-0000-0000-000000000003,00000000-0000-0000-0000-000000000004}}"},
		{"a_uuidn", "UUID[]", []any{"00000000-0000-0000-0000-000000000001", nil, "00000000-0000-0000-0000-000000000003"}, "[1:3]", "{00000000-0000-0000-0000-000000000001,NULL,00000000-0000-0000-0000-000000000003}"},
		{"a_inet1", "INET[]", []any{"10.0.0.1", "10.0.0.2"}, "[1:2]", "{10.0.0.1,10.0.0.2}"},
		{"a_inet2", "INET[]", []any{[]any{"10.0.0.1", "10.0.0.2"}, []any{"10.0.0.3", "10.0.0.4"}}, "[1:2][1:2]", "{{10.0.0.1,10.0.0.2},{10.0.0.3,10.0.0.4}}"},
		{"a_inetn", "INET[]", []any{"10.0.0.1", nil, "10.0.0.3"}, "[1:3]", "{10.0.0.1,NULL,10.0.0.3}"},
		{"a_cidr1", "CIDR[]", []any{"10.0.0.0/24", "10.1.0.0/24"}, "[1:2]", "{10.0.0.0/24,10.1.0.0/24}"},
		{"a_cidr2", "CIDR[]", []any{[]any{"10.0.0.0/24", "10.1.0.0/24"}, []any{"10.2.0.0/24", "10.3.0.0/24"}}, "[1:2][1:2]", "{{10.0.0.0/24,10.1.0.0/24},{10.2.0.0/24,10.3.0.0/24}}"},
		{"a_cidrn", "CIDR[]", []any{"10.0.0.0/24", nil, "10.2.0.0/24"}, "[1:3]", "{10.0.0.0/24,NULL,10.2.0.0/24}"},
		{"a_mac1", "MACADDR[]", []any{"08:00:2b:01:02:03", "08:00:2b:01:02:04"}, "[1:2]", "{08:00:2b:01:02:03,08:00:2b:01:02:04}"},
		{"a_dec1", "NUMERIC(20,4)[]", []any{"1.2500", "2.5000"}, "[1:2]", "{1.2500,2.5000}"},
		{"a_dec2", "NUMERIC(20,4)[]", []any{[]any{"1.2500", "2.5000"}, []any{"3.7500", "4.0000"}}, "[1:2][1:2]", "{{1.2500,2.5000},{3.7500,4.0000}}"},
		// numeric NULL-element (1-D + 2-D): numeric was the literal Bug-74
		// silent-flatten victim, and the NULL-element shape is the one the
		// matrix otherwise omits — highest-value pin. A per-OID array-codec
		// regression on numeric would surface here as a wrong array_dims.
		{"a_decn", "NUMERIC(20,4)[]", []any{"1.2500", nil, "3.7500"}, "[1:3]", "{1.2500,NULL,3.7500}"},
		{"a_decnn", "NUMERIC(20,4)[]", []any{[]any{"1.2500", nil}, []any{nil, "4.0000"}}, "[1:2][1:2]", "{{1.2500,NULL},{NULL,4.0000}}"},

		// --- temporal element families ---
		{"a_date1", "DATE[]", []any{mustDate("2024-01-02"), mustDate("2024-03-04")}, "[1:2]", "{2024-01-02,2024-03-04}"},
		{"a_date2", "DATE[]", []any{[]any{mustDate("2024-01-02"), mustDate("2024-03-04")}, []any{mustDate("2025-05-06"), mustDate("2025-07-08")}}, "[1:2][1:2]", "{{2024-01-02,2024-03-04},{2025-05-06,2025-07-08}}"},
		{"a_daten", "DATE[]", []any{mustDate("2024-01-02"), nil, mustDate("2024-03-04")}, "[1:3]", "{2024-01-02,NULL,2024-03-04}"},
		{"a_ts1", "TIMESTAMP[]", []any{mustTS("2024-01-02 03:04:05"), mustTS("2024-06-07 08:09:10")}, "[1:2]", `{"2024-01-02 03:04:05","2024-06-07 08:09:10"}`},
		{"a_ts2", "TIMESTAMP[]", []any{[]any{mustTS("2024-01-02 03:04:05"), mustTS("2024-06-07 08:09:10")}, []any{mustTS("2025-01-02 03:04:05"), mustTS("2025-06-07 08:09:10")}}, "[1:2][1:2]", `{{"2024-01-02 03:04:05","2024-06-07 08:09:10"},{"2025-01-02 03:04:05","2025-06-07 08:09:10"}}`},
		{"a_tsn", "TIMESTAMP[]", []any{mustTS("2024-01-02 03:04:05"), nil, mustTS("2024-06-07 08:09:10")}, "[1:3]", `{"2024-01-02 03:04:05",NULL,"2024-06-07 08:09:10"}`},
		{"a_tstz1", "TIMESTAMPTZ[]", []any{time.Date(2024, 1, 2, 3, 4, 5, 0, tz)}, "[1:1]", `{"2024-01-01 21:34:05+00"}`},
		// TIME (without time zone) array — the whole `time` element family was
		// absent from the matrix; convertArray emits the time-of-day from the
		// IR canonical string. (1-D, 2-D, NULL-element.) The tz-aware sibling
		// `timetz[]` is loud-refused, pinned separately below.
		{"a_time1", "TIME[]", []any{"01:02:03", "04:05:06"}, "[1:2]", "{01:02:03,04:05:06}"},
		{"a_time2", "TIME[]", []any{[]any{"01:02:03", "04:05:06"}, []any{"07:08:09", "10:11:12"}}, "[1:2][1:2]", "{{01:02:03,04:05:06},{07:08:09,10:11:12}}"},
		{"a_timen", "TIME[]", []any{"01:02:03", nil, "07:08:09"}, "[1:3]", "{01:02:03,NULL,07:08:09}"},
	}

	// Build + apply the seed DDL (one wide table holding every column).
	var ddl string
	ddl = "CREATE TABLE m (id BIGINT PRIMARY KEY"
	for _, c := range cols {
		ddl += fmt.Sprintf(", %s %s", c.name, c.pgType)
	}
	ddl += ");"
	applyPGApplier(t, dsn, ddl)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	applier := openPipelinedApplier(t, ctx, dsn)
	defer func() { _ = applier.Close() }()

	row := ir.Row{"id": int64(1)}
	for _, c := range cols {
		row[c.name] = c.value
	}
	events := []ir.Change{
		ir.Insert{Position: ir.Position{Engine: engineNamePostgres, Token: "m1"}, Schema: "public", Table: "m", Row: row},
	}
	// batchSize 100 → the single Insert flushes via the pipelined batch.
	pumpBatchedChanges(t, ctx, applier, events, 100)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open verify db: %v", err)
	}
	defer func() { _ = db.Close() }()

	for _, c := range cols {
		var dims sql.NullString
		var text string
		q := fmt.Sprintf("SELECT array_dims(%s), %s::text FROM m WHERE id = 1", c.name, c.name)
		if err := db.QueryRowContext(ctx, q).Scan(&dims, &text); err != nil {
			t.Fatalf("verify %s: %v", c.name, err)
		}
		if dims.String != c.wantDims {
			t.Errorf("%s array_dims = %q; want %q (per-OID array-codec flatten? Bug 74 class)", c.name, dims.String, c.wantDims)
		}
		if text != c.wantText {
			t.Errorf("%s ::text = %q; want %q", c.name, text, c.wantText)
		}
	}
}

func mustDate(s string) time.Time {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		panic(err)
	}
	return t
}

func mustTS(s string) time.Time {
	t, err := time.Parse("2006-01-02 15:04:05", s)
	if err != nil {
		panic(err)
	}
	return t
}

// TestPipelined_TimetzArray_LoudRefusal pins that the loud refusal for a
// `timetz[]` (TIME WITH TIME ZONE array) column — which has no faithful
// binary array leaf, see convertArray / row_writer.go — fires THROUGH the
// pipelined dispatch (it builds the SQL via the same prepareValue codec
// path, so the refusal must surface as an ApplyBatch error, never a silent
// pass / corrupt write). This is the loud-failure tenet under pipelining:
// a refused row beats a silently flattened one.
func TestPipelined_TimetzArray_LoudRefusal(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `CREATE TABLE tz (id BIGINT PRIMARY KEY, ts TIMETZ[]);`)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	applier := openPipelinedApplier(t, ctx, dsn)
	defer func() { _ = applier.Close() }()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	bad := []ir.Change{
		ir.Insert{Position: pos2("tz1"), Schema: "public", Table: "tz", Row: ir.Row{"id": int64(1), "ts": []any{"01:02:03+05"}}},
	}
	ch := make(chan ir.Change, len(bad))
	for _, e := range bad {
		ch <- e
	}
	close(ch)
	err := applier.ApplyBatch(ctx, testStreamID, ch, 100)
	if err == nil {
		t.Fatal("ApplyBatch: expected a loud refusal for the timetz[] column through the pipelined path; got nil (silent pass = corruption)")
	}
	if !strings.Contains(err.Error(), "timetz") {
		t.Errorf("ApplyBatch error %q should name timetz (loud, row-attributable refusal)", err)
	}

	// And nothing landed — the refused batch rolled back / never wrote.
	if got := countAllRows(t, dsn, "tz"); got != 0 {
		t.Errorf("after refused timetz[] batch: rows = %d; want 0", got)
	}
}

// scalarCol is one (column, PG type, IR value, expected ::text) entry in
// the pipelined scalar-rich-families matrix. The whole array matrix is, by
// construction, arrays; this exercises the SCALAR leaf codecs through the
// pipelined batch (float specials, bytea with NUL, JSON/JSONB, non-UTC
// timestamptz, wide NUMERIC, TIME, bit/varbit) so a DescribeExec binary
// re-encode that diverged from the serial path on a scalar OID is caught.
type scalarCol struct {
	name    string
	pgType  string
	value   any
	wantTxt string // expected col::text on the real target
}

// TestPipelined_ScalarRichFamilies pins the scalar leaf codecs through the
// pipelined (DescribeExec) batch. src==dst ground-truthed via col::text on
// the real target. Float specials, a bytea carrying an embedded NUL, JSON
// and JSONB, a non-UTC-source timestamptz, a wide NUMERIC, a TIME scalar,
// and bit/varbit — the families the all-arrays matrix never touched.
func TestPipelined_ScalarRichFamilies(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	tz := time.FixedZone("UTC+5:30", int((5*time.Hour+30*time.Minute)/time.Second))

	cols := []scalarCol{
		// float specials: NaN / +Inf / -Inf / -0 must round-trip exactly.
		{"f_nan", "DOUBLE PRECISION", math.NaN(), "NaN"},
		{"f_pinf", "DOUBLE PRECISION", math.Inf(1), "Infinity"},
		{"f_ninf", "DOUBLE PRECISION", math.Inf(-1), "-Infinity"},
		{"f_nzero", "DOUBLE PRECISION", math.Copysign(0, -1), "-0"},
		// bytea with an embedded NUL byte (bytea holds arbitrary bytes; the
		// text-type NUL refusal does NOT apply here).
		{"b_nul", "BYTEA", []byte{0x00, 0x01, 0x00, 0xff}, `\x000100ff`},
		// JSON / JSONB carried as the canonical text the readers emit.
		{"j_json", "JSON", `{"a": 1, "b": [2, 3]}`, `{"a": 1, "b": [2, 3]}`},
		{"j_jsonb", "JSONB", `{"b":[2,3],"a":1}`, `{"a": 1, "b": [2, 3]}`}, // jsonb re-canonicalizes key order/space
		// timestamptz from a non-UTC source zone → stored as the UTC instant.
		{"t_tstz", "TIMESTAMPTZ", time.Date(2024, 1, 2, 3, 4, 5, 0, tz), "2024-01-01 21:34:05+00"},
		// wide NUMERIC scalar (canonical numeric string).
		{"n_wide", "NUMERIC(40,10)", "123456789012345678.1234567890", "123456789012345678.1234567890"},
		// TIME (without tz) scalar — IR canonical "HH:MM:SS" string.
		{"tm_time", "TIME", "13:14:15", "13:14:15"},
		// bit / varbit — IR canonical bit-string ('0'/'1') form.
		{"bt_fixed", "BIT(8)", "10110001", "10110001"},
		{"bt_var", "BIT VARYING(16)", "1011", "1011"},
	}

	var ddl string
	ddl = "CREATE TABLE s (id BIGINT PRIMARY KEY"
	for _, c := range cols {
		ddl += fmt.Sprintf(", %s %s", c.name, c.pgType)
	}
	ddl += ");"
	applyPGApplier(t, dsn, ddl)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	applier := openPipelinedApplier(t, ctx, dsn)
	defer func() { _ = applier.Close() }()

	row := ir.Row{"id": int64(1)}
	for _, c := range cols {
		row[c.name] = c.value
	}
	events := []ir.Change{
		ir.Insert{Position: ir.Position{Engine: engineNamePostgres, Token: "s1"}, Schema: "public", Table: "s", Row: row},
	}
	pumpBatchedChanges(t, ctx, applier, events, 100)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open verify db: %v", err)
	}
	defer func() { _ = db.Close() }()

	for _, c := range cols {
		var text string
		q := fmt.Sprintf("SELECT %s::text FROM s WHERE id = 1", c.name)
		if err := db.QueryRowContext(ctx, q).Scan(&text); err != nil {
			t.Fatalf("verify %s: %v", c.name, err)
		}
		if text != c.wantTxt {
			t.Errorf("%s ::text = %q; want %q (scalar codec diverged under pipelined DescribeExec?)", c.name, text, c.wantTxt)
		}
	}
}

// TestPipelined_EquivalenceWithSerial drives one mixed change stream
// (Insert/Update/Delete + a SchemaSnapshot + a Truncate boundary) through
// BOTH the pipelined ApplyBatch path and the serial per-change Apply path
// on two identical fresh tables, and asserts byte-identical final target
// state. This is the ADR-0092 differential oracle: pipelining must change
// only WHEN statements are sent, never the resulting state.
func TestPipelined_EquivalenceWithSerial(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	const ddl = `
		CREATE TABLE t_pipe (id BIGINT PRIMARY KEY, n INT NOT NULL, s TEXT, arr INT[]);
		CREATE TABLE t_serial (id BIGINT PRIMARY KEY, n INT NOT NULL, s TEXT, arr INT[]);
	`
	applyPGApplier(t, dsn, ddl)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	mkStream := func(table string) []ir.Change {
		return []ir.Change{
			ir.Insert{Position: pos2("a1"), Schema: "public", Table: table, Row: ir.Row{"id": int64(1), "n": int64(10), "s": "one", "arr": []any{int64(1), int64(2)}}},
			ir.Insert{Position: pos2("a2"), Schema: "public", Table: table, Row: ir.Row{"id": int64(2), "n": int64(20), "s": "two", "arr": []any{nil, int64(9)}}},
			ir.Insert{Position: pos2("a3"), Schema: "public", Table: table, Row: ir.Row{"id": int64(3), "n": int64(30), "s": nil, "arr": nil}},
			ir.Update{
				Position: pos2("a4"), Schema: "public", Table: table,
				Before: ir.Row{"id": int64(1), "n": int64(10), "s": "one", "arr": []any{int64(1), int64(2)}},
				After:  ir.Row{"id": int64(1), "n": int64(11), "s": "ONE", "arr": []any{int64(7)}},
			},
			ir.Delete{
				Position: pos2("a5"), Schema: "public", Table: table,
				Before: ir.Row{"id": int64(2), "n": int64(20), "s": "two", "arr": []any{nil, int64(9)}},
			},
			// Idempotent replay of an earlier insert (upsert no-op).
			ir.Insert{Position: pos2("a6"), Schema: "public", Table: table, Row: ir.Row{"id": int64(3), "n": int64(30), "s": nil, "arr": nil}},
		}
	}

	// Pipelined path: batchSize 100 → all 6 ride the pipelined batch.
	applierP := openPipelinedApplier(t, ctx, dsn)
	defer func() { _ = applierP.Close() }()
	pumpBatchedChanges(t, ctx, applierP, mkStream("t_pipe"), 100)

	// Serial path: batchSize 1 → per-change Apply (serial *sql.Tx exec).
	applierS := openPipelinedApplier(t, ctx, dsn)
	defer func() { _ = applierS.Close() }()
	pumpBatchedChanges(t, ctx, applierS, mkStream("t_serial"), 1)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open verify db: %v", err)
	}
	defer func() { _ = db.Close() }()

	var diff int
	const cmp = `
		SELECT COUNT(*) FROM (
			SELECT id, n, s, arr::text FROM t_pipe
			EXCEPT
			SELECT id, n, s, arr::text FROM t_serial
		) d`
	if err := db.QueryRowContext(ctx, cmp).Scan(&diff); err != nil {
		t.Fatalf("compare: %v", err)
	}
	if diff != 0 {
		t.Errorf("pipelined target differs from serial target in %d row(s) — pipelining changed resulting state", diff)
	}
	// Symmetric difference (catch rows present in serial but absent in pipe).
	const cmp2 = `
		SELECT COUNT(*) FROM (
			SELECT id, n, s, arr::text FROM t_serial
			EXCEPT
			SELECT id, n, s, arr::text FROM t_pipe
		) d`
	if err := db.QueryRowContext(ctx, cmp2).Scan(&diff); err != nil {
		t.Fatalf("compare2: %v", err)
	}
	if diff != 0 {
		t.Errorf("serial target has %d row(s) absent from pipelined target", diff)
	}
}

func pos2(tok string) ir.Position {
	return ir.Position{Engine: engineNamePostgres, Token: tok}
}

// TestPipelined_AtomicityMidBatchError pins that an exec error inside the
// pipelined batch rolls back BOTH the data and the position (the ADR-0007
// contract under pipelining), and that replaying the stream from the prior
// boundary reproduces the batch idempotently.
//
// Mechanism: row id=2 carries a NOT-NULL violation (n = NULL) that fails
// at SendBatch result-read time (commit), after id=1 was queued in the
// same batch. The whole batch must roll back: zero rows, no advanced
// position. A second apply with the offending row corrected lands all
// rows idempotently.
func TestPipelined_AtomicityMidBatchError(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `CREATE TABLE atom (id BIGINT PRIMARY KEY, n INT NOT NULL);`)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	applier := openPipelinedApplier(t, ctx, dsn)
	defer func() { _ = applier.Close() }()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	bad := []ir.Change{
		ir.Insert{Position: pos2("x1"), Schema: "public", Table: "atom", Row: ir.Row{"id": int64(1), "n": int64(100)}},
		// n = nil violates NOT NULL → fails at the pipelined flush.
		ir.Insert{Position: pos2("x2"), Schema: "public", Table: "atom", Row: ir.Row{"id": int64(2), "n": nil}},
	}
	ch := make(chan ir.Change, len(bad))
	for _, e := range bad {
		ch <- e
	}
	close(ch)
	err := applier.ApplyBatch(ctx, testStreamID, ch, 100)
	if err == nil {
		t.Fatal("ApplyBatch: expected a NOT NULL violation error from the pipelined flush; got nil")
	}

	// Atomicity: the batch rolled back — neither id=1 nor id=2 landed.
	if got := countAllRows(t, dsn, "atom"); got != 0 {
		t.Errorf("after rolled-back pipelined batch: rows = %d; want 0 (data must roll back with the failed flush)", got)
	}
	// Position did not advance (rolled back with the data).
	if _, found, perr := applier.ReadPosition(ctx, testStreamID); perr != nil {
		t.Fatalf("ReadPosition: %v", perr)
	} else if found {
		t.Error("ReadPosition: position advanced after a rolled-back pipelined batch")
	}

	// Replay from the prior boundary with the offending row corrected:
	// every row lands, idempotently.
	good := []ir.Change{
		ir.Insert{Position: pos2("x1"), Schema: "public", Table: "atom", Row: ir.Row{"id": int64(1), "n": int64(100)}},
		ir.Insert{Position: pos2("x2"), Schema: "public", Table: "atom", Row: ir.Row{"id": int64(2), "n": int64(200)}},
	}
	pumpBatchedChanges(t, ctx, applier, good, 100)
	if got := countAllRows(t, dsn, "atom"); got != 2 {
		t.Errorf("after corrected replay: rows = %d; want 2", got)
	}
	// Idempotent re-replay.
	pumpBatchedChanges(t, ctx, applier, good, 100)
	if got := countAllRows(t, dsn, "atom"); got != 2 {
		t.Errorf("after idempotent re-replay: rows = %d; want 2", got)
	}
}

// TestPipelined_GAP3_AlterTypeWiden pins the ADR-0091 GAP #3 interaction
// (ADR-0092 §"GAP #3 is subsumed by the live re-describe"): a forwarded
// ALTER TYPE int4→bigint, then an INSERT carrying a value out of the OLD
// int4 range, must apply through the pipelined batch. The DescribeExec pool
// re-describes every distinct statement fresh within the SendBatch flush
// (caching nothing), so the widened column's parameter OID is taken from the
// live catalog — never bound against a stale cached int4 OID. Pre-fix (or
// under the default CacheStatement mode) this would fail to encode the
// out-of-old-range value.
//
// The forwarded boundary is delivered as a SchemaSnapshot (which the
// applier persists + uses to invalidate its per-table caches), then the
// out-of-range INSERT is applied in a later pipelined batch.
func TestPipelined_GAP3_AlterTypeWiden(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	// Target starts with an int4 id-like column, then is widened (the DDL
	// is applied out-of-band, mirroring the forwarded ALTER having already
	// been applied on the target before the SchemaSnapshot boundary's
	// post-commit cache invalidation fires).
	applyPGApplier(t, dsn, `CREATE TABLE widen (id BIGINT PRIMARY KEY, v INTEGER NOT NULL);`)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	applier := openPipelinedApplier(t, ctx, dsn)
	defer func() { _ = applier.Close() }()

	// First batch: an INSERT in the int4 range warms the per-table caches.
	warm := []ir.Change{
		ir.Insert{Position: pos2("w1"), Schema: "public", Table: "widen", Row: ir.Row{"id": int64(1), "v": int64(7)}},
	}
	pumpBatchedChanges(t, ctx, applier, warm, 100)

	// Widen v on the target (the forwarded ALTER, applied out-of-band).
	applyPGApplier(t, dsn, `ALTER TABLE widen ALTER COLUMN v TYPE BIGINT;`)

	// The SchemaSnapshot boundary carries the post-DDL IR; the applier
	// persists it and invalidates the per-table caches. Its column type
	// for v is now a 64-bit integer (the live catalog shape).
	widened := &ir.Table{
		Schema: "public",
		Name:   "widen",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "v", Type: ir.Integer{Width: 64}},
		},
	}
	snap := []ir.Change{
		ir.SchemaSnapshot{Position: pos2("snap1"), Schema: "public", Table: "widen", IR: widened},
	}
	pumpBatchedChanges(t, ctx, applier, snap, 100)

	// Now an INSERT with a value OUT of the old int4 range (> 2^31-1),
	// applied through the pipelined batch. Must succeed.
	const bigVal = int64(5_000_000_000)
	hot := []ir.Change{
		ir.Insert{Position: pos2("h1"), Schema: "public", Table: "widen", Row: ir.Row{"id": int64(2), "v": bigVal}},
	}
	pumpBatchedChanges(t, ctx, applier, hot, 100)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open verify db: %v", err)
	}
	defer func() { _ = db.Close() }()

	var got int64
	if err := db.QueryRowContext(ctx, "SELECT v FROM widen WHERE id = 2").Scan(&got); err != nil {
		t.Fatalf("verify widened insert: %v (stale-OID encode failure would surface as an apply error above)", err)
	}
	if got != bigVal {
		t.Errorf("v = %d; want %d (out-of-old-int4-range value through pipelined batch)", got, bigVal)
	}
}
