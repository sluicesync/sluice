//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Shared machinery for the adversarial value-fidelity end-to-end corpus
// (the holistic complement to the per-codec family matrices; see the
// "Pin the class, not the representative" section of CLAUDE.md).
//
// The corpus is a single source table per direction whose columns carry
// the WORST-CASE value of every IR type family — the values that have
// historically hidden silent loss (multi-byte text, max-precision
// decimals, -0.0 and 17-significant-digit doubles, signed extremes,
// fractional-second and DST-instant temporals, >2^53 JSON integers,
// NUL-bearing binaries, multi-dim arrays, …). Every cell is
// ground-truthed against the REAL target by direct SQL (PG
// float8send/encode/::text, MySQL CONCAT/HEX/MD5), never through
// sluice's own reader — the independent expected value for each cell is
// the hand-written `want` text (or the source literal re-parsed by Go
// for float-bit comparison), so a silently altered value cannot ratify
// itself.
//
// Consumers:
//
//   - migrate_adversarial_corpus_m2p_integration_test.go (MySQL → PG:
//     migrate + verify + refusal cells + backup→restore round)
//   - migrate_adversarial_corpus_p2m_integration_test.go (PG → MySQL:
//     same shape)
//   - streamer_adversarial_corpus_integration_test.go (both CDC lanes:
//     the binlog / pgoutput decoders are DIFFERENT codecs from the
//     bulk-copy readers — Bug-74 territory)
//
// Out of scope here, with reasons (the sibling enumeration):
//
//   - Geometry: a plain PG target refuses GEOMETRY without PostGIS at
//     DDL emit, so geometry cells live in the postgis-tagged suites
//     (migrate_postgis_integration_test.go and the NaN/Inf-coordinate
//     refusal pins behind SLUICE-E-VALUE-UNREPRESENTABLE, v0.122.0).
//   - Ragged arrays: a real PG source cannot produce one (PG arrays are
//     rectangular by definition); the reachable producer is the
//     pgtrigger to_jsonb() path, pinned by the
//     SLUICE-E-VALUE-RAGGED-ARRAY unit tests in the postgres engine.
//   - Vitess/PlanetScale wire variants: same corpus values, different
//     carrier — the vitess cluster suites own that lane.

package pipeline

import (
	"context"
	"crypto/md5"
	"database/sql"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"
)

// advCmp selects how a cell's probed target text is compared to want.
type advCmp int

const (
	// advCmpText is byte-exact string equality (the default).
	advCmpText advCmp = iota
	// advCmpFloat64 parses got and want with strconv.ParseFloat(…, 64)
	// and compares math.Float64bits — exact IEEE-754 identity through
	// each side's shortest-round-trip rendering (MySQL 8 CONCAT() and
	// Go both render shortest forms; exponent spelling differs, bit
	// comparison does not).
	advCmpFloat64
	// advCmpFloat32 is the single-precision variant.
	advCmpFloat32
)

// advCell is one adversarial corpus cell: a source column carrying a
// worst-case value, and the independent target-side ground truth.
type advCell struct {
	family string // type family for the anti-vacuity floor
	col    string // column name (also the subtest name suffix)
	ddl    string // source column type DDL (e.g. "DECIMAL(65,30) NOT NULL")
	lit    string // source SQL literal for the seed INSERT ("NULL" + seedVal for driver-param seeding)

	// probe is the TARGET-side SQL expression producing one text value;
	// every %s is replaced by the column name. It runs via direct SQL
	// against the real target — never through sluice's reader.
	probe string
	// want is the independent expected value: hand-written (advCmpText)
	// or the source literal re-parsed by Go (advCmpFloat*).
	want string
	cmp  advCmp

	// seedVal, when non-nil, is written by a parameterized UPDATE after
	// the seed INSERT (multi-MB blobs / long text that would bloat an
	// inline literal). lit should be "NULL" for such cells.
	seedVal any

	// cdcSkip, when non-empty, names why the CDC round excludes the
	// cell. Empty means the cell rides every round.
	cdcSkip string
}

// advBuildCreate renders the source CREATE TABLE for a corpus. The id
// column is the PK so rows are addressable across rounds.
func advBuildCreate(table string, cells []advCell, mysqlSource bool) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE %s (\n\tid BIGINT NOT NULL,\n", table)
	for _, c := range cells {
		fmt.Fprintf(&b, "\t%s %s,\n", c.col, c.ddl)
	}
	b.WriteString("\tPRIMARY KEY (id)\n)")
	if mysqlSource {
		b.WriteString(" ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
	}
	b.WriteString(";")
	return b.String()
}

// advBuildInsert renders the seed INSERT for one row of the corpus.
func advBuildInsert(table string, cells []advCell, id int) string {
	cols := make([]string, 0, len(cells)+1)
	vals := make([]string, 0, len(cells)+1)
	cols = append(cols, "id")
	vals = append(vals, strconv.Itoa(id))
	for _, c := range cells {
		cols = append(cols, c.col)
		vals = append(vals, c.lit)
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",
		table, strings.Join(cols, ", "), strings.Join(vals, ", "))
}

// advSeedParams applies the seedVal cells for one row via parameterized
// UPDATEs (large values that shouldn't ride an inline literal).
// placeholder is "?" (mysql) or "$1" (pgx).
func advSeedParams(t *testing.T, db *sql.DB, table string, cells []advCell, id int, pgPlaceholders bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for _, c := range cells {
		if c.seedVal == nil {
			continue
		}
		var q string
		if pgPlaceholders {
			q = fmt.Sprintf("UPDATE %s SET %s = $1 WHERE id = $2", table, c.col)
		} else {
			q = fmt.Sprintf("UPDATE %s SET %s = ? WHERE id = ?", table, c.col)
		}
		if _, err := db.ExecContext(ctx, q, c.seedVal, id); err != nil {
			t.Fatalf("seed %s.%s via param: %v", table, c.col, err)
		}
	}
}

// advProbeCells ground-truths every cell of a corpus row against the
// real target through the supplied *sql.Conn (a Conn, not a DB, so the
// caller's session setup — time_zone — holds for every probe). One
// subtest per cell; a mismatch names the family, the probe, and both
// values so a silent alteration reads as the headline it is.
func advProbeCells(t *testing.T, conn *sql.Conn, table string, cells []advCell, id int, pgPlaceholders bool) {
	t.Helper()
	for _, c := range cells {
		t.Run(c.family+"/"+c.col, func(t *testing.T) {
			expr := strings.ReplaceAll(c.probe, "%s", c.col)
			var q string
			if pgPlaceholders {
				q = fmt.Sprintf("SELECT %s FROM %s WHERE id = $1", expr, table)
			} else {
				q = fmt.Sprintf("SELECT %s FROM %s WHERE id = ?", expr, table)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			var got sql.NullString
			if err := conn.QueryRowContext(ctx, q, id).Scan(&got); err != nil {
				t.Fatalf("probe %s: %v\nquery: %s", c.col, err, q)
			}
			if !got.Valid {
				t.Fatalf("probe %s returned SQL NULL; want %q (value silently dropped?)", c.col, c.want)
			}
			if !advCompare(c.cmp, got.String, c.want) {
				t.Errorf("SILENT VALUE ALTERATION on %s (family %s):\n  target holds: %q\n  want:         %q\n  probe: %s",
					c.col, c.family, got.String, c.want, expr)
			}
		})
	}
}

// advCompare applies a cell's comparison mode.
func advCompare(mode advCmp, got, want string) bool {
	switch mode {
	case advCmpFloat64:
		g, gerr := strconv.ParseFloat(got, 64)
		w, werr := strconv.ParseFloat(want, 64)
		return gerr == nil && werr == nil && math.Float64bits(g) == math.Float64bits(w)
	case advCmpFloat32:
		g, gerr := strconv.ParseFloat(got, 32)
		w, werr := strconv.ParseFloat(want, 32)
		return gerr == nil && werr == nil && math.Float32bits(float32(g)) == math.Float32bits(float32(w))
	case advCmpText:
		return got == want
	default:
		return got == want
	}
}

// advAssertCorpusFloor is the anti-vacuity floor: a shrunken corpus
// (families or cells quietly dropped) fails LOUDLY here before any
// probing happens, so a green run always attests to the full breadth.
func advAssertCorpusFloor(t *testing.T, name string, cells []advCell, minFamilies, minCells int) {
	t.Helper()
	families := map[string]int{}
	for _, c := range cells {
		families[c.family]++
	}
	if len(families) < minFamilies || len(cells) < minCells {
		t.Fatalf("anti-vacuity floor: corpus %s has %d families / %d cells; floor is %d families / %d cells — "+
			"the corpus shrank, which would silently narrow every round-trip below (families: %v)",
			name, len(families), len(cells), minFamilies, minCells, families)
	}
}

// advFloat64Bits renders the IEEE-754 bit pattern of a decimal literal
// as the 16-hex-digit text PG's encode(float8send(col),'hex') produces.
// Go's parser is the independent authority: both MySQL and PG parse a
// decimal float literal to the nearest double (IEEE round-to-nearest),
// so Go's ParseFloat of the same literal is the expected bit pattern.
func advFloat64Bits(lit string) string {
	f, err := strconv.ParseFloat(lit, 64)
	if err != nil {
		panic("advFloat64Bits: bad literal " + lit + ": " + err.Error())
	}
	return fmt.Sprintf("%016x", math.Float64bits(f))
}

// advFloat32Bits is the float4send variant (8 hex digits).
func advFloat32Bits(lit string) string {
	f, err := strconv.ParseFloat(lit, 32)
	if err != nil {
		panic("advFloat32Bits: bad literal " + lit + ": " + err.Error())
	}
	return fmt.Sprintf("%08x", math.Float32bits(float32(f)))
}

// advMD5 is the hex MD5 of a byte string — the Go-side independent
// value matched against the target server's own md5()/MD5().
func advMD5(b []byte) string {
	sum := md5.Sum(b)
	return fmt.Sprintf("%x", sum)
}

// advBlobPattern builds a deterministic n-byte adversarial blob: a
// linear-congruential byte walk seeded to include 0x00 runs, 0xFF runs,
// and every byte value, so charset transcoding, NUL truncation, or hex
// case-folding anywhere in the pipe changes the MD5.
func advBlobPattern(n int) []byte {
	out := make([]byte, n)
	state := uint32(0x2545F491)
	for i := range out {
		switch {
		case i%1024 < 16:
			out[i] = 0x00 // embedded NUL runs
		case i%1024 >= 1008:
			out[i] = 0xFF // all-ones runs
		default:
			state = state*1664525 + 1013904223
			out[i] = byte(state >> 24)
		}
	}
	return out
}

// advDeepJSON renders {"a":{"a":…{"a":1}…}} nested depth levels deep,
// plus the matching PG #>> path and MySQL JSON_EXTRACT path.
func advDeepJSON(depth int) (doc, pgPath, mysqlPath string) {
	doc = strings.Repeat(`{"a":`, depth) + "1" + strings.Repeat("}", depth)
	parts := make([]string, depth)
	for i := range parts {
		parts[i] = "a"
	}
	pgPath = "{" + strings.Join(parts, ",") + "}"
	mysqlPath = "$." + strings.Join(parts, ".")
	return doc, pgPath, mysqlPath
}

// advOpenConn opens a dedicated *sql.Conn on driver/dsn and runs the
// supplied session-setup statements on it, so probes see a stable
// session (time_zone etc.). Cleanup is registered on t.
func advOpenConn(t *testing.T, driver, dsn string, setup ...string) *sql.Conn {
	t.Helper()
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatalf("open %s: %v", driver, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("conn %s: %v", driver, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	for _, s := range setup {
		if _, err := conn.ExecContext(ctx, s); err != nil {
			t.Fatalf("session setup %q: %v", s, err)
		}
	}
	return conn
}

// advRunVerify runs sluice's own Verifier (count + sample) after a
// migrate and requires a clean report. This is deliberately IN ADDITION
// to the direct-SQL probes: verify rides sluice's readers, so it can
// confirm-but-not-establish; the probes are the independent evidence.
func advRunVerify(t *testing.T, v *Verifier) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	// Depth count only: cross-engine sample verify is refused loudly by
	// design ("depth=sample requires same source and target engine"),
	// and every corpus run here is cross-engine. The independent
	// per-cell evidence lives in the direct-SQL probes, not here.
	v.Depth = VerifyDepthCount
	v.Out = io.Discard
	res, err := v.Run(ctx)
	if err != nil {
		t.Fatalf("verify --depth count: %v", err)
	}
	if res.HasMismatch() || res.HasUnverified() {
		t.Errorf("verify --depth count reports mismatch/unverified: %+v", res.Summary)
	}
}

// advWaitRowVisible polls the target until the corpus row with the
// given id is present (CDC rounds). Returns false on timeout.
func advWaitRowVisible(t *testing.T, driver, dsn, table string, id int, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var q string
	if driver == "pgx" {
		q = fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE id = $1", table)
	} else {
		q = fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE id = ?", table)
	}
	for time.Now().Before(deadline) {
		db, err := sql.Open(driver, dsn)
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			var n int
			qerr := db.QueryRowContext(ctx, q, id).Scan(&n)
			cancel()
			_ = db.Close()
			if qerr == nil && n == 1 {
				return true
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	return false
}

// advWaitCellEquals polls one cell's probe on the target until it
// matches want per the cell's compare mode (CDC UPDATE settling).
func advWaitCellEquals(t *testing.T, driver, dsn, table string, cell advCell, id int, setup []string, timeout time.Duration) bool {
	t.Helper()
	expr := strings.ReplaceAll(cell.probe, "%s", cell.col)
	var q string
	if driver == "pgx" {
		q = fmt.Sprintf("SELECT %s FROM %s WHERE id = $1", expr, table)
	} else {
		q = fmt.Sprintf("SELECT %s FROM %s WHERE id = ?", expr, table)
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		db, err := sql.Open(driver, dsn)
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			ok := func() bool {
				conn, cerr := db.Conn(ctx)
				if cerr != nil {
					return false
				}
				defer func() { _ = conn.Close() }()
				for _, s := range setup {
					if _, serr := conn.ExecContext(ctx, s); serr != nil {
						return false
					}
				}
				var got sql.NullString
				if qerr := conn.QueryRowContext(ctx, q, id).Scan(&got); qerr != nil || !got.Valid {
					return false
				}
				return advCompare(cell.cmp, got.String, cell.want)
			}()
			cancel()
			_ = db.Close()
			if ok {
				return true
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	return false
}
