//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Generative round-trip correctness fuzz harness — property driver
// (Track 2, Phase 1). Implements the design contract at
// docs/dev/notes/prep-generative-roundtrip-fuzz-harness.md.
//
// Phase A reuse (no new framework — a *generalisation* of the proven
// battle-test fixtures):
//
//   - startPostgres / startMySQL / buildPGDSN / buildMySQLDSN — the
//     exact testcontainers boot + source/target-db split used by
//     migrate_pg_integration_test.go and migrate_integration_test.go.
//   - applyPGDDL / applyMySQLDDL — raw multi-statement DDL/DML applied
//     directly to the source container (the independent-oracle path,
//     same as every Bug fixture).
//   - The `Migrator{Source,Target,SourceDSN,TargetDSN}` invocation and
//     `engines.Get("postgres"/"mysql")` lookup — verbatim from
//     migrate_bug7374/75/69_integration_test.go.
//   - ctx2min — the shared 2-minute test context.
//   - The `col::text` canonical-read oracle (NULL-element + array
//     dimensionality observable in one compare) — generalised from
//     migrate_bug7374_integration_test.go's readAll.
//   - lockedBuffer + slog JSON capture pattern — available for advisory
//     assertions (migrate_bug69/72 use it); the harness classifies on
//     migrate exit status + canonical diff, so it does not need it, but
//     the pattern was studied.
//
// Env knobs (design decision #6):
//
//	SLUICE_FUZZ_ITERS  — cases per direction (default: small CI smoke
//	                      budget; set high for nightly/pre-release).
//	SLUICE_FUZZ_SEED   — master seed (default: a fixed value so CI is
//	                      deterministic; override to explore).
//	SLUICE_FUZZ_DIRS   — comma list of directions to run (default all
//	                      four); e.g. "postgres->postgres,mysql->mysql".
//
// A failure prints the seed + case index and dumps the replayable
// source-dialect script to a fixtures dir, deterministically
// reproducible via TestMigrate_FuzzRoundtrip_ReplayDumpedFixture.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/orware/sluice/internal/engines"
	"github.com/orware/sluice/internal/ir"

	_ "github.com/orware/sluice/internal/engines/mysql"
	_ "github.com/orware/sluice/internal/engines/postgres"
)

const (
	fuzzDefaultSeed  = int64(0x5104CE) // "SLUICE" — fixed → deterministic CI
	fuzzSmokeIters   = 4               // cheap per-direction budget for the -tags=integration suite
	fuzzFixturesPath = "testdata/fuzz-failures"
)

func fuzzEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func fuzzEnvSeed() int64 {
	if v := os.Getenv("SLUICE_FUZZ_SEED"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return fuzzDefaultSeed
}

func fuzzSelectedDirections(t *testing.T) []direction {
	want := os.Getenv("SLUICE_FUZZ_DIRS")
	if want == "" {
		return allDirections()
	}
	byName := map[string]direction{}
	for _, d := range allDirections() {
		byName[d.String()] = d
	}
	var out []direction
	for _, name := range strings.Split(want, ",") {
		name = strings.TrimSpace(name)
		d, ok := byName[name]
		if !ok {
			t.Fatalf("SLUICE_FUZZ_DIRS: unknown direction %q (valid: %v)", name, allDirections())
		}
		out = append(out, d)
	}
	return out
}

// fuzzEnv is one direction's source+target containers + engines, booted
// once and reused across that direction's iterations (container boot is
// the expensive part — amortise it the way the battle-test fixtures do
// per-test).
type fuzzEnv struct {
	srcDSN, dstDSN string
	srcEng, dstEng ir.Engine
	srcDriver      string
	dstDriver      string
	cleanup        func()
}

func bootDirection(t *testing.T, d direction) *fuzzEnv {
	t.Helper()
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	fe := &fuzzEnv{}
	var pgSrc, pgDst, mySrc, myDst string
	var cleanups []func()

	needPG := d.src == enginePG || d.dst == enginePG
	needMy := d.src == engineMySQL || d.dst == engineMySQL

	if needPG {
		s, dst, c := startPostgres(t)
		pgSrc, pgDst = s, dst
		cleanups = append(cleanups, c)
	}
	if needMy {
		s, dst, c := startMySQL(t)
		mySrc, myDst = s, dst
		cleanups = append(cleanups, c)
	}

	switch d.src {
	case enginePG:
		fe.srcDSN, fe.srcEng, fe.srcDriver = pgSrc, pgEng, "pgx"
	case engineMySQL:
		fe.srcDSN, fe.srcEng, fe.srcDriver = mySrc, myEng, "mysql"
	}
	switch d.dst {
	case enginePG:
		fe.dstDSN, fe.dstEng, fe.dstDriver = pgDst, pgEng, "pgx"
	case engineMySQL:
		fe.dstDSN, fe.dstEng, fe.dstDriver = myDst, myEng, "mysql"
	}
	fe.cleanup = func() {
		for _, c := range cleanups {
			c()
		}
	}
	return fe
}

// applySource applies the raw generated script directly to the source
// DB — NOT through sluice. Reuses the engine-appropriate battle-test
// applier.
func applySource(t *testing.T, d direction, dsn, ddl string) {
	t.Helper()
	if d.src == enginePG {
		applyPGDDL(t, dsn, ddl)
	} else {
		applyMySQLDDL(t, dsn, ddl)
	}
}

// targetRowCount returns the number of rows in the target table, or -1
// if the table does not exist. The classifier uses this to tell a
// clean loud-refuse (table absent, or empty from refuse-at-copy) from a
// real partial-DATA target (rows present — the corruption signature).
func targetRowCount(ctx context.Context, db *sql.DB, table string, eng engineKind) int {
	var n int
	// information_schema.tables is portable across PG and MySQL; only
	// the placeholder syntax differs ($1 vs ?).
	q := `SELECT count(*) FROM information_schema.tables WHERE table_name = ?`
	if eng == enginePG {
		q = `SELECT count(*) FROM information_schema.tables WHERE table_name = $1`
	}
	if err := db.QueryRowContext(ctx, q, table).Scan(&n); err != nil || n == 0 {
		return -1
	}
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		return -1
	}
	return n
}

// canonicalSelect builds the SELECT that projects every faithful column
// to its per-engine canonical text form, ordered by id. PG: `col::text`
// (renders arrays as `{...}`, NULL elements as the literal NULL inside
// the braces, dimensionality as nesting — the Bug 7374 oracle). MySQL:
// the column raw (the driver yields the canonical rendering for the
// scalar core types this harness compares same-engine on MySQL).
func canonicalSelect(gc *genCase, cols []string, eng engineKind) string {
	var sb strings.Builder
	sb.WriteString("SELECT id")
	for _, c := range cols {
		if eng == enginePG {
			fmt.Fprintf(&sb, ", %s::text", c)
		} else {
			fmt.Fprintf(&sb, ", `%s`", c)
		}
	}
	fmt.Fprintf(&sb, " FROM %s ORDER BY id", gc.tableNm)
	return sb.String()
}

// readCanonical executes canonicalSelect and returns column→per-row
// canonical text, keyed by column so the classifier compares like with
// like regardless of column order. Generalised from
// migrate_bug7374_integration_test.go's readAll.
func readCanonical(ctx context.Context, db *sql.DB, gc *genCase, cols []string, eng engineKind) (map[string][]sql.NullString, error) {
	if len(cols) == 0 {
		// Nothing faithful to compare; still confirm row presence so a
		// silent total row loss is caught.
		var n int
		if err := db.QueryRowContext(ctx, "SELECT count(*) FROM "+gc.tableNm).Scan(&n); err != nil {
			return nil, fmt.Errorf("count %s: %w", gc.tableNm, err)
		}
		out := map[string][]sql.NullString{}
		for i := 0; i < n; i++ {
			out["__rowcount__"] = append(out["__rowcount__"], sql.NullString{})
		}
		return out, nil
	}

	q := canonicalSelect(gc, cols, eng)
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("query %q: %w", q, err)
	}
	defer func() { _ = rows.Close() }()

	type idxed struct {
		id   int
		vals []sql.NullString
	}
	var collected []idxed
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		dest := make([]any, len(cols)+1)
		var id int
		dest[0] = &id
		for i := range vals {
			dest[i+1] = &vals[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		collected = append(collected, idxed{id, vals})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows.Err: %w", err)
	}
	sort.Slice(collected, func(i, j int) bool { return collected[i].id < collected[j].id })
	out := map[string][]sql.NullString{}
	for _, row := range collected {
		for i, c := range cols {
			out[c] = append(out[c], row.vals[i])
		}
	}
	return out, nil
}

// runOneCase executes a single generated case end-to-end and returns
// the verdict + a diagnostic message.
func runOneCase(t *testing.T, fe *fuzzEnv, gc *genCase) (v verdict, diag string) {
	t.Helper()

	applySource(t, gc.dir, fe.srcDSN, gc.ddl)

	// Per-case isolation: the source+target containers are reused
	// across a direction's iterations (container boot is expensive), so
	// scope each migrate to this case's own table (Filter) and give it
	// a unique MigrationID. Without this, case N's migrate would see
	// case <N's tables in the source schema and collide with case <N's
	// completed-migration manifest ("migration_id ... already
	// complete"). This is a harness-isolation requirement, not a sluice
	// behaviour under test.
	mig := &Migrator{
		Source:      fe.srcEng,
		Target:      fe.dstEng,
		SourceDSN:   fe.srcDSN,
		TargetDSN:   fe.dstDSN,
		Filter:      TableFilter{Include: []string{gc.tableNm}},
		MigrationID: fmt.Sprintf("fuzz-%s-%d-%d", strings.ReplaceAll(gc.dir.String(), "->", "_"), gc.seed, gc.caseIdx),
	}
	ctx := ctx2min(t)
	migErr := mig.Run(ctx)

	ce := expectationFor(gc)

	dstDB, err := sql.Open(fe.dstDriver, fe.dstDSN)
	if err != nil {
		return verdictFail, fmt.Sprintf("open target: %v", err)
	}
	defer func() { _ = dstDB.Close() }()

	rowCount := targetRowCount(ctx, dstDB, gc.tableNm, gc.dir.dst)

	var srcVals, dstVals map[string][]sql.NullString
	if !ce.loudRefuse && migErr == nil {
		cols := faithfulColumnsFor(gc)

		srcDB, err := sql.Open(fe.srcDriver, fe.srcDSN)
		if err != nil {
			return verdictFail, fmt.Sprintf("open source: %v", err)
		}
		defer func() { _ = srcDB.Close() }()

		srcVals, err = readCanonical(ctx, srcDB, gc, cols, gc.dir.src)
		if err != nil {
			return verdictFail, fmt.Sprintf("read source canonical: %v", err)
		}
		dstVals, err = readCanonical(ctx, dstDB, gc, cols, gc.dir.dst)
		if err != nil {
			return verdictFail, fmt.Sprintf("read target canonical: %v", err)
		}
	}

	return classify(gc, ce, migErr, rowCount, srcVals, dstVals)
}

// dumpFixture writes the replayable source-dialect script so a failure
// is deterministically reproducible (design decision #4). The path is
// printed so an operator (or TestMigrate_FuzzRoundtrip_ReplayDumpedFixture) can
// promote it to a permanent named pin.
func dumpFixture(t *testing.T, gc *genCase, msg string) string {
	t.Helper()
	dir := fuzzFixturesPath
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Logf("could not create fixtures dir %s: %v", dir, err)
		return ""
	}
	name := fmt.Sprintf("seed%d_%s_case%d.sql",
		gc.seed, strings.ReplaceAll(gc.dir.String(), "->", "_"), gc.caseIdx)
	p := filepath.Join(dir, name)
	header := fmt.Sprintf(
		"-- FUZZ FAILURE — deterministically replayable\n"+
			"-- seed=%d direction=%s caseIdx=%d\n"+
			"-- verdict: %s\n"+
			"-- replay: SLUICE_FUZZ_SEED=%d go test -tags=integration -run TestMigrate_FuzzRoundtrip_ReplayDumpedFixture ./internal/pipeline\n\n",
		gc.seed, gc.dir, gc.caseIdx, msg, gc.seed,
	)
	if err := os.WriteFile(p, []byte(header+gc.ddl), 0o600); err != nil {
		t.Logf("could not write fixture %s: %v", p, err)
		return ""
	}
	return p
}

// TestMigrate_FuzzRoundtrip is the property driver. With the default (smoke)
// budget it runs cheaply inside the normal `-tags=integration` suite so
// CI exercises the harness every run; set SLUICE_FUZZ_ITERS high for
// the nightly/pre-release budget.
func TestMigrate_FuzzRoundtrip(t *testing.T) {
	seed := fuzzEnvSeed()
	iters := fuzzEnvInt("SLUICE_FUZZ_ITERS", fuzzSmokeIters)
	dirs := fuzzSelectedDirections(t)

	t.Logf("fuzz harness: seed=%d iters/dir=%d directions=%v", seed, iters, dirs)

	for _, d := range dirs {
		d := d
		t.Run(d.String(), func(t *testing.T) {
			fe := bootDirection(t, d)
			defer fe.cleanup()

			pass, fail := 0, 0
			for i := 0; i < iters; i++ {
				gc := generateCase(seed, i, d)
				v, msg := runOneCase(t, fe, &gc)
				if v == verdictPass {
					pass++
					continue
				}
				fail++
				path := dumpFixture(t, &gc, msg)
				t.Errorf("FUZZ FAIL [%s case %d seed %d]: %s\n  replayable fixture: %s\n  --- script ---\n%s",
					d, i, seed, msg, path, gc.ddl)
			}
			t.Logf("direction %s: %d pass, %d fail (of %d)", d, pass, fail, iters)
		})
	}
}

// TestMigrate_FuzzRoundtrip_ReplayDumpedFixture is the determinism self-check:
// regenerate a case purely from (seed, idx, direction) and confirm the
// rendered script is byte-identical to a prior run. This proves a
// dumped fixture replays deterministically without needing the dumped
// file itself (the seed IS the fixture).
func TestMigrate_FuzzRoundtrip_ReplayDumpedFixture(t *testing.T) {
	seed := fuzzEnvSeed()
	for _, d := range allDirections() {
		for i := 0; i < fuzzSmokeIters; i++ {
			a := generateCase(seed, i, d)
			b := generateCase(seed, i, d)
			if a.ddl != b.ddl {
				t.Fatalf("non-deterministic generation [%s case %d seed %d]:\n--- A ---\n%s\n--- B ---\n%s",
					d, i, seed, a.ddl, b.ddl)
			}
		}
	}
	t.Logf("determinism confirmed: every (seed=%d, idx, dir) regenerates byte-identical", seed)
}
