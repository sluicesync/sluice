//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Random-op sync-convergence property — the live half (docs/testing.md
// Layer 4, repo-audit task M3.12). The pure core (op generator, model,
// script renderer) lives in converge_gen.go; this file binds it to
// real databases: a rapid-driven property boots one source+target pair
// per direction, and each check creates a fresh table, starts a real
// Streamer over it, applies a random transaction sequence to the
// source, and asserts the target converges to EXACTLY the source's
// final ordered content. On failure rapid SHRINKS to a minimal failing
// op sequence and the harness dumps it as a replayable script
// (the migrate fuzz harness's dumped-fixture pattern).
//
// SHARD ROUTING: the TestSyncConverges_ prefix is deliberate — the CI
// integration matrix splits the pipeline package by test name, and
// anything matching neither ^TestMigrate_ nor ^TestStreamer_ rides the
// pipeline-rest-other shard. Keep the prefix for new directions.
//
// Budgets (the fuzz-harness two-budget pattern):
//
//	default            — the PR-CI smoke budget: convSmokeChecks
//	                     checks per direction, convSmokeMaxTxs txs per
//	                     sequence, deterministic seed.
//	SLUICE_CONVERGE_ITERS    — checks per direction (deep runs).
//	SLUICE_CONVERGE_OPS      — max transactions per sequence. A replay
//	                           must use the same value: the budget
//	                           shapes the draw sequence.
//	SLUICE_CONVERGE_SEED     — nonzero base seed (deterministic CI
//	                           default convDefaultSeed; 0 is refused —
//	                           it means "random" to rapid).
//	SLUICE_CONVERGE_TIMEOUT  — per-check convergence wait in seconds
//	                           (default 60). Lower it when re-running a
//	                           failure with a raised -rapid.shrinktime
//	                           so the shrinker gets more attempts.
//
// Phase A reuse: startPostgresLogical / startMySQLBinlog container
// boots, applyDDL / applyDDLMySQL raw-SQL appliers, waitForSourceSlot
// (the slot-existence capture guarantee) and waitForRowCount /
// waitForRowCountMySQL (bulk-copy completion), the Streamer + Filter +
// per-check StreamID/SlotName isolation pattern from the streamer
// integration suite, and fuzzEnvInt from the fuzz harness.

package pipeline

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"

	"pgregory.net/rapid"
)

const (
	convSmokeChecks  = 3
	convSmokeMaxTxs  = 6
	convDefaultSeed  = uint64(0x5104CE) // fixed → deterministic CI, same family as fuzzDefaultSeed
	convStableGrace  = 750 * time.Millisecond
	convFixturesPath = "testdata/converge-failures"
)

// convCaseSeq hands every check (including every shrink attempt) a
// unique index for its table/stream/slot names — the containers are
// reused across checks, so nothing per-check may collide.
var convCaseSeq atomic.Int64

// convApplyRapidBudget wires the SLUICE_CONVERGE_* env knobs into
// rapid's package flags. Named wart: rapid has no per-Check budget
// API — -rapid.checks / -rapid.seed (or their RAPID_* env-var
// defaults) are the only knobs, and rapid's defaults (100 checks,
// random seed) are wrong for a live-DB property that costs seconds
// per check and must be deterministic in PR CI. Precedence: an
// explicit -rapid.* flag wins (so the replay command rapid prints on
// failure keeps working verbatim), then SLUICE_CONVERGE_*, then the
// smoke default.
func convApplyRapidBudget(t *testing.T) (seed uint64, checks, maxTxs int) {
	t.Helper()
	explicit := map[string]bool{}
	flag.CommandLine.Visit(func(f *flag.Flag) { explicit[f.Name] = true })

	checks = fuzzEnvInt("SLUICE_CONVERGE_ITERS", convSmokeChecks)
	if !explicit["rapid.checks"] {
		if err := flag.Set("rapid.checks", strconv.Itoa(checks)); err != nil {
			t.Fatalf("set rapid.checks: %v", err)
		}
	}

	seed = convDefaultSeed
	if v := os.Getenv("SLUICE_CONVERGE_SEED"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil || n == 0 {
			t.Fatalf("SLUICE_CONVERGE_SEED=%q: want a nonzero uint64 (0 means random to rapid)", v)
		}
		seed = n
	}
	if !explicit["rapid.seed"] && !explicit["rapid.failfile"] {
		if err := flag.Set("rapid.seed", strconv.FormatUint(seed, 10)); err != nil {
			t.Fatalf("set rapid.seed: %v", err)
		}
	}

	maxTxs = fuzzEnvInt("SLUICE_CONVERGE_OPS", convSmokeMaxTxs)
	return seed, checks, maxTxs
}

func convConvergeTimeout() time.Duration {
	return time.Duration(fuzzEnvInt("SLUICE_CONVERGE_TIMEOUT", 60)) * time.Second
}

// convLiveEnv binds one direction's booted containers + engines to the
// engine-specific helpers the harness needs. dir carries the source and
// target engine kinds separately — the source side drives DDL/literal/
// CDC dialect and its dump; the target side drives the convergence dump.
// For a cross-engine direction the two sides differ in driver, dialect,
// and canonicalisation; convCrossEngine() flips the value-semantic
// compare on (design #2).
type convLiveEnv struct {
	dir            convDirection
	srcDSN, dstDSN string
	source, target ir.Engine

	// Echoed into failure dumps so the replay command is exact.
	seed           uint64
	checks, maxTxs int
}

func (e *convLiveEnv) crossEngine() bool { return e.dir.crossEngine() }

func engineDriver(eng engineKind) string {
	if eng == enginePG {
		return "pgx"
	}
	return "mysql"
}

func (e *convLiveEnv) srcDriver() string { return engineDriver(e.dir.src) }

func (e *convLiveEnv) dstDriver() string { return engineDriver(e.dir.dst) }

// applySQL applies a source-dialect script to the source. The script is
// always rendered in the source dialect (convCase.eng == dir.src), so
// only the source engine ever runs harness-generated SQL.
func (e *convLiveEnv) applySQL(t *testing.T, dsn, script string) {
	t.Helper()
	if e.dir.src == enginePG {
		applyDDL(t, dsn, script)
	} else {
		applyDDLMySQL(t, dsn, script)
	}
}

// convDump reads the table's full ordered content in the engine's
// server-side canonical text form: every column CAST to text ON THE
// SERVER (driver-side scanning of native types into strings is not
// portable), ORDER BY id (numeric — id is BIGINT — so the row order is
// identical across engines). When canon is true the per-family Go-side
// normaliser (convCanonField, design #2) folds the two engines'
// residual spelling differences inside the safe set to a common form;
// when false the dump is verbatim and two same-engine dumps compare
// byte for byte. Errors are returned, not fataled — during the
// convergence poll the target table legitimately doesn't exist yet.
func convDump(db *sql.DB, eng engineKind, table string, cols []convColumn, canon bool) (dump string, pks []int64, err error) {
	sel := make([]string, 0, len(cols)+1)
	if eng == enginePG {
		sel = append(sel, "id::text")
		for _, c := range cols {
			sel = append(sel, c.name+"::text")
		}
	} else {
		sel = append(sel, "CAST(id AS CHAR)")
		for _, c := range cols {
			sel = append(sel, "CAST("+c.name+" AS CHAR)")
		}
	}
	q := "SELECT " + strings.Join(sel, ", ") + " FROM " + table + " ORDER BY id"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return "", nil, err
	}
	defer func() { _ = rows.Close() }()

	type dumpRow struct {
		pk   int64
		line string
	}
	var dumped []dumpRow
	for rows.Next() {
		vals := make([]sql.NullString, len(cols)+1)
		dest := make([]any, len(vals))
		for i := range vals {
			dest[i] = &vals[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return "", nil, err
		}
		pk, err := strconv.ParseInt(vals[0].String, 10, 64)
		if err != nil {
			return "", nil, fmt.Errorf("non-integer id %q: %w", vals[0].String, err)
		}
		var line strings.Builder
		fmt.Fprintf(&line, "id=%s", vals[0].String)
		for i, c := range cols {
			switch {
			case !vals[i+1].Valid:
				fmt.Fprintf(&line, " %s=NULL", c.name)
			case canon:
				fmt.Fprintf(&line, " %s=%q", c.name, convCanonField(c.fam, vals[i+1].String))
			default:
				fmt.Fprintf(&line, " %s=%q", c.name, vals[i+1].String)
			}
		}
		dumped = append(dumped, dumpRow{pk: pk, line: line.String()})
	}
	if err := rows.Err(); err != nil {
		return "", nil, err
	}

	// Sort by NUMERIC pk in Go, NOT via the SQL ORDER BY. The two
	// engines disagree on what `ORDER BY id` means once id is also
	// projected as text: PG binds ORDER BY to the `id::text` OUTPUT
	// column (lexicographic — "12" < "4"), while MySQL binds it to the
	// bigint TABLE column (numeric). A server-side ORDER BY is therefore
	// NOT cross-engine-consistent, so two engines would emit byte-
	// different dumps of IDENTICAL content purely from row order — a
	// false non-convergence. Sorting here makes the ordering engine-
	// agnostic, so the dump-vs-dump string equality measures content,
	// not each dialect's ORDER BY name-resolution quirk. (Same-engine
	// was unaffected — both sides shared the quirk — which is why this
	// hid until the cross-engine directions ran.)
	slices.SortFunc(dumped, func(a, b dumpRow) int {
		switch {
		case a.pk < b.pk:
			return -1
		case a.pk > b.pk:
			return 1
		default:
			return 0
		}
	})
	var b strings.Builder
	pks = make([]int64, 0, len(dumped))
	for _, d := range dumped {
		b.WriteString(d.line)
		b.WriteByte('\n')
		pks = append(pks, d.pk)
	}
	return b.String(), pks, nil
}

// convDropPGSlot drops the per-check replication slot with a bounded
// retry (the walsender backend can outlive Streamer.Run by a beat,
// and an active slot refuses to drop). Required, not best-effort: the
// container is reused across checks with max_replication_slots=8, so
// leaked slots would wedge a deep run around check 8 with a confusing
// "all replication slots are in use" failure far from the leak.
func convDropPGSlot(t *testing.T, dsn, slot string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Errorf("converge teardown: open source for slot drop: %v", err)
		return
	}
	defer func() { _ = db.Close() }()

	deadline := time.Now().Add(15 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		var n int
		err := db.QueryRowContext(ctx,
			`SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&n)
		if err == nil && n == 0 {
			cancel()
			return
		}
		if err == nil {
			_, _ = db.ExecContext(ctx,
				`SELECT pg_drop_replication_slot($1) FROM pg_replication_slots WHERE slot_name = $1 AND active = false`, slot)
		}
		cancel()
		if time.Now().After(deadline) {
			t.Errorf("converge teardown: slot %s still present after 15s — later checks may exhaust max_replication_slots", slot)
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// convDropTable keeps the reused containers lean across deep runs.
// Best-effort: a failed drop only costs disk, never correctness (every
// check's names are unique).
func convDropTable(t *testing.T, env *convLiveEnv, table string) {
	t.Helper()
	sides := []struct {
		dsn    string
		driver string
	}{
		{env.srcDSN, env.srcDriver()},
		{env.dstDSN, env.dstDriver()},
	}
	for _, s := range sides {
		db, err := sql.Open(s.driver, s.dsn)
		if err != nil {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
			t.Logf("converge teardown: drop %s: %v", table, err)
		}
		cancel()
		_ = db.Close()
	}
}

// convDumpFixture writes the replayable script for the failing case.
// rapid re-runs the property on every shrink attempt and once more on
// the minimal case, and this file is overwritten on each failing
// attempt — so after the run it holds the MINIMAL shrunk script (the
// fuzz harness's dumped-fixture pattern adapted to rapid's shrink
// loop). rapid's own failfile (testdata/rapid/...) is the exact-replay
// twin; this .sql is the human-readable, psql/mysql-runnable form.
func convDumpFixture(t *testing.T, env *convLiveEnv, c *convCase, table string) string {
	t.Helper()
	if err := os.MkdirAll(convFixturesPath, 0o755); err != nil {
		t.Logf("could not create fixtures dir %s: %v", convFixturesPath, err)
		return ""
	}
	p := filepath.Join(convFixturesPath, strings.ReplaceAll(t.Name(), "/", "_")+".sql")
	header := fmt.Sprintf(
		"-- SYNC-CONVERGENCE FAILURE — minimal shrunk case (overwritten per shrink attempt)\n"+
			"-- test=%s direction=%s apply-batch=%d\n"+
			"-- replay: SLUICE_CONVERGE_SEED=%d SLUICE_CONVERGE_ITERS=%d SLUICE_CONVERGE_OPS=%d go test -tags=integration -run '^%s$' ./internal/pipeline\n"+
			"-- (or the -rapid.failfile command rapid prints; raise -rapid.shrinktime for a smaller script)\n\n",
		t.Name(), env.dir, c.applyBatch, env.seed, env.checks, env.maxTxs, t.Name(),
	)
	if err := os.WriteFile(p, []byte(header+c.renderScript(table)), 0o600); err != nil {
		t.Logf("could not write fixture %s: %v", p, err)
		return ""
	}
	return p
}

// runConvCheck is one property check: fresh table + initial rows on
// the source, a real Streamer scoped to that table, the random tx
// sequence applied to the live source, then the convergence
// assertion. Genuine property failures go through rt (so rapid
// shrinks); harness-infrastructure failures go through t (shrinking a
// broken harness is noise).
func runConvCheck(rt *rapid.T, t *testing.T, env *convLiveEnv, c *convCase) {
	idx := convCaseSeq.Add(1)
	table := fmt.Sprintf("conv_%d", idx)
	streamID := fmt.Sprintf("conv-%d", idx)
	slotName := fmt.Sprintf("sluice_conv_%d", idx)

	env.applySQL(t, env.srcDSN, c.renderSetup(table))

	streamer := &Streamer{
		Source:         env.source,
		Target:         env.target,
		SourceDSN:      env.srcDSN,
		TargetDSN:      env.dstDSN,
		StreamID:       streamID,
		SlotName:       slotName, // PG only; engines without slots ignore it
		Filter:         TableFilter{Include: []string{table}},
		ApplyBatchSize: c.applyBatch,
	}
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case <-runErr:
		case <-time.After(20 * time.Second):
			t.Errorf("converge check %d: streamer did not exit within 20s of cancel", idx)
		}
		if env.dir.src == enginePG {
			convDropPGSlot(t, env.srcDSN, slotName)
		}
		convDropTable(t, env, table)
	}()

	// Capture-guarantee gates before the finite op burst (the AIMD
	// "dest only saw 0/250" flake class): bulk-copy completion implies
	// the source position/slot was pinned strictly earlier, so every
	// commit from here on is captured by snapshot or CDC. A PG SOURCE
	// additionally waits on slot existence — the named guarantee (see
	// waitForSourceSlot's doc). The bulk-copy completion gate runs on
	// the TARGET, so its row-count helper is keyed on dir.dst.
	if env.dir.src == enginePG {
		waitForSourceSlot(t, env.srcDSN, 60*time.Second)
	}
	bulkCopied := false
	if env.dir.dst == enginePG {
		bulkCopied = waitForRowCount(t, env.dstDSN, table, len(c.initial), 60*time.Second)
	} else {
		bulkCopied = waitForRowCountMySQL(t, env.dstDSN, table, len(c.initial), 60*time.Second)
	}
	if !bulkCopied {
		rt.Fatalf("bulk copy never delivered the %d initial rows of %s", len(c.initial), table)
	}

	// The op burst: each tx block is its own driver call so tx
	// boundaries are unambiguous (see renderTx's doc). Synchronous —
	// when the loop ends, the source is quiesced and its final state
	// is fixed.
	for i, tx := range c.txs {
		env.applySQL(t, env.srcDSN, c.renderTx(tx, i, table))
	}

	final, err := c.finalModel()
	if err != nil {
		t.Fatalf("HARNESS BUG: generated case is model-invalid: %v", err)
	}

	srcDB, err := sql.Open(env.srcDriver(), env.srcDSN)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer func() { _ = srcDB.Close() }()
	dstDB, err := sql.Open(env.dstDriver(), env.dstDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = dstDB.Close() }()

	canon := env.crossEngine()
	srcDump, srcPKs, err := convDump(srcDB, env.dir.src, table, c.cols, canon)
	if err != nil {
		t.Fatalf("read source dump: %v", err)
	}

	// Independent oracle on the harness itself: the live source's PK
	// set must equal the model's. A mismatch means the generator or
	// the script applier is wrong — a harness bug, not a sluice bug.
	// convDump returns PKs in the dump's own ordering (lexicographic —
	// the rows are ordered by the text-rendered id, so e.g. 17 < 3),
	// while the model sorts numerically; normalize before comparing.
	// The dump-vs-dump convergence comparison below is unaffected:
	// both sides render and order identically, so its ordering only
	// needs to be consistent, not numeric.
	srcPKsSorted := slices.Clone(srcPKs)
	slices.Sort(srcPKsSorted)
	if want := final.livePKs(); !slices.Equal(srcPKsSorted, want) {
		t.Fatalf("HARNESS BUG: source PKs %v != model PKs %v\nscript:\n%s", srcPKsSorted, want, c.renderScript(table))
	}

	// Convergence: poll until the target's ordered canonical dump
	// EQUALS the source's, then re-check after a grace period — a
	// transiently-equal target (a late duplicate or out-of-order apply
	// still in flight) must not pass. The criterion cannot false-pass:
	// equality against the quiesced source's final dump IS the
	// property, and the stability re-check closes the
	// equal-then-diverges hole. The target dump renders in the TARGET
	// dialect and, cross-engine, is value-semantically canonicalised
	// (design #2) so it compares equal to the canonicalised source dump
	// even though raw server text differs by engine.
	timeout := convConvergeTimeout()
	deadline := time.Now().Add(timeout)
	var dstDump string
	for {
		var derr error
		dstDump, _, derr = convDump(dstDB, env.dir.dst, table, c.cols, canon)
		if derr == nil && dstDump == srcDump {
			time.Sleep(convStableGrace)
			again, _, aerr := convDump(dstDB, env.dir.dst, table, c.cols, canon)
			if aerr == nil && again == srcDump {
				return // converged, stably
			}
			dstDump = again
		}
		if time.Now().After(deadline) {
			path := convDumpFixture(t, env, c, table)
			rt.Fatalf("sync did not converge within %s (apply-batch=%d)\n"+
				"--- source (%d rows) ---\n%s--- target ---\n%s"+
				"--- replayable script (also at %s) ---\n%s\n"+
				"replay: SLUICE_CONVERGE_SEED=%d SLUICE_CONVERGE_ITERS=%d SLUICE_CONVERGE_OPS=%d "+
				"go test -tags=integration -run '^%s$' ./internal/pipeline "+
				"(add -rapid.shrinktime=10m and a lower SLUICE_CONVERGE_TIMEOUT for deeper shrinking)",
				timeout, c.applyBatch, len(srcPKs), srcDump, dstDump, path, c.renderScript(table),
				env.seed, env.checks, env.maxTxs, t.Name())
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// convMustEngine fetches a registered engine or fails the test.
func convMustEngine(t *testing.T, name string) ir.Engine {
	t.Helper()
	eng, ok := engines.Get(name)
	if !ok {
		t.Fatalf("%s engine not registered", name)
	}
	return eng
}

// convRunDirection is the shared driver for all four directions: apply
// the budget, build the env, log, and run the rapid property over a
// per-direction generator. The boot is supplied by the caller (the
// only per-direction difference besides the direction itself).
func convRunDirection(t *testing.T, dir convDirection, env *convLiveEnv) {
	t.Logf("sync-convergence property [%s]: seed=%d checks=%d max-txs=%d",
		dir, env.seed, env.checks, env.maxTxs)
	gen := convCaseGen(dir, env.maxTxs)
	rapid.Check(t, func(rt *rapid.T) {
		c := gen.Draw(rt, "case")
		runConvCheck(rt, t, env, &c)
	})
}

// TestSyncConverges_PGToPG runs the property over the slot-based PG
// CDC path — the historical bug surface (bug13/15, AIMD, rotation).
func TestSyncConverges_PGToPG(t *testing.T) {
	seed, checks, maxTxs := convApplyRapidBudget(t)
	srcDSN, dstDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	pgEng := convMustEngine(t, "postgres")
	env := &convLiveEnv{
		dir: convDirPGToPG, srcDSN: srcDSN, dstDSN: dstDSN,
		source: pgEng, target: pgEng,
		seed: seed, checks: checks, maxTxs: maxTxs,
	}
	convRunDirection(t, convDirPGToPG, env)
}

// TestSyncConverges_MySQLToMySQL runs the property over the binlog
// CDC path.
func TestSyncConverges_MySQLToMySQL(t *testing.T) {
	seed, checks, maxTxs := convApplyRapidBudget(t)
	srcDSN, dstDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	myEng := convMustEngine(t, "mysql")
	env := &convLiveEnv{
		dir: convDirMySQLToMySQL, srcDSN: srcDSN, dstDSN: dstDSN,
		source: myEng, target: myEng,
		seed: seed, checks: checks, maxTxs: maxTxs,
	}
	convRunDirection(t, convDirMySQLToMySQL, env)
}

// TestSyncConverges_PGToMySQL runs the property CROSS-ENGINE: a PG
// source (slot-based CDC) streaming into a MySQL target. The generator
// emits only the cross-engine-safe column families (no bool) and the
// convergence compare is value-semantic (per-family canonicalisation),
// not byte-level — see converge_gen.go's cross-engine wart note.
//
// Boot: a PG-logical pair (source_db is the CDC source) and a
// MySQL-binlog pair (its target_db receives the applies). Only the PG
// source_db and the MySQL target_db are used; the unused sibling DBs
// just ride along on the already-booted containers.
func TestSyncConverges_PGToMySQL(t *testing.T) {
	seed, checks, maxTxs := convApplyRapidBudget(t)
	pgSrc, _, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()
	_, myDst, myCleanup := startMySQLBinlog(t)
	defer myCleanup()

	env := &convLiveEnv{
		dir: convDirPGToMySQL, srcDSN: pgSrc, dstDSN: myDst,
		source: convMustEngine(t, "postgres"), target: convMustEngine(t, "mysql"),
		seed: seed, checks: checks, maxTxs: maxTxs,
	}
	convRunDirection(t, convDirPGToMySQL, env)
}

// TestSyncConverges_MySQLToPG runs the property CROSS-ENGINE: a MySQL
// source (binlog CDC) streaming into a PG target. Same safe-subset +
// value-semantic-compare discipline as PGToMySQL, mirrored.
//
// Boot: a MySQL-binlog pair (source_db is the CDC source) and a
// PG-logical pair (its target_db receives the applies).
func TestSyncConverges_MySQLToPG(t *testing.T) {
	seed, checks, maxTxs := convApplyRapidBudget(t)
	mySrc, _, myCleanup := startMySQLBinlog(t)
	defer myCleanup()
	_, pgDst, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()

	env := &convLiveEnv{
		dir: convDirMySQLToPG, srcDSN: mySrc, dstDSN: pgDst,
		source: convMustEngine(t, "mysql"), target: convMustEngine(t, "postgres"),
		seed: seed, checks: checks, maxTxs: maxTxs,
	}
	convRunDirection(t, convDirMySQLToPG, env)
}
