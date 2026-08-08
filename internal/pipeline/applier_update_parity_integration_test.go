//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Audit 2026-08-05 C-9 / C-10: the two ChangeAppliers must reach the SAME
// verdict on an ir.Update whose images are degenerate.
//
// C-9 filed the divergence itself as the defect, and it was right: one change
// stream that leaves a MySQL target and a Postgres target in different states
// cannot be correct on both. What the divergence actually was:
//
//	                              MySQL (coalescing batch)   Postgres (any path)
//	Update, Before nil            upserted, exit 0           42601 syntax error
//	Update, partial After, no row fabricated a row, exit 0   no-op (UPDATE missed)
//
// Both rows of that table are now one verdict per row, on both engines — a
// named refusal for the first, a no-op for the second.
//
// THE GATE'S SCOPE, stated rather than implied. It drives the two engines'
// REAL appliers, obtained from the registry through OpenChangeApplier, against
// real servers — there is no stub here, deliberately: a test double that
// satisfies ir.ChangeApplier satisfies both engines by construction and would
// prove nothing about either. It reaches, per engine, BOTH batch shapes the
// applier routes on: maxBatchSize=1 (the serial per-change path) and
// maxBatchSize>1 (MySQL's ADR-0139/0140 coalescing handle, Postgres's ADR-0092
// pipelined handle). It does NOT reach the --apply-concurrency lane paths;
// those share the same dispatch* functions, and the MySQL lane path is covered
// engine-side by TestPartialAfterUpdate_AbsentRowIsNotFabricated.
//
// The two appliers are the whole population: pgtrigger delegates
// OpenChangeApplier to the Postgres engine, and every other registered engine
// returns ErrNotImplemented. TestChangeApplierPopulationIsTwoEngines is the
// fail-by-default half of that claim — it fails when a third applier appears
// and this gate has not been widened to it.
package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
)

// applierVerdict is what the gate compares between engines: what the applier
// DID with the change, plus what the target actually held afterwards (read
// back with the target's own SELECT, never from the applier's report).
type applierVerdict struct {
	// refused is true when ApplyBatch returned an error wrapping
	// [appliershared.ErrNoRowPredicate] — the named refusal, as opposed to
	// any other error (a driver syntax error, say), which lands in errText.
	refused bool
	// errText is "" on success; otherwise the error, so a divergence report
	// shows WHICH loud failure each engine produced.
	errText string
	// rowPresent / tag / body are the target's own answer for the row the
	// change addressed.
	rowPresent bool
	tag        string
	body       string
	bodyNull   bool
}

func (v applierVerdict) String() string {
	switch {
	case v.refused:
		return "REFUSED(no-row-predicate)"
	case v.errText != "":
		return "ERROR(" + v.errText + ")"
	case !v.rowPresent:
		return "APPLIED(no row at target)"
	default:
		return fmt.Sprintf("APPLIED(row present tag=%q body=%q bodyNull=%v)", v.tag, v.body, v.bodyNull)
	}
}

// parityCase is one row of the divergence map.
type parityCase struct {
	name string
	// seed runs against the target before the change is applied; "" means
	// the row is absent.
	seed string
	// change builds the event. id 1 is the addressed row throughout.
	change func(engine string) ir.Change
	// want is the verdict BOTH engines must produce. Written out rather
	// than "whatever they agree on", so a future change that made them
	// agree on the WRONG answer still fails.
	want string
}

// parityDDL is the same table shape on both engines: a key, a small column an
// UPDATE touches, and a large column an UPDATE leaves alone (the pgoutput
// unchanged-TOAST shape). NOT NULL is avoided on `body` so a fabricating
// INSERT would SUCCEED rather than being caught by a constraint — the silent
// cell is the one worth gating.
func parityDDL(engine string) string {
	if engine == "mysql" {
		return `CREATE TABLE parity (
			id   INT          NOT NULL,
			tag  VARCHAR(32)  NULL,
			body TEXT         NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
	}
	return `CREATE TABLE parity (
		id   INT PRIMARY KEY,
		tag  VARCHAR(32),
		body TEXT
	)`
}

func parityCases() []parityCase {
	pos := func(engine, token string) ir.Position {
		return ir.Position{Engine: engine, Token: token}
	}
	return []parityCase{
		{
			// C-9, the shape that diverged hardest: no before-image at all.
			// MySQL's coalescing path rewrote it into an upsert and applied
			// it; Postgres rendered `WHERE ` and the server refused.
			name: "nil-Before/row-present",
			seed: "INSERT INTO parity (id, tag, body) VALUES (1, 'orig', 'big-body')",
			change: func(e string) ir.Change {
				return ir.Update{
					Position: pos(e, "par-1"), Table: "parity",
					Before: nil,
					After:  ir.Row{"id": int64(1), "tag": "upd", "body": "big-body"},
				}
			},
			want: "REFUSED(no-row-predicate)",
		},
		{
			name: "nil-Before/row-absent",
			seed: "",
			change: func(e string) ir.Change {
				return ir.Update{
					Position: pos(e, "par-2"), Table: "parity",
					Before: nil,
					After:  ir.Row{"id": int64(1), "tag": "upd", "body": "big-body"},
				}
			},
			want: "REFUSED(no-row-predicate)",
		},
		{
			// Before present but EMPTY — the same empty predicate by a
			// different door, and the one a `len(Before) == 0` guard catches
			// while a `Before == nil` guard does not.
			name: "empty-Before/row-present",
			seed: "INSERT INTO parity (id, tag, body) VALUES (1, 'orig', 'big-body')",
			change: func(e string) ir.Change {
				return ir.Update{
					Position: pos(e, "par-3"), Table: "parity",
					Before: ir.Row{},
					After:  ir.Row{"id": int64(1), "tag": "upd", "body": "big-body"},
				}
			},
			want: "REFUSED(no-row-predicate)",
		},
		{
			name: "nil-Before/Delete",
			seed: "INSERT INTO parity (id, tag, body) VALUES (1, 'orig', 'big-body')",
			change: func(e string) ir.Change {
				return ir.Delete{Position: pos(e, "par-4"), Table: "parity", Before: nil}
			},
			want: "REFUSED(no-row-predicate)",
		},
		{
			// C-10, the absent-row cell. `body` is omitted from the
			// after-image exactly as pgoutput omits an unchanged TOASTed
			// column. Neither engine may invent the row.
			name: "partial-After/row-absent",
			seed: "",
			change: func(e string) ir.Change {
				return ir.Update{
					Position: pos(e, "par-5"), Table: "parity",
					Before: ir.Row{"id": int64(1)},
					After:  ir.Row{"id": int64(1), "tag": "upd"},
				}
			},
			want: "APPLIED(no row at target)",
		},
		{
			// C-10, the present-row cell: the omitted column must keep the
			// target's value. This is the half that already worked, and the
			// half a fix could plausibly have broken.
			name: "partial-After/row-present",
			seed: "INSERT INTO parity (id, tag, body) VALUES (1, 'orig', 'big-body')",
			change: func(e string) ir.Change {
				return ir.Update{
					Position: pos(e, "par-6"), Table: "parity",
					Before: ir.Row{"id": int64(1)},
					After:  ir.Row{"id": int64(1), "tag": "upd"},
				}
			},
			want: `APPLIED(row present tag="upd" body="big-body" bodyNull=false)`,
		},
	}
}

// TestApplierUpdateImageParity is the divergence map. Every case × engine ×
// batch shape must produce the `want` verdict; a mismatch names the engine,
// the shape, and both verdicts.
func TestApplierUpdateImageParity(t *testing.T) {
	mysqlDSN, mysqlCleanup := parityMySQLTarget(t)
	defer mysqlCleanup()
	pgDSN, pgCleanup := parityPostgresTarget(t)
	defer pgCleanup()

	targets := []struct {
		engine string
		dsn    string
		schema string
	}{
		{"mysql", mysqlDSN, "target_db"},
		{"postgres", pgDSN, "public"},
	}

	for _, bs := range []struct {
		name      string
		batchSize int
	}{
		{"serial", 1},
		{"batched", 64},
	} {
		for _, tc := range parityCases() {
			for _, tgt := range targets {
				t.Run(fmt.Sprintf("%s/%s/%s", bs.name, tc.name, tgt.engine), func(t *testing.T) {
					got := runParityCase(t, tgt.engine, tgt.dsn, tgt.schema, tc, bs.batchSize)
					if got.String() != tc.want {
						t.Errorf("%s applier (%s):\n  got  %s\n  want %s",
							tgt.engine, bs.name, got, tc.want)
					}
				})
			}
		}
	}
}

// runParityCase resets the target table, seeds it, applies the one change
// through the engine's real applier, and reads the row back.
func runParityCase(t *testing.T, engine, dsn, schema string, tc parityCase, batchSize int) applierVerdict {
	t.Helper()
	db := parityOpen(t, engine, dsn)
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	parityExec(t, ctx, db, "DROP TABLE IF EXISTS parity")
	parityExec(t, ctx, db, parityDDL(engine))
	if tc.seed != "" {
		parityExec(t, ctx, db, tc.seed)
	}

	eng, ok := engines.Get(engine)
	if !ok {
		t.Fatalf("engine %q not registered", engine)
	}
	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier(%s): %v", engine, err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable(%s): %v", engine, err)
	}

	change := tc.change(engine)
	ch := make(chan ir.Change, 1)
	ch <- change
	close(ch)

	var v applierVerdict
	batched, ok := applier.(ir.BatchedChangeApplier)
	if !ok {
		t.Fatalf("%s applier does not implement BatchedChangeApplier", engine)
	}
	if err := batched.ApplyBatch(ctx, "parity-stream", ch, batchSize); err != nil {
		v.refused = errors.Is(err, appliershared.ErrNoRowPredicate)
		if !v.refused {
			v.errText = compactErr(err)
		}
		// A refusal aborts the batch, so the target is whatever the seed
		// left. Reading it back anyway keeps the verdict total.
	}
	// `schema` is unused for the read (the DSN binds the database on both
	// engines) but naming it keeps the caller's intent explicit.
	_ = schema
	v.rowPresent, v.tag, v.body, v.bodyNull = parityReadRow(t, ctx, db, engine)
	return v
}

// parityReadRow reads row id=1 back with the TARGET's own SELECT.
func parityReadRow(t *testing.T, ctx context.Context, db *sql.DB, engine string) (present bool, tag, body string, bodyNull bool) {
	t.Helper()
	q := "SELECT tag, body FROM parity WHERE id = ?"
	if engine == "postgres" {
		q = "SELECT tag, body FROM parity WHERE id = $1"
	}
	var tagN, bodyN sql.NullString
	err := db.QueryRowContext(ctx, q, 1).Scan(&tagN, &bodyN)
	if errors.Is(err, sql.ErrNoRows) {
		return false, "", "", false
	}
	if err != nil {
		t.Fatalf("read back (%s): %v", engine, err)
	}
	return true, tagN.String, bodyN.String, !bodyN.Valid
}

// compactErr flattens an error to one line so a divergence report stays
// readable; the SQLSTATE / errno is what matters for classifying it.
func compactErr(err error) string {
	s := strings.ReplaceAll(err.Error(), "\n", " ")
	if len(s) > 160 {
		s = s[:160] + "…"
	}
	return s
}

func parityExec(t *testing.T, ctx context.Context, db *sql.DB, stmt string) {
	t.Helper()
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		t.Fatalf("exec %q: %v", stmt, err)
	}
}

func parityOpen(t *testing.T, engine, dsn string) *sql.DB {
	t.Helper()
	driver := "mysql"
	if engine == "postgres" {
		driver = "pgx"
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatalf("open %s: %v", engine, err)
	}
	return db
}

// parityMySQLTarget / parityPostgresTarget reuse the package's existing
// container starters and hand back the TARGET DSN.
func parityMySQLTarget(t *testing.T) (dsn string, cleanup func()) {
	t.Helper()
	_, target, c := startMySQL(t)
	return target, c
}

func parityPostgresTarget(t *testing.T) (dsn string, cleanup func()) {
	t.Helper()
	_, target, c := startPostgres(t)
	return target, c
}

// TestChangeApplierPopulationIsTwoEngines is the anti-narrowing half of
// [TestApplierUpdateImageParity]: the parity gate above claims to cover the
// whole applier population, and that claim is only true while the population
// is {mysql, postgres}. Every registered engine must either return a working
// ir.ChangeApplier that the parity map covers, or decline — and a new one that
// does neither fails here rather than silently sitting outside the gate.
//
// The roster is derived from the registry, not from a hand-kept list, and
// carries two anti-vacuity floors: a minimum population (a broken walk fails
// loudly rather than passing on an empty set) and a minimum number of engines
// that DECLINE (if nothing declined, the not-implemented discriminator is
// broken and every engine would read as "covered").
//
// KNOWN LIMIT, stated rather than implied: the registry only holds engines
// whose package this test binary LINKS, so the population it grades is the set
// this package's tests import — 10 at the time of writing, not the whole tree.
// An engine package that no `internal/pipeline` test imports is invisible here.
// The floor is deliberately below the current count rather than equal to it: a
// gate pinned to an exact count fails when someone ADDS an engine, which trains
// people to edit the number instead of reading the finding.
func TestChangeApplierPopulationIsTwoEngines(t *testing.T) {
	// Engines whose applier IS covered by the parity map above.
	covered := map[string]string{
		"mysql":    "mysql.ChangeApplier — covered directly",
		"postgres": "postgres.ChangeApplier — covered directly",
	}
	// Engines that expose an applier by DELEGATION to a covered one. Each
	// entry names the engine it delegates to; the delegation itself is a
	// one-line forward, so covering the delegate covers these.
	delegates := map[string]string{
		"postgres-trigger": "postgres",
		"planetscale":      "mysql",
		"vitess":           "mysql",
		"mariadb":          "mysql",
	}

	names := engines.Names()
	const minLinkedEngines = 8
	if len(names) < minLinkedEngines {
		t.Fatalf("anti-vacuity: discovered only %d registered engines (%v); expected at least %d "+
			"— the registry walk is broken, not the population", len(names), names, minLinkedEngines)
	}

	ctx := context.Background()
	var uncovered []string
	var declined int
	for _, name := range names {
		eng, ok := engines.Get(name)
		if !ok {
			t.Fatalf("engines.Get(%q) missed a name engines.Names() reported", name)
		}
		// An empty DSN never connects; what we are grading is whether the
		// engine DECLINES the capability outright (ErrNotImplemented and
		// friends) or tries to open one.
		_, err := eng.OpenChangeApplier(ctx, "")
		if err != nil && isNotImplemented(err) {
			declined++
			continue
		}
		if _, ok := covered[name]; ok {
			continue
		}
		if _, ok := delegates[name]; ok {
			continue
		}
		uncovered = append(uncovered, name)
	}
	if declined == 0 {
		t.Fatal("anti-vacuity: no engine declined OpenChangeApplier; the not-implemented " +
			"discriminator is broken, so every engine would read as 'covered'")
	}
	t.Logf("graded %d registered engines: %d declined OpenChangeApplier, %d expose one",
		len(names), declined, len(names)-declined)
	if len(uncovered) > 0 {
		t.Errorf("engine(s) %v expose an ir.ChangeApplier that TestApplierUpdateImageParity does "+
			"not cover. Either add them to the parity targets or record here why their applier is "+
			"a delegation to a covered one.", uncovered)
	}
}

// isNotImplemented reports whether err is an engine declining a capability.
// Each engine spells its own sentinel (ErrNotImplemented), so this matches on
// the message rather than importing every engine package for its sentinel.
func isNotImplemented(err error) bool {
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "not implemented") || strings.Contains(s, "not supported")
}
