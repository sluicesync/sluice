//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-42 — key-scoped writes against a DEFERRABLE key, on a real Postgres.
//
// The defect (see change_applier_key_shape.go): a key-narrowed UPDATE/DELETE
// matched more than one row while a DEFERRABLE primary key was transiently
// shared, and the commit-time re-check passed on the final state. The triage
// repro (source `UPDATE s SET id=2 WHERE id=1; DELETE FROM s WHERE u='b'`,
// source ends {(2,a),(3,c)}) left the target at {(3,c)}, at exit 0. The fix
// refuses any such multi-row write; replica mode itself is unchanged, and
// its unchecked deferred constraint is named by a WARN.
//
// The class is pinned, not a representative (the Bug 74 rule): every apply
// path × both privilege modes (a role that can SET replica, and one that
// cannot — where PG's own deferred check runs) × the constraint kinds. The
// expected value is always the TARGET's own state read back over an
// independent connection — never the applier's report of what it did. Each
// cell's outcome is derived per path and mode, so a cell that silently
// converges where it must refuse, or refuses where it must converge, fails.
//
// Paths, and the code each one reaches:
//
//   - per-change  — Apply → applyOneImpl → dispatch (*sql.Tx), one target
//     transaction per change.
//   - batch       — ApplyBatch, one lane → the shared batch loop on the
//     ADR-0092 pipelined handle → dispatchPipelined + sendBatchUnderDeadline.
//   - batch-serial — the same loop with the pipelined pool unavailable →
//     serialBatchTx → dispatch.
//   - lanes       — ApplyBatch, W=4 → ApplyLaneBatch (pipelined) for routed
//     changes and ApplyBarrierChange → applyOneImpl → dispatch for a key
//     change. The lanes' serial fall-back (applyLaneBatchSerial) is not
//     reachable against a pgx pool; it calls the same dispatch as
//     batch-serial.
//
// NOTE: concurrency-adjacent (the lanes cells). The -race Integration job on
// CI is the authoritative gate; this box is CGO=0.

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/logcapture"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// gc42Path is one apply path of the matrix.
type gc42Path struct {
	name           string
	lanes          int  // > 1: the ADR-0105 concurrent lanes
	batch          int  // <= 1: the per-change Apply path
	serialFallback bool // the batch loop's serial *sql.Tx handle
}

func gc42Paths(batch int) []gc42Path {
	return []gc42Path{
		{name: "per-change", batch: 1},
		{name: "batch", batch: batch},
		{name: "batch-serial", batch: batch, serialFallback: true},
		{name: "lanes", lanes: 4, batch: batch},
	}
}

// gc42Env is one privilege mode's target: the DSN the applier connects with
// and the admin DSN the fixture and the ground-truth reads use.
type gc42Env struct {
	name     string
	adminDSN string
	applyDSN string
	role     string // non-empty: tables are handed to this role
	replica  bool   // the applier is expected to run in replica mode
}

// gc42Envs returns the two privilege modes on one fresh database. The
// premise each mode rests on — the admin CAN set replica and the role
// CANNOT — is asserted against the live server, not assumed.
func gc42Envs(t *testing.T) []gc42Env {
	t.Helper()
	adminDSN, _ := startPostgresForApplier(t)
	host, port, _, _ := ensureSharedPostgres(t)
	role := fmt.Sprintf("gc42_app_%d", time.Now().UnixNano())
	applyPGApplier(t, adminDSN, fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD 'pw'; GRANT ALL ON SCHEMA public TO %s;`, role, role))
	t.Cleanup(func() {
		applyPGApplier(t, adminDSN, fmt.Sprintf(`DROP OWNED BY %s; DROP ROLE IF EXISTS %s;`, role, role))
	})
	roleDSN := sharedPGDSN(host, port, role, "pw", "target_db")
	if v, ok := pgScalarString(t, adminDSN, "SHOW server_version"); ok {
		t.Logf("target server_version %s", v)
	}

	if err := gc42TrySetReplica(adminDSN); err != nil {
		t.Fatalf("premise broken: the admin role cannot SET session_replication_role=replica (%v) — the replica-mode cells would test nothing", err)
	}
	if err := gc42TrySetReplica(roleDSN); err == nil {
		t.Fatal("premise broken: the unprivileged role CAN set session_replication_role=replica — the no-privilege cells would test nothing")
	}
	// The role creates the control tables, so it owns them; the superuser
	// cells can use them regardless, the reverse would need grants.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	a := openConcurrentApplier(t, ctx, roleDSN, 0)
	defer func() { _ = a.Close() }()
	if err := a.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable as the unprivileged role: %v", err)
	}
	return []gc42Env{
		{name: "replica", adminDSN: adminDSN, applyDSN: adminDSN, replica: true},
		{name: "no-replica-privilege", adminDSN: adminDSN, applyDSN: roleDSN, role: role},
	}
}

func gc42TrySetReplica(dsn string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(ctx, replicaRoleSQL)
	return err
}

// gc42Abbrev shortens the matrix's axis names so a cell's table name stays
// inside Postgres's 63-byte identifier limit — past it the name is silently
// truncated, cells collide, and the applier skips the table as unknown.
var gc42Abbrev = map[string]string{
	"replica": "r", "no-replica-privilege": "n",
	"per-change": "pc", "batch": "b", "batch-serial": "bs", "lanes": "l",
	"batch-onetx": "b1", "batch-serial-onetx": "bs1", "batch-split": "bx",
	"batch-serial-split": "bsx", "lanes-split": "lx", "lanes-onetx": "l1",
	"insert": "ins", "update": "upd",
}

// gc42Name is a cell's table name.
func gc42Name(t *testing.T, parts ...string) string {
	t.Helper()
	for i, p := range parts {
		if a, ok := gc42Abbrev[p]; ok {
			parts[i] = a
		}
	}
	name := strings.ReplaceAll("gc42_"+strings.Join(parts, "_"), "-", "_")
	if len(name) > 55 { // room for the constraint-name suffixes
		t.Fatalf("cell table name %q is %d bytes; Postgres truncates identifiers at 63", name, len(name))
	}
	return name
}

// gc42Table creates one cell's table from ddl (with %[1]s standing for its
// name), runs seed, and hands it to the env's role.
func gc42Table(t *testing.T, env gc42Env, name, ddl, seed string) {
	t.Helper()
	applyPGApplier(t, env.adminDSN, fmt.Sprintf(ddl, name))
	if seed != "" {
		applyPGApplier(t, env.adminDSN, fmt.Sprintf(seed, name))
	}
	if env.role != "" {
		applyPGApplier(t, env.adminDSN, fmt.Sprintf(`ALTER TABLE %s OWNER TO %s;`, name, env.role))
	}
}

// gc42Apply pushes events through one path and returns Apply's error.
func gc42Apply(t *testing.T, env gc42Env, p gc42Path, streamID string, events []ir.Change) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	a := openConcurrentApplier(t, ctx, env.applyDSN, p.lanes)
	defer func() { _ = a.Close() }()
	if p.serialFallback {
		a.pipelineCfg = nil
	}
	if err := a.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}
	if got := a.foreignKeyBypassAvailable(ctx); got != env.replica {
		t.Fatalf("applier replica mode = %v; the %s cell needs %v", got, env.name, env.replica)
	}
	ch := make(chan ir.Change, len(events))
	for _, e := range events {
		ch <- e
	}
	close(ch)
	if p.batch <= 1 {
		return a.Apply(ctx, streamID, ch)
	}
	return a.ApplyBatch(ctx, streamID, ch, p.batch)
}

// gc42Tx wraps row changes in one source transaction with unique positions.
func gc42Tx(cell string, rows ...ir.Change) []ir.Change {
	n := 0
	pos := func() ir.Position { n++; return cpos(fmt.Sprintf("gc42-%s-%07d", cell, n)) }
	out := []ir.Change{ir.TxBegin{Position: pos()}}
	for _, r := range rows {
		switch v := r.(type) {
		case ir.Insert:
			v.Position = pos()
			r = v
		case ir.Update:
			v.Position = pos()
			r = v
		case ir.Delete:
			v.Position = pos()
			r = v
		}
		out = append(out, r)
	}
	return append(out, ir.TxCommit{Position: pos()})
}

// gc42State is the table's content as the TARGET holds it: every row's text
// form, sorted — independent of anything the applier reports.
func gc42State(t *testing.T, dsn, table string) string {
	t.Helper()
	s, _ := pgScalarString(t, dsn, fmt.Sprintf(`SELECT COALESCE(string_agg(x::text, ' ' ORDER BY x::text), '') FROM %s x`, table))
	return s
}

// gc42Dups counts rows beyond the first per value of keyExpr — the
// duplicates a "unique" index must never hold once committed.
func gc42Dups(t *testing.T, dsn, table, keyExpr string) int {
	t.Helper()
	return pgScalarInt(t, dsn, fmt.Sprintf(`SELECT count(*) - count(DISTINCT (%s)) FROM %s`, keyExpr, table))
}

// gc42Outcome is what a cell must end in.
type gc42Outcome int

const (
	gc42Converge     gc42Outcome = iota // nil error and exactly the expected state
	gc42MultiMatch                      // G1's terminal, coded refusal
	gc42DeferredFail                    // a commit refused by the deferred re-check
	gc42StatementErr                    // the constraint's own SQLSTATE at the statement (INITIALLY IMMEDIATE)
)

// gc42Assert checks one cell's error against the outcome it must have.
func gc42Assert(t *testing.T, err error, want gc42Outcome, wantCode string) {
	t.Helper()
	switch want {
	case gc42Converge:
		if err != nil {
			t.Fatalf("want a converged apply; got %v", err)
		}
	case gc42MultiMatch:
		if err == nil {
			t.Fatal("want the GC-42 multi-row refusal; Apply returned nil")
		}
		if !errors.Is(err, appliershared.ErrKeyScopedWriteMatchedMultipleRows) ||
			!strings.Contains(err.Error(), appliershared.KeyScopedWriteMultiMatchMarker) {
			t.Fatalf("want %s; got %v", appliershared.KeyScopedWriteMultiMatchMarker, err)
		}
		if !ir.IsTerminal(err) {
			t.Errorf("the multi-row refusal is not terminal: %v", err)
		}
		if c, ok := sluicecode.FromError(err); !ok || c.Code != sluicecode.CodeCDCKeyMatchedMultipleRows {
			t.Errorf("the multi-row refusal is not coded %s: %v", sluicecode.CodeCDCKeyMatchedMultipleRows, err)
		}
	case gc42DeferredFail:
		if err == nil || !strings.Contains(err.Error(), deferredCheckFailedMarker) {
			t.Fatalf("want the %s commit refusal; got %v", deferredCheckFailedMarker, err)
		}
		gc42AssertCode(t, err, wantCode)
	case gc42StatementErr:
		if err == nil {
			t.Fatal("want the constraint's statement-time refusal; Apply returned nil")
		}
		gc42AssertCode(t, err, wantCode)
	}
}

func gc42AssertCode(t *testing.T, err error, want string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != want {
		t.Fatalf("want SQLSTATE %s in the chain; got %v", want, err)
	}
}

// gc42Splits reports whether a path commits a key-changing change alone
// (per-change, or a lane barrier) — where a transiently-shared key is
// committed, or refused by the deferred check where that check runs.
func gc42Splits(p gc42Path) bool { return p.name == "per-change" || p.name == "lanes" }

// TestGC42_TriageRepro is the filed repro on every path and both privilege
// modes. Before the fix every replica-mode cell and the no-privilege batch
// cells ended at {(3,c)} with a nil error.
func TestGC42_TriageRepro(t *testing.T) {
	const ddl = `CREATE TABLE %[1]s (id int, u text NOT NULL UNIQUE, CONSTRAINT %[1]s_pk PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED)`
	const seed = `INSERT INTO %[1]s VALUES (1,'a'),(2,'b'),(3,'c')`
	for _, env := range gc42Envs(t) {
		for _, p := range gc42Paths(1000) {
			t.Run(env.name+"/"+p.name, func(t *testing.T) {
				table := gc42Name(t, "repro", env.name, p.name)
				gc42Table(t, env, table, ddl, seed)
				before := gc42State(t, env.adminDSN, table)
				err := gc42Apply(t, env, p, table, gc42Tx(
					table,
					ir.Update{Schema: "public", Table: table, Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(2), "u": "a"}},
					ir.Delete{Schema: "public", Table: table, Before: ir.Row{"id": int64(2)}},
				))
				// Where PG's deferred check runs, the key change committed
				// alone is refused at its commit. Everywhere else the DELETE
				// reaches the shared key and the multi-row guard refuses it.
				want := gc42MultiMatch
				if !env.replica && gc42Splits(p) {
					want = gc42DeferredFail
				}
				gc42Assert(t, err, want, pgUniqueViolation)
				got := gc42State(t, env.adminDSN, table)
				if got == "(3,c)" {
					t.Fatalf("target lost (2,a): %q", got)
				}
				if !env.replica || !gc42Splits(p) {
					// Nothing committed. (Replica mode on a splitting path
					// commits the key change before the DELETE is refused.)
					if got != before {
						t.Errorf("target = %q; want it untouched at %q", got, before)
					}
				}
			})
		}
	}
}

// gc42Shift builds UPDATE id = id + 1 over n rows in the order the source
// heap visits them; ascending makes every step transiently share a key. A
// byIndex narrows the before-image the way a REPLICA IDENTITY USING
// INDEX source does (to the index column, here u).
func gc42Shift(table string, n int, ascending, byIndex bool) []ir.Change {
	rows := make([]ir.Change, 0, n)
	for i := 0; i < n; i++ {
		k := int64(i + 1)
		if !ascending {
			k = int64(n - i)
		}
		before := ir.Row{"id": k}
		if byIndex {
			before = ir.Row{"u": k}
		}
		rows = append(rows, ir.Update{
			Schema: "public", Table: table,
			Before: before, After: ir.Row{"id": k + 1, "u": k},
		})
	}
	return gc42Tx(table, rows...)
}

// TestGC42_KeyShift pins UPDATE id = id + 1 over 3000 rows under a
// DEFERRABLE primary key, with a primary-key-narrowed before-image (a source
// under REPLICA IDENTITY FULL). Ascending, every step leaves a key shared and
// the next change addresses it by that key, so no path can tell the two rows
// apart and every path refuses. Descending, no state is ever invalid and
// every path converges — the over-refusal guard.
//
// Two table shapes, because they refuse differently:
//
//   - pkonly: the DEFERRABLE primary key is the only key, so the second
//     UPDATE's two-row match reaches the multi-row guard. (Such a table cannot
//     take INSERTs — SLUICE-E-TARGET-DEFERRABLE-KEY — but an update-only
//     window is real.)
//   - arbiter: the shape a real sync can stream into, with an immediate NOT
//     NULL UNIQUE beside the key as the INSERT arbiter. The two-row UPDATE
//     writes the same after-image into both rows, so that immediate index
//     refuses it at the statement (23505) before the guard reads the count.
//
// Where PG's deferred check runs (no replica privilege), a key change
// committed alone (per-change, a lane barrier) is refused at its commit first.
func TestGC42_KeyShift(t *testing.T) {
	const n = 3000
	shapes := []struct{ name, ddl string }{
		{"pkonly", `CREATE TABLE %[1]s (id int, u int NOT NULL, CONSTRAINT %[1]s_pk PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED)`},
		{"arbiter", `CREATE TABLE %[1]s (id int, u int NOT NULL UNIQUE, CONSTRAINT %[1]s_pk PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED)`},
	}
	seed := fmt.Sprintf(`INSERT INTO %%[1]s SELECT g, g FROM generate_series(1, %d) g`, n)
	for _, env := range gc42Envs(t) {
		for _, shape := range shapes {
			for _, p := range gc42Paths(1000) {
				for _, ascending := range []bool{true, false} {
					dir := map[bool]string{true: "asc", false: "desc"}[ascending]
					t.Run(env.name+"/"+shape.name+"/"+p.name+"/"+dir, func(t *testing.T) {
						table := gc42Name(t, "shift", env.name, shape.name, p.name, dir)
						gc42Table(t, env, table, shape.ddl, seed)
						err := gc42Apply(t, env, p, table, gc42Shift(table, n, ascending, false))
						if !ascending {
							gc42Assert(t, err, gc42Converge, "")
							gc42AssertShifted(t, env.adminDSN, table, n)
							return
						}
						want := gc42MultiMatch
						switch {
						case !env.replica && gc42Splits(p):
							want = gc42DeferredFail
						case shape.name == "arbiter":
							want = gc42StatementErr
						}
						gc42Assert(t, err, want, pgUniqueViolation)
						if d := gc42Dups(t, env.adminDSN, table, "id"); d > 1 {
							t.Errorf("the refused shift left %d duplicate ids committed; at most the one the first committed change made", d)
						}
					})
				}
			}
		}
	}
}

// gc42AssertShifted checks every row moved to id = u + 1, exactly once.
func gc42AssertShifted(t *testing.T, dsn, table string, n int) {
	t.Helper()
	shifted := pgScalarInt(t, dsn, fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = u + 1`, table))
	if shifted != n || countAllRows(t, dsn, table) != n {
		t.Fatalf("shift did not converge: %d of %d rows at id = u + 1", shifted, n)
	}
}

// TestGC42_UsingIndexShiftConverges is the review's MEDIUM-1 cell: a source
// under REPLICA IDENTITY USING INDEX (u) narrows every before-image to {u},
// which is unique at every step, so an ascending primary-key shift is never
// ambiguous. Every key change routes to the lane barrier
// (laneapply.PKChangedUpdate cannot see the key in the before-image) and
// commits alone. In replica mode — no deferred check — it must CONVERGE on
// every path; the withdrawn G2 role switch refused it forever. Where PG's
// deferred check does run (no replica privilege) a key change committed
// alone, or a batch split mid-shift, is refused by PostgreSQL itself, as it
// always was; one transaction converges.
func TestGC42_UsingIndexShiftConverges(t *testing.T) {
	const n = 3000
	const ddl = `CREATE TABLE %[1]s (id int, u int NOT NULL UNIQUE, CONSTRAINT %[1]s_pk PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED)`
	seed := fmt.Sprintf(`INSERT INTO %%[1]s SELECT g, g FROM generate_series(1, %d) g`, n)
	paths := append(gc42Paths(1000), gc42Path{name: "batch-onetx", batch: 5 * n})
	for _, env := range gc42Envs(t) {
		for _, p := range paths {
			t.Run(env.name+"/"+p.name, func(t *testing.T) {
				table := gc42Name(t, "ri_idx", env.name, p.name)
				gc42Table(t, env, table, ddl, seed)
				err := gc42Apply(t, env, p, table, gc42Shift(table, n, true, true))
				if env.replica || p.name == "batch-onetx" {
					gc42Assert(t, err, gc42Converge, "")
					gc42AssertShifted(t, env.adminDSN, table, n)
					return
				}
				gc42Assert(t, err, gc42DeferredFail, pgUniqueViolation)
				if d := gc42Dups(t, env.adminDSN, table, "id"); d != 0 {
					t.Fatalf("PG's deferred check refused, yet %d duplicate ids committed", d)
				}
			})
		}
	}
}

// TestGC42_UniqueSwap20k pins a 20,000-row value swap on a DEFERRABLE
// non-primary UNIQUE: the WHERE uses the immediate PRIMARY KEY, so no write
// is ambiguous and the multi-row guard never fires. In replica mode it
// converges on every path (the deferred check is off; between batches the
// target transiently shows duplicate u values, which the WARN names). Where
// PG's deferred check runs, one target transaction converges and a split is
// refused by PostgreSQL itself — unchanged by GC-42.
func TestGC42_UniqueSwap20k(t *testing.T) {
	const n = 20000
	const ddl = `CREATE TABLE %[1]s (id int PRIMARY KEY, u int NOT NULL, CONSTRAINT %[1]s_u UNIQUE (u) DEFERRABLE INITIALLY DEFERRED)`
	seed := fmt.Sprintf(`INSERT INTO %%[1]s SELECT g, g FROM generate_series(1, %d) g`, n)
	cells := []struct {
		p     gc42Path
		split bool // the swap cannot land in one target transaction
	}{
		{gc42Path{name: "per-change", batch: 1}, true},
		{gc42Path{name: "batch-onetx", batch: 3 * n}, false},
		{gc42Path{name: "batch-serial-onetx", batch: 3 * n, serialFallback: true}, false},
		{gc42Path{name: "batch-split", batch: 1000}, true},
		{gc42Path{name: "batch-serial-split", batch: 1000, serialFallback: true}, true},
		{gc42Path{name: "lanes-split", lanes: 4, batch: 1000}, true},
	}
	for _, env := range gc42Envs(t) {
		for _, c := range cells {
			t.Run(env.name+"/"+c.p.name, func(t *testing.T) {
				table := gc42Name(t, "swap", env.name, c.p.name)
				gc42Table(t, env, table, ddl, seed)
				rows := make([]ir.Change, 0, n)
				for k := int64(1); k <= n; k++ {
					rows = append(rows, ir.Update{
						Schema: "public", Table: table,
						Before: ir.Row{"id": k}, After: ir.Row{"id": k, "u": n + 1 - k},
					})
				}
				err := gc42Apply(t, env, c.p, table, gc42Tx(table, rows...))
				swapped := pgScalarInt(t, env.adminDSN, fmt.Sprintf(`SELECT count(*) FROM %s WHERE u = %d - id`, table, n+1))
				if env.replica || !c.split {
					gc42Assert(t, err, gc42Converge, "")
					if swapped != n {
						t.Fatalf("apply returned nil but only %d of %d rows are swapped", swapped, n)
					}
				} else {
					gc42Assert(t, err, gc42DeferredFail, pgUniqueViolation)
				}
				if d := gc42Dups(t, env.adminDSN, table, "u"); d != 0 {
					t.Fatalf("the target holds %d duplicate u values", d)
				}
			})
		}
	}
}

// gc42Kind is one deferrable constraint shape. ddl/seed take the table name
// as %[1]s; dupInsert and dupUpdate are the two writes that make keyExpr
// collide.
type gc42Kind struct {
	name       string
	ddl        string
	seed       string
	keyExpr    string
	code       string
	immediate  bool // INITIALLY IMMEDIATE: refused at the statement, not at COMMIT
	deferrable bool // false: the plain-constraint control
	dupInsert  func(table string) []ir.Change
	dupUpdate  func(table string) []ir.Change
}

func gc42Kinds() []gc42Kind {
	pkInsert := func(table string) []ir.Change {
		return []ir.Change{
			ir.Insert{Schema: "public", Table: table, Row: ir.Row{"id": int64(10), "u": int64(10)}},
			ir.Insert{Schema: "public", Table: table, Row: ir.Row{"id": int64(10), "u": int64(11)}},
		}
	}
	pkUpdate := func(table string) []ir.Change {
		return []ir.Change{ir.Update{Schema: "public", Table: table, Before: ir.Row{"id": int64(2)}, After: ir.Row{"id": int64(1), "u": int64(2)}}}
	}
	uInsert := func(table string) []ir.Change {
		return []ir.Change{
			ir.Insert{Schema: "public", Table: table, Row: ir.Row{"id": int64(10), "u": int64(10)}},
			ir.Insert{Schema: "public", Table: table, Row: ir.Row{"id": int64(11), "u": int64(10)}},
		}
	}
	uUpdate := func(table string) []ir.Change {
		return []ir.Change{ir.Update{Schema: "public", Table: table, Before: ir.Row{"id": int64(2)}, After: ir.Row{"id": int64(2), "u": int64(1)}}}
	}
	const seed = `INSERT INTO %[1]s VALUES (1, 1), (2, 2)`
	out := []gc42Kind{{
		name: "unique_plain", seed: seed, keyExpr: "u", code: pgUniqueViolation, immediate: true,
		ddl:       `CREATE TABLE %[1]s (id int PRIMARY KEY, u int NOT NULL UNIQUE)`,
		dupInsert: uInsert, dupUpdate: uUpdate,
	}}
	for _, mode := range []string{"DEFERRED", "IMMEDIATE"} {
		imm := mode == "IMMEDIATE"
		suffix := strings.ToLower(mode)
		out = append(
			out,
			// A deferrable PRIMARY KEY needs an immediate arbiter for its
			// INSERTs (Bug 211); u is that arbiter.
			gc42Kind{
				name: "pk_" + suffix, seed: seed, keyExpr: "id", code: pgUniqueViolation, immediate: imm, deferrable: true,
				ddl:       `CREATE TABLE %[1]s (id int, u int NOT NULL UNIQUE, CONSTRAINT %[1]s_pk PRIMARY KEY (id) DEFERRABLE INITIALLY ` + mode + `)`,
				dupInsert: pkInsert, dupUpdate: pkUpdate,
			},
			gc42Kind{
				name: "unique_" + suffix, seed: seed, keyExpr: "u", code: pgUniqueViolation, immediate: imm, deferrable: true,
				ddl:       `CREATE TABLE %[1]s (id int PRIMARY KEY, u int NOT NULL, CONSTRAINT %[1]s_u UNIQUE (u) DEFERRABLE INITIALLY ` + mode + `)`,
				dupInsert: uInsert, dupUpdate: uUpdate,
			},
			gc42Kind{
				name: "exclude_" + suffix, seed: seed, keyExpr: "u", code: pgExclusionViolation, immediate: imm, deferrable: true,
				ddl:       `CREATE TABLE %[1]s (id int PRIMARY KEY, u int NOT NULL, CONSTRAINT %[1]s_x EXCLUDE USING btree (u WITH =) DEFERRABLE INITIALLY ` + mode + `)`,
				dupInsert: uInsert, dupUpdate: uUpdate,
			},
		)
	}
	return out
}

// TestGC42_TargetStricterThanSource pins what a target holding a constraint
// the source lacks gets, per privilege mode, when the source sends a row
// pair that violates it:
//
//   - replica mode, deferrable constraint: the rows are applied exactly as
//     sent (the deferred check is off) and the one-time
//     DEFERRED-KEY-CHECK-OFF-IN-REPLICA-MODE WARN names the table and the
//     constraint — a constraint-enforcement gap, made visible, with the data
//     still matching the source.
//   - no replica privilege: PostgreSQL's own check refuses — at COMMIT
//     (marked DEFERRED-KEY-CHECK-FAILED-AT-COMMIT) for INITIALLY DEFERRED, at
//     the statement for INITIALLY IMMEDIATE.
//   - a plain (non-deferrable) constraint refuses at the statement in both
//     modes and draws no WARN — the control.
func TestGC42_TargetStricterThanSource(t *testing.T) {
	logs := &logcapture.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	for _, env := range gc42Envs(t) {
		for _, k := range gc42Kinds() {
			for _, p := range gc42Paths(1000) {
				for _, op := range []string{"insert", "update"} {
					t.Run(env.name+"/"+k.name+"/"+p.name+"/"+op, func(t *testing.T) {
						table := gc42Name(t, "strict", env.name, k.name, p.name, op)
						gc42Table(t, env, table, k.ddl, k.seed)
						rows := k.dupInsert(table)
						if op == "update" {
							rows = k.dupUpdate(table)
						}
						logs.Reset()
						err := gc42Apply(t, env, p, table, gc42Tx(table, rows...))
						warned := gc42WarnedFor(logs.String(), table)
						switch {
						case env.replica && k.deferrable:
							gc42Assert(t, err, gc42Converge, "")
							if !warned {
								t.Errorf("no %s WARN naming %s:\n%s", deferredCheckOffMarker, table, logs.String())
							}
							if d := gc42Dups(t, env.adminDSN, table, k.keyExpr); d == 0 {
								t.Error("the source's violating pair is not on the target — the rows no longer mirror the source")
							}
							return
						case k.immediate:
							gc42Assert(t, err, gc42StatementErr, k.code)
						default:
							gc42Assert(t, err, gc42DeferredFail, k.code)
						}
						if warned {
							t.Errorf("%s fired where the check runs:\n%s", deferredCheckOffMarker, logs.String())
						}
						if d := gc42Dups(t, env.adminDSN, table, k.keyExpr); d != 0 {
							t.Fatalf("the target committed %d duplicate %s values where the check runs", d, k.keyExpr)
						}
					})
				}
			}
		}
	}
}

// gc42WarnedFor reports whether the captured log carries the replica-mode
// WARN for table.
func gc42WarnedFor(log, table string) bool {
	for _, line := range strings.Split(log, "\n") {
		if strings.Contains(line, deferredCheckOffMarker) && strings.Contains(line, "public."+table) {
			return true
		}
	}
	return false
}

// TestGC42_MultiRowMatchRefused is the guard head-on, independent of any
// deferred check, on every path:
//
//   - corrupted: a target a pre-fix sluice already corrupted — two rows
//     share a DEFERRABLE primary key (seeded under replica mode). A non-key
//     UPDATE and a DELETE by that key each match both. This is the only
//     shape that reaches the guard on the lane PIPELINED path, because a key
//     change routes to the barrier.
//   - narrowed: the before-image carries a column the target does not hold
//     unique (a REPLICA IDENTITY USING INDEX source whose index the target
//     lacks), on a table keyed by an ordinary PRIMARY KEY.
//   - keyless_narrowed: an operator-made KEYLESS target fed a key-narrowed
//     before-image (a keyed source under REPLICA IDENTITY FULL): the target
//     has no key, but the WHERE is not the whole row, so it is not exempt.
//
// And the one thing it must NOT refuse: a write that matches zero rows
// (ADR-0010 resume idempotency). A keyless table's identical rows are
// TestGC42_KeylessIdenticalRows'.
func TestGC42_MultiRowMatchRefused(t *testing.T) {
	type cell struct {
		name, ddl, seed string
		rows            func(table string) []ir.Change
		want            gc42Outcome
	}
	const corruptDDL = `CREATE TABLE %[1]s (id int, u text NOT NULL UNIQUE, v text, CONSTRAINT %[1]s_pk PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED)`
	const corruptSeed = `BEGIN; SET LOCAL session_replication_role = replica; INSERT INTO %[1]s VALUES (7,'a','x'),(7,'b','x'); COMMIT;`
	cells := []cell{
		{"corrupted_update", corruptDDL, corruptSeed, func(table string) []ir.Change {
			return []ir.Change{ir.Update{Schema: "public", Table: table, Before: ir.Row{"id": int64(7)}, After: ir.Row{"id": int64(7), "v": "z"}}}
		}, gc42MultiMatch},
		{"corrupted_delete", corruptDDL, corruptSeed, func(table string) []ir.Change {
			return []ir.Change{ir.Delete{Schema: "public", Table: table, Before: ir.Row{"id": int64(7)}}}
		}, gc42MultiMatch},
		{"narrowed_update", `CREATE TABLE %[1]s (id int PRIMARY KEY, u text NOT NULL, v text)`, `INSERT INTO %[1]s VALUES (1,'x','a'),(2,'x','b')`, func(table string) []ir.Change {
			return []ir.Change{ir.Update{Schema: "public", Table: table, Before: ir.Row{"u": "x"}, After: ir.Row{"u": "x", "v": "z"}}}
		}, gc42MultiMatch},
		{"narrowed_delete", `CREATE TABLE %[1]s (id int PRIMARY KEY, u text NOT NULL, v text)`, `INSERT INTO %[1]s VALUES (1,'x','a'),(2,'x','b')`, func(table string) []ir.Change {
			return []ir.Change{ir.Delete{Schema: "public", Table: table, Before: ir.Row{"u": "x"}}}
		}, gc42MultiMatch},
		{"keyless_narrowed_update", `CREATE TABLE %[1]s (id int, v text)`, `INSERT INTO %[1]s VALUES (2,'a'),(2,'b')`, func(table string) []ir.Change {
			return []ir.Change{ir.Update{Schema: "public", Table: table, Before: ir.Row{"id": int64(2)}, After: ir.Row{"id": int64(2), "v": "z"}}}
		}, gc42MultiMatch},
		{"keyless_narrowed_delete", `CREATE TABLE %[1]s (id int, v text)`, `INSERT INTO %[1]s VALUES (2,'a'),(2,'b')`, func(table string) []ir.Change {
			return []ir.Change{ir.Delete{Schema: "public", Table: table, Before: ir.Row{"id": int64(2)}}}
		}, gc42MultiMatch},
		{"zero_rows", `CREATE TABLE %[1]s (id int PRIMARY KEY, v text)`, `INSERT INTO %[1]s VALUES (1,'a')`, func(table string) []ir.Change {
			return []ir.Change{
				ir.Update{Schema: "public", Table: table, Before: ir.Row{"id": int64(99)}, After: ir.Row{"id": int64(99), "v": "z"}},
				ir.Delete{Schema: "public", Table: table, Before: ir.Row{"id": int64(98)}},
			}
		}, gc42Converge},
	}
	for _, env := range gc42Envs(t) {
		for _, c := range cells {
			for _, p := range gc42Paths(1000) {
				t.Run(env.name+"/"+c.name+"/"+p.name, func(t *testing.T) {
					table := gc42Name(t, "g1", env.name, c.name, p.name)
					gc42Table(t, env, table, c.ddl, c.seed)
					before := gc42State(t, env.adminDSN, table)
					err := gc42Apply(t, env, p, table, gc42Tx(table, c.rows(table)...))
					gc42Assert(t, err, c.want, "")
					if c.want == gc42MultiMatch {
						if got := gc42State(t, env.adminDSN, table); got != before {
							t.Errorf("target = %q; want it untouched at %q", got, before)
						}
						if !strings.Contains(err.Error(), table) {
							t.Errorf("the refusal does not name the table: %v", err)
						}
					}
				})
			}
		}
	}
}

// TestGC42_KeylessIdenticalRows pins that a change to ONE of several
// identical rows of a keyless table reaches exactly one target row. The
// source (a keyless table under REPLICA IDENTITY FULL, a trigger source, a
// MySQL binlog full image) deletes or updates one physical row and sends its
// whole image; the applier's whole-row WHERE matches every identical copy, and
// before the fix it deleted or rewrote them all — silent, at exit 0, since
// v0.1.0. The partitioned cell is the reason the one-row address is
// (tableoid, ctid) and not ctid alone: a ctid repeats across partitions, so a
// bare `ctid = …` against the parent also hits the other partition's row.
func TestGC42_KeylessIdenticalRows(t *testing.T) {
	type cell struct {
		name, ddl, seed string
		rows            func(table string) []ir.Change
		wantState       string
	}
	const plain = `CREATE TABLE %[1]s (id int, v text)`
	cells := []cell{
		{"delete", plain, `INSERT INTO %[1]s VALUES (1,'a'),(1,'a'),(2,'b')`, func(table string) []ir.Change {
			return []ir.Change{ir.Delete{Schema: "public", Table: table, Before: ir.Row{"id": int64(1), "v": "a"}}}
		}, "(1,a) (2,b)"},
		{"update", plain, `INSERT INTO %[1]s VALUES (1,'a'),(1,'a')`, func(table string) []ir.Change {
			return []ir.Change{ir.Update{Schema: "public", Table: table, Before: ir.Row{"id": int64(1), "v": "a"}, After: ir.Row{"id": int64(1), "v": "z"}}}
		}, "(1,a) (1,z)"},
		{"delete_null", plain, `INSERT INTO %[1]s VALUES (1,NULL),(1,NULL)`, func(table string) []ir.Change {
			return []ir.Change{ir.Delete{Schema: "public", Table: table, Before: ir.Row{"id": int64(1), "v": nil}}}
		}, "(1,)"},
		{
			"partitioned", `CREATE TABLE %[1]s (id int, v text) PARTITION BY LIST (id);
			CREATE TABLE %[1]s_p1 PARTITION OF %[1]s FOR VALUES IN (1);
			CREATE TABLE %[1]s_p2 PARTITION OF %[1]s FOR VALUES IN (2);`,
			`INSERT INTO %[1]s VALUES (1,'a'),(1,'a'),(2,'b')`, func(table string) []ir.Change {
				return []ir.Change{ir.Delete{Schema: "public", Table: table, Before: ir.Row{"id": int64(1), "v": "a"}}}
			}, "(1,a) (2,b)",
		},
	}
	for _, env := range gc42Envs(t) {
		for _, c := range cells {
			for _, p := range gc42Paths(1000) {
				t.Run(env.name+"/"+c.name+"/"+p.name, func(t *testing.T) {
					table := gc42Name(t, "kl", env.name, c.name, p.name)
					gc42Table(t, env, table, c.ddl, c.seed)
					if env.role != "" && c.name == "partitioned" {
						for _, part := range []string{"_p1", "_p2"} {
							applyPGApplier(t, env.adminDSN, fmt.Sprintf(`ALTER TABLE %s%s OWNER TO %s;`, table, part, env.role))
						}
					}
					err := gc42Apply(t, env, p, table, gc42Tx(table, c.rows(table)...))
					gc42Assert(t, err, gc42Converge, "")
					if got := gc42State(t, env.adminDSN, table); got != c.wantState {
						t.Errorf("target = %q; want %q — one of the identical rows, not all of them", got, c.wantState)
					}
				})
			}
		}
	}
}

// TestGC42_ReplicaModeUnchanged pins that GC-42 leaves replica mode's apply
// semantics alone on a table WITH a deferrable key — the property the
// withdrawn role switch broke — on every path:
//
//   - a user BEFORE INSERT / BEFORE UPDATE trigger on the target does not
//     fire on a replicated row (it would rewrite the source's value);
//   - a parent DELETE whose children remain, and an orphaning child INSERT,
//     both apply (the Bug 164 FK bypass). Postgres refuses an FK onto a
//     deferrable key, so the child references the parent's immediate unique
//     column.
func TestGC42_ReplicaModeUnchanged(t *testing.T) {
	env := gc42Envs(t)[0]
	if !env.replica {
		t.Fatal("the first env must be the replica-mode one")
	}
	const trig = `CREATE TABLE %[1]s (id int, u int NOT NULL UNIQUE, v text, CONSTRAINT %[1]s_pk PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED);
		CREATE FUNCTION %[1]s_f() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.v := 'trigger'; RETURN NEW; END $$;
		CREATE TRIGGER %[1]s_t BEFORE INSERT OR UPDATE ON %[1]s FOR EACH ROW EXECUTE FUNCTION %[1]s_f();
		INSERT INTO %[1]s VALUES (1, 1, 'seed');`
	type cell struct {
		name, ddl string
		rows      func(base string) []ir.Change
		check     string // a query over the target that must return 1
	}
	cells := []cell{
		{"trigger_update", trig, func(base string) []ir.Change {
			return []ir.Change{ir.Update{Schema: "public", Table: base, Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(1), "u": int64(1), "v": "src"}}}
		}, `SELECT count(*) FROM %[1]s WHERE id = 1 AND v = 'src'`},
		{"trigger_insert", trig, func(base string) []ir.Change {
			return []ir.Change{ir.Insert{Schema: "public", Table: base, Row: ir.Row{"id": int64(2), "u": int64(2), "v": "src"}}}
		}, `SELECT count(*) FROM %[1]s WHERE id = 2 AND v = 'src'`},
		{"parent_delete", `CREATE TABLE %[1]s (id int, u int NOT NULL UNIQUE, CONSTRAINT %[1]s_pk PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED);
			CREATE TABLE %[1]s_c (id int PRIMARY KEY, pu int REFERENCES %[1]s (u));
			INSERT INTO %[1]s VALUES (1, 1); INSERT INTO %[1]s_c VALUES (1, 1);`, func(base string) []ir.Change {
			return []ir.Change{ir.Delete{Schema: "public", Table: base, Before: ir.Row{"id": int64(1)}}}
		}, `SELECT 1 - count(*) FROM %[1]s`},
		{"child_insert", `CREATE TABLE %[1]s_p (id int PRIMARY KEY);
			CREATE TABLE %[1]s (id int, u int NOT NULL UNIQUE, pid int REFERENCES %[1]s_p (id), CONSTRAINT %[1]s_pk PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED);`, func(base string) []ir.Change {
			return []ir.Change{ir.Insert{Schema: "public", Table: base, Row: ir.Row{"id": int64(1), "u": int64(1), "pid": int64(99)}}}
		}, `SELECT count(*) FROM %[1]s WHERE pid = 99`},
	}
	for _, c := range cells {
		for _, p := range gc42Paths(1000) {
			t.Run(c.name+"/"+p.name, func(t *testing.T) {
				base := gc42Name(t, "rm", c.name, p.name)
				applyPGApplier(t, env.adminDSN, fmt.Sprintf(c.ddl, base))
				err := gc42Apply(t, env, p, base, gc42Tx(base, c.rows(base)...))
				gc42Assert(t, err, gc42Converge, "")
				if got := pgScalarInt(t, env.adminDSN, fmt.Sprintf(c.check, base)); got != 1 {
					t.Errorf("%s: check %q = %d; want 1", c.name, fmt.Sprintf(c.check, base), got)
				}
			})
		}
	}
}
