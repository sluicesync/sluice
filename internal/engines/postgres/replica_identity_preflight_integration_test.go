//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Roadmap item 93 — the source-side replica-identity gate, ORACLED
// against a real Postgres.
//
// The classification this preflight makes is PostgreSQL's, not sluice's:
// which tables a publication with `pubupdate`/`pubdelete` will let the
// SOURCE APPLICATION keep updating. So the pin here does not encode an
// expected answer per shape and hope it matches — it asks the server.
// Each fixture table is put into a real publication, a real `UPDATE` is
// attempted against it inside a rolled-back transaction, and the set PG
// rejects is compared against the set sluice's preflight names. Any
// divergence in either direction fails: an over-refusal breaks working
// syncs, an under-refusal is the bug.
//
// The matrix is the shape family, not a representative — every
// relreplident setting (DEFAULT / USING INDEX / FULL / NOTHING) crossed
// with every index state that setting can be in (immediate PK,
// DEFERRABLE PK in both initial modes, DEFERRABLE PK plus an immediate
// UNIQUE, UNIQUE-only, partial/nullable UNIQUE, keyless).
//
// Note the deliberate asymmetry with the TARGET-side sibling
// (change_applier_deferrable_key_integration_test.go): a table with a
// DEFERRABLE primary key AND an immediate NOT NULL UNIQUE index is fine
// as a target (ON CONFLICT arbitrates on the unique index) and NOT fine
// as a source (REPLICA IDENTITY DEFAULT resolves to the primary key and
// nothing else). The oracle below is what proves that, and the refusal
// names the one-line USING INDEX fix for it.

package postgres

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// replicaIdentityFixtureDDL builds one table per shape in the family.
// Names are prefixed `ri_` so the oracle's table sweep can ignore
// anything else in the schema.
const replicaIdentityFixtureDDL = `
CREATE TABLE ri_imm_pk        (id BIGINT PRIMARY KEY, v TEXT);
CREATE TABLE ri_def_pk        (id BIGINT, v TEXT, CONSTRAINT ri_def_pk_pk PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED);
CREATE TABLE ri_defimm_pk     (id BIGINT, v TEXT, CONSTRAINT ri_defimm_pk_pk PRIMARY KEY (id) DEFERRABLE INITIALLY IMMEDIATE);
CREATE TABLE ri_def_pk_uk     (id BIGINT, v TEXT, k BIGINT NOT NULL,
                               CONSTRAINT ri_def_pk_uk_pk PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED,
                               CONSTRAINT ri_def_pk_uk_k  UNIQUE (k));
CREATE TABLE ri_def_pk_using  (id BIGINT, v TEXT, k BIGINT NOT NULL,
                               CONSTRAINT ri_def_pk_using_pk PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED,
                               CONSTRAINT ri_def_pk_using_k  UNIQUE (k));
ALTER TABLE ri_def_pk_using REPLICA IDENTITY USING INDEX ri_def_pk_using_k;
CREATE TABLE ri_def_pk_full   (id BIGINT, v TEXT, CONSTRAINT ri_def_pk_full_pk PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED);
ALTER TABLE ri_def_pk_full REPLICA IDENTITY FULL;
CREATE TABLE ri_uk_only       (id BIGINT NOT NULL, v TEXT, CONSTRAINT ri_uk_only_k UNIQUE (id));
CREATE TABLE ri_keyless       (id BIGINT, v TEXT);
CREATE TABLE ri_keyless_full  (id BIGINT, v TEXT);
ALTER TABLE ri_keyless_full REPLICA IDENTITY FULL;
CREATE TABLE ri_nothing       (id BIGINT PRIMARY KEY, v TEXT);
ALTER TABLE ri_nothing REPLICA IDENTITY NOTHING;
CREATE TABLE ri_nullable_uk   (id BIGINT, v TEXT, CONSTRAINT ri_nullable_uk_k UNIQUE (id));
CREATE TABLE ri_partial_uk    (id BIGINT NOT NULL, v TEXT);
CREATE UNIQUE INDEX ri_partial_uk_live ON ri_partial_uk (id) WHERE v IS NOT NULL;
`

// replicaIdentityFixtureTables is the fixture in a deterministic order,
// so the oracle sweep and the refusal comparison speak the same list.
var replicaIdentityFixtureTables = []string{
	"ri_def_pk",
	"ri_def_pk_full",
	"ri_def_pk_uk",
	"ri_def_pk_using",
	"ri_defimm_pk",
	"ri_imm_pk",
	"ri_keyless",
	"ri_keyless_full",
	"ri_nothing",
	"ri_nullable_uk",
	"ri_partial_uk",
	"ri_uk_only",
}

// pgRejectsPublishedUpdate is the ORACLE: with the table already a
// member of a publication that publishes updates, attempt a real UPDATE
// and report whether PostgreSQL refused it for want of a replica
// identity. The statement runs inside a transaction that is always
// rolled back, so the fixture is unchanged and the tables can be empty
// (the check fires when the plan is built, not per row).
func pgRejectsPublishedUpdate(t *testing.T, db *sql.DB, table string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin oracle probe for %q: %v", table, err)
	}
	defer func() { _ = tx.Rollback() }()

	_, err = tx.ExecContext(ctx, "UPDATE "+quoteIdent(table)+" SET v = v")
	if err == nil {
		return false
	}
	if strings.Contains(err.Error(), "does not have a replica identity") {
		return true
	}
	t.Fatalf("oracle probe on %q failed for an unrelated reason: %v", table, err)
	return false
}

// TestReplicaIdentityPreflight_OracleMatrix is the gate. sluice must
// refuse EXACTLY the tables PostgreSQL itself will stop accepting writes
// to once they publish updates — no more (over-refusal breaks working
// PG syncs), no fewer (under-refusal is the defect).
func TestReplicaIdentityPreflight_OracleMatrix(t *testing.T) {
	dsn, cleanup := newSharedPGDB(t, "ri_preflight_db")
	defer cleanup()
	applyPGApplier(t, dsn, replicaIdentityFixtureDDL)
	applyPGApplier(t, dsn, "CREATE PUBLICATION ri_oracle_pub FOR TABLE "+
		strings.Join(replicaIdentityFixtureTables, ", ")+";")

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Guard the oracle itself: the replica-identity check only fires on a
	// logically-logged relation, so a wal_level=replica server would make
	// EVERY probe report "accepted" and the comparison below vacuously
	// green in the wrong direction.
	var walLevel string
	if err := db.QueryRow("SHOW wal_level").Scan(&walLevel); err != nil {
		t.Fatalf("read wal_level: %v", err)
	}
	if walLevel != "logical" {
		t.Fatalf("wal_level = %q; the oracle needs 'logical' or PostgreSQL never runs the replica-identity check", walLevel)
	}

	var oracleRefuses []string
	for _, table := range replicaIdentityFixtureTables {
		if pgRejectsPublishedUpdate(t, db, table) {
			oracleRefuses = append(oracleRefuses, table)
		}
	}
	if len(oracleRefuses) == 0 {
		t.Fatal("the oracle rejected nothing — the fixture or the probe is broken, and any comparison below would be vacuous")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	reader := openReplicaIdentityReader(t, ctx, dsn)

	err = reader.PreflightReplicaIdentity(ctx, replicaIdentityFixtureTables)
	if err == nil {
		t.Fatalf("preflight accepted the whole fixture; PostgreSQL refuses writes to %v", oracleRefuses)
	}
	coded, ok := sluicecode.FromError(err)
	if !ok || coded.Code != sluicecode.CodeSourceReplicaIdentity {
		t.Fatalf("refusal is not %s: %v", sluicecode.CodeSourceReplicaIdentity, err)
	}
	if coded.Hint == "" {
		t.Error("refusal carries no remedy hint")
	}

	msg := err.Error()
	for _, table := range replicaIdentityFixtureTables {
		named := strings.Contains(msg, table+" (")
		refused := false
		for _, r := range oracleRefuses {
			if r == table {
				refused = true
				break
			}
		}
		switch {
		case refused && !named:
			t.Errorf("UNDER-REFUSAL: PostgreSQL refuses published UPDATEs on %q, but sluice's preflight does not name it:\n%s", table, msg)
		case !refused && named:
			t.Errorf("OVER-REFUSAL: PostgreSQL accepts published UPDATEs on %q, but sluice's preflight refuses over it:\n%s", table, msg)
		}
	}

	// The deferrable-PK-plus-UNIQUE cell is the one an operator is most
	// likely to think is fine (it IS fine on the target side), so its
	// refusal must carry the one-line fix rather than only "use FULL".
	if !strings.Contains(msg, "ri_def_pk_uk_k") {
		t.Errorf("the refusal for ri_def_pk_uk does not name the immediate UNIQUE index it could nominate:\n%s", msg)
	}
}

// TestReplicaIdentityPreflight_ScopeAndCleanSchema is the over-refusal
// guard in its own right: a schema whose tables can all publish must
// pass silently, and a table the stream never touches must never be
// refused over — `--exclude-table` is one of the remedies the refusal
// names, so it has to actually work.
func TestReplicaIdentityPreflight_ScopeAndCleanSchema(t *testing.T) {
	dsn, cleanup := newSharedPGDB(t, "ri_preflight_scope_db")
	defer cleanup()
	applyPGApplier(t, dsn, replicaIdentityFixtureDDL)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	reader := openReplicaIdentityReader(t, ctx, dsn)

	publishable := []string{"ri_imm_pk", "ri_def_pk_full", "ri_def_pk_using", "ri_keyless_full"}
	if err := reader.PreflightReplicaIdentity(ctx, publishable); err != nil {
		t.Fatalf("preflight refused a schema every table of which can publish: %v", err)
	}

	// One offender in scope, the rest excluded: the refusal names it and
	// stays silent about the tables it was not asked about.
	err := reader.PreflightReplicaIdentity(ctx, append(append([]string{}, publishable...), "ri_def_pk"))
	if err == nil {
		t.Fatal("preflight accepted a scope containing ri_def_pk")
	}
	if !strings.Contains(err.Error(), "ri_def_pk") {
		t.Errorf("refusal does not name the in-scope offender: %v", err)
	}
	for _, clean := range publishable {
		if strings.Contains(err.Error(), clean+" (") {
			t.Errorf("refusal names %q, which can publish: %v", clean, err)
		}
	}
	// …and an out-of-scope offender is not mentioned at all.
	if strings.Contains(err.Error(), "ri_keyless (") {
		t.Errorf("refusal names an out-of-scope table: %v", err)
	}
}

// openReplicaIdentityReader opens the source SchemaReader through the
// engine and returns it as the OPTIONAL preflighter surface, failing
// loudly if it is not implemented — an unimplemented surface is an INERT
// gate, which is the failure mode the pin exists to prevent.
func openReplicaIdentityReader(t *testing.T, ctx context.Context, dsn string) ir.ReplicaIdentityPreflighter {
	t.Helper()
	sr, err := (Engine{}).OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	t.Cleanup(func() {
		if c, ok := sr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	})
	pf, ok := sr.(ir.ReplicaIdentityPreflighter)
	if !ok {
		t.Fatal("the Postgres SchemaReader does not implement ir.ReplicaIdentityPreflighter — the preflight is inert")
	}
	return pf
}
