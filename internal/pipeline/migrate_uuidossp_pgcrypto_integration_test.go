//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0044 Tier 3 — extension-function defaults & generated
// expressions (uuid-ossp + pgcrypto) integration tests. Both
// extensions ship in the standard postgres contrib bundle, so the
// stock `postgres:16` image CI pre-pulls works as-is (same as the
// hstore / citext Tier 1 suite — no special image required).
//
// Scenarios mirror ADR-0044 §Testing 1–6 exactly:
//
//  1. PG → PG, --enable-pg-extension uuid-ossp, source+target have
//     uuid-ossp → DEFAULT uuid_generate_v4() round-trips.
//  2. PG → PG, flag set, target MISSING uuid-ossp → refused at
//     preflight (not a late apply error).
//  3. PG → PG, flag ABSENT, source uses uuid_generate_v4() → refused
//     at schema-read with the actionable message.
//  4. PG → PG, DEFAULT gen_random_uuid(), no flag → SUCCEEDS (core
//     function, never gated) — the core-vs-extension guard.
//  5. Cross-engine PG → MySQL: --enable-pg-extension uuid-ossp /
//     pgcrypto is refused up-front by the pre-existing ADR-0032
//     validateEnabledPGExtensions lossless-only policy (only
//     hstore/citext have cross-engine default translators). ADR-0044
//     §3 adds no new cross-engine translation or refusal arm — a
//     uuid v4 → MySQL v1 mapping would be a dishonest UUID-version
//     drift, and the ADR-0032 gate already refuses crypto. The
//     escape is --type-override / --expr-override per the existing
//     ADR-0032 message, or a PG target.
//  6. Generated column GENERATED ALWAYS AS (… via pgcrypto digest())
//     — same gate as defaults.

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"

	"github.com/testcontainers/testcontainers-go"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// startPostgresWithQuotedExtension is the hyphen-safe variant of
// startPostgresWithExtension: PG's CREATE EXTENSION requires the
// identifier double-quoted when the extension name contains a hyphen
// (`uuid-ossp`). Bare `CREATE EXTENSION uuid-ossp` is a syntax error.
func startPostgresWithQuotedExtension(t *testing.T, extensionName string, enableOnTarget bool) (sourceDSN, targetDSN string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	container, err := pgtc.Run(
		ctx,
		// Task #68: pre-baked PG image. See
		// pg_prebaked_integration_test.go for the full rationale.
		pgPrebakedImage,
		pgtc.WithDatabase("source_db"),
		pgtc.WithUsername("test"),
		pgtc.WithPassword("test"),
		pgtc.BasicWaitStrategies(),
		pgPrebakedWaitStrategy(),
	)
	if err != nil {
		t.Fatalf("start container: %v", err)
	}

	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}

	srcConn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		terminate()
		t.Fatalf("connection string: %v", err)
	}

	db, err := sql.Open("pgx", srcConn)
	if err != nil {
		terminate()
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.ExecContext(ctx, "CREATE DATABASE target_db"); err != nil {
		terminate()
		t.Fatalf("create target_db: %v", err)
	}

	tgtConn, err := buildPGDSN(srcConn, "target_db")
	if err != nil {
		terminate()
		t.Fatalf("build target DSN: %v", err)
	}

	// Double-quote the identifier so a hyphenated name (uuid-ossp) is
	// accepted.
	if _, err := db.ExecContext(ctx, `CREATE EXTENSION IF NOT EXISTS "`+extensionName+`"`); err != nil {
		terminate()
		t.Fatalf("CREATE EXTENSION %q on source: %v", extensionName, err)
	}

	if enableOnTarget {
		tgtDB, err := sql.Open("pgx", tgtConn)
		if err != nil {
			terminate()
			t.Fatalf("open target: %v", err)
		}
		defer func() { _ = tgtDB.Close() }()
		if _, err := tgtDB.ExecContext(ctx, `CREATE EXTENSION IF NOT EXISTS "`+extensionName+`"`); err != nil {
			terminate()
			t.Fatalf("CREATE EXTENSION %q on target: %v", extensionName, err)
		}
	}

	return srcConn, tgtConn, terminate
}

// Scenario 1: PG → PG, flag set, both sides have uuid-ossp →
// DEFAULT uuid_generate_v4() round-trips and produces real UUIDs.
func TestMigrate_PG_UUIDOSSP_DefaultRoundTrips(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresWithQuotedExtension(t, "uuid-ossp", true)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE widgets (
			id   uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
			name VARCHAR(64) NOT NULL
		);
		INSERT INTO widgets (name) VALUES ('alpha'), ('beta'), ('gamma');
	`
	applyPGDDL(t, sourceDSN, seedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	mig := &Migrator{
		Source:              pgEng,
		Target:              pgEng,
		SourceDSN:           sourceDSN,
		TargetDSN:           targetDSN,
		EnabledPGExtensions: []string{"uuid-ossp"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	tgtDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	var n int
	if err := tgtDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM widgets").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 3 {
		t.Errorf("target widgets rows = %d; want 3", n)
	}

	// The DEFAULT must be a real, working uuid_generate_v4() on the
	// target — inserting without an id must succeed and yield a UUID.
	if _, err := tgtDB.ExecContext(ctx,
		"INSERT INTO widgets (name) VALUES ('delta')"); err != nil {
		t.Fatalf("insert relying on uuid-ossp DEFAULT failed: %v", err)
	}
	var got string
	if err := tgtDB.QueryRowContext(ctx,
		"SELECT id::text FROM widgets WHERE name = 'delta'").Scan(&got); err != nil {
		t.Fatalf("select new uuid: %v", err)
	}
	if len(got) != 36 {
		t.Errorf("generated uuid = %q (len %d); want 36-char canonical UUID", got, len(got))
	}
}

// Scenario 2: flag set, target MISSING uuid-ossp → refused at
// PREFLIGHT (the validateAndPreflightExtensions target-presence
// check), NOT a late CREATE TABLE apply error.
func TestMigrate_PG_UUIDOSSP_TargetMissing_RefusedAtPreflight(t *testing.T) {
	// enableOnTarget=false → uuid-ossp on source only.
	sourceDSN, targetDSN, cleanup := startPostgresWithQuotedExtension(t, "uuid-ossp", false)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE widgets (
			id   uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
			name VARCHAR(64) NOT NULL
		);
	`
	applyPGDDL(t, sourceDSN, seedDDL)

	pgEng, _ := engines.Get("postgres")
	mig := &Migrator{
		Source:              pgEng,
		Target:              pgEng,
		SourceDSN:           sourceDSN,
		TargetDSN:           targetDSN,
		EnabledPGExtensions: []string{"uuid-ossp"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	err := mig.Run(ctx)
	if err == nil {
		t.Fatal("Migrator.Run = nil; want preflight refusal (target missing uuid-ossp)")
	}
	msg := err.Error()
	// The preflight refusal is the missing-extension wording from
	// extension_catalog.go::missingExtensionError — it names the
	// extension and the CREATE EXTENSION recovery, and crucially it is
	// NOT a Postgres parse error from a CREATE TABLE apply.
	if !strings.Contains(msg, "uuid-ossp") || !strings.Contains(msg, "CREATE EXTENSION") {
		t.Errorf("err = %v; want missing-extension preflight refusal naming uuid-ossp + CREATE EXTENSION", err)
	}
	if strings.Contains(msg, "syntax error") || strings.Contains(msg, "CREATE TABLE") {
		t.Errorf("err = %v; looks like a LATE apply error, want EARLY preflight refusal", err)
	}
}

// Scenario 3: flag ABSENT, source uses uuid_generate_v4() → refused
// at schema-read with the actionable message (names the function, the
// owning extension, --enable-pg-extension, --expr-override).
func TestMigrate_PG_UUIDOSSP_FlagAbsent_RefusedAtSchemaRead(t *testing.T) {
	sourceDSN, _, cleanup := startPostgresWithQuotedExtension(t, "uuid-ossp", false)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE widgets (
			id   uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
			name VARCHAR(64) NOT NULL
		);
	`
	applyPGDDL(t, sourceDSN, seedDDL)

	pgEng, _ := engines.Get("postgres")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sr, err := pgEng.OpenSchemaReader(ctx, sourceDSN)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(sr)

	// Deliberately do NOT EnableExtensions. The Tier-3 schema-read
	// gate must refuse.
	_, err = sr.ReadSchema(ctx)
	if err == nil {
		t.Fatal("ReadSchema = nil; want Tier-3 gate refusal (uuid-ossp not enabled)")
	}
	msg := err.Error()
	for _, frag := range []string{
		"uuid_generate_v4", "uuid-ossp",
		"--enable-pg-extension uuid-ossp", "--expr-override",
	} {
		if !strings.Contains(msg, frag) {
			t.Errorf("schema-read refusal missing %q; got: %s", frag, msg)
		}
	}
}

// Scenario 4: DEFAULT gen_random_uuid(), no flag → SUCCEEDS.
// gen_random_uuid() is core PostgreSQL 13+, NOT an extension
// function — the load-bearing core-vs-extension guard. This is the
// integration-level pin for the unit-level
// TestScanExtensionFunction_GenRandomUUIDNotGated.
func TestMigrate_PG_GenRandomUUID_NoFlag_Succeeds(t *testing.T) {
	// No extension needed at all — gen_random_uuid is core (pgcrypto
	// is NOT required for it on PG 13+). Use the plain helper.
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE widgets (
			id   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
			name VARCHAR(64) NOT NULL
		);
		INSERT INTO widgets (name) VALUES ('alpha'), ('beta');
	`
	applyPGDDL(t, sourceDSN, seedDDL)

	pgEng, _ := engines.Get("postgres")
	mig := &Migrator{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		// No EnabledPGExtensions — the gate must NOT fire on a core fn.
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run = %v; want SUCCESS (gen_random_uuid is core PG, never gated)", err)
	}

	tgtDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()
	var n int
	if err := tgtDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM widgets").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 2 {
		t.Errorf("target widgets rows = %d; want 2", n)
	}
}

// Scenario 5: cross-engine PG → MySQL. ADR-0044 §3 was rescoped:
// uuid-ossp and pgcrypto stay refused up-front by the pre-existing
// ADR-0032 validateEnabledPGExtensions lossless-only policy (only
// hstore/citext carry a cross-engine default translator). There is no
// new uuid-ossp → UUID() translation (a PG uuid_generate_v4 random/v4
// → MySQL UUID() time-based/v1 mapping would be a dishonest
// UUID-version drift) and no new cross-engine pgcrypto refusal arm —
// the ADR-0032 gate already refuses both, before schema-read. This
// pins the REAL behaviour against validateEnabledPGExtensions'
// message (target_schema.go).
func TestMigrate_PG_To_MySQL_UUIDOSSP_And_Pgcrypto_RefusedByCrossEnginePolicy(t *testing.T) {
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	pgEng, _ := engines.Get("postgres")
	mysqlEng, _ := engines.Get("mysql")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Part A — --enable-pg-extension uuid-ossp against a MySQL target
	// is refused by the ADR-0032 lossless-only gate before any schema
	// read (uuid-ossp is not in crossEngineDefaultTranslatedExtensions;
	// a uuid v4 → MySQL v1 mapping would be a dishonest version drift,
	// so ADR-0044 §3 deliberately adds no translator).
	pgSourceA, _, pgCleanupA := startPostgresWithQuotedExtension(t, "uuid-ossp", false)
	defer pgCleanupA()
	applyPGDDL(t, pgSourceA, `
		CREATE TABLE widgets (
			id   uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
			name VARCHAR(64) NOT NULL
		);
		INSERT INTO widgets (name) VALUES ('alpha'), ('beta');
	`)
	migA := &Migrator{
		Source:              pgEng,
		Target:              mysqlEng,
		SourceDSN:           pgSourceA,
		TargetDSN:           mysqlTarget,
		EnabledPGExtensions: []string{"uuid-ossp"},
	}
	errA := migA.Run(ctx)
	if errA == nil {
		t.Fatal("PG→MySQL --enable-pg-extension uuid-ossp = nil; " +
			"want ADR-0032 cross-engine policy refusal")
	}
	// The real validateEnabledPGExtensions message (target_schema.go):
	// names the extension, "cross-engine", the hstore/citext carve-out,
	// and the --type-override / PG-target escapes.
	for _, frag := range []string{
		"uuid-ossp", "cross-engine", "--type-override", "PG target",
	} {
		if !strings.Contains(errA.Error(), frag) {
			t.Errorf("uuid-ossp cross-engine refusal missing %q; got: %v", frag, errA)
		}
	}

	// Part B — --enable-pg-extension pgcrypto against a MySQL target is
	// refused by the same ADR-0032 gate (no new ADR-0044 §3 crypto
	// refusal arm; the lossless-only policy already covers it).
	pgSourceB, _, pgCleanupB := startPostgresWithQuotedExtension(t, "pgcrypto", false)
	defer pgCleanupB()
	applyPGDDL(t, pgSourceB, `
		CREATE TABLE secrets (
			id    BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			token TEXT NOT NULL DEFAULT crypt('seed', gen_salt('bf'))
		);
	`)
	migB := &Migrator{
		Source:              pgEng,
		Target:              mysqlEng,
		SourceDSN:           pgSourceB,
		TargetDSN:           mysqlTarget,
		EnabledPGExtensions: []string{"pgcrypto"},
	}
	errB := migB.Run(ctx)
	if errB == nil {
		t.Fatal("PG→MySQL --enable-pg-extension pgcrypto = nil; " +
			"want ADR-0032 cross-engine policy refusal")
	}
	for _, frag := range []string{
		"pgcrypto", "cross-engine", "--type-override", "PG target",
	} {
		if !strings.Contains(errB.Error(), frag) {
			t.Errorf("pgcrypto cross-engine refusal missing %q; got: %v", frag, errB)
		}
	}
}

// Scenario 6: generated column whose expression references a
// pgcrypto function is gated identically to a DEFAULT (flag-absent
// → schema-read refusal).
func TestMigrate_PG_Pgcrypto_GeneratedColumn_Gated(t *testing.T) {
	sourceDSN, _, cleanup := startPostgresWithQuotedExtension(t, "pgcrypto", false)
	defer cleanup()

	// A STORED generated column computing a digest of another column.
	const seedDDL = `
		CREATE TABLE docs (
			id     BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			body   TEXT NOT NULL,
			body_h TEXT GENERATED ALWAYS AS (encode(digest(body, 'sha256'), 'hex')) STORED
		);
	`
	applyPGDDL(t, sourceDSN, seedDDL)

	pgEng, _ := engines.Get("postgres")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sr, err := pgEng.OpenSchemaReader(ctx, sourceDSN)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(sr)

	// Flag absent → the generated-column path of the Tier-3 gate must
	// refuse just like the DEFAULT path (otherwise generated is a
	// silent bypass).
	_, err = sr.ReadSchema(ctx)
	if err == nil {
		t.Fatal("ReadSchema = nil; want generated-column Tier-3 gate refusal")
	}
	msg := err.Error()
	if !strings.Contains(msg, "digest") || !strings.Contains(msg, "pgcrypto") ||
		!strings.Contains(msg, "GENERATED") {
		t.Errorf("generated-col refusal = %v; want digest/pgcrypto/GENERATED", err)
	}

	// Sanity: with the flag, the same schema reads cleanly (the gate
	// only fires when the extension is NOT enabled).
	sr2, err := pgEng.OpenSchemaReader(ctx, sourceDSN)
	if err != nil {
		t.Fatalf("OpenSchemaReader (2): %v", err)
	}
	defer closeIf(sr2)
	if aware, ok := sr2.(ir.ExtensionAware); ok {
		if err := aware.EnableExtensions(ctx, []string{"pgcrypto"}); err != nil {
			t.Fatalf("EnableExtensions: %v", err)
		}
	}
	if _, err := sr2.ReadSchema(ctx); err != nil {
		t.Fatalf("ReadSchema with pgcrypto enabled = %v; want clean read", err)
	}
}
