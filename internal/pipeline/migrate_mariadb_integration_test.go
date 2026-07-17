//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Cross-engine integration tests for the mariadb flavor (roadmap item
// 73 Phase 1): migrate mariadb → Postgres and Postgres → mariadb, with
// row-value assertions and a `verify` pass in each direction. The
// same-family legs (mariadb ↔ MySQL 8, both LTS lines, defaults
// matrix, backup/restore, refusal shapes) live in the engine package
// (internal/engines/mysql/flavor_mariadb_integration_test.go); this
// file owns the cross-engine product use case per the testing layout.
//
// The mariadb → PG corpus deliberately has NO JSON column: MariaDB
// JSON is a LONGTEXT alias whose auto `json_valid` CHECK flows to the
// IR and fails loudly at a PG target (json_valid does not exist there)
// — JSON-identity recovery is item 73 Phase 2, and the loud failure is
// the intended Phase-1 posture.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"sluicesync.dev/sluice/internal/engines"

	// Both engines must be registered for engines.Get to find them.
	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// startMariaDB boots a mariadb:11.4 container with source_db/target_db
// databases, mirroring startMySQL's contract. The wait strategy is a
// SQL round-trip on the mapped port (the entrypoint's init phase runs
// a socket-only temp server, so port readiness + SQL success is the
// definitive signal).
func startMariaDB(t *testing.T) (sourceDSN, targetDSN string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "mariadb:11.4",
			Env: map[string]string{
				"MARIADB_ROOT_PASSWORD": "rootpw",
				"MARIADB_DATABASE":      "source_db",
			},
			ExposedPorts: []string{"3306/tcp"},
			WaitingFor: wait.ForSQL("3306/tcp", "mysql", func(host string, port network.Port) string {
				return fmt.Sprintf("root:rootpw@tcp(%s:%s)/source_db", host, port.Port())
			}).WithStartupTimeout(4 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("boot mariadb: %v", err)
	}
	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}

	host, err := container.Host(ctx)
	if err != nil {
		terminate()
		t.Fatalf("mariadb host: %v", err)
	}
	port, err := container.MappedPort(ctx, "3306/tcp")
	if err != nil {
		terminate()
		t.Fatalf("mariadb port: %v", err)
	}

	sourceDSN = fmt.Sprintf("root:rootpw@tcp(%s:%s)/source_db?parseTime=true", host, port.Port())
	targetDSN = fmt.Sprintf("root:rootpw@tcp(%s:%s)/target_db?parseTime=true", host, port.Port())

	db, err := sql.Open("mysql", sourceDSN)
	if err != nil {
		terminate()
		t.Fatalf("open mariadb: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "CREATE DATABASE target_db"); err != nil {
		terminate()
		t.Fatalf("create target_db: %v", err)
	}
	return sourceDSN, targetDSN, terminate
}

// TestMigrate_MariaDBToPostgres is the mariadb-source cross-engine
// validation leg: the same shape TestMigrate_MySQLToPostgres pins for
// vanilla MySQL, driven through the mariadb flavor — including the
// COLUMN_DEFAULT normalization (quoted literals, current_timestamp())
// crossing into PG DDL, TINYINT(1) → BOOLEAN, ENUM/SET, and unsigned
// integers — followed by a clean `verify` (count depth) over the pair.
func TestMigrate_MariaDBToPostgres(t *testing.T) {
	mariadbSource, _, mariadbCleanup := startMariaDB(t)
	defer mariadbCleanup()

	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	const seedDDL = `
		CREATE TABLE users (
			id         BIGINT          NOT NULL AUTO_INCREMENT,
			email      VARCHAR(255)    NOT NULL,
			active     TINYINT(1)      NOT NULL DEFAULT 1,
			nickname   VARCHAR(40)     DEFAULT 'anon',
			created_at TIMESTAMP(0)    NOT NULL DEFAULT CURRENT_TIMESTAMP,
			score      BIGINT UNSIGNED NOT NULL DEFAULT 0,
			role       ENUM('admin','user','guest') NOT NULL DEFAULT 'user',
			tags       SET('news','sports','tech')  NOT NULL DEFAULT 'news',
			PRIMARY KEY (id),
			UNIQUE KEY users_email_unique (email)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		CREATE TABLE posts (
			id      BIGINT NOT NULL AUTO_INCREMENT,
			user_id BIGINT NOT NULL,
			body    TEXT   NOT NULL,
			PRIMARY KEY (id),
			KEY posts_user_id_idx (user_id),
			CONSTRAINT posts_user_id_fk FOREIGN KEY (user_id)
				REFERENCES users (id) ON DELETE CASCADE
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		INSERT INTO users (email, active, score, role, tags) VALUES
			('alice@example.com', 1, 100, 'admin', 'news,tech'),
			('bob@example.com',   0, 42,  'user',  '');

		INSERT INTO posts (user_id, body) VALUES
			(1, 'first post'),
			(1, 'héllo 世界'),
			(2, 'a post by bob');
	`
	applyMySQLDDL(t, mariadbSource, seedDDL)

	mariadbEng, ok := engines.Get("mariadb")
	if !ok {
		t.Fatal("mariadb engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	mig := &Migrator{
		Source:    mariadbEng,
		Target:    pgEng,
		SourceDSN: mariadbSource,
		TargetDSN: pgTarget,
	}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("migrate mariadb → postgres: %v", err)
	}

	// Row-value spot checks on the PG target: booleans landed as
	// booleans, the multibyte body survived, the mariadb-normalized
	// string default carried.
	pg, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("open pg: %v", err)
	}
	defer func() { _ = pg.Close() }()

	var active bool
	if err := pg.QueryRowContext(ctx, `SELECT active FROM users WHERE email = 'bob@example.com'`).Scan(&active); err != nil {
		t.Fatalf("read bob.active: %v", err)
	}
	if active {
		t.Error("bob.active = true; want false (TINYINT(1) 0 → BOOLEAN false)")
	}
	var body string
	if err := pg.QueryRowContext(ctx, `SELECT body FROM posts WHERE id = 2`).Scan(&body); err != nil {
		t.Fatalf("read post body: %v", err)
	}
	if body != "héllo 世界" {
		t.Errorf("post body = %q; want the multibyte original", body)
	}
	var nickDefault string
	if err := pg.QueryRowContext(ctx, `
		SELECT column_default FROM information_schema.columns
		WHERE table_name = 'users' AND column_name = 'nickname'`).Scan(&nickDefault); err != nil {
		t.Fatalf("read nickname default: %v", err)
	}
	if !strings.Contains(nickDefault, "anon") || strings.Contains(nickDefault, "''anon''") {
		t.Errorf("nickname PG default = %q; want the unquoted-normalized 'anon' literal", nickDefault)
	}

	// verify (count depth), mariadb as SOURCE role.
	var buf strings.Builder
	v := &Verifier{Source: mariadbEng, Target: pgEng, SourceDSN: mariadbSource, TargetDSN: pgTarget, Out: &buf}
	res, err := v.Run(ctx)
	if err != nil {
		t.Fatalf("verify mariadb → postgres: %v\n%s", err, buf.String())
	}
	if res.HasMismatch() {
		t.Fatalf("verify mariadb → postgres found mismatches:\n%s", buf.String())
	}
}

// TestMigrate_PostgresToMariaDB is the mariadb-target cross-engine
// validation leg: the PG corpus (uuid, jsonb, timestamptz, text) lands
// on a real MariaDB 11.4 through the mariadb flavor — the migrate-state
// store's VALUES()-spelling upsert is on this path's critical line (the
// row-alias form is Error 1064 on MariaDB) — followed by a clean
// `verify` with mariadb in the TARGET role (the probe's leg-5c wall).
func TestMigrate_PostgresToMariaDB(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()

	_, mariadbTarget, mariadbCleanup := startMariaDB(t)
	defer mariadbCleanup()

	const seedDDL = `
		CREATE TABLE accounts (
			id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			ext_id     UUID         NOT NULL,
			email      VARCHAR(255) NOT NULL UNIQUE,
			meta       JSONB,
			note       TEXT,
			created_at TIMESTAMPTZ  NOT NULL DEFAULT now()
		);
		INSERT INTO accounts (ext_id, email, meta, note) VALUES
			('123e4567-e89b-12d3-a456-426614174000', 'a@example.com', '{"k": "v"}', 'héllo 世界'),
			('223e4567-e89b-12d3-a456-426614174000', 'b@example.com', NULL, NULL);
	`
	applyPGDDL(t, pgSource, seedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	mariadbEng, ok := engines.Get("mariadb")
	if !ok {
		t.Fatal("mariadb engine not registered")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	mig := &Migrator{
		Source:    pgEng,
		Target:    mariadbEng,
		SourceDSN: pgSource,
		TargetDSN: mariadbTarget,
	}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("migrate postgres → mariadb: %v", err)
	}

	mdb, err := sql.Open("mysql", mariadbTarget)
	if err != nil {
		t.Fatalf("open mariadb: %v", err)
	}
	defer func() { _ = mdb.Close() }()

	var n int
	if err := mdb.QueryRowContext(ctx, "SELECT COUNT(*) FROM accounts").Scan(&n); err != nil {
		t.Fatalf("count accounts: %v", err)
	}
	if n != 2 {
		t.Errorf("accounts rows = %d; want 2", n)
	}
	var ext, note string
	if err := mdb.QueryRowContext(ctx, "SELECT ext_id, note FROM accounts WHERE email = 'a@example.com'").Scan(&ext, &note); err != nil {
		t.Fatalf("read account: %v", err)
	}
	if ext != "123e4567-e89b-12d3-a456-426614174000" {
		t.Errorf("ext_id = %q; want the canonical uuid string (uuid → CHAR(36))", ext)
	}
	if note != "héllo 世界" {
		t.Errorf("note = %q; want the multibyte original", note)
	}

	// The migrate-state control tables were written on the mariadb
	// target via the VALUES() spelling — assert the header row exists
	// and completed (the row-alias spelling would have 1064'd the very
	// first write, killing the migrate before any data moved).
	var phase string
	if err := mdb.QueryRowContext(ctx, "SELECT phase FROM sluice_migrate_state LIMIT 1").Scan(&phase); err != nil {
		t.Fatalf("read migrate-state header: %v", err)
	}
	if phase != "complete" {
		t.Errorf("migrate-state phase = %q; want complete", phase)
	}

	// verify (count depth), mariadb as TARGET role.
	var buf strings.Builder
	v := &Verifier{Source: pgEng, Target: mariadbEng, SourceDSN: pgSource, TargetDSN: mariadbTarget, Out: &buf}
	res, err := v.Run(ctx)
	if err != nil {
		t.Fatalf("verify postgres → mariadb: %v\n%s", err, buf.String())
	}
	if res.HasMismatch() {
		t.Fatalf("verify postgres → mariadb found mismatches:\n%s", buf.String())
	}
}
