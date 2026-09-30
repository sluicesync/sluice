//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestConnectHint_DoesNotExist_RealServers pins GC-40 (b) (sluice-testing
// Bug 292) through the real drivers and the real Streamer: the connect-phase
// SLUICE-E-CONNECT-DATABASE-MISSING code is given to a missing DATABASE and
// to nothing else that says "does not exist". The unit matrix
// (TestDoesNotExistClassification_EveryObjectKind) grades the classifier;
// these cells grade the premise it rests on — that the driver error reaches
// the hint layer with its structure intact (a Postgres *pgconn.PgError, a
// MySQL error the engine marked) rather than flattened to text.
func TestConnectHint_DoesNotExist_RealServers(t *testing.T) {
	run := func(t *testing.T, s *Streamer) error {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		err := s.Run(ctx)
		if err == nil {
			t.Fatal("Run returned nil; want a connect-phase failure")
		}
		return err
	}
	code := func(err error) sluicecode.Code {
		if ce, ok := sluicecode.FromError(err); ok {
			return ce.Code
		}
		return ""
	}

	t.Run("postgres", func(t *testing.T) {
		sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
		defer cleanup()
		pgEng, ok := engines.Get("postgres")
		if !ok {
			t.Fatal("postgres engine not registered")
		}
		applyDDL(t, sourceDSN, `CREATE TABLE items (id BIGINT PRIMARY KEY, v TEXT); INSERT INTO items VALUES (1, 'a');`)
		applyDDL(t, targetDSN, `CREATE TABLE items (id BIGINT PRIMARY KEY, v TEXT);`)

		// Bug 292's shape: --schema-already-applied against a target that
		// has the user table but no sluice_cdc_state. The database exists;
		// the error must say what is actually missing.
		err := run(t, &Streamer{
			Source: pgEng, Target: pgEng, SourceDSN: sourceDSN, TargetDSN: targetDSN,
			StreamID: "gc40b-missing-ctl", SchemaAlreadyApplied: true,
		})
		if got := code(err); got == sluicecode.CodeConnectDatabaseMissing {
			t.Errorf("a missing control table was coded %s:\n%v", got, err)
		}
		if strings.Contains(err.Error(), "verify the database name") || !strings.Contains(err.Error(), `"sluice_cdc_state" does not exist`) {
			t.Errorf("error does not name the missing control table (or still says to verify the database name):\n%v", err)
		}

		// The control: a DSN naming a database that is really absent.
		missingDB, derr := buildPGDSN(targetDSN, "gc40b_no_such_db")
		if derr != nil {
			t.Fatal(derr)
		}
		err = run(t, &Streamer{
			Source: pgEng, Target: pgEng, SourceDSN: sourceDSN, TargetDSN: missingDB,
			StreamID: "gc40b-missing-db",
		})
		if got := code(err); got != sluicecode.CodeConnectDatabaseMissing {
			t.Errorf("a missing Postgres database was coded %q; want %s:\n%v", got, sluicecode.CodeConnectDatabaseMissing, err)
		}
	})

	t.Run("mysql", func(t *testing.T) {
		sourceDSN, targetDSN, cleanup := startMySQL(t)
		defer cleanup()
		myEng, ok := engines.Get("mysql")
		if !ok {
			t.Fatal("mysql engine not registered")
		}
		cfg, err := gomysql.ParseDSN(targetDSN)
		if err != nil {
			t.Fatal(err)
		}
		cfg.DBName = "gc40b_no_such_db"
		err = run(t, &Streamer{
			Source: myEng, Target: myEng, SourceDSN: sourceDSN, TargetDSN: cfg.FormatDSN(),
			StreamID: "gc40b-missing-db",
		})
		// MySQL's "Unknown database" never matched the old substring, so
		// before GC-40 (b) this carried no code at all.
		if got := code(err); got != sluicecode.CodeConnectDatabaseMissing {
			t.Errorf("a missing MySQL database (errno 1049) was coded %q; want %s:\n%v", got, sluicecode.CodeConnectDatabaseMissing, err)
		}
	})
}
