//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-42 — the PATH pin: the triage repro through a real PG→PG `sync`, so the
// before-image the applier sees is the one the real CDC reader narrows
// (cdc_reader.go, Bug 92), not a hand-built event. The engine-level matrix
// (internal/engines/postgres/change_applier_gc42_integration_test.go) pins
// every path × constraint kind × privilege mode against the applier directly.
//
// Pre-fix, on postgres:14 and 16, default lanes and --apply-concurrency 1
// both ended with the target at {(3,c)} while the source held {(2,a),(3,c)},
// and the stream stayed up, healthy, at exit 0. Post-fix the stream stops
// with a marked refusal and the target never reaches the lost-row state.
//
// SLUICE_TEST_PG_IMAGE points both containers at another Postgres (the triage
// ran postgres:14 and 16); unset, the pre-baked PG 16 image.

package pipeline

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

const gc42SeedDDL = `
CREATE TABLE s (id int, u text NOT NULL UNIQUE, CONSTRAINT s_pk PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED);
ALTER TABLE s REPLICA IDENTITY FULL;
INSERT INTO s VALUES (1,'a'),(2,'b'),(3,'c');
`

const gc42SourceTx = `BEGIN; UPDATE s SET id = 2 WHERE id = 1; DELETE FROM s WHERE u = 'b'; COMMIT;`

func TestGC42_DeferrablePKRepro_PGToPG(t *testing.T) {
	image := os.Getenv("SLUICE_TEST_PG_IMAGE")
	if image == "" {
		image = pgPrebakedImage
	}
	// batch 1000 is what the CLI's --apply-batch-size=auto resolves to on
	// Postgres; the Streamer's zero value would be the per-change path.
	for _, c := range []struct {
		name        string
		concurrency int
		batch       int
		marker      string
	}{
		// The test role is a superuser, so every path applies in replica
		// mode, where the deferred check is off: the key change commits
		// (alone, as a lane barrier or per change) and the DELETE that
		// follows reaches the shared key on every path.
		{"default-lanes", 0, 1000, "KEY-SCOPED-WRITE-MATCHED-MULTIPLE-ROWS"},
		{"apply-concurrency-1", 1, 1000, "KEY-SCOPED-WRITE-MATCHED-MULTIPLE-ROWS"},
		{"per-change", 1, 1, "KEY-SCOPED-WRITE-MATCHED-MULTIPLE-ROWS"},
	} {
		t.Run(c.name, func(t *testing.T) {
			sourceDSN, targetDSN, cleanup := startPostgresLogicalImage(t, image, 8)
			defer cleanup()
			applyDDL(t, sourceDSN, gc42SeedDDL)

			pgEng, ok := engines.Get("postgres")
			if !ok {
				t.Fatal("postgres engine not registered")
			}
			s := &Streamer{
				Source:           pgEng,
				Target:           pgEng,
				SourceDSN:        sourceDSN,
				TargetDSN:        targetDSN,
				StreamID:         "gc42-" + c.name,
				SlotName:         strings.ReplaceAll("gc42_"+c.name, "-", "_"),
				ApplyConcurrency: c.concurrency,
				ApplyBatchSize:   c.batch,
			}
			ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
			defer cancel()
			runErr := make(chan error, 1)
			go func() { runErr <- s.Run(ctx) }()

			if !waitForRowCount(t, targetDSN, "s", 3, 90*time.Second) {
				t.Fatal("cold start never delivered the seed rows")
			}
			applyDDL(t, sourceDSN, gc42SourceTx)

			select {
			case err := <-runErr:
				if err == nil {
					t.Fatal("the stream exited cleanly after a change it cannot apply faithfully")
				}
				if !strings.Contains(err.Error(), c.marker) {
					t.Fatalf("the stream stopped, but not with %s: %v", c.marker, err)
				}
				t.Logf("refused: %v", err)
			case <-time.After(60 * time.Second):
				t.Fatalf("the stream is still running 60s after the source transaction; target = %q (the pre-fix silent loss is %q)",
					gc42TargetState(t, targetDSN), "(3,c)")
			}
			if got := gc42TargetState(t, targetDSN); got == "(3,c)" {
				t.Fatalf("target lost (2,a): %q", got)
			}
		})
	}
}

// gc42TargetState reads s off the target over its own connection.
func gc42TargetState(t *testing.T, dsn string) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()
	var out string
	if err := db.QueryRow(`SELECT COALESCE(string_agg(x::text, ' ' ORDER BY x::text), '') FROM s x`).Scan(&out); err != nil {
		t.Fatalf("read target: %v", err)
	}
	return out
}
