//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestStreamer_MultiSchema_HierarchyPreflightsRefuse is the end-to-end pin
// for audit 2026-09-01 A2-2: the multi-schema `sync start` cold start must
// run the Bug 100 partition and item-68b inheritance refusals per selected
// schema, exactly as `migrate --include-schema` and the single-schema sync
// already did. Before the fix, observed on PG 16: no WARN, no refusal — the
// partitioned parent was flattened AND its leaf copied (duplicate rows),
// then the parent's copy froze at exit 0 while the leaf tracked CDC.
//
// Four cells, on one PG server (source database + same-server target):
//
//	(a) a schema holding a partitioned parent refuses with the partition
//	    sentinel, names the parent and the schema, and lands nothing;
//	(b) a schema holding an INHERITS parent refuses with the inheritance
//	    sentinel — both preflights, not one representative;
//	(c) a plain schema still cold-starts (the door does not over-refuse);
//	(d) `--exclude-table=<parent>` clears the door and the leaf copies as a
//	    heap — the recovery hint the refusal text gives, and the pin that
//	    the preflight sees the POST-filter schema on this path.
func TestStreamer_MultiSchema_HierarchyPreflightsRefuse(t *testing.T) {
	pgSource, pgTarget, cleanup := startPostgresLogicalMultiSchema(t)
	defer cleanup()

	applyPGDDL(t, pgSource, `
		CREATE SCHEMA sales;
		CREATE SCHEMA part;
		CREATE SCHEMA inh;
		CREATE TABLE sales.widgets (id BIGINT PRIMARY KEY, name TEXT NOT NULL);
		INSERT INTO sales.widgets (id, name) VALUES (1, 'a-one');
		CREATE TABLE part.ppart (id BIGINT NOT NULL, region INT NOT NULL, name TEXT, PRIMARY KEY (id, region))
			PARTITION BY LIST (region);
		CREATE TABLE part.ppart_1 PARTITION OF part.ppart FOR VALUES IN (1);
		INSERT INTO part.ppart (id, region, name) VALUES (1, 1, 'a');
		CREATE TABLE inh.ip (id BIGINT PRIMARY KEY, name TEXT);
		CREATE TABLE inh.ic (extra TEXT, PRIMARY KEY (id)) INHERITS (inh.ip);
		INSERT INTO inh.ip (id, name) VALUES (3, 'p3');
		INSERT INTO inh.ic (id, name, extra) VALUES (1, 'c', 'x');
	`)

	pgEng, _ := engines.Get("postgres")
	newStreamer := func(streamID string, schemas ...string) *Streamer {
		return &Streamer{
			Source:         pgEng,
			Target:         pgEng,
			SourceDSN:      pgSource,
			TargetDSN:      pgTarget,
			StreamID:       streamID,
			DatabaseFilter: DatabaseFilter{Include: schemas},
		}
	}

	// runRefusing runs the streamer to completion and returns its error;
	// a cold start that does NOT refuse would block in CDC, so the timeout
	// is the "no refusal" signal.
	runRefusing := func(t *testing.T, s *Streamer) error {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		err := s.Run(ctx)
		if err == nil || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Fatalf("multi-schema sync over %v did not refuse at cold start (err=%v)", s.DatabaseFilter.Include, err)
		}
		// The refusal fires inside the copy loop, after the spanning
		// snapshot's slot exists; Abandon drops it, but leave nothing to
		// chance for the next cell.
		dropSluiceSlots(t, pgSource)
		return err
	}
	// runCopies cold-starts the streamer, waits for one target table to hold
	// the expected count, then stops it.
	runCopies := func(t *testing.T, s *Streamer, schema, table string, want int) {
		t.Helper()
		streamCtx, streamCancel := context.WithCancel(context.Background())
		runErr := make(chan error, 1)
		go func() { runErr <- s.Run(streamCtx) }()
		if !waitForPGSchemaCount(t, pgTarget, schema, table, want, 60*time.Second) {
			streamCancel()
			<-runErr
			t.Fatalf("cold start over %v never copied %s.%s", s.DatabaseFilter.Include, schema, table)
		}
		streamCancel()
		select {
		case <-runErr:
		case <-time.After(30 * time.Second):
			t.Fatal("Streamer.Run did not return after cancel")
		}
		dropSluiceSlots(t, pgSource)
	}

	t.Run("partitioned parent refuses and lands nothing", func(t *testing.T) {
		err := runRefusing(t, newStreamer("multischema-part-refuse", "part"))
		if !errors.Is(err, errPartitionedTableRefused) {
			t.Fatalf("want errPartitionedTableRefused; got %v", err)
		}
		for _, want := range []string{`"ppart"`, `preflight database "part"`} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal does not carry %s: %v", want, err)
			}
		}
		if n := pgTablesInSchema(t, pgTarget, "part"); n != 0 {
			t.Errorf("refusal landed %d table(s) in target schema part; want 0 — the door must fire before this schema's copy", n)
		}
	})

	t.Run("inheritance parent refuses and lands nothing", func(t *testing.T) {
		err := runRefusing(t, newStreamer("multischema-inh-refuse", "inh"))
		if !errors.Is(err, errInheritanceTableRefused) {
			t.Fatalf("want errInheritanceTableRefused; got %v", err)
		}
		for _, want := range []string{`"ip"`, `preflight database "inh"`} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal does not carry %s: %v", want, err)
			}
		}
		if n := pgTablesInSchema(t, pgTarget, "inh"); n != 0 {
			t.Errorf("refusal landed %d table(s) in target schema inh; want 0", n)
		}
	})

	t.Run("a plain schema still cold-starts", func(t *testing.T) {
		runCopies(t, newStreamer("multischema-plain", "sales"), "sales", "widgets", 1)
	})

	t.Run("excluding the partitioned parent clears the door and copies the leaf", func(t *testing.T) {
		filter, err := migcore.NewTableFilter(nil, []string{"ppart"})
		if err != nil {
			t.Fatalf("NewTableFilter: %v", err)
		}
		s := newStreamer("multischema-part-excluded", "part")
		s.Filter = filter
		runCopies(t, s, "part", "ppart_1", 1)
		if n := pgTablesInSchema(t, pgTarget, "part"); n != 1 {
			t.Errorf("target schema part holds %d table(s); want exactly the leaf ppart_1 — the excluded parent must not be flattened onto the target", n)
		}
	})
}

// pgTablesInSchema counts the tables information_schema lists in one
// target schema (0 when the schema does not exist).
func pgTablesInSchema(t *testing.T, dsn, schema string) int {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(
		ctx,
		`SELECT count(*) FROM information_schema.tables WHERE table_schema = $1 AND table_type = 'BASE TABLE'`, schema,
	).Scan(&n); err != nil {
		t.Fatalf("count tables in %q: %v", schema, err)
	}
	return n
}
