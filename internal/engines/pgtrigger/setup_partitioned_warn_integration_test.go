//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

// The setup-time partitioned-parent advisory, graded in BOTH directions
// against a real PostgreSQL (audit SLP-5).
//
// Both cells are load-bearing and for different reasons. A probe that never
// fires is the gap this closes; a probe that fires on every healthy install is
// noise on every stream, which is how operators learn to ignore warnings.
//
// The premise is measured here rather than assumed: the test asserts that
// PostgreSQL actually CLONES the capture trigger onto the partitions and that
// the change log records the PARTITION's name. If PG ever stopped cloning, the
// advisory would be describing something that no longer happens, and this
// fails rather than quietly passing on the WARN alone.
// queryPGScalar reads a single integer from the source, for the premise cells
// that must observe PostgreSQL's own behaviour rather than sluice's report of it.
func queryPGScalar(t *testing.T, dsn string, out *int, query string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if err := db.QueryRow(query).Scan(out); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
}

func TestSetup_PartitionedParentWarns(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	applyPGSQL(t, dsn, `
		CREATE TABLE events (
			id bigint NOT NULL, region text NOT NULL, payload text,
			PRIMARY KEY (id, region)
		) PARTITION BY LIST (region);
		CREATE TABLE events_us PARTITION OF events FOR VALUES IN ('us');
		CREATE TABLE events_eu PARTITION OF events FOR VALUES IN ('eu');
		CREATE TABLE plain (id int PRIMARY KEY, note text);`)

	t.Run("a partitioned parent warns, naming the marker and the parent", func(t *testing.T) {
		var setupErr error
		out := captureWarnLogs(t, func() {
			_, setupErr = Setup(ctx, dsn, SetupOptions{Tables: []string{"events"}})
		})
		if setupErr != nil {
			t.Fatalf("Setup refused a partitioned parent; it is supposed to WARN and proceed, because "+
				"excluding the parent and copying the partitions as heaps is a supported route: %v", setupErr)
		}
		for _, want := range []string{partitionedParentMarker, "events", "--exclude-table"} {
			if !strings.Contains(out, want) {
				t.Errorf("the advisory does not name %q — an operator cannot act on it:\n%s", want, out)
			}
		}
	})

	t.Run("the premise: PG clones the trigger and the log records the PARTITION", func(t *testing.T) {
		// If this stops holding, the advisory's text is wrong and the WARN
		// above would keep passing while describing a behaviour PG no longer
		// has.
		var clones int
		queryPGScalar(t, dsn, &clones,
			`SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
			  WHERE t.tgname = 'sluice_capture' AND c.relname IN ('events_us','events_eu')`)
		if clones != 2 {
			t.Fatalf("PostgreSQL cloned the capture trigger onto %d partitions, want 2 — the advisory "+
				"describes cloning that is no longer happening", clones)
		}

		applyPGSQL(t, dsn, `INSERT INTO events VALUES (1,'us','a'),(2,'eu','b')`)
		var partitionNamed int
		queryPGScalar(t, dsn, &partitionNamed,
			`SELECT count(*) FROM sluice_change_log WHERE table_name IN ('events_us','events_eu')`)
		if partitionNamed != 2 {
			t.Fatalf("the change log recorded %d rows under partition names, want 2 — if it now records "+
				"the PARENT's name, this whole advisory is obsolete and should be deleted", partitionNamed)
		}
	})

	t.Run("a plain table does NOT warn", func(t *testing.T) {
		// The no-false-positive floor. A probe that warns on every healthy
		// install is noise, and noise is how a real warning gets ignored.
		var setupErr error
		out := captureWarnLogs(t, func() {
			_, setupErr = Setup(ctx, dsn, SetupOptions{Tables: []string{"plain"}})
		})
		if setupErr != nil {
			t.Fatalf("Setup: %v", setupErr)
		}
		if strings.Contains(out, partitionedParentMarker) {
			t.Errorf("the partitioned-parent advisory fired on an ordinary table:\n%s", out)
		}
	})
}
