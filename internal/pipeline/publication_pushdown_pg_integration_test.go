//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0176 end-to-end through the real Streamer on real PG: a filtered
// sync with one classifier-ELIGIBLE predicate and one INELIGIBLE one
// (a uuid column — valid PG SQL everywhere, but OUTSIDE the proven
// equivalence envelope, so it must not be pushed) must
//
//   - emit the eligible predicate into the derived per-stream
//     publication (prqual recorded by PG),
//   - leave the ineligible table's member BARE (the A0-style fallback:
//     unfiltered server-side, client evaluator as the filter), and
//   - converge the target to EXACTLY the in-scope subset for BOTH
//     tables, through cold start and live CDC — proving the fallback
//     actually filters and push-down did not change delivered behavior.
//
// Named TestPublicationScope_* to ride the existing pipeline-rest-other
// CI shard (the -skip catch-all), like the ratchet suite.

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// pubQualFor reads the pg_get_expr row filter recorded for one member of
// a publication ("" = bare; fails the test when the member is absent).
func pubQualFor(t *testing.T, dsn, publication, table string) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	const q = `
		SELECT COALESCE(pg_get_expr(pr.prqual, pr.prrelid), '')
		FROM   pg_publication p
		JOIN   pg_publication_rel pr ON pr.prpubid = p.oid
		JOIN   pg_class c            ON c.oid     = pr.prrelid
		WHERE  p.pubname = $1 AND c.relname = $2`
	var qual string
	if err := db.QueryRowContext(context.Background(), q, publication, table).Scan(&qual); err != nil {
		t.Fatalf("read prqual for %s.%s: %v", publication, table, err)
	}
	return qual
}

func TestPublicationScope_PushdownEmitsAndFallsBack(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	applyDDL(t, sourceDSN, `
		CREATE TABLE orders (id int PRIMARY KEY, country varchar(8) NOT NULL);
		CREATE TABLE audits (id int PRIMARY KEY, tag uuid NOT NULL);
		ALTER TABLE orders REPLICA IDENTITY FULL;
		ALTER TABLE audits REPLICA IDENTITY FULL;
		INSERT INTO orders (id, country) VALUES (1, 'US'), (2, 'CA');
		INSERT INTO audits (id, tag) VALUES
			(1, '11111111-1111-1111-1111-111111111111'),
			(2, '22222222-2222-2222-2222-222222222222');
	`)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	streamer := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  "push-fb",
		SlotName:  "push_fb",
		Filter:    migcore.TableFilter{Include: []string{"orders", "audits"}},
		RowFilters: map[string]string{
			"orders": "country = 'US'",                               // eligible: varchar, default collation
			"audits": "tag = '11111111-1111-1111-1111-111111111111'", // INELIGIBLE: uuid is outside the envelope
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(ctx) }()

	// Cold start converges both tables to their in-scope subset.
	if !waitForRowCount(t, targetDSN, "orders", 1, 90*time.Second) {
		t.Fatal("filtered cold-start: orders did not converge to the 1 in-scope row")
	}
	if !waitForRowCount(t, targetDSN, "audits", 1, 30*time.Second) {
		t.Fatal("filtered cold-start: audits did not converge to the 1 in-scope row (client-side fallback broken?)")
	}

	// The derived per-stream publication (sluice_<stream-id>) carries the
	// eligible filter and leaves the fallback member bare.
	const pub = "sluice_push_fb"
	if qual := pubQualFor(t, sourceDSN, pub, "orders"); qual == "" {
		t.Error("orders member is bare — the eligible predicate was not pushed into the publication")
	}
	if qual := pubQualFor(t, sourceDSN, pub, "audits"); qual != "" {
		t.Errorf("audits member carries prqual %q — the uuid predicate is outside the proven envelope and must NOT be pushed", qual)
	}

	// Live CDC: in-scope rows land, out-of-scope rows are excluded on both
	// paths — server-side for orders, client-side for audits (whose
	// out-of-scope row DOES cross the wire; dropping it here is the
	// non-vacuous fallback proof).
	applyDDL(t, sourceDSN, `
		INSERT INTO orders (id, country) VALUES (3, 'US'), (4, 'CA');
		INSERT INTO audits (id, tag) VALUES
			(3, '11111111-1111-1111-1111-111111111111'),
			(4, '33333333-3333-3333-3333-333333333333');
	`)
	if !waitForRowCount(t, targetDSN, "orders", 2, 60*time.Second) {
		t.Fatal("CDC through the row-filtered publication never delivered the in-scope insert")
	}
	if !waitForRowCount(t, targetDSN, "audits", 2, 60*time.Second) {
		t.Fatal("CDC through the fallback (client-filtered) table never delivered the in-scope insert")
	}
	// Give the out-of-scope pair no place to hide: counts must HOLD.
	time.Sleep(2 * time.Second)
	assertCount := func(table string, want int) {
		db, err := sql.Open("pgx", targetDSN)
		if err != nil {
			t.Fatalf("open target: %v", err)
		}
		defer func() { _ = db.Close() }()
		var n int
		if err := db.QueryRowContext(context.Background(), fmt.Sprintf("SELECT count(*) FROM %s", table)).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if n != want {
			t.Errorf("%s = %d rows; want %d (an out-of-scope row leaked)", table, n, want)
		}
	}
	assertCount("orders", 2)
	assertCount("audits", 2)

	cancel()
	select {
	case err := <-runErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Run exited with error: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Streamer.Run did not return after cancel")
	}
}
