//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/appliershared/streamlivenesstest"
	"sluicesync.dev/sluice/internal/ir"
)

// TestStreamLivenessStore_RoundTrip pins the Postgres liveness column
// (DRAINED-ADD-TABLE-RUNNING-STREAM-UNCHECKED) against a real server, from a
// control table a pre-column binary created (see
// [streamlivenesstest.RoundTrip]).
func TestStreamLivenessStore_RoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dsn, applier := tableSetsStoreFixture(ctx, t)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	streamlivenesstest.RoundTrip(ctx, t, applier.(ir.StreamLivenessStore), streamlivenesstest.Raw{
		Set: func(ms *int64) {
			if _, err := db.ExecContext(ctx, `UPDATE "public"."sluice_cdc_state" SET stream_alive_ms = $1 WHERE stream_id = 's1'`, ms); err != nil {
				t.Fatal(err)
			}
		},
		Drop: func() {
			if _, err := db.ExecContext(ctx, `DROP TABLE "public"."sluice_cdc_state"`); err != nil {
				t.Fatal(err)
			}
		},
	})
}

// TestStreamLivenessStore_ZoneIndependent: the stamp is the server's epoch
// milliseconds, so a database whose TimeZone is far from UTC (both
// directions) reads a just-written stamp as just written — the naive
// TIMESTAMP skew GC-39 fixed for updated_at cannot reach this column.
func TestStreamLivenessStore_ZoneIndependent(t *testing.T) {
	for _, zone := range []string{"Asia/Tokyo", "America/Los_Angeles"} {
		t.Run(zone, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			dsn, _ := tableSetsStoreFixture(ctx, t)
			sep := "?"
			if strings.Contains(dsn, "?") {
				sep = "&"
			}
			zoned := dsn + sep + "timezone=" + zone
			// The fixture really runs under the zone (a DSN key the driver
			// ignored would make this pin vacuous).
			probe, err := sql.Open("pgx", zoned)
			if err != nil {
				t.Fatal(err)
			}
			var got string
			if err := probe.QueryRowContext(ctx, "SHOW TimeZone").Scan(&got); err != nil || got != zone {
				t.Fatalf("session TimeZone = %q, %v; want %q", got, err, zone)
			}
			_ = probe.Close()
			applier, err := Engine{}.OpenChangeApplier(ctx, zoned)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = applier.(interface{ Close() error }).Close() }()
			store := applier.(ir.StreamLivenessStore)
			if _, err := store.StampStreamAlive(ctx, "s1"); err != nil {
				t.Fatal(err)
			}
			l, err := store.ReadStreamLiveness(ctx, "s1")
			if err != nil {
				t.Fatal(err)
			}
			if age := l.Age(); age < -5*time.Second || age > 5*time.Second || l.Stopped || !l.Recorded {
				t.Fatalf("a stamp just written under TimeZone=%s reads %v old (%+v)", zone, age, l)
			}
			if d := time.Since(l.Now); d < -time.Minute || d > time.Minute {
				t.Fatalf("server now %v is %v from this host's under TimeZone=%s", l.Now, d, zone)
			}
		})
	}
}

// TestApplierPinsReadCommitted (F3): on a database whose default isolation
// is REPEATABLE READ, the applier's sessions still run READ COMMITTED, so an
// apply transaction's position upsert does not fail with 40001 when the
// stream's liveness stamp updated the same row after the transaction began.
// The control session (a plain connection to the same DSN) shows the
// database setting is in force.
func TestApplierPinsReadCommitted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dsn, _ := tableSetsStoreFixture(ctx, t)
	applyPGApplier(t, dsn, "ALTER DATABASE target_db SET default_transaction_isolation = 'repeatable read'")
	t.Cleanup(func() { applyPGApplier(t, dsn, "ALTER DATABASE target_db RESET default_transaction_isolation") })
	plain, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = plain.Close() }()
	var level string
	if err := plain.QueryRowContext(ctx, "SHOW transaction_isolation").Scan(&level); err != nil || level != "repeatable read" {
		t.Fatalf("RIG: a plain session on the database runs %q (%v); want repeatable read", level, err)
	}
	applier, err := Engine{}.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = applier.(interface{ Close() error }).Close() }()
	if err := applier.(*ChangeApplier).db.QueryRowContext(ctx, "SHOW transaction_isolation").Scan(&level); err != nil || level != "read committed" {
		t.Fatalf("the applier's session runs %q (%v); want read committed", level, err)
	}
}

// TestPositionWriteStampsLiveness (F1): a position write stamps the liveness
// column in its own statement, overwriting a stopped marker, once the applier
// knows the column is there; on a control table without the column the same
// write works and names no such column (a role that could not add it keeps
// streaming).
func TestPositionWriteStampsLiveness(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dsn, applier := tableSetsStoreFixture(ctx, t)
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' "+
		"AND table_name = 'sluice_cdc_state' AND column_name = 'stream_alive_ms'").Scan(&n); err != nil || n != 0 {
		t.Fatalf("RIG: the legacy fixture already has the liveness column (%d, %v)", n, err)
	}
	if err := applier.(ir.PositionWriter).WritePosition(ctx, "s1", ir.Position{Engine: "postgres", Token: "tok-2"}); err != nil {
		t.Fatalf("a position write on a control table without the liveness column: %v", err)
	}
	store := applier.(ir.StreamLivenessStore)
	if _, err := store.StampStreamAlive(ctx, "s1"); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordStreamStopped(ctx, "s1"); err != nil {
		t.Fatal(err)
	}
	if err := applier.(ir.PositionWriter).WritePosition(ctx, "s1", ir.Position{Engine: "postgres", Token: "tok-3"}); err != nil {
		t.Fatal(err)
	}
	l, err := store.ReadStreamLiveness(ctx, "s1")
	if err != nil || l.Stopped || !l.Recorded || l.Age() > time.Minute || l.Age() < -time.Minute {
		t.Fatalf("after a position write over a stopped marker: %+v, %v; want a fresh running stamp", l, err)
	}
}
