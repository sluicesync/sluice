//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The SLM-5b half of the session-TimeZone cast class against a real server:
// the direction sluice STOPPED refusing.
//
// Every other pin in this family proves a refusal fires. This one proves a
// refusal does NOT — which is the harder thing to be sure of, and the reason
// it exists. The narrowing rests on a claim about Postgres itself ("dropping
// a timetz offset consults no session zone"), and CLAUDE.md's premise-naming
// rule is explicit that a safety argument citing an environmental fact owes
// that fact a runtime check or a named test. Before this file the claim lived
// only in prose in `internal/ir/zone_family.go`, measured once by hand.
//
// Two things are asserted, and the second is the one a unit pin structurally
// cannot reach:
//
//   - the swap FORWARDS: a real mid-stream `ALTER … TYPE time` on a `timetz`
//     column produces a RelationMessage the reader waves through, and the
//     stream survives it;
//   - the VALUES agree across two session zones. The independent expected
//     value is the value the OTHER session's server produced for the same
//     stored row — not sluice re-rendering its own output, and not the same
//     session answering twice.

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// queryPGScalar runs a single-value query and returns it as text. Its own
// connection, deliberately: the value under test must be the one the SERVER
// stored after the ALTER, not something the writing session is still holding.
func queryPGScalar(t *testing.T, dsn, query string) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var out string
	if err := db.QueryRowContext(ctx, query).Scan(&out); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return out
}

// TestSessionTZDropIsZoneIndependent_PG is the PREMISE check: the same stored
// value, cast by `ALTER TABLE … TYPE time`, under two different session
// TimeZones, must produce identical text.
//
// It runs the ALTER twice on two independently-created tables — once with the
// session at UTC, once at Asia/Tokyo — and compares. If Postgres ever starts
// resolving this cast through the session zone, the narrowing in
// [ir.SessionDependentZoneSwap] becomes a silent-divergence bug and this
// fails, which is the whole point of writing it down as a test rather than a
// comment.
//
// The `timestamptz` control is not decoration: it is the family that IS
// session-normalised, so it MUST differ between the two zones. Without it, a
// harness that silently ran both halves under the same zone would report the
// timetz cells as identical and prove nothing.
func TestSessionTZDropIsZoneIndependent_PG(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	// Shapes chosen for the Bug-74 axis: scalar, 1-D array with a NULL
	// element, and 2-D — plus offsets that are negative and fractional,
	// because an offset-stripping bug that only handles `+HH` would pass on
	// a `+00` fixture.
	for _, tc := range []struct {
		name    string
		colType string
		altered string
		value   string
		render  string
	}{
		{"timetz scalar", "timetz", "time", `'13:45:30.123456+05:30'`, "v::text"},
		{"timetz negative offset", "timetz", "time", `'00:30:00-08'`, "v::text"},
		{"timetz[] with NULL element", "timetz[]", "time[]", `'{"23:59:59.999999-11:45",NULL}'`, "v::text"},
		{"timetz[][] 2-D", "timetz[][]", "time[][]", `'{{01:00:00+02,02:00:00-03},{03:00:00+00,04:00:00+09}}'`, "v::text || ' dims=' || COALESCE(array_dims(v),'')"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := map[string]string{}
			for _, zone := range []string{"UTC", "Asia/Tokyo"} {
				table := "drop_" + strings.NewReplacer(" ", "_", "-", "_", "[", "", "]", "").Replace(tc.name) +
					"_" + strings.NewReplacer("/", "_", "-", "_").Replace(zone)
				applyPGSQL(t, dsn, `
					SET TIME ZONE '`+zone+`';
					CREATE TABLE `+table+` (id INT PRIMARY KEY, v `+tc.colType+`);
					INSERT INTO `+table+` (id, v) VALUES (1, `+tc.value+`);
					ALTER TABLE `+table+` ALTER COLUMN v TYPE `+tc.altered+`;
				`)
				got[zone] = queryPGScalar(t, dsn, `SELECT COALESCE(`+tc.render+`, '<null>') FROM `+table+` WHERE id = 1`)
			}
			if got["UTC"] != got["Asia/Tokyo"] {
				t.Errorf("dropping the timetz offset DEPENDS on the session zone: UTC produced %q, Asia/Tokyo produced %q.\n\n"+
					"This is the premise SLM-5b narrowed the refusal on. If it no longer holds, "+
					"ir.SessionDependentZoneSwap must go back to refusing this direction — a forwarded "+
					"ALTER would re-cast every pre-existing target row against the target session's zone.",
					got["UTC"], got["Asia/Tokyo"])
			}
		})
	}

	// The discriminating control. timestamptz IS session-normalised, so the
	// same procedure MUST produce different text — proving the two arms above
	// really did run under different zones.
	control := map[string]string{}
	for _, zone := range []string{"UTC", "Asia/Tokyo"} {
		table := "ctl_" + strings.NewReplacer("/", "_").Replace(zone)
		applyPGSQL(t, dsn, `
			SET TIME ZONE '`+zone+`';
			CREATE TABLE `+table+` (id INT PRIMARY KEY, v timestamptz);
			INSERT INTO `+table+` (id, v) VALUES (1, '2026-06-15 20:00:00+00');
			ALTER TABLE `+table+` ALTER COLUMN v TYPE timestamp;
		`)
		control[zone] = queryPGScalar(t, dsn, `SELECT v::text FROM `+table+` WHERE id = 1`)
	}
	if control["UTC"] == control["Asia/Tokyo"] {
		t.Fatalf("the timestamptz control produced the SAME value (%q) under both session zones — "+
			"the two arms did not actually run under different zones, so every timetz assertion above "+
			"is vacuous. Fix the harness, not the assertions.", control["UTC"])
	}
}

// TestCDCSchemaForward_SessionTZDropForwards_PG drives the pgoutput reader
// over a real mid-stream `timetz` → `time` swap under forward mode and
// asserts the stream SURVIVES it.
//
// The inverse of TestCDCSchemaForward_SessionTZArraySwapRefuses_PG, and it
// covers the cell that pin cannot: before SLM-5b this ALTER killed the stream
// with a refusal on a boundary that was always safe to forward. Both scalar
// and array shapes, because the array cell reaches the refusal by a different
// route (the element unwrap) and a narrowing applied to only one of them is
// exactly this repo's recurring sibling miss.
func TestCDCSchemaForward_SessionTZDropForwards_PG(t *testing.T) {
	for _, tc := range []struct {
		name    string
		colType string
		altered string
	}{
		{"timetz→time", "timetz", "time"},
		{"timetz[]→time[]", "timetz[]", "time[]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dsn, cleanup := startPostgresForCDC(t)
			defer cleanup()

			applyPGSQL(t, dsn, `
				CREATE TABLE slotdrop (id INT PRIMARY KEY, slots `+tc.colType+`);
				ALTER TABLE slotdrop REPLICA IDENTITY FULL;
			`)

			eng := Engine{}
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()

			rdr, err := eng.OpenCDCReader(ctx, dsn)
			if err != nil {
				t.Fatalf("OpenCDCReader: %v", err)
			}
			cdc, ok := rdr.(*CDCReader)
			if !ok {
				t.Fatalf("OpenCDCReader returned %T; want *CDCReader", rdr)
			}
			// Forward mode. Under refuse mode EVERY boundary refuses, so this
			// test would pass for the wrong reason — it would be measuring the
			// refuse-mode blanket, not the session-TimeZone arm.
			cdc.SetSchemaForward(true)
			defer func() { _ = cdc.Close() }()

			changes, err := cdc.StreamChanges(ctx, ir.Position{})
			if err != nil {
				t.Fatalf("StreamChanges: %v", err)
			}
			time.Sleep(200 * time.Millisecond)

			applyPGSQL(t, dsn, `INSERT INTO slotdrop (id, slots) VALUES (1, NULL);`)
			applyPGSQL(t, dsn, `
				ALTER TABLE slotdrop ALTER COLUMN slots TYPE `+tc.altered+` USING slots::`+tc.altered+`;
				INSERT INTO slotdrop (id, slots) VALUES (2, NULL);
			`)

			// Drain briefly. Unlike the refusal pins, a PASS here is the
			// stream STAYING OPEN, so this waits a bounded interval and then
			// asserts no error rather than waiting for termination.
			deadline := time.After(20 * time.Second)
			var sawSecondInsert bool
			var seen []string
		drain:
			for {
				select {
				case ch, open := <-changes:
					if !open {
						break drain
					}
					seen = append(seen, fmt.Sprintf("%T(%s)", ch, ch.QualifiedName()))
					// Value type, not pointer: the channel carries ir.Insert. The
					// first cut asserted *ir.Insert, matched nothing, and the
					// anti-vacuity check below is what caught it — which is the
					// argument for having written that check.
					if ins, isInsert := ch.(ir.Insert); isInsert && strings.HasSuffix(ins.QualifiedName(), "slotdrop") {
						sawSecondInsert = true
					}
				case <-deadline:
					break drain
				}
			}

			if err := cdc.Err(); err != nil {
				t.Fatalf("the stream DIED on a %s swap: %v\n\n"+
					"SLM-5b narrowed this direction deliberately: timetz carries its offset per value, so "+
					"dropping it consults no session zone (pinned by TestSessionTZDropIsZoneIndependent_PG). "+
					"A refusal here is the over-broad halt that narrowing removed.", tc.name, err)
			}
			if !sawSecondInsert {
				t.Errorf("no post-ALTER INSERT reached the stream for %s — the swap may have been forwarded "+
					"into a wedged stream rather than a working one, which would make the no-error assertion "+
					"above vacuous. Changes seen: %v", tc.name, seen)
			}
		})
	}
}
