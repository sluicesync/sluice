//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The Postgres array half of the session-TimeZone cast refusal against a
// real server (audit 2026-08-31 SL-3).
//
// The unit pins prove the predicate; these prove the two things a unit pin
// structurally cannot:
//
//   - the four array OIDs the unwrap depends on are the ones a real
//     pg_type publishes (they are literals, not derivations — an array
//     OID is not element+1), and
//   - a real `ALTER TABLE … TYPE timetz[]` mid-stream produces a
//     RelationMessage that reaches the refusal, rather than one whose
//     shape the classifier never fires on.
//
// The second is the half the regression lived in: from v0.134.0 the swap
// was classified, seen, and waved through.

package postgres

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"sluicesync.dev/sluice/internal/ir"
)

// TestPGArrayOIDs_GroundTruthedAgainstPgType reads the four temporal array
// OIDs out of a live pg_type and compares them to the values
// pgArrayElementOID is keyed on. The independent expected value is the
// server's catalog, not another sluice constant.
func TestPGArrayOIDs_GroundTruthedAgainstPgType(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, tc := range []struct {
		typname     string
		wantArrOID  uint32
		wantElemOID uint32
	}{
		{"time", 1183, pgtype.TimeOID},
		{"timetz", 1270, pgtype.TimetzOID},
		{"timestamp", 1115, pgtype.TimestampOID},
		{"timestamptz", 1185, pgtype.TimestamptzOID},
	} {
		var arrOID, elemOID uint32
		if err := db.QueryRowContext(
			ctx,
			`SELECT t.typarray, t.oid FROM pg_type t
			   WHERE t.typname = $1 AND t.typnamespace = 'pg_catalog'::regnamespace`,
			tc.typname,
		).Scan(&arrOID, &elemOID); err != nil {
			t.Fatalf("read pg_type for %s: %v", tc.typname, err)
		}
		if arrOID != tc.wantArrOID || elemOID != tc.wantElemOID {
			t.Errorf("pg_catalog.%s: server says array OID %d / element OID %d; sluice keys on %d / %d",
				tc.typname, arrOID, elemOID, tc.wantArrOID, tc.wantElemOID)
		}
		if got, ok := pgArrayElementOID[arrOID]; !ok || got != elemOID {
			t.Errorf("pgArrayElementOID[%d] = %d, present=%v; the server's _%s maps to element %d — the array session-TimeZone arm is inert for this family",
				arrOID, got, ok, tc.typname, elemOID)
		}
	}
}

// TestCDCSchemaForward_SessionTZArraySwapRefuses_PG drives the pgoutput
// reader over a real `time[]` → `timetz[]` swap under forward mode and
// asserts the stream dies with the session-TimeZone refusal naming the
// ARRAY pair. This is the exact ALTER that forwarded silently from
// v0.134.0 through the SL-3 fix.
func TestCDCSchemaForward_SessionTZArraySwapRefuses_PG(t *testing.T) {
	for _, tc := range []struct {
		name     string
		colType  string
		altered  string
		wantPair string
	}{
		{"time[]→timetz[]", "time[]", "timetz[]", "time[] and timetz[]"},
		{"timestamp[]→timestamptz[]", "timestamp[]", "timestamptz[]", "timestamp[] and timestamptz[]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dsn, cleanup := startPostgresForCDC(t)
			defer cleanup()

			applyPGSQL(t, dsn, `
				CREATE TABLE slotsched (
					id    INT PRIMARY KEY,
					slots `+tc.colType+`
				);
				ALTER TABLE slotsched REPLICA IDENTITY FULL;
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
			// Forward mode: without this the generic refuse-mode message
			// fires and the test would pass without ever reaching the
			// session-TimeZone arm — the vacuous-green shape.
			cdc.SetSchemaForward(true)
			defer func() { _ = cdc.Close() }()

			changes, err := cdc.StreamChanges(ctx, ir.Position{})
			if err != nil {
				t.Fatalf("StreamChanges: %v", err)
			}
			time.Sleep(200 * time.Millisecond)

			// Prime the relation cache with the pre-ALTER shape, then swap.
			applyPGSQL(t, dsn, `INSERT INTO slotsched (id, slots) VALUES (1, NULL);`)
			applyPGSQL(t, dsn, `
				ALTER TABLE slotsched ALTER COLUMN slots TYPE `+tc.altered+` USING slots::`+tc.altered+`;
				INSERT INTO slotsched (id, slots) VALUES (2, NULL);
			`)

			drained := make(chan struct{})
			go func() {
				defer close(drained)
				for range changes {
				}
			}()
			select {
			case <-drained:
			case <-time.After(60 * time.Second):
				t.Fatal("stream did not terminate within 60s — the array session-TimeZone swap was forwarded instead of refused (the v0.134.0 regression shape)")
			}

			streamErr := cdc.Err()
			if streamErr == nil {
				t.Fatal("stream ended with no error; the array swap must refuse loudly — a forwarded ALTER re-casts every ELEMENT of every pre-existing target row against the target session's TimeZone")
			}
			for _, want := range []string{
				"cannot be forwarded", `column "slots"`,
				"between " + tc.wantPair, "TimeZone", "drained model",
			} {
				if !strings.Contains(streamErr.Error(), want) {
					t.Errorf("stream error missing %q; got: %v", want, streamErr)
				}
			}
		})
	}
}

// TestCDCSchemaForward_ArrayNonTZSwapStillForwards_PG is the
// no-over-refusal floor's real-server companion for the array arm: an
// `_int4` → `_int8` swap is not a zone-sibling pair, so it must NOT take
// the session-TimeZone arm. Its projection moves, so it also escapes the
// pre-existing projection gate — it forwards and the stream survives. An
// unwrap broadened to "both sides are arrays" fails here.
func TestCDCSchemaForward_ArrayNonTZSwapStillForwards_PG(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	applyPGSQL(t, dsn, `
		CREATE TABLE nums (id INT PRIMARY KEY, ns int4[]);
		ALTER TABLE nums REPLICA IDENTITY FULL;
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
	cdc.SetSchemaForward(true)
	defer func() { _ = cdc.Close() }()

	changes, err := cdc.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	applyPGSQL(t, dsn, `INSERT INTO nums (id, ns) VALUES (1, '{1,2}');`)
	applyPGSQL(t, dsn, `
		ALTER TABLE nums ALTER COLUMN ns TYPE int8[] USING ns::int8[];
		INSERT INTO nums (id, ns) VALUES (2, '{3,4}');
	`)

	deadline := time.After(60 * time.Second)
	sawPostAlter := false
	for !sawPostAlter {
		select {
		case c, ok := <-changes:
			if !ok {
				t.Fatalf("stream closed before the post-ALTER row; _int4[]→_int8[] must forward. Err: %v", cdc.Err())
			}
			ins, isIns := c.(ir.Insert)
			if !isIns {
				continue
			}
			if id, ok := ins.Row["id"].(int32); ok && id == 2 {
				sawPostAlter = true
			}
			if id, ok := ins.Row["id"].(int64); ok && id == 2 {
				sawPostAlter = true
			}
		case <-deadline:
			t.Fatalf("post-ALTER row never arrived; _int4[]→_int8[] must forward. Err: %v", cdc.Err())
		}
	}
}
