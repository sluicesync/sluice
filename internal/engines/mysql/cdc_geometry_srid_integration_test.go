//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 236: the CDC lane's schema loader did not read the declared geometry
// SRID, so v0.118.0's per-row SRID guard refused every row of a column that
// had declared itself properly.

package mysql

import (
	"context"
	"errors"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestCDCReader_DeclaredSRIDColumnStreams is the over-refusal control, and it
// is the regression itself.
//
// A MySQL source column declaring `POINT SRID 4326` streamed correctly on
// v0.117.0 and was refused by v0.118.0 on the FIRST CDC row, because
// loadTableSchema — the binlog reader's schema source — never selected
// srs_id, so the column arrived as SRID 0 and the guard compared 4326 against
// a value nothing had read. The full SchemaReader path DOES read it, which is
// why `migrate` was correct in every direction and this stayed hidden.
//
// A refusal that fires on a working configuration is worse than the silent
// class it was added to catch, so this is the cell that matters most.
func TestCDCReader_DeclaredSRIDColumnStreams(t *testing.T) {
	dsn, cleanup := startMySQLForCDC(t)
	defer cleanup()

	applyMySQL(t, dsn, `
		CREATE TABLE geo_declared (
			id BIGINT NOT NULL PRIMARY KEY,
			p  POINT SRID 4326 NOT NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	applyMySQL(t, dsn, `
		INSERT INTO geo_declared (id, p) VALUES (1, ST_GeomFromText('POINT(1 2)', 4326));
		UPDATE geo_declared SET p = ST_GeomFromText('POINT(3 4)', 4326) WHERE id = 1;
	`)

	got := drainChanges(t, ctx, changes, 2, 30*time.Second)
	if len(got) != 2 {
		if cdcRdr, ok := rdr.(*CDCReader); ok {
			if streamErr := cdcRdr.Err(); streamErr != nil {
				t.Fatalf("got %d changes; want 2 — a column that DECLARES its SRID must stream. "+
					"stream error: %v", len(got), streamErr)
			}
		}
		t.Fatalf("got %d changes; want 2 (Insert, Update)", len(got))
	}
}

// TestCDCReader_UndeclaredSRIDColumnStillRefuses is the anti-vacuity half, and
// the reason the test above cannot stand alone.
//
// Making the guard stop firing is trivial and wrong: resolving nothing, or
// marking every column unknown, would turn the C-14 refusal off wholesale and
// this file would still be green. So this drives the shape that MUST still
// refuse — an UNDECLARED geometry column (MySQL reports srs_id 0 for it and
// accepts a different SRID per row) holding a row written at 4326.
//
// Its refusal is what proves the resolver genuinely READ `0` from
// st_geometry_columns, rather than having failed to read anything.
func TestCDCReader_UndeclaredSRIDColumnStillRefuses(t *testing.T) {
	dsn, cleanup := startMySQLForCDC(t)
	defer cleanup()

	applyMySQL(t, dsn, `
		CREATE TABLE geo_loose (
			id BIGINT NOT NULL PRIMARY KEY,
			p  POINT NOT NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	applyMySQL(t, dsn, `INSERT INTO geo_loose (id, p) VALUES (1, ST_GeomFromText('POINT(1 2)', 4326));`)

	deadline := time.Now().Add(30 * time.Second)
	var streamErr error
	for time.Now().Before(deadline) && streamErr == nil {
		if cdcRdr, ok := rdr.(*CDCReader); ok {
			streamErr = cdcRdr.Err()
		}
		select {
		case c, ok := <-changes:
			if !ok {
				continue
			}
			switch c.(type) {
			case ir.TxBegin, ir.TxCommit, ir.SchemaSnapshot:
				continue
			}
			t.Fatalf("a per-row SRID mismatch on an UNDECLARED column was delivered rather than refused: %+v — "+
				"the C-14 guard is inert on this lane, which the declared-column test above cannot detect", c)
		case <-time.After(200 * time.Millisecond):
		}
	}

	if streamErr == nil {
		t.Fatal("expected a refusal for a row whose SRID differs from its undeclared column; got neither a " +
			"change nor an error, so the resolver may be marking every column unknown")
	}
	if !errors.Is(streamErr, ir.ErrGeometryRowSRIDMismatch) {
		t.Errorf("stream error = %v; want one wrapping ir.ErrGeometryRowSRIDMismatch", streamErr)
	}
}
