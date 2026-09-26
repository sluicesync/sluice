//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"bytes"
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/laneapply"
)

// TestLaneKey_FloatKeyFromTheStreamMatchesTheCopyRead binds the premise the
// lane router's float arm rests on, against a real Postgres: a float key the
// copy reader hands over as float64 and the SAME key as this engine's change
// payload decodes it (to_jsonb — numeric's rendering of float8out, then
// [decodeJSONBRow]'s int64-or-json.Number rule) must encode to identical
// canonical bytes, or an ADD COLUMN fill's UPDATE and a later change event
// for one row take two lanes (2026-09-25 pre-tag review, item 4). Every
// float8 shape whose rendering differs between Go and Postgres is covered:
// PG writes plain decimals where Go's shortest form uses an exponent.
//
// The independent expected value is the server's own float8 (read back as
// float64 through the driver) — never sluice's own rendering.
func TestLaneKey_FloatKeyFromTheStreamMatchesTheCopyRead(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	for _, lit := range []string{
		"1.5e-7", "1e-5", "0.0001", "0.1", "1.5", "3", "0", "-0.5", "123456789012.5", "1e15", "1e21",
		"1.7976931348623157e308", "5e-324", "1.234567e-320", "-2.5e-300", "9007199254740993",
	} {
		var f float64
		var payload string
		if err := db.QueryRowContext(ctx,
			`SELECT $1::float8, jsonb_build_object('k', $1::float8)::text`, lit).Scan(&f, &payload); err != nil {
			t.Fatalf("%s: %v", lit, err)
		}
		row, err := decodeJSONBRow(payload)
		if err != nil {
			t.Fatalf("%s: decodeJSONBRow(%s): %v", lit, payload, err)
		}
		var copyKey, streamKey bytes.Buffer
		laneapply.WriteCanonicalKeyValue(&copyKey, f)
		laneapply.WriteCanonicalKeyValue(&streamKey, row["k"])
		if copyKey.String() != streamKey.String() {
			t.Errorf("float8 %s: the copy read (%v) encodes %q, the change payload (%T %v from %s) encodes %q — one row, two lanes",
				lit, f, copyKey.String(), row["k"], row["k"], payload, streamKey.String())
		}
	}
}
