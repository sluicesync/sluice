// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"testing"

	"vitess.io/vitess/go/vt/proto/binlogdata"

	"sluicesync.dev/sluice/internal/ir"
)

// v0.99.8 SILENT-DEGRADE fix — interrupted cold-start COPY must resume via
// the bulk snapshot path (batched COPY writer), NOT the plain CDC reader's
// per-row apply path (~10 rows/sec). These unit tests pin the routing
// discriminator (PositionCarriesCopyCursor) and the resume guard
// (OpenSnapshotStreamFromPosition refuses a cursor-less position rather
// than silently re-copying from row 0). The end-to-end bulk-resume claim
// — that vtgate continues the COPY from the cursor through copyPump — is
// grounded against real vtgate in the (integration && vstream)
// process-restart test.

// posWithCursor builds an ir.Position carrying a per-shard TablePKs cursor
// for the named table (the interrupted-cold-start shape). posWithoutCursor
// builds a pure-CDC position (the completed-cold-start shape).
func posWithCursor(t *testing.T, table string, pk int64) ir.Position {
	t.Helper()
	cursor, err := encodeTablePKs([]*binlogdata.TableLastPK{makeTableLastPK(t, table, "id", pk)})
	if err != nil {
		t.Fatalf("encodeTablePKs: %v", err)
	}
	pos, err := encodeVStreamPos([]shardGtid{{
		Keyspace: "main",
		Shard:    "-",
		Gtid:     "MySQL56/abcd:1-100",
		TablePKs: cursor,
	}})
	if err != nil {
		t.Fatalf("encodeVStreamPos: %v", err)
	}
	return pos
}

func posWithoutCursor(t *testing.T) ir.Position {
	t.Helper()
	pos, err := encodeVStreamPos([]shardGtid{{
		Keyspace: "main",
		Shard:    "-",
		Gtid:     "MySQL56/abcd:1-200",
	}})
	if err != nil {
		t.Fatalf("encodeVStreamPos: %v", err)
	}
	return pos
}

// TestPositionCarriesCopyCursor_Discriminates is the routing-decision pin:
// a mid-COPY position (TablePKs present) → true (bulk resume path); a
// pure-CDC position (no TablePKs) → false (fast plain-CDC warm-resume); the
// from-now sentinel and a non-PlanetScale flavor → false.
func TestPositionCarriesCopyCursor_Discriminates(t *testing.T) {
	ps := Engine{Flavor: FlavorPlanetScale}

	t.Run("mid-COPY position routes to bulk path", func(t *testing.T) {
		if !ps.PositionCarriesCopyCursor(posWithCursor(t, "widgets", 5000)) {
			t.Error("PositionCarriesCopyCursor = false for a cursor-carrying position; want true (must take the bulk resume path)")
		}
	})

	t.Run("completed-cold-start position stays on plain CDC", func(t *testing.T) {
		if ps.PositionCarriesCopyCursor(posWithoutCursor(t)) {
			t.Error("PositionCarriesCopyCursor = true for a cursor-less position; want false (completed cold-start must stay on the fast plain-CDC path)")
		}
	})

	t.Run("from-now sentinel is not a resume", func(t *testing.T) {
		if ps.PositionCarriesCopyCursor(ir.Position{}) {
			t.Error("PositionCarriesCopyCursor = true for the empty sentinel; want false")
		}
	})

	t.Run("non-PlanetScale flavor never carries a VStream cursor", func(t *testing.T) {
		vanilla := Engine{Flavor: FlavorVanilla}
		if vanilla.PositionCarriesCopyCursor(posWithCursor(t, "widgets", 5000)) {
			t.Error("vanilla flavor reported a VStream copy cursor; want false (it has no VStream snapshot)")
		}
	})

	t.Run("undecodable token is not a routing hit", func(t *testing.T) {
		// A garbage token must NOT route to the bulk path (the plain CDC
		// decoder surfaces the decode error loudly); the discriminator is a
		// hint, not a validation gate.
		if ps.PositionCarriesCopyCursor(ir.Position{Engine: engineNameVStream, Token: "{not-json"}) {
			t.Error("undecodable token reported a cursor; want false (routing hint must fail closed to the plain CDC path)")
		}
	})
}

// TestOpenSnapshotStreamFromPosition_RefusesCursorlessPosition is the
// loud-failure pin: seeding a bulk snapshot from a cursor-less position
// would make vtgate restart the COPY from row 0 against the
// partially-copied target (silent full re-copy). The resumer must refuse
// loudly. It also refuses the empty sentinel and a non-PlanetScale flavor.
func TestOpenSnapshotStreamFromPosition_RefusesCursorlessPosition(t *testing.T) {
	ps := Engine{Flavor: FlavorPlanetScale}
	ctx := context.Background()

	t.Run("cursor-less position is refused, not re-copied", func(t *testing.T) {
		_, err := ps.OpenSnapshotStreamFromPosition(ctx, "dsn", posWithoutCursor(t), nil)
		if err == nil {
			t.Fatal("OpenSnapshotStreamFromPosition accepted a cursor-less position; want a loud refusal (re-copying from row 0 is silent loss)")
		}
	})

	t.Run("empty sentinel is refused", func(t *testing.T) {
		_, err := ps.OpenSnapshotStreamFromPosition(ctx, "dsn", ir.Position{}, nil)
		if err == nil {
			t.Fatal("OpenSnapshotStreamFromPosition accepted the empty sentinel; want a loud refusal")
		}
	})

	t.Run("non-PlanetScale flavor is refused as not-implemented", func(t *testing.T) {
		vanilla := Engine{Flavor: FlavorVanilla}
		_, err := vanilla.OpenSnapshotStreamFromPosition(ctx, "dsn", posWithCursor(t, "widgets", 5000), nil)
		if !errors.Is(err, ErrNotImplemented) {
			t.Fatalf("vanilla resume err = %v; want ErrNotImplemented (binlog snapshot has no mid-COPY cursor)", err)
		}
	})
}

// Compile-time assertion that the Engine satisfies the optional pipeline
// resume surface, so the streamer's type-assert routing finds it. (A
// non-PlanetScale flavor still satisfies the interface — the methods
// refuse at runtime — which is correct: the pipeline gates on
// PositionCarriesCopyCursor, not on interface presence.)
var _ ir.SnapshotStreamResumer = Engine{Flavor: FlavorPlanetScale}
