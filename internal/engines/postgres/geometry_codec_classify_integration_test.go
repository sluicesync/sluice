//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"sluicesync.dev/sluice/internal/ir"
)

// TestAfterConnectRegisterGeometry_ClassifiesItsError drives the REAL hook and
// is the gate that actually holds the fix.
//
// Its unit sibling calls `classifyCopyError` directly, so it grades the
// classifier and says nothing about whether [afterConnectRegisterGeometry]
// USES it — proven by mutation: reverting the hook to `return err` left that
// test green. Same self-referential shape as the in-doubt commit gate earlier
// in this release, and worth stating twice because it is cheap to write a test
// that agrees with itself.
//
// This one takes the error FROM the hook. A closed connection makes the
// spatial-OID probe fail for real, through the real call path, with no
// stubbing — and the assertion is simply "the returned error carries a verdict
// the chunk-open retry can read", which is exactly the property whose absence
// killed a live migration.
//
// # Why that property matters here specifically
//
// This hook runs on EVERY connection the engine opens, including the per-chunk
// writer connections. Measured 2026-09-13 on a fresh PS-10 Neki whose 10 GiB
// volume filled mid-copy: the shard went read-only, its sidecars went
// unhealthy, and this probe returned `NK205 no healthy sidecars available`.
// NK205 is classified transient precisely so the copy can wait out that window
// — but the verdict was discarded at the hook's return, so the chunk-open retry
// saw nothing to honour and failed the table after 3m40s.
func TestAfterConnectRegisterGeometry_ClassifiesItsError(t *testing.T) {
	dsn, cleanup := newSharedPGDB(t, "geom_classify_db")
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	// Premise check: on a HEALTHY connection the hook succeeds. Without this
	// the test could "pass" against a hook that fails for an unrelated reason,
	// and the error below would not be the one under test.
	if err := afterConnectRegisterGeometry(ctx, conn); err != nil {
		t.Fatalf("the hook failed on a healthy connection (%v) — the failure below would then not be the "+
			"probe error this test is about", err)
	}

	// Now break the connection for real and re-run the hook. The type map
	// still lacks the spatial types (PostGIS is absent on this container), so
	// the hook reaches its probe rather than short-circuiting.
	if err := conn.Close(ctx); err != nil {
		t.Logf("closing the connection returned %v (expected; the close is the point)", err)
	}

	hookErr := afterConnectRegisterGeometry(ctx, conn)
	if hookErr == nil {
		t.Fatal("the hook returned nil on a CLOSED connection — it cannot have reached its probe, so this " +
			"test is not exercising the path it claims to")
	}

	// THE ASSERTION. Not "is it retriable" — that depends on which fault the
	// server reported — but "does it carry a verdict at all". An unclassified
	// error reaches the chunk-open retry with nothing to read and is treated
	// as fatal, whatever it actually was.
	var re ir.RetriableError
	var te ir.TerminalError
	if !errors.As(hookErr, &re) && !errors.As(hookErr, &te) {
		t.Fatalf("afterConnectRegisterGeometry returned an UNCLASSIFIED error: %v\n\n"+
			"This hook runs on every per-chunk writer connection, so its error lands on the chunk-open "+
			"path, whose retry decides by asking for an engine verdict. With none attached, a transient "+
			"platform condition — a sidecar outage, a volume mid-grow — fails the whole table. That is "+
			"the measured PS-10 failure this classification exists to prevent.", hookErr)
	}
}
