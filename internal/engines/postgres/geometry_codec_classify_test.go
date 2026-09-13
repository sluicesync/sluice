// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/ir"
)

// TestSpatialOIDProbeErrorIsClassified pins the fix for a failure MEASURED on a
// real PlanetScale Neki branch, and the reason it is worth a test of its own is
// where the error comes from rather than what it says.
//
// `afterConnectRegisterGeometry` is an AfterConnect hook, so it runs on EVERY
// connection the engine opens — including the per-chunk writer connections the
// parallel copy mints. Its error therefore lands on the chunk-OPEN path, whose
// retry ([isRetriableChunkOpenError]) decides by asking whether the error
// carries an engine verdict. Returned bare, a transient platform condition
// arrives with nothing to read and is treated as fatal.
//
// Measured 2026-09-13: a fresh PS-10 Neki with a 10 GiB volume, copying 13 GB.
// The volume filled, the shard went read-only, its sidecars went unhealthy, and
// this probe's `pg_type` query came back `NK205 no healthy sidecars available
// for shard … SIDECAR_TYPE_PRIMARY`. NK205 is classified transient precisely so
// a copy can ride out that window — but the verdict was discarded at this
// return, so chunk 6 failed the table and the whole migration died after 3m40s
// with only four chunk retries and a max attempt of 2. The retry budget was
// never the limit; the classification was.
//
// This is the Bug-207 class at a RETURN site rather than a setErr one, which is
// why `internal/errclassgate` could not see it: that gate walks PARKED errors,
// and a hook that returns is invisible to it.
func TestSpatialOIDProbeErrorIsClassified(t *testing.T) {
	t.Parallel()

	// The exact platform error the live run produced.
	nk205 := &pgconn.PgError{
		Code:    "NK205",
		Message: "no healthy sidecars available for shard sh5qistfbxcsz9 with type SIDECAR_TYPE_PRIMARY",
	}
	wrapped := fmt.Errorf("postgres: lookup spatial type OIDs: %w", nk205)

	// The engine must hand the chunk-open path a verdict it can act on.
	classified := classifyCopyError(wrapped)
	var re ir.RetriableError
	if !errors.As(classified, &re) || !re.Retriable() {
		t.Fatalf("the spatial-OID probe's NK205 is not classified retriable (%v). On the chunk-open path "+
			"that is read as fatal, so a copy dies on a condition that clears as soon as the platform "+
			"finishes growing the volume", classified)
	}

	// SCOPE, split honestly because the two halves live in different packages.
	// This test grades the ENGINE's verdict. The DECISION it feeds —
	// pipeline.isRetriableChunkOpenError honouring an engine-classified
	// ir.RetriableError — lives in internal/pipeline and is graded there by
	// TestChunkOpenRetryHonoursEngineVerdict. Neither half is sufficient alone:
	// this one would pass with the pipeline ignoring the verdict, and that one
	// would pass with the engine discarding it, which is exactly the shape that
	// produced the live failure.
	//
	// The anti-vacuity check for THIS half: the bare, unclassified error must
	// carry no verdict, or the assertion above proves nothing about the fix.
	var bare ir.RetriableError
	if errors.As(wrapped, &bare) {
		t.Fatal("the UNCLASSIFIED error already satisfies ir.RetriableError, so this test cannot " +
			"distinguish the fixed code from the broken code")
	}
}
