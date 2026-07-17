//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 194's verify face. SampleRowHashes computes the row-content hash
// SERVER-SIDE over ::text renderings, and float4/float8 text output
// depends on the rendering SESSION's extra_float_digits — inherited
// from each endpoint's server/database/role default. Unpinned, that
// breaks the hash comparison in BOTH directions:
//
//   - FALSE MISMATCH: identical stored floats render differently when
//     the two endpoints' defaults differ (Supabase ships 0 server-wide;
//     stock PG ≥ 12 ships 1) — verify reports corruption where none
//     exists.
//   - FALSE CLEAN: a source at efd=0 renders the TRUE value identically
//     to a target holding that value's ROUNDED corruption (both print
//     the legacy %.15g digits) — verify blesses the exact corruption
//     class it exists to catch (the Bug 194 raw-copy loss).
//
// The fix pins extra_float_digits=3 on the hash query's session
// (statement-level SET — poolers strip it as a startup parameter), so
// both sides hash canonical shortest-exact renderings. This test
// reproduces both directions on real PG with per-database defaults.

package postgres

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// sampleFloatHashes reads the deterministic row-hash sample off one
// endpoint via the real SchemaReader surface (the path `sluice verify
// --depth=sample` drives).
func sampleFloatHashes(t *testing.T, ctx context.Context, dsn string) []ir.SampledRowHash {
	t.Helper()
	rdr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	schema, err := rdr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	var table *ir.Table
	for _, tb := range schema.Tables {
		if tb.Name == "vfefd" {
			table = tb
		}
	}
	if table == nil {
		t.Fatal("table vfefd not found")
	}
	sv, ok := rdr.(ir.SampleVerifier)
	if !ok {
		t.Fatalf("SchemaReader %T does not implement ir.SampleVerifier", rdr)
	}
	hashes, err := sv.SampleRowHashes(ctx, table, 10, 42, ir.HashMD5)
	if err != nil {
		t.Fatalf("SampleRowHashes: %v", err)
	}
	return hashes
}

func TestSampleRowHashes_FloatCanonicalUnderEFDDefaults(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Endpoint A: the Supabase shape (database default efd=0), holding
	// the TRUE values. Endpoint B: stock PG (efd=1), same TRUE values.
	// Endpoint C: stock PG, holding the values a lossy text copy off A
	// would have produced (the ROUNDED corruptions).
	dsnA, cleanupA := newSharedPGDB(t, "vefd_a")
	defer cleanupA()
	dsnB, cleanupB := newSharedPGDB(t, "vefd_b")
	defer cleanupB()
	dsnC, cleanupC := newSharedPGDB(t, "vefd_c")
	defer cleanupC()

	const trueRows = `
		CREATE TABLE vfefd (id BIGINT PRIMARY KEY, f8 float8, f4 float4);
		INSERT INTO vfefd VALUES
			(1, pi(), 16777215.0),
			(2, 2.718281828459045, 3.4028235e38),
			(3, NULL, NULL);
	`
	applyPGSQL(t, dsnA, trueRows)
	applyPGSQL(t, dsnA, `ALTER DATABASE vefd_a SET extra_float_digits = 0`)
	applyPGSQL(t, dsnB, trueRows)
	applyPGSQL(t, dsnB, `ALTER DATABASE vefd_b SET extra_float_digits = 1`)
	// C holds what the legacy %.15g/%.6g renderings would have landed:
	// π rounded at digit 15, float4 2^24-1 rounded to 6 significant
	// digits, e rounded at digit 15, FLT_MAX rounded to 3.40282e38.
	applyPGSQL(t, dsnC, `
		CREATE TABLE vfefd (id BIGINT PRIMARY KEY, f8 float8, f4 float4);
		INSERT INTO vfefd VALUES
			(1, 3.14159265358979, 16777200.0),
			(2, 2.71828182845905, 3.40282e38),
			(3, NULL, NULL);
	`)

	hashesA := sampleFloatHashes(t, ctx, dsnA)
	hashesB := sampleFloatHashes(t, ctx, dsnB)
	hashesC := sampleFloatHashes(t, ctx, dsnC)
	if len(hashesA) != 3 || len(hashesB) != 3 || len(hashesC) != 3 {
		t.Fatalf("sample sizes = %d/%d/%d; want 3 each", len(hashesA), len(hashesB), len(hashesC))
	}

	// FALSE-MISMATCH direction: identical values must hash identically
	// even though A and B render floats differently by default.
	for i := range hashesA {
		if hashesA[i] != hashesB[i] {
			t.Errorf("row %s: identical values hash differently across efd defaults (A=%s B=%s) — false mismatch",
				hashesA[i].PrimaryKey, hashesA[i].Hash, hashesB[i].Hash)
		}
	}
	// FALSE-CLEAN direction: the rounded corruption must NOT hash equal
	// to the true values. Pre-fix, A's efd=0 session rendered the true π
	// as its own rounding, matching C exactly — blessing the corruption.
	for i := range hashesA {
		if hashesA[i].PrimaryKey == "3" {
			continue // the NULL row is genuinely identical on A and C
		}
		if hashesA[i] == hashesC[i] {
			t.Errorf("row %s: true value and its rounded corruption hash EQUAL (%s) — false clean",
				hashesA[i].PrimaryKey, hashesA[i].Hash)
		}
	}
}
