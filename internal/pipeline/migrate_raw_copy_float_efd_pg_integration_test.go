//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 194 pin — CRITICAL silent loss: the PG→PG raw-copy TEXT lane
// rendered float4/float8 through the SOURCE session's
// extra_float_digits, so a server/database/role default < 1 (Supabase
// ships 0 server-wide) silently rounded every float needing more digits
// than the legacy %.15g/%.6g renderings (π …2d18 → …2d11, float4 2^24 →
// 2^24-16; rc=0, "migration complete"). Only DBL_MAX failed loudly —
// its 15-digit rendering overflows float8in on COPY FROM (22003).
//
// The fix pins `SET extra_float_digits = 3` (statement-level — poolers
// strip it as a startup parameter) on the raw-copy EXPORT session for
// every non-binary format. This pin drives the full request-format ×
// source-default matrix on real PG — {text, binary, auto} ×
// efd ∈ {0, -15, 1, 3} (the Bug-74 discipline: the lane dispatches on
// format, the corruption dispatches on the source default; one green
// cell proves neither) — with the source default set PER DATABASE to
// reproduce the Supabase shape, and asserts SEND-BYTES equality
// (float8send/float4send hex), not display equality. The DBL_MAX row is
// in every corpus: under the fix the former loud-overflow cell must
// become a PASS with exact bytes.
//
// 'auto' has no distinct value at the Migrator layer — the CLI maps it
// to a binary REQUEST (pinned through the parser in
// cmd/sluice/raw_copy_format_flag_test.go, the Bug-180 lesson); it is
// still driven here as its own request column so a future divergence
// between the two spellings fails a cell.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"

	_ "sluicesync.dev/sluice/internal/engines/postgres"

	"github.com/testcontainers/testcontainers-go"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// floatEFDSeedDDL is the float corpus: every finite value needs MORE
// digits than the legacy renderings carry, plus the loud-overflow
// boundary, the denormal floor, and the non-finite / signed-zero
// specials (NaN / ±Infinity render as literal words and -0 keeps its
// sign bit through text COPY — all extra_float_digits-independent, but
// they pin that the session pins don't disturb the special-value
// spellings). NULL row included (the shape variant).
const floatEFDSeedDDL = `
	CREATE TABLE floats (
		id BIGINT PRIMARY KEY,
		f8 DOUBLE PRECISION,
		f4 REAL
	);
	INSERT INTO floats VALUES
		(1, pi(), 16777216.0),                                -- π needs 17 digits; float4 2^24 needs 8
		(2, 1.7976931348623157e308, 3.4028235e38),            -- DBL_MAX (the 22003 loud cell pre-fix), FLT_MAX
		(3, 5e-324, 1.4e-45),                                 -- denormal floors
		(4, -2.2250738585072014e-308, -1.17549435e-38),       -- smallest normals, negative
		(5, 'NaN'::float8, 'NaN'::float4),
		(6, 'Infinity'::float8, '-Infinity'::float4),
		(7, '-0'::float8, '-0'::float4),                      -- signed zero: send-bytes catch a dropped sign bit
		(8, NULL, NULL);
`

// floatEFDCorpusRows is the seeded row count (send-bytes assertions
// require every row to arrive).
const floatEFDCorpusRows = 8

// floatEFDSendBytes reads the per-row float send-bytes off one endpoint.
// float8send/float4send are the wire ground truth — display-independent,
// so the comparison cannot itself be fooled by extra_float_digits.
func floatEFDSendBytes(t *testing.T, dsn string) []string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, `
		SELECT id,
		       COALESCE(encode(float8send(f8), 'hex'), 'null'),
		       COALESCE(encode(float4send(f4), 'hex'), 'null')
		FROM floats ORDER BY id`)
	if err != nil {
		t.Fatalf("send-bytes query: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var id int64
		var f8hex, f4hex string
		if err := rows.Scan(&id, &f8hex, &f4hex); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, fmt.Sprintf("%d:%s:%s", id, f8hex, f4hex))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}

// TestRawCopy_FloatExactUnderSourceEFDDefaults is the Bug 194 matrix.
// One container, one cell per (request format × source-database
// extra_float_digits default): fresh src/dst databases, the Supabase
// shape reproduced via ALTER DATABASE ... SET extra_float_digits, a
// migrate whose raw lane is asserted TAKEN, and send-bytes equality on
// the full corpus.
func TestRawCopy_FloatExactUnderSourceEFDDefaults(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	// The pre-baked image's datadir is already initialized, so the
	// database/user must match what the image bakes (source_db/test) —
	// WithDatabase cannot mint a different admin db here. source_db
	// doubles as the admin connection; the per-cell src/dst databases
	// are created from it.
	container, err := pgtc.Run(
		ctx,
		pgPrebakedImage,
		pgtc.WithDatabase("source_db"),
		pgtc.WithUsername("test"),
		pgtc.WithPassword("test"),
		pgtc.BasicWaitStrategies(),
		pgPrebakedWaitStrategy(),
	)
	if err != nil {
		t.Fatalf("start container: %v", err)
	}
	defer func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}()

	adminDSN, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	admin, err := sql.Open("pgx", adminDSN)
	if err != nil {
		t.Fatalf("open admin: %v", err)
	}
	defer func() { _ = admin.Close() }()

	pg := pgEngineOrSkip(t)

	requests := []struct {
		name   string
		format ir.RawCopyFormat
	}{
		{"text", ir.RawCopyText},
		{"binary", ir.RawCopyBinary},
		// 'auto' == a binary request at the CLI layer; kept as its own
		// column so the spellings can never silently diverge.
		{"auto", ir.RawCopyBinary},
	}
	// Source-database defaults: 0 is the Supabase server default (the
	// live corruption), -15 is the legal floor (worst case), 1 is stock
	// PG ≥ 12, 3 is the max. Every cell must be byte-exact — the export
	// pin makes the source default irrelevant.
	efds := []int{0, -15, 1, 3}

	for i, req := range requests {
		for j, efd := range efds {
			name := fmt.Sprintf("%s/efd=%d", req.name, efd)
			t.Run(name, func(t *testing.T) {
				src := fmt.Sprintf("src_%d_%d", i, j)
				dst := fmt.Sprintf("dst_%d_%d", i, j)
				for _, db := range []string{src, dst} {
					if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+db); err != nil {
						t.Fatalf("create %s: %v", db, err)
					}
				}
				// The Supabase shape: the SOURCE's default is below the
				// shortest-exact threshold before sluice ever connects.
				if _, err := admin.ExecContext(ctx,
					fmt.Sprintf("ALTER DATABASE %s SET extra_float_digits = %d", src, efd)); err != nil {
					t.Fatalf("set efd default: %v", err)
				}

				srcDSN, err := buildPGDSN(adminDSN, src)
				if err != nil {
					t.Fatalf("src dsn: %v", err)
				}
				dstDSN, err := buildPGDSN(adminDSN, dst)
				if err != nil {
					t.Fatalf("dst dsn: %v", err)
				}
				applyPGDDL(t, srcDSN, floatEFDSeedDDL)

				rec := installRawCopyRecorder(t)
				runRawCopyMigrate(t, &Migrator{
					Source: pg, Target: pg,
					SourceDSN: srcDSN, TargetDSN: dstDSN,
					RawCopyFormat: req.format,
				})
				// The pin must be tested on the lane it fixes: a fallback
				// to the IR path would re-green this test while the raw
				// lane stayed lossy.
				if !rec.took("floats") {
					t.Fatalf("raw lane was NOT taken; recorded=%v", rec.tables)
				}

				srcBytes := floatEFDSendBytes(t, srcDSN)
				dstBytes := floatEFDSendBytes(t, dstDSN)
				if len(srcBytes) != floatEFDCorpusRows {
					t.Fatalf("source rows = %d; want %d", len(srcBytes), floatEFDCorpusRows)
				}
				for k := range srcBytes {
					if k >= len(dstBytes) || srcBytes[k] != dstBytes[k] {
						t.Errorf("send-bytes diverge at row %d:\n src=%v\n dst=%v", k+1, srcBytes, dstBytes)
						break
					}
				}
			})
		}
	}
}
