//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Item 135, the two end-to-end provenance rows: a src==dst migrate over
// the bytea value-shape matrix, and a backup→restore round trip.
//
// The engine-package matrix (internal/engines/postgres) proves the
// DECODER reads each door correctly. These two prove the value survives
// the orchestrator — which is the half that matters operationally,
// because a bytea that arrives shrunken here is shrunken in the target
// database and, for the backup row, DURABLE in the chain: `restore`
// reproduces the shrunken bytes faithfully and `backup verify` rehashes
// exactly the chunks that carry them, so nothing downstream ever
// disagrees. The independent expected value both tests compare against
// (the 2026-08-01 rule) is the SOURCE server's own
// `encode()`/`octet_length()`/`array_dims()`, never the archive's own
// record of itself.
//
// Reach, stated rather than implied: a `--where` row filter is set, so
// `rawCopyGate`'s G5 refuses the raw-COPY passthrough for the whole run
// and every row goes through the IR. That is the point — "PG→PG raw
// COPY is immune" is true and must NOT be read as "PG→PG is immune":
// the gate refuses raw copy for --redact, --type-override,
// --expr-override, --inject-shard-column and --where alike, and
// `identityProjection` additionally routes any array-carrying table to
// the IR path.
//
// **What these two rows deliberately do NOT cover, and why.** The
// fixture carries only the SCALAR bytea column: the Postgres row writer
// has no `bytea[]` COPY leaf at all and refuses one loudly
// ("array of element type ir.Blob not supported", row_writer.go), so a
// `bytea[]` cannot reach a PG target end-to-end today in any lane. That
// is a loud failure rather than a silent one, and it is a WRITER gap
// outside item 135's decode scope — but it is the reason the array
// leaves are pinned at the reader (internal/engines/postgres) and have
// no src==dst row here. Do not read that absence as the array lane
// being untested; read it as the array lane stopping at the reader.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// byteaProvSeedDDL is the value-shape matrix as a fixture: the four
// COLLISION cells whose server bytes spell `\x`+even-hex (which the
// pre-fix content sniffing shrank), the invalid-hex and odd-length
// neighbours that served as the mutation controls, and the ordinary
// shapes — genuine binary, empty, embedded NUL, NULL.
const byteaProvSeedDDL = `
	CREATE TABLE bytea_prov (
		id BIGINT PRIMARY KEY,
		b  bytea
	);
	INSERT INTO bytea_prov (id, b) VALUES
		(1,  convert_to('\xdead','UTF8')),
		(2,  convert_to('\x','UTF8')),
		(3,  convert_to('\xdeadbeef','UTF8')),
		(4,  convert_to('\xDEAD','UTF8')),
		(5,  convert_to('\xzz','UTF8')),
		(6,  convert_to('\xabc','UTF8')),
		(7,  '\x00ff1080'::bytea),
		(8,  ''::bytea),
		(9,  '\x610062'::bytea),
		(10, NULL);
	ANALYZE bytea_prov;
`

// byteaProvFingerprint asks a server what it actually holds, rendered
// through functions that never touch sluice's decoder. Comparing the
// source's answer to the target's is the src==dst check; comparing the
// source's answer to the RESTORED target's is the backup round trip.
func byteaProvFingerprint(t *testing.T, dsn string) map[int64]string {
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
		       coalesce(encode(b,'hex'), '<NULL>')
		         || ' len=' || coalesce(octet_length(b)::text, '<NULL>')
		FROM bytea_prov
		ORDER BY id`)
	if err != nil {
		t.Fatalf("fingerprint query: %v", err)
	}
	defer func() { _ = rows.Close() }()

	out := map[int64]string{}
	for rows.Next() {
		var (
			id int64
			fp string
		)
		if err := rows.Scan(&id, &fp); err != nil {
			t.Fatalf("fingerprint scan: %v", err)
		}
		out[id] = fp
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("fingerprint iterate: %v", err)
	}
	return out
}

// assertByteaProvFingerprintsEqual reports every diverging row by name,
// so a partial loss is not reported as one opaque mismatch.
func assertByteaProvFingerprintsEqual(t *testing.T, label string, src, dst map[int64]string) {
	t.Helper()
	if len(src) != 10 {
		t.Fatalf("%s: source has %d rows; the fixture seeds 10", label, len(src))
	}
	if len(dst) != len(src) {
		t.Errorf("%s: target has %d rows; source has %d", label, len(dst), len(src))
	}
	for id, want := range src {
		got, ok := dst[id]
		if !ok {
			t.Errorf("%s: id=%d missing on the target", label, id)
			continue
		}
		if got != want {
			t.Errorf("%s: id=%d\n  source %s\n  target %s", label, id, want, got)
		}
	}
}

// TestMigrate_ByteaProvenancePGToPG is the src==dst row: every value
// shape must land on the target byte-identical, on the CHUNKED copy
// path as well as the plain one.
//
// The row count is deliberately small and the thresholds are lowered
// instead: `--bulk-parallel-min-rows` is what decides whether the
// orchestrator drives ReadRowsBatchBounded rather than ReadRows, and
// seeding 100k rows to reach the default would exercise the identical
// reader code at ~5000× the cost. assertChunkFanout is what makes the
// chunked claim non-vacuous — without it this would silently degrade to
// the single-reader path and still pass.
func TestMigrate_ByteaProvenancePGToPG(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	applyPGDDL(t, sourceDSN, byteaProvSeedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	logs := captureSlog(t)
	mig := &Migrator{
		Source:              pgEng,
		Target:              pgEng,
		SourceDSN:           sourceDSN,
		TargetDSN:           targetDSN,
		BulkParallelism:     2,
		BulkParallelMinRows: 1,
		MigrationID:         "test-bytea-provenance",
		// rawCopyGate G5: an all-matching --where refuses the raw-COPY
		// passthrough for the run, so every value goes through the IR
		// decoder this item fixes. Without it a same-engine PG→PG copy
		// would byte-pipe and the test would pin nothing.
		RowFilters: map[string]string{"bytea_prov": "id > 0"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	assertChunkFanout(t, logs.String(), 2)
	assertByteaProvFingerprintsEqual(t, "migrate",
		byteaProvFingerprint(t, sourceDSN), byteaProvFingerprint(t, targetDSN))
}

// TestBackup_ByteaProvenanceChainRoundTripPG is the backup row, and it
// is a separate artifact for a reason: the archive carries whatever the
// reader produced, DURABLY. A shrunken value restores as the shrunken
// value, `backup verify` rehashes the same chunks and reports clean, and
// nothing in the chain ever disagrees with itself. The only independent
// number available is a post-restore read against the SOURCE, which is
// what this asserts.
func TestBackup_ByteaProvenanceChainRoundTripPG(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	applyPGDDL(t, sourceDSN, byteaProvSeedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	if err := (&backup.Backup{
		Source:        pgEng,
		SourceDSN:     sourceDSN,
		Store:         store,
		SluiceVersion: "item-135-test",
	}).Run(ctx); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}

	// backup verify is EVIDENCE-SHARING here and is run only to show it
	// stays green: it rehashes exactly the chunks the manifest lists, so
	// it agrees with a shrunken value as readily as with a correct one.
	total, mismatches, err := backup.VerifyBackup(ctx, store)
	if err != nil {
		t.Fatalf("VerifyBackup: %v", err)
	}
	if mismatches != 0 {
		t.Fatalf("VerifyBackup: %d of %d chunks failed", mismatches, total)
	}

	if err := (&backup.Restore{
		Target:    pgEng,
		TargetDSN: targetDSN,
		Store:     store,
	}).Run(ctx); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}

	assertByteaProvFingerprintsEqual(t, "backup→restore",
		byteaProvFingerprint(t, sourceDSN), byteaProvFingerprint(t, targetDSN))

	// Name the shape the round trip is really guarding, so a future
	// reader does not mistake "verify was green" for evidence.
	if t.Failed() {
		t.Log(fmt.Sprintf(
			"item 135: the chain verified clean (%d chunks) and still restored divergent bytes — "+
				"that combination is exactly why the archive needs an out-of-band check", total,
		))
	}
}
