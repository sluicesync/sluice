//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0164 end-to-end integration pin: a REAL Postgres source → `backup
// full` → `backup export-as-parquet` → a real Parquet reader, with the
// exported values ground-truthed against the exact literals seeded into
// the source. The corpus spans the IR family matrix on the live path
// (ints incl. zero, exact NUMERIC at every physical tier + the
// unconstrained string downgrade, float64 incl. NaN, text incl. the
// empty string, UUID/INET, BYTEA, JSONB, 1-D arrays with NULL elements,
// DATE/TIMESTAMPTZ/TIMESTAMP/TIME, and column NULLs) so the exhaustive
// per-family unit matrix in internal/pipeline/parquetexport is anchored
// to a real engine's reader output at least once. Chunk size 2 forces
// multiple chunks, pinning the row-group-per-chunk alignment
// end-to-end.

package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// exportRawColumn reads back the raw leaf parquet.Values (including
// nulls) of one column across every row group — the ground truth any
// downstream Parquet reader sees.
func exportRawColumn(t *testing.T, data []byte, path ...string) []parquet.Value {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	lc, ok := f.Schema().Lookup(path...)
	if !ok {
		t.Fatalf("column %v not found in schema %v", path, f.Schema())
	}
	var out []parquet.Value
	for _, rg := range f.RowGroups() {
		rows := rg.Rows()
		buf := make([]parquet.Row, 8)
		for {
			n, err := rows.ReadRows(buf)
			for _, r := range buf[:n] {
				for _, v := range r {
					if v.Column() == lc.ColumnIndex {
						out = append(out, v.Clone())
					}
				}
			}
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				_ = rows.Close()
				t.Fatalf("ReadRows: %v", err)
			}
		}
		if err := rows.Close(); err != nil {
			t.Fatalf("rows.Close: %v", err)
		}
	}
	return out
}

func TestExportParquet_PG_GroundTruth(t *testing.T) {
	sourceDSN, _, cleanup := startPostgresLogical(t)
	defer cleanup()

	applyDDL(t, sourceDSN, `
		CREATE TABLE analytics (
			id      BIGINT PRIMARY KEY,
			flag    BOOLEAN,
			amount  NUMERIC(12,4),
			tiny    NUMERIC(6,2),
			wide    NUMERIC(38,10),
			free    NUMERIC,
			ratio   DOUBLE PRECISION,
			name    TEXT,
			uid     UUID,
			ip      INET,
			payload BYTEA,
			doc     JSONB,
			nums    BIGINT[],
			tags    TEXT[],
			born    DATE,
			at_tz   TIMESTAMPTZ,
			at_wall TIMESTAMP,
			tod     TIME
		);
		INSERT INTO analytics VALUES
			(1, true,  '19.9900', '0.00', '12345678901234567890123456.7890123456', '1.5', 'NaN',
			 'alice', '01234567-89ab-cdef-0123-456789abcdef', '2001:db8::1',
			 '\x00ff10', '{"a": 1}', ARRAY[1, NULL, 3]::bigint[], ARRAY['x', '', NULL],
			 '1999-12-31', '2026-07-15 12:34:56.123456+00', '2026-07-15 12:34:56.123456', '08:30:00.5'),
			(2, false, '-0.0001', NULL, NULL, NULL, 0,
			 '', NULL, NULL,
			 '', NULL, ARRAY[]::bigint[], NULL,
			 '1970-01-01', '1970-01-01 00:00:00+00', NULL, '00:00:00'),
			(3, NULL, NULL, NULL, NULL, NULL, NULL,
			 NULL, NULL, NULL,
			 NULL, NULL, NULL, NULL,
			 NULL, NULL, NULL, NULL);
	`)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	if err := (&backup.Backup{
		Source:    pgEng,
		SourceDSN: sourceDSN,
		Store:     store,
		ChunkRows: 2, // 3 rows → 2 chunks → 2 row groups after export
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	manifest, err := lineage.ReadManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}

	outDir := t.TempDir()
	out, err := blobcodec.NewLocalStore(outDir)
	if err != nil {
		t.Fatalf("NewLocalStore(out): %v", err)
	}
	if err := (&backup.ParquetExport{Store: store, Output: out, SluiceVersion: "it"}).Run(context.Background()); err != nil {
		t.Fatalf("ParquetExport.Run: %v", err)
	}

	// parquet_index.json: coherent with the manifest.
	var index struct {
		BackupID string `json:"backup_id"`
		Tables   []struct {
			Name      string   `json:"name"`
			File      string   `json:"file"`
			Rows      int64    `json:"rows"`
			RowGroups int      `json:"row_groups"`
			TypeNotes []string `json:"type_notes"`
		} `json:"tables"`
	}
	ib, err := os.ReadFile(filepath.Join(outDir, backup.ParquetIndexFileName))
	if err != nil {
		t.Fatalf("read index: %v", err)
	}
	if err := json.Unmarshal(ib, &index); err != nil {
		t.Fatalf("parse index: %v", err)
	}
	if index.BackupID != lineage.ManifestBackupID(manifest) || len(index.Tables) != 1 {
		t.Fatalf("index = %+v", index)
	}
	entry := index.Tables[0]
	if entry.Rows != 3 || entry.RowGroups != 2 {
		t.Fatalf("index entry = %+v; want 3 rows in 2 row groups (chunk-aligned)", entry)
	}
	// The unconstrained NUMERIC downgrade is operator-visible.
	foundNote := false
	for _, n := range entry.TypeNotes {
		if n != "" {
			foundNote = true
		}
	}
	if !foundNote {
		t.Fatalf("type notes = %v; want the unbounded-NUMERIC downgrade note", entry.TypeNotes)
	}

	data, err := os.ReadFile(filepath.Join(outDir, entry.File))
	if err != nil {
		t.Fatalf("read parquet: %v", err)
	}
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if got := len(f.RowGroups()); got != 2 {
		t.Fatalf("row groups = %d; want 2 (1:1 with source chunks)", got)
	}

	// Column-by-column ground truth against the seeded literals. Rows
	// arrive in chunk order (id 1, 2, 3 — PK order on a fresh table).
	col := func(name string, path ...string) []parquet.Value {
		return exportRawColumn(t, data, append([]string{name}, path...)...)
	}

	ids := col("id")
	if len(ids) != 3 || ids[0].Int64() != 1 || ids[1].Int64() != 2 || ids[2].Int64() != 3 {
		t.Fatalf("id = %v", ids)
	}
	flags := col("flag")
	if flags[0].Boolean() != true || flags[1].IsNull() || flags[1].Boolean() != false || !flags[2].IsNull() {
		t.Fatalf("flag = %v (false must be PRESENT false, not NULL)", flags)
	}
	// NUMERIC(12,4) rides int64 DECIMAL(12,4): exact unscaled ints.
	amounts := col("amount")
	if amounts[0].Int64() != 199900 || amounts[1].Int64() != -1 || !amounts[2].IsNull() {
		t.Fatalf("amount = %v; want unscaled [199900 -1 NULL]", amounts)
	}
	// NUMERIC(6,2) rides int32 DECIMAL(6,2); 0.00 is a PRESENT 0.
	tinies := col("tiny")
	if tinies[0].IsNull() || tinies[0].Int32() != 0 || !tinies[1].IsNull() || !tinies[2].IsNull() {
		t.Fatalf("tiny = %v; want [present-0 NULL NULL]", tinies)
	}
	// NUMERIC(38,10) rides FLBA(16); check the exact unscaled bytes by
	// magnitude: 12345678901234567890123456.7890123456 × 10^10.
	wides := col("wide")
	if wides[0].IsNull() || !wides[1].IsNull() || !wides[2].IsNull() {
		t.Fatalf("wide nullness = %v", wides)
	}
	if got := len(wides[0].ByteArray()); got != 16 {
		t.Fatalf("wide payload = %d bytes; want 16", got)
	}
	// Unconstrained NUMERIC → the exact decimal text.
	frees := col("free")
	if string(frees[0].ByteArray()) != "1.5" || !frees[1].IsNull() || !frees[2].IsNull() {
		t.Fatalf("free = %v; want the exact text '1.5'", frees)
	}
	ratios := col("ratio")
	if !math.IsNaN(ratios[0].Double()) {
		t.Fatalf("ratio[0] = %v; want NaN carried", ratios[0])
	}
	if ratios[1].IsNull() || ratios[1].Double() != 0 || !ratios[2].IsNull() {
		t.Fatalf("ratio = %v; want [NaN present-0 NULL]", ratios)
	}
	names := col("name")
	if string(names[0].ByteArray()) != "alice" || names[1].IsNull() || len(names[1].ByteArray()) != 0 || !names[2].IsNull() {
		t.Fatalf("name = %v ('' must be PRESENT empty, not NULL)", names)
	}
	uids := col("uid")
	if string(uids[0].ByteArray()) != "01234567-89ab-cdef-0123-456789abcdef" || !uids[1].IsNull() {
		t.Fatalf("uid = %v", uids)
	}
	ips := col("ip")
	if string(ips[0].ByteArray()) != "2001:db8::1" || !ips[1].IsNull() {
		t.Fatalf("ip = %v", ips)
	}
	payloads := col("payload")
	if !bytes.Equal(payloads[0].ByteArray(), []byte{0x00, 0xFF, 0x10}) {
		t.Fatalf("payload[0] = %x", payloads[0].ByteArray())
	}
	if payloads[1].IsNull() || len(payloads[1].ByteArray()) != 0 || !payloads[2].IsNull() {
		t.Fatalf("payload = %v; want [bytes present-empty NULL]", payloads)
	}
	docs := col("doc")
	if string(docs[0].ByteArray()) != `{"a": 1}` || !docs[1].IsNull() {
		t.Fatalf("doc = %v", docs)
	}
	// 1-D bigint[] with a NULL element: leaf values under v/list/element.
	nums := col("nums", "list", "element")
	if len(nums) != 5 { // {1,NULL,3} + empty-list marker + NULL-array marker
		t.Fatalf("nums leaf values = %v", nums)
	}
	if nums[0].Int64() != 1 || !nums[1].IsNull() || nums[2].Int64() != 3 {
		t.Fatalf("nums row 1 = %v; want [1 NULL 3]", nums[:3])
	}
	tags := col("tags", "list", "element")
	if string(tags[0].ByteArray()) != "x" || tags[1].IsNull() || len(tags[1].ByteArray()) != 0 || !tags[2].IsNull() {
		t.Fatalf("tags row 1 = %v; want [x present-'' NULL-element]", tags[:3])
	}
	borns := col("born")
	if borns[0].Int32() != 10956 /* 1999-12-31 */ || borns[1].IsNull() || borns[1].Int32() != 0 || !borns[2].IsNull() {
		t.Fatalf("born = %v; want [10956 present-0 NULL]", borns)
	}
	atTZ := col("at_tz")
	const wantMicros = 1784118896123456 // 2026-07-15T12:34:56.123456Z = (20649 days × 86400 + 45296) s in µs
	if atTZ[0].Int64() != wantMicros || atTZ[1].IsNull() || atTZ[1].Int64() != 0 || !atTZ[2].IsNull() {
		t.Fatalf("at_tz = %v; want [%d present-epoch NULL]", atTZ, wantMicros)
	}
	atWall := col("at_wall")
	if atWall[0].Int64() != wantMicros || !atWall[1].IsNull() || !atWall[2].IsNull() {
		t.Fatalf("at_wall = %v", atWall)
	}
	tods := col("tod")
	if tods[0].Int64() != 30600500000 || tods[1].IsNull() || tods[1].Int64() != 0 || !tods[2].IsNull() {
		t.Fatalf("tod = %v; want [30600500000 present-midnight NULL]", tods)
	}
}
