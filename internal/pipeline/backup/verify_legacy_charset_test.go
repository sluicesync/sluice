// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-37 (j) re-review item 3: `backup verify` logs the same
// LEGACY-CHARSET-INCREMENT WARN as chain restore and sync from-backup. An
// incremental a pre-v0.156.3 sluice captured from a MySQL-family source
// hashes clean while holding U+FFFD (or a different character) for a
// non-UTF-8 column's non-ASCII values, so without it verify would report
// such a chain sound with nothing to say it is not.

package backup

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/logcapture"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// writeLegacyCharsetChain writes a MySQL full plus one incremental, both
// stamped with version, over a schema carrying a latin1 column.
func writeLegacyCharsetChain(t *testing.T, version string) irbackup.Store {
	t.Helper()
	ctx := context.Background()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema := func() *ir.Schema {
		return &ir.Schema{Tables: []*ir.Table{{
			Name: "people",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "name", Type: ir.Varchar{Length: 32, Charset: "latin1"}},
			},
			PrimaryKey: &ir.Index{Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
		}}}
	}
	hash, err := irbackup.ComputeSchemaHash(schema())
	if err != nil {
		t.Fatalf("ComputeSchemaHash: %v", err)
	}
	full := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC),
		SourceEngine:  "mysql",
		SluiceVersion: version,
		Kind:          irbackup.BackupKindFull,
		Schema:        schema(),
		SchemaHash:    hash,
		Tables:        []*irbackup.TableManifest{{Name: "people", RowCount: 0}},
	}
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("write full: %v", err)
	}
	_ = lineage.UpdateLineageForManifestBestEffort(ctx, store, full, lineage.ManifestFileName, blobcodec.CodecGzip)
	incr := &irbackup.Manifest{
		FormatVersion:  irbackup.BackupFormatVersion,
		CreatedAt:      time.Date(2026, 9, 20, 1, 0, 0, 0, time.UTC),
		SourceEngine:   "mysql",
		SluiceVersion:  version,
		Kind:           irbackup.BackupKindIncremental,
		ParentBackupID: full.BackupID,
		Schema:         schema(),
		SchemaHash:     hash,
	}
	incr.BackupID = irbackup.ComputeBackupID(incr)
	path := lineage.IncrementalManifestPrefix + "incr-0001.json"
	if err := lineage.WriteManifestAt(ctx, store, path, incr); err != nil {
		t.Fatalf("write incremental: %v", err)
	}
	_ = lineage.UpdateLineageForManifestBestEffort(ctx, store, incr, path, blobcodec.CodecGzip)
	return store
}

func TestVerifyBackup_LegacyCharsetIncrement_Warns(t *testing.T) {
	verify := func(version string) string {
		store := writeLegacyCharsetChain(t, version)
		prev := slog.Default()
		defer slog.SetDefault(prev)
		var buf logcapture.Buffer
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
		if _, err := VerifyBackupCodedReport(context.Background(), store, VerifyOptions{}); err != nil {
			t.Fatalf("%s chain: verify must still pass (a WARN, not a refusal): %v", version, err)
		}
		return buf.String()
	}

	out := verify("0.156.2")
	if n := strings.Count(out, migcore.LegacyCharsetIncrementMarker); n != 1 {
		t.Fatalf("pre-fix chain: %d %s WARNs; want exactly 1 (the incremental, not the full). log:\n%s",
			n, migcore.LegacyCharsetIncrementMarker, out)
	}
	if !strings.Contains(out, "backup verify") || !strings.Contains(out, "name (latin1)") {
		t.Errorf("the WARN does not name its origin and the column: %s", out)
	}
	if out := verify("0.156.3"); strings.Contains(out, migcore.LegacyCharsetIncrementMarker) {
		t.Errorf("a chain captured by a fixed release WARNed: %s", out)
	}
}
