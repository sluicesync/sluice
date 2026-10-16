//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/engines"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// TestBackupSegment_HiddenCollationStamp pins FormatVersionHiddenCollation
// on the two CDC-segment writers that swap in a window-end schema after the
// manifest was built (sixth review F3/F4): `backup incremental`
// (attachWindowEndSchema) and a `backup stream` rollover
// (refreshSchemaAndAttachDelta).
//
// The shape is a chain begun before the upgrade. The source has a collated
// functional key, but the full is rewritten as a pre-v0.161.0 binary wrote
// it — no hidden-column collation, its format version, fingerprint and id
// re-derived — so the segment's version, derived from its parent's schema,
// starts below 15. The window adds a column, which is a schema delta, so the
// writer swaps in the window-end schema it just read; that schema carries
// the key's hidden-column collation, and the segment must be stamped 15
// (else an older binary reads a manifest whose recorded fingerprint folds a
// field it drops) and still read back through the lineage door that refuses
// an unstamped carrier. The independent expected value is the source
// catalog: the key exists there, so the window-end read carries it.
func TestBackupSegment_HiddenCollationStamp(t *testing.T) {
	_, myRoot, cleanup := startMySQL(t)
	defer cleanup()
	myEng, _ := engines.Get("mysql")
	root := twfbDB{engine: "mysql", dsn: myRoot}
	for i, lane := range []string{"backup-incremental", "backup-stream-rollover"} {
		t.Run(lane, func(t *testing.T) {
			ctx := context.Background()
			srcDB := fmt.Sprintf("hcst_s%d", i)
			root.exec(t, "CREATE DATABASE "+srcDB)
			srcDSN, err := buildMySQLDSN(myRoot, srcDB)
			if err != nil {
				t.Fatal(err)
			}
			src := twfbDB{engine: "mysql", dsn: srcDSN}
			src.exec(t, "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) CHARACTER SET utf8mb4 "+
				"COLLATE utf8mb4_0900_ai_ci NOT NULL, v INT, UNIQUE KEY fk ((cast(k as char(10))))); "+
				"INSERT INTO kd VALUES (1, 'a', 1)")
			store, err := blobcodec.NewLocalStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := (&backup.Backup{Source: myEng, SourceDSN: srcDSN, Store: store, SluiceVersion: "test"}).Run(ctx); err != nil {
				t.Fatalf("full: %v", err)
			}
			full, err := lineage.ReadManifest(ctx, store)
			if err != nil {
				t.Fatal(err)
			}
			if full.FormatVersion != irbackup.FormatVersionHiddenCollation {
				t.Fatalf("this binary's full of a functional key is at %d; want %d", full.FormatVersion, irbackup.FormatVersionHiddenCollation)
			}
			// What a pre-v0.161.0 binary wrote for the same source.
			full.Schema = irbackup.WithoutHiddenCollation(full.Schema)
			full.FormatVersion = irbackup.FormatVersionFor(full.Schema)
			if full.SchemaHash, err = irbackup.ComputeSchemaHash(full.Schema); err != nil {
				t.Fatal(err)
			}
			full.BackupID = irbackup.ComputeBackupID(full)
			if err := lineage.WriteManifest(ctx, store, full); err != nil {
				t.Fatal(err)
			}
			if full.FormatVersion >= irbackup.FormatVersionHiddenCollation {
				t.Fatalf("the rewritten full is at %d; the cell starts at the tier and measures nothing", full.FormatVersion)
			}
			window := []string{"ALTER TABLE kd ADD COLUMN w INT", "INSERT INTO kd VALUES (2, 'b', 2, 2)"}
			if err := krwCapture(t, lane, myEng, srcDSN, "", store, full.BackupID, src, window); err != nil {
				t.Fatalf("capture: %v", err)
			}
			// The manifests as written, decoded raw: the lineage read refuses
			// an unstamped carrier itself, which would hide the stamp's own
			// failure behind the reverse door's.
			paths, err := store.List(ctx, lineage.IncrementalManifestPrefix)
			if err != nil {
				t.Fatal(err)
			}
			carriers := 0
			for _, p := range append([]string{lineage.ManifestFileName}, paths...) {
				m := hcstRawManifest(ctx, t, store, p)
				if m == nil || !irbackup.ManifestCarriesHiddenCollation(m) {
					continue
				}
				carriers++
				if m.FormatVersion < irbackup.FormatVersionHiddenCollation {
					t.Errorf("%s carries a hidden-column collation at format version %d; want %d",
						p, m.FormatVersion, irbackup.FormatVersionHiddenCollation)
				}
				if m.Kind == irbackup.BackupKindFull {
					t.Errorf("%s: the rewritten full carries a hidden-column collation", p)
				}
			}
			if carriers == 0 {
				t.Fatalf("no segment of the chain carries the functional key's hidden-column collation (%v) — "+
					"the window's delta did not swap in the window-end schema, so the cell measures nothing", paths)
			}
			if _, err := lineage.ListAllManifestsViaWalk(ctx, store); err != nil {
				t.Fatalf("this binary cannot read its own chain back: %v", err)
			}
		})
	}
}

// hcstRawManifest decodes the manifest at path as written, bypassing every
// read-side check; nil for a path that is not a manifest JSON.
func hcstRawManifest(ctx context.Context, t *testing.T, store irbackup.Store, path string) *irbackup.Manifest {
	t.Helper()
	if !strings.HasSuffix(path, ".json") {
		return nil
	}
	rc, err := store.Get(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rc.Close() }()
	var m irbackup.Manifest
	if err := json.NewDecoder(rc).Decode(&m); err != nil {
		return nil
	}
	return &m
}
