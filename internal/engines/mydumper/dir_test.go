// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mydumper

import (
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// writeDumpFile writes one file into dir, compressing per the filename
// suffix so fixtures exercise the same transparent-decompression path
// real compressed dumps take.
func writeDumpFile(t *testing.T, dir, name, content string) {
	t.Helper()
	data := []byte(content)
	switch {
	case strings.HasSuffix(name, ".gz"):
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		if _, err := zw.Write(data); err != nil {
			t.Fatal(err)
		}
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}
		data = buf.Bytes()
	case strings.HasSuffix(name, ".zst"):
		var buf bytes.Buffer
		zw, err := zstd.NewWriter(&buf)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := zw.Write(data); err != nil {
			t.Fatal(err)
		}
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}
		data = buf.Bytes()
	}
	if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
		t.Fatal(err)
	}
}

const traditionalMetadata = `Started dump at: 2026-07-14 10:00:00
SHOW MASTER STATUS:
	Log: mysql-bin.000002
	Pos: 12345
	GTID:00000000-0000-0000-0000-000000000001:1-42

Finished dump at: 2026-07-14 10:00:05
`

const iniMetadata = `[config]
quote_character = BACKTICK

[master]
# Channel_Name = ''
File = binlog.000007
Position = 999
Executed_Gtid_Set = 3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5
`

func TestOpenDumpDir_LayoutAndMetadata(t *testing.T) {
	dir := t.TempDir()
	writeDumpFile(t, dir, "metadata", traditionalMetadata)
	writeDumpFile(t, dir, "shop-schema-create.sql", "CREATE DATABASE `shop`;")
	writeDumpFile(t, dir, "shop.users-schema.sql", "CREATE TABLE `users` (`id` bigint NOT NULL);")
	writeDumpFile(t, dir, "shop.users.00000.sql", "INSERT INTO `users` VALUES (1);")
	writeDumpFile(t, dir, "shop.users.00001.sql", "INSERT INTO `users` VALUES (2);")
	writeDumpFile(t, dir, "shop.empty-schema.sql.gz", "CREATE TABLE `empty` (`id` bigint NOT NULL);")
	writeDumpFile(t, dir, "shop.users-metadata", "2\n")

	d, err := openDumpDir(dir)
	if err != nil {
		t.Fatalf("openDumpDir: %v", err)
	}
	if d.database != "shop" {
		t.Fatalf("database = %q", d.database)
	}
	if len(d.tableOrder) != 2 || d.tableOrder[0] != "empty" || d.tableOrder[1] != "users" {
		t.Fatalf("tableOrder = %v", d.tableOrder)
	}
	if got := len(d.tables["users"].chunks); got != 2 {
		t.Fatalf("users chunks = %d", got)
	}
	if got := len(d.tables["empty"].chunks); got != 0 {
		t.Fatalf("empty chunks = %d", got)
	}
	if d.binlogFile != "mysql-bin.000002" || d.binlogPos != "12345" ||
		d.gtidSet != "00000000-0000-0000-0000-000000000001:1-42" {
		t.Fatalf("metadata = %q %q %q", d.binlogFile, d.binlogPos, d.gtidSet)
	}
}

func TestParseMetadata_IniShape(t *testing.T) {
	dir := t.TempDir()
	writeDumpFile(t, dir, "metadata", iniMetadata)
	writeDumpFile(t, dir, "shop.t-schema.sql", "CREATE TABLE `t` (`id` bigint NOT NULL);")
	d, err := openDumpDir(dir)
	if err != nil {
		t.Fatalf("openDumpDir: %v", err)
	}
	if d.binlogFile != "binlog.000007" || d.binlogPos != "999" ||
		d.gtidSet != "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5" {
		t.Fatalf("metadata = %q %q %q", d.binlogFile, d.binlogPos, d.gtidSet)
	}
}

// TestOpenDumpDir_Refusals pins the detect-at-open contract: anything that
// is not a completed single-database mydumper dump refuses loudly.
func TestOpenDumpDir_Refusals(t *testing.T) {
	schema := "CREATE TABLE `t` (`id` bigint NOT NULL);"
	cases := []struct {
		name    string
		build   func(t *testing.T, dir string)
		wantErr string
	}{
		{"empty-dir", func(_ *testing.T, _ string) {}, "no `metadata` file"},
		{"no-metadata", func(t *testing.T, dir string) {
			writeDumpFile(t, dir, "shop.t-schema.sql", schema)
		}, "no `metadata` file"},
		{"no-schema-files", func(t *testing.T, dir string) {
			writeDumpFile(t, dir, "metadata", traditionalMetadata)
		}, "no `<db>.<table>-schema.sql` files"},
		{"partial-dump", func(t *testing.T, dir string) {
			writeDumpFile(t, dir, "metadata", traditionalMetadata)
			writeDumpFile(t, dir, "metadata.partial", "in flight")
			writeDumpFile(t, dir, "shop.t-schema.sql", schema)
		}, "did not complete"},
		{"stranger-file", func(t *testing.T, dir string) {
			writeDumpFile(t, dir, "metadata", traditionalMetadata)
			writeDumpFile(t, dir, "shop.t-schema.sql", schema)
			writeDumpFile(t, dir, "notes.txt", "hello")
		}, "does not recognise"},
		{"multi-database", func(t *testing.T, dir string) {
			writeDumpFile(t, dir, "metadata", traditionalMetadata)
			writeDumpFile(t, dir, "shop.t-schema.sql", schema)
			writeDumpFile(t, dir, "other.u-schema.sql", schema)
		}, "2 databases"},
		{"chunk-without-schema", func(t *testing.T, dir string) {
			writeDumpFile(t, dir, "metadata", traditionalMetadata)
			writeDumpFile(t, dir, "shop.t-schema.sql", schema)
			writeDumpFile(t, dir, "shop.ghost.00000.sql", "INSERT INTO `ghost` VALUES (1);")
		}, "no matching"},
		{"subdirectory", func(t *testing.T, dir string) {
			writeDumpFile(t, dir, "metadata", traditionalMetadata)
			writeDumpFile(t, dir, "shop.t-schema.sql", schema)
			if err := os.Mkdir(filepath.Join(dir, "sub"), 0o755); err != nil {
				t.Fatal(err)
			}
		}, "unexpected subdirectory"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			tc.build(t, dir)
			_, err := openDumpDir(dir)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v; want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestOpenDumpDir_NotADirectory(t *testing.T) {
	// A mysqldump-signature file now gets the recipe-bearing foreign-dump
	// refusal instead (ADR-0163, pinned in dir_wrong_driver_test.go); an
	// unrecognised file keeps the generic not-a-directory shape.
	dir := t.TempDir()
	file := filepath.Join(dir, "dump.sql")
	if err := os.WriteFile(file, []byte("SELECT 1; -- not any recognised dump signature"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := openDumpDir(file); err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("err = %v; want a not-a-directory refusal", err)
	}
}

func TestSortChunks_NumericOrder(t *testing.T) {
	chunks := []string{
		"/d/shop.t.00010.sql",
		"/d/shop.t.00002.sql.gz",
		"/d/shop.t.100000.sql", // widened field past 99999
		"/d/shop.t.00000.sql",
	}
	sortChunks(chunks)
	want := []string{
		"/d/shop.t.00000.sql",
		"/d/shop.t.00002.sql.gz",
		"/d/shop.t.00010.sql",
		"/d/shop.t.100000.sql",
	}
	for i := range want {
		if chunks[i] != want[i] {
			t.Fatalf("chunks = %v; want %v", chunks, want)
		}
	}
}

// TestOpenDumpDir_ChunkNumberGapWarn pins the MED-D0-2 posture, ground-
// truthed against real mydumper v1.0.3: chunk numbers are PK-RANGE-derived,
// not sequential — a table with PKs 1..500 + 90000000..90000500 dumped
// under `-r 200` as chunks 00001-00003 + 450001-450003, and `-r` dumps
// start at 00001 while unsplit tables start at 00000. A numbering gap is
// therefore a WARN (naming the gap), NOT the torn-dump refusal the
// metadata.partial marker gets: gaps can be legitimate, but a deleted
// middle chunk looks identical, and before this WARN it streamed silently
// short. The decisive cross-check is the row-count tripwire
// (TestReadRows_RowCountTripwire).
func TestOpenDumpDir_ChunkNumberGapWarn(t *testing.T) {
	const gapMarker = "not contiguous"
	build := func(t *testing.T, nums ...string) string {
		t.Helper()
		dir := t.TempDir()
		writeDumpFile(t, dir, "metadata", traditionalMetadata)
		writeDumpFile(t, dir, "shop.users-schema.sql", "CREATE TABLE `users` (`id` bigint NOT NULL);")
		for _, n := range nums {
			writeDumpFile(t, dir, "shop.users."+n+".sql", "INSERT INTO `users` VALUES (1);")
		}
		return dir
	}

	t.Run("gap-warns-and-still-opens", func(t *testing.T) {
		// The observed audit repro: chunks 00000+00002 with 00001 deleted.
		dir := build(t, "00000", "00002")
		logs := captureSlog(t)
		if _, err := openDumpDir(dir); err != nil {
			t.Fatalf("openDumpDir: %v (a gap is a WARN, not a refusal)", err)
		}
		out := logs.String()
		if !strings.Contains(out, gapMarker) || !strings.Contains(out, "table=users") ||
			!strings.Contains(out, "gap_after_chunk=0") || !strings.Contains(out, "next_chunk=2") {
			t.Fatalf("want a chunk-gap WARN naming the gap:\n%s", out)
		}
	})

	t.Run("contiguous-is-silent", func(t *testing.T) {
		dir := build(t, "00000", "00001", "00002")
		logs := captureSlog(t)
		if _, err := openDumpDir(dir); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(logs.String(), gapMarker) {
			t.Fatalf("gap WARN fired on a contiguous dump:\n%s", logs.String())
		}
	})

	t.Run("nonzero-start-is-silent", func(t *testing.T) {
		// Real `-r` dumps start at 00001 (ground truth), so a missing FIRST
		// chunk is structurally indistinguishable from a legitimate dump —
		// only middle gaps warn; the row-count tripwire nets the rest.
		dir := build(t, "00001", "00002")
		logs := captureSlog(t)
		if _, err := openDumpDir(dir); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(logs.String(), gapMarker) {
			t.Fatalf("gap WARN fired for a non-zero start:\n%s", logs.String())
		}
	})

	t.Run("sparse-pk-shape-warns", func(t *testing.T) {
		// The legitimate sparse-PK shape ALSO warns (it is honest about not
		// being able to tell) — the message says gaps can be legitimate.
		dir := build(t, "00001", "00002", "00003", "450001", "450002")
		logs := captureSlog(t)
		if _, err := openDumpDir(dir); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(logs.String(), gapMarker) {
			t.Fatalf("want the gap WARN for the sparse-PK shape:\n%s", logs.String())
		}
	})
}

// TestOpenDumpDir_MetadataRowCounts pins both sources of the dump's own
// per-table row counts feeding the tripwire: the ini `rows =` entries in
// the dump-wide metadata (mydumper ≥0.12 — the section/key shape below is
// the v1.0.3 ground truth, backtick-quoted sections and all) and the
// bare-integer per-table `-metadata` companion (older mydumper), with
// pscale-dump's EMPTY companion ignored leniently.
func TestOpenDumpDir_MetadataRowCounts(t *testing.T) {
	const schema = "CREATE TABLE `users` (`id` bigint NOT NULL);"
	// Mirrors a real v1.0.3 metadata file: bookkeeping sections, a
	// commented [source] position, the table section with checksums, the
	// db-only checksum section, and a trailing [config].
	iniWithRows := "# Started dump at: 2026-07-15 10:00:00\n" +
		"[config]\nquote-character = BACKTICK\n\n" +
		"[source]\n# SOURCE_LOG_FILE = \"binlog.000002\"\n# SOURCE_LOG_POS = 47893\n\n" +
		"[`shop`.`users`]\nreal_table_name=users\nrows = 7\ndata_checksum = 3187537694\n\n" +
		"[`shop`]\nschema_checksum = 95DC8DDE\n" +
		"[config]\nmax-statement-size = 12279\n" +
		"# Finished dump at: 2026-07-15 10:00:01\n"

	open := func(t *testing.T, files map[string]string) *dumpDir {
		t.Helper()
		dir := t.TempDir()
		for name, content := range files {
			writeDumpFile(t, dir, name, content)
		}
		d, err := openDumpDir(dir)
		if err != nil {
			t.Fatalf("openDumpDir: %v", err)
		}
		return d
	}

	t.Run("ini-rows", func(t *testing.T) {
		d := open(t, map[string]string{"metadata": iniWithRows, "shop.users-schema.sql": schema})
		tf := d.tables["users"]
		if !tf.hasMetadataRows || tf.metadataRows != 7 {
			t.Fatalf("metadataRows = %d,%v; want 7,true", tf.metadataRows, tf.hasMetadataRows)
		}
	})

	t.Run("unquoted-section", func(t *testing.T) {
		d := open(t, map[string]string{
			"metadata":              "[shop.users]\nrows = 4\n",
			"shop.users-schema.sql": schema,
		})
		tf := d.tables["users"]
		if !tf.hasMetadataRows || tf.metadataRows != 4 {
			t.Fatalf("metadataRows = %d,%v; want 4,true", tf.metadataRows, tf.hasMetadataRows)
		}
	})

	t.Run("foreign-db-section-ignored", func(t *testing.T) {
		d := open(t, map[string]string{
			"metadata":              "[`other`.`users`]\nrows = 9\n",
			"shop.users-schema.sql": schema,
		})
		if tf := d.tables["users"]; tf.hasMetadataRows {
			t.Fatalf("metadataRows = %d; want none (section names another database)", tf.metadataRows)
		}
	})

	t.Run("companion-bare-integer", func(t *testing.T) {
		d := open(t, map[string]string{
			"metadata":              traditionalMetadata,
			"shop.users-schema.sql": schema,
			"shop.users-metadata":   "2\n",
		})
		tf := d.tables["users"]
		if !tf.hasMetadataRows || tf.metadataRows != 2 {
			t.Fatalf("metadataRows = %d,%v; want 2,true", tf.metadataRows, tf.hasMetadataRows)
		}
	})

	t.Run("empty-companion-ignored", func(t *testing.T) {
		// pscale-dump writes the companion but leaves it empty (Bug 188
		// probe ground truth) — no count, no refusal.
		d := open(t, map[string]string{
			"metadata":              traditionalMetadata,
			"shop.users-schema.sql": schema,
			"shop.users-metadata":   "",
		})
		if tf := d.tables["users"]; tf.hasMetadataRows {
			t.Fatalf("metadataRows = %d; want none (empty companion)", tf.metadataRows)
		}
	})

	t.Run("ini-overrides-companion", func(t *testing.T) {
		d := open(t, map[string]string{
			"metadata":              iniWithRows,
			"shop.users-schema.sql": schema,
			"shop.users-metadata":   "2\n",
		})
		tf := d.tables["users"]
		if !tf.hasMetadataRows || tf.metadataRows != 7 {
			t.Fatalf("metadataRows = %d,%v; want the ini count 7", tf.metadataRows, tf.hasMetadataRows)
		}
	})

	t.Run("position-parse-unchanged", func(t *testing.T) {
		// The section-aware pass must not disturb the [master]/[source]
		// position parse (TestParseMetadata_IniShape covers the plain
		// shape; this pins it alongside table sections).
		d := open(t, map[string]string{
			"metadata":              iniMetadata + "\n[`shop`.`users`]\nrows = 7\n",
			"shop.users-schema.sql": schema,
		})
		if d.binlogFile != "binlog.000007" || d.binlogPos != "999" {
			t.Fatalf("position = %q %q; want binlog.000007 999", d.binlogFile, d.binlogPos)
		}
	})
}
