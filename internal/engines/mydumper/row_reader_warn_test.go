// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mydumper

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// captureSlog swaps the default slog logger for a buffer-backed one for
// the duration of the test. The reader goroutine's writes happen-before
// the row channel closes, so reading the buffer after a full drain is
// race-free.
func captureSlog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

// TestReadRows_FloatDisplayRoundingWarn pins the FLOAT wart's WARN shape
// (ADR-0161 §4): fires exactly once per ReadRows for a table with
// single-precision FLOAT columns, names the table and the FLOAT columns,
// and stays SILENT for DOUBLE — ground-truthed against mydumper v1.0.3,
// which renders DOUBLE at full shortest-roundtrip precision
// (3.141592653589793 / 0.1 / 1.7976931348623157e308 dump exactly) while
// FLOAT display-rounds (8388608 → 8.38861e6).
func TestReadRows_FloatDisplayRoundingWarn(t *testing.T) {
	const warnMarker = "single-precision FLOAT"

	t.Run("fires-once-naming-float-columns", func(t *testing.T) {
		dir := t.TempDir()
		writeDumpFile(t, dir, "metadata", traditionalMetadata)
		writeDumpFile(t, dir, "shop.t-schema.sql",
			"CREATE TABLE `t` (`id` bigint NOT NULL, `f32` float, `f64` double);")
		writeDumpFile(t, dir, "shop.t.00000.sql",
			"/*!40103 SET TIME_ZONE='+00:00' */;\nINSERT INTO `t` VALUES (1,1.5,0.1);")

		table := corpusTableNamed(t, dir)
		logs := captureSlog(t)
		rows := readAllRows(t, dir, table)
		if len(rows) != 1 {
			t.Fatalf("rows = %d; want 1", len(rows))
		}
		out := logs.String()
		if got := strings.Count(out, warnMarker); got != 1 {
			t.Fatalf("FLOAT warn fired %d times; want exactly 1\n%s", got, out)
		}
		if !strings.Contains(out, "table=t") || !strings.Contains(out, "float_columns=[f32]") {
			t.Fatalf("warn must name the table and ONLY the FLOAT columns:\n%s", out)
		}
	})

	t.Run("silent-for-double-only", func(t *testing.T) {
		dir := t.TempDir()
		writeDumpFile(t, dir, "metadata", traditionalMetadata)
		writeDumpFile(t, dir, "shop.t-schema.sql",
			"CREATE TABLE `t` (`id` bigint NOT NULL, `f64` double, `f64b` double);")
		writeDumpFile(t, dir, "shop.t.00000.sql",
			"INSERT INTO `t` VALUES (1,3.141592653589793,0.1);")

		table := corpusTableNamed(t, dir)
		logs := captureSlog(t)
		_ = readAllRows(t, dir, table)
		if strings.Contains(logs.String(), warnMarker) {
			t.Fatalf("FLOAT warn fired for a DOUBLE-only table:\n%s", logs.String())
		}
	})
}

// TestReadRows_MissingTimeZoneHeaderWarn pins the F2b posture: TIMESTAMP
// columns read from chunks that never declared a TIME_ZONE header WARN
// once (the values are interpreted as UTC; a non-UTC producer would have
// shifted them) — and the WARN stays silent when the header is present
// (mydumper v1.0.3 emits it unconditionally, ground-truthed against a
// +08:00 server) or when the table has no TIMESTAMP columns.
func TestReadRows_MissingTimeZoneHeaderWarn(t *testing.T) {
	const warnMarker = "no SET TIME_ZONE header"
	schema := "CREATE TABLE `t` (`id` bigint NOT NULL, `ts` timestamp(6) NULL);"

	t.Run("fires-without-header", func(t *testing.T) {
		dir := t.TempDir()
		writeDumpFile(t, dir, "metadata", traditionalMetadata)
		writeDumpFile(t, dir, "shop.t-schema.sql", schema)
		writeDumpFile(t, dir, "shop.t.00000.sql",
			"INSERT INTO `t` VALUES (1,'2026-01-02 03:04:05.000000');")

		table := corpusTableNamed(t, dir)
		logs := captureSlog(t)
		_ = readAllRows(t, dir, table)
		out := logs.String()
		if got := strings.Count(out, warnMarker); got != 1 {
			t.Fatalf("TZ warn fired %d times; want exactly 1\n%s", got, out)
		}
		if !strings.Contains(out, "timestamp_columns=[ts]") {
			t.Fatalf("warn must name the TIMESTAMP columns:\n%s", out)
		}
	})

	t.Run("silent-with-header", func(t *testing.T) {
		dir := t.TempDir()
		writeDumpFile(t, dir, "metadata", traditionalMetadata)
		writeDumpFile(t, dir, "shop.t-schema.sql", schema)
		writeDumpFile(t, dir, "shop.t.00000.sql",
			"/*!40103 SET TIME_ZONE='+00:00' */;\nINSERT INTO `t` VALUES (1,'2026-01-02 03:04:05.000000');")

		table := corpusTableNamed(t, dir)
		logs := captureSlog(t)
		_ = readAllRows(t, dir, table)
		if strings.Contains(logs.String(), warnMarker) {
			t.Fatalf("TZ warn fired despite a UTC header:\n%s", logs.String())
		}
	})

	t.Run("silent-without-timestamp-columns", func(t *testing.T) {
		dir := t.TempDir()
		writeDumpFile(t, dir, "metadata", traditionalMetadata)
		writeDumpFile(t, dir, "shop.t-schema.sql",
			"CREATE TABLE `t` (`id` bigint NOT NULL, `dt` datetime(6));")
		writeDumpFile(t, dir, "shop.t.00000.sql",
			"INSERT INTO `t` VALUES (1,'2026-01-02 03:04:05.000000');")

		table := corpusTableNamed(t, dir)
		logs := captureSlog(t)
		_ = readAllRows(t, dir, table)
		if strings.Contains(logs.String(), warnMarker) {
			// DATETIME is a wall-clock value with no zone semantics — only
			// TIMESTAMP instants shift with the writing session's zone.
			t.Fatalf("TZ warn fired for a table without TIMESTAMP columns:\n%s", logs.String())
		}
	})

	t.Run("non-utc-session-spelling-refuses", func(t *testing.T) {
		// The gate half of F2: a SESSION-qualified non-UTC header refuses
		// loudly end-to-end (not just in the checkSetStatement unit table).
		dir := t.TempDir()
		writeDumpFile(t, dir, "metadata", traditionalMetadata)
		writeDumpFile(t, dir, "shop.t-schema.sql", schema)
		writeDumpFile(t, dir, "shop.t.00000.sql",
			"SET SESSION TIME_ZONE='+08:00';\nINSERT INTO `t` VALUES (1,'2026-01-02 03:04:05.000000');")

		table := corpusTableNamed(t, dir)
		rr, err := Engine{}.OpenRowReader(context.Background(), dir)
		if err != nil {
			t.Fatal(err)
		}
		ch, err := rr.ReadRows(context.Background(), table)
		if err != nil {
			t.Fatalf("ReadRows: %v", err)
		}
		for range ch { //nolint:revive // drain to completion
		}
		if err := rr.Err(); err == nil || !strings.Contains(err.Error(), "+08:00") {
			t.Fatalf("Err = %v; want a non-UTC TIME_ZONE refusal", err)
		}
	})
}

// TestReadRows_MultiAssignmentSetGate pins audit-2026-07-15 MED-D0-1
// end-to-end: the OBSERVED bypass — a two-assignment SET header whose
// SECOND assignment carries a non-UTC time zone — refuses loudly through
// the real chunk-processing path instead of streaming every TIMESTAMP
// 5.5 hours shifted with exit 0.
func TestReadRows_MultiAssignmentSetGate(t *testing.T) {
	dir := t.TempDir()
	writeDumpFile(t, dir, "metadata", traditionalMetadata)
	writeDumpFile(t, dir, "shop.t-schema.sql",
		"CREATE TABLE `t` (`id` bigint NOT NULL, `ts` timestamp(6) NULL);")
	writeDumpFile(t, dir, "shop.t.00000.sql",
		"SET SESSION sql_mode='', SESSION time_zone='+05:30';\n"+
			"INSERT INTO `t` VALUES (1,'2026-01-02 03:04:05.000000');")

	table := corpusTableNamed(t, dir)
	rr, err := Engine{}.OpenRowReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	ch, err := rr.ReadRows(context.Background(), table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	for range ch { //nolint:revive // drain to completion
	}
	if err := rr.Err(); err == nil || !strings.Contains(err.Error(), "+05:30") {
		t.Fatalf("Err = %v; want a non-UTC TIME_ZONE refusal for the second assignment", err)
	}
}

// TestReadRows_RowCountTripwire pins the MED-D0-2 post-stream tripwire:
// when the dump's own metadata recorded a row count for a table, a full
// read that saw a different number of rows WARNs naming BOTH counts
// (deliberately not a refusal — real-dump metadata fidelity across
// producers is unverified, so the count is a tripwire, not an oracle).
// This is the net that catches a deleted/lost data chunk, which the
// chunk-number-gap WARN can only suspect (mydumper's PK-range numbering
// makes gaps legitimate) and which `verify --depth count` cannot see
// (it re-scans the same directory).
func TestReadRows_RowCountTripwire(t *testing.T) {
	const tripMarker = "different row count"

	build := func(t *testing.T, metadata string) (string, *ir.Table) {
		t.Helper()
		dir := t.TempDir()
		writeDumpFile(t, dir, "metadata", metadata)
		writeDumpFile(t, dir, "shop.t-schema.sql", "CREATE TABLE `t` (`id` bigint NOT NULL);")
		writeDumpFile(t, dir, "shop.t.00000.sql",
			"/*!40103 SET TIME_ZONE='+00:00' */;\nINSERT INTO `t` VALUES (1),(2);")
		return dir, corpusTableNamed(t, dir)
	}
	iniWithRows := func(rows int) string {
		return fmt.Sprintf("[config]\nquote-character = BACKTICK\n\n[source]\nFile = binlog.000007\nPosition = 999\n\n"+
			"[`shop`.`t`]\nreal_table_name=t\nrows = %d\ndata_checksum = 3187537694\n\n"+
			"[`shop`]\nschema_checksum = 95DC8DDE\n", rows)
	}

	t.Run("mismatch-warns-naming-both-counts", func(t *testing.T) {
		dir, table := build(t, iniWithRows(3)) // the dump says 3; the chunks hold 2
		logs := captureSlog(t)
		if got := len(readAllRows(t, dir, table)); got != 2 {
			t.Fatalf("rows = %d; want 2", got)
		}
		out := logs.String()
		if !strings.Contains(out, tripMarker) ||
			!strings.Contains(out, "metadata_rows=3") || !strings.Contains(out, "chunk_rows=2") ||
			!strings.Contains(out, "table=t") {
			t.Fatalf("want a row-count tripwire WARN naming both counts:\n%s", out)
		}
	})

	t.Run("match-is-silent", func(t *testing.T) {
		dir, table := build(t, iniWithRows(2))
		logs := captureSlog(t)
		_ = readAllRows(t, dir, table)
		if strings.Contains(logs.String(), tripMarker) {
			t.Fatalf("tripwire fired on a matching count:\n%s", logs.String())
		}
	})

	t.Run("no-recorded-count-is-silent", func(t *testing.T) {
		dir, table := build(t, traditionalMetadata) // clean dump unchanged
		logs := captureSlog(t)
		_ = readAllRows(t, dir, table)
		if strings.Contains(logs.String(), tripMarker) {
			t.Fatalf("tripwire fired with no recorded count:\n%s", logs.String())
		}
	})

	t.Run("verify-count-door-warns-too", func(t *testing.T) {
		dir, _ := build(t, iniWithRows(3))
		d, err := openDumpDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		logs := captureSlog(t)
		n, err := d.countTableRows(context.Background(), "t")
		if err != nil || n != 2 {
			t.Fatalf("countTableRows = %d, %v; want 2", n, err)
		}
		if !strings.Contains(logs.String(), tripMarker) {
			t.Fatalf("verify count door must trip the same wire:\n%s", logs.String())
		}
	})
}
