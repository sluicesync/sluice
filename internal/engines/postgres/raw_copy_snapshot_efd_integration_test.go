//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The ambient-join × efd=0 cell (v265 review nit). The Bug 194 efd matrix
// (pipeline migrate_raw_copy_float_efd_pg_integration_test.go) drives the
// raw text lane's *sql.DB branch, and the pgbouncer rig drives the
// transaction-mode pooler — but nothing drove the SNAPSHOT-TRANSACTION
// branch of rawCopyWithSessionPins: the sync cold-start reader is pinned
// inside the exported-snapshot transaction (TxStatus != 'I'), so the pins
// must JOIN the ambient transaction (SET LOCAL, no BEGIN — and above all
// no COMMIT, which would DESTROY the snapshot) instead of owning one.
//
// This test opens a real slot-exported SnapshotStream against a database
// whose DEFAULT is the Supabase shape (ALTER DATABASE … SET
// extra_float_digits = 0), runs the cold-start raw TEXT export on the
// pinned reader, and asserts BOTH halves of the cell:
//
//   - float exactness: the export renders shortest-exact digits, not the
//     efd-0 legacy rounding. (Two layers currently provide this — the
//     ambient SET LOCAL pin and the per-connection afterConnectSessionPins
//     belt; the pin here holds the OBSERVABLE, so a regression in either
//     layer's reach over this branch fails the cell.)
//   - snapshot preservation: a row committed after the stream opened stays
//     INVISIBLE across two consecutive exports. This is what proves the
//     ambient-join shape — an accidental BEGIN/COMMIT around the pins
//     would end the snapshot transaction and leak the new row into the
//     second export (and a fresh-pool-conn export would see it in both).
package postgres

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestSnapshotStream_RawCopyTextExport_FloatExactUnderEFD0Default(t *testing.T) {
	dsn, cleanup := startPostgresForSnapshotCDC(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// The float corpus: every value needs more digits than the efd-0
	// legacy renderings (%.15g / %.6g) carry.
	applyPGSQL(t, dsn, `
		CREATE TABLE pfloat (id BIGINT PRIMARY KEY, f8 DOUBLE PRECISION, f4 REAL);
		INSERT INTO pfloat VALUES (1, pi(), 16777215.0), (2, 2.718281828459045, NULL);
	`)
	// The Supabase shape: the database default is below the shortest-exact
	// threshold BEFORE sluice connects (new connections inherit it).
	applyPGSQL(t, dsn, "ALTER DATABASE source_db SET extra_float_digits = 0")

	eng := Engine{}
	stream, err := eng.OpenSnapshotStream(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSnapshotStream: %v", err)
	}
	// Abandon (not Close): no CDC anchor was persisted, and dropping the
	// just-created slot leaves the shared container clean for the next test.
	defer func() { _ = stream.Abandon() }()

	exp, ok := stream.Rows.(ir.RawCopyExporter)
	if !ok {
		t.Fatalf("snapshot RowReader %T does not implement ir.RawCopyExporter", stream.Rows)
	}
	table := &ir.Table{
		Schema: "public",
		Name:   "pfloat",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "f8", Type: ir.Float{Precision: ir.FloatDouble}, Nullable: true},
			{Name: "f4", Type: ir.Float{Precision: ir.FloatSingle}, Nullable: true},
		},
	}

	exportText := func(pass string) string {
		t.Helper()
		var buf bytes.Buffer
		if err := exp.ExportRawCopy(ctx, table, nil, ir.RawCopyText, &buf); err != nil {
			t.Fatalf("ExportRawCopy (%s) on the snapshot-pinned reader: %v", pass, err)
		}
		return buf.String()
	}

	assertExact := func(pass, out string) {
		t.Helper()
		// "1.6777215e+07" is float4out's 8-digit shortest-exact form of
		// 16777215; the unpinned efd-0 rendering is "1.67772e+07"
		// (rounded to 16777200 — the Bug 194 silent loss).
		for _, want := range []string{"3.141592653589793", "1.6777215e+07", "2.718281828459045"} {
			if !strings.Contains(out, want) {
				t.Errorf("%s export is missing the shortest-exact rendering %q — the float pin did not hold on the snapshot-transaction branch; stream:\n%s", pass, want, out)
			}
		}
	}

	first := exportText("first")
	assertExact("first", first)
	if strings.Contains(first, "3.333333333333333") {
		t.Fatal("marker value present before its insert — corpus/fixture mixup")
	}

	// Commit a row on a SEPARATE connection, after the snapshot's
	// consistent point. It must stay invisible to the pinned reader.
	applyPGSQL(t, dsn, "INSERT INTO pfloat VALUES (99, 3.333333333333333, 1.5)")

	second := exportText("second")
	assertExact("second", second)
	if strings.Contains(second, "3.333333333333333") {
		t.Error("post-snapshot row leaked into the second export — the pins ended the snapshot transaction (a BEGIN/COMMIT slipped into the ambient-join branch of rawCopyWithSessionPins)")
	}
}
