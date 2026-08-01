//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The real-server half of the control-table identifier-collation gate.
//
// The unit gate (control_table_collation_test.go) proves the DDL
// DECLARES `COLLATE utf8mb4_bin` on every identifier key. Only a real
// MySQL can prove that declaration actually keeps two identifiers
// apart: the defect these tests pin is a SERVER behaviour — the table's
// inherited utf8mb4_0900_ai_ci is case- AND accent-insensitive, so an
// `ON DUPLICATE KEY UPDATE` write for `Foo` landed on `foo`'s row and
// silently overwrote its copy cursor. On `--resume` that restarted
// `Foo` from `foo`'s LastPK and never copied the rows below it, exit 0.
//
// Both halves of the class are exercised: LETTER CASE (Foo/foo) and
// ACCENTS (café/cafe), because 0900_ai_ci folds both and pinning only
// the case pair would leave the accent half unproven.

package mysql

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestMigrateProgressKeysAreCaseAndAccentSensitive is the pin for the
// reported defect: two source tables differing only in case (or only in
// accents) must keep INDEPENDENT per-table copy cursors in
// sluice_migrate_table_progress.
func TestMigrateProgressKeysAreCaseAndAccentSensitive(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	store, err := eng.OpenMigrationStateStore(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenMigrationStateStore: %v", err)
	}
	defer func() { _ = store.Close() }()
	if err := store.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	const migrationID = "collation-pin"
	// The header row is what Read keys on; the per-table cursors hang off
	// it (ADR-0082).
	if err := store.Write(ctx, ir.MigrationState{
		MigrationID: migrationID,
		Phase:       ir.MigrationPhaseBulkCopy,
	}); err != nil {
		t.Fatalf("Write header: %v", err)
	}
	// Distinct cursors so a fold is visible as a WRONG value, not just a
	// missing row — the silent-overwrite shape, exactly.
	want := map[string]int64{
		"Foo":  10,
		"foo":  20,
		"café": 30,
		"cafe": 40,
	}
	for table, pk := range want {
		if err := store.WriteTableProgress(ctx, migrationID, table, ir.TableProgress{
			State:      ir.TableProgressInProgress,
			LastPK:     []any{pk},
			RowsCopied: pk,
		}); err != nil {
			t.Fatalf("WriteTableProgress(%q): %v", table, err)
		}
	}

	got, ok, err := store.Read(ctx, migrationID)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !ok {
		t.Fatal("Read ok=false after writing progress rows")
	}
	if len(got.TableProgress) != len(want) {
		t.Fatalf("TableProgress has %d entries (%v); want %d — identifiers differing only in case "+
			"or accents collapsed onto one primary-key row",
			len(got.TableProgress), keysOf(got.TableProgress), len(want))
	}
	for table, pk := range want {
		tp, ok := got.TableProgress[table]
		if !ok {
			t.Errorf("no progress row for %q", table)
			continue
		}
		if tp.RowsCopied != pk {
			t.Errorf("%q RowsCopied = %d; want %d — another table's cursor overwrote it",
				table, tp.RowsCopied, pk)
		}
	}
}

// TestControlTableStreamIDsAreCaseAndAccentSensitive is the same class
// on the CDC side: two stream ids differing only in case must keep
// independent positions in sluice_cdc_state, which is keyed on
// stream_id and written with an ON DUPLICATE KEY upsert.
func TestControlTableStreamIDsAreCaseAndAccentSensitive(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	writer, ok := applier.(ir.PositionWriter)
	if !ok {
		t.Fatalf("%T does not implement ir.PositionWriter", applier)
	}

	want := map[string]string{
		"Orders-Sync": "mysql-bin.000001:100",
		"orders-sync": "mysql-bin.000001:200",
		"réplica":     "mysql-bin.000001:300",
		"replica":     "mysql-bin.000001:400",
	}
	for streamID, pos := range want {
		if err := writer.WritePosition(ctx, streamID, ir.Position{Token: pos}); err != nil {
			t.Fatalf("WritePosition(%q): %v", streamID, err)
		}
	}
	for streamID, pos := range want {
		got, found, err := applier.ReadPosition(ctx, streamID)
		if err != nil {
			t.Fatalf("ReadPosition(%q): %v", streamID, err)
		}
		if !found {
			t.Errorf("no position row for stream %q — it collapsed onto a "+
				"case/accent-folded sibling's primary-key row", streamID)
			continue
		}
		if got.Token != pos {
			t.Errorf("stream %q position = %q; want %q — another stream id "+
				"differing only in case/accents overwrote it", streamID, got.Token, pos)
		}
	}
}

// TestLegacyControlTableCollationWarns pins the OTHER half of the fix —
// the one that is invisible without a test, because
// warnLegacyControlTableCollation swallows its own probe errors by
// design (an advisory must not fail an ensure that otherwise worked).
// A broken probe query would therefore be silent forever, which is the
// exact "written invariant nobody checks" shape: an upgraded target
// keeping the folding collation while a fresh one is exact, with no
// signal anywhere.
//
// Both directions are asserted. Positive: a hand-created legacy-shape
// table (no COLLATE clause, so MySQL 8 gives it utf8mb4_0900_ai_ci)
// WARNs, naming the table and the runnable ALTER. Negative control: a
// table this binary just created reports nothing — otherwise a probe
// that warned unconditionally would pass the positive case for the
// wrong reason.
func TestLegacyControlTableCollationWarns(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	// Pre-fix shape: no COLLATE on the identifier columns.
	applyMySQLApplier(t, dsn, "CREATE TABLE `sluice_migrate_table_progress` ("+
		"  migration_id VARCHAR(255) NOT NULL,"+
		"  table_name   VARCHAR(255) NOT NULL,"+
		"  progress     TEXT         NOT NULL,"+
		"  updated_at   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"+
		"  PRIMARY KEY (migration_id, table_name)"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	store, err := Engine{}.OpenMigrationStateStore(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenMigrationStateStore: %v", err)
	}
	defer func() { _ = store.Close() }()
	if err := store.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	logged := buf.String()
	for _, want := range []string{
		"sluice_migrate_table_progress",
		"utf8mb4_bin",
		"ALTER TABLE",
		"MODIFY `table_name`",
		"MODIFY `migration_id`",
	} {
		if !strings.Contains(logged, want) {
			t.Errorf("legacy-collation WARN does not mention %q; got:\n%s", want, logged)
		}
	}
	// sluice_migrate_state was created fresh by this binary in the SAME
	// EnsureControlTable call — it must not appear.
	if strings.Contains(logged, "sluice_migrate_state\"") {
		t.Errorf("a freshly created table was reported as legacy-collated; got:\n%s", logged)
	}

	// Negative control: a second ensure on a target where BOTH tables are
	// now... still legacy for the progress table (we do not auto-ALTER),
	// so the warning must persist — the advisory is per-ensure, not
	// once-per-process.
	buf.Reset()
	if err := store.EnsureControlTable(ctx); err != nil {
		t.Fatalf("second EnsureControlTable: %v", err)
	}
	if !strings.Contains(buf.String(), "sluice_migrate_table_progress") {
		t.Errorf("the legacy-collation WARN did not repeat on the second ensure; got:\n%s", buf.String())
	}
}

func keysOf(m map[string]ir.TableProgress) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
