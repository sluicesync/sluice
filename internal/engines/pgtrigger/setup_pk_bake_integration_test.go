//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// N-16 integration pins — the change-log index diet, the baked-PK
// capture trigger, and the stale-bake loud refusal, against real PG.
//
//   - TestSetup_ChangeLogIndexDiet: a fresh setup leaves EXACTLY the
//     PK's implicit index on the change-log, and a re-run against a
//     pre-N-16 install (the two legacy indexes present) converges it —
//     the cheap idempotent-cleanup migration story.
//   - TestCapture_CompositePK_EndToEnd_AllModes: the baked TG_ARGV list
//     must keep composite PKs working end-to-end (setup → capture →
//     reader → applier → byte-identical target) in EVERY payload mode —
//     the modes all project through the baked list, so pin the class,
//     not the single-column representative.
//   - TestCapture_StalePKBake_RefusesLoudly: a post-setup PK ALTER
//     makes the baked list stale; the capture trigger must refuse the
//     write loudly (never capture rows keyed on the wrong columns), and
//     a setup re-run must re-bake and recover.

package pgtrigger

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// changeLogIndexNames lists the index names present on the change-log
// table in the source's public schema.
func changeLogIndexNames(t *testing.T, dsn string) []string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx,
		"SELECT indexname FROM pg_indexes WHERE schemaname = 'public' AND tablename = $1 ORDER BY indexname",
		ChangeLogTable)
	if err != nil {
		t.Fatalf("list change-log indexes: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("scan index name: %v", err)
		}
		names = append(names, n)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate index names: %v", err)
	}
	return names
}

// TestSetup_ChangeLogIndexDiet pins the N-16 index diet on real PG: a
// fresh setup leaves only the PK's implicit index on the change-log,
// and re-running setup against a pre-N-16 install (both legacy indexes
// present) drops them — idempotent convergence, no versioned migration.
func TestSetup_ChangeLogIndexDiet(t *testing.T) {
	src, _, cleanup := startPGTrigSrcTgtPair(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	payloadExec(t, src, "CREATE TABLE diet_orders (id BIGINT PRIMARY KEY, v TEXT)")
	setupOnce := func(step string) {
		if _, err := Setup(ctx, src, SetupOptions{Tables: []string{"diet_orders"}, Schema: "public"}); err != nil {
			t.Fatalf("Setup (%s): %v", step, err)
		}
	}
	setupOnce("fresh install")

	wantOnly := []string{ChangeLogTable + "_pkey"}
	if got := changeLogIndexNames(t, src); !equalStrings(got, wantOnly) {
		t.Fatalf("fresh setup: change-log indexes = %v, want only %v (N-16 index diet)", got, wantOnly)
	}

	// Simulate a pre-N-16 install: recreate the two legacy indexes an
	// earlier release's setup would have left behind, then re-run
	// setup and assert the idempotent DROPs converge it.
	payloadExec(t, src,
		"CREATE INDEX sluice_change_log_id_idx ON "+ChangeLogTable+" (id);"+
			"CREATE INDEX sluice_change_log_table_idx ON "+ChangeLogTable+" (schema_name, table_name, id);")
	setupOnce("pre-N-16 convergence re-run")
	if got := changeLogIndexNames(t, src); !equalStrings(got, wantOnly) {
		t.Fatalf("re-run over a pre-N-16 install: change-log indexes = %v, want only %v", got, wantOnly)
	}
}

// compositeTable exercises the baked-PK projection with a 2-column PK.
const compositeTable = "composite_orders"

const compositeSeedDDL = `
	CREATE TABLE ` + compositeTable + ` (
		tenant_id  BIGINT  NOT NULL,
		order_id   TEXT    NOT NULL,
		qty        INTEGER NOT NULL,
		note       TEXT,
		price      NUMERIC(20,6),
		PRIMARY KEY (tenant_id, order_id)
	);
`

const compositeSeedRows = `
	INSERT INTO ` + compositeTable + ` (tenant_id, order_id, qty, note, price) VALUES
		(1, 'a', 10, 'one-a',  1.000001),
		(1, 'b', 11, NULL,     1.100011),
		(1, 'c', 12, 'one-c',  NULL),
		(2, 'a', 20, 'two-a',  2.000002),
		(2, 'b', 21, 'two-b',  2.100021),
		(2, 'c', 22, 'two-c',  2.200022);
`

// compositeCDCDML mirrors the single-PK payload workload's shape class:
// insert, narrow update, zero-column update (PK-only after), value →
// NULL, a PK-CHANGING update that mutates BOTH key columns (the apply
// WHERE must locate the row by its OLD composite key), and a delete.
const compositeCDCDML = `
	INSERT INTO ` + compositeTable + ` (tenant_id, order_id, qty, note, price)
	VALUES (3, 'z', 30, 'cdc-insert-é', 3.333333);

	UPDATE ` + compositeTable + ` SET qty = 999 WHERE tenant_id = 1 AND order_id = 'a';

	UPDATE ` + compositeTable + ` SET qty = qty WHERE tenant_id = 1 AND order_id = 'b';

	UPDATE ` + compositeTable + ` SET note = NULL WHERE tenant_id = 2 AND order_id = 'a';

	UPDATE ` + compositeTable + ` SET tenant_id = 9, order_id = 'moved' WHERE tenant_id = 2 AND order_id = 'b';

	DELETE FROM ` + compositeTable + ` WHERE tenant_id = 2 AND order_id = 'c';
`

func compositeFullyDrained(dsn string) bool {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return false
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var drained bool
	q := fmt.Sprintf(`
		SELECT
			EXISTS (SELECT 1 FROM %[1]s WHERE tenant_id = 3 AND order_id = 'z')
			AND EXISTS (SELECT 1 FROM %[1]s WHERE tenant_id = 9 AND order_id = 'moved')
			AND NOT EXISTS (SELECT 1 FROM %[1]s WHERE tenant_id = 2 AND order_id = 'b')
			AND NOT EXISTS (SELECT 1 FROM %[1]s WHERE tenant_id = 2 AND order_id = 'c')
			AND EXISTS (SELECT 1 FROM %[1]s WHERE tenant_id = 1 AND order_id = 'a' AND qty = 999)
			AND EXISTS (SELECT 1 FROM %[1]s WHERE tenant_id = 2 AND order_id = 'a' AND note IS NULL)
	`, compositeTable)
	if err := db.QueryRowContext(ctx, q).Scan(&drained); err != nil {
		return false
	}
	return drained
}

// TestCapture_CompositePK_EndToEnd_AllModes proves the setup-time-baked
// TG_ARGV PK list carries composite keys end-to-end in every payload
// mode: the target is built PURELY from the change-log replay, so a
// wrong or partial composite-key projection (pk_jsonb / minimal-mode
// before-image) would diverge the target.
func TestCapture_CompositePK_EndToEnd_AllModes(t *testing.T) {
	for _, mode := range []CapturePayload{CapturePayloadFull, CapturePayloadChanged, CapturePayloadMinimal} {
		mode := mode
		t.Run(string(mode), func(t *testing.T) {
			src, tgt, cleanup := startPGTrigSrcTgtPair(t)
			defer cleanup()

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()

			payloadExec(t, src, compositeSeedDDL)
			payloadExec(t, tgt, compositeSeedDDL)

			if _, err := Setup(ctx, src, SetupOptions{
				Tables:         []string{compositeTable},
				Schema:         "public",
				CapturePayload: mode,
			}); err != nil {
				t.Fatalf("Setup(%s): %v", mode, err)
			}

			e := Engine{}
			reader, err := e.OpenCDCReader(ctx, src)
			if err != nil {
				t.Fatalf("OpenCDCReader: %v", err)
			}
			defer func() {
				if c, ok := reader.(interface{ Close() error }); ok {
					_ = c.Close()
				}
			}()
			out, err := reader.StreamChanges(ctx, ir.Position{})
			if err != nil {
				t.Fatalf("StreamChanges: %v", err)
			}

			applier, err := e.OpenChangeApplier(ctx, tgt)
			if err != nil {
				t.Fatalf("OpenChangeApplier: %v", err)
			}
			if err := applier.EnsureControlTable(ctx); err != nil {
				t.Fatalf("EnsureControlTable: %v", err)
			}
			applyCtx, applyCancel := context.WithCancel(ctx)
			applyDone := make(chan error, 1)
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				applyDone <- applier.Apply(applyCtx, "composite-pk-e2e", out)
			}()

			payloadExec(t, src, compositeSeedRows)
			payloadExec(t, src, compositeCDCDML)

			drained := func() bool {
				deadline := time.Now().Add(90 * time.Second)
				for time.Now().Before(deadline) {
					if compositeFullyDrained(tgt) {
						return true
					}
					time.Sleep(200 * time.Millisecond)
				}
				return false
			}()
			if !drained {
				applyCancel()
				wg.Wait()
				t.Fatalf("mode %s: target never drained the composite-PK workload", mode)
			}

			// Byte-identity: whole-row digest over the composite key order.
			digest := fmt.Sprintf(
				"SELECT md5(COALESCE(string_agg(t::text, E'\\n' ORDER BY t.tenant_id, t.order_id), '')) FROM %s t",
				compositeTable,
			)
			if s, g := payloadScalar(t, src, digest), payloadScalar(t, tgt, digest); s != g {
				t.Errorf("mode %s: composite-PK target diverged from source (src md5=%s tgt md5=%s)", mode, s, g)
			}

			applyCancel()
			wg.Wait()
			select {
			case err := <-applyDone:
				if err != nil && !strings.Contains(err.Error(), "context canceled") {
					t.Errorf("applier.Apply returned non-cancel error: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Error("applier did not exit after ctx cancel")
			}
			if c, ok := applier.(interface{ Close() error }); ok {
				_ = c.Close()
			}
		})
	}
}

// TestCapture_StalePKBake_RefusesLoudly pins the honest posture of the
// setup-time bake: after a post-setup PK ALTER (here a PK-column
// rename) the baked list no longer projects out of the row image, and
// the capture trigger must REFUSE the write loudly — never capture a
// change-log row keyed on the wrong columns — until a setup re-run
// re-bakes the list.
func TestCapture_StalePKBake_RefusesLoudly(t *testing.T) {
	src, _, cleanup := startPGTrigSrcTgtPair(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	payloadExec(t, src, "CREATE TABLE stale_pk (id BIGINT PRIMARY KEY, v TEXT)")
	if _, err := Setup(ctx, src, SetupOptions{Tables: []string{"stale_pk"}, Schema: "public"}); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	// Sanity: the freshly-baked trigger captures normally.
	payloadExec(t, src, "INSERT INTO stale_pk VALUES (1, 'pre-alter')")

	// Rename the PK column — the baked '["id"]' list is now stale.
	payloadExec(t, src, "ALTER TABLE stale_pk RENAME COLUMN id TO order_ref")

	db, err := sql.Open("pgx", src)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	_, err = db.ExecContext(ctx, "INSERT INTO stale_pk VALUES (2, 'post-alter')")
	if err == nil {
		t.Fatal("INSERT after PK-column rename succeeded — the stale baked PK list captured a row keyed on the wrong columns (silent corruption)")
	}
	if !strings.Contains(err.Error(), "no longer matches the row image") {
		t.Fatalf("INSERT after PK-column rename failed, but not with the stale-bake refusal: %v", err)
	}

	// A setup re-run re-bakes the list and recovers the table.
	if _, err := Setup(ctx, src, SetupOptions{Tables: []string{"stale_pk"}, Schema: "public"}); err != nil {
		t.Fatalf("Setup (re-bake): %v", err)
	}
	payloadExec(t, src, "INSERT INTO stale_pk VALUES (2, 'post-rebake')")
}
