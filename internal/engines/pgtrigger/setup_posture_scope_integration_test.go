//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The install-wide posture, on a real server (audit 2026-08-31 A-2).
//
// The unit pins next door grade the render and the refusal in isolation;
// this one walks the operator sequence that produced the finding, on real
// catalogs, and requires each prescribed remedy to actually clear the state
// it is prescribed for — the half the shipped docs asserted and nothing
// checked.
package pgtrigger

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

func TestSetupPostureIsInstallWide(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	applyPGSQL(t, dsn, `
		CREATE TABLE orders    (id BIGINT PRIMARY KEY, note TEXT);
		CREATE TABLE shipments (id BIGINT PRIMARY KEY, note TEXT);
	`)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	setup := func(optIn bool, tables ...string) error {
		_, err := Setup(ctx, dsn, SetupOptions{Tables: tables, CaptureReplicatedWrites: optIn})
		return err
	}
	wantEnablement := func(t *testing.T, want string, tables ...string) {
		t.Helper()
		for _, tbl := range tables {
			for _, trg := range []string{CaptureTriggerRow, CaptureTriggerTruncate} {
				if got := triggerEnablement(t, ctx, db, tbl, trg); got != want {
					t.Errorf("tgenabled of %s on %s = %q; want %q", trg, tbl, got, want)
				}
			}
		}
	}
	openWantClean := func(t *testing.T) {
		t.Helper()
		r, err := openCDCReader(ctx, dsn, "")
		if err != nil {
			t.Fatalf("CDC open refused (the install is not internally consistent): %v", err)
		}
		_ = r.(*CDCReader).Close()
	}

	if err := setup(true, "orders"); err != nil {
		t.Fatalf("Setup(--tables=orders --capture-replicated-writes): %v", err)
	}
	wantEnablement(t, "A", "orders")

	t.Run("a plain run naming ONLY the new table refuses instead of half-converting", func(t *testing.T) {
		// Pre-fix this SUCCEEDED: shipments got plain triggers, the
		// install-wide posture flipped to false, orders stayed 'A', and the
		// next open refused with "flipped by hand" naming a remedy — re-run
		// `sluice trigger setup` — that is the command the operator had
		// just run and that leaves orders at 'A' forever.
		err := setup(false, "shipments")
		if err == nil {
			t.Fatal("Setup accepted the half-converting run")
		}
		for _, want := range []string{"orders", "--tables=orders,shipments", "--capture-replicated-writes"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal missing %q:\n%v", want, err)
			}
		}
		// Refused BEFORE any DDL: shipments must carry no capture trigger.
		var n int
		if err := db.QueryRowContext(ctx, `
SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
 WHERE c.relname = 'shipments' AND NOT t.tgisinternal`).Scan(&n); err != nil {
			t.Fatalf("count shipments triggers: %v", err)
		}
		if n != 0 {
			t.Errorf("the refused run installed %d trigger(s) on shipments; a refusal must touch nothing", n)
		}
		// And the install it refused to break still opens.
		openWantClean(t)
	})

	t.Run("the refusal's dry-run twin refuses too", func(t *testing.T) {
		_, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"shipments"}, DryRun: true})
		if err == nil {
			t.Fatal("the dry-run plan was produced; the operator would have applied it by hand")
		}
	})

	t.Run("remedy 1 — keep the opt-in for the whole install", func(t *testing.T) {
		if err := setup(true, "shipments"); err != nil {
			t.Fatalf("Setup(--tables=shipments --capture-replicated-writes): %v", err)
		}
		wantEnablement(t, "A", "orders", "shipments")
		openWantClean(t)
	})

	t.Run("remedy 2 — name every captured table to convert back to origin-only", func(t *testing.T) {
		if err := setup(false, "orders", "shipments"); err != nil {
			t.Fatalf("Setup(--tables=orders,shipments): %v", err)
		}
		wantEnablement(t, "O", "orders", "shipments")
		openWantClean(t)
	})

	t.Run("the opt-in WIDENS the tables it did not name", func(t *testing.T) {
		// From the all-plain state above, an opt-in run naming only orders
		// must leave the install coherent: shipments is outside --tables
		// and the door will grade it against the newly recorded posture.
		if err := setup(true, "orders"); err != nil {
			t.Fatalf("Setup(--tables=orders --capture-replicated-writes) over a plain install: %v", err)
		}
		wantEnablement(t, "A", "orders", "shipments")
		openWantClean(t)
	})

	t.Run("an install that ALREADY carries the divergence gets a remedy that runs", func(t *testing.T) {
		// The shape a pre-v0.137 setup could produce (and a hand-flip still
		// can): posture recorded origin-only, one table left at 'A'.
		if err := setup(false, "orders", "shipments"); err != nil {
			t.Fatalf("Setup(plain, both): %v", err)
		}
		applyPGSQL(t, dsn, `ALTER TABLE orders ENABLE ALWAYS TRIGGER sluice_capture`)

		r, err := openCDCReader(ctx, dsn, "")
		if err == nil {
			_ = r.(*CDCReader).Close()
			t.Fatal("the open passed on a posture divergence")
		}
		if !strings.Contains(err.Error(), "--tables=orders,shipments") {
			t.Errorf("the door's remedy does not name every captured table, so running it cannot converge the install:\n%v", err)
		}
		// The remedy the message prints, run verbatim.
		if err := setup(false, "orders", "shipments"); err != nil {
			t.Fatalf("the prescribed remedy failed: %v", err)
		}
		wantEnablement(t, "O", "orders", "shipments")
		openWantClean(t)
	})
}
