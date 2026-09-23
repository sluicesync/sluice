//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// unforwardedRefusalCorpus is every shape a recorded refusal can take: the
// store is a codec (it round-trips through a text column and is read back by
// a later process), so each must come back byte-exact.
var unforwardedRefusalCorpus = map[string]string{
	"ascii":         `postgres: cdc: UNFORWARDED-SCHEMA-CHANGE on public.t: ADD CONSTRAINT "u" UNIQUE (name)`,
	"2-byte utf8":   "UNFORWARDED-SCHEMA-CHANGE on public.café: ADD CONSTRAINT \"ü\" CHECK (x > 0)",
	"3-byte utf8":   "UNFORWARDED-SCHEMA-CHANGE on public.表: ALTER COLUMN \"✓\" SET DEFAULT '…'",
	"4-byte utf8":   "UNFORWARDED-SCHEMA-CHANGE on public.t: CREATE POLICY \"😀\" USING (true)",
	"quotes+escape": "UNFORWARDED-SCHEMA-CHANGE: 'single' \"double\" `back` \\backslash\\ %s %w $1 ?",
	"multiline":     "UNFORWARDED-SCHEMA-CHANGE:\n\tline two\r\nline three",
	"4 KiB":         strings.Repeat("é", 2048),
}

// TestUnforwardedRefusalStore_RoundTrip pins the Postgres persisted-refusal
// store against a real server, starting from a control table a pre-column
// binary created (the upgrade shape, and the --schema-already-applied shape
// that never runs EnsureControlTable): no record reads as none, Record adds
// the column itself, every corpus shape round-trips exactly, a missing
// stream row is a loud error, and Clear is idempotent — all without touching
// the stream's position.
func TestUnforwardedRefusalStore_RoundTrip(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()
	applyPGApplier(t, dsn, `
		CREATE TABLE "public"."sluice_cdc_state" (
			stream_id       VARCHAR(255) NOT NULL,
			source_position TEXT         NOT NULL,
			updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (stream_id)
		);
		INSERT INTO "public"."sluice_cdc_state" (stream_id, source_position) VALUES ('s1', 'tok');
	`)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	applier, err := Engine{}.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	store := applier.(ir.UnforwardedRefusalStore)

	if msg, ok, err := store.ReadUnforwardedRefusal(ctx, "s1"); err != nil || ok {
		t.Fatalf("pre-column table: Read = (%q, %v, %v); want none", msg, ok, err)
	}
	for name, want := range unforwardedRefusalCorpus {
		if err := store.RecordUnforwardedRefusal(ctx, "s1", want); err != nil {
			t.Fatalf("%s: Record: %v", name, err)
		}
		// Recording the same value again must not read as a missing row.
		if err := store.RecordUnforwardedRefusal(ctx, "s1", want); err != nil {
			t.Fatalf("%s: repeated Record: %v", name, err)
		}
		got, ok, err := store.ReadUnforwardedRefusal(ctx, "s1")
		if err != nil || !ok || got != want {
			t.Errorf("%s: Read = (%q, %v, %v); want the recorded value byte-exact", name, got, ok, err)
		}
	}
	if err := store.RecordUnforwardedRefusal(ctx, "no-such-stream", "x"); !errors.Is(err, errStreamNotFound) {
		t.Errorf("Record on a missing row = %v; want errStreamNotFound (a refusal that did not land must be loud)", err)
	}
	if _, ok, err := store.ReadUnforwardedRefusal(ctx, "no-such-stream"); err != nil || ok {
		t.Errorf("Read on a missing row = (%v, %v); want none", ok, err)
	}
	for range 2 {
		if err := store.ClearUnforwardedRefusal(ctx, "s1"); err != nil {
			t.Fatalf("Clear: %v", err)
		}
	}
	if _, ok, err := store.ReadUnforwardedRefusal(ctx, "s1"); err != nil || ok {
		t.Errorf("after Clear: Read = (%v, %v); want none", ok, err)
	}
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable over the column Record added: %v", err)
	}
	if pos, ok, err := applier.ReadPosition(ctx, "s1"); err != nil || !ok || pos.Token != "tok" {
		t.Errorf("position = (%+v, %v, %v); the refusal store must not touch it", pos, ok, err)
	}
}

// TestUnforwardedRefusalStore_NonOwnerRoleRecords pins the 2026-09-23
// second-pass review's finding 1: a role with only DML grants on a control
// table another role owns — the `--schema-already-applied` setup — must be
// able to RECORD a refusal when the column already exists. PostgreSQL checks
// ownership before IF NOT EXISTS, so the store's earlier unconditional
// ALTER failed with "must be owner of table", the refusal was never written,
// and the next restart accepted the change silently.
func TestUnforwardedRefusalStore_NonOwnerRoleRecords(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()
	applyPGApplier(t, dsn, `
		CREATE TABLE "public"."sluice_cdc_state" (
			stream_id           VARCHAR(255) NOT NULL,
			source_position     TEXT         NOT NULL,
			updated_at          TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
			unforwarded_refusal TEXT         NULL,
			PRIMARY KEY (stream_id)
		);
		INSERT INTO "public"."sluice_cdc_state" (stream_id, source_position) VALUES ('s1', 'tok');
		CREATE ROLE sluice_dml LOGIN PASSWORD 'dml';
		GRANT USAGE ON SCHEMA public TO sluice_dml;
		GRANT SELECT, INSERT, UPDATE, DELETE ON "public"."sluice_cdc_state" TO sluice_dml;
	`)
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	u.User = url.UserPassword("sluice_dml", "dml")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	applier, err := Engine{}.OpenChangeApplier(ctx, u.String())
	if err != nil {
		t.Fatalf("OpenChangeApplier as the DML-only role: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	store, ok := applier.(ir.UnforwardedRefusalStore)
	if !ok {
		t.Fatal("postgres applier does not implement ir.UnforwardedRefusalStore")
	}
	const msg = `UNFORWARDED-SCHEMA-CHANGE on public.t: ADD CONSTRAINT "u" UNIQUE (name)`
	if err := store.RecordUnforwardedRefusal(ctx, "s1", msg); err != nil {
		t.Fatalf("RecordUnforwardedRefusal as a non-owner with the column present: %v — the refusal would not survive a restart", err)
	}
	got, ok, err := store.ReadUnforwardedRefusal(ctx, "s1")
	if err != nil || !ok || got != msg {
		t.Fatalf("read back = (%q, %v, %v); want the recorded refusal", got, ok, err)
	}
}
