//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
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
