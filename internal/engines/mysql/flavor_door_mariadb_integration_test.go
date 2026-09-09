//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 280 (v0.148.2 regression cycle), on a real MariaDB: a MariaDB
// server addressed with the plain `mysql` driver must meet the WARN that
// names the right driver BEFORE it meets sluice's own SQL failing.
//
// What the cycle measured: `migrate` opens the migration-state store at
// phase 1.75, ahead of the schema reader and writer where the flavor
// probe used to run alone. That store renders its upsert in the flavor's
// spelling — MySQL 8.0.20's row alias, which MariaDB rejects — so the run
// died on `Error 1064 … near 'AS new ON DUPLICATE KEY UPDATE'` with zero
// occurrences of the steer in the log. The diagnosis was pre-empted by
// its own symptom.
//
// The cells drive the two doors the pipeline opens FIRST (the state store
// for migrate, the change applier for sync) and require both the steer
// and a working control table, which is the pair that was impossible
// before: the store could not be created at all.

package mysql

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestMariaDBUnderMySQLDriver_SteerPrecedesItsOwnSymptom(t *testing.T) {
	dsn, cleanup := newMariaDBDedicatedForCDC(t, mariadb114Image)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// The wrong driver on purpose: FlavorVanilla against a MariaDB server.
	eng := Engine{Flavor: FlavorVanilla}

	for _, cell := range []struct {
		name string
		open func(context.Context, string) error
	}{
		{"migration-state store (migrate's phase-1.75 door)", func(ctx context.Context, dsn string) error {
			s, err := eng.OpenMigrationStateStore(ctx, dsn)
			if err != nil {
				return err
			}
			if c, ok := s.(interface{ Close() error }); ok {
				_ = c.Close()
			}
			return nil
		}},
		{"change applier (sync's first door)", func(ctx context.Context, dsn string) error {
			a, err := eng.OpenChangeApplier(ctx, dsn)
			if err != nil {
				return err
			}
			if c, ok := a.(interface{ Close() error }); ok {
				_ = c.Close()
			}
			return nil
		}},
	} {
		t.Run(cell.name, func(t *testing.T) {
			// The steer is a WARN, and it is memoised per (server,
			// flavor) — so capture the log around a FRESH server key by
			// clearing the memo for this cell.
			flavorMemo.mu.Lock()
			flavorMemo.byServer = nil
			flavorMemo.mu.Unlock()

			var buf bytes.Buffer
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
			defer slog.SetDefault(prev)

			if err := cell.open(ctx, dsn); err != nil {
				t.Fatalf("%s: open returned %v; the door should open and WARN, not fail", cell.name, err)
			}
			got := buf.String()
			if !strings.Contains(got, "this server is MariaDB") {
				t.Fatalf("%s: the MariaDB steer did not fire at this door, so an operator meets sluice's own "+
					"`AS new ON DUPLICATE KEY UPDATE` 1064 with no hint that the driver is wrong (Bug 280):\n%s",
					cell.name, got)
			}
			if !strings.Contains(got, "mariadb") {
				t.Errorf("%s: the WARN does not name the driver to use:\n%s", cell.name, got)
			}
		})
	}

	t.Run("the memo answers once per server, and the verdict is stable", func(t *testing.T) {
		flavorMemo.mu.Lock()
		flavorMemo.byServer = nil
		flavorMemo.mu.Unlock()

		var buf bytes.Buffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
		defer slog.SetDefault(prev)

		for i := 0; i < 3; i++ {
			s, err := eng.OpenMigrationStateStore(ctx, dsn)
			if err != nil {
				t.Fatalf("open %d: %v", i, err)
			}
			if c, ok := s.(interface{ Close() error }); ok {
				_ = c.Close()
			}
		}
		if n := strings.Count(buf.String(), "this server is MariaDB"); n != 1 {
			t.Errorf("the steer fired %d times across three opens; want exactly 1 — the memo is what makes "+
				"probing every door affordable, and a steer naming one flag should be said once", n)
		}
	})

	t.Run("the correct driver is not steered", func(t *testing.T) {
		flavorMemo.mu.Lock()
		flavorMemo.byServer = nil
		flavorMemo.mu.Unlock()

		var buf bytes.Buffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
		defer slog.SetDefault(prev)

		s, err := (Engine{Flavor: FlavorMariaDB}).OpenMigrationStateStore(ctx, dsn)
		if err != nil {
			t.Fatalf("mariadb flavor against a MariaDB server: %v", err)
		}
		if c, ok := s.(interface{ Close() error }); ok {
			_ = c.Close()
		}
		if strings.Contains(buf.String(), "this server is MariaDB") {
			t.Errorf("the steer fired for the CORRECT driver — an over-warn on a working configuration:\n%s", buf.String())
		}
	})
}
