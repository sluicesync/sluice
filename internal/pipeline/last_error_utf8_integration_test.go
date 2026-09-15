//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
)

// TestTruncateLastError_LandsOnBothEngines is the real-store half of the
// audit 2026-09-15 A0915-STATE-MEDIUM-3 pin: every shape of error message
// that used to reach sluice_migrate_state unstorable must, after
// truncateLastError, be ACCEPTED by both engines' stores and read back
// byte-identical to what truncateLastError returned.
//
// The independent expected value is each server's own verdict on the RAW
// message: every control cell writes the pre-fix value and asserts the
// verdict measured on that engine, so a store that silently accepted a bad
// sequence could not make the positive cell look like evidence. Measured
// 2026-09-15 on postgres:16 and mysql:8.0 (default strict sql_mode):
//
//   - a byte cut through a 3-byte rune: PG refuses (22021), MySQL refuses
//     (1366);
//   - an invalid sequence mid-message: PG refuses (22021), MySQL refuses
//     (1366);
//   - a NUL byte mid-message: PG refuses (22021), MySQL ACCEPTS it — so on
//     MySQL that shape was never lost, and the escape is harmless there.
func TestTruncateLastError_LandsOnBothEngines(t *testing.T) {
	_, mysqlDSN, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()
	_, pgDSN, pgCleanup := startPostgres(t)
	defer pgCleanup()

	// 1020 ASCII bytes then 3-byte runes: the nominal cut (1024-3=1021)
	// lands one byte into the first CJK rune — the audit's exact repro.
	longCJK := strings.Repeat("e", 1020) + strings.Repeat("名", 20)
	cases := []struct {
		name string
		// raw is the value the pre-fix code wrote.
		raw string
		// input is what truncateLastError is given.
		input string
		// refusedBy is the measured verdict on raw, per engine.
		refusedBy map[string]bool
	}{
		{
			name:      "byte cut through a rune",
			raw:       longCJK[:lastErrorMaxLen-len("…")] + "…",
			input:     longCJK,
			refusedBy: map[string]bool{"postgres": true, "mysql": true},
		},
		{
			name:      "invalid sequence mid-message",
			raw:       "refused value \xe5\x90 from a byte-truncated snippet",
			input:     "refused value \xe5\x90 from a byte-truncated snippet",
			refusedBy: map[string]bool{"postgres": true, "mysql": true},
		},
		{
			name:      "NUL mid-message",
			raw:       "refused value a\x00b",
			input:     "refused value a\x00b",
			refusedBy: map[string]bool{"postgres": true, "mysql": false},
		},
	}
	for _, tc := range cases {
		if utf8.ValidString(tc.raw) && !strings.ContainsRune(tc.raw, 0) {
			t.Fatalf("%s: the raw control value is storable text; it proves nothing", tc.name)
		}
	}

	for _, tgt := range []struct{ engine, dsn string }{
		{"mysql", mysqlDSN},
		{"postgres", pgDSN},
	} {
		t.Run(tgt.engine, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			eng, ok := engines.Get(tgt.engine)
			if !ok {
				t.Fatalf("engine %q not registered", tgt.engine)
			}
			opener, ok := eng.(ir.MigrationStateStoreOpener)
			if !ok {
				t.Fatalf("%s engine has no migration state store", tgt.engine)
			}
			store, err := opener.OpenMigrationStateStore(ctx, tgt.dsn)
			if err != nil {
				t.Fatalf("OpenMigrationStateStore: %v", err)
			}
			defer func() { _ = store.Close() }()
			if err := store.EnsureControlTable(ctx); err != nil {
				t.Fatalf("EnsureControlTable: %v", err)
			}

			for i, tc := range cases {
				// Control: the pre-fix value meets the measured verdict.
				controlID := "utf8-control-" + string(rune('a'+i))
				err := store.Write(ctx, ir.MigrationState{MigrationID: controlID, Phase: ir.MigrationPhaseFailed, LastError: tc.raw})
				if refused := err != nil; refused != tc.refusedBy[tgt.engine] {
					t.Errorf("%s: %s refused=%v (err=%v); measured refused=%v — the control no longer matches the "+
						"server, so re-measure before trusting the positive cell", tc.name, tgt.engine, refused,
						err, tc.refusedBy[tgt.engine])
				} else if err != nil {
					t.Logf("%s: %s refused the raw value as measured: %s", tc.name, tgt.engine, compactErr(err))
				}

				// The truncateLastError value lands and reads back
				// byte-identical.
				fixed := truncateLastError(tc.input)
				fixedID := "utf8-fixed-" + string(rune('a'+i))
				if err := store.Write(ctx, ir.MigrationState{MigrationID: fixedID, Phase: ir.MigrationPhaseFailed, LastError: fixed}); err != nil {
					t.Errorf("%s: Write of truncateLastError's value failed on %s: %v", tc.name, tgt.engine, err)
					continue
				}
				got, found, err := store.Read(ctx, fixedID)
				if err != nil || !found {
					t.Errorf("%s: Read: found=%v err=%v", tc.name, found, err)
					continue
				}
				if got.LastError != fixed {
					t.Errorf("%s: last_error round trip altered the value on %s:\n got %q\nwant %q", tc.name, tgt.engine, got.LastError, fixed)
				}
			}
		})
	}
}
