// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/netdeadline"
)

// The item-146 (Bug 229) write-deadline pins.
//
// The defect: a cold copy whose target connection died mid-`LOAD DATA` parked
// in net.(*conn).Write forever — no error, no exit code, zero rows — because
// no write deadline was armed. The fix is [mysql.Config.WriteTimeout] set at
// the DSN choke point, so EVERY MySQL connection carries it.
//
// Scope this file reaches, stated so it cannot be read as broader than the
// truth: it pins the four DSN parse entry points. It does NOT prove the
// deadline surfaces a real dead socket — that is
// TestLoadDataWriteDeadline_DeadSocketFailsLoudly (integration, real server
// behind a blackholing proxy), and it is the one that would catch the driver
// dropping the field.

// mysqlDSNParsers is the family this fix dispatches over: the four entry
// points that turn an operator DSN into a [mysql.Config]. Every one of them
// feeds openDB, so a default applied to fewer than all four leaves whole
// connection classes uncovered — the database-optional server variants are
// what the multi-database fan-out (ADR-0074) uses, and the flavor variants
// are what PlanetScale/Vitess use, i.e. the deployment the bug was reported
// against. Pinning one representative would have been exactly the Bug-74
// mistake.
var mysqlDSNParsers = []struct {
	name  string
	parse func(dsn string) (writeTimeout time.Duration, err error)
}{
	{"parseDSN", func(dsn string) (time.Duration, error) {
		cfg, err := parseDSN(dsn)
		if err != nil {
			return 0, err
		}
		return cfg.WriteTimeout, nil
	}},
	{"parseServerDSN", func(dsn string) (time.Duration, error) {
		cfg, err := parseServerDSN(dsn)
		if err != nil {
			return 0, err
		}
		return cfg.WriteTimeout, nil
	}},
	{"parseDSNForFlavor/planetscale", func(dsn string) (time.Duration, error) {
		cfg, err := parseDSNForFlavor(dsn, FlavorPlanetScale)
		if err != nil {
			return 0, err
		}
		return cfg.WriteTimeout, nil
	}},
	{"parseServerDSNForFlavor/vitess", func(dsn string) (time.Duration, error) {
		cfg, err := parseServerDSNForFlavor(dsn, FlavorVitess)
		if err != nil {
			return 0, err
		}
		return cfg.WriteTimeout, nil
	}},
}

func TestParseDSN_EveryEntryPointArmsAWriteDeadline(t *testing.T) {
	// × shape: the plain DSN, one carrying unrelated params, and the
	// non-TCP (unix socket) form — the last because the driver field, unlike
	// a dialer wrapper, is the only mechanism that reaches a unix socket.
	shapes := []struct{ name, dsn string }{
		{"tcp", "user:pw@tcp(db.example:3306)/appdb"},
		{"tcp+params", "user:pw@tcp(db.example:3306)/appdb?charset=utf8mb4&timeout=5s"},
		{"unix", "user:pw@unix(/tmp/mysql.sock)/appdb"},
	}
	for _, p := range mysqlDSNParsers {
		for _, s := range shapes {
			t.Run(p.name+"/"+s.name, func(t *testing.T) {
				got, err := p.parse(s.dsn)
				if err != nil {
					t.Fatalf("parse %q: %v", s.dsn, err)
				}
				if got != netdeadline.Write {
					t.Fatalf("WriteTimeout = %v, want %v — a connection with no write deadline blocks forever on a peer "+
						"that stopped draining (Bug 229)", got, netdeadline.Write)
				}
			})
		}
	}
}

// The two-tier override: an operator's `writeTimeout=` in the DSN wins
// absolutely, on every entry point. Same shape as sql_mode / time_zone /
// the ADR-0109 source session timeouts.
func TestParseDSN_OperatorWriteTimeoutWins(t *testing.T) {
	const dsn = "user:pw@tcp(db.example:3306)/appdb?writeTimeout=45s"
	for _, p := range mysqlDSNParsers {
		t.Run(p.name, func(t *testing.T) {
			got, err := p.parse(dsn)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if got != 45*time.Second {
				t.Fatalf("WriteTimeout = %v, want the operator's 45s — a per-target tuning must never be clobbered", got)
			}
		})
	}
}

// The deadline must be generous enough that a LIVE-but-stalled target is not
// turned into a spurious failure. The bound that matters is ADR-0109's own
// premise: a PlanetScale storage auto-grow blocks target writes for
// seconds-to-MINUTES, and during one the target stops draining the socket, so
// sluice's in-flight write parks on a healthy connection. A deadline at or
// below that window would convert a normal grow pause into a copy abort —
// strictly worse than the hang it replaces, because it would fire on every
// grow rather than on a genuinely dead peer.
func TestWriteDeadline_ExceedsTheLongestLegitimateStall(t *testing.T) {
	if netdeadline.Write < 5*time.Minute {
		t.Fatalf("netdeadline.Write = %v; a PlanetScale storage-grow pause runs seconds-to-minutes and the target does "+
			"not drain during it, so a bound this tight fires on healthy copies", netdeadline.Write)
	}
	// Finite, for the same reason ADR-0109's source-side bound is: a dead
	// target must surface rather than hang.
	if netdeadline.Write == 0 {
		t.Fatal("netdeadline.Write = 0 (no deadline) — that is the pre-item-146 hang")
	}
	// The two halves of the same stall are bounded on the same clock. If one
	// moves without the other, say so here rather than discovering the skew
	// in a field report.
	if want := time.Duration(sourceReadSessionTimeoutSeconds) * time.Second; netdeadline.Write != want {
		t.Errorf("netdeadline.Write = %v but the ADR-0109 source-read session bound is %v; the target-write and "+
			"source-read halves of a target stall are deliberately the same duration — move both or record why not",
			netdeadline.Write, want)
	}
}
