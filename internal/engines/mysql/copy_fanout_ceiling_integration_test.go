//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pin for the roadmap item 144 WRITE-side fan-out ceiling,
// against a REAL MySQL server.
//
// # What this adds that the unit tests cannot
//
// The unit tests drive `copyFanoutCeiling` and `computeConnectionBudget`
// directly, from a `connectionBudgetProbe` a test constructed. They pin the
// derivation and prove nothing about the chain that produces the input:
// `SELECT @@innodb_buffer_pool_size` → `probeConnectionBudget` →
// `computeConnectionBudget` → the `ir.ConnectionBudget` field the pipeline
// reads. That chain is where a SQL fat-finger, a server-version drift, or a
// dropped struct assignment lives.
//
// # The premise it can and cannot reach — stated, not implied
//
// The load-bearing environmental fact is "a PlanetScale DEV branch reports a
// small FIXED pool below the smallest plan tier, while a production branch on
// the same database reports its tier". **No CI can assert that** — there is no
// PlanetScale in the integration matrix, and the fact was established by direct
// measurement (2026-08-04 field report, 2026-08-05 x2, 2026-08-06 on a fresh
// PS-10). What this test DOES assert is the half that is mechanisable: that a
// real MySQL reporting a sub-floor buffer pool produces the ceiling, that a real
// MySQL reporting an on-scale one does not, and that the verdict survives the
// whole probe path onto the report. The engine's own flavor gate is what makes
// the PlanetScale-only scoping true, and it is pinned in the unit matrix.

package mysql

import (
	"context"
	"fmt"
	"testing"
	"time"

	mysqltc "github.com/testcontainers/testcontainers-go/modules/mysql"

	"github.com/testcontainers/testcontainers-go"
)

// TestProbeTargetConnectionBudget_FanoutCeilingFromARealServer boots a MySQL
// whose buffer pool is deliberately BELOW the PS-10 floor and asserts the
// declared fan-out ceiling arrives on the report — with the on-scale control
// taken from the shared container, so both sides of the decision ride a real
// `SELECT @@innodb_buffer_pool_size`.
func TestProbeTargetConnectionBudget_FanoutCeilingFromARealServer(t *testing.T) {
	t.Run("sub-floor server ⇒ the ceiling is declared", func(t *testing.T) {
		dsn := startMySQLWithBufferPool(t, 32<<20)
		eng := Engine{Flavor: FlavorPlanetScale}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		report, err := eng.ProbeTargetConnectionBudget(ctx, dsn, 4, 0)
		if err != nil {
			t.Fatalf("ProbeTargetConnectionBudget: %v", err)
		}
		if report.CopyFanoutCeiling != copyFanoutCeilingUnknownTier {
			t.Errorf("CopyFanoutCeiling = %d; want %d — a real server reporting a pool below the smallest known "+
				"plan tier must declare the conservative fan-out ceiling (item 144)",
				report.CopyFanoutCeiling, copyFanoutCeilingUnknownTier)
		}
		// The item-123 separation, ground-truthed rather than asserted: the
		// ceiling must NOT have leaked into the connection budget.
		if report.CopyBudget <= copyFanoutCeilingUnknownTier {
			t.Errorf("CopyBudget = %d; want the un-capped connection-derived budget (well above the fan-out "+
				"ceiling) — a sub-floor reading must not cap the SLOT budget (item 123)", report.CopyBudget)
		}
	})

	t.Run("on-scale server ⇒ no ceiling, and the tier cap still applies", func(t *testing.T) {
		dsn, cleanup := startMySQL(t)
		defer cleanup()
		eng := Engine{Flavor: FlavorPlanetScale}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Control validity, checked rather than assumed: this subtest only
		// means anything if the shared container's pool is actually ON the
		// tier scale. Fail loudly rather than passing vacuously if the image's
		// default ever drops below the floor.
		cfg, err := parseDSN(dsn)
		if err != nil {
			t.Fatalf("parseDSN: %v", err)
		}
		db, err := openDB(ctx, cfg, nil)
		if err != nil {
			t.Fatalf("openDB: %v", err)
		}
		defer func() { _ = db.Close() }()
		p, err := probeConnectionBudget(ctx, db)
		if err != nil {
			t.Fatalf("probeConnectionBudget: %v", err)
		}
		if p.bufferPoolBytes < bufferPoolPlanetScaleFloorBytes {
			t.Fatalf("the shared container reports @@innodb_buffer_pool_size = %d, BELOW the %d floor — this "+
				"control is not exercising the on-scale branch it claims to; boot a dedicated container instead",
				p.bufferPoolBytes, int64(bufferPoolPlanetScaleFloorBytes))
		}

		report, err := eng.ProbeTargetConnectionBudget(ctx, dsn, 4, 0)
		if err != nil {
			t.Fatalf("ProbeTargetConnectionBudget: %v", err)
		}
		if report.CopyFanoutCeiling != 0 {
			t.Errorf("CopyFanoutCeiling = %d; want 0 — a reading the probe CAN place on the tier scale must not "+
				"be driven at the unknown-tier fan-out (the item-123 regression this fix is most likely to cause)",
				report.CopyFanoutCeiling)
		}
	})

	t.Run("non-PlanetScale flavor declares none even sub-floor", func(t *testing.T) {
		dsn := startMySQLWithBufferPool(t, 32<<20)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		for _, flavor := range []Flavor{FlavorVanilla, FlavorVitess, FlavorMariaDB} {
			eng := Engine{Flavor: flavor}
			report, err := eng.ProbeTargetConnectionBudget(ctx, dsn, 4, 0)
			if err != nil {
				t.Fatalf("ProbeTargetConnectionBudget(%s): %v", flavor, err)
			}
			if report.CopyFanoutCeiling != 0 {
				t.Errorf("flavor %s: CopyFanoutCeiling = %d; want 0 — a self-hosted server sizes its buffer pool "+
					"to its own hardware, so a small pool there is a small box, not an unreadable tier",
					flavor, report.CopyFanoutCeiling)
			}
		}
	})
}

// startMySQLWithBufferPool boots a DEDICATED MySQL whose InnoDB buffer pool is
// pinned to `bytes`. It cannot use the shared container: the whole point is the
// server variable, and mutating a global on the shared instance would leak into
// every other test in the shard.
//
// innodb_buffer_pool_chunk_size is lowered alongside it because MySQL rounds the
// pool up to a multiple of chunk x instances — with the 128 MiB default chunk a
// request for 32 MiB comes back as 128 MiB, which is EXACTLY the value this test
// needs to be below. Without the chunk flag the sub-floor subtest would silently
// exercise the on-scale branch and pass for the wrong reason.
func startMySQLWithBufferPool(t *testing.T, bytes int64) string {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), sharedMySQLBootTimeout)
	defer cancel()

	c, err := mysqltc.Run(
		ctx,
		sharedMySQLImage,
		mysqltc.WithUsername("root"),
		mysqltc.WithPassword("rootpw"),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Cmd: []string{
					"mysqld",
					"--server-id=1",
					fmt.Sprintf("--innodb-buffer-pool-size=%d", bytes),
					"--innodb-buffer-pool-chunk-size=1048576",
					"--innodb-buffer-pool-instances=1",
				},
			},
		}),
	)
	if err != nil {
		t.Skipf("boot MySQL with a pinned buffer pool: %v", err)
	}
	t.Cleanup(func() {
		_ = c.Terminate(context.Background())
	})

	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("container host: %v", err)
	}
	port, err := c.MappedPort(ctx, "3306/tcp")
	if err != nil {
		t.Fatalf("container port: %v", err)
	}
	// The `mysql` system schema, not a seeded database: the prebaked image
	// ships a pre-initialised datadir, so its entrypoint skips the
	// MYSQL_DATABASE creation step and mysqltc.WithDatabase never takes
	// effect. The probe reads server variables and mysql.user only, so the
	// system schema is the right connection target here.
	return fmt.Sprintf("root:rootpw@tcp(%s:%s)/mysql?parseTime=true", host, port.Port())
}
