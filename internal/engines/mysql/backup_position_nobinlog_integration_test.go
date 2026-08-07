//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The binlog-availability premise behind the backup end-position
// capture, against real servers with binary logging OFF.
//
// backup_position.go's doc used to close with "the binlog stream is
// always available to a privileged connection." It was never checked and
// it is false, and the failure was ASYMMETRIC across the two arms —
// which is why one representative would not have caught it. The file/pos
// arm refused loudly all along (`SHOW MASTER STATUS` returns zero rows
// on a binlog-less server), while the GTID arm read an EMPTY GTID set
// and encoded it into a well-formed-looking cursor: a backup that
// verifies clean and covers nothing.
//
// So the pin is the FAMILY, not a representative. There are two distinct
// routes into that GTID arm and they run different code:
//
//   - MySQL, where gtidModeOnFor PROBES `SHOW VARIABLES LIKE 'gtid_mode'`.
//     Reaching the arm needs `--skip-log-bin --gtid-mode=ON
//     --enforce-gtid-consistency=ON`, which MySQL 8.0 accepts.
//   - MariaDB, where gtidModeOnFor returns true UNCONDITIONALLY for the
//     flavor. Every binlog-less MariaDB took the arm with no contrivance
//     at all, which makes it the likelier of the two in the field.
//
// Both cells assert the same three things: the server really is in the
// state the premise denied (read from the server, not assumed), the
// capture records NO position rather than a plausible one, and a
// binlog-ENABLED control on the same code path still records a real one.

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"sluicesync.dev/sluice/internal/ir"
)

// TestCaptureBackupPosition_NoBinlog pins that neither backup-position
// capturer invents a cursor for a source that has no binlog to point at.
func TestCaptureBackupPosition_NoBinlog(t *testing.T) {
	cases := []struct {
		name   string
		image  string
		flavor Flavor
		// cmd boots the server with binary logging OFF but still on the
		// GTID arm; ctlCmd is the same server with binary logging ON.
		cmd    []string
		ctlCmd []string
		rootDB string
		envs   map[string]string
	}{
		{
			name:   "mysql-gtid-mode-on-with-log-bin-off",
			image:  "mysql:8.0",
			flavor: FlavorVanilla,
			cmd: []string{
				"mysqld", "--server-id=1", "--skip-log-bin",
				"--gtid-mode=ON", "--enforce-gtid-consistency=ON",
			},
			ctlCmd: []string{
				"mysqld", "--server-id=1", "--log-bin=mysql-bin",
				"--binlog-format=ROW", "--gtid-mode=ON", "--enforce-gtid-consistency=ON",
			},
			rootDB: "nobinlog",
			envs:   map[string]string{"MYSQL_ROOT_PASSWORD": "rootpw", "MYSQL_DATABASE": "nobinlog"},
		},
		{
			name:   "mariadb-flavor-takes-the-gtid-arm-unconditionally",
			image:  mariadb114Image,
			flavor: FlavorMariaDB,
			cmd:    []string{"--server-id=1", "--skip-log-bin"},
			ctlCmd: []string{"--server-id=1", "--log-bin=mysqld-bin", "--binlog-format=ROW"},
			rootDB: "nobinlog",
			envs:   map[string]string{"MARIADB_ROOT_PASSWORD": "rootpw", "MARIADB_DATABASE": "nobinlog"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
			defer cancel()

			dsn, cleanup := startMySQLFamilyContainer(t, ctx, tc.image, tc.rootDB, tc.envs, tc.cmd)
			defer cleanup()

			// Ground truth first: the server really is in the state the
			// retracted premise denied. Reading it (rather than trusting
			// the flags) is what stops this cell from silently degrading
			// into "the image ignored --skip-log-bin".
			assertServerVar(t, ctx, dsn, "@@GLOBAL.log_bin", "0")
			if tc.flavor == FlavorVanilla {
				assertServerVar(t, ctx, dsn, "@@GLOBAL.gtid_mode", "ON")
			}

			e := Engine{Flavor: tc.flavor}
			sr, err := e.OpenSchemaReader(ctx, dsn)
			if err != nil {
				t.Fatalf("OpenSchemaReader: %v", err)
			}
			defer func() {
				if c, ok := sr.(interface{ Close() error }); ok {
					_ = c.Close()
				}
			}()

			capturer, ok := sr.(interface {
				CaptureBackupPosition(context.Context, string) (ir.Position, error)
			})
			if !ok {
				t.Fatal("mysql SchemaReader no longer implements the backup PositionCapturer")
			}
			pos, err := capturer.CaptureBackupPosition(ctx, "")
			if err != nil {
				t.Fatalf("CaptureBackupPosition on a binlog-less source: %v "+
					"(a standalone full of such a source must still succeed)", err)
			}
			if pos.Engine != "" || pos.Token != "" {
				t.Fatalf("CaptureBackupPosition recorded {Engine:%q Token:%q} on a source with "+
					"binary logging OFF; want the empty position. A non-empty token here is a "+
					"manifest EndPosition that looks resumable and covers nothing — the whole "+
					"point of the log_bin preflight", pos.Engine, pos.Token)
			}

			// The SIBLING door. captureBackupPosition is the conn-scoped
			// capturer the lock-free snapshot path uses, and it had the
			// identical defect; a gate that reached only the SchemaReader
			// door would be exactly the "fixed one implementor" shape this
			// project keeps paying for, so it is graded here rather than
			// implied.
			connPos := captureOnPinnedConn(t, ctx, dsn, tc.flavor)
			if connPos.Engine != "" || connPos.Token != "" {
				t.Fatalf("captureBackupPosition (conn-scoped, lock-free snapshot path) recorded "+
					"{Engine:%q Token:%q} on a binlog-less source; want the empty position",
					connPos.Engine, connPos.Token)
			}
		})

		t.Run(tc.name+"-binlog-on-control", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
			defer cancel()

			dsn, cleanup := startMySQLFamilyContainer(t, ctx, tc.image, tc.rootDB, tc.envs, tc.ctlCmd)
			defer cleanup()

			assertServerVar(t, ctx, dsn, "@@GLOBAL.log_bin", "1")

			e := Engine{Flavor: tc.flavor}
			sr, err := e.OpenSchemaReader(ctx, dsn)
			if err != nil {
				t.Fatalf("OpenSchemaReader: %v", err)
			}
			defer func() {
				if c, ok := sr.(interface{ Close() error }); ok {
					_ = c.Close()
				}
			}()

			capturer := sr.(interface { //nolint:forcetypeassert // asserted in the sibling cell above
				CaptureBackupPosition(context.Context, string) (ir.Position, error)
			})
			pos, err := capturer.CaptureBackupPosition(ctx, "")
			if err != nil {
				t.Fatalf("CaptureBackupPosition on a binlog-ENABLED source: %v", err)
			}
			if pos.Token == "" {
				t.Fatal("CaptureBackupPosition recorded NO position on a binlog-ENABLED source — " +
					"the preflight is refusing the healthy case, which would make the cell above " +
					"vacuously green")
			}
			if connPos := captureOnPinnedConn(t, ctx, dsn, tc.flavor); connPos.Token == "" {
				t.Fatal("captureBackupPosition (conn-scoped) recorded NO position on a " +
					"binlog-ENABLED source — its preflight is refusing the healthy case")
			}
		})
	}
}

// captureOnPinnedConn drives the conn-scoped [captureBackupPosition] —
// the lock-free snapshot path's capturer — over a real pinned
// connection, which is the only way to reach it (it needs a *sql.Conn
// for snapshotMasterStatus, so no fake querier substitutes).
func captureOnPinnedConn(t *testing.T, ctx context.Context, dsn string, flavor Flavor) ir.Position {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("pin conn: %v", err)
	}
	defer func() { _ = conn.Close() }()
	pos, err := captureBackupPosition(ctx, conn, flavor)
	if err != nil {
		t.Fatalf("captureBackupPosition: %v", err)
	}
	return pos
}

// startMySQLFamilyContainer boots one MySQL-or-MariaDB container with an
// explicit Cmd and returns a root DSN. Deliberately not routed through
// the shared prebaked container: every cell here turns binary logging
// OFF, which is exactly what the shared image exists to have ON.
func startMySQLFamilyContainer(
	t *testing.T,
	ctx context.Context,
	image, database string,
	envs map[string]string,
	cmd []string,
) (dsn string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	req := testcontainers.ContainerRequest{
		Image:        image,
		Env:          envs,
		Cmd:          cmd,
		ExposedPorts: []string{"3306/tcp"},
		WaitingFor: wait.ForSQL("3306/tcp", "mysql", func(host string, port network.Port) string {
			return fmt.Sprintf("root:rootpw@tcp(%s:%s)/%s", host, port.Port(), database)
		}).WithStartupTimeout(5 * time.Minute),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("boot %s (cmd %v): %v", image, cmd, err)
	}
	cleanup = func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}
	host, err := container.Host(ctx)
	if err != nil {
		cleanup()
		t.Fatalf("host: %v", err)
	}
	port, err := container.MappedPort(ctx, "3306/tcp")
	if err != nil {
		cleanup()
		t.Fatalf("port: %v", err)
	}
	return fmt.Sprintf("root:rootpw@tcp(%s:%s)/%s?parseTime=true", host, port.Port(), database), cleanup
}

// assertServerVar reads a server variable and compares it as text — the
// ground-truth step that keeps a cell from passing because the image
// quietly ignored a boot flag.
func assertServerVar(t *testing.T, ctx context.Context, dsn, expr, want string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var got string
	if err := db.QueryRowContext(ctx, "SELECT "+expr).Scan(&got); err != nil {
		t.Fatalf("read %s: %v", expr, err)
	}
	if got != want {
		t.Fatalf("%s = %q; want %q — this cell's premise about the server does not hold, "+
			"so whatever it asserts next is not measuring what it claims", expr, got, want)
	}
}
