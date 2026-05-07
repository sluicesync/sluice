// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"strings"
	"testing"

	"github.com/alecthomas/kong"
)

// TestBackupCmdParse confirms the new commands parse cleanly under
// kong: subcommand routing, required flags, default values for
// chunk-size and max-buffer-bytes.
func TestBackupCmdParse(t *testing.T) {
	cases := []struct {
		name string
		args []string
		// expectErr is the substring an error should contain if
		// non-empty; empty means parse must succeed.
		expectErr string
		check     func(t *testing.T, cli *CLI)
	}{
		{
			name: "backup full happy path",
			args: []string{
				"backup", "full",
				"--source-driver=postgres",
				"--source=postgres://localhost/db",
				"--output-dir=/tmp/backup",
			},
			check: func(t *testing.T, cli *CLI) {
				if cli.Backup.Full.SourceDriver != "postgres" {
					t.Errorf("SourceDriver = %q", cli.Backup.Full.SourceDriver)
				}
				if cli.Backup.Full.OutputDir != "/tmp/backup" {
					t.Errorf("OutputDir = %q", cli.Backup.Full.OutputDir)
				}
				if cli.Backup.Full.ChunkSize != 100000 {
					t.Errorf("ChunkSize default = %d; want 100000", cli.Backup.Full.ChunkSize)
				}
			},
		},
		{
			name: "backup full custom chunk-size",
			args: []string{
				"backup", "full",
				"--source-driver=mysql",
				"--source=user:pwd@/db",
				"--output-dir=/tmp/b",
				"--chunk-size=50000",
			},
			check: func(t *testing.T, cli *CLI) {
				if cli.Backup.Full.ChunkSize != 50000 {
					t.Errorf("ChunkSize = %d", cli.Backup.Full.ChunkSize)
				}
			},
		},
		{
			name: "backup full include + exclude rejected by Run",
			// kong itself accepts both; the Run method enforces
			// mutual exclusion. Here we just confirm parse succeeds.
			args: []string{
				"backup", "full",
				"--source-driver=postgres",
				"--source=src",
				"--output-dir=/tmp",
				"--include-table=a",
				"--exclude-table=b",
			},
			check: func(t *testing.T, cli *CLI) {
				if len(cli.Backup.Full.IncludeTable) != 1 || cli.Backup.Full.IncludeTable[0] != "a" {
					t.Errorf("IncludeTable = %v", cli.Backup.Full.IncludeTable)
				}
				if len(cli.Backup.Full.ExcludeTable) != 1 || cli.Backup.Full.ExcludeTable[0] != "b" {
					t.Errorf("ExcludeTable = %v", cli.Backup.Full.ExcludeTable)
				}
			},
		},
		{
			name: "backup verify happy path",
			args: []string{"backup", "verify", "--from-dir=/tmp/backup"},
			check: func(t *testing.T, cli *CLI) {
				if cli.Backup.Verify.FromDir != "/tmp/backup" {
					t.Errorf("FromDir = %q", cli.Backup.Verify.FromDir)
				}
			},
		},
		{
			name:      "backup verify missing --from-dir",
			args:      []string{"backup", "verify"},
			expectErr: "from-dir",
		},
		{
			name: "restore happy path",
			args: []string{
				"restore",
				"--from-dir=/tmp/backup",
				"--target-driver=postgres",
				"--target=postgres://localhost/db",
			},
			check: func(t *testing.T, cli *CLI) {
				if cli.Restore.FromDir != "/tmp/backup" {
					t.Errorf("FromDir = %q", cli.Restore.FromDir)
				}
				if cli.Restore.TargetDriver != "postgres" {
					t.Errorf("TargetDriver = %q", cli.Restore.TargetDriver)
				}
				if cli.Restore.MaxBufferBytes != 67108864 {
					t.Errorf("MaxBufferBytes default = %d", cli.Restore.MaxBufferBytes)
				}
			},
		},
		{
			name:      "restore missing --target-driver",
			args:      []string{"restore", "--from-dir=/x", "--target=t"},
			expectErr: "target-driver",
		},
		{
			name:      "backup full missing --output-dir",
			args:      []string{"backup", "full", "--source-driver=mysql", "--source=x"},
			expectErr: "output-dir",
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			cli := &CLI{}
			parser, err := kong.New(cli,
				kong.Vars{"version": "test"},
				kong.Exit(func(int) {}),
			)
			if err != nil {
				t.Fatalf("kong.New: %v", err)
			}
			_, err = parser.Parse(c.args)
			if c.expectErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q; got nil", c.expectErr)
				}
				if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(c.expectErr)) {
					t.Errorf("err = %v; want containing %q", err, c.expectErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if c.check != nil {
				c.check(t, cli)
			}
		})
	}
}
