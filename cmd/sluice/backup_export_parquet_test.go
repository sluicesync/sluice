// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"strings"
	"testing"
)

// TestBackupExportAsParquet_FlagBinding pins the kong wiring through
// the REAL parser (the Bug-180 lesson: a fix reachable only through a
// CLI value must be pinned through the parser, not a direct struct
// literal) — the subcommand name, every load-bearing flag, and the
// shared EncryptionFlags embed.
func TestBackupExportAsParquet_FlagBinding(t *testing.T) {
	cli := parseInto(
		t,
		"backup", "export-as-parquet",
		"--from-dir", "/backups/chain",
		"--output-dir", "/lake/parquet",
		"--backup-id", "9e4b05a41101bc99",
		"--include-table", "users,orders",
		"--force-overwrite",
		"--encrypt", "--encryption-passphrase-env", "SLUICE_PASS",
		"--require-signature",
	)
	x := cli.Backup.ExportAsParquet
	if x.FromDir != "/backups/chain" || x.OutputDir != "/lake/parquet" {
		t.Fatalf("store paths = %q / %q", x.FromDir, x.OutputDir)
	}
	if x.BackupID != "9e4b05a41101bc99" {
		t.Fatalf("BackupID = %q", x.BackupID)
	}
	if len(x.IncludeTable) != 2 || x.IncludeTable[0] != "users" {
		t.Fatalf("IncludeTable = %v", x.IncludeTable)
	}
	if !x.ForceOverwrite {
		t.Fatal("ForceOverwrite not bound")
	}
	if !x.Encrypt || x.EncryptionPassphraseEnv != "SLUICE_PASS" || !x.RequireSignature {
		t.Fatalf("encryption flags = %+v", x.EncryptionFlags)
	}
}

// TestBackupExportAsParquet_MutualExclusions pins the Run-level flag
// validation so a misuse fails BEFORE any store is opened.
func TestBackupExportAsParquet_MutualExclusions(t *testing.T) {
	cases := []struct {
		name string
		cmd  BackupExportAsParquetCmd
		want string
	}{
		{"no source", BackupExportAsParquetCmd{OutputDir: "o"}, "one of --from-dir or --from"},
		{"both sources", BackupExportAsParquetCmd{FromDir: "a", From: "s3://b", OutputDir: "o"}, "mutually exclusive"},
		{"no destination", BackupExportAsParquetCmd{FromDir: "a"}, "one of --output-dir or --output"},
		{"both destinations", BackupExportAsParquetCmd{FromDir: "a", OutputDir: "o", Output: "s3://o"}, "mutually exclusive"},
		{
			"include+exclude",
			BackupExportAsParquetCmd{FromDir: "a", OutputDir: "o", IncludeTable: []string{"x"}, ExcludeTable: []string{"y"}},
			"mutually exclusive",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cmd.Run(&Globals{})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Run() = %v; want %q", err, tc.want)
			}
		})
	}
}
