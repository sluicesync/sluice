// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mydumper

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestOpenDumpDirWrongDriverRefusals pins the Phase 3 cross-driver misuse
// refusals on the mydumper side (ADR-0163): a FILE handed to the directory
// reader is classified — foreign dumps get the scratch-server recipe, flat
// files name their driver — instead of the generic not-a-directory error.
func TestOpenDumpDirWrongDriverRefusals(t *testing.T) {
	write := func(t *testing.T, name, content string) string {
		t.Helper()
		p := filepath.Join(t.TempDir(), name)
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}

	t.Run("mysqldump .sql names the scratch-server recipe", func(t *testing.T) {
		p := write(t, "dump.sql", "-- MySQL dump 10.13  Distrib 8.0.36\nCREATE TABLE t (a int);\n")
		_, err := openDumpDir(p)
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeSourceForeignDump {
			t.Fatalf("error = %v; want SLUICE-E-SOURCE-FOREIGN-DUMP", err)
		}
		if !strings.Contains(err.Error(), "scratch MySQL") {
			t.Errorf("error %q should carry the scratch-server recipe", err.Error())
		}
	})
	t.Run("csv file names the csv driver", func(t *testing.T) {
		p := write(t, "data.csv", "a,b\n1,2\n")
		_, err := openDumpDir(p)
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeSourceWrongDriver {
			t.Fatalf("error = %v; want SLUICE-E-SOURCE-WRONG-DRIVER", err)
		}
		if !strings.Contains(err.Error(), "--source-driver csv") {
			t.Errorf("error %q should name the csv driver", err.Error())
		}
	})
	t.Run("unrecognised file keeps the generic refusal", func(t *testing.T) {
		p := write(t, "notes.txt", "hello\n")
		_, err := openDumpDir(p)
		if err == nil || !strings.Contains(err.Error(), "is not a directory") {
			t.Fatalf("want the generic not-a-directory refusal; got %v", err)
		}
	})
}
