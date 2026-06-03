// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestRolloverHook_FiresWithEnvVars verifies the post-rollover hook
// receives the documented env-var contract. Writes a small script to
// disk and invokes it as the hook; the script dumps the env to a
// caller-supplied output file the test inspects.
func TestRolloverHook_FiresWithEnvVars(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)

	parent := &ir.Manifest{
		FormatVersion: ir.BackupFormatVersion,
		CreatedAt:     time.Now().UTC(),
		SourceEngine:  "postgres",
		Schema:        &ir.Schema{},
		Kind:          ir.BackupKindFull,
		EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/100"}`},
	}
	parent.BackupID = ir.ComputeBackupID(parent)
	writeParentFullManifest(t, store, parent)

	// Drive a single tx-commit-bounded rollover.
	src := &fakeCDCEngine{
		name:           "postgres",
		schemaSequence: []*ir.Schema{{}},
		cdcChanges: []ir.Change{
			ir.TxBegin{Position: ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/110"}`}},
			ir.Insert{Position: ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/120"}`}, Table: "users", Row: ir.Row{"id": int64(1)}},
			ir.TxCommit{Position: ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/130"}`}},
		},
		cdcExpectedFromOK: true,
	}

	scriptDir := t.TempDir()
	envFile := filepath.Join(scriptDir, "hook-env.txt")
	scriptPath := writeHookScript(t, scriptDir, envFile)

	stream := &BackupStream{
		Source:             src,
		SourceDSN:          "src",
		Store:              store,
		ParentRef:          parent.BackupID,
		RolloverWindow:     time.Minute,
		RolloverMaxChanges: 100,
		RolloverMaxBytes:   1 << 30,
		ChunkChanges:       100,
		RolloverHook:       hookCmdForScript(scriptPath),
		pidHostFn:          func() (int, string) { return 1, "h" },
	}
	if err := stream.Run(context.Background()); err != nil {
		t.Fatalf("stream.Run: %v", err)
	}

	// Read back the env-var dump.
	body, err := os.ReadFile(envFile)
	if err != nil {
		t.Fatalf("read hook env file: %v", err)
	}
	got := string(body)

	wants := []string{
		"SLUICE_ROLLOVER_MANIFEST_PATH=manifests/incr-",
		"SLUICE_ROLLOVER_PARENT_BACKUP_ID=" + parent.BackupID,
		"SLUICE_ROLLOVER_BACKUP_ID=",
		"SLUICE_ROLLOVER_CHANGES=3",
		"SLUICE_ROLLOVER_BYTES=",
		"SLUICE_ROLLOVER_ELAPSED_MS=",
	}
	for _, w := range wants {
		if !strings.Contains(got, w) {
			t.Errorf("hook output missing %q\nfull output:\n%s", w, got)
		}
	}
}

// writeHookScript writes a small platform-appropriate script to dir
// that dumps the SLUICE_ROLLOVER_* env vars to envFile. Returns the
// script's absolute path (no spaces by construction; t.TempDir returns
// a path the test framework owns).
func writeHookScript(t *testing.T, dir, envFile string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		path := filepath.Join(dir, "hook.bat")
		// Use & to chain echoes; redirect once at the end with `>`.
		body := "@echo off\r\n" +
			"echo SLUICE_ROLLOVER_MANIFEST_PATH=%SLUICE_ROLLOVER_MANIFEST_PATH% > \"" + envFile + "\"\r\n" +
			"echo SLUICE_ROLLOVER_PARENT_BACKUP_ID=%SLUICE_ROLLOVER_PARENT_BACKUP_ID% >> \"" + envFile + "\"\r\n" +
			"echo SLUICE_ROLLOVER_BACKUP_ID=%SLUICE_ROLLOVER_BACKUP_ID% >> \"" + envFile + "\"\r\n" +
			"echo SLUICE_ROLLOVER_CHANGES=%SLUICE_ROLLOVER_CHANGES% >> \"" + envFile + "\"\r\n" +
			"echo SLUICE_ROLLOVER_BYTES=%SLUICE_ROLLOVER_BYTES% >> \"" + envFile + "\"\r\n" +
			"echo SLUICE_ROLLOVER_ELAPSED_MS=%SLUICE_ROLLOVER_ELAPSED_MS% >> \"" + envFile + "\"\r\n"
		if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
			t.Fatalf("write script: %v", err)
		}
		return path
	}
	path := filepath.Join(dir, "hook.sh")
	body := "#!/bin/sh\nenv | grep ^SLUICE_ROLLOVER_ > '" + envFile + "'\n"
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}
	return path
}

// hookCmdForScript returns the hook-command form that invokes a
// pre-written script file. The outer shell wrapper (cmd /C on
// Windows, sh -c on Unix) takes the path verbatim; t.TempDir paths
// don't contain spaces in our test fixtures so no quoting is needed.
func hookCmdForScript(path string) string {
	return path
}

// TestRolloverHook_FailureIsWarned verifies a failing hook (non-zero
// exit) doesn't fail the stream — the rollover already committed.
func TestRolloverHook_FailureIsWarned(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)

	parent := &ir.Manifest{
		FormatVersion: ir.BackupFormatVersion,
		CreatedAt:     time.Now().UTC(),
		SourceEngine:  "postgres",
		Schema:        &ir.Schema{},
		Kind:          ir.BackupKindFull,
		EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/100"}`},
	}
	parent.BackupID = ir.ComputeBackupID(parent)
	writeParentFullManifest(t, store, parent)

	src := &fakeCDCEngine{
		name:           "postgres",
		schemaSequence: []*ir.Schema{{}},
		cdcChanges: []ir.Change{
			ir.TxBegin{Position: ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/110"}`}},
			ir.Insert{Position: ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/120"}`}, Table: "users", Row: ir.Row{"id": int64(1)}},
			ir.TxCommit{Position: ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/130"}`}},
		},
		cdcExpectedFromOK: true,
	}

	stream := &BackupStream{
		Source:             src,
		SourceDSN:          "src",
		Store:              store,
		ParentRef:          parent.BackupID,
		RolloverWindow:     time.Minute,
		RolloverMaxChanges: 100,
		RolloverMaxBytes:   1 << 30,
		ChunkChanges:       100,
		RolloverHook:       failHookCmdForOS(),
		pidHostFn:          func() (int, string) { return 1, "h" },
	}
	if err := stream.Run(context.Background()); err != nil {
		t.Errorf("stream.Run with failing hook = %v; want nil (hook failure must not fail the stream)", err)
	}

	// The rollover should still have committed.
	records, _ := listAllManifestsViaWalk(context.Background(), store)
	var sawIncr bool
	for _, r := range records {
		if r.manifest.Kind == ir.BackupKindIncremental {
			sawIncr = true
		}
	}
	if !sawIncr {
		t.Errorf("incremental manifest missing — hook failure incorrectly aborted rollover commit")
	}
}

// failHookCmdForOS returns a single-string command that exits non-zero
// without doing anything else.
func failHookCmdForOS() string {
	if runtime.GOOS == "windows" {
		return "exit /b 1"
	}
	return "exit 1"
}
