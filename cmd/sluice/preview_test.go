package main

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestOpenPreviewOutput_Stdout confirms that an empty path returns
// os.Stdout with a no-op finalize callback.
func TestOpenPreviewOutput_Stdout(t *testing.T) {
	w, finalize, err := openPreviewOutput("")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if w != os.Stdout {
		t.Errorf("expected stdout writer for empty path")
	}
	if err := finalize(nil); err != nil {
		t.Errorf("finalize: %v", err)
	}
}

// TestOpenPreviewOutput_AtomicSuccess writes content via the temp
// path, finalizes with nil, and verifies the destination file exists
// with the written content. The temp file should not be left behind.
func TestOpenPreviewOutput_AtomicSuccess(t *testing.T) {
	dir := t.TempDir()
	dest := filepath.Join(dir, "preview.txt")

	w, finalize, err := openPreviewOutput(dest)
	if err != nil {
		t.Fatalf("openPreviewOutput: %v", err)
	}
	if _, err := io.WriteString(w, "hello world"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := finalize(nil); err != nil {
		t.Fatalf("finalize: %v", err)
	}

	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("read dest: %v", err)
	}
	if string(got) != "hello world" {
		t.Errorf("dest content = %q; want %q", got, "hello world")
	}

	// No temp files should be left in the dir.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	for _, e := range entries {
		if strings.Contains(e.Name(), ".tmp.") {
			t.Errorf("temp file %q left behind in %q after success", e.Name(), dir)
		}
	}
}

// TestOpenPreviewOutput_DiscardOnError finalizes with a non-nil error
// and verifies the destination is NOT written and the temp file is
// cleaned up.
func TestOpenPreviewOutput_DiscardOnError(t *testing.T) {
	dir := t.TempDir()
	dest := filepath.Join(dir, "preview.txt")

	w, finalize, err := openPreviewOutput(dest)
	if err != nil {
		t.Fatalf("openPreviewOutput: %v", err)
	}
	if _, err := io.WriteString(w, "partial"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := finalize(errors.New("synthetic run failure")); err != nil {
		t.Errorf("finalize on error returned non-nil: %v", err)
	}

	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Errorf("dest %q should not exist after discard; stat err = %v", dest, err)
	}

	// Temp file should also be cleaned up.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	for _, e := range entries {
		if strings.Contains(e.Name(), ".tmp.") {
			t.Errorf("temp file %q left behind in %q after error", e.Name(), dir)
		}
	}
}

// TestOpenPreviewOutput_OverwritesExisting verifies the rename
// replaces an existing destination file.
func TestOpenPreviewOutput_OverwritesExisting(t *testing.T) {
	dir := t.TempDir()
	dest := filepath.Join(dir, "preview.txt")

	if err := os.WriteFile(dest, []byte("old content"), 0o644); err != nil {
		t.Fatalf("seed dest: %v", err)
	}

	w, finalize, err := openPreviewOutput(dest)
	if err != nil {
		t.Fatalf("openPreviewOutput: %v", err)
	}
	if _, err := io.WriteString(w, "new content"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := finalize(nil); err != nil {
		t.Fatalf("finalize: %v", err)
	}

	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("read dest: %v", err)
	}
	if string(got) != "new content" {
		t.Errorf("dest content = %q; want overwritten content", got)
	}
}
