// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package flatfile

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// stagedFilesIn lists the sluice-flatfile-*.db staging files under dir.
func stagedFilesIn(t *testing.T, dir string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "sluice-flatfile-*.db"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	return matches
}

// TestStageOnce_SchemaAndRowReadersShareOneCopy pins the MED-P2 fix: a
// configured engine stages the flat file ONCE for the schema + row reader
// pair (not a copy each), keeps the copy alive until BOTH close in either
// order, and removes it after the last Close. A later open re-stages.
func TestStageOnce_SchemaAndRowReadersShareOneCopy(t *testing.T) {
	ctx := context.Background()
	stageDir := t.TempDir()
	e := csvEngine(t, Options{HeaderDeclared: true, Header: true, StageDir: stageDir})
	src := writeSource(t, "orders.csv", "a,b\n1,\"x\"\n2,\"y\"\n")

	sr, err := e.OpenSchemaReader(ctx, src)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	rr, err := e.OpenRowReader(ctx, src)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}

	staged := stagedFilesIn(t, stageDir)
	if len(staged) != 1 {
		t.Fatalf("staged copies with both readers open = %d (%v); want exactly 1 shared copy", len(staged), staged)
	}

	// The migrate ordering: the ROW reader closes first while the
	// orchestrator keeps the schema reader open run-long — the shared
	// copy must survive that first Close.
	closeIf(rr)
	if got := stagedFilesIn(t, stageDir); len(got) != 1 {
		t.Fatalf("staged copies after row-reader Close = %d; the schema reader is still open, the shared copy must survive", len(got))
	}
	if _, err := sr.ReadSchema(ctx); err != nil {
		t.Fatalf("ReadSchema after row-reader Close: %v", err)
	}

	closeIf(sr)
	if got := stagedFilesIn(t, stageDir); len(got) != 0 {
		t.Fatalf("staged copies after the last Close = %d (%v); want 0", len(got), got)
	}

	// A later open (verify after migrate) re-stages a fresh copy.
	sr2, err := e.OpenSchemaReader(ctx, src)
	if err != nil {
		t.Fatalf("re-open after full release: %v", err)
	}
	if got := stagedFilesIn(t, stageDir); len(got) != 1 {
		t.Fatalf("staged copies after re-open = %d; want 1 (re-staged)", len(got))
	}
	closeIf(sr2)
	if got := stagedFilesIn(t, stageDir); len(got) != 0 {
		t.Fatalf("staged copies after re-open Close = %d; want 0", len(got))
	}
}

// TestStageOnce_ZeroValueEngineStagesPerOpen pins the zero-value-safe
// fallback: the registry's unconfigured engine (no WithFlatFileOptions,
// so no share) still works — one owned staged copy per open, removed on
// that reader's own Close (the pre-share behavior).
func TestStageOnce_ZeroValueEngineStagesPerOpen(t *testing.T) {
	ctx := context.Background()
	e := Engine{format: formatNDJSON} // ndjson needs no declared flags
	src := writeSource(t, "rows.ndjson", `{"a":1}`+"\n")

	sr, err := e.OpenSchemaReader(ctx, src)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	rr, err := e.OpenRowReader(ctx, src)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	closeIf(sr)
	// The row reader's independent copy must still be readable.
	if _, err := rr.(ir.RowCounter).CountRows(ctx, &ir.Table{Name: "rows"}); err != nil {
		t.Fatalf("CountRows after the schema reader closed its own copy: %v", err)
	}
	closeIf(rr)
}

// TestStageShare_ConcurrentAcquireReleaseHammer hammers the refcounted
// share from many goroutines (audit 2026-07-16: the prior pins were
// sequential-only, so CI's -race job was vacuous over the refcount —
// this test is what makes -race actually cover it). Correctness floor
// pinned without ordering assumptions: every acquire hands back a live
// staged copy however the other goroutines interleave their releases,
// every release succeeds exactly once, and after the last release
// nothing is left on disk.
func TestStageShare_ConcurrentAcquireReleaseHammer(t *testing.T) {
	ctx := context.Background()
	stageDir := t.TempDir()
	e := csvEngine(t, Options{HeaderDeclared: true, Header: true, StageDir: stageDir})
	src := writeSource(t, "orders.csv", "a,b\n1,\"x\"\n2,\"y\"\n")

	const goroutines, iterations = 8, 20
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				staged, release, err := e.acquireStaged(ctx, src)
				if err != nil {
					errs <- err
					return
				}
				// The handed-out copy must be live for as long as the
				// reference is held, however the other goroutines
				// interleave their releases.
				if _, err := os.Stat(staged); err != nil {
					errs <- err
					return
				}
				if err := release(); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("hammer goroutine: %v", err)
	}
	if got := stagedFilesIn(t, stageDir); len(got) != 0 {
		t.Errorf("staged copies after the hammer drained = %d (%v); want 0", len(got), got)
	}
}

// TestStageDir_MissingDirectoryRefusesLoudly pins the --stage-dir
// consumption at the engine layer: a nonexistent stage dir is a loud
// open-time refusal naming the flag, never a silent fallback to /tmp.
func TestStageDir_MissingDirectoryRefusesLoudly(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	e := csvEngine(t, Options{HeaderDeclared: true, Header: true, StageDir: missing})
	src := writeSource(t, "x.csv", "a\n1\n")
	_, err := e.OpenSchemaReader(context.Background(), src)
	if err == nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("open with a missing --stage-dir = %v; want a loud not-exist refusal", err)
	}
	if got := err.Error(); !strings.Contains(got, "--stage-dir") || !strings.Contains(got, missing) {
		t.Fatalf("refusal %q must name --stage-dir and the missing path", got)
	}
}
