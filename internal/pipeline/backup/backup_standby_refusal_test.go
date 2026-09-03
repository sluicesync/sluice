// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// Bug 260: `backup full` against a Postgres hot standby used to take the
// non-snapshot fallback, copy EVERY row, and then die raw at
// CaptureBackupPosition with SQLSTATE 55000 — the position capture cannot
// run on a standby either — leaving an uncommitted manifest that `restore`
// refuses. The v0.137.3 regression cycle observed it at both
// wal_level=replica and wal_level=logical.
//
// The door now keys on the shared error CODE, so the assertion that
// matters is behavioural and engine-neutral: a snapshot open that failed
// with SLUICE-E-CDC-STANDBY-SOURCE refuses BEFORE any row is read, and
// the SAME fallback still degrades for every other open failure (the
// wal_level case a one-shot full is entitled to).
//
// rowsRead is the independent expected value: not "did it error" (the old
// behaviour errored too, just 40 GB later) but "were any rows read".
type standbySnapshotEngine struct {
	*backupRecorderEngine
	openErr  error
	rowsRead atomic.Int64
}

func (e *standbySnapshotEngine) OpenBackupSnapshot(context.Context, string, irbackup.SnapshotOptions) (*irbackup.Snapshot, error) {
	return nil, e.openErr
}

func (e *standbySnapshotEngine) OpenRowReader(_ context.Context, _ string) (ir.RowReader, error) {
	return &countingRowReader{inner: &fakeRowReader{rows: e.backupRecorderEngine.rows}, n: &e.rowsRead}, nil
}

type countingRowReader struct {
	inner *fakeRowReader
	n     *atomic.Int64
}

func (r *countingRowReader) Err() error { return r.inner.Err() }

func (r *countingRowReader) ReadRows(ctx context.Context, table *ir.Table) (<-chan ir.Row, error) {
	in, err := r.inner.ReadRows(ctx, table)
	if err != nil {
		return nil, err
	}
	out := make(chan ir.Row)
	go func() {
		defer close(out)
		for row := range in {
			r.n.Add(1)
			select {
			case <-ctx.Done():
				return
			case out <- row:
			}
		}
	}()
	return out, nil
}

func TestBackupFull_StandbySnapshotRefusalDoesNotDegradeIntoAWastedCopy(t *testing.T) {
	standbyErr := sluicecode.Wrap(
		sluicecode.CodeCDCStandbySource,
		"point --source at the primary endpoint",
		errors.New("postgres: cdc: the source is a read-only hot standby / read replica"),
	)

	for _, tc := range []struct {
		name      string
		openErr   error
		wantErr   string
		wantRows  int64
		wantCoded bool
	}{
		{
			// The Bug 260 shape. Refuse, name the reason, read nothing.
			name:      "standby refuses before reading a row",
			openErr:   standbyErr,
			wantErr:   "read-only standby",
			wantRows:  0,
			wantCoded: true,
		},
		{
			// The control that keeps the fix honest: the fallback this
			// door sits in must still exist. A wal_level=replica primary
			// cannot open the anchor slot either, and a one-shot full
			// backup off it is a legitimate shape — it degrades, copies,
			// and completes.
			name:     "a non-standby open failure still degrades and copies",
			openErr:  errors.New("postgres: wal_level must be logical (is replica)"),
			wantErr:  "",
			wantRows: 3,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := blobcodec.NewLocalStore(t.TempDir())
			if err != nil {
				t.Fatalf("NewLocalStore: %v", err)
			}
			schema := &ir.Schema{Tables: []*ir.Table{{
				Name:    "users",
				Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
			}}}
			rows := map[string][]ir.Row{"users": {{"id": int64(1)}, {"id": int64(2)}, {"id": int64(3)}}}
			src := &standbySnapshotEngine{
				backupRecorderEngine: newBackupRecorderEngine("postgres", schema, rows),
				openErr:              tc.openErr,
			}

			runErr := (&Backup{Source: src, SourceDSN: "src", Store: store, SluiceVersion: "test"}).Run(context.Background())

			if tc.wantErr == "" {
				if runErr != nil {
					t.Fatalf("Run = %v; want nil (the non-standby fallback must still work)", runErr)
				}
			} else {
				// Errorf, not Fatalf: the rows-read assertion below is the
				// load-bearing half (Bug 260 DID error — after the copy), so
				// it has to report on the same run.
				if runErr == nil || !strings.Contains(runErr.Error(), tc.wantErr) {
					t.Errorf("Run = %v; want an error containing %q", runErr, tc.wantErr)
				}
				if ce, ok := sluicecode.FromError(runErr); tc.wantCoded && (!ok || ce.Code != sluicecode.CodeCDCStandbySource) {
					t.Errorf("refusal does not carry %s (got %v); an operator greps the code",
						sluicecode.CodeCDCStandbySource, runErr)
				}
			}
			if got := src.rowsRead.Load(); got != tc.wantRows {
				t.Errorf("rows read = %d; want %d — a standby refusal that arrives AFTER the copy is Bug 260", got, tc.wantRows)
			}
		})
	}
}
