// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Bug (#244 value-fidelity review): --restart-from-scratch and the
// ADR-0093 auto-resnapshot recovery both force a fresh cold-start onto a
// NON-dropped target, relying on the (misleading) hint that "the
// idempotent copy absorbs the overlap". That contract only holds for
// IDEMPOTENT snapshot readers (VStream/PlanetScale, Postgres) whose
// cold-copy upserts. The NATIVE MySQL binlog snapshot does NOT implement
// ir.IdempotentCopyReader, so its cold-copy runs PLAIN INSERT — which
// dup-key ERRORS (MySQL Error 1062) on a target that already holds the
// prior copy's rows.
//
// The fix: when restarting-from-scratch (or auto-resnapshotting) with a
// NON-idempotent reader, drop the in-scope target tables first
// (resetTargetTablesForRestart, reusing --reset-target-data's FK-safe
// drop machinery) so the plain-INSERT copy starts clean. The idempotent
// path is unchanged (still absorbs). These pins lock both halves of the
// dispatch decision and the helper's error/refusal shapes.

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// --- minimal snapshot RowReader stubs on the idempotency axis ---

// nonIdempotentReader models the native MySQL binlog snapshot: it does
// NOT implement ir.IdempotentCopyReader, so the cold-copy plain-INSERTs.
type nonIdempotentReader struct{}

func (nonIdempotentReader) ReadRows(context.Context, *ir.Table) (<-chan ir.Row, error) {
	ch := make(chan ir.Row)
	close(ch)
	return ch, nil
}
func (nonIdempotentReader) Err() error { return nil }

// idempotentReader models the VStream/PlanetScale snapshot: it declares
// CopyNeedsIdempotentWriter()==true, so the cold-copy upserts.
type idempotentReader struct{}

func (idempotentReader) ReadRows(context.Context, *ir.Table) (<-chan ir.Row, error) {
	ch := make(chan ir.Row)
	close(ch)
	return ch, nil
}
func (idempotentReader) Err() error                      { return nil }
func (idempotentReader) CopyNeedsIdempotentWriter() bool { return true }

// idempotentReaderFalse declares the surface but reports FALSE — must be
// treated as non-idempotent (mirrors runBulkCopyWithOpts's predicate).
type idempotentReaderFalse struct{}

func (idempotentReaderFalse) ReadRows(context.Context, *ir.Table) (<-chan ir.Row, error) {
	ch := make(chan ir.Row)
	close(ch)
	return ch, nil
}
func (idempotentReaderFalse) Err() error                      { return nil }
func (idempotentReaderFalse) CopyNeedsIdempotentWriter() bool { return false }

// emptyCheckingDropper is a RowWriter that implements BOTH TableDropper
// (records drops) and TableEmptyChecker (always empty). Used so the
// default preflight branch sees an empty target and the drop branch can
// be observed.
type emptyCheckingDropper struct {
	dropped []string
}

func (w *emptyCheckingDropper) WriteRows(context.Context, *ir.Table, <-chan ir.Row) error {
	return errors.New("emptyCheckingDropper.WriteRows should not be called by the gate")
}

func (w *emptyCheckingDropper) DropTable(_ context.Context, table *ir.Table) error {
	w.dropped = append(w.dropped, table.Name)
	return nil
}

func (w *emptyCheckingDropper) IsTableEmpty(context.Context, *ir.Table) (bool, error) {
	return true, nil
}

// --- copyReaderIsIdempotent: must mirror runBulkCopyWithOpts exactly ---

func TestCopyReaderIsIdempotent(t *testing.T) {
	cases := []struct {
		name string
		rows ir.RowReader
		want bool
	}{
		{"native MySQL binlog (no surface)", nonIdempotentReader{}, false},
		{"VStream (surface true)", idempotentReader{}, true},
		{"surface present but false", idempotentReaderFalse{}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := copyReaderIsIdempotent(c.rows); got != c.want {
				t.Errorf("copyReaderIsIdempotent = %v; want %v", got, c.want)
			}
		})
	}
}

// --- resetTargetTablesForRestart: drop / refuse / propagate ---

func TestResetTargetTablesForRestart_DropsInScopeTables(t *testing.T) {
	captureSlog(t)
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "users"}, {Name: "orders"}}}
	rw := &stubDroppingWriter{}

	if err := resetTargetTablesForRestart(context.Background(), schema, rw); err != nil {
		t.Fatalf("resetTargetTablesForRestart: %v", err)
	}
	if got := rw.dropped; len(got) != 2 || got[0] != "users" || got[1] != "orders" {
		t.Errorf("dropped = %v; want [users orders]", got)
	}
}

// The refusal must be ACCURATE: name restart-from-scratch + the
// non-idempotent source + the actionable recovery (manual drop /
// --reset-target-data), NOT the old misleading idempotent-absorb hint.
func TestResetTargetTablesForRestart_NoDropperRefusesAccurately(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "users"}}}

	err := resetTargetTablesForRestart(context.Background(), schema, stubWriterNoDropper{})
	if err == nil {
		t.Fatal("expected a refusal; got nil")
	}
	msg := err.Error()
	for _, want := range []string{"restart-from-scratch", "non-idempotent", "--reset-target-data", "DROP TABLE"} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal missing %q; got %q", want, msg)
		}
	}
	// The misleading promise must be GONE from the refusal path.
	if strings.Contains(msg, "absorbs the overlap") {
		t.Errorf("refusal still claims idempotent absorb: %q", msg)
	}
}

func TestResetTargetTablesForRestart_DropErrorPropagates(t *testing.T) {
	captureSlog(t)
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "users"}}}
	rw := &stubDroppingWriter{dropErr: errors.New("permission denied")}

	err := resetTargetTablesForRestart(context.Background(), schema, rw)
	if err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Errorf("err = %v; want it to wrap 'permission denied'", err)
	}
}

// --- gate dispatch: non-idempotent restart DROPS; idempotent SKIPS ---

// gateStream builds a SnapshotStream around the given reader with a no-op
// Close so coldStartGatePreflight's error-path teardown is harmless.
func gateStream(rows ir.RowReader) *ir.SnapshotStream {
	return &ir.SnapshotStream{Rows: rows, CloseFn: func() error { return nil }}
}

func TestColdStartGate_RestartFromScratch_NonIdempotent_DropsTarget(t *testing.T) {
	captureSlog(t)
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "users"}, {Name: "orders"}}}
	rw := &emptyCheckingDropper{}
	s := &Streamer{
		Source:             &copyResumeEngine{name: "mysql"},
		RestartFromScratch: true,
	}

	err := s.coldStartGatePreflight(
		context.Background(), schema, nil /*sw*/, rw, gateStream(nonIdempotentReader{}),
		&stubChangeApplier{}, "stream-1", false, /*resumingCopy*/
	)
	if err != nil {
		t.Fatalf("gate: %v", err)
	}
	if got := rw.dropped; len(got) != 2 || got[0] != "users" || got[1] != "orders" {
		t.Errorf("non-idempotent restart must DROP the in-scope tables; dropped = %v", got)
	}
}

func TestColdStartGate_RestartFromScratch_Idempotent_DoesNotDrop(t *testing.T) {
	captureSlog(t)
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "users"}}}
	rw := &emptyCheckingDropper{}
	s := &Streamer{
		Source:             &copyResumeEngine{name: "planetscale"},
		RestartFromScratch: true,
	}

	err := s.coldStartGatePreflight(
		context.Background(), schema, nil /*sw*/, rw, gateStream(idempotentReader{}),
		&stubChangeApplier{}, "stream-1", false, /*resumingCopy*/
	)
	if err != nil {
		t.Fatalf("gate: %v", err)
	}
	if len(rw.dropped) != 0 {
		t.Errorf("idempotent restart must NOT drop (absorb-the-overlap path); dropped = %v", rw.dropped)
	}
}

// A reader that declares the surface but reports FALSE is non-idempotent
// and MUST be dropped — guards against treating "implements the surface"
// as "is idempotent" (mirrors the runBulkCopyWithOpts predicate).
func TestColdStartGate_RestartFromScratch_SurfaceFalse_DropsTarget(t *testing.T) {
	captureSlog(t)
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "users"}}}
	rw := &emptyCheckingDropper{}
	s := &Streamer{
		Source:             &copyResumeEngine{name: "mysql"},
		RestartFromScratch: true,
	}

	err := s.coldStartGatePreflight(
		context.Background(), schema, nil, rw, gateStream(idempotentReaderFalse{}),
		&stubChangeApplier{}, "stream-1", false,
	)
	if err != nil {
		t.Fatalf("gate: %v", err)
	}
	if len(rw.dropped) != 1 {
		t.Errorf("surface-false reader must be treated as non-idempotent and dropped; dropped = %v", rw.dropped)
	}
}
