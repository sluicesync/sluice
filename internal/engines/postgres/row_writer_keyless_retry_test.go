// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// Audit B-9, PG sibling. The chunked-COPY retry replays a buffered chunk on
// a classified transient. The file's original rationale covered only the
// ROLLED-BACK branch ("a rolled-back chunk wrote NOTHING"); the
// committed-but-unacked branch — the server committed the COPY and the
// connection died before pgx read CommandComplete — replays rows that are
// already durable. On a keyed table the target says 23505; on a keyless one
// nothing does.
//
// The oracle here is the number of times the ATTEMPT closure ran, i.e. how
// many COPYs the code actually issued — not the writer's own report.
//
// Which paths this reaches: copyChunkWithRetry, which is the retry core for
// BOTH the chunked-COPY path (writeViaCopyChunked) and the idempotent
// upsert path (writeViaBatchIdempotent). The plain multi-row INSERT core
// (writeViaBatch) is exempt because it never replays at all — see
// quiesceAndReportTransient — and raw_copy.go likewise refuses to replay.

// pgKeyedPinTable is a minimal PK-carrying table for the retry pins: the
// replay is safe on it, so the retry loop behaves exactly as it always has.
func pgKeyedPinTable(name string) *ir.Table {
	return &ir.Table{
		Name: name,
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 8}, Nullable: false},
			{Name: "v", Type: ir.Text{}, Nullable: true},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}
}

// pgKeylessPinTable has no PRIMARY KEY and no unique index at all.
func pgKeylessPinTable(name string) *ir.Table {
	return &ir.Table{
		Name: name,
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 8}, Nullable: false},
			{Name: "v", Type: ir.Text{}, Nullable: true},
		},
	}
}

// TestPGCopyChunkRetry_KeylessRefusesRatherThanReplay is the PG crux: the
// second COPY of an ambiguous chunk must never be issued.
func TestPGCopyChunkRetry_KeylessRefusesRatherThanReplay(t *testing.T) {
	withFastPGCopyBackoff(t)
	gate := &recordingGrowGate{}
	w := &RowWriter{growGate: gate}

	attempts := 0
	err := w.copyChunkWithRetry(context.Background(), pgKeylessPinTable("events_raw"), 7, func(context.Context) error {
		attempts++
		return diskFull53100()
	})
	if err == nil {
		t.Fatal("keyless chunk after a transient: want a refusal, got nil (the chunk was silently replayed)")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeCopyRetryAmbiguousKeyless {
		t.Errorf("refusal code = %v (coded=%v); want %s", ce, ok, sluicecode.CodeCopyRetryAmbiguousKeyless)
	}
	if attempts != 1 {
		t.Errorf("COPY attempts = %d; want 1 — the ambiguous chunk must not be replayed", attempts)
	}
	if !strings.Contains(err.Error(), "events_raw") {
		t.Errorf("refusal must name the table; got: %v", err)
	}
	// The lane still coordinates: the transient was real, so siblings are
	// told about it even though this lane refuses.
	if got := gate.trips.Load(); got != 1 {
		t.Errorf("gate.Trip calls = %d; want 1 — refusing must not cost the run its coordination", got)
	}
}

// TestPGCopyChunkRetry_KeyedStillReplays is the false-refusal floor on the
// other side: a keyed table keeps riding the storage-grow window.
func TestPGCopyChunkRetry_KeyedStillReplays(t *testing.T) {
	withFastPGCopyBackoff(t)
	w := &RowWriter{growGate: &recordingGrowGate{}}

	attempts := 0
	err := w.copyChunkWithRetry(context.Background(), pgKeyedPinTable("orders"), 3, func(context.Context) error {
		attempts++
		if attempts == 1 {
			return diskFull53100()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("a keyed chunk must still ride the transient; got: %v", err)
	}
	if attempts != 2 {
		t.Errorf("COPY attempts = %d; want 2 (transient + replay)", attempts)
	}
}

// TestPGCopyChunkRetry_KeylessCleanChunkIsUnaffected pins that the gate
// hangs off the RETRY decision, not off the table: a keyless chunk that
// never hits a transient copies exactly as before.
func TestPGCopyChunkRetry_KeylessCleanChunkIsUnaffected(t *testing.T) {
	withFastPGCopyBackoff(t)
	w := &RowWriter{growGate: &recordingGrowGate{}}

	attempts := 0
	if err := w.copyChunkWithRetry(context.Background(), pgKeylessPinTable("events_raw"), 3, func(context.Context) error {
		attempts++
		return nil
	}); err != nil {
		t.Fatalf("a clean keyless chunk must still succeed; got: %v", err)
	}
	if attempts != 1 {
		t.Errorf("COPY attempts = %d; want 1", attempts)
	}
}

// TestPGReplayKeyPredicatesAgree is this package's own copy of the premise
// check, and it exists because the MySQL one does NOT cover Postgres.
//
// Both engines keep a private effectiveUpsertKeyColumns; the mysql package's
// TestReplayKeyPredicatesAgree binds irbackup.TableReplayIdempotent to the
// MYSQL one and says nothing about this package's. Citing that test here
// would be a gate whose coverage is narrower than the claim it supports —
// so the claim gets its own gate on this side of the boundary.
func TestPGReplayKeyPredicatesAgree(t *testing.T) {
	uniqueOn := func(name string, nullable bool, expr string) *ir.Table {
		return &ir.Table{
			Name:    name,
			Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 8}, Nullable: nullable}},
			Indexes: []*ir.Index{{
				Name: "uq_id", Unique: true,
				Columns: []ir.IndexColumn{{Column: "id", Expression: expr}},
			}},
		}
	}
	cases := []struct {
		name  string
		table *ir.Table
	}{
		{"nil", nil},
		{"keyless", pgKeylessPinTable("t")},
		{"single-column PK", pgKeyedPinTable("t")},
		{"not-null unique, no PK", uniqueOn("t_uniq", false, "")},
		{"nullable unique, no PK", uniqueOn("t_null_uniq", true, "")},
		{"expression unique only", uniqueOn("t_expr", false, "(id * 2)")},
		{"empty PK index", &ir.Table{
			Name:       "t_empty_pk",
			Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 8}}},
			PrimaryKey: &ir.Index{Unique: true},
		}},
		{"composite unique, all not null", &ir.Table{
			Name: "t_composite",
			Columns: []*ir.Column{
				{Name: "a", Type: ir.Integer{Width: 8}, Nullable: false},
				{Name: "b", Type: ir.Integer{Width: 8}, Nullable: false},
			},
			Indexes: []*ir.Index{{
				Name: "uq_ab", Unique: true,
				Columns: []ir.IndexColumn{{Column: "a"}, {Column: "b"}},
			}},
		}},
		{"non-unique index only", &ir.Table{
			Name:    "t_plain_index",
			Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 8}, Nullable: false}},
			Indexes: []*ir.Index{{Name: "ix_id", Columns: []ir.IndexColumn{{Column: "id"}}}},
		}},
	}
	// Anti-vacuity floor: a matrix that only ever produced one answer would
	// pass while proving nothing about agreement.
	sawTrue, sawFalse := false, false
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			replay := irbackup.TableReplayIdempotent(tc.table)
			_, upsertOK := effectiveUpsertKeyColumns(tc.table)
			if replay != upsertOK {
				t.Errorf("predicates disagree: irbackup.TableReplayIdempotent=%v, "+
					"postgres effectiveUpsertKeyColumns ok=%v. copyChunkWithRetry's B-9 gate and the "+
					"idempotent core's keyless refusal are no longer the same question, so the shared "+
					"gate can refuse a table the upsert path accepts", replay, upsertOK)
			}
			if replay {
				sawTrue = true
			} else {
				sawFalse = true
			}
		})
	}
	if !sawTrue || !sawFalse {
		t.Errorf("matrix produced only one classification (sawTrue=%v sawFalse=%v); "+
			"a single-valued matrix cannot detect divergence", sawTrue, sawFalse)
	}
}

// TestPGKeylessAmbiguousReplayRefusalShape pins the operator-facing surface.
func TestPGKeylessAmbiguousReplayRefusalShape(t *testing.T) {
	cause := errors.New(`could not extend file "base/1/2": No space left on device`)
	err := errKeylessAmbiguousReplay("events_raw", 50000, cause)
	if !errors.Is(err, cause) {
		t.Error("the refusal must wrap the underlying transient so it stays diagnosable")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeCopyRetryAmbiguousKeyless {
		t.Errorf("code = %v (coded=%v); want %s", ce, ok, sluicecode.CodeCopyRetryAmbiguousKeyless)
	}
	for _, want := range []string{"events_raw", "50000", "PRIMARY KEY"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("message must contain %q; got: %v", want, err)
		}
	}
}
