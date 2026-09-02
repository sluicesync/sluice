// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"strings"
	"testing"
	"time"
)

// TestCDCReader_SetPollInterval_OverridesDefault pins the roadmap
// item 18(c) / ADR-0066 §6 setter contract: an operator-supplied
// `--poll-interval=DUR` flows through Streamer.PollInterval to this
// reader via the streamer's pollIntervalSetter type assertion.
func TestCDCReader_SetPollInterval_OverridesDefault(t *testing.T) {
	r := &CDCReader{pollInterval: defaultPollInterval}
	if r.pollInterval != defaultPollInterval {
		t.Fatalf("precondition: default poll interval = %v; want %v", r.pollInterval, defaultPollInterval)
	}
	r.SetPollInterval(250 * time.Millisecond)
	if r.pollInterval != 250*time.Millisecond {
		t.Errorf("after SetPollInterval(250ms): pollInterval = %v; want 250ms", r.pollInterval)
	}
}

// TestCDCReader_SetPollInterval_ZeroIsNoop pins the "0 means leave
// the default in place" contract so the streamer's `if s.PollInterval
// > 0` gate isn't load-bearing alone — a future caller bypassing that
// gate and calling SetPollInterval(0) directly must NOT collapse the
// reader to a busy-loop.
func TestCDCReader_SetPollInterval_ZeroIsNoop(t *testing.T) {
	r := &CDCReader{pollInterval: defaultPollInterval}
	r.SetPollInterval(0)
	if r.pollInterval != defaultPollInterval {
		t.Errorf("after SetPollInterval(0): pollInterval = %v; want default %v (zero must NOT collapse the poll loop)",
			r.pollInterval, defaultPollInterval)
	}
}

// TestCDCReader_SetPollInterval_NegativeIsNoop mirrors the zero case
// — a negative duration is meaningless for a polling cadence; the
// setter rejects it rather than letting it propagate to time.Timer.
func TestCDCReader_SetPollInterval_NegativeIsNoop(t *testing.T) {
	r := &CDCReader{pollInterval: defaultPollInterval}
	r.SetPollInterval(-1 * time.Second)
	if r.pollInterval != defaultPollInterval {
		t.Errorf("after SetPollInterval(-1s): pollInterval = %v; want default %v (negative must NOT propagate)",
			r.pollInterval, defaultPollInterval)
	}
}

// TestPollQuery_BoundsTheWindowByTheSettledCeiling pins the poll's
// fetch shape: an id-ordered, LIMIT-ed window truncated at the shared
// settled ceiling. The window is what keeps a catch-up backlog from
// turning the ceiling's aggregates into a per-poll tail scan, and the
// `id > $1` / `ORDER BY id ASC` pair is what lets [CDCReader.poll] walk
// the contiguous run (cdc_gapfree.go).
func TestPollQuery_BoundsTheWindowByTheSettledCeiling(t *testing.T) {
	q := pollQuery(`"public"."sluice_change_log"`)
	for _, want := range []string{
		"WITH " + changeLogWindow + " AS MATERIALIZED",
		"WHERE id > $1 ORDER BY id ASC LIMIT $2",
		"WHERE id <= " + settledCeilingSQL(changeLogWindow),
	} {
		if !strings.Contains(q, want) {
			t.Errorf("poll query lost %q:\n%s", want, q)
		}
	}
	if strings.Contains(q, "xmin::text") {
		t.Errorf("poll query compares the 32-bit row xmin against a 64-bit xid8 — the epoch-wrap silent-gap bug:\n%s", q)
	}
}

// TestAnchorQuery_ComparesInXID8Domain is the anchor-side twin of
// TestPollQuery_ComparesInXID8Domain: with row xmin on the left, the
// `>=` arm never matches post-epoch-1, COALESCE falls through to
// pg_catalog.MAX(id), and the Bug-94 too-high-anchor cold-start gap resurfaces.
func TestAnchorQuery_ComparesInXID8Domain(t *testing.T) {
	q := anchorQuery(`"public"."sluice_change_log"`)
	if !strings.Contains(q, "txid >= pg_catalog.pg_snapshot_xmin(pg_catalog.pg_current_snapshot())::text::bigint") {
		t.Errorf("anchor query lost the xid8-domain in-flight arm:\n%s", q)
	}
	if strings.Contains(q, "xmin::text") {
		t.Errorf("anchor query compares the 32-bit row xmin against a 64-bit xid8 — the epoch-wrap Bug-94 regression:\n%s", q)
	}
	if !strings.Contains(q, "pg_catalog.MIN(id) - 1") || !strings.Contains(q, "COALESCE(pg_catalog.MAX(id), 0)") {
		t.Errorf("anchor query lost the (first-unsettled − 1, else MAX) shape:\n%s", q)
	}
}
