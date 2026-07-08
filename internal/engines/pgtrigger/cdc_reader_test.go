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

// TestPollQuery_ComparesInXID8Domain pins the epoch-safety shape of
// the §2 safety-lag hold-back: the poll must compare the
// trigger-recorded `txid` (xid8-as-bigint, epoch-carrying, NOT NULL
// since the engine's first release) against pg_snapshot_xmin — never
// the row's system `xmin`. Row xmin is a 32-bit epoch-LESS xid, so an
// xmin-based predicate degenerates to always-true once the cluster's
// lifetime txid count crosses 2^32 and in-flight rows silently stop
// being held back — the overlap-commit gap the predicate exists to
// prevent (live-confirmed on a pg_resetwal-epoch-bumped PG 16; the
// live pin is TestCDCReader_XIDEpochBump).
func TestPollQuery_ComparesInXID8Domain(t *testing.T) {
	q := pollQuery(`"public"."sluice_change_log"`)
	if !strings.Contains(q, "txid < pg_snapshot_xmin(pg_current_snapshot())::text::bigint") {
		t.Errorf("poll query lost the xid8-domain hold-back predicate:\n%s", q)
	}
	if strings.Contains(q, "xmin::text") {
		t.Errorf("poll query compares the 32-bit row xmin against a 64-bit xid8 — the epoch-wrap silent-gap bug:\n%s", q)
	}
}

// TestAnchorQuery_ComparesInXID8Domain is the anchor-side twin of
// TestPollQuery_ComparesInXID8Domain: with row xmin on the left, the
// `>=` arm never matches post-epoch-1, COALESCE falls through to
// MAX(id), and the Bug-94 too-high-anchor cold-start gap resurfaces.
func TestAnchorQuery_ComparesInXID8Domain(t *testing.T) {
	q := anchorQuery(`"public"."sluice_change_log"`)
	if !strings.Contains(q, "txid >= pg_snapshot_xmin(pg_current_snapshot())::text::bigint") {
		t.Errorf("anchor query lost the xid8-domain in-flight arm:\n%s", q)
	}
	if strings.Contains(q, "xmin::text") {
		t.Errorf("anchor query compares the 32-bit row xmin against a 64-bit xid8 — the epoch-wrap Bug-94 regression:\n%s", q)
	}
	if !strings.Contains(q, "MIN(id) - 1") || !strings.Contains(q, "COALESCE(MAX(id), 0)") {
		t.Errorf("anchor query lost the (first-unsettled − 1, else MAX) shape:\n%s", q)
	}
}
