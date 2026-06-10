// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// capsTxKiller mirrors the Vitess-backed flavors (planetscale,
// vitess): the vtgate ~20s transaction killer drives the conservative
// apply-batch warning + AIMD defaults.
var capsTxKiller = ir.Capabilities{TransactionKiller: true}

// TestMaybeWarnApplyBatchSizeRisky_FiresForTxKillerAboveThreshold
// covers the GitHub #18 Phase 2 safety-rail: WARN when the target
// declares TransactionKiller AND --apply-batch-size > 50.
func TestMaybeWarnApplyBatchSizeRisky_FiresForTxKillerAboveThreshold(t *testing.T) {
	buf := captureSlog(t)
	maybeWarnApplyBatchSizeRisky(context.Background(), capsTxKiller, "planetscale", 100)
	out := buf.String()
	if out == "" {
		t.Fatal("expected WARN log for planetscale + batch=100; got empty")
	}
	if !strings.Contains(out, "apply-batch-size > 50") {
		t.Errorf("warn missing threshold mention; got %q", out)
	}
	if !strings.Contains(out, "planetscale") {
		t.Errorf("warn missing target engine name; got %q", out)
	}
}

// TestMaybeWarnApplyBatchSizeRisky_QuietWithoutTxKiller confirms
// no warn fires for vanilla MySQL / PG targets — the safety rail
// is scoped to Vitess's documented tx-killer behaviour, which those
// engines don't declare.
func TestMaybeWarnApplyBatchSizeRisky_QuietWithoutTxKiller(t *testing.T) {
	buf := captureSlog(t)
	for _, tc := range []struct {
		caps ir.Capabilities
		name string
	}{
		{capsMySQL, "mysql"},
		{capsSlotPG, "postgres"},
		{ir.Capabilities{}, "sqlite"},
	} {
		maybeWarnApplyBatchSizeRisky(context.Background(), tc.caps, tc.name, 1000)
	}
	if buf.Len() != 0 {
		t.Errorf("expected silent for non-tx-killer targets even with batch=1000; got log %q", buf.String())
	}
}

// TestMaybeWarnApplyBatchSizeRisky_QuietForTxKillerAtSafeBatch
// confirms the warn doesn't fire when an operator is well within
// the 50-row safe zone on a tx-killer target.
func TestMaybeWarnApplyBatchSizeRisky_QuietForTxKillerAtSafeBatch(t *testing.T) {
	buf := captureSlog(t)
	for _, n := range []int{0, 1, 10, 25, 50} {
		maybeWarnApplyBatchSizeRisky(context.Background(), capsTxKiller, "planetscale", n)
	}
	if buf.Len() != 0 {
		t.Errorf("expected silent for planetscale at safe batch sizes (≤50); got log %q", buf.String())
	}
}

// TestIsTransientOpenError_NonTransientShapes covers the GitHub #17
// retry-policy WARN suppression: parse errors and credential failures
// must be classified as non-transient so the retry-policy fallback
// doesn't fire its noisy WARN before the real error surfaces.
func TestIsTransientOpenError_NonTransientShapes(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"invalid DSN", errors.New("mysql: invalid DSN: bad scheme")},
		{"DSN missing db", errors.New("mysql: DSN must include a database name")},
		{"parseDSN failure", errors.New("postgres: parseDSN: malformed url")},
		{"access denied", errors.New("Error 1045: Access denied for user")},
		{"unknown database", errors.New("Error 1049: Unknown database 'foo'")},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if isTransientOpenError(c.err) {
				t.Errorf("error %q wrongly classified transient; would fire noisy WARN per GitHub #17", c.err.Error())
			}
		})
	}
}

// TestIsTransientOpenError_TransientShapes confirms the fallback to
// "transient" for unknown / network-shape errors preserves the
// existing WARN-and-fall-through behaviour for legitimate transient
// startup failures.
func TestIsTransientOpenError_TransientShapes(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"connection reset", errors.New("dial tcp: connection reset by peer")},
		{"network timeout", errors.New("dial tcp: i/o timeout")},
		{"unknown error shape", errors.New("some unrecognised error")},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if !isTransientOpenError(c.err) {
				t.Errorf("error %q should be classified transient; got non-transient", c.err.Error())
			}
		})
	}
	// nil boundary
	if isTransientOpenError(nil) {
		t.Errorf("nil should not be transient")
	}
}
