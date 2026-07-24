// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 204 pins: the Bug 202 progress ledger's young-Run seeding gap. The
// ledger baseline (lastFailureToken) only ever advanced inside counted-
// failure iterations, so when a stream's FIRST target outage struck within
// one sentinel poll (~5s) of the Run's start, no successful observation ever
// landed in an iteration, the baseline stayed invalid forever, and the
// SECOND outage's evidence had nothing to compare against — the compressed
// young-Run repro still exhausted the budget across outages, even when the
// fatal iteration itself held two successful anchor-bearing reads with
// DIFFERENT tokens (provable durable commits during that very attempt).
//
// The fix is two complementary, strictly evidence-based legs (D0-4: a
// failed read is never evidence): (a) two successful observations WITHIN
// one iteration showing different tokens credit a fresh budget with no
// baseline at all — the sentinel now keeps its earliest AND latest
// successful observations so an attempt-spanning advance is visible even
// when both boundary reads failed; (b) the baseline is SEEDED from the
// Run's first successful anchor-bearing observation wherever it occurs
// (a before-read on an iteration that never reaches the counted-failure
// bookkeeping, e.g. the reactive-resnapshot continue), not only inside
// counted-failure iterations. The loud floors are pinned unchanged in
// streamer_retry_carryover_test.go: a stuck batch (frozen token) and a
// target down from birth (no observation at all) still exhaust at exactly
// `attempts`.

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestRunWithRetry_YoungRunFirstOutageBeforeFirstSentinelPoll is the Bug 204
// core pin (RED pre-fix), mirroring the compressed bugA_carryover.sh repro:
// the FIRST outage strikes within one sentinel poll of a cold start, so
// iterations 1..k accumulate with no successful observation anywhere; the
// recovery attempt then applies real durable progress that only the
// mid-attempt sentinel observes (the boundary before-read ran while the
// store was still down, the after-read hits outage #2); pre-fix the fatal
// iteration's own t1→t2 divergence could not credit (no baseline) and the
// budget exhausted despite verified progress between the outages.
func TestRunWithRetry_YoungRunFirstOutageBeforeFirstSentinelPoll(t *testing.T) {
	// Young Run: cold start — the store is up but holds no cdc-state anchor
	// row yet, so the first boundary read is found=false (not an
	// observation, per D0-4 only anchor-bearing reads are evidence).
	store := &flakyPosStore{found: false}
	s := carryoverStreamer(store, 3)

	var calls int
	s.runOnceFn = func(context.Context) error {
		calls++
		switch calls {
		case 1:
			// Cold copy + anchor write + first applies land durably (t0),
			// then outage #1 strikes before the sentinel's first poll
			// opportunity: this iteration ends with NO successful
			// observation (before: found=false; mid: no poll; after: down).
			store.set(true, "t0", true)
			return &retriableWrapper{err: errors.New("postgres: applier: exec: connection refused")}
		case 2:
			// Outage #1 persists: reads down, apply fails again.
			return &retriableWrapper{err: errors.New("postgres: applier: exec: connection refused")}
		case 3:
			// Target recovered as the attempt re-established; the stream
			// applies real durable progress (t1 → t2), observed ONLY by the
			// sentinel — the boundary before-read ran while the store was
			// still down, and the after-read hits outage #2. Two successful
			// observations with different tokens inside one iteration prove
			// durable commits during this very attempt.
			store.set(false, "t1", true)
			time.Sleep(60 * time.Millisecond) // several sentinel polls at 2ms
			store.set(false, "t2", true)
			time.Sleep(60 * time.Millisecond)
			store.set(true, "t2", true)
			return &retriableWrapper{err: errors.New("postgres: applier: exec: connection refused")}
		default:
			// Outage #2 rode out on the fresh budget.
			return nil
		}
	}

	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v; want nil — the second outage's first failure exhausted a budget the fatal iteration's own observed t1→t2 advance should have reset (Bug 204 young-Run gap)", err)
	}
	if calls != 4 {
		t.Fatalf("runOnce called %d times; want 4 (three failures + the recovery attempt)", calls)
	}
}

// TestRunWithRetry_ResnapshotIterationSeedsLedgerBaseline pins Bug 204 leg
// (b) in isolation: the reactive-resnapshot `continue` (ADR-0093) skips the
// counted-failure bookkeeping entirely, so pre-fix its before-read — here
// the Run's ONLY successful boundary observation — never seeded the ledger,
// and a later outage's single-token evidence (deliberately NO
// within-iteration divergence, so leg (a) cannot fire) had no baseline to
// prove cross-outage progress against.
func TestRunWithRetry_ResnapshotIterationSeedsLedgerBaseline(t *testing.T) {
	// Warm relaunch: the anchor row exists, so the first iteration's
	// before-read observes t0 — the seed.
	store := &flakyPosStore{token: "t0", found: true}
	s := carryoverStreamer(store, 2)

	var calls int
	s.runOnceFn = func(context.Context) error {
		calls++
		switch calls {
		case 1:
			// Reactive invalid position (ADR-0093): the resnapshot decision
			// retries WITHOUT counting — and pre-fix without consuming this
			// iteration's before-read (t0) into the ledger. The target goes
			// down in the same breath (outage #1 begins).
			store.set(true, "t0", true)
			return fmt.Errorf("vstream: resume: %w", ir.ErrPositionInvalid)
		case 2:
			// The forced cold-start re-snapshot fails fast against the
			// still-down target: a counted failure with no successful reads.
			return &retriableWrapper{err: errors.New("postgres: applier: exec: connection refused")}
		case 3:
			// Recovery mid-attempt: the re-copy durably advances the
			// position (t0 → t1) but every observation this iteration shows
			// the SAME token t1 (before-read: store was still down; after-
			// read: outage #2) — only the seeded t0 baseline can credit it.
			store.set(false, "t1", true)
			time.Sleep(60 * time.Millisecond) // several sentinel polls at 2ms
			store.set(true, "t1", true)
			return &retriableWrapper{err: errors.New("postgres: applier: exec: connection refused")}
		default:
			// Outage #2 rode out on the fresh budget.
			return nil
		}
	}

	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v; want nil — the resnapshot iteration's before-read (t0) must seed the ledger baseline so the observed t0→t1 advance credits a fresh budget (Bug 204 leg b)", err)
	}
	if calls != 4 {
		t.Fatalf("runOnce called %d times; want 4 (resnapshot + two failures + the recovery attempt)", calls)
	}
}
