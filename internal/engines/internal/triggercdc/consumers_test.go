// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package triggercdc

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// [RegistryCut] is the ONE place the item-115 safety argument lives, shared by
// all three trigger engines (pgtrigger, sqlite-trigger, d1-trigger — the last
// two through one CDCReader). These pins are therefore the class-level gate; the
// per-engine tests pin that each transport's SQL feeds it the right snapshot.

func consumers(pairs ...any) []Consumer {
	out := make([]Consumer, 0, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		out = append(out, Consumer{ID: pairs[i].(string), AppliedID: int64(pairs[i+1].(int))})
	}
	return out
}

// TestRegistryCut_CutsAtTheSlowestConsumer is the crux: with a fast peer at 100
// and a slow peer at 20, the cut is the SLOW one's frontier — the fast peer's
// prune must not reap the 80 rows the slow peer has not read.
func TestRegistryCut_CutsAtTheSlowestConsumer(t *testing.T) {
	got, err := RegistryCut(context.Background(), "eng",
		consumers("fast", 100, "slow", 20), "fast", 100, 0)
	if err != nil {
		t.Fatalf("RegistryCut: %v", err)
	}
	if got != 20 {
		t.Errorf("cut = %d; want 20 (the slowest registered consumer's frontier)", got)
	}
}

// TestRegistryCut_KeepIsSubtractedFromTheMin pins the margin's interaction with
// the MIN: keep is taken off the SLOWEST frontier, never off our own.
func TestRegistryCut_KeepIsSubtractedFromTheMin(t *testing.T) {
	got, err := RegistryCut(context.Background(), "eng",
		consumers("fast", 100, "slow", 20), "fast", 100, 5)
	if err != nil {
		t.Fatalf("RegistryCut: %v", err)
	}
	if got != 15 {
		t.Errorf("cut = %d; want 15 (20 - keep 5)", got)
	}
}

// TestRegistryCut_ClampsToTheCallersOwnFreshFrontier pins the INDEPENDENT bound:
// the caller's registry row was written from an earlier read, so a stale or
// wrong row must never lift the cut above the frontier the caller just read from
// its own target.
func TestRegistryCut_ClampsToTheCallersOwnFreshFrontier(t *testing.T) {
	// The registry claims we are at 90 (and we are the slowest), but a fresh
	// read of our own target says 30 — e.g. the target was restored to an
	// earlier point under us.
	got, err := RegistryCut(context.Background(), "eng",
		consumers("self", 90, "peer", 100), "self", 30, 0)
	if err != nil {
		t.Fatalf("RegistryCut: %v", err)
	}
	if got != 30 {
		t.Errorf("cut = %d; want 30 (the caller's fresh frontier, below its own registry row)", got)
	}
}

// TestRegistryCut_StaleLowSelfRowDoesNotBlockOurOwnPrune is the regression the
// pgtrigger auto-prune integration test caught: the caller's own registry row is
// a CACHE refreshed once a minute, so a stream that registered 0 at cold start
// and has since applied 500 rows would, if its own row were taken at face value,
// block its own prune until the next refresh. The fresh read is the authority
// for the caller; only PEERS are taken from the registry.
func TestRegistryCut_StaleLowSelfRowDoesNotBlockOurOwnPrune(t *testing.T) {
	got, err := RegistryCut(context.Background(), "eng",
		consumers("self", 0, "peer", 200), "self", 500, 0)
	if err != nil {
		t.Fatalf("RegistryCut: %v", err)
	}
	if got != 200 {
		t.Errorf("cut = %d; want 200 (the PEER's frontier — our own stale row must not hold us back)", got)
	}
}

// TestRegistryMin_SubstitutesSelfAndReportsNoEvidence pins the non-refusing half
// used by the operator prune: same substitution rule, and ok=false on an empty
// registry so that path can keep its pre-item-115 behaviour instead of refusing.
func TestRegistryMin_SubstitutesSelfAndReportsNoEvidence(t *testing.T) {
	if _, ok := RegistryMin(nil, "self", 100); ok {
		t.Error("RegistryMin(empty) reported evidence; want ok=false")
	}
	got, ok := RegistryMin(consumers("self", 0, "peer", 40), "self", 90)
	if !ok || got != 40 {
		t.Errorf("RegistryMin = (%d, %v); want (40, true) — self substituted, peer holds the MIN", got, ok)
	}
	// With no self identity (a direct engine call), every row is a peer.
	got, ok = RegistryMin(consumers("someone", 7), "", 90)
	if !ok || got != 7 {
		t.Errorf("RegistryMin(no self) = (%d, %v); want (7, true)", got, ok)
	}
}

// TestRegistryCut_EmptyRegistryRefuses is the primary hazard of the whole fix:
// an empty registry is indistinguishable from one nobody has written to yet, so
// reading it as "no consumers ⇒ prune everything" would be a WORSE silent-loss
// bug than the one item 115 closes.
func TestRegistryCut_EmptyRegistryRefuses(t *testing.T) {
	got, err := RegistryCut(context.Background(), "eng", nil, "self", 100, 0)
	if err == nil {
		t.Fatalf("RegistryCut(empty) = %d, nil; want a loud refusal", got)
	}
	if got != 0 {
		t.Errorf("refused cut = %d; want 0", got)
	}
	if !strings.Contains(err.Error(), "EMPTY") {
		t.Errorf("error %q does not name the empty registry", err)
	}
}

// TestRegistryCut_UnregisteredSelfRefuses: a cut derived from peers alone is not
// a safe bound for us — our own row is what proves the cut is below OUR frontier
// too. (It also means a registration that has been failing silently cannot be
// papered over by a peer's presence.)
func TestRegistryCut_UnregisteredSelfRefuses(t *testing.T) {
	_, err := RegistryCut(context.Background(), "eng", consumers("peer", 100), "self", 50, 0)
	if err == nil {
		t.Fatal("RegistryCut with the caller absent from the registry returned nil; want a loud refusal")
	}
	if !strings.Contains(err.Error(), "not in the change-log consumer registry") {
		t.Errorf("error %q does not name the missing registration", err)
	}
}

// TestRegistryCut_NonPositiveCutIsLegal: a consumer that has applied nothing
// (frontier 0 — a stream in cold copy) yields a non-positive cut, which the
// callers treat as "nothing safely reapable yet". It must be a value, not an
// error.
func TestRegistryCut_NonPositiveCutIsLegal(t *testing.T) {
	got, err := RegistryCut(context.Background(), "eng",
		consumers("copying", 0, "self", 500), "self", 500, 10)
	if err != nil {
		t.Fatalf("RegistryCut: %v", err)
	}
	if got > 0 {
		t.Errorf("cut = %d; want <= 0 (a consumer still in cold copy blocks the prune entirely)", got)
	}
}

// TestRegistryCut_SingleConsumerIsItsOwnFrontier pins the common case — one
// sync, no peers — still prunes exactly as ADR-0137 Phase B always did.
func TestRegistryCut_SingleConsumerIsItsOwnFrontier(t *testing.T) {
	got, err := RegistryCut(context.Background(), "eng", consumers("self", 80), "self", 80, 3)
	if err != nil {
		t.Fatalf("RegistryCut: %v", err)
	}
	if got != 77 {
		t.Errorf("cut = %d; want 77 (80 - keep 3), the pre-item-115 single-stream bound", got)
	}
}

// TestRegistryCut_StaleConsumerIsWarnedNotEvicted pins the deliberate
// non-behaviour: a registration that has gone quiet still holds the prune back.
// Evicting it automatically would delete the rows a stream that is down for
// maintenance has not read — the silent loss the registry exists to prevent.
func TestRegistryCut_StaleConsumerIsWarnedNotEvicted(t *testing.T) {
	stale := []Consumer{
		{ID: "self", AppliedID: 900},
		{ID: "abandoned", AppliedID: 5, AgeSeconds: int64(StaleConsumerAfter/time.Second) + 60},
	}
	got, err := RegistryCut(context.Background(), "eng", stale, "self", 900, 0)
	if err != nil {
		t.Fatalf("RegistryCut: %v", err)
	}
	if got != 5 {
		t.Errorf("cut = %d; want 5 — a stale consumer is WARNED about, never evicted", got)
	}
}

// TestErrConsumerRegistryUnavailable_IsWrappable pins that the fail-closed
// sentinel survives the engines' fmt.Errorf wrapping, so a caller can tell
// "this source predates the registry" from "the prune failed".
func TestErrConsumerRegistryUnavailable_IsWrappable(t *testing.T) {
	wrapped := errors.New("x")
	if errors.Is(wrapped, ErrConsumerRegistryUnavailable) {
		t.Fatal("unrelated error matched the sentinel")
	}
	if !errors.Is(ErrConsumerRegistryUnavailable, ErrConsumerRegistryUnavailable) {
		t.Fatal("sentinel does not match itself")
	}
}
