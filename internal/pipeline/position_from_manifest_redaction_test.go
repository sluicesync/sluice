// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/redact"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// newRedactedChainStore writes a one-link chain whose full carries the
// given marker (nil for an ordinary unredacted chain) and returns its
// store.
func newRedactedChainStore(t *testing.T, marker *irbackup.RedactionInfo) irbackup.Store {
	t.Helper()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("new local store: %v", err)
	}
	full := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 9, 6, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Schema:        &ir.Schema{},
		Kind:          irbackup.BackupKindFull,
		EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"1/200"}`},
		PartialState:  irbackup.BackupStateComplete,
		Redaction:     marker,
	}
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(context.Background(), store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("write full: %v", err)
	}
	return store
}

// TestPositionFromManifestCarriesTheRedactionMarker pins the half of the
// audit 2026-09-06 HIGH that the defect actually was: the loader HELD the
// chain's redaction marker and threw it away.
//
// LoadChainTerminalPosition builds the whole lineage and reads the
// terminal manifest, so the marker was in scope at the return statement.
// Returning only the position made `--position-from-manifest` the one
// redaction-posture door in the product that reached no guard — and the
// harm is worse than the archive bug this release closes, because the
// plaintext lands on a LIVE target rather than in a file.
//
// The marker is a RETURN VALUE rather than a separate lookup for exactly
// this reason: a caller cannot ignore it without writing a visible `_`.
func TestPositionFromManifestCarriesTheRedactionMarker(t *testing.T) {
	marker := &irbackup.RedactionInfo{RuleCount: 1, Fingerprint: "83c844a641723823"}
	store := newRedactedChainStore(t, marker)

	_, got, err := LoadChainTerminalPosition(context.Background(), store)
	if err != nil {
		t.Fatalf("LoadChainTerminalPosition: %v", err)
	}
	if got == nil {
		t.Fatal("the terminal manifest's redaction marker was dropped. The loader reads that manifest to " +
			"get EndPosition, so the marker is in scope at the return; dropping it is what let a redacted " +
			"chain be resumed by a non-redacting sync, overwriting restored redacted values with plaintext " +
			"on a live target at exit 0.")
	}
	if *got != *marker {
		t.Errorf("marker round-tripped wrong: got %+v want %+v", *got, *marker)
	}

	// Control: an unredacted chain must hand back nil, not a zero-valued
	// marker. A zero marker would make every ordinary sync disagree with
	// every ordinary chain and refuse.
	plain := newRedactedChainStore(t, nil)
	if _, got, err := LoadChainTerminalPosition(context.Background(), plain); err != nil || got != nil {
		t.Errorf("an unredacted chain must yield a nil marker; got %+v (err %v)", got, err)
	}
}

// TestRefusePositionFromRedactedChain grades the predicate across the
// four posture combinations. Two must pass and two must refuse; a door
// that only ever refuses is as broken as one that never does, and the
// agreeing-policy case is the one an operator doing the right thing
// hits.
func TestRefusePositionFromRedactedChain(t *testing.T) {
	redactor := redact.New()
	redactor.Set("public", "users", "email", redact.Null{})
	chainMarker := backup.MarkerFor(redactor)
	if chainMarker == nil {
		t.Fatal("MarkerFor returned nil for a configured registry — the rest of this test would be vacuous")
	}

	t.Run("plain chain, plain sync proceeds", func(t *testing.T) {
		if err := backup.RefusePositionFromRedactedChain(nil, nil, "--position-from-manifest"); err != nil {
			t.Fatalf("the ordinary case must proceed; got: %v", err)
		}
	})

	t.Run("redacted chain, matching sync proceeds", func(t *testing.T) {
		if err := backup.RefusePositionFromRedactedChain(chainMarker, redactor, "--position-from-manifest"); err != nil {
			t.Fatalf("a sync configured with the SAME policy is coherent and must proceed; got: %v", err)
		}
	})

	t.Run("redacted chain, plain sync refuses", func(t *testing.T) {
		err := backup.RefusePositionFromRedactedChain(chainMarker, nil, "--position-from-manifest")
		if err == nil {
			t.Fatal("a non-redacting sync resumed off a redacted chain was accepted — every replicated " +
				"UPDATE overwrites the restore's redacted value with plaintext on a live target")
		}
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeBackupRedactedChain {
			t.Errorf("refusal does not carry %s: %v", sluicecode.CodeBackupRedactedChain, err)
		}
		if !strings.Contains(ce.Hint, "Match the sync's redaction policy") {
			t.Errorf("remedy is not the position-from-manifest one: %q", ce.Hint)
		}
	})

	t.Run("plain chain, redacting sync refuses", func(t *testing.T) {
		// The reverse direction. The target already holds plaintext for
		// every pre-existing row, so redacting only the tail produces a
		// target nobody can audit.
		if err := backup.RefusePositionFromRedactedChain(nil, redactor, "--position-from-manifest"); err == nil {
			t.Fatal("a redacting sync resumed off an unredacted chain was accepted; the door checks " +
				"AGREEMENT in both directions, not just chain-is-clean")
		}
	})

	t.Run("a differing policy on both sides refuses", func(t *testing.T) {
		other := redact.New()
		other.Set("public", "users", "ssn", redact.Null{})
		if err := backup.RefusePositionFromRedactedChain(chainMarker, other, "--position-from-manifest"); err == nil {
			t.Fatal("two DIFFERENT redaction policies were treated as agreeing — the fingerprint " +
				"comparison is not reaching the decision")
		}
	})
}

// TestPhaseLookupPositionReachesTheRedactionDoor is the wiring half.
// RefusePositionFromRedactedChain can be perfect and phaseLookupPosition
// can still never call it — which is precisely the state this release
// shipped in until the pre-tag review, and which no amount of testing the
// predicate would have caught.
func TestPhaseLookupPositionReachesTheRedactionDoor(t *testing.T) {
	b, err := os.ReadFile("streamer_run_phases.go")
	if err != nil {
		t.Fatalf("read streamer_run_phases.go: %v", err)
	}
	src := string(b)

	loadAt := strings.Index(src, "LoadChainTerminalPosition(ctx,")
	if loadAt < 0 {
		t.Fatal("anchor 'LoadChainTerminalPosition(ctx,' not found in streamer_run_phases.go — the " +
			"position-from-manifest call site moved and this gate can no longer see it. Re-anchor it.")
	}
	doorAt := strings.Index(src, "RefusePositionFromRedactedChain(")
	if doorAt < 0 {
		t.Fatal("phaseLookupPosition no longer calls backup.RefusePositionFromRedactedChain. Resuming " +
			"CDC off a redacted chain under a non-redacting sync overwrites restored redacted values " +
			"with plaintext on a live target, at exit 0. Re-wire it rather than deleting this gate.")
	}
	// Ordering: the door must run before the stream is opened, and the
	// nearest observable proxy in this file is that it follows the load
	// (it needs the marker) and precedes the preflight that opens work.
	if doorAt < loadAt {
		t.Error("the redaction door runs before the chain is loaded, so it cannot be reading the " +
			"chain's marker")
	}
	preflightAt := strings.Index(src, "runPositionFromManifestPreflight(")
	if preflightAt > 0 && doorAt > preflightAt {
		t.Error("the redaction door runs after the position-from-manifest preflight; it must refuse " +
			"before anything probes or opens the source")
	}
}
