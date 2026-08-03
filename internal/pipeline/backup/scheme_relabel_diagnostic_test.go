// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The scheme-relabel diagnostic (audit 2026-08-01 Sec2).
//
// A signed chain's recorded scheme is folded into the signed canonical bytes,
// so an attacker cannot relabel it and still have it verify. They do not need
// it to verify. Editing the scheme to one the operator holds no key for moves
// the chain from "verified" to "UNVERIFIABLE" — and unverifiable is a WARN
// that proceeds. One field edit, every `.sig` left in place, exit 0.
//
// What made it worse than a missed check: the warning told the operator to
// "pass the chain's --encrypt key material to verify" — which is precisely
// what they had already done. The one signal worth acting on was worded as a
// forgotten step.
//
// sluice cannot distinguish a relabel from an operator who legitimately holds
// only the encryption KEK for an asymmetrically-signed chain; both present as
// "material supplied, wrong family". So this is NOT a refusal by default. What
// it must do is stop giving advice already followed, name the claimed scheme
// against what was actually supplied, and say that a relabel produces exactly
// this shape. These pin that split.

package backup

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

func TestUnverifiableSignedArtifact_DistinguishesRelabelFromNoKey(t *testing.T) {
	edPub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	t.Run("material supplied but wrong family names the claimed scheme", func(t *testing.T) {
		logs := captureBackupSlog(t)
		mat := verifyMaterial{verifyPub: edPub}

		if err := unverifiableSignedArtifact(
			context.Background(), "chain", "hmac-kek", mat, false,
		); err != nil {
			t.Fatalf("non-strict must proceed: %v", err)
		}

		got := logs.String()
		// The claimed scheme has to appear — it is the thing an operator
		// compares against what they passed.
		if !strings.Contains(got, "hmac-kek") {
			t.Errorf("warning does not name the claimed scheme; got:\n%s", got)
		}
		// And it must NOT tell them to supply what they already supplied.
		if strings.Contains(got, "no verification key is available") {
			t.Errorf("warning still reports 'no verification key is available' to an operator who "+
				"supplied one — this is the exact misdirection Sec2 filed; got:\n%s", got)
		}
		// The relabel possibility must be named, or the operator has no way
		// to know a downgrade looks like this.
		if !strings.Contains(got, "edited") {
			t.Errorf("warning does not raise the possibility that the recorded scheme was edited; got:\n%s", got)
		}
	})

	t.Run("no material at all keeps the plain missing-key advice", func(t *testing.T) {
		logs := captureBackupSlog(t)

		if err := unverifiableSignedArtifact(
			context.Background(), "chain", "hmac-kek", verifyMaterial{}, false,
		); err != nil {
			t.Fatalf("non-strict must proceed: %v", err)
		}

		got := logs.String()
		// This operator genuinely did not supply anything, so "pass key
		// material" is correct advice and must survive. A fix that showed the
		// tamper wording to everyone would be crying wolf on the ordinary
		// unsigned-verify path.
		if !strings.Contains(got, "no verification key is available") {
			t.Errorf("the genuine no-key case lost its correct advice; got:\n%s", got)
		}
		if strings.Contains(got, "edited") {
			t.Errorf("the genuine no-key case should NOT suggest tampering; got:\n%s", got)
		}
	})

	t.Run("strict refuses either way, and says which case it is", func(t *testing.T) {
		captureBackupSlog(t)

		relabel := unverifiableSignedArtifact(
			context.Background(), "chain", "hmac-kek", verifyMaterial{verifyPub: edPub}, true,
		)
		if relabel == nil {
			t.Fatal("--require-signature must refuse when the claimed scheme cannot be verified")
		}
		if !strings.Contains(relabel.Error(), "hmac-kek") {
			t.Errorf("strict refusal does not name the claimed scheme: %v", relabel)
		}

		noKey := unverifiableSignedArtifact(
			context.Background(), "chain", "", verifyMaterial{}, true,
		)
		if noKey == nil {
			t.Fatal("--require-signature must refuse when no key is available")
		}
	})
}

// describeVerifyMaterial is what lets the message contrast supplied against
// claimed. If it ever collapsed to a constant the warnings above would still
// print, just uselessly.
func TestDescribeVerifyMaterial_DistinguishesEachCombination(t *testing.T) {
	edPub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	seen := map[string]bool{}
	for _, mat := range []verifyMaterial{
		{},
		{verifyPub: edPub},
	} {
		d := describeVerifyMaterial(mat)
		if d == "" {
			t.Fatal("describeVerifyMaterial returned an empty description")
		}
		if seen[d] {
			t.Errorf("two distinct material combinations share the description %q — the message cannot "+
				"contrast supplied against claimed", d)
		}
		seen[d] = true
	}
}

// captureBackupSlog redirects the default logger into a buffer for the
// duration of a test. Concurrency-safe because these tests do not log from
// goroutines, but the buffer is guarded anyway so a future one can.
func captureBackupSlog(t *testing.T) *syncBuf {
	t.Helper()
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	buf := &syncBuf{}
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	return buf
}

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}
