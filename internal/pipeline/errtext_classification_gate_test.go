// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"testing"

	"sluicesync.dev/sluice/internal/errclassgate"
)

// TestNoErrorTextClassification_Pipeline instantiates the audit-backlog C-1
// gate over the orchestrator.
//
// This package is the one where the class was most defensible and still wrong.
// It cannot import an engine, so when it needed to recognise "the slot is
// already gone" it matched the error TEXT and wrote down why. The reasoning was
// sound; the conclusion was not. The fix was to name the marker in the
// [ir.SlotManager] contract — engine-neutrality and structural classification
// were never actually in tension.
//
// Coverage: string literals in `strings.Contains(x, "…")` and prose tokens in
// slice literals, in the non-test files of THIS directory only.
func TestNoErrorTextClassification_Pipeline(t *testing.T) {
	errclassgate.AssertNoErrorTextClassification(t, errclassgate.SQLStateTextConfig{
		Dir: ".",
		// Floor from the 18 calls measured 2026-08-08, minus slack.
		MinContainsCalls: 12,
		Allowed:          map[string]string{},
	})
}
