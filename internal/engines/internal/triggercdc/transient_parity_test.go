// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Corpus-parity change-detector for the trigger-CDC transient
// classifier (audit 2026-07-23 QUAL-1 / gate G-9): this site's
// effective transient shape set must equal the shared
// internal/nettransient corpus. This is the site the QUAL-1 drift
// actually hit — the Bug 199a/200 Windows wordings (`connectex:`,
// "actively refused", "connection timed out") reached the pipeline and
// both appliers but never this list, so a pgtrigger-source sync on
// Windows exited terminally on a routine managed-PG restart.

package triggercdc

import (
	"errors"
	"fmt"
	"testing"

	"sluicesync.dev/sluice/internal/nettransient"
)

func TestIsTransientTransportError_CorpusParity(t *testing.T) {
	for _, shape := range nettransient.TextShapes {
		shape := shape
		t.Run(shape, func(t *testing.T) {
			err := fmt.Errorf("pgtrigger: poll change log: %w",
				errors.New("driver: "+shape+" (framed)"))
			if !IsTransientTransportError(err) {
				t.Errorf("IsTransientTransportError does not accept shared corpus shape %q — the site drifted from internal/nettransient", shape)
			}
		})
	}
	// The shared exclusions hold at this site too.
	if IsTransientTransportError(errors.New(`Post "https://typo": dial tcp: lookup typo: no such host`)) {
		t.Error("'no such host' must stay terminal (operator error) at the trigger-CDC site")
	}
}
