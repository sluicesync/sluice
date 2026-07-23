// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Corpus-parity change-detector for the connect-phase retry (audit
// 2026-07-23 QUAL-1 / gate G-9): the pipeline's effective transient
// shape set must equal the shared internal/nettransient corpus. Before
// the shared matcher existed this list was one of four hand-mirrored
// copies, and they drifted one release after Bug 199 — if this site
// ever stops delegating (or grows a local list again), this pin fails
// instead of the drift shipping.

package pipeline

import (
	"errors"
	"fmt"
	"testing"

	"sluicesync.dev/sluice/internal/nettransient"
)

func TestIsTransientNetworkShape_CorpusParity(t *testing.T) {
	for _, shape := range nettransient.TextShapes {
		shape := shape
		t.Run(shape, func(t *testing.T) {
			err := fmt.Errorf("pipeline: open target change applier: %w",
				errors.New("driver: "+shape+" (framed)"))
			if !isTransientNetworkShape(err) {
				t.Errorf("isTransientNetworkShape does not accept shared corpus shape %q — the site drifted from internal/nettransient", shape)
			}
			// The full connect-retry gate: marked + matched must be the
			// retriable combination runWithRetry falls through on.
			if !isRetriableConnectFailure(connectHint(err)) {
				t.Errorf("isRetriableConnectFailure(connectHint(%q)) = false; want true", shape)
			}
		})
	}
	// The shared exclusions hold at this site too.
	if isTransientNetworkShape(errors.New("dial tcp: lookup db.example.com: no such host")) {
		t.Error("'no such host' must stay terminal (operator error) at the connect-retry site")
	}
}
