// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestRestartRefusalSentinelsAreTheMarkers binds each engine-neutral sentinel
// this engine wraps to the grep-stable marker its refusal prints. The
// sentinel's text IS the marker (it is wrapped with %w, not appended), so a
// drift between the two would change the operator-facing message.
func TestRestartRefusalSentinelsAreTheMarkers(t *testing.T) {
	for _, tc := range []struct {
		sentinel error
		marker   string
	}{
		{ir.ErrShardedTargetVindexUpdate, shardedTargetVindexUpdateMarker},
		{ir.ErrCharsetNotDecodable, charsetNotDecodableMarker},
		{ir.ErrDSNTimeZoneNotUTC, "DSN-TIME-ZONE-NOT-UTC"},
	} {
		if tc.sentinel.Error() != tc.marker {
			t.Errorf("sentinel text %q != marker %q", tc.sentinel.Error(), tc.marker)
		}
	}
	if errCharsetNotDecodable != ir.ErrCharsetNotDecodable { //nolint:errorlint // identity is the property under test
		t.Error("errCharsetNotDecodable is not ir.ErrCharsetNotDecodable; the charset refusals would not reach the supervisor")
	}
}
