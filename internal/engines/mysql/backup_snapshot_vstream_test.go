// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"strings"
	"testing"
)

// TestEngine_OpenBackupSnapshot_FlavorBranchRoutes verifies the
// v0.44.0 (GitHub issue #16) routing decision: FlavorPlanetScale
// goes through the VStream-COPY path, FlavorVanilla goes through
// the binlog-snapshot path. We confirm the routing by inspecting
// the error shape returned when both paths fail to dial (the test
// supplies an unreachable DSN, so both paths must error — but the
// error messages distinguish which path was taken).
//
// Without this branch test, a future refactor could silently move
// PlanetScale traffic back to the binlog-snapshot path and quietly
// reintroduce GitHub issue #16 (incremental + stream-run
// chain-resume broken because position is wrong shape).
func TestEngine_OpenBackupSnapshot_FlavorBranchRoutes(t *testing.T) {
	// Both paths must fail (the test DSN can't reach anything), but
	// the failure mode differs:
	//   - VStream path errors at vstream gRPC dial / endpoint
	//     resolution → message contains "vstream"
	//   - Binlog path errors at openDB / parseDSN / START TRANSACTION
	//     → message contains "snapshot" or "parseDSN" (NOT "vstream")
	cases := []struct {
		name      string
		flavor    Flavor
		wantInErr string
	}{
		{
			name:      "FlavorPlanetScale routes to VStream path",
			flavor:    FlavorPlanetScale,
			wantInErr: "vstream",
		},
		// Vanilla MySQL routes to the binlog snapshot path — we
		// can't easily assert a binlog-specific marker without
		// reaching openDB, but the absence of "vstream" in the
		// error is a strong negative signal that the VStream
		// branch did NOT fire.
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel() // pre-cancel so dial fails fast
			eng := Engine{Flavor: c.flavor}
			// Use a DSN that parses but can't reach a real endpoint.
			_, err := eng.OpenBackupSnapshot(ctx, "user:pw@tcp(127.0.0.1:1)/db", "")
			if err == nil {
				t.Fatalf("expected error from unreachable DSN; got nil")
			}
			if !strings.Contains(strings.ToLower(err.Error()), c.wantInErr) {
				t.Errorf("error %q does not contain expected marker %q — wrong branch fired", err.Error(), c.wantInErr)
			}
		})
	}
}

// TestEngine_OpenBackupSnapshot_VanillaDoesNotUseVStream covers the
// inverse routing assertion: vanilla MySQL must NOT touch the
// VStream gRPC machinery. Asserts the error message does NOT
// mention "vstream" (which would indicate misrouting to the
// PlanetScale path).
func TestEngine_OpenBackupSnapshot_VanillaDoesNotUseVStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	eng := Engine{Flavor: FlavorVanilla}
	_, err := eng.OpenBackupSnapshot(ctx, "user:pw@tcp(127.0.0.1:1)/db", "")
	if err == nil {
		t.Fatalf("expected error from unreachable DSN; got nil")
	}
	if strings.Contains(strings.ToLower(err.Error()), "vstream") {
		t.Errorf("vanilla flavor wrongly routed to VStream path; err = %q", err.Error())
	}
}
