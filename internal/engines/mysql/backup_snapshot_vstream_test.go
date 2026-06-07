// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// Compile-time assertion: the PlanetScale flavor satisfies the optional
// [ir.TableScopedBackupSnapshotOpener] surface, so the backup orchestrator
// can scope a PlanetScale backup's VStream COPY to --include-table (the
// backup-path counterpart to ir.TableScopedSnapshotOpener on the cold-
// start path). Engine implements it value-receiver, so a value satisfies it.
var _ ir.TableScopedBackupSnapshotOpener = Engine{Flavor: FlavorPlanetScale}

// Compile-time assertion: the engine satisfies the optional
// [ir.ServerCDCReaderOpener] surface (ADR-0074 Phase 1b.3). The
// multi-database `sync start` warm-resume type-asserts on this to open a
// server-wide CDC reader from the persisted server-wide position without a
// cold-start snapshot. Value receiver, so a value satisfies it.
var _ ir.ServerCDCReaderOpener = Engine{Flavor: FlavorVanilla}

// TestEngine_OpenServerCDCReader_VStreamRefusesLoudly pins the ADR-0074
// Phase 1b.3 / 1c boundary: the VStream flavors are keyspace-scoped, so a
// server-wide CDC reader is not their model. OpenServerCDCReader must refuse
// loudly with an [ErrNotImplemented]-shaped error rather than silently
// opening a binlog reader against a Vitess endpoint (which would mis-scope
// the resumed stream).
func TestEngine_OpenServerCDCReader_VStreamRefusesLoudly(t *testing.T) {
	for _, flavor := range []Flavor{FlavorPlanetScale, FlavorVitess} {
		flavor := flavor
		t.Run(flavor.String(), func(t *testing.T) {
			eng := Engine{Flavor: flavor}
			_, err := eng.OpenServerCDCReader(context.Background(), "user:pw@tcp(127.0.0.1:1)/")
			if err == nil {
				t.Fatalf("expected refusal for VStream flavor %q; got nil", flavor)
			}
			if !errors.Is(err, ErrNotImplemented) {
				t.Errorf("error %q does not wrap ErrNotImplemented", err.Error())
			}
			if !strings.Contains(strings.ToLower(err.Error()), "server-wide cdc") {
				t.Errorf("error %q does not name the server-wide-CDC refusal", err.Error())
			}
		})
	}
}

// TestEngine_OpenServerCDCReader_VanillaReachesOpen verifies vanilla MySQL
// passes the flavor gate and proceeds to the actual open (which then fails
// against the unreachable DSN, NOT at the VStream refusal). The negative
// marker — no "server-wide CDC resume is not supported" text — proves the
// vanilla path was taken.
func TestEngine_OpenServerCDCReader_VanillaReachesOpen(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel so the open fails fast
	eng := Engine{Flavor: FlavorVanilla}
	_, err := eng.OpenServerCDCReader(ctx, "user:pw@tcp(127.0.0.1:1)/")
	if err == nil {
		t.Fatalf("expected error from unreachable DSN; got nil")
	}
	if strings.Contains(err.Error(), "not supported on the VStream flavors") {
		t.Errorf("vanilla flavor wrongly hit the VStream refusal; err = %q", err.Error())
	}
}

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

// TestEngine_OpenBackupSnapshotForTables_VanillaDoesNotUseVStream is the
// table-scoped counterpart of the routing assertion: even WITH a non-empty
// table allowlist, vanilla MySQL must NOT touch the VStream gRPC machinery
// (its per-table pinned-conn snapshot reader never over-streams, so the
// scope is a no-op and it delegates to the base OpenBackupSnapshot). A
// "vstream" marker in the error would mean the PlanetScale path misfired.
func TestEngine_OpenBackupSnapshotForTables_VanillaDoesNotUseVStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	eng := Engine{Flavor: FlavorVanilla}
	_, err := eng.OpenBackupSnapshotForTables(ctx, "user:pw@tcp(127.0.0.1:1)/db", "", []string{"small_t", "other"})
	if err == nil {
		t.Fatalf("expected error from unreachable DSN; got nil")
	}
	if strings.Contains(strings.ToLower(err.Error()), "vstream") {
		t.Errorf("vanilla flavor wrongly routed to VStream path; err = %q", err.Error())
	}
}

// TestEngine_OpenBackupSnapshotForTables_PlanetScaleRoutesToVStream is the
// positive counterpart: a scoped PlanetScale backup must go through the
// VStream COPY path (so the COPY filter can be narrowed to the allowlist).
func TestEngine_OpenBackupSnapshotForTables_PlanetScaleRoutesToVStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	eng := Engine{Flavor: FlavorPlanetScale}
	_, err := eng.OpenBackupSnapshotForTables(ctx, "user:pw@tcp(127.0.0.1:1)/db", "", []string{"small_t"})
	if err == nil {
		t.Fatalf("expected error from unreachable DSN; got nil")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "vstream") {
		t.Errorf("planetscale scoped backup did not route to VStream path; err = %q", err.Error())
	}
}
