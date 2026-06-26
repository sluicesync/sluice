// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/pipeline"
)

// writeFleetYAML writes content to a temp file and returns its path.
func writeFleetYAML(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "syncs.yaml")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp fleet yaml: %v", err)
	}
	return p
}

// TestLoadFleetConfig_ParsesDurationsAndLists pins that the koanf loader
// decodes string durations and YAML lists into the typed SyncSpec.
func TestLoadFleetConfig_ParsesDurationsAndLists(t *testing.T) {
	path := writeFleetYAML(t, `
syncs:
  - stream-id: orders
    source-driver: postgres
    source: postgres://u:p@src:5432/app
    target-driver: mysql
    target: mysql://u:p@dst:3306/app
    slot-name: orders
    apply-concurrency: 4
    apply-delay: 5m
    include-table:
      - audit_*
      - orders
restart:
  backoff-base: 2s
  backoff-cap: 1m
  max-consecutive-failures: 5
`)
	fleet, err := loadFleetConfig(path)
	if err != nil {
		t.Fatalf("loadFleetConfig: %v", err)
	}
	if len(fleet.Syncs) != 1 {
		t.Fatalf("got %d syncs; want 1", len(fleet.Syncs))
	}
	s := fleet.Syncs[0]
	if s.StreamID != "orders" || s.ApplyConcurrency != 4 {
		t.Errorf("spec mismatch: %+v", s)
	}
	if s.ApplyDelay != 5*time.Minute {
		t.Errorf("ApplyDelay = %s; want 5m", s.ApplyDelay)
	}
	if len(s.IncludeTable) != 2 || s.IncludeTable[0] != "audit_*" {
		t.Errorf("IncludeTable = %v; want [audit_* orders]", s.IncludeTable)
	}
	if fleet.Restart.BackoffBase != 2*time.Second || fleet.Restart.BackoffCap != time.Minute || fleet.Restart.MaxConsecutiveFailures != 5 {
		t.Errorf("restart policy mismatch: %+v", fleet.Restart)
	}
}

// TestLoadFleetConfig_MissingPath pins the empty-path refusal.
func TestLoadFleetConfig_MissingPath(t *testing.T) {
	if _, err := loadFleetConfig(""); err == nil {
		t.Fatal("loadFleetConfig(\"\") = nil; want an error")
	}
}

// fleetFromSpecs is a tiny constructor for validation tests.
func fleetFromSpecs(specs ...SyncSpec) *SyncFleetConfig {
	return &SyncFleetConfig{Syncs: specs}
}

func pgSpec(id, slot string) SyncSpec {
	return SyncSpec{
		StreamID: id, SlotName: slot,
		SourceDriver: "postgres", Source: "postgres://u:p@src:5432/app",
		TargetDriver: "mysql", Target: "mysql://u:p@dst:3306/app",
	}
}

func mysqlSpec(id string) SyncSpec {
	return SyncSpec{
		StreamID:     id,
		SourceDriver: "mysql", Source: "mysql://u:p@src:3306/app",
		TargetDriver: "postgres", Target: "postgres://u:p@dst:5432/app",
	}
}

// TestFleetValidate covers the load-time invariants — the zero/one/many
// matrix, the required-field refusals, and (the load-bearing data-
// corruption guards) the slot-name + stream-id uniqueness refusals.
func TestFleetValidate(t *testing.T) {
	cases := []struct {
		name        string
		fleet       *SyncFleetConfig
		wantErr     bool
		wantSubstrs []string
	}{
		{
			name:        "zero syncs → refused",
			fleet:       fleetFromSpecs(),
			wantErr:     true,
			wantSubstrs: []string{"no syncs"},
		},
		{
			name:    "one valid sync → ok",
			fleet:   fleetFromSpecs(pgSpec("a", "slot_a")),
			wantErr: false,
		},
		{
			name:    "many distinct syncs → ok",
			fleet:   fleetFromSpecs(pgSpec("a", "slot_a"), pgSpec("b", "slot_b"), mysqlSpec("c")),
			wantErr: false,
		},
		{
			name:        "missing stream-id → refused",
			fleet:       fleetFromSpecs(pgSpec("", "slot_a")),
			wantErr:     true,
			wantSubstrs: []string{"stream-id is required"},
		},
		{
			name:        "missing source → refused",
			fleet:       fleetFromSpecs(SyncSpec{StreamID: "a", SourceDriver: "postgres", TargetDriver: "mysql", Target: "mysql://x"}),
			wantErr:     true,
			wantSubstrs: []string{"source-driver and source"},
		},
		{
			name:        "missing target → refused",
			fleet:       fleetFromSpecs(SyncSpec{StreamID: "a", SourceDriver: "postgres", Source: "postgres://x", TargetDriver: "mysql"}),
			wantErr:     true,
			wantSubstrs: []string{"target-driver and target"},
		},
		{
			name:        "duplicate stream-id → refused",
			fleet:       fleetFromSpecs(pgSpec("dup", "slot_a"), pgSpec("dup", "slot_b")),
			wantErr:     true,
			wantSubstrs: []string{"duplicate stream-id", "dup"},
		},
		{
			name:        "two PG syncs, both default slot → refused",
			fleet:       fleetFromSpecs(pgSpec("a", ""), pgSpec("b", "")),
			wantErr:     true,
			wantSubstrs: []string{"replication slot", "sluice_slot"},
		},
		{
			name:        "two PG syncs colliding after sluice_ prefix → refused",
			fleet:       fleetFromSpecs(pgSpec("a", "shard"), pgSpec("b", "sluice_shard")),
			wantErr:     true,
			wantSubstrs: []string{"sluice_shard"},
		},
		{
			name:    "two PG syncs, distinct slots → ok",
			fleet:   fleetFromSpecs(pgSpec("a", "slot_a"), pgSpec("b", "slot_b")),
			wantErr: false,
		},
		{
			name:    "two MySQL syncs share default slot → ok (no slot concept)",
			fleet:   fleetFromSpecs(mysqlSpec("a"), mysqlSpec("b")),
			wantErr: false,
		},
		{
			name: "out-of-range retry attempts → refused",
			fleet: fleetFromSpecs(func() SyncSpec {
				s := pgSpec("a", "slot_a")
				s.ApplyRetryAttempts = 1000
				return s
			}()),
			wantErr:     true,
			wantSubstrs: []string{"apply-retry-attempts"},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			err := c.fleet.validate()
			if c.wantErr != (err != nil) {
				t.Fatalf("validate() err = %v; wantErr = %v", err, c.wantErr)
			}
			if err != nil {
				for _, sub := range c.wantSubstrs {
					if !strings.Contains(err.Error(), sub) {
						t.Errorf("error %q missing substring %q", err.Error(), sub)
					}
				}
			}
		})
	}
}

// TestSyncSpecFingerprint pins that the hot-reload fingerprint is stable
// (equal specs hash identically) and discriminating (any field change
// produces a different hash) — the basis for Reconcile's changed-vs-
// unchanged decision.
func TestSyncSpecFingerprint(t *testing.T) {
	a := pgSpec("orders", "slot_a")
	b := pgSpec("orders", "slot_a")
	if a.fingerprint() != b.fingerprint() {
		t.Errorf("equal specs hashed differently: %q vs %q", a.fingerprint(), b.fingerprint())
	}
	// A changed field flips the fingerprint.
	c := pgSpec("orders", "slot_a")
	c.ApplyConcurrency = 8
	if a.fingerprint() == c.fingerprint() {
		t.Error("apply-concurrency change did not change the fingerprint")
	}
	// A changed slice field flips it too.
	d := pgSpec("orders", "slot_a")
	d.IncludeTable = []string{"t1"}
	if a.fingerprint() == d.fingerprint() {
		t.Error("include-table change did not change the fingerprint")
	}
}

// TestReloadFleet_RefusesBadConfig pins THE load-bearing hot-reload
// property at the CLI seam: a reload whose new config fails to parse OR
// fails validation (here, two Postgres syncs colliding on the default
// replication slot) is REFUSED — reloadFleet returns the error BEFORE it
// would ever call Reconcile, so the live fleet is never touched. The
// supervisor passed in is intentionally not running; if validation were
// (wrongly) skipped the test would instead see the "before Run" error
// from Reconcile, so a plain non-nil error is not enough — we assert the
// error names the violation.
func TestReloadFleet_RefusesBadConfig(t *testing.T) {
	sup := pipeline.NewSupervisor(
		[]pipeline.SupervisedSync{{ID: "x", Runner: noopRunner{}}},
		pipeline.RestartPolicy{},
	)

	t.Run("malformed yaml", func(t *testing.T) {
		path := writeFleetYAML(t, "syncs: [this is : not valid yaml")
		err := reloadFleet(path, sup)
		if err == nil {
			t.Fatal("reloadFleet on malformed yaml = nil; want a parse error")
		}
	})

	t.Run("slot collision", func(t *testing.T) {
		path := writeFleetYAML(t, `
syncs:
  - stream-id: a
    source-driver: postgres
    source: postgres://u:p@src:5432/app
    target-driver: mysql
    target: mysql://u:p@dst:3306/app
  - stream-id: b
    source-driver: postgres
    source: postgres://u:p@src:5432/app
    target-driver: mysql
    target: mysql://u:p@dst:3306/app
`)
		err := reloadFleet(path, sup)
		if err == nil {
			t.Fatal("reloadFleet on slot-colliding config = nil; want a refusal")
		}
		if !strings.Contains(err.Error(), "replication slot") {
			t.Errorf("error %q does not name the slot collision (validation was skipped?)", err.Error())
		}
	})

	t.Run("duplicate stream-id", func(t *testing.T) {
		path := writeFleetYAML(t, `
syncs:
  - stream-id: dup
    source-driver: mysql
    source: mysql://u:p@src:3306/app
    target-driver: postgres
    target: postgres://u:p@dst:5432/app
  - stream-id: dup
    source-driver: mysql
    source: mysql://u:p@src2:3306/app
    target-driver: postgres
    target: postgres://u:p@dst:5432/app
`)
		err := reloadFleet(path, sup)
		if err == nil || !strings.Contains(err.Error(), "duplicate stream-id") {
			t.Fatalf("reloadFleet on duplicate stream-id = %v; want a duplicate-stream-id refusal", err)
		}
	})
}

// noopRunner is an inert SyncRunner for tests that only need to construct
// a supervisor (reloadFleet's refusal paths never start it).
type noopRunner struct{}

func (noopRunner) Run(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }

// TestResolvedSlotName pins the slot-name resolution used by the guard.
func TestResolvedSlotName(t *testing.T) {
	cases := map[string]string{
		"":             "sluice_slot",
		"shard_a":      "sluice_shard_a",
		"sluice_slot":  "sluice_slot",
		"sluice_shard": "sluice_shard",
	}
	for in, want := range cases {
		if got := resolvedSlotName(in); got != want {
			t.Errorf("resolvedSlotName(%q) = %q; want %q", in, got, want)
		}
	}
}

// TestDSNEndpoint pins the coarse endpoint extraction for the shared-
// target WARN (URL form, keyword form, and full-DSN fallback).
func TestDSNEndpoint(t *testing.T) {
	cases := map[string]string{
		"postgres://u:p@host-a:5432/app":   "host-a:5432",
		"mysql://u:p@host-b:3306/app?x=1":  "host-b:3306",
		"postgres://host-c/app":            "host-c",
		"host=host-d port=5432 dbname=app": "host-d:5432",
		"host=host-e user=u":               "host-e",
		"some-opaque-dsn-with-no-host":     "some-opaque-dsn-with-no-host",
		"":                                 "",
	}
	for in, want := range cases {
		if got := dsnEndpoint(in); got != want {
			t.Errorf("dsnEndpoint(%q) = %q; want %q", in, got, want)
		}
	}
}

// TestSharedTargetGroups pins the connection-budget detection: only
// targets shared by 2+ syncs are surfaced; distinct targets aren't.
func TestSharedTargetGroups(t *testing.T) {
	fleet := &SyncFleetConfig{Syncs: []SyncSpec{
		{StreamID: "a", Target: "mysql://u:p@shared:3306/db1"},
		{StreamID: "b", Target: "mysql://u:p@shared:3306/db2"}, // same server, diff db
		{StreamID: "c", Target: "postgres://u:p@solo:5432/app"},
	}}
	shared := sharedTargetGroups(fleet)
	if len(shared) != 1 {
		t.Fatalf("shared groups = %d; want 1 (%+v)", len(shared), shared)
	}
	ids, ok := shared["shared:3306"]
	if !ok || len(ids) != 2 {
		t.Errorf("shared[shared:3306] = %v (ok=%v); want 2 stream-ids", ids, ok)
	}
}

// TestSyncSpecDefaults pins that the fleet defaults mirror sync start —
// empty knobs fall back to the documented defaults via the firstNonZero*
// helpers, exercised through buildStreamerFromSpec.
func TestSyncSpecDefaults(t *testing.T) {
	spec := pgSpec("a", "slot_a")
	streamer, err := buildStreamerFromSpec(&spec)
	if err != nil {
		t.Fatalf("buildStreamerFromSpec: %v", err)
	}
	if streamer.MaxBufferBytes != defaultMaxBufferBytes {
		t.Errorf("MaxBufferBytes = %d; want default %d", streamer.MaxBufferBytes, defaultMaxBufferBytes)
	}
	if streamer.ApplyExecTimeout != defaultApplyExecTimeout {
		t.Errorf("ApplyExecTimeout = %s; want default %s", streamer.ApplyExecTimeout, defaultApplyExecTimeout)
	}
	if streamer.HeartbeatInterval != defaultHeartbeatInterval {
		t.Errorf("HeartbeatInterval = %s; want default %s", streamer.HeartbeatInterval, defaultHeartbeatInterval)
	}
	if streamer.SchemaChanges != defaultSchemaChanges {
		t.Errorf("SchemaChanges = %q; want default %q", streamer.SchemaChanges, defaultSchemaChanges)
	}
	if streamer.ApplyRetryAttempts != defaultApplyRetryAttempts {
		t.Errorf("ApplyRetryAttempts = %d; want default %d", streamer.ApplyRetryAttempts, defaultApplyRetryAttempts)
	}
	if !streamer.AutoTune {
		t.Error("AutoTune = false; want true by default (NoAutoTune unset)")
	}
	// apply-batch-size "auto" resolves to the mysql/postgres ceiling 1000.
	if streamer.ApplyBatchSize != 1000 {
		t.Errorf("ApplyBatchSize = %d; want 1000 (auto → mysql/postgres ceiling)", streamer.ApplyBatchSize)
	}
}
