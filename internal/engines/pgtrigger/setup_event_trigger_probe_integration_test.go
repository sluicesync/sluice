//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pins for the attempt-based event-trigger capability probe
// (RDS validation F2, 2026-07-16). The prior probe checked membership
// in `pg_create_event_trigger` — a predefined role that does not exist
// in ANY stock PostgreSQL (verified against this PG 16 container: the
// only pg_create* predefined role is pg_create_subscription), so the
// probe was wrong everywhere: superuser-only on stock PG by accident,
// and falsely refusing the RDS master user (which CAN create event
// triggers via rds_superuser). The attempt-based probe reads the
// server's actual answer.

package pgtrigger

import (
	"context"
	"database/sql"
	"net/url"
	"strings"
	"testing"
	"time"
)

// grantEventTriggerProbeFixture creates a login role that can create
// functions in `public` (the probe's prerequisite, same as Setup's own
// DDL) but is NOSUPERUSER — on stock PG that means CREATE EVENT TRIGGER
// is denied. Returns the low-priv DSN.
func grantEventTriggerProbeFixture(t *testing.T, dsn string) string {
	t.Helper()
	applyPGSQL(t, dsn, `
		CREATE ROLE evtrig_low LOGIN PASSWORD 'app' NOSUPERUSER;
		GRANT CREATE, USAGE ON SCHEMA public TO evtrig_low;
	`)
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("rebind DSN: %v", err)
	}
	u.User = url.UserPassword("evtrig_low", "app")
	return u.String()
}

// assertNoEventTriggerProbeResidue fails if the probe's rolled-back
// function or event trigger survived — the probe must be side-effect-
// free on BOTH answers.
func assertNoEventTriggerProbeResidue(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var trigs, fns int
	if err := db.QueryRowContext(ctx, `
		SELECT
			(SELECT count(*) FROM pg_event_trigger WHERE evtname = 'sluice_evtrig_probe'),
			(SELECT count(*) FROM pg_proc WHERE proname = 'sluice_evtrig_probe')
	`).Scan(&trigs, &fns); err != nil {
		t.Fatalf("residue query: %v", err)
	}
	if trigs != 0 || fns != 0 {
		t.Fatalf("probe left residue: %d event trigger(s), %d function(s) named sluice_evtrig_probe — the rollback contract is broken", trigs, fns)
	}
}

// TestEventTriggerProbe_SuperuserCan is the positive control: the
// container superuser can CREATE EVENT TRIGGER, the probe says so, and
// nothing survives the rolled-back attempt.
func TestEventTriggerProbe_SuperuserCan(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	can, err := canCreateEventTrigger(ctx, db, "public")
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if !can {
		t.Error("expected superuser to probe event-trigger-capable; got false")
	}
	assertNoEventTriggerProbeResidue(t, dsn)
}

// TestEventTriggerProbe_NonSuperuserCannot pins the (false, nil) answer
// on stock PG for a role that can create functions but not event
// triggers — the SQLSTATE 42501 → fallback-signal mapping — plus the
// zero-residue contract on the refusal path, and that no
// pg_create_event_trigger role was involved (it does not exist).
func TestEventTriggerProbe_NonSuperuserCannot(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()
	lowDSN := grantEventTriggerProbeFixture(t, dsn)

	// Ground-truth the F2 premise on the real server: stock PG has no
	// pg_create_event_trigger predefined role.
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var roleExists bool
	if err := db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pg_create_event_trigger')`).Scan(&roleExists); err != nil {
		t.Fatalf("role query: %v", err)
	}
	if roleExists {
		t.Fatal("premise broken: pg_create_event_trigger EXISTS on this PG — revisit the probe design")
	}

	lowDB, err := sql.Open("pgx", lowDSN)
	if err != nil {
		t.Fatalf("open low-priv: %v", err)
	}
	defer func() { _ = lowDB.Close() }()
	can, err := canCreateEventTrigger(ctx, lowDB, "public")
	if err != nil {
		t.Fatalf("probe must map 42501 to (false, nil), not an error: %v", err)
	}
	if can {
		t.Error("expected non-superuser to probe event-trigger-INcapable on stock PG; got true")
	}
	assertNoEventTriggerProbeResidue(t, dsn)
}

// TestSetup_NonSuperuserRefusesWithPolledFingerprintHint pins the
// operator-facing refusal through the real Setup path: without
// --allow-polled-fingerprint the run refuses naming the actual
// incapability (not the phantom role) and the flag remedy.
func TestSetup_NonSuperuserRefusesWithPolledFingerprintHint(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()
	applyPGSQL(t, dsn, `CREATE TABLE evtrig_tbl (id BIGINT PRIMARY KEY, v TEXT);`)
	lowDSN := grantEventTriggerProbeFixture(t, dsn)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, err := Setup(ctx, lowDSN, SetupOptions{Tables: []string{"evtrig_tbl"}, Schema: "public"})
	if err == nil {
		t.Fatal("expected Setup to refuse for a role that cannot create event triggers; got nil")
	}
	if !strings.Contains(err.Error(), "cannot create event triggers") {
		t.Errorf("refusal should name the incapability; got %v", err)
	}
	if !strings.Contains(err.Error(), "--allow-polled-fingerprint") {
		t.Errorf("refusal should name the polled-fingerprint remedy; got %v", err)
	}
	if strings.Contains(err.Error(), "pg_create_event_trigger") {
		t.Errorf("refusal must not reference the non-existent pg_create_event_trigger role; got %v", err)
	}
}
