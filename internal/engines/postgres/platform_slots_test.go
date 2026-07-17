// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
)

// TestPlatformInternalSlotRoster pins the whitelist itself: both known
// platform-internal slots (Neon's wal_proposer_slot, the Aiven-lineage
// pghoard_local) are recognized with a provider note, and a stranger
// slot — including sluice's own — still surfaces un-annotated, so
// enumeration output keeps flagging genuinely unknown consumers.
func TestPlatformInternalSlotRoster(t *testing.T) {
	for name, wantMention := range map[string]string{
		"wal_proposer_slot": "Neon",
		"pghoard_local":     "pghoard",
	} {
		note, ok := platformInternalSlotNote(name)
		if !ok {
			t.Errorf("%s: not recognized as platform-internal", name)
			continue
		}
		if !strings.Contains(note, wantMention) {
			t.Errorf("%s: note %q should mention %q", name, note, wantMention)
		}
	}
	for _, stranger := range []string{
		"sluice_slot",
		"debezium",
		"pghoard",             // exact-name roster: the bare daemon name is NOT the slot name
		"wal_proposer_slot_2", // no prefix matching — a lookalike still surfaces
		"",
	} {
		if note, ok := platformInternalSlotNote(stranger); ok {
			t.Errorf("%q: unexpectedly whitelisted as platform-internal (%s)", stranger, note)
		}
	}
}

// failingConnector is a driver.Connector whose Connect always errors —
// it lets the Drop tests below prove which side of the platform-
// internal guard a call landed on without a real server: the guard
// refuses BEFORE any connection, so a connect error means the guard
// was passed.
type failingConnector struct{}

var errConnectorSentinel = errors.New("test connector: no server")

func (failingConnector) Connect(context.Context) (driver.Conn, error) {
	return nil, errConnectorSentinel
}
func (failingConnector) Driver() driver.Driver { return nil }

// TestSlotManagerDrop_PlatformInternalGuard pins the Drop wire-up for
// BOTH roster entries (pin the class): without --force the refusal
// names the slot and its provider and never touches the database;
// with --force the guard steps aside (the call proceeds to the DB and
// fails on the test connector instead); and a stranger slot passes the
// guard entirely.
func TestSlotManagerDrop_PlatformInternalGuard(t *testing.T) {
	ctx := context.Background()

	for _, name := range []string{"wal_proposer_slot", "pghoard_local"} {
		t.Run(name, func(t *testing.T) {
			// db deliberately nil: the guard must refuse before any
			// DB access, so a nil handle proves no round-trip ran.
			m := &SlotManager{}
			err := m.Drop(ctx, name, false)
			if err == nil {
				t.Fatal("Drop of a platform-internal slot without --force succeeded; want refusal")
			}
			for _, want := range []string{name, "platform-internal", "--force"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("refusal should mention %q; got: %v", want, err)
				}
			}

			// --force overrides the guard: with an always-failing
			// connector the call must reach the DB and surface the
			// connector error, not the platform-internal refusal.
			forced := &SlotManager{db: sql.OpenDB(failingConnector{})}
			t.Cleanup(func() { _ = forced.Close() })
			err = forced.Drop(ctx, name, true)
			if err == nil {
				t.Fatal("forced Drop against the failing connector returned nil")
			}
			if strings.Contains(err.Error(), "platform-internal") {
				t.Errorf("--force must bypass the platform-internal guard; got: %v", err)
			}
			if !errors.Is(err, errConnectorSentinel) {
				t.Errorf("forced Drop should have reached the DB (connector sentinel); got: %v", err)
			}
		})
	}

	// A stranger slot passes the guard even without --force — the
	// refusal is roster-scoped, not a new friction tier for normal
	// slot cleanup.
	m := &SlotManager{db: sql.OpenDB(failingConnector{})}
	t.Cleanup(func() { _ = m.Close() })
	err := m.Drop(ctx, "sluice_slot", false)
	if err == nil {
		t.Fatal("Drop of a normal slot against the failing connector returned nil")
	}
	if strings.Contains(err.Error(), "platform-internal") {
		t.Errorf("normal slot must not trip the platform-internal guard; got: %v", err)
	}
	if !errors.Is(err, errConnectorSentinel) {
		t.Errorf("normal slot Drop should have reached the DB (connector sentinel); got: %v", err)
	}
}
