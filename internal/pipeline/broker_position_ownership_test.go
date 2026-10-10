// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// corruptBrokerTokens are, for each broker sentinel, the three ways a
// broker-owned row can fail to decode (Bug 299): a field of the wrong type,
// invalid JSON after the marker, and a token cut off after the marker. Each
// keeps the `_engine` marker first, where every broker writes it.
func corruptBrokerTokens(sentinel string) map[string]string {
	head := `{"_engine":"` + sentinel + `"`
	return map[string]string{
		"wrong-typed field": head + `,"chain_url":12,"last_applied_backup_id":["x"]}`,
		"invalid JSON":      head + `,"chain_url":"x","last_applied_backup_id":"P",}`,
		"truncated token":   head + `,"chain_url":"x","last_applied_backup_id":"P`,
	}
}

// foreignTokens are rows a non-broker writer owns, or that cannot be shown to
// be a broker's: they must be refused with or without --reset-target-data.
var foreignTokens = map[string]string{
	"pg slot envelope":            `{"slot":"sluice_slot","lsn":"0/16B0000"}`,
	"opaque mysql gtid":           "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100",
	"another _engine":             `{"_engine":"something-else","last_applied_backup_id":"P"}`,
	"marker truncated mid-string": `{"_engine":"backup-bro`,
	"damaged before the marker":   `{"chain_url":,"_engine":"backup-broker-v2"}`,
	"non-string marker":           `{"_engine":7,"last_applied_backup_id":"P"}`,
	"json array":                  `["backup-broker-v2"]`,
}

// TestIsBrokerToken_OwnershipIsTheMarkerAlone is Bug 299's classifier gate:
// ownership is judged from the `_engine` marker, never from whether the rest
// of the token decodes. Every corrupt shape × both sentinels is broker-owned
// (and still refused by decodeBrokerPosition — the cell would otherwise be
// grading a clean token); every foreign shape is not.
func TestIsBrokerToken_OwnershipIsTheMarkerAlone(t *testing.T) {
	for _, sentinel := range []string{BackupBrokerPositionEngine, BackupBrokerPositionEngineV2} {
		for name, tok := range corruptBrokerTokens(sentinel) {
			p := ir.Position{Engine: "postgres", Token: tok}
			if !isBrokerToken(p) {
				t.Errorf("%s %s: not judged broker-owned: %s", sentinel, name, tok)
			}
			if _, err := decodeBrokerPosition(p); err == nil {
				t.Errorf("%s %s: decodes, so the cell does not grade a corrupt row: %s", sentinel, name, tok)
			}
		}
	}
	for name, tok := range map[string]string{
		"clean v2":              encodeBrokerPosition("test://f", "P").Token,
		"clean classic":         `{"_engine":"backup-broker","chain_url":"x","last_applied_backup_id":"P"}`,
		"case-folded key":       `{"_ENGINE":"backup-broker-v2","last_applied_backup_id":"P"}`,
		"marker not first":      `{"last_applied_backup_id":"P","_engine":"backup-broker-v2"}`,
		"later null is skipped": `{"_engine":"backup-broker-v2","_engine":null,"last_applied_backup_id":"P"}`,
	} {
		if !isBrokerToken(ir.Position{Engine: "mysql", Token: tok}) {
			t.Errorf("%s: not judged broker-owned: %s", name, tok)
		}
	}
	for name, tok := range foreignTokens {
		if isBrokerToken(ir.Position{Engine: "postgres", Token: tok}) {
			t.Errorf("%s: judged broker-owned: %s", name, tok)
		}
	}
	if isBrokerToken(ir.Position{}) {
		t.Error("an empty token is judged broker-owned")
	}
}

// TestBroker_CorruptBrokerRowIsTheBrokers is Bug 299 end to end through Run:
// over a broker-owned row that does not decode, --reset-target-data is
// honoured (it reaches the drop, the row cleared first) and a plain run
// refuses BROKER-POSITION-CORRUPT — never "owned by a non-broker writer" —
// with nothing applied. Every corrupt shape × both sentinels.
func TestBroker_CorruptBrokerRowIsTheBrokers(t *testing.T) {
	store, _ := doorFixture(t, doorRows(false, true, false, false), true)
	for _, sentinel := range []string{BackupBrokerPositionEngine, BackupBrokerPositionEngineV2} {
		for name, tok := range corruptBrokerTokens(sentinel) {
			t.Run(sentinel+"/"+name+"/reset", func(t *testing.T) {
				dropped, app, err := runOverPosition(t, store, tok, true)
				if dropped == 0 {
					t.Fatalf("--reset-target-data over a corrupt broker row never reached the drop (Run = %v)", err)
				}
				if app.clearedAtDropped != 0 {
					t.Errorf("the row was cleared after %d drop(s) (-1 = never); want before the first", app.clearedAtDropped)
				}
			})
			t.Run(sentinel+"/"+name+"/plain", func(t *testing.T) {
				dropped, app, err := runOverPosition(t, store, tok, false)
				if err == nil || !strings.Contains(err.Error(), BrokerPositionCorruptMarker) ||
					!strings.Contains(err.Error(), "--reset-target-data") || strings.Contains(err.Error(), "non-broker writer") {
					t.Fatalf("Run = %v; want %s naming --reset-target-data, not a non-broker writer", err, BrokerPositionCorruptMarker)
				}
				if dropped != 0 || len(app.received) != 0 || len(app.written) != 0 {
					t.Errorf("the refused run touched the target: %d drops, %d changes, %d position writes", dropped, len(app.received), len(app.written))
				}
			})
		}
	}
}

// TestBroker_ForeignRowRefusedWithOrWithoutReset is the other direction of
// Bug 299's ownership judgment: a row no broker owns is refused with and
// without --reset-target-data, before anything is dropped, and the refusal
// names what the token carries rather than the target applier's engine.
func TestBroker_ForeignRowRefusedWithOrWithoutReset(t *testing.T) {
	store, _ := doorFixture(t, doorRows(false, true, false, false), true)
	for name, tok := range foreignTokens {
		for _, reset := range []bool{false, true} {
			t.Run(name+map[bool]string{false: "/plain", true: "/reset"}[reset], func(t *testing.T) {
				dropped, app, err := runOverPosition(t, store, tok, reset)
				if err == nil || !strings.Contains(err.Error(), "owned by a non-broker writer") {
					t.Fatalf("Run = %v; want the non-broker-writer refusal", err)
				}
				if strings.Contains(err.Error(), `position engine "postgres"`) {
					t.Errorf("the refusal names the applier's engine as the row's: %v", err)
				}
				if dropped != 0 || app.clearedAtDropped != -1 || len(app.received) != 0 || len(app.written) != 0 {
					t.Errorf("the refused run touched the target: %d drops, cleared %d, %d changes, %d position writes",
						dropped, app.clearedAtDropped, len(app.received), len(app.written))
				}
			})
		}
	}
}

// runOverPosition runs a broker over a target holding token as the stream's
// position, as the Postgres applier reads it back (Engine "postgres").
func runOverPosition(t *testing.T, store irbackup.Store, token string, reset bool) (int, *cleaningApplier, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dropped := 0
	app := &cleaningApplier{dropped: &dropped, clearedAtDropped: -1}
	app.resume = &ir.Position{Engine: "postgres", Token: token}
	b := newReplayBroker(store, &app.replayApplier, true)
	b.Target = replayTargetEngine{applier: app, rw: resetDoorWriter{replayKeyWriter: replayKeyWriter{keyed: true}, dropped: &dropped}}
	b.ResetTargetData = reset
	err := b.Run(ctx)
	return dropped, app, err
}
