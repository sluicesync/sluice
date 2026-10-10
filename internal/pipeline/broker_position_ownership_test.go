// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"encoding/json"
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

// TestBrokerToken_MarkerIsFirstAndSurvivesTruncation is the check behind
// tokenEngineMarker's premise that every broker writes `_engine` FIRST (Bug
// 299 review F3): both of today's encoders and the frozen pre-ADR-0191 token
// struct (what a v0.156.12-or-older broker wrote) open with the marker, and
// every strict prefix of a REAL encoded token that still holds the whole
// marker is broker-owned yet refused by decodeBrokerPosition — so a token cut
// anywhere past the marker reaches the corrupt-position arms, never the
// foreign one.
func TestBrokerToken_MarkerIsFirstAndSurvivesTruncation(t *testing.T) {
	in := &brokerInProgress{BackupID: `inc"\ ü-1`, Chunks: strings.Repeat("ab", 32), Through: 1<<53 + 1}
	classicBody, err := json.Marshal(brokerPositionTokenV015611{Engine: backupBrokerPositionEngineV015611, ChainURL: "x", LastAppliedBackupID: "P"})
	if err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct{ tok, sentinel string }{
		"encodeBrokerPosition": {encodeBrokerPosition(`s3://b/"chain"`, "P ").Token, BackupBrokerPositionEngineV2},
		"encodeBrokerFrontier": {encodeBrokerFrontier(`s3://b/"chain"`, "P ", in).Token, BackupBrokerPositionEngineV2},
		"frozen v0.156.11":     {string(classicBody), BackupBrokerPositionEngine},
	} {
		if !strings.HasPrefix(tc.tok, `{"_engine":`) {
			t.Errorf("%s: the marker is not the token's first member: %s", name, tc.tok)
			continue
		}
		head := `{"_engine":"` + tc.sentinel + `"`
		if !strings.HasPrefix(tc.tok, head) {
			t.Errorf("%s: the token does not open with %s: %s", name, head, tc.tok)
			continue
		}
		if len(tc.tok)-len(head) < 20 {
			t.Fatalf("%s: only %d bytes after the marker; the truncation sweep would grade almost nothing", name, len(tc.tok)-len(head))
		}
		for n := len(head); n < len(tc.tok); n++ {
			p := ir.Position{Engine: "postgres", Token: tc.tok[:n]}
			if !isBrokerToken(p) {
				t.Errorf("%s cut at %d is not broker-owned: %s", name, n, p.Token)
			}
			if _, err := decodeBrokerPosition(p); err == nil {
				t.Errorf("%s cut at %d decodes: %s", name, n, p.Token)
			}
		}
	}
}

// FuzzTokenEngineMarker is the differential gate on tokenEngineMarker's
// binding rules (Bug 299 review): on every syntactically valid JSON input, the
// marker it reads must be the value encoding/json binds to
// brokerPositionToken.Engine — duplicate keys, case-folded keys, nested
// objects, null and non-string values included. Invalid input is out of its
// scope (encoding/json binds nothing there; tokenEngineMarker's
// member-by-member read of it is pinned by the truncation and corrupt-row
// gates above).
func FuzzTokenEngineMarker(f *testing.F) {
	for _, seed := range []string{
		`{"_engine":"backup-broker-v2"}`,
		`{"_engine":"backup-broker","_engine":"backup-broker-v2"}`,
		`{"_engine":"backup-broker-v2","_engine":"backup-broker"}`,
		`{"_Engine":"backup-broker-v2"}`,
		`{"_ENGINE":"backup-broker"}`,
		`{"_engine":"a","_ENGINE":"b"}`,
		`{"_ENGINE":"b","_engine":"a"}`,
		`{"x":{"_engine":"backup-broker-v2"}}`,
		`{"x":[{"_engine":"backup-broker-v2"}],"_engine":"backup-broker"}`,
		`{"_engine":null}`,
		`{"_engine":"backup-broker-v2","_engine":null}`,
		`{"_engine":7}`,
		`{"_engine":"backup-broker-v2","_engine":7}`,
		`{"_engine":true}`,
		`{"_engine":["backup-broker-v2"]}`,
		`{"_engine":{"v":"backup-broker-v2"}}`,
		`{"_engine":""}`,
		`{"_engine":"backup-broker-v2\u0000"}`,
		`{"` + "\\" + `u005fengine":"backup-broker-v2"}`, // the key spelled with a JSON escape
		`{"_engin":"backup-broker-v2"}`,
		`{"_engines":"backup-broker-v2"}`,
		`{"engine":"backup-broker-v2"}`,
		`{" _engine":"backup-broker-v2"}`,
		`{"_enginE":"backup-broker-v2","last_applied_backup_id":"P"}`,
		string(rune(0xFEFF)) + `{"_engine":"backup-broker-v2"}`, // a BOM: not valid JSON, so out of the differential's scope
		` {"_engine":"backup-broker-v2"} `,
		`["backup-broker-v2"]`,
		`"backup-broker-v2"`,
		`null`,
		`{}`,
		`{"slot":"sluice_slot","lsn":"0/16B0000"}`,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, token string) {
		if !json.Valid([]byte(token)) {
			return
		}
		var ref struct {
			Engine string `json:"_engine"`
		}
		_ = json.Unmarshal([]byte(token), &ref) // a type error still binds what it can, as the decoder does
		got, _ := tokenEngineMarker(token)
		if got != ref.Engine {
			t.Fatalf("tokenEngineMarker(%q) = %q; encoding/json binds %q", token, got, ref.Engine)
		}
	})
}
