// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package querytimeout

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/planetscale/api"
)

// fakePS is a stateful in-memory PlanetScale control plane for the ADR-0182
// config-change workflow. It models one branch's keyspaces, the current
// query-timeout value, and a rolling config-change that "settles" after a
// configurable number of polls — so the raise/revert can be driven end to end
// against the REAL api.Client (wire shape included), with an instant injected
// clock.
type fakePS struct {
	mu sync.Mutex

	keyspaces []string // resolution: 0 refuses, >1 refuses, 1 proceeds

	current                string // current queryserver-config-query-timeout
	configChangeInProgress bool
	inProgressPollsLeft    int // GetKeyspace decrements; while >0, reports in-progress

	pendingValue string // value staged by the last config-change, applied on rollout complete
	rolloutState string

	// recorded requests for assertions.
	configChangeBodies []string
	submittedIDs       [][]string
	staged             int
	nextID             int
}

func (f *fakePS) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		p := r.URL.Path
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(p, "/rollout-status"):
			// Complete the rollout on this poll: apply the staged value and
			// clear the in-progress flag.
			if f.pendingValue != "" {
				f.current = f.pendingValue
				f.pendingValue = ""
			}
			f.configChangeInProgress = false
			f.rolloutState = rolloutStateComplete
			writeJSON(w, api.RolloutStatus{State: f.rolloutState})

		case r.Method == http.MethodGet && strings.HasSuffix(p, "/keyspaces"):
			data := make([]api.Keyspace, len(f.keyspaces))
			for i, n := range f.keyspaces {
				data[i] = api.Keyspace{Name: n}
			}
			writeJSON(w, map[string]any{"data": data})

		case r.Method == http.MethodPost && strings.HasSuffix(p, "/config-changes"):
			body, _ := io.ReadAll(r.Body)
			f.configChangeBodies = append(f.configChangeBodies, string(body))
			var req struct {
				ChangeType string            `json:"change_type"`
				Options    map[string]string `json:"options"`
			}
			_ = json.Unmarshal(body, &req)
			f.staged++
			f.nextID++
			f.pendingValue = req.Options[queryTimeoutOption]
			f.configChangeInProgress = true
			f.rolloutState = "in_progress"
			writeJSON(w, api.ConfigChange{
				ID:              "cc-" + itoa(f.nextID),
				State:           "draft",
				ChangeType:      req.ChangeType,
				PreviousOptions: map[string]string{queryTimeoutOption: f.current},
				NewOptions:      req.Options,
			})

		case r.Method == http.MethodPost && strings.HasSuffix(p, "/config-changes/submit"):
			body, _ := io.ReadAll(r.Body)
			var req struct {
				IDs []string `json:"ids"`
			}
			_ = json.Unmarshal(body, &req)
			f.submittedIDs = append(f.submittedIDs, req.IDs)
			w.WriteHeader(http.StatusOK)

		case r.Method == http.MethodGet && strings.HasSuffix(p, "/keyspaces/main"):
			// A pre-existing config change settles after inProgressPollsLeft
			// GetKeyspace polls (models "wait it out before staging ours").
			if f.inProgressPollsLeft > 0 {
				f.inProgressPollsLeft--
				if f.inProgressPollsLeft == 0 {
					f.configChangeInProgress = false
				}
			}
			writeJSON(w, api.Keyspace{
				Name:                   "main",
				VTTabletOptions:        map[string]string{queryTimeoutOption: f.current},
				ConfigChangeInProgress: f.configChangeInProgress,
			})

		default:
			http.Error(w, "unexpected "+r.Method+" "+p, http.StatusNotFound)
		}
	}
}

func newRaiser(t *testing.T, f *fakePS) *Raiser {
	t.Helper()
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)
	return &Raiser{
		API: api.New(api.Config{
			TokenID: "id", Token: "sec", BaseURL: srv.URL,
			Sleep: func(context.Context, time.Duration) error { return nil }, // instant clock
		}),
		Org:      "org",
		Database: "db",
		Branch:   "main",
	}
}

func TestRaise_StagesSubmitsAndPollsToComplete(t *testing.T) {
	f := &fakePS{keyspaces: []string{"main"}, current: "900"}
	r := newRaiser(t, f)

	var recorded string
	recordedOK := false
	if err := r.Raise(context.Background(), func(previous string) error {
		recorded = previous
		recordedOK = true
		return nil
	}); err != nil {
		t.Fatalf("Raise: %v", err)
	}

	if !recordedOK {
		t.Fatal("record callback never called — the previous value was not persisted before the raise")
	}
	if recorded != "900" {
		t.Errorf("recorded previous = %q; want 900 (the value found before raising)", recorded)
	}
	if f.current != MaxQueryTimeout {
		t.Errorf("keyspace timeout after raise = %q; want %q", f.current, MaxQueryTimeout)
	}
	if f.staged != 1 || len(f.submittedIDs) != 1 {
		t.Errorf("want exactly one staged+submitted config change; got staged=%d submitted=%d", f.staged, len(f.submittedIDs))
	}
	// The load-bearing wire shape: field is `options` and the value is a JSON
	// STRING "3600" (not the number 3600, not `vttablet_options`).
	if body := f.configChangeBodies[0]; !strings.Contains(body, `"options":{"queryserver-config-query-timeout":"3600"}`) {
		t.Errorf("config-change body wire shape wrong: %s", body)
	}
}

func TestRaise_RecordFailureAbortsBeforeApplying(t *testing.T) {
	f := &fakePS{keyspaces: []string{"main"}, current: "900"}
	r := newRaiser(t, f)

	sentinel := "disk full"
	err := r.Raise(context.Background(), func(string) error { return errStr(sentinel) })
	if err == nil || !strings.Contains(err.Error(), sentinel) {
		t.Fatalf("Raise err = %v; want it to carry the record failure %q", err, sentinel)
	}
	// Nothing may have been staged — recording is the pre-condition to applying.
	if f.staged != 0 {
		t.Errorf("staged=%d; a failed record must abort before staging any change", f.staged)
	}
	if f.current != "900" {
		t.Errorf("keyspace timeout = %q; want it unchanged at 900", f.current)
	}
}

func TestRaise_AlreadyAtMaxIsNoOp(t *testing.T) {
	f := &fakePS{keyspaces: []string{"main"}, current: MaxQueryTimeout}
	r := newRaiser(t, f)

	recorded := false
	if err := r.Raise(context.Background(), func(string) error { recorded = true; return nil }); err != nil {
		t.Fatalf("Raise: %v", err)
	}
	if recorded {
		t.Error("record must NOT be called when the keyspace is already at the maximum (nothing to revert)")
	}
	if f.staged != 0 {
		t.Errorf("staged=%d; an already-at-max keyspace needs no config change", f.staged)
	}
}

func TestRevert_RestoresRecordedPreviousCustomValue(t *testing.T) {
	// Keyspace currently at the raised max; revert must set it back to 1200.
	f := &fakePS{keyspaces: []string{"main"}, current: MaxQueryTimeout}
	r := newRaiser(t, f)

	if err := r.Revert(context.Background(), "1200"); err != nil {
		t.Fatalf("Revert: %v", err)
	}
	if f.current != "1200" {
		t.Errorf("keyspace timeout after revert = %q; want 1200 (the operator's prior custom value)", f.current)
	}
	if !strings.Contains(f.configChangeBodies[0], `"queryserver-config-query-timeout":"1200"`) {
		t.Errorf("revert config-change did not target 1200: %s", f.configChangeBodies[0])
	}
}

func TestRevert_EmptyPreviousRestoresDocumentedDefault(t *testing.T) {
	f := &fakePS{keyspaces: []string{"main"}, current: MaxQueryTimeout}
	r := newRaiser(t, f)

	if err := r.Revert(context.Background(), ""); err != nil {
		t.Fatalf("Revert: %v", err)
	}
	if f.current != defaultQueryTimeout {
		t.Errorf("keyspace timeout after default revert = %q; want %q", f.current, defaultQueryTimeout)
	}
}

func TestRaise_WaitsOutAPreExistingConfigChange(t *testing.T) {
	// A config change is already rolling out; the raise must wait it out
	// (poll until it settles) before staging its own — never stack changes.
	f := &fakePS{
		keyspaces:              []string{"main"},
		current:                "900",
		configChangeInProgress: true,
		inProgressPollsLeft:    2, // settles after 2 GetKeyspace polls
	}
	r := newRaiser(t, f)

	if err := r.Raise(context.Background(), func(string) error { return nil }); err != nil {
		t.Fatalf("Raise: %v", err)
	}
	if f.current != MaxQueryTimeout {
		t.Errorf("after waiting out the pre-existing change, timeout = %q; want %q", f.current, MaxQueryTimeout)
	}
}

func TestRaise_RefusesMultiKeyspace(t *testing.T) {
	f := &fakePS{keyspaces: []string{"ks_a", "ks_b"}, current: "900"}
	r := newRaiser(t, f)

	err := r.Raise(context.Background(), func(string) error { return nil })
	if err == nil {
		t.Fatal("Raise on a multi-keyspace database must refuse")
	}
	if !strings.Contains(err.Error(), "ks_a") || !strings.Contains(err.Error(), "ks_b") {
		t.Errorf("multi-keyspace refusal must name the keyspaces; got %v", err)
	}
	if f.staged != 0 {
		t.Errorf("staged=%d; a refused resolution must touch nothing", f.staged)
	}
}

func TestRaise_RefusesZeroKeyspaces(t *testing.T) {
	f := &fakePS{keyspaces: nil, current: "900"}
	r := newRaiser(t, f)
	if err := r.Raise(context.Background(), func(string) error { return nil }); err == nil {
		t.Fatal("Raise on a database with no keyspaces must refuse")
	}
}

// --- tiny helpers so the test file needs no extra deps ---

type errStr string

func (e errStr) Error() string { return string(e) }

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
