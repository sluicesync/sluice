// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package notify

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func sampleNotification() Notification {
	return Notification{
		Level:     LevelCritical,
		StreamID:  "s1",
		Metric:    "storage_util",
		Title:     "target storage approaching capacity",
		Body:      "storage_util 0.91 ≥ 0.90",
		Value:     0.91,
		Threshold: 0.90,
		At:        time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC),
	}
}

func TestWebhookNotifier_PayloadShape(t *testing.T) {
	var got webhookPayload
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("content-type = %q, want application/json", ct)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("decode body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	n := sampleNotification()
	wh := &WebhookNotifier{URL: srv.URL}
	if err := wh.Notify(context.Background(), n); err != nil {
		t.Fatalf("Notify: %v", err)
	}
	if got.Level != string(n.Level) || got.StreamID != n.StreamID || got.Metric != n.Metric {
		t.Errorf("payload header mismatch: %+v", got)
	}
	if got.Title != n.Title || got.Body != n.Body {
		t.Errorf("payload text mismatch: %+v", got)
	}
	if got.Value != n.Value || got.Threshold != n.Threshold {
		t.Errorf("payload numbers mismatch: %+v", got)
	}
	if !got.At.Equal(n.At) {
		t.Errorf("payload At = %v, want %v", got.At, n.At)
	}
	if wh.Name() != "webhook" {
		t.Errorf("Name = %q", wh.Name())
	}
}

func TestWebhookNotifier_Non2xxIsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	err := (&WebhookNotifier{URL: srv.URL}).Notify(context.Background(), sampleNotification())
	if err == nil {
		t.Fatal("expected error on 500, got nil")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("error should name the status: %v", err)
	}
}

func TestSlackNotifier_PayloadShape(t *testing.T) {
	var got slackPayload
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("decode: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sl := &SlackNotifier{WebhookURL: srv.URL}
	if err := sl.Notify(context.Background(), sampleNotification()); err != nil {
		t.Fatalf("Notify: %v", err)
	}
	// Critical ⇒ rotating-light emoji + the readable metric line.
	for _, want := range []string{":rotating_light:", "[sluice s1]", "storage_util", "0.91", "0.90", "approaching capacity"} {
		if !strings.Contains(got.Text, want) {
			t.Errorf("slack text %q missing %q", got.Text, want)
		}
	}
	if sl.Name() != "slack" {
		t.Errorf("Name = %q", sl.Name())
	}
}

func TestSlackNotifier_WarningEmoji(t *testing.T) {
	n := sampleNotification()
	n.Level = LevelWarning
	if txt := slackText(n); !strings.HasPrefix(txt, ":warning:") {
		t.Errorf("warning level should use :warning:, got %q", txt)
	}
}

func TestSlackNotifier_Non2xxIsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	if err := (&SlackNotifier{WebhookURL: srv.URL}).Notify(context.Background(), sampleNotification()); err == nil {
		t.Fatal("expected error on 400")
	}
}

func TestMultiNotifier_FanOut(t *testing.T) {
	var hitA, hitB atomic.Int32
	srvA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hitA.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srvA.Close()
	srvB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hitB.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srvB.Close()

	m := NewMultiNotifier(&WebhookNotifier{URL: srvA.URL}, &SlackNotifier{WebhookURL: srvB.URL})
	if err := m.Notify(context.Background(), sampleNotification()); err != nil {
		t.Fatalf("Notify: %v", err)
	}
	if hitA.Load() != 1 || hitB.Load() != 1 {
		t.Errorf("both sinks should be hit once: a=%d b=%d", hitA.Load(), hitB.Load())
	}
}

func TestMultiNotifier_FailureIsolation(t *testing.T) {
	var hitHealthy atomic.Int32
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer bad.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hitHealthy.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer good.Close()

	// Bad sink FIRST: the healthy sink must still receive (no short-circuit),
	// and the joined error must name the failing sink.
	m := NewMultiNotifier(&WebhookNotifier{URL: bad.URL}, &WebhookNotifier{URL: good.URL})
	err := m.Notify(context.Background(), sampleNotification())
	if err == nil {
		t.Fatal("expected a joined error when one sink fails")
	}
	if !strings.Contains(err.Error(), "webhook") {
		t.Errorf("joined error should name the failing sink: %v", err)
	}
	if hitHealthy.Load() != 1 {
		t.Errorf("healthy sink must still receive despite earlier failure, hits=%d", hitHealthy.Load())
	}
}

func TestNewMultiNotifier_DropsNils(t *testing.T) {
	m := NewMultiNotifier(nil, nil)
	if m != nil {
		t.Errorf("all-nil should yield nil MultiNotifier, got %v", m)
	}
	// A nil MultiNotifier Notify is a safe no-op.
	if err := m.Notify(context.Background(), sampleNotification()); err != nil {
		t.Errorf("nil MultiNotifier Notify should be a no-op, got %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	m2 := NewMultiNotifier(nil, &WebhookNotifier{URL: srv.URL}, nil)
	if len(m2) != 1 {
		t.Errorf("nils should be dropped, len=%d", len(m2))
	}
}

func TestNotify_ContextCancelHonored(t *testing.T) {
	// A server that blocks; the request must abort on ctx cancel rather than
	// hang for the client timeout.
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	defer close(release)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	err := (&WebhookNotifier{URL: srv.URL}).Notify(ctx, sampleNotification())
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error should wrap context.Canceled, got %v", err)
	}
}

// errNotifier is a fake Notifier that always errors; used to confirm the
// joined-error path without a server.
type errNotifier struct{ name string }

func (e errNotifier) Notify(context.Context, Notification) error { return errors.New("dead") }
func (e errNotifier) Name() string                               { return e.name }

func TestMultiNotifier_JoinsAllErrors(t *testing.T) {
	m := NewMultiNotifier(errNotifier{"a"}, errNotifier{"b"})
	err := m.Notify(context.Background(), sampleNotification())
	if err == nil {
		t.Fatal("expected joined error")
	}
	for _, want := range []string{"a:", "b:"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("joined error %q missing %q", err.Error(), want)
		}
	}
}
