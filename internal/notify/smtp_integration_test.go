//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Hermetic SMTP-sink integration test. Boots a Mailpit container (a fake SMTP
// server that captures mail and exposes it over an HTTP API), sends a real
// alert through [SMTPNotifier] over the wire, and asserts the captured
// message's subject, recipient, and BOTH bodies (HTML + plaintext). No real
// provider, no credentials — mirrors the webhook sink's local-capture test,
// one layer down (an actual SMTP transaction instead of an HTTP POST).

package notify

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// startMailpit boots an axllent/mailpit container and returns the SMTP host +
// port to send to, the base HTTP API URL to read captured mail from, and a
// cleanup callback.
func startMailpit(t *testing.T) (smtpHost string, smtpPort int, apiBase string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	req := testcontainers.ContainerRequest{
		Image:        "axllent/mailpit:latest",
		ExposedPorts: []string{"1025/tcp", "8025/tcp"},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("1025/tcp"),
			wait.ForListeningPort("8025/tcp"),
		),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start mailpit: %v", err)
	}
	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}

	host, err := container.Host(ctx)
	if err != nil {
		terminate()
		t.Fatalf("container host: %v", err)
	}
	smtpMapped, err := container.MappedPort(ctx, "1025/tcp")
	if err != nil {
		terminate()
		t.Fatalf("mapped smtp port: %v", err)
	}
	apiMapped, err := container.MappedPort(ctx, "8025/tcp")
	if err != nil {
		terminate()
		t.Fatalf("mapped api port: %v", err)
	}
	sp, err := strconv.Atoi(smtpMapped.Port())
	if err != nil {
		terminate()
		t.Fatalf("parse smtp port %q: %v", smtpMapped.Port(), err)
	}
	return host, sp, fmt.Sprintf("http://%s:%s", host, apiMapped.Port()), terminate
}

// mailpitMessage is the subset of Mailpit's message JSON the test asserts on.
type mailpitMessage struct {
	ID      string `json:"ID"`
	Subject string `json:"Subject"`
	To      []struct {
		Address string `json:"Address"`
	} `json:"To"`
	From struct {
		Address string `json:"Address"`
	} `json:"From"`
	HTML string `json:"HTML"`
	Text string `json:"Text"`
}

func TestSMTPNotifier_SendsThroughMailpit(t *testing.T) {
	host, port, apiBase, cleanup := startMailpit(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sink := NewSMTPNotifier(SMTPConfig{
		Host: host,
		Port: port,
		From: "alerts@sluice.test",
		To:   []string{"ops@sluice.test"},
		TLS:  TLSNone, // Mailpit's default listener is plaintext, no auth.
		Auth: SMTPAuthNone,
	})
	if sink == nil {
		t.Fatal("expected a configured SMTP sink")
	}

	n := sampleNotification()
	n.StreamID = "prod-orders"
	if err := sink.Notify(ctx, n); err != nil {
		t.Fatalf("Notify through mailpit: %v", err)
	}

	msg := waitForOneMessage(ctx, t, apiBase)

	if !strings.Contains(msg.Subject, "storage_util threshold alert: prod-orders") {
		t.Errorf("subject = %q, want the metric + stream", msg.Subject)
	}
	if len(msg.To) != 1 || msg.To[0].Address != "ops@sluice.test" {
		t.Errorf("To = %+v, want [ops@sluice.test]", msg.To)
	}
	if msg.From.Address != "alerts@sluice.test" {
		t.Errorf("From = %q, want alerts@sluice.test", msg.From.Address)
	}
	// Both alternatives must arrive carrying the facts.
	for _, want := range []string{"storage_util", "0.91", "0.9", "prod-orders", "CRITICAL"} {
		if !strings.Contains(msg.HTML, want) {
			t.Errorf("HTML body missing %q", want)
		}
		if !strings.Contains(msg.Text, want) {
			t.Errorf("text body missing %q", want)
		}
	}
	if !strings.Contains(msg.HTML, "<!DOCTYPE html>") {
		t.Error("HTML alternative should be a full HTML document")
	}
}

// waitForOneMessage polls Mailpit's API until exactly one message has landed,
// then fetches its full body. The send is synchronous, but the SMTP→store
// step is briefly async, so a short poll avoids flakiness.
func waitForOneMessage(ctx context.Context, t *testing.T, apiBase string) mailpitMessage {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		var list struct {
			Messages []mailpitMessage `json:"messages"`
			Total    int              `json:"total"`
		}
		getJSON(ctx, t, apiBase+"/api/v1/messages", &list)
		if list.Total >= 1 && len(list.Messages) >= 1 {
			var full mailpitMessage
			getJSON(ctx, t, apiBase+"/api/v1/message/"+list.Messages[0].ID, &full)
			return full
		}
		if time.Now().After(deadline) {
			t.Fatalf("no message captured by mailpit within deadline (total=%d)", list.Total)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func getJSON(ctx context.Context, t *testing.T, url string, dst any) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		t.Fatalf("build request %s: %v", url, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d", url, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(dst); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}
}
