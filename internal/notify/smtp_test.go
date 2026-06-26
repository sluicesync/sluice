// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package notify

import (
	"context"
	"strings"
	"testing"

	mail "github.com/wneessen/go-mail"
)

func TestSMTPConfig_Configured(t *testing.T) {
	if (SMTPConfig{}).Configured() {
		t.Error("zero SMTPConfig must report not-configured (opt-in, zero-value-off)")
	}
	if (SMTPConfig{Host: "  "}).Configured() {
		t.Error("whitespace-only host must report not-configured")
	}
	if !(SMTPConfig{Host: "smtp.example.com"}).Configured() {
		t.Error("a non-empty host must report configured")
	}
}

func TestNewSMTPNotifier_NilWhenUnconfigured(t *testing.T) {
	// The zero value yields a nil sink so the builder can fold it into a chain
	// (the NewMultiNotifier nil-drop then applies) without an extra guard.
	if s := NewSMTPNotifier(SMTPConfig{}); s != nil {
		t.Errorf("unconfigured SMTPConfig must yield a nil sink, got %#v", s)
	}
	if s := NewSMTPNotifier(SMTPConfig{Host: "smtp.example.com"}); s == nil {
		t.Error("configured SMTPConfig must yield a non-nil sink")
	}
}

func TestSMTPConfig_Validate(t *testing.T) {
	base := SMTPConfig{Host: "smtp.example.com", From: "alerts@x.test", To: []string{"ops@x.test"}}
	cases := []struct {
		name    string
		cfg     SMTPConfig
		wantErr string // substring; "" ⇒ expect nil
	}{
		{"unconfigured is valid", SMTPConfig{}, ""},
		{"complete is valid", base, ""},
		{"missing from", SMTPConfig{Host: "h", To: []string{"a@b"}}, "--notify-smtp-from"},
		{"missing to", SMTPConfig{Host: "h", From: "a@b"}, "--notify-smtp-to"},
		{"blank-only to", SMTPConfig{Host: "h", From: "a@b", To: []string{"  ", ""}}, "--notify-smtp-to"},
		{"port too high", SMTPConfig{Host: "h", From: "a@b", To: []string{"c@d"}, Port: 70000}, "out of range"},
		{"bad tls mode", SMTPConfig{Host: "h", From: "a@b", To: []string{"c@d"}, TLS: "ssl"}, "--notify-smtp-tls"},
		{"bad auth mode", SMTPConfig{Host: "h", From: "a@b", To: []string{"c@d"}, Auth: "oauth"}, "--notify-smtp-auth"},
		{"plain needs username", SMTPConfig{Host: "h", From: "a@b", To: []string{"c@d"}, Auth: SMTPAuthPlain}, "--notify-smtp-username"},
		{"login with username ok", SMTPConfig{Host: "h", From: "a@b", To: []string{"c@d"}, Auth: SMTPAuthLogin, Username: "u"}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("expected nil error, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q missing %q", err.Error(), tc.wantErr)
			}
		})
	}
}

// TestSMTPNotifier_BuildMessage captures the built go-mail message via the
// sendFunc seam (no relay) and asserts the From/To/Subject and BOTH bodies
// (plaintext primary + HTML alternative) carry the alert facts.
func TestSMTPNotifier_BuildMessage(t *testing.T) {
	var captured *mail.Msg
	s := &SMTPNotifier{
		cfg: SMTPConfig{
			Host: "smtp.example.com",
			From: "alerts@sluice.test",
			To:   []string{"ops@sluice.test", " "}, // blank entry must be dropped
		},
		sendFunc: func(_ context.Context, m *mail.Msg) error {
			captured = m
			return nil
		},
	}
	if err := s.Notify(context.Background(), sampleNotification()); err != nil {
		t.Fatalf("Notify: %v", err)
	}
	if captured == nil {
		t.Fatal("sendFunc was not invoked")
	}

	from := captured.GetFromString()
	if len(from) != 1 || !strings.Contains(from[0], "alerts@sluice.test") {
		t.Errorf("From = %v, want alerts@sluice.test", from)
	}
	rcpts, err := captured.GetRecipients()
	if err != nil {
		t.Fatalf("GetRecipients: %v", err)
	}
	if len(rcpts) != 1 || !strings.Contains(rcpts[0], "ops@sluice.test") {
		t.Errorf("recipients = %v, want exactly one (ops@sluice.test, blank dropped)", rcpts)
	}
	subj := captured.GetGenHeader(mail.HeaderSubject)
	if len(subj) != 1 || !strings.Contains(subj[0], "storage_util threshold alert: s1") {
		t.Errorf("Subject = %v, want '[sluice] storage_util threshold alert: s1'", subj)
	}

	// Serialize the whole message and confirm both MIME alternatives + the
	// facts are present.
	var buf strings.Builder
	if _, err := captured.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	raw := buf.String()
	for _, want := range []string{"text/plain", "text/html", "storage_util", "0.91", "0.9", "approaching capacity"} {
		if !strings.Contains(raw, want) {
			t.Errorf("serialized message missing %q", want)
		}
	}
}

func TestSMTPNotifier_Name(t *testing.T) {
	if got := (&SMTPNotifier{}).Name(); got != "smtp" {
		t.Errorf("Name = %q, want smtp", got)
	}
}

// TestSMTPNotifier_FailureIsolation confirms a dead relay surfaces as a
// returned error (the MultiNotifier joins + the alerter swallows it) — never a
// panic and never silence. A connection to a closed local port is the cheap
// stand-in for a dead relay; the call must return within the dial timeout.
func TestSMTPNotifier_FailureIsolation(t *testing.T) {
	s := NewSMTPNotifier(SMTPConfig{
		// 127.0.0.1:1 — nothing listens; connect refused fast.
		Host: "127.0.0.1", Port: 1, From: "a@b.test", To: []string{"c@d.test"}, TLS: TLSNone,
	})
	err := s.Notify(context.Background(), sampleNotification())
	if err == nil {
		t.Fatal("expected an error from a dead relay")
	}
	if !strings.Contains(err.Error(), "smtp") {
		t.Errorf("error should name the smtp send path: %v", err)
	}

	// And it must be isolated inside a MultiNotifier: the joined error names
	// the sink, but a healthy sibling would still be delivered (covered by the
	// MultiNotifier tests). Here we just confirm the join wraps the sink name.
	m := NewMultiNotifier(s)
	if jerr := m.Notify(context.Background(), sampleNotification()); jerr == nil ||
		!strings.Contains(jerr.Error(), "smtp:") {
		t.Errorf("joined error should name the smtp sink: %v", jerr)
	}
}

// TestSMTPNotifier_BadAddressIsLoud confirms a malformed From address fails
// loudly at message build (naming the field) rather than silently dropping the
// alert.
func TestSMTPNotifier_BadAddressIsLoud(t *testing.T) {
	s := &SMTPNotifier{cfg: SMTPConfig{Host: "h", From: "not an address", To: []string{"c@d.test"}}}
	err := s.Notify(context.Background(), sampleNotification())
	if err == nil || !strings.Contains(err.Error(), "from") {
		t.Errorf("expected a loud 'from' error on a malformed address, got %v", err)
	}
}

func TestSMTPNotifier_ResolvePort(t *testing.T) {
	cases := []struct {
		tls  TLSMode
		port int
		want int
	}{
		{TLSStartTLS, 0, 587},
		{"", 0, 587},
		{TLSNone, 0, 587},
		{TLSImplicit, 0, 465},
		{TLSImplicit, 2525, 2525}, // explicit wins
		{TLSStartTLS, 2525, 2525},
	}
	for _, tc := range cases {
		s := &SMTPNotifier{cfg: SMTPConfig{TLS: tc.tls, Port: tc.port}}
		if got := s.resolvePort(); got != tc.want {
			t.Errorf("resolvePort(tls=%q port=%d) = %d, want %d", tc.tls, tc.port, got, tc.want)
		}
	}
}

// TestSMTPNotifier_BuildClient confirms every TLS × auth combination builds a
// client without error (and that unknown modes are rejected) — exercising the
// option-mapping dispatch, not just one representative.
func TestSMTPNotifier_BuildClient(t *testing.T) {
	for _, tls := range []TLSMode{"", TLSStartTLS, TLSImplicit, TLSNone} {
		for _, auth := range []SMTPAuth{"", SMTPAuthNone, SMTPAuthPlain, SMTPAuthLogin} {
			cfg := SMTPConfig{Host: "smtp.example.com", From: "a@b", To: []string{"c@d"}, TLS: tls, Auth: auth, Username: "u", Password: "p"}
			if _, err := (&SMTPNotifier{cfg: cfg}).buildClient(); err != nil {
				t.Errorf("buildClient(tls=%q auth=%q): %v", tls, auth, err)
			}
		}
	}
	if _, err := (&SMTPNotifier{cfg: SMTPConfig{Host: "h", TLS: "bogus"}}).buildClient(); err == nil {
		t.Error("unknown TLS mode should error in buildClient")
	}
	if _, err := (&SMTPNotifier{cfg: SMTPConfig{Host: "h", Auth: "bogus"}}).buildClient(); err == nil {
		t.Error("unknown auth mode should error in buildClient")
	}
}

func TestNonEmpty(t *testing.T) {
	got := nonEmpty([]string{"a@b", " ", "", "c@d"})
	if len(got) != 2 || got[0] != "a@b" || got[1] != "c@d" {
		t.Errorf("nonEmpty = %v, want [a@b c@d]", got)
	}
	// A nil/empty input is safe.
	if len(nonEmpty(nil)) != 0 {
		t.Error("nonEmpty(nil) should be empty")
	}
}
