// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package notify

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	mail "github.com/wneessen/go-mail"
)

// SMTPNotifier delivers a [Notification] as an email via an SMTP relay — the
// most universal alert channel, and the one sink that covers every
// transactional provider (SendGrid/Mailgun/SES/Postmark are all SMTP) AND
// self-hosted/corporate relays without N provider-specific integrations.
//
// It keeps the SAME semantics as the webhook/Slack sinks: opt-in (inert
// unless [SMTPConfig.Configured]), credential-via-env (the password is never
// taken on the command line), and ADVISORY + failure-isolated — a dead relay
// surfaces as a returned error that the [MultiNotifier] joins and the alerter
// logs-and-swallows, never affecting the sync.
//
// A fresh go-mail client (one connection) is built per send: a notification
// fires at most a few times an hour (edge-trigger + cooldown), so a pooled
// long-lived connection buys nothing and a per-send dial keeps a dead relay
// from holding a socket open against the advisory path.
type SMTPNotifier struct {
	cfg SMTPConfig

	// sendFunc is a test seam: when non-nil it replaces the real go-mail
	// dial+send, so the message-build path (from/to/subject/bodies) is
	// unit-testable without standing up a relay. nil in production.
	sendFunc func(ctx context.Context, msg *mail.Msg) error
}

// TLSMode selects the transport security of the SMTP connection.
type TLSMode string

const (
	// TLSStartTLS connects in cleartext then upgrades via STARTTLS
	// (the submission default, typically port 587). The zero value.
	TLSStartTLS TLSMode = "starttls"

	// TLSImplicit negotiates TLS from connect (SMTPS, typically port 465).
	TLSImplicit TLSMode = "implicit"

	// TLSNone disables TLS entirely. For a trusted local relay or a fake
	// SMTP server in tests only — never send credentials over it.
	TLSNone TLSMode = "none"
)

// SMTPAuth selects the SMTP authentication mechanism.
type SMTPAuth string

const (
	// SMTPAuthNone sends without authenticating (open/trusted relay). The
	// zero value.
	SMTPAuthNone SMTPAuth = "none"

	// SMTPAuthPlain is RFC 4616 PLAIN (the common provider mechanism).
	SMTPAuthPlain SMTPAuth = "plain"

	// SMTPAuthLogin is the LOGIN mechanism (older relays / Office 365).
	SMTPAuthLogin SMTPAuth = "login"
)

// defaultSMTPTimeout bounds a single dial+send. A notification is advisory;
// it must never block the alerter goroutine for long.
const defaultSMTPTimeout = 15 * time.Second

// Default submission ports, used when [SMTPConfig.Port] is unset (0): 587 for
// STARTTLS/none, 465 for implicit TLS.
const (
	smtpDefaultPortStartTLS = 587
	smtpDefaultPortImplicit = 465
)

// SMTPConfig holds the SMTP sink's settings. It is the typed carrier the
// pipeline assembles from the CLI flags + the env-only password and hands to
// [NewSMTPNotifier]. A zero Host means the sink is not configured (the
// opt-in, zero-value-off contract).
type SMTPConfig struct {
	Host     string   // relay hostname (e.g. smtp.sendgrid.net)
	Port     int      // 0 ⇒ default for the TLS mode (587 / 465)
	From     string   // envelope + header From address
	To       []string // one or more recipient addresses
	Username string   // SMTP auth username (e.g. "apikey" for SendGrid)
	Password string   // SMTP auth secret — set via env ONLY, never logged
	TLS      TLSMode  // "" ⇒ TLSStartTLS
	Auth     SMTPAuth // "" ⇒ SMTPAuthNone
	Timeout  time.Duration
}

// Configured reports whether the operator opted the SMTP sink in. Gating on a
// non-empty Host (not a bool) keeps the zero value safely OFF for every
// construction — the same posture the webhook/Slack URL gating uses.
func (c SMTPConfig) Configured() bool {
	return strings.TrimSpace(c.Host) != ""
}

// Validate checks a CONFIGURED sink has the fields a send requires. It is the
// loud-failure gate: an SMTP sink that can't form a valid message is refused
// at construction (naming the missing field) rather than silently dropping
// every alert at send time. A non-configured config validates trivially (the
// sink is simply absent).
func (c SMTPConfig) Validate() error {
	if !c.Configured() {
		return nil
	}
	if strings.TrimSpace(c.From) == "" {
		return errors.New("smtp: --notify-smtp-from is required when --notify-smtp-host is set")
	}
	if len(nonEmpty(c.To)) == 0 {
		return errors.New("smtp: at least one --notify-smtp-to recipient is required when --notify-smtp-host is set")
	}
	if c.Port < 0 || c.Port > 65535 {
		return fmt.Errorf("smtp: --notify-smtp-port %d out of range (1-65535)", c.Port)
	}
	switch c.TLS {
	case "", TLSStartTLS, TLSImplicit, TLSNone:
	default:
		return fmt.Errorf("smtp: unknown --notify-smtp-tls mode %q (want starttls|implicit|none)", c.TLS)
	}
	switch c.Auth {
	case "", SMTPAuthNone:
	case SMTPAuthPlain, SMTPAuthLogin:
		if strings.TrimSpace(c.Username) == "" {
			return fmt.Errorf("smtp: --notify-smtp-username is required for --notify-smtp-auth=%s", c.Auth)
		}
		if c.TLS == TLSNone {
			// Loud-failure tenet (the vstream_insecure_tls precedent): this
			// combination puts the SMTP credentials on the wire in CLEARTEXT.
			// [TLSNone] exists for trusted local relays / test fakes, which
			// don't authenticate — warn every time the combination is
			// validated rather than hard-refuse, so an existing localhost
			// config keeps working but can't silently downgrade a real
			// credential's transport security.
			slog.Warn("smtp: --notify-smtp-auth=" + string(c.Auth) + " with --notify-smtp-tls=none — SMTP " +
				"credentials will be sent in CLEARTEXT (intended for trusted local relays only; " +
				"use starttls or implicit TLS with a real relay)")
		}
	default:
		return fmt.Errorf("smtp: unknown --notify-smtp-auth mode %q (want none|plain|login)", c.Auth)
	}
	return nil
}

// NewSMTPNotifier builds an SMTP sink from cfg. Returns nil when cfg is not
// configured, so a caller can fold it straight into a sink chain (the
// [NewMultiNotifier] nil-drop then applies).
func NewSMTPNotifier(cfg SMTPConfig) *SMTPNotifier {
	if !cfg.Configured() {
		return nil
	}
	return &SMTPNotifier{cfg: cfg}
}

// Notify renders n to an email and sends it via a freshly dialled relay,
// honouring ctx for the whole dial+send. A relay/transport failure is
// returned (the caller isolates it).
func (s *SMTPNotifier) Notify(ctx context.Context, n Notification) error {
	msg, err := s.buildMessage(n)
	if err != nil {
		return err
	}
	if s.sendFunc != nil {
		return s.sendFunc(ctx, msg)
	}
	client, err := s.buildClient()
	if err != nil {
		return err
	}
	if err := client.DialAndSendWithContext(ctx, msg); err != nil {
		return fmt.Errorf("smtp send: %w", err)
	}
	return nil
}

// Name implements [Notifier].
func (s *SMTPNotifier) Name() string { return "smtp" }

// buildMessage renders the notification into a go-mail message: a plaintext
// body (the universal fallback) with the HTML body as the richer alternative.
func (s *SMTPNotifier) buildMessage(n Notification) (*mail.Msg, error) {
	subject, htmlBody, textBody, err := renderEmail(n)
	if err != nil {
		return nil, err
	}
	msg := mail.NewMsg()
	if err := msg.From(s.cfg.From); err != nil {
		return nil, fmt.Errorf("smtp from %q: %w", s.cfg.From, err)
	}
	if err := msg.To(nonEmpty(s.cfg.To)...); err != nil {
		return nil, fmt.Errorf("smtp to %v: %w", s.cfg.To, err)
	}
	msg.Subject(subject)
	msg.SetDate()
	// Plaintext primary + HTML alternative: a mail client renders the richest
	// alternative it can and falls back to text/plain otherwise.
	msg.SetBodyString(mail.TypeTextPlain, textBody)
	msg.AddAlternativeString(mail.TypeTextHTML, htmlBody)
	return msg, nil
}

// buildClient assembles the go-mail client from the config, mapping the
// engine-neutral TLS/auth modes onto go-mail's options. The port defaults per
// TLS mode when unset.
func (s *SMTPNotifier) buildClient() (*mail.Client, error) {
	timeout := s.cfg.Timeout
	if timeout <= 0 {
		timeout = defaultSMTPTimeout
	}
	opts := []mail.Option{
		mail.WithPort(s.resolvePort()),
		mail.WithTimeout(timeout),
	}

	switch s.cfg.TLS {
	case TLSImplicit:
		opts = append(opts, mail.WithSSL())
	case TLSNone:
		opts = append(opts, mail.WithTLSPolicy(mail.NoTLS))
	case "", TLSStartTLS:
		opts = append(opts, mail.WithTLSPolicy(mail.TLSMandatory))
	default:
		return nil, fmt.Errorf("smtp: unknown TLS mode %q", s.cfg.TLS)
	}

	switch s.cfg.Auth {
	case SMTPAuthPlain:
		opts = append(opts, mail.WithSMTPAuth(mail.SMTPAuthPlain),
			mail.WithUsername(s.cfg.Username), mail.WithPassword(s.cfg.Password))
	case SMTPAuthLogin:
		opts = append(opts, mail.WithSMTPAuth(mail.SMTPAuthLogin),
			mail.WithUsername(s.cfg.Username), mail.WithPassword(s.cfg.Password))
	case "", SMTPAuthNone:
		// No authentication (open/trusted relay).
	default:
		return nil, fmt.Errorf("smtp: unknown auth mode %q", s.cfg.Auth)
	}

	client, err := mail.NewClient(s.cfg.Host, opts...)
	if err != nil {
		return nil, fmt.Errorf("smtp client: %w", err)
	}
	return client, nil
}

// resolvePort returns the configured port, or the submission default for the
// TLS mode when unset (465 implicit, 587 otherwise).
func (s *SMTPNotifier) resolvePort() int {
	if s.cfg.Port > 0 {
		return s.cfg.Port
	}
	if s.cfg.TLS == TLSImplicit {
		return smtpDefaultPortImplicit
	}
	return smtpDefaultPortStartTLS
}

// nonEmpty returns ss with blank/whitespace-only entries dropped, so a
// comma-trailing or accidentally-empty --notify-smtp-to entry never becomes a
// bogus recipient.
func nonEmpty(ss []string) []string {
	out := make([]string, 0, len(ss))
	for _, s := range ss {
		if strings.TrimSpace(s) != "" {
			out = append(out, s)
		}
	}
	return out
}
