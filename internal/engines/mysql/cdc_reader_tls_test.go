// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"bytes"
	"context"
	"crypto/tls"
	"log/slog"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
)

// TestBinlogTLSFromDSN pins the DSN tls= → binlog-stream TLS mapping
// through the REAL parser (parseDSN), not a hand-built mysql.Config —
// the Bug-180 "pin through the layer that produces the value" lesson:
// go-sql-driver's normalize step is what resolves the tls= string into
// cfg.TLS, and a driver behavior change there must fail this pin, not
// silently re-open the plaintext downgrade (audit finding N-3).
func TestBinlogTLSFromDSN(t *testing.T) {
	const host = "db.example.com"
	cases := []struct {
		name string
		dsn  string

		wantNil      bool
		wantInsecure bool
	}{
		{
			name:    "absent tls param stays plaintext (today's behavior)",
			dsn:     "user:pw@tcp(db.example.com:3306)/app",
			wantNil: true,
		},
		{
			name:    "tls=false stays plaintext",
			dsn:     "user:pw@tcp(db.example.com:3306)/app?tls=false",
			wantNil: true,
		},
		{
			name: "tls=true verifies",
			dsn:  "user:pw@tcp(db.example.com:3306)/app?tls=true",
		},
		{
			name:         "tls=skip-verify encrypts without verification",
			dsn:          "user:pw@tcp(db.example.com:3306)/app?tls=skip-verify",
			wantInsecure: true,
		},
		{
			// go-mysql's binlog client has no try-TLS-then-plaintext
			// mode, so preferred maps to its TLS half (skip-verify)
			// with the plaintext fallback REFUSED — see
			// binlogTLSFromConfig's comment for the loud-failure
			// rationale.
			name:         "tls=preferred maps to skip-verify TLS, fallback refused",
			dsn:          "user:pw@tcp(db.example.com:3306)/app?tls=preferred",
			wantInsecure: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg, err := parseDSN(c.dsn)
			if err != nil {
				t.Fatalf("parseDSN: %v", err)
			}
			got := binlogTLSFromConfig(cfg, host)
			if c.wantNil {
				if got != nil {
					t.Fatalf("binlogTLSFromConfig = %+v; want nil (plaintext)", got)
				}
				return
			}
			if got == nil {
				t.Fatal("binlogTLSFromConfig = nil; want a TLS config")
			}
			if got.InsecureSkipVerify != c.wantInsecure {
				t.Errorf("InsecureSkipVerify = %v; want %v", got.InsecureSkipVerify, c.wantInsecure)
			}
			if !c.wantInsecure && got.ServerName != host {
				t.Errorf("ServerName = %q; want %q", got.ServerName, host)
			}
			if got.MinVersion != tls.VersionTLS12 {
				t.Errorf("MinVersion = %#x; want TLS 1.2 (%#x)", got.MinVersion, uint16(tls.VersionTLS12))
			}
		})
	}
}

// TestBinlogTLSFromDSN_UnregisteredCustomNameRefusedAtParse pins the
// refusal path for a custom tls= config name that was never registered
// via mysql.RegisterTLSConfig: go-sql-driver's own ParseDSN refuses it
// loudly, so the binlog mapping can never see (and silently downgrade)
// an unresolved name. If a driver upgrade ever relaxes that refusal,
// this pin fails and the mapping must grow its own named error.
func TestBinlogTLSFromDSN_UnregisteredCustomNameRefusedAtParse(t *testing.T) {
	_, err := parseDSN("user:pw@tcp(db.example.com:3306)/app?tls=no-such-config")
	if err == nil {
		t.Fatal("parseDSN accepted an unregistered custom tls config name; want loud refusal")
	}
	if !strings.Contains(err.Error(), "unknown config name") {
		t.Errorf("parseDSN error = %q; want it to name the unknown config", err)
	}
}

// TestBinlogTLSFromDSN_RegisteredCustomName pins that a REGISTERED
// custom tls= config (CA pool, client certs, explicit MinVersion, …)
// carries onto the binlog stream rather than being refused or silently
// downgraded — the binlog transport is a clone of exactly what the
// query connections negotiate. An explicit MinVersion is the
// operator's named choice and is respected; an unset one is raised to
// the TLS 1.2 floor (matching the VStream path).
func TestBinlogTLSFromDSN_RegisteredCustomName(t *testing.T) {
	const host = "db.example.com"

	if err := mysql.RegisterTLSConfig("sluice-unit-tls", &tls.Config{ServerName: "custom.example"}); err != nil {
		t.Fatalf("RegisterTLSConfig: %v", err)
	}
	t.Cleanup(func() { mysql.DeregisterTLSConfig("sluice-unit-tls") })
	if err := mysql.RegisterTLSConfig("sluice-unit-tls13", &tls.Config{MinVersion: tls.VersionTLS13}); err != nil {
		t.Fatalf("RegisterTLSConfig: %v", err)
	}
	t.Cleanup(func() { mysql.DeregisterTLSConfig("sluice-unit-tls13") })

	cfg, err := parseDSN("user:pw@tcp(db.example.com:3306)/app?tls=sluice-unit-tls")
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	got := binlogTLSFromConfig(cfg, host)
	if got == nil {
		t.Fatal("binlogTLSFromConfig = nil; want the registered custom config")
	}
	if got.ServerName != "custom.example" {
		t.Errorf("ServerName = %q; want the registered config's custom.example", got.ServerName)
	}
	if got.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion = %#x; want the unset value raised to TLS 1.2", got.MinVersion)
	}

	cfg13, err := parseDSN("user:pw@tcp(db.example.com:3306)/app?tls=sluice-unit-tls13")
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	got13 := binlogTLSFromConfig(cfg13, host)
	if got13 == nil {
		t.Fatal("binlogTLSFromConfig = nil; want the registered custom config")
	}
	if got13.MinVersion != tls.VersionTLS13 {
		t.Errorf("MinVersion = %#x; want the registered config's explicit TLS 1.3 respected", got13.MinVersion)
	}
}

// TestWarnBinlogTransport pins the stream-open transport WARN for
// every tls= mode, including the zero-value reader: a struct-literal
// CDCReader (every non-DSN construction) is plaintext and says so —
// never a nil-deref, never a silent skip (zero-value-safe default).
func TestWarnBinlogTransport(t *testing.T) {
	insecure := &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12}
	verifying := &tls.Config{ServerName: "db.example.com", MinVersion: tls.VersionTLS12}

	cases := []struct {
		name   string
		reader *CDCReader

		wantContains string // "" = no WARN expected
	}{
		{
			name:         "zero-value reader warns plaintext",
			reader:       &CDCReader{},
			wantContains: "UNENCRYPTED",
		},
		{
			name:         "tls=false warns plaintext",
			reader:       &CDCReader{binlogTLSMode: "false"},
			wantContains: "UNENCRYPTED",
		},
		{
			name:         "tls=skip-verify warns verification disabled",
			reader:       &CDCReader{binlogTLS: insecure, binlogTLSMode: "skip-verify"},
			wantContains: "certificate verification is DISABLED",
		},
		{
			name:         "tls=preferred warns fallback refused",
			reader:       &CDCReader{binlogTLS: insecure, binlogTLSMode: "preferred"},
			wantContains: "WITHOUT plaintext fallback",
		},
		{
			name:         "custom insecure config warns verification disabled",
			reader:       &CDCReader{binlogTLS: insecure, binlogTLSMode: "sluice-custom"},
			wantContains: "certificate verification is DISABLED",
		},
		{
			name:   "tls=true is silent",
			reader: &CDCReader{binlogTLS: verifying, binlogTLSMode: "true"},
		},
		{
			name:   "custom verifying config is silent",
			reader: &CDCReader{binlogTLS: verifying, binlogTLSMode: "sluice-custom"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var buf bytes.Buffer
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
			t.Cleanup(func() { slog.SetDefault(prev) })

			c.reader.warnBinlogTransport(context.Background())

			out := buf.String()
			if c.wantContains == "" {
				if out != "" {
					t.Fatalf("unexpected WARN output: %s", out)
				}
				return
			}
			if !strings.Contains(out, c.wantContains) {
				t.Errorf("WARN output %q does not contain %q", out, c.wantContains)
			}
		})
	}
}
