// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"
)

// TestParseURIDSN_InvalidDSNDoesNotLeakCredential pins the credential-
// in-logs fix at the postgres engine site: an invalid URI DSN whose
// userinfo carries a real secret must produce an error that names the
// failure WITHOUT echoing the password. url.Parse embeds the raw input
// (password included) in its *url.Error; parseURIDSN routes it through
// diagnose.SafeParseError to strip it.
func TestParseURIDSN_InvalidDSNDoesNotLeakCredential(t *testing.T) {
	const secret = "SUPERSECRET"
	// The \x7f control byte makes url.Parse fail only after it has
	// captured the userinfo — the exact shape that leaks the password.
	_, err := parseURIDSN("postgres://appuser:" + secret + "@host\x7f/db")
	if err == nil {
		t.Fatal("expected parseURIDSN to reject the control-char DSN")
	}
	if strings.Contains(err.Error(), secret) {
		t.Errorf("parseURIDSN leaked the credential in its error: %q", err.Error())
	}
	// The useful reason is still preserved.
	if !strings.Contains(err.Error(), "invalid DSN URI") {
		t.Errorf("parseURIDSN error lost its context prefix: %q", err.Error())
	}
	if !strings.Contains(err.Error(), "invalid control character") {
		t.Errorf("parseURIDSN error dropped the underlying reason: %q", err.Error())
	}
}

// TestParseURIDSN_ValidDSNUnchanged confirms the fix only touches the
// parse-ERROR path: a valid credentialed DSN still parses cleanly, with
// the schema defaulting and database-name handling unchanged.
func TestParseURIDSN_ValidDSNUnchanged(t *testing.T) {
	cfg, err := parseURIDSN("postgres://appuser:s3cr3t@localhost:5432/appdb?sslmode=disable")
	if err != nil {
		t.Fatalf("valid DSN unexpectedly rejected: %v", err)
	}
	if cfg.schema != "public" {
		t.Errorf("schema = %q; want default %q", cfg.schema, "public")
	}
}
