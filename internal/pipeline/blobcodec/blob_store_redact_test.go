// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// redactBlobURL must strip USERINFO, not just the query string
// (audit 2026-08-04).
//
// The original implementation cleared only u.RawQuery. Because gocloud's
// drivers read u.Host and ignore u.User, embedding credentials in the
// authority WORKS — so the failure was a silent success, and the redacted
// value carrying a live secret reached an INFO log, the sluice_cdc_state row
// on the TARGET database (durable, replicated, SELECT-readable), and a
// PrivacyBasic diagnose bundle whose help text promises no DSN.
//
// The cases below are the shapes an operator actually writes. The parse-
// failure fallback is included because a redactor whose error path leaks is
// not a redactor.

package blobcodec

import (
	"strings"
	"testing"
)

func TestRedactBlobURL_StripsCredentials(t *testing.T) {
	// Substrings that must never survive redaction, whatever the shape.
	secrets := []string{"wJalrXUtnFEMI", "hunter2", "AKIAIOSFODNN7EXAMPLE"}

	for _, tc := range []struct {
		name string
		in   string
	}{
		{"s3 key and secret in userinfo", "s3://AKIAIOSFODNN7EXAMPLE:wJalrXUtnFEMI@bucket/chain?region=us-east-1"},
		{"password only", "azblob://:hunter2@container/path"},
		{"userinfo and query", "gs://AKIAIOSFODNN7EXAMPLE:hunter2@bucket/p?cred=x"},
		{"unparseable, userinfo present", "s3://AKIAIOSFODNN7EXAMPLE:hunter2@buck et/p?x=1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := redactBlobURL(tc.in)
			for _, secret := range secrets {
				if strings.Contains(got, secret) {
					t.Errorf("redacted URL still carries %q.\n  in:  %s\n  out: %s\n\n"+
						"This value is logged at INFO, persisted into the cdc-state row on the TARGET "+
						"database, and collected into a PrivacyBasic diagnose bundle. A credential here "+
						"is durable and replicated.", secret, tc.in, got)
				}
			}
			// The locator must survive — a redactor that destroys the bucket
			// name makes the log line useless and invites someone to log the
			// raw URL instead.
			if !strings.Contains(got, "bucket") && !strings.Contains(got, "container") && !strings.Contains(got, "buck") {
				t.Errorf("redaction removed the host as well as the credentials: %q -> %q", tc.in, got)
			}
		})
	}
}

// TestRedactBlobURL_LeavesCredentialFreeURLsIntact is the other direction: the
// common case must be untouched, or the redactor silently degrades every log
// line it exists to make safe.
func TestRedactBlobURL_LeavesCredentialFreeURLsIntact(t *testing.T) {
	for _, in := range []string{
		"s3://my-bucket/chains/app",
		"file:///var/backups/app",
		"gs://bucket/path",
	} {
		if got := redactBlobURL(in); got != in {
			t.Errorf("credential-free URL was altered: %q -> %q", in, got)
		}
	}
}
