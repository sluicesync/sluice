// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The blob-URL PARSE-FAILURE paths must not leak an embedded credential
// (audit 2026-09-15 A0915-SEC-MEDIUM-1).
//
// The 2026-08-04 fix taught redactBlobURL to strip userinfo, and every
// error site called it — then wrapped the raw *url.Error with %w in the
// same message. net/url embeds the verbatim input in that error's .URL
// field, so the secret rode the wrapped error past the redaction one
// argument earlier. A third site (the no-scheme branch) echoed the raw
// string with no redactor at all. Both shapes are reachable from
// `backup --target` with no prior validation.
//
// Two leak shapes, pinned at every parse site and through the public
// entry point:
//
//   - a URL url.Parse REFUSES (a control byte in the path) — the
//     wrapped-error shape;
//   - a URL url.Parse ACCEPTS with an empty scheme (`//KEY:SECRET@…`,
//     a dropped `s3:`) — the no-scheme echo shape.

package blobcodec

import (
	"context"
	"strings"
	"testing"
)

func TestBlobStore_ParseFailureErrorsDoNotLeakCredentials(t *testing.T) {
	const (
		accessKey = "AKIAIOSFODNN7EXAMPLE"
		secret    = "wJalrXUtnFEMIsuperSecret123"
	)
	inputs := []struct {
		name string
		in   string
	}{
		{"parse refused: control byte after the prefix", "s3://" + accessKey + ":" + secret + "@mybucket/prefix\x7f"},
		{"parse refused: control byte, gs scheme", "gs://" + accessKey + ":" + secret + "@bucket/p\x01"},
		{"parse accepted with no scheme (dropped s3:)", "//" + accessKey + ":" + secret + "@mybucket/prefix"},
	}
	// Every site that turns the operator's --target into an error, plus
	// the public entry point that reaches them from cmd/sluice/backup.go.
	sites := []struct {
		name string
		call func(in string) error
	}{
		{"annotateBlobURL", func(in string) error {
			_, err := annotateBlobURL(in, BlobStoreOptions{})
			return err
		}},
		{"extractBlobPrefix", func(in string) error {
			_, err := extractBlobPrefix(in)
			return err
		}},
		{"OpenBlobStore", func(in string) error {
			_, err := OpenBlobStore(context.Background(), in, BlobStoreOptions{})
			return err
		}},
	}
	for _, in := range inputs {
		for _, site := range sites {
			t.Run(in.name+"/"+site.name, func(t *testing.T) {
				err := site.call(in.in)
				if err == nil {
					// extractBlobPrefix has no scheme check; an accepted
					// no-scheme parse is not an error there, and not a leak.
					if site.name == "extractBlobPrefix" && !strings.ContainsAny(in.in, "\x7f\x01") {
						return
					}
					t.Fatalf("%s accepted %q; expected a refusal", site.name, in.in)
				}
				msg := err.Error()
				for _, s := range []string{secret, accessKey} {
					if strings.Contains(msg, s) {
						t.Errorf("%s leaked %q into its error:\n  %s\n\n"+
							"This error reaches stderr and the structured log from `backup --target`. "+
							"A parse error must be wrapped through safeerr.SafeParseError and an echoed "+
							"URL must go through redactBlobURL.", site.name, s, msg)
					}
				}
				// The refusal must still be actionable — the host and
				// the reason survive.
				if !strings.Contains(msg, "bucket") {
					t.Errorf("%s dropped the locator with the credential: %s", site.name, msg)
				}
			})
		}
	}

	// The THIRD leak shape, reachable only through the public entry point:
	// a URL that parses cleanly and is refused by the gocloud DRIVER,
	// whose error text is `%v` of the *url.URL — userinfo included. Two
	// refusal families, each driver's own param validation (`?bogus=1`,
	// answered before any credential or network is touched) and the mux's
	// unregistered-scheme refusal (`s4://`), so a scrub that reached one
	// wrapper and not the other would show here. Every registered scheme
	// that accepts a userinfo-bearing URL is driven (azblob is not: its
	// opener needs an account before it validates params).
	t.Run("driver refusal echoes the URL", func(t *testing.T) {
		fileRoot := strings.ReplaceAll(t.TempDir(), "\\", "/")
		if !strings.HasPrefix(fileRoot, "/") {
			fileRoot = "/" + fileRoot // a Windows drive path needs the leading slash to form a three-slash file URL
		}
		driverInputs := []struct {
			name, in, reason string
		}{
			{"s3 unknown query parameter", "s3://" + accessKey + ":" + secret + "@mybucket/prefix?bogus=1", "bogus"},
			{"gs unknown query parameter", "gs://" + accessKey + ":" + secret + "@mybucket/prefix?bogus=1", "bogus"},
			{"file unknown query parameter", "file://" + accessKey + ":" + secret + "@" + fileRoot + "?bogus=1", "bogus"},
			{"unregistered scheme", "s4://" + accessKey + ":" + secret + "@mybucket/prefix", "s4"},
		}
		for _, in := range driverInputs {
			t.Run(in.name, func(t *testing.T) {
				_, err := OpenBlobStore(context.Background(), in.in, BlobStoreOptions{})
				if err == nil {
					t.Fatalf("OpenBlobStore accepted %q; expected the driver to refuse it", redactBlobURL(in.in))
				}
				msg := err.Error()
				for _, s := range []string{secret, accessKey} {
					if strings.Contains(msg, s) {
						t.Errorf("OpenBlobStore leaked %q through the DRIVER's error text:\n  %s\n\n"+
							"gocloud builds its refusal with %%v of the *url.URL, which prints userinfo; "+
							"the wrapped driver error must go through scrubBlobDriverErr.", s, msg)
					}
				}
				if !strings.Contains(msg, "bucket") {
					t.Errorf("the scrub dropped the locator with the credential: %s", msg)
				}
				if !strings.Contains(msg, in.reason) {
					t.Errorf("the scrub dropped the driver's reason (%q) with the credential: %s", in.reason, msg)
				}
			})
		}
	})
}
