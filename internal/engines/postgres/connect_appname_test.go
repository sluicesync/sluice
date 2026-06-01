// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"net/url"
	"strings"
	"testing"
	"unicode/utf8"
)

// withSavedApplicationID runs fn with applicationID set to id, restoring
// the process-global afterward so cases don't bleed into each other.
func withSavedApplicationID(t *testing.T, id string, fn func()) {
	t.Helper()
	prev := applicationID
	t.Cleanup(func() { applicationID = prev })
	SetApplicationID(id)
	fn()
}

func TestSetApplicationIDNormalisesEmpty(t *testing.T) {
	withSavedApplicationID(t, "", func() {
		if applicationID != "-" {
			t.Fatalf("empty id should normalise to %q, got %q", "-", applicationID)
		}
	})
}

// TestWithApplicationNameStampsURI checks the id+role are threaded into a
// URI DSN's application_name query parameter.
func TestWithApplicationNameStampsURI(t *testing.T) {
	withSavedApplicationID(t, "mystream", func() {
		got := withApplicationName("postgres://u:p@h:5432/db?sslmode=disable", roleSnapshot)
		u, err := url.Parse(got)
		if err != nil {
			t.Fatalf("result is not a valid URI: %v", err)
		}
		if want := "sluice/snapshot/mystream"; u.Query().Get("application_name") != want {
			t.Errorf("application_name = %q, want %q", u.Query().Get("application_name"), want)
		}
		// Existing params must survive.
		if u.Query().Get("sslmode") != "disable" {
			t.Errorf("sslmode param dropped: %q", got)
		}
	})
}

// TestWithApplicationNameStampsKV checks the libpq KV form.
func TestWithApplicationNameStampsKV(t *testing.T) {
	withSavedApplicationID(t, "mig1", func() {
		got := withApplicationName("host=localhost user=postgres dbname=app", roleApplier)
		if want := "application_name=sluice/applier/mig1"; !strings.Contains(got, want) {
			t.Errorf("KV DSN missing %q: %q", want, got)
		}
	})
}

// TestWithApplicationNameFallbackID checks the "-" fallback when no id
// was set (the bare go-test / no-main path).
func TestWithApplicationNameFallbackID(t *testing.T) {
	withSavedApplicationID(t, "", func() {
		got := withApplicationName("postgres://u:p@h/db", roleSchema)
		u, _ := url.Parse(got)
		if want := "sluice/schema/-"; u.Query().Get("application_name") != want {
			t.Errorf("application_name = %q, want %q", u.Query().Get("application_name"), want)
		}
	})
}

// TestWithApplicationNameDoesNotClobberURI asserts an operator-supplied
// application_name in a URI DSN is left untouched.
func TestWithApplicationNameDoesNotClobberURI(t *testing.T) {
	withSavedApplicationID(t, "mystream", func() {
		const dsn = "postgres://u:p@h/db?application_name=operator-tool"
		got := withApplicationName(dsn, roleSnapshot)
		u, _ := url.Parse(got)
		if got2 := u.Query().Get("application_name"); got2 != "operator-tool" {
			t.Errorf("operator application_name clobbered: got %q, want %q", got2, "operator-tool")
		}
	})
}

// TestWithApplicationNameDoesNotClobberKV asserts the same for the libpq
// KV form, including a case-insensitive key match.
func TestWithApplicationNameDoesNotClobberKV(t *testing.T) {
	withSavedApplicationID(t, "mystream", func() {
		const dsn = "host=h dbname=db Application_Name=operator-tool"
		got := withApplicationName(dsn, roleSnapshot)
		if got != dsn {
			t.Errorf("operator application_name clobbered: got %q, want unchanged %q", got, dsn)
		}
		if strings.Contains(got, "snapshot") {
			t.Errorf("sluice label leaked into operator-supplied DSN: %q", got)
		}
	})
}

// TestBuildApplicationNameTruncates pins the 63-byte (NAMEDATALEN-1)
// boundary: a long id is truncated from its tail while the `sluice/`
// prefix and the role — the discriminators the budget probe + Phase-2
// reaping match on — always survive.
func TestBuildApplicationNameTruncates(t *testing.T) {
	longID := strings.Repeat("x", 100)
	got := buildApplicationName(roleCDCReader, longID)

	if len(got) > maxAppNameBytes {
		t.Fatalf("application_name is %d bytes, exceeds the %d-byte limit: %q", len(got), maxAppNameBytes, got)
	}
	if want := "sluice/cdc-reader/"; !strings.HasPrefix(got, want) {
		t.Errorf("prefix+role did not survive truncation: %q does not start with %q", got, want)
	}
	if strings.Contains(got, longID) {
		t.Errorf("long id should have been truncated, but the full id survived: %q", got)
	}
}

// TestBuildApplicationNameBoundary checks the exact 63-byte boundary: a
// value that fits is left intact; one byte over is truncated to 63.
func TestBuildApplicationNameBoundary(t *testing.T) {
	const fixed = len("sluice/schema/") // 14 bytes of fixed structure

	idFit := strings.Repeat("a", maxAppNameBytes-fixed)
	if got := buildApplicationName(roleSchema, idFit); len(got) != maxAppNameBytes || !strings.HasSuffix(got, idFit) {
		t.Errorf("exact-fit id was altered: len=%d want=%d, got=%q", len(got), maxAppNameBytes, got)
	}

	idOver := strings.Repeat("a", maxAppNameBytes-fixed+1)
	if got := buildApplicationName(roleSchema, idOver); len(got) != maxAppNameBytes {
		t.Errorf("over-limit id not truncated to %d: len=%d, got=%q", maxAppNameBytes, len(got), got)
	}
}

// TestClampUTF8RuneBoundary ensures truncation never splits a multibyte
// rune, so the DSN handed to the driver stays valid UTF-8.
func TestClampUTF8RuneBoundary(t *testing.T) {
	s := strings.Repeat("é", 40) // 'é' is 2 bytes → 80 bytes total
	got := clampUTF8(s, 63)      // 63 is odd → must back off the half rune
	if !utf8.ValidString(got) {
		t.Errorf("clampUTF8 split a multibyte rune: %q is not valid UTF-8", got)
	}
	if len(got) > 63 {
		t.Errorf("clampUTF8 exceeded the byte budget: %d bytes", len(got))
	}
}
