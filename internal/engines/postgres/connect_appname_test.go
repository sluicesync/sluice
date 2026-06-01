// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"net/url"
	"strings"
	"testing"
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
		if want := "sluice/mystream/snapshot"; u.Query().Get("application_name") != want {
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
		if want := "application_name=sluice/mig1/applier"; !strings.Contains(got, want) {
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
		if want := "sluice/-/schema"; u.Query().Get("application_name") != want {
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
