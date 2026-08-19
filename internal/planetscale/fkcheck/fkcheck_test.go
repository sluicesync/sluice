// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package fkcheck

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/planetscale/api"
)

// serveFKStatus wires an httptest server answering the two GETs the Checker
// makes — the database (foreign_keys_enabled) and the branch (safe_migrations) —
// and returns a Checker pointed at it.
func serveFKStatus(t *testing.T, fkEnabled, safeMigrations bool, branchStatus int) *Checker {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/organizations/acme/databases/app":
			body := `{"name":"app","foreign_keys_enabled":false}`
			if fkEnabled {
				body = `{"name":"app","foreign_keys_enabled":true}`
			}
			_, _ = w.Write([]byte(body))
		case strings.HasPrefix(r.URL.Path, "/v1/organizations/acme/databases/app/branches/"):
			if branchStatus != 0 {
				w.WriteHeader(branchStatus)
				_, _ = w.Write([]byte(`{"code":"x","message":"branch boom"}`))
				return
			}
			body := `{"name":"main","safe_migrations":false}`
			if safeMigrations {
				body = `{"name":"main","safe_migrations":true}`
			}
			_, _ = w.Write([]byte(body))
		default:
			t.Errorf("unexpected path %q", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return &Checker{
		API:      api.New(api.Config{TokenID: "id", Token: "secret", BaseURL: srv.URL}),
		Org:      "acme",
		Database: "app",
		// Branch left empty on purpose — the default "main" must be used.
	}
}

// TestChecker_ForeignKeyStatus combines the two reads and defaults the branch to
// "main". The matrix covers each (fk, safe-migrations) corner so a swapped field
// would be caught.
func TestChecker_ForeignKeyStatus(t *testing.T) {
	for _, tc := range []struct {
		name           string
		fkEnabled      bool
		safeMigrations bool
	}{
		{"disabled+safe-off", false, false},
		{"enabled+safe-off", true, false},
		{"enabled+safe-on", true, true},
		{"disabled+safe-on", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := serveFKStatus(t, tc.fkEnabled, tc.safeMigrations, 0).ForeignKeyStatus(context.Background())
			if err != nil {
				t.Fatalf("ForeignKeyStatus: %v", err)
			}
			if got.ForeignKeysEnabled != tc.fkEnabled {
				t.Errorf("ForeignKeysEnabled = %v; want %v", got.ForeignKeysEnabled, tc.fkEnabled)
			}
			if got.SafeMigrations != tc.safeMigrations {
				t.Errorf("SafeMigrations = %v; want %v", got.SafeMigrations, tc.safeMigrations)
			}
		})
	}
}

// TestChecker_ForeignKeyStatus_BranchErrorSurfaces confirms a failed branch read
// is returned (not swallowed as safe_migrations=false), so the pipeline takes
// its advisory-degrade WARN path rather than proceeding on a wrong "safe".
func TestChecker_ForeignKeyStatus_BranchErrorSurfaces(t *testing.T) {
	_, err := serveFKStatus(t, true, false, http.StatusInternalServerError).ForeignKeyStatus(context.Background())
	if err == nil {
		t.Fatal("want an error when the branch read fails")
	}
	if !strings.Contains(err.Error(), "safe-migrations") {
		t.Errorf("error = %q; want it to name the branch/safe-migrations read", err.Error())
	}
}
