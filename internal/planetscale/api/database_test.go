// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestClient_GetDatabase_ForeignKeysEnabled pins the database read the FK
// preflight rides: the path, and that foreign_keys_enabled decodes both ways
// (the field defaults false on a fresh database, so a wrong tag would silently
// always read "disabled" and the preflight would refuse a healthy target).
func TestClient_GetDatabase_ForeignKeysEnabled(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want bool
	}{
		{"enabled", `{"name":"app","foreign_keys_enabled":true}`, true},
		{"disabled", `{"name":"app","foreign_keys_enabled":false}`, false},
		{"absent field defaults false", `{"name":"app"}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var gotPath string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotPath = r.URL.Path
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()

			db, err := newTestClient(srv, nil).GetDatabase(context.Background(), "acme", "app")
			if err != nil {
				t.Fatalf("GetDatabase: %v", err)
			}
			if wantPath := "/v1/organizations/acme/databases/app"; gotPath != wantPath {
				t.Errorf("path = %q; want %q", gotPath, wantPath)
			}
			if db.ForeignKeysEnabled != tc.want {
				t.Errorf("ForeignKeysEnabled = %v; want %v", db.ForeignKeysEnabled, tc.want)
			}
		})
	}
}

// TestClient_GetDatabase_PropagatesStatusError confirms a non-2xx surfaces as
// the typed StatusError (so the preflight's advisory-degrade WARN path fires
// rather than a silent "enabled").
func TestClient_GetDatabase_PropagatesStatusError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"code":"forbidden","message":"nope"}`))
	}))
	defer srv.Close()

	_, err := newTestClient(srv, nil).GetDatabase(context.Background(), "acme", "app")
	if err == nil {
		t.Fatal("want an error on 403")
	}
	if !strings.Contains(err.Error(), "HTTP 403") {
		t.Errorf("error = %q; want it to name HTTP 403", err.Error())
	}
}
