//go:build d1verify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Live-D1 verification of the SQT-1 invalid-UTF-8 premise (the
// UNVERIFIED PREMISE the backlog filed 2026-08-12, measured and pinned
// 2026-08-13). Gated behind the d1verify build tag — the same
// live-credential suite shape as psverify/kmsverify.
//
// Usage from a shell with Cloudflare credentials in the environment
// (CLOUDFLARE_API_TOKEN + CLOUDFLARE_ACCOUNT_ID; on the dev machine
// they live in C:\cloudflare\cloudflare.env):
//
//	go test -tags=d1verify -v -count=1 -timeout=5m \
//	  -run 'TestD1Verify' ./internal/engines/sqlite/...
//
// The suite creates a THROWAWAY D1 database named
// sluice-d1verify-<unix-nanos> and deletes it on cleanup; it never
// touches any existing database. Tests skip cleanly when credentials
// aren't in the environment, so a run without secrets is a no-op
// rather than a failure.

package sqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// TestD1Verify_InvalidUTF8TextIsMangledServerSide pins the MEASURED
// resolution of the d1_decode.go premise: Cloudflare's D1 stores
// invalid-UTF-8 TEXT intact, but its /query API replaces every invalid
// byte with U+FFFD in the JSON response — SERVER-SIDE, before any
// client guard can see the original bytes. Three facts, each asserted:
//
//  1. Storage is intact: hex(x) returns the original bytes, so the
//     loss is in the response serialization, not the write (this is
//     also the anti-vacuity floor — it proves the poison landed).
//  2. The response cell arrives as VALID UTF-8 carrying replacement
//     characters, so jsonString's utf8.Valid refusal CANNOT fire on
//     the D1 lane for this vector — driven through jsonString itself.
//  3. The d1-trigger capture shape (json_object('v', CAST(x AS TEXT)),
//     the trigger_seam contract) STORES the raw bytes verbatim in the
//     change-log image, and reading that image back through the API
//     mangles identically — both D1 lanes converge on the same truth.
//
// If Cloudflare ever changes the serialization (verbatim bytes, an
// error, base64 — anything), this test fails and BOTH downstream
// artifacts must be revisited together: the d1_decode.go premise
// comment and the docs/operator/sqlite-d1-import.md caveat.
func TestD1Verify_InvalidUTF8TextIsMangledServerSide(t *testing.T) {
	token := os.Getenv("CLOUDFLARE_API_TOKEN")
	account := os.Getenv("CLOUDFLARE_ACCOUNT_ID")
	if token == "" || account == "" {
		t.Skip("CLOUDFLARE_API_TOKEN / CLOUDFLARE_ACCOUNT_ID not set; d1verify needs live credentials")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	dbID := createThrowawayD1Database(ctx, t, account, token)

	client, err := openD1Client("d1://" + account + "/" + dbID)
	if err != nil {
		t.Fatalf("openD1Client: %v", err)
	}

	mustExecD1 := func(sql string) {
		t.Helper()
		if _, err := client.queryRows(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	mustExecD1(`CREATE TABLE t (id INTEGER PRIMARY KEY, x TEXT)`)
	mustExecD1(`INSERT INTO t(id, x) VALUES (1, CAST(x'FFFE61' AS TEXT))`)

	rows, err := client.queryRows(ctx, `SELECT x, hex(x) AS hx, length(x) AS len FROM t WHERE id = 1`)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows; want 1", len(rows))
	}

	// Fact 1 — storage intact (anti-vacuity: the poison actually landed).
	if hx := string(rows[0]["hx"]); hx != `"FFFE61"` {
		t.Fatalf("hex(x) = %s; want \"FFFE61\" — the poison did not land as intended, nothing below measures the premise", hx)
	}
	if l := string(rows[0]["len"]); l != "3" {
		t.Fatalf("length(x) = %s; want 3", l)
	}

	// Fact 2 — the response cell is valid UTF-8 carrying U+FFFD U+FFFD 'a':
	// the server already replaced the bytes, so the client-side guard has
	// nothing invalid to refuse.
	raw := rows[0]["x"]
	want := `"` + "\uFFFD\uFFFD" + `a"`
	if string(raw) != want {
		t.Fatalf("raw x cell = %q (hex % x); want %q — D1's serialization of invalid-UTF-8 TEXT changed; revisit the d1_decode.go premise comment and the sqlite-d1-import.md caveat with this pin", raw, raw, want)
	}
	if !utf8.Valid(raw) {
		t.Fatalf("raw cell is INVALID UTF-8 (% x) — D1 now returns bytes verbatim; the jsonString guard would be load-bearing on this lane, the opposite of the documented premise", raw)
	}
	s, ok, jsErr := jsonString(raw)
	if jsErr != nil {
		t.Fatalf("jsonString refused the mangled cell: %v — the premise says the guard CANNOT fire here", jsErr)
	}
	if !ok || s != "\uFFFD\uFFFDa" {
		t.Fatalf("jsonString = (%q, %v); want the two-replacement mangle", s, ok)
	}

	// Fact 3 — the d1-trigger capture shape stores the raw bytes verbatim
	// ({"v":"<FF FE 61>"} = 7B2276223A22FFFE61227D), and reading the image
	// back through the API mangles identically.
	capRows, err := client.queryRows(ctx,
		`SELECT hex(json_object('v', CAST(x AS TEXT))) AS ch, json_object('v', CAST(x AS TEXT)) AS img FROM t WHERE id = 1`)
	if err != nil {
		t.Fatalf("capture-shape select: %v", err)
	}
	if ch := string(capRows[0]["ch"]); ch != `"7B2276223A22FFFE61227D"` {
		t.Fatalf("capture image stores %s; want the verbatim {\"v\":\"<FFFE61>\"} bytes — the capture stage now mangles/errors, which changes WHERE the loss happens on the trigger lane", ch)
	}
	if img := capRows[0]["img"]; !bytes.Contains(img, []byte("\uFFFD\uFFFD")) {
		t.Fatalf("capture image read back as %q; want the U+FFFD mangle — the read-side premise changed", img)
	}
}

// TestD1Verify_ExportDumpManglesInvalidUTF8Text pins the export half of
// the same premise (measured 2026-08-14): the /export REST endpoint —
// the endpoint `wrangler d1 export` drives — mangles invalid-UTF-8 TEXT
// exactly like /query does, replacing each invalid byte with U+FFFD in
// the generated .sql dump while `hex(x)` proves storage is intact. So
// the export path is NOT an escape hatch for this vector (it IS one for
// BLOBs, which the dump renders as x'..' literals): the only faithful
// readout of such a value is a server-side `hex(x)`.
//
// If this pin fails, D1's export generator changed serialization —
// rewrite the sqlite-d1-import.md invalid-UTF-8 section alongside it.
func TestD1Verify_ExportDumpManglesInvalidUTF8Text(t *testing.T) {
	token := os.Getenv("CLOUDFLARE_API_TOKEN")
	account := os.Getenv("CLOUDFLARE_ACCOUNT_ID")
	if token == "" || account == "" {
		t.Skip("CLOUDFLARE_API_TOKEN / CLOUDFLARE_ACCOUNT_ID not set; d1verify needs live credentials")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	dbID := createThrowawayD1Database(ctx, t, account, token)
	client, err := openD1Client("d1://" + account + "/" + dbID)
	if err != nil {
		t.Fatalf("openD1Client: %v", err)
	}
	for _, sql := range []string{
		`CREATE TABLE t (id INTEGER PRIMARY KEY, x TEXT)`,
		`INSERT INTO t(id, x) VALUES (1, CAST(x'FFFE61' AS TEXT))`,
	} {
		if _, err := client.queryRows(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	// Anti-vacuity: the poison landed intact before we export.
	rows, err := client.queryRows(ctx, `SELECT hex(x) AS hx FROM t WHERE id = 1`)
	if err != nil {
		t.Fatalf("hex select: %v", err)
	}
	if hx := string(rows[0]["hx"]); hx != `"FFFE61"` {
		t.Fatalf("hex(x) = %s; want \"FFFE61\" — the poison did not land, the export below measures nothing", hx)
	}

	dump := exportD1Dump(ctx, t, account, dbID, token)
	if !bytes.Contains(dump, []byte(`INSERT INTO "t"`)) {
		t.Fatalf("dump carries no INSERT for t — export produced an unexpected shape:\n%s", dump)
	}
	if bytes.Contains(dump, []byte{0xFF, 0xFE}) {
		t.Fatalf("dump carries the raw invalid bytes — D1's export generator now preserves invalid UTF-8; the sqlite-d1-import.md caveat overstates the loss, rewrite it with this pin")
	}
	if !bytes.Contains(dump, []byte("'��a'")) {
		t.Fatalf("dump lacks the expected '\\uFFFD\\uFFFDa' literal — D1's export serialization of invalid-UTF-8 TEXT changed; revisit the sqlite-d1-import.md caveat. Dump:\n%s", dump)
	}
}

// exportD1Dump drives the /export polling protocol (the same API
// wrangler d1 export uses) to completion and returns the dump bytes.
// The job can complete and evict between polls on a tiny database
// ("Not currently exporting anything."); that quirk is handled by
// restarting the export, which is idempotent for a quiesced DB.
func exportD1Dump(ctx context.Context, t *testing.T, account, dbID, token string) []byte {
	t.Helper()
	url := "https://api.cloudflare.com/client/v4/accounts/" + account + "/d1/database/" + dbID + "/export"

	type exportInner struct {
		AtBookmark string `json:"at_bookmark"`
		SignedURL  string `json:"signed_url"`
		Error      string `json:"error"`
		Result     *struct {
			SignedURL string `json:"signed_url"`
		} `json:"result"`
	}
	poll := func(body string) exportInner {
		t.Helper()
		raw, err := d1AdminRequest(ctx, http.MethodPost, url, token, body)
		if err != nil {
			t.Fatalf("export poll: %v", err)
		}
		var env struct {
			Success bool        `json:"success"`
			Result  exportInner `json:"result"`
		}
		if err := json.Unmarshal(raw, &env); err != nil || !env.Success {
			t.Fatalf("export poll response not usable (err=%v): %s", err, raw)
		}
		return env.Result
	}

	start := `{"output_format":"polling"}`
	r := poll(start)
	for i := 0; i < 60; i++ {
		signed := r.SignedURL
		if signed == "" && r.Result != nil {
			signed = r.Result.SignedURL
		}
		if signed != "" {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, signed, nil)
			if err != nil {
				t.Fatalf("dump request: %v", err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("dump download: %v", err)
			}
			defer resp.Body.Close()
			out := new(bytes.Buffer)
			if _, err := out.ReadFrom(resp.Body); err != nil {
				t.Fatalf("dump read: %v", err)
			}
			if resp.StatusCode/100 != 2 {
				t.Fatalf("dump download: HTTP %d: %s", resp.StatusCode, out.String())
			}
			return out.Bytes()
		}
		switch {
		case strings.Contains(r.Error, "Not currently exporting"):
			r = poll(start)
		case r.AtBookmark != "":
			time.Sleep(2 * time.Second)
			r = poll(`{"output_format":"polling","current_bookmark":"` + r.AtBookmark + `"}`)
		default:
			t.Fatalf("export poll made no progress: %+v", r)
		}
	}
	t.Fatal("export never produced a signed URL within the poll budget")
	return nil
}

// createThrowawayD1Database creates a uniquely-named D1 database via the
// REST API and registers its deletion on test cleanup. A failed delete
// is reported loudly (it costs nothing but clutters the account).
func createThrowawayD1Database(ctx context.Context, t *testing.T, account, token string) string {
	t.Helper()
	return createThrowawayD1DatabaseNamed(ctx, t, account, token, fmt.Sprintf("sluice-d1verify-%d", time.Now().UnixNano()))
}

// createThrowawayD1DatabaseNamed is [createThrowawayD1Database] with the
// caller's database name — for a test whose leaked database should be
// recognisable by name (e.g. the audit LA-1 replay's `la1-<nanos>`).
func createThrowawayD1DatabaseNamed(ctx context.Context, t *testing.T, account, token, name string) string {
	t.Helper()
	base := "https://api.cloudflare.com/client/v4/accounts/" + account + "/d1/database"

	body, err := d1AdminRequest(ctx, http.MethodPost, base, token, `{"name":"`+name+`"}`)
	if err != nil {
		t.Fatalf("create throwaway D1 database: %v", err)
	}
	var created struct {
		Success bool `json:"success"`
		Result  struct {
			UUID string `json:"uuid"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &created); err != nil || !created.Success || created.Result.UUID == "" {
		t.Fatalf("create response not usable (err=%v): %s", err, body)
	}
	dbID := created.Result.UUID

	t.Cleanup(func() {
		delCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := d1AdminRequest(delCtx, http.MethodDelete, base+"/"+dbID, token, ""); err != nil {
			t.Errorf("FAILED to delete throwaway D1 database %s (name %s) — delete it manually: %v", dbID, name, err)
		}
	})
	return dbID
}

// d1AdminRequest is the minimal REST helper for the create/delete
// lifecycle (the query path deliberately goes through sluice's own
// d1Client — that transport is what the premise is about).
func d1AdminRequest(ctx context.Context, method, url, token, body string) ([]byte, error) {
	var rdr *strings.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	} else {
		rdr = strings.NewReader("")
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rdr)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	out := new(bytes.Buffer)
	if _, err := out.ReadFrom(resp.Body); err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("%s %s: HTTP %d: %s", method, url, resp.StatusCode, out.String())
	}
	return out.Bytes(), nil
}
