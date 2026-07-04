// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The `d1` engine (ADR-0132) is a SECOND migrate source registered by this
// package alongside `sqlite`: it reads a LIVE Cloudflare D1 database over D1's
// HTTP query API instead of a local file. It exists because D1's two DEFAULT
// extraction paths — `wrangler d1 export` AND the default (bare-JSON) query API
// — BOTH round integers larger than 2^53 through a JS/JSON double (a snowflake
// ID `9007199254740993` comes back `…992`; max int64 off by ~1,193). The loss
// is server-side, before sluice runs — undetectable on those paths. The query
// API reading each value via `CAST(col AS TEXT)` + `typeof(col)` is the ONLY
// lossless path: integers come back as their EXACT decimal text (parsed to
// int64 with no rounding), and `typeof` recovers INTEGER-vs-REAL (which the
// default JSON collapses) and the storage class for blob/null handling.
//
// It reuses this package's validated type resolution ([resolveColumnType], the
// affinity rules) and the ADR-0129 declared date/bool encoding policy: the
// (typeof, text/hex) pair is reconstructed into the SAME Go storage-class value
// the modernc file path produces (int64/float64/string/[]byte/nil), then handed
// to the shared [decodeCell] — so the D1 path inherits the file engine's full
// loud-failure storage-class fidelity for free. The ONLY genuinely new surface
// is the net/http transport, the CAST/typeof projection, and keyset pagination.
//
// It is a migrate SOURCE only (Capabilities.CDC = CDCNone); the write/CDC/
// snapshot Open* return a wrapped [ErrD1NotImplemented]. Reads do NOT take D1
// offline (only `export` does), so this reader is also operationally gentler
// than the export path for a live database.
//
//	sluice migrate --source-driver d1 --source d1://<account_id>/<database_id> \
//	  --target-driver postgres --target '<pg-dsn>'
//
// Secrets posture (ADR-0132 §3): the API token is ENV-ONLY
// (CLOUDFLARE_API_TOKEN) — never a flag, never logged. The account id may come
// from the DSN (`d1://<account_id>/<database_id>`) or the env
// (CLOUDFLARE_ACCOUNT_ID, with `d1://<database_id>`). A missing token, account,
// or database id is refused LOUDLY at Open*, before any HTTP request.

package sqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
)

// ErrD1NotImplemented is returned by the d1 engine's write/CDC/snapshot Open*
// methods: D1 is a migrate SOURCE only (the same posture as the file engine's
// [ErrNotImplemented]). Callers should check for it with [errors.Is].
var ErrD1NotImplemented = errors.New("d1 engine: not implemented (Cloudflare D1 is a migrate source only)")

// Environment variables carrying the D1 credentials. The token is env-ONLY by
// design (never a CLI flag) so it does not land in shell history, process
// listings, or logs; the account id is env-OPTIONAL (the DSN can carry it).
const (
	envD1Token   = "CLOUDFLARE_API_TOKEN"
	envD1Account = "CLOUDFLARE_ACCOUNT_ID"
)

// defaultD1EndpointBase is the Cloudflare API v4 root. It is a field on
// [d1Client] (not a hard-coded literal at the call site) so tests can point the
// client at an httptest.Server (ADR-0132: "make the endpoint base injectable").
const defaultD1EndpointBase = "https://api.cloudflare.com/client/v4"

// d1MaxResponseBytes bounds how much of a D1 response body is read.
// Cloudflare caps D1 query responses at ~1 MiB, so 8 MiB is generous for
// every legitimate response while an unbounded io.ReadAll against a
// misdirected endpoint can't exhaust memory.
const d1MaxResponseBytes = 8 << 20

// d1Engine is the Cloudflare D1 implementation of [ir.Engine]. Like the file
// engine it holds no connection state; the zero value is usable. It shares this
// package's [capabilities] (a migrate-source shape: CDCNone, no extension
// types) — D1 is SQLite, so the declared shape is identical.
type d1Engine struct {
	// dateEncoding carries the operator's --sqlite-date-encoding default
	// (ADR-0129), set via [d1Engine.WithDateEncoding] — the per-instance
	// replacement for the former SetDefaultDateEncoding global (task 2.5). The
	// zero value dateEncodingInherit resolves to ISO, so a bare NewD1Engine()
	// reads temporal columns as ISO-8601 text exactly as before.
	dateEncoding dateEncoding
}

// Name returns the engine's CLI identifier (`--source-driver d1`).
func (d1Engine) Name() string { return "d1" }

// WithDateEncoding returns a copy of the engine carrying the operator's
// --sqlite-date-encoding default (ADR-0129; task 2.5). The per-source
// `sqlite_date_encoding` DSN param still wins over this default. An empty string
// keeps the iso default. Mirrors [Engine.WithDateEncoding].
func (d d1Engine) WithDateEncoding(enc string) (ir.Engine, error) {
	e, err := parseDateEncoding(enc)
	if err != nil {
		return nil, fmt.Errorf("d1: invalid --sqlite-date-encoding %q (%w)", enc, err)
	}
	d.dateEncoding = e
	return d, nil
}

// Capabilities reuses the file engine's migrate-source capability declaration:
// D1 is SQLite over HTTP, so the honest shape (no CDC, no extension types, flat
// namespace, never a target) is identical.
func (d1Engine) Capabilities() ir.Capabilities { return capabilities }

// OpenSchemaReader returns a [D1SchemaReader] bound to the live D1 database
// named by dsn. Credentials are resolved here (token from env, account/db from
// the DSN or env) and refused loudly if missing — before any HTTP request.
func (d1Engine) OpenSchemaReader(ctx context.Context, dsn string) (ir.SchemaReader, error) {
	client, err := openD1Client(dsn)
	if err != nil {
		return nil, err
	}
	// Verify reachability + credentials up front so a bad token/account fails at
	// open (naming nothing secret), not mid-migration on the first table read.
	if err := client.ping(ctx); err != nil {
		return nil, err
	}
	return &D1SchemaReader{client: client}, nil
}

// OpenRowReader returns a [D1RowReader] bound to the live D1 database named by
// dsn. The per-source date encoding (`sqlite_date_encoding` DSN param, or the
// engine --sqlite-date-encoding default folded at OpenRowReader — ADR-0129 / task 2.5) is resolved here and carried for decode,
// exactly as the file engine's OpenRowReader does.
func (d d1Engine) OpenRowReader(ctx context.Context, dsn string) (ir.RowReader, error) {
	client, err := openD1Client(dsn)
	if err != nil {
		return nil, err
	}
	if err := client.ping(ctx); err != nil {
		return nil, err
	}
	// The per-source DSN param wins; absent, the engine's --sqlite-date-encoding
	// default applies (task 2.5). Both may be inherit → decode resolves to ISO.
	return &D1RowReader{client: client, dateEnc: foldDateEncoding(client.dateEnc, d.dateEncoding)}, nil
}

// OpenSchemaWriter is not implemented: D1 is a migrate source only.
func (d1Engine) OpenSchemaWriter(context.Context, string) (ir.SchemaWriter, error) {
	return nil, ErrD1NotImplemented
}

// OpenRowWriter is not implemented: D1 is a migrate source only.
func (d1Engine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	return nil, ErrD1NotImplemented
}

// OpenCDCReader is not implemented: D1 declares CDCNone (no CDC source).
func (d1Engine) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	return nil, ErrD1NotImplemented
}

// OpenChangeApplier is not implemented: D1 is a migrate source only.
func (d1Engine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return nil, ErrD1NotImplemented
}

// OpenSnapshotStream is not implemented: D1 has no CDC, so there is no
// snapshot→CDC handoff to capture.
func (d1Engine) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	return nil, ErrD1NotImplemented
}

// d1Client posts SQL to the D1 HTTP query API and parses the envelope. It is
// the only HTTP surface in the engine; the schema and row readers run their SQL
// through [d1Client.queryRows]. endpointBase is injectable for tests.
type d1Client struct {
	httpClient   *http.Client
	endpointBase string
	accountID    string
	databaseID   string
	token        string

	// dateEnc is the per-source temporal encoding resolved from the DSN
	// (ADR-0129); dateEncodingInherit is folded to the engine default at OpenRowReader (task 2.5), else ISO.
	dateEnc dateEncoding
}

// openD1Client parses a d1:// DSN, resolves the env-only token and the account
// id (DSN or env), and constructs a [d1Client] with the default endpoint base
// and a bounded HTTP client. A missing token, account, or database id is a LOUD
// refusal here, before any request — the token message never echoes the value.
func openD1Client(dsn string) (*d1Client, error) {
	accountID, databaseID, enc, err := parseD1DSN(dsn)
	if err != nil {
		return nil, err
	}

	// The account id may be supplied by the DSN OR the env; the DSN wins.
	if accountID == "" {
		accountID = strings.TrimSpace(os.Getenv(envD1Account))
	}
	if accountID == "" {
		return nil, fmt.Errorf(
			"d1: no account id (provide it in the DSN as d1://<account_id>/<database_id> "+
				"or set %s)", envD1Account,
		)
	}

	// The token is env-ONLY (never a flag); refuse loudly if absent. The error
	// names the env var, never the (absent) value.
	token := strings.TrimSpace(os.Getenv(envD1Token))
	if token == "" {
		return nil, fmt.Errorf("d1: %s is not set (the D1 API token is read from the "+
			"environment only, never a flag)", envD1Token)
	}

	return &d1Client{
		httpClient:   &http.Client{Timeout: 60 * time.Second},
		endpointBase: defaultD1EndpointBase,
		accountID:    accountID,
		databaseID:   databaseID,
		token:        token,
		dateEnc:      enc,
	}, nil
}

// parseD1DSN parses the two accepted DSN forms and the optional per-source date
// encoding param (ADR-0129), mirroring the file engine's
// [stripDateEncodingParam] handling:
//
//	d1://<account_id>/<database_id>   — account + database from the DSN
//	d1://<database_id>                — database only; account from the env
//
// Either form accepts a `?sqlite_date_encoding=…` query param. The token is
// NEVER in the DSN (env-only). An empty DSN, a missing database id, or an
// invalid date-encoding value is refused loudly.
func parseD1DSN(dsn string) (accountID, databaseID string, enc dateEncoding, err error) {
	if strings.TrimSpace(dsn) == "" {
		return "", "", dateEncodingInherit, errors.New("d1: DSN is empty")
	}

	clean, encRaw, present := stripDateEncodingParam(dsn)
	enc = dateEncodingInherit
	if present {
		enc, err = parseDateEncoding(encRaw)
		if err != nil {
			return "", "", dateEncodingInherit,
				fmt.Errorf("d1: invalid %s DSN param %q (%w)", dsnDateEncodingParam, encRaw, err)
		}
	}

	if !strings.HasPrefix(clean, "d1://") {
		return "", "", dateEncodingInherit,
			fmt.Errorf("d1: DSN %q must start with d1:// (d1://<account_id>/<database_id> "+
				"or d1://<database_id>)", dsn)
	}
	body := strings.TrimPrefix(clean, "d1://")
	body = strings.Trim(body, "/")
	if body == "" {
		return "", "", dateEncodingInherit,
			fmt.Errorf("d1: DSN %q has no database id", dsn)
	}

	if acct, db, ok := strings.Cut(body, "/"); ok {
		accountID = strings.TrimSpace(acct)
		databaseID = strings.TrimSpace(db)
	} else {
		databaseID = strings.TrimSpace(body)
	}
	if databaseID == "" {
		return "", "", dateEncodingInherit,
			fmt.Errorf("d1: DSN %q has no database id", dsn)
	}
	return accountID, databaseID, enc, nil
}

// queryURL builds the D1 query endpoint for this client's account + database.
// Both ids are path-escaped (mirroring the PlanetScale telemetry client's
// posture) so a DSN id carrying `/`, `?`, or `#` cannot re-point the request
// at a different API path — a malformed id yields a Cloudflare "no such
// account/database" error, loudly, instead of a silently redirected call.
func (c *d1Client) queryURL() string {
	return c.endpointBase + "/accounts/" + url.PathEscape(c.accountID) +
		"/d1/database/" + url.PathEscape(c.databaseID) + "/query"
}

// d1Row is one result row: the column name → its RAW JSON value, so the caller
// decodes each cell precisely (a JSON string vs number vs null) rather than
// through a lossy interface{} round-trip. The DATA path reads value columns as
// JSON strings (CAST AS TEXT / hex) or null; the catalog (PRAGMA) path reads
// small scalars as plain JSON.
type d1Row = map[string]json.RawMessage

// d1QueryResult is one entry of the envelope's `result` array.
type d1QueryResult struct {
	Results []d1Row `json:"results"`
	Success bool    `json:"success"`
}

// d1Envelope is the D1 query-API response envelope. A non-empty Errors, a false
// Success, or a non-2xx HTTP status is a loud error (the D1 error text is
// surfaced verbatim).
type d1Envelope struct {
	Result   []d1QueryResult `json:"result"`
	Errors   []d1Message     `json:"errors"`
	Messages []d1Message     `json:"messages"`
	Success  bool            `json:"success"`
}

// d1Message is one Cloudflare API message/error (`{code, message}`).
type d1Message struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// d1RequestBody is the POST body: the SQL plus optional positional params. The
// keyset bound is sent via params as a STRING so a > 2^53 bound value is NOT
// rounded through a JSON number, and SQLite applies the bound column's affinity
// to the text param (ADR-0132 §6).
type d1RequestBody struct {
	SQL    string   `json:"sql"`
	Params []string `json:"params,omitempty"`
}

// queryRows posts sql (with optional positional params) to the D1 query API and
// returns the first statement's result rows. Any transport, HTTP-status,
// envelope-level, or statement-level failure is a LOUD error naming the D1
// error text — never a silent empty result (an empty result set is a valid,
// distinct outcome and returns nil, nil).
func (c *d1Client) queryRows(ctx context.Context, sql string, params ...string) ([]d1Row, error) {
	reqBody, err := json.Marshal(d1RequestBody{SQL: sql, Params: params})
	if err != nil {
		return nil, fmt.Errorf("d1: marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.queryURL(), bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("d1: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("d1: query database %q: %w", c.databaseID, err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Bound the read: Cloudflare caps a D1 response at ~1 MiB, so 8 MiB is
	// generous headroom while a misdirected/hostile endpoint can't balloon
	// memory (the same cap the PlanetScale telemetry client uses).
	body, err := io.ReadAll(io.LimitReader(resp.Body, d1MaxResponseBytes))
	if err != nil {
		return nil, fmt.Errorf("d1: read response from database %q: %w", c.databaseID, err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("d1: query database %q failed: HTTP %d: %s",
			c.databaseID, resp.StatusCode, truncateForError(body))
	}

	var env d1Envelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("d1: decode response from database %q: %w (body: %s)",
			c.databaseID, err, truncateForError(body))
	}
	if !env.Success || len(env.Errors) > 0 {
		return nil, fmt.Errorf("d1: query database %q refused: %s",
			c.databaseID, formatD1Errors(env.Errors))
	}
	if len(env.Result) == 0 {
		return nil, fmt.Errorf("d1: query database %q returned no result block", c.databaseID)
	}
	if !env.Result[0].Success {
		return nil, fmt.Errorf("d1: query database %q: statement reported failure", c.databaseID)
	}
	return env.Result[0].Results, nil
}

// ping issues a trivial query to verify the endpoint, account, database, and
// token are good before any schema/row read — so a credential problem fails at
// Open* (per ADR-0132's "refused loudly at open"), not mid-migration.
func (c *d1Client) ping(ctx context.Context) error {
	_, err := c.queryRows(ctx, "SELECT 1")
	return err
}

// formatD1Errors renders the envelope's errors[] for a loud message. An empty
// list (defensive — the caller only calls this when success is false) still
// yields a non-empty string so the operator is never told "refused: ".
func formatD1Errors(errs []d1Message) string {
	if len(errs) == 0 {
		return "success=false with no error detail"
	}
	parts := make([]string, len(errs))
	for i, e := range errs {
		parts[i] = fmt.Sprintf("[%d] %s", e.Code, e.Message)
	}
	return strings.Join(parts, "; ")
}

// truncateForError bounds a response body echoed in an error so a giant or
// binary body does not flood the log.
func truncateForError(body []byte) string {
	const limit = 256
	s := strings.TrimSpace(string(body))
	if len(s) > limit {
		return s[:limit] + "…"
	}
	return s
}

// init registers the d1 engine alongside the file engine. A blank import of
// this package in cmd/sluice triggers both registrations.
func init() {
	engines.Register(d1Engine{})
}
