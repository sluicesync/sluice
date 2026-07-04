// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// This file is the minimal exported seam the sibling `d1-trigger` CDC engine
// (ADR-0136) reuses so the trigger setup DDL and the change-log poll run over
// the SAME validated D1 `/query` HTTP transport the cold-start `d1` reader uses
// (ADR-0132) — Phase 2 is transport substitution, not new CDC logic. The
// trigger logic itself (setup DDL, poll/decode, drift check, snapshot anchor)
// lives in the sqlite-trigger package and runs against a small executor
// interface; [D1Conn] is the transport that interface's D1 implementation wraps.

// D1Conn is an exported handle to the Cloudflare D1 HTTP query transport, for
// the `d1-trigger` CDC engine (ADR-0136). It wraps the same [d1Client] the
// cold-start `d1` reader uses, so setup DDL (CREATE TABLE/TRIGGER), the
// change-log poll, and the small catalog queries all run over the SAME
// `/query` endpoint and inherit its loud HTTP/envelope error surface. The
// cold-start schema/row reads still go through the `d1` engine's own readers
// ([NewD1Engine]); D1Conn is only the CDC/setup execution path.
//
// All queries use the DEFAULT primary (strongly-consistent) query path — D1's
// Sessions/read-replica routing is deliberately NOT used (ADR-0136 §4): the
// exactly-once `id > watermark` invariant rests on commit-order = id-order,
// which holds at the write-serialised primary but can wobble against a lagging
// replica. A replica-aware mode would have to re-introduce a safety-lag.
type D1Conn struct {
	c *d1Client
}

// OpenD1Conn parses a `d1://` DSN, resolves the env-only token + account id, and
// returns a [D1Conn]. A missing token, account, or database id is refused
// LOUDLY here, before any request — the same posture as the `d1` reader's
// [openD1Client]. Call [D1Conn.Ping] to verify reachability + credentials up
// front.
func OpenD1Conn(dsn string) (*D1Conn, error) {
	c, err := openD1Client(dsn)
	if err != nil {
		return nil, err
	}
	return &D1Conn{c: c}, nil
}

// Ping verifies the endpoint, account, database, and token are good before any
// setup or stream — so a credential problem fails at open, not mid-operation.
func (d *D1Conn) Ping(ctx context.Context) error { return d.c.ping(ctx) }

// Exec runs a non-SELECT statement (CREATE TABLE/TRIGGER, an INSERT for the
// meta/fingerprint rows) over the `/query` endpoint, discarding the (empty)
// result set. Any transport, HTTP-status, or statement-level failure is a loud
// error from [d1Client.queryRows].
func (d *D1Conn) Exec(ctx context.Context, sql string, params ...string) error {
	_, err := d.c.queryRows(ctx, sql, params...)
	return err
}

// Query runs a SELECT and returns the result rows as column-name → raw-JSON
// maps, exactly as the cold-start reader consumes them. The caller decodes each
// cell precisely (a JSON string vs number vs null). Bound params are sent as
// strings so a > 2^53 bound is not rounded through a JSON number (ADR-0132 §6 /
// ADR-0136 §3); SQLite applies the bound column's affinity to the text param.
func (d *D1Conn) Query(ctx context.Context, sql string, params ...string) ([]map[string]json.RawMessage, error) {
	return d.c.queryRows(ctx, sql, params...)
}

// CellDecoder returns a [CapturedCellDecoder] whose date/bool policy is the
// per-source `sqlite_date_encoding` DSN param parsed at [OpenD1Conn], else ISO
// (ADR-0129). NOTE (task 2.5): the trigger-CDC decoder does NOT receive the
// engine's --sqlite-date-encoding default — only the `d1`/`sqlite` migrate-source
// readers fold that per-instance default. A `d1-trigger` source that relied on
// the CLI default WITHOUT the DSN param now decodes as ISO (a loud storage-class
// refusal on mismatch, never silent-wrong); set the DSN param to carry it. Folding
// the engine default through the trigger backends is a scoped follow-up.
func (d *D1Conn) CellDecoder() *CapturedCellDecoder {
	return &CapturedCellDecoder{enc: d.c.dateEnc}
}

// D1ConnForTest constructs a [D1Conn] against an explicit endpoint base +
// credentials, bypassing the env-token resolution — for tests (incl. the
// `d1-trigger` CDC tests in the sqlite-trigger package) that point the transport
// at an httptest server. The endpoint base is injectable by design (ADR-0132 —
// "make the endpoint base injectable"). The date/bool policy is the inherit
// default (process-global, ISO); the encoding matrix itself is pinned by the
// shared decoder tests. NOT for production use — production callers use
// [OpenD1Conn] (env token, refuse-loudly).
func D1ConnForTest(endpointBase, accountID, databaseID, token string) *D1Conn {
	return &D1Conn{c: &d1Client{
		httpClient:   &http.Client{Timeout: 30 * time.Second},
		endpointBase: endpointBase,
		accountID:    accountID,
		databaseID:   databaseID,
		token:        token,
		dateEnc:      dateEncodingInherit,
	}}
}

// NewD1Engine returns the registered `d1` cold-start engine (the lossless
// HTTP-API reader, ADR-0132) as an [ir.Engine], so the `d1-trigger` CDC engine
// (ADR-0136) can compose it by delegation for OpenSchemaReader / OpenRowReader —
// mirroring how `sqlite-trigger` composes [Engine] for the file path. The zero
// value [d1Engine] holds no state.
func NewD1Engine() ir.Engine { return d1Engine{} }
