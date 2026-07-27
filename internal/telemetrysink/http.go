// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package telemetrysink

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"sluicesync.dev/sluice/internal/safeerr"
)

// defaultHTTPTimeout bounds one push. A sample push is advisory; it must
// never block the poll loop for long, so the per-request timeout is short
// (the same posture notify's sinks take).
const defaultHTTPTimeout = 10 * time.Second

// HTTPSink POSTs each poll tick's samples as one JSON document to a generic
// endpoint, so an operator can land the fleet's telemetry in their own
// service / TSDB ingest / Lambda without a file on the watch host.
type HTTPSink struct {
	// URL is the POST target. Treated as a credential (its path may embed
	// one) — set via env, never logged; transport errors are stripped of the
	// URL before they reach a log line.
	URL string

	// HTTPClient is optional; nil ⇒ a client with [defaultHTTPTimeout].
	HTTPClient *http.Client
}

// Compile-time proof the HTTP sink satisfies the seam.
var _ Sink = (*HTTPSink)(nil)

// httpPayload is the documented push body: a batch envelope around the SAME
// per-record encoding the file sink writes. Carrying pre-encoded
// [json.RawMessage] rather than re-marshalling the structs is what makes
// that byte-identity structural instead of coincidental — there is exactly
// one record codec, so a validation refusal cannot apply to one sink and not
// the other.
type httpPayload struct {
	Records []json.RawMessage `json:"records"`
}

// Name implements [Sink].
func (h *HTTPSink) Name() string { return "http" }

// Write POSTs recs as one JSON document. A non-2xx response is an error.
// ctx bounds the request in addition to the client timeout.
func (h *HTTPSink) Write(ctx context.Context, recs []Record) error {
	if len(recs) == 0 {
		return nil
	}
	var (
		payload httpPayload
		errs    []error
	)
	for _, r := range recs {
		line, err := EncodeRecord(r)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		payload.Records = append(payload.Records, json.RawMessage(bytes.TrimRight(line, "\n")))
	}
	if len(payload.Records) == 0 {
		return errors.Join(errs...)
	}
	if err := h.post(ctx, payload); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// Close implements [Sink]; the HTTP sink holds no resources.
func (h *HTTPSink) Close() error { return nil }

// post marshals and delivers the batch.
//
// URL-leak wart (audit N-12, same as notify's sinks): the URL IS a
// credential, and both url.Parse (inside NewRequest) and client.Do wrap
// failures in *url.Error whose Error() embeds the full URL. The caller
// WARN-logs err.Error(), so every error below passes through
// [safeerr.SafeParseError] to strip the wrapper.
func (h *HTTPSink) post(ctx context.Context, payload httpPayload) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("telemetrysink: marshal push batch: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.URL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("telemetrysink: build push request: %w", safeerr.SafeParseError(err))
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := h.client().Do(req)
	if err != nil {
		return fmt.Errorf("telemetrysink: push failed: %w", safeerr.SafeParseError(err))
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("telemetrysink: push returned HTTP %d: %s", resp.StatusCode, bytes.TrimSpace(snippet))
	}
	// Drain so the connection can be reused.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	return nil
}

func (h *HTTPSink) client() *http.Client {
	if h.HTTPClient != nil {
		return h.HTTPClient
	}
	return &http.Client{Timeout: defaultHTTPTimeout}
}
