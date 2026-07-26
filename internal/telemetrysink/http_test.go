// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package telemetrysink

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// TestHTTPSink_PushesTheBatchEnvelope pins the documented push shape: one
// POST per tick carrying a {"records":[…]} envelope with one entry per row.
func TestHTTPSink_PushesTheBatchEnvelope(t *testing.T) {
	var (
		hits atomic.Int32
		body atomic.Value
		ctyp atomic.Value
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		raw, _ := io.ReadAll(r.Body)
		body.Store(raw)
		ctyp.Store(r.Header.Get("Content-Type"))
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sink := &HTTPSink{URL: srv.URL}
	if err := sink.Write(t.Context(), []Record{sampleRec("alpha", 0.1), sampleRec("beta", 0.2)}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("want exactly one POST per tick, got %d", got)
	}
	if got, _ := ctyp.Load().(string); got != "application/json" {
		t.Errorf("content type: want application/json, got %q", got)
	}
	var payload struct {
		Records []Record `json:"records"`
	}
	raw, _ := body.Load().([]byte)
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode push body %s: %v", raw, err)
	}
	if len(payload.Records) != 2 {
		t.Fatalf("want 2 records in the envelope, got %d", len(payload.Records))
	}
	if payload.Records[0].Database != "alpha" || payload.Records[1].Database != "beta" {
		t.Errorf("record order/content lost: %+v", payload.Records)
	}
}

// TestHTTPSink_RecordBytesMatchTheFileLine pins the ONE-codec property: the
// bytes an HTTP push carries for a record are byte-identical to the file
// sink's line (minus the JSONL terminator). Structural, not coincidental —
// both sinks call EncodeRecord — so a validation refusal can never apply to
// one sink and not the other.
func TestHTTPSink_RecordBytesMatchTheFileLine(t *testing.T) {
	var body atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		body.Store(raw)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	rec := sampleRec("alpha", 0.42)
	rec.StorageCapacityBytes = i64((1 << 53) + 1)
	rec.Branch = "café/dev"

	path := filepath.Join(t.TempDir(), "metrics.jsonl")
	fs, err := NewFileSink(FileConfig{Path: path})
	if err != nil {
		t.Fatalf("NewFileSink: %v", err)
	}
	defer func() { _ = fs.Close() }()
	if err := fs.Write(t.Context(), []Record{rec}); err != nil {
		t.Fatalf("file write: %v", err)
	}
	sink := &HTTPSink{URL: srv.URL}
	if err := sink.Write(t.Context(), []Record{rec}); err != nil {
		t.Fatalf("http write: %v", err)
	}

	fileLine := strings.TrimRight(readLines(t, path)[0], "\n")
	var payload struct {
		Records []json.RawMessage `json:"records"`
	}
	raw, _ := body.Load().([]byte)
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode push body: %v", err)
	}
	if got := string(payload.Records[0]); got != fileLine {
		t.Fatalf("push record bytes must equal the file line:\n file: %s\n push: %s", fileLine, got)
	}
	if !bytes.Contains(raw, []byte("9007199254740993")) {
		t.Errorf("the >2^53 integer must ride the wire as exact digits: %s", raw)
	}
}

// TestHTTPSink_NonSuccessIsAnError pins that a dead endpoint surfaces an
// error (which the caller logs and swallows) rather than passing silently.
func TestHTTPSink_NonSuccessIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer srv.Close()

	sink := &HTTPSink{URL: srv.URL}
	err := sink.Write(t.Context(), []Record{sampleRec("alpha", 0.1)})
	if err == nil {
		t.Fatal("a non-2xx push must be an error")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("error should carry the status: %v", err)
	}
}

// TestHTTPSink_AllRecordsRefusedSkipsThePost pins that a batch whose every
// row the codec refused never issues a POST with an empty envelope.
func TestHTTPSink_AllRecordsRefusedSkipsThePost(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sink := &HTTPSink{URL: srv.URL}
	err := sink.Write(t.Context(), []Record{sampleRec(string([]byte{0xff}), 0.1)})
	if err == nil {
		t.Fatal("the refused row must surface an error")
	}
	if got := hits.Load(); got != 0 {
		t.Errorf("no POST should be issued when every row was refused, got %d", got)
	}
}
