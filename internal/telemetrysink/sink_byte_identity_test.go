// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The byte-identity family matrix (audit 2026-07-26 SL-16).
//
// The package doc promises "the bytes an HTTP push carries per record are the
// bytes the file line carries". That was FALSE for `<`, `>` and `&`: the file
// path encodes with SetEscapeHTML(false), while the HTTP path marshalled the
// assembled payload with the default escaping, which re-escapes those three
// characters inside the already-encoded record.
//
// The interesting part is not the defect — it is semantically lossless — but
// that a pin for exactly this existed and could not see it. Its fixture was
// `Branch: "café/dev"`: a non-ASCII shape that `json.Marshal` never re-escapes.
// Meanwhile a fixture carrying `a<b>&c` existed one file over and only ever ran
// the FILE path. One representative per surface, chosen without reference to
// what distinguishes the surfaces.
//
// So this drives the SAME corpus through BOTH sinks. The corpus is organised by
// what could make the two paths differ, not by what looks like a realistic
// branch name.
package telemetrysink

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// escapeCorpus is the family matrix: each entry is a string shape that some
// JSON encoder somewhere treats differently.
func escapeCorpus() map[string]string {
	return map[string]string{
		"html-escapable":      "a<b>&c",   // the defect's own shape
		"ampersand-only":      "x&y",      // & alone, the one people forget
		"angle-only":          "<script>", // both angles, no &
		"unicode-non-ascii":   "café/dev", // the OLD fixture — passes either way
		"line-separator":      "a b",      // U+2028: escaped by some encoders, not others
		"para-separator":      "a b",      // U+2029, its sibling
		"quote-and-backslash": `a"b\c`,    // ordinary JSON escapes, must agree
		"control-char":        "a\tb\nc",  // tab + newline inside a value
		"plain-ascii":         "main",     // the boring case must not regress
	}
}

// TestFileAndHTTPRecordBytesAreIdentical runs every corpus shape through both
// sinks and compares the per-record bytes.
func TestFileAndHTTPRecordBytesAreIdentical(t *testing.T) {
	for name, val := range escapeCorpus() {
		t.Run(name, func(t *testing.T) {
			rec := Record{
				At:       time.Unix(1000, 0).UTC(),
				Watch:    "metrics-watch:db",
				Database: "db",
				Branch:   val,
			}

			// The file path: EncodeRecord is the one encoder.
			fileLine, err := EncodeRecord(rec)
			if err != nil {
				t.Fatalf("EncodeRecord: %v", err)
			}
			fileBytes := bytes.TrimRight(fileLine, "\n")

			// The HTTP path: drive the REAL sink against a test server and
			// capture what it actually put on the wire. An earlier draft of
			// this test rebuilt the payload with its own encoder — which would
			// have kept passing if the sink regressed, since the test would
			// have been asserting against itself. Reimplementing production
			// inside a test is the same defect as pinning a function instead
			// of the path that reaches it.
			httpRecord := extractFirstRecord(t, captureSinkBody(t, rec))

			if !bytes.Equal(fileBytes, httpRecord) {
				t.Errorf("the two sinks disagree on this record's bytes, which the package doc says are identical:\n"+
					"  file: %s\n  http: %s\n\n"+
					"Both must encode with SetEscapeHTML(false); the HTTP path re-escaped `<`, `>` and `&` inside "+
					"the already-encoded record (audit SL-16).", fileBytes, httpRecord)
			}
		})
	}
}

// captureSinkBody runs the REAL HTTPSink against a test server and returns the
// body it posted.
func captureSinkBody(t *testing.T, rec Record) []byte {
	t.Helper()
	var got []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		got = b
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sink := &HTTPSink{URL: srv.URL}
	if err := sink.Write(context.Background(), []Record{rec}); err != nil {
		t.Fatalf("HTTPSink.Write: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("the sink posted an empty body")
	}
	return got
}

// extractFirstRecord pulls the first record's raw bytes back out of a push
// body, so the comparison is against what the wire carries.
func extractFirstRecord(t *testing.T, body []byte) []byte {
	t.Helper()
	var p struct {
		Records []json.RawMessage `json:"records"`
	}
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatalf("unmarshal push body: %v\nbody: %s", err, body)
	}
	if len(p.Records) != 1 {
		t.Fatalf("push body carries %d records, want 1", len(p.Records))
	}
	return p.Records[0]
}
