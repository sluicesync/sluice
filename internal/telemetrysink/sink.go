// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package telemetrysink is the engine-neutral PERSISTENT-SAMPLE sink layer
// for `sluice metrics-watch` (roadmap item 75c). Where `internal/notify`
// delivers edge-triggered THRESHOLD ALERTS, this package durably records
// EVERY polled sample, so "collect my fleet's telemetry and keep it for my
// own portal" no longer requires standing up a Prometheus to scrape
// `--metrics-listen`.
//
// Two sinks ship: a rotating local JSONL file ([FileSink]) and a generic
// JSON HTTP push ([HTTPSink]); [MultiSink] fans out to both with FAILURE
// ISOLATION. Like the notify layer, this is an OPTIONAL, advisory
// integration surface: the caller logs and swallows every error returned
// here, so a full disk or a dead endpoint can never stall or fail a poll.
//
// Deliberately standalone: this package imports NO engine package and NOT
// `internal/ir` — the caller maps its telemetry view into a plain [Record],
// exactly the posture `internal/notify` keeps.
//
// # The record is a CODEC
//
// A [Record] round-trips through a store, so it gets the full codec
// treatment (CLAUDE.md "new-surface pre-land checklist"). [EncodeRecord] is
// the ONE encoder for both sinks — the bytes an HTTP push carries per record
// are the bytes the file line carries — and it REFUSES, loudly and by field
// name, anything `encoding/json` would silently mangle:
//
//   - a string that is not valid UTF-8 (encoding/json substitutes U+FFFD
//     silently — the resume-cursor CRITICAL's mechanism);
//   - a non-finite float (NaN / ±Inf), which has no JSON spelling.
//
// Integers are emitted as JSON number literals with their exact decimal
// digits, so an int64 beyond 2^53 survives the write intact; a consumer must
// decode with a 64-bit integer type (or `json.Decoder.UseNumber`) rather than
// the `any`/float64 default, which is where the precision would be lost.
// That contract is pinned by the package's round-trip tests.
package telemetrysink

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"
	"unicode/utf8"
)

// Record is ONE persisted telemetry sample for ONE watched target.
//
// Every metric is a POINTER: an unobserved metric serializes as an explicit
// JSON `null`, never a misleading `0`. That is the persisted form of the
// snapshot's (value, *Known) honesty contract — a portal reading these rows
// can always tell "the provider did not observe this" from "the provider
// observed zero". The fields deliberately carry NO `omitempty`: an absent
// key and a null key would otherwise be indistinguishable, and a
// schema-on-read consumer benefits from every key being present in every
// row.
//
// The schema is additive-only by design: a future metric (the roadmap-75a
// egress figures, say) arrives as further nullable fields, so an existing
// consumer keeps parsing older rows unchanged.
type Record struct {
	// At is the poll wall-clock (the moment the watch recorded this row),
	// normalised to UTC. SampledAt is the underlying provider poll's stamp —
	// null when no sample has ever landed for this target.
	At        time.Time  `json:"at"`
	SampledAt *time.Time `json:"sampled_at"`

	// Watch is the operator-facing label of the watch that produced the row
	// (the `metrics-watch:<db>` / `metrics-watch:<org>` identity); Database
	// and Branch identify the target. In single-database mode Branch is the
	// resolved branch ("main" unless overridden).
	Watch    string `json:"watch"`
	Database string `json:"database"`
	Branch   string `json:"branch"`

	// Fresh is the (snap, ok) verdict for this row: false means the provider
	// had no usable sample at this tick, and every metric below is null.
	Fresh bool `json:"fresh"`

	CPUUtil               *float64 `json:"cpu_util"`
	MemUtil               *float64 `json:"mem_util"`
	StorageUtil           *float64 `json:"storage_util"`
	StorageAvailableBytes *int64   `json:"storage_available_bytes"`
	StorageCapacityBytes  *int64   `json:"storage_capacity_bytes"`
	ReplicaLagSeconds     *float64 `json:"replica_lag_seconds"`
	ActiveConnections     *int64   `json:"active_connections"`
	MaxConnections        *int64   `json:"max_connections"`
}

// Sink is one persistent-sample destination. Write persists a batch (one
// poll tick's worth of records, which is one record in single-database mode
// and one per target in org-wide mode). It MUST honour ctx and bound its own
// I/O so a wedged sink can't stall the poll loop. Close releases resources.
type Sink interface {
	Write(ctx context.Context, recs []Record) error
	Name() string
	Close() error
}

// MultiSink fans one batch out to every configured sink. Its Write NEVER
// short-circuits: a failing sink cannot suppress delivery to a healthy one
// (the same load-bearing failure-isolation contract notify.MultiNotifier
// carries). It returns a joined error; the caller logs and swallows it.
type MultiSink []Sink

// NewMultiSink builds a MultiSink from the supplied sinks, dropping nil
// entries. Returns nil when no non-nil sink remains, so a caller's
// "sink == nil ⇒ persistence off" guard stays exact.
func NewMultiSink(sinks ...Sink) MultiSink {
	var out MultiSink
	for _, s := range sinks {
		if s != nil {
			out = append(out, s)
		}
	}
	return out
}

// Write persists recs to every sink, collecting errors without
// short-circuiting. A nil/empty MultiSink is a no-op.
func (m MultiSink) Write(ctx context.Context, recs []Record) error {
	var errs []error
	for _, s := range m {
		if s == nil {
			continue
		}
		if err := s.Write(ctx, recs); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", s.Name(), err))
		}
	}
	return errors.Join(errs...)
}

// Name reports the aggregate sink name.
func (m MultiSink) Name() string { return "multi" }

// Close closes every sink, collecting errors without short-circuiting.
func (m MultiSink) Close() error {
	var errs []error
	for _, s := range m {
		if s == nil {
			continue
		}
		if err := s.Close(); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", s.Name(), err))
		}
	}
	return errors.Join(errs...)
}

// EncodeRecord validates r and marshals it into ONE JSONL line, terminated
// by a single "\n". It is the sole encoder for every sink, so the bytes an
// HTTP push carries for a record are byte-identical to the file line.
//
// It refuses (rather than silently mangles) the two shapes `encoding/json`
// would quietly rewrite — see the package doc. The error names the offending
// field AND the row's target, because the caller logs-and-swallows it: a
// dropped row must still tell the operator exactly which row and why.
func EncodeRecord(r Record) ([]byte, error) {
	for _, f := range []struct {
		name  string
		value string
	}{
		{"watch", r.Watch},
		{"database", r.Database},
		{"branch", r.Branch},
	} {
		if !utf8.ValidString(f.value) {
			return nil, fmt.Errorf(
				"telemetrysink: record field %q for target %s/%s is not valid UTF-8 (%q) — refusing to write a row encoding/json would silently rewrite with U+FFFD",
				f.name, r.Database, r.Branch, f.value,
			)
		}
	}
	for _, f := range []struct {
		name  string
		value *float64
	}{
		{"cpu_util", r.CPUUtil},
		{"mem_util", r.MemUtil},
		{"storage_util", r.StorageUtil},
		{"replica_lag_seconds", r.ReplicaLagSeconds},
	} {
		if f.value == nil {
			continue
		}
		if math.IsNaN(*f.value) || math.IsInf(*f.value, 0) {
			return nil, fmt.Errorf(
				"telemetrysink: record field %q for target %s/%s is non-finite (%v) — JSON has no spelling for NaN/±Inf, refusing the row",
				f.name, r.Database, r.Branch, *f.value,
			)
		}
	}

	// Normalise to UTC so rows from differently-zoned processes compare and
	// sort as written text, not just as parsed instants.
	r.At = r.At.UTC()
	if r.SampledAt != nil {
		utc := r.SampledAt.UTC()
		r.SampledAt = &utc
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	// Keep `<`, `>`, `&` literal: the escaped form round-trips identically,
	// but a metrics row is meant to be read by humans and grep as well as by
	// a parser.
	enc.SetEscapeHTML(false)
	if err := enc.Encode(r); err != nil {
		return nil, fmt.Errorf("telemetrysink: encode record for target %s/%s: %w", r.Database, r.Branch, err)
	}
	// json.Encoder already terminates with exactly one "\n" — the JSONL
	// framing — so the buffer IS the line.
	return buf.Bytes(), nil
}
