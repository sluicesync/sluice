// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestPrepareValue_ZeroTimeIsStringEncoded pins the go-sql-driver
// zero-time dodge (adversarial-corpus finding, 2026-08-22): Go's zero
// time.Time is a REAL IR value — PG DATE '0001-01-01' and timestamp
// '0001-01-01 00:00:00' decode to exactly time.Time{} — but the driver
// serializes any IsZero() time as MySQL's zero sentinel '0000-00-00'
// on both statement protocols. prepareValue must therefore hand the
// driver a string literal, never the zero time.Time itself.
//
// Reaches every MySQL write lane: prepareValue is the shared value
// seam for the batched-INSERT row writer, the CDC change applier
// (prepareApplierValue), and the LOAD DATA serializer (which calls
// prepareValue before tsvEncode) — pinned structurally by
// TestPrepareValueCallSiteRoster.
func TestPrepareValue_ZeroTimeIsStringEncoded(t *testing.T) {
	zero := time.Time{}
	dateCol := &ir.Column{Name: "d", Type: ir.Date{}}
	dtCol := &ir.Column{Name: "dt", Type: ir.DateTime{Precision: 6}}
	tsCol := &ir.Column{Name: "ts", Type: ir.Timestamp{Precision: 0, WithTimeZone: true}}
	domainDateCol := &ir.Column{Name: "dd", Type: ir.Domain{Name: "d_date", BaseType: ir.Date{}}}

	cases := []struct {
		name string
		col  *ir.Column
		want string
	}{
		{"date column", dateCol, "0001-01-01"},
		{"domain over date column", domainDateCol, "0001-01-01"},
		{"datetime column", dtCol, "0001-01-01 00:00:00"},
		{"timestamp column", tsCol, "0001-01-01 00:00:00"},
		// The applier's cache-miss path has no column descriptor; the
		// datetime form is safe there (MySQL truncates the zero
		// time-of-day into a DATE column).
		{"nil column descriptor", nil, "0001-01-01 00:00:00"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := prepareValue(zero, c.col)
			if err != nil {
				t.Fatalf("prepareValue(zero time): %v", err)
			}
			s, ok := got.(string)
			if !ok {
				t.Fatalf("prepareValue(zero time) = %T(%v); want a string literal — a time.Time reaching the driver is rewritten to '0000-00-00'", got, got)
			}
			if s != c.want {
				t.Errorf("prepareValue(zero time) = %q; want %q", s, c.want)
			}
		})
	}

	// A NON-zero time.Time must keep passing through as time.Time —
	// the driver serializes real instants correctly, and re-encoding
	// them as strings would forfeit the binary protocol.
	notZero := time.Date(1969, 12, 31, 23, 59, 59, 0, time.UTC)
	got, err := prepareValue(notZero, dtCol)
	if err != nil {
		t.Fatalf("prepareValue(non-zero time): %v", err)
	}
	if _, ok := got.(time.Time); !ok {
		t.Errorf("prepareValue(non-zero time) = %T; want time.Time passthrough", got)
	}
}
