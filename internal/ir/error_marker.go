// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

// MarkedError attaches an engine-neutral classification marker to an
// engine-specific error WITHOUT changing the operator-facing message.
//
// It exists for the case where the underlying error already says the thing the
// marker names — a Postgres 55006 renders as
// `replication slot "s" is active for PID 42 (SQLSTATE 55006)`, so appending
// ": slot is active" to reach errors.Is would make the message worse to read in
// order to make it possible to classify. [WithMarker] keeps the text exactly as
// the engine wrote it and adds the marker on the Unwrap path only.
//
// Both the wrapped error and the marker satisfy errors.Is/errors.As, via the
// multi-error Unwrap form.
type MarkedError struct {
	err    error
	marker error
}

// Error returns the wrapped error's message verbatim — the marker is
// classification, not prose.
func (m *MarkedError) Error() string { return m.err.Error() }

// Unwrap exposes both the original error and the marker.
func (m *MarkedError) Unwrap() []error { return []error{m.err, m.marker} }

// WithMarker returns err classified by marker. It returns nil when err is nil,
// and err unchanged when marker is nil, so callers can wrap unconditionally.
func WithMarker(err, marker error) error {
	if err == nil {
		return nil
	}
	if marker == nil {
		return err
	}
	return &MarkedError{err: err, marker: marker}
}
