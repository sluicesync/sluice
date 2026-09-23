// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import "sluicesync.dev/sluice/internal/ir"

// unforwardedBaselineCarrier is the optional CDC-reader surface that moves
// the unforwarded-schema-change door's baseline (GC-2) from one reader to
// the next reader of the same run (GC-32).
//
// # Why it exists
//
// The door compares each table's constraints, policies, defaults and the
// like against a baseline the reader takes at StreamChanges, and refuses
// on a difference. An automatic retry after a transient error opens a
// FRESH reader, and a fresh baseline read then already contains any change
// made between the source DDL and the failure — so a dropped connection in
// that window accepted the change silently, the one thing the door exists
// to prevent. Carrying the previous reader's baseline across the retry
// leaves an operator restart as the only way to re-baseline, which is the
// documented, deliberate case (the refusal's remedy says so).
//
// The value is opaque here: the pipeline never looks inside it, it only
// hands one reader's baseline to the next. An engine given another
// engine's value ignores it and takes its own baseline.
//
// Implemented by the Postgres pgoutput reader and the MySQL-family binlog
// reader — the two lanes that carry the door. A reader without it (VStream,
// the trigger-CDC lanes) is skipped, as before.
type unforwardedBaselineCarrier interface {
	UnforwardedBaseline() any
	SetUnforwardedBaseline(b any)
}

// carryUnforwardedBaseline hands prev's baseline to next when both carry
// the door. prev must be closed (its pump joined) — both call sites hand
// over only after the previous attempt's reader is torn down — though the
// engines guard the snapshot regardless.
func carryUnforwardedBaseline(prev, next ir.CDCReader) {
	if prev == nil || next == nil {
		return
	}
	p, ok := prev.(unforwardedBaselineCarrier)
	if !ok {
		return
	}
	n, ok := next.(unforwardedBaselineCarrier)
	if !ok {
		return
	}
	if b := p.UnforwardedBaseline(); b != nil {
		n.SetUnforwardedBaseline(b)
	}
}

// wireUnforwardedBaseline gives r the baseline of the previous reader this
// Streamer opened in the current Run, then remembers r as the source for
// the next attempt. Called from [Streamer.wireReaderSchemaSeed], which
// every streamer reader-open site already calls (held by the seed roster),
// so a new open site cannot skip it without also skipping the seed.
func (s *Streamer) wireUnforwardedBaseline(r ir.CDCReader) {
	carryUnforwardedBaseline(s.unforwardedBaselineFrom, r)
	s.unforwardedBaselineFrom = r
}
