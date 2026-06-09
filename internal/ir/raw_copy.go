// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"context"
	"io"
)

// Raw-copy passthrough surfaces (ADR-0078, roadmap item 3b(b)).
//
// These are the OPTIONAL, type-asserted surfaces that let the
// orchestrator byte-pipe a same-engine bulk copy WITHOUT decoding
// every row into the typed IR and re-encoding it on the target — the
// per-stream throughput gap the pgcopydb comparison measured
// (`docs/comparison-pgcopydb.md`, "At scale"). They carry OPAQUE bytes
// only: a [RawCopyExporter] streams the source's native COPY-TO-STDOUT
// wire bytes into an [io.Writer], and a same-engine [RawCopyImporter]
// consumes those exact bytes via COPY-FROM-STDIN. NO [Row] is ever
// produced, so none of the IR's value-fidelity machinery (decode/
// re-encode, redaction, type-overrides, shard-injection) runs on this
// path.
//
// THAT is precisely why the path is correctness-gated by a single
// auditable predicate in the orchestrator (`rawCopyGate`): bypassing
// the IR means bypassing every value transform, so the lane engages
// ONLY when there is provably no transform to skip (same engine, no
// redaction, no type/expr override, no shard injection, identical
// per-table column projection). Any transform present → the
// orchestrator falls back to the IR copy path. See ADR-0078.
//
// Engines that don't implement these surfaces simply never take the
// fast lane; the base [RowReader] / [RowWriter] contracts are
// unchanged. The shipping implementation is Postgres (PG→PG); the
// surfaces are engine-neutral so any same-engine pair can opt in.

// RawCopyFormat is the on-the-wire COPY format both endpoints agree on
// for a raw-copy passthrough. The exporter writes its source bytes in
// this format and the importer reads them in the same format, so the
// two MUST match or the receiver mis-parses the stream.
type RawCopyFormat int

const (
	// RawCopyText is PostgreSQL's text COPY format. It is the default
	// because it is stable across server major versions (it is also
	// pgcopydb's default). The throughput win of the raw lane comes
	// from eliminating the per-value decode/re-encode, NOT from
	// text-vs-binary, so text is the safe baseline.
	RawCopyText RawCopyFormat = iota

	// RawCopyBinary is PostgreSQL's binary COPY format. Faster on the
	// wire but version/codec-sensitive across server majors, so it is
	// only ever selected when both endpoints' server major versions
	// match (the negotiator probes both). On a mismatch the negotiator
	// downgrades to text loudly (INFO), never silently.
	RawCopyBinary
)

// String renders the format as the SQL COPY clause token, so callers
// can build "... WITH (FORMAT text)" / "... WITH (FORMAT binary)"
// uniformly.
func (f RawCopyFormat) String() string {
	switch f {
	case RawCopyBinary:
		return "binary"
	default:
		return "text"
	}
}

// RawCopyChunk bounds a raw-copy export to a single PK range, mirroring
// the existing parallel-copy chunk boundaries (ADR-0019). A nil
// *RawCopyChunk means "the whole table" (the single-stream path).
//
// v1 supports a SINGLE integer PK column only — exactly the shape the
// existing chunk machinery already restricts itself to
// (`canParallelChunkTable`). LowerPK is EXCLUSIVE and UpperPK is
// INCLUSIVE, matching the (pk > $lo AND pk <= $hi) predicate the
// chunked IR path uses, so chunk boundaries cover the PK range without
// gap or overlap. A nil LowerPK means "from the start of the table"
// (chunk 0); a nil UpperPK means "to the end of the table" (last
// chunk).
type RawCopyChunk struct {
	// PKColumn is the single integer primary-key column the range is
	// expressed over.
	PKColumn string

	// LowerPK is the exclusive lower bound (pk > LowerPK). Nil => start
	// of table.
	LowerPK any

	// UpperPK is the inclusive upper bound (pk <= UpperPK). Nil => end
	// of table.
	UpperPK any
}

// RawCopyExporter is the OPTIONAL source-side surface for raw-copy
// passthrough. ExportRawCopy streams the table's (or chunk's) rows as
// the source engine's native COPY-TO-STDOUT wire bytes into w, in the
// negotiated format.
//
// CRITICAL projection invariant: the exporter MUST project exactly the
// source-readable columns (generated columns EXCLUDED — the same
// projection [RowReader.ReadRows]'s SELECT uses), via
// `COPY (SELECT <readable cols> ...) TO STDOUT`. It must NEVER emit a
// bare `COPY <table> TO STDOUT`, which would include generated columns
// and desync the importer's column list. The importer builds its
// `COPY <table> (<non-generated cols>) FROM STDIN` from the SAME column
// helper so the two column lists line up by construction.
type RawCopyExporter interface {
	RowReader
	RawCopyVersionProber

	ExportRawCopy(ctx context.Context, table *Table, chunk *RawCopyChunk, format RawCopyFormat, w io.Writer) error
}

// RawCopyImporter is the OPTIONAL target-side surface for raw-copy
// passthrough. ImportRawCopy consumes same-engine COPY wire bytes from
// r (in the negotiated format) via COPY-FROM-STDIN and returns the
// number of rows the server reported copied.
//
// Because a byte-pipe yields no per-row visibility, the orchestrator
// can only learn the row count at completion (the returned value);
// progress is incremented once per chunk/table, not per row.
type RawCopyImporter interface {
	RowWriter
	RawCopyVersionProber

	ImportRawCopy(ctx context.Context, table *Table, format RawCopyFormat, r io.Reader) (rowsCopied int64, err error)
}

// RawCopyVersionProber reports the engine server major version of an
// endpoint (e.g. 16 for PostgreSQL 16.x). The orchestrator probes BOTH
// endpoints (the exporter's source server and the importer's target
// server) when an operator opts into binary raw-copy, and engages the
// binary wire format ONLY when the two majors match — binary COPY is
// version/codec-sensitive across server majors, so a mismatch downgrades
// to text loudly (INFO), never silently. Comparing two ints keeps the
// negotiation engine-neutral: the orchestrator never names an engine.
//
// This is folded into both [RawCopyExporter] and [RawCopyImporter]
// because the negotiation needs a major from each side; an endpoint that
// can byte-pipe can always answer its own version.
type RawCopyVersionProber interface {
	ServerMajorVersion(ctx context.Context) (int, error)
}
