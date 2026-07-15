// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package flatfile implements read-only sluice [ir.Engine]s for SCHEMA-LESS
// flat files — CSV, TSV, and NDJSON — registered as the `csv`, `tsv`, and
// `ndjson` source drivers (ADR-0163; roadmap item 55 Phase 2).
//
//	sluice migrate --source-driver csv --source ./users.csv --csv-header \
//	  --target-driver postgres --target '<pg-dsn>'
//
// # Stage-into-SQLite (schema-less input ONLY)
//
// These formats carry no types, so each Open* materializes the file into a
// temp SQLite database — every column declared TEXT, every value carried as
// its exact source text — and reads it through the sqlite engine's staged
// readers (the ADR-0130 materialize shape; the temp db is owned by the
// reader and removed on Close). The `migrate` CLI AUTO-ENGAGES
// `--infer-types` (ADR-0144) for these drivers, so name-hinted columns
// whose every value validates are promoted to richer target types
// (timestamp/timestamptz, jsonb, uuid); everything else lands as TEXT —
// lossless, byte-exact, and explicitly conservative. Staging typed dumps
// through SQLite remains FORBIDDEN (ADR-0161 non-goals): this path exists
// precisely because CSV/TSV/NDJSON have no types to degrade.
//
// # Explicit flags, never sniffed
//
// RFC 4180 has NO NULL representation — NULL-vs-empty-string is producer
// convention and the #1 silent-loss class for CSV ingest — so the
// convention is an explicit operator declaration, never a guess:
//
//   - `--csv-null REPR` declares the UNQUOTED field text that means SQL
//     NULL (`--csv-null='\N'`, `--csv-null=NULL`, or `--csv-null=”` for
//     the PostgreSQL-COPY-CSV empty-unquoted-field convention). A QUOTED
//     field is ALWAYS data — `"NULL"` is the four-character string — the
//     same bare-keyword-vs-quoted-string line the mydumper decoder draws.
//   - With NO --csv-null, an unquoted empty field is AMBIGUOUS and refused
//     loudly (SLUICE-E-CSV-NULL-AMBIGUOUS) naming the record and column;
//     a quoted empty field `""` is unambiguously the empty string.
//   - Header presence is declared with `--csv-header` / `--csv-no-header`;
//     opening a csv/tsv source with NEITHER is refused loudly
//     (SLUICE-E-CSV-HEADER-UNDECLARED) — a wrong guess silently eats a data
//     row or turns data into column names.
//   - Encoding is UTF-8, full stop: a UTF-16/32 BOM, invalid UTF-8 bytes,
//     or a NUL byte (the UTF-16-without-BOM tell) refuses loudly with a
//     transcode hint. A UTF-8 BOM is stripped (lossless) with a WARN.
//
// The `tsv` driver is the same engine with the delimiter fixed to TAB (the
// MySQL-flavor registration pattern); `--csv-delimiter` customises the
// `csv` driver only. NDJSON needs none of these flags and refuses them.
//
// # NDJSON value posture (the D1 2^53 lesson)
//
// JSON numbers are carried as their RAW TEXT end-to-end — never through a
// float64 — so int64 > 2^53, arbitrary-precision decimals, and big
// integers land byte-exact. Strings are JSON-decoded (\u escapes and all);
// true/false carry as the text `true`/`false`; null is SQL NULL; a nested
// object/array carries its raw JSON text verbatim. An ABSENT key and an
// explicit null both land as SQL NULL (the one representational collapse,
// documented in ADR-0163); a DUPLICATE key within one object is refused
// loudly rather than last-wins.
//
// # Capability shape
//
// A migrate SOURCE only — the d1/mydumper registry posture: CDC = CDCNone,
// BulkLoad = BulkLoadNone, every write/CDC/snapshot Open* returns a wrapped
// [ErrNotImplemented]. `sluice verify --depth count` works (the staged
// sqlite reader implements ir.Verifier; each open re-stages the file — the
// re-scan cost, same as the mydumper chunk re-scan). Sample depth is
// refused (documented limitation).
package flatfile

import (
	"context"
	"errors"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/sqlite"
	"sluicesync.dev/sluice/internal/ir"
)

// ErrNotImplemented is returned by the write/CDC/change-apply/snapshot
// Open* methods: a flat file is a migrate source only (the same posture as
// the sqlite/d1/mydumper engines). Check with [errors.Is].
var ErrNotImplemented = errors.New(
	"flatfile engine: not implemented (a csv/tsv/ndjson file is a migrate source only)",
)

// format selects the flat-file dialect an Engine instance reads.
type format int

const (
	formatCSV format = iota
	formatTSV
	formatNDJSON
)

// name returns the registry/driver name for the format.
func (f format) name() string {
	switch f {
	case formatTSV:
		return "tsv"
	case formatNDJSON:
		return "ndjson"
	default:
		return "csv"
	}
}

// Engine is the flat-file implementation of [ir.Engine]. One
// implementation, three registrations (csv/tsv/ndjson) — the MySQL-flavor
// pattern: same code, different name and per-format defaults. The DSN is
// the flat file's filesystem path. opts carries the operator's explicit
// --csv-* declarations, folded in via [Engine.WithFlatFileOptions]; the
// zero value (no declarations) is the safe default — the ambiguity
// refusals fire instead of anything being guessed.
type Engine struct {
	format format
	opts   Options
}

// Name returns the engine's CLI identifier (`--source-driver csv|tsv|ndjson`).
func (e Engine) Name() string { return e.format.name() }

// Options carries the operator's explicit flat-file declarations (the
// --csv-* flags). Fields distinguish "not declared" from "declared empty"
// because both refusal postures hang on that distinction.
type Options struct {
	// NullRepr is the unquoted-field text that denotes SQL NULL. nil =
	// undeclared (unquoted empty fields are refused as ambiguous); a
	// pointer to "" adopts the PG-COPY-CSV empty-unquoted-field convention.
	NullRepr *string

	// HeaderDeclared / Header carry the --csv-header / --csv-no-header
	// choice. HeaderDeclared=false means neither flag was passed (csv/tsv
	// opens refuse); Header is meaningful only when declared.
	HeaderDeclared bool
	Header         bool

	// Delimiter is the raw --csv-delimiter flag text ("" = not passed;
	// accepts a single ASCII character, or the spellings `\t` / `tab`).
	Delimiter string
}

// WithFlatFileOptions returns a copy of the engine carrying the operator's
// --csv-* declarations, validating them against the engine's format:
// NDJSON needs none of them and refuses each loudly; the tsv driver's
// delimiter is fixed to TAB. Mirrors the WithDateEncoding configured-copy
// pattern — the registry's engine value stays declaration-free.
func (e Engine) WithFlatFileOptions(o Options) (ir.Engine, error) {
	if e.format == formatNDJSON {
		switch {
		case o.NullRepr != nil:
			return nil, errors.New("ndjson: --csv-null does not apply (JSON null is the NULL representation)")
		case o.HeaderDeclared:
			return nil, errors.New("ndjson: --csv-header/--csv-no-header do not apply (object keys name the columns)")
		case o.Delimiter != "":
			return nil, errors.New("ndjson: --csv-delimiter does not apply")
		}
		e.opts = o
		return e, nil
	}
	// Validate the delimiter spelling up front (loud, before any file I/O).
	// The tsv driver is FIXED to tab: an explicit different delimiter is a
	// contradiction, not an override — the csv driver owns custom delimiters.
	if o.Delimiter != "" {
		d, err := parseDelimiter(o.Delimiter)
		if err != nil {
			return nil, err
		}
		if e.format == formatTSV && d != '\t' {
			return nil, errors.New(
				"tsv: the tsv driver is fixed to a TAB delimiter; for a different delimiter use " +
					"--source-driver csv --csv-delimiter='" + o.Delimiter + "'",
			)
		}
	}
	e.opts = o
	return e, nil
}

// Capabilities declares the honest source-only shape: a staged flat file
// presents SQLite-flavored TEXT columns (flat namespace, no CDC, never a
// target). Type richness comes later, from --infer-types promotions — not
// from engine capabilities.
func (Engine) Capabilities() ir.Capabilities { return capabilities }

// OpenSchemaReader stages the flat file into a temp SQLite database and
// returns the sqlite engine's staged SchemaReader over it (which owns the
// temp file, removes it on Close, and carries the ir.InferredTypeValidator
// surface --infer-types rides). The file is validated here — signature
// refusals (foreign dumps, wrong-driver inputs) and the explicit-flag
// refusals all fire before anything is staged.
func (e Engine) OpenSchemaReader(ctx context.Context, dsn string) (ir.SchemaReader, error) {
	staged, err := e.stage(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return sqlite.OpenStagedSchemaReader(ctx, staged, dsn)
}

// OpenRowReader stages the flat file (independently of OpenSchemaReader —
// each reader owns its own staged copy, the ADR-0130 dump posture) and
// returns the sqlite engine's staged RowReader over it.
func (e Engine) OpenRowReader(ctx context.Context, dsn string) (ir.RowReader, error) {
	staged, err := e.stage(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return sqlite.OpenStagedRowReader(ctx, staged, dsn)
}

// OpenSchemaWriter is not implemented: a flat file is a source only.
func (Engine) OpenSchemaWriter(context.Context, string) (ir.SchemaWriter, error) {
	return nil, ErrNotImplemented
}

// OpenRowWriter is not implemented: a flat file is a source only.
func (Engine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	return nil, ErrNotImplemented
}

// OpenCDCReader is not implemented: the engine declares CDCNone.
func (Engine) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	return nil, ErrNotImplemented
}

// OpenChangeApplier is not implemented: a flat file is a source only.
func (Engine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return nil, ErrNotImplemented
}

// OpenSnapshotStream is not implemented: with no CDC there is no
// snapshot→CDC handoff to capture.
func (Engine) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	return nil, ErrNotImplemented
}

// capabilities declares what these engines support: the staged file is
// SQLite-shaped TEXT (flat namespace, no extension types, no unsigned
// integers, no JSON type — a *_json column reaches jsonb only through the
// validated --infer-types promotion), and the operational surface is the
// source-only flat-file shape (no bulk-load path, no CDC).
var capabilities = ir.Capabilities{
	BulkLoad:                 ir.BulkLoadNone,
	CDC:                      ir.CDCNone,
	SchemaScope:              ir.SchemaScopeFlat,
	SupportedTypes:           ir.NewTypeSet(), // no extension types
	SupportsCheckConstraint:  false,
	SupportsGeneratedColumns: false,
	SupportsPartitioning:     false,
	EnumSupport:              ir.EnumNone,
	JSONSupport:              ir.JSONNone,
	UnsignedIntegers:         false,
	DDLDialect:               ir.DDLDialectANSI,
}

// init registers the three drivers. A blank import in cmd/sluice triggers
// this on binary startup (cli.go's named import serves the same purpose).
func init() {
	engines.Register(Engine{format: formatCSV})
	engines.Register(Engine{format: formatTSV})
	engines.Register(Engine{format: formatNDJSON})
}
