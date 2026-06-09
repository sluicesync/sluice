// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// PG→PG (any same-engine) raw-copy passthrough orchestration
// (ADR-0078, roadmap item 3b(b)).
//
// The IR copy path decodes every row into an ir.Row and re-encodes it
// on the target — the price of engine-neutral generality (cross-engine,
// redaction, type-overrides, value-fidelity). For a same-engine,
// no-transform copy that price buys nothing: the source's COPY-TO-STDOUT
// bytes are exactly the bytes the target's COPY-FROM-STDIN wants. This
// file wires a same-engine fast lane that byte-pipes them through an
// io.Pipe under an errgroup, when — and ONLY when — a single auditable
// value-fidelity gate proves there is no transform to skip.
//
// The lane stays engine-neutral: the orchestrator NEVER names an engine.
// It engages only when m.Source.Name() == m.Target.Name(), both the
// reader and writer type-assert to the optional [ir.RawCopyExporter] /
// [ir.RawCopyImporter] surfaces, AND [rawCopyGate] holds. Postgres is
// the shipping implementation; any same-engine pair that implements the
// surfaces opts in for free.

package pipeline

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"golang.org/x/sync/errgroup"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/redact"
)

// rawCopyTakenObserver is a test-only seam. When non-nil it is invoked
// with the table name every time a raw-copy chunk/table is byte-piped,
// so integration tests can assert the fast lane was actually TAKEN (a
// green zero-loss test alone can't distinguish the raw lane from the IR
// fallback). Mirrors ADR-0077's onTableCopiedObserver disposition.
var rawCopyTakenObserver func(table string)

// rawCopyConfig is the SHAREABLE transform-config the raw-copy gate
// reasons over, decoupled from any one orchestrator (ADR-0079). Both
// [Migrator] (migrate) and [Streamer] (sync cold-start) populate it from
// their own fields so the SINGLE auditable value-fidelity predicate
// ([rawCopyGate]) governs BOTH paths identically — the raw lane can never
// silently skip a transform on either. The fields are exactly the
// value-affecting transform knobs; a new transform that the byte-pipe
// would bypass MUST be added here (and gated below) or it is a silent-loss
// hole.
type rawCopyConfig struct {
	// sourceEngine / targetEngine are the engine names. Equal names are
	// the same-engine precondition (G1); the gate never spells an engine
	// out (IR-first tenet).
	sourceEngine string
	targetEngine string

	// redactor is the configured PII redaction policy (nil or Empty() ==
	// no redaction). The raw lane never sees a Row, so any active rule is
	// a transform it would skip (G2).
	redactor *redact.Registry

	// mappings / exprMappings are the --type-override / --expr-override
	// IR rewrites (G3); shard is the --inject-shard-column discriminator
	// stamp (G4). Any present forces the IR path.
	mappings     []config.Mapping
	exprMappings []config.ExpressionMapping
	shard        ShardColumnSpec
}

// rawCopyConfigForMigrator projects a [Migrator]'s transform configuration
// onto the shared [rawCopyConfig]. Keeping the projection in one place
// makes the migrate-vs-sync parity auditable: the two callers differ only
// in which struct they read from, never in what the gate checks.
func rawCopyConfigForMigrator(m *Migrator) rawCopyConfig {
	cfg := rawCopyConfig{
		redactor:     m.Redactor,
		mappings:     m.Mappings,
		exprMappings: m.ExpressionMappings,
		shard:        m.InjectShardColumn,
	}
	if m.Source != nil {
		cfg.sourceEngine = m.Source.Name()
	}
	if m.Target != nil {
		cfg.targetEngine = m.Target.Name()
	}
	return cfg
}

// rawCopyConfigForStreamer is the sync cold-start twin of
// [rawCopyConfigForMigrator] (ADR-0079): it projects a [Streamer]'s
// transform configuration onto the SAME [rawCopyConfig] the migrate path
// uses, so the raw-copy lane on the sync path is governed by the identical
// gate. The Streamer holds every transform field under a slightly different
// name; reading them here (not in the gate) keeps the predicate
// engine-/orchestrator-neutral and the parity auditable in one spot.
func rawCopyConfigForStreamer(s *Streamer) rawCopyConfig {
	cfg := rawCopyConfig{
		redactor:     s.Redactor,
		mappings:     s.Mappings,
		exprMappings: s.ExpressionMappings,
		shard:        s.InjectShardColumn,
	}
	if s.Source != nil {
		cfg.sourceEngine = s.Source.Name()
	}
	if s.Target != nil {
		cfg.targetEngine = s.Target.Name()
	}
	return cfg
}

// rawCopyGate is the SINGLE auditable value-fidelity predicate for the
// raw-copy passthrough lane. It is the load-bearing correctness surface:
// the raw lane bypasses the typed IR, so it bypasses EVERY value
// transform — a gate miss would silently skip a redaction / type-override
// / shard-stamp, a silent-loss class. So the gate is conservative by
// construction: it returns ok=true only when there is provably no
// transform to skip, and any single transform present returns
// (false, reason). It is checked ONCE per run at setup (after the
// IR-mutation steps ApplyMappings / ApplyExpressionOverrides /
// InjectShardColumn) and the result is threaded as a bool into the copy
// path; per-table identity is re-checked separately by
// [identityProjection] so one odd table falls back without disabling the
// lane.
//
// Pure: no I/O, no state mutation — directly unit-testable. It reasons
// over the engine-neutral [rawCopyConfig] so both migrate and sync route
// through the identical predicate (ADR-0079).
func rawCopyGate(cfg rawCopyConfig) (ok bool, reason string) {
	// G1 — same engine. Cross-engine copies MUST go through the IR (the
	// whole point of sluice over pgcopydb); a byte-pipe between different
	// engines would ship one engine's wire format to another. An empty
	// engine name (nil Source/Target) can never match a real one, so this
	// also refuses a misconfigured caller.
	if cfg.sourceEngine == "" || cfg.targetEngine == "" || cfg.sourceEngine != cfg.targetEngine {
		return false, "cross-engine (raw copy is same-engine only)"
	}
	// G2 — no redaction. The raw lane never sees a Row, so a redaction
	// rule would be silently skipped.
	if cfg.redactor != nil && !cfg.redactor.Empty() {
		return false, "redaction configured"
	}
	// G3 — no type/expr override. ApplyMappings / ApplyExpressionOverrides
	// rewrite the IR type/expression; byte-piping the source bytes would
	// ignore the override.
	if len(cfg.mappings) > 0 {
		return false, "type override (--type-override) configured"
	}
	if len(cfg.exprMappings) > 0 {
		return false, "expression override (--expr-override) configured"
	}
	// G4 — no shard-column injection. Shape A stamps a discriminator value
	// onto every row between read and write; the byte-pipe has no stamp
	// point.
	if cfg.shard.Engaged() {
		return false, "shard-column injection (--inject-shard-column) configured"
	}
	return true, ""
}

// identityProjection re-checks, per table, that the raw-copy lane is
// safe for THIS table (G6). It is invoked at per-table dispatch so a
// single OID/format-sensitive table falls back to the IR path without
// disabling the lane for the whole run.
//
// On a same-engine, no-transform run the source-readable projection
// (generated + SluiceInjected columns excluded) and the target's
// non-generated column list are derived from the SAME *ir.Table, so the
// names/order/wire-types match by construction — the gate (G2–G4)
// already excludes the only producer of a SluiceInjected column
// (shard-injection). What remains is the v1 CONSERVATIVE exclusion: any
// column whose IR type is OID- or wire-format-sensitive
// (ExtensionType / VerbatimType / Bit / Geometry) routes the table to
// the IR path, where the per-type codec machinery (e.g. the pgvector /
// hstore COPY codecs in row_writer.go) runs. Start strict; widen on
// evidence.
//
// ir.Array is also excluded — NOT because the raw byte-pipe is unsafe for
// arrays (the COPY stream preserves dimensionality perfectly, unlike the
// pgx-codec IR path that needed the Bug 73/74 loud-refusal), but because
// the array element-family matrix is exactly the "pin the class, not the
// representative" surface (Bug 74): proving the byte-pipe correct across
// every element family x shape is a separate evidence exercise. Routing
// arrays to the IR path keeps the existing Bug-73 loud-refusal /
// array-codec contract intact in v1; widen with the full element-family
// pin matrix later.
func identityProjection(table *ir.Table) bool {
	if table == nil {
		return false
	}
	for _, c := range table.Columns {
		switch c.Type.(type) {
		case ir.ExtensionType, ir.VerbatimType, ir.Bit, ir.Geometry, ir.Array:
			return false
		}
	}
	return true
}

// negotiateRawCopyFormat resolves the wire format the raw lane will use.
// requested is the operator's --raw-copy-format intent. Text is always
// safe (cross-major, pgcopydb's default) and is the floor; binary is
// engaged only when requested AND both endpoints' server majors match
// (binary COPY is version/codec-sensitive). On a probe failure or a
// major mismatch the format downgrades to text LOUDLY (INFO), never
// silently — the loud-failure tenet.
func negotiateRawCopyFormat(ctx context.Context, requested ir.RawCopyFormat, exp ir.RawCopyExporter, imp ir.RawCopyImporter) ir.RawCopyFormat {
	if requested != ir.RawCopyBinary {
		return ir.RawCopyText
	}
	srcMajor, serr := exp.ServerMajorVersion(ctx)
	dstMajor, derr := imp.ServerMajorVersion(ctx)
	if serr != nil || derr != nil {
		slog.InfoContext(ctx, "raw-copy: server-version probe failed; using text format",
			slog.Any("source_err", serr),
			slog.Any("target_err", derr))
		return ir.RawCopyText
	}
	if srcMajor != dstMajor {
		slog.InfoContext(ctx, "raw-copy: server majors differ; downgrading binary request to text",
			slog.Int("source_major", srcMajor),
			slog.Int("target_major", dstMajor))
		return ir.RawCopyText
	}
	slog.InfoContext(ctx, "raw-copy: binary format negotiated",
		slog.Int("server_major", srcMajor))
	return ir.RawCopyBinary
}

// runRawCopyChunk byte-pipes one table (chunk == nil) or one PK-bounded
// chunk from the exporter to the importer through an io.Pipe under an
// errgroup. The exporter writes the source's COPY-TO-STDOUT bytes into
// the pipe writer in one goroutine; the importer reads them from the
// pipe reader via COPY-FROM-STDIN in another. The exporter closes the
// write end on completion (signalling EOF to the reader); an importer
// error is propagated back to the exporter via pr.CloseWithError so a
// failed import unblocks a still-writing exporter instead of deadlocking.
//
// Returns the server-reported row count from the import side (a byte-pipe
// has no per-row visibility, so progress is incremented once per chunk).
func runRawCopyChunk(ctx context.Context, exp ir.RawCopyExporter, imp ir.RawCopyImporter, table *ir.Table, chunk *ir.RawCopyChunk, format ir.RawCopyFormat) (int64, error) {
	pr, pw := io.Pipe()

	var rowsCopied int64
	g, gctx := errgroup.WithContext(ctx)

	// Importer: drains the pipe reader. On error, close the reader with
	// the error so the exporter's CopyTo write returns instead of
	// blocking on a full pipe.
	g.Go(func() error {
		n, err := imp.ImportRawCopy(gctx, table, format, pr)
		if err != nil {
			_ = pr.CloseWithError(err)
			return err
		}
		rowsCopied = n
		// Fully drain so the exporter's writes always complete; CopyFrom
		// returns only after EOF, which the exporter signals via pw.Close.
		_ = pr.Close()
		return nil
	})

	// Exporter: streams source bytes into the pipe writer. Close the
	// write end on completion so the importer sees EOF; close-with-error
	// on failure so the importer's read returns the cause.
	g.Go(func() error {
		err := exp.ExportRawCopy(gctx, table, chunk, format, pw)
		if err != nil {
			_ = pw.CloseWithError(err)
			return err
		}
		return pw.Close()
	})

	if err := g.Wait(); err != nil {
		return 0, fmt.Errorf("raw copy %q: %w", table.Name, err)
	}
	if rawCopyTakenObserver != nil {
		rawCopyTakenObserver(table.Name)
	}
	return rowsCopied, nil
}

// asRawCopyEndpoints type-asserts a reader/writer pair to the raw-copy
// surfaces. Returns (exporter, importer, true) only when BOTH sides
// implement their respective surface — the orchestrator never byte-pipes
// half a pair.
func asRawCopyEndpoints(rr ir.RowReader, rw ir.RowWriter) (ir.RawCopyExporter, ir.RawCopyImporter, bool) {
	exp, eok := rr.(ir.RawCopyExporter)
	if !eok {
		return nil, nil, false
	}
	imp, iok := rw.(ir.RawCopyImporter)
	if !iok {
		return nil, nil, false
	}
	return exp, imp, true
}
