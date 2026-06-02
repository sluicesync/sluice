// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Multi-source aggregation v0.25.0 (ADR-0031): `--target-schema` flag
// + stream-id collision detection helpers. Lives in its own file
// because both the Migrator and Streamer paths thread the same
// helpers — keeping them next to each other makes the contract
// readable.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/orware/sluice/internal/ir"
)

// validateTargetSchema enforces the engine-capability gate for
// `--target-schema`. Engines whose schema scope is namespaced (PG)
// accept the override; engines with a flat namespace (MySQL) refuse
// with a clear message naming the DSN-choice workaround.
//
// Empty TargetSchema is a no-op — the orchestrator field defaults to
// the empty string, which preserves today's "use the DSN's schema"
// behaviour.
func validateTargetSchema(target ir.Engine, targetSchema string) error {
	if targetSchema == "" {
		return nil
	}
	if target == nil {
		return nil // validate() catches the nil engine separately
	}
	if target.Capabilities().SchemaScope == ir.SchemaScopeNamespaced {
		return nil
	}
	return fmt.Errorf(
		"pipeline: --target-schema is not supported on engine %q "+
			"(MySQL has no schema concept distinct from databases; "+
			"use a different --target DSN to namespace per-source "+
			"streams, e.g. --target=mysql://...:3306/customer_svc). "+
			"Multi-source --target-schema is PG-only in this release; "+
			"see docs/adr/adr-0031-multi-source-aggregation-target-schema.md",
		target.Name(),
	)
}

// applyTargetSchema threads an operator-supplied schema-name override
// to a freshly-opened engine reader/writer/applier via the optional
// [ir.SchemaSetter] surface. Engines that don't implement the setter
// are silently passed through — the validate gate has already refused
// the field for non-namespaced engines, so any engine that reaches
// this call with a non-empty targetSchema is expected to honour it.
//
// No-op when targetSchema is empty (today's default behaviour).
func applyTargetSchema(target any, targetSchema string) {
	if targetSchema == "" {
		return
	}
	if setter, ok := target.(ir.SchemaSetter); ok {
		setter.SetSchema(targetSchema)
	}
}

// applyIndexBuildMem threads the operator's `--index-build-mem` value
// (a per-build maintenance_work_mem in bytes; 0 = auto) to a freshly-
// opened target [ir.SchemaWriter] via the optional [ir.IndexBuildTuner]
// surface, before CreateIndexes runs. Engines that don't implement the
// tuner (today: MySQL) skip cleanly — the PG writer auto-tunes from a
// pg_settings probe regardless, so the flag only ever overrides the
// auto value on a PG target.
//
// Called unconditionally (even when bytes == 0): the PG writer treats
// 0 as the auto sentinel and still runs the dominant maintenance_work_mem
// auto-tune. That keeps the speedup on by default without a separate
// per-command opt-in. See docs/dev/notes/index-build-phase-tuning.md.
func applyIndexBuildMem(target any, bytes int64) {
	if tuner, ok := target.(ir.IndexBuildTuner); ok {
		tuner.SetIndexBuildMem(bytes)
	}
}

// applyIndexBuildParallelism threads the operator's
// `--index-build-parallelism` value (the concurrent index-build worker
// count; 0 = auto) to a freshly-opened target [ir.SchemaWriter] via the
// optional [ir.IndexBuildTuner] surface, before CreateIndexes runs
// (Phase B). Engines that don't implement the tuner (today: MySQL) skip
// cleanly. Called unconditionally (even when n == 0): the PG writer
// treats 0 as the auto sentinel and derives a conservative concurrency
// from the memory + connection budgets, so concurrent index builds are
// on by default without a per-command opt-in. See
// docs/dev/notes/index-build-phase-tuning.md.
func applyIndexBuildParallelism(target any, n int) {
	if tuner, ok := target.(ir.IndexBuildTuner); ok {
		tuner.SetIndexBuildParallelism(n)
	}
}

// applyEnabledPGExtensions threads the operator's
// `--enable-pg-extension` allowlist (ADR-0032) through to a freshly-
// opened engine reader / writer / applier via the optional
// [ir.ExtensionAware] surface. Engines that don't implement the
// interface (today: MySQL) skip cleanly — the validate gate
// upstream already refused the flag for non-PG sides via the
// engine-name check in [validateEnabledPGExtensions].
//
// Returns the error from [ir.ExtensionAware.EnableExtensions] when
// the engine refuses (unknown extension name, missing on the
// connected database). Empty / nil extensions is a no-op.
func applyEnabledPGExtensions(ctx context.Context, target any, extensions []string) error {
	if len(extensions) == 0 {
		return nil
	}
	if aware, ok := target.(ir.ExtensionAware); ok {
		return aware.EnableExtensions(ctx, extensions)
	}
	// Engine doesn't expose ExtensionAware. The validate gate normally
	// catches this upstream; defending here keeps the helper safe to
	// call against any opened reader/writer.
	return nil
}

// applyVerbatimExtensionPassthrough threads the ADR-0047 verbatim
// passthrough decision to a freshly-opened engine reader / writer via
// the optional [ir.VerbatimExtensionAware] surface. Engines that don't
// implement it (today: MySQL) skip cleanly.
//
// The orchestrator is the determination authority and stays
// engine-neutral: it passes a boolean computed purely from engine
// *names* (never importing an engine package). enabled MUST be true
// only when the run provably does not need semantic type
// understanding for uncatalogued extension types:
//
//   - live PG → PG: source engine name == target engine name ==
//     "postgres" (see [verbatimLiveSameEnginePG]); or
//   - a PG backup: the source is PG and the restore-target engine is
//     unknown at backup time, so verbatim columns are recorded on the
//     lineage segment and a loud restore-time engine gate enforces
//     PG-restore-only.
//
// Cross-engine and non-PG runs pass enabled=false (or never call
// this), preserving ADR-0047 tier (c): the existing loud refusal for
// uncatalogued user-defined types is unchanged.
func applyVerbatimExtensionPassthrough(target any, enabled bool) {
	if !enabled {
		return
	}
	if aware, ok := target.(ir.VerbatimExtensionAware); ok {
		aware.SetVerbatimExtensionPassthrough(true)
	}
}

// applyTableScope threads the operator's table filter to a freshly-
// opened source [ir.SchemaReader] via the optional [ir.TableScoper]
// surface, so per-column type validation is scoped to the
// to-be-migrated tables (catalog Bug 76). Engines that don't implement
// TableScoper (today: MySQL) skip cleanly — the authoritative
// post-read [applyTableFilter] still prunes the schema there; only the
// Bug-76 usability gap (a scoped-out unsupported column aborting the
// run) remains until that engine grows the same push-down.
//
// An empty filter is still threaded: the predicate then admits every
// table, which is exactly the unscoped behaviour, so this is safe to
// call unconditionally. The filter passed here MUST already have
// engine-default exclusions merged (effectiveTableFilter) so the
// push-down matches the post-read prune.
func applyTableScope(reader any, filter TableFilter) {
	scoper, ok := reader.(ir.TableScoper)
	if !ok {
		return
	}
	if filter.IsEmpty() {
		scoper.SetTableScope(nil)
		return
	}
	scoper.SetTableScope(filter.Allows)
}

// verbatimLiveSameEnginePG reports whether a LIVE run (migrate / sync)
// qualifies for the ADR-0047 verbatim tier: both engines are present
// and are the same PostgreSQL engine. This is the orchestrator's
// engine-neutral, name-only determination for tier (b) on the live
// path — no engine package import, no DSN sniffing.
func verbatimLiveSameEnginePG(source, target ir.Engine) bool {
	return source != nil && target != nil &&
		source.Name() == "postgres" && target.Name() == "postgres"
}

// verbatimBackupSourcePG reports whether a BACKUP run qualifies for
// the ADR-0047 verbatim tier: the source engine is PostgreSQL. The
// restore-target engine is unknown at backup time, so qualifying here
// only enables CAPTURE; the PG-restore-only constraint is enforced by
// the recorded lineage marker + the loud restore-time engine gate
// ([refuseVerbatimRestoreToNonPG]).
func verbatimBackupSourcePG(source ir.Engine) bool {
	return source != nil && source.Name() == "postgres"
}

// validateEnabledPGExtensions enforces the engine-name gate for
// `--enable-pg-extension` (ADR-0032). For most extensions the flag
// is meaningful only on same-engine PG → PG paths — cross-engine
// translation keeps loud-failure as the default when the target
// isn't PG. The exception: extensions with a default cross-engine
// translator (today: hstore → MySQL JSON, citext → MySQL VARCHAR-
// with-collation) are permitted against non-PG targets because the
// translator handles them losslessly. The source engine queries
// its [ir.CrossEngineExtensionTranslator] surface (when implemented)
// to declare which extensions carry that capability.
//
// Empty extensions is a no-op (the field defaults to empty).
//
// Refuses (with operator-actionable wording) when:
//
//   - The source engine isn't postgres — the flag has no meaning on
//     a non-PG source.
//   - The target engine isn't postgres AND none of the named
//     extensions has a default cross-engine translator — the
//     cross-engine refusal would fire later anyway; surface it
//     earlier with a clearer pointer to the right escape hatch
//     (--type-override).
func validateEnabledPGExtensions(source, target ir.Engine, extensions []string) error {
	if len(extensions) == 0 {
		return nil
	}
	if source != nil && source.Name() != "postgres" {
		return fmt.Errorf(
			"pipeline: --enable-pg-extension is only supported on PG sources "+
				"(source engine is %q); the flag opts into PG → PG extension "+
				"passthrough per ADR-0032",
			source.Name(),
		)
	}
	if target != nil && target.Name() != "postgres" {
		// Per-extension cross-engine gate: an extension with a
		// default translator declared by the source engine may pass
		// against a non-PG target; the translator rewrites the
		// column type at emit time (mysql/ddl_emit.go) and where
		// needed translates values (mysql/row_writer.go::prepareValue).
		// Without that declaration, fall back to the strict refusal.
		translator, _ := source.(ir.CrossEngineExtensionTranslator)
		for _, ext := range extensions {
			ext = strings.TrimSpace(ext)
			if ext == "" {
				continue
			}
			if translator == nil || !translator.HasCrossEngineDefaultTranslator(ext) {
				return fmt.Errorf(
					"pipeline: --enable-pg-extension %q is not cross-engine "+
						"translatable for target engine %q; ADR-0032 § "+
						"\"Cross-engine policy\" reserves cross-engine "+
						"translators for hstore and citext (lossless MySQL "+
						"mappings). Supply --type-override per column for "+
						"the named extension, or use a PG target",
					ext, target.Name(),
				)
			}
		}
	}
	// PG → PG hstore is fully supported as of v0.32.1 — the COPY-
	// protocol binary codec (internal/engines/postgres/hstore_codec.go)
	// translates the IR's text-form hstore bytes (`"k"=>"v"`) into
	// hstore's pair-array wire format at encode time. No refusal
	// branch needed here; the engine's catalog entry +
	// validateAndPreflightExtensions handle the per-extension preflight.
	return nil
}

// applySourceFingerprint records the streamer's source-DSN fingerprint
// on a freshly-opened applier via the optional
// [ir.SourceFingerprintRecorder] surface. Engines that don't implement
// the recorder are silently passed through; the streamer's collision
// check then no-ops for that engine.
func applySourceFingerprint(applier ir.ChangeApplier, fingerprint string) {
	if fingerprint == "" {
		return
	}
	if rec, ok := applier.(ir.SourceFingerprintRecorder); ok {
		rec.SetSourceDSNFingerprint(fingerprint)
	}
}

// fingerprintSourceDSN returns the truncated SHA-256 hex of the DSN's
// host+port+database tuple (ADR-0031). User and password are
// deliberately excluded so credential rotation doesn't trip the
// stream-id collision check.
//
// Returns the empty string for a DSN sluice can't normalise (unknown
// shape, missing host); the caller treats empty as "fingerprint
// unavailable" and skips the collision check rather than refusing
// loudly. The orchestrator records the fingerprint on every position
// commit when non-empty.
//
// Truncated to 12 hex chars for log readability — full SHA-256 would
// be 64 chars and sluice's status output is the load-bearing display
// surface here. A future widening (16+ chars) is straightforward if
// real-world fingerprint collisions ever surface; the
// `source_dsn_fingerprint` column is `TEXT` (no length cap) so the
// storage shape doesn't bound the truncation.
func fingerprintSourceDSN(dsn string) string {
	host, port, database := extractDSNTriple(dsn)
	if host == "" {
		return ""
	}
	host = strings.ToLower(strings.TrimSpace(host))
	port = strings.TrimSpace(port)
	database = strings.TrimSpace(database)

	// Apply engine-default ports when the DSN omits them — keeps the
	// fingerprint stable across DSN-shape variations (`host` vs
	// `host:5432`, `tcp(h)` vs `tcp(h:3306)`).
	if port == "" {
		port = defaultPortForDSN(dsn)
	}

	canonical := host + ":" + port + ":" + database
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:])[:12]
}

// extractDSNTriple parses a DSN into (host, port, database). Both
// URI-form (postgres://, mysql://) and KV / DSN-string forms are
// accepted.
//
// Returns ("", "", "") on a DSN sluice can't recognise; the caller
// treats that as "fingerprint unavailable" rather than refusing.
func extractDSNTriple(dsn string) (host, port, database string) {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return "", "", ""
	}

	// URI-form: postgres://user:pass@host:port/db?params, mysql://...
	if strings.HasPrefix(dsn, "postgres://") ||
		strings.HasPrefix(dsn, "postgresql://") ||
		strings.HasPrefix(dsn, "mysql://") {
		if u, err := url.Parse(dsn); err == nil {
			host = u.Hostname()
			port = u.Port()
			database = strings.TrimPrefix(u.Path, "/")
			return host, port, database
		}
	}

	// MySQL DSN form: user:pass@tcp(host:port)/db?params
	if at := strings.Index(dsn, "@tcp("); at >= 0 {
		body := dsn[at+5:]
		end := strings.Index(body, ")")
		if end >= 0 {
			hostPort := body[:end]
			if colon := strings.Index(hostPort, ":"); colon >= 0 {
				host = hostPort[:colon]
				port = hostPort[colon+1:]
			} else {
				host = hostPort
			}
			rest := body[end+1:]
			rest = strings.TrimPrefix(rest, "/")
			if q := strings.Index(rest, "?"); q >= 0 {
				rest = rest[:q]
			}
			database = rest
			return host, port, database
		}
	}

	// libpq KV form: "host=localhost port=5432 dbname=mydb user=..."
	for _, tok := range strings.Fields(dsn) {
		k, v, ok := strings.Cut(tok, "=")
		if !ok {
			continue
		}
		switch strings.ToLower(k) {
		case "host":
			host = v
		case "port":
			port = v
		case "dbname", "database":
			database = v
		}
	}
	return host, port, database
}

// defaultPortForDSN returns the default port string for a DSN's
// engine, used when the DSN didn't carry one. Keeps the fingerprint
// stable across DSN shapes that elide vs name the default port.
func defaultPortForDSN(dsn string) string {
	switch {
	case strings.HasPrefix(dsn, "postgres://"),
		strings.HasPrefix(dsn, "postgresql://"),
		strings.Contains(strings.ToLower(dsn), "host="):
		return strconv.Itoa(5432)
	case strings.HasPrefix(dsn, "mysql://"),
		strings.Contains(dsn, "@tcp("):
		return strconv.Itoa(3306)
	}
	return ""
}

// errStreamIDCollision is returned by the streamer when an existing
// `sluice_cdc_state` row's recorded source-DSN fingerprint differs
// from the streamer's own fingerprint. Operator typo / wrong source —
// loud failure beats silent corruption.
//
// Wrapped via fmt.Errorf so the streamer can include the offending
// fingerprints in the error message; tests use errors.Is to assert
// the sentinel.
var errStreamIDCollision = errors.New("pipeline: stream-id reused with different source DSN")

// checkStreamIDCollision compares the current source-DSN fingerprint
// against the fingerprint recorded on an existing
// `sluice_cdc_state` row for the same stream-id. Refuses with
// errStreamIDCollision when both fingerprints are non-empty and
// differ; allows otherwise (including legacy rows with empty
// fingerprint, which pre-date the column).
//
// The orchestrator calls this at streamer startup, after
// EnsureControlTable + ListStreams, so the operator gets a clean
// refusal before any data moves.
func checkStreamIDCollision(streamID, currentFingerprint string, streams []ir.StreamStatus) error {
	if currentFingerprint == "" {
		return nil
	}
	for _, s := range streams {
		if s.StreamID != streamID {
			continue
		}
		if s.SourceDSNFingerprint == "" {
			// Legacy row pre-dating ADR-0031 (or engine without
			// fingerprint storage). Treat as "unknown — allow"; the
			// next position-write will populate the column going
			// forward.
			return nil
		}
		if s.SourceDSNFingerprint == currentFingerprint {
			return nil
		}
		return fmt.Errorf("%w: stream %q exists on target with a different "+
			"source DSN (existing fingerprint: %s, new: %s) — pick a "+
			"different --stream-id, or run with --reset-target-data to wipe "+
			"and start fresh",
			errStreamIDCollision, streamID, s.SourceDSNFingerprint, currentFingerprint)
	}
	return nil
}
