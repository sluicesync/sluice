// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// Compile-time proof the ENGINE (not a test stub) answers the pre-copy
// column-type question, so the orchestrator's [ir.ColumnTypeEmitPreflighter]
// gate engages. The registry holds an Engine VALUE, so the assertion is on the
// value type.
var _ ir.ColumnTypeEmitPreflighter = Engine{}

// PreflightColumnTypes reports whether every column type in s can be rendered
// on a Postgres target, WITHOUT a connection and before any data moves.
//
// It delegates to [emitColumnType] — the same function [emitColumnDef] calls on
// the create path — so the early answer and the late one cannot drift.
//
// # Postgres has the widest type surface, and that is why it needs this
//
// The refusals this reaches are not exotic PG-native shapes; they are shapes a
// MYSQL source routinely carries that PG has no legal spelling for. `VARCHAR(0)`
// and `CHAR(0)` are the load-bearing pair (catalog Bug 107): a legal, and in
// long-lived MySQL schemas a common, marker column that PG rejects at CREATE
// TABLE with SQLSTATE 22023. No cross-engine check covers that direction —
// [migcore.CheckCrossEngineSupportable] only ever looks at PG-source → MySQL —
// so before this the refusal arrived from the schema-apply phase, after the
// plan was printed and with every table ahead of it already created.
//
// # It runs the emitter under the MOST PERMISSIVE options the run could have
//
// Two of this emitter's refusals are configuration-dependent and therefore
// deliberately out of scope for a connection-free surface (see
// [ir.ColumnTypeEmitPreflighter]'s GAP note): GEOMETRY needs PostGIS, which is
// probed when the writer opens, and an [ir.ExtensionType] column needs
// `--enable-pg-extension`, a flag this surface does not carry. Rather than
// guess at either — guessing "no" is an over-refusal that breaks runs which
// work today — [preflightEmitOpts] declares PostGIS present and every extension
// the schema mentions enabled. What survives that is refused under EVERY
// configuration, which is the only thing this surface is entitled to refuse.
// Both configuration-dependent refusals still fire from the emitter in the
// create-tables phase, which is phase one, before any row moves.
//
// # The one type it skips, and why skipping is required rather than cautious
//
// A top-level [ir.Enum] column NEVER reaches [emitColumnType] on the real path:
// [emitColumnDef] resolves it to a per-column generated type name (or to TEXT
// for a generated column) and only calls emitColumnType for everything else.
// emitColumnType's Enum arm is an internal guard that says "use emitColumnDef",
// so a preflight that called it would refuse every enum column on every PG
// target. The skip mirrors emitColumnDef's branch exactly — TOP-LEVEL only: an
// `ir.Array` whose element is an Enum has no emitColumnDef branch and IS
// refused by the emitter, so it is refused here too.
//
// A nil column, or a column with a nil Type, is likewise skipped: a nil type is
// a corrupt IR rather than an unrepresentable one, and the emitter's own
// handling of it is left unchanged.
func (Engine) PreflightColumnTypes(s *ir.Schema) error {
	if s == nil {
		return errors.New("postgres: PreflightColumnTypes: schema is nil")
	}
	opts := preflightEmitOpts(s)
	for _, table := range s.Tables {
		if table == nil {
			continue
		}
		for _, col := range table.Columns {
			if col == nil || col.Type == nil {
				continue
			}
			if _, isEnum := col.Type.(ir.Enum); isEnum {
				continue
			}
			if _, err := emitColumnType(col.Type, opts); err != nil {
				return fmt.Errorf("postgres: table %q column %q: %w", table.Name, col.Name, err)
			}
		}
	}
	return nil
}

// preflightEmitOpts builds the most permissive [emitOpts] the run could
// legitimately have, so [Engine.PreflightColumnTypes] refuses only what no
// configuration can rescue.
//
// HasPostGIS is true because the real value is probed from the target server
// this surface has no connection to, and TargetSchema is empty because it
// changes only how a DOMAIN reference is QUALIFIED, never whether the type can
// be rendered. EnabledExtensions is derived from the schema itself — every
// extension name it mentions, at any depth an array can nest one — rather than
// from the catalog, so the map stays a fact about this schema and an
// uncatalogued extension is still refused by [emitExtensionColumn] (that
// refusal no flag can lift).
func preflightEmitOpts(s *ir.Schema) emitOpts {
	enabled := map[string]bool{}
	for _, table := range s.Tables {
		if table == nil {
			continue
		}
		for _, col := range table.Columns {
			if col == nil {
				continue
			}
			collectExtensionNames(col.Type, enabled)
		}
	}
	return emitOpts{HasPostGIS: true, EnabledExtensions: enabled}
}

// collectExtensionNames records every extension an IR type mentions, following
// the ONE nesting [emitColumnType] itself follows (an array's element). A
// DOMAIN is deliberately not descended into: the emitter renders a domain by
// NAME and never looks at its base type, so a base-type extension name would be
// a name this emitter never asks about.
func collectExtensionNames(t ir.Type, into map[string]bool) {
	switch v := t.(type) {
	case ir.ExtensionType:
		if v.Extension != "" {
			into[v.Extension] = true
		}
	case ir.Array:
		collectExtensionNames(v.Element, into)
	}
}
