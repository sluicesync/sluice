// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"testing"

	"sluicesync.dev/sluice/internal/domaingate"
)

// TestDomainTransparency_PostgresDispatchRoster is this engine's instantiation
// of the Bug 233 gate (audit A-3). Postgres is the engine that PRODUCES
// [ir.Domain] (its SchemaReader wraps a domain-typed column, Bug 113), so it is
// the likeliest place a Domain-transparency defect lives — hence a gate rather
// than the domaingate doc's original "PG is exempt because it re-creates the
// DOMAIN" hand-wave.
//
// What the roster below establishes, walked once against the code:
//
//   - The VALUE paths are already domain-transparent — prepareValue,
//     value_decode and emitColumnType each recurse through ir.Domain to the base
//     type, so no VALUE dispatch reads the wrapper.
//   - The PHYSICAL-WRITE-PATH predicates (COPY-vs-INSERT routing and per-conn
//     codec registration in row_writer / timetz_codec / macaddr8_codec) were the
//     real gap: a DOMAIN-wrapped verbatim/interval/timetz/macaddr8[]/vector/
//     hstore column reached them and matched no arm, so it took the default
//     binary-COPY path that mis-encodes or refuses. Those six now UnwrapDomain
//     (the fix in this change) and are pinned in domain_gate_pin_test.go.
//   - Every remaining RAW dispatch is exempt for a written, code-verified reason
//     below — the operand can never be a Domain on that path, or the site
//     deliberately reads the wrapper, or unwrapping would be inert.
//
// See the domaingate package doc for what a pass proves and what it does not: it
// grades which BRANCH runs for a DOMAIN-typed column, and says nothing about
// what that branch then computes.
func TestDomainTransparency_PostgresDispatchRoster(t *testing.T) {
	domaingate.Assert(t, domaingate.Config{
		Dir:    ".",
		Engine: "postgres",
		// 34 dispatch nodes are walked (6 now read storage via UnwrapDomain);
		// the floor sits a few below so a refactor that dissolves the shape is
		// caught, not the exact count.
		MinSites: 28,
		Allowed:  postgresDomainDispatchExemptions,
	})
}

// postgresDomainDispatchExemptions is the fail-by-default roster. Unlike the
// MySQL engine — where every exemption reduces to "MySQL has no DOMAIN, so this
// engine's own SchemaReader can never populate ir.Domain here" — Postgres DOES
// produce domains, so each reason has to say something sharper: why a Domain
// cannot reach THIS operand, or why the raw dispatch is correct anyway. The
// recurring shapes:
//
//   - WIRE / TARGET-DERIVED — the column descriptors come from the CDC relation
//     cache (pgoutput projects bare wire types) or the applier's loadColumnTypes
//     (which reads the TARGET's information_schema — that view UNWRAPS a domain
//     to its base type and loadColumnTypes, unlike the SchemaReader, does NOT
//     re-wrap). An ir.Domain cannot appear in either.
//   - CROSS-ENGINE-ONLY FAMILY — the operand's type family is one only a
//     non-PG source produces (ir.Set, MySQL-only) or one the path only reaches
//     for a MySQL/SQLite-dialect expression; those sources have no DOMAIN, so
//     the two never co-occur.
//   - DOMAIN IS FIRST-CLASS ON PG — the writer re-creates the DOMAIN and emits
//     the column BY NAME (emitColumnType's ir.Domain arm), so a column-level
//     base-type special-case correctly does nothing for a domain column; the
//     base-type concern is encapsulated in the CREATE DOMAIN, not the column.
//   - IMPOSSIBLE SHAPE — a serial/identity column cannot be DOMAIN-typed in PG.
//   - LOUD-REFUSED UPSTREAM — a domain-over-enum is refused at
//     emitCreateDomainType (emitColumnType cannot render an enum base without
//     column context), pinned in domain_gate_pin_test.go, so the enum-type
//     lifecycle sites never silently mishandle it.
//   - DELIBERATE WRAPPER READ — Phase 1a' dispatches on ir.Domain on purpose to
//     emit CREATE DOMAIN; that IS the domain-handling site.
var postgresDomainDispatchExemptions = map[string]string{
	// ---- WIRE: the CDC relation cache never carries ir.Domain ----
	"cdc_geometry_srid.go:resolveGeometryColumnSRIDs:entry.Columns[i].Type": "WIRE: the *relationCacheEntry " +
		"is built by buildRelationCacheEntry from the pgoutput RelationMessage, which maps a column's wire OID " +
		"to a BARE ir type (ir.Geometry{} etc.) and constructs no ir.Domain — there is no domain-OID arm. So a " +
		"Domain cannot reach this SRID-annotation walk; unwrapping would be inert.",
	"cdc_geometry_srid.go:entryHasGeometryColumn:entry.Columns[i].Type": "WIRE: same relation-cache entry, same " +
		"argument — this only answers whether a catalog SRID lookup is worth making, over wire-projected bare " +
		"types that are never ir.Domain.",

	// ---- TARGET-DERIVED: the applier's colTypes never carries ir.Domain ----
	"change_applier.go:equalityPredicate:c.Type": "TARGET-DERIVED: the applier's colTypes come from " +
		"loadColumnTypes, which reads the TARGET's information_schema — that view UNWRAPS a DOMAIN to its base " +
		"type, and loadColumnTypes (unlike SchemaReader.populateColumns) has NO typtype='d' re-wrap, so an " +
		"ir.Domain can never appear in the map. A domain-over-json TARGET column already presents here as " +
		"ir.JSON and correctly gets the ::text equality cast. Pinned by TestPin_ApplierLoadColumnTypes_* " +
		"(integration).",
	"change_applier.go:applyPlaceholder:c.Type": "TARGET-DERIVED: same loadColumnTypes-sourced colTypes as " +
		"equalityPredicate — no ir.Domain in the map, so a domain-over-verbatim TARGET column already presents " +
		"as ir.VerbatimType and gets its $N::<type> cast.",
	"change_applier.go:prepareApplierValue:col.Type": "TARGET-DERIVED: same colTypes source; and the value " +
		"itself is routed through prepareValue, which recurses through ir.Domain to the base type regardless. " +
		"The raw check here only adds the verbatim []byte→string wire fix, and a verbatim TARGET column already " +
		"presents as ir.VerbatimType (not domain-wrapped).",

	// ---- DOMAIN IS RENDERABLE: preflight must NOT skip a domain ----
	"column_type_preflight.go:PreflightColumnTypes:col.Type": "DOMAIN-RENDERABLE: the skip is for a TOP-LEVEL " +
		"ir.Enum column, which emitColumnType REFUSES (it needs emitColumnDef's column context). A DOMAIN — even " +
		"a domain-over-enum — flows through emitColumnType's ir.Domain arm and renders by NAME, so it is " +
		"legitimately preflightable; unwrapping here would wrongly skip a renderable domain-over-enum and hide a " +
		"real preflight answer.",

	// ---- IMPOSSIBLE SHAPE: identity/serial cannot be DOMAIN-typed on PG ----
	"cutover_sequence.go:ReadSequenceState:col.Type": "IMPOSSIBLE-SHAPE: the guard fires only for an " +
		"AutoIncrement ir.Integer, and PostgreSQL does not allow GENERATED … AS IDENTITY (or SERIAL) on a " +
		"DOMAIN-typed column — so ir.Domain never wraps an AutoIncrement integer and the arm is unreachable for a " +
		"domain.",
	"cutover_sequence.go:PrimeSequences:col.Type": "IMPOSSIBLE-SHAPE: same AutoIncrement-integer guard; a " +
		"serial/identity column cannot be DOMAIN-typed on PG.",
	"schema_writer.go:SyncIdentitySequences:col.Type": "IMPOSSIBLE-SHAPE: same AutoIncrement-integer guard; a " +
		"serial/identity column cannot be DOMAIN-typed on PG.",

	// ---- CROSS-ENGINE-ONLY / DOMAIN-FIRST-CLASS DDL sites ----
	"ddl_emit.go:emitColumnDef:c.Type": "DOMAIN-FIRST-CLASS: a domain column falls to emitColumnType's ir.Domain " +
		"arm and renders BY NAME; the enum branch (enum type ref) and the SET/enum DEFAULT-cast special-cases are " +
		"about the column's DIRECT type. For a domain those concerns live in the CREATE DOMAIN (Phase 1a'), not " +
		"the column, so skipping them here is correct. (ir.Set is MySQL-source-only and can never be " +
		"domain-wrapped anyway.)",
	"ddl_emit.go:emitTableDef:col.Type": "CROSS-ENGINE/LOCKSTEP: the two synthesized CHECKs are for ir.Set " +
		"(MySQL-source-only — a PG-only Domain can never wrap it) and for a GENERATED ir.Enum lowered to TEXT " +
		"(Bug 25). A domain-over-enum renders BY NAME via emitColumnType, never as TEXT, so it needs no " +
		"value-list CHECK; and this must dispatch identically to PredictEmittedChecks or the prediction drifts " +
		"from the DDL.",
	"ddl_emit.go:translateDefaultExpr:c.Type": "CROSS-ENGINE-ONLY: this is the SQLite-source DEFAULT-expression " +
		"translation path; the ir.Boolean check only narrows a bare-TRUE/FALSE eligibility for SQLite's " +
		"integer-bool idiom. SQLite has no DOMAIN, so a domain-wrapped column never carries a SQLite-dialect " +
		"default here.",
	"ddl_emit.go:translateGeneratedExpr:c.Type": "CROSS-ENGINE-ONLY: the ir.Integer flag sets integer-division " +
		"semantics for translating a MySQL-dialect generated-column body, reached only when GeneratedExprDialect " +
		"is MySQL. MySQL has no DOMAIN; a PG-source domain generated column returns verbatim before this point.",
	"ddl_emit.go:exprContextForTable:col.Type": "CROSS-ENGINE-ONLY: BoolColumns drives the MySQL/SQLite " +
		"bool-idiom rewrite in translated CHECK/generated expressions; those source engines have no DOMAIN, so a " +
		"domain-over-bool never needs membership. PG→PG expressions emit verbatim (no rewrite).",
	"emitted_checks.go:PredictEmittedChecks:col.Type": "LOCKSTEP: this PREDICTS the two CHECKs emitTableDef " +
		"synthesizes — one per ir.Set column and one per GENERATED ir.Enum column — and must dispatch IDENTICALLY " +
		"to emitTableDef (raw) or the prediction drifts from the DDL. ir.Set is MySQL-source-only (never " +
		"domain-wrapped); a domain-over-enum renders BY NAME, not as the TEXT+CHECK Bug-25 fallback, so it " +
		"correctly predicts no CHECK.",
	"row_writer_float_repair.go:floatRepairCastType:col.Type": "DOMAIN-FIRST-CLASS: the ir.Integer special-case " +
		"exists ONLY to avoid the ` GENERATED … AS IDENTITY` suffix emitColumnType appends, which a DOMAIN " +
		"(never identity-typed) does not carry. emitColumnType renders a domain BY NAME, which is itself a valid " +
		"PG CAST target (`$N::domainname`), so the repair-table placeholder cast is correct without unwrapping.",

	// ---- ENUM-TYPE LIFECYCLE: domain-over-enum is created via the DOMAIN (Bug 255) ----
	"schema_writer.go:CreateTablesWithoutConstraints:col.Type": "TWO SITES, one key (same function+operand): " +
		"(a) Phase 1a' dispatches on ir.Domain DELIBERATELY to emit CREATE DOMAIN — that IS the domain-handling " +
		"site, the wrapper read is the point (and, Bug 255, when the domain's BaseType is ir.Enum, Phase 1a' " +
		"pre-creates the enum TYPE named from the DOMAIN before the CREATE DOMAIN references it — that create " +
		"dispatches on dom.BaseType, not col.Type, so it is outside this gate's graded operand). (b) Phase 1a " +
		"creates the standalone enum TYPE for a TOP-LEVEL ir.Enum column; a domain-over-enum's col.Type is " +
		"ir.Domain (not ir.Enum), so it is correctly skipped here and handled by (a) instead.",
	"schema_writer.go:previewEnumTypes:col.Type": "TOP-LEVEL-ENUM-ONLY: this previews the CREATE TYPE for a " +
		"column whose DIRECT type is ir.Enum; a domain-over-enum's col.Type is ir.Domain and is skipped. " +
		"PreviewDDL emits no CREATE DOMAIN phase at all (a pre-existing preview-completeness gap for the whole " +
		"domain feature, NOT silent loss — the apply path creates the type + domain correctly), so skipping the " +
		"domain-over-enum enum type here keeps preview in step with what it does render.",
	"schema_writer.go:ensureEnumType:col.Type": "TOP-LEVEL-ENUM-ONLY: the forward ADD-COLUMN/ALTER path's " +
		"enum-type ensure fires only for a column whose DIRECT type is ir.Enum. A forwarded ADD COLUMN of a " +
		"DOMAIN column renders BY NAME via emitColumnDef and needs the DOMAIN (and, for an enum base, its enum " +
		"type) to pre-exist on the target; the forward path creates neither — a pre-existing limitation shared " +
		"by every domain base kind (Bug 255 note) whose absence fails LOUDLY at ADD COLUMN with 42704, not " +
		"silently. So this site correctly skips domains.",
	"schema_writer.go:orphanedEnumTypeDrop:col.Type": "INERT: this drops ONLY MySQL-synthesized (TypeName=='') " +
		"enum types on column drop; a domain-over-enum is PG-sourced, whose ir.Enum carries TypeName!='' and is " +
		"NEVER auto-dropped by policy (catalog Bug 19c). So even unwrapped the guard returns false — unwrapping " +
		"is inert.",
	"row_writer.go:DropSchemaTypes:col.Type": "LOCKSTEP + DELIBERATE-WRAPPER-READ: the reset-time drop mirrors " +
		"CreateTablesWithoutConstraints' enum-type CREATE. Bug 255 gave Phase 1a' a domain-over-enum enum-TYPE " +
		"create, so this function now ALSO drops that type (via ir.DomainOf(col) → dom.BaseType.(ir.Enum), " +
		"CASCADE removing the dependent domain); the raw col.Type.(ir.Enum) arm keyed here handles TOP-LEVEL " +
		"enum columns and correctly skips domains (they take the DomainOf branch). Stays in lockstep with the " +
		"create side.",
	"schema_writer.go:AlterColumnType:want.Type": "CROSS-ENGINE-ORIGIN: this routes a MySQL `MODIFY col ENUM(...)` " +
		"add-value ALTER to ALTER TYPE … ADD VALUE (Bug 145); MySQL has no DOMAIN. A PG→PG shape-delta that " +
		"changes a domain column's type flows through emitColumnType (by NAME); an enum-value addition under a " +
		"domain is a diff-level concern that does not surface as this alter-column-type shape.",
}
