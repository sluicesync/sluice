// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Cross-engine supportability check. Phase 5 of the logical-backup
// feature (`docs/dev/design-logical-backups-phase-5.md`): cross-engine
// chain restore reuses [translate.RetargetForEngine] to rewrite types
// where a clean translation exists (UUID → CHAR(36) etc.), but a
// handful of source-engine-native types have no portable target-engine
// equivalent — PostGIS geometry on PG → MySQL, hstore on PG → MySQL.
// These shapes are caught here so chain restore can refuse with an
// operator-actionable message naming the offending entity, rather than
// bubbling up an opaque emit-time error from the schema writer.
//
// Same-engine pairs always succeed (the source engine's emitter
// natively handles its own types). Unknown engine pairs (a future
// engine) fall through as "supportable" — the schema writer will
// surface its own error if needed; this check is a conservative
// pre-flight, not an exhaustive one.

import (
	"fmt"
	"strings"

	"github.com/orware/sluice/internal/ir"
)

// checkCrossEngineSupportable scans the schema for column types that
// can't be cleanly translated from sourceEngine to targetEngine and
// returns a non-nil error naming the first offending (table, column,
// type) triple. Returns nil when every column either has a portable
// target-engine equivalent (handled by the engine's auto-emit) or a
// rewrite rule in [translate.RetargetForEngine].
//
// Used by chain restore's cross-engine path to refuse early with an
// operator-actionable message. Same-engine pairs and unknown engine
// pairs return nil — the latter on the principle that a new engine's
// emitter will surface its own error if it can't handle a type.
func checkCrossEngineSupportable(
	schema *ir.Schema,
	sourceEngine, targetEngine string,
	contextID string,
) error {
	if schema == nil || sourceEngine == targetEngine {
		return nil
	}
	// Today's only supported cross-engine direction is PG ↔ MySQL.
	// We refuse PG-native types that have no MySQL equivalent in
	// either RetargetForEngine's rewrite table or MySQL's auto-emit
	// rules. PostGIS Geometry is the load-bearing case (the IR type
	// is shared between PG and MySQL spatial, but PG's PostGIS
	// extension carries SRID + complex spatial-reference metadata
	// that doesn't round-trip through a MySQL target without
	// operator intervention).
	pgToMySQL := sourceEngine == "postgres" &&
		(targetEngine == "mysql" || targetEngine == "planetscale")
	if !pgToMySQL {
		return nil
	}
	for _, tbl := range schema.Tables {
		if tbl == nil {
			continue
		}
		for _, col := range tbl.Columns {
			if col == nil {
				continue
			}
			if reason := unsupportablePGtoMySQL(col.Type); reason != "" {
				return fmt.Errorf(
					"%s: column %q.%q has %s — no clean cross-engine translation. "+
						"Recovery: re-run with --exclude-table=%s to skip the table, "+
						"or supply a --type-override mapping the column to a portable IR type",
					contextID, tbl.Name, col.Name, reason, tbl.Name)
			}
		}
		// ADR-0044 Tier 3: a column DEFAULT / GENERATED expression
		// that references a pgcrypto crypto/digest function has no
		// honest MySQL equivalent. Silently rewriting crypt() /
		// digest() / pgp_sym_encrypt() / … to a MySQL function would
		// change the security semantics the operator relied on —
		// exactly the silent corruption the loud-failure tenet
		// forbids. (uuid-ossp's uuid_generate_v1/v1mc/v4 DO have an
		// honest mapping → MySQL UUID(); those are translated in
		// mysql/ddl_emit.go::pgToMySQLDefaultExpr and are NOT refused
		// here.) The PG schema-read gate already enforced the opt-in,
		// so a pgcrypto function reaching this point means the
		// operator enabled the extension for a PG → MySQL run — refuse
		// with a pointer to --expr-override as the escape hatch.
		for _, col := range tbl.Columns {
			if col == nil {
				continue
			}
			if fn, exprKind := unsupportablePGCryptoDefaultExpr(col); fn != "" {
				return fmt.Errorf(
					"%s: column %q.%q %s expression uses PG pgcrypto "+
						"function %s() — no honest MySQL equivalent "+
						"(translating crypto would change security "+
						"semantics). Recovery: re-run with "+
						"--exclude-table=%s to skip the table, or supply "+
						"--expr-override for the column with a "+
						"MySQL-portable expression",
					contextID, tbl.Name, col.Name, exprKind, fn, tbl.Name)
			}
		}

		// ADR-0032 Tier 2 lite: indexes carrying an extension-owned
		// operator class (today's load-bearing case is pg_trgm's
		// `gin_trgm_ops` / `gist_trgm_ops` on a `text` column) have
		// no clean MySQL translation either. Sluice never populates
		// [ir.IndexColumn.OperatorClass] for core-PG opclasses (Bug
		// 47 design), so any non-empty OperatorClass on a PG-source
		// index is by construction extension-introduced; the cross-
		// engine refusal mirrors the column-type refusal above.
		if reason, idxName, colRef := unsupportablePGIndexToMySQL(tbl); reason != "" {
			return fmt.Errorf(
				"%s: index %q on table %q has %s (column %q) — "+
					"no clean cross-engine translation. "+
					"Recovery: re-run with --exclude-table=%s to skip the table, "+
					"drop the index on the source before migrating, "+
					"or supply a --type-override / --index-override mapping",
				contextID, idxName, tbl.Name, reason, colRef, tbl.Name)
		}
	}
	return nil
}

// unsupportablePGIndexToMySQL scans a table's indexes (including
// PrimaryKey) for any [ir.IndexColumn.OperatorClass] that's
// non-empty. Returns a (reason, indexName, columnRef) triple naming
// the offending shape, or empty strings when every index is
// MySQL-portable. Sluice's PG schema reader populates OperatorClass
// only for extension-introduced opclasses (ADR-0032 / Bug 47); a
// non-empty value passing through the IR therefore indicates an
// extension-owned opclass with no MySQL counterpart.
func unsupportablePGIndexToMySQL(tbl *ir.Table) (reason, indexName, columnRef string) {
	if tbl == nil {
		return "", "", ""
	}
	check := func(idx *ir.Index) (string, string, string) {
		if idx == nil {
			return "", "", ""
		}
		// Extension-introduced operator class on any column. The PG
		// reader populates OperatorClass only for extension-owned
		// opclasses (ADR-0032 / Bug 47), so non-empty here means MySQL
		// has no counterpart.
		for _, c := range idx.Columns {
			if c.OperatorClass != "" {
				ref := c.Column
				if ref == "" {
					ref = c.Expression
				}
				return fmt.Sprintf("PG extension-owned operator class %q", c.OperatorClass),
					idx.Name, ref
			}
		}
		// PG index kinds MySQL has no counterpart for. v0.30.0 caught
		// the opclass-bearing case but missed the no-flag scenario:
		// the operator-class gets stripped from IR yet idx.Kind stays
		// `IndexKindGIN` / `IndexKindGIST`. MySQL has btree, hash,
		// FULLTEXT, and SPATIAL — gin/gist don't translate. The PG
		// schema reader sets `idx.Kind` from `pg_am.amname` regardless
		// of whether the AM is extension-owned, so this check fires
		// for core-PG gin/gist indexes even when `idx.Method` is
		// empty (only extension-introduced AMs like pgvector's
		// ivfflat / hnsw populate `idx.Method`). FULLTEXT and SPATIAL
		// are MySQL-portable so they stay unflagged here; ir.Geometry
		// auto-emits MySQL SPATIAL at write-time.
		if kind := idx.Kind; kind == ir.IndexKindGIN || kind == ir.IndexKindGIST ||
			kind == ir.IndexKindSPGist || kind == ir.IndexKindBRIN {
			ref := ""
			if len(idx.Columns) > 0 {
				ref = idx.Columns[0].Column
				if ref == "" {
					ref = idx.Columns[0].Expression
				}
			}
			label := "GIN"
			switch kind {
			case ir.IndexKindGIST:
				label = "GiST"
			case ir.IndexKindSPGist:
				label = "SP-GiST"
			case ir.IndexKindBRIN:
				label = "BRIN"
			}
			return fmt.Sprintf("PG %s index has no MySQL counterpart", label),
				idx.Name, ref
		}
		return "", "", ""
	}
	if r, n, ref := check(tbl.PrimaryKey); r != "" {
		return r, n, ref
	}
	for _, idx := range tbl.Indexes {
		if r, n, ref := check(idx); r != "" {
			return r, n, ref
		}
	}
	return "", "", ""
}

// unsupportablePGtoMySQL returns a non-empty human-readable reason
// when t can't be cleanly emitted on a MySQL target via the existing
// RetargetForEngine rules + MySQL auto-emit. Returns "" for
// supportable types.
//
// PostGIS Geometry was refused pre-v0.28.0 because the SRID metadata
// didn't round-trip cleanly. ADR-0035 closes that gap: the PG schema
// reader populates ir.Geometry.SRID from PostGIS's geometry_columns
// view, and the MySQL writer emits `SRID <n>` on the column DDL
// (8.0+) so ST_SRID(col) on the target returns the source's SRID
// instead of 0. Cross-engine geometry values pass through as
// supportable now.
//
// PG extension passthrough types (ADR-0032) — pgvector / pg_trgm /
// postgis — have no portable MySQL equivalent; the refusal here
// keeps the cross-engine loud-failure default in place even when
// the source side was opened with `--enable-pg-extension`.
// Operators wanting a translation supply `--type-override
// TABLE.COL=<MySQL_type>`.
//
// Exception: hstore and citext have default cross-engine
// translators (hstore → MySQL JSON; citext → MySQL VARCHAR with
// case-insensitive collation), so the refusal carves them out.
// The MySQL writer's `emitColumnType` handles the type rewrite
// directly; the writer's `prepareValue` translates hstore wire
// format to JSON at value-write time. See the catalog's
// `crossEngineDefaultTranslatedExtensions` for the policy source
// of truth.
func unsupportablePGtoMySQL(t ir.Type) string {
	if v, ok := t.(ir.ExtensionType); ok {
		if isCrossEngineTranslatablePGExtension(v.Extension) {
			return ""
		}
		return fmt.Sprintf("PG extension type %s.%s", v.Extension, v.Name)
	}
	return ""
}

// pgcryptoCrossEngineUnsafeFns is the set of pgcrypto function
// barewords that have no honest MySQL equivalent and therefore
// trigger the ADR-0044 §3 cross-engine refusal when they appear in a
// PG-source column DEFAULT / GENERATED expression bound for a MySQL
// target. Kept inline in the pipeline package (engine-neutral
// orchestrator rule — no postgres-engine import); it MUST stay in
// lock-step with pgCryptoDef.defaultExprFunctions in
// internal/engines/postgres/extension_catalog.go.
//
// uuid-ossp's generators are deliberately ABSENT — they DO have an
// honest MySQL mapping (→ UUID(), via mysql/ddl_emit.go::
// pgToMySQLDefaultExpr) and must not be refused here. gen_random_uuid
// is core PG and never reaches an extension gate at all.
var pgcryptoCrossEngineUnsafeFns = map[string]struct{}{
	"digest":           {},
	"hmac":             {},
	"crypt":            {},
	"gen_salt":         {},
	"gen_random_bytes": {},
	"encrypt":          {},
	"decrypt":          {},
	"encrypt_iv":       {},
	"decrypt_iv":       {},
	"pgp_sym_encrypt":  {},
	"pgp_sym_decrypt":  {},
	"pgp_pub_encrypt":  {},
	"pgp_pub_decrypt":  {},
}

// unsupportablePGCryptoDefaultExpr returns the pgcrypto function name
// and the clause kind ("DEFAULT" / "GENERATED") when col's default
// expression or generated expression references a pgcrypto
// crypto/digest function with no honest MySQL equivalent. Returns
// ("", "") for portable columns.
//
// The scan is the same conservative shape as the PG engine's
// scanExtensionFunctionInExpr (bareword followed by `(`, ignore
// matches inside single-quoted string literals, no qualified
// matches), reimplemented here to keep the pipeline package free of
// an engine import. False negatives degrade to the pre-ADR-0044
// late MySQL parse error (no worse than status quo); false positives
// would refuse a valid migration, so the matcher stays tight.
func unsupportablePGCryptoDefaultExpr(col *ir.Column) (fn, exprKind string) {
	if col == nil {
		return "", ""
	}
	if de, ok := col.Default.(ir.DefaultExpression); ok {
		if name := scanForCryptoFn(de.Expr); name != "" {
			return name, "DEFAULT"
		}
	}
	if col.GeneratedExpr != "" {
		if name := scanForCryptoFn(col.GeneratedExpr); name != "" {
			return name, "GENERATED"
		}
	}
	return "", ""
}

// scanForCryptoFn walks expr and returns the first pgcrypto-unsafe
// bareword that is used as a function call (followed by `(`, not
// inside a string literal, not schema-qualified). Returns "" when
// none is referenced. Case-insensitive (PG lower-cases unquoted
// identifiers).
func scanForCryptoFn(expr string) string {
	for i := 0; i < len(expr); {
		c := expr[i]
		if c == '\'' {
			// Skip a single-quoted string literal (doubled '' escape).
			i++
			for i < len(expr) {
				if expr[i] == '\'' {
					if i+1 < len(expr) && expr[i+1] == '\'' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			continue
		}
		if !isExprIdentStart(c) {
			i++
			continue
		}
		start := i
		j := i + 1
		for j < len(expr) && isExprIdentCont(expr[j]) {
			j++
		}
		word := expr[start:j]
		k := j
		for k < len(expr) && (expr[k] == ' ' || expr[k] == '\t' || expr[k] == '\n' || expr[k] == '\r') {
			k++
		}
		if k < len(expr) && expr[k] == '(' {
			qualified := start > 0 && expr[start-1] == '.'
			if !qualified {
				if _, bad := pgcryptoCrossEngineUnsafeFns[strings.ToLower(word)]; bad {
					return strings.ToLower(word)
				}
			}
		}
		i = j
	}
	return ""
}

func isExprIdentStart(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || b == '_'
}

func isExprIdentCont(b byte) bool {
	return isExprIdentStart(b) || (b >= '0' && b <= '9')
}

// isCrossEngineTranslatablePGExtension reports whether an
// ir.ExtensionType column from a PG source has a default
// cross-engine translator on the MySQL side. Mirrors the catalog's
// `crossEngineDefaultTranslatedExtensions` set; kept duplicated
// in the pipeline package to avoid importing the postgres engine
// package (engine-neutral orchestrator rule). The two lists must
// stay in lock-step — see `internal/engines/postgres/extension_catalog.go`.
func isCrossEngineTranslatablePGExtension(name string) bool {
	switch name {
	case "hstore", "citext":
		return true
	}
	return false
}

// checkCrossEngineDeltaSupportable scans an incremental's schema-delta
// entries for shapes whose translated form would not be cleanly
// supportable on the target engine. Mirrors
// [checkCrossEngineSupportable] but only inspects the after-shape of
// AddTable / AlterTable entries (DropTable / DropColumn don't carry a
// portable-type concern). Returns nil for same-engine pairs and unknown
// engine pairs; a wrapped error naming the offending column otherwise.
func checkCrossEngineDeltaSupportable(
	deltas []*ir.SchemaDeltaEntry,
	sourceEngine, targetEngine, backupID string,
) error {
	if sourceEngine == targetEngine || sourceEngine == "" {
		return nil
	}
	for _, d := range deltas {
		if d == nil || d.After == nil {
			continue
		}
		switch d.Kind {
		case ir.SchemaDeltaAddTable, ir.SchemaDeltaAlterTable:
			tbl := &ir.Schema{Tables: []*ir.Table{d.After}}
			ctxID := fmt.Sprintf("chain restore: incremental %s schema delta on table %q",
				backupID, d.Table)
			if err := checkCrossEngineSupportable(tbl, sourceEngine, targetEngine, ctxID); err != nil {
				return err
			}
		}
	}
	return nil
}
