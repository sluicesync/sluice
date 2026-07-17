// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// MariaDB-flavor leaves (roadmap item 73 Phase 1). Everything MariaDB-
// specific that the shared engine code dispatches on lives here: the
// COLUMN_DEFAULT normalization shim, the upsert-spelling selector, the
// cross-family collation maps, the server-fingerprint guard, and the
// coded CDC refusal. Ground truth for every convention in this file was
// captured live against mariadb:11.4.12 and mariadb:10.11.18 side by
// side with mysql:8.4 (the 2026-07-16 scoping probe, sluice-testing
// workspace/mariadb/scoping-probe.md, plus this chunk's implementation
// probes); the unit matrix in flavor_mariadb_test.go pins each row.

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// upsertSpelling selects how an INSERT-upsert references the incoming
// row's values in its ON DUPLICATE KEY UPDATE tail. MySQL 8.0.20+ uses
// the row-alias form sluice adopted everywhere; MariaDB never
// implemented the alias (all versions reject `AS new` with Error 1064)
// and instead kept the legacy VALUES() function MySQL deprecates. The
// spelling is flavor-derived and threaded to every upsert builder —
// the migrate-state store, the change applier (single-, multi-row, and
// position/schema-history control writes), and the batched-insert row
// writer — so one selector covers the whole class of `AS new` emission
// sites rather than one representative.
//
// The zero value is upsertRowAlias, so every pre-existing construction
// (tests, direct-API callers, the non-mariadb flavors) keeps today's
// byte-identical statements without setting anything.
type upsertSpelling uint8

const (
	// upsertRowAlias — INSERT … VALUES (…) AS new
	// ON DUPLICATE KEY UPDATE col = new.col (MySQL 8.0.20+; sluice's
	// default spelling on every non-MariaDB flavor).
	upsertRowAlias upsertSpelling = iota

	// upsertValuesFunc — INSERT … VALUES (…)
	// ON DUPLICATE KEY UPDATE col = VALUES(col) (MariaDB, all
	// versions). MySQL 8 still accepts this form with a deprecation
	// warning, which is why it is flavor-gated rather than the new
	// blanket default.
	upsertValuesFunc
)

// upsertSpelling returns the ON DUPLICATE KEY UPDATE spelling this
// flavor's target server accepts.
func (f Flavor) upsertSpelling() upsertSpelling {
	if f == FlavorMariaDB {
		return upsertValuesFunc
	}
	return upsertRowAlias
}

// clauseOpen returns the text between the VALUES groups and the first
// SET-list entry — including the row alias when the spelling uses one.
func (u upsertSpelling) clauseOpen() string {
	if u == upsertValuesFunc {
		return " ON DUPLICATE KEY UPDATE "
	}
	return " AS new ON DUPLICATE KEY UPDATE "
}

// newRowRef renders a reference to the incoming row's value for col
// (col arrives quoted or bare exactly as the caller's statement uses
// it elsewhere; the spelling wraps, never quotes).
func (u upsertSpelling) newRowRef(col string) string {
	if u == upsertValuesFunc {
		return "VALUES(" + col + ")"
	}
	return "new." + col
}

// translateMariaDBDefault is the MariaDB counterpart of
// [translateDefault] — the ATOMIC companion of the catalog-query fix
// (roadmap item 73 P1): patching the srs_id wall without this shim
// would let MariaDB's COLUMN_DEFAULT conventions flow through the
// MySQL-convention translator as silent default corruption (quoted
// strings kept their quotes, every defaultless nullable column gained
// the 4-char string default "NULL", and current_timestamp() became a
// string literal).
//
// MariaDB (≥ 10.2.7) reports COLUMN_DEFAULT as the DEFAULT *expression*
// text, ground-truthed as:
//
//	declared default            reported COLUMN_DEFAULT
//	(none, NOT NULL)            SQL NULL
//	nullable / DEFAULT NULL     the 4-char keyword NULL (unquoted)
//	DEFAULT 'abc'               'abc'   (quotes INCLUDED; '' doubling +
//	                            the \0 \n \r \\ escape set — the same
//	                            schema-metadata printing MySQL uses for
//	                            ENUM labels, decoded by
//	                            scanMySQLQuotedString)
//	DEFAULT 42 / -7 / 1e3       42 / -7 / 1000 (evaluated, bare)
//	DEFAULT CURRENT_TIMESTAMP   current_timestamp()  (extra EMPTY — no
//	                            DEFAULT_GENERATED token exists)
//	DEFAULT b'1010'             b'1010' (same as MySQL)
//	BINARY(2) DEFAULT X'4142'   'AB'    (quoted raw bytes, NULs escape-
//	                            encoded — NOT the MySQL 0x4142 hex form,
//	                            and NOT NUL-truncated)
//	DEFAULT uuid() / (1+1)      uuid() / (1 + 1) (bare expression text)
//
// So the discriminator is the surface form itself: SQL NULL and the
// bare NULL keyword mean no default; a leading quote means a string
// literal (decoded, and re-tagged as a hex-literal expression on
// binary-family columns so the IR matches what the same logical schema
// produces via MySQL 8); a numeric shape is a literal; b'…' is a bit
// literal; anything else is an expression. Expression text is folded
// through the same [normalizeMySQLExpressionText] pass as the MySQL
// path, plus [canonMariaDBTimestampExpr] so the CURRENT_TIMESTAMP
// family lands in the IR byte-identically to MySQL 8's keyword form.
func translateMariaDBDefault(def sql.NullString, extra string, typ ir.Type) ir.DefaultValue {
	_ = extra // MariaDB has no DEFAULT_GENERATED token; kept for signature symmetry.
	if !def.Valid {
		return ir.DefaultNone{}
	}
	raw := def.String
	// The bare keyword NULL: MariaDB's spelling for "no default" on a
	// nullable column (explicit DEFAULT NULL and defaultless nullable
	// report identically). A column whose default is the STRING "NULL"
	// reports the quoted form 'NULL' and takes the literal branch.
	if raw == "NULL" {
		return ir.DefaultNone{}
	}
	if bits, ok := bitLiteralBits(raw); ok {
		// Identical to the MySQL path: BIT(N>1) keeps the bit string,
		// BIT(1)/Boolean collapses to the decimal form (catalog #4/62).
		if _, isBit := typ.(ir.Bit); isBit {
			return ir.DefaultExpression{Expr: "b'" + bits + "'", Dialect: bitLiteralDialect}
		}
		return ir.DefaultLiteral{Value: bitsToDecimal(bits)}
	}
	if strings.HasPrefix(raw, "'") {
		val, end, ok := scanMySQLQuotedString(raw)
		if !ok || end != len(raw) {
			// Malformed / unrecognized quoted shape: carry it verbatim so
			// the target rejects it loudly rather than guess (the same
			// fall-back posture as bitLiteralBits).
			return ir.DefaultLiteral{Value: raw}
		}
		if isBinaryFamilyType(typ) && len(val) > 0 {
			// Re-encode the decoded bytes as MySQL's bare hex-literal form
			// so the IR (and the emitted DDL) is byte-identical to what the
			// same logical schema produces via a MySQL 8 read — including
			// NUL-bearing defaults, which MariaDB escape-encodes instead of
			// C-truncating, so no SHOW CREATE recovery pass is needed here.
			return ir.DefaultExpression{Expr: fmt.Sprintf("0x%X", val), Dialect: hexLiteralDialect}
		}
		return ir.DefaultLiteral{Value: string(val)}
	}
	if mariadbNumericDefault(raw) {
		return ir.DefaultLiteral{Value: raw}
	}
	// Everything else is expression text (MariaDB stores it bare, with
	// the same backtick/introducer decorations MySQL uses).
	expr := canonMariaDBTimestampExpr(normalizeMySQLExpressionText(raw))
	return ir.DefaultExpression{Expr: expr, Dialect: "mysql"}
}

// mariadbNumericDefault reports whether raw has the shape of the
// evaluated numeric literal MariaDB stores for numeric defaults:
// optional sign, digits, optional fraction, optional exponent. A miss
// here classifies the default as an expression, which re-emits
// verbatim — value-identical DDL, so the check errs on the strict
// side rather than swallowing expression text.
func mariadbNumericDefault(raw string) bool {
	if raw == "" {
		return false
	}
	s := raw
	if s[0] == '-' || s[0] == '+' {
		s = s[1:]
	}
	digits := func(t string) (rest string, n int) {
		i := 0
		for i < len(t) && t[i] >= '0' && t[i] <= '9' {
			i++
		}
		return t[i:], i
	}
	var n int
	s, n = digits(s)
	if n == 0 {
		return false
	}
	if strings.HasPrefix(s, ".") {
		s, _ = digits(s[1:])
	}
	if s != "" && (s[0] == 'e' || s[0] == 'E') {
		s = s[1:]
		if s != "" && (s[0] == '-' || s[0] == '+') {
			s = s[1:]
		}
		s, n = digits(s)
		if n == 0 {
			return false
		}
	}
	return s == ""
}

// canonMariaDBTimestampExpr folds MariaDB's function-call spelling of
// the CURRENT_TIMESTAMP default family — current_timestamp() /
// current_timestamp(3) — to the keyword form MySQL 8 reports
// (CURRENT_TIMESTAMP / CURRENT_TIMESTAMP(3)), so the IR is identical
// across the two flavors and the cross-engine writers' existing
// CURRENT_TIMESTAMP handling applies unchanged. Only this family needs
// the fold: MySQL treats CURRENT_TIMESTAMP as a keyword (uppercase, no
// parens) while storing every other default function lowercase with
// parens — exactly MariaDB's spelling — so uuid(), curdate(), etc.
// already match byte-for-byte.
func canonMariaDBTimestampExpr(expr string) string {
	const fn = "current_timestamp"
	if len(expr) < len(fn)+2 || !strings.EqualFold(expr[:len(fn)], fn) {
		return expr
	}
	rest := expr[len(fn):]
	if rest == "()" {
		return "CURRENT_TIMESTAMP"
	}
	// current_timestamp(N) — keep the fractional-seconds precision.
	if strings.HasPrefix(rest, "(") && strings.HasSuffix(rest, ")") {
		if _, err := strconv.Atoi(rest[1 : len(rest)-1]); err == nil {
			return "CURRENT_TIMESTAMP" + rest
		}
	}
	return expr
}

// mariadbTargetCollations maps the language-agnostic MySQL-8
// utf8mb4_0900_* collations to the closest MariaDB equivalent that
// exists on BOTH supported LTS lines (10.11 and 11.4): the UCA-14.0.0
// set, plus the codepoint-binary nopad form for 0900_bin. MariaDB 11.4
// aliases the 0900 names itself, but 10.11 rejects them (Error 1273) —
// mapping unconditionally keeps the emitted DDL valid on the whole
// supported floor. The swap is surfaced (never silent): the CREATE
// path WARNs per table, the ALTER paths per column. Semantics note:
// UCA 9.0.0 vs 14.0.0 weights differ for edge-case characters, and the
// uca1400 forms are PAD SPACE where 0900 is NO PAD — the closest
// faithful equivalent, not a byte-identical one. Language-specific
// 0900 collations (utf8mb4_de_pb_0900_ai_ci, …) are deliberately NOT
// mapped: they pass through verbatim and fail loudly on a pre-11.4
// target rather than guess a language table.
var mariadbTargetCollations = map[string]string{
	"utf8mb4_0900_ai_ci": "utf8mb4_uca1400_ai_ci",
	"utf8mb4_0900_as_ci": "utf8mb4_uca1400_as_ci",
	"utf8mb4_0900_as_cs": "utf8mb4_uca1400_as_cs",
	"utf8mb4_0900_bin":   "utf8mb4_nopad_bin", // codepoint binary, NO PAD on both sides
}

// mysqlTargetCollations is the mirror map for the reverse direction: a
// MariaDB source's uca1400 collations (utf8mb4_uca1400_ai_ci is the
// 11.4 server default, so every string column of a default-collation
// 11.4 schema carries it) arriving at a MySQL-family target, which has
// no uca1400 set. Same surfacing and same closest-equivalent caveats
// as [mariadbTargetCollations]; language-specific uca1400 variants
// pass through verbatim and fail loudly.
var mysqlTargetCollations = map[string]string{
	"utf8mb4_uca1400_ai_ci": "utf8mb4_0900_ai_ci",
	"utf8mb4_uca1400_as_ci": "utf8mb4_0900_as_ci",
	"utf8mb4_uca1400_as_cs": "utf8mb4_0900_as_cs",
	"utf8mb4_nopad_bin":     "utf8mb4_0900_bin", // codepoint binary, NO PAD on both sides
}

// crossFlavorCollationRemap returns the collation remap the emitter
// applies for this flavor's target server family, nil when the flavor
// needs none.
func (f Flavor) crossFlavorCollationRemap() map[string]string {
	if f == FlavorMariaDB {
		return mariadbTargetCollations
	}
	// Every other flavor (vanilla / planetscale / vitess) is a MySQL-8
	// family server: fold MariaDB-only collations to their 0900
	// equivalents so mariadb → mysql-family migrations of default-
	// collation schemas don't die on Error 1273.
	return mysqlTargetCollations
}

// mariadbVersionFloorMajor/Minor is the supported MariaDB floor:
// 10.11 LTS. Below it the schema reader WARNs (older MariaDB may work
// but is unpinned — the integration matrix runs 10.11 and 11.4).
const (
	mariadbVersionFloorMajor = 10
	mariadbVersionFloorMinor = 11
)

// parseMariaDBVersion fingerprints a SELECT VERSION() string. MariaDB
// reports e.g. "11.4.12-MariaDB-ubu2404" or "10.11.18-MariaDB-…-log";
// some proxy paths prepend the replication-compat "5.5.5-" prefix,
// which is stripped before the numeric parse. ok is false when the
// string does not identify a MariaDB server.
func parseMariaDBVersion(version string) (major, minor int, ok bool) {
	if !strings.Contains(version, "MariaDB") {
		return 0, 0, false
	}
	v := strings.TrimPrefix(version, "5.5.5-")
	parts := strings.SplitN(v, ".", 3)
	if len(parts) < 2 {
		return 0, 0, true // MariaDB, version shape unknown
	}
	major, _ = strconv.Atoi(parts[0])
	minor, _ = strconv.Atoi(parts[1])
	return major, minor, true
}

// checkServerFlavor is the connect-time fingerprint guard, run by
// OpenSchemaReader / OpenSchemaWriter (the gateway opens every
// migrate / sync / backup / restore / verify run passes through).
// Direction 1 — declared mariadb, server is NOT MariaDB: REFUSED
// (coded): the defaults shim and catalog variants actively mis-read a
// MySQL 8 server (a bare `abc` default would classify as an
// expression), so proceeding would be the silent-corruption class this
// flavor exists to close. Direction 2 — declared plain mysql, server
// fingerprints as MariaDB: WARN steering to the mariadb driver (the
// probe-verified failure that follows — Unknown column 'srs_id' — is
// loud but names SQL, not the real incompatibility). Plain-mysql usage
// is deliberately not hard-refused: the WARN steers, the honest flavor
// is the remedy. VStream flavors skip the probe entirely.
//
// The probe is one SELECT VERSION() round-trip. On the vanilla WARN
// path a probe failure is swallowed (a diagnostics aid must not add a
// failure mode); on the mariadb path it propagates — if VERSION()
// fails the connection is unusable anyway.
func (e Engine) checkServerFlavor(ctx context.Context, db *sql.DB) error {
	if e.Flavor.usesVStream() {
		return nil
	}
	var version string
	err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version)
	if e.Flavor != FlavorMariaDB {
		if err == nil {
			warnMariaDBUnderMySQLDriver(version)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("mariadb: probe server version: %w", err)
	}
	major, minor, isMariaDB := parseMariaDBVersion(version)
	if !isMariaDB {
		return sluicecode.Wrap(
			sluicecode.CodeDriverHostMismatch,
			"use --source-driver/--target-driver mysql for a MySQL-family server",
			fmt.Errorf(
				"mariadb: the server reports version %q, which is not MariaDB — the mariadb flavor's "+
					"catalog queries and COLUMN_DEFAULT normalization are MariaDB-specific and would "+
					"mis-read a MySQL/Percona server's defaults; use --source-driver/--target-driver mysql "+
					"(or planetscale/vitess) for this server",
				version,
			),
		)
	}
	if major < mariadbVersionFloorMajor ||
		(major == mariadbVersionFloorMajor && minor < mariadbVersionFloorMinor) {
		slog.Warn(
			"mariadb: server version is below sluice's supported MariaDB floor (10.11 LTS); "+
				"the catalog conventions this flavor relies on are pinned against 10.11 and 11.4 only — "+
				"proceeding, but behavior on older MariaDB is unverified",
			slog.String("server_version", version),
		)
	}
	return nil
}

// warnMariaDBUnderMySQLDriver emits the steering WARN when a plain
// `mysql`-declared source/target fingerprints as MariaDB. Deliberately
// per-open (schema reader/writer opens are once-per-run-per-role, and
// a fleet process handling several DSNs should surface each).
func warnMariaDBUnderMySQLDriver(version string) {
	if _, _, isMariaDB := parseMariaDBVersion(version); !isMariaDB {
		return
	}
	slog.Warn(
		"mysql: this server is MariaDB — the plain mysql driver reads MySQL-8-only catalog columns "+
			"(srs_id, statistics.expression) and MySQL default conventions, so schema reads will fail "+
			"(Unknown column 'srs_id') and writes use MySQL-8-only upsert syntax; "+
			"use --source-driver/--target-driver mariadb for this server",
		slog.String("server_version", version),
		slog.String("hint", "pass --source-driver mariadb (or --target-driver mariadb) instead of mysql"),
	)
}

// mariadbCDCUnsupportedError is the coded Phase-1 CDC refusal
// (SLUICE-E-CDC-MARIADB-UNSUPPORTED). It fires from OpenCDCReader /
// OpenServerCDCReader and — via [Engine.ExplainCDCUnsupported] — from
// the pipeline's CDC-capability preflights (sync start, backup
// stream/incremental, add-table), so the operator sees the real story
// (MariaDB domain GTIDs, roadmap item 73 Phase 3) and the trigger-less
// alternatives instead of a generic "declares CDC=None".
func mariadbCDCUnsupportedError() error {
	return sluicecode.Wrap(
		sluicecode.CodeCDCMariaDBUnsupported,
		"use `sluice migrate` + application cutover (or backup/restore) until MariaDB CDC ships (roadmap item 73 P3)",
		fmt.Errorf(
			"mariadb: CDC (continuous sync / incremental backup) is not supported yet: MariaDB replicates "+
				"with domain-based GTID positions (e.g. 0-100-38) that sluice's MySQL binlog reader cannot "+
				"parse or resume — MariaDB-native GTID support is roadmap item 73 Phase 3. "+
				"Available today without CDC: bulk `sluice migrate` plus an application cutover, or "+
				"`sluice backup` / `sluice restore` for point-in-time copies: %w",
			ErrNotImplemented,
		),
	)
}

// ExplainCDCUnsupported implements [ir.CDCUnsupportedExplainer]: for
// the mariadb flavor it supplies the coded refusal above; every other
// flavor returns nil so the orchestrator's generic CDC=None message
// (or the flavor's real CDC support) applies.
func (e Engine) ExplainCDCUnsupported() error {
	if e.Flavor == FlavorMariaDB {
		return mariadbCDCUnsupportedError()
	}
	return nil
}
