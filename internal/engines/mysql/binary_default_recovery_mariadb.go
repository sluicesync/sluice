// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// MariaDB binary-default recovery (GC-36) — the MariaDB sibling of the
// SHOW CREATE recovery in binary_default_recovery.go, for a different loss.
//
// MariaDB does not NUL-truncate a BINARY/VARBINARY literal default the way
// MySQL does; it reports it QUOTED and escape-encoded (translateMariaDBDefault's
// table). But it stores that text in information_schema's utf8mb3 COLUMN_DEFAULT
// column, and every byte that is not part of a well-formed UTF-8 sequence is
// converted to 0x3F ('?') ON THE SERVER, before any client sees it. Measured on
// mariadb 10.6.28, 10.11.19 and 11.4.13, identically:
//
//	declared                         HEX(COLUMN_DEFAULT)      true bytes
//	BINARY(3)    DEFAULT 0x00AB00    '\0?\0'                  00AB00
//	VARBINARY(4) DEFAULT 0xAB00CD    '?\0?'                   AB00CD
//	VARBINARY(6) DEFAULT 0x27FF5C00  '''?\\\0'                27FF5C00
//	VARBINARY(8) DEFAULT 0xF09F9880  '????'                   F09F9880 (utf8mb3 rejects 4-byte UTF-8)
//	VARBINARY(4) DEFAULT 0xDEAD      'ޭ'  (DE AD, valid UTF-8) DEAD     (survives)
//
// The loss is in the stored catalog text, so no session setting recovers it
// there (HEX(COLUMN_DEFAULT) under SET NAMES binary reads the same 0x3F). SHOW
// CREATE TABLE prints the same '?' under the utf8mb4 results charset sluice's
// connections use, and is faithful only under character_set_results=binary —
// session state a pooled *sql.DB cannot scope to one query. A true '?' and a
// lost byte are indistinguishable in the text, so the class cannot be detected
// from the catalog alone; the bytes have to be re-read.
//
// The re-read is a DEFAULT() probe over an empty outer join:
//
//	SELECT HEX(DEFAULT(t.c1)), … FROM (SELECT 1) AS o LEFT JOIN db.tbl AS t ON FALSE
//
// which evaluates each column's stored default without reading a row (the
// table may be empty) and returns it as ASCII hex, so no charset conversion
// touches it. BINARY(N) comes back zero-padded to width, exactly as the catalog
// already reports it, so a lossless default recovers to the identical IR value.
// It needs only SELECT on the table, which the copy needs anyway, so a failed
// probe is an error, not a degraded mode.
//
// Independent evidence: the probed bytes are reconciled against the catalog
// text ([reconcileMariaDBBinaryDefault]) — same length, and equal at every
// position the catalog did not replace with '?'. That binds the probe to the
// column it was meant to read AND asserts the premise above (each lost byte
// becomes exactly one '?'); a server that ever breaks either is refused loudly,
// naming the column, rather than trusted.

// pendingMariaDBBinaryDefault is one MariaDB BINARY/VARBINARY column whose
// quoted catalog default must be re-read by the DEFAULT() probe.
type pendingMariaDBBinaryDefault struct {
	table, column string
	// catalog is the raw quoted COLUMN_DEFAULT the probe is reconciled against.
	catalog string
	// set receives the recovered bytes as the `0x<HEX>` literal; each caller
	// stores it in its own shape (an IR Default, the door's compared fact).
	set func(hexLiteral string)
}

// setIRDefault is the schema readers' setter: the byte-exact hexLiteralDialect
// DefaultExpression — the IR value translateMariaDBDefault produces for a
// lossless default and the MySQL recovery produces for the same declared DDL.
func setIRDefault(col *ir.Column) func(string) {
	return func(hexLiteral string) {
		col.Default = ir.DefaultExpression{Expr: hexLiteral, Dialect: hexLiteralDialect}
	}
}

// mariadbBinaryDefaultNeedsProbe reports whether a MariaDB column's catalog
// default is a non-empty quoted literal on a binary-family column — the only
// shape whose bytes the catalog can have replaced with '?'. An empty default
// carries no bytes to lose; expression text (concat(0xab,…)) keeps its literals in hex;
// a BLOB default is reported as bare hex (0xabcd). All measured on 11.4.
func mariadbBinaryDefaultNeedsProbe(isBinary bool, def sql.NullString) bool {
	return isBinary && def.Valid && strings.HasPrefix(def.String, "'") && def.String != "''"
}

// recoverMariaDBBinaryDefaults re-reads every pending column's default with one
// DEFAULT() probe per table, reconciles it against the catalog text, and hands
// the byte-exact `0x<HEX>` literal to the column's setter, replacing the
// provisional (possibly '?'-mangled) value.
func recoverMariaDBBinaryDefaults(ctx context.Context, db *sql.DB, schema string, pending []pendingMariaDBBinaryDefault) error {
	byTable := map[string][]pendingMariaDBBinaryDefault{}
	for _, p := range pending {
		byTable[p.table] = append(byTable[p.table], p)
	}
	for _, table := range sortedKeys(byTable) {
		ps := byTable[table]
		names := make([]string, len(ps))
		for i, p := range ps {
			names[i] = p.column
		}
		got, err := probeMariaDBColumnDefaults(ctx, db, schema, table, names)
		if err != nil {
			return err
		}
		for i, p := range ps {
			if err := reconcileMariaDBBinaryDefault(p.catalog, got[i]); err != nil {
				return fmt.Errorf("mysql: mariadb binary default of %s.%s.%s: %w", schema, table, p.column, err)
			}
			p.set(bytesToHexLiteral(got[i]))
		}
	}
	return nil
}

// probeMariaDBColumnDefaults evaluates DEFAULT(col) for each named column of
// schema.table in one statement and returns the raw bytes in cols' order. The
// empty outer join yields exactly one row whatever the table holds; HEX keeps
// the result ASCII so the connection's result charset cannot mangle it.
func probeMariaDBColumnDefaults(ctx context.Context, db *sql.DB, schema, table string, cols []string) ([][]byte, error) {
	exprs := make([]string, len(cols))
	for i, c := range cols {
		exprs[i] = "HEX(DEFAULT(`sluice_t`.`" + mysqlQuoteIdent(c) + "`))"
	}
	q := "SELECT " + strings.Join(exprs, ", ") +
		" FROM (SELECT 1) AS `sluice_one` LEFT JOIN `" + mysqlQuoteIdent(schema) + "`.`" + mysqlQuoteIdent(table) +
		"` AS `sluice_t` ON FALSE"
	hexes := make([]sql.NullString, len(cols))
	dest := make([]any, len(cols))
	for i := range hexes {
		dest[i] = &hexes[i]
	}
	if err := db.QueryRowContext(ctx, q).Scan(dest...); err != nil {
		return nil, fmt.Errorf("mysql: probe the true bytes of %s.%s's binary column defaults (DEFAULT() read; "+
			"information_schema replaces every non-UTF-8 byte with '?'): %w", schema, table, err)
	}
	out := make([][]byte, len(cols))
	for i, h := range hexes {
		if !h.Valid {
			return nil, fmt.Errorf("mysql: DEFAULT(%s) on %s.%s is NULL, but information_schema reports a literal default",
				cols[i], schema, table)
		}
		b, err := hex.DecodeString(h.String)
		if err != nil {
			return nil, fmt.Errorf("mysql: DEFAULT(%s) on %s.%s: undecodable HEX %q: %w", cols[i], schema, table, h.String, err)
		}
		out[i] = b
	}
	return out, nil
}

// reconcileMariaDBBinaryDefault checks probed bytes against the catalog's quoted
// rendering of the same default: equal length, and equal at every byte the
// catalog did not replace with '?'. A catalog string that does not decode is a
// refusal too — there is nothing to reconcile against.
func reconcileMariaDBBinaryDefault(catalog string, probed []byte) error {
	shown, end, ok := scanMySQLQuotedString(catalog)
	if !ok || end != len(catalog) {
		return fmt.Errorf("information_schema default %q is not a single quoted literal", catalog)
	}
	if len(shown) != len(probed) {
		return fmt.Errorf("DEFAULT() read %d bytes (0x%X) but information_schema shows %d (%q)",
			len(probed), probed, len(shown), catalog)
	}
	for i := range shown {
		if shown[i] != probed[i] && shown[i] != '?' {
			return fmt.Errorf("DEFAULT() read 0x%X, which disagrees with information_schema's %q at byte %d "+
				"(a byte the catalog did not replace with '?')", probed, catalog, i)
		}
	}
	return nil
}
