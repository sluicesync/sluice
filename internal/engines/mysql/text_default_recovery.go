// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"unicode/utf8"

	"sluicesync.dev/sluice/internal/ir"
)

// Character-column literal-default recovery (GC-37 (h)) — the text sibling
// of the binary-default recoveries in binary_default_recovery.go and
// binary_default_recovery_mariadb.go, for a loss both flavors share.
//
// information_schema.COLUMNS.COLUMN_DEFAULT is a utf8mb3 column, and
// utf8mb3 cannot hold a character outside the Basic Multilingual Plane
// (anything encoded in four UTF-8 bytes: emoji, many CJK extension
// ideographs, historic scripts). The server replaces each such character
// with '?' when it stores the catalog text, before any client sees it.
// Measured on MySQL 8.0.46 and MariaDB 11.4.13, identically in kind:
//
//	declared                                  COLUMN_DEFAULT       true value
//	VARCHAR(20) DEFAULT '😀x'   (utf8mb4)      ?x    / '?x'         😀x
//	CHAR(4)     DEFAULT '😀'                   ?     / '?'          😀
//	TEXT        DEFAULT '😀z'   (MariaDB)            / '????z'      😀z   (one '?' per BYTE there)
//	VARCHAR(10) CHARSET utf16 DEFAULT '😀w'    ?w    / '?w'         😀w
//	VARCHAR(5)  DEFAULT '?'                    ?     / '?'          ?     (a genuine '?')
//	VARCHAR(10) DEFAULT 'é€'                   é€    / 'é€'         é€    (BMP — faithful)
//
// (MySQL / MariaDB spellings; MariaDB quotes a literal default.) So a text
// default that reads back with a '?' may be lossy and one that reads back
// without one is not: the '?' is the whole trigger, and a genuine '?' costs
// one extra query per table, which confirms it. The loss is in the stored
// catalog text, so no session charset recovers it there, and SHOW CREATE
// TABLE is no better (MySQL prints a 0x… literal in the COLUMN's charset —
// UTF-16 for a utf16 column — and MariaDB prints the same '?').
//
// The re-read is the DEFAULT() probe the MariaDB binary recovery already
// uses, converted to utf8mb4 so every column charset comes back as UTF-8:
//
//	SELECT HEX(CONVERT(DEFAULT(t.c) USING utf8mb4)), … FROM (SELECT 1) AS o LEFT JOIN db.tbl AS t ON FALSE
//
// It works on vanilla MySQL and MariaDB, on an empty table, with SELECT on
// the table. vtgate rejects it, so the PlanetScale and Vitess flavors read
// SHOW CREATE TABLE instead ([readTextDefaults] has the measurements and
// the one refusal that path adds).
//
// Independent evidence: the probed value is reconciled against the catalog
// text ([reconcileTextDefault]) — equal character for character, except
// that a run of '?' in the catalog may stand for ONE supplementary
// character (1 to 4 '?' — MariaDB's TEXT columns lose one per byte). That
// binds the probe to the column it was meant to read and asserts the
// premise above; a probe that disagrees is refused, naming the column.
//
// ENUM and SET: the probed default is faithful, but the column's LABELS are
// read from COLUMN_TYPE, which loses them the same way (and SHOW CREATE
// with them, even under character_set_results=binary — measured). A
// default that is not one of the labels as read means the type itself
// could not be read faithfully, and that is refused rather than carried
// ([checkEnumSetDefault]); see GC-37 (i) for the labels.

// pendingTextDefault is one character column whose literal default read
// back from information_schema with a '?' and must be re-read.
type pendingTextDefault struct {
	table, column string
	// catalog is the default's text as information_schema reported it,
	// decoded from the flavor's literal spelling (MariaDB's quotes removed).
	catalog string
	// charset is the column's CHARACTER_SET_NAME (see [readTextDefaults]).
	charset string
	// check, when non-nil, vets the recovered value against the column's
	// type (the ENUM/SET label check).
	check func(recovered string) error
	// set receives the recovered text; each caller stores it in its own
	// shape (an IR Default, the door's compared fact).
	set func(recovered string)
}

// textDefaultCatalogText returns the literal text a character column's
// catalog default carries, and whether it must be re-read: a non-NULL
// literal (never an expression) on a column with a character set, whose
// text contains a '?'. charset is information_schema's CHARACTER_SET_NAME
// ("" for every non-character column, binary strings included).
func textDefaultCatalogText(flavor Flavor, charset, extra string, def sql.NullString) (string, bool) {
	if !def.Valid || charset == "" {
		return "", false
	}
	text := def.String
	if flavor == FlavorMariaDB {
		// MariaDB quotes a literal default; anything unquoted (the bare
		// NULL keyword, a number, an expression) is not a text literal.
		if !strings.HasPrefix(text, "'") {
			return "", false
		}
		val, end, ok := scanMySQLQuotedString(text)
		if !ok || end != len(text) {
			return "", false
		}
		text = string(val)
	} else if strings.Contains(strings.ToUpper(extra), "DEFAULT_GENERATED") {
		// An expression default: its text is GC-37 (a)'s, not a literal.
		return "", false
	}
	return text, strings.ContainsRune(text, '?')
}

// setTextIRDefault is the schema readers' setter: the recovered text as the
// literal default the catalog would have reported had it been able to.
func setTextIRDefault(col *ir.Column) func(string) {
	return func(recovered string) {
		col.Default = ir.DefaultLiteral{Value: recovered}
	}
}

// textDefaultFact is the unforwarded door's setter shape: the recovered text
// in the spelling the flavor's catalog uses for a literal it CAN hold
// (MySQL bare, MariaDB single-quoted with each quote doubled), so the operator sees
// a familiar default in a refusal. The door compares two reads made the same
// way — the probe fires on the catalog text, so one true value is either
// recovered on both reads or on neither — so the spelling only has to be
// stable, not a byte-exact replica of what MariaDB would print.
func textDefaultFact(flavor Flavor, recovered string) string {
	if flavor == FlavorMariaDB {
		return "'" + strings.ReplaceAll(recovered, "'", "''") + "'"
	}
	return recovered
}

// enumSetDefaultCheck returns the ENUM/SET label check for typ, nil for any
// other type.
func enumSetDefaultCheck(typ ir.Type) func(string) error {
	switch t := typ.(type) {
	case ir.Enum:
		return func(v string) error { return checkEnumSetDefault(v, t.Values, false) }
	case ir.Set:
		return func(v string) error { return checkEnumSetDefault(v, t.Values, true) }
	}
	return nil
}

// checkEnumSetDefault refuses a recovered ENUM/SET default that is not a
// label of the column's type as read (for a SET, every comma-separated
// member): the labels came from COLUMN_TYPE, which loses supplementary
// characters exactly as COLUMN_DEFAULT does, so a mismatch means the TYPE
// was not read faithfully either.
func checkEnumSetDefault(v string, labels []string, isSet bool) error {
	known := make(map[string]bool, len(labels))
	for _, l := range labels {
		known[l] = true
	}
	members := []string{v}
	if isSet {
		members = []string{}
		if v != "" {
			members = strings.Split(v, ",")
		}
	}
	for _, m := range members {
		if !known[m] {
			return fmt.Errorf("its default %q is not one of the labels information_schema reports (%q): the labels "+
				"hold characters outside the Basic Multilingual Plane, which information_schema stores as '?', so "+
				"sluice cannot read this column's type faithfully (GC-37 (i))", v, labels)
		}
	}
	return nil
}

// recoverTextDefaults re-reads every pending column's default — one read
// per table, see [readTextDefaults] — reconciles it against the catalog
// text, and hands the recovered text to the column's setter. Any failure is
// an error: the read needs only what the copy needs anyway, and carrying
// the '?' text instead is the silent loss this exists to prevent.
func recoverTextDefaults(ctx context.Context, db *sql.DB, schema string, flavor Flavor, pending []pendingTextDefault) error {
	byTable := map[string][]pendingTextDefault{}
	for _, p := range pending {
		byTable[p.table] = append(byTable[p.table], p)
	}
	for _, table := range sortedKeys(byTable) {
		ps := byTable[table]
		got, err := readTextDefaults(ctx, db, schema, table, flavor, ps)
		if err != nil {
			return err
		}
		for i, p := range ps {
			where := fmt.Sprintf("%s.%s.%s", schema, table, p.column)
			if !utf8.Valid(got[i]) {
				return fmt.Errorf("mysql: text default of %s: the re-read returned 0x%X, which is not UTF-8", where, got[i])
			}
			recovered := string(got[i])
			if err := reconcileTextDefault(p.catalog, recovered); err != nil {
				return fmt.Errorf("mysql: text default of %s: %w", where, err)
			}
			if p.check != nil {
				if err := p.check(recovered); err != nil {
					return fmt.Errorf("mysql: column %s: %w", where, err)
				}
			}
			p.set(recovered)
		}
	}
	return nil
}

// readTextDefaults returns the true text of each pending column's default,
// as UTF-8 bytes in ps's order, from ONE read of table.
//
// Vanilla MySQL and MariaDB: the utf8mb4 DEFAULT() probe, which returns
// every column charset as UTF-8.
//
// PlanetScale and self-hosted Vitess read the catalog through vtgate, whose
// parser rejects that probe (measured on vitess/vttestserver:mysql80:
// "syntax error at position 39"; the simpler `SELECT DEFAULT(c) FROM t`
// parses but needs a row). vtgate passes SHOW CREATE TABLE through to the
// tablet's mysqld, which prints a default it cannot hold in utf8mb3 as a
// 0x… literal of the column's own bytes (measured through vtgate:
// `DEFAULT 0xF09F988078` for '😀x'). Those bytes are UTF-8 only for a
// utf8-family column, so any other charset is taken only when SHOW CREATE
// agrees with the catalog exactly (a genuine '?', printed quoted) and is
// otherwise refused, naming the column — sluice does not transcode a
// column charset here, and a utf16 default holding an emoji is rare enough
// that a loud refusal is the right trade for no guess.
func readTextDefaults(ctx context.Context, db *sql.DB, schema, table string, flavor Flavor, ps []pendingTextDefault) ([][]byte, error) {
	if !flavor.usesVStream() {
		names := make([]string, len(ps))
		for i, p := range ps {
			names[i] = p.column
		}
		return probeColumnDefaults(ctx, db, schema, table, names, true)
	}
	sr := &SchemaReader{db: db, schema: schema, flavor: flavor}
	ddl, err := sr.showCreateTable(ctx, table)
	if err != nil {
		return nil, fmt.Errorf("mysql: re-read %s.%s's text column defaults from SHOW CREATE TABLE "+
			"(information_schema stores what utf8mb3 cannot hold as '?'): %w", schema, table, err)
	}
	out := make([][]byte, len(ps))
	for i, p := range ps {
		raw, ok := parseShowCreateColumnDefault(ddl, p.column)
		if !ok {
			return nil, fmt.Errorf("mysql: text default of %s.%s.%s: SHOW CREATE TABLE carries no DEFAULT clause sluice can "+
				"decode for it, and information_schema's %q may hold '?' in place of characters it cannot store",
				schema, table, p.column, p.catalog)
		}
		if !utf8FamilyCharset(p.charset) && string(raw) != p.catalog {
			return nil, fmt.Errorf("mysql: text default of %s.%s.%s: the column's charset is %s, SHOW CREATE TABLE prints "+
				"its default as that charset's bytes (0x%X), and sluice decodes only UTF-8 there; information_schema's "+
				"%q holds '?' in place of characters it cannot store, so neither reading is carried",
				schema, table, p.column, p.charset, raw, p.catalog)
		}
		out[i] = raw
	}
	return out, nil
}

// utf8FamilyCharset reports a MySQL character set whose bytes are UTF-8.
func utf8FamilyCharset(charset string) bool {
	switch strings.ToLower(charset) {
	case "utf8mb4", "utf8mb3", "utf8":
		return true
	}
	return false
}

// maxQuestionMarksPerLostChar is the most '?' information_schema writes for
// one supplementary character: one per UTF-8 byte (MariaDB TEXT columns).
const maxQuestionMarksPerLostChar = 4

// reconcileTextDefault checks that recovered is what information_schema
// would have shown as catalog: character for character equal, except that
// each supplementary character (above U+FFFF) of recovered may appear in
// catalog as a run of 1 to 4 '?'. A genuine '?' in recovered matches only a
// '?'. Small inputs (a column default), so the search memoizes positions.
func reconcileTextDefault(catalog, recovered string) error {
	c := []rune(catalog)
	r := []rune(recovered)
	memo := map[[2]int]bool{}
	var match func(i, j int) bool
	match = func(i, j int) bool {
		if i == len(c) && j == len(r) {
			return true
		}
		key := [2]int{i, j}
		if v, ok := memo[key]; ok {
			return v
		}
		ok := false
		if j < len(r) && r[j] > 0xFFFF {
			for k := 1; k <= maxQuestionMarksPerLostChar && i+k <= len(c) && c[i+k-1] == '?'; k++ {
				if match(i+k, j+1) {
					ok = true
					break
				}
			}
		} else if i < len(c) && j < len(r) && c[i] == r[j] {
			ok = match(i+1, j+1)
		}
		memo[key] = ok
		return ok
	}
	if !match(0, 0) {
		return fmt.Errorf("DEFAULT() read %q, which does not agree with information_schema's %q "+
			"(only a character outside the Basic Multilingual Plane may read back as '?')", recovered, catalog)
	}
	return nil
}
