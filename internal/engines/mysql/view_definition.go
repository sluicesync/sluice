package mysql

import "strings"

// stripOwnSchemaQualifier removes the source database's own name from the
// identifiers of a view body, so the view resolves against the database it
// is created in rather than the one it was read from (Bug 314).
//
// MySQL stores information_schema.VIEWS.VIEW_DEFINITION fully qualified:
// `CREATE VIEW v AS SELECT a FROM t` in database app reads back as
// select `app`.`t`.`a` AS `a` from `app`.`t`. Emitted verbatim on a target
// whose database has another name, that body either fails (no database
// app) or, when the target server also holds a database named app, reads
// app's tables at exit 0 — a view on the target serving the source's data.
//
// Only a backquoted identifier spelling exactly the source database's name
// and followed directly by a dot and another backquoted identifier is
// removed. References to other databases are left qualified: they point
// outside the migration, and rewriting them would guess. String literals
// (single- or double-quoted, with backslash escapes and doubled quotes) are
// copied untouched, so a literal that happens to spell `app`.`t` survives.
func stripOwnSchemaQualifier(definition, schema string) string {
	if schema == "" {
		return definition
	}
	qualifier := "`" + strings.ReplaceAll(schema, "`", "``") + "`."
	if !strings.Contains(definition, qualifier) {
		return definition
	}
	var b strings.Builder
	b.Grow(len(definition))
	for i := 0; i < len(definition); {
		switch c := definition[i]; c {
		case '\'', '"':
			end := skipQuotedLiteral(definition, i)
			b.WriteString(definition[i:end])
			i = end
		case '`':
			if strings.HasPrefix(definition[i:], qualifier) && strings.HasPrefix(definition[i+len(qualifier):], "`") {
				i += len(qualifier)
				continue
			}
			end := skipBackquoted(definition, i)
			b.WriteString(definition[i:end])
			i = end
		default:
			b.WriteByte(c)
			i++
		}
	}
	return b.String()
}

// skipBackquoted returns the index just past the backquoted identifier
// opening at s[start], where a doubled backquote is a literal one.
func skipBackquoted(s string, start int) int {
	for i := start + 1; i < len(s); i++ {
		if s[i] == '`' {
			if i+1 < len(s) && s[i+1] == '`' {
				i++
				continue
			}
			return i + 1
		}
	}
	return len(s)
}
