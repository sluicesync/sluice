// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import "strings"

// viewQualifierAmbiguousMarker is the grep-stable marker of the refusal a
// view gets when its body uses its own database's name as an alias.
const viewQualifierAmbiguousMarker = "VIEW-QUALIFIER-AMBIGUOUS"

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
// A backquoted identifier spelling exactly the source database's name is
// removed, with its dot, when it is directly followed by a dot and another
// backquoted identifier and is not itself the second part of a name. The
// identifier after a removed qualifier is copied as is, never tested again,
// so a table named like its database (`app`.`app`.`x`) loses one qualifier,
// not two. References to other databases stay qualified: they point outside
// the migration, and rewriting them would guess. String literals are copied
// untouched.
//
// The body is not rewritten at all, and ambiguous reports true, when the
// database's name is used as anything else, other than a column alias after
// AS: a table alias (`app`.`t` `app`), a derived table's or a CTE's name. Then
// `app`.`id` may be the alias's column, and removing "`app`." would re-bind
// it, inside a correlated subquery, to a column of the inner table at exit 0.
// The caller refuses such a view rather than guess.
func stripOwnSchemaQualifier(definition, schema string) (stripped string, ambiguous bool) {
	if schema == "" {
		return definition, false
	}
	ident := "`" + strings.ReplaceAll(schema, "`", "``") + "`"
	if !strings.Contains(definition, ident) {
		return definition, false
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
			end := skipBackquoted(definition, i)
			if definition[i:end] != ident || followsDot(definition, i) {
				b.WriteString(definition[i:end])
				i = end
				continue
			}
			if strings.HasPrefix(definition[end:], ".`") {
				// The qualifier: drop it, and copy the name it qualifies.
				next := skipBackquoted(definition, end+1)
				b.WriteString(definition[end+1 : next])
				i = next
				continue
			}
			if !followsAS(definition, i) {
				return definition, true
			}
			b.WriteString(definition[i:end])
			i = end
		default:
			b.WriteByte(c)
			i++
		}
	}
	return b.String(), false
}

// followsDot reports whether the token starting at s[i] is preceded, past
// any spaces, by a dot: the second or third part of a dotted name.
func followsDot(s string, i int) bool {
	j := i - 1
	for j >= 0 && s[j] == ' ' {
		j--
	}
	return j >= 0 && s[j] == '.'
}

// followsAS reports whether the token starting at s[i] is preceded by the
// keyword AS, which in a stored MySQL view body introduces a column alias.
func followsAS(s string, i int) bool {
	j := i - 1
	for j >= 0 && s[j] == ' ' {
		j--
	}
	if j < 1 || !strings.EqualFold(s[j-1:j+1], "as") {
		return false
	}
	return j < 2 || !isIdentByte(s[j-2])
}

func isIdentByte(c byte) bool {
	return c == '_' || c == '$' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80
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
