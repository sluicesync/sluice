// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// ViewCrossDialectMarker is the grep-stable marker of the refusal a view gets
// when its body would cross to another engine family in a form that, before
// Bug 314's fix, could never have been created there.
const ViewCrossDialectMarker = "VIEW-BODY-CROSS-ENGINE"

// RefuseUnportableView reports why view must not be created on a target of
// dialect targetDialect ("mysql", "postgres" or "sqlite"), or nil.
//
// Two reasons, both checked before any data moves:
//
//   - The reader could not make the body portable and recorded why on
//     [ir.View.Refusal] (a MySQL view using its own database's name as an
//     alias).
//   - The body is in another engine's dialect and, until Bug 314's fix, named
//     its schema or database on every table: a MySQL body always, a Postgres
//     body read from a schema other than public. Those bodies could not be
//     created on another engine family (no such database, or SQLite's
//     "cannot reference objects in database"). With the qualifier removed they
//     parse there and compute something else at exit 0: MySQL's `a / b` is
//     3.5000 where SQLite's and Postgres's are 3, `s = 'abc'` matches 'ABC'
//     under a MySQL ci collation and not elsewhere, MySQL's concat(s, NULL) is
//     NULL where SQLite's is the string. So they stay refused, now by name.
//
// A Postgres body read from public, or a SQLite body, crossing engines is left
// to the existing verbatim policy: it never carried a qualifier, so this fix
// changes nothing for it (production-readiness "Cross-engine view bodies are
// emitted verbatim").
func RefuseUnportableView(view *ir.View, targetDialect string) error {
	if view == nil {
		return nil
	}
	if view.Refusal != "" {
		return errors.New(view.Refusal)
	}
	src := view.DefinitionDialect
	if src == "" || src == targetDialect {
		return nil
	}
	if src == "mysql" || (src == "postgres" && view.Schema != "" && view.Schema != "public") {
		return fmt.Errorf("%s: view %q is written in %s's SQL dialect and would be created on a %s target "+
			"verbatim, where the same text can compute different results (integer division, string "+
			"comparison and NULL handling differ between engines). Leave it out with --exclude-view %s "+
			"and create an equivalent view on the target yourself",
			ViewCrossDialectMarker, view.Name, displayDialect(src), displayDialect(targetDialect), view.Name)
	}
	return nil
}

// RefuseUnportableViews is [RefuseUnportableView] over every view in s,
// joining each refusal so the operator sees all of them at once.
func RefuseUnportableViews(s *ir.Schema, targetDialect string) error {
	if s == nil {
		return nil
	}
	var errs []error
	for _, v := range s.Views {
		if err := RefuseUnportableView(v, targetDialect); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func displayDialect(d string) string {
	switch d {
	case "mysql":
		return "MySQL"
	case "postgres":
		return "Postgres"
	case "sqlite":
		return "SQLite"
	}
	return d
}
