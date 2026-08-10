// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// PredictEmittedChecks satisfies [ir.EmittedCheckPredictor]: it names
// the CHECK constraints THIS writer synthesizes for table beyond the
// ones the source declares in [ir.Table.CheckConstraints].
//
// One kind, and it is exactly what [mysqlEmitter.emitTableDefWithDomainChecks]
// inlines: a clause per TRANSLATABLE check on every `ir.Domain`-typed
// column. MySQL has no DOMAIN, so the column lands as the domain's base
// type and its CHECKs survive only as these table-level clauses.
//
// # Gated on the live server, deliberately
//
// The emitter inlines nothing unless the target is MySQL 8.0.16+
// (`w.inlineCheckSupported`, probed by SELECT VERSION() at writer open —
// older servers PARSED and IGNORED CHECK, which would have re-introduced
// the Bug 113 silent-loss class). Predicting a constraint such a server
// never received would invent a permanent "missing on target" line no
// action on the target could close, which is precisely the unclosable-
// drift shape the row-level-security suppression exists to avoid. So the
// prediction asks the same field the emission does.
//
// # The name is a PREDICTION and cannot be the matching key
//
// MySQL auto-names an unnamed inline CHECK `<table>_chk_N`, numbered by
// position among the table's unnamed CHECK clauses — a number the
// expected side cannot know, because it depends on how many
// source-declared unnamed CHECKs precede it. The name below is a
// deterministic synthetic one, used only to render a suggestion for an
// UNMATCHED constraint; the diff matches these by normalized EXPRESSION
// (see [ir.EmittedCheckPredictor]). An operator who runs the rendered
// `ADD CONSTRAINT <name> CHECK (...)` gets a named constraint whose
// expression still matches, so the suggestion converges rather than
// oscillating.
//
// UN-translatable domain CHECKs produce nothing here, matching the
// emitter: they are dropped with the v0.96.2 WARN, so the target
// genuinely does not hold them and there is nothing to reconcile.
func (w *SchemaWriter) PredictEmittedChecks(table *ir.Table) []*ir.CheckConstraint {
	if table == nil || !w.inlineCheckSupported {
		return nil
	}
	var out []*ir.CheckConstraint
	for _, col := range table.Columns {
		// [ir.DomainOf], not [ir.UnwrapDomain]: this asks about the
		// WRAPPER's checks, the same reading the emitter takes.
		dom, ok := ir.DomainOf(col)
		if !ok {
			continue
		}
		for i, chk := range dom.Checks {
			body, ok := w.emitter.domainCheckBodyForMySQL(col.Name, chk)
			if !ok {
				continue
			}
			out = append(out, &ir.CheckConstraint{
				Name:          predictedDomainCheckName(table.Name, col.Name, i),
				Expr:          body,
				ExprDialect:   "mysql",
				SluiceEmitted: true,
			})
		}
	}
	return out
}

// predictedDomainCheckName is the synthetic name a predicted DOMAIN
// CHECK carries. It is NOT what MySQL will call the constraint (see
// [SchemaWriter.PredictEmittedChecks]); it exists so an unmatched
// prediction renders a runnable `ADD CONSTRAINT` suggestion instead of
// an empty identifier. The ordinal disambiguates a domain carrying more
// than one translatable check on the same column.
func predictedDomainCheckName(tableName, columnName string, ordinal int) string {
	if ordinal == 0 {
		return fmt.Sprintf("%s_%s_domain_chk", tableName, columnName)
	}
	return fmt.Sprintf("%s_%s_domain_chk_%d", tableName, columnName, ordinal+1)
}
