// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Text-render surfaces for the schema-diff deltas that the comparison
// could see and the renderer could not say: foreign keys and EXCLUDE
// constraints (audit 2026-08-04 C-7 / Bug 227), row-level security and
// standalone sequences (audit 2026-08-04 B-10).
//
// Every renderer here follows the convention [renderIndexMismatch]
// established for the index half of roadmap item 125: LEAD WITH THE
// CONSEQUENCE, not the attribute. An operator reading a diff is deciding
// urgency, and "the target ACCEPTS ROWS THE SOURCE REJECTS" is the
// sentence that decides it — `on_delete: RESTRICT -> CASCADE` is not.
// Suggestions are comments rather than paste-ready DDL wherever applying
// them to a live target takes a lock or can fail against existing rows.

import (
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	irdiff "sluicesync.dev/sluice/internal/ir/diff"
)

// renderForeignKeySection writes every foreign-key delta for one table.
//
// The whole section was missing until now: v0.112.0 added the comparison
// and wired [irdiff.SchemaDiff.HasChanges], so the command exited 1 and
// printed NOTHING about why. The index half of the same item enumerated
// its render surfaces; the FK half swept the comparison and not the
// rendering.
func renderForeignKeySection(sb *strings.Builder, td irdiff.TableDiff, quote func(string) string, expected *ir.Schema) {
	for _, name := range td.ForeignKeysMissing {
		fmt.Fprintf(sb, "-- foreign key %s is missing on the target, so the target ACCEPTS ORPHAN ROWS THE SOURCE REJECTS\n",
			quote(name))
		if fk := lookupForeignKey(expected, td.Name, name); fk != nil {
			fmt.Fprintf(sb, "ALTER TABLE %s ADD CONSTRAINT %s %s;\n",
				quote(td.Name), quote(name), renderFKClause(fk, quote))
			continue
		}
		fmt.Fprintf(sb, "-- expected definition unavailable for an ADD CONSTRAINT suggestion on %s\n", quote(name))
	}

	for _, name := range td.ForeignKeysExtra {
		fmt.Fprintf(sb, "ALTER TABLE %s DROP CONSTRAINT %s; -- foreign key not in source schema\n",
			quote(td.Name), quote(name))
	}

	for _, fd := range td.ForeignKeysMismatched {
		renderForeignKeyMismatch(sb, td.Name, fd, quote)
	}

	if td.ForeignKeysUnnamed > 0 {
		// A coverage statement, not a drift statement. Reported because a
		// clean FK report that silently skipped N constraints is exactly
		// the kind of gap that makes an "in sync" answer misleading.
		fmt.Fprintf(sb, "-- NOTE: %d foreign key(s) on %s carry no constraint name and were NOT compared\n",
			td.ForeignKeysUnnamed, quote(td.Name))
		fmt.Fprintln(sb, "--   ^ an unnamed FK has no stable identity to match across two schema reads; name it at the source to bring it into the comparison")
	}
}

// renderForeignKeyMismatch writes the suggestion for a foreign key that
// exists on both sides under one name and enforces something different.
func renderForeignKeyMismatch(sb *strings.Builder, table string, fd irdiff.ForeignKeyDiff, quote func(string) string) {
	fmt.Fprintf(sb, "-- foreign key %s on %s differs between source and target:\n",
		quote(fd.Name), quote(table))

	if fd.ExpectedReference != "" || fd.ActualReference != "" {
		fmt.Fprintf(sb, "--   reference:    source %s   target %s\n", fd.ExpectedReference, fd.ActualReference)
		fmt.Fprintln(sb, "--   ^ the constraint points at a DIFFERENT parent or different columns, so the two sides "+
			"do not constrain the same relationship at all")
	}
	if fd.ExpectedOnDelete != "" || fd.ActualOnDelete != "" {
		fmt.Fprintf(sb, "--   on delete:    source %s   target %s\n", fd.ExpectedOnDelete, fd.ActualOnDelete)
		renderReferentialActionConsequence(sb, "deleted", fd.ExpectedOnDelete, fd.ActualOnDelete)
	}
	if fd.ExpectedOnUpdate != "" || fd.ActualOnUpdate != "" {
		fmt.Fprintf(sb, "--   on update:    source %s   target %s\n", fd.ExpectedOnUpdate, fd.ActualOnUpdate)
		renderReferentialActionConsequence(sb, "updated", fd.ExpectedOnUpdate, fd.ActualOnUpdate)
	}
	if fd.ExpectedMatch != "" || fd.ActualMatch != "" {
		fmt.Fprintf(sb, "--   match:        source %s   target %s\n", fd.ExpectedMatch, fd.ActualMatch)
		if fd.ExpectedMatch == "FULL" && fd.ActualMatch != "FULL" {
			fmt.Fprintln(sb, "--   ^ the source is MATCH FULL and the target is not, so the target ACCEPTS "+
				"PARTIALLY-NULL COMPOSITE KEYS THE SOURCE REJECTS")
		}
	}
	if fd.DeferrabilityMismatched {
		fmt.Fprintf(sb, "--   deferrable:   source %v (initially deferred %v)   target %v (initially deferred %v)\n",
			fd.ExpectedDeferrable, fd.ExpectedInitiallyDeferred, fd.ActualDeferrable, fd.ActualInitiallyDeferred)
		if fd.ExpectedDeferrable && !fd.ActualDeferrable {
			fmt.Fprintln(sb, "--   ^ the source defers the check to COMMIT and the target does not, so the target "+
				"REJECTS A TRANSACTION ORDERING THE SOURCE ACCEPTS")
		}
	}
	// UPR-1. Rendered for the same reason deferrability is: this is the third
	// strength axis, and the asymmetry matters to whoever is reading. A target
	// that VALIDATES where the source does not rejects writes the source
	// accepts; the reverse is a target quietly holding rows nothing checked.
	if fd.ValidityMismatched {
		fmt.Fprintf(sb, "--   validated:    source %v   target %v\n", !fd.ExpectedNotValid, !fd.ActualNotValid)
		if fd.ExpectedNotValid && !fd.ActualNotValid {
			fmt.Fprintln(sb, "--   ^ the source leaves this constraint UNVALIDATED and the target enforces it, "+
				"so the target REJECTS ROWS THE SOURCE HOLDS TODAY")
		}
		if !fd.ExpectedNotValid && fd.ActualNotValid {
			fmt.Fprintln(sb, "--   ^ the target carries this constraint as NOT VALID, so rows already on the "+
				"target were NEVER CHECKED against it")
		}
	}
	fmt.Fprintf(sb, "-- review before acting; re-adding %s revalidates every existing row\n", quote(fd.Name))
}

// renderReferentialActionConsequence names what a referential-action
// divergence DOES to the child rows. CASCADE is the one direction that
// destroys data without an error, so it is called out explicitly rather
// than left for the operator to infer from the keyword pair.
func renderReferentialActionConsequence(sb *strings.Builder, verb, expected, actual string) {
	switch {
	case actual == "CASCADE" && expected != "CASCADE":
		fmt.Fprintf(sb, "--   ^ the target CASCADEs where the source does not: a parent row %s on the target "+
			"SILENTLY DESTROYS child rows the source would have protected\n", verb)
	case expected == "CASCADE" && actual != "CASCADE":
		fmt.Fprintf(sb, "--   ^ the source CASCADEs and the target does not, so the target REFUSES a parent-row "+
			"%s the source performs\n", verb)
	case actual == "SET NULL" && expected != "SET NULL":
		fmt.Fprintf(sb, "--   ^ the target NULLs the referencing columns where the source does not: a parent row "+
			"%s on the target silently detaches its children\n", verb)
	}
}

// renderFKClause renders the FOREIGN KEY clause of an ADD CONSTRAINT for
// a missing-on-target foreign key, carrying every attribute the
// comparison treats as load-bearing so the suggestion reproduces the
// source's constraint rather than a weaker one.
func renderFKClause(fk *ir.ForeignKey, quote func(string) string) string {
	var sb strings.Builder
	ref := fk.ReferencedTable
	if fk.ReferencedSchema != "" {
		ref = fk.ReferencedSchema + "." + fk.ReferencedTable
	}
	fmt.Fprintf(&sb, "FOREIGN KEY (%s) REFERENCES %s (%s)",
		quoteJoin(fk.Columns, quote), quote(ref), quoteJoin(fk.ReferencedColumns, quote))
	if fk.Match == ir.FKMatchFull {
		sb.WriteString(" MATCH FULL")
	}
	if fk.OnDelete != ir.FKActionNoAction {
		fmt.Fprintf(&sb, " ON DELETE %s", fk.OnDelete)
	}
	if fk.OnUpdate != ir.FKActionNoAction {
		fmt.Fprintf(&sb, " ON UPDATE %s", fk.OnUpdate)
	}
	if fk.Deferrable {
		sb.WriteString(" DEFERRABLE")
		if fk.InitiallyDeferred {
			sb.WriteString(" INITIALLY DEFERRED")
		}
	}
	return sb.String()
}

// quoteJoin quotes each identifier and joins them with ", ".
func quoteJoin(names []string, quote func(string) string) string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = quote(n)
	}
	return strings.Join(out, ", ")
}

// lookupForeignKey returns the named foreign key on the named table
// within s, or nil. Mirrors [lookupCheckExpr].
func lookupForeignKey(s *ir.Schema, tableName, fkName string) *ir.ForeignKey {
	if s == nil {
		return nil
	}
	for _, t := range s.Tables {
		if t == nil || t.Name != tableName {
			continue
		}
		for _, fk := range t.ForeignKeys {
			if fk != nil && fk.Name == fkName {
				return fk
			}
		}
	}
	return nil
}

// renderExcludeSection writes every EXCLUDE-constraint delta for one
// table. Same omission as the FK section and found by the same sweep:
// ADR-0053 added the comparison, and neither the renderer nor the JSON
// counts ever grew a surface for it.
func renderExcludeSection(sb *strings.Builder, td irdiff.TableDiff, quote func(string) string, expected *ir.Schema) {
	for _, name := range td.ExcludesMissing {
		fmt.Fprintf(sb, "-- EXCLUDE constraint %s is missing on the target, so the target ACCEPTS OVERLAPPING ROWS THE SOURCE REJECTS\n",
			quote(name))
		if def := lookupExcludeDefinition(expected, td.Name, name); def != "" {
			fmt.Fprintf(sb, "ALTER TABLE %s ADD CONSTRAINT %s %s;\n", quote(td.Name), quote(name), def)
			continue
		}
		fmt.Fprintf(sb, "-- expected definition unavailable for an ADD CONSTRAINT suggestion on %s\n", quote(name))
	}
	for _, name := range td.ExcludesExtra {
		fmt.Fprintf(sb, "ALTER TABLE %s DROP CONSTRAINT %s; -- EXCLUDE not in source schema\n",
			quote(td.Name), quote(name))
	}
	for _, ed := range td.ExcludesMismatched {
		fmt.Fprintf(sb, "-- EXCLUDE %s on %s differs between source and target:\n", quote(ed.Name), quote(td.Name))
		fmt.Fprintf(sb, "--   source: %s\n", ed.ExpectedDefinition)
		fmt.Fprintf(sb, "--   target: %s\n", ed.ActualDefinition)
		fmt.Fprintln(sb, "--   ^ the two sides forbid DIFFERENT overlaps; review which rows each admits before reconciling")
	}
}

// lookupExcludeDefinition returns the pg_get_constraintdef text for the
// named EXCLUDE constraint, or "".
func lookupExcludeDefinition(s *ir.Schema, tableName, name string) string {
	if s == nil {
		return ""
	}
	for _, t := range s.Tables {
		if t == nil || t.Name != tableName {
			continue
		}
		for _, ec := range t.ExcludeConstraints {
			if ec != nil && ec.Name == name {
				return ec.Definition
			}
		}
	}
	return ""
}

// renderRowLevelSecuritySection writes the RLS flag delta and the policy
// deltas for one table (audit 2026-08-04 B-10).
//
// This is the highest-consequence section in the diff and it renders
// first among equals for that reason: a target with row-level security
// switched off does not fail, does not warn, and returns every tenant's
// rows to every tenant.
func renderRowLevelSecuritySection(sb *strings.Builder, td irdiff.TableDiff, quote func(string) string) {
	if td.RLSMismatched {
		fmt.Fprintf(sb, "-- row-level security on %s differs between source and target:\n", quote(td.Name))
		fmt.Fprintf(sb, "--   enabled:  source %v   target %v\n", td.ExpectedRLSEnabled, td.ActualRLSEnabled)
		fmt.Fprintf(sb, "--   forced:   source %v   target %v\n", td.ExpectedRLSForced, td.ActualRLSForced)
		switch {
		case td.ExpectedRLSEnabled && !td.ActualRLSEnabled:
			fmt.Fprintln(sb, "--   ^ the target enforces NO row-level security: every policy on it is INERT and "+
				"EVERY ROW IS VISIBLE TO EVERY ROLE that can read the table")
			fmt.Fprintf(sb, "ALTER TABLE %s ENABLE ROW LEVEL SECURITY;\n", quote(td.Name))
			if td.ExpectedRLSForced {
				fmt.Fprintf(sb, "ALTER TABLE %s FORCE ROW LEVEL SECURITY;\n", quote(td.Name))
			}
		case td.ExpectedRLSForced && !td.ActualRLSForced:
			fmt.Fprintln(sb, "--   ^ the target does not FORCE row-level security, so the table's OWNER bypasses "+
				"every policy — reads made as the owner return all rows")
			fmt.Fprintf(sb, "ALTER TABLE %s FORCE ROW LEVEL SECURITY;\n", quote(td.Name))
		default:
			fmt.Fprintln(sb, "--   ^ the target enforces row-level security the source does not; confirm this is "+
				"intentional before reconciling, as relaxing it is not reversible after rows leak")
		}
	}

	for _, name := range td.PoliciesMissing {
		fmt.Fprintf(sb, "-- policy %s on %s is missing from the target, so the rows it would have filtered are UNFILTERED there\n",
			quote(name), quote(td.Name))
		fmt.Fprintf(sb, "-- re-create it with CREATE POLICY %s ON %s ...; sluice does not emit policy DDL from a diff\n",
			quote(name), quote(td.Name))
	}
	for _, name := range td.PoliciesExtra {
		fmt.Fprintf(sb, "DROP POLICY %s ON %s; -- policy not in source schema\n", quote(name), quote(td.Name))
	}
	for _, pd := range td.PoliciesMismatched {
		renderPolicyMismatch(sb, td.Name, pd, quote)
	}
}

// renderPolicyMismatch writes the suggestion for a policy present on both
// sides under one name that filters something different.
func renderPolicyMismatch(sb *strings.Builder, table string, pd irdiff.PolicyDiff, quote func(string) string) {
	fmt.Fprintf(sb, "-- policy %s on %s differs between source and target:\n", quote(pd.Name), quote(table))
	if pd.ExpectedCommand != "" || pd.ActualCommand != "" {
		fmt.Fprintf(sb, "--   command:    source %s   target %s\n", pd.ExpectedCommand, pd.ActualCommand)
		fmt.Fprintln(sb, "--   ^ the two sides filter DIFFERENT statements; whichever statements the target's "+
			"policy omits run UNFILTERED there")
	}
	if pd.PermissiveMismatched {
		fmt.Fprintf(sb, "--   permissive: source %v   target %v\n", pd.ExpectedPermissive, pd.ActualPermissive)
		if pd.ActualPermissive && !pd.ExpectedPermissive {
			fmt.Fprintln(sb, "--   ^ the source's policy is RESTRICTIVE (AND'd, it can only narrow) and the "+
				"target's is PERMISSIVE (OR'd, it can only widen), so the target ADMITS ROWS THE SOURCE HIDES")
		}
	}
	if pd.ExpectedRoles != "" || pd.ActualRoles != "" {
		fmt.Fprintf(sb, "--   roles:      source %s   target %s\n", pd.ExpectedRoles, pd.ActualRoles)
		fmt.Fprintln(sb, "--   ^ a policy that does not name the role doing the reading does not constrain it")
	}
	if pd.ExpectedUsing != "" || pd.ActualUsing != "" {
		fmt.Fprintf(sb, "--   using:      source %q   target %q\n", pd.ExpectedUsing, pd.ActualUsing)
		if pd.ExpectedUsing != "" && pd.ActualUsing == "" {
			fmt.Fprintln(sb, "--   ^ the target has NO read filter for this policy, so it RETURNS EVERY ROW "+
				"the source would have hidden")
		}
	}
	if pd.ExpectedCheck != "" || pd.ActualCheck != "" {
		fmt.Fprintf(sb, "--   with check: source %q   target %q\n", pd.ExpectedCheck, pd.ActualCheck)
		if pd.ExpectedCheck != "" && pd.ActualCheck == "" {
			fmt.Fprintln(sb, "--   ^ the target has NO write filter for this policy, so it ACCEPTS WRITES "+
				"THE SOURCE REJECTS (including rows attributed to another tenant)")
		}
	}
	fmt.Fprintf(sb, "-- reconcile with DROP POLICY %s ON %s; then re-create it from the source definition\n",
		quote(pd.Name), quote(table))
}

// renderSequenceSections writes the standalone-sequence deltas (audit
// 2026-08-04 B-10). Sequences are schema-level objects, so these render
// alongside the table and view sections rather than inside a table's.
func renderSequenceSections(sb *strings.Builder, d irdiff.SchemaDiff, quote func(string) string, expected *ir.Schema) {
	for _, name := range d.SequencesMissing {
		fmt.Fprintf(sb, "-- ──────────── sequence %s (missing on target) ────────────\n", name)
		fmt.Fprintln(sb, "-- the target has no such sequence; any DEFAULT nextval() referencing it fails, and a "+
			"column relying on it issues NO values")
		if seq := lookupSequence(expected, name); seq != nil {
			fmt.Fprintf(sb, "%s;\n", renderCreateSequence(seq, quote))
			if seq.OwnedByTable != "" {
				fmt.Fprintf(sb, "ALTER SEQUENCE %s OWNED BY %s.%s;\n",
					quote(name), quote(seq.OwnedByTable), quote(seq.OwnedByColumn))
			}
		} else {
			fmt.Fprintf(sb, "-- expected definition unavailable; manually create %s\n", quote(name))
		}
		sb.WriteByte('\n')
	}

	for _, name := range d.SequencesExtra {
		fmt.Fprintf(sb, "-- ──────────── sequence %s (extra on target) ────────────\n", name)
		fmt.Fprintf(sb, "DROP SEQUENCE %s;\n", quote(name))
		fmt.Fprintf(sb, "-- ^ not in source schema; sluice would not create it\n\n")
	}

	for _, sd := range d.SequencesMismatched {
		fmt.Fprintf(sb, "-- ──────────── sequence %s (mismatched) ────────────\n", sd.Name)
		fmt.Fprintln(sb, "-- the target's sequence PRODUCES DIFFERENT VALUES than the source's would:")
		for _, row := range [...]struct{ label, exp, act string }{
			{"data type", sd.ExpectedDataType, sd.ActualDataType},
			{"start", sd.ExpectedStart, sd.ActualStart},
			{"increment", sd.ExpectedIncrement, sd.ActualIncrement},
			{"min value", sd.ExpectedMinValue, sd.ActualMinValue},
			{"max value", sd.ExpectedMaxValue, sd.ActualMaxValue},
			{"cache", sd.ExpectedCache, sd.ActualCache},
			{"owned by", sd.ExpectedOwnedBy, sd.ActualOwnedBy},
		} {
			if row.exp == "" && row.act == "" {
				continue
			}
			fmt.Fprintf(sb, "--   %-10s source %s   target %s\n", row.label+":", row.exp, row.act)
		}
		if sd.CycleMismatched {
			fmt.Fprintf(sb, "--   %-10s source %v   target %v\n", "cycle:", sd.ExpectedCycle, sd.ActualCycle)
			if sd.ActualCycle && !sd.ExpectedCycle {
				fmt.Fprintln(sb, "--   ^ the target CYCLEs and the source does not: at the boundary the target "+
					"WRAPS AND RE-ISSUES VALUES it has already handed out, where the source would refuse")
			}
		}
		// A comment, not DDL: ALTER SEQUENCE on a live target changes what
		// the next insert gets, and the operator has to pick that moment.
		fmt.Fprintf(sb, "-- reconcile with ALTER SEQUENCE %s ...; review before acting, it changes the next value issued\n",
			quote(sd.Name))
		sb.WriteByte('\n')
	}
}

// renderCreateSequence renders a CREATE SEQUENCE for a missing-on-target
// sequence. Every option is emitted explicitly, for the same reason the
// PG writer does it: the catalog values reproduce the source's behaviour
// for ascending AND descending sequences without re-deriving PG's
// direction-dependent defaults.
//
// The sequence's POSITION is deliberately absent — see [irdiff.SequenceDiff]
// for why the diff does not compare it. The created sequence therefore
// starts fresh, which is the safe direction (it can be advanced by hand;
// values already issued cannot be recalled).
func renderCreateSequence(s *ir.Sequence, quote func(string) string) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "CREATE SEQUENCE %s", quote(s.Name))
	if s.DataType != "" {
		fmt.Fprintf(&sb, " AS %s", s.DataType)
	}
	fmt.Fprintf(&sb, " INCREMENT BY %d MINVALUE %d MAXVALUE %d START WITH %d CACHE %d",
		s.Increment, s.MinValue, s.MaxValue, s.Start, s.Cache)
	if s.Cycle {
		sb.WriteString(" CYCLE")
	} else {
		sb.WriteString(" NO CYCLE")
	}
	return sb.String()
}

// lookupSequence returns the named sequence from s, or nil. Mirrors
// [lookupView].
func lookupSequence(s *ir.Schema, name string) *ir.Sequence {
	if s == nil {
		return nil
	}
	for _, seq := range s.Sequences {
		if seq != nil && seq.Name == name {
			return seq
		}
	}
	return nil
}
