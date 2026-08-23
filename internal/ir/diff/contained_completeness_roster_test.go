// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package diff

// The CONTAINED-struct completeness rosters (audit backlog G-6 follow-on,
// filed at the container gate's own definition and built 2026-08-22).
//
// # What this gate is for
//
// [TestIRCompletenessRosterSchemaAndTableFieldsAreComparedOrExempt] walks
// the CONTAINER structs ([ir.Schema], [ir.Table]) and proves the diff
// reaches each collection — a column-type change surfaces, so Columns is
// compared. It says NOTHING about whether every attribute of the
// contained object is: an [ir.ForeignKey] whose OnDelete was never
// compared would pass it, because SOME column probe fires. That is the
// same class the container gate closed, one level down — and it is not
// hypothetical: OnDelete, Match and Deferrable were exactly the
// attributes roadmap item 125 found uncompared AFTER the FK collection
// itself was "covered".
//
// So this walks the contained structs by reflection and requires every
// field to be either PROBED — mutated on the actual side of an otherwise
// identical pair, with [Schemas] required to report drift AND populate
// that field's OWN slot on the nested diff struct — or exempted with a
// written reason. Adding a field to any of these structs fails this test
// until somebody makes that choice.
//
// # What it reaches, stated rather than implied
//
// It reaches the fields of [ir.Column], [ir.Index], [ir.IndexColumn],
// [ir.ForeignKey], [ir.Policy] and [ir.Sequence] — IndexColumn is
// included because an Index.Columns probe alone proves only that SOME
// entry attribute reaches the render, which is this gate's own defect
// one more level down (the per-column prefix Length was the S8 class).
// It does NOT walk [ir.CheckConstraint], [ir.ExcludeConstraint] or
// [ir.View] (small structs whose compared fields are the name plus one
// verbatim body; their uncompared fields are dialect/provenance tags of
// the same kinds exempted below), and it does not walk the [ir.Type]
// implementations (the type-family universe, which has its own
// family-matrix pins). Those are named residuals, not covered surface.
//
// # Why each probe names the nested diff field, not just the summary
//
// The container gate binds a probe to its comparator via the summary
// phrase. At this level that is too coarse: every ColumnDiff attribute
// summarises as "type mismatch", so a Nullable probe satisfied by an
// accidental Type drift would prove nothing. Each probe therefore also
// asserts the specific Expected/Actual slot its own comparator populates
// — the independent expected value for THIS field.

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// containedFieldProbe mutates one contained-struct field on the actual
// side of an otherwise-identical schema pair, and names both halves of
// the proof: the summary phrase the comparison must produce, and the
// nested-diff slot the field's own comparator must populate.
type containedFieldProbe struct {
	mutate        func(actual *ir.Schema)
	wantInSummary string

	// wantInDiff returns nil when the field's own diff slot is populated,
	// and an error naming what is missing otherwise. This is what stops a
	// Nullable probe being satisfied by a Type drift.
	wantInDiff func(d SchemaDiff) error
}

// firstMismatchedTable returns the single mismatched TableDiff the
// contained probes produce, or an error.
func firstMismatchedTable(d SchemaDiff) (TableDiff, error) {
	if len(d.TablesMismatched) != 1 {
		return TableDiff{}, fmt.Errorf("want exactly 1 mismatched table, got %d", len(d.TablesMismatched))
	}
	return d.TablesMismatched[0], nil
}

func oneColumnDiff(d SchemaDiff) (ColumnDiff, error) {
	td, err := firstMismatchedTable(d)
	if err != nil {
		return ColumnDiff{}, err
	}
	if len(td.ColumnsMismatched) != 1 {
		return ColumnDiff{}, fmt.Errorf("want exactly 1 ColumnDiff, got %d", len(td.ColumnsMismatched))
	}
	return td.ColumnsMismatched[0], nil
}

func oneIndexDiff(d SchemaDiff) (IndexDiff, error) {
	td, err := firstMismatchedTable(d)
	if err != nil {
		return IndexDiff{}, err
	}
	if len(td.IndexesMismatched) != 1 {
		return IndexDiff{}, fmt.Errorf("want exactly 1 IndexDiff, got %d", len(td.IndexesMismatched))
	}
	return td.IndexesMismatched[0], nil
}

func oneForeignKeyDiff(d SchemaDiff) (ForeignKeyDiff, error) {
	td, err := firstMismatchedTable(d)
	if err != nil {
		return ForeignKeyDiff{}, err
	}
	if len(td.ForeignKeysMismatched) != 1 {
		return ForeignKeyDiff{}, fmt.Errorf("want exactly 1 ForeignKeyDiff, got %d", len(td.ForeignKeysMismatched))
	}
	return td.ForeignKeysMismatched[0], nil
}

func onePolicyDiff(d SchemaDiff) (PolicyDiff, error) {
	td, err := firstMismatchedTable(d)
	if err != nil {
		return PolicyDiff{}, err
	}
	if len(td.PoliciesMismatched) != 1 {
		return PolicyDiff{}, fmt.Errorf("want exactly 1 PolicyDiff, got %d", len(td.PoliciesMismatched))
	}
	return td.PoliciesMismatched[0], nil
}

func oneSequenceDiff(d SchemaDiff) (SequenceDiff, error) {
	if len(d.SequencesMismatched) != 1 {
		return SequenceDiff{}, fmt.Errorf("want exactly 1 SequenceDiff, got %d", len(d.SequencesMismatched))
	}
	return d.SequencesMismatched[0], nil
}

// ---- ir.Column ----

var columnFieldProbes = map[string]containedFieldProbe{
	"Name": {
		mutate:        func(s *ir.Schema) { s.Tables[0].Columns[1].Name = "tenant_ref" },
		wantInSummary: "missing column",
		wantInDiff: func(d SchemaDiff) error {
			td, err := firstMismatchedTable(d)
			if err != nil {
				return err
			}
			for _, c := range td.ColumnsMissing {
				if c == "tenant_id" {
					return nil
				}
			}
			return fmt.Errorf("renamed column not reported missing under its expected name: %v", td.ColumnsMissing)
		},
	},
	"Type": {
		mutate:        func(s *ir.Schema) { s.Tables[0].Columns[1].Type = ir.Integer{Width: 16} },
		wantInSummary: "type mismatch",
		wantInDiff: func(d SchemaDiff) error {
			cd, err := oneColumnDiff(d)
			if err != nil {
				return err
			}
			if cd.ExpectedType == "" || cd.ActualType == "" {
				return fmt.Errorf("ExpectedType/ActualType not populated: %+v", cd)
			}
			return nil
		},
	},
	"Nullable": {
		mutate:        func(s *ir.Schema) { s.Tables[0].Columns[1].Nullable = true },
		wantInSummary: "type mismatch",
		wantInDiff: func(d SchemaDiff) error {
			cd, err := oneColumnDiff(d)
			if err != nil {
				return err
			}
			if cd.ExpectedNullable == nil || cd.ActualNullable == nil {
				return fmt.Errorf("ExpectedNullable/ActualNullable not populated: %+v", cd)
			}
			return nil
		},
	},
	"Default": {
		mutate:        func(s *ir.Schema) { s.Tables[0].Columns[1].Default = ir.DefaultLiteral{Value: "7"} },
		wantInSummary: "type mismatch",
		wantInDiff: func(d SchemaDiff) error {
			cd, err := oneColumnDiff(d)
			if err != nil {
				return err
			}
			if cd.ExpectedDefault == "" || cd.ActualDefault == "" {
				return fmt.Errorf("ExpectedDefault/ActualDefault not populated: %+v", cd)
			}
			return nil
		},
	},
	"GeneratedExpr": {
		mutate:        func(s *ir.Schema) { s.Tables[0].Columns[1].GeneratedExpr = "id * 10" },
		wantInSummary: "type mismatch",
		wantInDiff: func(d SchemaDiff) error {
			cd, err := oneColumnDiff(d)
			if err != nil {
				return err
			}
			if cd.ActualGeneratedExpr != "id * 10" {
				return fmt.Errorf("ActualGeneratedExpr not populated: %+v", cd)
			}
			return nil
		},
	},
}

var columnFieldExempt = map[string]string{
	"Comment": "a column comment is documentation, not enforcement — same class as the exempted Table.Comment on the container roster",
	"OnUpdateCurrentTimestamp": "NOT COMPARED — a NAMED GAP, not a nothing-to-see: `schema diff` cannot tell an operator a target column stopped " +
		"re-stamping itself (MySQL↔MySQL, where both sides support it). Comparing it needs the Bug-234-shaped target-cannot-hold suppression " +
		"first: PG/SQLite targets cannot hold the attribute and sluice's own emit WARNs-and-drops it there, so a bare comparison would report " +
		"irreconcilable drift on every MySQL→PG diff, get suppressed, and hide real drift alongside it. Filed in docs/dev/audit-backlog.md " +
		"(G-6 follow-on entry)",
	"GeneratedStored": "STORED vs VIRTUAL computes the same values from the same expression — a storage/latency property that cannot change " +
		"which rows are legal or what they contain, same class as the index Kind/Method exemption on IndexDiff",
	"GeneratedExprDialect": "a provenance tag on the expression TEXT, not a schema property: the expected side is dialect-tagged by the " +
		"readers/translate passes and the actual side's tag reflects the target catalog, so comparing tags would report the translation " +
		"itself as drift while the expression body (which IS compared) is what enforcement runs",
	"SourceColumnType": "sluice's own rewrite bookkeeping (--type-override / retarget context), populated only on the expected/translated side " +
		"and never by a schema reader — comparing it would report sluice's translation as target drift on every retargeted column",
	"SluiceInjected": "a provenance bit the diff CONSUMES rather than compares: it suppresses the injected shard column from ColumnsExtra " +
		"(ADR-0048 Decision 2), pinned by TestSchemas_SluiceInjected_SuppressedFromExtras and its negative sibling",
	"StableID": "metadata by contract: ir.Column documents that two columns differing ONLY in StableID are the SAME column for every " +
		"schema-identity purpose; it exists for CDC rename-proof (ADR-0091 F7b), and the cold-start SchemaReaders this diff consumes leave it 0",
}

// ---- ir.Index ----

var indexFieldProbes = map[string]containedFieldProbe{
	"Name": {
		mutate:        func(s *ir.Schema) { s.Tables[0].Indexes[0].Name = "idx_tenant_v2" },
		wantInSummary: "missing index",
		wantInDiff: func(d SchemaDiff) error {
			td, err := firstMismatchedTable(d)
			if err != nil {
				return err
			}
			for _, name := range td.IndexesMissing {
				if name == "idx_tenant" {
					return nil
				}
			}
			return fmt.Errorf("renamed index not reported missing under its expected name: %v", td.IndexesMissing)
		},
	},
	"Columns": {
		mutate:        func(s *ir.Schema) { s.Tables[0].Indexes[0].Columns = []ir.IndexColumn{{Column: "id"}} },
		wantInSummary: "index mismatch",
		wantInDiff: func(d SchemaDiff) error {
			id, err := oneIndexDiff(d)
			if err != nil {
				return err
			}
			if id.ExpectedColumns == "" || id.ActualColumns == "" {
				return fmt.Errorf("ExpectedColumns/ActualColumns not populated: %+v", id)
			}
			return nil
		},
	},
	"Unique": {
		mutate:        func(s *ir.Schema) { s.Tables[0].Indexes[0].Unique = true },
		wantInSummary: "index mismatch",
		wantInDiff: func(d SchemaDiff) error {
			id, err := oneIndexDiff(d)
			if err != nil {
				return err
			}
			if !id.UniqueMismatched {
				return fmt.Errorf("UniqueMismatched not set: %+v", id)
			}
			return nil
		},
	},
	"Predicate": {
		mutate:        func(s *ir.Schema) { s.Tables[0].Indexes[0].Predicate = "tenant_id > 0" },
		wantInSummary: "index mismatch",
		wantInDiff: func(d SchemaDiff) error {
			id, err := oneIndexDiff(d)
			if err != nil {
				return err
			}
			if id.ActualPredicate != "tenant_id > 0" {
				return fmt.Errorf("ActualPredicate not populated: %+v", id)
			}
			return nil
		},
	},
}

var indexFieldExempt = map[string]string{
	"Kind": "deliberately not compared — a performance property no cross-engine pair agrees on; the reasoning (and the noise a comparison " +
		"would add to every MySQL↔PG diff) is written on IndexDiff's own doc",
	"Method": "the verbatim access-method sibling of Kind (ivfflat/hnsw), excluded for the same written reason",
	"ConstraintBacked": "drives the UNIQUE re-emit SHAPE (ADD CONSTRAINT vs CREATE UNIQUE INDEX) — catalog plumbing that changes no row's " +
		"legality; the enforced property (Unique) IS probed",
	"ConstraintNamed": "a name-carry signal for the writer (roadmap item 84); the primary key is matched by ROLE precisely so that " +
		"engine-generated names stop reporting as drift (Bug 234, primaryKeyMatchKey's own doc)",
	"ConstraintDeferrable": "NOT COMPARED — a NAMED GAP shared by the four UNIQUE-constraint attribute flags: they are metadata-only today " +
		"(no emitter reads them; every target lands a plain UNIQUE and the PG reader WARNs per affected constraint at read time — ir.Index's " +
		"own doc), so comparing them before the carry ships would report drift against every target sluice itself created. The filed " +
		"follow-up is roadmap \"UNIQUE-constraint attribute fidelity\"; when an emitter starts carrying them, these four exemptions go stale " +
		"in this roster and force the comparison decision",
	"ConstraintInitiallyDeferred": "see ConstraintDeferrable — same named gap, same filed follow-up",
	"ConstraintNullsNotDistinct":  "see ConstraintDeferrable — same named gap, same filed follow-up",
	"ConstraintWithoutOverlaps":   "see ConstraintDeferrable — same named gap, same filed follow-up",
	"IncludeColumns": "covering-index PAYLOAD columns: stored in the leaves but not part of the key, so they change no ordering, comparison " +
		"or uniqueness scope — a performance property, same class as Kind/Method",
	"PredicateDialect": "a provenance tag on the predicate TEXT; the predicate body (which IS compared) is what enforcement runs — same " +
		"class as Column.GeneratedExprDialect",
}

// ---- ir.IndexColumn ----

var indexColumnFieldProbes = map[string]containedFieldProbe{
	"Column": {
		mutate:        func(s *ir.Schema) { s.Tables[0].Indexes[0].Columns[0].Column = "id" },
		wantInSummary: "index mismatch",
		wantInDiff: func(d SchemaDiff) error {
			id, err := oneIndexDiff(d)
			if err != nil {
				return err
			}
			if !strings.Contains(id.ActualColumns, "id") {
				return fmt.Errorf("ActualColumns does not carry the changed entry: %+v", id)
			}
			return nil
		},
	},
	"Expression": {
		mutate: func(s *ir.Schema) {
			s.Tables[0].Indexes[0].Columns[0] = ir.IndexColumn{Expression: "lower(tenant_id)"}
		},
		wantInSummary: "index mismatch",
		wantInDiff: func(d SchemaDiff) error {
			id, err := oneIndexDiff(d)
			if err != nil {
				return err
			}
			if !strings.Contains(id.ActualColumns, "lower(tenant_id)") {
				return fmt.Errorf("ActualColumns does not carry the expression entry: %+v", id)
			}
			return nil
		},
	},
	"Desc": {
		mutate:        func(s *ir.Schema) { s.Tables[0].Indexes[0].Columns[0].Desc = true },
		wantInSummary: "index mismatch",
		wantInDiff: func(d SchemaDiff) error {
			id, err := oneIndexDiff(d)
			if err != nil {
				return err
			}
			if !strings.Contains(id.ActualColumns, "DESC") {
				return fmt.Errorf("ActualColumns does not carry DESC: %+v", id)
			}
			return nil
		},
	},
	"Length": {
		// The S8 class itself: a dropped prefix length widens a unique key.
		mutate:        func(s *ir.Schema) { s.Tables[0].Indexes[0].Columns[0].Length = 10 },
		wantInSummary: "index mismatch",
		wantInDiff: func(d SchemaDiff) error {
			id, err := oneIndexDiff(d)
			if err != nil {
				return err
			}
			if !strings.Contains(id.ActualColumns, "(10)") {
				return fmt.Errorf("ActualColumns does not carry the prefix length: %+v", id)
			}
			return nil
		},
	},
}

var indexColumnFieldExempt = map[string]string{
	"NullsFirst": "per-column NULL placement changes scan order, never which rows are legal or unique (SQL NULLs are distinct in a unique " +
		"index regardless of placement) — the same ordering-is-out-of-scope reading ADR-0029 applies to index column ordering",
	"OperatorClass": "an access-method property (pgvector opclasses), same class as the exempted Index.Kind/Method — it selects HOW the " +
		"index searches, not what the table admits",
	"ExpressionDialect": "a provenance tag on the expression TEXT — same class as Column.GeneratedExprDialect; the expression body IS " +
		"rendered into the compared column list",
}

// ---- ir.ForeignKey ----

var foreignKeyFieldProbes = map[string]containedFieldProbe{
	"Name": {
		mutate:        func(s *ir.Schema) { s.Tables[0].ForeignKeys[0].Name = "fk_orders_tenant_v2" },
		wantInSummary: "missing foreign key",
		wantInDiff: func(d SchemaDiff) error {
			td, err := firstMismatchedTable(d)
			if err != nil {
				return err
			}
			for _, name := range td.ForeignKeysMissing {
				if name == "fk_orders_tenant" {
					return nil
				}
			}
			return fmt.Errorf("renamed FK not reported missing under its expected name: %v", td.ForeignKeysMissing)
		},
	},
	"Columns": {
		mutate:        func(s *ir.Schema) { s.Tables[0].ForeignKeys[0].Columns = []string{"id"} },
		wantInSummary: "foreign-key mismatch",
		wantInDiff:    fkReferencePopulated,
	},
	"ReferencedSchema": {
		mutate:        func(s *ir.Schema) { s.Tables[0].ForeignKeys[0].ReferencedSchema = "audit" },
		wantInSummary: "foreign-key mismatch",
		wantInDiff:    fkReferencePopulated,
	},
	"ReferencedTable": {
		mutate:        func(s *ir.Schema) { s.Tables[0].ForeignKeys[0].ReferencedTable = "accounts" },
		wantInSummary: "foreign-key mismatch",
		wantInDiff:    fkReferencePopulated,
	},
	"ReferencedColumns": {
		mutate:        func(s *ir.Schema) { s.Tables[0].ForeignKeys[0].ReferencedColumns = []string{"uid"} },
		wantInSummary: "foreign-key mismatch",
		wantInDiff:    fkReferencePopulated,
	},
	"OnDelete": {
		mutate:        func(s *ir.Schema) { s.Tables[0].ForeignKeys[0].OnDelete = ir.FKActionCascade },
		wantInSummary: "foreign-key mismatch",
		wantInDiff: func(d SchemaDiff) error {
			fd, err := oneForeignKeyDiff(d)
			if err != nil {
				return err
			}
			if fd.ActualOnDelete != "CASCADE" {
				return fmt.Errorf("ActualOnDelete not populated as the keyword: %+v", fd)
			}
			return nil
		},
	},
	"OnUpdate": {
		mutate:        func(s *ir.Schema) { s.Tables[0].ForeignKeys[0].OnUpdate = ir.FKActionSetNull },
		wantInSummary: "foreign-key mismatch",
		wantInDiff: func(d SchemaDiff) error {
			fd, err := oneForeignKeyDiff(d)
			if err != nil {
				return err
			}
			if fd.ActualOnUpdate != "SET NULL" {
				return fmt.Errorf("ActualOnUpdate not populated as the keyword: %+v", fd)
			}
			return nil
		},
	},
	"Match": {
		mutate:        func(s *ir.Schema) { s.Tables[0].ForeignKeys[0].Match = ir.FKMatchFull },
		wantInSummary: "foreign-key mismatch",
		wantInDiff: func(d SchemaDiff) error {
			fd, err := oneForeignKeyDiff(d)
			if err != nil {
				return err
			}
			if fd.ActualMatch != "FULL" {
				return fmt.Errorf("ActualMatch not populated: %+v", fd)
			}
			return nil
		},
	},
	"Deferrable": {
		mutate:        func(s *ir.Schema) { s.Tables[0].ForeignKeys[0].Deferrable = true },
		wantInSummary: "foreign-key mismatch",
		wantInDiff: func(d SchemaDiff) error {
			fd, err := oneForeignKeyDiff(d)
			if err != nil {
				return err
			}
			if !fd.DeferrabilityMismatched || !fd.ActualDeferrable {
				return fmt.Errorf("DeferrabilityMismatched/ActualDeferrable not populated: %+v", fd)
			}
			return nil
		},
	},
	"InitiallyDeferred": {
		mutate:        func(s *ir.Schema) { s.Tables[0].ForeignKeys[0].InitiallyDeferred = true },
		wantInSummary: "foreign-key mismatch",
		wantInDiff: func(d SchemaDiff) error {
			fd, err := oneForeignKeyDiff(d)
			if err != nil {
				return err
			}
			if !fd.DeferrabilityMismatched || !fd.ActualInitiallyDeferred {
				return fmt.Errorf("DeferrabilityMismatched/ActualInitiallyDeferred not populated: %+v", fd)
			}
			return nil
		},
	},
}

func fkReferencePopulated(d SchemaDiff) error {
	fd, err := oneForeignKeyDiff(d)
	if err != nil {
		return err
	}
	if fd.ExpectedReference == "" || fd.ActualReference == "" {
		return fmt.Errorf("ExpectedReference/ActualReference not populated: %+v", fd)
	}
	return nil
}

var foreignKeyFieldExempt = map[string]string{}

// ---- ir.Policy ----

var policyFieldProbes = map[string]containedFieldProbe{
	"Name": {
		mutate:        func(s *ir.Schema) { s.Tables[0].Policies[0].Name = "tenant_isolation_v2" },
		wantInSummary: "missing policy",
		wantInDiff: func(d SchemaDiff) error {
			td, err := firstMismatchedTable(d)
			if err != nil {
				return err
			}
			for _, name := range td.PoliciesMissing {
				if name == "tenant_isolation" {
					return nil
				}
			}
			return fmt.Errorf("renamed policy not reported missing under its expected name: %v", td.PoliciesMissing)
		},
	},
	"Command": {
		mutate:        func(s *ir.Schema) { s.Tables[0].Policies[0].Command = "SELECT" },
		wantInSummary: "policy mismatch",
		wantInDiff: func(d SchemaDiff) error {
			pd, err := onePolicyDiff(d)
			if err != nil {
				return err
			}
			if pd.ActualCommand != "SELECT" {
				return fmt.Errorf("ActualCommand not populated: %+v", pd)
			}
			return nil
		},
	},
	"Permissive": {
		mutate:        func(s *ir.Schema) { s.Tables[0].Policies[0].Permissive = false },
		wantInSummary: "policy mismatch",
		wantInDiff: func(d SchemaDiff) error {
			pd, err := onePolicyDiff(d)
			if err != nil {
				return err
			}
			if !pd.PermissiveMismatched {
				return fmt.Errorf("PermissiveMismatched not set: %+v", pd)
			}
			return nil
		},
	},
	"Roles": {
		mutate:        func(s *ir.Schema) { s.Tables[0].Policies[0].Roles = []string{"app", "reporting"} },
		wantInSummary: "policy mismatch",
		wantInDiff: func(d SchemaDiff) error {
			pd, err := onePolicyDiff(d)
			if err != nil {
				return err
			}
			if !strings.Contains(pd.ActualRoles, "reporting") {
				return fmt.Errorf("ActualRoles not populated: %+v", pd)
			}
			return nil
		},
	},
	"Using": {
		mutate:        func(s *ir.Schema) { s.Tables[0].Policies[0].Using = "true" },
		wantInSummary: "policy mismatch",
		wantInDiff: func(d SchemaDiff) error {
			pd, err := onePolicyDiff(d)
			if err != nil {
				return err
			}
			if pd.ActualUsing != "true" {
				return fmt.Errorf("ActualUsing not populated: %+v", pd)
			}
			return nil
		},
	},
	"Check": {
		mutate:        func(s *ir.Schema) { s.Tables[0].Policies[0].Check = "false" },
		wantInSummary: "policy mismatch",
		wantInDiff: func(d SchemaDiff) error {
			pd, err := onePolicyDiff(d)
			if err != nil {
				return err
			}
			if pd.ActualCheck != "false" {
				return fmt.Errorf("ActualCheck not populated: %+v", pd)
			}
			return nil
		},
	},
}

var policyFieldExempt = map[string]string{}

// ---- ir.Sequence ----

var sequenceFieldProbes = map[string]containedFieldProbe{
	"Name": {
		mutate:        func(s *ir.Schema) { s.Sequences[0].Name = "order_number_seq_v2" },
		wantInSummary: "missing sequence",
		wantInDiff: func(d SchemaDiff) error {
			for _, name := range d.SequencesMissing {
				if name == "order_number_seq" {
					return nil
				}
			}
			return fmt.Errorf("renamed sequence not reported missing under its expected name: %v", d.SequencesMissing)
		},
	},
	"DataType": {
		mutate:        func(s *ir.Schema) { s.Sequences[0].DataType = "integer" },
		wantInSummary: "sequence mismatch",
		wantInDiff: func(d SchemaDiff) error {
			sd, err := oneSequenceDiff(d)
			if err != nil {
				return err
			}
			if sd.ActualDataType != "integer" {
				return fmt.Errorf("ActualDataType not populated: %+v", sd)
			}
			return nil
		},
	},
	"Start": {
		mutate:        func(s *ir.Schema) { s.Sequences[0].Start = 2000 },
		wantInSummary: "sequence mismatch",
		wantInDiff:    seqSlotPopulated(func(sd SequenceDiff) string { return sd.ActualStart }, "ActualStart"),
	},
	"Increment": {
		mutate:        func(s *ir.Schema) { s.Sequences[0].Increment = 9 },
		wantInSummary: "sequence mismatch",
		wantInDiff:    seqSlotPopulated(func(sd SequenceDiff) string { return sd.ActualIncrement }, "ActualIncrement"),
	},
	"MinValue": {
		mutate:        func(s *ir.Schema) { s.Sequences[0].MinValue = 3 },
		wantInSummary: "sequence mismatch",
		wantInDiff:    seqSlotPopulated(func(sd SequenceDiff) string { return sd.ActualMinValue }, "ActualMinValue"),
	},
	"MaxValue": {
		mutate:        func(s *ir.Schema) { s.Sequences[0].MaxValue = 1 << 20 },
		wantInSummary: "sequence mismatch",
		wantInDiff:    seqSlotPopulated(func(sd SequenceDiff) string { return sd.ActualMaxValue }, "ActualMaxValue"),
	},
	"Cache": {
		mutate:        func(s *ir.Schema) { s.Sequences[0].Cache = 50 },
		wantInSummary: "sequence mismatch",
		wantInDiff:    seqSlotPopulated(func(sd SequenceDiff) string { return sd.ActualCache }, "ActualCache"),
	},
	"Cycle": {
		mutate:        func(s *ir.Schema) { s.Sequences[0].Cycle = true },
		wantInSummary: "sequence mismatch",
		wantInDiff: func(d SchemaDiff) error {
			sd, err := oneSequenceDiff(d)
			if err != nil {
				return err
			}
			if !sd.CycleMismatched || !sd.ActualCycle {
				return fmt.Errorf("CycleMismatched/ActualCycle not populated: %+v", sd)
			}
			return nil
		},
	},
	"OwnedByTable": {
		mutate:        func(s *ir.Schema) { s.Sequences[0].OwnedByTable = "orders" },
		wantInSummary: "sequence mismatch",
		wantInDiff:    seqSlotPopulated(func(sd SequenceDiff) string { return sd.ActualOwnedBy }, "ActualOwnedBy"),
	},
	"OwnedByColumn": {
		mutate:        func(s *ir.Schema) { s.Sequences[0].OwnedByColumn = "id" },
		wantInSummary: "sequence mismatch",
		wantInDiff:    seqSlotPopulated(func(sd SequenceDiff) string { return sd.ActualOwnedBy }, "ActualOwnedBy"),
	},
}

func seqSlotPopulated(get func(SequenceDiff) string, slot string) func(SchemaDiff) error {
	return func(d SchemaDiff) error {
		sd, err := oneSequenceDiff(d)
		if err != nil {
			return err
		}
		if get(sd) == "" {
			return fmt.Errorf("%s not populated: %+v", slot, sd)
		}
		return nil
	}
}

var sequenceFieldExempt = map[string]string{
	"Schema": "sequences are matched by BARE NAME for the same ADR-0031 reason tables are (the target-side reader is pinned to one " +
		"namespace), written at diffSequences' own definition",
	"LastValue": "the sequence's POSITION, a moving runtime value — comparing it would report drift on every run of a healthy pair under " +
		"write load, get suppressed, and hide the structural drift. A NAMED GAP, stated at SequenceDiff's own doc: `schema diff` will not " +
		"tell an operator a target sequence is BEHIND its source; the position is the writer's business (setval priming)",
	"LastValueIsCalled": "see LastValue — same position snapshot, same named gap",
	"LastValueValid":    "see LastValue — the validity flag for the same position snapshot",
}

// containedProbedFloor guards each roster against a walker that stopped
// matching its type: reflection returning a tiny field list would make
// every entry trivially satisfied. Floors equal today's probe counts.
var containedRosters = []struct {
	typ    reflect.Type
	probes map[string]containedFieldProbe
	exempt map[string]string
	floor  int
}{
	{reflect.TypeOf(ir.Column{}), columnFieldProbes, columnFieldExempt, 5},
	{reflect.TypeOf(ir.Index{}), indexFieldProbes, indexFieldExempt, 4},
	{reflect.TypeOf(ir.IndexColumn{}), indexColumnFieldProbes, indexColumnFieldExempt, 4},
	{reflect.TypeOf(ir.ForeignKey{}), foreignKeyFieldProbes, foreignKeyFieldExempt, 10},
	{reflect.TypeOf(ir.Policy{}), policyFieldProbes, policyFieldExempt, 6},
	{reflect.TypeOf(ir.Sequence{}), sequenceFieldProbes, sequenceFieldExempt, 10},
}

// TestContainedIRCompletenessRosterEveryFieldComparedOrExempt is the G-6
// follow-on gate. See the file comment for exactly which structs it
// reaches and which it deliberately does not.
func TestContainedIRCompletenessRosterEveryFieldComparedOrExempt(t *testing.T) {
	for _, r := range containedRosters {
		t.Run("ir."+r.typ.Name(), func(t *testing.T) {
			assertContainedRosterCovers(t, r.typ, r.probes, r.exempt, r.floor)
		})
	}
}

// assertContainedRosterCovers mirrors the container harness
// (assertRosterCovers) — every exported field probed or exempted, no
// stale entries, an anti-vacuity floor — and then runs each probe with
// the tighter binding: drift must be reported, the summary must name the
// field's comparator family, and the field's OWN nested-diff slot must
// be populated.
func assertContainedRosterCovers(t *testing.T, typ reflect.Type, probes map[string]containedFieldProbe, exempt map[string]string, floor int) {
	t.Helper()

	fields := make(map[string]bool, typ.NumField())
	for i := range typ.NumField() {
		f := typ.Field(i)
		if !f.IsExported() {
			continue
		}
		fields[f.Name] = true
	}
	if len(fields) == 0 {
		t.Fatalf("reflection found NO exported fields on %s — the walker has stopped matching the type", typ)
	}

	for name := range fields {
		_, probed := probes[name]
		reason, exempted := exempt[name]
		switch {
		case probed && exempted:
			t.Errorf("%s.%s is both probed and exempted — pick one", typ.Name(), name)
		case !probed && !exempted:
			t.Errorf("%s.%s is neither COMPARED by a probe nor exempted.\n"+
				"  Add a probe proving diff.Schemas() populates this field's own slot on the nested diff struct,\n"+
				"  or an exemption naming why it cannot change what the target enforces.\n"+
				"  This is the level the container roster explicitly does not reach — OnDelete, Match and\n"+
				"  Deferrable were exactly the attributes found uncompared here (roadmap item 125).", typ.Name(), name)
		case exempted && strings.TrimSpace(reason) == "":
			t.Errorf("%s.%s is exempted with an empty reason", typ.Name(), name)
		}
	}

	for name := range probes {
		if !fields[name] {
			t.Errorf("probe roster names %s.%s, which is not a field of the type (renamed or removed?)", typ.Name(), name)
		}
	}
	for name := range exempt {
		if !fields[name] {
			t.Errorf("exemption roster names %s.%s, which is not a field of the type (renamed or removed?)", typ.Name(), name)
		}
	}

	if len(probes) < floor {
		t.Errorf("only %d probed field(s) on %s; floor is %d — either the walker broke or fields were mass-exempted",
			len(probes), typ.Name(), floor)
	}

	for name, p := range probes {
		t.Run(name, func(t *testing.T) {
			expected := rosterBaseline()
			actual := rosterBaseline()

			if got := Schemas(expected, rosterBaseline(), Options{}); got.HasChanges() {
				t.Fatalf("baseline compares UNEQUAL to itself (%s) — the probe result would be meaningless", got.Summary())
			}

			p.mutate(actual)
			d := Schemas(expected, actual, Options{})
			if !d.HasChanges() {
				t.Fatalf("mutating %s.%s produced NO drift — the field is not compared", typ.Name(), name)
			}
			if got := d.Summary(); !strings.Contains(got, p.wantInSummary) {
				t.Errorf("mutating %s.%s summarised as %q; want it to contain %q", typ.Name(), name, got, p.wantInSummary)
			}
			if err := p.wantInDiff(d); err != nil {
				t.Errorf("mutating %s.%s fired a comparator, but not this field's own: %v\n"+
					"  (that is the accidental-satisfaction shape this gate exists to exclude)", typ.Name(), name, err)
			}
		})
	}
}
