// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// The [FormatVersionHiddenCollation] tier's predicates and stamps
// (FUNCTIONAL-CAST-HIDDEN-COLLATION; sixth review F3/F4). The tier is
// proportional: only a manifest that carries a functional key part's
// hidden-column collation moves.

// StampHiddenCollation raises m.FormatVersion to
// [FormatVersionHiddenCollation] when m's recorded schema or schema delta
// carries a functional key part's hidden-column collation
// ([ManifestCarriesHiddenCollation]). A full is stamped through
// [FormatVersionFor] when its manifest is built; this is the CDC segment's
// stamp, for the two writers that swap in a window-end schema or attach a
// delta after the manifest was built. Idempotent; a no-op when nothing
// carries the field. MUST run before [ComputeBackupID], for the reason
// [StampExactNumbers] gives.
func StampHiddenCollation(m *Manifest) {
	if ManifestCarriesHiddenCollation(m) {
		m.FormatVersion = max(m.FormatVersion, FormatVersionHiddenCollation)
	}
}

// RefuseUnstampedHiddenCollation refuses a manifest recorded below
// [FormatVersionHiddenCollation] that nonetheless carries a hidden-column
// collation. No writer produces that shape — every writer that records the
// field stamps the tier — so it is an edit, and honouring it would let an
// injected collation decide how a restored key compares. On a manifest
// signed under an older canonicalization the field is authenticated only
// through the recorded schema fingerprint, which an injected field fails to
// reproduce on the paths that recompute it ([ComputeSchemaHash] folds it);
// this refusal covers the read itself, before any path decides whether to
// recompute. nil for every manifest that is not that shape.
func RefuseUnstampedHiddenCollation(m *Manifest) error {
	if m == nil || m.FormatVersion >= FormatVersionHiddenCollation || !ManifestCarriesHiddenCollation(m) {
		return nil
	}
	return fmt.Errorf("backup: manifest format version %d records a functional key's hidden-column collation, which "+
		"only format version %d and above carry; no sluice writes that shape, so the manifest was edited after it "+
		"was written. Refusing to let the edit decide how a restored key compares; restore from an unmodified copy "+
		"of the backup", m.FormatVersion, FormatVersionHiddenCollation)
}

// ManifestCarriesHiddenCollation reports whether m's recorded schema, or
// either side of any of its schema-delta entries, carries a functional key
// part's hidden-column collation.
func ManifestCarriesHiddenCollation(m *Manifest) bool {
	if m == nil {
		return false
	}
	if schemaCarriesHiddenCollation(m.Schema) {
		return true
	}
	for _, d := range m.SchemaDelta {
		if d != nil && (tableCarriesHiddenCollation(d.Before) || tableCarriesHiddenCollation(d.After)) {
			return true
		}
	}
	// SchemaHistory too. No writer puts the field there today: only the
	// MySQL schema reader sets it, and MySQL's history projections carry
	// columns and a bare primary key. But pgtrigger's first-touch versions
	// now carry full tables with secondary indexes, so "history records no
	// indexes" is no longer a premise to lean on, and the key-collation
	// guard consumes a history version's table as its source shape. An
	// entry carrying the field below FormatVersionHiddenCollation is the
	// same edited shape as a schema carrying it. An entry that does not
	// decode is left to the readers that decode it, which refuse it loudly.
	for _, e := range m.SchemaHistory {
		if e == nil || len(e.TableJSON) == 0 {
			continue
		}
		if t, err := ir.UnmarshalTable(e.TableJSON); err == nil && tableCarriesHiddenCollation(t) {
			return true
		}
	}
	return false
}

// schemaCarriesHiddenCollation reports whether any table of s carries a
// hidden-column collation on any of its keys.
func schemaCarriesHiddenCollation(s *ir.Schema) bool {
	if s == nil {
		return false
	}
	for _, t := range s.Tables {
		if tableCarriesHiddenCollation(t) {
			return true
		}
	}
	return false
}

// tableCarriesHiddenCollation reports whether t's primary key or any of its
// indexes carries a hidden-column collation.
func tableCarriesHiddenCollation(t *ir.Table) bool {
	if t == nil {
		return false
	}
	for _, idx := range append([]*ir.Index{t.PrimaryKey}, t.Indexes...) {
		if idx == nil {
			continue
		}
		for _, c := range idx.Columns {
			if c.HiddenCollation != "" {
				return true
			}
		}
	}
	return false
}

// WithoutHiddenCollation returns s with every hidden-column collation
// cleared, copying only the tables and indexes that carried one (s itself
// is not mutated; nil stays nil). For a writer that cannot stamp
// [FormatVersionHiddenCollation] — a full RESUMING an encrypted attempt
// written below the injective-AAD tier, whose kept chunks pin its format
// version. A cleared part is "not known" ([ir.IndexColumn.HiddenCollation]):
// the MySQL writer emits its expression bare and the key-collation guard
// arms it, which is the behaviour before the field existed — loud where the
// target compares differently, never silent.
func WithoutHiddenCollation(s *ir.Schema) *ir.Schema {
	if !schemaCarriesHiddenCollation(s) {
		return s
	}
	out := *s
	out.Tables = make([]*ir.Table, len(s.Tables))
	for i, t := range s.Tables {
		if !tableCarriesHiddenCollation(t) {
			out.Tables[i] = t
			continue
		}
		ct := *t
		ct.PrimaryKey = indexWithoutHiddenCollation(t.PrimaryKey)
		ct.Indexes = make([]*ir.Index, len(t.Indexes))
		for j, idx := range t.Indexes {
			ct.Indexes[j] = indexWithoutHiddenCollation(idx)
		}
		out.Tables[i] = &ct
	}
	return &out
}

// HiddenCollationComparable returns a and b with each functional part's
// hidden-column collation cleared where the OTHER schema's matching part
// (same table, same index name, same position) records none — the
// both-sides rule migcore's index equality and `schema diff` apply. A
// schema an older binary recorded (no field) then compares equal to the
// same schema read by this one, while two schemas that both record the
// field and disagree still differ. Neither input is mutated.
func HiddenCollationComparable(a, b *ir.Schema) (comparableA, comparableB *ir.Schema) {
	return withPeerHiddenCollations(a, b), withPeerHiddenCollations(b, a)
}

// withPeerHiddenCollations is s with every hidden-column collation cleared
// that peer's matching part does not also record.
func withPeerHiddenCollations(s, peer *ir.Schema) *ir.Schema {
	if !schemaCarriesHiddenCollation(s) {
		return s
	}
	recorded := map[string]bool{}
	if peer != nil {
		for _, t := range peer.Tables {
			if t == nil {
				continue
			}
			for _, idx := range append([]*ir.Index{t.PrimaryKey}, t.Indexes...) {
				if idx == nil {
					continue
				}
				for i, c := range idx.Columns {
					if c.HiddenCollation != "" {
						recorded[hiddenPartKey(t, idx, i)] = true
					}
				}
			}
		}
	}
	out := *s
	out.Tables = make([]*ir.Table, len(s.Tables))
	for i, t := range s.Tables {
		if !tableCarriesHiddenCollation(t) {
			out.Tables[i] = t
			continue
		}
		strip := func(idx *ir.Index) *ir.Index {
			if idx == nil {
				return nil
			}
			ci := *idx
			ci.Columns = make([]ir.IndexColumn, len(idx.Columns))
			copy(ci.Columns, idx.Columns)
			for j := range ci.Columns {
				if !recorded[hiddenPartKey(t, idx, j)] {
					ci.Columns[j].HiddenCollation = ""
				}
			}
			return &ci
		}
		ct := *t
		ct.PrimaryKey = strip(t.PrimaryKey)
		ct.Indexes = make([]*ir.Index, len(t.Indexes))
		for j, idx := range t.Indexes {
			ct.Indexes[j] = strip(idx)
		}
		out.Tables[i] = &ct
	}
	return &out
}

// hiddenPartKey names a functional part across two reads of a schema.
func hiddenPartKey(t *ir.Table, idx *ir.Index, i int) string {
	return fmt.Sprintf("%s\x00%s\x00%s\x00%d", t.Schema, t.Name, idx.Name, i)
}

func indexWithoutHiddenCollation(in *ir.Index) *ir.Index {
	if in == nil {
		return nil
	}
	out := *in
	out.Columns = make([]ir.IndexColumn, len(in.Columns))
	copy(out.Columns, in.Columns)
	for i := range out.Columns {
		out.Columns[i].HiddenCollation = ""
	}
	return &out
}
