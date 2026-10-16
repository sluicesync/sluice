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
