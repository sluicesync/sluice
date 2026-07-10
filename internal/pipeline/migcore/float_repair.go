// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import "sluicesync.dev/sluice/internal/ir"

// SinglePrecisionFloatColumns returns t's single-precision FLOAT columns
// (ir.Float{FloatSingle}). It is the shared detector for the VStream-COPY
// FLOAT display-rounding mitigation (roadmap open-bug 2026-07-09): the sync
// cold-start re-read repair and the backup WARN / --strict-float refusal
// both key off single-precision FLOAT columns in the SOURCE schema.
//
// DOUBLE columns (ir.Float{FloatDouble}) transit the VStream COPY exactly
// and are excluded — only single-precision FLOAT is rendered at mysqld's
// 6-significant-digit display precision by vttablet's rowstreamer.
func SinglePrecisionFloatColumns(t *ir.Table) []*ir.Column {
	if t == nil {
		return nil
	}
	var out []*ir.Column
	for _, c := range t.Columns {
		if f, ok := c.Type.(ir.Float); ok && f.Precision == ir.FloatSingle {
			out = append(out, c)
		}
	}
	return out
}

// PrimaryKeyHasSinglePrecisionFloat reports whether any of t's primary-key
// columns is itself a single-precision FLOAT (ir.Float{FloatSingle}).
//
// Such a table CANNOT be repaired by the VStream-COPY float re-read: the
// re-read scans the PK exactly (`(col * 1E0)`) while the bulk-COPY wrote the
// PK display-rounded, so the exact-vs-rounded PK values never match and the
// PK-keyed UPDATE / patch-map lookup silently hits zero rows. The row's own
// IDENTITY is rounded on the target, so there is no reliable key to target
// the re-read at all — the whole table is non-repairable, not just its PK
// float column. Both the sync cold-start planner and the backup planner gate
// on this so the rule stays in lock-step (a divergence would silently
// re-open the SL-F1 class in one path only).
func PrimaryKeyHasSinglePrecisionFloat(t *ir.Table) bool {
	if t == nil {
		return false
	}
	pkNames := PrimaryKeyColumnNames(t)
	if len(pkNames) == 0 {
		return false
	}
	pkSet := make(map[string]struct{}, len(pkNames))
	for _, n := range pkNames {
		pkSet[n] = struct{}{}
	}
	for _, c := range SinglePrecisionFloatColumns(t) {
		if _, isPK := pkSet[c.Name]; isPK {
			return true
		}
	}
	return false
}

// SchemaHasSinglePrecisionFloat reports whether any table in s has a
// single-precision FLOAT column.
func SchemaHasSinglePrecisionFloat(s *ir.Schema) bool {
	if s == nil {
		return false
	}
	for _, t := range s.Tables {
		if len(SinglePrecisionFloatColumns(t)) > 0 {
			return true
		}
	}
	return false
}
