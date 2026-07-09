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
