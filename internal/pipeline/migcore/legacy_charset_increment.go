// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"log/slog"
	"strconv"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// LegacyCharsetIncrementMarker is the grep-stable token on the WARN a
// chain replay logs for an incremental that may carry U+FFFD in place of
// non-ASCII text (GC-37 (j)).
const LegacyCharsetIncrementMarker = "LEGACY-CHARSET-INCREMENT"

// WarnLegacyCharsetIncrement names the columns of an incremental that a
// sluice before v0.156.3 captured from a MySQL-family source while the
// change stream carried a non-UTF-8 character column's STORED bytes: the
// change chunk's JSON encoder wrote every non-ASCII value of such a column
// as U+FFFD, so the incremental holds '�' where the source held the text,
// and no restore can recover it (MEASURED: a v0.156.2-captured chain
// restored into Postgres held U+FFFD for latin1 'é', cp1250 'Š', sjis 'あ').
//
// It WARNs rather than refuses, and the choice is deliberate: the values
// are gone from the chain, so a refusal would throw away every other table
// and column the increment carries correctly without recovering anything.
// The WARN names the table and columns and the remedy — take a fresh full
// backup, and repair any target already restored from such a chain against
// the source. Only rows the incremental CHANGED are affected; rows the full
// copied were converted by the server and are exact.
//
// origin prefixes the log line ("chain restore", "sync from-backup").
func WarnLegacyCharsetIncrement(ctx context.Context, origin string, m *irbackup.Manifest) {
	if m == nil || m.Schema == nil || !IsMySQLFamilyEngine(m.SourceEngine) || !capturedBeforeCharsetDecode(m.SluiceVersion) {
		return
	}
	for _, t := range m.Schema.Tables {
		var cols []string
		for _, c := range t.Columns {
			if cs, ok := stringColumnCharsetName(c.Type); ok && !utf8FamilyCharset(cs) {
				cols = append(cols, c.Name+" ("+cs+")")
			}
		}
		if len(cols) == 0 {
			continue
		}
		slog.WarnContext(ctx, origin+": "+LegacyCharsetIncrementMarker+
			" — this incremental was captured by sluice "+strings.TrimSpace(m.SluiceVersion)+
			", whose change stream carried these non-UTF-8 columns' stored bytes; every non-ASCII value the incremental "+
			"changed in them was recorded as U+FFFD ('�') and restores that way. Rows the full backup copied are exact. "+
			"Remedy: take a fresh full backup with sluice v0.156.3 or later, and repair rows restored from this chain against the source",
			"table", t.Name, "columns", strings.Join(cols, ", "))
	}
}

// capturedBeforeCharsetDecode reports whether a manifest's SluiceVersion is
// v0.156.2 or earlier. An unparseable version ("dev", "") is not assumed
// either way and returns false.
func capturedBeforeCharsetDecode(v string) bool {
	v = strings.TrimPrefix(strings.TrimSpace(v), "v")
	parts := strings.SplitN(v, ".", 3)
	if len(parts) < 3 {
		return false
	}
	patch := parts[2]
	if i := strings.IndexFunc(patch, func(r rune) bool { return r < '0' || r > '9' }); i >= 0 {
		patch = patch[:i]
	}
	nums := [3]int{}
	for i, p := range []string{parts[0], parts[1], patch} {
		n, err := strconv.Atoi(p)
		if err != nil {
			return false
		}
		nums[i] = n
	}
	switch {
	case nums[0] != 0:
		return false
	case nums[1] != 156:
		return nums[1] < 156
	default:
		return nums[2] <= 2
	}
}

// stringColumnCharsetName returns an IR string column's declared charset.
func stringColumnCharsetName(t ir.Type) (string, bool) {
	switch v := ir.UnwrapDomain(t).(type) {
	case ir.Char:
		return v.Charset, v.Charset != ""
	case ir.Varchar:
		return v.Charset, v.Charset != ""
	case ir.Text:
		return v.Charset, v.Charset != ""
	}
	return "", false
}

// utf8FamilyCharset reports whether a MySQL charset's stored bytes are
// already UTF-8 (so the pre-fix change stream carried them faithfully).
func utf8FamilyCharset(cs string) bool {
	switch strings.ToLower(cs) {
	case "utf8mb4", "utf8mb3", "utf8", "ascii", "binary":
		return true
	}
	return false
}
