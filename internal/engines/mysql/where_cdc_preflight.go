// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// PreflightFilteredCDCBeforeImage implements [ir.FilteredCDCPreflighter]
// for a continuous filtered sync (`sync --where`, ADR-0173 Phase 2). The
// client-side row-move evaluation needs the FULL before-image of every
// UPDATE/DELETE on a filtered table; on MySQL that is
// @@GLOBAL.binlog_row_image=FULL (a MINIMAL/NOBLOB image omits non-key /
// unchanged columns, so a predicate on such a column could not be
// evaluated on the old row).
//
// This is a where-specific, sync-start-time refusal that names the
// filtered tables. It complements — and fires ahead of — the general
// [preflightBinlogRowImage] (Bug 193) that runs at every CDC start: both
// demand FULL, but this one surfaces at sync-start with the row-filter
// context so the operator sees WHY the filter needs it.
//
// tables are the SOURCE table names carrying a `--where` predicate; an
// empty list is a no-op.
func (e Engine) PreflightFilteredCDCBeforeImage(ctx context.Context, dsn string, tables []string) error {
	if len(tables) == 0 {
		return nil
	}
	cfg, err := parseDSNForFlavor(dsn, e.Flavor)
	if err != nil {
		return err
	}
	db, err := openDB(ctx, cfg, e.opts.sqlMode)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	var image string
	if err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.binlog_row_image").Scan(&image); err != nil {
		return fmt.Errorf("mysql: continuous filtered sync: read @@GLOBAL.binlog_row_image: %w", err)
	}
	if strings.EqualFold(image, "FULL") {
		return nil
	}
	named := append([]string(nil), tables...)
	sort.Strings(named)
	return sluicecode.Wrap(
		sluicecode.CodeWhereCDCBeforeImage,
		rowImageRemedyHint,
		fmt.Errorf(
			"continuous filtered sync: --where is set on table(s) %s, but the source streams partial binlog row "+
				"images (@@GLOBAL.binlog_row_image=%s). The --where row-move evaluation needs the complete "+
				"before-image of each UPDATE/DELETE to decide whether a row moved into or out of the filter's "+
				"scope; a partial image omits non-key/unchanged columns, so a predicate on such a column cannot be "+
				"evaluated on the old row and the stream would silently leak or drop rows. Set the source to full "+
				"row images before starting CDC: SET GLOBAL binlog_row_image=FULL (dynamic, no restart; applies to "+
				"sessions opened after the change). Then re-run",
			strings.Join(named, ", "), image,
		),
	)
}

// compile-time assertion that Engine satisfies the preflighter surface.
var _ ir.FilteredCDCPreflighter = Engine{}
