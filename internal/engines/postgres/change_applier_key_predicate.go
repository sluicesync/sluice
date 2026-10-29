// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
)

// keyPredicateFor returns the part of before an UPDATE's or DELETE's WHERE
// compares on this target ([appliershared.KeyPredicate], Bug 339): the
// table's primary key, plus the sharded-Neki routing columns in shardKeys so
// the statement still routes to the row's shard. Every UPDATE/DELETE arm of
// [ChangeApplier.dispatch] and [ChangeApplier.dispatchPipelined] builds its
// statement, row address and multi-row check from the returned row;
// TestKeyPredicateRoster_EveryUpdateDeleteBuilderGetsTheNarrowedImage
// (internal/engines) holds every builder call site to it.
//
// The key is read on the primary pool through the redactor's cached lookup,
// like [ChangeApplier.colTypesFor], never on the apply transaction.
func (a *ChangeApplier) keyPredicateFor(ctx context.Context, schema, table string, before ir.Row, colTypes map[string]*ir.Column, shardKeys []string) (ir.Row, error) {
	pk, err := a.pkForRedact(ctx, schema, table)
	if err != nil {
		return nil, fmt.Errorf("postgres: applier: primary key of %s.%s: %w", schema, table, err)
	}
	return appliershared.KeyPredicate(before, pk, colTypes, shardKeys), nil
}
