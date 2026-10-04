// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// The replay-duplication judge (audit F-E1).
//
// Every path that re-applies backup content onto a target — the
// `sync from-backup` broker re-applying an interrupted incremental, a
// `restore` re-run onto the target an earlier attempt already wrote — is
// sound only for tables on which re-applying a row that already landed
// converges. That holds when the applier has a key to upsert on (ADR-0010).
// On a table without one the applier falls back to a plain INSERT, and the
// re-applied rows land a second time, silently, at exit 0.
//
// "Without a key" is judged TWICE, from two independent sources, and a
// table failing either is reported:
//
//   - the RECORDED schema (the backup manifest's table), by
//     [irbackup.TableReplayIdempotent] — what the source declared;
//   - the TARGET's live catalog, through the engine's
//     [ir.ReplayKeyProber] — what the applier will actually key on, which
//     is the predicate the applier itself uses (see each engine's
//     ProbeReplayKey). A target table created or altered without the
//     source's key is caught here and nowhere else.
//
// Each side is the other's independent expected value. The recorded schema
// cannot see a target that dropped a key; the target cannot see that the
// source holds legitimate duplicate rows a target-only key would collapse.
// A table absent on the target is judged on the recorded schema alone: it
// will be created from that schema, key included.

// ReplayKeylessReason names which judgment found a table keyless.
type ReplayKeylessReason string

const (
	// ReplayKeylessRecorded means the recorded (source) schema declares no
	// PRIMARY KEY and no all-NOT-NULL plain-column UNIQUE index.
	ReplayKeylessRecorded ReplayKeylessReason = "the backup's recorded schema has no PRIMARY KEY and no NOT NULL UNIQUE index"
	// ReplayKeylessTarget means the target table exists and the applier
	// would plain-INSERT into it, whatever the recorded schema says.
	ReplayKeylessTarget ReplayKeylessReason = "the target table has no PRIMARY KEY and no NOT NULL UNIQUE index the applier can upsert on"
)

// ReplayKeylessTable is one table a re-apply would duplicate rows in.
type ReplayKeylessTable struct {
	Name   string
	Reason ReplayKeylessReason
}

// ReplayJudgeOptions selects which of the judgments apply.
type ReplayJudgeOptions struct {
	// ProbeTarget enables the target-catalog judgment. A caller that is
	// about to DROP and recreate the tables from the recorded schema (the
	// broker's --reset-target-data) turns it off: the current target shape
	// is about to stop existing.
	ProbeTarget bool

	// OnlyNonEmpty skips a table the target holds no rows in, BEFORE either
	// judgment. The restore re-run door uses it: a keyless table is only at
	// risk when something is already in it. Requires the writer to
	// implement [ir.TableEmptyChecker]; see [FindReplayKeylessTables] for
	// what happens when it does not.
	OnlyNonEmpty bool
}

// FindReplayKeylessTables returns, in input order, the tables among tables
// a re-applied INSERT would duplicate rows in. rw is the target's row
// writer, used for the [ir.ReplayKeyProber] and [ir.TableEmptyChecker]
// probes; it may be nil when neither option is set.
//
// An engine whose writer lacks ir.ReplayKeyProber is judged on the
// recorded schema alone. An engine whose writer lacks ir.TableEmptyChecker
// cannot answer OnlyNonEmpty; rather than refuse every restore on such an
// engine (it cannot tell a fresh target from a re-run), the emptiness
// filter is skipped with a DEBUG line and every table is treated as EMPTY —
// i.e. that engine's restore re-run door is open. That residual is stated
// in the operator docs; today every target that can run a restore or a
// broker (postgres, mysql and their flavors) implements both surfaces.
func FindReplayKeylessTables(
	ctx context.Context,
	rw ir.RowWriter,
	tables []*ir.Table,
	opts ReplayJudgeOptions,
) ([]ReplayKeylessTable, error) {
	var out []ReplayKeylessTable
	var prober ir.ReplayKeyProber
	if opts.ProbeTarget && rw != nil {
		prober, _ = rw.(ir.ReplayKeyProber)
	}
	var checker ir.TableEmptyChecker
	if opts.OnlyNonEmpty {
		if rw != nil {
			checker, _ = rw.(ir.TableEmptyChecker)
		}
		if checker == nil {
			slog.DebugContext(ctx, "replay-duplication door: target writer cannot report table emptiness; no table is treated as populated")
			return nil, nil
		}
	}
	for _, t := range tables {
		if t == nil {
			continue
		}
		if checker != nil {
			empty, err := checker.IsTableEmpty(ctx, t)
			if err != nil {
				return nil, fmt.Errorf("probe target table %q for rows: %w", t.Name, err)
			}
			if empty {
				continue
			}
		}
		if !irbackup.TableReplayIdempotent(t) {
			out = append(out, ReplayKeylessTable{Name: t.Name, Reason: ReplayKeylessRecorded})
			continue
		}
		if prober == nil {
			continue
		}
		exists, keyed, err := prober.ProbeReplayKey(ctx, t)
		if err != nil {
			return nil, fmt.Errorf("probe target table %q for a replay key: %w", t.Name, err)
		}
		if exists && !keyed {
			out = append(out, ReplayKeylessTable{Name: t.Name, Reason: ReplayKeylessTarget})
		}
	}
	return out, nil
}

// RenderReplayKeylessTables formats the list for a refusal message:
// `"a" (reason); "b" (reason)`.
func RenderReplayKeylessTables(tables []ReplayKeylessTable) string {
	parts := make([]string, len(tables))
	for i, t := range tables {
		parts[i] = fmt.Sprintf("%q (%s)", t.Name, t.Reason)
	}
	return strings.Join(parts, "; ")
}
