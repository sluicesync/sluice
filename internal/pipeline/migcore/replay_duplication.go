// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// The replay-duplication judge (audit F-E1).
//
// Every path that re-applies backup content onto a target — the
// `sync from-backup` broker re-applying an interrupted incremental, a
// `restore` re-run onto the target an earlier attempt already wrote — is
// sound only for tables on which re-writing a row that already landed
// COLLIDES on a key: the upsert converges (ADR-0010), or a plain load fails
// loudly. On a table where it does not, the re-written rows land a second
// time, silently, at exit 0.
//
// "Collides" is judged TWICE, from two independent sources, and a table
// failing either is reported:
//
//   - the RECORDED schema (the backup manifest's table), by
//     [irbackup.TableReplayIdempotent] — what the source declared;
//   - the TARGET's live catalog, through the engine's
//     [ir.ReplayKeyProber] — whether the key the engine's write path
//     collides on is one the replayed rows actually SUPPLY. A target
//     created or altered without the source's key, or re-keyed on a
//     defaulted surrogate the rows never carry, is caught here and nowhere
//     else.
//
// Each side covers a blind spot of the other. The recorded schema cannot see
// a target that dropped or replaced a key; the target cannot see that a
// KEYLESS source may hold legitimate duplicate rows, which the recorded half
// refuses outright. Neither sees a target key COARSER than a keyed source's
// identity — a MySQL prefix UNIQUE, or a case-insensitive collation over a
// case-sensitive source column: the recorded table is keyed, the target key
// is supplied and does collide, and distinct source rows merge on the FIRST
// apply, not only on a replay (measured on MySQL; audit backlog
// F-E1-COARSER-TARGET-KEY, LOW, open). A table absent on the target is
// judged on the recorded schema alone: it will be created from that schema,
// key included.
//
// The judge FAILS CLOSED. A writer that cannot answer a judgment the
// caller asked for is an error, never a skip: before this, a SQLite target
// (whose writer implemented neither probe) had its restore door silently
// open, and a keyless table restored twice held 200 rows for 100.

// ReplayKeylessReason names which judgment found a table keyless.
type ReplayKeylessReason string

const (
	// ReplayKeylessRecorded means the recorded (source) schema declares no
	// PRIMARY KEY and no all-NOT-NULL, non-partial, plain-column UNIQUE
	// index.
	ReplayKeylessRecorded ReplayKeylessReason = "the backup's recorded schema has no PRIMARY KEY and no NOT NULL UNIQUE index"
	// ReplayKeylessTarget means the target table exists and a re-written
	// row would not collide on any key of it: it has no usable key, or the
	// key the engine collides on includes a column the backup's rows do
	// not carry (a defaulted surrogate), whatever the recorded schema says.
	ReplayKeylessTarget ReplayKeylessReason = "the target table has no PRIMARY KEY or NOT NULL UNIQUE index made of columns the backup's rows carry " +
		"(a key on a column the rows do not supply, such as a serial, identity or defaulted surrogate, never collides)"
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
	// is about to stop existing. When on, the writer MUST implement
	// [ir.ReplayKeyProber].
	ProbeTarget bool

	// OnlyNonEmpty skips a table the target holds no rows in, BEFORE either
	// judgment. The restore re-run door uses it: a keyless table is only at
	// risk when something is already in it. When on, the writer MUST
	// implement [ir.TableEmptyChecker].
	OnlyNonEmpty bool
}

// FindReplayKeylessTables returns, in input order, the tables among tables
// a re-applied INSERT would duplicate rows in. rw is the target's row
// writer, used for the [ir.ReplayKeyProber] and [ir.TableEmptyChecker]
// probes; it may be nil only when neither option is set.
//
// A writer lacking a surface an option needs is refused with an error
// naming the writer and the surface. That is unreachable for every engine
// sluice registers today (TestReplayDoorSurfaceRoster_EveryTargetWriter in
// internal/engines derives the set from the registry), so it exists to keep
// a NEW engine from inheriting an open door, which is what happened to
// SQLite.
func FindReplayKeylessTables(
	ctx context.Context,
	rw ir.RowWriter,
	tables []*ir.Table,
	opts ReplayJudgeOptions,
) ([]ReplayKeylessTable, error) {
	var prober ir.ReplayKeyProber
	if opts.ProbeTarget {
		p, ok := rw.(ir.ReplayKeyProber)
		if !ok {
			return nil, errReplayDoorSurface(rw, "ir.ReplayKeyProber", "whether a re-written row collides on a key")
		}
		prober = p
	}
	var checker ir.TableEmptyChecker
	if opts.OnlyNonEmpty {
		c, ok := rw.(ir.TableEmptyChecker)
		if !ok {
			return nil, errReplayDoorSurface(rw, "ir.TableEmptyChecker", "whether a target table already holds rows")
		}
		checker = c
	}
	var out []ReplayKeylessTable
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
		// One predicate for the doors and the engines' in-run retry gates
		// (irbackup.JudgeReplayKey). A nil prober (ProbeTarget off) judges
		// the recorded schema alone. A table absent on the target passes:
		// it will be created from the recorded schema, key included.
		verdict, err := irbackup.JudgeReplayKey(ctx, prober, t)
		if err != nil {
			return nil, fmt.Errorf("probe target table %q for a replay key: %w", t.Name, err)
		}
		switch verdict {
		case irbackup.ReplayKeylessRecorded:
			out = append(out, ReplayKeylessTable{Name: t.Name, Reason: ReplayKeylessRecorded})
		case irbackup.ReplayKeylessTarget:
			out = append(out, ReplayKeylessTable{Name: t.Name, Reason: ReplayKeylessTarget})
		case irbackup.ReplayKeyCollides, irbackup.ReplayTargetAbsent:
		default:
			return nil, fmt.Errorf("judge table %q for a replay key: unknown verdict %d", t.Name, verdict)
		}
	}
	return out, nil
}

// errReplayDoorSurface is the fail-closed refusal for a target writer that
// cannot answer one of the judgments.
func errReplayDoorSurface(rw ir.RowWriter, surface, question string) error {
	return fmt.Errorf(
		"the target's row writer (%T) does not implement %s, so sluice cannot tell %s; refusing rather than "+
			"writing blind, because a re-written row that does not collide is silently duplicated (audit F-E1). "+
			"This is a sluice bug for any engine that can be a restore or broker target: please report it",
		rw, surface, question,
	)
}

// ReplayKeyFingerprint renders every part of a recorded table the judge
// reads — each column's name, nullability and generated flag, the PRIMARY
// KEY, and every index's uniqueness, predicate and key parts — so a caller
// that caches a table's clearance can tell when a later definition of the
// same table (an AlterTable delta that dropped or replaced a key) needs
// judging again. Two definitions with equal fingerprints are judged
// identically by [FindReplayKeylessTables]'s recorded half.
func ReplayKeyFingerprint(t *ir.Table) string {
	if t == nil {
		return ""
	}
	var sb strings.Builder
	sb.WriteString("cols:")
	for _, c := range t.Columns {
		if c == nil {
			continue
		}
		fmt.Fprintf(&sb, "%q/%t/%t,", c.Name, c.Nullable, c.IsGenerated())
	}
	writeIndex := func(tag string, idx *ir.Index) {
		if idx == nil {
			return
		}
		fmt.Fprintf(&sb, "|%s:%q/%t/%q:", tag, idx.Name, idx.Unique, strings.TrimSpace(idx.Predicate))
		for _, c := range idx.Columns {
			fmt.Fprintf(&sb, "%q/%q,", c.Column, c.Expression)
		}
	}
	writeIndex("pk", t.PrimaryKey)
	for _, idx := range t.Indexes {
		writeIndex("ix", idx)
	}
	return sb.String()
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
