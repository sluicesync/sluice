// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"strings"
	"sync"

	"sluicesync.dev/sluice/internal/ir"
)

// ReplayKeyVerdict is the answer to "does re-writing a row of this table
// that may already have landed collide on a key, or land a second copy?"
// It combines the two judgments every re-write path owes (audit F-E1): the
// RECORDED table, by [TableReplayIdempotent], and the TARGET table, by the
// engine's [ir.ReplayKeyProber] against [ReplaySuppliedColumns].
//
// The zero value is deliberately not a verdict: a caller that forgets to
// judge must not read as "collides".
type ReplayKeyVerdict int

const (
	// ReplayKeyCollides means the recorded table has a key, and the target
	// table's key — the one its write path collides on — is made of
	// columns the rows supply. A re-written row converges (upsert) or fails
	// loudly (plain load); it never lands twice.
	ReplayKeyCollides ReplayKeyVerdict = iota + 1
	// ReplayKeylessRecorded means the recorded table declares no PRIMARY KEY and
	// no all-NOT-NULL, non-partial, plain-column UNIQUE index.
	ReplayKeylessRecorded
	// ReplayKeylessTarget means the target table exists and no key of it that
	// the write path collides on is fully supplied by the rows — it has no
	// usable key, or its key includes a column the rows do not carry (an
	// AUTO_INCREMENT, serial, identity or defaulted surrogate draws a fresh
	// value for every re-written row and collides with nothing).
	ReplayKeylessTarget
	// ReplayTargetAbsent means the recorded table has a key and the target has
	// no table of that name. A door about to CREATE the table from the
	// recorded schema treats this as keyed; a path that is already writing
	// into the table cannot, because the table it writes is not the one
	// the probe found.
	ReplayTargetAbsent
)

// JudgeReplayKey is the one F-E1 replay-key predicate. It judges the
// recorded table first and, only when that passes, asks prober about the
// target table. A nil prober judges the recorded table alone, returning
// [ReplayKeyCollides] for a keyed one; a caller that needs the target
// judgment must refuse a writer lacking [ir.ReplayKeyProber] before it
// gets here (the replay doors in internal/pipeline/migcore do). An error
// from the probe is returned as-is with a zero verdict: a probe that
// cannot answer never reads as "keyed".
//
// Its callers are the replay doors (migcore.FindReplayKeylessTables) and
// the engines' in-run retry gates, through [ReplayKeyCache].
func JudgeReplayKey(ctx context.Context, prober ir.ReplayKeyProber, table *ir.Table) (ReplayKeyVerdict, error) {
	if !TableReplayIdempotent(table) {
		return ReplayKeylessRecorded, nil
	}
	if prober == nil {
		return ReplayKeyCollides, nil
	}
	exists, keyed, err := prober.ProbeReplayKey(ctx, table)
	if err != nil {
		return 0, err
	}
	return targetVerdict(exists, keyed), nil
}

// Describe renders the verdict as the clause a refusal states about its
// table ("table %q <clause>"), so every refusal built on the verdict says
// which judgment failed in the same words.
func (v ReplayKeyVerdict) Describe() string {
	switch v {
	case ReplayKeyCollides:
		return "has a key the rows carry and collide on"
	case ReplayKeylessRecorded:
		return "has no PRIMARY KEY and no NOT NULL UNIQUE index"
	case ReplayKeylessTarget:
		return "has no PRIMARY KEY or NOT NULL UNIQUE index on the target made of columns the rows carry " +
			"(a key on a column the rows do not supply, such as an AUTO_INCREMENT, serial, identity or defaulted " +
			"surrogate, draws a fresh value for every re-written row and never collides)"
	case ReplayTargetAbsent:
		return "was not found on the target by the key probe, so whether a re-written row collides cannot be judged"
	default:
		return "could not be judged for a key"
	}
}

// RemedyHint is the operator remedy for a verdict other than
// [ReplayKeyCollides], for a refusal's hint.
func (v ReplayKeyVerdict) RemedyHint() string {
	if v == ReplayKeylessTarget {
		return "give the TARGET table a PRIMARY KEY or NOT NULL UNIQUE index made of columns the source rows carry " +
			"(not an AUTO_INCREMENT, serial, identity or defaulted surrogate), or let sluice create the table, then re-run"
	}
	return "add a PRIMARY KEY or a NOT NULL UNIQUE index to the table, then re-run"
}

func targetVerdict(exists, keyed bool) ReplayKeyVerdict {
	switch {
	case !exists:
		return ReplayTargetAbsent
	case !keyed:
		return ReplayKeylessTarget
	default:
		return ReplayKeyCollides
	}
}

// ReplayKeyCache is [JudgeReplayKey] with the target probe memoised, for a
// row writer that has to judge the same table once per retried batch from
// several parallel workers. The recorded half is pure and is re-judged
// every call; only a SUCCESSFUL probe is cached (an error is retried on the
// next call), keyed by the table name and the columns the rows supply —
// everything of the recorded table the probe reads. Residual, stated: a
// key dropped from the target out of band after the first probe is not
// seen for the rest of the writer's life (the broker door carries the same
// residual). A writer whose own target scope can move (Postgres's
// [ir.SchemaSetter]) calls [ReplayKeyCache.Reset] when it does.
//
// The zero value is ready to use. It must not be copied after first use.
type ReplayKeyCache struct {
	mu     sync.Mutex
	probed map[string]probedReplayKey
}

type probedReplayKey struct{ exists, keyed bool }

// Judge is [JudgeReplayKey] through the cache. Unlike JudgeReplayKey it
// has no recorded-only mode: prober must be non-nil (the retry gates pass
// the writer itself).
func (c *ReplayKeyCache) Judge(ctx context.Context, prober ir.ReplayKeyProber, table *ir.Table) (ReplayKeyVerdict, error) {
	if !TableReplayIdempotent(table) {
		return ReplayKeylessRecorded, nil
	}
	key := table.Name + "\x00" + strings.Join(ReplaySuppliedColumns(table), "\x00")
	c.mu.Lock()
	got, ok := c.probed[key]
	c.mu.Unlock()
	if ok {
		return targetVerdict(got.exists, got.keyed), nil
	}
	exists, keyed, err := prober.ProbeReplayKey(ctx, table)
	if err != nil {
		return 0, err
	}
	c.mu.Lock()
	if c.probed == nil {
		c.probed = map[string]probedReplayKey{}
	}
	c.probed[key] = probedReplayKey{exists: exists, keyed: keyed}
	c.mu.Unlock()
	return targetVerdict(exists, keyed), nil
}

// Reset forgets every cached probe.
func (c *ReplayKeyCache) Reset() {
	c.mu.Lock()
	c.probed = nil
	c.mu.Unlock()
}
