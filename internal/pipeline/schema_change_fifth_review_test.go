// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// GC-44 F5 fifth review: the unit pins.
//
//   - Finding 1 (MEDIUM-HIGH, silent): an AMBIGUOUS-SCHEMA-BOUNDARY
//     acknowledgement was not bound to its occurrence. Its fingerprint
//     hashed the table and the refused columns, and the value was read on
//     every wiring and never cleared, so an acknowledged replay of
//     (10,2) → (12,4) pre-accepted a later GENUINE rollback (12,4) → (10,2)
//     of the same column — after an ADR-0038 retry in the same process, or
//     on a restart with the flag left in a unit file. The fingerprint now
//     hashes the position the attempt resumed from, and the Streamer
//     consumes the value once a later position persists.
//   - Finding 2: the AMBIGUOUS note no longer calls a matching definition
//     proof of a replay.
//
// The independent expected value of every case is the table written here
// from what the source did (a replay, or a later genuine change at a newer
// persisted position), not derived from the check.

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// pgResume is a Postgres resume position (a persisted LSN).
func pgResume(lsn string) ir.Position { return ir.Position{Engine: "postgres", Token: lsn} }

// ambiguousAckShape is one AMBIGUOUS boundary the acknowledgement can
// vouch for: deps (the target and history the stream holds) and the replay
// boundary that refuses against them.
type ambiguousAckShape struct {
	name   string
	deps   func() unforwardedBoundaryDeps
	replay ir.SchemaSnapshot
}

// ambiguousAckShapes is every arm an AMBIGUOUS refusal is raised on — each
// narrowing family, the DROP COLUMN, and the unwitnessed fallback — so the
// binding is pinned for the class, not one representative.
func ambiguousAckShapes() []ambiguousAckShape {
	var shapes []ambiguousAckShape
	for _, f := range narrowingFamilies() {
		wide, narrow := witnessTable(wcol("v", f.wide)), witnessTable(wcol("v", f.narrow))
		shapes = append(shapes, ambiguousAckShape{
			name:   "narrowing / " + f.name,
			deps:   func() unforwardedBoundaryDeps { return fourthReviewDeps(wide, wide, 0, wide) },
			replay: refuseAt(narrow, 0),
		})
	}
	withA, withoutA := witnessTable(wcol("a", ir.Integer{Width: 32})), witnessTable()
	shapes = append(shapes, ambiguousAckShape{
		name:   "DROP COLUMN",
		deps:   func() unforwardedBoundaryDeps { return fourthReviewDeps(withA, withA, 0, withA) },
		replay: refuseAt(withoutA, 0),
	})
	wide := witnessTable(wcol("v", ir.Decimal{Precision: 12, Scale: 4}))
	narrow := witnessTable(wcol("v", ir.Decimal{Precision: 10, Scale: 2}))
	shapes = append(shapes, ambiguousAckShape{
		name: "unwitnessed fallback",
		deps: func() unforwardedBoundaryDeps {
			w := newFakeWitness(&fakeCatalog{tables: map[string]*ir.Table{}}, "postgres", "postgres")
			w.orderer = numericOrderer{}
			w.history = []*ir.Table{wide}
			w.retained = retainedHistory{wide: {anchor: testPos(0), recorded: []*ir.Table{wide}}}
			return unforwardedBoundaryDeps{
				witnessFor: func(string) *firstBoundaryWitness { return w },
				orderer:    numericOrderer{}, why: "--schema-changes=refuse",
			}
		},
		replay: refuseAt(narrow, 0),
	})
	return shapes
}

// TestAmbiguousAck_BoundToTheResumePosition is Finding 1 at the intercept,
// every arm: the replay refused at one persisted position is accepted on
// its fingerprint from that position (a retry, or a restart that persisted
// nothing), and the IDENTICAL boundary arriving after the stream has
// persisted past it — the reviewer's genuine rollback narrowing — is
// refused, saying the acknowledgement does not name it.
func TestAmbiguousAck_BoundToTheResumePosition(t *testing.T) {
	t.Parallel()
	shapes := ambiguousAckShapes()
	if len(shapes) < 17 {
		t.Fatalf("matrix holds %d shapes; floor 17", len(shapes))
	}
	replayAt, laterAt := pgResume("0/16B3748"), pgResume("0/16C0000")
	for _, sh := range shapes {
		at := func(pos ir.Position, ack string) unforwardedBoundaryDeps {
			d := sh.deps()
			d.resumedFrom, d.acknowledged = pos, ack
			return d
		}
		_, err := runRefuseIntercept(t, at(replayAt, ""), sh.replay)
		if err == nil || !strings.Contains(err.Error(), ambiguousBoundaryMarker) {
			t.Errorf("%s: err %v; want the AMBIGUOUS refusal", sh.name, err)
			continue
		}
		fp := ambiguousFingerprint(t, err)

		if out, err := runRefuseIntercept(t, at(replayAt, fp), sh.replay); err != nil || len(out) != 1 {
			t.Errorf("%s: the replay, acknowledged from its own position: out %d, err %v; want accepted", sh.name, len(out), err)
		}
		_, err = runRefuseIntercept(t, at(laterAt, fp), sh.replay)
		switch {
		case !errors.Is(err, ir.ErrSchemaChangeRefused):
			t.Errorf("%s: SILENT — the same change after the stream persisted past the replay was accepted on "+
				"the replay's acknowledgement (err %v)", sh.name, err)
		case !strings.Contains(err.Error(), "does not name this boundary"):
			t.Errorf("%s: refused, but without saying the acknowledgement was not applied: %v", sh.name, err)
		case ambiguousFingerprint(t, err) == fp:
			t.Errorf("%s: the later boundary printed the replay's fingerprint", sh.name)
		}
	}
}

// TestStreamer_AmbiguousAckConsumedOncePastTheReplay is Finding 1 at the
// Streamer: an attempt accepts the replay on the acknowledgement; a retry
// from the SAME persisted position keeps it (the same replay is
// re-delivered); the first attempt that resumes from a later position
// clears it in process, so the reviewer's genuine narrowing refuses —
// and a fresh process with the flag left in its unit file, resuming from
// that later position, refuses too.
func TestStreamer_AmbiguousAckConsumedOncePastTheReplay(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	wide := witnessTable(wcol("v", ir.Decimal{Precision: 12, Scale: 4}))
	narrow := witnessTable(wcol("v", ir.Decimal{Precision: 10, Scale: 2}))
	base := func() unforwardedBoundaryDeps { return fourthReviewDeps(wide, wide, 0, wide) }
	replayAt, laterAt := pgResume("0/16B3748"), pgResume("0/16C0000")

	s := &Streamer{}
	s.bindAmbiguousAcknowledgement(ctx, replayAt, true)
	_, err := runRefuseIntercept(t, s.ambiguousAckDeps(base()), refuseAt(narrow, 0))
	fp := ambiguousFingerprint(t, err)

	s.AcceptUnforwardedSchemaChange = fp
	s.bindAmbiguousAcknowledgement(ctx, replayAt, true)
	if _, err := runRefuseIntercept(t, s.ambiguousAckDeps(base()), refuseAt(narrow, 0)); err != nil {
		t.Fatalf("the acknowledged replay: %v", err)
	}
	if s.ambiguousAck.acceptedAt.Load() == nil {
		t.Fatal("the acceptance was not reported to the Streamer")
	}

	// An ADR-0038 retry before anything persisted: the same replay.
	s.bindAmbiguousAcknowledgement(ctx, replayAt, true)
	if s.AcceptUnforwardedSchemaChange != fp {
		t.Fatal("a retry from the same position consumed the acknowledgement; the re-delivered replay would refuse")
	}
	if _, err := runRefuseIntercept(t, s.ambiguousAckDeps(base()), refuseAt(narrow, 0)); err != nil {
		t.Fatalf("the replay re-delivered by a retry: %v", err)
	}

	// The stream applied past the replay and persisted; a later retry
	// meets the genuine rollback narrowing.
	s.bindAmbiguousAcknowledgement(ctx, laterAt, true)
	if s.AcceptUnforwardedSchemaChange != "" {
		t.Errorf("the acknowledgement survived a persisted position past the replay it accepted: %q", s.AcceptUnforwardedSchemaChange)
	}
	if _, err := runRefuseIntercept(t, s.ambiguousAckDeps(base()), refuseAt(narrow, 0)); !errors.Is(err, ir.ErrSchemaChangeRefused) {
		t.Errorf("SILENT — in process, after a retry past the replay, the genuine narrowing was accepted (err %v)", err)
	}

	// A restart with the flag left in the unit file.
	fresh := &Streamer{AcceptUnforwardedSchemaChange: fp}
	fresh.bindAmbiguousAcknowledgement(ctx, laterAt, true)
	if _, err := runRefuseIntercept(t, fresh.ambiguousAckDeps(base()), refuseAt(narrow, 0)); !errors.Is(err, ir.ErrSchemaChangeRefused) {
		t.Errorf("SILENT — after a restart past the replay, the genuine narrowing was accepted on the stale flag (err %v)", err)
	}
}

// TestAmbiguousNote_AMatchIsNotProof is Finding 2: the note calls a
// matching definition consistent with a replay, names the two histories a
// definition comparison cannot see, asks about DDL while stopped, and
// names re-copying as the safe answer.
func TestAmbiguousNote_AMatchIsNotProof(t *testing.T) {
	t.Parallel()
	note := newAmbiguousBoundary("public.w", "d", unforwardedBoundaryDeps{}, true).note()
	for _, want := range []string{
		"consistent with a replay but does not prove one",
		"narrowed and widened back",
		"dropped and added again",
		"nobody ran DDL",
		"re-copy the table",
	} {
		if !strings.Contains(note, want) {
			t.Errorf("the AMBIGUOUS note lacks %q: %s", want, note)
		}
	}
	if strings.Contains(note, "If they are the same, it is (1)") {
		t.Errorf("the note still treats a matching definition as proof of a replay: %s", note)
	}
}

// TestShardReattemptRecovery_WordedForTheCommand is the Shape A half of
// Finding 3: the shard-value-present refusal a sync cold start hits on a
// re-copy offered `--resume` (a migrate flag sync lacks) and
// `--reset-target-data` (which drops every sibling shard's rows). The sync
// wording names the per-shard delete; both name what the reset destroys.
func TestShardReattemptRecovery_WordedForTheCommand(t *testing.T) {
	t.Parallel()
	sync := shardReattemptRecovery(preflightModeSync, "mt", "source_shard_id", "shard_a")
	for _, want := range []string{"DELETE FROM mt WHERE source_shard_id = 'shard_a'", "sibling shards' rows stay", "EVERY shard's rows"} {
		if !strings.Contains(sync, want) {
			t.Errorf("the sync recovery lacks %q: %s", want, sync)
		}
	}
	if strings.Contains(sync, "--resume") {
		t.Errorf("the sync recovery offers --resume, a migrate flag: %s", sync)
	}
	migrate := shardReattemptRecovery(preflightModeMigrate, "mt", "source_shard_id", "shard_a")
	for _, want := range []string{"--resume", "EVERY shard's rows"} {
		if !strings.Contains(migrate, want) {
			t.Errorf("the migrate recovery lacks %q: %s", want, migrate)
		}
	}
}
