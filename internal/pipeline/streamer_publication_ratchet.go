// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The ADR-0176 prerequisite chunk: per-stream publications with the
// control-state ratchet.
//
// ADR-0175 rejected per-stream publications as an unconditional
// default-flip because the publication name rides every
// START_REPLICATION, including warm resume — a silently-derived new
// default would break every running PG deployment on upgrade. The
// compatibility ratchet here gets the isolation without the break:
//
//   - The stream's effective publication name is RECORDED in its
//     sluice_cdc_state row (the publication_name column, slot_name's
//     exact sibling) on every position-write.
//   - Warm resume READS the record back and reuses it when the
//     operator didn't pass --publication-name; an explicit flag wins
//     over the record (with a WARN naming both when they differ) and
//     updates the record.
//   - Only a genuinely NEW stream (no control row) that opts into
//     per-table publication attributes — today: a `--where` row filter
//     on a publication-scoped source — derives a per-stream default
//     (`sluice_<stream-id>`). Every other combination keeps the shared
//     `sluice_pub` engine default, so a stream with no recorded name
//     behaves byte-identically to before this chunk (zero-value-safe:
//     empty recorded name == legacy).

package pipeline

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// publicationNameSetter is the optional applier-side surface for
// engines that record the active stream's effective publication name
// on the per-target control table — the sibling of [slotNameSetter]
// (ADR-0176 prerequisite chunk). Both shipping appliers implement it
// (the control table lives on the TARGET, and a PG-source sync can
// target any engine); test stubs that don't are skipped cleanly.
type publicationNameSetter interface {
	SetPublicationName(name string)
}

// rowFilterHashSetter is [publicationNameSetter]'s exact sibling (audit
// 2026-07-23 D0-2): the optional applier-side surface for recording the
// canonical hash of the `--where` subset the stream pushes into its
// publication row filter, persisted beside publication_name on every
// position-write. Both shipping appliers implement it; test stubs that
// don't are skipped cleanly.
type rowFilterHashSetter interface {
	SetRowFilterHash(hash string)
}

// rowFilterPushdownHash canonically hashes the classifier-approved pushed
// row-filter subset (table → raw predicate text): fnv64a over the
// lower-cased table name and the raw predicate of each entry, sorted by
// table, each field NUL-terminated so pair boundaries can't be forged by
// concatenation. Rendered as 16 lower-hex bytes — the row_filter_hash
// stored-codec value.
//
// The empty subset hashes to the fnv64a offset basis
// ("cbf29ce484222325") — a deliberate NON-empty sentinel: for a
// publication-scoped source "nothing pushed" is a positive fact that must
// OVERWRITE a previously-recorded hash on the next cold start (the
// --restart-from-scratch escape after removing --where), while the empty
// STRING stays reserved for "not recorded" (legacy rows, non-publication
// sources) which the COALESCE position-write shape preserves rather than
// clobbers.
func rowFilterPushdownHash(filters map[string]string) string {
	type pair struct{ table, pred string }
	pairs := make([]pair, 0, len(filters))
	for t, p := range filters {
		pairs = append(pairs, pair{strings.ToLower(t), p})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].table < pairs[j].table })
	h := fnv.New64a()
	for _, p := range pairs {
		_, _ = h.Write([]byte(p.table))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(p.pred))
		_, _ = h.Write([]byte{0})
	}
	return fmt.Sprintf("%016x", h.Sum64())
}

// pgMaxIdentifierBytes is Postgres's NAMEDATALEN-1 identifier limit.
// CREATE PUBLICATION silently TRUNCATES a longer identifier (a NOTICE,
// not an error), while START_REPLICATION's publication_names argument
// is matched verbatim — so a derived name over the limit would create
// one publication and stream from another, failing with a confusing
// "publication does not exist". The derivation below never emits a
// name over this limit.
const pgMaxIdentifierBytes = 63

// derivePerStreamPublicationName builds the per-stream default
// publication name for a NEW filtered PG-source stream:
// `sluice_<stream-id>`, normalized to a SAFE Postgres identifier.
//
// Normalization (a named wart, deliberately wider than the bare
// [resolvePublicationName] prefix rule): auto-generated stream ids
// look like `postgres://host:5432 -> postgres://host2:5432` — bytes
// PG would accept only quoted and that the operator would then have
// to quote in every psql interaction — so every byte outside
// [a-z0-9_] maps to '_' (uppercase folds to lowercase, matching the
// slot-name character set PG itself enforces). When the result would
// exceed [pgMaxIdentifierBytes], the tail is replaced with an
// fnv64a hash of the full sanitized name so two long stream ids that
// differ only past the cut cannot silently collide on one
// publication. Two SHORT ids that sanitize identically (e.g. case- or
// punctuation-only differences) can still derive the same name — that
// collision is caught LOUDLY by the ADR-0175 scope-conflict guard at
// the second stream's cold start, and --publication-name is the
// documented escape.
func derivePerStreamPublicationName(streamID string) string {
	sanitized := make([]byte, 0, len(streamID))
	for _, r := range streamID {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_':
			sanitized = append(sanitized, byte(r))
		case r >= 'A' && r <= 'Z':
			sanitized = append(sanitized, byte(r-'A'+'a'))
		default:
			sanitized = append(sanitized, '_')
		}
	}
	name := sluiceSlotPrefix + string(sanitized)
	if len(name) <= pgMaxIdentifierBytes {
		return name
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(name))
	suffix := fmt.Sprintf("_%010x", h.Sum64()&0xffffffffff) // 1 + 10 hex bytes
	return name[:pgMaxIdentifierBytes-len(suffix)] + suffix
}

// resolveEffectivePublication implements the ADR-0176 prerequisite
// ratchet decision. Inputs are the operator's explicit
// --publication-name (already prefix-resolved; empty = not passed),
// the name recorded on the stream's control row (empty = none
// recorded — the legacy shape), whether a control row exists at all
// (rowExists=false means a genuinely NEW stream), whether this run is
// a `--reset-target-data` destructive restart, and whether the stream
// opts into per-table publication attributes (today: at least one
// `--where` row filter on a publication-scoped source).
//
// Precedence, highest first:
//
//  1. explicit flag — wins over everything; explicitOverridesRecorded
//     reports a non-empty recorded name that differs (the caller WARNs
//     naming both, and the record updates via the next position-write).
//  2. recorded name — warm-resume continuity without re-passing the flag.
//  3. derived per-stream default `sluice_<stream-id>` — ONLY for a
//     genuinely new stream (no control row) that carries a row filter.
//  4. "" — the engine default (`sluice_pub`), byte-identical to today.
//
// resetTargetData folds into rowExists (audit 2026-07-23 ARCH-3): a
// `--reset-target-data` run IS a from-scratch stream, but its
// ClearStream runs later — inside coldStart, after this phase already
// read the row — so the raw rowExists is stale-true. Treating it as
// false lets a filtered reset derive its per-stream publication instead
// of landing row filters on the shared `sluice_pub` default. A recorded
// name still wins above the derivation: the reset re-uses the stream's
// own already-isolated publication rather than orphaning it (the
// derivation is deterministic from the stream id, so the empty-recorded
// legacy case lands on the same per-stream name either way).
//
// Zero-value-safe: every input empty/false yields "", so legacy
// streams, non-PG sources, and every programmatic construction that
// never touches publications keep today's behaviour exactly.
func resolveEffectivePublication(explicit, recorded string, rowExists, resetTargetData, filtered bool, streamID string) (effective string, explicitOverridesRecorded bool) {
	if resetTargetData {
		rowExists = false
	}
	if explicit != "" {
		return explicit, recorded != "" && recorded != explicit
	}
	if recorded != "" {
		return recorded, false
	}
	if !rowExists && filtered {
		return derivePerStreamPublicationName(streamID), false
	}
	return "", false
}

// rowFilterHashDrift is the D0-2 warm-resume drift decision (audit
// 2026-07-23), pure so the whole matrix is unit-pinnable:
//
//   - only an EXISTING control row can drift (a genuinely new stream has
//     nothing recorded — cold start pushes and records the current subset);
//   - --restart-from-scratch / --reset-target-data skip the comparison:
//     both force a cold start that re-ensures the publication with the
//     CURRENT filters and re-records the hash — they are two of the
//     refusal's own named escapes and must never be blocked by it;
//   - an empty recorded hash is "not recorded" (legacy rows, pre-drift
//     binaries, non-publication paths): unknown — allow, the same
//     tolerance the ADR-0031 fingerprint check uses;
//   - otherwise any mismatch refuses — INCLUDING current-empty-subset vs
//     recorded-non-empty (a REMOVED --where, D0-2's worst variant,
//     previously zero-signal): the empty subset hashes to the non-empty
//     sentinel, never to "".
func rowFilterHashDrift(rowExists, restartFromScratch, resetTargetData bool, recorded, current string) bool {
	if !rowExists || restartFromScratch || resetTargetData || recorded == "" {
		return false
	}
	return recorded != current
}

// readRecordedPublicationState loads the stream's control row (if any)
// and returns it whole — the recorded publication name the ratchet keys
// on, plus the recorded row_filter_hash the D0-2 drift comparison reads.
// rowExists=false is the genuinely-new-stream signal the
// per-stream-default derivation keys on; a row with an empty
// PublicationName is the legacy shape and stays on the shared engine
// default. Tolerant of a missing control table by construction —
// ListStreams returns "no streams" then.
func readRecordedPublicationState(ctx context.Context, applier ir.ChangeApplier, streamID string) (st ir.StreamStatus, rowExists bool, err error) {
	streams, err := applier.ListStreams(ctx)
	if err != nil {
		return ir.StreamStatus{}, false, err
	}
	for _, s := range streams {
		if s.StreamID == streamID {
			return s, true, nil
		}
	}
	return ir.StreamStatus{}, false, nil
}

// phaseResolvePublicationScope is runOnce's ---- 2.8 ---- step: the
// ADR-0176 prerequisite ratchet. It runs AFTER the control table is
// prepared (the record lives there) and BEFORE any source connection
// opens (the publication name rides EnsurePublication and every
// START_REPLICATION), and it finalizes the stream's effective
// publication:
//
//   - resolves explicit-vs-recorded-vs-derived per
//     [resolveEffectivePublication] (WARN when an explicit flag
//     overrides a different recorded name);
//   - re-pushes the scope into the source engine when the effective
//     name differs from what phaseResolveStreamIdentity pushed;
//   - records the effective name on the applier so every subsequent
//     position-write persists it (the cold-start anchor write is the
//     first, landing after EnsurePublication succeeds).
//
// No-op for sources without [ir.PublicationScoper] (the MySQL family,
// trigger-CDC engines): nothing to ratchet, nothing recorded.
func (s *Streamer) phaseResolvePublicationScope(ctx context.Context, applier ir.ChangeApplier, streamID string) error {
	scoper, ok := s.Source.(ir.PublicationScoper)
	if !ok {
		return nil
	}
	st, rowExists, err := readRecordedPublicationState(ctx, applier, streamID)
	if err != nil {
		// connectHint, not PhaseSchemaApply: phase 2.8 runs on every retry
		// re-establish, so a transient network blip in this control-table
		// read must ride runWithRetry's connect-transient fall-through like
		// the applier-open sites (audit 2026-07-23 ARCH-4). The drift
		// REFUSAL below keeps its coded terminal shape.
		return connectHint(fmt.Errorf("pipeline: read recorded publication name: %w", err))
	}
	recorded := st.PublicationName
	effective, overrode := resolveEffectivePublication(s.PublicationName, recorded, rowExists, s.ResetTargetData, len(s.RowFilters) > 0, streamID)
	if overrode {
		slog.WarnContext(
			ctx, "explicit --publication-name overrides the publication recorded for this stream; the record updates to the explicit name. The stream will read through the EXPLICIT publication — make sure that is intended, or drop the flag to resume through the recorded one",
			slog.String("stream_id", streamID),
			slog.String("recorded", recorded),
			slog.String("explicit", s.PublicationName),
		)
	}
	if effective != s.PublicationName {
		switch {
		case recorded != "":
			slog.InfoContext(
				ctx, "resuming through the publication recorded for this stream (pass --publication-name to override)",
				slog.String("stream_id", streamID),
				slog.String("publication", effective),
			)
		default:
			slog.InfoContext(
				ctx, "new filtered stream: using a per-stream publication so its row-filter scope can never collide with another stream's (ADR-0176 prerequisite; pass --publication-name to override)",
				slog.String("stream_id", streamID),
				slog.String("publication", effective),
			)
		}
		s.PublicationName = effective
		s.Source = scoper.WithPublicationScope(s.PublicationName, s.SlotName)
	}
	if !s.DryRun {
		if setter, ok := applier.(publicationNameSetter); ok {
			setter.SetPublicationName(s.PublicationName)
		}
	}
	// ADR-0176 + audit 2026-07-23 D0-2: the pushed row-filter subset is
	// DURABLE source-side catalog state (warm resume never re-ensures the
	// publication), so a source that can carry publication row filters gets
	// the reconciliation contract here — compare the recorded hash against
	// the current run's pushed subset, refuse loudly on drift, and record
	// the current hash beside publication_name on every position-write.
	if _, ok := s.Source.(ir.PublicationRowFilterer); ok {
		currentHash := rowFilterPushdownHash(s.publicationRowFilters)
		if rowFilterHashDrift(rowExists, s.RestartFromScratch, s.ResetTargetData, st.RowFilterHash, currentHash) {
			pushed := make([]string, 0, len(s.publicationRowFilters))
			for table := range s.publicationRowFilters {
				pushed = append(pushed, table)
			}
			sort.Strings(pushed)
			return sluicecode.Wrap(
				sluicecode.CodeWherePushdownDrift,
				"re-run with the --where this stream was established with, or --restart-from-scratch to re-snapshot under the new predicate (required for a widened filter anyway; on a PG source the restart first refuses on the stream's existing replication slot — drop it as that refusal instructs, then re-run), or --reset-target-data for a destructive reset",
				fmt.Errorf(
					"pipeline: warm resume refused: the current --where flags don't match the row-filter subset stream %q "+
						"pushed into its publication at cold start (recorded row_filter_hash %s, current %s; currently-pushed "+
						"tables: [%s]). The publication row filter is durable source-side state a warm resume never re-ensures, "+
						"so resuming would leave the SERVER silently filtering on the stale predicate — rows the new flags admit "+
						"would never be delivered, unobservably (audit 2026-07-23 D0-2)",
					streamID, st.RowFilterHash, currentHash, strings.Join(pushed, ", "),
				),
			)
		}
		if !s.DryRun {
			if setter, ok := applier.(rowFilterHashSetter); ok {
				setter.SetRowFilterHash(currentHash)
			}
		}
	}
	// ADR-0176: thread the classifier-approved `--where` predicates into the
	// source engine alongside the publication scope, so cold start's
	// EnsurePublication emits them as per-table row filters (PG 15+; the
	// engine gates the version). Applied HERE — after the effective
	// publication is final and before any source connection opens — for the
	// same reason the scope itself is. Warm resume never re-ensures the
	// publication, so carrying the filters on the engine value never mutates
	// anything on resume (the unchanged ADR-0175/0176 invariant).
	if len(s.publicationRowFilters) > 0 {
		if rf, ok := s.Source.(ir.PublicationRowFilterer); ok {
			s.Source = rf.WithPublicationRowFilters(s.publicationRowFilters)
		}
	}
	return nil
}
