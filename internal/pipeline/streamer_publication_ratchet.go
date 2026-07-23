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

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
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
// (rowExists=false means a genuinely NEW stream), and whether the
// stream opts into per-table publication attributes (today: at least
// one `--where` row filter on a publication-scoped source).
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
// Zero-value-safe: every input empty/false yields "", so legacy
// streams, non-PG sources, and every programmatic construction that
// never touches publications keep today's behaviour exactly.
func resolveEffectivePublication(explicit, recorded string, rowExists, filtered bool, streamID string) (effective string, explicitOverridesRecorded bool) {
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

// readRecordedPublicationName loads the stream's control row (if any)
// and returns its recorded publication name. rowExists=false is the
// genuinely-new-stream signal the per-stream-default derivation keys
// on; a row with an empty PublicationName is the legacy shape and
// stays on the shared engine default. Tolerant of a missing control
// table by construction — ListStreams returns "no streams" then.
func readRecordedPublicationName(ctx context.Context, applier ir.ChangeApplier, streamID string) (recorded string, rowExists bool, err error) {
	streams, err := applier.ListStreams(ctx)
	if err != nil {
		return "", false, err
	}
	for _, st := range streams {
		if st.StreamID == streamID {
			return st.PublicationName, true, nil
		}
	}
	return "", false, nil
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
	recorded, rowExists, err := readRecordedPublicationName(ctx, applier, streamID)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseSchemaApply, fmt.Errorf("pipeline: read recorded publication name: %w", err))
	}
	effective, overrode := resolveEffectivePublication(s.PublicationName, recorded, rowExists, len(s.RowFilters) > 0, streamID)
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
	return nil
}
