// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// The table-identity half of a stopped cold start's recorded copy
// (RESUMED-COLD-START-CERTIFIES-RECREATED-TABLE).
//
// The stopped-cold-start resume ([Streamer.resumeStoppedColdStart]) SKIPS
// the copy of every table the stopped run recorded complete. Its gates
// proved the copy finished, the flags are the same, the target still holds
// rows and the slot still stands at the snapshot. None of them proved the
// tables are the same tables: the progress store keys them by name. A table
// dropped and re-created under its name while the stream was stopped was
// therefore treated as copied. Measured on Postgres 16: under FOR ALL TABLES
// the target kept the old table's rows, missed the new one's, and took its
// later changes onto the old copy, permanently, at exit 0; under FOR TABLE
// the resumed run applied none of the new table's changes until the next
// start refused. The anchor's record then certified the new table's id, so
// every later identity check passed.
//
// So the run records each in-scope table's engine id when its copy starts,
// beside the copy-shape fingerprint (the `table_ids` aspect), and the resume
// compares them table by table: a different id refuses, naming the table;
// an id it cannot read, or a record without them, means the resume cannot
// prove the tables are the ones copied, and it stands down. Only ids that
// are proof count ([ir.TableIdentityReader.TableIdentitySurvivesRebuild]:
// Postgres `pg_class.oid`). A source whose ids change on a rebuild (InnoDB
// TABLE_ID changes on OPTIMIZE TABLE) has no proof to offer, so a resume
// from it stands down too. Today only a Postgres source can reach the
// resume at all ([ir.SnapshotAnchorVerifier]); the rule is written for the
// next engine that does.
//
// Residual, stated: the ids are read when the run records its anchor, just
// after the snapshot opened, so a table re-created between those two
// statements is recorded with its new id. That window is inside the stopped
// run, not across the stop this check exists for.

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// copyShapeTableIDsKey is the copy-shape aspect holding the ids: one per
// table, in sorted table-name order, joined by "." ("" where none was read).
// It is compared by [Streamer.resumeTableIdentityGate], never by
// [copyShapeDrift], which reads only the keys the current shape carries.
const copyShapeTableIDsKey = "table_ids"

// copyTableIdentities reads the engine id of every table in schema, in
// sorted name order. proof is false when the source's ids do not prove a
// table's identity, and then names and ids are empty.
func (s *Streamer) copyTableIdentities(ctx context.Context, schema *ir.Schema) (names []string, ids map[string]string, proof bool) {
	reader, ok := s.Source.(ir.TableIdentityReader)
	if !ok || !reader.TableIdentitySurvivesRebuild() || schema == nil {
		return nil, nil, false
	}
	for _, t := range schema.Tables {
		if t != nil {
			names = append(names, t.Name)
		}
	}
	sort.Strings(names)
	return names, s.establishedIDs(ctx, names), true
}

// withCopyTableIDs adds the table_ids aspect to shape, when the source's
// ids are proof.
func (s *Streamer) withCopyTableIDs(ctx context.Context, shape string, schema *ir.Schema) string {
	names, ids, proof := s.copyTableIdentities(ctx, schema)
	if !proof {
		return shape
	}
	vals := make([]string, len(names))
	for i, n := range names {
		vals[i] = ids[n]
	}
	return shape + ";" + copyShapeTableIDsKey + "=" + strings.Join(vals, ".")
}

// resumeTableIdentityGate is the identity gate of the stopped-cold-start
// resume (see the file comment): (true, nil) when every table is proven the
// one the stopped run copied; (false, nil) when that cannot be proven, so
// the run proceeds as if there were nothing to resume; (false, refusal)
// when a table is proven a different one.
func (s *Streamer) resumeTableIdentityGate(ctx context.Context, streamID string, state ir.MigrationState, schema *ir.Schema) (ok bool, refusal error) {
	notResuming := func(why string) (bool, error) {
		slog.InfoContext(ctx, "pipeline: this stream's stopped cold start is not resumed: "+why+
			"; a resume skips the copy, so it must prove each table is the one the stopped run copied",
			slog.String("stream_id", streamID))
		return false, nil
	}
	names, ids, proof := s.copyTableIdentities(ctx, schema)
	if !proof {
		return notResuming("this source's table ids do not prove a table's identity")
	}
	recorded, has := parseCopyShape(state.CopyShape)[copyShapeTableIDsKey]
	if !has {
		return notResuming("the recorded copy carries no table ids (recorded by an earlier release)")
	}
	rec := strings.Split(recorded, ".")
	if len(rec) != len(names) {
		return notResuming("the recorded table ids do not match the current table set")
	}
	var changed, unproven []string
	for i, n := range names {
		switch now := ids[n]; {
		case rec[i] == "" || now == "":
			unproven = append(unproven, n)
		case rec[i] != now:
			changed = append(changed, fmt.Sprintf("%s (copied as id %s, now id %s)", n, rec[i], now))
		}
	}
	if len(changed) > 0 {
		slot := resolveSlotNameForDisplay(s.SlotName)
		return false, fmt.Errorf("pipeline: %s: TABLE-IDENTITY-CHANGED: this stream's recorded cold start finished its "+
			"bulk copy, but %s %s no longer the table it copied: dropped and re-created under the same name (or swapped) "+
			"while the stream was stopped. Resuming would SKIP the copy and leave the target holding the old table's rows "+
			"and receiving the new one's changes, silently. sluice has changed NOTHING. Re-copy: drop the stopped run's "+
			"replication slot with `sluice slot drop --source-driver %s --source <SOURCE-DSN> %s --yes`, then re-run "+
			"your `sluice sync start` command with --restart-from-scratch added", // remedy-partial: the operator's own invocation carries their DSNs and flags
			coldStartShapeChangedMarker, strings.Join(changed, ", "), tableIsAre(len(changed)), s.Source.Name(), slot)
	}
	if len(unproven) > 0 {
		return notResuming("no table id could be read for " + strings.Join(unproven, ", "))
	}
	return true, nil
}

// tableIsAre is the verb for n tables.
func tableIsAre(n int) string {
	if n == 1 {
		return "is"
	}
	return "are"
}
