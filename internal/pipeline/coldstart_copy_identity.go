// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// The table identity a cold start's copy carries forward
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
// The class is wider than the resume: every id written into the stream's
// record after a copy was a FRESH read, so a table re-created between its
// copy and that write was recorded as the table that was copied. The ids
// are therefore read once, when the copy starts ([Streamer.captureCopyIdentities]),
// and carried:
//
//   - into the recorded copy's fingerprint (the `table_ids` aspect), which
//     the resume compares table by table ([Streamer.resumeTableIdentityGate]);
//   - into the CDC anchor, which re-reads them and refuses on any table
//     whose id moved since the copy ([Streamer.anchorTableIDs]); the record
//     written after the anchor takes the anchor's verified ids, never a new
//     read ([Streamer.recordedTableIDs]).
//
// `schema add-table` follows the same rule for its one table
// ([AddTable.verifiedTableIdentity]).
//
// Only ids that are proof count ([ir.TableIdentityReader.TableIdentitySurvivesRebuild]:
// Postgres `pg_class.oid`). A source whose ids change on a rebuild (InnoDB
// TABLE_ID changes on OPTIMIZE TABLE) has none to offer: its anchor and
// record keep the fresh read they always had, a changed id there being as
// likely routine maintenance (the scope door's rule), and a resume from it
// stands down. Today only a Postgres source can reach the resume at all
// ([ir.SnapshotAnchorVerifier]).
//
// Residual, stated: the copy-time ids are read just after the snapshot
// opens, so a table re-created between those two statements is recorded
// with its new id. That window is a few statements inside the run, not the
// copy or the stop this check exists for.

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

// coldStartIdentities is what a cold-start lane knows of its tables' ids:
// the ones its copy started with, whether the source's ids are proof, and
// the ones its anchor verified (nil until the anchor is written).
type coldStartIdentities struct {
	copied   map[string]string
	proof    bool
	anchored map[string]string
}

// identityProof reports whether the source's table ids prove a table's
// identity.
func (s *Streamer) identityProof() bool {
	reader, ok := s.Source.(ir.TableIdentityReader)
	return ok && reader.TableIdentitySurvivesRebuild()
}

// captureCopyIdentities reads, when the copy starts, the ids of the tables
// it is about to copy (keys as the stream's record keys them).
func (s *Streamer) captureCopyIdentities(ctx context.Context, keys []string) {
	s.coldStartIDs = coldStartIdentities{proof: s.identityProof()}
	if s.coldStartIDs.proof {
		s.coldStartIDs.copied = nonNil(s.establishedIDs(ctx, keys))
	}
}

// coldStartBeforeAnchorIDsForTest, when set, runs right before a cold-start
// anchor re-reads its tables' ids: the window between the copy and the
// anchor a re-created table is caught in.
var coldStartBeforeAnchorIDsForTest func()

// anchorTableIDs returns the ids a cold-start anchor records for keys. With
// proof it re-reads them and refuses any table whose id moved since the
// copy, recording the copy-time ids (none for a table the copy did not
// identify, and none at all on a lane that captured none); without proof it
// records a fresh read, as before.
func (s *Streamer) anchorTableIDs(ctx context.Context, keys []string) (map[string]string, error) {
	if coldStartBeforeAnchorIDsForTest != nil {
		coldStartBeforeAnchorIDsForTest()
	}
	current := s.establishedIDs(ctx, keys)
	if !s.identityProof() {
		s.coldStartIDs.anchored = nonNil(current)
		return current, nil
	}
	ids, changed := verifyCopyIdentities(s.coldStartIDs.copied, current, keys)
	if len(changed) > 0 {
		return nil, s.copyIdentityChanged(changed, "between its copy and its CDC anchor (sluice has not written the anchor)")
	}
	s.coldStartIDs.anchored = ids
	return ids, nil
}

// recordedTableIDs returns the ids the record written after the anchor
// carries: the anchor's verified ones, or a fresh read where no anchor of
// this lane verified any.
func (s *Streamer) recordedTableIDs(ctx context.Context, keys []string) map[string]string {
	if s.coldStartIDs.anchored != nil {
		return s.coldStartIDs.anchored
	}
	if s.identityProof() {
		ids, _ := verifyCopyIdentities(s.coldStartIDs.copied, nil, keys)
		return ids
	}
	return s.establishedIDs(ctx, keys)
}

// verifyCopyIdentities compares the ids a copy started with against current
// ones: changed names each key whose id moved (both read, and different);
// ids holds the copy-time id of every key that has one.
func verifyCopyIdentities(copied, current map[string]string, keys []string) (ids map[string]string, changed []string) {
	ids = map[string]string{}
	for _, k := range keys {
		was, now := copied[k], current[k]
		if was == "" {
			continue
		}
		if now != "" && now != was {
			changed = append(changed, fmt.Sprintf("%s (copied as id %s, now id %s)", displayKey(k), was, now))
			continue
		}
		ids[k] = was
	}
	return ids, changed
}

// copyIdentityChanged is the TABLE-IDENTITY-CHANGED refusal of a copy whose
// tables moved: when says where in the cold start it was found.
func (s *Streamer) copyIdentityChanged(changed []string, when string) error {
	return fmt.Errorf("pipeline: %s: TABLE-IDENTITY-CHANGED: %s %s no longer the table this cold start copied: dropped and "+
		"re-created under the same name (or swapped) %s. Going on would leave the target holding the old table's rows and "+
		"receiving the new one's changes, silently. Re-copy: drop this stream's replication slot with `sluice slot drop "+
		"--source-driver %s --source <SOURCE-DSN> %s --yes` if one is left, then re-run your `sluice sync start` command "+ // remedy-partial: the operator's own invocation carries their DSNs and flags
		"with --restart-from-scratch added",
		coldStartShapeChangedMarker, strings.Join(changed, ", "), tableIsAre(len(changed)), when, s.Source.Name(),
		resolveSlotNameForDisplay(s.SlotName))
}

// copyTableIdentities reads the engine id of every table in schema, in
// sorted name order. proof is false when the source's ids do not prove a
// table's identity, and then names and ids are empty.
func (s *Streamer) copyTableIdentities(ctx context.Context, schema *ir.Schema) (names []string, ids map[string]string, proof bool) {
	if !s.identityProof() || schema == nil {
		return nil, nil, false
	}
	names = schemaTableNames(schema)
	return names, s.establishedIDs(ctx, names), true
}

// schemaTableNames is schema's table names, sorted.
func schemaTableNames(schema *ir.Schema) []string {
	var names []string
	for _, t := range schema.Tables {
		if t != nil {
			names = append(names, t.Name)
		}
	}
	sort.Strings(names)
	return names
}

// withCopyTableIDs adds the table_ids aspect to shape from the ids
// [Streamer.captureCopyIdentities] read, when they are proof.
func (s *Streamer) withCopyTableIDs(shape string, schema *ir.Schema) string {
	if !s.coldStartIDs.proof || schema == nil {
		return shape
	}
	names := schemaTableNames(schema)
	vals := make([]string, len(names))
	for i, n := range names {
		vals[i] = s.coldStartIDs.copied[n]
	}
	return shape + ";" + copyShapeTableIDsKey + "=" + strings.Join(vals, ".")
}

// resumeTableIdentityGate is the identity gate of the stopped-cold-start
// resume (see the file comment): (true, nil) when every table is proven the
// one the stopped run copied, which then become the lane's copy-time ids;
// (false, nil) when that cannot be proven, so the run proceeds as if there
// were nothing to resume; (false, refusal) when a table is proven a
// different one.
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
	copied := map[string]string{}
	var unproven []string
	for i, n := range names {
		if rec[i] == "" || ids[n] == "" {
			unproven = append(unproven, n)
			continue
		}
		copied[n] = rec[i]
	}
	if _, changed := verifyCopyIdentities(copied, ids, names); len(changed) > 0 {
		return false, s.copyIdentityChanged(changed, "while the stream was stopped. sluice has changed NOTHING: the "+
			"resume would have SKIPPED the copy")
	}
	if len(unproven) > 0 {
		return notResuming("no table id could be read for " + strings.Join(unproven, ", "))
	}
	s.coldStartIDs = coldStartIdentities{copied: copied, proof: true}
	return true, nil
}

// tableIsAre is the verb for n tables.
func tableIsAre(n int) string {
	if n == 1 {
		return "is"
	}
	return "are"
}

// displayKey renders a record key for a message: "ns.table" for a
// NUL-namespaced key, the bare name otherwise.
func displayKey(key string) string {
	return scopeTable{namespace: keyNamespace(key), name: bareTableOfKey(key)}.display()
}

// nonNil is m, or an empty map for nil.
func nonNil(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}
