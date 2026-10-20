// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Following a RENAME TABLE made while the stream was stopped (Bug 321 R6).
//
// The documented drained-model recovery for a rename — rename the table on
// the target too, then restart (docs/schema-change-runbook.md) — met the
// INCLUDE-TABLE-SCOPE-DRIFT door as "t8b, never copied by this stream": the
// record held t8, the source held t8b. A rename keeps the table's identity
// on the engine (Postgres pg_class.oid, InnoDB TABLE_ID), so the record keeps
// that id beside each established name and the door matches it: an
// established entry whose name is gone from the source, whose id now carries
// a new in-scope name, is that table renamed. The record follows it (the new
// name, the same id) and nothing refuses — provided the target was renamed
// too: it holds the new name and no longer the old one (R6-S2); otherwise the
// refusal says the target was not renamed. A drop and re-create under a new
// name has a new id, and still refuses.
//
// Where no id proves it (a VStream/PlanetScale source, a name InnoDB stores
// in its filename encoding, a record written without ids), the refusal names
// the acknowledgement: `--accept-table-rename <old>=<new>`, a human's
// statement that <new> is <old> renamed, which the record then follows the
// same way.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// keyNamespace is the namespace of an established-record entry ("" on a
// single-namespace stream).
func keyNamespace(key string) string {
	if i := strings.LastIndexByte(key, 0); i >= 0 {
		return key[:i]
	}
	return ""
}

// parseAcceptTableRenames parses --accept-table-rename values (`old=new`,
// bare table names) into new → old.
func parseAcceptTableRenames(values []string) (map[string]string, error) {
	out := map[string]string{}
	for _, v := range values {
		old, renamed, ok := strings.Cut(v, "=")
		if !ok || old == "" || renamed == "" || old == renamed {
			return nil, fmt.Errorf("pipeline: --accept-table-rename %q: want <old-table>=<new-table>", v)
		}
		out[renamed] = old
	}
	return out, nil
}

// detectRenames finds the in-scope tables the record does not hold that are
// established tables renamed (see the file comment). followed maps a new
// entry to the old one it is; targetNotRenamed maps a new entry to what the
// target holds instead where it was not renamed too; gone lists the
// established tables missing from the source for the refusal's hint.
//
// ids holds the current engine id of each in-scope table, by key
// ([Streamer.readTableIdentities]).
func (s *Streamer) detectRenames(ctx context.Context, streamID string, scope streamScope, rec ir.EstablishedRecord, established, dropped map[string]bool, ids map[string]string) (followed map[string]string, targetNotRenamed map[string]renameMiss, gone []string, err error) {
	followed, targetNotRenamed = map[string]string{}, map[string]renameMiss{}
	acks, err := parseAcceptTableRenames(s.AcceptTableRenames)
	if err != nil {
		return nil, nil, nil, err
	}
	inScope := map[string]bool{}
	for _, t := range scope.tables {
		inScope[t.key()] = true
	}
	goneKeys := map[string]bool{}
	for _, k := range rec.Established {
		if !inScope[k] {
			goneKeys[k] = true
			gone = append(gone, strings.ReplaceAll(k, "\x00", "."))
		}
	}
	sort.Strings(gone)
	var unestablished []scopeTable
	for _, t := range scope.tables {
		if !established[t.key()] && !dropped[t.key()] {
			unestablished = append(unestablished, t)
		}
	}
	usedAcks := map[string]bool{}
	defer func() {
		for renamed, old := range acks {
			if !usedAcks[renamed] {
				slog.WarnContext(ctx, "--accept-table-rename matches no rename on this stream (already followed, or the "+
					"names do not match the record); it changes nothing", slog.String("stream_id", streamID),
					slog.String("rename", old+"="+renamed))
			}
		}
	}()
	if len(goneKeys) == 0 || len(unestablished) == 0 {
		return followed, targetNotRenamed, nil, nil
	}

	byID := map[string]string{} // namespace\x00id → old entry
	for k := range goneKeys {
		if id := rec.IDs[k]; id != "" {
			byID[keyNamespace(k)+"\x00"+id] = k
		}
	}
	claimed := map[string]bool{}
	match := map[string]string{} // new entry → old entry
	for _, t := range unestablished {
		var old string
		if ackOld, ok := acks[t.name]; ok {
			candidate := establishedKey(t.namespace, ackOld)
			if goneKeys[candidate] {
				old, usedAcks[t.name] = candidate, true
			}
		}
		if old == "" {
			if id := ids[t.key()]; id != "" {
				old = byID[t.namespace+"\x00"+id]
			}
		}
		if old != "" && !claimed[old] {
			claimed[old] = true
			match[t.key()] = old
		}
	}
	if len(match) == 0 {
		return followed, targetNotRenamed, gone, nil
	}
	if err := s.checkRenamedOnTarget(ctx, scope, unestablished, match, followed, targetNotRenamed); err != nil {
		return nil, nil, nil, err
	}
	return followed, targetNotRenamed, gone, nil
}

// readTableIdentities reads the engine-stable id of each table, per
// namespace. A source without ids, a record without any, or a failed read
// yields none — the door then refuses and names the acknowledgement.
func (s *Streamer) readTableIdentities(ctx context.Context, scope streamScope, tables []scopeTable, recordHasIDs bool) map[string]string {
	out := map[string]string{}
	reader, ok := s.Source.(ir.TableIdentityReader)
	if !ok || !recordHasIDs {
		return out
	}
	byNamespace := map[string][]string{}
	for _, t := range tables {
		byNamespace[t.namespace] = append(byNamespace[t.namespace], t.name)
	}
	for ns, names := range byNamespace {
		ids, err := reader.TableIdentities(ctx, scope.pairs[ns].sourceDSN, names)
		if err != nil {
			slog.WarnContext(ctx, "could not read the source's table ids; a table renamed while the stream was stopped needs "+
				"--accept-table-rename", slog.String("error", err.Error()))
			continue
		}
		for name, id := range ids {
			out[establishedKey(ns, name)] = id
		}
	}
	return out
}

// TableIdentityChangedMarker names an established table whose engine id no
// longer matches the stream's record (R6-S3).
const TableIdentityChangedMarker = "TABLE-IDENTITY-CHANGED"

// TableIdentityUnreadMarker names a table whose id could not be re-read
// where a copy's id was to be verified: a failed read proves nothing, so
// the verification refuses rather than passing it
// (ANCHOR-ID-REREAD-FAILURE-PASSES).
const TableIdentityUnreadMarker = "TABLE-IDENTITY-UNREAD"

// judgeTableIdentities compares each established, in-scope table's recorded
// engine id with the source's id for the same name (R6-S3). The rename
// match ([Streamer.detectRenames]) only looks at names gone from the source,
// so a swap made while the stream was stopped (`t8 → tmp, t9 → t8, tmp →
// t9`) left every name present and passed unnoticed; a FOR TABLE
// publication tracks its members by oid, so N2 did not fire either.
//
// A changed id is PROOF of a different table when the recorded id now
// belongs to another in-scope name (the recorded table moved away), or the
// current id is one the record holds for another entry (this name now holds
// another established table). On an engine whose id survives a rebuild
// ([ir.TableIdentityReader.TableIdentitySurvivesRebuild]: Postgres) any
// change is proof too — a drop and re-create under the same name, or a swap
// with a table the stream does not have. Each proof is refused
// (changed: key → why).
//
// On an engine whose rebuilds assign new ids (InnoDB: OPTIMIZE TABLE, a
// copying ALTER, TRUNCATE) a bare change is routine maintenance as often as
// a different table, and refusing it would break the former; the record
// takes the new id instead (update: key → id) and the narrowing logs it.
// What that leaves open on MySQL — a drop and re-create under the same name
// while the stream was stopped — is TABLE-RECREATED-WHILE-STOPPED's
// residual (docs/dev/audit-backlog.md).
func (s *Streamer) judgeTableIdentities(ctx context.Context, streamID string, scope streamScope, rec ir.EstablishedRecord, established map[string]bool, ids map[string]string) (changed, update map[string]string) {
	reader, ok := s.Source.(ir.TableIdentityReader)
	if !ok || len(rec.IDs) == 0 || len(ids) == 0 {
		return nil, nil
	}
	recordedOwner := map[string]string{} // namespace\x00id → established entry
	for _, k := range rec.Established {
		if id := rec.IDs[k]; id != "" {
			recordedOwner[keyNamespace(k)+"\x00"+id] = k
		}
	}
	currentOwner := map[string]string{} // namespace\x00id → in-scope entry
	for _, t := range scope.tables {
		if id := ids[t.key()]; id != "" {
			currentOwner[t.namespace+"\x00"+id] = t.key()
		}
	}
	shown := func(k string) string { return strings.ReplaceAll(k, "\x00", ".") }
	changed, update = map[string]string{}, map[string]string{}
	for _, t := range scope.tables {
		k := t.key()
		recorded, current := rec.IDs[k], ids[k]
		if !established[k] || recorded == "" || current == "" || current == recorded {
			continue
		}
		var why []string
		if other := recordedOwner[t.namespace+"\x00"+current]; other != "" && other != k {
			why = append(why, fmt.Sprintf("it now holds the table this stream recorded as %s", shown(other)))
		}
		if other := currentOwner[t.namespace+"\x00"+recorded]; other != "" && other != k {
			why = append(why, fmt.Sprintf("the table this stream recorded under this name is now %s", shown(other)))
		}
		switch {
		case len(why) > 0:
			changed[k] = "the source swapped it with another table while the stream was stopped: " + strings.Join(why, ", and ")
		case reader.TableIdentitySurvivesRebuild():
			changed[k] = fmt.Sprintf("the source table under this name is a different one (engine id %s, recorded %s): it was "+
				"dropped and re-created, or swapped with a table this stream does not have, while the stream was stopped",
				current, recorded)
		default:
			update[k] = current
		}
	}
	if len(update) > 0 {
		slog.DebugContext(ctx, "table ids changed without proof of a different table; the record takes the new ids",
			slog.String("stream_id", streamID), slog.Int("tables", len(update)))
	}
	return changed, update
}

// establishedIDs reads the engine-stable id of each established entry a
// cold-start lane copied, for the record. Best-effort: a source without ids,
// or a failed read, records the entries without them (a later rename then
// needs --accept-table-rename).
func (s *Streamer) establishedIDs(ctx context.Context, keys []string) map[string]string {
	out, err := s.readEstablishedIDs(ctx, keys)
	if err != nil {
		slog.WarnContext(ctx, "cold start: could not read the source's table ids; the record is written without them",
			slog.String("error", err.Error()))
	}
	return out
}

// readEstablishedIDs is [Streamer.establishedIDs] with its failures
// reported: out holds every id that was read, and err every namespace whose
// read failed, so "the read failed" is never the same answer as "the table
// is gone" (a table the source does not hold is simply absent from out).
// A caller that verifies ids against the copy's must refuse on err
// (ANCHOR-ID-REREAD-FAILURE-PASSES).
func (s *Streamer) readEstablishedIDs(ctx context.Context, keys []string) (map[string]string, error) {
	reader, ok := s.Source.(ir.TableIdentityReader)
	if !ok || len(keys) == 0 {
		return nil, nil
	}
	pairs, err := s.uniqueKeyDoorScopes(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve the source namespaces: %w", err)
	}
	dsnOf := map[string]string{}
	for _, p := range pairs {
		dsnOf[p.namespace] = p.sourceDSN
	}
	byNamespace := map[string][]string{}
	for _, k := range keys {
		ns := keyNamespace(k)
		byNamespace[ns] = append(byNamespace[ns], bareTableOfKey(k))
	}
	out := map[string]string{}
	var errs []error
	for ns, names := range byNamespace {
		dsn, ok := dsnOf[ns]
		if !ok {
			errs = append(errs, fmt.Errorf("namespace %q is not among the stream's source namespaces", ns))
			continue
		}
		ids, err := reader.TableIdentities(ctx, dsn, names)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for name, id := range ids {
			out[establishedKey(ns, name)] = id
		}
	}
	return out, errors.Join(errs...)
}

// tableIdentity is the added table's engine-stable id, "" when the source
// exposes none.
func (a *AddTable) tableIdentity(ctx context.Context) string {
	id, err := a.readTableIdentity(ctx)
	if err != nil {
		slog.WarnContext(ctx, "add-table: could not read the table's id; a later rename of it needs --accept-table-rename",
			slog.String("table", a.TableName), slog.String("error", err.Error()))
	}
	return id
}

// readTableIdentity is [AddTable.tableIdentity] with its failure reported.
func (a *AddTable) readTableIdentity(ctx context.Context) (string, error) {
	reader, ok := a.Source.(ir.TableIdentityReader)
	if !ok {
		return "", nil
	}
	ids, err := reader.TableIdentities(ctx, a.SourceDSN, []string{a.TableName})
	if err != nil {
		return "", err
	}
	return ids[a.TableName], nil
}

// verifiedTableIdentity is the id the add records for its table: on a
// source whose ids are proof, the id its copy started with, re-read now, and
// refused if it moved — the table was re-created between its copy and this
// record, so the target holds the old table's rows
// (coldstart_copy_identity.go). On any other source a fresh read, as
// before.
func (a *AddTable) verifiedTableIdentity(ctx context.Context) (string, error) {
	reader, ok := a.Source.(ir.TableIdentityReader)
	if !ok || !reader.TableIdentitySurvivesRebuild() {
		return a.tableIdentity(ctx), nil
	}
	now, err := a.readTableIdentity(ctx)
	if err != nil && a.copyTimeID != "" {
		// A failed re-read proves nothing either way, and recording the
		// copy-time id on it would certify a table nothing checked
		// (ANCHOR-ID-REREAD-FAILURE-PASSES).
		return "", fmt.Errorf("pipeline: add-table: %s: %q: could not re-read the table's id to prove it is the table this "+
			"add copied, so the add is not recorded complete: %w. Once the source can be read, empty the target table "+
			"and run `sluice schema add-table` for it again", // remedy-partial: the operator's own add-table invocation carries their DSNs and flags
			TableIdentityUnreadMarker, a.TableName, err)
	}
	if a.copyTimeID != "" && now != "" && now != a.copyTimeID {
		return "", fmt.Errorf("pipeline: add-table: %s: %q is no longer the table this add copied (copied as id %s, now id %s): "+
			"dropped and re-created under the same name (or swapped) during the add, so the target holds the old table's "+
			"rows. The add is not recorded complete. Empty the target table and run `sluice schema add-table` for it again", // remedy-partial: the operator's own add-table invocation carries their DSNs and flags
			TableIdentityChangedMarker, a.TableName, a.copyTimeID, now)
	}
	return a.copyTimeID, nil
}

// checkRenamedOnTarget sorts each matched rename by whether the target was
// renamed too: it holds the new name AND no longer holds the old one — what
// the runbook's rename of both sides leaves. A target holding the new name
// beside the old one is not that (R6-S2): its new-named table is some other
// table (a stale leftover, an unrelated one), the stream's real copy is
// still under the old name, and following the rename would apply onto the
// wrong table and strand the real copy. The acknowledgement path is held to
// the same rule; what it cannot check is what the new-named target table
// holds, which is trusted as-is (the flag's help says so).
func (s *Streamer) checkRenamedOnTarget(ctx context.Context, scope streamScope, tables []scopeTable, match, followed map[string]string, targetNotRenamed map[string]renameMiss) error {
	byNamespace := map[string]map[string]bool{}
	for _, t := range tables {
		old := match[t.key()]
		if old == "" {
			continue
		}
		if byNamespace[t.namespace] == nil {
			byNamespace[t.namespace] = map[string]bool{}
		}
		byNamespace[t.namespace][strings.ToLower(t.name)] = true
		byNamespace[t.namespace][strings.ToLower(bareTableOfKey(old))] = true
	}
	for ns, names := range byNamespace {
		tgt, err := s.readUniqueKeyDoorTarget(ctx, scope.pairs[ns], names)
		if err != nil && (ns == "" || !ir.IsDatabaseNotFound(err)) {
			return fmt.Errorf("pipeline: include-table scope door: read target tables: %w", err)
		}
		for _, t := range tables {
			old := match[t.key()]
			if old == "" || t.namespace != ns {
				continue
			}
			hasNew := matchTargetTable(tgt, t.name) != nil
			hasOld := matchTargetTable(tgt, bareTableOfKey(old)) != nil
			if hasNew && !hasOld {
				followed[t.key()] = old
				continue
			}
			targetNotRenamed[t.key()] = renameMiss{
				old: strings.ReplaceAll(old, "\x00", "."), oldTable: bareTableOfKey(old),
				hasNew: hasNew, hasOld: hasOld,
			}
		}
	}
	return nil
}

// renameMiss is a source rename the target does not mirror: what the target
// holds under each name.
type renameMiss struct {
	old      string // the established table's display name
	oldTable string // and its bare table name

	hasNew, hasOld bool
}

// targetState says what the target holds instead of the renamed table.
func (m renameMiss) targetState(newName string) string {
	old := m.oldTable
	switch {
	case m.hasNew && m.hasOld:
		return fmt.Sprintf("the target holds both %s and %s, so its %s is not this stream's copy (that is still %s) "+
			"and following the rename would apply onto the wrong table", old, newName, newName, old)
	case m.hasOld:
		return fmt.Sprintf("the target still holds %s and has no table named %s", old, newName)
	default:
		return "the target has no table named " + newName
	}
}
