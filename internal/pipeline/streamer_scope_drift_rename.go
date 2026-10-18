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
// name, the same id) and nothing refuses — provided the target holds the new
// name; otherwise the refusal says the target was not renamed. A drop and
// re-create under a new name has a new id, and still refuses.
//
// Where no id proves it (a VStream/PlanetScale source, a name InnoDB stores
// in its filename encoding, a record written without ids), the refusal names
// the acknowledgement: `--accept-table-rename <old>=<new>`, a human's
// statement that <new> is <old> renamed, which the record then follows the
// same way.

import (
	"context"
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
// entry to the old one it is; targetNotRenamed maps a new entry to the old
// table's display name where the target lacks the new name; gone lists the
// established tables missing from the source for the refusal's hint.
func (s *Streamer) detectRenames(ctx context.Context, streamID string, scope streamScope, rec ir.EstablishedRecord, established, dropped map[string]bool) (followed, targetNotRenamed map[string]string, gone []string, err error) {
	followed, targetNotRenamed = map[string]string{}, map[string]string{}
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

	ids := s.readTableIdentities(ctx, scope, unestablished, len(rec.IDs) > 0)
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

// establishedIDs reads the engine-stable id of each established entry a
// cold-start lane copied, for the record. Best-effort: a source without ids,
// or a failed read, records the entries without them (a later rename then
// needs --accept-table-rename).
func (s *Streamer) establishedIDs(ctx context.Context, keys []string) map[string]string {
	reader, ok := s.Source.(ir.TableIdentityReader)
	if !ok || len(keys) == 0 {
		return nil
	}
	pairs, err := s.uniqueKeyDoorScopes(ctx)
	if err != nil {
		slog.WarnContext(ctx, "cold start: could not resolve the source namespaces to read table ids", slog.String("error", err.Error()))
		return nil
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
	for ns, names := range byNamespace {
		dsn, ok := dsnOf[ns]
		if !ok {
			continue
		}
		ids, err := reader.TableIdentities(ctx, dsn, names)
		if err != nil {
			slog.WarnContext(ctx, "cold start: could not read the source's table ids; the record is written without them",
				slog.String("error", err.Error()))
			continue
		}
		for name, id := range ids {
			out[establishedKey(ns, name)] = id
		}
	}
	return out
}

// tableIdentity is the added table's engine-stable id, "" when the source
// exposes none.
func (a *AddTable) tableIdentity(ctx context.Context) string {
	reader, ok := a.Source.(ir.TableIdentityReader)
	if !ok {
		return ""
	}
	ids, err := reader.TableIdentities(ctx, a.SourceDSN, []string{a.TableName})
	if err != nil {
		slog.WarnContext(ctx, "add-table: could not read the table's id; a later rename of it needs --accept-table-rename",
			slog.String("table", a.TableName), slog.String("error", err.Error()))
		return ""
	}
	return ids[a.TableName]
}

// checkRenamedOnTarget sorts each matched rename by whether the target
// holds the new name.
func (s *Streamer) checkRenamedOnTarget(ctx context.Context, scope streamScope, tables []scopeTable, match, followed, targetNotRenamed map[string]string) error {
	byNamespace := map[string]map[string]bool{}
	for _, t := range tables {
		if match[t.key()] == "" {
			continue
		}
		if byNamespace[t.namespace] == nil {
			byNamespace[t.namespace] = map[string]bool{}
		}
		byNamespace[t.namespace][strings.ToLower(t.name)] = true
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
			if matchTargetTable(tgt, t.name) != nil {
				followed[t.key()] = old
			} else {
				targetNotRenamed[t.key()] = strings.ReplaceAll(old, "\x00", ".")
			}
		}
	}
	return nil
}
