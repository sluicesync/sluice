// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

// Phase 3 helpers for the logical-backup chain feature: schema
// fingerprinting and manifest identity.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// ComputeSchemaHash returns a deterministic hex-encoded SHA-256 over
// a canonical JSON serialisation of s. Used to populate
// [Manifest.SchemaHash] on incremental and full manifests so a chain-
// restore walk can sanity-check that the schema lineage hasn't been
// tampered with.
//
// nil schemas hash to a stable sentinel ("schema:nil") rather than an
// empty string so a nil-vs-empty-Schema distinction stays visible in
// the manifest. The marshaller is the standard encoding/json (with
// the IR's MarshalJSON hooks for sealed interfaces) over a CANONICAL
// view of the schema: per-table collections whose order is not
// semantic (indexes, foreign keys, check/exclude constraints,
// policies) are sorted by name first, so two semantically-identical
// schemas hash identically even when their collections were gathered
// in different orders (task #41 — catalog reads historically drained
// these through randomized map iteration). Table order and column
// order ARE semantic (DDL position) and hash as-is.
//
// The canonical view also normalizes a nil Column.Default to
// [ir.DefaultNone] (task #49): the two are operationally equivalent
// ("no default"), but [ir.Column.UnmarshalJSON] materializes an
// explicit DefaultNone for an absent wire field, so without the
// normalization a reader-fresh schema and the SAME schema re-read
// from a manifest would fingerprint differently. The hash is thereby
// stable across manifest JSON round-trips.
func ComputeSchemaHash(s *ir.Schema) (string, error) {
	if s == nil {
		h := sha256.Sum256([]byte("schema:nil"))
		return hex.EncodeToString(h[:]), nil
	}
	b, err := json.Marshal(canonicalSchemaForHash(s))
	if err != nil {
		return "", fmt.Errorf("backup: compute schema hash: marshal: %w", err)
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}

// canonicalSchemaForHash returns a shallow copy of s whose per-table
// non-semantic collections are name-sorted copies and whose columns
// carry the round-trip-stable default normalization. The input is
// never mutated — manifests must record schemas exactly as the reader
// produced them; only the FINGERPRINT is canonical.
func canonicalSchemaForHash(s *ir.Schema) *ir.Schema {
	out := *s
	out.Tables = make([]*ir.Table, len(s.Tables))
	for i, t := range s.Tables {
		if t == nil {
			continue
		}
		ct := *t
		ct.Columns = canonicalColumnsForHash(t.Columns)
		ct.Indexes = sortedByName(t.Indexes, func(x *ir.Index) string { return x.Name })
		ct.ForeignKeys = sortedByName(t.ForeignKeys, func(x *ir.ForeignKey) string { return x.Name })
		ct.CheckConstraints = sortedByName(t.CheckConstraints, func(x *ir.CheckConstraint) string { return x.Name })
		ct.ExcludeConstraints = sortedByName(t.ExcludeConstraints, func(x *ir.ExcludeConstraint) string { return x.Name })
		ct.Policies = sortedByName(t.Policies, func(x *ir.Policy) string { return x.Name })
		out.Tables[i] = &ct
	}
	// Standalone sequences (item 51): the fingerprint tracks the
	// sequence's SHAPE (name, type, options, ownership), never its
	// POSITION — LastValue / is_called advance with ordinary DML, and
	// a schema hash that churned per-insert would make every no-DDL
	// window read as a schema change. Zeroing the position fields here
	// is also what lets the incremental manifest carry end-of-window
	// positions (the chain-tail re-prime input) without disturbing the
	// recorded before-schema hash contract.
	if len(s.Sequences) > 0 {
		out.Sequences = make([]*ir.Sequence, len(s.Sequences))
		for i, q := range s.Sequences {
			if q == nil {
				continue
			}
			cq := *q
			cq.LastValue = 0
			cq.LastValueIsCalled = false
			cq.LastValueValid = false
			out.Sequences[i] = &cq
		}
		out.Sequences = sortedByName(out.Sequences, func(x *ir.Sequence) string { return x.Name })
	}
	return &out
}

// canonicalColumnsForHash returns columns with a nil Default
// normalized to [ir.DefaultNone] — copying only the columns it
// changes, and returning the input slice untouched when nothing needs
// normalizing (the common round-tripped case). Column ORDER is
// semantic and preserved as-is.
func canonicalColumnsForHash(in []*ir.Column) []*ir.Column {
	normalize := false
	for _, c := range in {
		if c != nil && c.Default == nil {
			normalize = true
			break
		}
	}
	if !normalize {
		return in
	}
	out := make([]*ir.Column, len(in))
	for i, c := range in {
		if c == nil || c.Default != nil {
			out[i] = c
			continue
		}
		cc := *c
		cc.Default = ir.DefaultNone{}
		out[i] = &cc
	}
	return out
}

// sortedByName returns a name-sorted copy of in (nil stays nil; the
// input slice is not mutated). Nil elements sort first, keeping the
// function total over whatever shape a decoded manifest carries.
func sortedByName[T any](in []*T, name func(*T) string) []*T {
	if len(in) == 0 {
		return in
	}
	out := make([]*T, len(in))
	copy(out, in)
	sort.SliceStable(out, func(i, j int) bool {
		switch {
		case out[i] == nil:
			return out[j] != nil
		case out[j] == nil:
			return false
		default:
			return name(out[i]) < name(out[j])
		}
	})
	return out
}

// ComputeBackupID derives a deterministic identifier for a manifest
// from its content. Used to populate [Manifest.BackupID] and to link
// incrementals to their parent via [Manifest.ParentBackupID].
//
// The identity carries CreatedAt, SourceEngine, Kind, and (for
// incrementals) the EndPosition's engine + token. Two manifests
// produced from the same source with the same content produce the
// same ID; two manifests with different windows differ. The hex
// digest is truncated to 16 hex chars (8 bytes / 64 bits) — enough
// to make collision negligible across an operator's chain count
// while keeping log lines readable.
//
// Pre-Phase-3 manifests carry an empty BackupID; the chain-restore
// walk treats those as orphan fulls, which is the right degraded
// behaviour for v0.16.x backups nobody can chain anyway.
func ComputeBackupID(m *Manifest) string {
	if m == nil {
		return ""
	}
	// Order is part of the contract; do NOT reorder these fields.
	parts := []string{
		"v=1",
		"created_at=" + m.CreatedAt.UTC().Format(time.RFC3339Nano),
		"source_engine=" + m.SourceEngine,
		"kind=" + canonicalKind(m.Kind),
		"end_position_engine=" + m.EndPosition.Engine,
		"end_position_token=" + m.EndPosition.Token,
	}
	h := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	return hex.EncodeToString(h[:8])
}

// canonicalKind normalises empty Kind to BackupKindFull so a v0.16.x
// manifest that gets its BackupID computed under v0.17.0 produces a
// stable value. Defensive — the manifest writer fills Kind in
// explicitly on every write — but this keeps the helper safe to call
// against in-memory manifests built by tests or older code paths.
func canonicalKind(k string) string {
	if k == "" {
		return BackupKindFull
	}
	return k
}
