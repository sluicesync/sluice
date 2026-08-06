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
//
// Finally, the canonical view DROPS the fields listed under
// [fingerprintIndex] — read that comment before adding to the set. It
// is the mechanism that keeps a re-emission hint (does this index's
// name come from a real constraint?) from partitioning every backup
// chain by release.
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
		ct.PrimaryKey = fingerprintIndex(t.PrimaryKey)
		ct.Indexes = sortedByName(fingerprintIndexes(t.Indexes), func(x *ir.Index) string { return x.Name })
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
// It also zeroes [ir.Column.OnUpdateCurrentTimestamp], which is
// fingerprint-EXCLUDED for the same compatibility reason
// [ir.Index.ConstraintNamed] is (see the block comment below). That field
// landed after several backup-format epochs already existed, and it is TRUE
// on the most idiomatic column in the MySQL world — the
// `updated_at TIMESTAMP ... ON UPDATE CURRENT_TIMESTAMP` that appears in a
// large share of real schemas. Including it would therefore have moved the
// fingerprint of nearly every MySQL chain and minted a FIFTH epoch, breaking
// restore of existing chains on exactly the schemas most likely to have them
// (roadmap items 102/104). Excluding it costs a narrow, stated thing: chain
// drift detection does not notice this one attribute changing between the
// backup and the restore target.
// Finally it strips [ir.Macaddr.Width] via [fingerprintType], for the same
// reason and with the same cost — see that function.
func canonicalColumnsForHash(in []*ir.Column) []*ir.Column {
	normalize := false
	for _, c := range in {
		if c != nil && columnNeedsFingerprintNormalizing(c) {
			normalize = true
			break
		}
	}
	if !normalize {
		return in
	}
	out := make([]*ir.Column, len(in))
	for i, c := range in {
		if c == nil || !columnNeedsFingerprintNormalizing(c) {
			out[i] = c
			continue
		}
		cc := *c
		if cc.Default == nil {
			cc.Default = ir.DefaultNone{}
		}
		cc.OnUpdateCurrentTimestamp = false
		if t, changed := fingerprintType(cc.Type); changed {
			cc.Type = t
		}
		out[i] = &cc
	}
	return out
}

// columnNeedsFingerprintNormalizing is the early-out predicate for
// [canonicalColumnsForHash]. It is deliberately ONE function used by both the
// scan and the rewrite: when those two drifted apart the scan short-circuited
// on a column the rewrite would have changed, and the exclusion silently did
// not apply. Extend THIS when a new field or type joins the excluded set.
func columnNeedsFingerprintNormalizing(c *ir.Column) bool {
	if c.Default == nil || c.OnUpdateCurrentTimestamp {
		return true
	}
	_, changed := fingerprintType(c.Type)
	return changed
}

// fingerprintType strips the fingerprint-EXCLUDED fields from an IR type,
// returning the rewritten type and whether anything changed. It recurses
// through the two types that nest another one — [ir.Array.Element] and
// [ir.Domain.BaseType] — because `macaddr8[]` is a real Postgres column and
// an exclusion that only reached the scalar would have moved the fingerprint
// of every chain carrying the array form.
//
// THE SET IS FOUR FIELDS: [ir.Macaddr.Width] (roadmap item 117 / Bug 225),
// [ir.Enum.TypeName] (item 121), and [ir.Inet.Family] / [ir.Cidr.Family]
// (item 133). All are governed by the same reason as the exclusion block
// below — the Postgres reader sets each for an ORDINARY column of that type,
// so `omitempty` (which drops only the zero value) would not hold any of them
// back: the key would be present in the type envelope of every real Postgres
// chain carrying a MAC, enum or network column, the fingerprint would move,
// and a new epoch would be minted that makes those chains unrestorable.
// Excluding them restores the pre-change bytes EXACTLY.
//
// Enum.TypeName is the stronger case of the four. A MAC column is uncommon; an
// enum column is not, and the PG reader populates the type name for EVERY one
// of them, so folding it in would repartition close to every Postgres chain
// in existence. Inet/Cidr.Family sits between the two on frequency and is
// exactly as unconditional: the PG reader sets [ir.InetFamilyAny] on every
// inet and cidr column it reads, so the key is present whenever such a column
// exists at all.
//
// WHAT THE EXCLUSIONS COST, stated exactly. The recorded schema still carries
// all of them (they ride the wire — see [ir.MarshalType]), so a restore
// rebuilds `macaddr8` at the right width and re-creates an enum under its real
// name; what is lost is the fingerprint's ability to NOTICE a `macaddr` ⇄
// `macaddr8` retype, an enum TYPE RENAME, or an `inet4` ⇄ `inet6` retype
// between a backup and its restore target, and — since
// [canonicalManifestBytes] folds the fingerprint STRING rather than the schema
// bytes — signing does not backstop a tampered width, type name or family
// either. The schema DIFF still sees the MAC case ([ir.Macaddr.String] renders
// the two widths differently, which is what both diff paths compare); it does
// NOT see an enum type rename, because [ir.Enum.String] renders only the
// VALUES, and it does NOT see a family retype, because [ir.Inet.String]
// deliberately renders every family the same (that doc says why: the family
// changes no target DDL). That is a real, named residual, not an oversight:
// each of the three changes no column type on the target, and the alternative
// — folding them in — costs every existing chain.
//
// The input is never mutated: the manifest records the schema exactly as the
// reader produced it, and only the FINGERPRINT is canonical.
func fingerprintType(t ir.Type) (ir.Type, bool) {
	switch v := t.(type) {
	case ir.Macaddr:
		if v.Width == 0 {
			return t, false
		}
		return ir.Macaddr{}, true
	case ir.Enum:
		// Enum.TypeName (roadmap item 121) — same treatment, same reason,
		// and a stronger case than Macaddr.Width: the Postgres reader sets
		// it for EVERY enum column, so it is non-zero on ordinary source
		// data and folding it in would move the fingerprint of every chain
		// carrying an enum. It rides the wire so restore re-creates the
		// type under its real name; it is excluded here so no chain is
		// repartitioned.
		if v.TypeName == "" {
			return t, false
		}
		out := v
		out.TypeName = ""
		return out, true
	case ir.Inet:
		// Inet.Family / Cidr.Family (roadmap item 133) — same treatment,
		// same reason: the Postgres reader sets InetFamilyAny on every
		// inet/cidr column, so the key is non-zero on ordinary source data
		// and folding it in would move the fingerprint of every chain
		// carrying one. It rides the wire so a manifest records which
		// MariaDB network type the column was.
		if v.Family == 0 {
			return t, false
		}
		return ir.Inet{}, true
	case ir.Cidr:
		if v.Family == 0 {
			return t, false
		}
		return ir.Cidr{}, true
	case ir.Array:
		elem, changed := fingerprintType(v.Element)
		if !changed {
			return t, false
		}
		return ir.Array{Element: elem}, true
	case ir.Domain:
		base, changed := fingerprintType(v.BaseType)
		if !changed {
			return t, false
		}
		out := v
		out.BaseType = base
		return out, true
	default:
		return t, false
	}
}

// FINGERPRINT-EXCLUDED FIELDS (roadmap item 104 / Bug 216).
//
// Some fields on [ir.Index] describe how a name should be RE-EMITTED,
// not what the schema IS, and those are deliberately kept out of the
// fingerprint. Today the set is exactly one field —
// [ir.Index.ConstraintNamed] — and the reason is compatibility, not
// taste.
//
// The fingerprint is a SHA-256 over the schema's default struct JSON,
// so a field added to Index moves the fingerprint of every schema that
// has an index, and a chain restore REFUSES a chain whose recorded
// fingerprint does not reproduce. `omitempty` was supposed to contain
// that: a false field marshals away, older schemas all carry false, so
// the bytes stay put. It does not hold for THIS field, because the
// Postgres reader sets it TRUE for every constraint-owned index and
// Postgres auto-names every primary key — so on any real Postgres
// source the field is present, the fingerprint moves, and v0.104.3
// minted a fourth epoch that the omitempty was written to prevent.
// Relying on a zero value only works when the source rarely produces
// the interesting case; here it always does.
//
// Excluding the field entirely restores the fingerprint of a v0.103.0
// – v0.104.2 schema BYTE-IDENTICALLY (the field is the only addition
// to any hashed struct across that span), which is why this is an
// exclusion rather than a fifth epoch. Chains written BY v0.104.3 from
// a Postgres source with a primary key remain their own orphan epoch —
// their manifests record a fingerprint computed WITH the field, and
// nothing here re-derives it.
//
// WHAT THE EXCLUSION COSTS, stated exactly rather than waved at. The
// check verifies a manifest against its OWN recorded schema, so what
// is lost is detection of an edit to `ConstraintNamed` alone — and
// signing does NOT backstop it, because [canonicalManifestBytes] folds
// the fingerprint STRING (`schema_hash`) and the same
// [deltaTableFingerprint] for schema deltas, not the schema bytes. The
// one place still covering it verbatim is a SchemaHistory entry, whose
// TableJSON is folded raw. So a coherent tamper could flip the flag on
// a manifest's recorded schema undetected; its entire effect on a
// restore is whether a primary key is re-emitted as
// `CONSTRAINT <name> PRIMARY KEY` or a bare `PRIMARY KEY` — a DDL
// naming difference, no data, no row count, no position. That is the
// trade: one bit of tamper-detection on a name, against partitioning
// every chain in the field by release.
//
// Before adding a field here, read the block on `goldenSchemaHash`
// (schema_hash_golden_test.go): exclusion is the third answer to a
// moved fingerprint, and the only one that can move it BACK.
func fingerprintIndex(in *ir.Index) *ir.Index {
	if in == nil {
		return nil
	}
	out := *in
	out.ConstraintNamed = false
	return &out
}

// fingerprintIndexes maps [fingerprintIndex] over a slice, copying it
// rather than mutating the caller's indexes (the input schema is the
// one the manifest records verbatim; only the FINGERPRINT is canonical).
func fingerprintIndexes(in []*ir.Index) []*ir.Index {
	if in == nil {
		return nil
	}
	out := make([]*ir.Index, len(in))
	for i, idx := range in {
		out[i] = fingerprintIndex(idx)
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
	// FormatVersion 8+ (item 57 fold) folds CDCPositionCommitsAfterRows into
	// the identity: that flag flips restore's replay semantics (whether the
	// recorded EndPosition commits AFTER the window's rows), so on an unsigned
	// CDC manifest it must invalidate the id if edited, exactly as an
	// EndPosition edit does. APPENDED (never reordered into the base fields)
	// and gated on the manifest's OWN recorded version so every pre-8
	// manifest — whose recorded BackupID was computed before this field
	// existed — keeps its legacy id and still recompute-verifies clean, and a
	// mixed-version chain stays coherent (see [StampCDCPositionBinding]).
	if m.FormatVersion >= FormatVersionCDCPositionBinding {
		v := "false"
		if m.CDCPositionCommitsAfterRows {
			v = "true"
		}
		parts = append(parts, "cdc_position_commits_after_rows="+v)
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
