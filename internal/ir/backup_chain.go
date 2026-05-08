// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

// Phase 3 helpers for the logical-backup chain feature: schema
// fingerprinting and manifest identity. Lives next to the rest of
// the manifest types in [internal/ir/backup.go]; split into its own
// file to keep the manifest-types diff in v0.17.0 contained.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
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
// the IR's MarshalJSON hooks for sealed interfaces), so the same
// schema always produces the same bytes — operators inspecting two
// chain manifests with matching SchemaHash can rely on the schemas
// being byte-equal under the IR's wire shape.
func ComputeSchemaHash(s *Schema) (string, error) {
	if s == nil {
		h := sha256.Sum256([]byte("schema:nil"))
		return hex.EncodeToString(h[:]), nil
	}
	b, err := json.Marshal(s)
	if err != nil {
		return "", fmt.Errorf("ir: compute schema hash: marshal: %w", err)
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
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
