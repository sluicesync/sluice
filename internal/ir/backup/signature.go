// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

// Signed-manifest contract (ADR-0154 Phase 1): the canonical
// serialization the detached signature covers, and the on-disk shape of
// the detached signature file itself.
//
// The signature authenticates the WHOLE manifest against whole-manifest
// substitution / rollback (R1) and change-list tail truncation (R2) —
// the residuals ADR-0152's per-chunk AAD binding left open. It rides in
// a DETACHED sibling object (`<manifest>.sig`, and `lineage.json.sig`
// for the chain tip), never inside the manifest it signs, so the signed
// bytes stay byte-stable and an old reader can ignore it.
//
// The canonical bytes are ON-DISK CONTRACT, golden-pinned: changing the
// serialization strands every signature ever written, so a change needs
// a new [ManifestCanonVersion] (and, since the version is itself in the
// signed bytes, a signature that records the new version). Determinism
// is enforced by fixed field order + name-sorting every collection whose
// order is not semantic — no map iteration reaches the wire.

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	// ManifestCanonVersion versions the canonical serialization the
	// signature covers. Bumped only on an incompatible change to
	// [CanonicalManifestBytes]; it is the first line of the canonical
	// payload, so a verifier keyed on an old scheme fails closed.
	ManifestCanonVersion = "sluice-manifest-canon/v1"

	// SignatureSchemeHMACKEK is the Phase 1 scheme tag recorded in the
	// detached signature: HMAC-SHA-256 keyed off a key HKDF-derived from
	// the chain KEK. Phases 2-3 add "ed25519" / "kms".
	SignatureSchemeHMACKEK = "hmac-kek"

	// SignatureFileSuffix is appended to a manifest's path to name its
	// detached signature object (`manifest.json` → `manifest.json.sig`).
	// `.sig` never collides with the `.json` manifest-discovery filters.
	SignatureFileSuffix = ".sig"
)

// ManifestSignature is the on-disk shape of a detached `<manifest>.sig`
// object (indented JSON, operator-inspectable). It records what the
// signature claims — scheme, key id, the freshness anchors (sequence +
// chunk count) — plus the MAC over the canonical bytes. The MAC covers a
// canonical serialization that INCLUDES Sequence and ChunkCount, so the
// claimed anchors here are redundant-but-convenient: the MAC is what
// authenticates them; a verifier recomputes the canonical bytes from the
// on-disk manifest + the expected sequence and checks the MAC.
type ManifestSignature struct {
	// CanonVersion is the [ManifestCanonVersion] the signer used; a
	// verifier keyed on a different scheme refuses rather than guess.
	CanonVersion string `json:"canon_version"`

	// Scheme is the signature scheme ([SignatureSchemeHMACKEK] in Phase
	// 1). Recorded so a verifier knows what to check.
	Scheme string `json:"scheme"`

	// KeyID is a stable, non-secret fingerprint of the signing key
	// ([crypto.ManifestSigKeyID]) so rotation is expressible and a
	// verifier can report which key a signature claims.
	KeyID string `json:"key_id"`

	// Sequence is this manifest's monotonic position in the lineage's
	// flat manifest order (the full is 0). Signed (it is in the canonical
	// bytes); chain-restore checks the sequence is gap-free across links.
	Sequence int `json:"sequence"`

	// ChunkCount is the total number of chunks (row + change) the
	// manifest lists. Signed; a truncated change-list (R2) fails both the
	// count check and the MAC (the canonical bytes carry the full list).
	ChunkCount int `json:"chunk_count"`

	// MAC is the hex-encoded HMAC-SHA-256 over [CanonicalManifestBytes].
	MAC string `json:"mac"`
}

// ManifestChunkCount is the total number of chunk files a manifest lists
// across every table (row chunks) plus its change chunks. The signed
// freshness anchor: a store adversary who truncates the tail of a
// change-list shrinks this below the recorded value.
func ManifestChunkCount(m *Manifest) int {
	if m == nil {
		return 0
	}
	n := len(m.ChangeChunks)
	for _, t := range m.Tables {
		if t != nil {
			n += len(t.Chunks)
		}
	}
	return n
}

// CanonicalManifestBytes returns the deterministic serialization of the
// manifest's security-relevant fields that the ADR-0154 signature
// covers: format version, identity (created_at / source_engine / kind /
// backup_id), the lineage parent pointer, the schema fingerprint, the
// chain-encryption descriptor, the table→row-count mapping, the full
// chunk list (row chunks by path; change chunks by ordinal, because
// change-replay order is semantic — the ADR-0152 rationale), and the
// freshness anchors (sequence + chunk count).
//
// The output is line-oriented UTF-8 text with a fixed field order;
// every collection whose order is not semantic is name-sorted. It is
// ON-DISK CONTRACT (golden-pinned) — see the package doc.
func CanonicalManifestBytes(m *Manifest, seq int) []byte {
	var b strings.Builder
	b.WriteString(ManifestCanonVersion)
	b.WriteByte('\n')
	writeLine(&b, "format_version", strconv.Itoa(m.FormatVersion))
	writeLine(&b, "source_engine", m.SourceEngine)
	writeLine(&b, "created_at", m.CreatedAt.UTC().Format(time.RFC3339Nano))
	writeLine(&b, "kind", canonicalKind(m.Kind))
	writeLine(&b, "backup_id", m.BackupID)
	writeLine(&b, "parent_backup_id", m.ParentBackupID)
	writeLine(&b, "schema_hash", m.SchemaHash)
	writeLine(&b, "sequence", strconv.Itoa(seq))
	writeLine(&b, "chunk_count", strconv.Itoa(ManifestChunkCount(m)))
	writeLine(&b, "chain_encryption", canonicalChainEncryption(m.ChainEncryption))

	// Table → row-count mapping, sorted by (schema, name).
	tbls := make([]*TableManifest, 0, len(m.Tables))
	tbls = append(tbls, m.Tables...)
	sort.SliceStable(tbls, func(i, j int) bool {
		if tbls[i].Schema != tbls[j].Schema {
			return tbls[i].Schema < tbls[j].Schema
		}
		return tbls[i].Name < tbls[j].Name
	})
	for _, t := range tbls {
		if t == nil {
			continue
		}
		writeLine(&b, "table:"+t.Schema+"."+t.Name, strconv.FormatInt(t.RowCount, 10))
	}

	// Row chunks across every table, sorted by file. A table's rows are a
	// SET (chunk order is not semantic; ADR-0149 range workers append out
	// of order), so the path binding suffices — no ordinal.
	rowChunks := make([]*ChunkInfo, 0)
	for _, t := range m.Tables {
		if t == nil {
			continue
		}
		rowChunks = append(rowChunks, t.Chunks...)
	}
	sort.SliceStable(rowChunks, func(i, j int) bool { return rowChunks[i].File < rowChunks[j].File })
	for _, c := range rowChunks {
		if c == nil {
			continue
		}
		writeLine(&b, "rowchunk:"+c.File, c.SHA256+":"+strconv.FormatInt(c.RowCount, 10))
	}

	// Change chunks in list order, ordinal-prefixed: change-replay order
	// IS semantic (ADR-0152), so a reorder must change the canonical bytes.
	for i, c := range m.ChangeChunks {
		if c == nil {
			continue
		}
		writeLine(&b, "changechunk:"+strconv.Itoa(i)+":"+c.File, c.SHA256+":"+strconv.FormatInt(c.RowCount, 10))
	}
	return []byte(b.String())
}

// canonicalChainEncryption renders the chain-encryption descriptor
// deterministically (or "none" for a plaintext chain). The Argon2id KDF
// params are included because a tampered KDF param is a pre-auth bomb
// (ADR-0152 N-7) and belongs under the signature.
func canonicalChainEncryption(enc *ChainEncryption) string {
	if enc == nil {
		return "none"
	}
	parts := []string{
		"algorithm=" + enc.Algorithm,
		"mode=" + enc.Mode,
		"kek_mode=" + enc.KEKMode,
		"kek_ref=" + enc.KEKRef,
		"wrapped_cek=" + hex.EncodeToString(enc.WrappedCEK),
	}
	if enc.Argon2id != nil {
		a := enc.Argon2id
		parts = append(
			parts,
			"argon2id_salt="+hex.EncodeToString(a.Salt),
			"argon2id_memory="+strconv.FormatUint(uint64(a.Memory), 10),
			"argon2id_iterations="+strconv.FormatUint(uint64(a.Iterations), 10),
			"argon2id_parallelism="+strconv.FormatUint(uint64(a.Parallelism), 10),
			"argon2id_keylen="+strconv.FormatUint(uint64(a.KeyLen), 10),
		)
	}
	return strings.Join(parts, "|")
}

// writeLine appends `key=value\n`. `=` and `\n` never appear in a key
// (keys are fixed literals or table/chunk identifiers, which the store's
// path sanitisation already constrains), so the framing is unambiguous.
func writeLine(b *strings.Builder, key, value string) {
	b.WriteString(key)
	b.WriteByte('=')
	b.WriteString(value)
	b.WriteByte('\n')
}

// IsSignedFormat reports whether m's recorded FormatVersion asserts a
// detached signature ([FormatVersionSignedManifest]+). The read side
// gates verification on this — a v6 manifest MUST carry a valid
// signature; a pre-v6 manifest carries none.
func IsSignedFormat(m *Manifest) bool {
	return m != nil && m.FormatVersion >= FormatVersionSignedManifest
}

// MarshalManifestSignature serialises sig as indented JSON for the
// detached `.sig` object.
func MarshalManifestSignature(sig *ManifestSignature) ([]byte, error) {
	b, err := json.MarshalIndent(sig, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("backup: marshal manifest signature: %w", err)
	}
	return b, nil
}

// UnmarshalManifestSignature decodes a detached `.sig` object.
func UnmarshalManifestSignature(body []byte) (*ManifestSignature, error) {
	var sig ManifestSignature
	if err := json.Unmarshal(body, &sig); err != nil {
		return nil, fmt.Errorf("backup: decode manifest signature: %w", err)
	}
	return &sig, nil
}
