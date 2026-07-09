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
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

const (
	// ManifestCanonVersion versions the canonical serialization the
	// signature covers. Bumped only on an incompatible change to
	// [CanonicalManifestBytes]; it is the first field of the canonical
	// payload, so a verifier keyed on an old scheme fails closed.
	//
	// v2: the encoding became length-prefixed (provably INJECTIVE —
	// distinct field tuples can never render to identical bytes, closing
	// the raw-concatenation forgery where an embedded newline / delimiter
	// in a source-derived table name or chunk path let two distinct
	// manifests collide), and folded [Manifest.SchemaDelta] (restore
	// drives DDL from it), [Manifest.SchemaHistory] (replayed into the
	// schema-history table), and [Manifest.StartPosition]/[Manifest.EndPosition]
	// (resume anchors) under the signature.
	//
	// v3 (this build, ADR-0154 Phase 2): the signature SCHEME
	// ([SignatureSchemeHMACKEK] / [SignatureSchemeEd25519]) is now folded
	// into the canonical bytes as a dedicated token. This is the
	// scheme-BINDING that prevents scheme confusion: the verifier reads
	// the claimed scheme from the detached `.sig`, selects the matching
	// verification primitive, and recomputes the canonical bytes
	// INCLUDING that scheme — so an adversary who relabels an HMAC
	// signature as `ed25519` (or vice versa) to force a different/weaker
	// verification path changes the signed bytes and fails verification.
	//
	// The WRITER always emits the newest version (this constant); the
	// VERIFIER is DUAL-VERSION — it recomputes at the signature's OWN
	// recorded [ManifestSignature.CanonVersion] via
	// [CanonicalManifestBytesForVersion], so a v2 signature written by the
	// shipped Phase-1 (v0.99.208) binary still verifies GREEN on a Phase-2
	// binary (the "newer sluice always reads older" invariant). v2 has NO
	// scheme token — Phase 1 only had HMAC-off-KEK — so a v2 signature's
	// scheme is implicitly [SignatureSchemeHMACKEK].
	ManifestCanonVersion = "sluice-manifest-canon/v3"

	// ManifestCanonVersionV2 is the Phase-1 (v0.99.208) canonical
	// serialization the shipped binary signed with: byte-identical to
	// [CanonicalManifestBytes] MINUS the scheme token, tagged v2. Preserved
	// verbatim as ON-DISK CONTRACT so the dual-version verifier can still
	// authenticate every chain the Phase-1 binary wrote. NEVER change the
	// v2 rendering — it must byte-match what v0.99.208 emitted.
	ManifestCanonVersionV2 = "sluice-manifest-canon/v2"

	// SignatureSchemeHMACKEK is the Phase 1 scheme tag recorded in the
	// detached signature: HMAC-SHA-256 keyed off a key HKDF-derived from
	// the chain KEK (symmetric, encrypted-chains-only, zero new key mgmt).
	SignatureSchemeHMACKEK = "hmac-kek"

	// SignatureSchemeEd25519 is the Phase 2 scheme tag: an asymmetric
	// Ed25519 signature under an operator-managed keypair. The private key
	// signs; the public key (via `--verify-key`) verifies. Works for
	// plaintext AND encrypted chains — the keypair is independent of the
	// encryption keystore. Phase 3 adds "kms".
	SignatureSchemeEd25519 = "ed25519"

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
	// CanonVersion is the canonical-serialization version the signer used
	// ([ManifestCanonVersion] for new signatures; [ManifestCanonVersionV2]
	// for signatures written by the shipped Phase-1 binary). The verifier
	// is DUAL-VERSION: it recomputes at THIS recorded version
	// ([CanonicalManifestBytesForVersion]) so an older signature still
	// verifies, and refuses a NEWER version with an "upgrade" message
	// ([ErrUnsupportedCanonVersion]), never as tamper.
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
// covers: the canon version, the signature SCHEME (the Phase 2
// scheme-binding — see below), format version, identity (created_at /
// source_engine / kind / backup_id), the lineage parent pointer, the
// schema fingerprint, the resume anchors (start/end position), the
// chain-encryption descriptor, the table→row-count mapping, the full
// chunk list (row chunks by path; change chunks by ordinal, because
// change-replay order is semantic — the ADR-0152 rationale), the schema
// deltas (restore drives DDL from them) and schema-history entries
// (replayed into the schema-history table), and the freshness anchors
// (sequence + chunk count).
//
// scheme ([SignatureSchemeHMACKEK] / [SignatureSchemeEd25519]) is folded
// in as a dedicated token so a scheme swap changes the signed bytes: an
// adversary cannot relabel an HMAC `.sig` as Ed25519 (or vice versa) to
// force a weaker verification path, because the verifier recomputes these
// bytes with the CLAIMED scheme and its scheme-specific primitive fails.
//
// The output is a sequence of LENGTH-PREFIXED tokens (`<len>:<bytes>\n`).
// Length-prefixing makes the encoding provably injective: the byte stream
// decodes to exactly one token sequence regardless of what bytes a value
// contains (embedded `\n` / `=` / `:` / `|` in a source-derived table
// name or chunk path can no longer forge a structural boundary), so two
// manifests differing in ANY signed field render to DIFFERENT bytes.
// Collections whose order is not semantic are sorted first. Returns an
// error only if a SchemaDelta table cannot be fingerprinted. It is
// ON-DISK CONTRACT (golden-pinned) — see the package doc.
func CanonicalManifestBytes(m *Manifest, seq int, scheme string) ([]byte, error) {
	return CanonicalManifestBytesForVersion(m, seq, ManifestCanonVersion, scheme)
}

// ErrUnsupportedCanonVersion is returned when a detached signature records
// a canonical-serialization version this build does not know how to
// recompute (a signature written by a NEWER sluice). It is NOT a tamper
// signal — the caller surfaces it as "upgrade sluice", never as
// SIGNATURE-INVALID.
var ErrUnsupportedCanonVersion = errors.New("backup manifest signed with a newer canonicalization than this build supports; upgrade sluice to restore/verify it")

// manifestCanonHasScheme maps each SUPPORTED manifest canon version to
// whether it folds the scheme token in. v2 (Phase 1) does not; v3 (Phase
// 2) does. A version absent from this map is unsupported (future).
var manifestCanonHasScheme = map[string]bool{
	ManifestCanonVersionV2: false,
	ManifestCanonVersion:   true,
}

// CanonicalManifestBytesForVersion renders the canonical bytes at a
// SPECIFIC canon version — the dual-version verify entry point. The
// writer always uses [ManifestCanonVersion] (via [CanonicalManifestBytes]);
// the verifier passes the signature's recorded version so a Phase-1 v2
// signature is recomputed WITHOUT the scheme token (byte-matching what
// v0.99.208 signed) and a v3 signature WITH it. Everything AFTER the
// version+scheme prefix is identical across versions — v2 and v3 differ
// only in the version tag and the presence of the scheme token.
// Returns [ErrUnsupportedCanonVersion] for an unknown (newer) version.
func CanonicalManifestBytesForVersion(m *Manifest, seq int, canonVersion, scheme string) ([]byte, error) {
	withScheme, ok := manifestCanonHasScheme[canonVersion]
	if !ok {
		return nil, fmt.Errorf("%w (canon version %q)", ErrUnsupportedCanonVersion, canonVersion)
	}
	var b strings.Builder
	tok(&b, canonVersion)
	if withScheme {
		field(&b, "scheme", scheme)
	}
	field(&b, "format_version", strconv.Itoa(m.FormatVersion))
	field(&b, "source_engine", m.SourceEngine)
	field(&b, "created_at", m.CreatedAt.UTC().Format(time.RFC3339Nano))
	field(&b, "kind", canonicalKind(m.Kind))
	field(&b, "backup_id", m.BackupID)
	field(&b, "parent_backup_id", m.ParentBackupID)
	field(&b, "schema_hash", m.SchemaHash)
	field(&b, "sequence", strconv.Itoa(seq))
	field(&b, "chunk_count", strconv.Itoa(ManifestChunkCount(m)))
	canonPosition(&b, "start_position", m.StartPosition)
	canonPosition(&b, "end_position", m.EndPosition)
	canonChainEncryption(&b, m.ChainEncryption)

	// Table → row-count mapping, sorted by (schema, name). Schema and name
	// are SEPARATE tokens, so `(a, b.c)` and `(a.b, c)` no longer collide.
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
		tok(&b, "table")
		tok(&b, t.Schema)
		tok(&b, t.Name)
		tok(&b, strconv.FormatInt(t.RowCount, 10))
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
		tok(&b, "rowchunk")
		tok(&b, c.File)
		tok(&b, c.SHA256)
		tok(&b, strconv.FormatInt(c.RowCount, 10))
	}

	// Change chunks in list order, ordinal-tokened: change-replay order IS
	// semantic (ADR-0152), so a reorder must change the canonical bytes.
	for i, c := range m.ChangeChunks {
		if c == nil {
			continue
		}
		tok(&b, "changechunk")
		tok(&b, strconv.Itoa(i))
		tok(&b, c.File)
		tok(&b, c.SHA256)
		tok(&b, strconv.FormatInt(c.RowCount, 10))
	}

	// Schema deltas in OBSERVATION order (semantic — restore replays them
	// in slice order and drives DDL from them). The before/after table
	// shapes are folded as their round-trip-stable fingerprint
	// ([ComputeSchemaHash] of a single-table schema — the same
	// canonicalization the manifest's own schema_hash uses, so a legit
	// manifest's decoded-then-re-fingerprinted delta matches the signer's).
	for i, d := range m.SchemaDelta {
		if d == nil {
			continue
		}
		beforeFP, err := deltaTableFingerprint(d.Before)
		if err != nil {
			return nil, fmt.Errorf("backup: canonical: schema delta %d before: %w", i, err)
		}
		afterFP, err := deltaTableFingerprint(d.After)
		if err != nil {
			return nil, fmt.Errorf("backup: canonical: schema delta %d after: %w", i, err)
		}
		tok(&b, "schemadelta")
		tok(&b, strconv.Itoa(i))
		tok(&b, d.Kind)
		tok(&b, d.Schema)
		tok(&b, d.Table)
		tok(&b, beforeFP)
		tok(&b, afterFP)
	}

	// Schema history in emission order. TableJSON is the already-marshalled
	// bytes recorded in the manifest (byte-identical across a JSON
	// round-trip via base64), so it is folded verbatim.
	for i, h := range m.SchemaHistory {
		if h == nil {
			continue
		}
		tok(&b, "schemahistory")
		tok(&b, strconv.Itoa(i))
		tok(&b, h.StreamID)
		tok(&b, h.Schema)
		tok(&b, h.Table)
		tok(&b, h.AnchorPosition.Engine)
		tok(&b, h.AnchorPosition.Token)
		tok(&b, string(h.TableJSON))
	}
	return []byte(b.String()), nil
}

// deltaTableFingerprint returns the round-trip-stable fingerprint of a
// SchemaDelta before/after table (empty for a nil table), reusing the
// schema-hash canonicalization so a decoded-from-manifest table
// fingerprints identically to the reader-fresh one the signer saw.
func deltaTableFingerprint(t *ir.Table) (string, error) {
	if t == nil {
		return "", nil
	}
	return ComputeSchemaHash(&ir.Schema{Tables: []*ir.Table{t}})
}

// canonChainEncryption folds the chain-encryption descriptor as a series
// of length-prefixed tokens (or a single "none" token for a plaintext
// chain). Every sub-field is its own token — a `|`/`=` in an operator- or
// KMS-supplied kek_ref cannot forge a boundary. The Argon2id KDF params
// are included because a tampered KDF param is a pre-auth bomb (ADR-0152
// N-7) and belongs under the signature.
func canonChainEncryption(b *strings.Builder, enc *ChainEncryption) {
	if enc == nil {
		tok(b, "chain_encryption")
		tok(b, "none")
		return
	}
	tok(b, "chain_encryption")
	tok(b, enc.Algorithm)
	tok(b, enc.Mode)
	tok(b, enc.KEKMode)
	tok(b, enc.KEKRef)
	tok(b, hex.EncodeToString(enc.WrappedCEK))
	if enc.Argon2id == nil {
		tok(b, "argon2id_none")
		return
	}
	a := enc.Argon2id
	tok(b, "argon2id")
	tok(b, hex.EncodeToString(a.Salt))
	tok(b, strconv.FormatUint(uint64(a.Memory), 10))
	tok(b, strconv.FormatUint(uint64(a.Iterations), 10))
	tok(b, strconv.FormatUint(uint64(a.Parallelism), 10))
	tok(b, strconv.FormatUint(uint64(a.KeyLen), 10))
}

// canonPosition folds an engine-tagged position as label + engine + token
// tokens (each length-prefixed, so no delimiter in the opaque token can
// forge a boundary).
func canonPosition(b *strings.Builder, label string, p ir.Position) {
	tok(b, label)
	tok(b, p.Engine)
	tok(b, p.Token)
}

// field writes a fixed field name + its value as two length-prefixed
// tokens.
func field(b *strings.Builder, name, value string) {
	tok(b, name)
	tok(b, value)
}

// tok appends one length-prefixed token: the decimal byte length, a ':'
// separator, the raw bytes, and a '\n'. The length prefix is what makes
// the encoding injective — a decoder reads exactly len bytes, so no
// embedded byte (`\n`, `=`, `:`, `|`, …) can be misread as structure.
func tok(b *strings.Builder, s string) {
	b.WriteString(strconv.Itoa(len(s)))
	b.WriteByte(':')
	b.WriteString(s)
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
