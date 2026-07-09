// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package lineage

// Chain-level codec probing for catalog rebuild + the legacy synthetic
// root (audit N-14). The per-segment codec is recorded-never-sniffed at
// RESTORE time (ADR-0046 §5); this file exists for the one moment the
// record itself is being (re)created because lineage.json was lost —
// there the truthful source is the chunk bytes, and stamping
// [blobcodec.DefaultCodec] unconditionally (the pre-fix behavior) wrote
// a record that LIED for gzip/none chains, sending restore into a bare
// zstd decode error whose remediation loop pointed back at the rebuild
// tool that wrote the lie.
//
// Layout fact that shapes everything here: the codec sits INSIDE the
// encryption envelope (blobcodec.ChunkWriter compresses, then encrypts
// the codec output on Close), so an encrypted chunk's on-disk bytes
// start with a random GCM nonce and carry no codec magic. Probing an
// encrypted chunk therefore requires DECRYPTING it first — which needs
// the chain's key material — and is gated on the chunk's RECORDED
// encryption metadata, never attempted on raw ciphertext (a random
// nonce matches gzip's 2-byte magic 1 in 2^16 reads).

import (
	"context"
	"errors"
	"fmt"
	"io"

	"sluicesync.dev/sluice/internal/crypto"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
)

// ErrCodecSniffEncrypted reports that a codec probe hit an encrypted
// chunk without key material: the codec is sealed inside the encryption
// envelope, so it cannot be read from the bytes alone. Callers that
// WRITE a durable record ([RebuildLineageCatalogAt]) refuse loudly on
// this and tell the operator to supply --encrypt; the in-memory
// synthetic-root path ([ResolveLineage]) degrades to the default codec
// with a WARN naming the assumption.
var ErrCodecSniffEncrypted = errors.New(
	"chunk is encrypted: the compression codec is sealed inside the encryption envelope and cannot be sniffed without the chain's key material",
)

// codecProbe identifies one sniffable chunk within a manifest: the
// chunk plus how to derive its AAD. changeIdx is the chunk's ordinal in
// [irbackup.Manifest.ChangeChunks] (change chunks bind their replay
// ordinal into the AAD — ADR-0152), or -1 for a row chunk.
type codecProbe struct {
	m         *irbackup.Manifest
	chunk     *irbackup.ChunkInfo
	changeIdx int
}

// firstProbeableChunk returns m's first chunk reference (row chunks in
// table order, then change chunks), or nil when the manifest references
// no chunks at all (empty-table full, chunk-less incremental).
func firstProbeableChunk(m *irbackup.Manifest) *codecProbe {
	if m == nil {
		return nil
	}
	for _, t := range m.Tables {
		if t == nil {
			continue
		}
		for _, c := range t.Chunks {
			if c != nil && c.File != "" {
				return &codecProbe{m: m, chunk: c, changeIdx: -1}
			}
		}
	}
	for i, c := range m.ChangeChunks {
		if c != nil && c.File != "" {
			return &codecProbe{m: m, chunk: c, changeIdx: i}
		}
	}
	return nil
}

// chainCodecSniffer probes chunks for their codec. env may be nil (no
// key material — encrypted probes then fail with
// [ErrCodecSniffEncrypted]); chainRoot is the manifest carrying the
// chain's [irbackup.ChainEncryption] (the CEK-wrap owner for per-chain
// mode and the Azure KEK-rebind source), nil for plaintext chains. The
// unwrapped per-chain CEK is cached across probes.
type chainCodecSniffer struct {
	store     irbackup.Store
	env       crypto.EnvelopeEncryption
	chainRoot *irbackup.Manifest
	chainCEK  []byte
}

// sniff classifies the codec of p's chunk. Plaintext chunks are
// classified from their first [blobcodec.SniffCodecPrefixLen] bytes;
// encrypted chunks are read whole, decrypted (same CEK/AAD resolution
// as restore's chunk open), and classified from the plaintext prefix.
func (s *chainCodecSniffer) sniff(ctx context.Context, p *codecProbe) (blobcodec.Codec, error) {
	if p.chunk.Encryption == nil {
		rc, err := s.store.Get(ctx, p.chunk.File)
		if err != nil {
			return "", fmt.Errorf("codec probe: open chunk %q: %w", p.chunk.File, err)
		}
		buf := make([]byte, blobcodec.SniffCodecPrefixLen)
		n, err := io.ReadFull(rc, buf)
		_ = rc.Close()
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
			return "", fmt.Errorf("codec probe: read chunk %q: %w", p.chunk.File, err)
		}
		c, err := blobcodec.SniffCodec(buf[:n])
		if err != nil {
			return "", fmt.Errorf("codec probe: chunk %q: %w", p.chunk.File, err)
		}
		return c, nil
	}
	// Encrypted: no codec magic on disk (random nonce first) — decrypt,
	// then sniff the plaintext codec stream.
	if s.env == nil {
		return "", fmt.Errorf("codec probe: chunk %q: %w", p.chunk.File, ErrCodecSniffEncrypted)
	}
	cek, err := s.probeCEK(p.chunk)
	if err != nil {
		return "", err
	}
	rc, err := s.store.Get(ctx, p.chunk.File)
	if err != nil {
		return "", fmt.Errorf("codec probe: open chunk %q: %w", p.chunk.File, err)
	}
	ct, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		return "", fmt.Errorf("codec probe: read chunk %q: %w", p.chunk.File, err)
	}
	// AAD mirrors restore's chunk open: derived from the owning
	// manifest's RECORDED FormatVersion (nil pre-v5), with change
	// chunks binding their list ordinal.
	var aad []byte
	if p.changeIdx >= 0 {
		aad = irbackup.ChangeChunkAADFor(p.m, p.chunk, p.changeIdx)
	} else {
		aad = irbackup.ChunkAADFor(p.m, p.chunk)
	}
	pt, err := crypto.DecryptChunkWithAAD(ct, cek, aad)
	if err != nil {
		return "", fmt.Errorf("codec probe: decrypt chunk %q (wrong passphrase / KMS key, or tampered chunk): %w", p.chunk.File, err)
	}
	if len(pt) > blobcodec.SniffCodecPrefixLen {
		pt = pt[:blobcodec.SniffCodecPrefixLen]
	}
	c, err := blobcodec.SniffCodec(pt)
	if err != nil {
		return "", fmt.Errorf("codec probe: chunk %q (decrypted): %w", p.chunk.File, err)
	}
	return c, nil
}

// probeCEK resolves the CEK for one encrypted chunk, mirroring
// restore's resolution: a per-chunk WrappedCEK wins; otherwise the
// chain-level CEK is unwrapped once (via the [UnwrapChainCEK]
// chokepoint — identity binding + Azure KEK retarget) and cached.
func (s *chainCodecSniffer) probeCEK(chunk *irbackup.ChunkInfo) ([]byte, error) {
	if len(chunk.Encryption.WrappedCEK) > 0 {
		RebindEnvelopeKEK(s.env, s.chainRoot)
		cek, err := s.env.UnwrapCEK(chunk.Encryption.WrappedCEK)
		if err != nil {
			return nil, fmt.Errorf("codec probe: unwrap chunk cek (wrong passphrase / KMS key?): %w", err)
		}
		return cek, nil
	}
	if s.chainCEK != nil {
		return s.chainCEK, nil
	}
	if s.chainRoot == nil || s.chainRoot.ChainEncryption == nil || len(s.chainRoot.ChainEncryption.WrappedCEK) == 0 {
		return nil, errors.New("codec probe: encrypted chunk in per-chain mode but no chain-level WrappedCEK on the chain root (manifest corrupted?)")
	}
	cek, err := UnwrapChainCEK(s.env, s.chainRoot.ChainEncryption.WrappedCEK, s.chainRoot)
	if err != nil {
		return nil, fmt.Errorf("codec probe: unwrap chain cek (wrong passphrase / KMS key?): %w", err)
	}
	s.chainCEK = cek
	return cek, nil
}

// SniffChainCodec probes ONE chunk from EVERY manifest in recs and
// verifies the probes agree (sniff-and-VERIFY, audit N-14). Returns the
// agreed codec with found=true; ("", false, nil) when no manifest
// references any chunk (nothing to probe — the caller decides the
// fallback and owns naming the assumption). Disagreeing probes are a
// LOUD error: a sluice-written segment is single-codec by construction
// ([UpdateLineageForManifest] pins it on first write), so a mixed chain
// was spliced or partially rewritten and recording either codec would
// corrupt half the chain's restore. Encrypted chunks need env; without
// it the returned error wraps [ErrCodecSniffEncrypted].
func SniffChainCodec(ctx context.Context, store irbackup.Store, recs []ManifestRecord, env crypto.EnvelopeEncryption) (blobcodec.Codec, bool, error) {
	var chainRoot *irbackup.Manifest
	for _, r := range recs {
		if r.Manifest != nil && r.Manifest.ChainEncryption != nil {
			chainRoot = r.Manifest
			break
		}
	}
	if env != nil && chainRoot != nil && chainRoot.ChainEncryption.KEKMode != "" && env.Mode() != chainRoot.ChainEncryption.KEKMode {
		return "", false, fmt.Errorf("codec probe: envelope mode %q does not match the chain's recorded kek_mode %q",
			env.Mode(), chainRoot.ChainEncryption.KEKMode)
	}
	s := &chainCodecSniffer{store: store, env: env, chainRoot: chainRoot}
	var (
		agreed   blobcodec.Codec
		agreedOn string
		found    bool
	)
	for _, r := range recs {
		p := firstProbeableChunk(r.Manifest)
		if p == nil {
			continue
		}
		c, err := s.sniff(ctx, p)
		if err != nil {
			return "", false, err
		}
		if !found {
			agreed, agreedOn, found = c, p.chunk.File, true
			continue
		}
		if c != agreed {
			return "", false, fmt.Errorf(
				"codec probe: chain chunks disagree on compression codec: %q (%s) vs %q (%s) — "+
					"a sluice-written segment is single-codec by construction, so this chain was spliced or "+
					"partially rewritten; refusing to record a codec (DR data — never guess)",
				agreed, agreedOn, c, p.chunk.File,
			)
		}
	}
	return agreed, found, nil
}

// sniffLegacyRootCodec is [SniffChainCodec]'s lightweight sibling for
// [ResolveLineage]'s synthetic root: it probes the FIRST chunk it can
// find — the full's, else each incremental's in chain order (reading
// those manifests lazily) — with no key material (this layer has none;
// encrypted probes surface [ErrCodecSniffEncrypted] and the caller
// degrades with a WARN). One probe suffices here: the synthetic result
// is in-memory (re-derived per call, never written), a wrong codec
// still fails loudly at the first chunk decode, and the durable-record
// writer ([RebuildLineageCatalogAt]) does the full per-manifest
// sniff-and-verify.
func sniffLegacyRootCodec(ctx context.Context, store irbackup.Store, fullM *irbackup.Manifest, incPaths []string) (blobcodec.Codec, bool, error) {
	s := &chainCodecSniffer{store: store, chainRoot: fullM}
	if p := firstProbeableChunk(fullM); p != nil {
		c, err := s.sniff(ctx, p)
		if err != nil {
			return "", false, err
		}
		return c, true, nil
	}
	for _, ip := range incPaths {
		im, err := ReadManifestAt(ctx, store, ip)
		if err != nil {
			return "", false, fmt.Errorf("codec probe: read incremental manifest %q: %w", ip, err)
		}
		p := firstProbeableChunk(im)
		if p == nil {
			continue
		}
		c, err := s.sniff(ctx, p)
		if err != nil {
			return "", false, err
		}
		return c, true, nil
	}
	return "", false, nil
}
