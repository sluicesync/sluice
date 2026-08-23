// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/crypto"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// # The chain-maintenance readability gate (Bug 214, roadmap item 95)
//
// `backup compact` and `backup prune` are the operations that DELETE
// chain artifacts, and both delete passes reason LOCALLY: "the catalog no
// longer references these files, so they are orphans." That reasoning is
// locally correct and whole-chain incomplete — it never asks the only
// question an operator cares about, which is whether the chain left
// behind still RESTORES. Hence one guard, shared verbatim by both, with
// `op` naming which of them is asking.
//
// Bug 214 is what the gap costs. The orphan sweep deleted the chain-root
// `manifest.json` because the merged segment owns a byte-identical copy;
// true of the BYTES, false of the IDENTITY. The chain CEK's wrap is bound
// to the root manifest (ADR-0152) and, for a passphrase chain, the read
// side re-derives its KEK from the Argon2id salt recorded ONLY there
// ([lineage.ReadRootManifest] → the CLI's buildReadEnvelope). Compaction
// deleted it, logged `groups_merged=1 segments_removed=3`, and exited 0;
// `verify` and `restore` then both refused at `unwrap chain cek` with zero
// rows, on a chain that had restored 230/230 md5-exact minutes earlier —
// including segments compaction never touched.
//
// The durable answer to that shape is not a more careful sweep. It is
// making the operation PROVE the chain still reads, twice:
//
//   - BEFORE the catalog swap, against the prospective post-compact
//     catalog. A refusal here has destroyed nothing: no catalog was
//     written, no file deleted, every source segment is intact.
//   - AFTER the delete pass, against what is actually on disk. A refusal
//     here cannot undo the deletes, but it converts Bug 214's exit-0-with-
//     an-unreadable-chain into a loud, named failure — which is the whole
//     difference between an operator who knows their DR archive is broken
//     and one who finds out at restore time.
//
// The gate is deliberately CHEAP: it re-walks the lineage, re-resolves
// the chain's identity + key material, and STATS every chunk file the
// surviving manifests reference ([verifyChunksPresent] — one Exists per
// chunk, no reads). It does not decrypt chunks, verify hashes, or replay
// changes — a full `verify` inline would make every compact run pay a
// whole-chain read; `sluice backup verify` remains the deep check.
//
// The presence leg exists because of what the identity legs honestly
// could not see (2026-08-22 invariant sweep): the cmd-layer claim "a run
// can never report success over a chain it just made unreadable" rested
// on the DELETE-SET GEOMETRY — retired-prefix sweeps can't touch a
// survivor's files — with nothing binding it. A future sweep bug that
// eats a survivor's chunk was invisible to a header-level gate, so the
// "never" was one refactor away from false. Statting the referenced
// files converts that geometry argument into a checked property, at
// maintenance-op frequency and O(chunks) stat cost. What remains out of
// scope, deliberately: chunk CONTENT (bit rot, truncation, a wrong key
// on per-chunk mode) — that is `backup verify --depth` territory.

// verifyChainReadable re-reads the chain described by cat the way a
// RESTORE would and refuses — under one stable, machine-matchable code —
// when it cannot. See [checkChainReadable] for the check itself.
//
// The code lives HERE rather than at the two call sites so a future
// caller cannot acquire the guard without acquiring its refusal class:
// the whole point of the refusal is that a script or agent branching on
// `SLUICE-E-*` (which is what docs/operator/error-codes.md tells
// operators to do) can see it, and that it exits 3 — "a re-run will not
// help" — rather than the generic 1 a bare fmt.Errorf produced.
func verifyChainReadable(
	ctx context.Context,
	store irbackup.Store,
	cat *lineage.Catalog,
	env crypto.EnvelopeEncryption,
	op, stage string,
) error {
	return sluicecode.Wrap(sluicecode.CodeBackupChainUnreadable, chainUnreadableHint,
		checkChainReadable(ctx, store, cat, env, op, stage))
}

// chainUnreadableHint is the standalone remedy [sluicecode.CodedError]
// carries alongside the prose. Deliberately stage-agnostic — the prose
// names which stage fired, and only the prose can, since the recovery
// differs sharply between them.
const chainUnreadableHint = "re-read the refusal's stage: a pre-swap/pre-sweep refusal deleted nothing, a post-sweep one names the chain-root manifest recovery; pass --encrypt with the chain's key material for the full check"

// checkChainReadable performs the readability check. op names the
// operation for the operator ("backup compact"); stage names WHICH of
// the two calls fired, because the recovery differs sharply between
// them.
//
// env is the operator's read envelope when key material was supplied
// (`--encrypt`), nil otherwise. With an envelope the gate performs the
// real chain-CEK unwrap — the exact operation Bug 214 broke. Without one
// it verifies the chain's identity + recorded key-derivation material and
// says so; an encrypted chain can be compacted with no key (only a SIGNED
// chain requires one), and refusing that would be a new refusal for a
// workflow that has always worked.
func checkChainReadable(
	ctx context.Context,
	store irbackup.Store,
	cat *lineage.Catalog,
	env crypto.EnvelopeEncryption,
	op, stage string,
) error {
	// 1. The structural walk: every segment's full + incrementals read,
	//    every boundary validates. nil comparator — positions are
	//    engine-native and maintenance has no live source engine to order
	//    them with; the structural same-engine guarantee is what the
	//    cross-engine restore path relies on too.
	links, err := lineage.BuildLineageChainFromCatalog(ctx, store, cat, nil)
	if err != nil {
		return fmt.Errorf("%s: %s readability check: %w: %w", op, stage, errChainDoesNotWalk, err)
	}
	if len(links) == 0 {
		// Never let the gate pass vacuously: an empty walk means there is
		// nothing to have proven readable.
		return fmt.Errorf("%s: %s %w", op, stage, errNoChainToVerify)
	}

	// 1b. Chunk presence: every chunk file the surviving manifests
	//    reference must still exist. The walk above reads only MANIFESTS,
	//    so on its own it passes a chain whose data files a buggy sweep
	//    has eaten — see the header for why this leg was added.
	if err := verifyChunksPresent(ctx, store, links, op, stage); err != nil {
		return err
	}

	// 2. The chain's identity. Restore reads it from the lineage ROOT by
	//    absolute path — never through the catalog — which is precisely
	//    why a catalog-driven "is it still referenced?" sweep could not
	//    see that it was load-bearing.
	root, err := lineage.ReadRootManifest(ctx, store)
	if err != nil {
		return fmt.Errorf("%s: %s readability check: read chain-root manifest: %w", op, stage, err)
	}

	enc := links[0].Manifest.ChainEncryption
	if enc == nil {
		if root == nil {
			// A plaintext chain restores fine without the root manifest
			// (nothing derives a key from it), and a chain compacted by a
			// pre-fix binary legitimately has none. WARN, never refuse —
			// a refusal here would reject chains that restore.
			slog.WarnContext(
				ctx, op+": the chain-root manifest is absent (a chain compacted by sluice <= v0.104.1, which deleted it). The chain is plaintext so it still restores; an ENCRYPTED chain in this state does not — see `sluice backup compact`'s Bug-214 note",
				slog.String("stage", stage),
			)
		}
		return nil
	}
	return verifyEncryptedChainIdentity(ctx, links[0].Manifest, root, enc, env, op, stage)
}

// verifyEncryptedChainIdentity is [verifyChainReadable]'s encrypted leg:
// the chain-root manifest must still carry the key material the read side
// rebuilds its envelope from, and — when the operator supplied a key —
// the chain CEK must actually unwrap.
func verifyEncryptedChainIdentity(
	ctx context.Context,
	first, root *irbackup.Manifest,
	enc *irbackup.ChainEncryption,
	env crypto.EnvelopeEncryption,
	op, stage string,
) error {
	// Passphrase chains derive their KEK from the Argon2id salt recorded
	// on the chain-root manifest and NOWHERE else, so losing that file
	// revokes readability for the whole chain. KMS chains do not (the KEK
	// lives in the KMS; the KEKRef travels on every segment full), so a
	// missing root manifest costs them nothing and must not be refused.
	if enc.KEKMode == "" || enc.KEKMode == crypto.KEKModePassphrase {
		switch {
		case root == nil:
			// The recovery names both surviving-copy shapes because ONE
			// guard now serves two operations: compaction leaves a merged
			// segment holding a byte-identical copy, and prune leaves none —
			// but every rotation-born segment full carries the SAME chain
			// salt (stream_rotation.go runs backup.Backup with the aligned
			// envelope), so any surviving segment's header restores the
			// chain's identity either way.
			return fmt.Errorf("%s: %s readability check: this chain is encrypted with a passphrase-derived key, and its chain-root %q — the file the restore side reads the Argon2id salt from to re-derive that key — is missing. Restoring it would fail at `unwrap chain cek` with a perfectly good passphrase. Recovery: every surviving segment full carries the same chain salt, so copying any one of them back restores the chain's identity — `cp <chain>/%s<id>/%s <chain>/%s` after a compact, `cp <chain>/%s<id>/%s <chain>/%s` for a rotation-born segment",
				op, stage, lineage.ManifestFileName,
				mergedSegmentDirPrefix, lineage.ManifestFileName, lineage.ManifestFileName,
				lineage.RotationSegmentDirPrefix, lineage.ManifestFileName, lineage.ManifestFileName)
		case root.ChainEncryption == nil:
			return fmt.Errorf("%s: %s readability check: the chain-root %q records no chain-encryption metadata while the chain's first segment is encrypted (kek_mode=%q) — the restore side would build a plaintext read path for an encrypted chain",
				op, stage, lineage.ManifestFileName, enc.KEKMode)
		case root.ChainEncryption.Argon2id == nil:
			return fmt.Errorf("%s: %s readability check: the chain-root %q records no Argon2id parameters, so the restore side cannot re-derive this chain's passphrase KEK",
				op, stage, lineage.ManifestFileName)
		case enc.Argon2id != nil && !bytes.Equal(root.ChainEncryption.Argon2id.Salt, enc.Argon2id.Salt):
			return fmt.Errorf("%s: %s readability check: the chain-root %q records a DIFFERENT Argon2id salt than the chain's first restorable segment — a read envelope built from the root would not unwrap the segment's CEK",
				op, stage, lineage.ManifestFileName)
		}
	}

	// The behavioural half: with key material in hand, do the unwrap the
	// restore preflight does, against the same owner manifest it uses
	// (the first restorable segment's full — the CEK wrap's ADR-0152
	// identity binding). Per-chunk mode carries no chain-level CEK; its
	// per-chunk wraps are probed by `backup verify`.
	mode := enc.Mode
	if mode == "" {
		mode = crypto.EncryptModePerChain
	}
	if env == nil {
		slog.WarnContext(
			ctx, op+": no key material was supplied (--encrypt), so the post-maintenance readability check verified the chain's identity + recorded key-derivation material but could NOT prove its key unwraps. Pass --encrypt with the chain's key to get the full check",
			slog.String("stage", stage),
			slog.String("kek_mode", enc.KEKMode),
		)
		return nil
	}
	if mode != crypto.EncryptModePerChain || len(enc.WrappedCEK) == 0 {
		return nil
	}
	if _, err := lineage.UnwrapChainCEK(env, enc.WrappedCEK, first); err != nil {
		return fmt.Errorf("%s: %s readability check: the chain's key does not unwrap (%s): %w",
			op, stage, lineage.CEKUnwrapHint, err)
	}
	return nil
}

// verifyChunksPresent stats every chunk file the chain's manifests
// reference — table chunks and incremental change-chunks — through the
// same per-segment prefixed store the restore path resolves them with
// (paths in a manifest are relative to its SEGMENT's dir). One
// [irbackup.Store.Exists] per chunk; nothing is read.
//
// A manifest can legitimately reference zero chunks (empty tables), so
// there is no per-link floor; the caller's len(links)==0 refusal remains
// the gate's anti-vacuity floor, and every listed chunk was uploaded
// BEFORE the committer listed it, so "listed ⇒ once existed" holds even
// for a Partial entry.
func verifyChunksPresent(ctx context.Context, root irbackup.Store, links []lineage.SegmentRecord, op, stage string) error {
	for i := range links {
		link := &links[i]
		ss := root
		segDir := "(chain root)"
		if link.Segment != nil {
			ss = link.Segment.Store(root)
			if link.Segment.Dir != "" {
				segDir = link.Segment.Dir
			}
		}
		refuse := func(file string, err error) error {
			if err != nil {
				return fmt.Errorf("%s: %s readability check: probe chunk %q (segment %s): %w", op, stage, file, segDir, err)
			}
			return fmt.Errorf(
				"%s: %s readability check: chunk file %q (segment %s, manifest %q) is MISSING from the backup store — the chain's manifests read but a restore would fail at this chunk; every file a surviving manifest references must outlive the maintenance sweep",
				op, stage, file, segDir, link.Path,
			)
		}
		for _, tm := range link.Manifest.Tables {
			for _, ch := range tm.Chunks {
				ok, err := ss.Exists(ctx, ch.File)
				if err != nil || !ok {
					return refuse(ch.File, err)
				}
			}
		}
		for _, ch := range link.Manifest.ChangeChunks {
			ok, err := ss.Exists(ctx, ch.File)
			if err != nil || !ok {
				return refuse(ch.File, err)
			}
		}
	}
	return nil
}

// errPreSweepReadability decorates a pre-delete gate failure with the one
// fact that changes what the operator does next: nothing has been deleted.
func errPreSweepReadability(op string, err error) error {
	return fmt.Errorf("%w — refusing to delete the superseded segments; NO files were removed and every pre-maintenance segment is intact, so the chain is exactly as restorable as it was before this %s run", err, op)
}

// errPostSweepReadability decorates a post-delete gate failure. This is
// the Bug-214 shape itself: the operation succeeded structurally and left
// a chain that does not read. It cannot un-delete, so its whole job is to
// make sure the run does NOT report success.
func errPostSweepReadability(op string, err error) error {
	return fmt.Errorf("%w — the superseded segments have already been deleted, so this %s CANNOT be undone by re-running it; restore from another copy of this chain, or apply the recovery the message above names before taking further backups against it", err, op)
}

// errNoChainToVerify guards the gate against a vacuous pass: a walk that
// produced no links proved nothing.
var errNoChainToVerify = errors.New("readability check: the chain walks to zero links — nothing was proven readable")

// errChainDoesNotWalk marks the gate's STRUCTURAL leg — the lineage walk
// itself refused — as distinct from its identity/key legs, which fail
// with the chain perfectly walkable and want an entirely different
// remedy. It exists for callers that can EXPLAIN a structural refusal in
// their own terms; prune matched on it while its within-segment trim
// could still reach the gate, and stopped when segment-granular
// retention made that shape unplannable (roadmap item 100). The prose is
// unchanged from when this was an inline literal, so the sentinel costs
// the operator nothing whether or not anyone is matching on it.
var errChainDoesNotWalk = errors.New("the chain does not walk")
