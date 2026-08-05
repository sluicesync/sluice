// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"errors"
	"fmt"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// # Retiring a manifest is TWO objects, not one (audit C-8)
//
// A manifest an ADR-0154 signed chain carries has a detached sibling —
// `<manifest>.sig` — and that signature is a CLAIM: "this manifest is link
// N of this chain, and here is the MAC that proves it". When maintenance
// retires the manifest, the claim stops being true; the artifact does not
// stop verifying. Nothing in the signature covers whether the catalog still
// names the file, so a `.sig` left beside a retired manifest answers GREEN
// to anyone checking signatures to decide what to trust — about a manifest
// that is no longer part of the backup.
//
// It bites hardest on the one manifest maintenance deliberately KEEPS. When
// prune or compact retires the lineage ROOT segment, `manifest.json` stays
// on disk as the chain's IDENTITY — the ADR-0152 CEK wrap and the only
// recorded Argon2id salt (see [keepsChainIdentity] and
// [sweepRootSegmentArtifacts]). Its signature is not re-issued, because
// [lineage.ResignLineage] signs the CATALOGUED survivor set and the retired
// root is not in it; so the file kept for its key material also kept a
// valid-looking signature asserting it is link 0 of a chain it left. Paired
// with the B-6 dispatch defect (restore reading that very manifest), the
// signature check confirmed the wrong artifact.
//
// So every maintenance path that retires a manifest goes through
// [retireManifest]. The sibling sweep, stated rather than promised:
//
//   - `backup prune`, whole dropped segments — [pruneWholeSegments], for the
//     segment full (body kept for the chain-identity root, signature always
//     retired) and for every dropped incremental. FIXED here.
//   - `backup prune`, the floor segment's leading incrementals —
//     [pruneFloorLeadingIncrementals]. FIXED here.
//   - `backup compact`, a merged-away ROOT segment —
//     [sweepRootSegmentArtifacts], for its incrementals and for the
//     identity manifest it keeps. FIXED here.
//   - `backup compact`, a merged-away sub-dir segment —
//     [sweepSegmentSubdir]. EXEMPT: it deletes every object under the
//     segment's `seg-*/` prefix, and a `.sig` is an object under that
//     prefix, so signatures already go with their manifests. Asserted, not
//     assumed, by TestCompactRetiresSignatureSiblings.
//   - `backup stream` rotation abort / recovery — `discardProvisional` and
//     `recoverRotationState` in internal/pipeline. EXEMPT for the same
//     reason: both list-and-delete the whole provisional `seg-*/` dir.
//   - The compaction staging sweeps (`renameStagingToFinal`,
//     `cleanupStagingDirs`). EXEMPT: prefix sweeps over
//     `.compact-staging-*`, and staged manifests are signed only after they
//     become catalogued survivors.
//
// Pinned by TestPruneRetiresSignatureSiblings,
// TestCompactRetiresSignatureSiblings, and the `.sig`-orphan assertion in
// TestPruneToFloorFull_RestoreDispatchMatrix (internal/pipeline), which
// sweeps the whole store rather than the paths this code touched.

// retireManifest removes a manifest the post-maintenance catalog no longer
// names, together with its detached ADR-0154 `.sig` sibling.
//
// keepBody leaves the manifest FILE in place while still retiring its
// signature — the chain-identity root ([keepsChainIdentity]). The asymmetry
// is the point: that file is kept for its key material, not as a link, and
// a signature asserting otherwise is exactly the stale green this helper
// exists to remove.
//
// Best-effort in the same shape compaction's sweeps use: every delete is
// ATTEMPTED and each failure is collected (naming its path) into the joined
// return, so a caller can surface a persistently failing delete instead of
// leaking store objects silently. A missing object is not a failure — both
// LocalStore and BlobStore treat delete-of-absent as nil — so an unsigned
// chain pays one no-op Delete per retired manifest and a re-run after a
// partial sweep stays clean.
func retireManifest(ctx context.Context, store irbackup.Store, manifestPath string, keepBody bool) error {
	var errs []error
	if !keepBody {
		if err := store.Delete(ctx, manifestPath); err != nil {
			errs = append(errs, fmt.Errorf("delete %q: %w", manifestPath, err))
		}
	}
	sigPath := lineage.ManifestSigPath(manifestPath)
	if err := store.Delete(ctx, sigPath); err != nil {
		errs = append(errs, fmt.Errorf("delete %q: %w", sigPath, err))
	}
	return errors.Join(errs...)
}
