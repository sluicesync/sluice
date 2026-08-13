// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// The Bug 248 gate: a LOCAL backup store on a case-folding filesystem
// (Windows NTFS measured; macOS default APFS shares the semantics) is a
// FOLDING NAMESPACE, and per-table store paths derive from table names
// — so a source legally holding `Orders` and `orders` (Linux MySQL
// lower_case_table_names=0, or PG) writes chunk paths that fold to ONE
// host file. Serial table writes then exit 0 having silently
// self-clobbered one table's chunks (an un-restorable backup taken at
// exit 0 — verify later blames bit-rot for damage sluice itself wrote),
// and parallel workers die on a spurious rename-lock error.
//
// The restore direction of this fold family was enumerated twice (the
// PRF-2 target-fold gates; the v0.124.2 filter-aware name-fold
// preflight) — the STORE as a fold target never was. This gate closes
// the class for the store side:
//
//   - `backup full` (chunkFilePath: `chunks/<dir>/<dir>-N.jsonl.gz`,
//     dir = the chunkDirName below) — refused here before any write.
//   - `backup export-as-parquet` (flat `<manifestTableKey>.parquet`
//     names) — same gate over the parquet keys.
//   - `backup incremental` is EXEMPT BY CONSTRUCTION: change-chunk
//     paths are namespaced by the manifest's CreatedAt UnixMilli
//     (changeChunkPath), never by table names, so no table-derived
//     path exists to fold.
//
// Whether the store actually folds is MEASURED, never assumed from
// GOOS (the premise-naming rule): the gate first looks for
// fold-colliding names — schemas without them, i.e. almost all, never
// probe — and only then writes two case-twin probe objects to the
// store and reads one back. A Linux ext4 store, a case-sensitive NTFS
// directory, and every cloud object store measure non-folding and
// proceed; a folding store refuses loudly naming the colliding tables.
// Folding is keyed on strings.ToLower — an approximation of NTFS's
// own upcase table that is exact for ASCII table names; a non-ASCII
// pair the two disagree on would slip past the cheap pre-check and
// never probe, degrading to today's behaviour, never to a false
// refusal.

// storeFoldProbePrefix namespaces the two probe objects. Outside every
// real layout prefix (chunks/, manifest, exports) so a probe can never
// touch backup content.
const storeFoldProbePrefix = "fold-probe/"

// chunkDirName is the table-derived directory segment of
// chunkFilePath — extracted so the Bug 248 gate and the path builder
// share ONE definition and cannot drift on what name the store sees.
func chunkDirName(t *ir.Table) string {
	if t.Schema != "" {
		return t.Schema + "__" + t.Name
	}
	return t.Name
}

// caseFoldCollisions groups names by their lower-cased form and
// returns each group of ≥2 distinct names, sorted for stable messages.
func caseFoldCollisions(names []string) [][]string {
	byFold := map[string][]string{}
	for _, n := range names {
		f := strings.ToLower(n)
		byFold[f] = append(byFold[f], n)
	}
	var out [][]string
	for _, group := range byFold {
		distinct := map[string]bool{}
		for _, n := range group {
			distinct[n] = true
		}
		if len(distinct) < 2 {
			continue
		}
		g := make([]string, 0, len(distinct))
		for n := range distinct {
			g = append(g, n)
		}
		sort.Strings(g)
		out = append(out, g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i][0] < out[j][0] })
	return out
}

// storeFoldsCase measures whether store's namespace folds case: write
// two probe objects whose keys differ only by case, read the
// lower-cased key back, and see whose content answers. Both probes are
// deleted best-effort on every path. An error from the probe's own
// Put/Get is returned as-is — a store that cannot Put/Get cannot back
// up either, and guessing would un-measure the premise.
func storeFoldsCase(ctx context.Context, store irbackup.Store) (bool, error) {
	lower := storeFoldProbePrefix + "sluice-case-probe"
	upper := storeFoldProbePrefix + "SLUICE-CASE-PROBE"
	defer func() {
		_ = store.Delete(ctx, lower)
		_ = store.Delete(ctx, upper)
	}()
	if err := store.Put(ctx, lower, strings.NewReader("lower")); err != nil {
		return false, fmt.Errorf("store fold probe: put %q: %w", lower, err)
	}
	if err := store.Put(ctx, upper, strings.NewReader("UPPER")); err != nil {
		return false, fmt.Errorf("store fold probe: put %q: %w", upper, err)
	}
	rc, err := store.Get(ctx, lower)
	if err != nil {
		return false, fmt.Errorf("store fold probe: get %q: %w", lower, err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		return false, fmt.Errorf("store fold probe: read %q: %w", lower, err)
	}
	return bytes.Equal(got, []byte("UPPER")), nil
}

// refuseStoreFoldCollision is the Bug 248 door: names are the
// table-derived store names the operation will write (chunk dir names
// for `backup full`, parquet keys for the export), mode labels the
// door. Nil when no names fold-collide (the store is never probed), or
// when they do but the store measures case-sensitive.
func refuseStoreFoldCollision(ctx context.Context, store irbackup.Store, names []string, mode string) error {
	collisions := caseFoldCollisions(names)
	if len(collisions) == 0 {
		return nil
	}
	folds, err := storeFoldsCase(ctx, store)
	if err != nil {
		return fmt.Errorf("%s: %w", mode, err)
	}
	if !folds {
		return nil
	}
	rendered := make([]string, len(collisions))
	for i, g := range collisions {
		rendered[i] = strings.Join(g, " / ")
	}
	return sluicecode.Wrap(sluicecode.CodeBackupStoreNameCollision,
		"back up to a case-sensitive store (a Linux filesystem, or an object store such as S3), "+
			"exclude one colliding table per group with `--exclude-table`, or rename one source table per group",
		fmt.Errorf("%s: the destination store folds letter case (measured by probe), and %d table-name group(s) would "+
			"write to ONE folded path each — the later table's data would silently overwrite the earlier's, "+
			"leaving a backup that cannot restore: %s",
			mode, len(collisions), strings.Join(rendered, "; ")))
}
