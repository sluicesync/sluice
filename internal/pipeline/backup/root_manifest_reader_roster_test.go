// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// # The chain-root manifest reader roster (audit B-6)
//
// `manifest.json` at the lineage root is TWO things wearing one filename:
//
//   - a LINK — segment 0's full, while segment 0 is still catalogued; and
//   - the chain's IDENTITY — the ADR-0152 CEK wrap and the only recorded
//     Argon2id salt, which [keepsChainIdentity] and
//     [sweepRootSegmentArtifacts] deliberately KEEP after retention or
//     compaction has retired segment 0.
//
// After maintenance retires that segment the file is identity ONLY. Reading
// it as a link then restores data the chain no longer holds. Both of the
// places that keep it carried a comment saying the survivor was "safe by
// construction", each naming ONE reader that could reach it without the
// catalog — and both were short by the same one: [Restore.Run]'s
// single-manifest path resolves the full by CONVENTION, so on the
// prune-to-floor shape it was handed the retired root while `backup verify`
// walked the catalog floor. The invariant was written down, never checked,
// and stopped holding.
//
// This is the derived enumeration those comments now point at. Every call
// to a by-convention root-manifest reader — the three that resolve
// [lineage.ManifestFileName] from a store rather than from a catalogued
// segment path — must be classified here, with a reason. A new one fails
// the build until its author says which of the two files they meant.
//
// SCOPE, stated so the name cannot be read as broader than the truth: this
// walks the four packages that read manifests (`internal/pipeline`,
// `internal/pipeline/backup`, `internal/pipeline/lineage`, `cmd/sluice`).
// It does NOT see [lineage.ReadManifestAt], which takes an explicit path
// and is how catalog-driven readers resolve a segment full — that call is
// safe by its signature, not by review. A fifth package that grows a
// root-manifest read is caught by rootManifestReaderDirs going stale only
// if someone adds it there; the anti-vacuity floor below catches the
// opposite failure (the walk silently matching nothing).

// rootManifestReaders are the three functions that resolve
// [lineage.ManifestFileName] from a STORE (never an explicit path).
// ReadRootManifest is a thin alias of ReadManifestIfPresent; both are
// listed because a caller reads whichever name it typed.
var rootManifestReaders = map[string]bool{
	"ReadManifest":          true,
	"ReadManifestIfPresent": true,
	"ReadRootManifest":      true,
}

// rootManifestReaderDirs are the package directories walked, keyed by the
// label that prefixes a roster entry (so two `Run` methods in two packages
// cannot share one key).
var rootManifestReaderDirs = map[string]string{
	"backup": ".",
	"line":   "../lineage",
	"pipe":   "..",
	"cmd":    "../../../cmd/sluice",
}

// rootManifestReaderDefinitions are the reader implementations themselves —
// a call from inside one of these is the wrapper delegating, not a consumer.
var rootManifestReaderDefinitions = map[string]bool{
	"ReadRootManifest":      true,
	"ReadManifestIfPresent": true,
}

// rootManifestReaderRoster classifies every call site. The KEY is
// "<file>.go:<enclosing func>"; the VALUE is why reaching a possibly-RETIRED
// root manifest is correct there. Two classifications exist:
//
//	IDENTITY — reads only chain-level key material (ChainEncryption: the
//	  wrapped CEK, the KEK mode/ref, the Argon2id salt) or reports the
//	  header for an operator. Correct on a retired root BY DESIGN: that is
//	  the reason the file is kept.
//	DISPATCHED — reads the manifest as a LINK, and is only reached on a
//	  lineage shape where the root manifest IS the catalogued full. The
//	  predicate that guarantees that is [verifyLineageNeedsWalk].
//
// A third answer — "reads it as a link with nothing guaranteeing it is
// still one" — is the B-6 defect, and has no entry.
var rootManifestReaderRoster = map[string]string{
	// ---- DISPATCHED ----
	"backup/restore.go:Restore.Run": "DISPATCHED. The single-manifest restore path, reached only " +
		"when verifyLineageNeedsWalk reports false — which now requires the one restorable segment to " +
		"live at the conventional root paths (singleManifestPathReaches). On every other shape, " +
		"including the prune-to-floor chain this roster is about, it dispatches to ChainRestore, " +
		"which resolves each segment full through the catalog. THIS is the site audit B-6 was.",

	// ---- IDENTITY (chain-level key material / header only) ----
	"backup/restore.go:verifyChainIdentityManifest": "IDENTITY. `backup verify` reads the root's " +
		"ChainEncryption to pick the CEK a change chunk was sealed under; substitutes only when the " +
		"root actually carries that metadata, and never reads its tables or chunks.",
	"backup/chain_restore.go:ChainRestore.chainIdentityManifest": "IDENTITY. The restore-side twin " +
		"of the above — the chain-level encryption preflight, ChainEncryption only.",
	"backup/chain_readable_gate.go:checkChainReadable": "IDENTITY. The maintenance readability gate " +
		"proves the chain's key material still unwraps; it reads the identity header for exactly " +
		"that, and walks the CATALOG for structure.",
	"line/encryption.go:ChainRootEncryption": "IDENTITY. THE canonical identity read — it exists to " +
		"recover the CEK wrap and the Argon2id salt recorded ONLY on the root manifest, which is the " +
		"whole reason the file is kept when its segment is retired.",
	"pipe/broker.go:SyncFromBackup.preflightChainEncryption": "IDENTITY. The broker's chain-root " +
		"encryption preflight; its data replay walks the catalog through BuildBrokerChain.",
	"cmd/backup.go:EncryptionFlags.buildMaintenanceSigner": "IDENTITY. Feeds buildReadEnvelope so an " +
		"HMAC-off-KEK maintenance signer derives the chain's KEK; key material only.",
	"cmd/backup.go:BackupVerifyCmd.Run": "IDENTITY (both sites). --rebuild-catalog and the verify " +
		"scan each feed buildReadEnvelope for the chain's Argon2id params; the chain STRUCTURE both " +
		"then read comes from the catalog walk, never from this manifest.",
	"cmd/backup.go:BackupPruneCmd.Run":   "IDENTITY. buildReadEnvelope input for prune's readability gate.",
	"cmd/backup.go:BackupCompactCmd.Run": "IDENTITY. buildReadEnvelope input for compact's readability gate.",
	"cmd/backup.go:RestoreCmd.run": "IDENTITY. buildReadEnvelope input — the KEK derivation that " +
		"precedes the restore. The manifest RESTORED is chosen inside backup.Restore.Run, which is " +
		"the DISPATCHED entry above; this read never selects it.",
	"cmd/backup_export_parquet.go:BackupExportAsParquetCmd.Run": "IDENTITY. buildReadEnvelope input; " +
		"the exported snapshot is selected from the catalog walk (selectExportFull).",
	"cmd/cli.go:SyncFromBackupCmd.Run": "IDENTITY. buildReadEnvelope input for the broker cold start.",

	// ---- WRITER-SIDE (extends the chain at its root; never restores) ----
	"backup/backup.go:Backup.resolveResumeState": "WRITER-SIDE. `backup full` resume reads the prior " +
		"root manifest to inherit chain encryption and per-table progress. A backup EXTENDS the chain " +
		"at its root and never reads a manifest as restorable data, so a retired segment 0 is not a " +
		"shape it can misread — and a root manifest kept as pure identity is refused as a resume base " +
		"by the ordinary partial-state/identity checks that follow.",
}

// rootManifestReaderMinSites is the anti-vacuity floor: below this the walk
// has stopped matching the real readers (a rename, a package move) and would
// pass forever. It is deliberately a few under the current count so an
// ordinary refactor that drops one site does not fail the gate for the wrong
// reason.
const rootManifestReaderMinSites = 8

// TestRootManifestReaderRoster is the derived enumeration the "safe by
// construction" comments in [keepsChainIdentity] and
// [sweepRootSegmentArtifacts] now cite instead of asserting one.
func TestRootManifestReaderRoster(t *testing.T) {
	fset := token.NewFileSet()
	var (
		offenders []string
		checked   int
		seen      = map[string]bool{}
	)
	labels := make([]string, 0, len(rootManifestReaderDirs))
	for label := range rootManifestReaderDirs {
		labels = append(labels, label)
	}
	sort.Strings(labels)
	for _, label := range labels {
		dir := rootManifestReaderDirs[label]
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read package dir %s: %v", dir, err)
		}
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			path := filepath.Join(dir, name)
			f, perr := parser.ParseFile(fset, path, nil, 0)
			if perr != nil {
				t.Fatalf("parse %s: %v", path, perr)
			}
			for _, decl := range f.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || rootManifestReaderDefinitions[fn.Name.Name] {
					continue
				}
				key := label + "/" + name + ":" + funcLabel(fn)
				ast.Inspect(fn, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok || !isRootManifestReaderCall(call) {
						return true
					}
					checked++
					seen[key] = true
					if _, classified := rootManifestReaderRoster[key]; classified {
						return true
					}
					pos := fset.Position(call.Pos())
					offenders = append(offenders, key+"  ("+filepath.Base(pos.Filename)+":"+strconv.Itoa(pos.Line)+")")
					return true
				})
			}
		}
	}

	if checked < rootManifestReaderMinSites {
		t.Fatalf("found only %d by-convention root-manifest read(s) (floor %d); the walk has probably "+
			"stopped matching the real readers (a rename, a package move) and is now vacuous — re-point it",
			checked, rootManifestReaderMinSites)
	}
	if len(offenders) > 0 {
		sort.Strings(offenders)
		t.Errorf("%d UNCLASSIFIED read(s) of the chain-root manifest by convention:\n  %s\n\n"+
			"`manifest.json` at the lineage root is segment 0's full ONLY while segment 0 is catalogued; "+
			"after `backup prune`/`backup compact` retires that segment the file is kept as the chain's "+
			"IDENTITY (its CEK wrap + Argon2id salt) and is no longer a link. Reading it as one restores "+
			"data the chain does not hold — that is audit B-6, which shipped because the comment claiming "+
			"this was safe enumerated one reader and there were two.\n"+
			"Add an entry to rootManifestReaderRoster saying IDENTITY (key material / header only) or "+
			"DISPATCHED (guarded by verifyLineageNeedsWalk), with the reason.",
			len(offenders), strings.Join(offenders, "\n  "))
	}
	// The roster must not accumulate entries for sites that no longer exist:
	// a stale exemption is how a gate quietly stops covering what it names.
	var stale []string
	for key := range rootManifestReaderRoster {
		if !seen[key] {
			stale = append(stale, key)
		}
	}
	if len(stale) > 0 {
		sort.Strings(stale)
		t.Errorf("rootManifestReaderRoster has %d entry/entries for call site(s) that no longer exist "+
			"(renamed or removed): %s — drop them so the roster keeps describing the code",
			len(stale), strings.Join(stale, ", "))
	}
}

// funcLabel renders a declaration as `Receiver.Method` (or `Func`), so two
// same-named methods in one file are distinct roster keys.
func funcLabel(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	typ := fn.Recv.List[0].Type
	if star, ok := typ.(*ast.StarExpr); ok {
		typ = star.X
	}
	if id, ok := typ.(*ast.Ident); ok {
		return id.Name + "." + fn.Name.Name
	}
	return fn.Name.Name
}

// isRootManifestReaderCall reports whether call is `lineage.ReadX(...)` (from
// outside package lineage) or a bare `ReadX(...)` (inside it), for the three
// by-convention readers.
func isRootManifestReaderCall(call *ast.CallExpr) bool {
	switch fn := call.Fun.(type) {
	case *ast.SelectorExpr:
		pkg, ok := fn.X.(*ast.Ident)
		return ok && pkg.Name == "lineage" && rootManifestReaders[fn.Sel.Name]
	case *ast.Ident:
		return rootManifestReaders[fn.Name]
	default:
		return false
	}
}
