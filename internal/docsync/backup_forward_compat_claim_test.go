// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package docsync

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// "Newer sluice always reads older" is a COMPATIBILITY claim about backup
// chains, it was written in four places, and it is false.
//
// # The verdict, and the evidence
//
// A newer binary refuses an OLDER chain whenever the two sit on opposite
// sides of a schema-fingerprint epoch. `verifySchemaHashes` recomputes each
// link's recorded `SchemaHash` with the CURRENT build's `ComputeSchemaHash`
// — a SHA-256 over the marshalled IR structs that keys off no recorded
// algorithm version — so an untagged field addition to `ir.Index` or
// `ir.Column` re-fingerprints schemas that never changed. Two such
// boundaries shipped INSIDE format version 8 (v0.100.0, v0.103.0; roadmap
// item 102), where the format-version gate structurally cannot see them,
// and a third at v0.104.3 (item 104 / Bug 216). The chain is byte-perfect
// and unreadable, and the operator finds out at restore time.
//
// # Why this gate and not a golden hash
//
// Item 104 already shipped the golden-hash answer to this question, and it
// could not work: its fixture carried the new field at its new value, so it
// proved the current binary agrees with itself. The project's own rule is
// that a compatibility gate's evidence must be behavioural and
// cross-version, and that evidence EXISTS —
// `TestBackup_CrossVersionChainCompat` cells 5 and 6, real binaries built
// from real tags. What was missing is not evidence; it is anything binding
// the PROSE to it, which is why the same wrong sentence survived in four
// places while the test that disproves it ran green next door.
//
// So this gate does the binding, in the `TestFilteredSyncEngineListMatchesTheCode`
// shape: derive the caveat set from the CODE, hold every doc site's marker
// to it. A new manifest-integrity preflight that can refuse an older
// manifest fails the build until someone classifies it and the four
// sentences are updated together.
//
// # Scope — which paths this reaches, and which it does not
//
// It derives from `restoreManifestIntegrityPreflights`, the function whose
// own doc calls itself "the single definition of that LIST" for restore AND
// `backup verify`. That is deliberate and it is NOT the complete set of
// things a newer binary can refuse an older chain on. The two neighbours
// outside it, named here rather than left implied:
//
//   - `verifyChainSignatures` (ADR-0154) — dual-version BY CONSTRUCTION: it
//     recomputes at the signature's own recorded `CanonVersion`, and a
//     signature recording a version newer than this build refuses with an
//     "upgrade sluice" message rather than a false INVALID. Forward-safe for
//     the same reason the format version is.
//   - the chain ENCRYPTION preflight (ADR-0152 / ADR-0181) — likewise keyed
//     on the manifest's recorded FormatVersion, which is the whole point of
//     versions 5, 7 and 9.
//
// Both are forward-safe because they key on something the ARTIFACT records.
// That is the discriminator this gate's roster classifies on, and the
// schema fingerprint is the one member of the family that records nothing.
func TestNewerReadsOlderIsQualifiedByThePartitioningPreflights(t *testing.T) {
	preflights := manifestIntegrityPreflightCallees(t)

	// Anti-vacuity: a walker that found nothing would bless any doc text.
	if len(preflights) < 3 {
		t.Fatalf("derived only %d preflight(s) from restoreManifestIntegrityPreflights (%s) — the function calls at "+
			"least three (refuse-interrupted, schema hashes, backup ids). The AST walk is broken, and a green from a "+
			"broken walk is exactly the vacuous pass this gate exists to prevent",
			len(preflights), strings.Join(preflights, ", "))
	}

	var caveats []string
	for _, name := range preflights {
		entry, ok := manifestPreflightForwardCompat[name]
		if !ok {
			t.Errorf("restoreManifestIntegrityPreflights calls %s, and nothing classifies it for FORWARD compatibility.\n"+
				"  Ask one question: can this check refuse a manifest an OLDER sluice wrote and accepted?\n"+
				"  - NO, because it keys on something the ARTIFACT RECORDS (a format version, a canon version, a "+
				"recorded literal) — add it as forwardSafe with that reason.\n"+
				"  - YES, because it RECOMPUTES a value with this build's algorithm and no recorded version to key on "+
				"— add it with a caveat id, and add that id to the marker at every doc site listed in "+
				"forwardCompatClaimSites. Those sentences are a compatibility promise to operators; one of them going "+
				"stale is how an operator learns at restore time.", name)
			continue
		}
		if strings.TrimSpace(entry.why) == "" {
			t.Errorf("%s is classified with an empty reason — the reason is the whole gate here", name)
		}
		if entry.caveat == "" {
			continue
		}
		if strings.TrimSpace(entry.pinnedBy) == "" {
			t.Errorf("%s is classified as PARTITIONING older chains but names no behavioural pin. A partition claim "+
				"without a cross-version test behind it is the item-104 shape (a gate that compares the current "+
				"binary against itself) — name the test", name)
		}
		caveats = append(caveats, entry.caveat)
	}
	sort.Strings(caveats)

	if len(caveats) == 0 {
		t.Fatalf("no manifest-integrity preflight is classified as partitioning older chains, so this gate is asserting "+
			"that \"newer sluice always reads older\" holds unconditionally. It does not: crossversion cell 5 pins a "+
			"newer binary refusing an older chain over the schema fingerprint. If that has genuinely been closed "+
			"(roadmap item 102's compatibility shim), update cell 5 and the four doc sites FIRST, then delete this "+
			"gate deliberately — do not delete this check. Preflights seen: %s", strings.Join(preflights, ", "))
	}

	for name := range manifestPreflightForwardCompat {
		if !containsString(preflights, name) {
			t.Errorf("the forward-compat classification lists %q, which restoreManifestIntegrityPreflights no longer "+
				"calls. A stale entry silently keeps a caveat alive (or hides one) — remove it", name)
		}
	}

	// The binding half: every site that makes the newer-reads-older claim
	// carries the derived caveat list, and none of them still carries the
	// unqualified sentence.
	for _, site := range forwardCompatClaimSites {
		raw, err := os.ReadFile(filepath.Join("..", "..", filepath.FromSlash(site)))
		if err != nil {
			t.Errorf("read %s: %v (if the file moved, move the claim's marker with it)", site, err)
			continue
		}
		text := string(raw)

		m := forwardCompatMarkerRe.FindStringSubmatch(text)
		if m == nil {
			t.Errorf("%s makes the newer-reads-older claim but carries no `forward-compat-caveats: …` marker. The "+
				"marker is the checkable form of the qualification; add it listing: %s",
				site, strings.Join(caveats, ", "))
			continue
		}
		fromDoc := splitList(m[1])
		if !equalStringSets(caveats, fromDoc) {
			t.Errorf("%s's forward-compat caveat list disagrees with the code.\n"+
				"  code (preflights that can refuse an older manifest): %s\n"+
				"  doc  (marker):                                       %s\n\n"+
				"Update the marker AND the sentence beside it. A caveat that exists in the code and not in the prose "+
				"is a compatibility promise sluice does not keep.",
				site, strings.Join(caveats, ", "), strings.Join(fromDoc, ", "))
		}
		if loc := unqualifiedClaimRe.FindString(flattenProse(text)); loc != "" {
			t.Errorf("%s still says %q. The unqualified form is the false one — it is what four separate sites said "+
				"while crossversion cell 5 proved the opposite next door. Say what is actually guaranteed (the "+
				"per-link, artifact-recorded-version rule) and name the caveat.", site, loc)
		}
	}
}

// manifestPreflightForwardCompatEntry classifies one manifest-integrity
// preflight by whether a NEWER binary running it can refuse a manifest an
// OLDER binary wrote and accepted.
//
// The discriminator is one question: does the check key on something the
// ARTIFACT RECORDS? A recorded literal or a recorded version travels with
// the bytes and cannot drift. A recompute with this build's algorithm and
// no recorded version to key on partitions the corpus at every release that
// moves the algorithm.
type manifestPreflightForwardCompatEntry struct {
	// caveat is the marker id this preflight contributes to every doc site,
	// empty when the preflight is forward-safe.
	caveat   string
	why      string
	pinnedBy string
}

// manifestPreflightForwardCompat is fail-by-default over the callees of
// restoreManifestIntegrityPreflights.
var manifestPreflightForwardCompat = map[string]manifestPreflightForwardCompatEntry{
	"refuseInterruptedManifests": {
		why: "FORWARD-SAFE: compares the RECORDED PartialState literal against a constant and recomputes nothing, so " +
			"no algorithm drift can turn an older-written manifest into a refusal. The one way it could have — " +
			"treating an EMPTY PartialState as in-progress, which every pre-Phase-1 manifest carries — is carved out " +
			"explicitly at refuseInterruptedManifest, and that carve-out is why it cannot start refusing old backups.",
	},
	"verifySchemaHashes": {
		caveat: "schema-fingerprint-epoch",
		why: "PARTITIONS OLDER CHAINS. ComputeSchemaHash is a SHA-256 over the marshalled IR structs and takes only " +
			"the schema — there is no recorded algorithm version for it to key on, and no re-derivation at the " +
			"writing release's field set (roadmap item 102, deliberately demand-gated). So an untagged field " +
			"addition to ir.Index/ir.Column re-fingerprints schemas that never changed and this build refuses a " +
			"byte-perfect older chain. It runs on the CHAIN path only (needsWalk), which is why a bare full legitimately " +
			"crosses an epoch and a chain does not.",
		pinnedBy: "TestBackup_CrossVersionChainCompat cell 5 (an epoch-crossing chain is refused, with the honest " +
			"two-cause wording and an untouched target) and cell 6 (a chain THIS build writes still restores on the " +
			"newest release at the same format version). Both run real binaries built from real tags — the " +
			"behavioural, cross-version form item 104 proved a self-referential golden hash cannot substitute for.",
	},
	"verifyBackupIDs": {
		why: "FORWARD-SAFE, by version-keying rather than by luck: ComputeBackupID gates its extra field on the " +
			"manifest's OWN recorded FormatVersion, so a pre-v8 segment recomputes to its legacy id and a v8 segment " +
			"to the folded one, and a mixed-version chain stays coherent. RESIDUAL, stated because it is the way this " +
			"entry would go stale: that holds for the fields folded SO FAR. A future fold that does not gate on the " +
			"recorded version moves this into the partitioning column, and the marker set has to grow with it.",
	},
}

// forwardCompatClaimSites are the files that make the newer-reads-older
// claim in prose and are therefore held to the derived caveat set.
//
// Deliberately NOT listed, with reasons, because a sibling sweep that
// silently drops members is the failure mode this whole exercise is about:
//
//   - `internal/ir/backup/signature.go` names the phrase to identify the
//     per-link dual-version rule the signature verifier implements. In that
//     scope the claim is TRUE and the caveat is irrelevant — the verifier
//     recomputes at the signature's own recorded CanonVersion.
//   - `docs/adr/adr-0154-*.md` says the same thing in the same scope.
//   - CHANGELOG.md and docs/releases/* are PUBLISHED, immutable records of
//     what a release said at the time. sluice does not rewrite shipped
//     release notes; the correction lives at the live sites.
var forwardCompatClaimSites = []string{
	"internal/ir/backup/manifest.go",
	"docs/dev/design/logical-backups.md",
	"internal/pipeline/backup_crossversion_integration_test.go",
}

var (
	forwardCompatMarkerRe = regexp.MustCompile(`forward-compat-caveats:\s*([^\n>]*?)\s*(?:-->|\n|$)`)

	// unqualifiedClaimRe is the exact false form, in the spellings the sites
	// used. It matches SUBJECT + phrase rather than the bare phrase, and
	// that is a named wart: every correction below has to be able to quote
	// the sentence it is correcting, or the record of the defect disappears
	// with the defect. So the corrections elide the subject — *"…always
	// reads older"* — and this regex is what makes that elision meaningful
	// rather than incidental. Restating the full false sentence at a live
	// site fails, quoting it in a correction does not.
	unqualifiedClaimRe = regexp.MustCompile(`(?i)(newer|new) sluice always reads older`)

	// proseNoiseRe strips line-leading comment and blockquote markers so a
	// claim WRAPPED across two comment lines is still one string to match.
	// Without it the check would pass on a hard-wrapped occurrence, which is
	// the difference between a gate and a coincidence — the first draft of
	// this file passed exactly that way.
	proseNoiseRe = regexp.MustCompile(`(?m)^[\t ]*(?://+|>)[\t ]*`)
	whitespaceRe = regexp.MustCompile(`\s+`)
)

// flattenProse renders a Go or markdown file as one whitespace-normalized
// line, so line wrapping cannot hide a phrase from a regex.
func flattenProse(text string) string {
	return whitespaceRe.ReplaceAllString(proseNoiseRe.ReplaceAllString(text, " "), " ")
}

// manifestIntegrityPreflightCallees returns the package-level functions
// `restoreManifestIntegrityPreflights` calls — the shared restore/verify
// preflight LIST, derived rather than copied.
func manifestIntegrityPreflightCallees(t *testing.T) []string {
	t.Helper()
	path := filepath.Join("..", "..", "internal", "pipeline", "backup", "chain_restore.go")
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || fn.Name.Name != "restoreManifestIntegrityPreflights" {
			continue
		}
		seen := map[string]bool{}
		var out []string
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, isCall := n.(*ast.CallExpr)
			if !isCall {
				return true
			}
			id, isIdent := call.Fun.(*ast.Ident)
			if !isIdent || seen[id.Name] {
				return true
			}
			seen[id.Name] = true
			out = append(out, id.Name)
			return true
		})
		sort.Strings(out)
		return out
	}
	t.Fatalf("restoreManifestIntegrityPreflights not found in %s — it is the single definition of the preflight list "+
		"and this gate derives from it; if it was renamed or moved, repoint the walk", path)
	return nil
}
