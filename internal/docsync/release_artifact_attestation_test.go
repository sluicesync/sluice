// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Every artifact a release publishes is attested, or is named here with the
// reason it is not (audit 2026-08-05, C-12).
//
// The finding was not a missing feature — provenance shipped in v0.110.1 and
// works. It was the SENTENCE above the step, which said the attestation
// covered "everything a consumer downloads". That was true of the thirteen
// files attached to the GitHub release and false of the one artifact people
// run in production: `gh attestation verify oci://ghcr.io/sluicesync/sluice:X`
// returned HTTP 404 while the same release's tarball verified green. A
// narrowed security claim is worse than an absent one, because it is exactly
// what stops the next reader from checking.
//
// So the durable half is not "the image is attested now" — it is that the
// ROSTER cannot go quietly out of date. Every top-level section of
// .goreleaser.yaml must be classified here as either producing a published
// artifact (with how it is attested) or producing none (with why), and every
// classification that claims attestation must name a string that really
// appears in .github/workflows/release.yml. Add `sboms:`, a third `dockers:`
// entry, or an `nfpms` format, and this fails until someone decides.
//
// WHAT THIS GATE REACHES, stated because a gate whose coverage is narrower
// than its name is the same defect one level up:
//   - It grades the goreleaser CONFIG's publishing surfaces against the
//     workflow's attestation steps. It is a coverage roster, not a proof that
//     any particular release was attested — only a real `gh attestation
//     verify` against a published artifact is that.
//   - Artifacts that do not originate in .goreleaser.yaml cannot be derived
//     from it. GitHub's auto-generated "Source code (zip/tar.gz)" is the one
//     that exists; it is carried as an explicit roster entry below rather
//     than left implied, and it is the reason the roster is a superset of the
//     config's keys rather than exactly them.

package docsync

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// attestVerdict is what happens to the artifacts a goreleaser section
// produces.
type attestVerdict string

const (
	// attestedByPath: the files are hashed out of dist/ by a `subject-path`
	// glob in release.yml.
	attestedByPath attestVerdict = "attested-by-path"
	// attestedByDigest: an OCI subject, attested by `subject-digest` against
	// the digest the push itself reported.
	attestedByDigest attestVerdict = "attested-by-digest"
	// publishedUnattested: really published, deliberately not attested. Needs
	// a reason that survives being read out loud to someone verifying a
	// supply chain.
	publishedUnattested attestVerdict = "published-unattested"
	// notPublished: the section produces something, but this workflow never
	// puts it anywhere a consumer can fetch.
	notPublished attestVerdict = "not-published"
	// producesNoArtifact: configuration, not output.
	producesNoArtifact attestVerdict = "produces-no-artifact"
)

// releaseArtifactRoster records, for every publishing surface a release has,
// what happens to its provenance. Keys are .goreleaser.yaml's top-level
// sections, plus the artifacts that come from outside that file entirely.
var releaseArtifactRoster = map[string]struct {
	Verdict attestVerdict
	// Proof strings must all appear literally in release.yml. For an attested
	// entry this is what binds the roster to the mechanism; for an entry that
	// is NOT attested it is the roster comment that admits the gap, so
	// deleting that comment fails this test rather than silently restoring
	// the narrowed claim C-12 was filed about.
	Proof []string
	Why   string
	// DocLabel is how this surface is named in SECURITY.md's two markers.
	// Required on everything that produces a published artifact and empty on
	// everything that does not, so the operator-facing roster is derived from
	// this map rather than maintained beside it.
	DocLabel string
	// FromConfig is false for artifacts that .goreleaser.yaml does not
	// describe, so the config-key sweep below does not expect to find them.
	FromConfig bool
}{
	"archives": {
		Verdict:    attestedByPath,
		Proof:      []string{"dist/*.tar.gz", "dist/*.zip"},
		Why:        "the six platform archives; the primary download",
		DocLabel:   "archives (.tar.gz/.zip)",
		FromConfig: true,
	},
	"nfpms": {
		Verdict:    attestedByPath,
		Proof:      []string{"dist/*.deb", "dist/*.rpm", "dist/*.apk"},
		Why:        "native Linux packages attached to the release",
		DocLabel:   "native packages (.deb/.rpm/.apk)",
		FromConfig: true,
	},
	"checksum": {
		Verdict:    attestedByPath,
		Proof:      []string{"dist/checksums.txt"},
		Why:        "authenticates the binaries; before v0.110.1 nothing authenticated it",
		DocLabel:   "checksums.txt",
		FromConfig: true,
	},
	"dockers": {
		Verdict: attestedByDigest,
		Proof: []string{
			"subject-name: ghcr.io/sluicesync/sluice",
			"steps.image.outputs.amd64",
			"steps.image.outputs.arm64",
		},
		Why: "the per-arch GHCR manifests; :<ver>-amd64 and :<ver>-arm64 are published, " +
			"pullable tags with digests of their own, which an attestation on the index does not cover",
		DocLabel:   "GHCR image per-arch manifests",
		FromConfig: true,
	},
	"docker_manifests": {
		Verdict: attestedByDigest,
		Proof: []string{
			"subject-name: ghcr.io/sluicesync/sluice",
			"steps.image.outputs.index",
		},
		Why: "the multi-arch OCI index behind :<ver> and :latest — the digest `gh attestation " +
			"verify oci://…` resolves a tag to, so this is the one an operator's `docker pull` reaches",
		DocLabel:   "GHCR image multi-arch index",
		FromConfig: true,
	},
	"brews": {
		Verdict:    publishedUnattested,
		Proof:      []string{"homebrew formula, scoop manifest"},
		Why:        "a git commit in a sibling repo, not a build output: a URL plus the sha256 of an archive that IS attested, so verification routes through the archive",
		DocLabel:   "homebrew formula",
		FromConfig: true,
	},
	"scoops": {
		Verdict:    publishedUnattested,
		Proof:      []string{"homebrew formula, scoop manifest"},
		Why:        "same shape as brews — a manifest committed to sluicesync/scoop-bucket referencing an attested archive by sha256",
		DocLabel:   "scoop manifest",
		FromConfig: true,
	},
	"winget": {
		Verdict:    notPublished,
		Proof:      []string{`winget manifest — `},
		Why:        `skip_upload: "true" — generated into dist/winget/ and never pushed by this workflow; submitted by hand`,
		DocLabel:   "winget manifest",
		FromConfig: true,
	},
	"github-source-archives": {
		Verdict:    publishedUnattested,
		Proof:      []string{`GitHub's auto "Source code (zip/tar.gz)"`},
		Why:        "produced by GitHub from the tag, never present in dist/, so the job has nothing to hash; the integrity control is the commit SHA the tag names",
		DocLabel:   "GitHub source archives",
		FromConfig: false,
	},
	"builds": {
		Verdict:    producesNoArtifact,
		Why:        "raw binaries never ship alone — every one of them reaches a consumer inside an archive, a package or the image, all of which are attested",
		FromConfig: true,
	},
	"version":      {Verdict: producesNoArtifact, Why: "config schema version", FromConfig: true},
	"project_name": {Verdict: producesNoArtifact, Why: "naming only", FromConfig: true},
	"before":       {Verdict: producesNoArtifact, Why: "pre-build hooks (go mod tidy, go test)", FromConfig: true},
	"snapshot":     {Verdict: producesNoArtifact, Why: "affects untagged local builds only; never runs on a release tag", FromConfig: true},
	"changelog":    {Verdict: producesNoArtifact, Why: "release-note text, not an artifact", FromConfig: true},
	"release":      {Verdict: producesNoArtifact, Why: "the GitHub release object itself (draft flag, header); its assets come from the producing sections above", FromConfig: true},
}

// goreleaserConfig is the slice of .goreleaser.yaml this gate reasons about.
type goreleaserConfig struct {
	Archives []struct {
		Formats         []string `yaml:"formats"`
		FormatOverrides []struct {
			Formats []string `yaml:"formats"`
		} `yaml:"format_overrides"`
	} `yaml:"archives"`
	NFPMs []struct {
		Formats []string `yaml:"formats"`
	} `yaml:"nfpms"`
	Dockers []struct {
		ID             string   `yaml:"id"`
		ImageTemplates []string `yaml:"image_templates"`
	} `yaml:"dockers"`
	DockerManifests []struct {
		NameTemplate string `yaml:"name_template"`
	} `yaml:"docker_manifests"`
}

func TestEveryPublishedReleaseArtifactIsAttestedOrExempt(t *testing.T) {
	root := repoRootFromDocsync(t)
	cfgBytes := mustReadRepoFile(t, root, ".goreleaser.yaml")
	workflow := string(mustReadRepoFile(t, root, filepath.Join(".github", "workflows", "release.yml")))

	// Anti-vacuity: a roster that lost its rows, or a config that failed to
	// parse into an empty map, would satisfy every assertion below.
	if len(releaseArtifactRoster) < 10 {
		t.Fatalf("releaseArtifactRoster has %d entries; it is supposed to enumerate every top-level "+
			"section of .goreleaser.yaml plus the artifacts that come from outside it", len(releaseArtifactRoster))
	}

	// (1) Fail by default: every top-level key of .goreleaser.yaml must be
	// classified. This is the half that catches `sboms:` being added.
	var top map[string]yaml.Node
	if err := yaml.Unmarshal(cfgBytes, &top); err != nil {
		t.Fatalf("parse .goreleaser.yaml: %v", err)
	}
	if len(top) < 8 {
		t.Fatalf("parsed only %d top-level keys out of .goreleaser.yaml; the parse is wrong, not the config", len(top))
	}
	for key := range top {
		if _, ok := releaseArtifactRoster[key]; !ok {
			t.Errorf(".goreleaser.yaml has a top-level section %q that releaseArtifactRoster does not "+
				"classify.\n\nA new publishing surface is UNATTESTED until someone decides otherwise. Add it "+
				"with a verdict — and if the verdict is that it ships unattested, say so in release.yml's "+
				"roster comment too, because the claim there is what an operator reads.", key)
		}
	}

	// (2) Every roster entry carries a reason, and every claim of coverage
	// names a string that really appears in the workflow.
	attestedCount, gapCount := 0, 0
	for _, name := range sortedRosterNames() {
		entry := releaseArtifactRoster[name]
		if strings.TrimSpace(entry.Why) == "" {
			t.Errorf("roster entry %q carries no reason", name)
		}
		switch entry.Verdict {
		case attestedByPath, attestedByDigest:
			attestedCount++
		case publishedUnattested, notPublished:
			gapCount++
		case producesNoArtifact:
		default:
			t.Errorf("roster entry %q has unknown verdict %q", name, entry.Verdict)
			continue
		}
		if entry.Verdict == producesNoArtifact {
			if len(entry.Proof) != 0 {
				t.Errorf("roster entry %q produces no artifact but names proof strings", name)
			}
			continue
		}
		if len(entry.Proof) == 0 {
			t.Errorf("roster entry %q is %q but names nothing in release.yml — the row is a promise", name, entry.Verdict)
			continue
		}
		for _, proof := range entry.Proof {
			if !strings.Contains(workflow, proof) {
				t.Errorf("roster entry %q (%s) cites %q, which does not appear in "+
					".github/workflows/release.yml.\n\nEither the attestation moved and the roster is stale, or "+
					"the roster comment that admits this gap was deleted. A stale row here reads as coverage, "+
					"which is the C-12 defect exactly.", name, entry.Verdict, proof)
			}
		}
	}
	if attestedCount < 4 {
		t.Errorf("only %d roster entries claim attestation; archives, nfpms, checksum, dockers and "+
			"docker_manifests are all attested surfaces", attestedCount)
	}
	if gapCount < 3 {
		t.Errorf("only %d roster entries record a gap; a roster that cannot express 'published and NOT "+
			"attested' is not grading anything", gapCount)
	}

	// (3) The file-asset half: every archive and package FORMAT the config
	// actually emits must have a matching subject-path glob. Adding
	// `formats: [archlinux]` to nfpms fails here.
	var cfg goreleaserConfig
	if err := yaml.Unmarshal(cfgBytes, &cfg); err != nil {
		t.Fatalf("parse .goreleaser.yaml into goreleaserConfig: %v", err)
	}
	for _, format := range emittedFileFormats(t, cfg) {
		glob := "dist/*." + format
		if !strings.Contains(workflow, glob) {
			t.Errorf(".goreleaser.yaml emits %q files but release.yml has no %q subject-path.\n\n"+
				"Those files are attached to the GitHub release and would ship unattested.", format, glob)
		}
	}

	// (4) The image half — the one C-12 was actually about. Every distinct
	// digest a release pushes to GHCR must be attested, addressed by the tag
	// goreleaser publishes it under.
	assertEveryPublishedImageDigestIsAttested(t, cfg, workflow)
}

// assertEveryPublishedImageDigestIsAttested derives, from the goreleaser
// config, the set of image references whose digests a release publishes, and
// requires release.yml to resolve and attest each one.
//
// Why the version-tagged template is the addressing key: goreleaser pushes one
// built image under every name in a `dockers[]` entry, so all of that entry's
// tags share a digest — verified against goreleaser v2.16.0's own artifact
// metadata, where `:0.1.0-amd64` and `:latest-amd64` both reported
// sha256:d6b40c70…, and the two `docker_manifests` entries (`:<ver>` and
// `:latest`) both reported sha256:855d1fbe…, a manifest list not containing
// its own tag. One attested digest per entry therefore covers that entry's
// aliases; the ALIAS-SHARES-A-DIGEST premise is asserted structurally below
// rather than assumed.
func assertEveryPublishedImageDigestIsAttested(t *testing.T, cfg goreleaserConfig, workflow string) {
	t.Helper()

	if len(cfg.Dockers) < 2 {
		t.Fatalf("parsed %d `dockers` entries; the release builds linux/amd64 and linux/arm64, so a "+
			"count below 2 means the parse broke and every assertion below is vacuous", len(cfg.Dockers))
	}
	if len(cfg.DockerManifests) == 0 {
		t.Fatal("parsed no `docker_manifests` entries; the multi-arch index is what `docker pull` resolves")
	}

	const versionToken = "{{ .Version }}"

	// The image repository must be one name, and the workflow must use that
	// exact name — a rename in .goreleaser.yaml that release.yml did not
	// follow is precisely how the image goes silently unattested again.
	repos := map[string]bool{}
	collect := func(ref string) {
		if i := strings.LastIndex(ref, ":"); i > 0 {
			repos[ref[:i]] = true
		}
	}
	for _, d := range cfg.Dockers {
		for _, tmpl := range d.ImageTemplates {
			collect(tmpl)
		}
	}
	for _, m := range cfg.DockerManifests {
		collect(m.NameTemplate)
	}
	if len(repos) != 1 {
		t.Fatalf("expected one image repository across dockers + docker_manifests, got %v", sortedSet(repos))
	}
	repo := sortedSet(repos)[0]
	for _, want := range []string{"image=" + repo, "subject-name: " + repo} {
		if !strings.Contains(workflow, want) {
			t.Errorf(".goreleaser.yaml publishes to %q but release.yml does not contain %q.\n\n"+
				"The attestation would be minted for a different name than the one that was pushed.", repo, want)
		}
	}

	// Every published digest, addressed by its version-tagged reference.
	type wanted struct {
		artifactType string // goreleaser's artifacts.json `type`
		suffix       string // what follows {{ .Version }} in the tag
	}
	var want []wanted
	for _, m := range cfg.DockerManifests {
		if !strings.Contains(m.NameTemplate, versionToken) {
			continue // the `:latest` alias; same index bytes, same digest
		}
		want = append(want, wanted{"Docker Manifest", tagSuffixAfterVersion(m.NameTemplate, versionToken)})
	}
	for _, d := range cfg.Dockers {
		var versioned []string
		for _, tmpl := range d.ImageTemplates {
			if strings.Contains(tmpl, versionToken) {
				versioned = append(versioned, tmpl)
			}
		}
		// The structural half of the alias premise: an entry whose tags are
		// all aliases has no stable address to attest, and an entry with two
		// version-tagged names is two addresses wearing one id.
		if len(versioned) != 1 {
			t.Errorf("`dockers` entry %q has %d version-tagged image_templates (%v); exactly one is "+
				"needed as the address the attestation is minted against", d.ID, len(versioned), versioned)
			continue
		}
		want = append(want, wanted{"Published Docker Image", tagSuffixAfterVersion(versioned[0], versionToken)})
	}
	if len(want) < 3 {
		t.Fatalf("derived only %d image digests to attest; the release publishes an index plus one "+
			"manifest per architecture", len(want))
	}

	for _, w := range want {
		// The resolve step addresses each digest by artifact type AND tag, so
		// the pair is what must appear. Matching only the type would pass
		// while the amd64 line was attesting the arm64 tag.
		line := `digest_of '` + w.artifactType + `' "$image:$version` + w.suffix + `"`
		if !strings.Contains(workflow, line) {
			t.Errorf("release.yml never resolves the digest for `%s:<ver>%s` (%s).\n\nExpected the line:\n"+
				"  %s\n\nThat image tag is pushed to the registry on every release; without this it ships "+
				"unattested and `gh attestation verify oci://%s:<ver>%s` returns 404 — the C-12 defect.",
				repo, w.suffix, w.artifactType, line, repo, w.suffix)
		}
	}

	// Every resolved digest must actually reach an attestation step. A
	// resolve step whose outputs nothing consumes is the shape that looks
	// like coverage and is not.
	for _, out := range []string{"index", "amd64", "arm64"} {
		ref := "subject-digest: ${{ steps.image.outputs." + out + " }}"
		if !strings.Contains(workflow, ref) {
			t.Errorf("release.yml resolves the %q image digest but never passes it to an attestation "+
				"step (expected %q)", out, ref)
		}
	}
}

// TestSecurityMDAttestationRosterMatchesTheCode holds SECURITY.md's two
// operator-facing markers to releaseArtifactRoster.
//
// The workflow comment is where a maintainer reads the coverage; SECURITY.md
// is where someone deciding whether to trust a download reads it, and that is
// the audience the C-12 wording actually misled. Prose stays prose — the
// markers are what fail, so a new publishing surface cannot be attested (or
// knowingly left unattested) without the security doc saying which.
func TestSecurityMDAttestationRosterMatchesTheCode(t *testing.T) {
	root := repoRootFromDocsync(t)
	security := string(mustReadRepoFile(t, root, "SECURITY.md"))

	var attested, unattested []string
	for _, name := range sortedRosterNames() {
		entry := releaseArtifactRoster[name]
		switch entry.Verdict {
		case attestedByPath, attestedByDigest:
			attested = append(attested, entry.DocLabel)
		case publishedUnattested, notPublished:
			unattested = append(unattested, entry.DocLabel)
		case producesNoArtifact:
			if entry.DocLabel != "" {
				t.Errorf("roster entry %q produces no artifact but carries a DocLabel %q; SECURITY.md's "+
					"roster is about what ships, and listing a config section there is noise", name, entry.DocLabel)
			}
			continue
		}
		if entry.DocLabel == "" {
			t.Errorf("roster entry %q publishes an artifact but has no DocLabel, so it cannot appear in "+
				"SECURITY.md's roster — which is how a surface goes unmentioned to the people verifying it", name)
		}
	}
	sort.Strings(attested)
	sort.Strings(unattested)

	for _, m := range []struct {
		marker string
		want   []string
	}{
		{"attested-release-artifacts", attested},
		{"unattested-release-artifacts", unattested},
	} {
		if len(m.want) == 0 {
			t.Fatalf("derived an empty %q list from the roster", m.marker)
		}
		want := "<!-- " + m.marker + ": " + strings.Join(m.want, ", ") + " -->"
		if !strings.Contains(security, want) {
			t.Errorf("SECURITY.md's %q marker disagrees with releaseArtifactRoster.\n\nexpected line:\n  %s\n\n"+
				"Update SECURITY.md (and the prose around it) so the roster an operator reads matches the "+
				"one the release workflow implements.", m.marker, want)
		}
	}
}

// TestReleaseWorkflowAttestationStepsAreWellFormed parses release.yml as YAML
// and grades the attestation steps structurally.
//
// The roster gate above matches substrings, which cannot tell a well-formed
// step from a comment that happens to contain the same text — and nothing in
// this repo lints workflow YAML at all (there is no actionlint job), so a
// malformed `with:` block or a stray indent in release.yml surfaces for the
// first time when a tag is pushed. That is the most expensive moment to find
// it: the binaries and the image have already published by the time these
// steps run.
func TestReleaseWorkflowAttestationStepsAreWellFormed(t *testing.T) {
	root := repoRootFromDocsync(t)
	raw := mustReadRepoFile(t, root, filepath.Join(".github", "workflows", "release.yml"))

	var wf struct {
		Jobs map[string]struct {
			Steps []struct {
				Name string            `yaml:"name"`
				ID   string            `yaml:"id"`
				Uses string            `yaml:"uses"`
				Run  string            `yaml:"run"`
				With map[string]string `yaml:"with"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &wf); err != nil {
		t.Fatalf("release.yml is not valid YAML: %v", err)
	}
	job, ok := wf.Jobs["goreleaser"]
	if !ok {
		t.Fatalf("release.yml has no `goreleaser` job; jobs are %v", wf.Jobs)
	}
	if len(job.Steps) < 6 {
		t.Fatalf("parsed %d steps out of the goreleaser job; the parse is wrong, not the workflow", len(job.Steps))
	}

	attestSteps, sawResolver := 0, false
	for _, step := range job.Steps {
		if step.ID == "image" {
			sawResolver = true
			// The trailing paren is load-bearing and was found by a mutation
			// run: `dist/artifacts.json` alone also appears in this step's
			// ERROR MESSAGE, so pointing jq at a different file left the
			// naive assertion green. `dist/artifacts.json)` is the closing of
			// the jq invocation — the file actually being read.
			if !strings.Contains(step.Run, "dist/artifacts.json)") {
				t.Errorf("the `image` step does not pass dist/artifacts.json to jq; the digests must come "+
					"from GoReleaser's own metadata. Got:\n%s", step.Run)
			}
			// Re-resolving the tag from the registry would attest whatever it
			// points at NOW, not what this run pushed — a different question,
			// and one an attacker with registry write can answer for you.
			for _, forbidden := range []string{"imagetools inspect", "docker manifest inspect", "crane digest"} {
				if strings.Contains(step.Run, forbidden) {
					t.Errorf("the `image` step calls %q — the digest must be the one the PUSH reported, "+
						"not one re-read from the registry afterwards", forbidden)
				}
			}
		}
		if !strings.Contains(step.Uses, "actions/attest-build-provenance@") {
			continue
		}
		attestSteps++
		_, byPath := step.With["subject-path"]
		name, hasName := step.With["subject-name"]
		digest, hasDigest := step.With["subject-digest"]
		switch {
		case byPath && !hasDigest:
			// File subjects: hashed straight out of dist/.
		case hasName && hasDigest:
			if strings.TrimSpace(name) == "" || strings.TrimSpace(digest) == "" {
				t.Errorf("attestation step %q has an empty subject-name/subject-digest", step.Name)
			}
		default:
			t.Errorf("attestation step %q has neither a `subject-path` nor a complete "+
				"`subject-name` + `subject-digest` pair (with: %v).\n\nAn OCI subject missing its digest "+
				"attests nothing, and the step can still succeed.", step.Name, step.With)
		}
	}
	if attestSteps < 4 {
		t.Errorf("found %d attest-build-provenance steps; the release attests its files plus three image "+
			"digests (the multi-arch index and one manifest per architecture)", attestSteps)
	}
	if !sawResolver {
		t.Error("release.yml has no step with `id: image`; nothing resolves the digests the image " +
			"attestation steps consume")
	}
}

// emittedFileFormats returns every file extension the release attaches to the
// GitHub release, derived from the config rather than listed by hand.
//
// The archives default is goreleaser's, not an assumption: an `archives`
// entry with no `formats` produces tar.gz. Ground-truthed against a real
// release — v0.116.1 published six archives, four .tar.gz and two .zip, the
// .zip pair being exactly the `format_overrides` goos.
func emittedFileFormats(t *testing.T, cfg goreleaserConfig) []string {
	t.Helper()
	set := map[string]bool{}
	for _, a := range cfg.Archives {
		if len(a.Formats) == 0 {
			set["tar.gz"] = true
		}
		for _, f := range a.Formats {
			set[f] = true
		}
		for _, o := range a.FormatOverrides {
			for _, f := range o.Formats {
				set[f] = true
			}
		}
	}
	for _, n := range cfg.NFPMs {
		for _, f := range n.Formats {
			set[f] = true
		}
	}
	if len(set) < 4 {
		t.Fatalf("derived only %d release file formats (%v); the release ships tar.gz, zip, deb, rpm and apk",
			len(set), sortedSet(set))
	}
	return sortedSet(set)
}

// tagSuffixAfterVersion returns whatever follows the version token in an
// image-name template — "" for `repo:{{ .Version }}`, "-amd64" for
// `repo:{{ .Version }}-amd64`.
func tagSuffixAfterVersion(tmpl, versionToken string) string {
	_, after, found := strings.Cut(tmpl, versionToken)
	if !found {
		return ""
	}
	return after
}

// sortedRosterNames keeps the failure output stable across runs.
func sortedRosterNames() []string {
	out := make([]string, 0, len(releaseArtifactRoster))
	for k := range releaseArtifactRoster {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedSet(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func mustReadRepoFile(t *testing.T, root, rel string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, rel))
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	return b
}
