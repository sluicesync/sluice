// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package docsync

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The cloud-blob backends a real server has actually answered for, kept
// honest against the tests that boot one.
//
// # Why this exists
//
// `blobcodec.BlobStore` registers four `gocloud.dev/blob` drivers, and for
// most of the project's life exactly one of them (s3blob, via MinIO) had ever
// been booted. That was invisible in the obvious places: sluice's own code is
// provider-neutral, `--backup-endpoint` names six S3-compatible providers, and
// the destination list names four schemes — so every operator-facing surface
// read as broad coverage while the evidence was narrow.
//
// The abstraction is why the gap was easy to miss AND why it was smaller than
// it looked: there is no per-provider code path to test. What an abstraction
// cannot cover is what the BACKEND does, and one backend behaviour is
// load-bearing — `PutIfAbsent` (the ADR-0160 chain concurrent-writer guard)
// maps gocloud's `IfNotExist` onto each backend's native precondition, and a
// provider that ignores it behaves like a plain `Put` while the guard degrades
// to a WARN. Quiet by design, which is the kind that needs a real server.
//
// # What is derived, and from what
//
// Two sets, both from code rather than from memory:
//
//   - REGISTERED: the blank driver imports in blobcodec's blob_store.go, which
//     is what makes a URL scheme reachable at all.
//   - EXERCISED: the URL schemes that appear in an integration test that opens
//     a BlobStore, i.e. the schemes some container has actually served.
//
// Every registered driver must be exercised or carry an explicit exemption
// below with a reason. The doc marker must equal the exercised set exactly.
//
// # The marker
//
//	<!-- blob-backends-verified: azblob, gs, s3 -->
//
// in docs/testing.md. Prose next to it is for humans; the marker is what
// fails. Adding a driver's first test, or deleting its last, breaks this
// gate until the doc says so — which is the point, because "which backends
// are verified" is exactly the kind of claim that is true when written and
// quietly stops being true.
func TestBlobBackendsVerifiedListMatchesTheTests(t *testing.T) {
	registered := registeredBlobDrivers(t)
	exercised := exercisedBlobSchemes(t)

	// Anti-vacuity on BOTH derivations. An empty registered set means the
	// import scan broke; an empty exercised set would let the marker claim
	// nothing and pass.
	if len(registered) == 0 {
		t.Fatal("no gocloud blob driver imports found in blobcodec — the scan broke, and a green run here " +
			"would be comparing two empty sets")
	}
	if len(exercised) == 0 {
		t.Fatal("no blob URL scheme found in any integration test — either the scan broke or every backend " +
			"lost its coverage; both are failures, and neither may pass quietly")
	}

	// Registered-but-unexercised must be deliberate. One entry today, and it
	// is the one that genuinely does not need a container.
	exempt := map[string]string{
		"fileblob": "the hardened LocalStore is the supported local backend and carries its own coverage; " +
			"fileblob exists for URL-scheme parity and OpenBlobStore WARNs that its files are " +
			"world-readable, so it is not a destination we ask anyone to rely on",
	}
	for _, drv := range registered {
		scheme := blobDriverScheme(drv)
		if scheme != "" && exercised[scheme] {
			continue
		}
		if reason, ok := exempt[drv]; ok {
			t.Logf("driver %s is registered and unexercised, exempt: %s", drv, reason)
			continue
		}
		t.Errorf("driver %s is registered (its URL scheme is reachable by operators) but no integration "+
			"test opens a %s:// BlobStore, and it carries no exemption. Either boot a server for it or "+
			"add an exemption with the reason it does not need one", drv, blobDriverScheme(drv))
	}

	want := sortedBlobSchemes(exercised)
	got := blobBackendsMarker(t)
	if strings.Join(got, ", ") != strings.Join(want, ", ") {
		t.Errorf("docs/testing.md's blob-backends-verified marker says %v; the integration tests exercise "+
			"%v.\nThe marker is the published answer to \"which backends has a real server answered for\" — "+
			"update it in the same change that adds or removes a backend's coverage.", got, want)
	}
}

// registeredBlobDrivers returns the gocloud blob driver packages blobcodec
// blank-imports, e.g. "s3blob". These are what make a URL scheme reachable.
func registeredBlobDrivers(t *testing.T) []string {
	t.Helper()
	path := filepath.Join("..", "..", "internal", "pipeline", "blobcodec", "blob_store.go")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	re := regexp.MustCompile(`_ "gocloud\.dev/blob/([a-z]+)"`)
	matches := re.FindAllStringSubmatch(string(raw), -1)
	out := make([]string, 0, len(matches))
	for _, m := range matches {
		out = append(out, m[1])
	}
	sort.Strings(out)
	return out
}

// blobDriverScheme maps a driver package name to the URL scheme it registers.
// Hand-written because the mapping lives inside gocloud, not in our tree —
// and deliberately returns "" for anything unrecognised so a NEW driver fails
// the roster above rather than being silently skipped.
func blobDriverScheme(driver string) string {
	switch driver {
	case "s3blob":
		return "s3"
	case "gcsblob":
		return "gs"
	case "azureblob":
		return "azblob"
	case "fileblob":
		return "file"
	default:
		return ""
	}
}

// exercisedBlobSchemes returns the set of blob URL schemes some integration
// test actually opens a BlobStore against.
//
// Scoped to files that MENTION a BlobStore constructor, so a `gs://` in a
// comment or an unrelated fixture cannot inflate the set — the question is
// which backends a container has served, not which strings appear.
func exercisedBlobSchemes(t *testing.T) map[string]bool {
	t.Helper()
	dir := filepath.Join("..", "..", "internal", "pipeline")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	schemeRe := regexp.MustCompile(`"(s3|gs|azblob|file)://`)

	out := map[string]bool{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, "_integration_test.go") {
			continue
		}
		raw, err := os.ReadFile(filepath.Clean(filepath.Join(dir, name)))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		body := string(raw)
		if !strings.Contains(body, "OpenBlobStore") && !strings.Contains(body, "BlobStore(") {
			continue
		}
		for _, m := range schemeRe.FindAllStringSubmatch(body, -1) {
			out[m[1]] = true
		}
	}
	return out
}

// blobBackendsMarker reads the marker docs/testing.md publishes.
func blobBackendsMarker(t *testing.T) []string {
	t.Helper()
	path := filepath.Join("..", "..", "docs", "testing.md")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	re := regexp.MustCompile(`<!--\s*blob-backends-verified:\s*([^>]*?)\s*-->`)
	m := re.FindStringSubmatch(string(raw))
	if m == nil {
		t.Fatalf("docs/testing.md carries no `<!-- blob-backends-verified: … -->` marker; this gate has "+
			"nothing to hold the tests to. Expected it beside the cloud-blob coverage table (%s)", path)
	}
	var out []string
	for _, part := range strings.Split(m[1], ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}

func sortedBlobSchemes(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
