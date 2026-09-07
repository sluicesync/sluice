// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package docsync

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/redact"
)

// A copy-pasteable keyset example must actually load.
//
// # Why this exists
//
// The v0.145.0 regression cycle ran the PII-redaction cookbook recipe and
// reported PASS. It had exercised the `file:` option. The `env:` option, one
// screen further down the same page, documented a scheme sluice has never
// implemented: it exported one variable PER KEY holding raw base64 bytes
//
//	export SLUICE_KEYSET_email_v1='base64-of-32-random-bytes-here...'
//
// and then passed a PREFIX, `--keyset-source 'env:SLUICE_KEYSET_'`, as though
// the loader would glob the environment. It does not, and never did:
// `loadKeysetFromEnv` reads ONE variable, named exactly, holding the WHOLE
// keyset YAML — which is what ADR-0041 specifies and what docs/redaction.md
// has always said. An operator following the recipe verbatim got
// `environment variable is empty or unset` and no way to tell from the page
// that the page was wrong.
//
// It failed loudly, which is why this is a papercut and not a data-loss bug.
// The interesting part is how it survived: a doc example is a claim about
// behaviour, and this one had never been executed by anything. The recipe was
// "verified" by a cycle that ran one of its three options and reported on the
// recipe.
//
// # What this gate reaches, and what it does not
//
// Naming the scope matters more than usual here, because a gate called
// something like "documented examples work" would imply far more than this
// one delivers.
//
//	REACHED    fenced blocks in docs/ that BOTH export an environment
//	           variable AND pass an `env:` keyset-source. That pairing is
//	           what makes a block a runnable example rather than a syntax
//	           reference, and it is the shape the broken recipe had.
//
//	NOT REACHED  scheme-reference listings that name `env:SLUICE_KEYSET`
//	           without setting it (docs/redaction.md's "Sources" block is
//	           the legitimate case, and it is correct); `file:` examples,
//	           whose paths do not exist at test time; `db:` examples, which
//	           need a server; and every doc example that is not a keyset.
//
// Check A — name agreement — is universal over the reached blocks and needs
// no shell emulation: a block that passes `env:NAME` must export `NAME`. That
// alone catches the historical defect exactly.
//
// Check B — the real loader — runs only where the exported value is
// extractable (a quoted heredoc). It feeds that value through
// `redact.LoadKeyset` and then resolves every key the block's own `--redact`
// rules name, so the example is graded end to end rather than merely parsed.
// B is best-effort by construction, so it carries an anti-vacuity floor: if
// no block in the whole docs tree reaches it, this test FAILS rather than
// passing on an empty set. A gate whose strong half quietly stops running is
// the failure mode it exists to prevent.
func TestDocumentedEnvKeysetExamplesLoad(t *testing.T) {
	root := repoRootFromDocsync(t)
	docs := filepath.Join(root, "docs")

	var (
		blocksReached int
		blocksLoaded  int
	)

	err := filepath.Walk(docs, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(path, ".md") {
			return nil
		}
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		rel, _ := filepath.Rel(root, path)
		for _, block := range fencedBlocks(string(data)) {
			envRefs := envKeysetRefs(block)
			exports := exportedVars(block)
			if len(envRefs) == 0 || len(exports) == 0 {
				continue
			}
			blocksReached++

			for _, name := range envRefs {
				// Check A.
				value, ok := exports[name]
				if !ok {
					t.Errorf("%s: the example passes --keyset-source env:%s but never exports %s.\n"+
						"  It exports: %s\n"+
						"  The loader reads ONE variable, named exactly, holding the whole keyset YAML —\n"+
						"  it does not treat the value as a prefix and glob the environment.",
						rel, name, name, strings.Join(sortedKeys(exports), ", "))
					continue
				}
				if value == "" {
					// Not extractable (an unquoted expansion, a $(...) we
					// do not emulate). Check A still applied; B does not.
					continue
				}

				// Check B.
				blocksLoaded++
				t.Setenv(name, value)
				ks, lerr := redact.LoadKeyset(context.Background(), "env:"+name)
				if lerr != nil {
					t.Errorf("%s: the example's own exported %s does not load: %v\n"+
						"  An operator pasting this block gets exactly this error.",
						rel, name, lerr)
					continue
				}
				if ks == nil {
					t.Errorf("%s: env:%s loaded to a nil keyset", rel, name)
					continue
				}
				for _, keyName := range redactRuleKeyNames(block) {
					if _, _, _, rerr := ks.ResolveKey(keyName); rerr != nil {
						t.Errorf("%s: the example redacts with key %q, which its own keyset does not "+
							"resolve: %v", rel, keyName, rerr)
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk docs: %v", err)
	}

	if blocksReached == 0 {
		t.Fatal("no docs block both exports a variable and passes an env: keyset-source, so check A " +
			"graded nothing. Either the extraction stopped matching or the runnable env-backed " +
			"example was deleted; both mean this gate is no longer watching anything.")
	}
	if blocksLoaded == 0 {
		t.Fatal("check A ran but check B — the REAL loader — reached no block, so nothing was actually " +
			"loaded. The env-backed example's value is no longer extractable (a heredoc that changed " +
			"shape?), which leaves only name agreement graded. Restore an extractable example or " +
			"teach exportedVars the new shape; do not let this pass empty.")
	}
}

var (
	fenceRe     = regexp.MustCompile("(?s)```[a-zA-Z]*\n(.*?)```")
	envKeysetRe = regexp.MustCompile(`--keyset-source[= ]+'?"?env:([A-Za-z_][A-Za-z0-9_]*)`)
	redactKeyRe = regexp.MustCompile(`--redact\s+'?[^'\s]*=(?:hash:hmac-sha256|tokenize:dict):([A-Za-z_][A-Za-z0-9_]*)`)
	exportPlain = regexp.MustCompile(`(?m)^\s*export\s+([A-Za-z_][A-Za-z0-9_]*)=('([^']*)'|"([^"$]*)")\s*$`)
	// RE2 has no backreferences, so the heredoc's closing delimiter cannot
	// be matched in the pattern; it is located in code below.
	exportHeredocOpen = regexp.MustCompile(`(?m)^\s*export\s+([A-Za-z_][A-Za-z0-9_]*)="\$\(cat <<'([A-Za-z_]+)'\s*$`)
	exportAnyShape    = regexp.MustCompile(`(?m)^\s*export\s+([A-Za-z_][A-Za-z0-9_]*)=`)
)

// heredocExports finds `export NAME="$(cat <<'DELIM'` … `DELIM` … `)"` and
// returns NAME → the heredoc body.
func heredocExports(block string) map[string]string {
	out := map[string]string{}
	for _, loc := range exportHeredocOpen.FindAllStringSubmatchIndex(block, -1) {
		name := block[loc[2]:loc[3]]
		delim := block[loc[4]:loc[5]]
		body := block[loc[1]:]
		body = strings.TrimPrefix(body, "\n")
		end := strings.Index(body, "\n"+delim+"\n")
		if end < 0 {
			continue
		}
		out[name] = body[:end]
	}
	return out
}

func fencedBlocks(md string) []string {
	ms := fenceRe.FindAllStringSubmatch(md, -1)
	out := make([]string, 0, len(ms))
	for _, m := range ms {
		out = append(out, m[1])
	}
	return out
}

func envKeysetRefs(block string) []string {
	ms := envKeysetRe.FindAllStringSubmatch(block, -1)
	out := make([]string, 0, len(ms))
	for _, m := range ms {
		out = append(out, m[1])
	}
	return out
}

func redactRuleKeyNames(block string) []string {
	ms := redactKeyRe.FindAllStringSubmatch(block, -1)
	out := make([]string, 0, len(ms))
	for _, m := range ms {
		out = append(out, m[1])
	}
	return out
}

// exportedVars returns the variables a shell block exports, mapped to their
// value where the value is extractable without emulating a shell. A name
// present with an empty value means "exported, value not extractable" — check
// A applies, check B does not.
func exportedVars(block string) map[string]string {
	out := heredocExports(block)
	for _, m := range exportPlain.FindAllStringSubmatch(block, -1) {
		if _, done := out[m[1]]; done {
			continue
		}
		v := m[3]
		if v == "" {
			v = m[4]
		}
		out[m[1]] = v
	}
	// Any remaining `export NAME=` shape we did not parse still counts for
	// name agreement.
	for _, m := range exportAnyShape.FindAllStringSubmatch(block, -1) {
		if _, done := out[m[1]]; !done {
			out[m[1]] = ""
		}
	}
	return out
}
