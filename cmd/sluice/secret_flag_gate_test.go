// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The Tier-1 gate for audit 2026-07-26 SEC-1.
//
// A diagnose bundle exists to be ATTACHED TO A GITHUB ISSUE. Its redaction
// allowlist (internal/diagnose.isSecretFlag) is hand-maintained, so it is only
// as good as the thing that notices when the CLI grows a credential-shaped
// flag and the list does not. Nothing noticed: metrics-watch grew org-wide
// discovery with a `--planetscale-service-token`, and the token landed in
// cli_args.json verbatim — two lines below a DSN that WAS correctly redacted,
// which is what makes it so easy to miss in review. The bundle looked
// sanitized.
//
// This walks the REAL kong model (not a hand-copied list of flag names — that
// would be a mirror with the same drift problem) and asserts every flag whose
// name or help text reads as a credential is covered by the redactor.
package main

import (
	"regexp"
	"strings"
	"testing"

	"github.com/alecthomas/kong"

	"sluicesync.dev/sluice/internal/diagnose"
)

// credentialish matches flag NAMES that carry a secret value. Deliberately
// narrow and name-based: help-text matching produced false positives on flags
// that merely mention credentials (e.g. a path to a credentials FILE).
var credentialish = regexp.MustCompile(`(token|password|passphrase|secret|api-?key|credential)`)

// notASecret lists flags that match the pattern but carry no secret VALUE.
// Every entry needs a reason — an entry without one is a redaction bug being
// annotated away rather than fixed.
var notASecret = map[string]string{
	"--encryption-passphrase-file": "a FILE PATH, not the passphrase; the path is diagnostically useful and carries no secret",
	"--encryption-key-file":        "a file path, as above",
	"--notify-smtp-password-file":  "a file path, as above",
	"--encryption-passphrase-env":  "the NAME of an environment variable to read the passphrase from, not the passphrase; the name is diagnostically useful",
}

func TestEveryCredentialShapedFlagIsRedacted(t *testing.T) {
	parser, err := kong.New(&CLI{}, kong.Vars{"version": "test"})
	if err != nil {
		t.Fatalf("build kong model: %v", err)
	}

	var walked, checked int
	seen := map[string]bool{}
	var offenders []string

	var walk func(n *kong.Node)
	walk = func(n *kong.Node) {
		if n == nil {
			return
		}
		for _, f := range n.Flags {
			if f == nil || f.Name == "" {
				continue
			}
			walked++
			name := "--" + f.Name
			if !credentialish.MatchString(f.Name) {
				continue
			}
			if _, ok := notASecret[name]; ok {
				continue
			}
			checked++
			// The same flag appears under every command node that declares it;
			// report it once.
			if !diagnose.IsSecretFlag(name) && !seen[name] {
				seen[name] = true
				offenders = append(offenders, name+"  (help: "+truncate(f.Help, 60)+")")
			}
		}
		for _, c := range n.Children {
			walk(c)
		}
	}
	walk(parser.Model.Node)

	// Anti-vacuity, two floors. If the walk stops finding flags at all, or
	// stops finding credential-shaped ones, this gate silently passes forever
	// — the exact failure mode it exists to prevent, one level up.
	if walked < 50 {
		t.Fatalf("kong walk saw only %d flags; the model traversal has probably broken and this gate is now vacuous", walked)
	}
	if checked < 5 {
		t.Fatalf("only %d credential-shaped flags matched; the pattern or the CLI changed shape and this gate is now vacuous", checked)
	}

	if len(offenders) > 0 {
		t.Errorf("%d credential-shaped CLI flag(s) are NOT redacted from diagnose bundles.\n  %s\n\n"+
			"A diagnose bundle is meant to be attached to a GitHub issue, so an unredacted credential here is "+
			"published, not merely logged. Add each to isSecretFlag in internal/diagnose/redact.go — or, if the "+
			"flag carries no secret VALUE (a file path, say), add it to notASecret in this file WITH a reason.",
			len(offenders), strings.Join(offenders, "\n  "))
	}
}

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
