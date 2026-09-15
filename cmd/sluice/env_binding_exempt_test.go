// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"sort"
	"strings"
	"testing"

	"github.com/alecthomas/kong"

	"sluicesync.dev/sluice/internal/config"
)

// The config loader overlays SLUICE_* environment variables onto the
// config file and WARNs — "ignoring SLUICE_ environment variable that
// matches no config key (typo?)" — for any it cannot place. Variables
// that belong to the CLI instead (kong `env:` bindings) are exempted by
// config.IsProcessLevelEnvVar, whose doc says the set "mirrors the kong
// bindings in cmd/sluice". Nothing held that mirror, and it drifted:
// SLUICE_STAGE_DIR and SLUICE_METRICS_SINK_HTTP were real bindings that
// the loader reported as typos being ignored while kong consumed them —
// `--stage-dir`'s own help text ends "Env: SLUICE_STAGE_DIR", so one run
// contradicted itself (2026-09-15 audit, LOW: SLUICE_STAGE_DIR and SLUICE_METRICS_SINK_HTTP WARNed as typos).
//
// This derives the universe from kong's own model rather than from a
// grep: every flag on every command node, every `env:` name it binds
// with the SLUICE_ prefix, must be in the exempt set. Anti-vacuity: the
// walk must find at least five such bindings (seven exist today), so a
// model traversal that stops finding envs fails rather than passing on
// an empty set.
func TestEveryKongEnvBindingIsProcessLevel(t *testing.T) {
	t.Parallel()
	parser, err := kong.New(&CLI{}, kong.Vars{"version": "test"}, kong.Exit(func(int) {}))
	if err != nil {
		t.Fatalf("build kong model: %v", err)
	}

	bound := map[string]struct{}{}
	var walk func(n *kong.Node)
	walk = func(n *kong.Node) {
		if n == nil {
			return
		}
		for _, f := range n.Flags {
			if f == nil {
				continue
			}
			for _, env := range f.Envs {
				if strings.HasPrefix(env, "SLUICE_") {
					bound[env] = struct{}{}
				}
			}
		}
		for _, c := range n.Children {
			walk(c)
		}
	}
	walk(parser.Model.Node)

	names := make([]string, 0, len(bound))
	for env := range bound {
		names = append(names, env)
	}
	sort.Strings(names)
	if len(names) < 5 {
		t.Fatalf("kong walk found only %d SLUICE_* env bindings (%v); the model traversal has probably broken "+
			"and this gate is now vacuous", len(names), names)
	}

	var missing []string
	for _, env := range names {
		if !config.IsProcessLevelEnvVar(env) {
			missing = append(missing, env)
		}
	}
	if len(missing) > 0 {
		t.Errorf("kong binds %v, but config.IsProcessLevelEnvVar does not exempt them, so the config loader "+
			"WARNs that sluice is \"ignoring\" a variable the CLI is consuming. Add each to the switch in "+
			"internal/config/config.go.", missing)
	}
	t.Logf("graded %d kong env bindings: %v", len(names), names)
}
