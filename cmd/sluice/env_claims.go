// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

// Collecting the environment-variable names the operator's flags point
// at, so [config.Load]'s SLUICE_ prefix scan doesn't report sluice's
// own recommended secret-handling pattern as a typo. See the package
// comment in internal/config/env_claims.go for the defect this closes.
//
// The walk is tag-driven rather than a hand-maintained list of flags,
// because the list is exactly the kind that goes stale: a NEW
// credential flag that takes an env-var name is covered the moment it
// declares the tag, with no second place to remember.
//
//   - `envvar:"name"` — the flag value IS the variable name
//     (--encryption-passphrase-env VAR).
//   - `envvar:"spec"` — the flag value is a source spec whose `env:`
//     form names a variable (--sign-key / --verify-key /
//     --keyset-source, all of which also accept a path or a kms:// URL).
//
// Kong ignores struct tags it doesn't know, so the marker rides along
// on the existing flag declarations without affecting parsing —
// TestClaimedEnvVarNames_ParsedCLI pins that end to end.

import (
	"reflect"
	"sort"

	"sluicesync.dev/sluice/internal/config"
)

// registerClaimedEnvVars is main's startup step, factored out so the
// parse → register → [config.Load] chain is testable end to end rather
// than only in its two halves.
func registerClaimedEnvVars(cli *CLI) {
	config.SetClaimedEnvVars(claimedEnvVarNames(cli))
}

// claimedEnvVarNames walks the parsed CLI tree and returns the sorted,
// de-duplicated set of environment-variable names its flag values point
// at. Only the SELECTED command's flags carry values, so the rest of
// the tree contributes nothing.
func claimedEnvVarNames(root any) []string {
	seen := map[string]struct{}{}
	claim := func(name string) {
		if name != "" {
			seen[name] = struct{}{}
		}
	}
	visitEnvVarFields(reflect.ValueOf(root), claim, 0)

	names := make([]string, 0, len(seen))
	for n := range seen {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// maxEnvClaimDepth bounds the struct walk. The kong command tree is a
// shallow tree of plain structs; the bound is cheap insurance against a
// future self-referential type turning a UX nicety into a stack
// overflow at startup.
const maxEnvClaimDepth = 12

func visitEnvVarFields(v reflect.Value, claim func(string), depth int) {
	if depth > maxEnvClaimDepth {
		return
	}
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return
	}
	t := v.Type()
	for i := range t.NumField() {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		switch f.Tag.Get("envvar") {
		case "name":
			claim(v.Field(i).String())
		case "spec":
			claim(config.EnvVarNameFromSpec(v.Field(i).String()))
		default:
			visitEnvVarFields(v.Field(i), claim, depth+1)
		}
	}
}
