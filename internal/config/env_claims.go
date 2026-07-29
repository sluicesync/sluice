// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package config

// Flag-claimed environment variables.
//
// `--encryption-passphrase-env VAR` is the DOCUMENTED production shape
// for a backup passphrase — the flag's own help text steers operators
// to it over `--encryption-passphrase`. Naming that variable with the
// natural `SLUICE_` prefix then made EVERY backup command warn that
// sluice's own recommendation was a mistake:
//
//	WARN config: ignoring SLUICE_ environment variable that matches no
//	config key (typo?) var=SLUICE_PASS valid_keys="…"
//
// The koanf prefix scan in [Load] cannot know about those variables on
// its own: they are named at RUNTIME by a flag value (or by a
// `keyset_source: env:VARNAME` in the config file), not by a field on
// the [Config] struct. So the CLI registers the names it has been
// pointed at, once, after parsing — and the scan then treats a claimed
// name exactly the way it already treats a kong `env:` binding:
// consumed elsewhere, not a typo.
//
// This does NOT weaken the warning: only a name the operator explicitly
// wrote on the command line (or in the config file) is exempt. A
// genuine `SLUICE_TPYO_KEY` still warns.

import (
	"strings"
	"sync"
)

// claimedEnvVars holds the names registered by [SetClaimedEnvVars].
// Package-level because the registration is a single process-startup
// chokepoint (cmd/sluice's main, right after kong.Parse) while the
// readers are the nine [Load] call sites scattered across the command
// tree — threading it through Load's signature would mean nine chances
// to forget it, on a warning nobody would notice was missing.
var claimedEnvVars struct {
	mu    sync.RWMutex
	names map[string]struct{}
}

// SetClaimedEnvVars records the environment-variable NAMES that CLI
// flags have explicitly pointed at (`--encryption-passphrase-env VAR`,
// `--sign-key env:VAR`, `--keyset-source env:VAR`, …). Replaces any
// previously registered set; nil clears it.
//
// Call it before the first [Load]. Values that are not `SLUICE_`-
// prefixed are harmless — the unknown-variable scan only ever looks at
// that prefix.
func SetClaimedEnvVars(names []string) {
	set := make(map[string]struct{}, len(names))
	for _, n := range names {
		if n != "" {
			set[n] = struct{}{}
		}
	}
	claimedEnvVars.mu.Lock()
	defer claimedEnvVars.mu.Unlock()
	claimedEnvVars.names = set
}

// isClaimedEnvVar reports whether name was registered by
// [SetClaimedEnvVars].
func isClaimedEnvVar(name string) bool {
	claimedEnvVars.mu.RLock()
	defer claimedEnvVars.mu.RUnlock()
	_, ok := claimedEnvVars.names[name]
	return ok
}

// EnvVarNameFromSpec extracts the variable NAME from a `env:VARNAME`
// source spec — the shared form of `--keyset-source`, `--sign-key` and
// `--verify-key` — and returns "" for every other form (a file path, a
// `kms://…` reference, a `db:` DSN, empty). One definition so the
// claim registration and the scan agree on what "names an env var"
// means.
func EnvVarNameFromSpec(spec string) string {
	name, ok := strings.CutPrefix(spec, "env:")
	if !ok {
		return ""
	}
	return name
}
