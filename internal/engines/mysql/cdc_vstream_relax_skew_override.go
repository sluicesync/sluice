// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"sync/atomic"

	gomysql "github.com/go-sql-driver/mysql"
)

// ADR-0120: the steady-state multi-shard VStream CDC request is opened with
// MinimizeSkew=true (vtgate holds the ahead shard back to keep the merged
// stream commit-time ordered). The --vstream-relax-skew opt-in flips it to
// MinimizeSkew=false so both shards stream + drain concurrently during an
// apply-deficit backlog. The relaxation is correctness-safe under range-sharding
// (a (table, PK) lives in one shard; the key-hash apply lanes serialize same-key
// within a shard; StopOnReshard closes the only cross-shard window) — see the
// ADR-0120 consumer audit. Threaded into the engine the same way the ADR-0118
// copy-parallelism overrides are: a package-level setter called once from the
// composition root before any connection opens, with the source DSN's
// vstream_relax_skew param as the lower-precedence form (the CLI flag wins,
// because the operator typed it for this run).
//
// Zero-value-safety (the v0.99.51 trap, inverted): the override defaults to
// false = "not set — fall back to the DSN param (also default false =
// MinimizeSkew on)", so every constructor / test / non-CLI caller that never
// sets it keeps today's proven MinimizeSkew=true behaviour byte for byte. Only
// an explicit --vstream-relax-skew (or vstream_relax_skew=true) relaxes it. The
// flag is OPT-IN-named (relax = the non-default action), so the safe/common
// behaviour is the zero value.
//
// atomic.Bool because the setter is called once at startup from main-flow and
// the reader reads it on the connection-open path; the value never changes after
// startup, but the atomic keeps the happens-before edge clean.
var vstreamRelaxSkewOverride atomic.Bool

// SetVStreamRelaxSkewOverride records the operator's explicit
// --vstream-relax-skew CLI value (ADR-0120). true wins over the source DSN's
// vstream_relax_skew param; false (the default) means "unset — fall back to the
// DSN param, then the MinimizeSkew=true default". Call once at startup before
// any engine opens a connection. Only a true value is recorded, so a non-CLI
// caller never inverts the DSN-then-default behaviour.
func SetVStreamRelaxSkewOverride(relax bool) {
	if relax {
		vstreamRelaxSkewOverride.Store(true)
	}
}

// vstreamRelaxSkewFromDSN resolves the effective relax-skew setting for a reader:
// the CLI override (if set) OR the source DSN's vstream_relax_skew=true param.
// Default false = MinimizeSkew stays on (today's behaviour).
func vstreamRelaxSkewFromDSN(cfg *gomysql.Config) bool {
	if vstreamRelaxSkewOverride.Load() {
		return true
	}
	return cfg.Params["vstream_relax_skew"] == "true"
}
