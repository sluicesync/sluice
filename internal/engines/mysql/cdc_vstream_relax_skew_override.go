// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"sync/atomic"

	gomysql "github.com/go-sql-driver/mysql"
)

// ADR-0120 (default flipped 2026-06-26): the steady-state multi-shard VStream
// CDC request is now opened with MinimizeSkew=FALSE by default — both shards
// stream + drain concurrently. The prior default (MinimizeSkew=true, vtgate
// holding the ahead shard back to keep the merged stream commit-time ordered)
// was shown by a real cross-region (82 ms) A/B to *freeze* the lagging shard's
// stream entirely under an apply-deficit backlog (a liveness wedge, reproduced
// 4×), while the relaxed path drained to completion with exactly-once intact.
// The relaxation is correctness-safe under range-sharding (a (table, PK) lives
// in one shard; the key-hash apply lanes serialize same-key within a shard;
// StopOnReshard closes the only cross-shard window) — see the ADR-0120 consumer
// audit + the four gated A/B harnesses.
//
// `--vstream-preserve-skew` (DSN: vstream_preserve_skew=true) is the OPT-OUT
// that restores the old MinimizeSkew=true behaviour. Threaded into the engine
// the same way the ADR-0118 copy-parallelism overrides are: a package-level
// setter called once from the composition root before any connection opens, with
// the source DSN param as the lower-precedence form (the CLI flag wins).
//
// Zero-value-safety (the v0.99.51 trap): the override defaults to false =
// "not preserving — use the new relaxed default", so every constructor / test /
// non-CLI caller that never sets it gets the new default (MinimizeSkew=false).
// The OPT-OUT name means the safe/common behaviour (relaxed) is the zero value.
//
// atomic.Bool because the setter is called once at startup from main-flow and
// the reader reads it on the connection-open path; the value never changes after
// startup, but the atomic keeps the happens-before edge clean.
var vstreamPreserveSkewOverride atomic.Bool

// SetVStreamPreserveSkewOverride records the operator's explicit
// --vstream-preserve-skew CLI value (ADR-0120). true wins over the source DSN's
// vstream_preserve_skew param and restores the old MinimizeSkew=true behaviour;
// false (the default) means "unset — fall back to the DSN param, then the
// relaxed MinimizeSkew=false default". Call once at startup before any engine
// opens a connection. Only a true value is recorded, so a non-CLI caller never
// inverts the DSN-then-default behaviour.
func SetVStreamPreserveSkewOverride(preserve bool) {
	if preserve {
		vstreamPreserveSkewOverride.Store(true)
	}
}

// vstreamPreserveSkewFromDSN resolves whether to PRESERVE the old MinimizeSkew=
// true behaviour for a reader: the CLI override (if set) OR the source DSN's
// vstream_preserve_skew=true param. Default false = the new relaxed default
// (MinimizeSkew=false). The reader inverts this into its relaxSkew field.
func vstreamPreserveSkewFromDSN(cfg *gomysql.Config) bool {
	if vstreamPreserveSkewOverride.Load() {
		return true
	}
	return cfg.Params["vstream_preserve_skew"] == "true"
}
