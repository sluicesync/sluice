// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
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
// that restores the old MinimizeSkew=true behaviour. The CLI flag was formerly a
// process-wide MUTABLE package global (SetVStreamPreserveSkewOverride); task 2.5
// (finding A-4) moves it onto the per-instance [Engine] value
// (engineOptions.preserveSkew, set via [Engine.WithVStreamPreserveSkew]), so a
// fleet `sync run` can carry a distinct value per sync. The CLI flag wins over
// the source DSN param, exactly as the global did; the resolver threads the
// override in as an explicit bool argument rather than reading a global.

// vstreamPreserveSkewFromDSN resolves whether to PRESERVE the old MinimizeSkew=
// true behaviour for a reader: the per-instance CLI override cliPreserve (if
// set) OR the source DSN's vstream_preserve_skew=true param. Default false = the
// new relaxed default (MinimizeSkew=false). The reader inverts this into its
// relaxSkew field. cliPreserve is engineOptions.preserveSkew (write-once-true via
// [Engine.WithVStreamPreserveSkew]), so a non-CLI caller passes false and never
// inverts the DSN-then-default behaviour.
func vstreamPreserveSkewFromDSN(cfg *gomysql.Config, cliPreserve bool) bool {
	if cliPreserve {
		return true
	}
	return cfg.Params["vstream_preserve_skew"] == "true"
}
