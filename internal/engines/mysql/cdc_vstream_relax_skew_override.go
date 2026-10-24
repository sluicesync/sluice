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

// The skew policy, per request (VSTREAM-TAIL-SKEW-HOLD, Bug 334). Every
// VStream request the package builds carries one of the two values below;
// TestVStreamRequestSkewRoster_EveryBuilderClassified walks the source and
// fails on a request whose MinimizeSkew is anything else, or whose builder is
// not classified COPY or CDC.
//
// COPY requests keep MinimizeSkew=true (ADR-0120's scope exemption: the copy
// wants the shards synchronized at the cut). Requests that carry CDC follow
// ADR-0120: relaxed unless the operator opted out with --vstream-preserve-skew.
// That includes the snapshot stream's post-copy tail. Through v0.163.10 the
// tail kept the copy's MinimizeSkew=true, and after a skewed multi-shard cold
// start it stalled: the copy keeps every shard that finished early at its own
// copy GTID (Bug 331), so the tail opens with two or more shards replaying a
// window seconds in the past. vtgate (vendored vstream_manager.go
// computeSkew/mustPause) names the oldest the laggard and pauses every other
// shard on its next event. The second rewound shard pauses on its first
// replayed event, its clock frozen in the past; the up-to-date shards' clocks
// move on heartbeats until their own first event pauses them; and the hold
// clears only when every clock is within MaxSkew (2s) of the others, which a
// clock frozen in the past never is. Three of four shards delivered nothing
// until vtgate's 10-minute skew timeout, then re-held (measured on a real
// four-shard Vitess 24 cluster). The steady-state reader never hit it because
// it was already relaxed; a warm restart through it converged exactly.
//
// Relaxing is ordering-safe for the tail for the reason it is for the
// steady-state reader (ADR-0120 §2): MinimizeSkew only delays a shard's
// events. It never changes their per-shard order or the transaction-sized
// batches vtgate interleaves across shards, which interleave under
// MinimizeSkew=true too whenever the shards are within 2s of each other. No
// consumer orders across shards: positions are per-shard GTID sets, the apply
// lanes route by key, and a key lives on one shard. The tail's dispatch
// ([vstreamSnapshotStream.dispatchCDCEvent]) mirrors the steady-state
// reader's, which has run relaxed by default since ADR-0120.
//
// An operator who sets --vstream-preserve-skew gets MinimizeSkew=true on the
// tail too, and with it this hold's risk, which the flag's help already names
// ("accept the catch-up wedge risk"). The per-shard stall WARN covers the
// tail, so that case is not silent.

// vstreamCopyMinimizeSkew is the MinimizeSkew every COPY request carries.
const vstreamCopyMinimizeSkew = true

// vstreamCDCMinimizeSkew is the MinimizeSkew a request carrying CDC sets: on
// only when the operator preserved the skew hold.
func vstreamCDCMinimizeSkew(preserveSkew bool) bool {
	return preserveSkew
}
