// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"sluicesync.dev/sluice/internal/engines/mysql"
	"sluicesync.dev/sluice/internal/ir"
)

// The ADR-0118 inert-flag registry (GC-19 / census C-4).
//
// A flag whose --help says it is inert on some engine is a documented
// silent no-op: the operator sets it, kong accepts it, nothing reads it.
// ADR-0118 finding 1(b) made three of those loud on `sync start`; this
// registry is the data-driven generalization — one row per (command, flag)
// whose inert-ness is decided by an ENGINE CAPABILITY, so adding a flag is
// one row and the WARN text is derived from the row, not hand-written.
//
// The predicate is the load-bearing half. Each one names the capability the
// pipeline itself dispatches on (an optional interface it type-asserts, or a
// Capabilities field it reads) rather than an engine-name allowlist, so a
// new engine or flavor is classified correctly by declaration: the old
// `case "mysql", "planetscale", "vitess"` allowlist was silently wrong on
// every trigger-CDC, mydumper and flat-file source (GC-19's second
// narrowness). Where the deciding surface only exists post-dial (a writer
// or reader capability), the predicate uses the engine-level declaration
// that mirrors it and says so in its comment.
//
// TestInertFlagWarnCoversEveryInertMarkedFlag holds this table to the kong
// model: every flag whose help text carries an inert-marker phrase must be a
// row here or a reasoned entry in inertFlagExemptions, and every row must
// name a real flag on a real command. TestInertFlagWarnWiringRoster holds
// every row's command to a warnInertFlags call site.

// inertFlag is one registry row.
type inertFlag struct {
	command string // kong command path, e.g. "sync start"
	flag    string // flag name without the leading --

	// side says which resolved engine the predicate judges, for the
	// operator-facing "with a mysql source" / "against a sqlite target"
	// clause in the WARN.
	side inertSide

	// inertWhen reports whether the flag is inert for the resolved engines.
	// Either engine may be nil (a command that carries only one side, or an
	// unknown driver name the command's own resolution will refuse); a nil
	// engine never satisfies a predicate.
	inertWhen func(source, target ir.Engine) bool

	// reason is the operator-facing clause after the flag and engine: WHY it
	// is inert here and, where one exists, the knob that applies instead.
	reason string
}

// inertSide names the engine a row judges.
type inertSide uint8

const (
	inertOnSource inertSide = iota
	inertOnTarget
)

// inertOn renders the "with a <name> source" / "against a <name> target"
// clause for the WARN from whichever engine the row judges.
func (r inertFlag) inertOn(source, target ir.Engine) string {
	if r.side == inertOnTarget {
		return "against a " + target.Name() + " target"
	}
	return "with a " + source.Name() + " source"
}

// ---- Capability predicates -------------------------------------------------
//
// Each predicate is named for the capability it reads and is pinned per
// engine by TestInertFlagPredicates_EngineMatrix, which derives the engine
// universe from the registry and holds every predicate to an explicit
// applies-set — so a new engine that lands without being classified fails
// that test rather than silently inheriting "inert" or "applies".

// sourceLacksFastColdStart: the ADR-0079 FAST cold-start (and everything
// that rides it — the within-table cursor path, the raw-copy lane) needs a
// source that can mint N readers pinned to one exported snapshot. That is
// [ir.SnapshotImporterOpener], the same optional interface
// coldStartFastEligible type-asserts (streamer_coldstart_parallel.go); a
// source without it always takes the serial engine-internal cold-copy, so
// the fast-lane knobs never get read.
func sourceLacksFastColdStart(source, _ ir.Engine) bool {
	if source == nil {
		return false
	}
	_, ok := source.(ir.SnapshotImporterOpener)
	return !ok
}

// sourceCDCIsNot returns a predicate that holds when the source's declared
// CDC method is not want — the [ir.Capabilities.CDC] declaration every
// VStream-vs-binlog-vs-trigger branch in the tree dispatches on.
func sourceCDCIsNot(want ir.CDCMethod) func(source, _ ir.Engine) bool {
	return func(source, _ ir.Engine) bool {
		return source != nil && source.Capabilities().CDC != want
	}
}

// targetLacksConnectionBudgetProber mirrors migcore.ResolveTargetCopyParallelism:
// a target without [ir.TargetConnectionBudgetProber] has no connection-slot
// model, so the budget step is a no-op and --max-target-connections is
// never consulted.
func targetLacksConnectionBudgetProber(_, target ir.Engine) bool {
	if target == nil {
		return false
	}
	_, ok := target.(ir.TargetConnectionBudgetProber)
	return !ok
}

// targetLacksStaleBackendReaper mirrors pipeline.preflightStaleBackends: a
// target without [ir.TargetStaleBackendReaper] has no backend model, so the
// stale-backend preflight is a no-op and --reap-stale-backends is never
// consulted.
func targetLacksStaleBackendReaper(_, target ir.Engine) bool {
	if target == nil {
		return false
	}
	_, ok := target.(ir.TargetStaleBackendReaper)
	return !ok
}

// targetLacksIndexBuildTuning: the deciding surface is [ir.IndexBuildTuner]
// on the OPENED schema writer (pipeline.applyIndexBuildMem type-asserts it
// after dialing), which no pre-dial predicate can see. The engine-level
// declaration that mirrors it is [ir.Capabilities.PostgresBackend]: the
// tuner sets maintenance_work_mem / max_parallel_maintenance_workers, PG
// server settings, and the only writer implementing it is postgres's, which
// every PostgresBackend engine opens (postgres directly; postgres-trigger's
// OpenSchemaWriter is a one-line delegate to it).
// TestInertFlagPredicates_IndexBuildTuningPremise binds both halves: the
// PostgresBackend engine set, and the delegate.
func targetLacksIndexBuildTuning(_, target ir.Engine) bool {
	return target != nil && !target.Capabilities().PostgresBackend
}

// targetLacksControlKeyspace mirrors applyControlKeyspace, the single
// chokepoint every --control-keyspace caller goes through: it type-asserts
// the concrete [mysql.Engine] (the keyspace concept is Vitess's, and the
// resolver lives on that engine); any other target passes through with the
// flag unread.
func targetLacksControlKeyspace(_, target ir.Engine) bool {
	if target == nil {
		return false
	}
	_, ok := target.(mysql.Engine)
	return !ok
}

// namespaceIsFlat holds when the engine declares a single flat table
// namespace ([ir.SchemaScopeFlat]) — there is no schema for a PG-schema flag
// to name.
func namespaceIsFlat(e ir.Engine) bool {
	return e != nil && e.Capabilities().SchemaScope == ir.SchemaScopeFlat
}

func sourceNamespaceIsFlat(source, _ ir.Engine) bool { return namespaceIsFlat(source) }
func targetNamespaceIsFlat(_, target ir.Engine) bool { return namespaceIsFlat(target) }

// ---- The registry ----------------------------------------------------------

// Shared reason clauses, so the flags of one family say the same thing.
const (
	reasonFastColdStart = "it governs the ADR-0079 FAST cold-start, which needs a source that can mint snapshot-pinned readers (postgres); " +
		"this source's cold-copy is engine-internal — use --copy-fanout-degree / --vstream-copy-table-parallelism / --copy-table-parallelism to tune it"
	reasonBinlogOnly  = "it tunes the native-MySQL (binlog) cold-copy read axis, and this source does not stream a binlog"
	reasonVStreamOnly = "it governs the VStream (PlanetScale/Vitess) path, and this source does not stream VStream"
	reasonNoProber    = "it bounds a connection-slot budget, and this target declares no connection-budget probe (nothing reads the ceiling)"
	reasonNoReaper    = "it authorises reaping sluice's own stale backends, and this target declares no backend model (the preflight is a no-op)"
	reasonNoTuner     = "it tunes the deferred CREATE INDEX phase's maintenance_work_mem / parallel workers, which only a Postgres-backed target's writer reads"
	reasonNoKeyspace  = "keyspaces are a Vitess concept; only a MySQL-family target resolves a control keyspace (the flag is passed through unread)"
	reasonNoLSN       = "it reads Postgres replication-slot state (LSN byte distance / slot name), which this source does not have"
	reasonFlatSchema  = "this engine has a single flat namespace — there is no schema for the flag to name"
)

// inertFlagRegistry is the table. Keep it grouped by command and, within a
// command, by capability family; the gate does not care about order.
var inertFlagRegistry = []inertFlag{
	// --- migrate ---
	{"migrate", "index-build-mem", inertOnTarget, targetLacksIndexBuildTuning, reasonNoTuner},
	{"migrate", "index-build-parallelism", inertOnTarget, targetLacksIndexBuildTuning, reasonNoTuner},
	{"migrate", "max-target-connections", inertOnTarget, targetLacksConnectionBudgetProber, reasonNoProber},
	{"migrate", "reap-stale-backends", inertOnTarget, targetLacksStaleBackendReaper, reasonNoReaper},

	// --- sync start: the ADR-0079 fast-lane knobs (the original finding 1(b) three, plus the two that shared their sentence) ---
	{"sync start", "bulk-parallelism", inertOnSource, sourceLacksFastColdStart, reasonFastColdStart},
	{"sync start", "table-parallelism", inertOnSource, sourceLacksFastColdStart, reasonFastColdStart},
	{"sync start", "bulk-parallel-min-rows", inertOnSource, sourceLacksFastColdStart, reasonFastColdStart},
	{"sync start", "bulk-batch-size", inertOnSource, sourceLacksFastColdStart, reasonFastColdStart},
	{"sync start", "raw-copy-format", inertOnSource, sourceLacksFastColdStart, "the raw-copy passthrough lane rides the ADR-0079 FAST cold-start, which needs a source that can mint snapshot-pinned readers (postgres)"},
	// --- sync start: native-MySQL (binlog) read axis ---
	{"sync start", "copy-table-parallelism", inertOnSource, sourceCDCIsNot(ir.CDCBinlog), reasonBinlogOnly},
	{"sync start", "no-intra-table-stealing", inertOnSource, sourceCDCIsNot(ir.CDCBinlog), reasonBinlogOnly},
	// --- sync start: VStream knobs ---
	{"sync start", "vstream-copy-table-parallelism", inertOnSource, sourceCDCIsNot(ir.CDCVStream), reasonVStreamOnly},
	{"sync start", "vstream-preserve-skew", inertOnSource, sourceCDCIsNot(ir.CDCVStream), reasonVStreamOnly},
	{"sync start", "no-float-exact-reread", inertOnSource, sourceCDCIsNot(ir.CDCVStream), reasonVStreamOnly + " (its snapshot COPY is already float-exact)"},
	{"sync start", "strict-float", inertOnSource, sourceCDCIsNot(ir.CDCVStream), reasonVStreamOnly + " (its snapshot COPY is already float-exact)"},
	// --- sync start: trigger-CDC change log ---
	{"sync start", "auto-prune-change-log", inertOnSource, sourceCDCIsNot(ir.CDCTriggers), "it reaps a trigger-CDC source's sluice_change_log, and this source has no change log (it is not a trigger-CDC engine)"},
	// --- sync start: target-side knobs ---
	{"sync start", "index-build-mem", inertOnTarget, targetLacksIndexBuildTuning, reasonNoTuner},
	{"sync start", "index-build-parallelism", inertOnTarget, targetLacksIndexBuildTuning, reasonNoTuner},
	{"sync start", "max-target-connections", inertOnTarget, targetLacksConnectionBudgetProber, reasonNoProber},
	{"sync start", "reap-stale-backends", inertOnTarget, targetLacksStaleBackendReaper, reasonNoReaper},
	{"sync start", "control-keyspace", inertOnTarget, targetLacksControlKeyspace, reasonNoKeyspace},

	// --- backup full: the VStream FLOAT trio (the post-open gate is
	// ir.LossyFloatCopyReader on the snapshot reader; the VStream CDC
	// declaration is the engine-level mirror — only the VStream reader
	// display-rounds FLOAT) ---
	{"backup full", "strict-float", inertOnSource, sourceCDCIsNot(ir.CDCVStream), reasonVStreamOnly + " (its snapshot COPY is already float-exact)"},
	{"backup full", "no-float-exact-reread", inertOnSource, sourceCDCIsNot(ir.CDCVStream), reasonVStreamOnly + " (its snapshot COPY is already float-exact)"},
	{"backup full", "float-reread-max-rows", inertOnSource, sourceCDCIsNot(ir.CDCVStream), reasonVStreamOnly + " (its snapshot COPY is already float-exact)"},

	// --- restore + the sync lifecycle commands: --control-keyspace ---
	{"restore", "control-keyspace", inertOnTarget, targetLacksControlKeyspace, reasonNoKeyspace},
	{"sync decommission", "control-keyspace", inertOnTarget, targetLacksControlKeyspace, reasonNoKeyspace},
	{"sync health", "control-keyspace", inertOnTarget, targetLacksControlKeyspace, reasonNoKeyspace},
	{"sync status", "control-keyspace", inertOnTarget, targetLacksControlKeyspace, reasonNoKeyspace},
	{"sync stop", "control-keyspace", inertOnTarget, targetLacksControlKeyspace, reasonNoKeyspace},
	{"schema add-table", "control-keyspace", inertOnTarget, targetLacksControlKeyspace, reasonNoKeyspace},

	// --- sync health / diagnose: PG replication-slot flags ---
	{"sync health", "max-lag-bytes", inertOnSource, sourceCDCIsNot(ir.CDCLogicalReplication), reasonNoLSN + " — the lag stays informational and the exit-1 threshold never fires"},
	{"sync health", "slot-name", inertOnSource, sourceCDCIsNot(ir.CDCLogicalReplication), reasonNoLSN},
	{"diagnose", "slot-name", inertOnSource, sourceCDCIsNot(ir.CDCLogicalReplication), reasonNoLSN},

	// --- cutover: --target-schema (no ValidateTargetSchema door on this path;
	// pg_get_serial_sequence namespaces exist only on a schema-scoped target) ---
	{"cutover", "target-schema", inertOnTarget, targetNamespaceIsFlat, reasonFlatSchema},

	// --- trigger setup/teardown/prune: the PG --schema on a flat-namespace source ---
	{"trigger setup", "schema", inertOnSource, sourceNamespaceIsFlat, reasonFlatSchema},
	{"trigger teardown", "schema", inertOnSource, sourceNamespaceIsFlat, reasonFlatSchema},
	{"trigger prune", "schema", inertOnSource, sourceNamespaceIsFlat, reasonFlatSchema},
}

// inertFlagExemptions lists the inert-marked flags the gate must NOT expect
// in the registry, each with the reason. Keyed "<command> --<flag>", or
// "* --<flag>" for a flag whose reason holds on every command carrying it.
// Every entry must still match a real inert-marked flag (a stale exemption
// fails the gate), so this map cannot rot into a list of flags that no
// longer exist.
//
// Four exemption shapes, and only these:
//   - refuses loudly: the flag already fails the run on the engine it names
//     (the gate cites the refusal site) — a WARN would be redundant;
//   - already warns: the pipeline emits its own inert notice;
//   - not engine-keyed: inert-ness is decided by something no engine
//     capability declares — a store URL scheme, another flag, a runtime
//     probe (sharding topology, routing layer, resume state);
//   - recorded decision: a written design choice to stay silent.
var inertFlagExemptions = map[string]string{
	// --- store-URL-scheme keyed (not an engine capability) ---
	"* --backup-endpoint":   "keyed on the backup store's URL scheme (s3://), not an engine capability; a non-S3 store never reads it",
	"* --backup-region":     "keyed on the backup store's URL scheme (s3://), not an engine capability; a non-S3 store never reads it",
	"* --backup-path-style": "keyed on the backup store's URL scheme (s3://), not an engine capability; a non-S3 store never reads it",

	// --- flag-dependency prose ('inert unless --x', 'ignored when --resume') ---
	"backup compact --compaction-pk-strategy": "no effect without --smart-compaction: a flag dependency, not an engine capability",
	"migrate --force-cold-start":              "ignored when --resume is set: a flag dependency, not an engine capability",
	"sync start --force-cold-start":           "ignored on the warm-resume path: persisted-state dependent, not an engine capability",
	"sync start --notify-schema-drift":        "'inert unless a sink is configured' is sink-dependency prose about the --notify-* family, not an engine",
	"sync start --notify-slot-health":         "'inert unless a sink is configured' is sink-dependency prose about the --notify-* family, not an engine",
	"sync start --notify-smtp-host":           "'the email sink is inert unless this is set' describes the flag's own opt-in, not an engine",
	"metrics-watch --notify-smtp-host":        "'the email sink is inert unless this is set' describes the flag's own opt-in, not an engine",
	"sync start --apply-concurrency":          "its 'inert' clause is about --max-target-connections' effect on the MySQL lane count (registered on its own row), not about this flag",
	"metrics-watch --metrics-listen":          "ignored with --once: a flag dependency, not an engine capability (census C-7 owns the missing WARN)",

	// --- runtime-probe keyed (topology / chain shape / predicate shape) ---
	"migrate --allow-cross-shard-merge":    "no effect on a single-shard source: sharding topology is a runtime vtgate probe, not a static capability",
	"sync start --allow-cross-shard-merge": "no effect on a single-shard source: sharding topology is a runtime vtgate probe, not a static capability",
	"sync start --notify-router-cpu-util":  "inert on a target with no routing layer: Neki router presence is a runtime probe, not a static capability",
	"sync start --where-strict-collation":  "no effect on numeric/byte-exact comparisons: keyed on the --where predicate's shape, not an engine",
	"restore --apply-concurrency":          "no effect on a single-full restore: keyed on the archive's chain shape (a manifest fact), not an engine",
	"sync start --copy-fanout-degree": "inert on the ADR-0079 FAST cold-start path, which coldStartFastEligible decides at RUNTIME (resume state, --schema-already-applied, the A0 client-copy fallback); " +
		"a static engine predicate would WARN on exactly the postgres cold-starts that DO take the serial path and use it",

	// --- refuses loudly (a WARN would be redundant with the refusal) ---
	"* --csv-delimiter":                            "refuses loudly on a non-flat-file source: applySourceEngineOptions",
	"migrate --infer-types":                        "refuses loudly on a non-SQLite/D1/flat-file source: MigrateCmd.resolveEngines",
	"migrate --stage-local":                        "refuses loudly on a non-D1 source: MigrateCmd.resolveEngines",
	"migrate --allow-degraded-fks":                 "refuses loudly on a target without degraded-FK support: pipeline/migrate.go (--allow-degraded-fks is set but the target engine doesn't support degraded FKs)",
	"migrate --planetscale-raise-query-timeout":    "refuses loudly on a non-planetscale target: MigrateCmd.planetScaleQueryTimeoutController",
	"sync start --planetscale-raise-query-timeout": "refuses loudly on a non-planetscale target: SyncStartCmd.planetScaleQueryTimeoutController",
	"migrate --target-schema":                      "refuses loudly on a flat-namespace target: migcore.ValidateTargetSchema (pipeline/migrate.go)",
	"sync start --target-schema":                   "refuses loudly on a flat-namespace target: migcore.ValidateTargetSchema (pipeline/streamer.go)",
	"restore --target-schema":                      "refuses loudly on a flat-namespace target: migcore.ValidateTargetSchema (pipeline/backup/restore.go)",
	"schema add-table --target-schema":             "refuses loudly on a flat-namespace target: migcore.ValidateTargetSchema (pipeline/add_table.go)",
	"schema diff --target-schema":                  "refuses loudly on a flat-namespace target: migcore.ValidateTargetSchema (pipeline/diff.go)",
	"schema preview --target-schema":               "refuses loudly on a flat-namespace target: migcore.ValidateTargetSchema (pipeline/preview.go)",
	"matview refresh --target-driver":              "refuses loudly on a non-postgres driver: MatviewRefreshCmd.Run",

	// --- already warns ---
	"sync start --notify-dead-tuple-ratio": "the pipeline already WARNs once on a target without vacuum health (pipeline/vacuum_health_notify.go)",

	// --- recorded decision ---
	"migrate --planetscale-org": "deliberately silent on a non-planetscale target: the value routinely arrives from the ambient PLANETSCALE_ORG env var and is shared with the telemetry opt-in, " +
		"so a WARN would fire on every unrelated run (composePlanetScaleIndexFallback's own comment records the decision)",
}
