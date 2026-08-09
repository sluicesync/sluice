// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/alecthomas/kong"
)

// The migrate ↔ sync-start FLAG-SURFACE gate, and the construction-site
// ASSIGNMENT gate that backs it.
//
// # Why these exist
//
// CLAUDE.md's working agreement — "an operator-facing flag/capability
// touching a shared pipeline phase applies to BOTH `migrate` and `sync`
// cold-start unless a written reason says otherwise" — was mechanically
// enforced by exactly one test, [pipeline.TestCopyPhaseFlagParityMigratorStreamer],
// and that test could not enforce it. It iterated a HAND-CURATED list of
// seven flag names, so a new one-sided flag passed silently: the gate
// required a human to do the very thing the gate existed to guarantee.
// And it checked only struct-field PRESENCE via reflect.FieldByName, so a
// field that exists and is never assigned, or is assigned a hardcoded
// constant at the call site, passed too. Two live escapees proved both
// halves (2026-08-01):
//
//   - `--no-intra-table-stealing` lived on the Streamer and the sync CLI,
//     was absent from the Migrator, and was read as the Go zero value —
//     invisible to a curated list that never named it.
//   - `Streamer.CoordinateLiveDDL` (now [pipeline.Streamer.NoCoordinateLiveDDL])
//     was PRESENT on the struct, documented "default true", and assigned
//     in exactly one of the two production construction sites — so the
//     field-presence check was green while the fleet path silently ran the
//     opposite DDL posture.
//
// So this file inverts the polarity. Both gates FAIL BY DEFAULT:
//
//   - [TestMigrateSyncFlagSurfaceParity] diffs the two commands' REAL kong
//     flag sets and requires every difference to carry a written
//     architectural reason in [migrateSyncDivergenceReason]. A flag added
//     to one side and not the other fails the build until someone wires it
//     or records why not.
//   - [TestSharedPhaseFieldAssignmentParity] walks the AST of every
//     production `pipeline.Streamer` / `pipeline.Migrator` construction
//     site in cmd/sluice and requires each shared-pipeline-phase field to
//     be ASSIGNED there — declaration is not enough.
//
// Both carry an anti-vacuity floor (the internal/errclassgate MinSites
// pattern): a walk that finds no sites FAILS rather than passing silently.

// ---------------------------------------------------------------------------
// Gate 1 — the migrate ↔ sync-start flag surface
// ---------------------------------------------------------------------------

// The recurring divergence reasons, named once. A shared constant means the
// reason really IS the same for every flag that cites it — where it is not,
// the entry carries its own prose.
const (
	// reasonCDCApply: the steady-state change-apply loop. `migrate` is a
	// one-shot copy that finishes; there is no continuous apply to tune.
	reasonCDCApply = "inapplicable: tunes the steady-state CDC apply loop, which `migrate` (a one-shot copy that terminates) does not have"

	// reasonCDCSource: the replication-source plumbing (slot/publication/
	// position/heartbeat/change-log) that only a following stream owns.
	reasonCDCSource = "inapplicable: CDC source-side plumbing (slot / publication / position / heartbeat / change-log) that exists only for a following stream"

	// reasonResumeFromBackup: `sync start --position-from-manifest` and its
	// S3 overrides — an entry point into a stream, not into a copy.
	reasonResumeFromBackup = "inapplicable: modifier of --position-from-manifest, the resume-a-stream-from-a-backup entry point; `migrate` has no position to resume from"

	// reasonLongRunningProcess: metrics endpoints and threshold alerting
	// only make sense for a process that keeps running.
	reasonLongRunningProcess = "inapplicable: observability/alerting for a long-running supervised process; `migrate` runs to completion and reports once"

	// reasonVStreamSnapshot: the VStream (PlanetScale/Vitess) CDC snapshot
	// COPY reader. `migrate` reads over a plain SQL RowReader and never
	// opens a VStream snapshot, so none of its properties apply.
	reasonVStreamSnapshot = "inapplicable: governs the VStream CDC-snapshot COPY reader; `migrate` reads over a plain SQL RowReader and never opens one"

	// reasonShapeALiveDDL: ADR-0054 live cross-shard DDL coordination
	// between concurrently-following per-shard streams.
	reasonShapeALiveDDL = "inapplicable: ADR-0054 coordination BETWEEN concurrently-following per-shard streams; `migrate` is the one-shot DDL apply those streams coordinate around"
)

// migrateSyncDivergenceReason records, per flag, why it exists on exactly
// one of `migrate` / `sync start`. Every entry is one of two kinds and says
// which in its first word:
//
//	inapplicable: — the phase or surface the flag governs does not exist on
//	               the other entry point. Verified against the code, not the
//	               help text.
//	GAP:          — it WOULD be meaningful on the other side and is simply
//	               not wired. Recorded so it is a decision on the record
//	               rather than an accident, and so it can be found again.
//
// A flag missing from this map fails the gate. Removing a flag from one
// command without dropping its entry fails too (stale-entry hygiene below).
var migrateSyncDivergenceReason = map[string]string{
	// ---- migrate-only ----
	"allow-degraded-fks": "GAP: the sync cold-start runs the SAME CreateConstraints phase against the same PG target and can hit the same SQLSTATE 23503 on a dirty source, but ir.DegradedFKAllower is armed only at migrate's writer-open (migrate.go's AllowDegradedFKs block). Not wired to the Streamer; filed 2026-08-01",
	"infer-types":        "inapplicable: ADR-0144 rich-type inference is refused for every source but sqlite / d1 / flat-file, and NONE of those can be a `sync start` source (sqlite + d1 declare CDCNone; flat-file has no CDC reader)",
	"stage-local":        "inapplicable: ADR-0145 D1 local staging; refused for any source but d1, which declares CDCNone and so is never a sync source",
	"no-stage-local":     "inapplicable: the opt-out half of --stage-local; travels with it",
	"migration-id":       "paired, not missing: this is migrate's run-identity key (`sluice_migrate_state`); sync's is --stream-id, which is sync-only for the mirror-image reason",
	"resume":             "paired, not missing: migrate opts INTO resuming a recorded migration; a sync stream resumes from its persisted position automatically, with no flag to ask for it",

	"allow-unmapped-tables": "inapplicable: the condition cannot arise on migrate, which CREATES the target tables it copies into (schema-apply phase) rather than requiring them to pre-exist. The refusal it opts out of (audit backlog C-11) fires only where a CDC stream carries changes for a table the target never got — a steady-state apply concern with no migrate counterpart",

	// ---- sync-only: steady-state CDC apply loop ----
	"apply-batch-size":          reasonCDCApply,
	"apply-concurrency":         reasonCDCApply,
	"apply-delay":               reasonCDCApply,
	"apply-exec-timeout":        reasonCDCApply,
	"apply-retry-attempts":      reasonCDCApply,
	"apply-retry-backoff-base":  reasonCDCApply,
	"apply-retry-backoff-cap":   reasonCDCApply,
	"apply-tune-target-latency": reasonCDCApply,
	"no-auto-tune":              reasonCDCApply,
	"poll-interval":             reasonCDCApply,
	"heartbeat-interval":        reasonCDCApply,

	// ---- sync-only: CDC source plumbing / stream identity ----
	"slot-name":                     reasonCDCSource,
	"publication-name":              reasonCDCSource,
	"stream-id":                     "paired, not missing: sync's run-identity key; migrate's is --migration-id",
	"schema-changes":                reasonCDCSource,
	"forward-schema-add-column":     reasonCDCSource,
	"backfill-added-column":         reasonCDCSource,
	"no-auto-resnapshot":            reasonCDCSource,
	"restart-from-scratch":          reasonCDCSource,
	"source-heartbeat-interval":     reasonCDCSource,
	"source-heartbeat-prune-window": reasonCDCSource,
	"source-heartbeat-table-name":   reasonCDCSource,
	"no-source-heartbeat":           reasonCDCSource,
	"auto-prune-change-log":         reasonCDCSource,
	"auto-prune-interval":           reasonCDCSource,
	"auto-prune-keep":               reasonCDCSource,
	"control-keyspace":              "GAP: the sidecar-keyspace resolution (applyControlKeyspace) reaches `sync start`, `sync run`, `sync health`, `sync decommission`, `schema add-table` and `backup restore` — but not `migrate`, which also writes a control table (`sluice_migrate_state`) on a possibly-sharded Vitess target. Filed 2026-08-01",

	// ---- sync-only: resume-a-stream-from-a-backup ----
	"position-from-manifest": reasonResumeFromBackup,
	"strict-preflight":       reasonResumeFromBackup,
	"patroni-mode":           reasonResumeFromBackup,
	"backup-endpoint":        reasonResumeFromBackup,
	"backup-region":          reasonResumeFromBackup,
	"backup-path-style":      reasonResumeFromBackup,

	// ---- sync-only: long-running-process observability + alerting ----
	"metrics-listen":                  reasonLongRunningProcess,
	"suppress-target-metrics-history": reasonLongRunningProcess,
	"planetscale-metrics-branch":      reasonLongRunningProcess,
	"planetscale-metrics-db":          reasonLongRunningProcess,
	"planetscale-metrics-token":       reasonLongRunningProcess,
	"planetscale-metrics-token-id":    reasonLongRunningProcess,
	"notify-cooldown":                 reasonLongRunningProcess,
	"notify-cpu-util":                 reasonLongRunningProcess,
	"notify-dead-tuple-ratio":         reasonLongRunningProcess,
	"notify-lag-seconds":              reasonLongRunningProcess,
	"notify-mem-util":                 reasonLongRunningProcess,
	"notify-schema-drift":             reasonLongRunningProcess,
	"notify-slack":                    reasonLongRunningProcess,
	"notify-slot-health":              reasonLongRunningProcess,
	"notify-smtp-auth":                reasonLongRunningProcess,
	"notify-smtp-from":                reasonLongRunningProcess,
	"notify-smtp-host":                reasonLongRunningProcess,
	"notify-smtp-password":            reasonLongRunningProcess,
	"notify-smtp-port":                reasonLongRunningProcess,
	"notify-smtp-tls":                 reasonLongRunningProcess,
	"notify-smtp-to":                  reasonLongRunningProcess,
	"notify-smtp-username":            reasonLongRunningProcess,
	"notify-storage-growth-per-min":   reasonLongRunningProcess,
	"notify-storage-util":             reasonLongRunningProcess,
	"notify-sync-lag-seconds":         reasonLongRunningProcess,
	"notify-webhook":                  reasonLongRunningProcess,
	"notify-xid-age":                  reasonLongRunningProcess,

	// ---- sync-only: cold-start copy knobs scoped to a sync-only mechanism ----
	//
	// These four look like copy-phase parity candidates and are not. The
	// discriminator is which orchestrator entry point READS them:
	// runBulkCopyWithOpts is called ONLY from the two sync cold-start paths
	// (streamer_coldstart.go, streamer_multidb.go) — `migrate` drives
	// runBulkCopyPhases, whose within-table concurrency is the ADR-0123
	// keyset-chunk + copy-budget gate, a different mechanism these knobs do
	// not govern. Verified by call-site walk, not by reading the help text.
	"copy-fanout-degree":      "inapplicable: ADR-0097 WRITE-side fan-out for the idempotent CDC-snapshot cold-start copy, read only inside runBulkCopyWithOpts (sync cold-start only); migrate's within-table axis is the ADR-0123 keyset-chunk + copy-budget gate",
	"no-intra-table-stealing": "inapplicable: ADR-0119 intra-table PK-range work-stealing lives in runConcurrentTableCopy, reachable only from runBulkCopyWithOpts (sync cold-start only); migrate chunks within a table through the ADR-0123 gate instead, which this flag does not govern",

	"no-float-exact-reread":          reasonVStreamSnapshot,
	"strict-float":                   reasonVStreamSnapshot,
	"vstream-copy-table-parallelism": reasonVStreamSnapshot,
	"copy-table-parallelism":         reasonVStreamSnapshot,
	"vstream-preserve-skew":          reasonVStreamSnapshot,

	// ---- sync-only: Shape-A live cross-shard DDL coordination ----
	"no-coordinate-live-ddl":            reasonShapeALiveDDL,
	"shard-coordination-lease-duration": reasonShapeALiveDDL,
	"shard-coordination-renew-deadline": reasonShapeALiveDDL,
	"shard-coordination-retry-period":   reasonShapeALiveDDL,

	// ---- sync-only: the rest ----
	"where-strict-collation": "inapplicable: ADR-0174 Piece 1's strictness applies to the CDC leg's CLIENT-SIDE predicate evaluator (buildWhereCDCFilter is its only consumer). migrate pushes --where down to the source, which applies its own collation natively — there is no client-side re-evaluation to be strict about",
	"schema-already-applied": "GAP: the promise \"the target catalog is already correct, skip the DDL phases\" is equally meaningful for a migrate that is only moving rows, but only the Streamer has the flag and the SkipSchemaApply plumbing. Not wired to the Migrator; filed 2026-08-01",
}

// commandFlagNames enumerates one command's flags through kong's REAL model
// (the Bug-180 lesson: enumerate through the actual parser, never a
// hand-kept list). floor is the anti-vacuity minimum.
func commandFlagNames(t *testing.T, cli any, floor int, what string) map[string]bool {
	t.Helper()
	kp, err := kong.New(cli, kong.Exit(func(int) {}))
	if err != nil {
		t.Fatalf("kong.New(%s): %v", what, err)
	}
	node := kp.Model.Children[0]
	names := make(map[string]bool, len(node.Flags))
	for _, f := range node.Flags {
		names[f.Name] = true
	}
	if len(names) < floor {
		t.Fatalf("kong enumerated only %d flags for %s (floor %d) — enumeration broke; re-point the gate", len(names), what, floor)
	}
	return names
}

func TestMigrateSyncFlagSurfaceParity(t *testing.T) {
	var migCLI struct {
		Migrate MigrateCmd `cmd:""`
	}
	var synCLI struct {
		Start SyncStartCmd `cmd:""`
	}
	migrateFlags := commandFlagNames(t, &migCLI, 50, "`migrate`")
	syncFlags := commandFlagNames(t, &synCLI, 100, "`sync start`")

	var unexplained []string
	for name := range migrateFlags {
		if !syncFlags[name] && strings.TrimSpace(migrateSyncDivergenceReason[name]) == "" {
			unexplained = append(unexplained, "--"+name+" (on `migrate`, not on `sync start`)")
		}
	}
	for name := range syncFlags {
		if !migrateFlags[name] && strings.TrimSpace(migrateSyncDivergenceReason[name]) == "" {
			unexplained = append(unexplained, "--"+name+" (on `sync start`, not on `migrate`)")
		}
	}
	sort.Strings(unexplained)
	if len(unexplained) > 0 {
		t.Errorf("%d flag(s) exist on exactly ONE of `migrate` / `sync start` with no recorded reason:\n  %s\n\n"+
			"CLAUDE.md: a flag touching a shared pipeline phase applies to BOTH entry points unless a written reason "+
			"says otherwise. Either wire it to the sibling, or add an entry to migrateSyncDivergenceReason saying "+
			"'inapplicable: <the phase does not exist there>' or 'GAP: <what would need wiring>'. Do not write a "+
			"reason you have not checked against the code — a decorative reason is worse than no gate.",
			len(unexplained), strings.Join(unexplained, "\n  "))
	}

	// Stale-entry hygiene, both directions: an entry must name a flag that
	// exists on exactly one of the two commands. Present on BOTH means the
	// divergence was closed and the entry must go; present on NEITHER means
	// the flag was renamed or removed.
	for name, reason := range migrateSyncDivergenceReason {
		onMig, onSyn := migrateFlags[name], syncFlags[name]
		switch {
		case onMig && onSyn:
			t.Errorf("migrateSyncDivergenceReason[%q] records a divergence, but the flag now exists on BOTH commands — "+
				"drop the entry so the gate enforces parity (recorded reason was: %s)", name, reason)
		case !onMig && !onSyn:
			t.Errorf("migrateSyncDivergenceReason[%q] matches NO flag on either command — it was renamed or removed; "+
				"drop or update the stale entry", name)
		}
	}
}

// ---------------------------------------------------------------------------
// Gate 2 — construction-site assignment parity
// ---------------------------------------------------------------------------

// sharedPhaseFields are the orchestrator fields that carry a shared-pipeline-
// phase capability (schema-apply / bulk-copy / index build / constraint add /
// query-timeout / redaction / shard injection / DDL posture). Declaring one
// is not enough — every production construction site must ASSIGN it, or say
// why not. Keyed by the orchestrator type name.
var sharedPhaseFields = map[string][]string{
	"Migrator": {
		"IndexBuildFallback",
		"Redactor",
		"InjectShardColumn",
		"AllowCrossShardMerge",
		"BulkBatchSize",
		"UpfrontIndexes",
		"AnalyzeAfter",
		"RaiseQueryTimeout",
	},
	"Streamer": {
		"IndexBuildFallback",
		"Redactor",
		"InjectShardColumn",
		"AllowCrossShardMerge",
		"BulkBatchSize",
		"UpfrontIndexes",
		"AnalyzeAfter",
		"RaiseQueryTimeout",
		// Not a copy-phase field, but the one whose single-site assignment
		// was the v0.99.51 trap this gate was built to catch. It governs the
		// DDL posture of a shared phase, which is the same contract.
		"NoCoordinateLiveDDL",
	},
}

// sharedPhaseFieldFlag maps a field to the `sync start` flag that sets it, so
// a fleet-curation exemption can be DELEGATED to syncStartFleetExclusions
// instead of duplicating its prose here (see reasonFleetCurated).
var sharedPhaseFieldFlag = map[string]string{
	"Redactor":          "redact",
	"BulkBatchSize":     "bulk-batch-size",
	"UpfrontIndexes":    "upfront-indexes",
	"AnalyzeAfter":      "analyze-after",
	"RaiseQueryTimeout": "planetscale-raise-query-timeout",
}

// reasonFleetCurated delegates an omission to [syncStartFleetExclusions]: the
// gate then REQUIRES a live exclusion entry for the field's flag. Curating the
// flag into the fleet spec drops that entry, which makes this gate start
// demanding the assignment — one source of truth, no prose to drift.
const reasonFleetCurated = "fleet-curated"

// sharedPhaseUnassignedReason records, per "file.go:Type.Field", why a
// production construction site does NOT assign a shared-phase field.
var sharedPhaseUnassignedReason = map[string]string{
	// The fleet SyncSpec is a CURATED SUBSET of the `sync start` flag surface
	// (ADR-0122 §3). Each of these has its rationale in
	// syncStartFleetExclusions and is enforced there by TestSyncSpecFlagParity;
	// the delegation keeps the two gates from disagreeing.
	"sync_run.go:Streamer.Redactor":          reasonFleetCurated,
	"sync_run.go:Streamer.BulkBatchSize":     reasonFleetCurated,
	"sync_run.go:Streamer.UpfrontIndexes":    reasonFleetCurated,
	"sync_run.go:Streamer.AnalyzeAfter":      reasonFleetCurated,
	"sync_run.go:Streamer.RaiseQueryTimeout": reasonFleetCurated,
}

// constructionSite is one production `pipeline.X{...}` literal plus any
// `v.Field = …` assignments to the variable it was bound to in the same file.
type constructionSite struct {
	file     string
	typeName string
	assigned map[string]bool
}

// findConstructionSites AST-walks the package directory for composite
// literals of pipeline.<typeName> and collects, per site, the union of the
// literal's keys and the post-literal field assignments on the variable it
// was bound to (cli.go sets IndexBuildFallback / RaiseQueryTimeout /
// Redactor that way, after the literal). Test files are skipped — this gate
// is about PRODUCTION construction.
func findConstructionSites(t *testing.T, dir string, types map[string][]string) []*constructionSite {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read package dir %s: %v", dir, err)
	}
	fset := token.NewFileSet()

	var sites []*constructionSite
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(dir, name)
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", path, perr)
		}
		base := filepath.Base(path)

		// Pass 1: composite literals, and the variable each is bound to.
		boundVar := map[string]*constructionSite{}
		ast.Inspect(f, func(n ast.Node) bool {
			lit, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}
			sel, ok := lit.Type.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			pkgIdent, ok := sel.X.(*ast.Ident)
			if !ok || pkgIdent.Name != "pipeline" {
				return true
			}
			if _, want := types[sel.Sel.Name]; !want {
				return true
			}
			site := &constructionSite{file: base, typeName: sel.Sel.Name, assigned: map[string]bool{}}
			for _, elt := range lit.Elts {
				kv, ok := elt.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				if key, ok := kv.Key.(*ast.Ident); ok {
					site.assigned[key.Name] = true
				}
			}
			sites = append(sites, site)
			// Remember which variable (if any) this literal was assigned to,
			// so pass 2 can attribute `mig.Field = …` to it.
			if v := assignedVarName(f, lit); v != "" {
				boundVar[v] = site
			}
			return true
		})

		// Pass 2: `<boundVar>.Field = …` assignments.
		ast.Inspect(f, func(n ast.Node) bool {
			as, ok := n.(*ast.AssignStmt)
			if !ok {
				return true
			}
			for _, lhs := range as.Lhs {
				sel, ok := lhs.(*ast.SelectorExpr)
				if !ok {
					continue
				}
				recv, ok := sel.X.(*ast.Ident)
				if !ok {
					continue
				}
				if site := boundVar[recv.Name]; site != nil {
					site.assigned[sel.Sel.Name] = true
				}
			}
			return true
		})
	}
	return sites
}

// assignedVarName returns the identifier a composite literal was bound to
// (`mig := &pipeline.Migrator{…}`), or "" for an unbound literal such as a
// bare `return &pipeline.Streamer{…}`.
func assignedVarName(f *ast.File, lit *ast.CompositeLit) string {
	name := ""
	ast.Inspect(f, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok || len(as.Lhs) != 1 || len(as.Rhs) != 1 {
			return true
		}
		rhs := as.Rhs[0]
		if u, ok := rhs.(*ast.UnaryExpr); ok && u.Op == token.AND {
			rhs = u.X
		}
		if got, ok := rhs.(*ast.CompositeLit); !ok || got != lit {
			return true
		}
		if id, ok := as.Lhs[0].(*ast.Ident); ok {
			name = id.Name
		}
		return false
	})
	return name
}

func TestSharedPhaseFieldAssignmentParity(t *testing.T) {
	sites := findConstructionSites(t, ".", sharedPhaseFields)

	// Anti-vacuity floor (the errclassgate MinSites pattern). Today: the
	// Migrator in cli.go, the Streamer in cli.go (`sync start`), and the
	// Streamer in sync_run.go (the fleet). A walk that finds fewer has
	// stopped matching the real construction shape and would pass forever.
	if len(sites) < 3 {
		t.Fatalf("found only %d pipeline.Migrator/Streamer construction site(s) in cmd/sluice (floor 3); "+
			"the AST walk has stopped matching the real construction shape and is now vacuous — re-point it", len(sites))
	}
	perType := map[string]int{}
	for _, s := range sites {
		perType[s.typeName]++
	}
	for typeName := range sharedPhaseFields {
		if perType[typeName] == 0 {
			t.Fatalf("found ZERO pipeline.%s construction sites — the walk is vacuous for that type", typeName)
		}
	}

	for _, site := range sites {
		for _, field := range sharedPhaseFields[site.typeName] {
			if site.assigned[field] {
				continue
			}
			key := site.file + ":" + site.typeName + "." + field
			reason, recorded := sharedPhaseUnassignedReason[key]
			if !recorded {
				t.Errorf("%s constructs a pipeline.%s WITHOUT assigning %s — the field is declared but this site "+
					"silently takes its Go zero value (the v0.99.51 trap: CoordinateLiveDDL was assigned at exactly "+
					"one of two sites and the field-presence gate stayed green). Assign it, or record %q in "+
					"sharedPhaseUnassignedReason with the architectural reason.",
					site.file, site.typeName, field, key)
				continue
			}
			if reason == reasonFleetCurated {
				flag := sharedPhaseFieldFlag[field]
				if flag == "" {
					t.Errorf("%s is marked %s but %s has no entry in sharedPhaseFieldFlag, so the delegation checks "+
						"nothing — add the flag name", key, reasonFleetCurated, field)
					continue
				}
				if _, excluded := syncStartFleetExclusions[flag]; !excluded {
					t.Errorf("%s delegates to the fleet curation, but --%s no longer has a syncStartFleetExclusions "+
						"entry — the flag WAS curated into the fleet spec, so this site must now assign %s",
						key, flag, field)
				}
				continue
			}
			if strings.TrimSpace(reason) == "" {
				t.Errorf("sharedPhaseUnassignedReason[%q] must carry a non-empty architectural reason", key)
			}
		}
	}

	// Stale-entry hygiene: a recorded omission that is now assigned must be
	// dropped, so an exemption cannot outlive the gap it documents.
	for key := range sharedPhaseUnassignedReason {
		parts := strings.SplitN(key, ":", 2)
		if len(parts) != 2 {
			t.Errorf("sharedPhaseUnassignedReason key %q is malformed; want \"file.go:Type.Field\"", key)
			continue
		}
		tf := strings.SplitN(parts[1], ".", 2)
		if len(tf) != 2 {
			t.Errorf("sharedPhaseUnassignedReason key %q is malformed; want \"file.go:Type.Field\"", key)
			continue
		}
		matched := false
		for _, site := range sites {
			if site.file != parts[0] || site.typeName != tf[0] {
				continue
			}
			matched = true
			if site.assigned[tf[1]] {
				t.Errorf("sharedPhaseUnassignedReason[%q] records an omission, but the site now ASSIGNS %s — "+
					"drop the stale entry", key, tf[1])
			}
		}
		if !matched {
			t.Errorf("sharedPhaseUnassignedReason[%q] matches no construction site — the file or type was renamed; "+
				"drop or update the stale entry", key)
		}
	}
}
