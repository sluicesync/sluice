// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"reflect"
	"strings"
	"testing"

	"github.com/alecthomas/kong"
)

// TestSyncSpecFlagParity is the DEVEX-1 class-gate (audit 2026-07-23 /
// G-14): every `sync start` flag must be EXPLICITLY classified against
// the fleet `sync run` spec — either it has a SyncSpec koanf sibling
// (same kebab-case name), or it is declared below with a rationale for
// why the fleet surface deliberately lacks it. The audit found the gap
// this gate ratchets: the same delta that plumbed the notify flags into
// SyncSpec missed `--publication-name`, leaving the ADR-0175 refusal's
// PRIMARY documented escape unexpressible on the surface most likely to
// need it (two differently-scoped PG-source fleet legs). With this
// test, a new per-stream flag cannot ship half-plumbed: adding it to
// SyncStartCmd without a spec sibling or an exclusion entry fails here.
//
// The flag inventory comes from kong's REAL model (the Bug-180 lesson:
// enumerate through the actual parser, not a hand-kept list), and the
// spec inventory from SyncSpec's koanf tags via reflection.

// syncStartFleetExclusions lists `sync start` flags that DELIBERATELY
// have no SyncSpec key, each with the reason. Two kinds of entry:
// "cli-only" semantics (interactive/one-shot/process-shaped knobs that
// make no sense in a supervised fleet spec) and "curation gap"
// (per-stream-meaningful, but consciously outside the ADR-0122 §3
// curated subset — adding one to the fleet is a deliberate design
// decision, not plumbing). An entry here that GAINS a spec sibling
// fails below until it is removed (self-tidying, the
// check-shard-coverage DEFENSIVE_PACKAGES pattern).
var syncStartFleetExclusions = map[string]string{
	// ---- interactive / one-shot / process-shaped (cli-only) ----
	"dry-run":                   "one-shot planning output; a supervised fleet spec is not a plan preview surface",
	"format":                    "stdout envelope shape of the single-stream process; the fleet has its own status panel",
	"yes":                       "interactive confirmation bypass; fleet specs must never pre-authorize destructive resets",
	"diagnose-on-crash-dir":     "crash-diagnostics bundle knob (CrashHookFlags); process-shaped, not per-stream config",
	"diagnose-on-crash-privacy": "see diagnose-on-crash-dir",
	"force-cold-start":          "destructive one-shot recovery override; deliberate per-invocation, never durable config",
	"reset-target-data":         "destructive one-shot recovery; requires typed confirmation, meaningless as standing config",
	"restart-from-scratch":      "destructive one-shot recovery; a standing 'always restart' key would re-copy on every supervisor restart",
	"no-auto-resnapshot":        "recovery-posture override tied to the interactive recovery flow above; not in the curated subset (ADR-0122 §3)",

	// ---- position-from-manifest / broker resume family (cli-only) ----
	"position-from-manifest": "one-shot resume-from-backup entry point; the fleet path is `sync from-backup` (the broker)",
	"strict-preflight":       "modifier of --position-from-manifest's soft warnings; travels with it",
	"patroni-mode":           "modifier of the position-from-manifest preflight; travels with it",
	"backup-endpoint":        "S3 override for --position-from-manifest; travels with it",
	"backup-region":          "S3 override for --position-from-manifest; travels with it",
	"backup-path-style":      "S3 override for --position-from-manifest; travels with it",

	// ---- curation gaps: per-stream-meaningful, deliberately outside the
	// ADR-0122 §3 curated subset. Adding one is a design decision — move
	// the name OUT of this map in the same change that adds the koanf key.
	"include-database":                  "multi-database fan-out (ADR-0074) is a whole-topology choice; fleet legs are single-scope today",
	"exclude-database":                  "see include-database",
	"all-databases":                     "see include-database",
	"include-schema":                    "multi-schema fan-out (ADR-0075); see include-database",
	"exclude-schema":                    "see include-schema",
	"all-schemas":                       "see include-schema",
	"map-database":                      "namespace rename rides the fan-out surface (ADR-0142); see include-database",
	"map-schema":                        "see map-database",
	"include-view":                      "cold-start view filtering; not yet curated into the fleet subset",
	"exclude-view":                      "see include-view",
	"skip-views":                        "see include-view",
	"skip-foreign-keys":                 "cold-start schema-shape knob; not yet curated into the fleet subset",
	"include-orm-tables":                "cold-start table-selection nuance; not yet curated into the fleet subset",
	"skip-orm-tables":                   "see include-orm-tables",
	"where-strict-collation":            "strictness modifier of `where`; not yet curated (fleet `where` uses the faithful default)",
	"schema-already-applied":            "cold-start DDL skip promise; not yet curated into the fleet subset",
	"apply-tune-target-latency":         "AIMD tuning override; not yet curated (fleet uses engine defaults)",
	"index-build-mem":                   "cold-start index-phase tuning; not yet curated into the fleet subset",
	"index-build-parallelism":           "see index-build-mem",
	"bulk-parallelism":                  "cold-start copy tuning (ADR-0079); not yet curated into the fleet subset",
	"table-parallelism":                 "see bulk-parallelism",
	"bulk-parallel-min-rows":            "see bulk-parallelism",
	"bulk-batch-size":                   "see bulk-parallelism",
	"copy-fanout-degree":                "see bulk-parallelism",
	"vstream-copy-table-parallelism":    "cold-copy read-axis tuning; DSN param form works in fleet specs today",
	"copy-table-parallelism":            "see vstream-copy-table-parallelism",
	"vstream-preserve-skew":             "VStream skew posture; DSN param form works in fleet specs today",
	"no-intra-table-stealing":           "cold-copy work-stealing opt-out; not yet curated into the fleet subset",
	"no-float-exact-reread":             "VStream FLOAT repair opt-out; not yet curated into the fleet subset",
	"raw-copy-format":                   "same-engine raw-copy wire format; not yet curated into the fleet subset",
	"max-target-connections":            "connection-budget ceiling; not yet curated into the fleet subset",
	"reap-stale-backends":               "backend-reaping authorization; not yet curated into the fleet subset",
	"enable-pg-extension":               "extension passthrough opt-in; not yet curated into the fleet subset",
	"no-coordinate-live-ddl":            "Shape-A DDL-coordination opt-out; rides inject-shard-column's deeper surface (ADR-0054)",
	"forward-schema-add-column":         "DEPRECATED alias of schema-changes; must never gain a spec key",
	"backfill-added-column":             "ADD-COLUMN backfill opt-in (ADR-0058 §1c); not yet curated into the fleet subset",
	"shard-coordination-lease-duration": "Shape-A lease tuning (ADR-0054); not yet curated into the fleet subset",
	"shard-coordination-renew-deadline": "see shard-coordination-lease-duration",
	"shard-coordination-retry-period":   "see shard-coordination-lease-duration",
	"redact":                            "PII redaction rules; the fleet story needs a dictionaries/keyset design first (YAML `redaction:` blocks exist on the config side)",
	"keyset-source":                     "see redact",
	"source-heartbeat-interval":         "F17 source-side heartbeat writer; not yet curated into the fleet subset",
	"source-heartbeat-prune-window":     "see source-heartbeat-interval",
	"source-heartbeat-table-name":       "see source-heartbeat-interval",
	"no-source-heartbeat":               "see source-heartbeat-interval",
	"auto-prune-change-log":             "trigger-CDC change-log reaping; not yet curated into the fleet subset",
	"auto-prune-interval":               "see auto-prune-change-log",
	"auto-prune-keep":                   "see auto-prune-change-log",
	"source-tls-ca":                     "TLS CA paths; not yet curated into the fleet subset (DSN param forms work today)",
	"target-tls-ca":                     "see source-tls-ca",
	"notify-smtp-password":              "env-only secret (SLUICE_NOTIFY_SMTP_PASSWORD); a YAML key would invite committed credentials",
}

// specKoanfKeys reflects SyncSpec's koanf tags.
func specKoanfKeys(t *testing.T) map[string]bool {
	t.Helper()
	keys := map[string]bool{}
	typ := reflect.TypeOf(SyncSpec{})
	for i := 0; i < typ.NumField(); i++ {
		tag := typ.Field(i).Tag.Get("koanf")
		if tag == "" {
			continue
		}
		keys[strings.Split(tag, ",")[0]] = true
	}
	if len(keys) < 30 {
		t.Fatalf("reflected only %d koanf keys from SyncSpec — reflection broke", len(keys))
	}
	return keys
}

// syncStartFlagNames enumerates `sync start`'s flags through kong's real
// model.
func syncStartFlagNames(t *testing.T) []string {
	t.Helper()
	var cli struct {
		Start SyncStartCmd `cmd:""`
	}
	parser, err := kong.New(&cli, kong.Exit(func(int) {}))
	if err != nil {
		t.Fatalf("kong.New: %v", err)
	}
	node := parser.Model.Children[0]
	names := make([]string, 0, len(node.Flags))
	for _, f := range node.Flags {
		names = append(names, f.Name)
	}
	if len(names) < 60 {
		t.Fatalf("kong model enumerated only %d flags for `sync start` — enumeration broke", len(names))
	}
	return names
}

func TestSyncSpecFlagParity(t *testing.T) {
	spec := specKoanfKeys(t)
	flags := syncStartFlagNames(t)
	flagSet := map[string]bool{}

	for _, name := range flags {
		flagSet[name] = true
		hasSibling := spec[name]
		rationale, excluded := syncStartFleetExclusions[name]
		switch {
		case hasSibling && excluded:
			t.Errorf("flag --%s has a SyncSpec sibling AND an exclusion entry — remove the stale exclusion (rationale was: %s)", name, rationale)
		case !hasSibling && !excluded:
			t.Errorf("flag --%s is UNCLASSIFIED: add a SyncSpec koanf sibling (plumb it through buildStreamerFromSpec) or declare it in syncStartFleetExclusions with a rationale — the DEVEX-1 half-plumbed-flag class (audit 2026-07-23 G-14)", name)
		}
	}

	// Exclusion entries must name real flags (rename/removal hygiene).
	for name := range syncStartFleetExclusions {
		if !flagSet[name] {
			t.Errorf("syncStartFleetExclusions entry %q matches no `sync start` flag — the flag was renamed or removed; drop the stale entry", name)
		}
	}

	// Reverse: every SyncSpec key must correspond to a `sync start` flag,
	// except the declared spec-only keys (per-sync forms of GLOBAL flags).
	specOnly := map[string]string{
		"zero-date": "per-sync form of the process-global --zero-date (ADR-0127); sugar over the source DSN's zero_date param",
	}
	for key := range spec {
		if !flagSet[key] && specOnly[key] == "" {
			t.Errorf("SyncSpec key %q has no `sync start` flag of that name — the flag was renamed without the spec, or the key needs a specOnly rationale", key)
		}
	}
	for key, why := range specOnly {
		if !spec[key] {
			t.Errorf("specOnly entry %q (%s) is not a SyncSpec key any more — drop the stale entry", key, why)
		}
		if flagSet[key] {
			t.Errorf("specOnly entry %q now has a `sync start` flag — it is no longer spec-only; remove the entry", key)
		}
	}
}
