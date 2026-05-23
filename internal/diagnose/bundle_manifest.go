// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package diagnose

import (
	"errors"
	"fmt"
	"strings"
)

// PrivacyLevel scopes which subsystems contribute to the operator
// bundle. The three levels are concentric — each higher level is a
// strict superset of the levels below. See ADR-0056 for the exact
// inclusion / exclusion contract pinned per level.
type PrivacyLevel int

const (
	// PrivacyUnset is the zero value — callers MUST set a level
	// explicitly. The assembler rejects PrivacyUnset rather than
	// silently defaulting (loud-failure tenet).
	PrivacyUnset PrivacyLevel = iota

	// PrivacyBasic includes ONLY state-table dumps for the requested
	// stream-id:
	//
	//   - sluice_cdc_state row for --stream-id.
	//   - sluice_cdc_schema_history rows for --stream-id (capped at
	//     SchemaHistoryRowCap most-recent boundaries).
	//   - sluice_shard_consolidation_lease rows visible to the
	//     applier (ADR-0054 Shape A live coordination state).
	//
	// EXCLUDES: version metadata, DSN locators (even redacted), engine
	// health probes, per-table row counts, log samples, capabilities,
	// CLI arguments. PrivacyBasic is the safest default for the
	// auto-on-crash hook because an unattended bundle landing on disk
	// shouldn't carry any signal an operator hasn't explicitly
	// authorised.
	PrivacyBasic

	// PrivacyStandard adds operational metadata on top of
	// PrivacyBasic:
	//
	//   - sluice version + commit + Go runtime + OS.
	//   - DSN-redacted CLI arguments and effective config.
	//   - Engine health probes (ir.HealthReporter + ir.BytesLagReporter
	//     + ir.SlotSpillReporter output, mirrors `sluice sync health`).
	//   - ir.DiagnoseProber engine snapshot (PG slot state, MySQL
	//     master-status, etc.) for both source and target.
	//   - Engine declared Capabilities.
	//
	// EXCLUDES: per-table row counts (slow COUNT(*) — opt-in only),
	// log samples (size + content sensitivity).
	PrivacyStandard

	// PrivacyVerbose is the operator's "I am happy to share
	// everything that isn't row data" level:
	//
	//   - PrivacyStandard +
	//   - Per-table row counts on the target (one COUNT(*) per
	//     filtered table; slow path on large tables — see ADR-0056
	//     for the warning).
	//   - Last N lines of sluice's slog output, when the operator has
	//     configured --log-file (the assembler reads the file from
	//     the configured path — sluice does NOT inspect the parent
	//     process's stderr).
	//
	// PrivacyVerbose still excludes row-level data. An operator who
	// needs to share row content is expected to do so out-of-band; the
	// diagnose bundle is for server-state diagnostics, not data dumps.
	PrivacyVerbose
)

// String renders the privacy level in the same lowercase form the CLI
// flag accepts. Stable across releases — bundle manifests embed it.
func (p PrivacyLevel) String() string {
	switch p {
	case PrivacyBasic:
		return "basic"
	case PrivacyStandard:
		return "standard"
	case PrivacyVerbose:
		return "verbose"
	}
	return "unset"
}

// ParsePrivacyLevel maps the kong-flag string to the typed level.
// Refuses the empty string and unknown values loudly — the caller
// should never reach here with an unrecognised value because kong's
// enum tag already filters at parse time, but the defensive check
// catches any drift between the enum and the parser.
func ParsePrivacyLevel(s string) (PrivacyLevel, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "basic":
		return PrivacyBasic, nil
	case "standard":
		return PrivacyStandard, nil
	case "verbose":
		return PrivacyVerbose, nil
	case "":
		return PrivacyUnset, errors.New("diagnose: privacy level is empty (expected basic|standard|verbose)")
	}
	return PrivacyUnset, fmt.Errorf("diagnose: unknown privacy level %q (expected basic|standard|verbose)", s)
}

// SchemaHistoryRowCap is the maximum count of most-recent
// schema-history rows the bundle assembler embeds. ADR-0056 pins this
// at 100 — high enough that the operator filing a bug against a long-
// running stream sees the recent DDL window (the usual cause), low
// enough that the bundle stays small.
const SchemaHistoryRowCap = 100

// VerboseLogTailLines is the maximum count of log lines the
// PrivacyVerbose level includes from the operator's --log-file. ADR-
// 0056 pins this at 200; the tail size matters less than the BOUND
// (operators with huge log files shouldn't ship them in the bundle).
const VerboseLogTailLines = 200

// Manifest is the JSON document at bundle.json carrying the bundle's
// metadata. The shape is stable — version-bumped via ManifestVersion
// when an incompatible change is needed.
type Manifest struct {
	// ManifestVersion is the bundle-format version. The bundle reader
	// branches on this; an older reader against a newer bundle should
	// refuse loudly rather than silently mis-decode.
	ManifestVersion int `json:"manifest_version"`

	// GeneratedAt is the wall-clock instant the bundle was assembled,
	// in RFC3339 UTC. Echoed in the bundle filename when the auto-on-
	// crash hook writes it.
	GeneratedAt string `json:"generated_at"`

	// SluiceVersion + SluiceCommit + SluiceBuildDate identify the
	// sluice binary that produced the bundle. Populated at
	// PrivacyStandard and above; absent at PrivacyBasic.
	SluiceVersion   string `json:"sluice_version,omitempty"`
	SluiceCommit    string `json:"sluice_commit,omitempty"`
	SluiceBuildDate string `json:"sluice_build_date,omitempty"`

	// GoVersion + GOOS + GOARCH identify the Go runtime. Populated at
	// PrivacyStandard and above; absent at PrivacyBasic.
	GoVersion string `json:"go_version,omitempty"`
	GOOS      string `json:"goos,omitempty"`
	GOARCH    string `json:"goarch,omitempty"`

	// PrivacyLevel echoes the level the bundle was assembled at —
	// the operator + the recipient (sluice maintainer) both need to
	// know what's in vs out.
	PrivacyLevel string `json:"privacy_level"`

	// StreamID is the --stream-id the bundle was scoped to. Always
	// populated; the bundle is meaningless without it.
	StreamID string `json:"stream_id"`

	// SourceDSNRedacted + TargetDSNRedacted are the redacted DSN
	// locators (host:port + database name only). Populated at
	// PrivacyStandard and above; absent at PrivacyBasic.
	SourceDSNRedacted string `json:"source_dsn_redacted,omitempty"`
	TargetDSNRedacted string `json:"target_dsn_redacted,omitempty"`

	// SourceEngine + TargetEngine are the engine NAMES (mysql,
	// postgres, planetscale). Always populated when the engines were
	// resolved; absent if the engine name couldn't be resolved at all
	// (the DSN flag was malformed).
	SourceEngine string `json:"source_engine,omitempty"`
	TargetEngine string `json:"target_engine,omitempty"`

	// CrashContext is set when the bundle was written by the auto-on-
	// crash hook, NOT by an explicit `sluice diagnose` invocation.
	// Carries the original error string the hook caught so the
	// recipient sees the crash signal at the top of the bundle.
	// Empty for operator-initiated bundles.
	CrashContext string `json:"crash_context,omitempty"`
}

// ManifestVersion is the current bundle-format version. Bump when the
// shape of files-in-the-zip changes incompatibly; new readers
// branching on this can still parse older bundles.
const ManifestVersion = 1
