// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package sluicecode defines sluice's stable operator-facing error
// codes, the [CodedError] wrapper that carries them through error
// chains, and the process exit-code taxonomy.
//
// A code names an error CLASS an operator (or an agent driving the
// CLI) can branch on without regexing prose: the human-facing message
// text stays exactly as it was, and the code + remedy hint ride along
// as structured metadata — surfaced as `code` and `hint` attributes
// on the slog record at the CLI exit boundary (visible under
// --log-format json) and extractable by callers via [FromError].
//
// Design tenets, mirroring the hint registry in internal/pipeline:
//
//   - Tiny, load-bearing registry. A code is a compatibility promise
//     forever — it is only minted for errors that already carry an
//     operator hint or a named remedy, and the registry grows
//     organically as new remedies earn one. No sweeping the codebase
//     assigning codes to every error.
//   - Codes are stable. Renaming or removing a published code is a
//     breaking change; the string, once shipped, is frozen.
//   - One place. This package is the single definition point; the
//     docs table (docs/operator/error-codes.md) is test-enforced to
//     match the registry, in both directions.
//   - Presentation-free. Error() delegates to the wrapped error, so
//     wrapping a site in a CodedError never changes what a human
//     sees. The metadata is for machines.
package sluicecode

import (
	"errors"
	"log/slog"
	"sort"
)

// Code is a stable, machine-parsable identifier for an operator-facing
// error class. The format is SLUICE-E-<DOMAIN>-<SLUG>: the domain
// groups codes by pipeline area (CONNECT, BULKCOPY, SCHEMA, INDEX,
// CDC, COLDSTART, VALUE, EXPR), the slug names the class.
type Code string

// The registered codes. Every constant here MUST have a registry
// entry below and a row in docs/operator/error-codes.md (both are
// test-enforced). Per-code context lives in the registry summaries.
const (
	CodeConnectRefused           Code = "SLUICE-E-CONNECT-REFUSED"
	CodeConnectAuthFailed        Code = "SLUICE-E-CONNECT-AUTH-FAILED"
	CodeConnectDatabaseMissing   Code = "SLUICE-E-CONNECT-DATABASE-MISSING"
	CodeBulkCopyTargetMissing    Code = "SLUICE-E-BULKCOPY-TARGET-TABLE-MISSING"
	CodeBulkCopyTableFailed      Code = "SLUICE-E-BULKCOPY-TABLE-FAILED"
	CodeSchemaPermissionDenied   Code = "SLUICE-E-SCHEMA-PERMISSION-DENIED"
	CodeIndexStatementTimeLimit  Code = "SLUICE-E-INDEX-STATEMENT-TIME-LIMIT"
	CodeIndexDirectDDLDisabled   Code = "SLUICE-E-INDEX-DIRECT-DDL-DISABLED"
	CodeIndexMissing             Code = "SLUICE-E-INDEX-MISSING"
	CodeCDCReplicationPermission Code = "SLUICE-E-CDC-REPLICATION-PERMISSION"

	CodeColdStartTargetNotEmpty   Code = "SLUICE-E-COLDSTART-TARGET-NOT-EMPTY"
	CodeSchemaExtensionNotEnabled Code = "SLUICE-E-SCHEMA-EXTENSION-NOT-ENABLED"
	CodeValueZeroDate             Code = "SLUICE-E-VALUE-ZERO-DATE"
	CodeValueNULByte              Code = "SLUICE-E-VALUE-NUL-BYTE"
	CodeValueUnrepresentable      Code = "SLUICE-E-VALUE-UNREPRESENTABLE"
	CodeExprBackslashLiteral      Code = "SLUICE-E-EXPR-BACKSLASH-LITERAL"
	CodeConfirmationRequired      Code = "SLUICE-E-CONFIRMATION-REQUIRED"
	CodeDriverHostMismatch        Code = "SLUICE-E-DRIVER-HOST-MISMATCH"
	CodeVStreamFloatLossy         Code = "SLUICE-E-VSTREAM-FLOAT-LOSSY"

	CodeBackupSignatureInvalid     Code = "SLUICE-E-BACKUP-SIGNATURE-INVALID"
	CodeBackupSignatureMissing     Code = "SLUICE-E-BACKUP-SIGNATURE-MISSING"
	CodeBackupSignatureUnsupported Code = "SLUICE-E-BACKUP-SIGNATURE-UNSUPPORTED"
	CodeBackupChunkAuthFailed      Code = "SLUICE-E-BACKUP-CHUNK-AUTH-FAILED"
	CodeBackupChunkCorrupt         Code = "SLUICE-E-BACKUP-CHUNK-CORRUPT"
	CodeBackupIncomplete           Code = "SLUICE-E-BACKUP-INCOMPLETE"
	CodeBackupManifestInvalid      Code = "SLUICE-E-BACKUP-MANIFEST-INVALID"
	CodeBackupChainConflict        Code = "SLUICE-E-BACKUP-CHAIN-CONFLICT"

	CodeBackfillNoPrimaryKey      Code = "SLUICE-E-BACKFILL-NO-PRIMARY-KEY"
	CodeBackfillUnsupportedEngine Code = "SLUICE-E-BACKFILL-UNSUPPORTED-ENGINE"
	CodeBackfillUnknownColumn     Code = "SLUICE-E-BACKFILL-UNKNOWN-COLUMN"
	CodeBackfillIncomplete        Code = "SLUICE-E-BACKFILL-INCOMPLETE"

	CodePSSafeMigrationsDisabled Code = "SLUICE-E-PS-SAFE-MIGRATIONS-DISABLED"
	CodePSDeployRequestFailed    Code = "SLUICE-E-PS-DEPLOY-REQUEST-FAILED"
	CodePSBranchStaleBase        Code = "SLUICE-E-PS-BRANCH-STALE-BASE"
)

// Class partitions codes by how the process should exit when the
// error is terminal. It exists for exactly one consumer — the exit-
// code taxonomy — so it stays a two-value enum until a third exit
// class genuinely earns a distinct code.
type Class int

const (
	// ClassRuntime is a mid-run failure (connect drop, driver error,
	// vendor limit): the classic exit-1 shape, now with a code.
	ClassRuntime Class = iota

	// ClassRefusal is sluice's loud-failure tenet made machine-
	// readable: a preflight/validation refusal where sluice declined
	// to proceed (or to silently corrupt a value) and named the
	// remedy. Terminal refusals exit with [ExitRefusal].
	ClassRefusal
)

// Info is a code's registry metadata: its exit class and a one-line
// summary. The full meaning + remedy prose lives in the docs table.
type Info struct {
	Class   Class
	Summary string
}

// registry is the single source of truth for which codes exist. The
// doc-sync test walks it against docs/operator/error-codes.md.
var registry = map[Code]Info{
	CodeConnectRefused:           {ClassRuntime, "cannot reach the database host/port"},
	CodeConnectAuthFailed:        {ClassRuntime, "database rejected the DSN credentials"},
	CodeConnectDatabaseMissing:   {ClassRuntime, "the DSN names a database that does not exist"},
	CodeBulkCopyTargetMissing:    {ClassRuntime, "bulk-copy target table not found on the target"},
	CodeBulkCopyTableFailed:      {ClassRuntime, "a table failed mid-bulk-copy; earlier tables lack secondary indexes"},
	CodeSchemaPermissionDenied:   {ClassRuntime, "target role lacks CREATE on the schema"},
	CodeIndexStatementTimeLimit:  {ClassRuntime, "index build hit PlanetScale's statement-time limit (errno 3024)"},
	CodeIndexDirectDDLDisabled:   {ClassRuntime, "PlanetScale safe-migrations blocks direct DDL (errno 1105)"},
	CodeCDCReplicationPermission: {ClassRuntime, "connecting role lacks the REPLICATION attribute"},

	CodeColdStartTargetNotEmpty:    {ClassRefusal, "cold-start refused: a target table already contains data"},
	CodeSchemaExtensionNotEnabled:  {ClassRefusal, "column type owned by a PG extension not opted into"},
	CodeValueZeroDate:              {ClassRefusal, "MySQL zero/partial date has no valid calendar value"},
	CodeValueNULByte:               {ClassRefusal, "string value carries a NUL byte PostgreSQL text types cannot store"},
	CodeValueUnrepresentable:       {ClassRefusal, "a value no target column type can represent (e.g. NaN/±Infinity into a MySQL FLOAT/DOUBLE)"},
	CodeExprBackslashLiteral:       {ClassRefusal, "SQLite expression string literal with a backslash has no faithful MySQL spelling"},
	CodeConfirmationRequired:       {ClassRefusal, "destructive operation requires explicit --yes confirmation"},
	CodeDriverHostMismatch:         {ClassRefusal, "the driver cannot drive the DSN's host (e.g. mysql pointed at a PlanetScale endpoint)"},
	CodeIndexMissing:               {ClassRefusal, "a secondary index the migration was expected to build is absent on the target"},
	CodeVStreamFloatLossy:          {ClassRefusal, "--strict-float: a VStream-COPY backup FLOAT column cannot be re-read exactly (keyless / over the row cap / no streamed row matched the exact re-read)"},
	CodeBackupSignatureInvalid:     {ClassRefusal, "a signed backup manifest's detached signature failed verification (tampered / rolled-back / wrong key)"},
	CodeBackupSignatureMissing:     {ClassRefusal, "a signed (v6) backup manifest is missing its detached signature"},
	CodeBackupSignatureUnsupported: {ClassRefusal, "a signed backup manifest uses a newer signature scheme/canonicalization than this build supports; upgrade sluice (not a tamper signal)"},
	CodeBackupChunkAuthFailed:      {ClassRefusal, "an encrypted backup chunk failed authenticated decryption (tampered / corrupt / spliced or reordered store) — the loud, coded twin of the signed-manifest tamper refusal for backups that are encrypted but not signed"},
	CodeBackupChunkCorrupt:         {ClassRefusal, "a backup chunk's stored bytes do not match the SHA-256 recorded for it in the manifest — at-rest corruption / bit-rot, or a tamper that altered the stored bytes; caught by rehashing at restore, broker replay, and backup verify, before decryption, so it fires on plaintext and encrypted chunks alike (the integrity twin of -CHUNK-AUTH-FAILED, which is the GCM/AAD check)"},
	CodeBackupIncomplete:           {ClassRefusal, "a restored/replayed incremental applied fewer changes than its manifest records (its change-chunk tail was truncated, or a table's chunk row-count was zeroed) — the signing-independent backstop against silent tail-truncation of an unsigned incremental"},
	CodeBackupManifestInvalid:      {ClassRefusal, "a backup manifest's recorded BackupID does not match its content — a BackupID-covered field (created_at / source_engine / kind / EndPosition) was edited without recomputing the id (corruption or lazy tamper)"},
	CodeBackupChainConflict:        {ClassRefusal, "another writer advanced this backup chain's lineage mid-operation (a duplicate cron backup incremental, a backup racing a compact/prune, or an operator double-start) — the conditional catalog write refused rather than interleave; no catalog change was written"},

	CodeBackfillNoPrimaryKey:      {ClassRefusal, "backfill refused: the table has no usable orderable primary key to drive the keyset-chunked walk"},
	CodeBackfillUnsupportedEngine: {ClassRefusal, "backfill refused: the engine does not implement the in-place backfill surface"},
	CodeBackfillUnknownColumn:     {ClassRefusal, "backfill refused: a --set column does not exist on the table"},
	CodeBackfillIncomplete:        {ClassRuntime, "backfill verify found rows still matching the --where guard after the walk — online catch-up needed (rows written behind the cursor), or the guard does not self-describe doneness"},

	CodePSSafeMigrationsDisabled: {ClassRefusal, "expand-contract refused: the PlanetScale production branch does not have safe migrations enabled (the deploy-request prerequisite); sluice never auto-enables it"},
	CodePSDeployRequestFailed:    {ClassRuntime, "a PlanetScale deploy request entered a failure state (or never became deployable/complete before the timeout) — the message carries the DR number, state, and URL"},
	CodePSBranchStaleBase:        {ClassRuntime, "a PlanetScale dev branch's schema still differs from production after a rebase backup — a new dev branch's schema can lag production (intermittent, timing undocumented), and deploying from a stale base would silently revert newer production schema"},
}

// Describe returns the registry metadata for c, and whether c is a
// registered code.
func Describe(c Code) (Info, bool) {
	info, ok := registry[c]
	return info, ok
}

// All returns every registered code, sorted, for the doc-sync test
// and any future machine-readable listing (e.g. a `sluice errors`
// subcommand).
func All() []Code {
	codes := make([]Code, 0, len(registry))
	for c := range registry {
		codes = append(codes, c)
	}
	sort.Slice(codes, func(i, j int) bool { return codes[i] < codes[j] })
	return codes
}

// Process exit codes. 0 and 1 keep their traditional meanings so any
// script checking `!= 0` is unaffected; 2 and 3 carve config errors
// and named refusals out of the generic-failure bucket. Documented in
// docs/operator/error-codes.md and docs/operator/running-as-a-service.md.
const (
	// ExitSuccess: the command completed (for verify/diff/sync-health,
	// completed AND clean).
	ExitSuccess = 0

	// ExitFailure is the generic runtime failure — any terminal error
	// without a more specific class below. For verify, diff, and
	// sync-health it retains those commands' long-documented per-
	// command meaning of "ran cleanly but found drift/mismatch/stale".
	ExitFailure = 1

	// ExitConfig: the config file could not be loaded/parsed
	// ([ConfigError]). Flag/usage errors are kong's 80, not this.
	ExitConfig = 2

	// ExitRefusal: a ClassRefusal coded error was terminal — sluice
	// refused to proceed (or to silently alter a value) and named the
	// remedy. Distinct from 1 so a driving agent can tell "retry won't
	// help; a flag or a source-side fix is required" without parsing
	// prose.
	ExitRefusal = 3

	// ExitUsage documents (does not implement) kong's built-in exit
	// code for flag/command parse errors: kong exits 80 on usage
	// errors (a square/exit semantic code) before any Run method is
	// reached. sluice adopts it as-is rather than remapping.
	ExitUsage = 80
)

// CodedError attaches a stable [Code] and a machine-readable remedy
// hint to an error without changing its message: Error() delegates to
// the wrapped error, so humans see exactly the prose the construction
// site wrote (remedy included). Extract it with [FromError] (or
// errors.As) at a logging/exit/rendering boundary — e.g. to lift Code
// and Hint into a JSON result envelope.
//
// It implements kong's ExitCoder contract so a terminal CodedError
// maps to the exit-code taxonomy with no extra plumbing in main.
type CodedError struct {
	// Code is the registered class identifier.
	Code Code

	// Hint is the concise remedy — typically the flag to pass — as a
	// standalone string, even when the same advice is embedded in the
	// wrapped error's prose.
	Hint string

	// Err is the wrapped error; the chain stays fully traversable.
	Err error
}

func (e *CodedError) Error() string { return e.Err.Error() }

func (e *CodedError) Unwrap() error { return e.Err }

// ExitCode implements kong's ExitCoder: refusal-class codes exit
// [ExitRefusal], everything else keeps the traditional [ExitFailure].
// An unregistered code (a bug the registry tests should catch) safely
// degrades to ExitFailure.
func (e *CodedError) ExitCode() int {
	if info, ok := registry[e.Code]; ok && info.Class == ClassRefusal {
		return ExitRefusal
	}
	return ExitFailure
}

// Wrap returns err annotated with code and hint, or nil when err is
// nil so construction sites can use it inline without a guard.
func Wrap(code Code, hint string, err error) error {
	if err == nil {
		return nil
	}
	return &CodedError{Code: code, Hint: hint, Err: err}
}

// FromError extracts the outermost CodedError in err's chain.
func FromError(err error) (*CodedError, bool) {
	var ce *CodedError
	if errors.As(err, &ce) {
		return ce, true
	}
	return nil, false
}

// Attrs returns `code` and `hint` slog attributes for err when its
// chain carries a CodedError, nil otherwise — shaped for splicing
// into a variadic slog call: append(args, sluicecode.Attrs(err)...).
func Attrs(err error) []any {
	ce, ok := FromError(err)
	if !ok {
		return nil
	}
	return []any{
		slog.String("code", string(ce.Code)),
		slog.String("hint", ce.Hint),
	}
}

// ConfigError marks a config-file load/parse failure so the exit
// boundary can map it to [ExitConfig]. It deliberately carries no
// Code: config errors are a startup shape, not a registered operator-
// hint class (kong owns the sibling usage-error shape at exit 80).
type ConfigError struct{ Err error }

func (e *ConfigError) Error() string { return e.Err.Error() }

func (e *ConfigError) Unwrap() error { return e.Err }

// ExitCode implements kong's ExitCoder contract.
func (e *ConfigError) ExitCode() int { return ExitConfig }
