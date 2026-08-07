// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

// Operator-facing hint layer for wrapped engine errors.
//
// Engine errors today come through wrapped with phase prefixes (e.g.
// "pipeline: bulk copy: postgres: insert into \"users\": ERROR:
// relation \"users\" does not exist"). The phase prefix is good; the
// inner Postgres/MySQL error is correct but cryptic to operators who
// haven't memorised every database's surface. A single hint line —
// appended after the wrapped error, on its own line — turns most
// head-scratchers into one-glance diagnoses.
//
// Design tenets for this file:
//
//   - Tiny, load-bearing registry. Each hint is maintenance forever;
//     anything beyond the most common 5-10 errors is noise. If a
//     hint would only fire on an obscure SQLSTATE that 99% of
//     operators will never hit, it doesn't belong here.
//   - Substring matching is intentional. v1 doesn't try to extract
//     SQLSTATE codes or structured fields from driver errors —
//     case-insensitive substrings are good enough and survive minor
//     wording changes between database versions.
//   - Hints never replace the original error. They're appended after
//     a newline with a "hint:" prefix; the underlying error stays
//     intact for [errors.Is]/[errors.As] traversal.
//   - Phase-scoped. The same substring can mean different things in
//     different phases (e.g. "does not exist" during bulk-copy means
//     the target table is missing; during CDC it could mean a
//     replication slot is gone). Empty phase = match in any phase.
//
// What NOT to put here:
//
//   - Translation/localisation of every possible driver error.
//   - Anything that requires parsing the error message structure.
//   - Hints that duplicate information already in the wrapped phase
//     prefix (e.g. "this happened during bulk-copy").
//   - Engine-version-specific or vendor-specific errors that surface
//     for <1% of operators (e.g. Vitess error 1370). When in doubt,
//     leave it out.

import (
	"errors"
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// Phase identifiers used by [hintFor] and [WrapWithHint] to scope
// hint matching. The strings are stable: tests reference them, and
// the registry keys off them. Empty string means "any phase".
const (
	PhaseConnect     = "connect"
	PhaseSchemaApply = "schema-apply"
	PhaseBulkCopy    = "bulk-copy"
	PhaseIndexes     = "indexes"
	PhaseConstraints = "constraints"
	PhaseViews       = "views"
	PhaseCDC         = "cdc"
	PhaseSnapshot    = "snapshot"
)

// errorHint is one entry in the hint registry: a substring match
// against the wrapped error's text plus a phase scope, mapped to a
// short operator-facing line. Order matters — first match wins.
type errorHint struct {
	// phase scopes the match. Empty matches in any phase.
	phase string

	// contains is matched case-insensitively against err.Error().
	contains string

	// hint is the line emitted after the wrapped error. No leading
	// "hint:" prefix; that's added by WrapWithHint.
	//
	// It is the COMMAND-NEUTRAL text: every command that reaches this
	// phase sees it unless perCommand carries a sharper one, so it must
	// name no flag that is not valid on every one of them. See
	// hints_command.go for why (Bug 230).
	hint string

	// perCommand overrides hint for one kong command path, so a remedy
	// can name the flags THAT command accepts. Absent = the neutral text,
	// which is less specific but never unrunnable.
	perCommand map[Command]string

	// code is the stable machine-parsable identifier for this hint's
	// error class (docs/operator/error-codes.md). Every entry MUST
	// carry one — a hint without a code is a registry bug the tests
	// catch. WrapWithHint attaches it as [sluicecode.CodedError]
	// metadata; the human-facing message is unchanged.
	code sluicecode.Code
}

// Per-command remedy prose, assembled from shared fragments so the same
// explanation is not maintained in three near-copies. The naming is
// mechanical: <entry>Explain is the part every command sees, <entry>ForX the
// clause only command X can act on. See hints_command.go for the rule these
// exist to satisfy (a text names only flags its command accepts).
const (
	// The bulk-copy catch-all (Bug 114's hint, Bug 230's defect). The
	// "earlier tables" framing is itself command-specific: `schema
	// add-table` copies exactly one table, so there are no earlier tables.
	copyFailedEarlierTables = "any earlier tables in this run have data but NOT their declared secondary indexes " +
		"(the indexes phase runs after ALL tables finish bulk-copy); "

	// PlanetScale statement-time wall, index phase (errno 3024).
	psIndexWallExplain = "the index build hit PlanetScale's statement-time limit (errno 3024); " +
		"the data is already copied"
	psIndexWallResumeForMigrate = ", so --resume finishes just the indexes with NO re-copy (increase the " +
		"PlanetScale resource size first — a larger cluster builds the index faster, more likely under the limit)"
	psIndexWallGrowOnly = ". Increase the PlanetScale resource size first — a larger cluster builds the index " +
		"faster, more likely under the limit"
	psIndexWallUpfrontAndToken = ". Alternatively start fresh with --upfront-indexes to build indexes during " +
		"the copy — or, with safe migrations ON, give sluice a PlanetScale service token (--planetscale-org + " +
		"PLANETSCALE_SERVICE_TOKEN_ID/_TOKEN) and it builds the index via a deploy request automatically (ADR-0148)"
	psIndexWallTokenNeutral = " — or, with safe migrations ON, give sluice a PlanetScale service token (this " +
		"command's PlanetScale org setting + PLANETSCALE_SERVICE_TOKEN_ID/_TOKEN) and it builds the index via a " +
		"deploy request automatically (ADR-0148)"

	// PlanetScale safe-migrations direct-DDL block (errno 1105).
	psDirectDDLExplain = "PlanetScale safe-migrations is enabled on the target branch, which blocks direct DDL; " +
		"give sluice a service token ("
	psDirectDDLTokenFlags   = "--planetscale-org + PLANETSCALE_SERVICE_TOKEN_ID/_TOKEN env"
	psDirectDDLTokenNeutral = "this command's PlanetScale org setting + PLANETSCALE_SERVICE_TOKEN_ID/_TOKEN env"
	psDirectDDLRemedy       = ") and it builds the indexes through a deploy request automatically (ADR-0148), " +
		"or disable safe migrations on the branch for the migration"

	// PlanetScale statement-time wall, constraints phase (roadmap item 109).
	psFKWallExplain = "the foreign-key build hit PlanetScale's statement-time limit (errno 3024); ADD FOREIGN " +
		"KEY validates every child row against the parent, so on a large table it is heavier than an index " +
		"build and cannot finish under the ~900 s wall"
	psFKWallResumeForMigrate = " — and --resume re-runs the identical validating ADD and re-hits the same wall, " +
		"so it never converges"
	psFKWallFlagRemedy = ". An UNFILTERED migrate adds such a constraint metadata-only automatically (the " +
		"copied rows are FK-consistent by construction), so you are seeing this because a --where row filter " +
		"is active (which can legitimately orphan children, so validation is kept) or this is not a migrate. " +
		"Re-run with --skip-foreign-keys to complete WITHOUT the foreign keys (each FK's referencing columns " +
		"stay indexed, so you can add the constraints out-of-band afterward), fix the --where filter so it " +
		"does not orphan children, or grow the PlanetScale cluster so the child-row validation finishes under " +
		"the limit"
	psFKWallNeutralRemedy = ", and re-running the identical validating ADD re-hits the same wall. An UNFILTERED " +
		"migrate adds such a constraint metadata-only automatically (the copied rows are FK-consistent by " +
		"construction), so you are seeing this because a row filter is active (which can legitimately orphan " +
		"children, so validation is kept) or this is not a migrate. Grow the PlanetScale cluster so the " +
		"child-row validation finishes under the limit, or fix the row filter so it does not orphan children; " +
		"if this command can skip foreign keys, completing without them keeps each FK's referencing columns " +
		"indexed so the constraints can be added out-of-band afterward"
)

// hintRegistry is the ordered list of hints. Order matters because
// the first match wins — put more-specific entries before more-
// general ones. Each entry's comment explains when it fires.
var hintRegistry = []errorHint{
	// Bulk-copy: target table not present. Postgres surfaces
	// "relation \"x\" does not exist"; MySQL surfaces
	// "Table 'x.y' doesn't exist". Both point at the same root
	// cause: schema-apply silently failed or wrote into a
	// different schema/database than the bulk-copy target uses.
	{
		phase:    PhaseBulkCopy,
		contains: "does not exist",
		hint:     "target table not found — did the schema-apply phase fail or apply to a different schema?",
		code:     sluicecode.CodeBulkCopyTargetMissing,
	},
	{
		phase:    PhaseBulkCopy,
		contains: "doesn't exist",
		hint:     "target table not found — did the schema-apply phase fail or apply to a different schema?",
		code:     sluicecode.CodeBulkCopyTargetMissing,
	},

	// Bulk-copy: any per-table failure that wasn't caught by a more
	// specific entry above. The migrate phases are tables → bulk_copy
	// → identity_sync → indexes → constraints → views, so a mid-
	// bulk_copy abort leaves earlier tables with data but WITHOUT
	// their declared secondary indexes — the index phase only runs
	// after ALL tables finish bulk_copy. Operators inspecting the
	// target see "table has rows" and may conclude it migrated
	// cleanly; in fact only the PK index is present. The substring
	// matches the wrapper prefix in migrate_bulk.go's copy-table
	// failure paths so this fires for any underlying engine error.
	//
	// Bug 230: the remedy is per-command. `migrate` keeps the original
	// text; `sync start` has no --resume (it re-runs under the same
	// --stream-id); `sync run` takes its scope from the fleet config and
	// has neither flag; `schema add-table` has neither AND copies exactly
	// one table, so the "earlier tables" framing is false there too.
	{
		phase:    PhaseBulkCopy,
		contains: "pipeline: copy table",
		hint: copyFailedEarlierTables + "fix the offending table and re-run this command, or take the " +
			"table out of this run's table scope and re-run",
		perCommand: map[Command]string{
			CommandMigrate: copyFailedEarlierTables + "use --resume to continue after fixing the offending " +
				"table, or --exclude-table=<name> to skip it",
			// "there is no resume flag", NOT "--resume": the house idiom for
			// naming an absent flag writes it as prose without the `--`, and
			// the gate is what keeps that honest — a remedy text may name
			// only flags its command accepts, including when it is telling
			// the operator one does not exist.
			CommandSyncStart: copyFailedEarlierTables + "there is no resume flag on `sluice sync start`, so " +
				"fix the offending table and re-run the same command with the same --stream-id, or add " +
				"--exclude-table=<name> to skip it",
			CommandSyncRun: copyFailedEarlierTables + "a fleet sync takes its table scope from the config, " +
				"not from flags: fix the offending table and let the supervisor's restart policy re-run this " +
				"sync, or add the table to this sync's `exclude-table` list in the fleet config",
			CommandAddTable: "`sluice schema add-table` copies exactly the ONE table you named, so no other " +
				"table is affected and there is nothing to resume past or exclude — it has neither flag. Fix " +
				"the table on the source and re-run the same command; the row copy is idempotent, so a re-run " +
				"does not duplicate rows",
		},
		code: sluicecode.CodeBulkCopyTableFailed,
	},

	// Connect-time DSN errors. These three cover the bulk of
	// "I can't even start the migration" reports: the host is
	// unreachable, the credentials are wrong, or the named
	// database isn't there.
	{
		phase:    PhaseConnect,
		contains: "connection refused",
		hint:     "verify the DSN host/port and that the database is reachable from this machine",
		code:     sluicecode.CodeConnectRefused,
	},
	{
		phase:    PhaseConnect,
		contains: "password authentication failed",
		hint:     "verify the DSN username and password",
		code:     sluicecode.CodeConnectAuthFailed,
	},
	// Connect-phase "database does not exist": PG emits
	// "database \"foo\" does not exist". The substring is narrow
	// enough that scoping to PhaseConnect avoids overlap with the
	// bulk-copy "does not exist" hint above.
	{
		phase:    PhaseConnect,
		contains: "does not exist",
		// Not "--target": a fleet `sync run` reaches connect too and takes
		// its endpoints from the config, not from a --target flag.
		hint: "verify the database name in the target DSN",
		code: sluicecode.CodeConnectDatabaseMissing,
	},

	// Schema-apply: target role lacks CREATE on the schema.
	// Postgres surfaces "permission denied for schema X". The
	// fix is operator-side (GRANT or use a different role); the
	// hint nudges operators toward that diagnosis without us
	// silently retrying with elevated privileges.
	{
		phase:    PhaseSchemaApply,
		contains: "permission denied for schema",
		hint:     "the target role lacks CREATE on the schema; verify GRANT or use a different role",
		code:     sluicecode.CodeSchemaPermissionDenied,
	},

	// Indexes: PlanetScale's max-statement-execution-time limit (MySQL
	// errno 3024) killing a large post-copy `ALTER … ADD INDEX`. On a
	// big PlanetScale-MySQL target the deferred index build runs past
	// the ~900 s statement-time limit and fails ("Query execution was
	// interrupted, maximum statement execution time exceeded"), leaving
	// the data copied but the declared secondary indexes uncreated.
	// --upfront-indexes builds them during the copy (maintained
	// per-row) so no post-copy ALTER is ever issued. When the ADR-0148
	// deploy-request fallback is armed (planetscale target + a service
	// token + safe migrations ON) sluice recovers automatically and this
	// hint never fires — it remains the no-token / no-safe-migrations
	// path.
	//
	// Bug 230 sibling: the index phase runs under `migrate`, both `sync`
	// cold-starts, `schema add-table` AND `restore`, so only `migrate` may
	// be told about --resume and only migrate/sync-start about
	// --upfront-indexes / --planetscale-org.
	{
		phase:    PhaseIndexes,
		contains: "maximum statement execution time",
		hint:     psIndexWallExplain + psIndexWallGrowOnly + psIndexWallTokenNeutral,
		perCommand: map[Command]string{
			CommandMigrate:   psIndexWallExplain + psIndexWallResumeForMigrate + psIndexWallUpfrontAndToken,
			CommandSyncStart: psIndexWallExplain + psIndexWallGrowOnly + psIndexWallUpfrontAndToken,
		},
		code: sluicecode.CodeIndexStatementTimeLimit,
	},
	// Indexes: PlanetScale safe-migrations blocks direct DDL. With
	// safe-migrations enabled on the target branch, a direct ALTER is
	// rejected with "direct DDL is disabled" (1105). --upfront-indexes
	// does NOT help here (its ALTER is also direct DDL) — the recovery is
	// the ADR-0148 deploy-request fallback (needs a service token), which
	// engages automatically when armed; this hint is the no-token path.
	{
		phase:    PhaseIndexes,
		contains: "direct ddl is disabled",
		hint:     psDirectDDLExplain + psDirectDDLTokenNeutral + psDirectDDLRemedy,
		perCommand: map[Command]string{
			CommandMigrate:   psDirectDDLExplain + psDirectDDLTokenFlags + psDirectDDLRemedy,
			CommandSyncStart: psDirectDDLExplain + psDirectDDLTokenFlags + psDirectDDLRemedy,
		},
		code: sluicecode.CodeIndexDirectDDLDisabled,
	},

	// Constraints: the SAME PlanetScale statement-time wall (errno 3024), one
	// phase later, on a large post-copy `ALTER … ADD FOREIGN KEY`. InnoDB
	// validates every child row against the parent as it adds the FK, so on a
	// big table the ADD is HEAVIER than an index build and cannot finish under
	// the ~900 s wall (roadmap item 109; field-captured 2026-07-30 on a
	// 153M-row events table, FK died at elapsed 15m0.0006s). A DISTINCT code
	// and remedy from the index wall above: there is no deploy-request
	// fallback for FKs yet (roadmap C), and the index remedies do not apply —
	// --resume re-runs the identical validating ADD and re-hits the SAME
	// deterministic wall (the field --resume loop that never converged), and
	// --upfront-indexes is about indexes, not FKs. The converging remedy is
	// --skip-foreign-keys, which completes the migration and keeps each FK's
	// referencing columns indexed so the constraints can be added out-of-band.
	{
		phase:    PhaseConstraints,
		contains: "maximum statement execution time",
		hint:     psFKWallExplain + psFKWallNeutralRemedy,
		perCommand: map[Command]string{
			CommandMigrate:   psFKWallExplain + psFKWallResumeForMigrate + psFKWallFlagRemedy,
			CommandSyncStart: psFKWallExplain + psFKWallFlagRemedy,
		},
		code: sluicecode.CodeConstraintStatementTimeLimit,
	},

	// CDC: replication-role attribute missing. Postgres surfaces
	// "permission denied for replication" when the connecting
	// role doesn't have the REPLICATION attribute. docs/postgres-
	// source-prep.md documents the GUCs and roles this needs.
	{
		phase:    PhaseCDC,
		contains: "permission denied for replication",
		hint:     "the connecting role needs the REPLICATION attribute (ALTER ROLE x REPLICATION); see docs/postgres-source-prep.md",
		code:     sluicecode.CodeCDCReplicationPermission,
	},
}

// hintFor returns the hint line for the given phase and error, or
// "" when no entry matches. Lookup is case-insensitive substring
// matching; the first registered entry whose phase scope and
// substring match both apply wins.
//
// Phase-empty entries (errorHint.phase == "") match in any phase.
// An empty phase argument matches only entries that are themselves
// phase-empty — the orchestrator always passes a concrete phase, so
// this case is mostly for future use and tests.
// The text is resolved against the command this process is running
// ([RunningCommand]), so a remedy names only flags that command accepts;
// see hints_command.go.
func hintFor(phase string, err error) string {
	h, ok := matchErrorHint(phase, err)
	if !ok {
		return ""
	}
	return h.textFor(RunningCommand())
}

// matchErrorHint returns the first registry entry whose phase scope
// and substring both match, and whether one matched.
func matchErrorHint(phase string, err error) (errorHint, bool) {
	if err == nil {
		return errorHint{}, false
	}
	msg := strings.ToLower(err.Error())
	for _, h := range hintRegistry {
		if h.phase != "" && h.phase != phase {
			continue
		}
		if !strings.Contains(msg, strings.ToLower(h.contains)) {
			continue
		}
		return h, true
	}
	return errorHint{}, false
}

// WrapWithHint returns err with a "hint: ..." line appended when
// the registry has a relevant entry for the given phase. When no
// hint matches, returns err unchanged so the call site reads the
// same as it did before this layer existed.
//
// The wrapping uses %w, so [errors.Is] and [errors.As] still
// traverse the chain normally — the hint is presentation, not
// structure. A nil err returns nil.
//
// The matched entry's stable code + hint additionally ride along as
// [sluicecode.CodedError] metadata, extractable at the CLI's exit/
// logging boundary (the `code`/`hint` attrs under --log-format json).
// The Error() text is byte-identical to the pre-metadata wrapping.
func WrapWithHint(phase string, err error) error {
	if err == nil {
		return nil
	}
	// Item 91 / Bug 213: a specific refusal beats a generic phase guess.
	// The registry matches on message SUBSTRINGS, so a precisely-coded
	// error travelling up through a phase wrapper will also match that
	// phase's catch-all entry — and re-coding it there replaces both the
	// code operators are told to match on AND, because ExitCode() is
	// derived from the code's class, the process exit status. A deferrable
	// upsert-key refusal reached `schema add-table` as
	// SLUICE-E-BULKCOPY-TABLE-FAILED / exit 1 where every other caller
	// reported SLUICE-E-TARGET-DEFERRABLE-KEY / exit 3.
	//
	// The generic hint is not merely redundant in that case, it is wrong:
	// the bulk-copy catch-all tells the operator earlier tables are missing
	// their secondary indexes, when a refusal by definition copied nothing.
	// So an already-coded error is returned untouched — its own code and
	// its own remedy are strictly better than anything a substring match
	// can infer.
	var coded *sluicecode.CodedError
	if errors.As(err, &coded) {
		return err
	}
	// Dynamic classifiers first: they match on error STRUCTURE (a
	// [net.DNSError] in the chain) rather than message substrings, so a
	// structural match beats the static registry. Phase-independent —
	// an IPv6-only resolve failure means the same thing at connect, at
	// CDC open, everywhere. See hints_dns.go.
	h, ok := dnsResolveHint(err)
	if !ok {
		h, ok = matchErrorHint(phase, err)
	}
	if !ok {
		return err
	}
	text := h.textFor(RunningCommand())
	return &sluicecode.CodedError{
		Code: h.code,
		Hint: text,
		Err:  fmt.Errorf("%w\nhint: %s", err, text),
	}
}
