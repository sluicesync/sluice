// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Source identity for resumable simple-mode migrations: what a
// `migrate --resume` must find unchanged before it adopts recorded
// state as its own.
//
// # The gap this closes
//
// A migration id is a NAME, not an identity. [loadOrInitState] adopts
// recorded state by id alone, so a --resume pointed at a DIFFERENT
// source that shares the id reads a copy it never performed as its own.
// Both shapes exit 0 having copied nothing: at phase `complete` the run
// logs "already complete; nothing to do" and stops, and mid-phase it
// skips every table the other run recorded complete. Measured on real
// servers two ways (audit 2026-09-15 A0915-STATE-MEDIUM-1):
//
//   - an operator-supplied --migration-id reused across two sources;
//   - with NO typed id at all, two databases on one host colliding on
//     the auto-derived id, because [deriveMigrationID] hashes the
//     source and target HOSTS and deliberately not the database — its
//     own comment names that collision as a known limitation.
//
// The `sync-` aliasing arm of the same class is refused separately, at
// [Migrator.resolveMigrationID]. This is the general door.
//
// # What identity is, and what it deliberately is NOT
//
// Source ENGINE + source DATABASE + source SCHEMA where the engine
// scopes by one. NOT the host, and that exclusion is the load-bearing
// half rather than an omission: ADR-0015 frames --migration-id as an
// operator-asserted stable identity across DNS shifts and host renames
// ("Operators who need a stable identity across DSN changes … pass
// --migration-id explicitly"), so a legitimate DNS move, a failover, or
// a resume pointed at a replica of the same database MUST still resume.
// Keying on the host would refuse precisely the case the flag exists to
// serve. A host-keyed comparison is the mutation this file's gate runs
// in the second direction.
//
// Neither credentials nor host ever enter the rendered value, so a
// control row cannot leak where the source lives or how to reach it.

package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// sourceIdentityUnrecordedMarker is the grep-stable token the WARN
// carries when recorded state predates the source_identity column, so
// an operator can search their logs for every resume that proceeded
// without identity evidence.
const sourceIdentityUnrecordedMarker = "RESUME-SOURCE-UNRECORDED"

// pgDefaultSchema mirrors the default the Postgres engine's own DSN
// parser applies when a DSN carries no `schema` parameter
// (internal/engines/postgres/connect.go — parseURIDSN and parseKVDSN
// both fall back to "public").
//
// The pipeline must not import an engine (internal/archgate forbids it),
// so this is a MIRROR, and a mirror that silently drifts would make two
// spellings of one source — `?schema=public` and no parameter at all —
// render different identities and refuse a legitimate resume.
// TestSourceIdentitySchemaDefaultMatchesTheEngine reads the engine's
// source file and holds the two together.
const pgDefaultSchema = "public"

// renderSourceIdentity renders the identity of the source a migration
// reads from, as the value stored in sluice_migrate_state.source_identity.
//
// # The encoding, and why it round-trips
//
// The value is persisted and read back, so it is a codec and gets the
// codec treatment. Each field is rendered with [strconv.Quote], which is
// lossless for EVERY Go string — including one that is not valid UTF-8
// (escaped as \xNN) and one containing a NUL (escaped as \x00) — and
// whose output is therefore always valid, NUL-free UTF-8. That is
// exactly the property a TEXT column on both engines requires:
// PostgreSQL refuses an invalid byte sequence with SQLSTATE 22021 and
// MySQL in strict mode with Error 1366, which is how a neighbouring
// column lost its writes (A0915-STATE-MEDIUM-3). A database name is
// operator input arriving through a DSN, so none of that is theoretical.
//
// The rendering is INJECTIVE — two different (engine, database, schema)
// triples cannot render the same string — because each value is a
// complete quoted Go literal at a fixed key, so a separator or an `=`
// appearing INSIDE a name is escaped within its own literal rather than
// framing a new field. A naive `engine=x;database=y` concatenation
// aliases the moment a database is named `a;database=b`; this is the
// same length-prefix-everything lesson [copyShapeTokenHash] learned.
//
// The value is compared as ONE STRING and never parsed back into
// fields, so the decode half cannot be got wrong: there is no decoder.
// The refusal prints the recorded value verbatim, which is what makes
// that affordable.
func renderSourceIdentity(engineName, dsn string) string {
	return "engine=" + strconv.Quote(engineName) +
		";database=" + strconv.Quote(sourceDSNDatabase(dsn)) +
		";schema=" + strconv.Quote(sourceDSNSchema(dsn))
}

// isPGURIDSN reports whether dsn is the Postgres URI form. Same test
// [redactedHost] makes, kept identical on purpose: the two functions
// dispatch over the same DSN forms and a divergence would mean one of
// them is reading a shape the other does not.
func isPGURIDSN(dsn string) bool {
	return strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://")
}

// isMySQLDSN reports whether dsn is the go-sql-driver MySQL form. The
// protocol section is the tell, exactly as in [redactedHost]: a bare "@"
// would misclassify a libpq KV DSN whose PASSWORD contains one.
func isMySQLDSN(dsn string) bool {
	return strings.Contains(dsn, "@tcp(") || strings.Contains(dsn, "@unix(")
}

// sourceDSNDatabase extracts the database name from a DSN, or "" when
// it cannot be derived. Accepts the same three forms [redactedHost]
// does — Postgres URI, go-sql-driver MySQL, libpq KV — and is
// deliberately structured to mirror it, because a form that function
// reads and this one does not would silently render every such source
// as the same identity.
//
// "" is a legitimate value, not a failure: it means "this DSN does not
// name a database". It is rendered and compared like any other, so two
// runs whose database is equally underivable still agree — the check
// never becomes silently vacuous, it just carries less evidence, and
// the ENGINE half of the identity still discriminates.
func sourceDSNDatabase(dsn string) string {
	if isPGURIDSN(dsn) {
		u, err := url.Parse(dsn)
		if err != nil {
			return ""
		}
		return strings.TrimPrefix(u.Path, "/")
	}
	if isMySQLDSN(dsn) {
		// `user:pass@tcp(host:port)/dbname?params` — strip the params,
		// then take everything after the LAST slash, which is correct for
		// the unix-socket form too (`@unix(/var/run/mysqld.sock)/dbname`).
		body := dsn
		if q := strings.IndexByte(body, '?'); q >= 0 {
			body = body[:q]
		}
		if slash := strings.LastIndexByte(body, '/'); slash >= 0 {
			return body[slash+1:]
		}
		return ""
	}
	for _, tok := range strings.Fields(dsn) {
		if k, v, ok := strings.Cut(tok, "="); ok && strings.EqualFold(k, "dbname") {
			return v
		}
	}
	return ""
}

// sourceDSNSchema extracts the source NAMESPACE a schema-scoping engine
// reads, or "" for an engine with a flat namespace.
//
// MySQL is flat — its database IS its namespace, already carried by
// [sourceDSNDatabase] — so a MySQL DSN contributes "" here rather than
// duplicating the database into a second field.
//
// Postgres scopes by schema and takes it from the DSN's `schema`
// parameter, defaulting to [pgDefaultSchema]; this mirrors that rule in
// both DSN forms so `?schema=public` and an absent parameter render the
// same identity and a resume across the two spellings is not refused.
func sourceDSNSchema(dsn string) string {
	if isPGURIDSN(dsn) {
		u, err := url.Parse(dsn)
		if err != nil {
			return pgDefaultSchema
		}
		if s := u.Query().Get("schema"); s != "" {
			return s
		}
		return pgDefaultSchema
	}
	if isMySQLDSN(dsn) {
		return ""
	}
	for _, tok := range strings.Fields(dsn) {
		if k, v, ok := strings.Cut(tok, "="); ok && strings.EqualFold(k, "schema") && v != "" {
			return v
		}
	}
	return pgDefaultSchema
}

// refuseForeignSourceOnResume is the door: recorded migration state may
// only be adopted by a run reading from the source that recorded it.
//
// Three outcomes, and the two that are not the refusal are the ones
// worth stating:
//
//   - live == "": the CONSTRUCTING caller supplies no identity. Not
//     reachable from `migrate`, whose identity always carries at least a
//     non-empty engine name ([ir.Engine.Name] is never empty for a
//     registered engine); it is the zero value every other
//     [resumeContext] construction gets — the sync recording context,
//     which cannot reach [loadOrInitState] at all (it is noResume), and
//     the unit tests. Spelled as an opt-IN rather than defaulting to
//     refuse so the zero value is the pre-existing behaviour, per the
//     v0.99.51 rule.
//
//   - recorded == "": the state was written by a binary older than the
//     source_identity column, which the store surfaces as "" (NO
//     EVIDENCE, never "no identity"). This WARNs and PROCEEDS. Refusing
//     would strand every migration in flight at the moment an operator
//     upgrades — a migration they cannot restart cheaply, which is the
//     whole reason --resume exists — and that population can only
//     SHRINK: every header row this binary writes records an identity,
//     so no new member is ever created. A row is never back-filled on
//     resume either: the identity is set-once at INSERT, so adopting the
//     resuming run's identity would be recording a guess as evidence and
//     would silently arm the refusal against whichever source happened
//     to run second.
func refuseForeignSourceOnResume(ctx context.Context, migrationID, recorded, live string) error {
	if live == "" {
		return nil
	}
	if recorded == "" {
		slog.WarnContext(
			ctx,
			"migration: "+sourceIdentityUnrecordedMarker+": this migration's recorded state carries no source "+
				"identity, so the resume cannot prove it is continuing the same source's copy. It proceeds "+
				"because the state was written by a sluice older than the source_identity column; a resume "+
				"from a DIFFERENT source would look identical to this one, so confirm the source is the one "+
				"the recorded copy came from before trusting the result",
			slog.String("migration_id", migrationID),
			slog.String("live_source", live),
		)
		return nil
	}
	if recorded == live {
		return nil
	}
	return &sluicecode.CodedError{
		Code: sluicecode.CodeResumeSourceMismatch,
		// Both remedies must be RUNNABLE. "Use a different --migration-id"
		// is only correct together with dropping --resume: --resume against
		// an id with no recorded state refuses with "no migration found".
		Hint: "re-run --resume against the source this migration id belongs to; or, to migrate THIS source, " +
			"drop --resume and pass a fresh --migration-id (omit it entirely to derive one from the " +
			"source/target pair) so the run starts its own migration instead of adopting this one",
		Err: fmt.Errorf(
			"pipeline: --resume: migration_id %q records a copy made from a DIFFERENT source"+
				"\n  recorded: %s"+
				"\n  live:     %s"+
				"\nresuming would adopt that copy as this run's own — every table it recorded complete is "+
				"SKIPPED, so the run would exit 0 having copied nothing from this source"+
				"\nidentity is the source engine, database and schema and deliberately NOT the host, so a DNS "+
				"change, a failover, or a replica of the same database still resumes",
			migrationID, recorded, live,
		),
	}
}
