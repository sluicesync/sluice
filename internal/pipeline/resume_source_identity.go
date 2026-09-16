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
// # Where the DSN grammar lives, and why it is not here
//
// It lives in the ENGINE ([ir.SourceIdentityDescriber]). The first cut
// of this file parsed DSNs in the orchestrator and knew three shapes —
// Postgres URI, libpq key/value, and the go-sql-driver `@tcp(`/`@unix(`
// forms — returning "" for everything else. The damage was not a missing
// feature but a VACUOUS door, and it was wide (audit 2026-09-15 A0915-VF2-F1,
// measured by the reviewer against byte-identical copies of the shipped
// functions): two different SQLite files, two different D1 databases,
// two mydumper dumps, two flat files, and two MySQL DSNs spelled
// `root:pw@/db` all rendered ONE identity — and the auto-derived id
// collapsed with them, since [deriveMigrationID] hashes [redactedHost],
// which is equally "" for those shapes. Both discriminators failed
// together, so the run adopted the other source's completed copy and
// exited 0.
//
// The migrate-state store lives on the TARGET, so every one of those is
// a supported resumable configuration: a SQLite, D1, flat-file or
// mydumper source into a Postgres or MySQL target.
//
// Asking the engine is what makes that class closed rather than patched:
// an engine cannot forget to teach the orchestrator a grammar the
// orchestrator never learns. The registry-derived roster
// docsync.TestEverySourceEngineDescribesItsIdentity is the gate, with an
// EMPTY exemption map.
//
// # What identity is, and what it deliberately is NOT
//
// Source ENGINE + the engine's own dataset + its namespace where it
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
	"strconv"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// sourceIdentityUnrecordedMarker is the grep-stable token the WARN
// carries when recorded state predates the source_identity column, so
// an operator can search their logs for every resume that proceeded
// without identity evidence.
const sourceIdentityUnrecordedMarker = "RESUME-SOURCE-UNRECORDED"

// sourceIdentityUndiscriminatedMarker is the grep-stable token for the
// other way this door can be quiet: the LIVE identity carries no dataset
// at all, so it discriminates only by engine name and two different
// sources under that engine compare equal.
//
// It exists so the absence of a refusal is never read as proof. Every
// registered engine describes its identity (the roster gate holds that),
// and every engine that does still answers "" for a DSN it cannot parse
// or one that genuinely names no dataset — so this is reachable, rare,
// and exactly the state an operator must not mistake for a check that
// passed.
const sourceIdentityUndiscriminatedMarker = "RESUME-SOURCE-UNDISCRIMINATED"

// renderSourceIdentity renders the identity of the source a migration
// reads from, as the value stored in sluice_migrate_state.source_identity.
//
// It ASKS THE ENGINE ([ir.SourceIdentityDescriber]) — see the file
// header for why the grammar is not here. The second return reports
// whether the answer carries a discriminator beyond the engine name;
// false means a comparison against it can only distinguish engines, and
// the door says so out loud.
func renderSourceIdentity(e ir.Engine, dsn string) (identity string, discriminating bool) {
	var id ir.SourceIdentity
	if d, ok := e.(ir.SourceIdentityDescriber); ok {
		id = d.SourceIdentity(dsn)
	}
	return renderSourceIdentityFields(e.Name(), id), id != (ir.SourceIdentity{})
}

// renderSourceIdentityFields is the FRAMING: it turns an engine name and
// the engine's own identity fields into the one string that is stored
// and compared.
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
// Injectivity of the FRAMING is only half of what the door needs, and
// the half that was never in doubt: the other half is that two different
// DSNs produce two different triples, which is the engine's job and is
// graded against real engines by
// docsync.TestSourceIdentityIsInjectiveThroughRealDSNs.
//
// The value is compared as ONE STRING and never parsed back into
// fields, so the decode half cannot be got wrong: there is no decoder.
// The refusal prints the recorded value verbatim, which is what makes
// that affordable.
func renderSourceIdentityFields(engineName string, id ir.SourceIdentity) string {
	return "engine=" + strconv.Quote(engineName) +
		";database=" + strconv.Quote(id.Database) +
		";schema=" + strconv.Quote(id.Schema)
}

// refuseForeignSourceOnResume is the door: recorded migration state may
// only be adopted by a run reading from the source that recorded it.
//
// Four outcomes, and the three that are not the refusal are the ones
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
//   - discriminating == false: the live identity names no dataset, so it
//     can only tell engines apart. WARNs and CONTINUES to the comparison
//     — the engine half is still real evidence, and a mismatch there is
//     still a refusal worth making. The point of the WARN is that the
//     absence of a refusal must not be read as proof. Zero-value-safe
//     per v0.99.51: false is the noisy answer, so a future caller that
//     sets an identity and forgets this flag gets the warning rather
//     than silence.
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
func refuseForeignSourceOnResume(ctx context.Context, migrationID, recorded, live string, discriminating bool) error {
	if live == "" {
		return nil
	}
	if !discriminating {
		slog.WarnContext(
			ctx,
			"migration: "+sourceIdentityUndiscriminatedMarker+": this source's DSN names no database, file or "+
				"dataset this engine can report, so the resume's source check can only tell one ENGINE from "+
				"another — two different sources under this engine would compare equal and this resume would "+
				"adopt the other one's copy. It is not a refusal because the engine half still holds; confirm "+
				"the source is the one the recorded copy came from",
			slog.String("migration_id", migrationID),
			slog.String("live_source", live),
		)
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
				"\nidentity is the source engine, its database or file, and its schema where the engine has "+
				"one, and deliberately NOT the host, so a DNS change, a failover, or a replica of the same "+
				"database still resumes",
			migrationID, recorded, live,
		),
	}
}
