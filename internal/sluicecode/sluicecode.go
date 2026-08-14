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
	CodeCDCPoolerEndpoint        Code = "SLUICE-E-CDC-POOLER-ENDPOINT"
	CodeCDCRowImagePartial       Code = "SLUICE-E-CDC-ROW-IMAGE-PARTIAL"
	CodeCDCGeneratedPrimaryKey   Code = "SLUICE-E-CDC-GENERATED-PRIMARY-KEY"
	CodeCDCChangeLogIDReuse      Code = "SLUICE-E-CDC-CHANGELOG-ID-REUSE"
	CodeCDCStandbySource         Code = "SLUICE-E-CDC-STANDBY-SOURCE"
	CodeCDCXAUnsupported         Code = "SLUICE-E-CDC-XA-UNSUPPORTED"
	CodeConnectIPv6Only          Code = "SLUICE-E-CONNECT-IPV6-ONLY"

	// Audit 2026-08-11 C1-1: sluice's emitters only ever produce ONE SQL
	// statement, and the restore path inlines RECORDED expression bodies
	// from a backup manifest verbatim into that DDL before running it
	// through pgx's simple protocol (which executes multi-statement
	// strings). A tampered manifest body that closed the surrounding
	// syntax and appended `; DROP TABLE …; --` executed as the restore
	// role at exit 0. The single-statement door refuses structurally
	// multi-statement emitted DDL before the server sees it.
	CodeDDLEmitMultiStatement Code = "SLUICE-E-DDL-EMIT-MULTI-STATEMENT"

	// Roadmap item 109: the errno-3024 statement-time wall reaches the
	// CONSTRAINTS phase, not just the index phase. A large post-copy
	// `ALTER … ADD FOREIGN KEY` on a PlanetScale/Vitess target runs InnoDB's
	// child-row validation synchronously and blows the ~900 s wall
	// (field-captured 2026-07-30 at 153M rows). A DISTINCT code from the index
	// wall because the remedy differs: --skip-foreign-keys completes the
	// migration (backing indexes preserved), where the index wall's remedy is
	// --resume / --upfront-indexes / the deploy-request fallback. The FK ADD
	// has no deploy-request fallback wired yet (roadmap C).
	CodeConstraintStatementTimeLimit Code = "SLUICE-E-CONSTRAINT-STATEMENT-TIME-LIMIT"

	// Roadmap item 109 (loud-failure recovery): the item-109 metadata-only FK
	// statement-wall recovery adds a wall-blocked foreign key under
	// foreign_key_checks=0, then PROVES the child rows satisfy it with a
	// bounded chunked orphan scan (each query PK-range-bounded so it cannot
	// itself hit the wall). When that scan finds a child row whose foreign-key
	// value has no matching parent, the run is REFUSED (the FK is dropped
	// first, so the target never keeps a trusted-wrong constraint) — the loud
	// signal the `--allow-degraded-fks` baseline demands, recovered without
	// re-incurring the wall a validating ADD would have hit.
	CodeFKSourceOrphan Code = "SLUICE-E-FK-SOURCE-ORPHAN"

	// Roadmap item 68e: binlog CDC (vanilla MySQL / MariaDB flavors)
	// refuses at every CDC start when @@GLOBAL.binlog_format is not ROW.
	// Ground truth (Phase A, 2026-07-23, real mysql:8.0 STATEMENT): the
	// cold copy lands, then every live DML is SILENTLY LOST — QueryEvents
	// hit the dispatcher's generic-DDL arm, nothing applies, the position
	// never advances, the stream stays green. MIXED is the same class
	// (deterministic writes statement-logged; MariaDB's DEFAULT).
	// VStream flavors never take this path (vtgate owns the row-event
	// contract); bulk-only runs are not gated.
	CodeCDCBinlogFormatNotRow Code = "SLUICE-E-CDC-BINLOG-FORMAT-NOT-ROW"

	// Roadmap item 68d: a slot-creating PG CDC cold start refuses
	// UPFRONT when the source's max_replication_slots are all in use
	// (or max_wal_senders all attached) — the census names the existing
	// slots so the operator can free leftovers instead of decoding the
	// raw mid-cold-start 53400. Cold-start only; warm resume reuses its
	// existing slot and never probes. A failed probe WARNs and
	// continues (advisory degrade) — the refusal requires a successful
	// census that proves the ceiling.
	CodeCDCReplicationHeadroom Code = "SLUICE-E-CDC-REPLICATION-HEADROOM"

	// ADR-0175: a cold start would NARROW a Postgres publication while
	// another sluice replication slot EXISTS on the source (active or
	// not — existence semantics since v0.99.289: an inactive slot is a
	// stream stopped mid-migration that will resume expecting its
	// scope), silently de-scoping that stream (its slot keeps advancing
	// while pgoutput emits nothing for its tables).
	CodeCDCPublicationScopeConflict Code = "SLUICE-E-CDC-PUBLICATION-SCOPE-CONFLICT"

	// audit 2026-07-23 D0-9: an operator-supplied --publication-name
	// outside [a-z0-9_] / over 63 bytes is refused at resolve time —
	// sluice's quoted CREATE PUBLICATION preserves case while
	// START_REPLICATION's publication_names is downcased server-side (and
	// CREATE silently truncates past NAMEDATALEN-1), so an unsafe name
	// creates one publication and streams from another: green through the
	// whole bulk copy, then 42704 at the first change, or a silently idle
	// stream. The mirror of PG's own slot-name charset enforcement.
	CodeCDCPublicationNameInvalid Code = "SLUICE-E-CDC-PUBLICATION-NAME-INVALID"

	// RETAINED-BUT-UNUSED (ADR-0170): the Phase-1/2 refusal for MariaDB CDC
	// is lifted — MariaDB CDC via domain GTIDs shipped v0.99.271, so the
	// flavor declares CDCBinlog and this code is no longer emitted. The
	// string stays registered because removing a published catalog code is
	// breaking. Mirrors CodeCDCMariaDBNativeTypeUnsupported below.
	CodeCDCMariaDBUnsupported Code = "SLUICE-E-CDC-MARIADB-UNSUPPORTED"

	// RETAINED-BUT-UNUSED (ADR-0171): the P3 refusal for MariaDB native
	// uuid/inet6/inet4 CDC is lifted — the binlog decode now formats those
	// raw storage bytes faithfully ([mysql.decodeMariaDBNative]). The code
	// stays registered because removing a published catalog code is
	// breaking; it is no longer emitted. See ADR-0170 (refusal) / ADR-0171
	// (decode + lift).
	CodeCDCMariaDBNativeTypeUnsupported Code = "SLUICE-E-CDC-MARIADB-NATIVE-TYPE-UNSUPPORTED"

	// audit 2026-07-23 DEVEX-3 / open question Q3: `sync decommission`
	// refuses while the stream's replication slot is ACTIVE on the
	// source — an attached walsender means the stream is live, and
	// decommissioning a live stream yanks its slot (and publication)
	// out from under it mid-stream. The remedy is a drain first
	// (`sluice sync stop --wait`), then re-run.
	CodeDecommissionStreamActive Code = "SLUICE-E-DECOMMISSION-STREAM-ACTIVE"

	CodeColdStartTargetNotEmpty   Code = "SLUICE-E-COLDSTART-TARGET-NOT-EMPTY"
	CodeSchemaExtensionNotEnabled Code = "SLUICE-E-SCHEMA-EXTENSION-NOT-ENABLED"
	CodeSchemaIdentifierInvalid   Code = "SLUICE-E-SCHEMA-IDENTIFIER-INVALID"

	// An emitted PostgreSQL identifier exceeds NAMEDATALEN-1 (63 bytes).
	// PG truncates silently at CREATE time, and sluice emits the
	// `IF NOT EXISTS` form for both indexes and tables, so a truncation
	// collision is a SILENT no-op rather than an error — a missing index,
	// or (for a table) a second table's rows landing inside the first.
	CodeSchemaIdentifierTooLong Code = "SLUICE-E-SCHEMA-IDENTIFIER-TOO-LONG"

	// Two distinct source indexes resolve to ONE index identifier on a
	// target whose index names are SCHEMA-scoped, and
	// `CREATE INDEX IF NOT EXISTS` makes the second build a silent no-op.
	// Postgres reaches it through a non-injective transform
	// (`("posts","user_id")` and `("posts","posts_user_id")` both render
	// `posts_user_id`); SQLite reaches it with no transform at all — two
	// tables' identically named indexes are one name there (item 134).
	CodeSchemaIndexNameCollision Code = "SLUICE-E-SCHEMA-INDEX-NAME-COLLISION"

	// A source VIEW's name lands on a name a TABLE or another view
	// already occupies on a SQLite target, where `CREATE VIEW IF NOT
	// EXISTS` returns OK and creates nothing — so the view is simply
	// absent at exit 0 (item 147). Deliberately NOT the index code: the
	// object lost is a view, the namespace is the table/view one rather
	// than the index one, and the remedy names a different rename.
	CodeSchemaViewNameCollision Code = "SLUICE-E-SCHEMA-VIEW-NAME-COLLISION"

	// Two distinct source TABLES land on one table identifier on a target
	// whose `CREATE TABLE IF NOT EXISTS` treats the second create as a
	// success that creates nothing (item 148). Strictly worse than either
	// sibling above: the second table's rows are then INSERTed under a name
	// that resolves to the FIRST table, so both tables' rows end up in one
	// table, the surviving name's count is right, and the run exits 0.
	CodeSchemaTableNameCollision Code = "SLUICE-E-SCHEMA-TABLE-NAME-COLLISION"

	// A multi-namespace fan-out (`--all-databases` / `--include-schema` /
	// `--map-database`, migrate or sync) was aimed at a target engine that
	// can neither derive a per-namespace DSN nor accept a schema override,
	// so every source namespace would be written into ONE flat target
	// namespace — silently merging same-named tables (item 148, route 2).
	CodeConfigMultiNamespaceTargetFlat Code = "SLUICE-E-CONFIG-MULTI-NAMESPACE-TARGET-FLAT"

	CodeValueZeroDate        Code = "SLUICE-E-VALUE-ZERO-DATE"
	CodeValueNULByte         Code = "SLUICE-E-VALUE-NUL-BYTE"
	CodeValueUnrepresentable Code = "SLUICE-E-VALUE-UNREPRESENTABLE"
	CodeValueRaggedArray     Code = "SLUICE-E-VALUE-RAGGED-ARRAY"

	// A PostgreSQL `bytea` value arrived as a TEXT rendering sluice
	// cannot read — not the `\x`+even-hex form `bytea_output = hex`
	// produces. Item 135: the decoder used to fall through to a
	// verbatim copy here, storing the ASCII of the rendering as the
	// value.
	CodeValueByteaTextUnrecognized Code = "SLUICE-E-VALUE-BYTEA-TEXT-UNRECOGNIZED"

	CodeExprBackslashLiteral Code = "SLUICE-E-EXPR-BACKSLASH-LITERAL"
	CodeConfirmationRequired Code = "SLUICE-E-CONFIRMATION-REQUIRED"
	CodeDriverHostMismatch   Code = "SLUICE-E-DRIVER-HOST-MISMATCH"
	CodeVStreamFloatLossy    Code = "SLUICE-E-VSTREAM-FLOAT-LOSSY"

	CodeBackupSignatureInvalid     Code = "SLUICE-E-BACKUP-SIGNATURE-INVALID"
	CodeBackupSignatureMissing     Code = "SLUICE-E-BACKUP-SIGNATURE-MISSING"
	CodeBackupSignatureUnsupported Code = "SLUICE-E-BACKUP-SIGNATURE-UNSUPPORTED"
	CodeBackupChunkAuthFailed      Code = "SLUICE-E-BACKUP-CHUNK-AUTH-FAILED"
	CodeBackupChunkCorrupt         Code = "SLUICE-E-BACKUP-CHUNK-CORRUPT"
	CodeBackupChunkUnreadable      Code = "SLUICE-E-BACKUP-CHUNK-UNREADABLE"
	CodeBackupIncomplete           Code = "SLUICE-E-BACKUP-INCOMPLETE"
	CodeBackupInterrupted          Code = "SLUICE-E-BACKUP-INTERRUPTED"
	CodeBackupManifestInvalid      Code = "SLUICE-E-BACKUP-MANIFEST-INVALID"
	CodeBackupChainConflict        Code = "SLUICE-E-BACKUP-CHAIN-CONFLICT"
	CodeBackupEncryptionMismatch   Code = "SLUICE-E-BACKUP-ENCRYPTION-MISMATCH"

	// CodeBackupRecordedSchemaMalformed is the Bug 243 refusal: the
	// chain's RECORDED schema carries an expression whose string literal
	// never closes (the mangle a pre-v0.120.0 MySQL-family reader
	// produced for apostrophe-carrying expressions), so its DDL cannot
	// be emitted as valid SQL. Raised by `backup verify` (any depth),
	// by `restore`/`chain restore` BEFORE any DDL is emitted, and
	// WARNed (not refused) when `backup incremental` extends such a
	// chain — the incremental's data is valid; the chain's restorability
	// is what is broken.
	CodeBackupRecordedSchemaMalformed Code = "SLUICE-E-BACKUP-RECORDED-SCHEMA-MALFORMED"

	// CodeBackupStoreNameCollision is the Bug 248 gate: the destination
	// store folds letter case (measured by probe — Windows NTFS / macOS
	// default filesystems for a local dir), and two or more table-derived
	// store paths would fold to one host path, so the later table's data
	// would silently overwrite the earlier's at exit 0. Refused before
	// any write, at `backup full` and `backup export-as-parquet`.
	CodeBackupStoreNameCollision Code = "SLUICE-E-BACKUP-STORE-NAME-COLLISION"

	// CodeBackupChainUnreadable is the chain-maintenance readability
	// gate's refusal: `backup compact` / `backup prune` re-read the chain
	// the way a restore would — before the destructive sweep, and again
	// after it — and could not. It is emitted by ONE guard shared by both
	// operations (the `op` names which refused), because both are
	// chain-SHAPING operations whose local "these files are orphans"
	// reasoning is whole-chain incomplete: Bug 214 was compaction deleting
	// the chain-root manifest, and prune deleted the same file whenever
	// retention dropped segment 0. Refusal class so the exit status says
	// "a re-run will not help" — the pre-sweep leg has destroyed nothing,
	// and the post-sweep leg names a recovery.
	CodeBackupChainUnreadable Code = "SLUICE-E-BACKUP-CHAIN-UNREADABLE"

	// CodeBackupSchemaDeltaUnsupported is the chain-replay side's
	// refusal for an `alter_table` schema delta carrying a structural
	// change that has no faithful replay on the target: a dropped
	// column, a changed primary key, a column that gained or lost its
	// GENERATED-ness, or a malformed entry. Both replay sites (chain
	// restore and the broker's incremental follower) previously looked
	// only for ADDED columns and skipped every other shape in silence —
	// a pure retype (numeric(10,2) → numeric(20,8)) therefore restored
	// at the OLD width and let the target round the replayed values on
	// insert, exit 0. The shapes an engine can apply are now applied;
	// this code is the loud half for the ones it cannot.
	CodeBackupSchemaDeltaUnsupported Code = "SLUICE-E-BACKUP-SCHEMA-DELTA-UNSUPPORTED"

	CodeBackfillNoPrimaryKey      Code = "SLUICE-E-BACKFILL-NO-PRIMARY-KEY"
	CodeBackfillUnsupportedEngine Code = "SLUICE-E-BACKFILL-UNSUPPORTED-ENGINE"
	CodeBackfillUnknownColumn     Code = "SLUICE-E-BACKFILL-UNKNOWN-COLUMN"
	CodeBackfillIncomplete        Code = "SLUICE-E-BACKFILL-INCOMPLETE"
	CodeBackfillCorruptCursor     Code = "SLUICE-E-BACKFILL-CORRUPT-CURSOR"
	CodeBackfillConcurrentRun     Code = "SLUICE-E-BACKFILL-CONCURRENT-RUN"

	// CodeBackfillVerifyNoEvidence is the completion gate's refusal to
	// AUTHORIZE the contract step when the run it is verifying did no
	// work. `CountRemaining` renders the same verbatim `--where` the
	// chunk UPDATE does, so a predicate that is valid SQL but
	// semantically wrong (wrong column, a coercion matching nothing)
	// counts 0 before the walk, updates 0 rows, and counts 0 after —
	// every step agrees and none of them is evidence. `--verify`
	// therefore authorizes only when the backfill it verifies updated at
	// least one row (this run, a run it resumed, or the completed run it
	// no-ops over); the three zero-work shapes refuse.
	CodeBackfillVerifyNoEvidence Code = "SLUICE-E-BACKFILL-VERIFY-NO-EVIDENCE"

	CodeTargetTableShapeMismatch Code = "SLUICE-E-TARGET-TABLE-SHAPE-MISMATCH"

	// CodeTargetPreexistingForeignKey fires when the TARGET already
	// carries a foreign key on a table this run is about to copy into
	// AND that constraint's parent table is copied by the same run.
	// sluice's deferred-constraint discipline (create tables bare, copy,
	// then add indexes and constraints) governs only the constraints
	// sluice CREATES; a target branched from an existing database arrives
	// with its own, the copy is not parent-first ordered, and a child row
	// reaches the target before its parent. A field report (2026-08-05,
	// roadmap item 140) died ~20 s into a `sync` cold start on exactly
	// this, with nothing having warned first. Refused before any data
	// moves instead. `--skip-foreign-keys` is deliberately NOT an
	// exemption: it strips foreign keys from the IR so the constraints
	// phase creates none, issues no DDL against the target, and therefore
	// leaves a pre-existing constraint enforcing.
	CodeTargetPreexistingForeignKey Code = "SLUICE-E-TARGET-PREEXISTING-FOREIGN-KEY"

	// CodeTargetDeferrableKey fires when a target table's only usable
	// upsert key is a DEFERRABLE unique constraint. Postgres refuses a
	// non-immediate index as an `ON CONFLICT` arbiter (SQLSTATE 55000),
	// and every idempotent write sluice makes — the CDC applier's
	// `INSERT … ON CONFLICT (key) DO UPDATE`, the idempotent bulk-copy
	// writer's batch upsert — needs one. Refused at sync-start preflight
	// (and, defence-in-depth, on first sight of the table) rather than
	// mid-stream: v0.103.1 made a Postgres target CARRY a source's
	// `PRIMARY KEY … DEFERRABLE`, which is correct, and that made the
	// apply path illegal — the stream died on the first change to such a
	// table with no retry and no recovery (Bug 211).
	CodeTargetDeferrableKey Code = "SLUICE-E-TARGET-DEFERRABLE-KEY"

	// CodeSourceReplicaIdentity is CodeTargetDeferrableKey's source-side
	// sibling — the same `pg_index.indimmediate` bit, the opposite end of
	// the pipeline. Postgres will not use a DEFERRABLE key's index as a
	// replica identity, so a PG source table whose only key is a
	// deferrable PRIMARY KEY has none; the moment sluice adds it to a
	// publication with `pubupdate=t pubdelete=t`, Postgres refuses the
	// SOURCE APPLICATION's own UPDATE/DELETE on it. INSERT keeps working,
	// so it looks healthy until the first update, and the failure surfaces
	// in the operator's application rather than in sluice (roadmap item
	// 93). Refused at PG-source cold start BEFORE the publication is
	// scoped — the keyless table and an explicit `REPLICA IDENTITY
	// NOTHING` fall out of the same catalog read.
	CodeSourceReplicaIdentity Code = "SLUICE-E-SOURCE-REPLICA-IDENTITY"

	CodePSSafeMigrationsDisabled Code = "SLUICE-E-PS-SAFE-MIGRATIONS-DISABLED"
	CodePSDeployRequestFailed    Code = "SLUICE-E-PS-DEPLOY-REQUEST-FAILED"
	CodePSBranchStaleBase        Code = "SLUICE-E-PS-BRANCH-STALE-BASE"
	CodePSDirectDDLBlocked       Code = "SLUICE-E-PS-DIRECT-DDL-BLOCKED"

	// CodePSDeployRequestIncomplete is the NON-failure sibling of
	// CodePSDeployRequestFailed: a PlanetScale deploy request sluice was
	// waiting on never reached a terminal state, and nothing about it
	// failed — either a wall-clock bound was hit while the deploy was
	// healthy and progressing, or the request is parked on a gate only a
	// person can clear (deploy approval, a manual cutover confirmation).
	// It exists because reporting that as FAILED misled badly in the
	// field: a 106 GB / 153 M-row index build ~29 % of the way through a
	// projected 3 h 25 m VReplication deploy exited
	// SLUICE-E-PS-DEPLOY-REQUEST-FAILED, which reads as "the index build
	// broke" when the truth was "come back later". On this code the
	// deployment is still running in PlanetScale and its dev branch must
	// be LEFT ALONE until it finishes.
	CodePSDeployRequestIncomplete Code = "SLUICE-E-PS-DEPLOY-REQUEST-INCOMPLETE"

	// CodePSDevBranchNotAdoptable fires when a re-run finds its own
	// deterministic dev branch still there from an earlier attempt and
	// CANNOT adopt the deploy request on it (roadmap item 108). Adoption
	// is the normal outcome — a deploy request that is already deploying
	// is joined by the same poller the fresh path uses, which is what
	// rescues an operator holding a branch whose multi-hour index build is
	// 78 % done. This code is the residue: no deploy request at all, more
	// than one, one aimed at a different production branch, one that
	// never got deployed, or one that ended in a terminal failure. Every
	// shape names what was found and what to do with the branch, and the
	// message is only ever "delete it" when nothing is deploying — the
	// refusal it replaces said that unconditionally, and following it
	// mid-build would have discarded hours of completed work.
	CodePSDevBranchNotAdoptable Code = "SLUICE-E-PS-DEV-BRANCH-NOT-ADOPTABLE"

	CodeSourceForeignDump     Code = "SLUICE-E-SOURCE-FOREIGN-DUMP"
	CodeSourceWrongDriver     Code = "SLUICE-E-SOURCE-WRONG-DRIVER"
	CodeCSVNullAmbiguous      Code = "SLUICE-E-CSV-NULL-AMBIGUOUS"
	CodeCSVHeaderUndeclared   Code = "SLUICE-E-CSV-HEADER-UNDECLARED"
	CodeExportUnrepresentable Code = "SLUICE-E-EXPORT-UNREPRESENTABLE"

	CodeWhereFilterFKOrphan Code = "SLUICE-E-WHERE-FK-ORPHAN"

	// CodeWhereFilterUnknownTable fires at migrate/verify start when a
	// `--where TABLE=<predicate>` key names no table in the (table-scoped)
	// source schema — a typo, or a case-fold mismatch the readers' exact-case
	// lookup would silently drop, disabling the filter and copying/counting
	// the WHOLE table (scope-escape). Refusing beats a silent whole-table
	// copy, mirroring the CDC sibling (which already refuses the same class)
	// and `--map`'s unknown-table rejection.
	CodeWhereFilterUnknownTable Code = "SLUICE-E-WHERE-UNKNOWN-TABLE"

	// The ADR-0173 Phase 2 (continuous filtered sync) refusals.
	// CodeWhereCDCUnsupportedPredicate fires at sync-start when a
	// `--where` predicate uses a construct the client-side CDC evaluator
	// cannot faithfully evaluate (a function, subquery, collation-sensitive
	// string op, or an unknown token) — refusing beats silently diverging
	// from the source's own evaluation. CodeWhereCDCBeforeImage fires when a
	// filtered CDC source is not configured to deliver full before-images
	// (MySQL binlog_row_image!=FULL, PG REPLICA IDENTITY not FULL on a
	// filtered table), which the row-move evaluation requires.
	// CodeWhereCDCAfterImage is the mid-stream AFTER-image sibling (audit
	// 2026-07-23 D0-1): a filtered UPDATE whose after-image omits a
	// predicate column would be mis-classified as a move-OUT and emit a
	// spurious DELETE for an in-scope row; the PG reader backfills the one
	// known producer (pgoutput's unchanged-TOAST omission), and this belt
	// stops the stream loudly for any shape that slips past it.
	CodeWhereCDCUnsupportedPredicate Code = "SLUICE-E-WHERE-CDC-UNSUPPORTED-PREDICATE"
	CodeWhereCDCBeforeImage          Code = "SLUICE-E-WHERE-CDC-BEFORE-IMAGE"
	CodeWhereCDCAfterImage           Code = "SLUICE-E-WHERE-CDC-AFTER-IMAGE"

	// CodeWherePushdownDrift fires at sync-start when a warm resume's
	// current `--where` flags don't match the row-filter subset the stream
	// PUSHED into its Postgres publication at cold start (recorded as
	// row_filter_hash in sluice_cdc_state — audit 2026-07-23 D0-2). The
	// pushed filter is durable source-side catalog state a warm resume
	// never re-ensures, so silently resuming would leave the SERVER
	// filtering on the stale predicate — under-delivery the client cannot
	// observe. Refusing names the escapes (re-pass the original --where,
	// --restart-from-scratch, --reset-target-data).
	CodeWherePushdownDrift Code = "SLUICE-E-WHERE-PUSHDOWN-DRIFT"

	// CodeCopyRetryAmbiguousKeyless is the cold-copy write cores' refusal
	// to RE-SEND a batch/chunk on a table that could not notice a
	// duplicate. Both engines' bounded transient retry re-sends the same
	// rows on a classified transient; that is sound only because a table
	// with a key turns "the prior attempt committed but lost its ack" into
	// an observable collision. Keyless, the two outcomes are identical to
	// every observer — so the retry is refused instead (audit B-9).
	CodeCopyRetryAmbiguousKeyless Code = "SLUICE-E-COPY-RETRY-AMBIGUOUS-KEYLESS"

	// CodeCDCSchemaReplayMismatch is the MySQL binlog reader's refusal
	// when a buffered row event's TABLE_MAP type vector disagrees with the
	// table's CURRENT information_schema shape. Live at the binlog head
	// that cannot happen (MySQL serialises DDL against DML on one table),
	// but a warm resume REPLAYS history recorded before a DDL that ran
	// while sluice was down — and a DDL that keeps the column COUNT the
	// same decodes old values against the new types, right count and wrong
	// meaning (audit CDC-4).
	CodeCDCSchemaReplayMismatch Code = "SLUICE-E-CDC-SCHEMA-REPLAY-MISMATCH"
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
	CodeCDCPoolerEndpoint:        {ClassRuntime, "the source appears to be a connection pooler (Supavisor/pgbouncer) that stripped the replication startup parameter; CDC needs the direct endpoint"},
	CodeCDCStandbySource:         {ClassRefusal, "the CDC source is a read-only standby / read replica (pg_is_in_recovery() = true); point --source at the primary endpoint — a replica remains fine for bulk migrate"},
	CodeCDCXAUnsupported:         {ClassRefusal, "a replicated table is written inside a MySQL XA (distributed) transaction, which sluice cannot faithfully replicate: the rows are invisible on the source until XA COMMIT, so a rollback would fabricate them on the target, and mid-body positions are not valid restart points"},
	CodeConnectIPv6Only:          {ClassRuntime, "the DSN host resolves to an AAAA record only (IPv6-only) and this network appears IPv4-only"},
	CodeDDLEmitMultiStatement:    {ClassRefusal, "an emitted DDL string is structurally more than one statement — sluice's emitters only ever produce one, so recorded schema content (a backup manifest's expression bodies) is corrupt or tampered, and executing it would run the extra SQL as the connected role; refused before the server saw it"},

	CodeConstraintStatementTimeLimit: {ClassRuntime, "foreign-key ADD hit PlanetScale's statement-time limit (errno 3024) — ADD FOREIGN KEY validates every child row, so on a large table it is heavier than an index build and cannot finish under the ~900 s wall; --resume re-hits the identical deterministic wall, so the converging remedy is --skip-foreign-keys (completes the migration, keeps each FK's backing index)"},

	CodeFKSourceOrphan: {ClassRefusal, "the item-109 metadata-only FK wall recovery added a foreign key under foreign_key_checks=0, then a bounded chunked orphan scan found a child row whose foreign-key value has no matching parent — the source was not FK-consistent after all; the FK is dropped and the run refused (loud-failure per the --allow-degraded-fks baseline). Clean the orphaned child rows on the source, or re-run with --skip-foreign-keys to complete without the constraint"},

	CodeCDCBinlogFormatNotRow: {ClassRefusal, "binlog CDC refused at start: @@GLOBAL.binlog_format is STATEMENT or MIXED, under which DML arrives as SQL text sluice cannot replay — the stream would run green while silently applying nothing; SET GLOBAL binlog_format=ROW (or the provider console) and re-run"},

	CodeCDCPublicationScopeConflict: {ClassRefusal, "cold start refused: rescoping the Postgres publication would REMOVE tables another sluice replication slot (active or inactive) holds a claim on, silently de-scoping that stream — give each stream its own --publication-name, drain the other stream first, or drop its slot if the stream is truly abandoned"},
	CodeCDCReplicationHeadroom:      {ClassRefusal, "cold start refused: the source has no replication headroom for this stream's new slot/WAL sender (max_replication_slots or max_wal_senders exhausted) — free a leftover slot (`sluice slot list`, `sync decommission`, `slot drop`) or raise the ceiling; warm resume reuses its existing slot and never trips this"},
	CodeCDCPublicationNameInvalid:   {ClassRefusal, "sync start refused: the --publication-name is not a safe Postgres replication identifier ([a-z0-9_], max 63 bytes) — CREATE PUBLICATION preserves a quoted mixed-case/over-length spelling while START_REPLICATION downcases (and CREATE truncates) it, so the stream would create one publication and stream from another: green through the whole bulk copy, then 42704 at the first change (or silently idle); use lowercase [a-z0-9_]"},

	CodeCDCChangeLogIDReuse:             {ClassRefusal, "the sqlite-trigger / d1-trigger change log can allocate an id at or below the stream's resume watermark — the id column is not AUTOINCREMENT, or sqlite_sequence was lowered — under which a captured change is never emitted by the `id > watermark` poll"},
	CodeCDCGeneratedPrimaryKey:          {ClassRefusal, "a CDC source table's row identity includes a GENERATED column that the change stream does not carry — MySQL's row decoder drops it (the target's GENERATED clause recomputes it) and PostgreSQL does not publish it before 18 — so the change has no usable identity, and the UPDATE/DELETE's WHERE would be empty, `pk IS NULL`, or a NON-UNIQUE prefix of the key that deletes target rows the source never touched; refused rather than applied"},
	CodeCDCRowImagePartial:              {ClassRefusal, "the MySQL/Vitess source streams partial row images (binlog_row_image != FULL, or binlog_row_value_options=PARTIAL_JSON; on a self-hosted Vitess/VStream source the RowChange.DataColumns bitmap marks a NOBLOB-omitted column), under which CDC silently loses UPDATEs — refused at CDC start on the binlog path, and loudly mid-stream when a partial image reaches the reader (a slipped-past global preflight, or a VStream after-image whose bitmap flags an omitted column)"},
	CodeCDCMariaDBUnsupported:           {ClassRefusal, "RETAINED-BUT-UNEMITTED: MariaDB CDC (domain-GTID positions) shipped v0.99.271 (ADR-0170); the flavor now declares CDCBinlog and this refusal is no longer emitted. Kept registered because removing a published catalog code is breaking"},
	CodeCDCMariaDBNativeTypeUnsupported: {ClassRefusal, "RETAINED-BUT-UNEMITTED: MariaDB native uuid/inet6/inet4 CDC binlog decode shipped v0.99.272 (ADR-0171), lifting this refusal — the CDC tail now converges byte-for-byte with the bulk-copy text. Kept registered because removing a published code is breaking; no longer emitted"},

	CodeDecommissionStreamActive: {ClassRefusal, "sync decommission refused: the stream's replication slot is active on the source (a CDC consumer is attached — the stream looks live); drain it with `sluice sync stop --wait` first, then re-run decommission"},

	CodeColdStartTargetNotEmpty:   {ClassRefusal, "cold-start refused: a target table already contains data"},
	CodeSchemaExtensionNotEnabled: {ClassRefusal, "column type owned by a PG extension not opted into"},
	CodeSchemaIdentifierInvalid:   {ClassRefusal, "a schema value bound for a DDL position that takes a BARE (unquotable) identifier — index access method, operator class, sequence data type, RLS policy command, MySQL charset/collation — is not a bare identifier or not an accepted keyword; refused rather than interpolated, because at those positions a `;` in the value is a second statement"},

	CodeSchemaIdentifierTooLong:  {ClassRefusal, "an emitted PostgreSQL identifier (table, column, index, constraint or enum type) exceeds NAMEDATALEN-1 = 63 bytes, which PG truncates SILENTLY at CREATE time — and because sluice emits the IF NOT EXISTS form, the resulting collision is a silent no-op rather than an error; shorten or alias the source name"},
	CodeSchemaIndexNameCollision: {ClassRefusal, "two distinct source indexes resolve to ONE index identifier on a target whose index names are SCHEMA-scoped — on Postgres because sluice's table-scoping transform is not injective (index `user_id` on table `posts` and index `posts_user_id` on the same table both render `posts_user_id`), on SQLite because there is no transform at all and two tables' identically named indexes land on one name — and CREATE INDEX IF NOT EXISTS would silently no-op the second build; rename one of the two source indexes"},
	CodeSchemaViewNameCollision:  {ClassRefusal, "a source VIEW resolves to a name a TABLE or another view already occupies on a SQLite target, where `CREATE VIEW IF NOT EXISTS` returns OK and creates NOTHING — the view is absent at exit 0; rename the view (or the table) on the source"},

	CodeSchemaTableNameCollision:       {ClassRefusal, "two distinct source TABLES resolve to ONE table identifier on a SQLite target (its object names are compared case-insensitively, so `orders` and `Orders` are one name), where `CREATE TABLE IF NOT EXISTS` returns OK and creates NOTHING — the second table's rows are then INSERTed under a name that resolves to the FIRST table, so both tables' rows land in one table at exit 0 and the surviving name's count is right; rename one of the two source tables"},
	CodeConfigMultiNamespaceTargetFlat: {ClassRefusal, "a multi-namespace fan-out (--all-databases / --include-database / --include-schema / --map-database, on migrate or sync) was aimed at a target engine that can neither derive a per-namespace DSN nor accept a schema override, so every source namespace would be written into ONE flat target namespace and same-named tables would silently merge; migrate one namespace per run (one target file/DSN each), or choose a target that namespaces"},
	CodeValueZeroDate:                  {ClassRefusal, "MySQL zero/partial date has no valid calendar value"},
	CodeValueNULByte:                   {ClassRefusal, "string value carries a NUL byte PostgreSQL text types cannot store"},
	CodeValueUnrepresentable:           {ClassRefusal, "a value no target column type can represent (e.g. NaN/±Infinity into a MySQL FLOAT/DOUBLE)"},
	CodeValueRaggedArray:               {ClassRefusal, "a non-rectangular (jagged) array value bound for a PostgreSQL array column — PG arrays are rectangular by definition, so there is no faithful target value"},
	CodeValueByteaTextUnrecognized:     {ClassRefusal, "a PostgreSQL bytea value rendered as text sluice cannot read — not the `\\x`+even-hex form `bytea_output = hex` produces"},
	CodeExprBackslashLiteral:           {ClassRefusal, "SQLite expression string literal with a backslash has no faithful MySQL spelling"},
	CodeConfirmationRequired:           {ClassRefusal, "destructive operation requires explicit --yes confirmation"},
	CodeDriverHostMismatch:             {ClassRefusal, "the driver cannot drive the server (a non-VStream flavor at a PlanetScale endpoint or Vitess-reporting server, or mariadb at a non-MariaDB server)"},
	CodeIndexMissing:                   {ClassRefusal, "a secondary index the migration was expected to build is absent on the target — or, on Postgres, present under the expected name but NOT UNIQUE when a UNIQUE index was requested (the shape a silently-no-op'd colliding build leaves behind)"},
	CodeVStreamFloatLossy:              {ClassRefusal, "--strict-float: a VStream-COPY FLOAT column cannot be re-read exactly on a backup or a sync cold-start (keyless / float-PK / over the backup row cap / no streamed row matched / contradicted by --no-float-exact-reread)"},
	CodeBackupSignatureInvalid:         {ClassRefusal, "a signed backup manifest's detached signature failed verification (tampered / rolled-back / wrong key)"},
	CodeBackupSignatureMissing:         {ClassRefusal, "a signed (v6) backup manifest is missing its detached signature"},
	CodeBackupSignatureUnsupported:     {ClassRefusal, "a signed backup manifest uses a newer signature scheme/canonicalization than this build supports; upgrade sluice (not a tamper signal)"},
	CodeBackupChunkAuthFailed:          {ClassRefusal, "an encrypted backup chunk failed authenticated decryption (tampered / corrupt / spliced or reordered store) — the loud, coded twin of the signed-manifest tamper refusal for backups that are encrypted but not signed"},
	CodeBackupChunkCorrupt:             {ClassRefusal, "a backup chunk's stored bytes do not match the SHA-256 recorded for it in the manifest — at-rest corruption / bit-rot, or a tamper that altered the stored bytes; caught by rehashing at restore, broker replay, and backup verify, before decryption, so it fires on plaintext and encrypted chunks alike (the integrity twin of -CHUNK-AUTH-FAILED, which is the GCM/AAD check)"},
	CodeBackupChunkUnreadable:          {ClassRefusal, "a backup chunk's bytes are INTACT — they hash to the manifest's SHA-256 and, when encrypted, open under the chain's key and binding — and its own reader still cannot decode it: an over-long row line, a truncated codec stream, a wrong recorded codec, or a row the decoder rejects. Raised only by `backup verify --depth read`, which is the depth that parses; the hash-only default cannot see this class at all"},
	CodeBackupIncomplete:               {ClassRefusal, "a restore/replay/export decoded or applied a DIFFERENT number of rows/changes than the manifest records (change-chunk tail truncated, a layer-2 row-count mismatch, or a zeroed RowCount) — the signing-independent backstop against silent truncation/edit of an unsigned backup"},
	CodeBackupInterrupted:              {ClassRefusal, "a backup manifest records partial_state=in_progress — the run that wrote it was interrupted (or is still going), so it lists only the tables finished at that moment; refused by restore, backup verify, and export-as-parquet before any data is read, because reading it creates every table and loads only some while exiting 0"},
	CodeBackupManifestInvalid:          {ClassRefusal, "a backup manifest (or the chain of manifests) fails an internal-consistency check — recorded BackupID or schema hash not matching the recomputed content, or a segment mixing encrypted and plaintext chunks (corruption, a mis-stitched lineage, or a lazy tamper)"},
	CodeBackupChainConflict:            {ClassRefusal, "another writer advanced this backup chain mid-operation (a duplicate cron backup incremental, a backup racing a compact/prune, or an operator double-start) — either the conditional catalog write refused rather than interleave, or this run's parent is no longer the chain's tip and committing it would fork the lineage; no catalog change was written"},
	CodeBackupEncryptionMismatch:       {ClassRefusal, "the supplied encryption configuration does not match the chain's recorded encryption metadata — an encrypted chain opened (or extended by a writer) without --encrypt + key material, or an envelope whose KEK mode (passphrase / KMS) differs from the chain's recorded kek_mode"},
	CodeBackupRecordedSchemaMalformed:  {ClassRefusal, "the chain's recorded schema carries an expression whose string literal never closes (a pre-v0.120.0 MySQL-family reader mangled apostrophe-carrying expressions at schema read), so the recorded DDL cannot be emitted as valid SQL — raised by backup verify and pre-DDL by restore/chain restore; Bug 243"},
	CodeBackupStoreNameCollision:       {ClassRefusal, "the destination store folds letter case (measured by a two-object probe) and case-colliding table names would write to one folded path, silently overwriting one table's data at exit 0 — refused before any write at backup full / export-as-parquet; back up to a case-sensitive store, exclude one colliding table, or rename one source table; Bug 248"},
	CodeBackupChainUnreadable:          {ClassRefusal, "backup compact / backup prune re-read the chain the way a restore would and could not: the chain does not walk, or its identity/key material (the chain-root manifest.json a passphrase chain's Argon2id salt is recorded on) is missing or inconsistent — refused BEFORE the destructive sweep with nothing deleted, or reported AFTER it so the run never exits 0 over a chain it just made unreadable"},

	CodeBackupSchemaDeltaUnsupported: {ClassRefusal, "a chain restore / broker replay hit an alter_table schema delta whose shape has no faithful replay on the target (a dropped column, a changed primary key, a column that gained or lost GENERATED, or a malformed entry) — the appliable shapes (added column, column type, nullability, indexes) are emitted through the engine's delta surface; this is the loud half for the rest"},

	CodeBackfillNoPrimaryKey:      {ClassRefusal, "backfill refused: the table has no usable orderable primary key to drive the keyset-chunked walk"},
	CodeBackfillUnsupportedEngine: {ClassRefusal, "backfill refused: the engine does not implement the in-place backfill surface"},
	CodeBackfillUnknownColumn:     {ClassRefusal, "backfill refused: a --set column does not exist on the table"},
	CodeBackfillIncomplete:        {ClassRuntime, "backfill verify found rows still matching the --where guard after the walk — online catch-up needed (rows written behind the cursor), or the guard does not self-describe doneness"},
	CodeBackfillCorruptCursor:     {ClassRefusal, "backfill refused to resume: the persisted cursor was written by an older sluice whose JSON store mangled binary or large-integer PK values — resuming from it would silently skip or replay PK ranges; re-run with --restart"},
	CodeBackfillConcurrentRun:     {ClassRefusal, "backfill refused to start: the spec's state row shows a heartbeat fresher than the freshness window while still in the walking phase — another run (typically an overlapping cron invocation) looks live, and two concurrent walks of one spec interleave cursor writes; wait for it to finish or for its heartbeat to go stale"},
	CodeBackfillVerifyNoEvidence:  {ClassRefusal, "backfill --verify refused to authorize the contract step: the run it verified updated no rows, so its zero remaining-count is not evidence the backfill happened — the guard matched nothing before the walk (typically a wrong --where), rows matched and none were updated, or the completed run it no-ops over recorded no work"},

	CodeSourceForeignDump:   {ClassRefusal, "the --source file is a plain mysqldump/pg_dump SQL dump or a pg_dump custom-format (PGDMP) archive — formats sluice deliberately does not parse; the message carries the scratch-server-replay recipe"},
	CodeSourceWrongDriver:   {ClassRefusal, "the --source is a recognisable format this source driver does not read (e.g. a mydumper directory handed to the csv driver) — the message names the right driver or preparation step"},
	CodeCSVNullAmbiguous:    {ClassRefusal, "a csv/tsv source contains an unquoted empty field and no NULL representation was declared — RFC 4180 has no NULL, so sluice refuses to guess; declare the convention with --csv-null"},
	CodeCSVHeaderUndeclared: {ClassRefusal, "a csv/tsv source was opened without declaring header presence — sluice never sniffs it; pass --csv-header or --csv-no-header"},

	CodeSourceReplicaIdentity: {ClassRefusal, "PG-source cold start refused before the publication was scoped: an in-scope source table has no usable replica identity (its only key is DEFERRABLE, it has no key at all, REPLICA IDENTITY is NOTHING, or the identity includes a STORED GENERATED column Postgres does not publish) — publishing its UPDATEs/DELETEs would make Postgres reject the SOURCE APPLICATION's own writes to it; make the key immediate, point REPLICA IDENTITY USING INDEX at an immediate NOT NULL UNIQUE index, or exclude the table — REPLICA IDENTITY FULL fixes the first three shapes but NOT the generated one"},

	CodeTargetDeferrableKey: {ClassRefusal, "refused before applying anything: a target table's primary key (or only usable unique key) is DEFERRABLE, and Postgres rejects a deferrable constraint as an `ON CONFLICT` arbiter — so sluice's idempotent apply/copy upsert cannot key on it; recreate the target constraint as immediate (NOT DEFERRABLE), pre-create the target table with an immediate key, or take the table out of scope"},

	CodeTargetTableShapeMismatch: {ClassRefusal, "migrate refused before any data moved: a target table with the same name already exists but its column shape (names/types/nullability) differs from what the migration would create — proceeding would fail mid-copy or land rows in the wrong columns"},

	CodeTargetPreexistingForeignKey: {ClassRefusal, "migrate/sync cold-start refused before any data moved: the target already carries a foreign key on a table this run copies into, and that constraint's parent table is copied by the SAME run — the copy is not parent-first ordered, so a child row reaches the target before its parent and the constraint rejects it (MySQL Error 1452 / Postgres SQLSTATE 23503); sluice's deferred-constraint discipline only governs the constraints it creates itself"},

	CodePSSafeMigrationsDisabled: {ClassRefusal, "expand-contract refused: the PlanetScale production branch does not have safe migrations enabled (the deploy-request prerequisite); sluice never auto-enables it"},
	CodePSDeployRequestFailed:    {ClassRuntime, "a PlanetScale deploy request entered a failure state, was closed without deploying, computed an empty diff, or computed a diff outside the leg's intended blast radius — the message carries the DR number, state, and URL. A deploy sluice merely stopped WAITING on is the separate SLUICE-E-PS-DEPLOY-REQUEST-INCOMPLETE"},

	CodePSDeployRequestIncomplete: {ClassRuntime, "a PlanetScale deploy request sluice was waiting on did not reach a terminal state and NOTHING failed: a wall-clock bound was hit while the deploy was healthy and progressing, or the request is parked on a gate only a person can clear (deploy approval, a manual cutover confirmation). The deployment keeps running in PlanetScale and its dev branch must be LEFT ALONE until it finishes"},

	CodePSDevBranchNotAdoptable: {ClassRefusal, "a re-run found its own deterministic PlanetScale dev branch left by an earlier attempt and could not ADOPT its deploy request: there is none, there is more than one, it merges into a different production branch, it was never deployed (sluice cannot re-establish the pre-deploy freshness/blast-radius guarantees for a branch it did not provision this run), or it ended in a terminal failure. A deploy request that is deploying or already deployed is adopted instead — no code, no refusal"},

	CodePSBranchStaleBase:  {ClassRuntime, "a PlanetScale dev branch's schema still differs from production after a rebase backup (a new dev branch's schema can lag production — intermittent, timing undocumented), or production's schema changed while a deploy request sat in its review/deploy wait — either way, deploying would silently revert newer production schema"},
	CodePSDirectDDLBlocked: {ClassRefusal, "PlanetScale safe migrations refused a direct DDL statement sluice needs (Error 1105) — a user-table CREATE during schema apply, or sluice's own control tables. Ship the DDL through the governed channel (`sluice deploy-ddl`; `sluice control-tables ddl` / `sluice schema preview` print the statements) or disable safe migrations for the window — the message carries the per-case recipe"},

	CodeExportUnrepresentable: {ClassRefusal, "backup export-as-parquet refused: a column type or value has no faithful Parquet representation (multi-dimensional array, out-of-day-range TIME, NUMERIC NaN/Infinity, sub-microsecond timestamp) — exclude the table or query the JSON-Lines chunks directly; sluice never silently narrows a value on export"},

	CodeWhereFilterFKOrphan:     {ClassRefusal, "a --where row filter on a parent table orphaned its children, so the deferred ADD CONSTRAINT FOREIGN KEY failed (SQLSTATE 23503) on the target — filter consistently so the referenced parent rows are copied, or pass --allow-degraded-fks to attach the FK as NOT VALID"},
	CodeWhereFilterUnknownTable: {ClassRefusal, "a migrate/verify --where TABLE=<predicate> key names no table in the (table-scoped) source schema — a typo or a case-fold mismatch would silently disable the filter and copy/count the whole table; correct the table name (matching is case-insensitive) or remove the --where entry"},

	CodeWhereCDCUnsupportedPredicate: {ClassRefusal, "continuous filtered sync (--where on `sync`) refused at start: the predicate uses a construct the client-side CDC evaluator cannot faithfully evaluate (a function/subquery, an ordering or collation-sensitive string comparison, an unknown column, or unrecognized syntax) — evaluating it client-side could diverge from the source's own evaluation and silently leak or drop rows"},
	CodeWhereCDCBeforeImage:          {ClassRefusal, "continuous filtered sync (--where on `sync`) refused at start: the source is not configured to deliver full row before-images (MySQL binlog_row_image!=FULL, or a filtered PG table without REPLICA IDENTITY FULL), which the --where row-move evaluation requires to decide whether an UPDATE moved a row into or out of the filter's scope"},
	CodeWhereCDCAfterImage:           {ClassRefusal, "continuous filtered sync (--where on `sync`) stopped mid-stream: an UPDATE on a filtered table arrived with an AFTER-image missing a column the predicate references, so the row-move evaluation cannot decide whether the row left the filter's scope — evaluating over the missing column would read NULL and could emit a spurious DELETE for a row still in scope at the source (the audit 2026-07-23 D0-1 unchanged-TOAST class; the PG reader backfills that producer, so this belt firing means an unexpected partial after-image reached the filter)"},
	CodeWherePushdownDrift:           {ClassRefusal, "warm resume refused: the current --where flags don't match the row-filter subset this stream PUSHED into its Postgres publication at cold start — the publication filter is durable source-side state a warm resume never re-ensures, so resuming would leave the SERVER silently filtering on the stale predicate (under-delivery the client cannot observe); re-run with the --where the stream was established with, or --restart-from-scratch to re-snapshot under the new predicate (on a PG source the restart first refuses on the stream's existing replication slot — drop it as that refusal instructs, then re-run), or --reset-target-data for a destructive reset"},

	CodeCopyRetryAmbiguousKeyless: {ClassRefusal, "a cold copy hit a transient target error mid-batch on a table with no PRIMARY KEY and no NOT NULL UNIQUE index, and refused to re-send those rows: an attempt that committed but lost its acknowledgement is indistinguishable from one that rolled back, and with no unique key there is nothing that would make the second write fail — so the retry that rides a PlanetScale reparent or a storage-grow window on every other table would silently duplicate this one"},
	CodeCDCSchemaReplayMismatch:   {ClassRefusal, "a MySQL binlog row event's TABLE_MAP column-type vector disagrees with the table's current information_schema shape — the event was recorded under a DIFFERENT schema than the one it is about to be decoded against, which for a same-column-count DDL (a type change) would remap every replayed value silently; refused rather than decoded"},
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
