// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"context"
	"errors"
	"time"
)

// Position is an opaque, durable bookmark within a source's change
// stream. Engines populate Token in their own format (binlog file plus
// offset, Postgres LSN, etc.); the IR treats it as opaque. Engine names
// the engine that produced the position so a position store can guard
// against cross-engine confusion.
type Position struct {
	Engine string
	Token  string
}

// ErrPositionInvalid signals that a persisted [Position] no longer
// references resumable state on the source — the PG slot was dropped,
// the MySQL binlog was purged, the VStream GTIDs are too old. The
// pipeline orchestrator detects this via [errors.Is] from a CDC
// reader's start path and falls through to cold-start (re-snapshot +
// fresh slot/position) rather than surfacing an unrecoverable error
// the operator has no flag to bypass.
//
// Engines wrap their specific diagnostic with this sentinel via %w:
//
//	return fmt.Errorf("postgres: replication slot %q no longer exists: %w",
//	    name, ir.ErrPositionInvalid)
//
// See ADR-0022 for the full rationale and the trigger-narrowness
// contract (only applies when the referenced state is gone, not when
// it's merely degraded — `wal_status='lost'` stays strict).
var ErrPositionInvalid = errors.New("ir: persisted position is no longer valid; cold-start is the only recovery path")

// ErrPositionForeignLineage is the verdict that the source answering the
// DSN is NOT the instance or lineage the persisted position was captured
// from — a replaced, restored-from-the-wrong-backup, or DNS-failed-over
// endpoint, or a keyspace that is simply a different database.
//
// It deliberately does NOT satisfy errors.Is(err, ErrPositionInvalid).
// That sentinel routes the streamer's automatic recovery, which drops the
// target's in-scope tables and re-copies from whatever now answers the
// DSN; correct for a routine purge on the SAME source, destructive here.
// Audit 2026-09-09 (A0909-MYSQL-HIGH-1) measured the GTID arm doing
// exactly that: four correct target rows replaced by the wrong
// database's one row, at exit 0. Engines wrap this via %w at every site
// whose diagnosis is "different lineage / different instance", and at
// the sites whose automatic re-copy would reduce a target that is AHEAD
// of its source (MySQL: an executed set BEHIND the position under the
// server's own uuid; MariaDB: an empty binlog state, which cannot be
// told from a rebuilt node). A site whose shape is a same-server reset
// (MySQL: an empty executed set under the server's own uuid) keeps
// ErrPositionInvalid, and says so.
//
// The pipeline refuses on it — loudly, with the deliberate re-copy
// flags named — on BOTH the pre-flight warm-resume path and the reactive
// path, which asks the reader through [LineageVerifier] before it drops
// anything.
var ErrPositionForeignLineage = errors.New("ir: the source answering this DSN is not the lineage the persisted position was captured from; refusing the automatic re-copy")

// PositionOrderer is an optional Engine capability the ADR-0049 CDC
// schema-history store uses to resolve an event's position to the
// schema version in effect at that position. Positions are
// engine-opaque (ADR-0007); only the engine can order them. The
// ordering is a PARTIAL "is-at-or-after" causal predicate — MySQL GTID
// sets are subsets, NOT a total order; modelling them as a -1/0/1
// comparator is the Bug-74-class trap and is explicitly rejected.
type PositionOrderer interface {
	// PositionAtOrAfter reports whether event position p is at or
	// after anchor — i.e. whether the schema snapshotted at anchor is
	// the one in effect for an event at p. Reuse the engine's existing
	// causal test (MySQL: GTID-subset, the verifyGTIDSetReachable
	// primitive; PG: LSN <=). err is non-nil ONLY for a
	// malformed/unparseable position (a real bug), never for an
	// ordinary "false" answer.
	PositionAtOrAfter(p, anchor Position) (bool, error)
}

// Row is a single tuple of values keyed by column name. Values use
// Go-native types corresponding to the column's IR type; the engine
// reader is responsible for converting from driver-native types into
// IR-native ones.
type Row map[string]any

// Change is the sealed interface for events in a continuous-sync change
// stream. Implementations are [Insert], [Update], [Delete], [Truncate],
// [TxBegin], [TxCommit], and [SchemaSnapshot]. New variants must be
// added in this package.
type Change interface {
	// isChange seals the interface.
	isChange()
	// Pos returns the durable position from which this change can be
	// resumed. The value is opaque to consumers of the IR.
	Pos() Position
	// QualifiedName returns "schema.table" for the affected table, or
	// just "table" when Schema is empty. Transaction-boundary events
	// ([TxBegin], [TxCommit]) carry no table reference and return "".
	QualifiedName() string
	// SourceCommitTime returns the source-side commit timestamp of the
	// transaction this change belongs to — the instant it committed on
	// the SOURCE (MySQL binlog/VStream event time, PG pgoutput commit
	// time, postgres-trigger committed_at). It is the input to the
	// engine-neutral "seconds behind source" sync-lag metric (roadmap
	// item 45). A zero time means the source/path does not surface one;
	// consumers MUST treat zero as "unknown" and omit the derived lag
	// rather than report a misleading value (the *Known honesty contract
	// the metrics layer already uses elsewhere).
	SourceCommitTime() time.Time
}

// qualified is a small helper that mirrors Table's identification logic.
func qualified(schema, table string) string {
	if schema == "" {
		return table
	}
	return schema + "." + table
}

// UnqualifiedTableName strips a leading `schema.` from a qualified name,
// scanning from the END.
//
// # Why this is a shared function and not two local loops
//
// [qualified] has no unambiguous inverse: it joins with a dot, and a
// TABLE NAME may itself contain one, so `db.a.b` could be database `db`
// with table `a.b` or schema `db.a` with table `b`. The string alone
// cannot say. What matters is not which reading is philosophically right
// but that every consumer picks the SAME one — because the places that
// compare a name against the operator's `--include-table` /
// `--exclude-table` filter must agree with the places that decide
// whether a refusal applies.
//
// They did not. Audit 2026-09-09 VF0909C-3: the pipeline's dispatch
// filter scanned from the end while the MySQL reader's scope predicate
// split at the FIRST dot, so for a table named `a.b` the reader tested
// `a.b` and the dispatch tested `b`. A stream-killing refusal the reader
// skipped as out-of-scope could then have its boundary forwarded by a
// dispatch that considered the table in scope — the reader declining to
// refuse a re-cast that the target then received. Low likelihood, since
// dotted table names are rare, and a genuine silent-divergence path.
//
// Scanning from the end is the dispatch filter's long-standing
// behaviour, so it is the reading kept: matching the reader to the
// dispatch changes no filter semantics for anyone, while matching the
// dispatch to the reader would silently change which tables an existing
// `--include-table` selects.
//
// A name with no dot is returned unchanged, which is the Schema-empty
// case.
func UnqualifiedTableName(qualifiedName string) string {
	for i := len(qualifiedName) - 1; i >= 0; i-- {
		if qualifiedName[i] == '.' {
			return qualifiedName[i+1:]
		}
	}
	return qualifiedName
}

// ApplyID is a row change's stable identity within its source
// transaction (ADR-0190): TxID names the source transaction, Seq is the
// change's ordinal among that transaction's changes to the SAME table,
// counted by the reader before any sluice-side filter.
//
// The READER assigns it — never the pipeline or the applier — so a
// pipeline filter, an --include-table change or a live add-table cannot
// shift it, and a re-delivery of the same transaction numbers every
// change exactly as the first delivery did. That stability is the whole
// contract: an applier skips a re-delivered change only when a durable
// apply mark written with the change's own data names the same (TxID,
// Seq), so an identity that could drift would skip work the target never
// received.
//
// The zero value means "no identity": a reader that does not declare
// [ApplyIdentityProvider] leaves it zero, as do the changes a declaring
// reader cannot name (VStream COPY rows and interleaved shard groups), and
// an applier NEVER skips a change with a zero identity — today's
// at-least-once replay, exactly.
//
// A backup carries the identity THROUGH, it never assigns one (ADR-0191):
// the change-chunk codec records the reader's identity at capture and
// restores it byte-exact on every read, so `sync from-backup` and chain
// restore present a replayed change with the same (TxID, Seq) the reader
// stamped. An incremental written before ADR-0191, or rewritten by smart
// compaction ([WithoutApplyID]), replays with zero identities. Held by
// TestReplayIdentityIsPassThrough: on the replay paths, the codec's decode
// is the only place an ApplyID is ever assigned.
type ApplyID struct {
	TxID string
	Seq  uint64
}

// IsZero reports whether id carries no identity.
func (id ApplyID) IsZero() bool { return id.TxID == "" }

// ApplyIDOf returns a row change's [ApplyID], or the zero value for a
// change kind that carries none (Truncate, Tx boundaries,
// SchemaSnapshot).
func ApplyIDOf(c Change) ApplyID {
	switch v := c.(type) {
	case Insert:
		return v.ApplyID
	case Update:
		return v.ApplyID
	case Delete:
		return v.ApplyID
	}
	return ApplyID{}
}

// WithoutApplyID returns c with its [ApplyID] cleared — the one way a
// replay-side rewrite (smart compaction, ADR-0191 Q9) drops the identity of a
// change it re-encodes, so a collapsed change can never carry an identity
// naming a transaction it no longer reproduces. A change kind that carries no
// identity is returned unchanged.
func WithoutApplyID(c Change) Change {
	switch v := c.(type) {
	case Insert:
		v.ApplyID = ApplyID{}
		return v
	case Update:
		v.ApplyID = ApplyID{}
		return v
	case Delete:
		v.ApplyID = ApplyID{}
		return v
	}
	return c
}

// ApplyIdentityProvider is the optional [CDCReader] capability declaring
// that the reader stamps a stable [ApplyID] on every row change of a
// source transaction (ADR-0190). It is a declaration, not a switch: an
// applier consults each change's own identity, and a reader without this
// capability simply produces zero identities. The declaration is what a
// doc or a gate that names "the sources exactly-once apply marks cover"
// derives that set from.
type ApplyIdentityProvider interface {
	// StampsApplyIdentity is a marker method; implementations return
	// true.
	StampsApplyIdentity() bool
}

// ApplyMarksClearer is the optional [ChangeApplier] surface that deletes a
// stream's ADR-0190 apply marks. Every cold start calls it before its copy:
// the copy re-seeds the target, so a mark naming a transaction of the old
// stream can never again vouch for anything. The skip rule only trusts a
// mark whose TxID the replayed change shares, so an old mark is inert while
// source transaction identities stay unique — the clear is what keeps it
// inert across the one event that can recycle them, a source whose history
// was reset (a new binlog lineage, RESET MASTER), which always lands here as
// a cold start. Idempotent; tolerant of a target that has no mark table.
type ApplyMarksClearer interface {
	ClearApplyMarks(ctx context.Context, streamID string) error
}

// ApplyMarksCoverageProber is the optional [ChangeApplier] surface a replay
// path asks before it replays a table whose re-applied rows nothing but
// ADR-0190's apply marks can keep from duplicating — a keyless table, or one
// keyed only on a column the rows do not carry (ADR-0191 §3.5). The marks
// make such a replay exactly-once only when all three hold, and only the
// target can say whether they do:
//
//   - the mark table is usable by this role (else the run applies without
//     marks behind APPLY-MARKS-UNAVAILABLE);
//   - a mark and its rows commit atomically (not a vtgate MULTI target with a
//     `--control-keyspace` sidecar, where a tear leaves rows without marks by
//     design; not a PlanetScale Neki target, whose cross-shard-group
//     atomicity is an UNVERIFIED PREMISE);
//   - the table's mark key on the target is computable from the replayed
//     rows: the table has no key there (marks are table-wide), or every key
//     column is one the recorded table carries. A key on a column the rows
//     do not supply yields no mark key, and the change applies unmarked.
//
// table == nil asks only the first two (the target as a whole). A table the
// target does not hold is judged by its recorded key, which is the one it is
// created with. MarksCoverReason returns "" when the marks cover the table,
// otherwise the reason they do not, for a refusal to name; a non-nil error is
// a probe that could not answer, which a caller must treat as "not covered".
type ApplyMarksCoverageProber interface {
	MarksCoverReason(ctx context.Context, table *Table) (reason string, err error)
}

// ApplyMarksRequirer is the optional [ChangeApplier] surface that closes the
// gap between a replay path's coverage question and the apply that relies on
// the answer (ADR-0191 review): [ApplyMarksCoverageProber] is asked before an
// incremental is applied, but the applier decides whether the mark table is
// usable when its apply starts, and on a definite "unusable" it disables the
// marks behind an APPLY-MARKS-UNAVAILABLE WARN and applies anyway. With
// RequireApplyMarks(true), that decision refuses instead — before any change
// of the apply is written — with an error wrapping
// applymarks.ErrMarksRequired. false restores the WARN. Every applier that
// declares [ApplyMarksCoverageProber] must declare this too: a caller that
// lifted a refusal on the prober's word fails closed without it.
type ApplyMarksRequirer interface {
	RequireApplyMarks(on bool)
}

// Insert is a row-insertion change event.
type Insert struct {
	Position Position
	Schema   string
	Table    string
	Row      Row
	// CommitTime is the source-side commit timestamp; see
	// [Change.SourceCommitTime]. Zero when the source/path does not
	// supply one.
	CommitTime time.Time
	// ApplyID is the change's identity within its source transaction
	// (ADR-0190); zero when the reader supplies none.
	ApplyID ApplyID
}

func (Insert) isChange()                     {}
func (e Insert) Pos() Position               { return e.Position }
func (e Insert) QualifiedName() string       { return qualified(e.Schema, e.Table) }
func (e Insert) SourceCommitTime() time.Time { return e.CommitTime }

// Update is a row-modification change event. Before captures the prior
// state (when available from the source); After captures the new state.
// Engines that cannot supply a Before image should leave it nil; the
// applier's behaviour in that case is engine-pair specific.
type Update struct {
	Position Position
	Schema   string
	Table    string
	Before   Row // optional; nil when the source does not supply it
	After    Row
	// CommitTime is the source-side commit timestamp; see
	// [Change.SourceCommitTime].
	CommitTime time.Time
	// ApplyID is the change's identity within its source transaction
	// (ADR-0190); zero when the reader supplies none.
	ApplyID ApplyID
}

func (Update) isChange()                     {}
func (e Update) Pos() Position               { return e.Position }
func (e Update) QualifiedName() string       { return qualified(e.Schema, e.Table) }
func (e Update) SourceCommitTime() time.Time { return e.CommitTime }

// Delete is a row-removal change event. Before holds the row that was
// removed, when the source supplies it.
type Delete struct {
	Position Position
	Schema   string
	Table    string
	Before   Row // optional; nil when the source does not supply it
	// CommitTime is the source-side commit timestamp; see
	// [Change.SourceCommitTime].
	CommitTime time.Time
	// ApplyID is the change's identity within its source transaction
	// (ADR-0190); zero when the reader supplies none.
	ApplyID ApplyID
}

func (Delete) isChange()                     {}
func (e Delete) Pos() Position               { return e.Position }
func (e Delete) QualifiedName() string       { return qualified(e.Schema, e.Table) }
func (e Delete) SourceCommitTime() time.Time { return e.CommitTime }

// Truncate is a whole-table truncation event. Some sources emit this as
// a DDL-flavoured event; the IR treats it as a data-stream change so
// appliers can react without parsing DDL.
//
// Cascade + RestartIdentity carry the PG pgoutput TruncateMessage
// option flags (bit 0 = CASCADE, bit 1 = RESTART IDENTITY) through to
// the target applier. Bug 98 (v0.92.0): pre-fix these were discarded,
// so a source-side `TRUNCATE ... CASCADE` on a parent of an FK chain
// landed on the target as a naked `TRUNCATE`, which fails with
// `ERROR: cannot truncate a table referenced in a foreign key
// constraint` (SQLSTATE 0A000) and crashes the stream. Engines that
// don't surface CASCADE-at-truncate semantics (MySQL: TRUNCATE has no
// FK CASCADE concept) silently ignore the flags on emit (applier WARN
// at apply time when set); cross-engine PG → MySQL with CASCADE set
// is the same: best-effort plain TRUNCATE, WARN logged.
type Truncate struct {
	Position        Position
	Schema          string
	Table           string
	Cascade         bool
	RestartIdentity bool
	// CommitTime is the source-side commit timestamp; see
	// [Change.SourceCommitTime].
	CommitTime time.Time
}

func (Truncate) isChange()                     {}
func (e Truncate) Pos() Position               { return e.Position }
func (e Truncate) QualifiedName() string       { return qualified(e.Schema, e.Table) }
func (e Truncate) SourceCommitTime() time.Time { return e.CommitTime }

// TxBegin marks the start of a source-side transaction. Engines that
// observe transaction boundaries in their replication protocol
// (Postgres' BeginMessage, MySQL's BEGIN QueryEvent) emit one of
// these immediately before the row events that belong to the
// transaction; [TxCommit] marks the end.
//
// The IR carries these so a [BatchedChangeApplier] can preserve
// transactional cohesion: a 5000-row source transaction commits as
// one 5000-row target transaction instead of being split across
// multiple batches by the row-count cap. Engines that don't surface
// boundaries (legacy CDCReaders, future engines that lack the
// concept) simply omit the events; the applier's row-count and idle
// flush paths still work as before, so backwards compatibility is
// automatic.
//
// Position carries the boundary's source position. For Postgres,
// pgoutput's `BeginMessage.FinalLSN` (the commit LSN of the
// transaction this Begin opens — known up front because pgoutput
// emits Begin only after the source transaction has committed). For
// MySQL, the position of the BEGIN QueryEvent in the binlog stream.
//
// Per-change [ChangeApplier.Apply] implementations treat TxBegin /
// TxCommit as no-ops: each row event already commits its own target
// transaction, so the boundary signal carries no extra information.
// See ADR-0027.
type TxBegin struct {
	Position Position
	// CommitTime is the source-side commit timestamp of the transaction
	// this boundary opens; see [Change.SourceCommitTime].
	CommitTime time.Time
}

func (TxBegin) isChange()                     {}
func (e TxBegin) Pos() Position               { return e.Position }
func (e TxBegin) QualifiedName() string       { return "" }
func (e TxBegin) SourceCommitTime() time.Time { return e.CommitTime }

// TxCommit marks the end of a source-side transaction. See [TxBegin]
// for the design rationale. A [BatchedChangeApplier] that observes
// TxCommit flushes the in-flight target transaction at this boundary
// (subject to the empty-source-tx skip — a TxBegin → TxCommit pair
// with no row events between them flushes no rows, though every apply
// path still persists its position: a Postgres reader emits such pairs
// as keepalive boundaries so an idle stream's slot advances, GC-41 (j)).
//
// Position carries the source-side POST-commit point — the position a
// resume starts AFTER this transaction from, never one that re-delivers
// it. For Postgres, pgoutput's `CommitMessage.TransactionEndLSN` (the
// first LSN after the commit record; `CommitLSN`, the record's start, is
// what logical decoding re-delivers — audit 2026-09-01 A2-1). For MySQL,
// the executed GTID set with this transaction's GTID folded in (item
// 132) or the XIDEvent's file/pos. Row events and [TxBegin] carry the
// PRE-transaction point instead, so a position persisted mid-transaction
// replays the transaction whole. Every applier persists the TxCommit
// position at a clean source-transaction boundary, which is what makes
// a warm resume after a clean stop begin at the next transaction.
type TxCommit struct {
	Position Position
	// CommitTime is the source-side commit timestamp of the transaction
	// this boundary closes; see [Change.SourceCommitTime].
	CommitTime time.Time
}

func (TxCommit) isChange()                     {}
func (e TxCommit) Pos() Position               { return e.Position }
func (e TxCommit) QualifiedName() string       { return "" }
func (e TxCommit) SourceCommitTime() time.Time { return e.CommitTime }

// SchemaSnapshot is the ADR-0049 (Chunk B) position-anchored
// schema-history boundary event. A CDC reader emits exactly one of
// these the moment it detects a *true* DDL-boundary delta for a table
// (DP-1 sign-off point ii: a real change to the column-name set or
// ordered column types, NOT merely "a FIELD/Relation event arrived"),
// carrying the table's post-DDL IR schema (Table) and the boundary
// event's OWN source position (Position — locked decision #4c: the
// DDL/FIELD/Relation event's position captured at detection, never the
// first subsequent row's).
//
// The applier persists Table keyed by Position into the engine's
// sluice_cdc_schema_history control table (ADR-0049 Chunk A store API)
// *inside the same target transaction as the ADR-0007 position write*
// (locked decision #4a). A persist failure is fatal to the stream
// (locked decision #4b) — never logged-and-continued, because a lost
// version silently degrades every future resume across that boundary,
// the exact silent-mis-decode class this ADR exists to kill.
//
// SchemaSnapshot carries no row data; it sits between the DDL boundary
// and the first post-DDL row event so the applier durably records the
// new schema-version anchor before any row decoded in the new shape is
// applied. It is NOT a no-op for per-change appliers (unlike
// [TxBegin] / [TxCommit]): the version write must reach a transaction.
type SchemaSnapshot struct {
	Position Position
	Schema   string
	Table    string
	// IR is the affected table's post-DDL IR schema, built from the
	// reader's in-stream position-anchored metadata at the boundary —
	// NEVER a fresh information_schema / catalog re-introspection
	// (ADR-0049 locked decision #2; the ADR rejects re-introspection
	// in Alternatives). Non-nil for a well-formed event.
	IR *Table
}

func (SchemaSnapshot) isChange()       {}
func (e SchemaSnapshot) Pos() Position { return e.Position }
func (e SchemaSnapshot) QualifiedName() string {
	return qualified(e.Schema, e.Table)
}

// SourceCommitTime returns the zero time: a schema-boundary event is not a
// row-bearing change and carries no transaction commit timestamp (the
// sync-lag metric derives from row events only). See
// [Change.SourceCommitTime].
func (SchemaSnapshot) SourceCommitTime() time.Time { return time.Time{} }
