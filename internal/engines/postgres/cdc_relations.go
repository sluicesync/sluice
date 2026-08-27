// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"fmt"
	"reflect"

	"github.com/jackc/pgx/v5/pgtype"

	"sluicesync.dev/sluice/internal/ir"
)

// relationCacheEntry is the IR-typed projection of a pgoutput
// RelationMessage. Built once per RelationMessage and consulted on
// every subsequent DML event for the same relation OID.
//
// The pgoutput protocol guarantees a RelationMessage precedes the
// first DML event for any relation in a stream, and a fresh
// RelationMessage is emitted whenever the relation's schema changes.
// That makes the relations cache its own invalidation channel — no
// separate DDL listener is needed, in contrast to MySQL CDC where
// schema changes arrive as opaque QueryEvents.
type relationCacheEntry struct {
	Schema string
	Name   string

	// ReplicaIdentity is the pg_class.relreplident byte:
	//   'd' default (PK columns only in old tuple)
	//   'n' nothing (no old tuple at all)
	//   'f' full (every column in old tuple)
	//   'i' using-index (named index columns in old tuple)
	// Drives Update/Delete Before-image semantics; the dispatcher
	// records this so a future v2 can warn the user about tables
	// without a usable identity.
	ReplicaIdentity uint8

	Columns []relationColumn

	// IdentityKeyCols is the ordered set of column names that identify
	// a row for the purpose of building an UPDATE/DELETE WHERE clause —
	// the "Before image must narrow to these" set (Bug 92).
	//
	// It is NOT simply the per-column wire key flag (relationColumn.
	// KeyColumn = pgoutput's RelationMessageColumn.Flags&1). Under
	// REPLICA IDENTITY FULL pgoutput flags EVERY column as a key column
	// (empirically confirmed: a 12-column FULL table arrives with all 12
	// flagged), so trusting the wire flag would narrow to "all columns"
	// — a no-op that leaves rich-typed columns (jsonb / timestamptz /
	// bytea / high-precision numeric) in the WHERE, where their
	// decoded→rebound text fails to `=`-match the stored target value
	// and the statement silently matches zero rows (the Bug 92 silent
	// UPDATE/DELETE-loss class).
	//
	// Resolution, by replica identity:
	//   - DEFAULT / USING INDEX: the wire key flags ARE the replica-
	//     identity index columns, so IdentityKeyCols = the flagged
	//     columns (no DB round-trip needed).
	//   - FULL: the wire flags are useless (all-set); IdentityKeyCols is
	//     resolved to the table's TRUE PRIMARY KEY via pg_index. A FULL
	//     table with no PK leaves this empty, and the Before-narrowing
	//     falls back to the full row (the only available identity).
	//   - NOTHING: no old tuple is ever emitted; the upstream emit paths
	//     reject before this is consulted.
	IdentityKeyCols []string
}

// relationColumn carries the resolved IR view of one column. The raw
// OID is kept alongside the IR type so unknown-type errors can name
// the OID (the lookup table omission, not the IR type) for users.
type relationColumn struct {
	Name      string
	OID       uint32
	TypeMod   int32
	Type      ir.Type
	KeyColumn bool // RelationMessageColumn.Flags & 1

	// StableID is pg_attribute.attnum for this column — the catalog's
	// per-relation column ordinal that is STABLE across a RENAME COLUMN
	// (a rename changes attname, never attnum). Resolved by
	// [CDCReader.resolveColumnStableIDs] on each RelationMessage (the
	// same relation-boundary round-trip that resolves identity-key
	// cols), then carried into [ir.Column.StableID] by [projectRelation]
	// so the pipeline's ADR-0091 F7b rename intercept can PROVE a
	// rename (same attnum + new name) versus a drop+add (different
	// attnum). 0 = unresolved (no live pool, or column not found —
	// either way the intercept treats 0 as "unproven" and refuses,
	// which is the safe direction). pgoutput's RelationMessage does NOT
	// carry attnum, hence the catalog lookup.
	StableID int
}

// detectIncompatibleRelationChange compares a previously-cached relation
// against a newly-received one for the same relation OID and returns a
// short human-readable description of an incompatible change, or "" if
// the change is either no-op (pgoutput re-sends RelationMessage on
// reconnect / first-touch with identical content) or compatible with
// the ADR-0058 ADD COLUMN forwarding path.
//
// Bugs 112 (RENAME) / 119 (DROP COLUMN) / 120 (DROP+CREATE same name)
// closure (v0.93.0). Pre-fix the applier's colTypeCache (keyed by
// "schema.table" with no invalidation) silently used stale shape: writes
// to a renamed table vanished from dst, DROP COLUMN drifted dst's column
// (populated as NULL on new INSERTs), DROP+CREATE silently dropped the
// new table's writes onto the old cached entry. With this detector, the
// reader surfaces the race as a loud-failure error that crashes the
// stream cleanly with the drained-model recovery hint.
//
// ADD COLUMN (new columns appended to the existing list, prior columns
// unchanged in name and OID) returns "" so the existing ADR-0058
// `--forward-schema-add-column` opt-in forwarding path continues to
// work. ALTER COLUMN TYPE (existing column's OID changed) is rejected
// — the forwarding path doesn't cover that shape either.
//
// The DROP+CREATE-same-name case (Bug 120) is detected separately at
// the call site by scanning the relations map for an existing entry
// with the same (Schema, Name) but a different OID, because pgoutput
// allocates a fresh OID for the new relation and this function only
// sees one OID at a time.
func detectIncompatibleRelationChange(prev, current *relationCacheEntry) string {
	change := classifyRelationChange(prev, current)
	return change.Description
}

// relationChangeKind enumerates the mid-stream same-OID relation shapes
// the CDC reader distinguishes. ADR-0091 F7a GAP #1 made the
// forward-vs-refuse decision per-shape (rather than refuse-everything-but-
// ADD), so the detector now classifies the shape and lets [checkSchemaRace]
// apply the policy:
//
//   - relationChangeNone / relationChangeAddColumn always pass.
//   - relationChangeDropColumn / relationChangeAlterColumnType pass when
//     --schema-changes=forward (the ADR-0091 default): they reach the
//     forward intercept as SchemaSnapshots, and the GAP #3 applier-cache
//     invalidation keeps decode correct. They refuse under
//     --schema-changes=refuse (the Bug 112/119/120 behavior, preserved).
//   - relationChangeRenameTable always refuses — a table rename (or schema
//     move) is genuinely ambiguous mid-stream.
//   - relationChangeRenameColumn passes to the intercept under forward (so
//     the intercept's ADR-0091 §3 attnum-unprovable data-loss refusal
//     fires with the specific message); refuses at the reader otherwise.
type relationChangeKind int

const (
	relationChangeNone relationChangeKind = iota
	relationChangeAddColumn
	relationChangeDropColumn
	relationChangeAlterColumnType
	relationChangeRenameColumn
	relationChangeRenameTable
)

// relationChange is the classified result of comparing a cached relation
// against a freshly-received one for the same OID. Description is the
// human-readable shape text used in the refuse-loudly error (empty for
// None / AddColumn, which never refuse).
type relationChange struct {
	Kind        relationChangeKind
	Description string
}

// classifyRelationChange compares a previously-cached relation against a
// newly-received one for the same OID and classifies the shape. A nil
// prev (first-touch) or an identical re-send (pgoutput reconnect) is
// relationChangeNone. ADD COLUMN (existing columns unchanged in order,
// name and OID; new columns appended) is relationChangeAddColumn. The
// remaining shapes — table rename, column drop, column rename, column
// type change — each get a distinct kind so the forward-vs-refuse policy
// can be applied per-shape (ADR-0091 F7a GAP #1).
func classifyRelationChange(prev, current *relationCacheEntry) relationChange {
	if prev == nil {
		return relationChange{Kind: relationChangeNone}
	}
	if prev.Schema != current.Schema || prev.Name != current.Name {
		return relationChange{
			Kind: relationChangeRenameTable,
			Description: fmt.Sprintf("RENAME %s.%s → %s.%s",
				prev.Schema, prev.Name, current.Schema, current.Name),
		}
	}
	// A shorter column list is a DROP COLUMN. (A middle-column drop also
	// shortens the list, so it classifies here as DROP rather than as the
	// ordinal-mismatch RENAME COLUMN below — the correct call, since the
	// net effect is a removed column.)
	if len(current.Columns) < len(prev.Columns) {
		return relationChange{
			Kind: relationChangeDropColumn,
			Description: fmt.Sprintf("DROP COLUMN (column count %d → %d)",
				len(prev.Columns), len(current.Columns)),
		}
	}
	for i, col := range prev.Columns {
		nc := current.Columns[i]
		if col.Name != nc.Name {
			return relationChange{
				Kind: relationChangeRenameColumn,
				Description: fmt.Sprintf("RENAME COLUMN %s → %s (ordinal %d)",
					col.Name, nc.Name, i),
			}
		}
		if col.OID != nc.OID {
			return relationChange{
				Kind: relationChangeAlterColumnType,
				Description: fmt.Sprintf("ALTER COLUMN TYPE %s (type OID %d → %d, ordinal %d)",
					col.Name, col.OID, nc.OID, i),
			}
		}
		// Same OID, different typmod IS an ALTER COLUMN TYPE (capture-
		// completeness G3, 2026-08-26): numeric(10,4)→numeric(10,1),
		// varchar(20)→varchar(10), timestamp(6)→timestamp(3), bit(8)→bit(4)
		// all keep the OID and change only the modifier — and the shrink
		// members REWRITE every stored value (numeric rounds, varchar
		// refuses-or-truncates on cast) with ZERO decoded messages for the
		// rewrite, because PG never logically decodes table-rewrite
		// contents. The post-rewrite RelationMessage's new typmod is the
		// ONLY wire artifact, so ignoring it classified the shape as None
		// and bypassed every schema-change door: silent value divergence
		// under refuse mode, and under forward mode the boundary still
		// reached the intercept via the SchemaSignature delta but the
		// reader-side refuse gate never fired. With this compare the shape
		// rides the standard AlterColumnType policy: refuse loudly under
		// --schema-changes=refuse, forward under the default (the
		// forwarded USING-less ALTER applies the same deterministic cast
		// the source's own USING-less rewrite did, so pre-rewrite target
		// rows CONVERGE). A same-type ALTER whose typmod is also unchanged
		// (`USING v*10`) stays undetectable — the wire is content-identical
		// — and is documented as such in the capture-completeness matrix
		// and ADR-0091. pgoutput reconnect re-sends carry identical
		// typmods, so this cannot false-fire (pinned).
		if col.TypeMod != nc.TypeMod {
			return relationChange{
				Kind: relationChangeAlterColumnType,
				Description: fmt.Sprintf("ALTER COLUMN TYPE %s (type OID %d unchanged, typmod %d → %d, ordinal %d)",
					col.Name, col.OID, col.TypeMod, nc.TypeMod, i),
			}
		}
	}
	// Existing columns identical; either no change or new columns
	// appended (ADD COLUMN). Both pass.
	if len(current.Columns) > len(prev.Columns) {
		return relationChange{Kind: relationChangeAddColumn}
	}
	return relationChange{Kind: relationChangeNone}
}

// passesUnderSchemaForward reports whether a classified relation change is
// allowed to reach the ADR-0091 forward intercept (i.e. the reader emits a
// SchemaSnapshot rather than refusing) when --schema-changes=forward.
//
// DROP COLUMN and ALTER COLUMN TYPE pass: the GAP #3 applier-cache
// invalidation (invalidateTargetCachesForBoundary) refreshes the target
// decode path on the SchemaSnapshot boundary, closing the Bug 119 silent-
// drift root cause the original gate guarded against. RENAME COLUMN also
// passes — but only so the intercept's ADR-0091 §3 data-loss refusal
// (refuseRenameAmbiguous) fires with its specific, attnum-unprovable
// message instead of the generic reader error; the rename is still
// refused, just one layer down with a better diagnostic. RENAME TABLE
// (and the DROP+CREATE-same-name case handled separately in
// checkSchemaRace) is never forwardable — it stays a loud reader refusal.
//
// An ALTER COLUMN TYPE whose projected IR type does not move is an
// exception applied by checkSchemaRace itself (the TYPMOD-PROJECTION-GATE,
// audit 2026-08-27 A2): the forward intercept can only ever see a change
// that moves the projected signature, so a projection-invisible delta
// refuses loudly there instead of passing here. See
// [unforwardableTypmodColumn].
func (k relationChangeKind) passesUnderSchemaForward() bool {
	switch k {
	case relationChangeDropColumn,
		relationChangeAlterColumnType,
		relationChangeRenameColumn:
		return true
	default:
		return false
	}
}

// checkSchemaRace surfaces incompatible-DDL situations as a loud
// stream-killing error. It runs on every RelationMessage arrival, just
// before the cache entry is replaced, and detects:
//
//   - Same-OID shape change (RENAME / DROP COLUMN / RENAME COLUMN /
//     ALTER COLUMN TYPE) by comparing the new entry against the
//     previously-cached entry for the same OID. See
//     [detectIncompatibleRelationChange].
//   - DROP+CREATE-same-name by scanning the relations map for any other
//     OID with the same (Schema, Name) — pgoutput allocates a fresh
//     OID for the new relation, so the old entry is orphaned in the
//     map but still detectable.
//
// Returns nil when no race is detected, when the change is a pure
// ADD COLUMN, or — when schemaForward is true (--schema-changes=forward,
// the ADR-0091 default) — when the change is a DROP COLUMN / ALTER COLUMN
// TYPE / RENAME COLUMN, which then reach the forward intercept as
// SchemaSnapshots (F7a GAP #1; the intercept forwards DROP/ALTER and
// refuses RENAME COLUMN with its specific data-loss message). RENAME TABLE
// and DROP+CREATE-same-name always refuse, regardless of schemaForward.
//
// schemaForward=false preserves the EXACT pre-ADR-0091 behavior: every
// shape but ADD COLUMN refuses loudly (Bug 112/119/120 closure).
//
// Bug 112 (RENAME silent drop) / Bug 119 (DROP COLUMN silent drift) /
// Bug 120 (DROP+CREATE silent drop) v0.93.0 closure; relaxed for the
// forward-mode shapes by ADR-0091 F7a GAP #1.
func checkSchemaRace(relations map[uint32]*relationCacheEntry, relationID uint32, current *relationCacheEntry, schemaForward bool) error {
	change := classifyRelationChange(relations[relationID], current)
	// A non-empty Description means a shape was detected. Under forward mode
	// the unambiguous / intercept-routable shapes pass through to the
	// SchemaSnapshot path; everything else (and everything under refuse
	// mode) is a loud reader refusal.
	forwarded := schemaForward && change.Kind.passesUnderSchemaForward()
	// TYPMOD-PROJECTION-GATE (audit 2026-08-27 A2): an ALTER COLUMN TYPE the
	// classifier caught but whose PROJECTED IR type is unchanged can never be
	// forwarded — [CDCReader.maybeSnapshotSchema] emits a boundary iff the
	// projected signature moved, so forward mode would emit no boundary, no
	// ALTER, no WARN, while the source's table rewrite (interval(6)→(3) rounds
	// every fractional second; numeric(10,4)[]→(10,1)[] rounds every element)
	// silently diverges pre-existing target rows at exit 0. Detected-but-
	// unforwardable is exactly the refuse-mode situation, so refuse loudly with
	// the same drained-model remedy instead of waving it through.
	if forwarded && change.Kind == relationChangeAlterColumnType {
		if col, unforwardable := unforwardableTypmodColumn(relations[relationID], current); unforwardable {
			return fmt.Errorf("postgres: cdc: schema change mid-stream on %s.%s (OID %d) is detected but cannot be forwarded: %s — column %q's projected IR type is unchanged by this delta (interval precision/field restrictions and array-element modifiers do not survive the wire projection), so --schema-changes=forward would emit no schema boundary and the source's table rewrite would silently diverge pre-existing target rows. %s",
				current.Schema, current.Name, relationID, change.Description, col, schemaRaceRecoveryHint)
		}
	}
	if change.Description != "" && !forwarded {
		return fmt.Errorf("postgres: cdc: incompatible schema change mid-stream on %s.%s (OID %d): %s. %s",
			current.Schema, current.Name, relationID, change.Description, schemaRaceRecoveryHint)
	}
	// DROP+CREATE-same-name (Bug 120): a different OID claims the same
	// (Schema, Name). pgoutput re-issues RelationMessage for the new
	// relation; the old OID's entry is now orphaned but still cached.
	// This is genuinely ambiguous (the new relation is a fresh table that
	// merely reuses the name) and is never forwardable, so it refuses
	// regardless of schemaForward.
	for otherOID, other := range relations {
		if otherOID == relationID {
			continue
		}
		if other != nil && other.Schema == current.Schema && other.Name == current.Name {
			return fmt.Errorf(
				"postgres: cdc: DROP+CREATE detected mid-stream on %s.%s (old OID %d, new OID %d). %s",
				current.Schema, current.Name, otherOID, relationID, schemaRaceRecoveryHint,
			)
		}
	}
	return nil
}

// unforwardableTypmodColumn scans an AlterColumnType-classified prev/current
// pair for a changed column whose PROJECTED IR type did not move — the
// TYPMOD-PROJECTION-GATE predicate (audit 2026-08-27 A2). It returns the
// first such column's name.
//
// The compare is per-COLUMN, not the whole-table [ir.SchemaSignatureOf]
// equality the audit sketched: a single `ALTER TABLE` can carry a
// signature-moving sub-ALTER (numeric scale) AND an unforwardable one
// (interval precision) in one statement / one RelationMessage, and the
// table-level compare would see the moved signature, forward the boundary,
// and let the interval rewrite diverge — the sketch's own sibling gap. A
// column is "changed" when its wire OID or typmod differs; its projection is
// compared through the same [containSRIDSentinel] lens [projectRelation]
// applies, so this predicate is exactly "would [CDCReader.maybeSnapshotSchema]
// see this column move".
//
// Deliberately the RAW projected type, never the [normalizeTypeForCDCComparison]
// lens: the temporal-collapse members (bare ≡ (0) ≡ (6)) DO move the raw
// projection (Precision/PrecisionUnspecified differ), so they emit a boundary
// and keep their documented downstream posture (the normalizer false-negative,
// cdc_normalize.go) — keying on the normalized form would false-refuse the
// value-identical bare→(6) ALTER.
//
// False-fire analysis (pinned by TestClassifyRelationChange_TypmodFamilies'
// identical-resend cells): this only runs when the classifier fired
// AlterColumnType, which requires an OID or typmod delta at some ordinal — a
// pgoutput reconnect/first-touch re-send is field-identical, classifies None,
// and never reaches it. Ordinals whose NAME differs are skipped (rename
// business — the intercept's ADR-0091 §3 refusal owns those). A nil projected
// type on a changed column compares equal and refuses — the safe direction
// (production entries always carry a resolved type; buildRelationCacheEntry
// errors on unresolvable OIDs).
func unforwardableTypmodColumn(prev, current *relationCacheEntry) (string, bool) {
	n := min(len(prev.Columns), len(current.Columns))
	for i := 0; i < n; i++ {
		pc, cc := prev.Columns[i], current.Columns[i]
		if pc.Name != cc.Name {
			continue
		}
		if pc.OID == cc.OID && pc.TypeMod == cc.TypeMod {
			continue
		}
		if reflect.DeepEqual(containSRIDSentinel(pc.Type), containSRIDSentinel(cc.Type)) {
			return pc.Name, true
		}
	}
	return "", false
}

// schemaRaceRecoveryHint is the operator-actionable recovery text the
// reader appends to every schema-race error. Centralised here so the
// wording stays consistent across RENAME / DROP COLUMN / ALTER TYPE /
// DROP+CREATE call sites and is straightforward for operators to grep
// for. The "drained model" workflow is the same one ADR-0058's existing
// non-ADD-COLUMN refusal directs operators to.
const schemaRaceRecoveryHint = "sluice does not support this DDL shape mid-stream. Drained-model recovery: " +
	"(1) `sluice sync stop --wait` on every shard, " +
	"(2) apply the schema change via your migration tool on source AND target, " +
	"(3) `sluice sync start` with the SAME --stream-id to continue from the last applied LSN " +
	"(there is no resume flag: a restart looks up the persisted position and skips the " +
	"snapshot + bulk-copy phase). " +
	"For ADD COLUMN only, opt-in to live forwarding via --forward-schema-add-column (ADR-0058)."

// projectRelation builds an [ir.Table] from a relationCacheEntry —
// the ADR-0049 Chunk B3 boundary projector. The entry is ALREADY
// IR-typed (buildRelationCacheEntry resolved every column's OID via
// oidToType when the RelationMessage arrived), so this is the
// cheapest of the three engine boundary paths: no re-introspection,
// no second type mapping (the locked decision #2 "build from
// in-stream position-anchored metadata, never re-introspection" is
// satisfied for free — pgoutput's RelationMessage IS that metadata).
//
// Nullability is not carried on a pgoutput RelationMessage column
// (the protocol only sends name/OID/typmod/key-flag), so projected
// columns are left Nullable=false. The schema-history decode contract
// (ir.SchemaSignature) compares only column name + IR type, so this
// does not affect resolve correctness — it is a faithful projection
// of exactly what the wire carries.
func projectRelation(rel *relationCacheEntry) *ir.Table {
	cols := make([]*ir.Column, len(rel.Columns))
	for i, c := range rel.Columns {
		// StableID carries pg_attribute.attnum (ADR-0091 F7b) so the
		// pipeline rename intercept can prove rename-vs-drop+add. It is
		// METADATA only — SchemaSignatureOf / diffAlteredColumn ignore it,
		// so it does not perturb the decode contract or alter-detection.
		cols[i] = &ir.Column{Name: c.Name, Type: containSRIDSentinel(c.Type), StableID: c.StableID}
	}
	tbl := &ir.Table{Schema: rel.Schema, Name: rel.Name, Columns: cols}
	// Bug 89: surface PK column names from the RelationMessage's
	// per-column KeyColumn flag. ADR-0058 backfill (and any future per-PK
	// path consuming a CDC-emitted SchemaSnapshot) needs the PK to drive
	// cursor-paginated iteration. KeyColumn=true on a pgoutput Relation
	// is set for replica-identity columns; with REPLICA IDENTITY DEFAULT
	// (the default) this is the PK column set, which is what
	// runBackfillForAddedColumn requires.
	var pkCols []ir.IndexColumn
	for _, c := range rel.Columns {
		if c.KeyColumn {
			pkCols = append(pkCols, ir.IndexColumn{Column: c.Name})
		}
	}
	if len(pkCols) > 0 {
		tbl.PrimaryKey = &ir.Index{Columns: pkCols}
	}
	return tbl
}

// oidToType maps a Postgres data-type OID (as carried in
// RelationMessageColumn.DataType) to the corresponding IR type.
// Unknown OIDs return an error rather than a fallback — silent
// type fallbacks produce data corruption that's hard to spot in
// review, while a loud error names the OID and stops the stream.
//
// Custom types (enums from CREATE TYPE, composite types, domains)
// have OIDs that aren't in pgtype's constant set; resolving those
// would require a one-time pg_type lookup. Punted to a follow-up
// chunk; for v1 they error out with the OID number so users have
// a concrete signal.
//
// typmod encodes per-instance metadata for parameterised types
// (VARCHAR length, NUMERIC precision/scale, TIMESTAMP precision).
// Postgres uses typmod = -1 to mean "no parameter set"; helpers
// below decode the conventional layouts.
func oidToType(oid uint32, typmod int32) (ir.Type, error) {
	switch oid {
	// ---- Boolean ----
	case pgtype.BoolOID:
		return ir.Boolean{}, nil

	// ---- Integer family ----
	case pgtype.Int2OID:
		return ir.Integer{Width: 16}, nil
	case pgtype.Int4OID:
		return ir.Integer{Width: 32}, nil
	case pgtype.Int8OID:
		return ir.Integer{Width: 64}, nil

	// ---- Decimal / float ----
	case pgtype.Float4OID:
		return ir.Float{Precision: ir.FloatSingle}, nil
	case pgtype.Float8OID:
		return ir.Float{Precision: ir.FloatDouble}, nil
	case pgtype.NumericOID:
		p, s := numericTypmod(typmod)
		return ir.Decimal{Precision: p, Scale: s}, nil

	// ---- Character ----
	case pgtype.VarcharOID:
		l := charTypmod(typmod)
		if l == 0 {
			// Unbounded VARCHAR is exotic but possible; the IR has
			// no "varchar with no length" so we land on Text/long.
			return ir.Text{Size: ir.TextLong}, nil
		}
		return ir.Varchar{Length: l}, nil
	case pgtype.BPCharOID:
		return ir.Char{Length: charTypmod(typmod)}, nil
	case pgtype.QCharOID:
		// PG's internal single-byte "char" type (distinct from CHARACTER(n)).
		// Mirrors the schema reader's `_char` → "character" mapping
		// (builtinArrayElement) so a "char"[] array element — and a scalar
		// "char" column — resolves instead of falling through to the
		// unsupported-OID refusal. Length 1: the type holds one byte.
		return ir.Char{Length: 1}, nil
	case pgtype.TextOID:
		return ir.Text{Size: ir.TextLong}, nil

	// ---- Binary ----
	case pgtype.ByteaOID:
		return ir.Blob{Size: ir.BlobLong}, nil

	// ---- Bit strings ----
	// Registry-parity with the schema reader's "bit"/"bit varying" arm
	// (types.go) — the Bug 97 dual-registry-drift shape, refound by the
	// adversarial corpus (2026-08-22): a bit(n) column migrated and
	// cold-started fine, then the FIRST DML crashed the sync stream
	// with "unsupported column type OID 1560". Bit typmods carry the
	// raw declared length with NO +4 offset (ground-truthed: bit(8)
	// arrives with typmod 8; the schema reader documents the same for
	// atttypmod). An unmodified declaration (-1) collapses to bit(1) —
	// PG's default — mirroring the schema reader. The value side needs
	// nothing new: pgoutput carries bit/varbit as the canonical
	// '0'/'1' text, which IS the ir.Bit contract form (decodeValue's
	// ir.Bit arm).
	case pgtype.BitOID, pgtype.VarbitOID:
		n := int(typmod)
		if n < 1 {
			n = 1
		}
		return ir.Bit{Length: n, Varying: oid == pgtype.VarbitOID}, nil

	// ---- Temporal ----
	case pgtype.DateOID:
		return ir.Date{}, nil
	case pgtype.IntervalOID:
		// PG duration type; reads as ir.Interval (a span), distinct from
		// ir.Time (a time-of-day). Keeps PG → PG CDC of an interval
		// column working symmetrically with the schema-read path.
		return ir.Interval{}, nil
	case pgtype.TimeOID, pgtype.TimetzOID:
		p, unspec := temporalTypmod(typmod)
		return ir.Time{Precision: p, PrecisionUnspecified: unspec}, nil
	case pgtype.TimestampOID:
		p, unspec := temporalTypmod(typmod)
		return ir.DateTime{Precision: p, PrecisionUnspecified: unspec}, nil
	case pgtype.TimestamptzOID:
		p, unspec := temporalTypmod(typmod)
		return ir.Timestamp{Precision: p, WithTimeZone: true, PrecisionUnspecified: unspec}, nil

	// ---- Structured ----
	case pgtype.JSONOID:
		return ir.JSON{Binary: false}, nil
	case pgtype.JSONBOID:
		return ir.JSON{Binary: true}, nil

	// ---- Identity / network ----
	case pgtype.UUIDOID:
		return ir.UUID{}, nil
	// Family-parity with the schema reader's text-keyed arm (roadmap
	// item 133): PG constrains neither type to an address family, and the
	// answer is DECLARED so an unspecified family stays a "nobody decided"
	// signal rather than PG's normal state.
	case pgtype.InetOID:
		return ir.Inet{Family: ir.InetFamilyAny}, nil
	case pgtype.CIDROID:
		return ir.Cidr{Family: ir.InetFamilyAny}, nil
	// Split by width, in family-parity with the schema reader's text-keyed
	// arm (Bug 97's dual-registry-drift lesson): a CDC relation that read a
	// `macaddr8` column as a bare Macaddr would hand the applier a 6-byte
	// target type for an 8-byte value. TestOIDToType pins both OIDs.
	case pgtype.MacaddrOID:
		return ir.Macaddr{Width: ir.MacaddrEUI48}, nil
	case pgtype.Macaddr8OID:
		return ir.Macaddr{Width: ir.MacaddrEUI64}, nil
	}
	// Bug 97 (v0.92.0): verbatim-carry families landed in the schema
	// reader via [coreVerbatimEligibleTypes] (ADR-0051 Stage 1, then
	// ADR-0070 Stage 2) — but the schema reader uses a text-keyed map
	// while the CDC reader sees pgoutput OIDs. The two registries
	// drifted: a same-engine PG→PG migrate worked because schema
	// translation hit the eligible map; the first DML on the same
	// schema crashed the sync stream because oidToType fell through
	// to the unsupported-type refusal. The OID-based lookup below
	// reconciles the two for every verbatim family. Cross-engine
	// safety is preserved by the orchestrator's `ir.VerbatimType`
	// refusal in migcore/cross_engine_supportable.go.
	// ---- Array families (Bug 144) ----
	// pgoutput carries an array column under its array OID (e.g. _int4 = 1007).
	// The element type is resolved by recursing oidToType on the element OID, so
	// an array element decodes BYTE-IDENTICALLY to the same scalar column — the
	// shared value_decode.decodeArray path (reached via the ir.Array arm of
	// decodeValue) consumes the resulting ir.Array. The element-OID table MUST
	// stay in family-parity with the schema reader's text-keyed
	// builtinArrayElement (the Bug 97/118 dual-registry-drift lesson — a family
	// supported at schema-read but missing here crashes the stream on the first
	// array DML); TestOIDToType_ArrayParity pins that.
	if elemOID, ok := pgArrayElementOID[oid]; ok {
		// Element resolved with typmod -1: pgoutput's RelationMessage carries
		// the array column's own typmod, not the element's, so element
		// length/precision (varchar(n)[], numeric(p,s)[], timestamp(p)[]) is
		// intentionally defaulted here. This does NOT affect VALUE fidelity —
		// the writer binds the element's text/numeric verbatim and the target
		// table already carries the precise type from cold-start; only the
		// in-memory ir.Array.Element metadata is coarser than the cold-start
		// path's.
		elem, err := oidToType(elemOID, -1)
		if err != nil {
			return nil, fmt.Errorf("postgres: cdc: array OID %d element OID %d: %w", oid, elemOID, err)
		}
		return ir.Array{Element: elem}, nil
	}
	if name, ok := coreVerbatimCDCOIDs[oid]; ok {
		return ir.VerbatimType{Definition: name}, nil
	}
	return nil, fmt.Errorf("postgres: cdc: unsupported column type OID %d (typmod %d)", oid, typmod)
}

// domainBase records a DOMAIN type's immediate base type OID and the type
// modifier the domain applies to that base — pg_type.typbasetype and
// pg_type.typtypmod. It is what [resolveDomainBase] walks to unwrap a domain to
// its ultimate concrete base (Bug 254).
//
// The distinction that makes the catalog lookup mandatory: `CREATE DOMAIN d AS
// varchar(10)` stores the varchar(10) typmod HERE (baseTypmod), while the
// pgoutput RelationMessage reports the domain COLUMN's own typmod as -1. So the
// base length/precision can only be recovered from pg_type, never off the wire.
type domainBase struct {
	baseOID    uint32
	baseTypmod int32
}

// resolveDomainBase unwraps a domain OID to its ultimate NON-domain base type,
// returning the base OID and the effective type modifier to resolve it with. ok
// is false when oid is not a domain (not present in domainBases).
//
// domain-over-domain is flattened by walking the chain. PostgreSQL forbids
// attaching a type modifier when a domain's base is itself a domain, so only the
// innermost domain (the one sitting directly on the concrete base) carries a
// non -1 typtypmod; the walk keeps the last non -1 modifier it sees, which is
// that innermost one. The iteration is bounded by the map size as a defence
// against a pathological catalog cycle (impossible in a valid pg_type).
func resolveDomainBase(oid uint32, domainBases map[uint32]domainBase) (baseOID uint32, baseTypmod int32, ok bool) {
	if _, isDomain := domainBases[oid]; !isDomain {
		return 0, 0, false
	}
	baseTypmod = int32(-1)
	cur := oid
	for i := 0; i <= len(domainBases); i++ {
		d, isDomain := domainBases[cur]
		if !isDomain {
			return cur, baseTypmod, true
		}
		if d.baseTypmod != -1 {
			baseTypmod = d.baseTypmod
		}
		cur = d.baseOID
	}
	// Cycle guard tripped (never in a valid catalog): stop at the last base.
	return cur, baseTypmod, true
}

// resolveWireColumnType maps a pgoutput column's wire OID + typmod to an IR
// type, applying the same dynamic-OID fallbacks buildRelationCacheEntry needs:
// PostGIS geometry, user-defined enums, and — Bug 254 — DOMAINs. The static
// [oidToType] table is tried first; only when it declines (a dynamic OID) do the
// fallbacks run.
//
// A DOMAIN is unwrapped to its ultimate base type ([resolveDomainBase]) and THAT
// is resolved recursively, so domain-over-varchar(10) decodes as varchar(10),
// domain-over-enum resolves through the enum arm, domain-over-int[] through the
// array arm, and so on. The result is deliberately NOT wrapped in ir.Domain: the
// applier resolves TARGET column types from the target's information_schema,
// which unwraps a domain to its base, so the CDC-decoded value must be the base
// type to match — and the wire bytes are already in the base type's
// representation. Producing the base type also keeps this reader in lockstep
// with the schema reader, which likewise resolves a domain column's BASE type
// off information_schema (Bug 113 wraps ir.Domain only around the SCHEMA the
// writer re-creates, never the value path). This is the wire-OID sibling of the
// Bug 233 ir.Domain-transparency class.
//
// The recursion terminates because resolveDomainBase always returns a
// non-domain base OID, so the recursive call's domain lookup misses.
func resolveWireColumnType(oid uint32, typmod int32, geomOID uint32, enumOIDs map[uint32]bool, domainBases map[uint32]domainBase) (ir.Type, error) {
	t, err := oidToType(oid, typmod)
	if err == nil {
		return t, nil
	}
	switch {
	case geomOID != 0 && oid == geomOID:
		// SRID starts UNKNOWN, not 0. pgoutput carries no SRID, and 0 is a real
		// declared value (an unconstrained column), so a bare ir.Geometry{}
		// would assert "this column declares 0" about something never read —
		// which refuses every row of a geometry(Point,4326) column.
		// [CDCReader.resolveGeometryColumnSRIDs] fills in the catalog's answer
		// right after the relation cache entry is built.
		return ir.Geometry{SRID: ir.GeometrySRIDUnknown}, nil
	case enumOIDs[oid]:
		return ir.Enum{}, nil
	}
	if baseOID, baseTypmod, isDomain := resolveDomainBase(oid, domainBases); isDomain {
		return resolveWireColumnType(baseOID, baseTypmod, geomOID, enumOIDs, domainBases)
	}
	// Not a builtin, not geometry, not an enum, not a domain — the loud
	// unknown-OID refusal (the original oidToType error, which names the OID).
	return nil, err
}

// macaddr8ArrayOID is PG's `_macaddr8` array type OID. pgx's pgtype exposes
// Macaddr8OID (774, the scalar) but not the array constant; 775 is the stable
// pg_catalog OID for `_macaddr8` specifically. (Array OIDs are NOT generally
// element+1 — e.g. macaddr is 829 but _macaddr is 1040 — so this literal is
// the verified catalog value for this one type, not a derived offset.)
const macaddr8ArrayOID = 775

// pgArrayElementOID maps a Postgres built-in array type OID to its element type
// OID, for CDC array support (Bug 144). oidToType recurses on the element OID so
// each array element decodes identically to the same scalar column. This is the
// OID-keyed CDC mirror of the schema reader's text-keyed builtinArrayElement;
// the two MUST cover the same element families (TestOIDToType_ArrayParity is the
// drift guard — see the Bug 97/118 write-up above on coreVerbatimCDCOIDs).
var pgArrayElementOID = map[uint32]uint32{
	pgtype.BoolArrayOID:        pgtype.BoolOID,
	pgtype.Int2ArrayOID:        pgtype.Int2OID,
	pgtype.Int4ArrayOID:        pgtype.Int4OID,
	pgtype.Int8ArrayOID:        pgtype.Int8OID,
	pgtype.Float4ArrayOID:      pgtype.Float4OID,
	pgtype.Float8ArrayOID:      pgtype.Float8OID,
	pgtype.NumericArrayOID:     pgtype.NumericOID,
	pgtype.TextArrayOID:        pgtype.TextOID,
	pgtype.VarcharArrayOID:     pgtype.VarcharOID,
	pgtype.BPCharArrayOID:      pgtype.BPCharOID,
	pgtype.QCharArrayOID:       pgtype.QCharOID,
	pgtype.ByteaArrayOID:       pgtype.ByteaOID,
	pgtype.DateArrayOID:        pgtype.DateOID,
	pgtype.TimeArrayOID:        pgtype.TimeOID,
	pgtype.TimetzArrayOID:      pgtype.TimetzOID,
	pgtype.TimestampArrayOID:   pgtype.TimestampOID,
	pgtype.TimestamptzArrayOID: pgtype.TimestamptzOID,
	pgtype.JSONArrayOID:        pgtype.JSONOID,
	pgtype.JSONBArrayOID:       pgtype.JSONBOID,
	pgtype.UUIDArrayOID:        pgtype.UUIDOID,
	pgtype.InetArrayOID:        pgtype.InetOID,
	pgtype.CIDRArrayOID:        pgtype.CIDROID,
	pgtype.MacaddrArrayOID:     pgtype.MacaddrOID,
	macaddr8ArrayOID:           pgtype.Macaddr8OID,
}

// coreVerbatimCDCOIDs is the CDC-side mirror of the schema reader's
// `coreVerbatimEligibleTypes` allowlist (defined in types.go). The two
// registries MUST stay in sync — a type that's eligible at schema-read
// but missing here crashes the sync stream on the first DML; a type
// that's here but not eligible at schema-read would silently translate
// CDC events for a column type the migration refused.
//
// Manual synchronization is the v0.92.0 hotfix shape; a unified
// registry that both files consume is the structural follow-up
// (deferred per the bug-finding sweep's recommendation).
//
// Definition string is the pg_catalog `typname` (matches what
// `format_type` would emit for the same OID at typmod=-1). For Stage
// 2 types this is sufficient since none of the five carry meaningful
// typmod data (xml/money/pg_lsn/txid_snapshot/pg_snapshot are all
// fixed-shape). For Stage 1 (tsvector/tsquery/range/multirange) the
// same holds — none take parameters.
var coreVerbatimCDCOIDs = map[uint32]string{
	// Stage 1 FTS family (ADR-0051 — tsvector/tsquery; pgtype has
	// TSVectorOID but not TsqueryOID, hence the literal for the
	// latter).
	pgtype.TSVectorOID: "tsvector",
	3615:               "tsquery",

	// Stage 1 range family (ADR-0051).
	pgtype.Int4rangeOID: "int4range",
	pgtype.Int8rangeOID: "int8range",
	pgtype.NumrangeOID:  "numrange",
	pgtype.TsrangeOID:   "tsrange",
	pgtype.TstzrangeOID: "tstzrange",
	pgtype.DaterangeOID: "daterange",

	// Stage 1 multirange family (PG 14+, ADR-0051).
	pgtype.Int4multirangeOID: "int4multirange",
	pgtype.Int8multirangeOID: "int8multirange",
	pgtype.NummultirangeOID:  "nummultirange",
	pgtype.TsmultirangeOID:   "tsmultirange",
	pgtype.TstzmultirangeOID: "tstzmultirange",
	pgtype.DatemultirangeOID: "datemultirange",

	// Stage 2 (ADR-0070). pgtype exposes XMLOID; the others are
	// hardcoded literals because pgtype doesn't expose a constant
	// for them. OIDs are stable per PG catalog conventions.
	pgtype.XMLOID: "xml",
	790:           "money",
	3220:          "pg_lsn",
	2970:          "txid_snapshot",
	5038:          "pg_snapshot",
}

// charTypmod extracts the declared length N from a typmod value
// produced by character types (VARCHAR(N), CHAR(N)). Postgres stores
// these as N+4 with -1 meaning "no length specified".
func charTypmod(typmod int32) int {
	if typmod < 4 {
		return 0
	}
	return int(typmod - 4)
}

// numericTypmod decodes the (precision, scale) pair from a NUMERIC
// typmod value. Postgres encodes (P, S) as ((P << 16) | (S & 0x7FF)) + 4
// with -1 meaning "no precision specified" (max precision NUMERIC).
// Scale is an 11-bit SIGNED field (PG 15 allows negative scale,
// numeric(5,-2), which rounds to the left of the decimal point); the
// XOR/subtract pair below is PG's own NUMERIC_TYPMOD_SCALE sign
// extension (numeric.c) — masking with 0xFFFF instead mis-read a
// negative scale as a huge positive one.
func numericTypmod(typmod int32) (precision, scale int) {
	if typmod < 4 {
		return 0, 0
	}
	t := typmod - 4
	return int((t >> 16) & 0xFFFF), int(((t & 0x7FF) ^ 1024) - 1024)
}

// temporalTypmod returns the fractional-second precision N from a
// TIMESTAMP(N) / TIME(N) typmod. Postgres stores precision directly
// (no +4 offset for these types); -1 is the bare declared form with
// no precision (unspecified=true — the [ir.Timestamp]/[ir.Time]/
// [ir.DateTime] PrecisionUnspecified state, TRIAGE #3), which behaves
// as the engine default (6) but is catalog-distinct from an explicit
// (6). Keeps the CDC projection in lockstep with the schema reader's
// [temporalPrecisionOf].
func temporalTypmod(typmod int32) (precision int, unspecified bool) {
	if typmod < 0 {
		return 0, true
	}
	return int(typmod), false
}

// containSRIDSentinel maps [ir.GeometrySRIDUnknown] back to 0 on its way OUT of
// the relation cache.
//
// The sentinel exists for exactly one consumer — the value decoder, which must
// tell "declares 0" from "never read" before refusing a row. Everything else
// downstream of [projectRelation] treats ir.Geometry.SRID as a number to put in
// DDL or to compare: schema-forward would emit `geometry(Point,-1)`, which is
// not valid PostGIS. 0 is what this lane projected before the sentinel existed,
// so restoring it here keeps every other consumer bit-identical and confines
// the new value to the cache it was invented for.
func containSRIDSentinel(t ir.Type) ir.Type {
	g, ok := t.(ir.Geometry)
	if !ok || g.SRID != ir.GeometrySRIDUnknown {
		return t
	}
	g.SRID = 0
	return g
}
