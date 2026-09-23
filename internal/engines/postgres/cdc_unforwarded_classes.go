// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// The unforwarded-class door (GC-2, gap census 2026-09-22).
//
// # The gap
//
// pgoutput's RelationMessage carries column names, type OIDs, typmods and
// key flags — nothing else. A source-side ADD/DROP CONSTRAINT (PRIMARY
// KEY, UNIQUE, FOREIGN KEY, EXCLUDE, CHECK), ENABLE/FORCE ROW LEVEL
// SECURITY, CREATE/ALTER/DROP POLICY, SET/DROP NOT NULL, SET/DROP DEFAULT
// or an identity/generated change never reaches the stream as data, and
// the boundary classifier (pipeline.ClassifyShape over the projection
// [projectRelation] builds) has no field to see it in. So each was
// ignored at exit 0, and the sharpest shape was a single statement:
// `ALTER TABLE t ADD COLUMN x int, ADD CONSTRAINT fk FOREIGN KEY (x) …`
// forwarded the column and dropped the foreign key — a target weaker than
// the source behind a successful forward. For a POLICY that is a tenant
// isolation boundary.
//
// # The door
//
// Every one of those DDLs invalidates the relation's relcache entry, and
// pgoutput re-sends the RelationMessage before the next change it decodes
// for that relation. So the RelationMessage is where the change becomes
// visible even though the message itself does not describe it. At
// StreamChanges the reader fingerprints every user table's untracked
// classes from the live catalog ([readRelationFacts] with relid 0) — the
// BASELINE. At each RelationMessage for an in-scope relation it
// fingerprints that one relation again and diffs ([diffRelationFacts]).
// A delta ends the stream with the grep-stable marker
// UNFORWARDED-SCHEMA-CHANGE, as a TERMINAL error: a retry cannot succeed
// (the next reader is handed this baseline and refuses again; GC-32), and
// without the carry it would re-baseline and accept the change silently.
//
// # What the diff exempts, and why each is safe
//
// Changes that are the consequence of a column change the pipeline
// already handles (ADR-0091 forwarding, or its own loud refusals):
//
//   - a column added this boundary: its NOT NULL / DEFAULT / identity ride
//     the ADD COLUMN forward, so new attnums are not compared. A
//     CONSTRAINT added alongside it is NOT exempt — that is the sharp
//     shape above.
//   - a constraint that disappears because a column it keys on is gone:
//     the target's own DROP COLUMN removes it too.
//   - a PRIMARY KEY, UNIQUE or EXCLUDE whose definition text changed
//     because a column it keys on was renamed or re-typed: the rename/type
//     change itself is classified (and forwarded or refused) by the
//     pipeline. CHECKs and POLICIES need no such exemption for a rename:
//     they compare on their stored node tree (positions stripped), which
//     names columns by attnum, so a rename leaves them unchanged and a
//     real edit in the same window is still seen. A CHECK is still exempt
//     on a retype of a column it reads (PG re-renders `(0)::numeric`).
//   - a generation expression re-rendered by a type change. A plain
//     DEFAULT is NOT exempt on a retype: PG does not re-render it
//     (measured: int→bigint keeps `0`), and the forwarded ALTER TYPE
//     carries no DEFAULT.
//   - an attnum of 0 in conkey is an expression element, never a lost
//     column.
//
// Constraint VALIDITY is not compared: ADD … NOT VALID then VALIDATE is
// the standard zero-downtime pattern, and a NOT VALID target constraint
// enforces new rows exactly as a validated one does. Every rule above is
// grounded on a real server by TestRelationFacts_DiffPremisesOnARealServer.
//
// FOREIGN KEYs compare on their structured catalog fields (referenced
// relation OID and attnums, actions, match, deferrability, validity), not
// their text, so a rename of the REFERENCED table or column — which
// rewrites pg_get_constraintdef on this side without any change here —
// is not a false refusal.
//
// # Scope, stated so it cannot be read as broader
//
//   - The baseline is taken at StreamChanges, so a change made while the
//     stream was stopped, or during a cold start's bulk copy (between the
//     snapshot and StreamChanges), is IN the baseline and never refused.
//     Precisely: anything made after the last ACKNOWLEDGED position and
//     before StreamChanges — a DDL a lagging stream had not reached yet
//     when it stopped counts too, because the baseline is the current
//     catalog while replay starts from the persisted position. Closing
//     this needs a source↔target comparison, not a source↔source one
//     (filed as the periodic full-diff follow-up to GC-2).
//   - Detection needs a RelationMessage, which needs a decoded change on
//     that relation: a change on a table that is never written again is
//     not seen until it is.
//   - The catalog read is current, not position-anchored: a change made
//     after the one being decoded can be reported early (still a real
//     change), and a change reverted before the read is missed (net no
//     change).
//   - Not compared at all: column COLLATION (`ALTER COLUMN … TYPE text
//     COLLATE "C"`), constraint and index storage (tablespace, fillfactor —
//     these DO change pg_get_constraintdef, so they refuse loudly rather
//     than slip), and policies/RLS on a PARTITIONED root (only the root
//     carries them while leaves get the messages; moot while Bug 100
//     refuses partitioned sources). A constraint RENAME refuses as
//     DROP+ADD.
//   - `backup incremental` opens a fresh reader every run, so a change
//     between two incrementals is always in the next one's baseline.
//   - Plain (non-constraint) indexes are deliberately NOT compared: index
//     DDL is the documented not-forwarded tradeoff for this lane
//     (docs/production-readiness.md), it never alters what a row may
//     contain, and refusing on every source-side CREATE INDEX would end
//     streams on routine tuning.

// unforwardedChangeMarker is the grep-stable token every refusal from this
// door carries.
const unforwardedChangeMarker = "UNFORWARDED-SCHEMA-CHANGE"

// pgRelationFacts is one relation's fingerprint of the classes pgoutput
// does not carry.
type pgRelationFacts struct {
	schema, name string
	rlsEnabled   bool
	rlsForced    bool
	constraints  map[string]pgConstraintFact
	policies     map[string]pgPolicyFact
	columns      map[int16]pgColumnFact
}

// pgConstraintFact is one pg_constraint row. compare is what the diff
// equates (the definition text, or the structured FK tuple); display is
// pg_get_constraintdef for the refusal text.
type pgConstraintFact struct {
	kind    string
	compare string
	display string
	attnums []int16
}

// pgPolicyFact is one pg_policy row: compare is what the diff equates,
// display is the readable form for the refusal text.
type pgPolicyFact struct {
	compare string
	display string
}

// pgColumnFact is one live pg_attribute row.
type pgColumnFact struct {
	name      string
	typ       string
	notNull   bool
	identity  string
	generated string
	def       string
}

// readRelationFacts fingerprints relid (or, with relid 0, every ordinary
// and partitioned table outside the system schemas) from the live
// catalog. Four queries regardless of how many relations are read.
func readRelationFacts(ctx context.Context, db *sql.DB, relid uint32) (map[uint32]*pgRelationFacts, error) {
	const relFilter = `
		c.relkind IN ('r', 'p')
		AND n.nspname NOT IN ('pg_catalog', 'information_schema')
		AND n.nspname NOT LIKE 'pg\_toast%'
		AND n.nspname NOT LIKE 'pg\_temp%'
		AND ($1::oid = 0 OR c.oid = $1::oid)`

	out := map[uint32]*pgRelationFacts{}
	err := eachCatalogRow(ctx, db, "relations", `
		SELECT c.oid, n.nspname, c.relname, c.relrowsecurity, c.relforcerowsecurity
		FROM   pg_class c
		JOIN   pg_namespace n ON n.oid = c.relnamespace
		WHERE `+relFilter, relid, func(rows *sql.Rows) error {
		var oid uint32
		f := &pgRelationFacts{
			constraints: map[string]pgConstraintFact{},
			policies:    map[string]pgPolicyFact{},
			columns:     map[int16]pgColumnFact{},
		}
		if err := rows.Scan(&oid, &f.schema, &f.name, &f.rlsEnabled, &f.rlsForced); err != nil {
			return err
		}
		out[oid] = f
		return nil
	})
	if err != nil {
		return nil, err
	}

	// contype 'n' (PG 18's catalogued NOT NULL) is excluded: nullability is
	// compared per column below, where a column added this boundary can be
	// told apart from a SET NOT NULL on an existing one.
	err = eachCatalogRow(ctx, db, "constraints", `
		SELECT c.oid, con.conname, con.contype::text,
		       pg_catalog.pg_get_constraintdef(con.oid),
		       COALESCE(pg_catalog.array_to_string(con.conkey, ','), ''),
		       con.confrelid::oid,
		       COALESCE(pg_catalog.array_to_string(con.confkey, ','), ''),
		       con.confupdtype::text, con.confdeltype::text, con.confmatchtype::text,
		       con.condeferrable, con.condeferred,
		       COALESCE(pg_catalog.regexp_replace(con.conbin::text, ':location -?[0-9]+', '', 'g'), '')
		FROM   pg_constraint con
		JOIN   pg_class c ON c.oid = con.conrelid
		JOIN   pg_namespace n ON n.oid = c.relnamespace
		WHERE  con.contype IN ('p', 'u', 'f', 'x', 'c')
		  AND `+relFilter, relid, func(rows *sql.Rows) error {
		var (
			oid, confrelid                   uint32
			name, kind, def, conkey, confkey string
			updType, delType, matchType      string
			deferrable, deferred             bool
			nodeTree                         string
		)
		if err := rows.Scan(&oid, &name, &kind, &def, &conkey, &confrelid, &confkey,
			&updType, &delType, &matchType, &deferrable, &deferred, &nodeTree); err != nil {
			return err
		}
		f := out[oid]
		if f == nil {
			return nil
		}
		compare := def
		switch kind {
		case "f":
			// Structured, so a rename of the referenced table or column (which
			// rewrites the definition text here) is not a change. Validity is
			// deliberately NOT compared: ADD … NOT VALID then VALIDATE is the
			// standard zero-downtime pattern, and a NOT VALID target constraint
			// enforces every new row exactly as a validated one does.
			compare = fmt.Sprintf("fk conkey=%s confrelid=%d confkey=%s upd=%s del=%s match=%s deferrable=%t deferred=%t",
				conkey, confrelid, confkey, updType, delType, matchType, deferrable, deferred)
		case "c":
			// The stored node tree with parse positions stripped: it names
			// columns by attnum, so a column rename leaves it unchanged (measured
			// on PG 16) and a real edit made in the same window as the rename is
			// still seen. Validity is not part of it, for the reason above.
			compare = "check " + nodeTree
		}
		f.constraints[name] = pgConstraintFact{kind: kind, compare: compare, display: def, attnums: parseAttnums(conkey)}
		return nil
	})
	if err != nil {
		return nil, err
	}

	err = eachCatalogRow(ctx, db, "policies", `
		SELECT c.oid, p.polname, p.polcmd::text, p.polpermissive,
		       COALESCE((SELECT pg_catalog.string_agg(CASE WHEN r = 0 THEN 'public' ELSE pg_catalog.pg_get_userbyid(r)::text END, ',' ORDER BY 1)
		                 FROM pg_catalog.unnest(p.polroles) AS r), ''),
		       COALESCE(pg_catalog.pg_get_expr(p.polqual, p.polrelid), ''),
		       COALESCE(pg_catalog.pg_get_expr(p.polwithcheck, p.polrelid), ''),
		       COALESCE(pg_catalog.regexp_replace(p.polqual::text, ':location -?[0-9]+', '', 'g'), ''),
		       COALESCE(pg_catalog.regexp_replace(p.polwithcheck::text, ':location -?[0-9]+', '', 'g'), '')
		FROM   pg_policy p
		JOIN   pg_class c ON c.oid = p.polrelid
		JOIN   pg_namespace n ON n.oid = c.relnamespace
		WHERE `+relFilter, relid, func(rows *sql.Rows) error {
		var (
			oid              uint32
			name, cmd, roles string
			using, check     string
			usingTree        string
			checkTree        string
			permissive       bool
		)
		if err := rows.Scan(&oid, &name, &cmd, &permissive, &roles, &using, &check, &usingTree, &checkTree); err != nil {
			return err
		}
		if f := out[oid]; f != nil {
			// Compared on the stored node trees (attnum-based, positions
			// stripped), so a column rename is not a policy change and a policy
			// edited in the same window as a rename is still one.
			f.policies[name] = pgPolicyFact{
				compare: fmt.Sprintf("cmd=%s permissive=%t roles=[%s] using=%s with_check=%s", cmd, permissive, roles, usingTree, checkTree),
				display: fmt.Sprintf("cmd=%s permissive=%t roles=[%s] using=(%s) with_check=(%s)", cmd, permissive, roles, using, check),
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	err = eachCatalogRow(ctx, db, "columns", `
		SELECT c.oid, a.attnum, a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod),
		       a.attnotnull, a.attidentity::text, a.attgenerated::text,
		       COALESCE(pg_catalog.pg_get_expr(d.adbin, d.adrelid), '')
		FROM   pg_attribute a
		JOIN   pg_class c ON c.oid = a.attrelid
		JOIN   pg_namespace n ON n.oid = c.relnamespace
		LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
		WHERE  a.attnum > 0 AND NOT a.attisdropped
		  AND `+relFilter, relid, func(rows *sql.Rows) error {
		var (
			oid    uint32
			attnum int16
			col    pgColumnFact
		)
		if err := rows.Scan(&oid, &attnum, &col.name, &col.typ, &col.notNull, &col.identity, &col.generated, &col.def); err != nil {
			return err
		}
		if f := out[oid]; f != nil {
			f.columns[attnum] = col
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// eachCatalogRow runs one of readRelationFacts' catalog queries with relid
// as $1 and hands each row to scan.
func eachCatalogRow(ctx context.Context, db *sql.DB, what, query string, relid uint32, scan func(*sql.Rows) error) error {
	rows, err := db.QueryContext(ctx, query, relid)
	if err != nil {
		return fmt.Errorf("read %s: %w", what, err)
	}
	defer rows.Close()
	for rows.Next() {
		if err := scan(rows); err != nil {
			return fmt.Errorf("read %s: %w", what, err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read %s: %w", what, err)
	}
	return nil
}

// parseAttnums decodes array_to_string(int2[], ',').
func parseAttnums(s string) []int16 {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]int16, 0, len(parts))
	for _, p := range parts {
		n, err := strconv.ParseInt(strings.TrimSpace(p), 10, 16)
		if err != nil {
			continue
		}
		out = append(out, int16(n))
	}
	return out
}

// diffRelationFacts returns one human-readable line per change between
// prior and cur that the stream cannot carry, after the exemptions the
// file comment lists. Empty means nothing to refuse.
func diffRelationFacts(prior, cur *pgRelationFacts) []string {
	var deltas []string

	// Columns present in both, and which of them were renamed or re-typed
	// this boundary.
	renamedOrRetyped := map[int16]bool{}
	for attnum, pc := range prior.columns {
		cc, ok := cur.columns[attnum]
		if !ok {
			continue
		}
		if pc.name != cc.name {
			renamedOrRetyped[attnum] = true
		}
		retyped := pc.typ != cc.typ
		if retyped {
			renamedOrRetyped[attnum] = true
		}
		if pc.notNull != cc.notNull {
			verb := "DROP NOT NULL"
			if cc.notNull {
				verb = "SET NOT NULL"
			}
			deltas = append(deltas, fmt.Sprintf("ALTER COLUMN %q %s", cc.name, verb))
		}
		if pc.identity != cc.identity {
			deltas = append(deltas, fmt.Sprintf("ALTER COLUMN %q identity %s -> %s", cc.name, identityWord(pc.identity), identityWord(cc.identity)))
		}
		if !retyped && pc.generated != cc.generated {
			deltas = append(deltas, fmt.Sprintf("ALTER COLUMN %q generated-ness changed", cc.name))
		}
		// A retype does NOT re-render a plain DEFAULT (measured on PG 16:
		// int→bigint keeps `0`, int→text keeps `5`, timestamp→timestamptz keeps
		// `now()`), so a default changed in the same statement as a retype is
		// still a change — the forwarded ALTER TYPE carries no DEFAULT. Only a
		// generation expression is exempt on a retype, since it is part of the
		// column the forward rebuilds.
		generatedRetype := retyped && (pc.generated != "" || cc.generated != "")
		if pc.def != cc.def && !generatedRetype {
			switch {
			case cc.def == "":
				deltas = append(deltas, fmt.Sprintf("ALTER COLUMN %q DROP DEFAULT (was %s)", cc.name, pc.def))
			case pc.generated != "" || cc.generated != "":
				deltas = append(deltas, fmt.Sprintf("ALTER COLUMN %q generation expression %s -> %s", cc.name, pc.def, cc.def))
			default:
				deltas = append(deltas, fmt.Sprintf("ALTER COLUMN %q SET DEFAULT %s", cc.name, cc.def))
			}
		}
	}

	touches := func(attnums []int16, set map[int16]bool) bool {
		for _, a := range attnums {
			if set[a] {
				return true
			}
		}
		return false
	}
	lostColumn := func(attnums []int16) bool {
		for _, a := range attnums {
			if a == 0 {
				continue // an expression element (EXCLUDE, expression index): not a column
			}
			if _, ok := cur.columns[a]; !ok {
				return true
			}
		}
		return false
	}

	for _, name := range sortedFactKeys(cur.constraints) {
		cc := cur.constraints[name]
		pc, ok := prior.constraints[name]
		switch {
		case !ok:
			deltas = append(deltas, fmt.Sprintf("ADD CONSTRAINT %q %s", name, cc.display))
		case pc.compare != cc.compare && !touches(cc.attnums, renamedOrRetyped):
			deltas = append(deltas, fmt.Sprintf("CONSTRAINT %q changed: %s -> %s", name, pc.display, cc.display))
		}
	}
	for _, name := range sortedFactKeys(prior.constraints) {
		if _, ok := cur.constraints[name]; ok {
			continue
		}
		pc := prior.constraints[name]
		if lostColumn(pc.attnums) {
			continue
		}
		deltas = append(deltas, fmt.Sprintf("DROP CONSTRAINT %q (was %s)", name, pc.display))
	}

	if prior.rlsEnabled != cur.rlsEnabled {
		deltas = append(deltas, fmt.Sprintf("row level security %s", enabledWord(cur.rlsEnabled)))
	}
	if prior.rlsForced != cur.rlsForced {
		deltas = append(deltas, fmt.Sprintf("FORCE row level security %s", enabledWord(cur.rlsForced)))
	}
	for _, name := range sortedFactKeys(cur.policies) {
		pp, ok := prior.policies[name]
		cp := cur.policies[name]
		switch {
		case !ok:
			deltas = append(deltas, fmt.Sprintf("CREATE POLICY %q (%s)", name, cp.display))
		case pp.compare != cp.compare:
			deltas = append(deltas, fmt.Sprintf("ALTER POLICY %q: %s -> %s", name, pp.display, cp.display))
		}
	}
	for _, name := range sortedFactKeys(prior.policies) {
		if _, ok := cur.policies[name]; !ok {
			deltas = append(deltas, fmt.Sprintf("DROP POLICY %q", name))
		}
	}
	return deltas
}

func sortedFactKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func enabledWord(on bool) string {
	if on {
		return "ENABLED"
	}
	return "DISABLED"
}

func identityWord(code string) string {
	switch code {
	case "a":
		return "GENERATED ALWAYS"
	case "d":
		return "GENERATED BY DEFAULT"
	default:
		return "none"
	}
}

// pgUnforwardedBaseline is the door's baseline as it crosses from one
// reader to the next through the pipeline (GC-32). Opaque to the
// pipeline: it only moves the value between readers of this engine.
type pgUnforwardedBaseline map[uint32]*pgRelationFacts

// unforwardedDoorState holds the door's baseline. The pump reads and
// writes facts; [CDCReader.UnforwardedBaseline] reads it from the
// pipeline between retry attempts, so it is guarded. A facts entry is
// never mutated in place — a re-baseline replaces the pointer — so a
// shallow map copy is a consistent snapshot.
type unforwardedDoorState struct {
	mu      sync.Mutex
	facts   map[uint32]*pgRelationFacts
	carried pgUnforwardedBaseline
}

// UnforwardedBaseline returns a snapshot of this reader's baseline — the
// StreamChanges fingerprint plus every re-baseline a passing relation
// made since — for the NEXT reader of the same run (GC-32). nil before
// StreamChanges or when the door is inert.
//
// Why it exists: the baseline lives in the reader, and an automatic retry
// after a transient error opens a fresh reader. Re-reading the catalog
// there would take a baseline that already contains any change made
// between the DDL and the failure, and accept it silently. Carrying the
// previous reader's baseline across the retry means only an operator
// restart re-baselines, which is the documented, deliberate case.
func (r *CDCReader) UnforwardedBaseline() any {
	r.unforwarded.mu.Lock()
	defer r.unforwarded.mu.Unlock()
	if r.unforwarded.facts == nil {
		// Never captured (an attempt that failed between being handed a
		// baseline and StreamChanges): pass on what it was handed, or a
		// chain of transients would drop the baseline at the first
		// attempt that failed early and the next would absorb the change.
		if r.unforwarded.carried == nil {
			return nil
		}
		return r.unforwarded.carried
	}
	out := make(pgUnforwardedBaseline, len(r.unforwarded.facts))
	for k, v := range r.unforwarded.facts {
		out[k] = v
	}
	return out
}

// SetUnforwardedBaseline hands this reader the previous reader's baseline
// (GC-32); StreamChanges then starts from it instead of the live catalog.
// Must be called before StreamChanges. A value from another engine is
// ignored and the reader takes its own baseline.
func (r *CDCReader) SetUnforwardedBaseline(b any) {
	carried, ok := b.(pgUnforwardedBaseline)
	if !ok {
		return
	}
	r.unforwarded.mu.Lock()
	defer r.unforwarded.mu.Unlock()
	r.unforwarded.carried = carried
}

// captureUnforwardedBaseline fingerprints every user table at
// StreamChanges — or, on a retry within one run, adopts the previous
// reader's baseline (GC-32). A nil catalog pool leaves the door inert
// (unit-test readers built without one).
func (r *CDCReader) captureUnforwardedBaseline(ctx context.Context) error {
	if r.db == nil {
		return nil
	}
	r.unforwarded.mu.Lock()
	carried := r.unforwarded.carried
	r.unforwarded.mu.Unlock()
	var facts map[uint32]*pgRelationFacts
	if carried == nil {
		var err error
		facts, err = readRelationFacts(ctx, r.db, 0)
		if err != nil {
			return fmt.Errorf("postgres: cdc: baseline the schema objects logical replication does not carry: %w", err)
		}
	} else {
		facts = make(map[uint32]*pgRelationFacts, len(carried))
		for k, v := range carried {
			facts[k] = v
		}
	}
	r.unforwarded.mu.Lock()
	r.unforwarded.facts = facts
	r.unforwarded.mu.Unlock()
	return nil
}

// gradeUnforwardedClasses is the per-RelationMessage half of the door. It
// is scope-gated exactly as the tier-2 schema race is: a change on a
// relation this stream emits nothing for must not end the stream. A
// relation with no baseline (created after StreamChanges) is baselined
// here; a relation that passes is re-baselined so an exempt change (a
// forwarded DROP COLUMN's cascade) is not re-diffed at the next boundary.
func (r *CDCReader) gradeUnforwardedClasses(ctx context.Context, relationID uint32, entry *relationCacheEntry) error {
	r.unforwarded.mu.Lock()
	armed := r.unforwarded.facts != nil
	prior := r.unforwarded.facts[relationID]
	r.unforwarded.mu.Unlock()
	if !armed || r.db == nil || !r.relationInScope(entry.Schema, entry.Name) {
		return nil
	}
	facts, err := readRelationFacts(ctx, r.db, relationID)
	if err != nil {
		return fmt.Errorf("postgres: cdc: relation %s.%s: read the schema objects logical replication does not carry: %w", entry.Schema, entry.Name, err)
	}
	cur := facts[relationID]
	if cur == nil {
		return nil // dropped since the message was written; the DML path owns that
	}
	if prior != nil {
		if deltas := diffRelationFacts(prior, cur); len(deltas) > 0 {
			return &terminalPGError{err: unforwardedChangeError(entry.Schema, entry.Name, deltas)}
		}
	}
	r.unforwarded.mu.Lock()
	r.unforwarded.facts[relationID] = cur
	r.unforwarded.mu.Unlock()
	return nil
}

func unforwardedChangeError(schema, table string, deltas []string) error {
	return markUnforwardedRefusal(fmt.Errorf("postgres: cdc: %s on %s.%s: the source changed schema objects that logical replication does not carry "+
		"and sluice cannot forward: %s. The target (or the backup chain) does not have this change, so continuing would leave it "+
		"silently weaker than the source — for a policy or row level security change, a security boundary. Remedy: "+
		"(1) apply the same change to the target yourself (for `backup stream`, take a new full backup instead); "+
		"(2) restart with the SAME --stream-id and --accept-unforwarded-schema-change. sluice records this refusal, so a restart "+
		"WITHOUT that flag refuses again; the flag takes a fresh baseline of these objects, so passing it WITHOUT step 1 "+
		"accepts the difference permanently and nothing will report it again",
		unforwardedChangeMarker, schema, table, strings.Join(deltas, "; ")))
}
