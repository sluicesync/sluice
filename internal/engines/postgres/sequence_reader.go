// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// Standalone-sequence capture (item-51 TRIAGE finding #1).
//
// PG sequences fall into three classes, and only one of them is a
// first-class schema object sluice must carry:
//
//   - IDENTITY-backed (pg_depend deptype 'i'): implicit storage for a
//     `GENERATED ... AS IDENTITY` column. Never surfaced — the column
//     itself carries Integer.AutoIncrement and the writer re-emits the
//     identity clause.
//   - serial-backed (deptype 'a', factory-default options, referenced
//     ONLY by its owning column's default): the classic
//     `SERIAL`/`BIGSERIAL` shape. sluice deliberately modernizes these
//     to identity columns on emit (a DOCUMENTED decision — see
//     docs/type-mapping.md "Serial columns"), so the sequence object
//     itself is collapsed into Integer.AutoIncrement.
//   - STANDALONE (everything else): explicitly created sequences,
//     serials whose sequence was re-optioned (custom START/INCREMENT/
//     ...), and sequences shared by more than one column. These carry
//     operator-visible semantics (the numbers future inserts produce),
//     so they are read into [ir.Schema.Sequences] with their exact
//     options + current position, and the referencing columns keep
//     their `DEFAULT nextval(...)` verbatim. Pre-fix, ALL of these
//     collapsed to identity columns — a silent semantic change
//     (post-migration inserts generated different numbers than the
//     source would).

// pgSequenceOwner identifies the column a serial-collapsible sequence
// backs, within the reader's bound schema.
type pgSequenceOwner struct {
	table  string
	column string
}

// pgSequenceRef is one column whose DEFAULT expression depends on a
// sequence (a pg_depend edge from the column's pg_attrdef row).
type pgSequenceRef struct {
	schema string
	table  string
	column string
}

// readSequences loads every user sequence in the bound schema and
// splits them into the standalone set (returned as IR, sorted by
// name — the catalog query orders by relname) and the
// serial-collapsible map consumed by populateColumns' auto-increment
// classification (sequence name → owning column). Identity-backed and
// extension-owned sequences are skipped entirely.
func (r *SchemaReader) readSequences(ctx context.Context) ([]*ir.Sequence, map[string]pgSequenceOwner, error) {
	extMembers, err := r.extensionMemberRelations(ctx)
	if err != nil {
		return nil, nil, err
	}

	// One row per sequence: options from pg_sequence, plus the owning
	// column when an 'a' (serial OWNED BY) or 'i' (identity) pg_depend
	// edge exists. format_type gives the canonical data-type spelling
	// ("smallint" / "integer" / "bigint").
	const q = `
		SELECT c.relname,
		       pg_catalog.format_type(s.seqtypid, NULL),
		       s.seqstart, s.seqincrement, s.seqmin, s.seqmax,
		       s.seqcache, s.seqcycle,
		       COALESCE(dep.deptype::text, ''),
		       COALESCE(onn.nspname, ''),
		       COALESCE(ot.relname, ''),
		       COALESCE(oa.attname, '')
		FROM   pg_class     c
		JOIN   pg_namespace n ON n.oid = c.relnamespace
		JOIN   pg_sequence  s ON s.seqrelid = c.oid
		LEFT JOIN pg_depend dep ON dep.classid    = 'pg_class'::regclass
		                       AND dep.objid      = c.oid
		                       AND dep.refclassid = 'pg_class'::regclass
		                       AND dep.deptype    IN ('a', 'i')
		LEFT JOIN pg_class     ot  ON ot.oid       = dep.refobjid
		LEFT JOIN pg_namespace onn ON onn.oid      = ot.relnamespace
		LEFT JOIN pg_attribute oa  ON oa.attrelid  = dep.refobjid
		                          AND oa.attnum    = dep.refobjsubid
		WHERE  n.nspname = $1
		  AND  c.relkind = 'S'
		ORDER  BY c.relname`

	rows, err := r.catalogQuery(ctx, q, r.schema)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = rows.Close() }()

	type seqRow struct {
		seq      *ir.Sequence
		deptype  string
		ownerNS  string
		ownerTbl string
		ownerCol string
	}
	var seqs []seqRow
	for rows.Next() {
		var (
			name, dataType                     string
			start, increment, minv, maxv, cch  int64
			cycle                              bool
			deptype, ownerNS, ownerT, ownerCol string
		)
		if err := rows.Scan(
			&name, &dataType,
			&start, &increment, &minv, &maxv, &cch, &cycle,
			&deptype, &ownerNS, &ownerT, &ownerCol,
		); err != nil {
			return nil, nil, err
		}
		// Identity-backed sequences are implicit column storage; the
		// identity clause re-creates them on the target.
		if deptype == "i" {
			continue
		}
		// Bug 96 discipline: extension-owned sequences belong to the
		// extension and are recreated by CREATE EXTENSION.
		if _, isExt := extMembers[name]; isExt {
			continue
		}
		seqs = append(seqs, seqRow{
			seq: &ir.Sequence{
				Schema:    r.schema,
				Name:      name,
				DataType:  dataType,
				Start:     start,
				Increment: increment,
				MinValue:  minv,
				MaxValue:  maxv,
				Cache:     cch,
				Cycle:     cycle,
			},
			deptype:  deptype,
			ownerNS:  ownerNS,
			ownerTbl: ownerT,
			ownerCol: ownerCol,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	if len(seqs) == 0 {
		return nil, nil, nil
	}

	refs, err := r.readSequenceDefaultRefs(ctx)
	if err != nil {
		return nil, nil, err
	}

	var standalone []*ir.Sequence
	serial := map[string]pgSequenceOwner{}
	for _, sr := range seqs {
		if r.isSerialCollapsible(sr.seq, sr.deptype, sr.ownerNS, sr.ownerTbl, sr.ownerCol, refs[sr.seq.Name]) {
			serial[sr.seq.Name] = pgSequenceOwner{table: sr.ownerTbl, column: sr.ownerCol}
			continue
		}
		// Carry the OWNED BY only when the owner lives in the bound
		// schema — a cross-schema owner can't be re-bound by this
		// single-schema writer pass (OWNED BY only affects DROP
		// cascade, so the drop is cosmetic, and cross-schema sequence
		// topology is out of the single-schema reader's scope anyway).
		if sr.deptype == "a" && sr.ownerNS == r.schema {
			sr.seq.OwnedByTable = sr.ownerTbl
			sr.seq.OwnedByColumn = sr.ownerCol
		}
		if err := r.readSequencePosition(ctx, sr.seq); err != nil {
			return nil, nil, err
		}
		standalone = append(standalone, sr.seq)
	}
	return standalone, serial, nil
}

// readSequenceDefaultRefs maps each sequence name in the bound schema
// to the columns whose DEFAULT expressions depend on it (pg_depend
// edges from pg_attrdef rows — PG records one per `nextval('seq')`
// regclass literal). Consumed by the serial-collapsibility test: a
// sequence referenced by any column other than its owner is SHARED
// topology and must be carried standalone.
func (r *SchemaReader) readSequenceDefaultRefs(ctx context.Context) (map[string][]pgSequenceRef, error) {
	const q = `
		SELECT sc.relname, tn.nspname, tc.relname, a.attname
		FROM   pg_depend    d
		JOIN   pg_attrdef   ad ON ad.oid      = d.objid
		JOIN   pg_class     tc ON tc.oid      = ad.adrelid
		JOIN   pg_namespace tn ON tn.oid      = tc.relnamespace
		JOIN   pg_attribute a  ON a.attrelid  = ad.adrelid
		                      AND a.attnum    = ad.adnum
		JOIN   pg_class     sc ON sc.oid      = d.refobjid
		JOIN   pg_namespace sn ON sn.oid      = sc.relnamespace
		WHERE  d.classid    = 'pg_attrdef'::regclass
		  AND  d.refclassid = 'pg_class'::regclass
		  AND  sc.relkind   = 'S'
		  AND  sn.nspname   = $1`

	rows, err := r.catalogQuery(ctx, q, r.schema)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := map[string][]pgSequenceRef{}
	for rows.Next() {
		var seqName, refNS, refTable, refCol string
		if err := rows.Scan(&seqName, &refNS, &refTable, &refCol); err != nil {
			return nil, err
		}
		out[seqName] = append(out[seqName], pgSequenceRef{schema: refNS, table: refTable, column: refCol})
	}
	return out, rows.Err()
}

// isSerialCollapsible reports whether seq is the classic serial shape
// sluice modernizes into an identity column (docs/type-mapping.md
// "Serial columns"): OWNED BY a column in the bound schema, carrying
// factory-default options for its data type, and referenced by NO
// column default other than its owner's. Anything looser — custom
// options, shared references, no owner — is operator-visible sequence
// semantics and must be carried standalone.
func (r *SchemaReader) isSerialCollapsible(seq *ir.Sequence, deptype, ownerNS, ownerTbl, ownerCol string, refs []pgSequenceRef) bool {
	if deptype != "a" || ownerNS != r.schema || ownerTbl == "" || ownerCol == "" {
		return false
	}
	if !serialDefaultOptions(seq) {
		return false
	}
	// Exactly the owner's own default may reference it. Zero refs
	// (owned serial whose default was dropped) or extra refs (shared
	// sequence) both disqualify: collapsing would either orphan the
	// object or break the sibling column's default.
	if len(refs) != 1 {
		return false
	}
	ref := refs[0]
	return ref.schema == r.schema && ref.table == ownerTbl && ref.column == ownerCol
}

// serialDefaultOptions reports whether seq carries exactly the options
// `CREATE TABLE t (c serial)` produces: START 1 INCREMENT 1 MINVALUE 1
// CACHE 1 NO CYCLE, MAXVALUE = the data type's positive ceiling.
func serialDefaultOptions(seq *ir.Sequence) bool {
	return seq.Start == 1 && seq.Increment == 1 && seq.MinValue == 1 &&
		seq.Cache == 1 && !seq.Cycle && seq.MaxValue == sequenceTypeMax(seq.DataType)
}

// sequenceTypeMax is the default MAXVALUE for an ascending sequence of
// the given data type (format_type spelling). Unknown spellings return
// 0 so serialDefaultOptions can never match them — an unrecognized
// type falls through to the standalone carry, the safe side.
func sequenceTypeMax(dataType string) int64 {
	switch dataType {
	case "smallint":
		return math.MaxInt16
	case "integer":
		return math.MaxInt32
	case "bigint":
		return math.MaxInt64
	}
	return 0
}

// readSequencePosition captures the sequence's current position
// (`last_value` + `is_called`, selected from the sequence relation
// itself — the pg_sequences view hides is_called) so the writer can
// prime the target via setval and post-migration nextval() continues
// exactly where the source would.
func (r *SchemaReader) readSequencePosition(ctx context.Context, seq *ir.Sequence) error {
	q := fmt.Sprintf(`SELECT last_value, is_called FROM %s.%s`,
		quoteIdent(r.schema), quoteIdent(seq.Name))
	var lastValue int64
	var isCalled bool
	if err := r.db.QueryRowContext(ctx, q).Scan(&lastValue, &isCalled); err != nil {
		return fmt.Errorf("read position of sequence %q: %w", seq.Name, err)
	}
	seq.LastValue = lastValue
	seq.LastValueIsCalled = isCalled
	seq.LastValueValid = true
	return nil
}

// parseNextvalSequence extracts the sequence identifier from a
// `nextval('...'::regclass)` column default as PG renders it in
// information_schema.columns.column_default. Returns the sequence's
// schema qualifier (empty when unqualified), its bare name, and
// whether the text was a recognizable nextval default at all.
//
// Handled shapes: `nextval('seq'::regclass)`,
// `nextval('sch.seq'::regclass)`, `nextval('"Mixed Case"'::regclass)`,
// `nextval('sch."Mixed.Name"'::regclass)`. Anything else returns
// ok=false and the caller treats the default as a plain expression —
// carried verbatim, never silently collapsed.
func parseNextvalSequence(defaultText string) (schema, name string, ok bool) {
	const prefix = "nextval("
	if !strings.HasPrefix(defaultText, prefix) {
		return "", "", false
	}
	rest := defaultText[len(prefix):]
	if rest == "" || rest[0] != '\'' {
		return "", "", false
	}
	// Scan the single-quoted literal, honouring doubled '' escapes.
	var b strings.Builder
	i := 1
	for {
		if i >= len(rest) {
			return "", "", false // unterminated literal
		}
		if rest[i] == '\'' {
			if i+1 < len(rest) && rest[i+1] == '\'' {
				b.WriteByte('\'')
				i += 2
				continue
			}
			break
		}
		b.WriteByte(rest[i])
		i++
	}
	return splitSequenceQualifier(b.String())
}

// splitSequenceQualifier splits an (optionally schema-qualified,
// optionally double-quoted) sequence reference into its parts,
// unquoting each. The split point is the last '.' outside double
// quotes so quoted names containing dots stay intact.
func splitSequenceQualifier(ref string) (schema, name string, ok bool) {
	dot := -1
	inQuotes := false
	for i := 0; i < len(ref); i++ {
		switch ref[i] {
		case '"':
			inQuotes = !inQuotes
		case '.':
			if !inQuotes {
				dot = i
			}
		}
	}
	if inQuotes {
		return "", "", false // unbalanced quoting
	}
	unquote := func(s string) string {
		if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
			return strings.ReplaceAll(s[1:len(s)-1], `""`, `"`)
		}
		return s
	}
	if dot >= 0 {
		schema, name = unquote(ref[:dot]), unquote(ref[dot+1:])
	} else {
		name = unquote(ref)
	}
	if name == "" {
		return "", "", false
	}
	return schema, name, true
}

// classifyAutoIncrement is [ReadSchema]'s replacement for the legacy
// two-argument [isAutoIncrement] heuristic (which treated EVERY
// nextval() default as SERIAL — the item-51 silent collapse). Identity
// columns stay auto-increment; a nextval() default counts as
// auto-increment ONLY when it references the serial-collapsible
// sequence owned by exactly this column. Every other nextval() shape
// (standalone sequence, shared sequence, re-optioned serial,
// cross-schema reference) classifies as a plain expression default so
// translateDefault carries it verbatim.
//
// The CDC applier / shard-probe projections keep the legacy
// [isAutoIncrement]: they compare projections against other
// projections from the same code path and have no sequence catalog in
// hand, so the looser heuristic is internally consistent there.
func (r *SchemaReader) classifyAutoIncrement(isIdentity string, columnDefault sql.NullString, tableName, colName string, serialSeqs map[string]pgSequenceOwner) bool {
	if strings.EqualFold(isIdentity, "YES") {
		return true
	}
	if !columnDefault.Valid {
		return false
	}
	seqSchema, seqName, ok := parseNextvalSequence(columnDefault.String)
	if !ok {
		return false
	}
	if seqSchema != "" && seqSchema != r.schema {
		return false
	}
	owner, collapsible := serialSeqs[seqName]
	return collapsible && owner.table == tableName && owner.column == colName
}
