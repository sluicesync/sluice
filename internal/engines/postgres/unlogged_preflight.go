// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// UNLOGGED-table capture census (capture-completeness sweep 2026-08-26, G2).
//
// Unlogged tables write no WAL, so no logical-replication mechanism can
// ever stream their changes — and the two publication forms fail in
// OPPOSITE ways. A scoped `FOR TABLE` publication is refused by Postgres
// itself ("cannot add relation … to publication / This operation is not
// supported for unlogged tables" — observed on PG 16 for both CREATE and
// ALTER … ADD). A `FOR ALL TABLES` publication — the multi-schema
// spanning-sync shape (ADR-0075: a logical slot is database-wide) and the
// `backup full --chain-slot` shape — SILENTLY EXCLUDES the table: no error, no
// notice, it simply never appears in pg_publication_tables. The cold copy
// (and the full backup) census is information_schema BASE TABLE, which
// INCLUDES unlogged tables — so without this door the target (or the
// chain) receives the table's initial rows and then freezes at the
// snapshot forever while the stream stays green. Once later logged
// transactions advance the durable resume position past the unlogged
// writes' LSNs the loss is permanent, not a lag.
//
// This file is the shared census + coded refusal; the DOORS that consult
// it are enumerated on [refuseUnloggedTables]. It mirrors pgtrigger's §14
// unlogged-table setup refusal (setup.go, reason "unlogged-table") — the
// same class, spelled for the slot lane.

// unloggedTable is one census hit: an UNLOGGED ordinary table
// (relpersistence='u', relkind='r') in an in-scope schema.
type unloggedTable struct {
	Schema string
	Name   string
}

// String renders the qualified name for refusal messages.
func (u unloggedTable) String() string { return u.Schema + "." + u.Name }

// unloggedTablesInSchemas runs the census over the given schemas,
// optionally restricted to a bare-name table allowlist (nil = every
// table). relkind='r' is deliberate and sufficient: partition ROOTS are
// relkind 'p' and always permanent, while unlogged partition MEMBERS are
// 'r' and caught here (and a partitioned parent can never be in a sync's
// scope anyway — the Bug 100 preflight refuses it first).
func unloggedTablesInSchemas(ctx context.Context, db *sql.DB, schemas, tables []string) ([]unloggedTable, error) {
	const q = `
		SELECT n.nspname, c.relname
		FROM   pg_class     c
		JOIN   pg_namespace n ON n.oid = c.relnamespace
		WHERE  n.nspname = ANY($1)
		  AND  c.relkind = 'r'
		  AND  c.relpersistence = 'u'
		  AND  ($2::text[] IS NULL OR c.relname = ANY($2::text[]))
		ORDER  BY n.nspname, c.relname`
	rows, err := db.QueryContext(ctx, q, schemas, tableListOrNull(tables))
	if err != nil {
		return nil, fmt.Errorf("postgres: unlogged-table census: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []unloggedTable
	for rows.Next() {
		var u unloggedTable
		if err := rows.Scan(&u.Schema, &u.Name); err != nil {
			return nil, fmt.Errorf("postgres: unlogged-table census: %w", err)
		}
		out = append(out, u)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: unlogged-table census: %w", err)
	}
	return out, nil
}

// tableListOrNull maps an empty allowlist to SQL NULL so the census
// query's `$2 IS NULL` arm means "no table restriction".
func tableListOrNull(tables []string) any {
	if len(tables) == 0 {
		return nil
	}
	return tables
}

// refuseUnloggedTables builds the coded SLUICE-E-CDC-UNLOGGED-TABLE
// refusal, naming every offending table and the two remedies (mirroring
// pgtrigger §14's unlogged-table hint). lane names the door for the
// operator ("spanning sync cold start", "sync publication scope",
// "backup full --chain-slot"); why states the lane-specific harm.
//
// Door roster (the sibling-sweep enumeration for this class — every path
// that creates/ensures a publication over a scope that can hold an
// unlogged table):
//
//   - Spanning sync COLD START: pipeline coldStartMultiDatabase →
//     [Engine.PreflightSpanningUnloggedTables], BEFORE the spanning
//     snapshot opener runs ensureAllTablesPublication. Filter-aware via
//     the pipeline's predicate.
//   - Spanning sync WARM RESUME: pipeline warmResumeMultiDatabase →
//     the same preflight, before the server-wide CDC reader opens —
//     because `ALTER TABLE … SET UNLOGGED` SUCCEEDS mid-sync under FOR
//     ALL TABLES (PG only blocks the flip for scoped FOR TABLE
//     membership; observed on PG 16) and silently drops the table from
//     the publication. A flip DURING a live window stays undetectable
//     until the next open (documented residual, capture-completeness
//     matrix).
//   - Single-schema sync (scoped FOR TABLE): [ensurePublication] runs the
//     census over the explicit table list before CREATE/ALTER — Postgres
//     would refuse those tables anyway, so this door only upgrades the
//     raw PG error to the coded, pre-DDL form. It can never refuse a
//     configuration PG would have accepted.
//   - backup full --chain-slot: [Engine.OpenBackupSnapshot] under
//     opts.PersistChainSlot, before its ensureAllTablesPublication;
//     scoped by opts.InScopeTables (the backup's post-filter table set).
//
// NOT covered, stated per the gate-scope rule:
//
//   - The single-schema snapshot opener's nil-table ensurePublication
//     fallback (cdc_snapshot.go) when the publication is absent — a
//     direct-API/test-only shape (every pipeline path pre-ensures the
//     scoped publication, which Door 3 censuses first).
//   - The standalone CDC reader's own ensure (cdc_reader.go
//     StreamChanges): a no-op when the publication exists (the normal
//     warm-resume state); its absent-publication FOR-ALL-TABLES
//     recreate is a doubly-degenerate state (publication dropped
//     mid-life) where a census could false-refuse an operator whose
//     scoped stream legitimately excludes an unlogged table — the Bug
//     246 shape — so it stays uncensused.
//   - `backup incremental`'s CDC open: [irbackup.ChainResumePreflighter]
//     carries no table scope to honour the backup's filter, so a
//     mid-chain SET UNLOGGED flip is detected only by the next
//     `backup full --chain-slot`. Filed with the SET-UNLOGGED TOCTOU
//     follow-up in the capture-completeness matrix.
func refuseUnloggedTables(lane, why string, tables []unloggedTable) error {
	names := make([]string, len(tables))
	for i, u := range tables {
		names[i] = u.String()
	}
	plural := "table"
	if len(tables) > 1 {
		plural = "tables"
	}
	return sluicecode.Wrap(
		sluicecode.CodeCDCUnloggedTable,
		"exclude the UNLOGGED table(s) explicitly via --exclude-table, or convert them with ALTER TABLE ... SET LOGGED, then re-run",
		fmt.Errorf(
			"postgres: %s refused: UNLOGGED %s in scope: %s. Unlogged tables write no WAL, so their changes can never be streamed — %s. "+
				"Recovery: exclude them explicitly (--exclude-table), or make them durable (ALTER TABLE ... SET LOGGED; takes a rewrite lock) and re-run",
			lane, plural, strings.Join(names, ", "), why,
		),
	)
}

// preflightChainSlotUnlogged is Door 4's body (roster on
// [refuseUnloggedTables]): the census [Engine.OpenBackupSnapshot] runs
// under opts.PersistChainSlot, BEFORE its FOR ALL TABLES publication
// ensure. The publication silently excludes unlogged tables while the
// full backup's sweep includes them, so the chain's incrementals would
// never see their changes and a chain restore would silently serve the
// full's stale rows. inScopeTables is the backup's post-filter table set
// (Bug 246: an --exclude-table'd unlogged table must not trip the door);
// nil means the whole schema is in the backup's scope.
func preflightChainSlotUnlogged(ctx context.Context, db *sql.DB, schema string, inScopeTables []string) error {
	census, err := unloggedTablesInSchemas(ctx, db, []string{schema}, inScopeTables)
	if err != nil {
		return err
	}
	if len(census) == 0 {
		return nil
	}
	return refuseUnloggedTables(
		"backup full --chain-slot",
		"the chain's FOR ALL TABLES publication silently excludes them while the full backup includes them, so incrementals would never carry their changes and a chain restore would silently serve the full's stale rows",
		census,
	)
}

// PreflightSpanningUnloggedTables implements
// [ir.UnloggedCapturePreflighter]: the FOR-ALL-TABLES-lane census the
// pipeline runs at BOTH spanning stream-open chokepoints (cold start and
// warm resume — see the door roster on [refuseUnloggedTables]). allowed
// is the sync's effective table-scope predicate; nil means everything in
// the selected schemas is in scope.
func (e Engine) PreflightSpanningUnloggedTables(ctx context.Context, dsn string, schemas []string, allowed func(schema, table string) bool) error {
	if len(schemas) == 0 {
		return nil
	}
	cfg, err := e.parseDSN(dsn)
	if err != nil {
		return err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	census, err := unloggedTablesInSchemas(ctx, db, schemas, nil)
	if err != nil {
		// Fail CLOSED: an unverified census must not stream (the F2 door's
		// posture) — a probe error silently skipping the check is exactly
		// the SL-1 shape.
		return err
	}
	inScope := census[:0]
	for _, u := range census {
		if allowed == nil || allowed(u.Schema, u.Name) {
			inScope = append(inScope, u)
		}
	}
	if len(inScope) == 0 {
		return nil
	}
	return refuseUnloggedTables(
		"multi-schema spanning sync",
		"the FOR ALL TABLES publication this spanning sync streams through silently EXCLUDES them (no error, no notice) while the cold copy includes them, so the target would freeze each at its snapshot forever at exit 0",
		inScope,
	)
}
