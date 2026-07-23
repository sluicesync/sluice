// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// Publication lifecycle for the Postgres CDC reader — everything that
// creates, scopes, guards, verifies, or drops a pg_publication.
//
// Extracted verbatim from cdc_reader.go (audit 2026-07-23 ARCH-6): the
// ADR-0175/0176 arc grew the publication surface to ~1,000 lines of
// catalog reads and guard decisions with their own tests and no
// dependence on the WAL pump, which had turned cdc_reader.go into a
// god file. Same package, same behavior — only the file boundary moved.
//
// The pieces, in reading order:
//
//   - [ensurePublication] / [ensureAllTablesPublication] /
//     [addTablesToPublication] — the create/rescope/add entry points
//     (Bug 13, ADR-0021, ADR-0075 multi-schema, the mid-stream
//     add-table flow), with [publicationRowFiltersForVersion] applying
//     the ADR-0176 PG-15 version gate and [formatPublicationTableList]
//     rendering the single-sourced DDL table list.
//   - The ADR-0175/0176 scope-conflict guard family —
//     [guardPublicationNarrowing] and its pure decision helpers
//     ([removedFromPublication], [attributeChangedSurvivors],
//     [changedMemberDefs], the key-set projections), the catalog reads
//     they compare against ([publicationMemberDefs] /
//     [publicationMemberDefsAt] / [publicationMemberSet]), the
//     [otherSluiceSlots] existence probe, and the transactional no-op
//     rescope probe [rescopeRowFilteredPublication].
//   - The post-slot re-verification [verifyPublicationUnchanged]
//     (audit 2026-07-23 D0-7) and both coded refusal renderers
//     ([publicationScopeConflictRefusal],
//     [publicationPostSlotConflictRefusal]).
//   - Lifecycle odds and ends: [publicationIsEmpty] (the silent-stall
//     warning probe), [isDuplicatePublication] (the fleet create-race
//     tolerance), and [dropOwnPublicationIfPerStream] (cleanup parity
//     for per-stream publications).

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// ensurePublication CREATEs the publication if it doesn't already
// exist, or ALTERs an existing publication's table set when one of
// the call sites supplies an explicit list (Bug 13, ADR-0021).
//
// Three cases:
//
//   - tables == nil: legacy "FOR ALL TABLES" shape. The caller
//     hasn't told us which tables to scope to — typically a non-
//     streamer test path or a code path that doesn't yet have the
//     schema in hand. CREATE FOR ALL TABLES if missing; leave any
//     pre-existing publication alone.
//   - tables non-nil and missing: CREATE PUBLICATION … FOR TABLE
//     <list> with each name qualified by schema. The publication
//     is scoped to just those tables so a CREATE TABLE on the
//     source mid-stream stays out of the WAL stream and the
//     applier never sees events for a non-existent target table.
//   - tables non-nil and the publication already exists: ALTER
//     PUBLICATION … SET TABLE <list>. This handles the migration
//     path from a v0.4.0-or-earlier "FOR ALL TABLES" publication
//     to a scoped one. ALTER ... SET TABLE replaces the entire
//     table set atomically.
//
// The schema-qualification matters because a publication's table
// references resolve in the session's search_path; quoting and
// schema-qualifying both the relation and identifiers keeps the
// behaviour robust against unusual search_path settings.
// excludeSlot is the caller's OWN replication slot, excluded from the
// ADR-0175 conflict probe. Empty is safe: the probe only considers
// ACTIVE slots, and cold start ensures the publication before its own
// slot exists.
//
// filters is the ADR-0176 per-table row-filter map (source table name →
// raw predicate text, the [ir.PublicationRowFilterer] contract). It is
// consulted only when an explicit table list is supplied, and only on
// PG 15+ — below that the catalog has no per-table attributes, so the
// filters are dropped and the emitted DDL is byte-identical to today
// (silently-safe: the client-side evaluator remains the filter).
func ensurePublication(ctx context.Context, db *sql.DB, name, schema string, tables []string, excludeSlot string, filters map[string]string) error {
	var exists, allTables bool
	const checkQuery = "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1), " +
		"COALESCE((SELECT puballtables FROM pg_publication WHERE pubname = $1), false)"
	if err := db.QueryRowContext(ctx, checkQuery, name).Scan(&exists, &allTables); err != nil {
		return fmt.Errorf("postgres: check publication: %w", err)
	}

	// Resolve the server version once when a row filter could be emitted;
	// version drives both the DDL rendering here and the definition
	// re-read inside the ADR-0176 no-op probe below.
	version := 0
	if len(tables) > 0 && len(filters) > 0 {
		v, err := serverVersionNum(ctx, db)
		if err != nil {
			return err
		}
		version = v
		filters = publicationRowFiltersForVersion(ctx, version, filters)
	}

	if !exists {
		var createQuery string
		if len(tables) == 0 {
			createQuery = fmt.Sprintf(`CREATE PUBLICATION %s FOR ALL TABLES`, quoteIdent(name))
		} else {
			createQuery = fmt.Sprintf(`CREATE PUBLICATION %s FOR TABLE %s`,
				quoteIdent(name), formatPublicationTableList(schema, tables, filters))
		}
		if _, err := db.ExecContext(ctx, createQuery); err != nil {
			if !isDuplicatePublication(err) {
				return fmt.Errorf("postgres: create publication %q: %w", name, err)
			}
			// Lost the create race against a concurrent ensurer. Several
			// PG-source syncs sharing one source routinely ensure the same
			// publication at fleet cold-start (ADR-0122), and a check-then-
			// create has a TOCTOU window where two sessions both pass the
			// existence check and both CREATE, so one hits a unique-violation
			// on pg_publication's pubname index (SQLSTATE 23505). That is
			// benign — the publication now exists — so re-read and fall
			// through to reconcile our scope rather than failing the sync
			// (which the supervisor would then spuriously restart).
			if err := db.QueryRowContext(ctx, checkQuery, name).Scan(&exists, &allTables); err != nil {
				return fmt.Errorf("postgres: re-check publication after create race: %w", err)
			}
			if !exists {
				return fmt.Errorf("postgres: publication %q reported a duplicate on create yet is absent on re-check", name)
			}
			// fall through to the exists-reconcile below
		} else {
			return nil
		}
	}

	// Publication exists. If the caller supplied an explicit table
	// list, sync the scope (ALTER … SET TABLE replaces the whole
	// list atomically; safe to run repeatedly). If the existing
	// publication is FOR ALL TABLES and the caller wants a scoped
	// list, ALTER ... SET TABLE on a FOR-ALL-TABLES publication
	// errors with "publication ... is defined as FOR ALL TABLES";
	// in that case we drop and recreate. The drop is safe because
	// the publication is metadata only — slots reference WAL by
	// LSN, not by publication name binding.
	if len(tables) == 0 {
		// Caller hasn't supplied a scope, so respect whatever the
		// publication currently is. One shape is worth a loud warning
		// though: a publication that is NOT FOR ALL TABLES and has no
		// member tables / no FOR-TABLES-IN-SCHEMA memberships can never
		// emit a pgoutput row, so streaming from it pins the slot's
		// confirmed_flush_lsn and replicates nothing — a silent stall
		// that is painful to diagnose. It is usually a stale publication
		// left from an aborted run (DROP SCHEMA does not drop
		// publications).
		//
		// We WARN rather than refuse: an empty publication legitimately
		// occurs on this no-scope path (a reader reusing a publication
		// whose tables were just dropped; the streamer's own scoped
		// EnsurePublication call is what establishes scope in the normal
		// migrate/sync flow), so a hard error here would break those
		// callers. The warning names the publication and the recovery so
		// the stall is no longer silent. FOR ALL TABLES is implicitly
		// non-empty, so it is excluded.
		if !allTables {
			empty, err := publicationIsEmpty(ctx, db, name)
			if err != nil {
				return err
			}
			if empty {
				slog.WarnContext(
					ctx,
					"postgres: publication has no tables and is not FOR ALL TABLES — "+
						"the CDC stream will replicate nothing until it is scoped. This is "+
						"usually a stale publication left from a prior or aborted run "+
						"(DROP SCHEMA does not drop publications); recover by dropping it "+
						"(sluice recreates it) or `ALTER PUBLICATION ... ADD TABLE <table>`",
					slog.String("publication", name),
				)
			}
		}
		return nil
	}
	if allTables {
		// ADR-0175: demoting FOR ALL TABLES to a scoped list removes
		// every unlisted table from the stream. If another active
		// sluice slot is reading through this publication, that is the
		// silent-loss shape — refuse rather than rescope.
		if _, err := guardPublicationNarrowing(ctx, db, name, nil, excludeSlot); err != nil {
			return err
		}
		// Migrate: drop the FOR ALL TABLES publication and recreate
		// scoped. ALTER cannot demote FOR ALL TABLES → FOR TABLE
		// directly.
		dropQuery := fmt.Sprintf(`DROP PUBLICATION %s`, quoteIdent(name))
		if _, err := db.ExecContext(ctx, dropQuery); err != nil {
			return fmt.Errorf("postgres: drop FOR-ALL-TABLES publication %q for migration: %w", name, err)
		}
		createQuery := fmt.Sprintf(`CREATE PUBLICATION %s FOR TABLE %s`,
			quoteIdent(name), formatPublicationTableList(schema, tables, filters))
		if _, err := db.ExecContext(ctx, createQuery); err != nil {
			return fmt.Errorf("postgres: re-create publication %q with scoped tables: %w", name, err)
		}
		return nil
	}
	// ADR-0175: `SET TABLE` replaces the member set atomically, so an
	// incoming list that omits a current member REMOVES it — and, one
	// level down (ADR-0176), a list that clears/adds/changes a surviving
	// member's row filter changes the DEFINITION. Guard both; a widening
	// or equal rescope changes nothing and proceeds.
	ambiguous, err := guardPublicationNarrowing(ctx, db, name, qualifiedTableQuals(schema, tables, filters), excludeSlot)
	if err != nil {
		return err
	}
	alterQuery := fmt.Sprintf(`ALTER PUBLICATION %s SET TABLE %s`,
		quoteIdent(name), formatPublicationTableList(schema, tables, filters))
	if len(ambiguous) > 0 {
		// ADR-0176: a survivor carries a row filter and the incoming list
		// carries one too. Raw predicate text cannot be compared to the
		// catalog's pg_get_expr rendering (`country = 'US'` vs
		// `(country = 'US'::text)`), so only PG itself can decide whether
		// this rescope is the routine no-op re-assert (our own cold
		// restart) or a genuine definition change (another stream's filter
		// being replaced). Resolve it with the transactional probe.
		return rescopeRowFilteredPublication(ctx, db, name, alterQuery, version, excludeSlot)
	}
	if _, err := db.ExecContext(ctx, alterQuery); err != nil {
		return fmt.Errorf("postgres: alter publication %q tables: %w", name, err)
	}
	return nil
}

// publicationRowFiltersForVersion applies the ADR-0176 version gate:
// per-table publication row filters exist from PG 15
// ([pgVersionPublicationAttrs]); on an older server the filters are
// DROPPED (nil) so the emitted DDL is byte-identical to the pre-ADR-0176
// shape. Silently-safe by design — the client-side evaluator always runs
// and remains the filter there — but logged at DEBUG so the degradation
// is observable.
func publicationRowFiltersForVersion(ctx context.Context, version int, filters map[string]string) map[string]string {
	if version >= pgVersionPublicationAttrs {
		return filters
	}
	// INFO deliberately (audit 2026-07-23 DEVEX-4): the preflight has
	// already told the operator at INFO that these predicates "will be
	// pushed", so the version-gated drop must be equally visible or the
	// operator believes WAL delivery is server-filtered when it is not
	// (capacity/security expectations — data stays correct either way,
	// the client evaluator filters).
	slog.InfoContext(
		ctx, "postgres: source predates publication row filters (PG 15) — these tables stream UNFILTERED server-side and the client-side evaluator filters them (ADR-0176 version gate; correctness preserved, out-of-scope change traffic is discarded client-side)",
		slog.Int("server_version_num", version),
		slog.Int("filtered_tables", len(filters)),
	)
	return nil
}

// rescopeRowFilteredPublication resolves the ambiguous both-sides-carry-a-
// row-filter rescope (ADR-0176): run the SET TABLE inside a transaction,
// re-read the resulting definition, and let PG's own pg_get_expr
// normalization decide whether anything actually changed.
//
//   - Definition unchanged → the rescope was a no-op re-assert (a filtered
//     stream's own cold restart with the same predicate) → COMMIT.
//   - Definition changed and no other sluice slot exists → the operator's
//     own deliberate rescope (e.g. a changed --where on the only stream)
//     → COMMIT.
//   - Definition changed and another sluice slot exists → ROLLBACK and
//     refuse with the ADR-0175 scope-conflict error: the change could be
//     silently replacing a peer stream's filter. The rollback leaves the
//     publication — and every stream reading through it — untouched, the
//     same no-mutation-on-refusal guarantee the pre-mutation guard gives.
func rescopeRowFilteredPublication(ctx context.Context, db *sql.DB, name, alterQuery string, version int, excludeSlot string) error {
	before, err := publicationMemberDefsAt(ctx, db, name, version)
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("postgres: begin publication rescope probe: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if _, err := tx.ExecContext(ctx, alterQuery); err != nil {
		return fmt.Errorf("postgres: alter publication %q tables: %w", name, err)
	}
	after, err := publicationMemberDefsAt(ctx, tx, name, version)
	if err != nil {
		return err
	}
	changed := changedMemberDefs(before, after)
	if len(changed) == 0 {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("postgres: commit publication rescope: %w", err)
		}
		committed = true
		return nil
	}
	// A genuine definition change. Same decision as the pre-mutation
	// guard: allowed when no other sluice slot holds a claim on this
	// source, refused otherwise. The slot probe runs on db (its own
	// connection) — pg_replication_slots is not transactional state.
	slots, err := otherSluiceSlots(ctx, db, excludeSlot)
	if err != nil {
		return err
	}
	if len(slots) == 0 {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("postgres: commit publication rescope: %w", err)
		}
		committed = true
		return nil
	}
	_ = tx.Rollback()
	committed = true // rollback done; suppress the deferred one
	return publicationScopeConflictRefusal(name, []string{"CHANGE the per-table row filter on " + strings.Join(changed, ", ")}, slots)
}

// changedMemberDefs returns the sorted members whose definition differs
// between two catalog reads (both sides pg_get_expr-normalized, so text
// equality is exact), each named for the refusal message. Pure, like
// [removedFromPublication].
//
// The BEFORE side's predicate text is REDACTED (audit 2026-07-23 SEC-1):
// it is catalog state that may quote a PEER stream's row filter, and a
// predicate routinely carries data values (customer identifiers, date
// cutoffs) that must not be echoed into another operator's terminal. The
// AFTER side is the caller's OWN incoming definition as PG normalized
// it, so it stays visible — the operator already typed it.
func changedMemberDefs(before, after map[string]publicationMemberAttrs) []string {
	var changed []string
	for m, b := range before {
		a, ok := after[m]
		if !ok || a == b {
			continue
		}
		changed = append(changed, fmt.Sprintf("%s (row filter changed: WHERE (<current filter hidden — it may belong to another stream>) → WHERE (%s))", m, a.Qual))
	}
	for m := range after {
		if _, ok := before[m]; !ok {
			changed = append(changed, m+" (added)")
		}
	}
	sort.Strings(changed)
	return changed
}

// qualifiedTableQuals renders the incoming publication definition the
// ADR-0175/0176 guard compares against the catalog: "schema.table" keys
// matching [publicationMemberSet]'s shape (mirroring
// [formatPublicationTableList]'s empty-schema handling), each mapped to
// its raw ADR-0176 row-filter text — "" for a bare (filter-free) member,
// which is every member when filters is nil.
func qualifiedTableQuals(schema string, tables []string, filters map[string]string) map[string]string {
	keys := make(map[string]string, len(tables))
	for _, t := range tables {
		keys[schema+"."+t] = filters[t]
	}
	return keys
}

// incomingKeySet projects the incoming definition down to the bare
// member-key set [removedFromPublication] speaks.
func incomingKeySet(incoming map[string]string) map[string]struct{} {
	keys := make(map[string]struct{}, len(incoming))
	for m := range incoming {
		keys[m] = struct{}{}
	}
	return keys
}

// removedFromPublication returns the sorted members that `incoming`
// would drop from the publication — the ADR-0175 narrowing set. Empty
// means the rescope widens or leaves scope unchanged, which is always
// safe: it can only ADD tables to another stream's view, never take
// them away.
//
// Pure set difference, split out from [guardPublicationNarrowing] so
// the decision that gates a refusal is testable without a database.
func removedFromPublication(members, incoming map[string]struct{}) []string {
	var removed []string
	for m := range members {
		if _, keep := incoming[m]; !keep {
			removed = append(removed, m)
		}
	}
	sort.Strings(removed)
	return removed
}

// attributeChangedSurvivors classifies the members that SURVIVE the
// rescope (present in both the current definition and the incoming set)
// by what the rescope would do to their per-table attributes — the
// ADR-0176 widening of the ADR-0175 guard (two identical table SETS with
// differing per-table attributes are a scope conflict one level down: a
// stream reading through its row filter would silently start receiving —
// or a peer would stop filtering — every row).
//
// Two buckets:
//
//   - hard: the change is provable from the catalog alone — a current
//     row filter the bare incoming member would CLEAR, a column list any
//     incoming SET TABLE clears (sluice never emits one), or a row
//     filter the incoming member would ADD where none exists. Each
//     entry names the attribute so the refusal is operator-actionable.
//   - ambiguous: BOTH sides carry a row filter. Equality is undecidable
//     here — the incoming side is raw operator text, the catalog side is
//     pg_get_expr-normalized — so the caller resolves it with the
//     transactional no-op probe ([rescopeRowFilteredPublication]).
//
// Catalog-side predicate text (attrs.Qual) is never echoed (audit
// 2026-07-23 SEC-1): it may be a PEER stream's row filter carrying data
// values, and the refusal can land in another operator's terminal — the
// entry names the member and that it "carries a row filter" instead. The
// INCOMING qual is the caller's own `--where` text and stays visible.
//
// Pure, like [removedFromPublication], so the decision that gates a
// refusal is testable without a database.
func attributeChangedSurvivors(defs map[string]publicationMemberAttrs, incoming map[string]string) (hard, ambiguous []string) {
	for m, attrs := range defs {
		inQual, keep := incoming[m]
		if !keep {
			continue // removal — reported by removedFromPublication
		}
		switch {
		case attrs.HasColumnList:
			// sluice never emits column lists, so any rescope clears it —
			// with or without an incoming row filter.
			detail := m + " (a column list would be cleared"
			if attrs.Qual != "" && inQual == "" {
				detail += ", and the row filter it carries too"
			}
			hard = append(hard, detail+")")
		case attrs.Qual != "" && inQual == "":
			hard = append(hard, m+" (carries a row filter this rescope would clear)")
		case attrs.Qual == "" && inQual != "":
			hard = append(hard, m+" (a row filter WHERE ("+inQual+") would be added, narrowing the stream)")
		case attrs.Qual != "" && inQual != "":
			ambiguous = append(ambiguous, m)
		}
	}
	sort.Strings(hard)
	sort.Strings(ambiguous)
	return hard, ambiguous
}

// memberKeySet projects a definition map down to the bare member-key
// set [removedFromPublication] and [addTablesToPublication] speak.
func memberKeySet(defs map[string]publicationMemberAttrs) map[string]struct{} {
	keys := make(map[string]struct{}, len(defs))
	for m := range defs {
		keys[m] = struct{}{}
	}
	return keys
}

// publicationMemberAttrs carries one member table's per-table
// publication attributes (PG 15+): the row-filter WHERE clause
// (pg_publication_rel.prqual, rendered via pg_get_expr) and whether a
// column list (prattrs) is attached. sluice's own definitions carry at
// most a row filter (the ADR-0176 push-down); it never emits a column
// list, so a column list on a surviving member always makes a rescope
// a definition change ([attributeChangedSurvivors]). On PG < 15 the
// catalog has no attribute columns and every member reports the zero
// value.
type publicationMemberAttrs struct {
	Qual          string
	HasColumnList bool
}

// publicationMemberDefs returns the publication's current member
// DEFINITIONS keyed "schema.table": each member's per-table attributes
// on PG 15+ (prqual / prattrs, gated on [serverVersionNum] — the same
// precedent as pgVersionFailoverSupport), the attribute-free zero
// value on older servers where the columns don't exist. The ADR-0175
// guard compares against these so two identical table SETS with
// differing attributes still register as a scope conflict (the
// ADR-0176 prerequisite widening).
func publicationMemberDefs(ctx context.Context, db *sql.DB, name string) (map[string]publicationMemberAttrs, error) {
	version, err := serverVersionNum(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("postgres: publication member definitions: %w", err)
	}
	return publicationMemberDefsAt(ctx, db, name, version)
}

// publicationMemberDefsAt is [publicationMemberDefs] with the server
// version already in hand, over any [querier] — split out so the
// ADR-0176 rescope probe can re-read the definition INSIDE its
// transaction (*sql.Tx) and compare against the pre-ALTER read.
func publicationMemberDefsAt(ctx context.Context, q querier, name string, version int) (map[string]publicationMemberAttrs, error) {
	memberQuery := `
		SELECT n.nspname, c.relname, '' AS qual, false AS has_column_list
		FROM   pg_publication p
		JOIN   pg_publication_rel pr ON pr.prpubid = p.oid
		JOIN   pg_class c            ON c.oid     = pr.prrelid
		JOIN   pg_namespace n        ON n.oid     = c.relnamespace
		WHERE  p.pubname = $1`
	if version >= pgVersionPublicationAttrs {
		memberQuery = `
		SELECT n.nspname, c.relname,
		       COALESCE(pg_get_expr(pr.prqual, pr.prrelid), ''),
		       pr.prattrs IS NOT NULL
		FROM   pg_publication p
		JOIN   pg_publication_rel pr ON pr.prpubid = p.oid
		JOIN   pg_class c            ON c.oid     = pr.prrelid
		JOIN   pg_namespace n        ON n.oid     = c.relnamespace
		WHERE  p.pubname = $1`
	}
	rows, err := q.QueryContext(ctx, memberQuery, name)
	if err != nil {
		return nil, fmt.Errorf("postgres: list publication member definitions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	defs := make(map[string]publicationMemberAttrs)
	for rows.Next() {
		var (
			nsp, rel string
			attrs    publicationMemberAttrs
		)
		if err := rows.Scan(&nsp, &rel, &attrs.Qual, &attrs.HasColumnList); err != nil {
			return nil, fmt.Errorf("postgres: scan publication member definition: %w", err)
		}
		defs[nsp+"."+rel] = attrs
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: list publication member definitions: %w", err)
	}
	return defs, nil
}

// pgVersionPublicationAttrs is the first server version whose
// pg_publication_rel carries per-table attributes (prqual row filters
// + prattrs column lists) — PG 15. Below it the catalog columns don't
// exist and the guard's definition comparison degrades to the member
// SET, which is complete there (no attributes can exist to differ).
const pgVersionPublicationAttrs = 150000

// publicationMemberSet returns the publication's current member tables
// keyed "schema.table". Shared by the ADR-0175 narrowing guard and by
// [addTablesToPublication]'s duplicate skip so both agree on the key
// shape.
func publicationMemberSet(ctx context.Context, db *sql.DB, name string) (map[string]struct{}, error) {
	const memberQuery = `
		SELECT n.nspname, c.relname
		FROM   pg_publication p
		JOIN   pg_publication_rel pr ON pr.prpubid = p.oid
		JOIN   pg_class c            ON c.oid     = pr.prrelid
		JOIN   pg_namespace n        ON n.oid     = c.relnamespace
		WHERE  p.pubname = $1`
	rows, err := db.QueryContext(ctx, memberQuery, name)
	if err != nil {
		return nil, fmt.Errorf("postgres: list publication members: %w", err)
	}
	defer func() { _ = rows.Close() }()

	members := make(map[string]struct{})
	for rows.Next() {
		var nsp, rel string
		if err := rows.Scan(&nsp, &rel); err != nil {
			return nil, fmt.Errorf("postgres: scan publication member: %w", err)
		}
		members[nsp+"."+rel] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: list publication members: %w", err)
	}
	return members, nil
}

// otherActiveSluiceSlots returns the ACTIVE sluice-convention
// replication slots other than excludeSlot — the signal that some
// other stream holds a claim on this source.
//
// EXISTENCE, not activity, is the conflict signal (the ADR-0175
// residual closure, 2026-07-23). A replication slot is the durable,
// source-side object a stream owns for exactly as long as it intends
// to resume — a slot that is merely INACTIVE (stream stopped
// mid-migration, crashed, or between retry attempts) still names a
// stream that will warm-resume expecting its scope, and "momentarily
// inactive" was precisely the window the original activity predicate
// left open. A stream with NO slot must cold-start, and cold start
// re-asserts scope under this same guard. The cost is a conservative
// refusal against a genuinely abandoned slot — but an abandoned slot
// pins WAL and deserves the operator's attention anyway, and the
// refusal message names both escapes.
//
// Still a source-side signal by design: sluice_cdc_state lives on the
// TARGET, and concurrent waves may have entirely different targets, so
// no single target's control table is authoritative about who is
// reading the source.
//
// Each returned entry is rendered "name (active)" / "name (inactive)"
// so the refusal tells the operator which conflicting streams are
// running right now vs holding a resumable position.
func otherSluiceSlots(ctx context.Context, db *sql.DB, excludeSlot string) ([]string, error) {
	const slotQuery = `
		SELECT slot_name, active
		FROM   pg_replication_slots
		WHERE  slot_name LIKE 'sluice\_%'
		  AND  slot_name <> $1
		ORDER  BY slot_name`
	rows, err := db.QueryContext(ctx, slotQuery, excludeSlot)
	if err != nil {
		return nil, fmt.Errorf("postgres: list sluice replication slots: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var slots []string
	for rows.Next() {
		var (
			s      string
			active bool
		)
		if err := rows.Scan(&s, &active); err != nil {
			return nil, fmt.Errorf("postgres: scan replication slot: %w", err)
		}
		if active {
			slots = append(slots, s+" (active)")
		} else {
			slots = append(slots, s+" (inactive — holds a resumable position; drop it with SELECT pg_drop_replication_slot('"+s+"') if the stream is truly abandoned)")
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: list sluice replication slots: %w", err)
	}
	return slots, nil
}

// guardPublicationNarrowing implements the ADR-0175 refusal. It runs
// BEFORE any mutation, so a refused attempt leaves the publication —
// and every stream reading through it — completely untouched.
//
// incoming is the definition the caller is about to scope the
// publication to — "schema.table" keys mapped to their ADR-0176 raw
// row-filter text ("" = bare, [qualifiedTableQuals]); a nil map means
// "everything is being removed" (the FOR ALL TABLES demotion, where
// the current members aren't enumerable from pg_publication_rel).
//
// "Narrowing" is defined over the whole publication DEFINITION, not
// the table set (the ADR-0176 widening): a rescope conflicts when it
// removes a member OR when a surviving member's per-table attributes
// (PG 15+ row filter / column list) would change. No removal and no
// attribute change ⇒ no conflict, so widening and equal-scope rescopes
// of attribute-free publications never reach the slot probe. That
// keeps the ADR-0122 fleet shape (identical scopes) and
// `schema add-table` (additive) on their existing path at near-zero
// added cost (one server_version_num read).
//
// The returned ambiguous list names surviving members where BOTH the
// current definition and the incoming one carry a row filter — a
// change only PG's own normalization can rule in or out, which the
// caller resolves via [rescopeRowFilteredPublication]. It is non-empty
// only when nothing else conflicts (a provable conflict refuses here;
// a rescope this stream is entitled to — no other slots — proceeds
// unprobed).
func guardPublicationNarrowing(
	ctx context.Context,
	db *sql.DB,
	name string,
	incoming map[string]string,
	excludeSlot string,
) (ambiguous []string, err error) {
	var removed, attrChanged []string
	if incoming == nil {
		// FOR ALL TABLES → scoped. Every table not in the new list
		// loses scope; we can't enumerate them (a FOR ALL TABLES
		// publication has no pg_publication_rel rows), so treat the
		// demotion itself as the removal.
		removed = []string{"(every table — the publication is currently FOR ALL TABLES)"}
	} else {
		defs, err := publicationMemberDefs(ctx, db, name)
		if err != nil {
			return nil, err
		}
		removed = removedFromPublication(memberKeySet(defs), incomingKeySet(incoming))
		attrChanged, ambiguous = attributeChangedSurvivors(defs, incoming)
		if len(removed) == 0 && len(attrChanged) == 0 {
			// Widening or equal, with no provable attribute change — the
			// only open question (if any) is the ambiguous filter-vs-filter
			// comparison, which the caller's probe settles.
			return ambiguous, nil
		}
	}

	slots, err := otherSluiceSlots(ctx, db, excludeSlot)
	if err != nil {
		return nil, err
	}
	if len(slots) == 0 {
		// No other stream holds a claim; the rescope is the operator's
		// own. That entitlement covers the ambiguous members too — no
		// probe needed.
		return nil, nil
	}

	// Compose what the rescope would change: removed members, changed
	// per-table attributes (the ADR-0176 widening), or both.
	var changes []string
	if len(removed) > 0 {
		changes = append(changes, "REMOVE "+strings.Join(removed, ", ")+" from the stream")
	}
	if len(attrChanged) > 0 {
		changes = append(changes, "silently CHANGE per-table attributes: "+strings.Join(attrChanged, ", "))
	}
	return nil, publicationScopeConflictRefusal(name, changes, slots)
}

// publicationScopeConflictRefusal builds the ADR-0175/0176 coded
// refusal, shared by the pre-mutation guard and the transactional
// rescope probe so both refusal paths speak with one voice.
func publicationScopeConflictRefusal(name string, changes, slots []string) error {
	return sluicecode.Wrap(
		sluicecode.CodeCDCPublicationScopeConflict,
		"pass --publication-name to give this stream its own publication, stop the other stream first, or drop its slot if abandoned",
		fmt.Errorf(
			"postgres: refusing to rescope publication %q: it would %s, "+
				"but %d other sluice replication slot(s) exist on this source (%s). "+
				"A slot — active or not — is a stream that is reading through this publication or "+
				"holds a resumable position expecting its scope (including any per-table row filter "+
				"or column list); nothing binds a publication to a "+
				"slot, so this rescope would leave those stream(s) advancing normally (or resuming "+
				"later) while receiving the WRONG change set — a silent divergence with a "+
				"healthy-looking sync status. "+
				"Recovery: give each concurrent stream over this source its own publication "+
				"(--publication-name NAME, per stream), drain the other stream first "+
				"(sluice sync stop --wait --stream-id ID), or — for a truly abandoned stream — "+
				"drop its slot. If THIS stream is rescoping its own already-isolated publication "+
				"(e.g. a changed --where with --restart-from-scratch), a brand-new "+
				"--publication-name for this stream always proceeds without touching any peer "+
				"(audit 2026-07-23 ARCH-2). See ADR-0175",
			name,
			strings.Join(changes, " and "),
			len(slots),
			strings.Join(slots, ", "),
		),
	)
}

// verifyPublicationUnchanged is the audit 2026-07-23 D0-7 post-slot
// re-verification: it proves the publication still matches the
// definition THIS stream ensured, using PG's own normalization as the
// judge, and mutates nothing.
//
// Why it exists: cold start ensures the publication BEFORE its own
// replication slot does — so two streams cold-starting simultaneously
// under one publication name each pass the ADR-0175 scope guard (the
// peer has no slot yet to conflict with) and can swap definitions in
// the ensure→slot-creation window. Once a slot exists, any later
// rescope by a peer is refused by the guard; this call closes exactly
// the pre-slot window by re-reading the catalog AFTER the slot is
// created and comparing against what this stream would ensure.
//
// Mechanics mirror [rescopeRowFilteredPublication]'s transactional
// probe (raw `--where` text cannot be text-compared to the catalog's
// pg_get_expr rendering, so only PG can decide equality): read the
// current member definitions, re-issue this stream's own SET TABLE
// inside a transaction, re-read, and ALWAYS roll back. An empty diff
// means the catalog still holds this stream's definition; a non-empty
// diff means a concurrent cold start redefined it — the loud
// SCOPE-CONFLICT refusal, before any data moves.
func verifyPublicationUnchanged(ctx context.Context, db *sql.DB, name, schema string, tables []string, filters map[string]string) error {
	version, err := serverVersionNum(ctx, db)
	if err != nil {
		return fmt.Errorf("postgres: post-slot publication re-verification: %w", err)
	}
	if version < pgVersionPublicationAttrs {
		// Same gate as [publicationRowFiltersForVersion], without re-logging
		// — the ensure already announced the drop at INFO.
		filters = nil
	}
	current, err := publicationMemberDefsAt(ctx, db, name, version)
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("postgres: begin post-slot publication re-verification probe: %w", err)
	}
	// The probe NEVER commits: verification must not mutate. On the
	// happy path the rolled-back ALTER is byte-equivalent to the catalog
	// state anyway; on the conflict path the rollback preserves the
	// no-mutation-on-refusal guarantee the guard family gives.
	defer func() { _ = tx.Rollback() }()
	alterQuery := fmt.Sprintf(`ALTER PUBLICATION %s SET TABLE %s`,
		quoteIdent(name), formatPublicationTableList(schema, tables, filters))
	if _, err := tx.ExecContext(ctx, alterQuery); err != nil {
		// e.g. the publication was recreated FOR ALL TABLES (SET TABLE
		// refuses there), or dropped outright — either way it no longer
		// matches what this stream ensured moments ago. Loud.
		return fmt.Errorf(
			"postgres: post-slot publication re-verification: probe rescope of %q failed — the publication was redefined between this stream's ensure and its slot creation: %w",
			name, err,
		)
	}
	ensured, err := publicationMemberDefsAt(ctx, tx, name, version)
	if err != nil {
		return err
	}
	if diff := verifyMemberDiff(current, ensured); len(diff) > 0 {
		return publicationPostSlotConflictRefusal(name, diff)
	}
	return nil
}

// verifyMemberDiff returns the sorted differences between the CATALOG
// definition (current) and the definition THIS stream ensured (ensured,
// read back through PG's own normalization so text equality is exact),
// rendered from the verifying stream's perspective. Empty means the
// catalog still holds this stream's definition.
//
// Catalog-side predicate text is never echoed (audit 2026-07-23 SEC-1
// — in the D0-7 shape the current filter is by construction ANOTHER
// stream's): entries name the member and the kind of divergence only.
//
// Pure, like [removedFromPublication], so the decision that gates a
// refusal is testable without a database.
func verifyMemberDiff(current, ensured map[string]publicationMemberAttrs) []string {
	var diff []string
	for m, cur := range current {
		ens, ok := ensured[m]
		switch {
		case !ok:
			diff = append(diff, m+" (present in the publication but not in this stream's definition)")
		case cur != ens:
			diff = append(diff, m+" (its row filter or column list differs from what this stream ensured)")
		}
	}
	for m := range ensured {
		if _, ok := current[m]; !ok {
			diff = append(diff, m+" (removed from the publication)")
		}
	}
	sort.Strings(diff)
	return diff
}

// publicationPostSlotConflictRefusal is the D0-7 flavor of the
// ADR-0175/0176 coded refusal: the concurrent-first-boot window closed
// after the fact, before any data moved.
func publicationPostSlotConflictRefusal(name string, diff []string) error {
	return sluicecode.Wrap(
		sluicecode.CodeCDCPublicationScopeConflict,
		"give each concurrently-starting stream over this source its own --publication-name, then restart this stream",
		fmt.Errorf(
			"postgres: cold start refused after slot creation: publication %q no longer matches the definition this "+
				"stream ensured at the start of this cold start (%s). A concurrently cold-starting stream sharing this "+
				"publication name redefined it inside the ensure→slot-creation window — before either stream's slot "+
				"existed, so the scope-conflict guard had no slot to refuse against (audit 2026-07-23 D0-7). Streaming "+
				"on would deliver the WRONG change set through the peer's definition; no data has moved. Recovery: give "+
				"each concurrent stream over this source its own publication (--publication-name NAME, per stream) and "+
				"restart. See ADR-0175",
			name,
			strings.Join(diff, "; "),
		),
	)
}

// ensureAllTablesPublication guarantees the named publication exists as
// FOR ALL TABLES (ADR-0075 Phase 2b multi-schema CDC). A PG logical slot
// is DATABASE-WIDE, so a multi-schema fan-out streams every selected
// schema through one slot + one publication; the reader-side inScope
// filter ([CDCReader.SetCDCDatabaseScope]) is the selection boundary, not
// the publication. FOR ALL TABLES works on every supported PG version
// (the PG15+ FOR TABLES IN SCHEMA trim is a later WAL-volume optimisation,
// not a correctness requirement).
//
// Three cases, mirroring [ensurePublication]'s create/recreate logic but
// in the opposite direction (toward FOR ALL TABLES, not toward a scoped
// list):
//
//   - missing → CREATE PUBLICATION … FOR ALL TABLES.
//   - exists and already FOR ALL TABLES → no-op (idempotent).
//   - exists but SCOPED (a leftover FOR TABLE publication from a prior
//     single-schema run) → DROP + recreate FOR ALL TABLES. ALTER cannot
//     promote a FOR TABLE publication to FOR ALL TABLES, so a drop is
//     required; it is safe because the publication is metadata only —
//     slots reference WAL by LSN, not by a publication-name binding, and
//     this opener creates a fresh slot in the same call.
func ensureAllTablesPublication(ctx context.Context, db *sql.DB, name string) error {
	var exists, allTables bool
	const checkQuery = "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1), " +
		"COALESCE((SELECT puballtables FROM pg_publication WHERE pubname = $1), false)"
	if err := db.QueryRowContext(ctx, checkQuery, name).Scan(&exists, &allTables); err != nil {
		return fmt.Errorf("postgres: check publication: %w", err)
	}
	if exists && allTables {
		return nil
	}
	if exists {
		dropQuery := fmt.Sprintf(`DROP PUBLICATION %s`, quoteIdent(name))
		if _, err := db.ExecContext(ctx, dropQuery); err != nil {
			return fmt.Errorf("postgres: drop scoped publication %q for multi-schema FOR ALL TABLES: %w", name, err)
		}
	}
	createQuery := fmt.Sprintf(`CREATE PUBLICATION %s FOR ALL TABLES`, quoteIdent(name))
	if _, err := db.ExecContext(ctx, createQuery); err != nil {
		// A concurrent ensurer winning the create race (shared-source fleet
		// cold-start, ADR-0122) surfaces as a unique-violation on
		// pg_publication. Benign: every racer here is creating the SAME
		// FOR ALL TABLES publication, so an existing one is exactly the goal.
		if isDuplicatePublication(err) {
			return nil
		}
		return fmt.Errorf("postgres: create FOR ALL TABLES publication %q: %w", name, err)
	}
	return nil
}

// isDuplicatePublication reports whether err is the benign
// "publication already exists" shape — either a concurrent CREATE losing
// the race (23505 unique_violation on pg_publication's pubname index, the
// shape two sessions hit when both pass a check-then-create existence
// check) or the single-session 42710 duplicate_object. Used to make
// publication creation idempotent for shared-source fleets where several
// syncs ensure the same publication concurrently at cold-start.
func isDuplicatePublication(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505" || pgErr.Code == "42710"
	}
	return false
}

// publicationIsEmpty reports whether the named publication has no
// member tables (pg_publication_rel) and no schema-level memberships
// (pg_publication_namespace, FOR TABLES IN SCHEMA — PG 15+). The
// caller must have already established that the publication exists and
// is not FOR ALL TABLES; an empty publication in that state can never
// emit a pgoutput row, so streaming from it stalls the slot silently.
//
// The schema-membership catalog only exists on PG 15+. We probe for it
// with to_regclass and skip its count on older servers (where FOR
// TABLES IN SCHEMA isn't a feature) so the query stays valid there
// rather than failing to parse on a missing relation.
func publicationIsEmpty(ctx context.Context, db *sql.DB, name string) (bool, error) {
	const relCountQuery = `SELECT count(*) FROM pg_publication_rel pr
		JOIN pg_publication p ON p.oid = pr.prpubid
		WHERE p.pubname = $1`
	var relCount int
	if err := db.QueryRowContext(ctx, relCountQuery, name).Scan(&relCount); err != nil {
		return false, fmt.Errorf("postgres: count tables in publication %q: %w", name, err)
	}
	if relCount > 0 {
		return false, nil
	}

	var hasSchemaCatalog bool
	if err := db.QueryRowContext(
		ctx,
		`SELECT to_regclass('pg_catalog.pg_publication_namespace') IS NOT NULL`,
	).Scan(&hasSchemaCatalog); err != nil {
		return false, fmt.Errorf("postgres: probe pg_publication_namespace: %w", err)
	}
	if !hasSchemaCatalog {
		// PG < 15: no schema-level publications possible, so zero
		// member tables means genuinely empty.
		return true, nil
	}

	const nsCountQuery = `SELECT count(*) FROM pg_publication_namespace pn
		JOIN pg_publication p ON p.oid = pn.pnpubid
		WHERE p.pubname = $1`
	var nsCount int
	if err := db.QueryRowContext(ctx, nsCountQuery, name).Scan(&nsCount); err != nil {
		return false, fmt.Errorf("postgres: count schemas in publication %q: %w", name, err)
	}
	return nsCount == 0, nil
}

// addTablesToPublication issues `ALTER PUBLICATION ... ADD TABLE
// <list>` so the named tables join the publication scope without
// disturbing the existing scope. Used by the mid-stream add-table
// flow where ensurePublication's `SET TABLE` semantics would replace
// the entire list and silently drop tables that were already in
// scope.
//
// Refuses (with a clear error) when the publication is FOR ALL
// TABLES — adding a specific table to a FOR ALL TABLES publication
// is meaningless and almost always indicates an operator
// misconfiguration. The publication must already exist.
//
// Tables already in the publication are skipped so the call is
// idempotent on a partial-add re-run. Schema-qualifies each table.
func addTablesToPublication(ctx context.Context, db *sql.DB, name, schema string, tables []string) error {
	if len(tables) == 0 {
		return nil
	}
	var exists, allTables bool
	const checkQuery = "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1), " +
		"COALESCE((SELECT puballtables FROM pg_publication WHERE pubname = $1), false)"
	if err := db.QueryRowContext(ctx, checkQuery, name).Scan(&exists, &allTables); err != nil {
		return fmt.Errorf("postgres: check publication: %w", err)
	}
	if !exists {
		return fmt.Errorf("postgres: add tables to publication %q: publication does not exist (mid-stream add-table requires an active stream's publication)", name)
	}
	if allTables {
		return fmt.Errorf("postgres: add tables to publication %q: publication is FOR ALL TABLES; nothing to add (the new table is already implicitly in scope)", name)
	}

	// Look up the current member set so we can skip duplicates and
	// emit a clean ALTER even if some of the supplied tables are
	// already in the publication. Shares [publicationMemberSet] with
	// the ADR-0175 narrowing guard so both agree on the key shape.
	existing, err := publicationMemberSet(ctx, db, name)
	if err != nil {
		return err
	}

	toAdd := make([]string, 0, len(tables))
	for _, t := range tables {
		key := schema + "." + t
		if schema == "" {
			// Match the unqualified shape used by formatPublicationTableList
			// for empty-schema callers.
			key = "." + t
		}
		if _, ok := existing[key]; ok {
			continue
		}
		toAdd = append(toAdd, t)
	}
	if len(toAdd) == 0 {
		return nil
	}

	// nil filters: the mid-stream add-table flow adds tables BARE. A
	// `--where` for a live-added table is not pushed down until the
	// stream's next cold start re-asserts the publication — the
	// client-side evaluator (always on) keeps it correct in the interim
	// (ADR-0176 §2).
	alterQuery := fmt.Sprintf(`ALTER PUBLICATION %s ADD TABLE %s`,
		quoteIdent(name), formatPublicationTableList(schema, toAdd, nil))
	if _, err := db.ExecContext(ctx, alterQuery); err != nil {
		return fmt.Errorf("postgres: alter publication %q add tables: %w", name, err)
	}
	return nil
}

// formatPublicationTableList renders a comma-separated list of
// schema-qualified, double-quoted table identifiers for use after
// `FOR TABLE` / `SET TABLE`. filters is the ADR-0176 row-filter map
// (bare source table name → raw predicate text): a table with an entry
// renders `schema."table" WHERE (<predicate>)` via the same
// single-sourced renderer the snapshot SELECT uses
// ([rowFilterWhereSQL]), so the publication filter and the snapshot
// push-down can never drift on predicate text. nil/absent renders the
// bare identifier — byte-identical to the pre-ADR-0176 shape.
func formatPublicationTableList(schema string, tables []string, filters map[string]string) string {
	parts := make([]string, len(tables))
	for i, t := range tables {
		if schema == "" {
			parts[i] = quoteIdent(t)
		} else {
			parts[i] = quoteIdent(schema) + "." + quoteIdent(t)
		}
		parts[i] += rowFilterWhereSQL(filters[t])
	}
	return strings.Join(parts, ", ")
}

// dropOwnPublicationIfPerStream drops the stream's own PER-STREAM
// publication in the same teardown that drops its replication slot
// (ADR-0176 prerequisite chunk, cleanup parity): a per-stream
// publication shares the slot's lifecycle, so whatever path discards
// the slot must discard the publication too or dead publications
// accumulate on the source. Two deliberate limits:
//
//   - The shared default (`sluice_pub`) is NEVER dropped — every
//     legacy deployment reads through it, and it may serve other
//     streams. Empty (engine default) is likewise a no-op.
//   - IF EXISTS tolerates absence: an abandoned cold start can refuse
//     before its publication was ever created, or a retry can find a
//     prior attempt's cleanup already removed it.
//
// The known edge this accepts (matching the slot-drop posture): an
// operator who deliberately pointed TWO streams at one custom
// publication via --publication-name loses it when either stream's
// pre-anchor teardown fires — the peer then fails LOUDLY
// ("publication does not exist") and its next cold start recreates
// scope, never silently diverges. Per-stream names (the default this
// chunk introduces for filtered streams) are 1:1 by construction and
// don't hit this.
//
// Best-effort like the slot drop: the caller logs/surfaces a failure
// without letting it mask the primary error.
func dropOwnPublicationIfPerStream(ctx context.Context, db *sql.DB, name string) error {
	if name == "" || name == defaultPublication {
		return nil
	}
	if _, err := db.ExecContext(ctx, "DROP PUBLICATION IF EXISTS "+quoteIdent(name)); err != nil {
		return fmt.Errorf("postgres: drop per-stream publication %q: %w", name, err)
	}
	return nil
}
