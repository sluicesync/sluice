// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// Placing sluice's own control tables on a sharded PlanetScale Neki target.
//
// # The defect this closes (NEKI-NK306, the control-table half)
//
// sluice's control tables — the CDC position, the migrate breadcrumbs, the
// skipped-table ledger and their siblings — carry no shard key, because on
// every other engine they have no reason to. On a SHARDED Neki the
// database's default shard group covers `public`, so every INSERT into them
// is refused for want of a routing column they do not have:
//
//	ERROR: shard-key column "tenant_id" of primary index 0 is required
//	but missing from INSERT (SQLSTATE NK306)
//
// Measured 2026-09-10 and bisected 2026-09-14 on live 2- and 3-shard
// clusters: it blocks BOTH CDC apply lanes (the serial lane reaches its
// position write — meaning its data INSERT succeeded — and dies there), and
// it turned a 40-row keyless table into 80 rows across a `--resume`, because
// the breadcrumb that would have said "already copied" could not be written.
//
// # Why placement, and why the AUTHORITATIVE group
//
// Neki's topology names a `shard_group` PER TABLE in an operator-declared
// document, and `CREATE TABLE` does not enrol anything into it — so a table
// sluice creates is absent from the document by construction and falls to
// the sharded default. sluice cannot fix this by creating the table
// differently. Three candidates were probed on a live cluster (2026-09-14,
// nekiverify_control_schema_probe_test.go), and two were rejected by
// measurement:
//
//   - a separate SCHEMA does not escape the default group — refused
//     identically;
//   - a CONSTANT shard-key column works through the full lifecycle, but
//     makes control-table DDL depend on the target's routing column — a
//     first for any engine — and is kept on file only as the fallback.
//
// What works, and matches PlanetScale's own guidance that the authoritative
// shard group "holds unsharded data" and is "sized for metadata, catalog
// work, and sequences": assign the control table to the AUTHORITATIVE group.
// A table placed there takes a shard-key-less INSERT and reads back unpinned,
// and sluice's own role could write the topology. No DDL shape change, no
// target-dependent column, no operator prerequisite.
//
// # What this helper deliberately does and does not do
//
//   - It is a NO-OP off Neki. Every other PostgreSQL target is byte-identical
//     to before.
//   - It writes the topology ONLY when a control table would otherwise be
//     routed by a shard index. An unsharded Neki database (no shard index
//     resolves for the table) is left alone — a cluster that does not need
//     the placement never sees a topology revision from sluice.
//   - It compares before it writes. EnsureControlTable runs on every start,
//     and a start must not mint a topology revision when the placement
//     already holds.
//   - A topology READ failure is a WARN, not a refusal. The failure mode of
//     skipping placement is the same loud NK306 this exists to prevent,
//     never silent loss, and refusing here would take down an unsharded
//     cluster whose role merely lacks SELECT on the topology.
//   - A topology WRITE failure IS a refusal, coded, with the exact placement
//     the operator has to make themselves. This is the governance door: a
//     cluster whose topology writes are restricted denies sluice's role, and
//     sluice says precisely what to ask for rather than dying later on NK306.
//
// The enumeration of the tables that reach this helper is
// [TestEveryPostgresControlTableIsPlacedOnNeki] — derived from the package's
// own table-name constants, so a control table added without a placement
// call fails the build rather than the first sharded migration.

// nekiTopologyWriteAttempts bounds the read-modify-write retry when two
// sluice processes enrol concurrently. Each retry re-reads and re-plans, so
// a process that loses the race normally finds the other's placement already
// in the document and writes nothing.
const nekiTopologyWriteAttempts = 4

// nekiSQLStateInsufficientPrivilege is PostgreSQL's insufficient_privilege,
// which is what a role that may not call set_data_topology receives.
const nekiSQLStateInsufficientPrivilege = "42501"

// ensureNekiControlTablePlacement places the named control tables (already
// created in `schema`) in the target's authoritative shard group when the
// target is a PlanetScale Neki whose topology would otherwise route them by a
// shard index. See the file comment for everything it deliberately does not
// do.
func ensureNekiControlTablePlacement(ctx context.Context, db *sql.DB, isNeki bool, serverKey, schema string, tables []string) error {
	if !isNeki || len(tables) == 0 {
		return nil
	}
	if schema == "" {
		schema = "public"
	}

	var (
		lastConflict error
		lastSnap     *nekiTopologyForWrite
		lastPlan     nekiControlPlacementPlan
	)
	for attempt := 1; attempt <= nekiTopologyWriteAttempts; attempt++ {
		snap, err := readNekiTopologyForWrite(ctx, db)
		if err == nil {
			// A document Go cannot parse is a READ failure in every sense
			// that matters, and gets the read-failure policy below rather
			// than a refusal — see finding 4 of the pre-tag review.
			if !utf8.Valid(snap.raw) {
				err = errors.New("postgres: Neki data topology is not valid UTF-8")
			} else if !json.Valid(snap.raw) {
				err = errors.New("postgres: Neki data topology is not valid JSON")
			}
		}
		if err != nil {
			// Not a verdict. See the file comment: the consequence of
			// skipping is loud, and refusing would break clusters that do
			// not need the placement.
			slog.WarnContext(ctx, "could not read the PlanetScale Neki data topology, so sluice's control tables "+
				"were not placed in the authoritative shard group; on a SHARDED database their writes will be "+
				"refused with SQLSTATE NK306 (an unsharded database is unaffected)",
				slog.String("schema", schema), slog.Any("tables", tables), slog.Any("err", err))
			return nil
		}
		plan, err := planNekiControlPlacement(snap.raw, snap.database, schema, tables)
		if err != nil {
			return err
		}
		lastSnap, lastPlan = snap, plan
		if !plan.changed {
			if attempt > 1 {
				slog.InfoContext(ctx, "sluice's control tables were placed in the authoritative shard group by a "+
					"concurrent sluice process; nothing to write", slog.String("group", plan.group))
			}
			return nil
		}

		rev, conflict, err := writeNekiTopology(ctx, db, plan.doc, snap)
		if err != nil {
			return refuseNekiControlPlacement(snap.database, schema, plan, err)
		}
		if conflict {
			// Somebody else wrote the topology between our read and our
			// write. Re-read and re-plan; the loop bounds it.
			lastConflict = fmt.Errorf("topology revision %d was superseded before sluice's write landed", snap.revision)
			continue
		}
		invalidateNekiTopologyMemo(serverKey)
		waitForNekiTopology(ctx, db, rev)
		slog.InfoContext(ctx, "placed sluice's control tables in the PlanetScale Neki authoritative shard group "+
			"(they carry no shard key; the platform's guidance is that this group holds unsharded metadata)",
			slog.String("group", plan.group), slog.String("schema", schema),
			slog.Any("tables", plan.placed), slog.Int64("topology_revision", rev))
		return nil
	}
	// Exhausted. The refusal must still name the tables, database and group
	// the operator has to place, so it is built from the last plan rather
	// than from nothing.
	return refuseNekiControlPlacement(lastSnap.database, schema, lastPlan,
		fmt.Errorf("gave up after %d attempts, each finding the topology revision superseded by another "+
			"writer before sluice's placement landed: %w", nekiTopologyWriteAttempts, lastConflict))
}

// nekiTopologyForWrite is one fresh read of the topology, un-memoised: a
// write is about to be based on it, so a cached copy is exactly the wrong
// thing to plan from.
type nekiTopologyForWrite struct {
	raw      []byte
	database string
	// revision is the stored document's revision when the router exposes
	// it, and hasRevision says whether it does. The write passes it as
	// `expected_revision` so a concurrent editor's change is refused rather
	// than silently overwritten. Without it the write is last-writer-wins,
	// which loses the loser's placement whenever two writers place
	// DIFFERENT table sets — a `migrate` and a `sync` starting together
	// go through different doors — or an operator edits concurrently. The
	// loss is loud (the next write to the unplaced table is NK306) and
	// heals on restart; it is not silent. Every measured router exposes
	// the revision, so this is the fallback, not the path.
	revision    int64
	hasRevision bool
}

// readNekiTopologyForWrite reads the current document, the database name it
// is keyed by, and — where the router exposes one — the stored revision.
func readNekiTopologyForWrite(ctx context.Context, db *sql.DB) (*nekiTopologyForWrite, error) {
	var raw, database string
	if err := db.QueryRowContext(ctx,
		"SELECT __neki.get_data_topology()::text, pg_catalog.current_database()").Scan(&raw, &database); err != nil {
		return nil, fmt.Errorf("postgres: read Neki data topology: %w", err)
	}
	snap := &nekiTopologyForWrite{raw: []byte(raw), database: database}
	rev, ok, err := readNekiTopologyRevision(ctx, db)
	if err != nil {
		return nil, err
	}
	snap.revision, snap.hasRevision = rev, ok
	return snap, nil
}

// nekiControlPlacementPlan is the DECISION, split out from the I/O so it can
// be graded exhaustively against measured documents without a cluster.
type nekiControlPlacementPlan struct {
	// changed reports whether the topology needs writing at all.
	changed bool
	// doc is the document to write when changed.
	doc []byte
	// placed are the tables the plan assigns; a subset of the input.
	placed []string
	// group is the authoritative shard group they are assigned to.
	group string
}

// planNekiControlPlacement decides which of `tables` must be assigned to the
// authoritative shard group and produces the document that does it.
//
// A table needs placing exactly when a shard index currently routes it —
// [nekiTopology.shardKeyFor] resolving to a non-empty column list — because
// that is the condition under which the router demands a shard key on
// INSERT. A table nothing routes is left where it is, so an unsharded
// database never has its topology touched.
//
// The document is edited as generic JSON with numbers preserved
// (`UseNumber`, so an int64 beyond 2^53 and a float's spelling survive) and
// with HTML escaping off, so a field this code does not understand keeps its
// VALUE across the round trip; the platform is in preview and a field sluice
// has never heard of must survive a sluice write. Key order and string escape
// spellings are not preserved — those are not values. Two shapes ARE lossy
// and are stated rather than hidden: invalid UTF-8 (refused before the edit,
// by the caller's validity check) and duplicate keys within one object
// (undetectable after decode; the last one wins). UNVERIFIED PREMISE: the
// router emits neither — every live read so far has cast the document to
// jsonb successfully, which forbids both.
func planNekiControlPlacement(raw []byte, database, schema string, tables []string) (nekiControlPlacementPlan, error) {
	var topo nekiTopology
	if err := json.Unmarshal(raw, &topo); err != nil {
		return nekiControlPlacementPlan{}, fmt.Errorf("postgres: parse Neki data topology: %w", err)
	}

	var need []string
	for _, t := range tables {
		cols, _ := topo.shardKeyFor(database, schema, t)
		if len(cols) == 0 {
			continue
		}
		need = append(need, t)
	}
	if len(need) == 0 {
		return nekiControlPlacementPlan{}, nil
	}
	sort.Strings(need)

	auth := topo.AuthoritativeShardGroup
	if auth == "" {
		return nekiControlPlacementPlan{}, refuseNekiControlPlacement(database, schema,
			nekiControlPlacementPlan{placed: need},
			errors.New("the data topology names no authoritative_shard_group, so there is no unsharded group "+
				"to place them in"))
	}
	// The authoritative group must itself be unsharded — no default shard
	// index — or the placement changes nothing: the table would still be
	// routed, and still refused. The platform's guidance is that this group
	// holds unsharded data; a topology that gives it a shard index is
	// deviating from that, and sluice says so rather than writing a
	// placement it can predict will not work.
	if cols, _ := topo.columnsForGroup(auth, ""); len(cols) > 0 {
		return nekiControlPlacementPlan{}, refuseNekiControlPlacement(database, schema,
			nekiControlPlacementPlan{placed: need, group: auth},
			fmt.Errorf("the authoritative shard group %q declares a default shard index on (%s), so a table "+
				"placed there would still need a shard key on INSERT", auth, strings.Join(cols, ", ")))
	}
	// A control table the operator has already pinned to a shard index is
	// a declaration sluice will not overwrite; it cannot be honoured
	// either, so it is named.
	if db, ok := topo.Databases[database]; ok {
		if sc, ok := db.Schemas[schema]; ok {
			for _, t := range need {
				if tb, ok := sc.Tables[t]; ok && tb.ShardIndex != "" {
					return nekiControlPlacementPlan{}, refuseNekiControlPlacement(database, schema,
						nekiControlPlacementPlan{placed: need, group: auth},
						fmt.Errorf("the topology pins sluice's control table %q to shard index %q; sluice will not "+
							"remove an explicit shard_index (it does re-point a shard_group), and a routed control "+
							"table cannot take sluice's shard-key-less writes", t, tb.ShardIndex))
				}
			}
		}
	}

	doc, err := assignTablesToShardGroup(raw, database, schema, need, auth)
	if err != nil {
		return nekiControlPlacementPlan{}, err
	}
	return nekiControlPlacementPlan{changed: true, doc: doc, placed: need, group: auth}, nil
}

// assignTablesToShardGroup returns `raw` with
// databases.<database>.schemas.<schema>.tables.<t>.shard_group = group for
// every t, creating intermediate objects as needed and leaving every other
// byte of meaning intact.
func assignTablesToShardGroup(raw []byte, database, schema string, tables []string, group string) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var root map[string]any
	if err := dec.Decode(&root); err != nil {
		return nil, fmt.Errorf("postgres: parse Neki data topology for placement: %w", err)
	}
	if root == nil {
		root = map[string]any{}
	}
	tablesObj, err := descendObject(root, "databases", database, "schemas", schema, "tables")
	if err != nil {
		return nil, err
	}
	for _, t := range tables {
		entry, ok := tablesObj[t].(map[string]any)
		if !ok {
			if _, present := tablesObj[t]; present {
				return nil, fmt.Errorf("postgres: Neki data topology: tables.%s is not an object", t)
			}
			entry = map[string]any{}
		}
		entry["shard_group"] = group
		tablesObj[t] = entry
	}
	// An encoder rather than json.Marshal so `<`, `>` and `&` in the
	// operator's strings are not rewritten as <-style escapes: equal
	// JSON, but a needless diff in the topology's own change log.
	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(root); err != nil {
		return nil, fmt.Errorf("postgres: encode Neki data topology for placement: %w", err)
	}
	return bytes.TrimRight(out.Bytes(), "\n"), nil
}

// descendObject walks `root` along `path`, creating a missing object at each
// step, and returns the final object. A step that exists and is not an
// object is an error rather than an overwrite.
func descendObject(root map[string]any, path ...string) (map[string]any, error) {
	cur := root
	for i, key := range path {
		next, ok := cur[key].(map[string]any)
		if !ok {
			if _, present := cur[key]; present {
				return nil, fmt.Errorf("postgres: Neki data topology: %s is not an object",
					strings.Join(path[:i+1], "."))
			}
			next = map[string]any{}
			cur[key] = next
		}
		cur = next
	}
	return cur, nil
}

// writeNekiTopology stores `doc`, passing the snapshot's revision as
// `expected_revision` when one was read. Returns the new revision, or
// conflict=true when the router refused the write because the stored
// revision had moved.
func writeNekiTopology(ctx context.Context, db *sql.DB, doc []byte, snap *nekiTopologyForWrite) (rev int64, conflict bool, err error) {
	opts := map[string]any{"comment": "sluice: place control tables in the authoritative shard group"}
	if snap.hasRevision {
		opts["expected_revision"] = snap.revision
	}
	optsJSON, err := json.Marshal(opts)
	if err != nil {
		return 0, false, err
	}
	var ok bool
	err = db.QueryRowContext(ctx,
		"SELECT success, revision FROM __neki.set_data_topology($1, true, $2)",
		string(doc), string(optsJSON)).Scan(&ok, &rev)
	switch {
	case err != nil:
		if snap.hasRevision && isNekiRevisionConflict(err) {
			return 0, true, nil
		}
		return 0, false, err
	case !ok:
		// Measured 2026-09-14: an expected_revision mismatch is reported as
		// `success=false, revision=0` — data, not an error. But success=false
		// is also what any other refused write looks like, so it is a
		// conflict only if the stored revision actually moved; otherwise it
		// is a failure to report, not a retry.
		if snap.hasRevision {
			cur, have, rerr := readNekiTopologyRevision(ctx, db)
			if rerr == nil && have && cur != snap.revision {
				return 0, true, nil
			}
		}
		return 0, false, fmt.Errorf("__neki.set_data_topology reported success=false (revision %d); "+
			"`SELECT * FROM __neki.validate_data_topology(<document>)` explains a refused document", rev)
	}
	return rev, false, nil
}

// waitForNekiTopology blocks until every router has the revision, so the
// control-table write that follows cannot land on a router that still routes
// the table by shard key. Best-effort: the write has already succeeded, and
// a router that has not converged refuses the next write LOUDLY (NK306)
// rather than accepting it wrongly, so a failed wait is logged, not fatal.
func waitForNekiTopology(ctx context.Context, db *sql.DB, rev int64) {
	if _, err := db.ExecContext(ctx, "SELECT __neki.wait_for_data_topology($1)", rev); err != nil {
		slog.WarnContext(ctx, "wait_for_data_topology did not confirm the placement on every router; "+
			"a control-table write that lands on a router still on the old revision is refused loudly (NK306), "+
			"and a restart retries it",
			slog.Int64("topology_revision", rev), slog.Any("err", err))
	}
}

// refuseNekiControlPlacement is the coded refusal for a placement sluice
// could not make. The error names the database, schema, tables and group so
// the operator can make the placement by hand; the hint carries the shape of
// the topology entry and the call.
func refuseNekiControlPlacement(database, schema string, plan nekiControlPlacementPlan, cause error) error {
	var pgErr *pgconn.PgError
	permission := errors.As(cause, &pgErr) && pgErr.Code == nekiSQLStateInsufficientPrivilege
	what := fmt.Sprintf("postgres: sluice's control tables (%s) in schema %q of database %q must be placed in an "+
		"unsharded shard group on this PlanetScale Neki target — they carry no shard key, and the database's "+
		"default shard group would refuse every write to them with SQLSTATE NK306",
		strings.Join(plan.placed, ", "), schema, database)
	if permission {
		what += fmt.Sprintf("\nsluice's role may not call __neki.set_data_topology to place them itself: %v", cause)
	} else {
		what += fmt.Sprintf("\nsluice could not place them: %v", cause)
	}
	group := plan.group
	if group == "" {
		group = "<the authoritative shard group>"
	}
	return sluicecode.Wrap(
		sluicecode.CodeTargetControlTablePlacement,
		"place the tables yourself with a role that may write the topology: read `__neki.get_data_topology()`, "+
			"add `\"<table>\": {\"shard_group\": \""+group+"\"}` under `databases.<db>.schemas.<schema>.tables` "+
			"for each table named, write it back with `__neki.set_data_topology(<document>, true, "+
			"'{\"comment\":\"sluice control tables\"}')`, then re-run; or grant sluice's role the right to "+
			"call set_data_topology and it will do this on the next start. If the authoritative group "+
			"declares a default shard index, remove it — the platform's guidance is that the group holds "+
			"unsharded metadata",
		errors.New(what),
	)
}

// readNekiTopologyRevision returns the stored topology's current revision.
//
// `__neki.get_data_topology_revision()` is not in the vendor's data-topology
// page, which documents the `expected_revision` option and not how the
// revision is read; it was found by enumerating pg_proc on a live router
// (nekiverify_topology_write_probe_test.go, 2026-09-14). A router without it
// answers ok=false rather than an error, and the write then goes without an
// expected revision — last-writer-wins, whose failure mode is a LOST placement
// when two writers place different table sets or an operator edits
// concurrently; loud (NK306 on the next write) and healed by a restart, never
// silent. See [nekiTopologyForWrite].
func readNekiTopologyRevision(ctx context.Context, db *sql.DB) (rev int64, ok bool, err error) {
	err = db.QueryRowContext(ctx, "SELECT __neki.get_data_topology_revision()").Scan(&rev)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgSQLStateUndefinedFunction {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("postgres: read Neki data topology revision: %w", err)
	}
	return rev, true, nil
}

// pgSQLStateUndefinedFunction is PostgreSQL's undefined_function.
const pgSQLStateUndefinedFunction = "42883"

// isNekiRevisionConflict reports whether a set_data_topology ERROR is the
// expected_revision mismatch. Measured 2026-09-14: the router reports a
// mismatch as `success=false, revision=0` — data, not an error — so this is
// the belt to that braces, matched on the message rather than a code because
// no code was observed.
func isNekiRevisionConflict(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "expected_revision")
}
