// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # Index builds on a PlanetScale Neki target go through online DDL
//
// A Neki branch ships `statement_timeout = 30s` as a PLATFORM DEFAULT (see
// NEKI-018), and a `CREATE INDEX` is one statement. Measured 2026-09-12 on a
// live branch: building a single-column index on a 26.3M-row table died at
// **31 seconds** with `canceling statement due to user request`. So on Neki the
// ordinary deferred index phase cannot build an index on any table large enough
// to matter, and re-running hits the same wall deterministically.
//
// # Why the statement_timeout pin does not cover this
//
// [withCopySessionPins] pins `statement_timeout = 0` for the COPY lanes, and
// index builds are DELIBERATELY excluded from it — see that helper's doc. pgx
// cancels a statement by breaking the socket rather than sending a
// CancelRequest, so a backend streaming COPY notices at once while a backend
// inside `CREATE INDEX` is not reading its socket and would run the build to
// completion before discovering the client had gone. For those phases the
// server's timeout is the only bound there is, and removing it would make a
// runaway build unkillable. That argument still holds; what it means on Neki is
// that the bound is 30 seconds, which is a wall rather than a safety net.
//
// # The remedy, measured end to end
//
// `__neki.online_ddl_create` applies a schema change ASYNCHRONOUSLY, so no
// single statement runs long and the timeout never applies. Proven on the same
// branch and the same index that died at 31s: submitted in ~2 seconds, built in
// the background, and the index landed.
//
// The lifecycle has a trap that would hang a naive integration forever:
//
//	1. online_ddl_create(workflow, db, ddl, '')   → returns in ~2s
//	2. poll online_ddl_status(workflow)           → ~36 min on 28.9M rows
//	3. workflow_complete(workflow)                → the index becomes visible
//
// The top-level `status` column stays `running` right up until step 3 — it does
// NOT go terminal when the build finishes. The done signal is PER-SHARD, inside
// `status_by_shard`: `current_readiness` flips false → true. Something polling
// `status` for it to stop saying `running` waits forever on a workflow that has
// been ready for half an hour. Both states were observed on one workflow.
//
// Other constraints, each learned by hitting it:
//
//   - Table names must be SCHEMA-QUALIFIED. An unqualified name is refused with
//     "a migration does not guess the search path". sluice's emitter already
//     qualifies (`ON "schema"."table"`), so this is a premise to keep rather
//     than work to do — [TestNekiOnlineDDLRequiresQualifiedNames] pins it.
//   - No `CONCURRENTLY` in an index built this way (PlanetScale's docs).
//   - One table and one execution category per workflow.
//
// This is the Neki analogue of ADR-0148's deploy-request fallback for
// PlanetScale MySQL (`IndexBuildFallback`): a second implementor of an existing
// pattern — submit the DDL to a control plane, poll, cut over — rather than a
// new subsystem.

package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"
)

// Neki online-DDL polling envelope. Package vars so tests can shrink them;
// production never mutates them.
//
// The wall bound is generous on purpose. A measured single-column index on
// 28.9M rows took ~36 minutes, and an index phase may carry several — so a
// bound tight enough to feel responsive would abandon builds that were going to
// succeed, which is the worse failure. ctx cancellation is the responsive path;
// this is only the runaway backstop.
var (
	nekiOnlineDDLPollInterval = 15 * time.Second
	nekiOnlineDDLMaxWall      = 4 * time.Hour
)

// nekiWorkflowNameUnsafe matches every character not allowed in a workflow
// name. Neki takes the name as an identifier-ish token; sluice derives it from
// a table and index name, either of which may contain anything PostgreSQL
// permits in a quoted identifier.
var nekiWorkflowNameUnsafe = regexp.MustCompile(`[^A-Za-z0-9_]+`)

// nekiOnlineDDLWorkflowName derives a DETERMINISTIC workflow name for one
// index build.
//
// Deterministic rather than random so a re-run addresses the same workflow: a
// resumed migration can find a previous attempt's leftovers and clean them up
// (see [SchemaWriter.nekiOnlineDDLCleanup]) instead of accumulating orphaned
// workflows named after a timestamp nobody can correlate.
//
// The `sluice_` prefix marks ownership: an operator listing workflows on a
// branch can tell which are ours.
func nekiOnlineDDLWorkflowName(tableName, indexName string) string {
	base := fmt.Sprintf("sluice_ix_%s_%s", tableName, indexName)
	safe := nekiWorkflowNameUnsafe.ReplaceAllString(base, "_")
	// Neki rejects an over-long name; keep well inside any plausible limit
	// while preserving the distinguishing tail (index names collide less at
	// their end than their start).
	const maxLen = 60
	if len(safe) > maxLen {
		safe = safe[:20] + "_" + safe[len(safe)-(maxLen-21):]
	}
	return safe
}

// buildIndexViaNekiOnlineDDL builds one index through Neki's online-DDL
// workflow instead of executing the CREATE INDEX directly.
//
// stmt is the already-emitted, schema-qualified CREATE INDEX. It is passed as a
// QUERY PARAMETER rather than interpolated, so no DDL text ever needs escaping
// into the calling SQL.
func (w *SchemaWriter) buildIndexViaNekiOnlineDDL(ctx context.Context, conn *sql.Conn, job indexBuildJob, stmt string) error {
	// Idempotent resume. The direct path gets this from `IF NOT EXISTS`, which
	// is not available here: an online-DDL workflow that re-creates an existing
	// index fails, and it fails ~36 minutes after being submitted. Checking
	// first is both cheaper and the only way to make a resumed index phase a
	// no-op rather than a long failure.
	exists, err := w.nekiIndexExists(ctx, conn, job.idx.Name)
	if err != nil {
		return fmt.Errorf("postgres: neki online DDL: check whether index %q exists: %w", job.idx.Name, err)
	}
	if exists {
		slog.InfoContext(ctx, "postgres: index already present; skipping Neki online DDL",
			slog.String("table", job.tableName),
			slog.String("index", job.idx.Name))
		return nil
	}

	// CONCURRENTLY is rejected in an online-DDL build. sluice's emitter does
	// not produce it today; strip defensively rather than fail 36 minutes in
	// if that ever changes.
	if strings.Contains(strings.ToUpper(stmt), " CONCURRENTLY") {
		stmt = nekiStripConcurrently(stmt)
	}

	wf := nekiOnlineDDLWorkflowName(job.tableName, job.idx.Name)

	// A previous attempt may have left a TERMINAL workflow under this name
	// (a crash between submit and complete, or a failed build). Its artifacts
	// block re-creating the same name, so clear them first. Best-effort: on a
	// clean branch there is nothing to clean and the call errors harmlessly.
	w.nekiOnlineDDLCleanup(ctx, conn, wf)

	started := time.Now()
	var gotWorkflow, migrationID string
	row := conn.QueryRowContext(ctx,
		`SELECT workflow, migration_id FROM __neki.online_ddl_create($1, pg_catalog.current_database(), $2, '')`,
		wf, stmt)
	if err := row.Scan(&gotWorkflow, &migrationID); err != nil {
		return fmt.Errorf(
			"postgres: neki online DDL: submit index %q on %q: %w",
			job.idx.Name, job.tableName, err,
		)
	}

	slog.InfoContext(ctx, "postgres: index build submitted to Neki online DDL",
		slog.String("table", job.tableName),
		slog.String("index", job.idx.Name),
		slog.String("workflow", gotWorkflow),
		slog.String("migration_id", migrationID),
		slog.String("why", "a direct CREATE INDEX on a Neki target dies at the 30s statement_timeout"))

	if err := w.awaitNekiOnlineDDL(ctx, conn, wf, job); err != nil {
		// Leave the branch clean: abandon the workflow and drop its artifacts
		// so a re-run can submit the same name again. Both are best-effort —
		// the build error is what the operator needs, not a cleanup failure.
		w.nekiOnlineDDLAbandon(ctx, conn, wf)
		return err
	}

	if _, err := conn.ExecContext(ctx, `SELECT __neki.workflow_complete($1)`, wf); err != nil {
		w.nekiOnlineDDLAbandon(ctx, conn, wf)
		return fmt.Errorf(
			"postgres: neki online DDL: cut over index %q on %q (the build finished; the cutover did not): %w",
			job.idx.Name, job.tableName, err,
		)
	}

	slog.InfoContext(ctx, "postgres: Neki online-DDL index build complete",
		slog.String("table", job.tableName),
		slog.String("index", job.idx.Name),
		slog.Duration("elapsed", time.Since(started)))
	return nil
}

// awaitNekiOnlineDDL polls until every shard reports readiness.
//
// READINESS IS PER-SHARD AND THE TOP-LEVEL STATUS IS NOT THE SIGNAL — see the
// file header.
//
// THE FOLD RUNS IN GO, NOT IN SQL, and that is a correction rather than a
// preference. The first cut asked the server to fold readiness across shards
// with `bool_and(...) FROM jsonb_each(status_by_shard)` as a subquery over the
// outer row. That is a CORRELATED RANGE FUNCTION, and a Neki router refuses it:
//
//	ERROR: not implemented: correlated range function referencing an
//	enclosing query is not yet supported  (SQLSTATE NK013)
//
// Measured 2026-09-12 — the submit succeeded and the very first poll failed.
// Neki implements a deliberately narrow SQL surface (the same NK013 family
// covers pg_size_bytes, pg_database_size and pg_total_relation_size), so any
// server-side cleverness here is a liability. Reading the raw JSON and folding
// it locally depends on nothing beyond returning a column.
func (w *SchemaWriter) awaitNekiOnlineDDL(ctx context.Context, conn *sql.Conn, wf string, job indexBuildJob) error {
	deadline := time.Now().Add(nekiOnlineDDLMaxWall)
	const readyQuery = `SELECT status, status_by_shard::text FROM __neki.online_ddl_status($1)`

	for {
		var status, shardsJSON string
		if err := conn.QueryRowContext(ctx, readyQuery, wf).Scan(&status, &shardsJSON); err != nil {
			return fmt.Errorf(
				"postgres: neki online DDL: poll index %q on %q: %w",
				job.idx.Name, job.tableName, err,
			)
		}
		allReady, shards := nekiAllShardsReady(shardsJSON)
		if allReady {
			return nil
		}
		// A workflow that has gone terminal without reaching readiness will
		// never become ready; waiting out the wall would turn a definite
		// failure into a four-hour one.
		switch strings.ToLower(status) {
		case "error", "failed", "cancelled", "canceled":
			// Carry the PER-SHARD detail. A bare "failed" is undiagnosable,
			// and the workflow is about to be cleaned up by the abandon path,
			// so this is the last moment the reason exists anywhere.
			return fmt.Errorf(
				"postgres: neki online DDL: index %q on %q ended in status %q without reaching readiness "+
					"(%d shard(s) reported); per-shard detail: %s",
				job.idx.Name, job.tableName, status, shards, shardsJSON,
			)
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf(
				"postgres: neki online DDL: index %q on %q was still building after %s (last status %q). "+
					"The workflow is left in place — inspect it with "+
					"`SELECT * FROM __neki.online_ddl_status('%s')` and complete it with "+
					"`SELECT __neki.workflow_complete('%s')` once ready",
				job.idx.Name, job.tableName, nekiOnlineDDLMaxWall, status, wf, wf,
			)
		}
		timer := time.NewTimer(nekiOnlineDDLPollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf(
				"postgres: neki online DDL: index %q on %q abandoned on cancellation (the workflow keeps "+
					"building server-side; complete or cancel it with __neki.workflow_complete / "+
					"__neki.workflow_cancel on %q): %w",
				job.idx.Name, job.tableName, wf, ctx.Err(),
			)
		case <-timer.C:
		}
	}
}

// nekiIndexExists reports whether an index of this name is already present.
func (w *SchemaWriter) nekiIndexExists(ctx context.Context, conn *sql.Conn, indexName string) (bool, error) {
	var n int
	err := conn.QueryRowContext(ctx,
		`SELECT pg_catalog.count(*) FROM pg_catalog.pg_indexes WHERE schemaname = $1 AND indexname = $2`,
		w.schema, indexName).Scan(&n)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// nekiOnlineDDLCleanup drops a terminal workflow's artifacts so its name can be
// reused. Best-effort by design: on a clean branch there is nothing to clean and
// the call errors, which is not worth surfacing.
func (w *SchemaWriter) nekiOnlineDDLCleanup(ctx context.Context, conn *sql.Conn, wf string) {
	if _, err := conn.ExecContext(ctx, `SELECT __neki.online_ddl_cleanup($1)`, wf); err != nil {
		slog.DebugContext(ctx, "postgres: neki online DDL: no prior workflow to clean up",
			slog.String("workflow", wf), slog.String("err", err.Error()))
	}
}

// nekiOnlineDDLAbandon cancels a workflow and drops its artifacts, so a failed
// build leaves the branch in a state a re-run can use. Both calls are
// best-effort on their own context: the caller is already returning the real
// error and must not have it replaced by a cleanup failure.
func (w *SchemaWriter) nekiOnlineDDLAbandon(ctx context.Context, conn *sql.Conn, wf string) {
	// The caller's ctx may already be cancelled — that is one of the paths
	// here — so cleanup gets its own bounded context.
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	if _, err := conn.ExecContext(cctx, `SELECT __neki.workflow_cancel($1)`, wf); err != nil {
		slog.WarnContext(ctx, "postgres: neki online DDL: could not cancel the failed workflow; it may "+
			"need clearing by hand before a re-run can reuse the name",
			slog.String("workflow", wf), slog.String("err", err.Error()))
	}
	if _, err := conn.ExecContext(cctx, `SELECT __neki.online_ddl_cleanup($1)`, wf); err != nil {
		slog.DebugContext(ctx, "postgres: neki online DDL: cleanup after cancel did not apply",
			slog.String("workflow", wf), slog.String("err", err.Error()))
	}
}

// nekiStripConcurrently removes a CONCURRENTLY keyword from a CREATE INDEX.
// Online DDL rejects it; PlanetScale's docs say so explicitly.
func nekiStripConcurrently(stmt string) string {
	// Case-preserving removal of the keyword and exactly one following space.
	for _, form := range []string{" CONCURRENTLY ", " concurrently ", " Concurrently "} {
		if strings.Contains(stmt, form) {
			return strings.Replace(stmt, form, " ", 1)
		}
	}
	upper := strings.ToUpper(stmt)
	i := strings.Index(upper, " CONCURRENTLY")
	if i < 0 {
		return stmt
	}
	return stmt[:i] + stmt[i+len(" CONCURRENTLY"):]
}

// nekiAllShardsReady folds per-shard readiness out of online_ddl_status's
// status_by_shard JSON, returning whether EVERY shard is ready and how many
// shards reported.
//
// Folded in Go rather than SQL because the server-side form needs a correlated
// range function, which a Neki router refuses with NK013 — see
// [SchemaWriter.awaitNekiOnlineDDL].
//
// An EMPTY or unparseable map is NOT ready, deliberately. A workflow whose
// shard map has not appeared yet must keep waiting rather than cut over early:
// treating "no shards reported" as "all shards ready" is vacuously true and
// would complete a workflow that had not built anything. The shard count is
// returned so a caller's error message can say whether it saw any at all.
func nekiAllShardsReady(shardsJSON string) (ready bool, shards int) {
	var byShard map[string]struct {
		CurrentReadiness *bool `json:"current_readiness"`
	}
	if err := json.Unmarshal([]byte(shardsJSON), &byShard); err != nil {
		return false, 0
	}
	if len(byShard) == 0 {
		return false, 0
	}
	for _, s := range byShard {
		// A shard that omits the field has not declared itself ready.
		if s.CurrentReadiness == nil || !*s.CurrentReadiness {
			return false, len(byShard)
		}
	}
	return true, len(byShard)
}
