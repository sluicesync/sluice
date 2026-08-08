// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/engines/internal/namecollide"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// MySQL's table-name namespace folds or does not fold ACCORDING TO THE SERVER,
// and this file is the one place that turns that setting into a refusal
// (roadmap item 149).
//
// # Ground truth, measured rather than assumed
//
// Real `mysql:8` containers, one statement pair, both settings — pinned by
// TestMySQLTableNameFoldGroundTruth (internal/pipeline, integration tag — it
// lives beside the end-to-end tests because it shares their containers):
//
//	lct=1  CREATE TABLE `orders`; CREATE TABLE IF NOT EXISTS `Orders`
//	       -> Note 1050 "Table 'orders' already exists" — a WARNING, not an
//	          error. information_schema holds ONE table; a row inserted under
//	          each spelling leaves TWO rows in it.
//	lct=0  the same pair -> both `orders` and `Orders` exist. No fold, no
//	       collision, nothing to refuse.
//	lct=2  requested on a case-sensitive filesystem, mysqld logs MY-010160 and
//	       runs at 0 — @@global reports 0 and the pair does NOT fold. See
//	       [Engine.lowerCaseTableNames] for why the predicate below is
//	       therefore `!= 0` on the EFFECTIVE value rather than `== 1`.
//	lct=1  `é` then CREATE TABLE IF NOT EXISTS `É` (utf8mb4 client and
//	       database) -> Note 1050, ONE table `é`. The server folds non-ASCII
//	       case too.
//
// Identical results on `mariadb:11.4` for all three of lct=1, lct=2 and the
// non-ASCII pair, which is what makes the `mariadb` flavor a member rather
// than an assumption.
//
// # The VStream flavors answer the probe, which is not obvious and matters
//
// This file's probe runs on EVERY MySQL-flavor target, including PlanetScale
// and Vitess, where the connection terminates at vtgate rather than at mysqld —
// and vtgate implements its own, partial system-variable surface. A variable it
// declined would turn this check into a create-tables failure on every VStream
// target: the over-refusal direction, on the flavors least able to absorb it.
// Measured on `vitess/vttestserver:mysql80` (2026-08-07): vtgate answers both
// `SELECT @@global.lower_case_table_names` and the session form with **0**, so
// those flavors take the no-fold path and this adds no failure mode. That
// measurement is now CHECKED rather than merely recorded: the two vtgate
// premise pins — TestVStream_BackupPositionProbesAnswerThroughVtgate (per-PR,
// vtcombo) and TestVitessCluster_BackupPositionProbesAnswerThroughVtgate (the
// weekly version matrix, vtgate v21..v24) — grade `readLowerCaseTableNames`
// against a live vtgate alongside the backup-position preflight's own probe,
// because it is the same surface with the same failure direction. What is
// pinned is that vtgate ANSWERS it; the specific value stays a property of the
// deployment. Its failure mode is a loud refusal at the create-tables phase,
// never a silent merge.
//
// Those rows are the whole difficulty. The defect is identical to SQLite's
// (item 148) — bare `CREATE TABLE IF NOT EXISTS`, a name that folds onto one
// another table already holds, the losing table's rows INSERTed into the winner
// at exit 0 — but the fold is a property of the SERVER, not of the schema. A
// check that folded unconditionally would refuse `orders` + `Orders` on every
// stock Linux MySQL, where that pair is two ordinary tables. So this question
// needs a connection, which is why it is [ir.TableNameFoldPreflighter] and not
// one more answer off the connection-free [ir.TableEmitPreflighter].
//
// # The fold is the SERVER's, and Go is imitating it
//
// [foldMySQLIdentifier] wraps [strings.ToLower], which is what
// [Engine.FoldNamespace] has always used for the DATABASE axis; the TABLE axis
// goes through the same function so the two axes cannot disagree about one
// server setting. It is an IMITATION of a server-side rule: MySQL lowercases
// identifiers with its own charset machinery, not Go's Unicode tables.
//
// UNVERIFIED PREMISE: that the two agree in general on non-ASCII identifiers.
// What is measured is one cell — `é`/`É` folds on both `mysql:8` and
// `mariadb:11.4` under lct=1, exactly as Go's fold does, pinned by
// that ground-truth test — and that cell is the one that would
// otherwise have been assumed the other way. Beyond it the two folds could
// diverge in either direction: a pair Go folds and the server does not is
// OVER-refusal (loud, and the operator renames a table that did not need it);
// a pair the server folds and Go does not is UNDER-refusal, and that one is
// silent. Neither is closed here.
//
// Do NOT "unify" this fold with SQLite's (item 150): they are different
// engines' rules that happen to be spelled alike today — SQLite's is ASCII-only
// and this one is the server's — and the shared thing is the [namecollide]
// seam, not the function.

// readLowerCaseTableNames reads the EFFECTIVE global
// `lower_case_table_names` off any single-row query surface, so the engine's
// own probe (which opens a connection from a DSN) and the schema writer's
// second door (which already holds one) ask the identical question. See
// [Engine.lowerCaseTableNames] for what each value means.
//
// The EFFECTIVE global is the load-bearing word: asked for lct=2 on a
// case-sensitive filesystem, mysqld logs MY-010160 and runs at 0, and this
// read returns the 0 — measured on `mysql:8` and on `mariadb:11.4`. A check
// keyed on the operator's configured intent would over-refuse every such
// server.
func readLowerCaseTableNames(ctx context.Context, q rowQuerier) (int, error) {
	var lct int
	if err := q.QueryRowContext(ctx, "SELECT @@global.lower_case_table_names").Scan(&lct); err != nil {
		return 0, fmt.Errorf("mysql: read lower_case_table_names: %w", err)
	}
	return lct, nil
}

// serverTableNameFold returns the identifier fold the target SERVER applies to
// table names under lct, or nil when it applies none.
//
// nil is the load-bearing return, not an error case: lct=0 is the stock Linux
// default, and on such a server two names that differ in case are two tables.
// Callers that see nil must refuse NOTHING — over-refusal here breaks every
// case-sensitive MySQL migration that works today.
func serverTableNameFold(lct int) func(string) string {
	if lct == 0 {
		return nil
	}
	return foldMySQLIdentifier
}

// validateMySQLTableNameFold refuses a schema in which two source TABLES would
// land on one MySQL table name once the server's fold is applied.
//
// It claims the schema's names against EACH OTHER only, never against what the
// target already holds. That is deliberate and is the direction in which
// checking more would be wrong: under lct != 0 the server stores the names
// sluice's own earlier run created already folded, so claiming against the
// catalog would refuse a `--resume` of a source table named `Orders` for
// having previously succeeded.
//
// Claim order is schema order, which is the order [orderedTables] emits in, so
// the prior claimant reported is the table that would actually have survived.
func validateMySQLTableNameFold(lct int, tables []*ir.Table) error {
	fold := serverTableNameFold(lct)
	if fold == nil {
		return nil
	}
	seen := namecollide.New[string](fold)
	for _, table := range tables {
		if table == nil || table.Name == "" {
			continue
		}
		prior, dup := seen.Claim(table.Name, table.Name)
		if !dup {
			continue
		}
		return sluicecode.Wrap(
			sluicecode.CodeSchemaTableNameCollision,
			"rename one of the two source tables so they no longer fold to the same MySQL name, or "+
				"point the run at a server initialized with lower_case_table_names=0, then re-run",
			fmt.Errorf(
				"mysql: table-name collision: source tables %q and %q both fold to the MySQL identifier "+
					"%q because the target server runs lower_case_table_names=%d (it compares table names "+
					"case-insensitively) and sluice emits CREATE TABLE IF NOT EXISTS with a bare name, so "+
					"creating the second table would return a WARNING (Note 1050) and create NOTHING — ITS "+
					"ROWS WOULD BE INSERTED INTO THE FIRST TABLE, both tables' rows in one table, the right "+
					"count under the surviving name, at exit 0 — rename one of the two source tables",
				prior, table.Name, fold(table.Name), lct,
			),
		)
	}
	return nil
}

// Compile-time proof the ENGINE (not a test stub) answers the pre-copy
// table-name-fold question, so the orchestrator's
// [ir.TableNameFoldPreflighter] gate engages for every MySQL flavor — the
// registry holds Engine VALUES, so the assertion is on the value type.
var _ ir.TableNameFoldPreflighter = Engine{}

// PreflightTableNameFold reports whether two tables in s would land on one
// MySQL table name on the server dsn points at, before any data moves
// (roadmap item 149).
//
// One variable read per call, and none at all for a schema that cannot
// collide. On a non-folding server (lct=0, the stock Linux default) it costs
// exactly that read and can never refuse.
//
// # This door is not redundant with the writer's
//
// [SchemaWriter.CreateTablesWithoutConstraints] asks the same question with its
// own connection, and both are needed for the reason item 148 measured on the
// sibling engine: a run against a target whose tables already exist
// (`--resume`, a `sync` cold start onto a populated target, a restore) never
// reaches the create-tables phase at all and INSERTs straight into the folded
// name. This is the only door those paths pass.
func (e Engine) PreflightTableNameFold(ctx context.Context, dsn string, s *ir.Schema) error {
	if s == nil {
		return errors.New("mysql: PreflightTableNameFold: schema is nil")
	}
	if len(s.Tables) < 2 {
		// One name cannot collide with itself, and this is the common case for
		// `add-table`. Skipping the probe keeps the surface free for a target
		// the run has not otherwise had to reach yet.
		return nil
	}
	lct, err := e.lowerCaseTableNames(ctx, dsn)
	if err != nil {
		return err
	}
	return validateMySQLTableNameFold(lct, s.Tables)
}
