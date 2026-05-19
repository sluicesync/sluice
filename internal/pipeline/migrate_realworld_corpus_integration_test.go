//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Real-world schema corpus harness (Idea 3, prep-new-test-surfaces.md).
// Exercises sluice's schema reader + cross-engine translation against
// operator-shaped real schemas (not the synthetic fuzz generator).
//
// Signal = Migrator{DryRun:true}: reads the source schema and plans
// the cross-engine target DDL WITHOUT moving data — where translation
// bugs / unexpected loud-refusals live. The corpus *.sql are
// fetch-on-demand (gitignored); missing files SKIP (not fail) so this
// is green on a fresh checkout until `fetch.sh` is run.
//
// Chinook legs assert (small, clean, matched MySQL/PG pair → an
// oracle). GitLab is a *characterization* leg: it's a 1444-table real
// PG schema that vanilla PG may not even fully load — outcomes are
// logged, not hard-asserted, like the fuzz harness's verdict shape.

package pipeline

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"

	_ "github.com/orware/sluice/internal/engines/mysql"
	_ "github.com/orware/sluice/internal/engines/postgres"
)

// applyPGDDLBestEffort runs the script in one autocommit batch and
// RETURNS the error instead of failing the test — for characterizing
// a real-world schema that vanilla PG may not fully accept (roles /
// extensions / OWNER). Statements before the first failure are
// applied (no enclosing txn), so the reader still sees a partial
// schema. Statement-splitting is deliberately NOT attempted (GitLab's
// dollar-quoted function bodies make naive splitting wrong).
func applyPGDDLBestEffort(dsn, ddl string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	_, err = db.ExecContext(ctx, ddl)
	return err
}

func readCorpus(t *testing.T, name string) string {
	t.Helper()
	p := filepath.Join("testdata", "real-world-corpus", name)
	b, err := os.ReadFile(p)
	if err != nil {
		t.Skipf("corpus %s not present (%v) — run internal/pipeline/testdata/real-world-corpus/fetch.sh", name, err)
	}
	if len(b) == 0 {
		t.Skipf("corpus %s is empty — re-run fetch.sh", name)
	}
	return string(b)
}

// Chinook MySQL → PG, DryRun: source read + cross-engine plan, no data.
func TestCorpus_Chinook_MySQLToPG_DryRun(t *testing.T) {
	ddl := readCorpus(t, "chinook_mysql.ddl.sql")
	src, _, cleanup := startMySQL(t)
	defer cleanup()
	_, tgt, pgCleanup := startPostgres(t)
	defer pgCleanup()

	applyMySQLDDL(t, src, ddl)

	myEng, _ := engines.Get("mysql")
	pgEng, _ := engines.Get("postgres")
	mig := &Migrator{Source: myEng, Target: pgEng, SourceDSN: src, TargetDSN: tgt, DryRun: true}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Chinook MySQL→PG DryRun: schema read/plan failed: %v", err)
	}
	t.Log("Chinook MySQL→PG DryRun: schema read + cross-engine plan OK (11-table matched-pair source)")
}

// Chinook PG → MySQL, DryRun.
func TestCorpus_Chinook_PGToMySQL_DryRun(t *testing.T) {
	ddl := readCorpus(t, "chinook_postgres.ddl.sql")
	src, _, cleanup := startPostgres(t)
	defer cleanup()
	_, tgt, myCleanup := startMySQL(t)
	defer myCleanup()

	applyPGDDL(t, src, ddl)

	pgEng, _ := engines.Get("postgres")
	myEng, _ := engines.Get("mysql")
	mig := &Migrator{Source: pgEng, Target: myEng, SourceDSN: src, TargetDSN: tgt, DryRun: true}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Chinook PG→MySQL DryRun: schema read/plan failed: %v", err)
	}
	t.Log("Chinook PG→MySQL DryRun: schema read + cross-engine plan OK")
}

// GitLab db/structure.sql — CHARACTERIZATION (not asserted). 2.8MB /
// 1444 tables of real PG (partitioning, extensions, functions). Two
// signals: (1) does vanilla PG even load it; (2) if so, can sluice's
// PG reader + PG→MySQL DryRun handle it at scale / how does it refuse.
// Outcomes are logged so a failing apply or a loud refusal is
// recorded, never a red bar (the schema isn't expected to be
// cross-engine-clean — the point is to find where sluice strains).
func TestCorpus_GitLab_PG_Characterize(t *testing.T) {
	ddl := readCorpus(t, "gitlab_structure.pg.sql")
	src, _, cleanup := startPostgres(t)
	defer cleanup()
	_, tgt, myCleanup := startMySQL(t)
	defer myCleanup()

	// Best-effort apply: GitLab's structure.sql may reference roles /
	// extensions / settings a vanilla PG16 lacks. Capture, don't fail.
	applyErr := applyPGDDLBestEffort(src, ddl)
	if applyErr != nil {
		t.Logf("GitLab structure.sql apply into vanilla PG: PARTIAL/FAILED — %v\n"+
			"  → iteration-2 finding: needs preprocessing (strip roles/OWNER/extensions) before it's a usable PG-reader corpus.", truncErr(applyErr))
	} else {
		t.Log("GitLab structure.sql applied into vanilla PG cleanly (unexpected — good).")
	}

	pgEng, _ := engines.Get("postgres")
	myEng, _ := engines.Get("mysql")
	mig := &Migrator{Source: pgEng, Target: myEng, SourceDSN: src, TargetDSN: tgt, DryRun: true}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Logf("GitLab PG→MySQL DryRun: refused/failed (EXPECTED-ish at this scale) — %v", truncErr(err))
		return
	}
	t.Log("GitLab PG→MySQL DryRun: schema read + cross-engine plan completed (characterization).")
}

func truncErr(err error) string {
	s := err.Error()
	if len(s) > 600 {
		return s[:600] + " …[truncated]"
	}
	return s
}
