//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pin for sluice's PG `money` type stance (broader-mining
// gap #5 — docs/dev/notes/test-gap-mining-broader.md).
//
// PG `money` is a 64-bit integer count of monetary units in the
// current locale's smallest unit. Its text I/O is locale-dependent
// (`SET lc_monetary` changes the input/output format), which makes
// round-trip translation fraught — both same-engine (PG→PG can pick
// up a different `lc_monetary` on the target) and cross-engine
// (MySQL has no native money type; DECIMAL would lose locale, BIGINT
// would lose semantics).
//
// sluice's same-engine PG→PG verbatim-carry allowlist (`coreVerbatim
// EligibleTypes` in internal/engines/postgres/types.go) explicitly
// LISTS `money` as a Stage 2 deferral with a comment:
//
//	"Each has a known text-IO / locale / dialect concern worth a
//	 per-type round-trip integration test before adding to the
//	 allowlist."
//
// This pin IS that integration test. It seeds a source with a `money`
// column, runs migrate, and documents which of three outcomes sluice
// produces today:
//
//	(a) Migrator refuses-loudly at schema-read with an operator-
//	    actionable diagnostic. Acceptable per the tenet.
//	(b) Migrator silently maps money to NUMERIC / DECIMAL on the
//	    target. Same-engine PG→PG: target's `pg_attribute.atttypid`
//	    would be `numeric` rather than `money` (silent type-loss);
//	    cross-engine PG→MySQL would also lose the type identity
//	    + the locale. Loud-failure tenet violation.
//	(c) Migrator preserves money on the target. Path (c) for PG→PG
//	    is the correctness baseline; cross-engine N/A.
//
// PG → PG only here (the simplest baseline); cross-engine PG → MySQL
// is a deliberate follow-up that needs its own policy design pass.

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestMigrate_PostgresToPostgres_MoneyTypeStance is the gap #5 policy
// pin. Sluice must either preserve `money` (path c) or refuse-loudly
// with the type named (path a); silent flatten to numeric without
// operator opt-in is the silent-loss class.
func TestMigrate_PostgresToPostgres_MoneyTypeStance(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	// Schema: a single money column + a label so the row is
	// identifiable. Money values include positive, negative, large,
	// and a zero — the boundary cases PG locale handling sometimes
	// surprises operators on.
	const seedDDL = `
		CREATE TABLE ledger (
			id     BIGINT PRIMARY KEY,
			label  VARCHAR(64) NOT NULL,
			amount MONEY      NOT NULL
		);

		INSERT INTO ledger (id, label, amount) VALUES
			(1, 'small positive', '$12.34'::money),
			(2, 'large positive', '$1,000,000.00'::money),
			(3, 'negative',       '-$5.67'::money),
			(4, 'zero',           '$0.00'::money);
	`
	applyPGDDL(t, sourceDSN, seedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	mig := &Migrator{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	runErr := mig.Run(ctx)
	if runErr != nil {
		// Path (a) — refuse-loudly. Acceptable IF the error names the
		// money type so the operator can act on it (Stage-2-list
		// language or even just "money" in the error would point at
		// the gap).
		errStr := runErr.Error()
		t.Logf("Migrator.Run returned: %v", runErr)
		hasContext := false
		for _, want := range []string{"money", "MONEY", "type", "unsupported"} {
			if strings.Contains(errStr, want) {
				hasContext = true
				break
			}
		}
		if !hasContext {
			t.Errorf("Migrator.Run failed but the error doesn't name the money type / "+
				"unsupported-type shape; operators reading CI output need a hint.\n"+
				"got: %v", runErr)
		}
		// Acceptable LOUD-failure landing.
		return
	}

	// Migrator succeeded — distinguish path (b) silent-flatten from
	// path (c) money-preserved by inspecting the target's column type.
	target, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("sql.Open target: %v", err)
	}
	defer func() { _ = target.Close() }()

	var typname string
	const colQ = `
		SELECT t.typname FROM pg_attribute a
		JOIN pg_class c ON a.attrelid = c.oid
		JOIN pg_type  t ON a.atttypid = t.oid
		WHERE c.relname = 'ledger' AND a.attname = 'amount' AND a.attnum > 0
	`
	if err := target.QueryRowContext(ctx, colQ).Scan(&typname); err != nil {
		t.Fatalf("query target ledger.amount type: %v", err)
	}

	// Row-count sanity in both paths.
	var rowCount int
	if err := target.QueryRowContext(ctx, `SELECT count(*) FROM ledger`).Scan(&rowCount); err != nil {
		t.Fatalf("count target ledger rows: %v", err)
	}
	if rowCount != 4 {
		t.Errorf("target ledger rows = %d; want 4 (the seed)", rowCount)
	}

	switch typname {
	case "money":
		// Path (c): money preserved on the target. The correctness
		// baseline if/when sluice's schema writer promotes money out
		// of the Stage 2 deferral list. Verify a sample value text-
		// round-trips by reading it back via PG's native cast.
		var amountText string
		if err := target.QueryRowContext(
			ctx,
			`SELECT amount::text FROM ledger WHERE label = 'small positive'`,
		).Scan(&amountText); err != nil {
			t.Fatalf("read target money value: %v", err)
		}
		// Money's text rendering depends on lc_monetary; a freshly-
		// initialised PG container is `en_US.utf8` by default so we
		// expect '$12.34'. Loosen this if the test container's
		// lc_monetary ever differs.
		if !strings.Contains(amountText, "12.34") {
			t.Errorf("money round-trip lost the value: got %q; want a string containing '12.34'", amountText)
		}
		t.Logf("path (c) — money preserved on target as type=money (correctness baseline)")

	case "numeric", "int4", "int8", "":
		// Path (b): silently mapped to something else. NUMERIC is the
		// natural-but-not-policy-blessed landing; int4/int8 would be
		// even more surprising.
		t.Errorf("SILENT-TYPE-LOSS: target ledger.amount has typname=%q "+
			"(want 'money' or a clean refuse-loudly). Sluice's Stage 2 deferral list "+
			"says: 'each has a known text-IO / locale / dialect concern worth a per-type "+
			"round-trip integration test before adding to the allowlist.' Either preserve "+
			"money OR refuse-loudly with the type named — silent map to %q is the "+
			"loud-failure-tenet regression this pin catches.",
			typname, typname)

	default:
		// Unanticipated landing — surface for triage so the maintainer
		// can codify the policy.
		t.Errorf("unexpected target ledger.amount type: %q (want 'money' / refuse / a known mapping)", typname)
	}
}
