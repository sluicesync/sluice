//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestPin_ApplierLoadColumnTypes_DomainUnwrapsToBase is the ground-truth pin
// under the change_applier exemptions in postgresDomainDispatchExemptions: the
// applier's colTypes come from loadColumnTypes, which reads the TARGET's
// information_schema. That view UNWRAPS a DOMAIN to its base type, and
// loadColumnTypes (unlike SchemaReader.populateColumns) has NO typtype='d'
// re-wrap — so a DOMAIN-typed target column presents to equalityPredicate /
// applyPlaceholder / prepareApplierValue as its BASE ir type, never ir.Domain.
//
// The two families pinned are exactly the ones those sites dispatch on: json
// (drives the `::text` equality cast) and money (an ir.VerbatimType that drives
// the `$N::money` cast). If a future change taught loadColumnTypes to
// reconstruct ir.Domain, this pin fails and the exemptions must be revisited —
// because then a real ir.Domain WOULD reach those raw dispatches.
func TestPin_ApplierLoadColumnTypes_DomainUnwrapsToBase(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	const ddl = `
		DROP TABLE IF EXISTS gl_apply_dom CASCADE;
		DROP DOMAIN IF EXISTS doc_json;
		DROP DOMAIN IF EXISTS usd_money;
		CREATE DOMAIN doc_json  AS json;
		CREATE DOMAIN usd_money AS money;
		CREATE TABLE gl_apply_dom (
		  id      bigserial PRIMARY KEY,
		  payload doc_json,
		  price   usd_money
		);
	`
	applyDDL(t, dsn, ddl)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	cols, err := loadColumnTypes(ctx, db, "public", "gl_apply_dom")
	if err != nil {
		t.Fatalf("loadColumnTypes: %v", err)
	}

	payload, ok := cols["payload"]
	if !ok {
		t.Fatalf("payload column missing from loadColumnTypes result")
	}
	if _, isDom := payload.Type.(ir.Domain); isDom {
		t.Errorf("payload (DOMAIN over json) came back as ir.Domain; the applier's raw ir.JSON dispatch would " +
			"then miss the ::text equality cast — the change_applier exemptions are no longer valid")
	}
	if _, isJSON := payload.Type.(ir.JSON); !isJSON {
		t.Errorf("payload type = %T; want ir.JSON (information_schema unwraps the domain to its base)", payload.Type)
	}

	price, ok := cols["price"]
	if !ok {
		t.Fatalf("price column missing from loadColumnTypes result")
	}
	if _, isDom := price.Type.(ir.Domain); isDom {
		t.Errorf("price (DOMAIN over money) came back as ir.Domain; the applier's raw ir.VerbatimType dispatch " +
			"would then miss the $N::money cast — the change_applier exemptions are no longer valid")
	}
	if _, isVerbatim := price.Type.(ir.VerbatimType); !isVerbatim {
		t.Errorf("price type = %T; want ir.VerbatimType (money carries verbatim on a PG target)", price.Type)
	}
}
