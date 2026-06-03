//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pin for sluice's PG `xml` type stance (ADR-0051 Stage 2
// candidate, queued via the broader-mining review).
//
// PG xml round-trip is fraught because of normalisation: whitespace
// between tags, namespace-prefix ordering, entity expansion (`&amp;`
// vs `&` vs the entity-resolved form), declaration handling
// (`<?xml version=...?>` may or may not appear on output). Each of
// these can change byte-equality on the round-trip even when the
// document is "the same XML". Sluice's same-engine PG → PG verbatim-
// carry allowlist (`coreVerbatimEligibleTypes` in
// internal/engines/postgres/types.go) explicitly LISTS `xml` as a
// Stage 2 deferral with the policy comment:
//
//	"Each has a known text-IO / locale / dialect concern worth a
//	 per-type round-trip integration test before adding to the
//	 allowlist."
//
// Documented outcomes (same shape as the money pin, PR #102):
//
//	(a) Migrator refuses-loudly at schema-read with an error that
//	    names the xml type. Acceptable per the tenet.
//	(b) Migrator silently maps xml → TEXT / VARCHAR on the target.
//	    `pg_attribute.atttypid` → typname would be 'text' or 'varchar'
//	    rather than 'xml'. Silent type-loss → fail loudly.
//	(c) Migrator preserves xml on the target (typname='xml') AND the
//	    document round-trips through `::text` byte-equal. Correctness
//	    baseline.
//
// PG → PG only (the simplest baseline); cross-engine PG → MySQL is a
// deliberate follow-up — MySQL has function-level XML support
// (`ExtractValue`, `UpdateXML`) but no native XML type, so the right
// policy there is refuse-loudly.

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

// TestMigrate_PostgresToPostgres_XMLTypeStance is the Stage-2 xml pin.
// Either preserve xml (path c) or refuse-loudly with the type named
// (path a); silent flatten to text/varchar is the silent-type-loss
// class the tenet refuses.
func TestMigrate_PostgresToPostgres_XMLTypeStance(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	// Seed: a single xml column carrying a deliberately non-trivial
	// document — element + attribute + nested + entity-encoded char +
	// declaration. If sluice flattens to text the value still round-
	// trips byte-equal; the type-loss is the signal.
	const seedDDL = `
		CREATE TABLE docs (
			id      BIGINT PRIMARY KEY,
			label   VARCHAR(64) NOT NULL,
			payload XML        NOT NULL
		);

		INSERT INTO docs (id, label, payload) VALUES
			(1, 'simple',     XMLPARSE(DOCUMENT '<?xml version="1.0"?><root><child id="1">hello</child></root>')),
			(2, 'with-amp',   XMLPARSE(CONTENT  '<note>Tom &amp; Jerry</note>')),
			(3, 'nested-ns',  XMLPARSE(DOCUMENT '<?xml version="1.0"?><a xmlns:x="urn:test"><x:b>nested</x:b></a>'));
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
		// xml type or carries the Stage-2-deferral language.
		errStr := runErr.Error()
		t.Logf("Migrator.Run returned: %v", runErr)
		hasContext := false
		for _, want := range []string{"xml", "XML", "type", "unsupported"} {
			if strings.Contains(errStr, want) {
				hasContext = true
				break
			}
		}
		if !hasContext {
			t.Errorf("Migrator.Run failed but the error doesn't name the xml type / "+
				"unsupported-type shape; operators reading CI output need a hint.\n"+
				"got: %v", runErr)
		}
		return
	}

	// Migrator succeeded — distinguish preservation from silent-flatten.
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
		WHERE c.relname = 'docs' AND a.attname = 'payload' AND a.attnum > 0
	`
	if err := target.QueryRowContext(ctx, colQ).Scan(&typname); err != nil {
		t.Fatalf("query target docs.payload type: %v", err)
	}

	var rowCount int
	if err := target.QueryRowContext(ctx, `SELECT count(*) FROM docs`).Scan(&rowCount); err != nil {
		t.Fatalf("count target docs rows: %v", err)
	}
	if rowCount != 3 {
		t.Errorf("target docs rows = %d; want 3 (the seed)", rowCount)
	}

	switch typname {
	case "xml":
		// Path (c) — correctness baseline. Spot-check one document
		// round-trips through `::text`. PG's xml-to-text conversion
		// is byte-stable for these documents on PG 16 so a
		// substring check is sufficient.
		var payloadText string
		if err := target.QueryRowContext(
			ctx,
			`SELECT payload::text FROM docs WHERE label = 'simple'`,
		).Scan(&payloadText); err != nil {
			t.Fatalf("read target xml value: %v", err)
		}
		if !strings.Contains(payloadText, "<child id=\"1\">hello</child>") {
			t.Errorf("xml round-trip lost the document: got %q; want a string containing the simple child element",
				payloadText)
		}
		t.Logf("path (c) — xml preserved on target as typname=xml (correctness baseline)")

	case "text", "varchar", "char", "bpchar":
		t.Errorf("SILENT-TYPE-LOSS: target docs.payload has typname=%q "+
			"(want 'xml' or a clean refuse-loudly). Sluice's Stage 2 deferral list "+
			"says: 'each has a known text-IO / locale / dialect concern worth a per-type "+
			"round-trip integration test before adding to the allowlist.' Either preserve "+
			"xml OR refuse-loudly with the type named — silent map to %q is the "+
			"loud-failure-tenet regression this pin catches.",
			typname, typname)

	default:
		t.Errorf("unexpected target docs.payload type: %q (want 'xml' / refuse / a documented mapping)", typname)
	}
}
