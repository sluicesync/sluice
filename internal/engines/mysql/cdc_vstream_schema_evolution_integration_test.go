//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Track 1c — Phase A ground-truth + Phase B validation: VStream CDC
// resumability under mid-stream schema evolution.
//
// The genuinely-open behaviour (per
// docs/dev/notes/prep-planetscale-vitess-readiness.md, Phase 1c, and
// docs/vitess-vstream-troubleshooting.md §4): with Vitess
// schema-tracking OFF (the default — vttablet not run with
// --track_schema_versions / --watch_replication_stream), what does
// sluice's VStream reader actually do when a deploy-request-style
// ADD / DROP / MODIFY column DDL lands while a sync is streaming?
//
// The oracle (loud-failure tenet floor): faithful continued sync OR a
// loud, actionable failure. NEVER a silent application of mis-aligned
// rows (silent corruption == FAIL).
//
// Reuses the proven vttestserver scaffolding from
// cdc_vstream_integration_test.go (startVTTestServer, applyVTTestSQL,
// drainVTTestChanges) verbatim — this is a generalisation of that
// harness, not a new framework. Build-tag rationale identical to that
// file: the vitess/vttestserver image (~700 MB) is the defining cost
// and the existing `vstream` tag already gates exactly it. A full
// multi-node `vitessreshard` cluster is NOT needed — vttestserver's
// vtcombo runs a real vttablet+vtgate+MySQL-with-binlog, which is the
// complete code path for the schema-tracking question (the cluster
// tag's cost only buys reshard topology, irrelevant here).
//
// vttestserver's vttablet runs WITHOUT --track_schema_versions by
// default (its run.sh does not pass it), so this exercises exactly
// the schema-tracking-DISABLED case the contract calls the open one.
// The test asserts the empirical outcome rather than presuming it; if
// it observes silent corruption it FAILS loudly with a precise dump
// (that would be the highest-value finding).
//
// Usage:
//
//	$env:PATH += ";C:\Program Files\Rancher Desktop\resources\resources\win32\bin"
//	$env:TESTCONTAINERS_RYUK_DISABLED = "true"
//	go test -tags='integration vstream' -v -count=1 -timeout=20m \
//	  -run 'TestVStream_SchemaEvolution' ./internal/engines/mysql/...

package mysql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// TestVStream_SchemaEvolution_AddColumnMidStream is the headline
// Phase-A ground-truth test. Sequence:
//
//  1. Seed users(id,email); open VStream CDC at "current".
//  2. INSERT a row (pre-DDL) — assert it arrives faithfully.
//  3. ALTER TABLE users ADD COLUMN signup_country VARCHAR(2) — a
//     deploy-request-style additive DDL, applied mid-stream while
//     the reader is live.
//  4. INSERT a row carrying the NEW column (post-DDL).
//  5. Drain and characterise:
//     - faithful: post-DDL insert arrives with signup_country
//     populated and every other column correct → sync tracked it.
//     - loud: the stream terminates with a non-nil Err() (e.g. the
//     "row event without preceding FIELD event" surface) — loud
//     and recoverable (streamer would fall through / restart).
//     - SILENT CORRUPTION (FAIL): post-DDL insert arrives but the
//     column values are mis-aligned (email holds the country, a
//     column is shifted/dropped, types garbled) with NO error.
//
// The assertion logic does not presume which branch fires — it
// classifies what actually happened and fails only on the silent-
// corruption branch (and on a total no-event stall, which would be a
// different loud-but-stuck bug).
func TestVStream_SchemaEvolution_AddColumnMidStream(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
	`
	applyVTTestSQL(t, mysqlDSN, seedDDL)

	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
		mysqlDSN, grpcEndpoint,
	)

	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	// Settle window so the "current"-anchored stream is registered
	// before we generate events (mirrors the basic-stream test).
	time.Sleep(3 * time.Second)

	// ---- Step 2: pre-DDL insert (must arrive faithfully). ----
	applyVTTestSQL(t, mysqlDSN, "INSERT INTO users (email) VALUES ('pre-ddl@example.com')")

	pre := drainVTTestChanges(t, ctx, changes, 1, 60*time.Second)
	if len(pre) != 1 {
		if streamErr := readerErr(rdr); streamErr != nil {
			t.Fatalf("pre-DDL: got %d changes; want 1 (stream error: %v)", len(pre), streamErr)
		}
		t.Fatalf("pre-DDL: got %d changes; want 1", len(pre))
	}
	preIns, ok := pre[0].(ir.Insert)
	if !ok {
		t.Fatalf("pre-DDL change = %T; want ir.Insert", pre[0])
	}
	if email, _ := preIns.Row["email"].(string); email != "pre-ddl@example.com" {
		t.Fatalf("pre-DDL insert email = %#v; want pre-ddl@example.com (baseline decode already broken)", preIns.Row["email"])
	}
	if _, hasNew := preIns.Row["signup_country"]; hasNew {
		t.Errorf("pre-DDL insert unexpectedly has signup_country column %#v", preIns.Row["signup_country"])
	}

	// ---- Step 3: mid-stream additive DDL (deploy-request analog). ----
	applyVTTestSQL(t, mysqlDSN, "ALTER TABLE users ADD COLUMN signup_country VARCHAR(2) NULL")

	// ---- Step 4: post-DDL insert carrying the new column. ----
	applyVTTestSQL(t, mysqlDSN,
		"INSERT INTO users (email, signup_country) VALUES ('post-ddl@example.com', 'US')")

	// ---- Step 5: characterise. Drain up to a few events with a
	// generous deadline; the post-DDL insert is the one under test.
	post := drainVTTestChanges(t, ctx, changes, 1, 60*time.Second)
	streamErr := readerErr(rdr)

	switch {
	case len(post) == 0 && streamErr != nil:
		// LOUD branch: the reader terminated with an error rather
		// than emitting a mis-decoded row. This is the loud-failure
		// floor satisfied at the reader level (the streamer's outer
		// loop turns this into a fatal sync exit → operator restart →
		// position-invalid → ADR-0022 cold-start, per the prep doc's
		// established path). Acceptable per the oracle.
		t.Logf("PHASE-A VERDICT (add-column): LOUD-FAIL — reader terminated, Err()=%v", streamErr)
		t.Logf("PHASE-A VERDICT (add-column): oracle satisfied (loud + recoverable; no silent corruption)")

	case len(post) == 0 && streamErr == nil:
		// Stream neither delivered nor errored — a silent STALL.
		// Distinct from silent corruption but still a loud-failure-
		// tenet violation (silent gap). Fail.
		t.Fatalf("PHASE-A VERDICT (add-column): SILENT STALL — no post-DDL event and no error; " +
			"the post-DDL insert was neither delivered nor surfaced as a failure (silent gap)")

	default:
		// Events arrived post-DDL. Verify they are FAITHFUL, not
		// mis-aligned. The defining silent-corruption signature:
		// the new row's known-correct values land in the wrong
		// columns, or types are garbled, with no error.
		ins, ok := post[0].(ir.Insert)
		if !ok {
			t.Fatalf("PHASE-A VERDICT (add-column): post-DDL change = %T; want ir.Insert", post[0])
		}
		email, _ := ins.Row["email"].(string)
		country, hasCountry := ins.Row["signup_country"]

		corrupt := false
		var why string
		if email != "post-ddl@example.com" {
			corrupt = true
			why = fmt.Sprintf("email column = %#v; want post-ddl@example.com (column misalignment)", ins.Row["email"])
		}
		if !corrupt && !hasCountry {
			// New column entirely absent from the decoded row while
			// the source row has it set: the reader decoded against
			// the OLD field list — a silent drop of the new column's
			// value. That is silent data loss for that column.
			corrupt = true
			why = "decoded row has no signup_country key though the source row set it 'US' " +
				"(reader decoded against stale field metadata — silent column drop)"
		}
		if !corrupt && hasCountry {
			if cs, _ := country.(string); cs != "US" {
				corrupt = true
				why = fmt.Sprintf("signup_country = %#v; want \"US\" (value misalignment)", country)
			}
		}

		if corrupt {
			t.Fatalf("PHASE-A VERDICT (add-column): SILENT CORRUPTION — post-DDL insert delivered "+
				"WITH NO ERROR but mis-decoded: %s. Full decoded row: %#v. "+
				"This is a CRITICAL silent-loss finding (schema-tracking-OFF VStream + mid-stream DDL).",
				why, ins.Row)
		}

		t.Logf("PHASE-A VERDICT (add-column): FAITHFUL — post-DDL insert decoded correctly "+
			"(email=%q signup_country=%v); VStream re-emitted FIELD metadata and sluice's "+
			"dispatchDDL cache-clear realigned the decode. Oracle satisfied.", email, country)
	}
}

// TestVStream_SchemaEvolution_DropColumnMidStream is the DROP-column
// counterpart. A dropped column is the higher-risk silent-corruption
// shape: the positional row image shrinks by one, so decoding a
// post-DROP ROW event against the pre-DROP field list shifts every
// subsequent column. Same oracle and classification logic as the
// add-column test.
func TestVStream_SchemaEvolution_DropColumnMidStream(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE accounts (
			id      BIGINT       NOT NULL AUTO_INCREMENT,
			email   VARCHAR(255) NOT NULL,
			legacy  VARCHAR(64)  NULL,
			region  VARCHAR(16)  NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
	`
	applyVTTestSQL(t, mysqlDSN, seedDDL)

	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
		mysqlDSN, grpcEndpoint,
	)

	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	time.Sleep(3 * time.Second)

	// Pre-DROP insert — baseline faithful decode.
	applyVTTestSQL(t, mysqlDSN,
		"INSERT INTO accounts (email, legacy, region) VALUES ('pre-drop@example.com', 'oldval', 'us-east')")
	pre := drainVTTestChanges(t, ctx, changes, 1, 60*time.Second)
	if len(pre) != 1 {
		if streamErr := readerErr(rdr); streamErr != nil {
			t.Fatalf("pre-DROP: got %d changes; want 1 (stream error: %v)", len(pre), streamErr)
		}
		t.Fatalf("pre-DROP: got %d changes; want 1", len(pre))
	}
	preIns, ok := pre[0].(ir.Insert)
	if !ok {
		t.Fatalf("pre-DROP change = %T; want ir.Insert", pre[0])
	}
	if r, _ := preIns.Row["region"].(string); r != "us-east" {
		t.Fatalf("pre-DROP region = %#v; want us-east (baseline decode broken)", preIns.Row["region"])
	}

	// Mid-stream DROP COLUMN — shrinks the row image by one column.
	applyVTTestSQL(t, mysqlDSN, "ALTER TABLE accounts DROP COLUMN legacy")

	// Post-DROP insert: if the reader decodes this against the
	// pre-DROP 4-column field list, `region` would absorb whatever
	// the now-3-column image puts in slot 3 — classic shift
	// corruption.
	applyVTTestSQL(t, mysqlDSN,
		"INSERT INTO accounts (email, region) VALUES ('post-drop@example.com', 'eu-west')")

	post := drainVTTestChanges(t, ctx, changes, 1, 60*time.Second)
	streamErr := readerErr(rdr)

	switch {
	case len(post) == 0 && streamErr != nil:
		t.Logf("PHASE-A VERDICT (drop-column): LOUD-FAIL — reader terminated, Err()=%v", streamErr)
		t.Logf("PHASE-A VERDICT (drop-column): oracle satisfied (loud + recoverable)")

	case len(post) == 0 && streamErr == nil:
		t.Fatalf("PHASE-A VERDICT (drop-column): SILENT STALL — no post-DROP event and no error (silent gap)")

	default:
		ins, ok := post[0].(ir.Insert)
		if !ok {
			t.Fatalf("PHASE-A VERDICT (drop-column): post-DROP change = %T; want ir.Insert", post[0])
		}
		email, _ := ins.Row["email"].(string)
		region, hasRegion := ins.Row["region"].(string)
		_, hasLegacy := ins.Row["legacy"]

		corrupt := false
		var why string
		switch {
		case email != "post-drop@example.com":
			corrupt, why = true, fmt.Sprintf("email = %#v; want post-drop@example.com (shift corruption)", ins.Row["email"])
		case hasLegacy:
			corrupt, why = true, fmt.Sprintf("decoded row still carries dropped column 'legacy' = %#v (stale field metadata)", ins.Row["legacy"])
		case !hasRegion || region != "eu-west":
			corrupt, why = true, fmt.Sprintf("region = %#v (hasRegion=%v); want eu-west (column shift after DROP)", ins.Row["region"], hasRegion)
		}

		if corrupt {
			t.Fatalf("PHASE-A VERDICT (drop-column): SILENT CORRUPTION — post-DROP insert delivered "+
				"WITH NO ERROR but mis-decoded: %s. Full decoded row: %#v. CRITICAL silent-loss finding.",
				why, ins.Row)
		}

		t.Logf("PHASE-A VERDICT (drop-column): FAITHFUL — post-DROP insert decoded correctly "+
			"(email=%q region=%q, no stale 'legacy'). Oracle satisfied.", email, region)
	}
}

// TestVStream_SchemaEvolution_ModifyColumnMidStream covers a
// type-widening MODIFY (the third deploy-request shape). VARCHAR(8) →
// VARCHAR(64) doesn't change column count but does change the field
// metadata; the risk is the reader decoding the wider value against a
// stale narrower field descriptor. Same oracle.
func TestVStream_SchemaEvolution_ModifyColumnMidStream(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE prefs (
			id    BIGINT      NOT NULL AUTO_INCREMENT,
			code  VARCHAR(8)  NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
	`
	applyVTTestSQL(t, mysqlDSN, seedDDL)

	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
		mysqlDSN, grpcEndpoint,
	)

	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	time.Sleep(3 * time.Second)

	applyVTTestSQL(t, mysqlDSN, "INSERT INTO prefs (code) VALUES ('short')")
	pre := drainVTTestChanges(t, ctx, changes, 1, 60*time.Second)
	if len(pre) != 1 {
		if streamErr := readerErr(rdr); streamErr != nil {
			t.Fatalf("pre-MODIFY: got %d changes; want 1 (stream error: %v)", len(pre), streamErr)
		}
		t.Fatalf("pre-MODIFY: got %d changes; want 1", len(pre))
	}

	applyVTTestSQL(t, mysqlDSN, "ALTER TABLE prefs MODIFY COLUMN code VARCHAR(64) NOT NULL")

	// A value longer than the pre-MODIFY VARCHAR(8) — if decoded
	// against the stale narrower descriptor it could be truncated.
	const wideVal = "this-is-a-much-longer-code-value-than-8-bytes"
	applyVTTestSQL(t, mysqlDSN, "INSERT INTO prefs (code) VALUES ('"+wideVal+"')")

	post := drainVTTestChanges(t, ctx, changes, 1, 60*time.Second)
	streamErr := readerErr(rdr)

	switch {
	case len(post) == 0 && streamErr != nil:
		t.Logf("PHASE-A VERDICT (modify-column): LOUD-FAIL — reader terminated, Err()=%v", streamErr)
		t.Logf("PHASE-A VERDICT (modify-column): oracle satisfied (loud + recoverable)")

	case len(post) == 0 && streamErr == nil:
		t.Fatalf("PHASE-A VERDICT (modify-column): SILENT STALL — no post-MODIFY event and no error (silent gap)")

	default:
		ins, ok := post[0].(ir.Insert)
		if !ok {
			t.Fatalf("PHASE-A VERDICT (modify-column): post-MODIFY change = %T; want ir.Insert", post[0])
		}
		code, _ := ins.Row["code"].(string)
		if code != wideVal {
			t.Fatalf("PHASE-A VERDICT (modify-column): SILENT CORRUPTION — post-MODIFY code = %#v; "+
				"want %q (value truncated/garbled decoding against stale field metadata, no error). "+
				"Full row: %#v", ins.Row["code"], wideVal, ins.Row)
		}
		t.Logf("PHASE-A VERDICT (modify-column): FAITHFUL — widened value round-tripped intact "+
			"(len=%d). Oracle satisfied.", len(code))
	}
}

// readerErr pulls the terminal stream error off whichever CDC reader
// implementation the engine handed back. Both *vstreamCDCReader and
// *CDCReader expose Err() (the ir.CDCReader contract: valid after the
// change channel closes). Centralised so the three tests above don't
// each re-assert the type.
func readerErr(rdr ir.CDCReader) error {
	if e, ok := rdr.(interface{ Err() error }); ok {
		return e.Err()
	}
	return nil
}
