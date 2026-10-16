//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestFirstTouch_OutOfScopeCapturedTableDoesNotStopTheStream pins the scope of
// the first-touch boundary's per-schema shape read (FIRST-TOUCH-BATCH-SCOPE,
// found by the re-review of the first-touch follow-ups; measured before the
// fix: "first-touch schema boundary of public.ft_good: … column "ft_bad"."id"
// DEFAULT expression references uuid_generate_v4()", terminal). The read took
// every table this install captures in the schema, on the premise that setup
// had vetted them all. It had not: setup does not check an extension-function
// default, and the schema reader refuses one without --enable-pg-extension.
//
// Two cells, one per half of the fix: a stream with the pipeline's scope
// predicate (the read is narrowed to the stream's tables), and a reader with
// none — the backup capture lanes — where the per-schema read fails and the
// fallback reads the touched table on its own. Either way the in-scope table
// must stream, with a well-formed boundary (drainEvents checks it).
func TestFirstTouch_OutOfScopeCapturedTableDoesNotStopTheStream(t *testing.T) {
	for _, withScope := range []bool{true, false} {
		name := "no-scope-predicate"
		if withScope {
			name = "scope-predicate"
		}
		t.Run(name, func(t *testing.T) {
			dsn, cleanup := startPGForTrigger(t)
			defer cleanup()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()

			applyPGSQL(t, dsn, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
				CREATE TABLE ft_bad (id uuid PRIMARY KEY DEFAULT uuid_generate_v4(), v text);
				CREATE TABLE ft_good (id bigint PRIMARY KEY, v text);`)
			if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"ft_bad", "ft_good"}, Schema: "public"}); err != nil {
				t.Fatalf("Setup: %v", err)
			}
			r, err := openCDCReader(ctx, dsn, "")
			if err != nil {
				t.Fatalf("openCDCReader: %v", err)
			}
			reader := r.(*CDCReader)
			defer func() { _ = reader.Close() }()
			if withScope {
				reader.SetCDCScopePredicate(func(_, table string) bool { return table == "ft_good" })
			}
			out, err := reader.StreamChanges(ctx, ir.Position{})
			if err != nil {
				t.Fatalf("StreamChanges: %v", err)
			}
			applyPGSQL(t, dsn, `INSERT INTO ft_good (id, v) VALUES (1, 'a')`)
			got := drainEvents(t, out, 1, 20*time.Second)
			if len(got) != 1 {
				t.Fatalf("the in-scope table's change never arrived (%d events); reader.Err() = %v", len(got), reader.Err())
			}
			if ins, ok := got[0].(ir.Insert); !ok || ins.Table != "ft_good" {
				t.Errorf("got %+v; want the ft_good insert", got[0])
			}
		})
	}
}

// simulatePolledTier removes the event-trigger tier from an install, leaving
// what a --allow-polled-fingerprint install has: no DDL capture function, no
// DDL event trigger, so no DDL leaves a marker. The capture-shape door exempts
// that tier and warnDDLDetectionAbsent reports it.
func simulatePolledTier(t *testing.T, dsn string) {
	t.Helper()
	applyPGSQL(t, dsn, "DROP EVENT TRIGGER IF EXISTS "+quoteIdent(CaptureTriggerDDL)+"; "+
		"DROP EVENT TRIGGER IF EXISTS "+quoteIdent(CaptureTriggerDrop)+"; "+
		"DROP FUNCTION IF EXISTS public."+quoteIdent(CaptureFunctionDDL)+"(); "+
		"DROP FUNCTION IF EXISTS public."+quoteIdent(CaptureFunctionDrop)+"()")
}

// TestFirstTouch_PolledTierReadsEachTableAtItsOwnFirstTouch pins
// FIRST-TOUCH-POLLED-TIER-STALE (third review). On the polled tier DDL leaves
// no marker, so the per-schema shape read taken at the schema's first touch
// would describe a table as it was then: a UNIQUE key added to ft_b after
// ft_a's first change, and before ft_b's, was missing from ft_b's boundary.
// The reader now reads per table on that tier; ft_b's boundary must carry the
// key.
func TestFirstTouch_PolledTierReadsEachTableAtItsOwnFirstTouch(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	applyPGSQL(t, dsn, `CREATE TABLE ft_a (id bigint PRIMARY KEY, v text);
		CREATE TABLE ft_b (id bigint PRIMARY KEY, k varchar(20) NOT NULL);`)
	if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"ft_a", "ft_b"}, Schema: "public"}); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	simulatePolledTier(t, dsn)
	r, err := openCDCReader(ctx, dsn, "")
	if err != nil {
		t.Fatalf("openCDCReader: %v", err)
	}
	reader := r.(*CDCReader)
	defer func() { _ = reader.Close() }()
	if !reader.shapePerTable {
		t.Fatal("premise: the simulated polled tier was not recognised (shapePerTable is false)")
	}
	out, err := reader.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	applyPGSQL(t, dsn, `INSERT INTO ft_a VALUES (1, 'a')`)
	if got := drainEvents(t, out, 1, 20*time.Second); len(got) != 1 {
		t.Fatalf("ft_a's change never arrived; reader.Err() = %v", reader.Err())
	}
	applyPGSQL(t, dsn, `ALTER TABLE ft_b ADD CONSTRAINT ft_b_k UNIQUE (k)`)
	applyPGSQL(t, dsn, `INSERT INTO ft_b VALUES (1, 'x')`)
	var shape *ir.Table
	deadline := time.After(20 * time.Second)
	for shape == nil {
		select {
		case c, ok := <-out:
			if !ok {
				t.Fatalf("stream closed: %v", reader.Err())
			}
			if s, isSnap := c.(ir.SchemaSnapshot); isSnap && s.Table == "ft_b" {
				shape = s.IR
			}
		case <-deadline:
			t.Fatalf("no first-touch boundary for ft_b; reader.Err() = %v", reader.Err())
		}
	}
	for _, idx := range shape.Indexes {
		if idx != nil && idx.Unique && len(idx.Columns) == 1 && idx.Columns[0].Column == "k" {
			return
		}
	}
	t.Errorf("ft_b's first-touch boundary lacks the UNIQUE (k) added after the schema's first touch: %+v — the shape was read before the key existed", shape.Indexes)
}

// TestFirstTouch_PerSchemaAndPerTableReadsAgree holds the session cache to the
// read it replaces: for each table, the shape served from the per-schema read
// equals a per-table read of it, field for field.
//
// The table carries every shape the two reads assemble from different
// catalog queries (fourth review): a foreign key to the other captured table,
// a nondeterministic ICU collation, char(n) and text, temporal precision, an
// identity column, beside an enum, a domain, a numeric, an array and jsonb —
// in the public schema and in a non-public one. No generated column: setup
// refuses a GENERATED ... STORED column (generated-stored-column), so no
// captured table, and so no first-touch read, carries one.
func TestFirstTouch_PerSchemaAndPerTableReadsAgree(t *testing.T) {
	for _, schema := range []string{"public", "ft_app"} {
		t.Run(schema, func(t *testing.T) {
			dsn, cleanup := startPGForTrigger(t)
			defer cleanup()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			q := quoteIdent(schema) + "."
			applyPGSQL(t, dsn, `CREATE SCHEMA IF NOT EXISTS `+quoteIdent(schema)+`;
				CREATE TYPE `+q+`ft_mood AS ENUM ('sad','ok');
				CREATE DOMAIN `+q+`ft_short AS varchar(30);
				CREATE COLLATION `+q+`ft_ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false);
				CREATE TABLE `+q+`ft_y (id bigint PRIMARY KEY);
				CREATE TABLE `+q+`ft_x (
					id bigint PRIMARY KEY,
					k varchar(20) COLLATE "C" UNIQUE,
					y bigint REFERENCES `+q+`ft_y (id),
					ci text COLLATE `+q+`ft_ci,
					c char(5),
					tx text,
					ts timestamp(3),
					tz timestamptz(0),
					tm time(2),
					seq int GENERATED BY DEFAULT AS IDENTITY,
					m `+q+`ft_mood, d `+q+`ft_short, n numeric(10,2), a int[], j jsonb);`)
			if plan, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"ft_x", "ft_y"}, Schema: schema}); err != nil {
				t.Fatalf("Setup: %v (%+v)", err, plan.Refusals)
			}
			// The change log lives in the stream's schema; the DSN names it.
			sep := "?"
			if strings.Contains(dsn, "?") {
				sep = "&"
			}
			r, err := openCDCReader(ctx, dsn+sep+"schema="+schema, "")
			if err != nil {
				t.Fatalf("openCDCReader: %v", err)
			}
			reader := r.(*CDCReader)
			defer func() { _ = reader.Close() }()
			if reader.shapePerTable {
				t.Fatal("premise: the event-trigger tier was not recognised")
			}
			for _, table := range []string{"ft_x", "ft_y"} {
				reader.shapePerTable, reader.shapes = false, nil
				batched, err := reader.readTableShape(ctx, schema, table, 0)
				if err != nil {
					t.Fatalf("batched read of %s: %v", table, err)
				}
				if len(reader.shapes[schema]) == 0 {
					t.Fatalf("premise: the per-schema read of %s cached nothing beside it", table)
				}
				reader.shapePerTable, reader.shapes = true, nil
				single, err := reader.readTableShape(ctx, schema, table, 0)
				if err != nil {
					t.Fatalf("per-table read of %s: %v", table, err)
				}
				if !reflect.DeepEqual(batched, single) {
					t.Errorf("the per-schema and per-table reads of %s differ:\nbatched %+v\nsingle  %+v", table, batched, single)
				}
				if table == "ft_x" {
					requireShapeCarries(t, batched)
				}
			}
		})
	}
}

// requireShapeCarries is the anti-vacuity floor of
// [TestFirstTouch_PerSchemaAndPerTableReadsAgree]: two reads that both dropped
// a shape would agree, so ft_x's read must actually carry the foreign key, the
// ICU collation and the identity.
func requireShapeCarries(t *testing.T, shape *ir.Table) {
	t.Helper()
	if len(shape.ForeignKeys) != 1 {
		t.Errorf("ft_x's shape carries %d foreign keys; want 1", len(shape.ForeignKeys))
	}
	byName := map[string]*ir.Column{}
	for _, c := range shape.Columns {
		byName[c.Name] = c
	}
	if c := byName["ci"]; c == nil || !nondeterministicText(c.Type) {
		t.Errorf("ft_x.ci carries no nondeterministic collation: %#v", c)
	}
	if c := byName["seq"]; c == nil || c.Identity == nil {
		t.Errorf("ft_x.seq carries no identity: %+v", c)
	}
}

// nondeterministicText reports whether typ is text stamped with a named
// nondeterministic collation.
func nondeterministicText(typ ir.Type) bool {
	tx, ok := typ.(ir.Text)
	return ok && tx.Collation != "" && tx.Determinism == ir.CollationNonDeterministic
}
