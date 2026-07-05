//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Cross-engine type-breadth integration test for the simple-mode
// orchestrator: Postgres source → MySQL target. Extends the
// coverage of TestMigrate_PostgresToMySQL (which is intentionally
// conservative: BIGINT/BOOLEAN/TEXT/JSONB) into the broader type
// matrix that real-world PG schemas use.
//
// Categories covered:
//
//   - Numeric: SMALLINT, INTEGER, NUMERIC(p,s), REAL, DOUBLE PRECISION.
//   - String: CHAR(N), VARCHAR(N) (the bounded variants, complementing
//     TEXT which the conservative test already covers).
//   - Binary: BYTEA (PG's unbounded byte string) → MySQL BLOB family.
//   - Date/time: DATE, TIME, TIMESTAMP, TIMESTAMP WITH TIME ZONE.
//   - Identifier: UUID → MySQL CHAR(36).
//   - Constraint shapes: composite PRIMARY KEY, multi-column UNIQUE,
//     CHECK constraint, FK with ON DELETE SET NULL (different from
//     the conservative test's CASCADE).
//
// All categories share a single seed DDL + single Migrator run +
// single container pair, so the per-subtest cost is just the
// assertion block. Container churn (the expensive part) is paid
// once for the test.

package pipeline

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestMigrate_PostgresToMySQL_TypeBreadth extends the cross-engine
// PG→MySQL coverage past the conservative spine asserted by
// TestMigrate_PostgresToMySQL. Subtests verify each category's
// translation shape and per-category row round-trips where the
// translation is non-trivial.
func TestMigrate_PostgresToMySQL_TypeBreadth(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	// One seed DDL with all categories represented. Kept in a single
	// `breadth` table where possible so the assertions can navigate
	// `findColumn(table, "<name>")` without juggling table identity;
	// `keyed` covers the constraint shapes that need composite PKs.
	const seedDDL = `
		CREATE TABLE breadth (
			-- Identifier (UUID, fixed-length char surrogate on MySQL)
			id           UUID NOT NULL,

			-- Numeric breadth
			small_n      SMALLINT          NOT NULL,
			int_n        INTEGER           NOT NULL,
			big_n        BIGINT            NOT NULL,
			money        NUMERIC(15, 2)    NOT NULL,
			ratio        REAL              NOT NULL,
			precise      DOUBLE PRECISION  NOT NULL,

			-- String breadth (TEXT covered by the conservative test)
			code         CHAR(8)           NOT NULL,
			label        VARCHAR(64)       NOT NULL,

			-- Binary
			payload      BYTEA             NULL,

			-- Date/Time
			calendar_day DATE              NOT NULL,
			clock_time   TIME(3)           NOT NULL,
			made_at      TIMESTAMP(3)      NOT NULL,
			made_at_tz   TIMESTAMPTZ       NOT NULL,

			-- Constraint shape: CHECK
			score INTEGER NOT NULL,

			PRIMARY KEY (id),
			CONSTRAINT breadth_score_nonneg CHECK (score >= 0),
			CONSTRAINT breadth_label_uniq UNIQUE (label, code)
		);

		CREATE TABLE keyed (
			tenant_id    BIGINT  NOT NULL,
			row_id       BIGINT  NOT NULL,
			breadth_id   UUID    NULL,
			label        VARCHAR(64) NOT NULL,
			PRIMARY KEY (tenant_id, row_id),
			CONSTRAINT keyed_breadth_fk FOREIGN KEY (breadth_id)
				REFERENCES breadth (id) ON DELETE SET NULL
		);

		-- Seed: a few rows that exercise the round-trip.
		INSERT INTO breadth (
			id, small_n, int_n, big_n, money, ratio, precise,
			code, label, payload,
			calendar_day, clock_time, made_at, made_at_tz, score
		) VALUES
			(
				'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',
				42, 123456, 9876543210, 12345.67, 1.5::real, 2.71828182845,
				'CODE0001', 'first label', '\x68656c6c6f'::bytea,
				DATE '2026-05-15', TIME '12:34:56.789', TIMESTAMP '2026-05-15 12:34:56.123',
				TIMESTAMPTZ '2026-05-15 12:34:56.123-07', 100
			),
			(
				'bbbbbbbb-cccc-dddd-eeee-ffffffffffff',
				-7, -123456789, -111222333444, -0.01, 0.5::real, 3.14159265358,
				'CODE0002', 'second label', NULL,
				DATE '2025-12-31', TIME '00:00:00.000', TIMESTAMP '2025-12-31 23:59:59.999',
				TIMESTAMPTZ '2025-12-31 23:59:59.999+00', 0
			);

		INSERT INTO keyed (tenant_id, row_id, breadth_id, label) VALUES
			(1, 1, 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee', 'first label'),
			(1, 2, 'bbbbbbbb-cccc-dddd-eeee-ffffffffffff', 'second label'),
			(2, 1, NULL, 'orphan');
	`
	applyPGDDL(t, pgSource, seedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	mig := &Migrator{
		Source:    pgEng,
		Target:    mysqlEng,
		SourceDSN: pgSource,
		TargetDSN: mysqlTarget,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	sr, err := mysqlEng.OpenSchemaReader(ctx, mysqlTarget)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer migcore.CloseIf(sr)
	target, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}

	breadth := findTable(target, "breadth")
	keyed := findTable(target, "keyed")
	if breadth == nil || keyed == nil {
		t.Fatalf("missing target tables; have %v", targetTableNames(target))
	}

	rr, err := mysqlEng.OpenRowReader(ctx, mysqlTarget)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer migcore.CloseIf(rr)
	breadthRows := readAll(t, ctx, rr, breadth)
	keyedRows := readAll(t, ctx, rr, keyed)
	if len(breadthRows) != 2 {
		t.Errorf("breadth rows = %d; want 2", len(breadthRows))
	}
	if len(keyedRows) != 3 {
		t.Errorf("keyed rows = %d; want 3", len(keyedRows))
	}

	t.Run("numeric", func(t *testing.T) {
		// PG SMALLINT → MySQL SMALLINT → ir.Integer{Width:16}
		if c := findColumn(breadth, "small_n"); c == nil {
			t.Fatal("breadth.small_n missing")
		} else if intT, ok := c.Type.(ir.Integer); !ok || intT.Width != 16 {
			t.Errorf("breadth.small_n type = %#v; want ir.Integer{Width:16}", c.Type)
		}
		// PG INTEGER → MySQL INT → ir.Integer{Width:32}
		if c := findColumn(breadth, "int_n"); c == nil {
			t.Fatal("breadth.int_n missing")
		} else if intT, ok := c.Type.(ir.Integer); !ok || intT.Width != 32 {
			t.Errorf("breadth.int_n type = %#v; want ir.Integer{Width:32}", c.Type)
		}
		// PG BIGINT → MySQL BIGINT → ir.Integer{Width:64}
		if c := findColumn(breadth, "big_n"); c == nil {
			t.Fatal("breadth.big_n missing")
		} else if intT, ok := c.Type.(ir.Integer); !ok || intT.Width != 64 {
			t.Errorf("breadth.big_n type = %#v; want ir.Integer{Width:64}", c.Type)
		}
		// PG NUMERIC(15,2) → MySQL DECIMAL(15,2) → ir.Decimal{15,2}
		if c := findColumn(breadth, "money"); c == nil {
			t.Fatal("breadth.money missing")
		} else if dT, ok := c.Type.(ir.Decimal); !ok || dT.Precision != 15 || dT.Scale != 2 {
			t.Errorf("breadth.money type = %#v; want ir.Decimal{Precision:15, Scale:2}", c.Type)
		}
		// PG REAL → MySQL FLOAT → ir.Float{FloatSingle}
		if c := findColumn(breadth, "ratio"); c == nil {
			t.Fatal("breadth.ratio missing")
		} else if fT, ok := c.Type.(ir.Float); !ok || fT.Precision != ir.FloatSingle {
			t.Errorf("breadth.ratio type = %#v; want ir.Float{FloatSingle}", c.Type)
		}
		// PG DOUBLE PRECISION → MySQL DOUBLE → ir.Float{FloatDouble}
		if c := findColumn(breadth, "precise"); c == nil {
			t.Fatal("breadth.precise missing")
		} else if fT, ok := c.Type.(ir.Float); !ok || fT.Precision != ir.FloatDouble {
			t.Errorf("breadth.precise type = %#v; want ir.Float{FloatDouble}", c.Type)
		}

		// Row values: small_n positive and negative.
		if v, ok := breadthRows[0]["small_n"].(int64); !ok || v != 42 {
			t.Errorf("breadthRows[0].small_n = %#v; want 42", breadthRows[0]["small_n"])
		}
		if v, ok := breadthRows[1]["small_n"].(int64); !ok || v != -7 {
			t.Errorf("breadthRows[1].small_n = %#v; want -7", breadthRows[1]["small_n"])
		}
	})

	t.Run("string", func(t *testing.T) {
		// PG CHAR(8) → MySQL CHAR(8) → ir.Char{Length:8}
		if c := findColumn(breadth, "code"); c == nil {
			t.Fatal("breadth.code missing")
		} else if ch, ok := c.Type.(ir.Char); !ok || ch.Length != 8 {
			t.Errorf("breadth.code type = %#v; want ir.Char{Length:8}", c.Type)
		}
		// PG VARCHAR(64) → MySQL VARCHAR(64) → ir.Varchar{Length:64}
		if c := findColumn(breadth, "label"); c == nil {
			t.Fatal("breadth.label missing")
		} else if v, ok := c.Type.(ir.Varchar); !ok || v.Length != 64 {
			t.Errorf("breadth.label type = %#v; want ir.Varchar{Length:64}", c.Type)
		}
		// Row values arrived intact.
		if v, ok := breadthRows[0]["label"].(string); !ok || v != "first label" {
			t.Errorf("breadthRows[0].label = %#v; want 'first label'", breadthRows[0]["label"])
		}
	})

	t.Run("binary", func(t *testing.T) {
		// PG BYTEA (unbounded) → MySQL BLOB-family. Accept any
		// ir.Blob/ir.Varbinary variant rather than pinning the
		// specific MySQL TINYBLOB/BLOB/MEDIUMBLOB/LONGBLOB choice
		// since either side may evolve its mapping.
		c := findColumn(breadth, "payload")
		if c == nil {
			t.Fatal("breadth.payload missing")
		}
		switch c.Type.(type) {
		case ir.Blob, ir.Varbinary, ir.Binary:
			// OK; any byte-string type accepted.
		default:
			t.Errorf("breadth.payload type = %#v; want a byte-string (ir.Blob/Varbinary/Binary)", c.Type)
		}
		// Non-null payload should arrive as []byte == "hello".
		if v, ok := breadthRows[0]["payload"].([]byte); !ok {
			t.Errorf("breadthRows[0].payload type = %T; want []byte", breadthRows[0]["payload"])
		} else if string(v) != "hello" {
			t.Errorf("breadthRows[0].payload = %q; want 'hello'", v)
		}
		// NULL payload should arrive as nil.
		if breadthRows[1]["payload"] != nil {
			t.Errorf("breadthRows[1].payload = %#v; want nil", breadthRows[1]["payload"])
		}
	})

	t.Run("datetime", func(t *testing.T) {
		// PG DATE → MySQL DATE → ir.Date{}
		if c := findColumn(breadth, "calendar_day"); c == nil {
			t.Fatal("breadth.calendar_day missing")
		} else if _, ok := c.Type.(ir.Date); !ok {
			t.Errorf("breadth.calendar_day type = %#v; want ir.Date", c.Type)
		}
		// PG TIME(3) → MySQL TIME(3) → ir.Time{Precision:3}
		if c := findColumn(breadth, "clock_time"); c == nil {
			t.Fatal("breadth.clock_time missing")
		} else if ti, ok := c.Type.(ir.Time); !ok || ti.Precision != 3 {
			t.Errorf("breadth.clock_time type = %#v; want ir.Time{Precision:3}", c.Type)
		}
		// PG TIMESTAMP(3) → MySQL DATETIME(3) or TIMESTAMP(3) — accept either.
		// The IR's DateTime/Timestamp distinction is engine-specific.
		c := findColumn(breadth, "made_at")
		if c == nil {
			t.Fatal("breadth.made_at missing")
		}
		switch ty := c.Type.(type) {
		case ir.DateTime:
			if ty.Precision != 3 {
				t.Errorf("breadth.made_at precision = %d; want 3", ty.Precision)
			}
		case ir.Timestamp:
			if ty.Precision != 3 {
				t.Errorf("breadth.made_at precision = %d; want 3", ty.Precision)
			}
			if ty.WithTimeZone {
				t.Errorf("breadth.made_at = TIMESTAMP WITH TIME ZONE; want without TZ")
			}
		default:
			t.Errorf("breadth.made_at type = %#v; want ir.DateTime or ir.Timestamp", c.Type)
		}
		// PG TIMESTAMPTZ → MySQL TIMESTAMP (MySQL stores as UTC,
		// surfaces as ir.Timestamp{WithTimeZone:true}).
		if c := findColumn(breadth, "made_at_tz"); c == nil {
			t.Fatal("breadth.made_at_tz missing")
		} else if ty, ok := c.Type.(ir.Timestamp); !ok {
			t.Errorf("breadth.made_at_tz type = %#v; want ir.Timestamp", c.Type)
		} else if !ty.WithTimeZone {
			t.Errorf("breadth.made_at_tz: WithTimeZone=false; want true (TIMESTAMPTZ)")
		}
	})

	t.Run("identifier_uuid", func(t *testing.T) {
		// PG UUID → MySQL CHAR(36) is the documented translation. The
		// MySQL schema reader surfaces it as ir.Char{Length:36}, since
		// MySQL has no native UUID type.
		if c := findColumn(breadth, "id"); c == nil {
			t.Fatal("breadth.id missing")
		} else if ch, ok := c.Type.(ir.Char); !ok || ch.Length != 36 {
			t.Errorf("breadth.id type = %#v; want ir.Char{Length:36} (PG UUID → MySQL CHAR(36))", c.Type)
		}
		// Row value: UUID arrives as 36-char canonical hyphenated form.
		if v, ok := breadthRows[0]["id"].(string); !ok || v != "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee" {
			t.Errorf("breadthRows[0].id = %#v; want 36-char UUID", breadthRows[0]["id"])
		}
	})

	t.Run("constraints", func(t *testing.T) {
		// Composite primary key on (tenant_id, row_id).
		if keyed.PrimaryKey == nil {
			t.Fatal("keyed PK missing")
		}
		if len(keyed.PrimaryKey.Columns) != 2 {
			t.Errorf("keyed PK cols = %d; want 2", len(keyed.PrimaryKey.Columns))
		} else {
			if keyed.PrimaryKey.Columns[0].Column != "tenant_id" ||
				keyed.PrimaryKey.Columns[1].Column != "row_id" {
				t.Errorf("keyed PK cols = %#v; want [tenant_id, row_id]", keyed.PrimaryKey.Columns)
			}
		}
		// Multi-column UNIQUE on (label, code) preserved on breadth.
		hasMultiUniq := false
		for _, ix := range breadth.Indexes {
			if !ix.Unique || len(ix.Columns) != 2 {
				continue
			}
			c0, c1 := ix.Columns[0].Column, ix.Columns[1].Column
			if (c0 == "label" && c1 == "code") || (c0 == "code" && c1 == "label") {
				hasMultiUniq = true
				break
			}
		}
		if !hasMultiUniq {
			t.Errorf("breadth indexes = %#v; want a 2-col unique on (label, code)", breadth.Indexes)
		}
		// FK ON DELETE SET NULL preserved on keyed (different from
		// the conservative test's CASCADE).
		if len(keyed.ForeignKeys) != 1 {
			t.Fatalf("keyed FKs = %d; want 1", len(keyed.ForeignKeys))
		}
		fk := keyed.ForeignKeys[0]
		if fk.ReferencedTable != "breadth" {
			t.Errorf("keyed FK ref = %q; want 'breadth'", fk.ReferencedTable)
		}
		if fk.OnDelete != ir.FKActionSetNull {
			t.Errorf("keyed FK on-delete = %v; want SET NULL", fk.OnDelete)
		}
	})
}
