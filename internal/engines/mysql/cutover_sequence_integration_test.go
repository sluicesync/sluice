//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for the MySQL cutover sequence primer (F10 /
// ADR-0062). Uses the shared mysqld container (two fresh databases,
// one for source, one for target), installs schemas with
// AUTO_INCREMENT columns, exercises the source-side reader, applies
// the priming pass against the target, and pins the per-table
// outcome (primed / noop / refused / skipped).
//
// Matrix:
//   - Standard `id BIGINT AUTO_INCREMENT` shape with source ahead of
//     target → "primed".
//   - Idempotent re-run → "noop" (no AUTO_INCREMENT regression).
//   - Composite PK without AUTO_INCREMENT → no action emitted.
//   - Target ahead of source by more than margin → "refused" +
//     ErrCutoverSequenceTargetAhead.

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestCutoverSequencePrimer_MySQL_PrimesPlusIdempotent pins the cold
// path: prime once → expect "primed"; prime again → expect "noop"
// without regressing AUTO_INCREMENT.
func TestCutoverSequencePrimer_MySQL_PrimesPlusIdempotent(t *testing.T) {
	srcDSN, srcCleanup := newSharedDB(t, "sluice_cutover_src")
	defer srcCleanup()
	tgtDSN, tgtCleanup := newSharedDB(t, "sluice_cutover_tgt")
	defer tgtCleanup()

	applyDDL(t, srcDSN, `
		CREATE TABLE orders (
			id   BIGINT NOT NULL AUTO_INCREMENT,
			name VARCHAR(64) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO orders (name) VALUES ('a'),('b'),('c'),('d'),('e');
	`)
	applyDDL(t, tgtDSN, `
		CREATE TABLE orders (
			id   BIGINT NOT NULL AUTO_INCREMENT,
			name VARCHAR(64) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	sr, err := Engine{}.OpenSchemaReader(ctx, srcDSN)
	if err != nil {
		t.Fatalf("open source reader: %v", err)
	}
	defer func() {
		if c, ok := sr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	mysr := sr.(*SchemaReader)
	schema, err := mysr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	states, err := mysr.ReadSequenceState(ctx, schema)
	if err != nil {
		t.Fatalf("read sequence state: %v", err)
	}
	if len(states) != 1 || states[0].Table != "orders" {
		t.Fatalf("source states = %+v; want one entry for orders", states)
	}
	if states[0].Value != 5 {
		t.Errorf("source last-issued = %d; want 5", states[0].Value)
	}

	sw, err := Engine{}.OpenSchemaWriter(ctx, tgtDSN)
	if err != nil {
		t.Fatalf("open target writer: %v", err)
	}
	mysw := sw.(*SchemaWriter)
	margin := int64(1000)
	report, err := mysw.PrimeSequences(ctx, schema, states, margin)
	if err != nil {
		t.Fatalf("first prime err = %v", err)
	}
	if len(report.Actions) != 1 {
		t.Fatalf("actions = %+v; want one", report.Actions)
	}
	a := report.Actions[0]
	if a.Outcome != "primed" {
		t.Errorf("outcome = %q; want %q (action = %+v)", a.Outcome, "primed", a)
	}
	// After prime, the next INSERT against the target should land at
	// source+margin+1 = 1006. Verify via a probe INSERT.
	verifyDB, err := sql.Open("mysql", tgtDSN)
	if err != nil {
		t.Fatalf("open verify db: %v", err)
	}
	defer func() { _ = verifyDB.Close() }()

	if _, err := verifyDB.ExecContext(ctx, "INSERT INTO orders (name) VALUES ('probe')"); err != nil {
		t.Fatalf("probe insert: %v", err)
	}
	var observed int64
	if err := verifyDB.QueryRowContext(ctx, "SELECT id FROM orders WHERE name = 'probe'").Scan(&observed); err != nil {
		t.Fatalf("scan observed: %v", err)
	}
	wantObserved := int64(5 + margin + 1) // 1006
	if observed != wantObserved {
		t.Errorf("probe id = %d; want %d (source=%d + margin=%d + 1)",
			observed, wantObserved, 5, margin)
	}

	// Idempotency. Re-read state (source still at 5) and re-prime;
	// the target has now issued ID 1006, so AUTO_INCREMENT is at
	// 1007 which is >= source+margin+1=1006 → noop.
	states2, err := mysr.ReadSequenceState(ctx, schema)
	if err != nil {
		t.Fatalf("read state 2nd time: %v", err)
	}
	report2, err := mysw.PrimeSequences(ctx, schema, states2, margin)
	if err != nil {
		t.Fatalf("second prime err = %v", err)
	}
	if len(report2.Actions) != 1 {
		t.Fatalf("second prime actions = %+v; want one", report2.Actions)
	}
	if report2.Actions[0].Outcome != "noop" {
		t.Errorf("second prime outcome = %q; want %q (Bug 74 idempotency pin)",
			report2.Actions[0].Outcome, "noop")
	}
}

// TestCutoverSequencePrimer_MySQL_RefusesTargetAhead pins the
// loud-failure refusal class: target AUTO_INCREMENT is far ahead of
// source+margin → refused + ErrCutoverSequenceTargetAhead.
func TestCutoverSequencePrimer_MySQL_RefusesTargetAhead(t *testing.T) {
	srcDSN, srcCleanup := newSharedDB(t, "sluice_cutover_refusal_src")
	defer srcCleanup()
	tgtDSN, tgtCleanup := newSharedDB(t, "sluice_cutover_refusal_tgt")
	defer tgtCleanup()

	applyDDL(t, srcDSN, `
		CREATE TABLE orders (
			id   BIGINT NOT NULL AUTO_INCREMENT,
			name VARCHAR(64) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO orders (name) VALUES ('a'),('b'),('c'),('d'),('e');
	`)
	// Target: operator INSERTed 50k rows post-cutover before running
	// the priming pass — AUTO_INCREMENT now well ahead.
	applyDDL(t, tgtDSN, `
		CREATE TABLE orders (
			id   BIGINT NOT NULL AUTO_INCREMENT,
			name VARCHAR(64) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB AUTO_INCREMENT=50000 DEFAULT CHARSET=utf8mb4;
	`)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	sr, _ := Engine{}.OpenSchemaReader(ctx, srcDSN)
	defer func() {
		if c, ok := sr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	mysr := sr.(*SchemaReader)
	schema, _ := mysr.ReadSchema(ctx)
	states, err := mysr.ReadSequenceState(ctx, schema)
	if err != nil {
		t.Fatalf("read sequence state: %v", err)
	}

	sw, _ := Engine{}.OpenSchemaWriter(ctx, tgtDSN)
	mysw := sw.(*SchemaWriter)
	report, err := mysw.PrimeSequences(ctx, schema, states, 1000)
	if !errors.Is(err, ir.ErrCutoverSequenceTargetAhead) {
		t.Errorf("err = %v; want ErrCutoverSequenceTargetAhead", err)
	}
	if report == nil || len(report.Actions) != 1 {
		t.Fatalf("report = %+v; want one action", report)
	}
	if report.Actions[0].Outcome != "refused" {
		t.Errorf("outcome = %q; want %q", report.Actions[0].Outcome, "refused")
	}
}

// TestCutoverSequencePrimer_MySQL_SkipsCompositePK pins the
// no-AUTO_INCREMENT path: a composite-PK table yields no entry in the
// source state and no action on the target.
func TestCutoverSequencePrimer_MySQL_SkipsCompositePK(t *testing.T) {
	srcDSN, srcCleanup := newSharedDB(t, "sluice_cutover_skip_src")
	defer srcCleanup()
	tgtDSN, tgtCleanup := newSharedDB(t, "sluice_cutover_skip_tgt")
	defer tgtCleanup()

	applyDDL(t, srcDSN, `
		CREATE TABLE memberships (
			user_id  BIGINT NOT NULL,
			group_id BIGINT NOT NULL,
			PRIMARY KEY (user_id, group_id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)
	applyDDL(t, tgtDSN, `
		CREATE TABLE memberships (
			user_id  BIGINT NOT NULL,
			group_id BIGINT NOT NULL,
			PRIMARY KEY (user_id, group_id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	sr, _ := Engine{}.OpenSchemaReader(ctx, srcDSN)
	defer func() {
		if c, ok := sr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	mysr := sr.(*SchemaReader)
	schema, _ := mysr.ReadSchema(ctx)
	states, err := mysr.ReadSequenceState(ctx, schema)
	if err != nil {
		t.Fatalf("read sequence state: %v", err)
	}
	if len(states) != 0 {
		t.Errorf("states = %+v; want empty (no AUTO_INCREMENT columns)", states)
	}

	sw, _ := Engine{}.OpenSchemaWriter(ctx, tgtDSN)
	mysw := sw.(*SchemaWriter)
	report, err := mysw.PrimeSequences(ctx, schema, states, 1000)
	if err != nil {
		t.Fatalf("PrimeSequences err = %v", err)
	}
	if len(report.Actions) != 0 {
		t.Errorf("actions = %+v; want empty (no AUTO_INCREMENT)", report.Actions)
	}
}
