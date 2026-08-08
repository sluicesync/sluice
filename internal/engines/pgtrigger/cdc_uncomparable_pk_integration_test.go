//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Roadmap item 154 — the reachability half.
//
// laneapply.PKChangedUpdate compared two primary-key values with Go's `==`,
// which PANICS when the dynamic type is uncomparable. This file is the pin
// that the crash was reachable on a SUPPORTED configuration rather than a
// theoretical one, and it deliberately ground-truths every link of that
// argument against a real server instead of asserting the conclusion:
//
//  1. Postgres PERMITS an array and a jsonb PRIMARY KEY (btree `array_ops`
//     / `jsonb_ops`). The CREATE TABLE below is that check — the premise-
//     naming step; if a future PG major stopped allowing it, this fails
//     loudly rather than leaving the claim in a comment.
//  2. pgtrigger's Setup ACCEPTS such a table. `text[]` columns are already
//     pinned as accepted by TestSetup_RefusesJSONArrayColumn's negative
//     half — only `json[]`/`jsonb[]` are refused — but neither pin put one
//     in the KEY.
//  3. The reader decodes the captured to_jsonb payload to `[]any` /
//     `map[string]any`, asserted here by Go type rather than assumed.
//  4. That exact decoded value, fed to the exact function both concurrent
//     appliers call, returns instead of panicking.
//
// Step 4 is what makes this a BINDING test rather than two facts sitting
// next to each other: laneapply's own family matrix pins the comparison
// against a hand-built `[]any`, and this pins that a real reader on a real
// table actually produces one. Neither alone says the crash was reachable.

package pgtrigger

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/laneapply"
)

// TestCDCReader_UncomparablePrimaryKeyReachesTheLaneRouter covers both
// uncomparable PK families the trigger payload can carry, each at the two
// shapes that matter to the router: a non-key UPDATE (must report "key
// unchanged" → stay on the fast lane path) and a key-MIGRATING UPDATE (must
// report "key changed" → barrier). One family is not enough: the array case
// decodes through normalizePayloadValue's `[]any` arm and the jsonb case
// does not descend at all, so they are different decode paths that happen to
// share a crash.
func TestCDCReader_UncomparablePrimaryKeyReachesTheLaneRouter(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	// Step 1 — the environmental premise, checked rather than cited.
	applyPGSQL(t, dsn, `
		CREATE TABLE arr_pk  (k TEXT[] PRIMARY KEY, v TEXT);
		CREATE TABLE json_pk (k JSONB  PRIMARY KEY, v TEXT);
	`)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Step 2 — setup must not refuse either table.
	plan, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"arr_pk", "json_pk"}, Schema: "public"})
	if err != nil {
		t.Fatalf("Setup: an array/jsonb PRIMARY KEY is a legal PG shape and must not be refused: %v (plan=%+v)", err, plan)
	}

	e := Engine{}
	reader, err := e.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := reader.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	out, err := reader.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	// Two UPDATEs per table: one leaving the key alone, one migrating it.
	// The seed INSERTs are captured too, so the expected event count is 8.
	applyPGSQL(t, dsn, `
		INSERT INTO arr_pk  (k, v) VALUES ('{a,b}', 'one');
		INSERT INTO json_pk (k, v) VALUES ('{"q": 1}', 'one');
		UPDATE arr_pk  SET v = 'two' WHERE k = '{a,b}';
		UPDATE json_pk SET v = 'two' WHERE k = '{"q": 1}';
		UPDATE arr_pk  SET k = '{a,c}' WHERE k = '{a,b}';
		UPDATE json_pk SET k = '{"q": 2}' WHERE k = '{"q": 1}';
	`)

	got := drainEvents(t, out, 6, 20*time.Second)
	if len(got) != 6 {
		t.Fatalf("got %d events; want 6 — %+v", len(got), got)
	}

	updates := map[string][]ir.Update{}
	for _, ev := range got {
		if u, ok := ev.(ir.Update); ok {
			updates[u.Table] = append(updates[u.Table], u)
		}
	}

	for _, tc := range []struct {
		table string
		// wantKind is the Go dynamic type the decode must produce for the
		// key column. Asserted by a type switch rather than %T so a change
		// of shape is a compile-visible edit, not a string mismatch.
		isWantKind func(any) bool
		kindName   string
	}{
		{"arr_pk", func(v any) bool { _, ok := v.([]any); return ok }, "[]any"},
		{"json_pk", func(v any) bool { _, ok := v.(map[string]any); return ok }, "map[string]any"},
	} {
		us := updates[tc.table]
		if len(us) != 2 {
			t.Errorf("%s: got %d UPDATE events; want 2 (non-key, then key-migrating)", tc.table, len(us))
			continue
		}
		for i, u := range us {
			if u.Before == nil || u.After == nil {
				t.Fatalf("%s update %d: Before/After must both be present for the comparison to be reached (before=%v after=%v)",
					tc.table, i, u.Before, u.After)
			}
			// Step 3 — the decoded key is genuinely uncomparable.
			for _, img := range []struct {
				name string
				row  ir.Row
			}{{"Before", u.Before}, {"After", u.After}} {
				if !tc.isWantKind(img.row["k"]) {
					t.Fatalf("%s update %d: %s[k] = %v (%T); want %s — this pin is vacuous unless the key is uncomparable",
						tc.table, i, img.name, img.row["k"], img.row["k"], tc.kindName)
				}
			}
		}

		// Step 4 — the exact value, through the exact function the
		// postgres and mysql concurrent appliers call. Before item 154
		// each of these four calls panicked with "comparing uncomparable
		// type"; the run never got as far as a wrong answer.
		if laneapply.PKChangedUpdate(us[0], []string{"k"}) {
			t.Errorf("%s: PKChangedUpdate on a non-key UPDATE = true; want false (an unnecessary barrier on every such row)", tc.table)
		}
		if !laneapply.PKChangedUpdate(us[1], []string{"k"}) {
			t.Errorf("%s: PKChangedUpdate on a key-MIGRATING UPDATE = false; want true — the old-key and new-key "+
				"effects would apply on unordered lanes", tc.table)
		}
	}
}
