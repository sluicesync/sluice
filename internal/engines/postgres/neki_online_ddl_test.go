// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestNekiOnlineDDLRequiresQualifiedNames is the PREMISE CHECK the file header
// promises, and it is the reason this test exists rather than a comment.
//
// Neki refuses an unqualified table name in a schema migration — measured:
//
//	OnlineDDL workflow …: statement 1 (an unqualified name, soak_rows_pg) is
//	not allowed in a schema migration: schema-qualify every table name; a
//	migration does not guess the search path
//
// sluice's emitter happens to qualify already (`ON "schema"."table"`), so the
// online-DDL path needs no rewriting — but that is a FACT ABOUT ANOTHER
// FUNCTION, and the project's rule is that a safety argument citing such a fact
// owes it a check. If someone ever makes emitCreateIndex emit a bare table
// name, the direct path keeps working (search_path resolves it) and only Neki
// breaks, ~36 minutes after submission. This is the gate that fails first.
func TestNekiOnlineDDLRequiresQualifiedNames(t *testing.T) {
	t.Parallel()

	idx := &ir.Index{Name: "ix_created", Columns: []ir.IndexColumn{{Column: "created_at"}}}
	stmt, err := emitCreateIndex("public", "soak_rows_pg", idx, emitOpts{})
	if err != nil {
		t.Fatalf("emitCreateIndex: %v", err)
	}
	if stmt == "" {
		t.Fatal("emitCreateIndex produced nothing for an ordinary single-column index")
	}

	// The ON clause must name schema AND table, quoted.
	if !strings.Contains(stmt, `ON "public"."soak_rows_pg"`) {
		t.Fatalf("the emitted CREATE INDEX does not schema-qualify its table, so Neki's online DDL "+
			"will refuse it (\"a migration does not guess the search path\"). stmt: %s", stmt)
	}
}

// TestNekiStripConcurrently pins the other documented constraint: an online-DDL
// index build must not carry CONCURRENTLY.
//
// sluice does not emit it today, so this guards a future change rather than
// current behaviour — the failure it prevents is expensive and late (the
// workflow is accepted and fails during the build), which is exactly when a
// cheap upfront guard is worth having.
func TestNekiStripConcurrently(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "uppercase in the middle",
			in:   `CREATE INDEX CONCURRENTLY "ix" ON "public"."t" ("c")`,
			want: `CREATE INDEX "ix" ON "public"."t" ("c")`,
		},
		{
			name: "lowercase",
			in:   `create index concurrently "ix" on "public"."t" ("c")`,
			want: `create index "ix" on "public"."t" ("c")`,
		},
		{
			name: "absent — left exactly alone",
			in:   `CREATE INDEX "ix" ON "public"."t" ("c")`,
			want: `CREATE INDEX "ix" ON "public"."t" ("c")`,
		},
		{
			// The word must not be stripped out of an IDENTIFIER that merely
			// contains it — a column or index legitimately named for it.
			name: "a column named concurrently_at is untouched",
			in:   `CREATE INDEX "ix" ON "public"."t" ("concurrently_at")`,
			want: `CREATE INDEX "ix" ON "public"."t" ("concurrently_at")`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := nekiStripConcurrently(tc.in); got != tc.want {
				t.Fatalf("nekiStripConcurrently\n got: %s\nwant: %s", got, tc.want)
			}
		})
	}
}

// TestNekiOnlineDDLWorkflowName pins the properties the name must have: it is
// DETERMINISTIC (so a re-run addresses the same workflow and can clean up a
// previous attempt's leftovers rather than orphaning them), it is safe as an
// identifier-ish token, it is bounded in length, and it stays DISTINCT across
// tables and indexes.
func TestNekiOnlineDDLWorkflowName(t *testing.T) {
	t.Parallel()

	t.Run("deterministic", func(t *testing.T) {
		t.Parallel()
		a := nekiOnlineDDLWorkflowName("orders", "ix_created")
		b := nekiOnlineDDLWorkflowName("orders", "ix_created")
		if a != b {
			t.Fatalf("not deterministic: %q vs %q — a re-run could not find and clean up the "+
				"previous attempt's workflow", a, b)
		}
	})

	t.Run("sanitises awkward identifiers", func(t *testing.T) {
		t.Parallel()
		got := nekiOnlineDDLWorkflowName(`my table"; DROP`, "ix-a.b")
		for _, r := range got {
			ok := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_'
			if !ok {
				t.Fatalf("workflow name %q contains %q, which is not [A-Za-z0-9_]", got, r)
			}
		}
	})

	t.Run("bounded in length", func(t *testing.T) {
		t.Parallel()
		got := nekiOnlineDDLWorkflowName(strings.Repeat("t", 200), strings.Repeat("i", 200))
		if len(got) > 60 {
			t.Fatalf("workflow name is %d chars (%q) — Neki rejects an over-long name", len(got), got)
		}
	})

	t.Run("distinct across tables and across indexes", func(t *testing.T) {
		t.Parallel()
		// Distinctness must survive truncation, which is where a naive
		// prefix-only trim loses it.
		long := strings.Repeat("orders_archive_", 8)
		a := nekiOnlineDDLWorkflowName(long, "ix_created_at")
		b := nekiOnlineDDLWorkflowName(long, "ix_updated_at")
		if a == b {
			t.Fatalf("two different indexes on the same table produced the same workflow name %q — "+
				"the second build would collide with the first", a)
		}
		c := nekiOnlineDDLWorkflowName("orders", "ix_x")
		d := nekiOnlineDDLWorkflowName("payments", "ix_x")
		if c == d {
			t.Fatalf("the same index name on two tables produced the same workflow name %q", c)
		}
	})

	t.Run("carries the sluice_ ownership prefix", func(t *testing.T) {
		t.Parallel()
		if got := nekiOnlineDDLWorkflowName("orders", "ix_created"); !strings.HasPrefix(got, "sluice_") {
			t.Errorf("workflow name %q does not mark itself as sluice's; an operator listing workflows "+
				"on the branch cannot tell which are ours", got)
		}
	})
}

// TestNekiAllShardsReady pins the readiness fold, including the vacuous-truth
// trap that would complete a workflow which had built nothing.
//
// The fold lives in Go because the server-side form needs a correlated range
// function, which a Neki router refuses (NK013, measured — the submit
// succeeded and the very first poll failed). So this logic is entirely ours and
// entirely untested by the platform.
func TestNekiAllShardsReady(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		json       string
		wantReady  bool
		wantShards int
	}{
		{
			name:       "single shard ready",
			json:       `{"sh1": {"ddl_status": "running", "current_readiness": true}}`,
			wantReady:  true,
			wantShards: 1,
		},
		{
			name:       "single shard not ready",
			json:       `{"sh1": {"ddl_status": "running", "current_readiness": false}}`,
			wantReady:  false,
			wantShards: 1,
		},
		{
			// EVERY shard must be ready — one laggard blocks cutover.
			name: "one of three still building",
			json: `{"sh1": {"current_readiness": true},
			        "sh2": {"current_readiness": false},
			        "sh3": {"current_readiness": true}}`,
			wantReady:  false,
			wantShards: 3,
		},
		{
			name: "all three ready",
			json: `{"sh1": {"current_readiness": true},
			        "sh2": {"current_readiness": true},
			        "sh3": {"current_readiness": true}}`,
			wantReady:  true,
			wantShards: 3,
		},
		{
			// THE VACUOUS-TRUTH TRAP. "every shard is ready" is trivially true
			// of no shards. Treating an empty map as ready would call
			// workflow_complete on a workflow that had not built anything.
			name:       "empty shard map is NOT ready",
			json:       `{}`,
			wantReady:  false,
			wantShards: 0,
		},
		{
			// A shard that omits the field has not declared itself ready.
			name:       "missing current_readiness is not ready",
			json:       `{"sh1": {"ddl_status": "running"}}`,
			wantReady:  false,
			wantShards: 1,
		},
		{
			// Unparseable input must not read as ready either.
			name:       "garbage is not ready",
			json:       `not json at all`,
			wantReady:  false,
			wantShards: 0,
		},
		{
			name:       "null is not ready",
			json:       `null`,
			wantReady:  false,
			wantShards: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ready, shards := nekiAllShardsReady(tc.json)
			if ready != tc.wantReady {
				t.Fatalf("ready = %v, want %v — a wrong TRUE cuts over a workflow that has not "+
					"finished building; a wrong FALSE waits out the four-hour envelope on a "+
					"finished one. json: %s", ready, tc.wantReady, tc.json)
			}
			if shards != tc.wantShards {
				t.Errorf("shards = %d, want %d (used in the failure message so an operator can "+
					"tell 'no shards reported' from 'a shard is lagging')", shards, tc.wantShards)
			}
		})
	}
}
