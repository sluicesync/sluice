// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// N-16 unit pins — the change-log index diet and the baked-PK trigger
// shape. The two legacy indexes (an exact duplicate of the PK's
// implicit index, and a (schema_name, table_name, id) composite no
// engine query reads) were pure write amplification on every captured
// source DML; the capture trigger's per-fired-row PK catalog lookup was
// the same class of overhead. These pins keep both from creeping back
// and pin the TG_ARGV baking shape across every payload mode.

package pgtrigger

import (
	"strings"
	"testing"
)

// TestRenderSetupDDL_NoChangeLogIndexes pins the index diet: the setup
// DDL must create NO index on the change-log (the BIGSERIAL PK's
// implicit index is the only one any engine query needs — poll /
// anchor / settle-clamp / prune / stats all key on id and txid), and
// must emit the idempotent DROPs that converge a pre-N-16 install.
func TestRenderSetupDDL_NoChangeLogIndexes(t *testing.T) {
	t.Parallel()
	stmts := renderSetupDDL(
		"public",
		[]tableTriggerSpec{{Name: "orders", PKCols: []string{"id"}}},
		true, CapturePayloadFull,
	)
	for _, s := range stmts {
		// Prefix match, not Contains: the DDL event trigger's TAG list
		// legitimately names 'CREATE INDEX' as a watched command tag.
		if strings.HasPrefix(s, "CREATE INDEX") {
			t.Errorf("setup DDL creates an index on the change-log (write amplification per captured DML, N-16):\n%s", s)
		}
	}
	joined := strings.Join(stmts, "\n")
	for _, want := range []string{
		`DROP INDEX IF EXISTS "public"."sluice_change_log_id_idx"`,
		`DROP INDEX IF EXISTS "public"."sluice_change_log_table_idx"`,
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("setup DDL missing the pre-N-16 convergence statement %q", want)
		}
	}
}

// TestRenderSetupDDL_BakedPKArgs pins the TG_ARGV shape: each per-table
// CREATE TRIGGER carries the table's PK column list as a JSON array
// (ADR-0066 §3), composite PKs keep conkey order, and identifier
// escaping survives the JSON + SQL-literal nesting. The vestigial v1
// table-name argument must be gone.
func TestRenderSetupDDL_BakedPKArgs(t *testing.T) {
	t.Parallel()
	stmts := renderSetupDDL(
		"public",
		[]tableTriggerSpec{
			{Name: "orders", PKCols: []string{"id"}},
			{Name: "line_items", PKCols: []string{"tenant_id", "order_id"}},
			{Name: "weird", PKCols: []string{`na"me`, "o'clock"}},
		},
		false, CapturePayloadFull,
	)
	joined := strings.Join(stmts, "\n")
	for _, want := range []string{
		`ON "public"."orders" FOR EACH ROW EXECUTE FUNCTION "public"."sluice_capture_change"('["id"]')`,
		`ON "public"."line_items" FOR EACH ROW EXECUTE FUNCTION "public"."sluice_capture_change"('["tenant_id","order_id"]')`,
		// JSON escapes the double quote; quoteSQLString doubles the single quote.
		`ON "public"."weird" FOR EACH ROW EXECUTE FUNCTION "public"."sluice_capture_change"('["na\"me","o''clock"]')`,
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("setup DDL missing baked-PK trigger shape %q in:\n%s", want, joined)
		}
	}
	if strings.Contains(joined, `"sluice_capture_change"('orders')`) {
		t.Error("setup DDL still passes the vestigial v1 table-name TG_ARGV argument")
	}
}

// TestPKColsJSON pins the TG_ARGV[0] payload encoding, including the
// empty (§14-refused, dry-run-only) shape.
func TestPKColsJSON(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   []string
		want string
	}{
		{nil, "[]"},
		{[]string{}, "[]"},
		{[]string{"id"}, `["id"]`},
		{[]string{"tenant_id", "order_id"}, `["tenant_id","order_id"]`},
		{[]string{`na"me`}, `["na\"me"]`},
	}
	for _, c := range cases {
		if got := pkColsJSON(c.in); got != c.want {
			t.Errorf("pkColsJSON(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestRenderCaptureRowFunction_BakedPKList pins the function-body half
// of the bake across EVERY payload mode (the PK-list block is shared
// scaffold, but pin the class, not the representative): the body reads
// TG_ARGV[0] as JSON, never touches the catalogs, and carries all three
// loud-failure guards — missing bake, empty PK list, and the
// stale-baked-list projection guard that refuses writes after a
// post-setup PK ALTER instead of capturing rows keyed on the wrong
// columns.
func TestRenderCaptureRowFunction_BakedPKList(t *testing.T) {
	t.Parallel()
	for _, mode := range []CapturePayload{CapturePayloadFull, CapturePayloadChanged, CapturePayloadMinimal} {
		mode := mode
		t.Run(string(mode), func(t *testing.T) {
			t.Parallel()
			ddl := renderCaptureRowFunction("public", `"public"."sluice_change_log"`, mode)

			for _, want := range []string{
				// The baked-list parse.
				"v_pk_cols := ARRAY(SELECT jsonb_array_elements_text(TG_ARGV[0]::jsonb))",
				// Guard 1: manually-attached trigger with no baked list.
				"carries no baked PK column list (TG_ARGV[0])",
				// Guard 2: empty list (the §14 no-PK refusal, defensively).
				"has no PRIMARY KEY; refuse-loudly per ADR-0066 §14",
				// Guard 3: stale bake after a post-setup PK ALTER.
				"no longer matches the row image",
				"(SELECT count(*) FROM jsonb_object_keys(v_pk)) <> cardinality(v_pk_cols)",
			} {
				if !strings.Contains(ddl, want) {
					t.Errorf("capture function (mode %s) missing %q", mode, want)
				}
			}
			// The per-fired-row catalog lookup must be gone (N-16).
			for _, gone := range []string{"pg_constraint", "pg_attribute", "TG_RELID"} {
				if strings.Contains(ddl, gone) {
					t.Errorf("capture function (mode %s) still references %q — the per-fired-row catalog lookup was removed by N-16", mode, gone)
				}
			}
		})
	}
}
