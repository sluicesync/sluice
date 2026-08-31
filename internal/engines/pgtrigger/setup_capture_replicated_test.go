// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the ADR-0185 render shape: `trigger setup
// --capture-replicated-writes` emits the ENABLE ALWAYS ALTERs for BOTH
// members of every table's trigger pair and records the posture in the
// meta upsert; the default render emits neither, byte-preserving today's
// plain install. The posture-column migration (ADD COLUMN IF NOT EXISTS)
// rides EVERY render — it is what upgrades a pre-v3 install's meta table
// on the next setup re-run.

package pgtrigger

import (
	"fmt"
	"strings"
	"testing"
)

func TestRenderSetupDDL_CaptureReplicatedWrites(t *testing.T) {
	t.Parallel()
	specs := []tableTriggerSpec{
		{Name: "orders", PKCols: []string{"id"}},
		{Name: "line_items", PKCols: []string{"tenant_id", "order_id"}},
	}
	plain := strings.Join(renderSetupDDL("public", specs, true, CapturePayloadFull, false), "\n")
	optIn := strings.Join(renderSetupDDL("public", specs, true, CapturePayloadFull, true), "\n")

	t.Run("opt-in emits ENABLE ALWAYS for both triggers of every table", func(t *testing.T) {
		for _, want := range []string{
			`ALTER TABLE "public"."orders" ENABLE ALWAYS TRIGGER "sluice_capture"`,
			`ALTER TABLE "public"."orders" ENABLE ALWAYS TRIGGER "sluice_capture_truncate"`,
			`ALTER TABLE "public"."line_items" ENABLE ALWAYS TRIGGER "sluice_capture"`,
			`ALTER TABLE "public"."line_items" ENABLE ALWAYS TRIGGER "sluice_capture_truncate"`,
		} {
			if !strings.Contains(optIn, want) {
				t.Errorf("opt-in render missing %q — a member of the pair left plain silently loses its replicated writes", want)
			}
		}
	})

	t.Run("default render emits no ENABLE ALWAYS anywhere", func(t *testing.T) {
		if strings.Contains(plain, "ENABLE ALWAYS") {
			t.Errorf("plain render emits ENABLE ALWAYS — the opt-in leaked into the default posture:\n%s", plain)
		}
	})

	t.Run("meta upsert records the posture, both values", func(t *testing.T) {
		// The schema version comes from the constant, not a literal: it moves
		// (v3 → v4 for the SEC-2 evidence columns) and this pin is about the
		// posture, not the version.
		if want := fmt.Sprintf("%s) VALUES (TRUE, %d, true)", metaCaptureReplicatedCol, ChangeLogSchemaVer); !strings.Contains(optIn, want) {
			t.Errorf("opt-in render's meta upsert does not record capture_replicated_writes=true — the CDC open would grade the 'A' triggers against a recorded false and refuse every open")
		}
		if want := fmt.Sprintf("%s) VALUES (TRUE, %d, false)", metaCaptureReplicatedCol, ChangeLogSchemaVer); !strings.Contains(plain, want) {
			t.Errorf("plain render's meta upsert does not record capture_replicated_writes=false — a plain re-run of an opt-in install would leave the stale true recorded")
		}
	})

	t.Run("the posture-column migration rides every render", func(t *testing.T) {
		want := `ALTER TABLE "public"."` + ChangeLogMetaTable + `" ADD COLUMN IF NOT EXISTS ` + metaCaptureReplicatedCol
		for name, ddl := range map[string]string{"plain": plain, "opt-in": optIn} {
			if !strings.Contains(ddl, want) {
				t.Errorf("%s render missing the ADD COLUMN IF NOT EXISTS migration — a pre-v3 install's upsert would fail on the absent column", name)
			}
		}
	})
}
