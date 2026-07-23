// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"
	"testing"
)

// The sidecar-keyspace prototype routes EVERY control-table statement through
// controlTableRef / controlSchemaPredicate. These unit tests pin the seam:
// the unset path is byte-identical to the historical single-keyspace behaviour,
// and the set path qualifies with each identifier backtick-quoted separately
// (a bare `ks.table` would be one wrong identifier to MySQL/Vitess).

func TestControlTableRef(t *testing.T) {
	cases := []struct {
		name            string
		controlKeyspace string
		table           string
		want            string
	}{
		{"unset is bare", "", "sluice_cdc_state", "`sluice_cdc_state`"},
		{"set qualifies, separate backticks", "ctl", "sluice_cdc_state", "`ctl`.`sluice_cdc_state`"},
		{"set qualifies schema-history", "sidecar", schemaHistoryTableName, "`sidecar`.`sluice_cdc_schema_history`"},
		{"set qualifies lease table", "sidecar", shardConsolidationLeaseTableName, "`sidecar`.`sluice_shard_consolidation_lease`"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := controlTableRef(tc.controlKeyspace, tc.table); got != tc.want {
				t.Fatalf("controlTableRef(%q, %q) = %q; want %q", tc.controlKeyspace, tc.table, got, tc.want)
			}
		})
	}
}

func TestControlSchemaPredicate(t *testing.T) {
	// Unset: bare DATABASE() with NO extra bound arg — byte-identical scoping
	// to the single-keyspace column probes.
	rhs, args := controlSchemaPredicate("")
	if rhs != "DATABASE()" {
		t.Fatalf("unset rhs = %q; want DATABASE()", rhs)
	}
	if len(args) != 0 {
		t.Fatalf("unset args = %v; want none", args)
	}

	// Set: a bound `?` placeholder carrying the keyspace name (bound, not
	// interpolated — an operator-supplied name can't reshape the query).
	rhs, args = controlSchemaPredicate("sidecar")
	if rhs != "?" {
		t.Fatalf("set rhs = %q; want ?", rhs)
	}
	if len(args) != 1 || args[0] != "sidecar" {
		t.Fatalf("set args = %v; want [sidecar]", args)
	}
}

func TestValidateControlKeyspace(t *testing.T) {
	// Hyphens are valid: a PlanetScale database's default unsharded keyspace is
	// named after the DB (e.g. `sluice-ck-dst`) — the intended default sidecar.
	valid := []string{"", "ctl", "sidecar_ks", "Ks123", "_leading", "a", "my-db-keyspace", "sluice-ck-dst"}
	for _, name := range valid {
		if err := validateControlKeyspace(name); err != nil {
			t.Errorf("validateControlKeyspace(%q) = %v; want nil", name, err)
		}
	}
	// A backtick would break the identifier quoting in controlTableRef; a
	// dot/space/quote/semicolon are not plain Vitess identifiers. All must
	// refuse loudly (loud-failure tenet) rather than emit a malformed statement.
	invalid := []string{"a`b", "a.b", "a b", "a'b", "a;b", "a\"b"}
	for _, name := range invalid {
		if err := validateControlKeyspace(name); err == nil {
			t.Errorf("validateControlKeyspace(%q) = nil; want error", name)
		}
	}
}

// TestSelectControlKeyspace pins the auto-detect decision matrix (the pure
// function; the live vtgate enumeration is validated separately). The cases
// mirror the feature's contract: explicit override wins for any target;
// unsharded/non-Vitess targets get no control keyspace (unchanged); a sharded
// target auto-selects its sole unsharded sidecar and refuses loudly on zero or
// multiple candidates.
func TestSelectControlKeyspace(t *testing.T) {
	const data = "app_sharded"
	cases := []struct {
		name        string
		shardCount  int
		candidates  []string
		explicit    string
		want        string
		wantErr     bool
		errContains string
	}{
		{
			name:       "explicit flag wins over auto-detect (any target)",
			shardCount: 4,
			candidates: []string{"a", "b"}, // ambiguous, but override skips the check
			explicit:   "chosen_ks",
			want:       "chosen_ks",
		},
		{
			name:       "explicit flag wins even on an unsharded target",
			shardCount: 1,
			explicit:   "chosen_ks",
			want:       "chosen_ks",
		},
		{
			name:       "unsharded target, flag unset -> no control keyspace",
			shardCount: 1,
			candidates: []string{"some_unsharded"},
			want:       "",
		},
		{
			name:       "zero-shard (undeterminable) target -> no control keyspace",
			shardCount: 0,
			want:       "",
		},
		{
			name:       "sharded, exactly one unsharded sidecar -> auto-select it",
			shardCount: 2,
			candidates: []string{"sluice-ck-dst"},
			want:       "sluice-ck-dst",
		},
		{
			name:       "sharded, the only unsharded candidate is the data keyspace itself -> refuse (zero)",
			shardCount: 2,
			candidates: []string{data},
			wantErr:    true,
			// data keyspace filtered out, leaving zero.
			errContains: "no unsharded sidecar keyspace was found",
		},
		{
			name:        "sharded, no unsharded candidates -> loud refusal",
			shardCount:  2,
			candidates:  nil,
			wantErr:     true,
			errContains: "no unsharded sidecar keyspace was found",
		},
		{
			name:        "sharded, multiple unsharded candidates -> loud refusal naming them",
			shardCount:  2,
			candidates:  []string{"ks_two", "ks_one", data},
			wantErr:     true,
			errContains: "multiple unsharded keyspaces exist (ks_one, ks_two)",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := selectControlKeyspace(tc.shardCount, tc.candidates, data, tc.explicit)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("selectControlKeyspace = (%q, nil); want error", got)
				}
				if tc.errContains != "" && !strings.Contains(err.Error(), tc.errContains) {
					t.Fatalf("error %q does not contain %q", err.Error(), tc.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("selectControlKeyspace = err %v; want %q", err, tc.want)
			}
			if got != tc.want {
				t.Fatalf("selectControlKeyspace = %q; want %q", got, tc.want)
			}
		})
	}
}

// TestWritePositionUpsertSQL pins the byte-exactness of the ADR-0007 position
// write — the atomicity-critical statement that rides the per-change data tx.
// The set shape qualifies the table AND every COALESCE column source (yielding
// valid three-part `ks`.`table`.column references). ADR-0156 phase 2 added the
// rows_applied ACCUMULATING column (COALESCE(existing, 0) + delta), the last
// column in both the INSERT list and the ON DUPLICATE KEY UPDATE set.
func TestWritePositionUpsertSQL(t *testing.T) {
	const wantBare = "INSERT INTO `sluice_cdc_state` " +
		"(stream_id, source_position, slot_name, publication_name, source_dsn_fingerprint, target_schema, rows_applied) " +
		"VALUES (?, ?, NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), ?) " +
		"AS new ON DUPLICATE KEY UPDATE " +
		"source_position = new.source_position, " +
		"slot_name = COALESCE(new.slot_name, `sluice_cdc_state`.slot_name), " +
		"publication_name = COALESCE(new.publication_name, `sluice_cdc_state`.publication_name), " +
		"source_dsn_fingerprint = COALESCE(new.source_dsn_fingerprint, `sluice_cdc_state`.source_dsn_fingerprint), " +
		"target_schema = COALESCE(new.target_schema, `sluice_cdc_state`.target_schema), " +
		"rows_applied = COALESCE(`sluice_cdc_state`.rows_applied, 0) + new.rows_applied"
	if got := writePositionUpsertSQL("", upsertRowAlias); got != wantBare {
		t.Fatalf("writePositionUpsertSQL(\"\") not byte-identical to expected statement:\n got: %s\nwant: %s", got, wantBare)
	}

	const wantQualified = "INSERT INTO `ctl`.`sluice_cdc_state` " +
		"(stream_id, source_position, slot_name, publication_name, source_dsn_fingerprint, target_schema, rows_applied) " +
		"VALUES (?, ?, NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), ?) " +
		"AS new ON DUPLICATE KEY UPDATE " +
		"source_position = new.source_position, " +
		"slot_name = COALESCE(new.slot_name, `ctl`.`sluice_cdc_state`.slot_name), " +
		"publication_name = COALESCE(new.publication_name, `ctl`.`sluice_cdc_state`.publication_name), " +
		"source_dsn_fingerprint = COALESCE(new.source_dsn_fingerprint, `ctl`.`sluice_cdc_state`.source_dsn_fingerprint), " +
		"target_schema = COALESCE(new.target_schema, `ctl`.`sluice_cdc_state`.target_schema), " +
		"rows_applied = COALESCE(`ctl`.`sluice_cdc_state`.rows_applied, 0) + new.rows_applied"
	if got := writePositionUpsertSQL("ctl", upsertRowAlias); got != wantQualified {
		t.Fatalf("writePositionUpsertSQL(\"ctl\"):\n got: %s\nwant: %s", got, wantQualified)
	}
}

// TestWithControlKeyspace pins the engine builder: an empty value is the
// zero-value default (byte-identical bare-name behaviour), a valid name is
// recorded on the per-instance opts, and an invalid name is refused loudly
// (never silently coerced) so a broken identifier can't corrupt every
// control-table statement.
func TestWithControlKeyspace(t *testing.T) {
	// Empty is accepted and leaves the zero-value default.
	e, err := Engine{Flavor: FlavorPlanetScale}.WithControlKeyspace("")
	if err != nil {
		t.Fatalf("WithControlKeyspace(\"\") = %v; want nil", err)
	}
	if got := e.(Engine).opts.controlKeyspace; got != "" {
		t.Fatalf("empty controlKeyspace recorded as %q; want empty", got)
	}

	// A valid name is recorded per-instance.
	e, err = Engine{Flavor: FlavorPlanetScale}.WithControlKeyspace("sidecar")
	if err != nil {
		t.Fatalf("WithControlKeyspace(\"sidecar\") = %v; want nil", err)
	}
	if got := e.(Engine).opts.controlKeyspace; got != "sidecar" {
		t.Fatalf("controlKeyspace = %q; want sidecar", got)
	}

	// An invalid name is refused loudly.
	if _, err := (Engine{Flavor: FlavorPlanetScale}).WithControlKeyspace("bad name.ks"); err == nil {
		t.Fatal("WithControlKeyspace(\"bad name.ks\") = nil; want error")
	}
}
