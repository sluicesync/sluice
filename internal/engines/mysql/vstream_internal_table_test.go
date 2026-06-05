// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import "testing"

// TestIsVitessInternalTable pins the ADR-0073 (c) exclusion set so a
// Vitess wording change (the naming convention is version-dependent;
// vitessio/vitess#14582) fails a test rather than silently leaking an
// internal table into the COPY / CDC stream.
//
// PIN THE CLASS, not a representative: the matcher dispatches across
// several name families (the unified v20+ `_vt_<op>_…` form, the legacy
// gh-ost/vreplication `_<uuid>_…_(gho|ghc|del|new|vrepl)` form, and the
// pt-osc `_…_old` form), and each family has multiple op codes. We
// exercise EVERY op code of the unified form plus each legacy family —
// AND a battery of non-internal lookalikes that must NOT be excluded
// (the silent-loss direction: a user table wrongly treated as internal
// would be DROPPED from the migration).
func TestIsVitessInternalTable(t *testing.T) {
	const (
		// A condensed 32-hex UUID + 14-digit timestamp, the shape the
		// unified internal-table regexp requires.
		uuid = "6ace8bcef73211ea87e9f875a4d24e90"
		ts   = "20200915120410"
	)

	internal := []struct {
		name string
		tbl  string
	}{
		// Unified v20+ format `_vt_<op>_<uuid>_<ts>_`, every op code.
		{"unified_hld_gc_hold", "_vt_hld_" + uuid + "_" + ts + "_"},
		{"unified_prg_gc_purge", "_vt_prg_" + uuid + "_" + ts + "_"},
		{"unified_evc_gc_evac", "_vt_evc_" + uuid + "_" + ts + "_"},
		{"unified_drp_gc_drop", "_vt_drp_" + uuid + "_" + ts + "_"},
		// vrp == vreplication / online-DDL — the Bug-125 shadow code.
		{"unified_vrp_vreplication", "_vt_vrp_" + uuid + "_" + ts + "_"},
		{"unified_gho_ghost_shadow", "_vt_gho_" + uuid + "_" + ts + "_"},
		{"unified_ghc_ghost_changelog", "_vt_ghc_" + uuid + "_" + ts + "_"},
		{"unified_del_ghost_deleted", "_vt_del_" + uuid + "_" + ts + "_"},

		// Legacy gh-ost / vreplication form
		// `_<uuid-with-underscores>_<ts>_(gho|ghc|del|new|vrepl)`.
		{"legacy_ghost_gho", "_7cee19dd_354b_11eb_82cd_f875a4d24e90_" + ts + "_gho"},
		{"legacy_ghost_ghc", "_7cee19dd_354b_11eb_82cd_f875a4d24e90_" + ts + "_ghc"},
		{"legacy_ghost_del", "_7cee19dd_354b_11eb_82cd_f875a4d24e90_" + ts + "_del"},
		{"legacy_ptosc_new", "_7cee19dd_354b_11eb_82cd_f875a4d24e90_" + ts + "_new"},
		{"legacy_vreplication_vrepl", "_7cee19dd_354b_11eb_82cd_f875a4d24e90_" + ts + "_vrepl"},

		// pt-online-schema-change form `_..._old`.
		{"ptosc_old", "_users_old"},
	}
	for _, c := range internal {
		t.Run("internal/"+c.name, func(t *testing.T) {
			if !isVitessInternalTable(c.tbl) {
				t.Errorf("isVitessInternalTable(%q) = false; want true (Vitess-internal, must be excluded)", c.tbl)
			}
		})
	}

	// Non-internal lookalikes — MUST NOT be excluded. A false positive
	// here is a silent-loss bug: a real user table would be dropped from
	// the migration. The task specifically calls out `_vtok` and
	// `vt_foo` as lookalikes.
	notInternal := []string{
		"users",
		"orders",
		"_vtok",              // starts with `_vt` but not `_vt_`
		"vt_foo",             // no leading underscore
		"_vt_foo",            // `_vt_` prefix but not the op_uuid_ts_ shape
		"_vt_users_backup",   // ditto — operator's own `_vt_`-named table
		"_vt_vrp_short_123_", // malformed uuid/ts — not the exact shape
		"my_vt_hld_table",    // op code embedded mid-name
		"_old",               // bare suffix, no leading name
		"orders_old_archive", // contains "old" but not the `_..._old` form
		"_vt_",               // prefix only
		"users_vt_vrp",       // op tokens present but wrong structure
	}
	for _, tbl := range notInternal {
		t.Run("user/"+tbl, func(t *testing.T) {
			if isVitessInternalTable(tbl) {
				t.Errorf("isVitessInternalTable(%q) = true; want false (user table, must NOT be excluded — silent loss)", tbl)
			}
		})
	}
}

// TestIsVitessInternalDDL pins the cutover-survival DDL detector
// (ADR-0073 (c)): a DDL on a `_vt_*` shadow table must be recognized so
// it doesn't invalidate the logical field cache, while a DDL on a
// logical user table must flow through normal invalidation. Phase A
// ground-truthed the exact statement shapes vtgate streams during an
// online ALTER (CREATE/ALTER on `_vt_vrp_*`).
func TestIsVitessInternalDDL(t *testing.T) {
	const internalName = "_vt_vrp_edde06e9611211f1ad9bdebcfa3e7809_20260605191536_"

	internalDDL := []string{
		"CREATE TABLE `" + internalName + "` (\n\t`id` bigint NOT NULL AUTO_INCREMENT,\n\tPRIMARY KEY (`id`)\n) ENGINE InnoDB",
		"ALTER TABLE `" + internalName + "` ADD COLUMN `nickname` VARCHAR(64) NULL",
		"ALTER TABLE `" + internalName + "` AUTO_INCREMENT=4",
		"DROP TABLE IF EXISTS `" + internalName + "`",
		"RENAME TABLE `" + internalName + "` TO `users`",
		"alter table " + internalName + " add column c int", // unquoted, lowercase
		"CREATE TABLE test.`" + internalName + "` (id int)", // keyspace-qualified
	}
	for i, stmt := range internalDDL {
		t.Run("internal", func(t *testing.T) {
			if !isVitessInternalDDL(stmt) {
				t.Errorf("[%d] isVitessInternalDDL(%q) = false; want true (shadow DDL must not clear logical cache)", i, stmt)
			}
		})
	}

	logicalDDL := []string{
		"ALTER TABLE `users` ADD COLUMN `nickname` VARCHAR(64) NULL",
		"CREATE TABLE orders (id BIGINT PRIMARY KEY)",
		"DROP TABLE IF EXISTS orders",
		"RENAME TABLE users TO users_archive",
		"ALTER TABLE test.users DROP COLUMN legacy",
		"TRUNCATE TABLE users",          // not a CREATE/ALTER/DROP/RENAME — falls through
		"ALTER TABLE `_vtok` ADD x INT", // lookalike user table
	}
	for i, stmt := range logicalDDL {
		t.Run("logical", func(t *testing.T) {
			if isVitessInternalDDL(stmt) {
				t.Errorf("[%d] isVitessInternalDDL(%q) = true; want false (logical DDL must clear cache normally)", i, stmt)
			}
		})
	}
}
