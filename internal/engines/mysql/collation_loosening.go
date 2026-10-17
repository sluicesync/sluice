// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// CollationLoosening implements [ir.CollationLooseningResolver] for the MySQL
// family (MySQL, MariaDB, PlanetScale, Vitess), read off the collation NAME,
// the only signal every flavor and version shares (MariaDB through 12.0 has
// no PAD_ATTRIBUTE column; see [collationNoPad]):
//
//   - case: a `_ci` collation folds case; `_cs`, `_bin` and `binary` do not;
//   - accent: `_ai` folds accents; a legacy `_ci` with no `_as`/`_ai` token
//     (utf8mb4_general_ci, latin1_swedish_ci) folds them too; `_as`, `_bin`
//     and `binary` do not;
//   - pad: every collation but the NO PAD ones ([collationNoPad]) ignores
//     trailing spaces, and a CHAR column ignores them under any collation:
//     the server stores a CHAR value space-padded to its length, so `'a'`
//     and `'a '` are one stored value even under a NO PAD collation (a MySQL
//     8 CHAR key against a Postgres bpchar target is not a weakening).
//
// ok is false for a non-character type or one with no recorded collation:
// the stream's unique-key door then has nothing to compare. Every faithful
// cross-flavor translation keeps the axes (MySQL 8 → MariaDB maps 0900_ai_ci
// to uca1400_nopad_ai_ci and 0900_bin to nopad_bin, flavor_mariadb.go).
func (Engine) CollationLoosening(t ir.Type) (ir.CollationLoosening, bool) {
	var coll string
	var l ir.CollationLoosening
	switch v := ir.UnwrapDomain(t).(type) {
	case ir.Char:
		coll, l = v.Collation, ir.LoosePad
	case ir.Varchar:
		coll = v.Collation
	case ir.Text:
		coll = v.Collation
	default:
		return 0, false
	}
	lc := strings.ToLower(strings.TrimSpace(coll))
	if lc == "" {
		return 0, false
	}
	if !collationNoPad(lc) {
		l |= ir.LoosePad
	}
	if collationByteExactMySQL(lc) {
		return l, true
	}
	switch {
	case strings.Contains(lc, "_ai"):
		l |= ir.LooseAccent
	case strings.Contains(lc, "_ci") && !strings.Contains(lc, "_as"):
		l |= ir.LooseAccent
	}
	if strings.Contains(lc, "_ci") {
		l |= ir.LooseCase
	}
	return l, true
}
