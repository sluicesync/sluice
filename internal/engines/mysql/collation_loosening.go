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
// ok is false for a non-character type, one with no recorded collation, or
// a name whose axes are not all named ([collationAxesNamed]):
// the stream's unique-key door then compares the collation NAMES, and a
// differing name counts as looser. Every faithful
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
	if !collationAxesNamed(lc) {
		return 0, false
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

// collationAxesNamed reports a collation name whose tokens state its case
// and accent behaviour (`_ci` or `_cs`), with no token naming an axis this
// classification does not model: kana sensitivity (`_ks` — a `_ja_…_as_cs`
// collation folds hiragana and katakana, its `_ks` twin does not) or a
// MariaDB weight-level suffix (`utf8mb4_thai_520_w2`). Anything else is
// UNKNOWN (ok=false), which the stream's unique-key door treats as looser
// than any source collation of another name: the conservative direction is
// the loud one (round-3 review LOW).
func collationAxesNamed(lc string) bool {
	if strings.Contains(lc, "_ks") {
		return false
	}
	for _, part := range strings.Split(lc, "_") {
		if len(part) == 2 && part[0] == 'w' && part[1] >= '0' && part[1] <= '9' {
			return false
		}
	}
	return strings.Contains(lc, "_ci") || strings.Contains(lc, "_cs")
}
