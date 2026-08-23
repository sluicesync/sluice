// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package rowpredicate

import (
	"encoding/hex"
	"fmt"
	"net"
	"net/netip"
	"regexp"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// The identifier / time-of-day literal lens (audit 2026-07-26 SL-3).
//
// # The false premise this replaces
//
// UUID, Inet, Cidr, Macaddr and Time columns were classified
// FamilyString{Faithful: true} on the reasoning that "the source's `=` is
// exact, so a byte compare is faithful". The premise is wrong in one specific
// way: the source does not compare the operator's literal as TEXT. It coerces
// the literal to the COLUMN's type and compares typed values, so every
// non-canonical spelling of the same value compares TRUE server-side —
// observed on real PG 16 and MySQL 8.0 for uppercase and unhyphenated UUIDs,
// `10.0.0.1/32` vs `10.0.0.1`, `010.000.000.001`, dash- and dot-separated MAC
// addresses, and `08:30` vs `08:30:00`.
//
// The client evaluator compares the literal against the DECODED value, which
// is always canonical (decodeUUID lowercases and hyphenates, decodeNetwork
// goes through netip, decodeMacaddr through net.HardwareAddr). Each member of
// that parenthetical is a premise about the ENGINES' decoders, and each is
// bound to a real server by a named test rather than by this sentence:
// network/MAC by the S2/C1 pins (postgres network_rendering / macaddr_width
// integration tests), temporal by temporal_realdb_integration_test.go, and
// UUID — the member that until the 2026-08-22 sweep had only
// self-referential unit pins — by
// TestUUIDCanonicalForm_BothLegsDeliverTheEvaluatorsForm
// (internal/engines/postgres), which drives both real legs' decodes through
// the REAL resolver+compiler+Eval. So a
// non-canonical literal compiles, the cold-start snapshot copies the row (that
// leg is server-evaluated), and then the CDC leg scores every change to that
// row as "not in scope" and drops it. The target row goes permanently stale at
// exit 0 — the same harm model this project already adjudicated HIGH for the
// temporal-granularity finding.
//
// # Why refuse rather than normalize
//
// The temporal fix (D0-5) NORMALIZES, because the engine declares its coercion
// rule via ir.TemporalLiteralSemantics and the normalization is therefore
// engine-faithful by construction. Nothing declares a coercion rule for these
// families, so normalizing would mean sluice INVENTING a canonicalization and
// silently rewriting the operator's predicate under it — and if that invention
// ever disagreed with an engine, the failure would be silent again, in exactly
// the place this fix exists to make loud.
//
// Refusing costs the operator one edit and names the spelling to use. It also
// leaves the door open: a future engine that declares its coercion rule can
// normalize instead, without this having guessed first.
type identifierKind uint8

const (
	identifierNone identifierKind = iota
	identifierUUID
	identifierNetwork // inet / cidr
	identifierMAC
	identifierTime
)

// timeCanonicalRE accepts the time-of-day spellings a source would: an
// optional sign and 1–3 hour digits (MySQL TIME spans −838:59:59…838:59:59),
// optional seconds, optional fraction.
var timeCanonicalRE = regexp.MustCompile(`^(-?)(\d{1,3}):(\d{2})(?::(\d{2}))?(?:\.(\d+))?$`)

// canonicalIdentifierLiteral returns the canonical spelling of s for the
// column described by info — the form the engine's decoder produces, and
// therefore the only form the client evaluator can compare byte-exactly. ok is
// false when s is not a valid value of that kind at all.
//
// It takes the whole [ColumnInfo] rather than the kind alone because two of
// the four kinds need per-column engine facts to answer: the network rendering
// AND the network family (two independent axes — see [coerceToInetFamily]),
// and the MAC width.
func canonicalIdentifierLiteral(s string, info ColumnInfo) (canonical string, ok bool) {
	network := info.NetworkRendering
	switch info.Identifier {
	case identifierUUID:
		// Accept the spellings a source would (braces, no hyphens, any case)
		// and render the decoder's form: lowercase hex, 8-4-4-4-12.
		t := strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(s), "{"), "}")
		t = strings.ReplaceAll(t, "-", "")
		if len(t) != 32 {
			return "", false
		}
		b, err := hex.DecodeString(strings.ToLower(t))
		if err != nil || len(b) != 16 {
			return "", false
		}
		h := hex.EncodeToString(b)
		return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32], true

	case identifierNetwork:
		// A zone-scoped address is refused here as well as by the caller. This
		// arm is what defines "canonical", and there is no canonical spelling
		// of a value neither server can store — see checkNetworkLiteralZone.
		if strings.Contains(s, "%") {
			return "", false
		}
		// netip PARSES; it does not get to decide the spelling. WHICH form is
		// canonical is the engine's call (audit 2026-08-01 S2), and so is how
		// the address itself is rendered (audit 2026-08-04 C1) — both servers
		// use the BSD inet_ntop6 dotted-quad convention that netip applies
		// only to IPv4-mapped addresses, so `::1.2.3.4` is delivered as
		// `::1.2.3.4` and netip would have written `::102:304`. Every
		// rendering below therefore goes through [ir.RenderNetworkAddr] /
		// [ir.RenderNetworkPrefix] rather than netip's String.
		//
		// Getting either axis wrong is SILENT: the literal simply never equals
		// the delivered value, so every row scores out-of-scope and every CDC
		// change to a matching row is dropped at exit 0. An engine that has
		// not named its rendering is refused by the caller before reaching
		// here.
		//
		// The earlier version of this comment asserted that PG delivers
		// through pgx's InetCodec as a netip.Prefix. That is real code on a
		// path this program does not take — sluice reads through database/sql
		// — and it is what legitimised netip as the canonicaliser in the first
		// place. See [ir.NetworkLiteralRendering] for the live ground truth.
		//
		// netip deliberately REJECTS zero-padded octets ("010.0.0.1") because
		// they are ambiguous with octal in C-family resolvers. Postgres accepts
		// them and reads them as decimal, so a literal netip refuses can still
		// be a perfectly valid predicate the source would have matched. Strip
		// the padding and retry, so the operator gets "write it as 10.0.0.1"
		// rather than a misleading "not a valid value".
		for _, cand := range []string{s, stripIPv4LeadingZeros(s)} {
			if cand == "" {
				continue
			}
			if p, err := netip.ParsePrefix(cand); err == nil {
				coerced, ok := coerceToInetFamily(p.Addr(), info.InetFamily)
				if !ok {
					return "", false
				}
				// Mapping an IPv4 address into IPv6 prepends 96 bits, so the
				// mask moves with it: a /32 host prefix becomes /128 and a
				// /24 network becomes /120. Without the shift a mapped host
				// literal would read as a 32-bit network of a 128-bit
				// address and lose its full-width status.
				p = netip.PrefixFrom(coerced, p.Bits()+coerced.BitLen()-p.Addr().BitLen())
				fullWidth := p.Bits() == p.Addr().BitLen()
				switch network {
				case ir.NetworkLiteralRenderingHostBare:
					// PG `inet`: a full-width mask is not delivered, so the
					// canonical spelling drops it. A narrower mask IS
					// delivered and stays.
					if fullWidth {
						return ir.RenderNetworkAddr(p.Addr(), network), true
					}
					return ir.RenderNetworkPrefix(p, network), true
				case ir.NetworkLiteralRenderingAlwaysMasked:
					// PG `cidr`: the mask is always delivered, at every width.
					return ir.RenderNetworkPrefix(p, network), true
				case ir.NetworkLiteralRenderingAddressOnly:
					// MariaDB inet4/inet6 hold an address, never a network. A
					// full-width mask reduces to the address; anything
					// narrower names a network no stored value can equal, so
					// it is refused rather than silently widened.
					if fullWidth {
						return ir.RenderNetworkAddr(p.Addr(), network), true
					}
					return "", false
				}
				return "", false
			}
			if a, err := netip.ParseAddr(cand); err == nil {
				var ok bool
				if a, ok = coerceToInetFamily(a, info.InetFamily); !ok {
					return "", false
				}
				switch network {
				case ir.NetworkLiteralRenderingHostBare, ir.NetworkLiteralRenderingAddressOnly:
					return ir.RenderNetworkAddr(a, network), true
				case ir.NetworkLiteralRenderingAlwaysMasked:
					// The delivered value always carries a prefix length, so
					// the canonical spelling of a bare address is its
					// full-width prefix — "10.0.0.1" → "10.0.0.1/32".
					return ir.RenderNetworkPrefix(netip.PrefixFrom(a, a.BitLen()), network), true
				}
				return "", false
			}
		}
		return "", false

	case identifierMAC:
		// net.ParseMAC also accepts a 20-byte InfiniBand form, which neither
		// PG MAC type can hold; checkMACLiteralWidth has already refused
		// every width the column cannot store by the time this runs.
		hw, err := net.ParseMAC(s)
		if err != nil {
			return "", false
		}
		switch {
		case len(hw) == info.MACWidth:
			return hw.String(), true
		case len(hw) == ir.MacaddrEUI48 && info.MACWidth == ir.MacaddrEUI64:
			// A `macaddr8` column ACCEPTS a 6-byte literal and stores it
			// WIDENED — so the canonical spelling of `08:00:2b:01:02:03` on
			// such a column is `08:00:2b:ff:fe:01:02:03`, and the generic
			// not-the-canonical-spelling refusal names it. That is the C1
			// residual: before the width existed this literal compiled clean
			// and then matched nothing, forever, at exit 0.
			return widenEUI48(hw).String(), true
		}
		return "", false

	case identifierTime:
		// Only reached for a TIME(0) column — a fractional one is refused
		// outright by checkIdentifierLiteral because its rendering is not
		// stable across legs. Accept the spellings a source would (`08:30`,
		// an explicit `.000000`, a MySQL out-of-range or negative hour) and
		// render the decoder's fixed-width form.
		m := timeCanonicalRE.FindStringSubmatch(s)
		if m == nil {
			return "", false
		}
		sign, hh, mm, ss, frac := m[1], m[2], m[3], m[4], m[5]
		if ss == "" {
			ss = "00"
		}
		// A non-zero fraction on a TIME(0) column is a value the column cannot
		// hold; refusing beats silently truncating it to the second.
		if frac != "" && strings.Trim(frac, "0") != "" {
			return "", false
		}
		if len(hh) < 2 {
			hh = "0" + hh
		}
		return sign + hh + ":" + mm + ":" + ss, true
	}
	return "", false
}

// stripIPv4LeadingZeros rewrites a dotted-quad (optionally with a /mask) so
// each octet has no leading zeros, leaving anything that is not a dotted-quad
// untouched. Postgres reads padded octets as decimal; netip refuses them.
func stripIPv4LeadingZeros(s string) string {
	addr, mask, hasMask := strings.Cut(s, "/")
	parts := strings.Split(addr, ".")
	if len(parts) != 4 {
		return ""
	}
	for i, p := range parts {
		if p == "" {
			return ""
		}
		trimmed := strings.TrimLeft(p, "0")
		if trimmed == "" {
			trimmed = "0"
		}
		parts[i] = trimmed
	}
	out := strings.Join(parts, ".")
	if hasMask {
		out += "/" + mask
	}
	return out
}

// checkIdentifierLiteral is the compile-time gate. It returns nil when the
// comparison can be evaluated faithfully client-side, and a loud, specific
// refusal otherwise.
func checkIdentifierLiteral(col string, info ColumnInfo, lit literal) error {
	if info.Identifier == identifierNone || lit.kind != litString {
		return nil
	}

	// A fractional-second TIME column renders DIFFERENTLY on different legs of
	// the same sync: go-mysql's binlog formatter omits the fraction entirely
	// when it is zero ("08:30:00") while the snapshot path renders it in full
	// ("08:30:00.000000"). One compiled predicate would therefore classify the
	// same row two ways depending on which leg delivered it, and no choice of
	// literal is right for both. There is nothing to normalise to.
	if info.Identifier == identifierTime && info.TimeFractionAmbiguous {
		return fmt.Errorf(
			"column %q is a TIME with fractional-second precision, whose value is rendered differently by the "+
				"snapshot leg (`08:30:00.000000`) and the binlog leg (`08:30:00`) — so no single literal compares "+
				"correctly on both, and a filter on it would classify the same row differently depending on which "+
				"leg delivered the change. Filter on a different column, or use a TIME(0) column",
			col,
		)
	}

	// An inet/cidr comparison depends on the source engine having named how its
	// change stream renders the value, because the two eligible engines
	// disagree and neither spelling is safe to assume: Postgres always delivers
	// a prefix length, MariaDB never does, and comparing against the wrong one
	// drops every change to every matching row with no error. Refuse rather
	// than guess (audit 2026-08-01 S2).
	if info.Identifier == identifierNetwork && info.NetworkRendering == ir.NetworkLiteralRenderingUnknown {
		return fmt.Errorf(
			"column %q is an inet/cidr column, but this source engine has not declared how its change stream "+
				"renders network values — Postgres delivers them with a prefix length (`10.0.0.1/32`) and MariaDB "+
				"delivers them bare (`10.0.0.1`), so no literal spelling can be known to compare correctly, and the "+
				"wrong one would score every row out of scope and silently drop every change to it. Filter on a "+
				"different column",
			col,
		)
	}

	// The second network axis, independent of the rendering above: which
	// address family the COLUMN constrains its values to. MariaDB's INET4 and
	// INET6 share one rendering and disagree here — an INET6 column widens
	// `10.0.0.1` to `::ffff:10.0.0.1` on the way in and delivers the widened
	// form, while still matching the bare literal server-side — so a filtered
	// sync copied the row in the cold phase and then silently dropped every
	// CDC change to it, freezing caught-up at exit 0 (roadmap item 133,
	// live-verified on mariadb:11.4). The zero value refuses for the reason
	// [ir.InetFamily] gives: every producer that can reach this decides, so an
	// unspecified family means a new one appeared without deciding.
	if info.Identifier == identifierNetwork && info.InetFamily == ir.InetFamilyUnspecified {
		return fmt.Errorf(
			"column %q is an inet/cidr column, but this source engine has not declared which address family the "+
				"column constrains its values to — a MariaDB INET6 column stores `10.0.0.1` as the IPv4-mapped "+
				"`::ffff:10.0.0.1` and delivers it that way while still matching the bare literal server-side, so "+
				"without knowing the family no literal spelling can be known to compare correctly, and the wrong "+
				"one would score every row out of scope and silently drop every change to it. Filter on a "+
				"different column",
			col,
		)
	}

	if info.Identifier == identifierNetwork {
		if err := checkNetworkLiteralZone(col, lit.str); err != nil {
			return err
		}
	}
	if info.Identifier == identifierMAC {
		if err := checkMACLiteralWidth(col, lit.str, info.MACWidth); err != nil {
			return err
		}
	}

	canonical, ok := canonicalIdentifierLiteral(lit.str, info)
	if !ok {
		return fmt.Errorf("literal %q is not a valid value for %s column %q",
			lit.str, identifierKindName(info.Identifier), col)
	}
	if canonical != lit.str {
		return fmt.Errorf(
			"literal %q is not the canonical spelling of that %s value — the source would accept it (it coerces "+
				"the literal to the column type before comparing), but a change stream delivers the value already "+
				"canonicalised as %q, so a client-side filter would score every row NOT in scope and silently drop "+
				"every change to it. Write it as %q",
			lit.str, identifierKindName(info.Identifier), canonical, canonical,
		)
	}
	return nil
}

// coerceToInetFamily applies the COLUMN's address-family coercion to a literal
// address, returning ok=false when the column could never store it (roadmap
// item 133).
//
// This is the second axis of a network literal, independent of
// [ir.NetworkLiteralRendering]'s "is a prefix length shown?": before the value
// is rendered at all, a family-constrained column may have moved the address
// into another family. Ground-truthed on mariadb:11.4 (`HEX(ip)` on the stored
// row, plus the server's own `WHERE ip = <literal>` count):
//
//	INET6 stored '10.0.0.1'        HEX 00000000000000000000FFFF0A000001
//	                               SELECT delivers ::ffff:10.0.0.1
//	INET6 stored '0.0.0.0'         SELECT delivers ::ffff:0.0.0.0
//	INET6 stored '::1.2.3.4'       SELECT delivers ::1.2.3.4 (NOT re-mapped)
//	INET4 INSERT '::ffff:10.0.0.1' errno 1292, Incorrect inet4 value
//	INET4 WHERE  ip='::ffff:10.0.0.1'  0 rows (no coercion, matches nothing)
//
// The IPv6 row is what made item 133 silent: `WHERE ip = '10.0.0.1'` matches
// server-side (2 of 2 rows on the fixture above), so the cold copy is right and
// only the client-side CDC leg, comparing against the delivered
// `::ffff:10.0.0.1`, scores every change out of scope — forever, at exit 0.
//
// [ir.InetFamilyUnspecified] never reaches here: checkIdentifierLiteral
// refuses the column outright first.
func coerceToInetFamily(a netip.Addr, family ir.InetFamily) (netip.Addr, bool) {
	switch family {
	case ir.InetFamilyIPv6:
		// The column widens every address into IPv6. netip's As16 produces
		// the IPv4-MAPPED bytes for an IPv4 address, which is exactly what
		// MariaDB stores, and AddrFrom16 renders them `::ffff:a.b.c.d` —
		// byte-for-byte the server's own spelling. An address already in v6
		// (including a v4-COMPATIBLE `::1.2.3.4`) is returned untouched,
		// because the server does not re-map those either.
		if a.Is4() {
			return netip.AddrFrom16(a.As16()), true
		}
		return a, true
	case ir.InetFamilyIPv4:
		// The column holds only IPv4 and coerces nothing — the server
		// REFUSES an IPv6 literal on insert and matches nothing for one in a
		// WHERE, so a predicate naming one is refused rather than compiled
		// into a filter that can never be true. An IPv4-mapped spelling is
		// refused with it: `::ffff:10.0.0.1` is an IPv6 literal to MariaDB,
		// and unmapping it here would silently rewrite a predicate the
		// server rejects into one it accepts.
		return a, a.Is4()
	default:
		// InetFamilyAny — Postgres, which constrains nothing.
		return a, true
	}
}

// checkNetworkLiteralZone refuses a zone-scoped address (`fe80::1%eth0`).
//
// netip PARSES a zone, and [ir.RenderNetworkAddr] deliberately returns such an
// address unchanged so that the zone survives to be seen here rather than
// being silently dropped by the dotted-quad branch. That makes the literal
// round-trip to itself, so without this check it compiles clean — and then
// matches nothing, for the whole life of the stream, at exit 0.
//
// Nothing can match it: neither engine can STORE a zone. Postgres raises
// "invalid input syntax for type inet" and MariaDB errno 1292, so there is no
// value in any column of either type that a zoned literal could equal. This is
// the same adjudication as a narrower-than-full prefix on a MariaDB inet4
// column — refuse a literal no stored value can be, rather than compare
// against it forever.
func checkNetworkLiteralZone(col, lit string) error {
	bare, zone, hasZone := strings.Cut(lit, "%")
	if !hasZone {
		return nil
	}
	return fmt.Errorf(
		"literal %q on inet/cidr column %q carries an IPv6 zone (%q), which neither Postgres nor MariaDB can "+
			"store in a network column — Postgres rejects it as invalid input syntax and MariaDB raises errno "+
			"1292. No stored value can carry a zone, so this filter would compile and then match nothing, "+
			"silently dropping every change to every row. Write it as %q",
		lit, col, "%"+zone, bare,
	)
}

// checkMACLiteralWidth refuses a MAC literal the column cannot STORE, because
// [net.ParseMAC] accepts 6-byte EUI-48, 8-byte EUI-64 and 20-byte InfiniBand
// spellings and each renders back to itself — so a too-wide one compiles clean
// and then matches nothing.
//
// THE TRUTH TABLE, now that [ir.Macaddr] carries the width (roadmap item 117;
// the previous version of this comment recorded the same four rows under the
// constraint that the width was unknowable, and over-refused row 2 as a
// result):
//
//   - 8-byte literal on `macaddr`  — impossible to store. REFUSED here.
//   - 8-byte literal on `macaddr8` — correct, and now ACCEPTED. This is the
//     over-refusal v0.110.1 shipped deliberately and this lifts.
//   - 6-byte literal on `macaddr`  — correct. Accepted.
//   - 6-byte literal on `macaddr8` — accepted HERE and then refused by the
//     canonical-spelling check, which names the widened form to write. That
//     is the C1 residual closing: Postgres stores a 6-byte input in a
//     macaddr8 column widened to EUI-64 (`08:00:2b:01:02:03` is delivered as
//     `08:00:2b:ff:fe:01:02:03`), so the literal never equals the delivered
//     value and every change to every matching row was silently dropped.
//   - 20-byte InfiniBand literal on either — no PG MAC type holds it.
//     REFUSED.
//
// THE WIDENING IS AN ENGINE FACT, not sluice's invention: it is Postgres's
// documented macaddr8 input coercion (FF:FE inserted in the middle; the 7th-bit
// flip is the separate `macaddr8_set7bit()` function and is NOT applied on
// input). Per the premise-naming rule it is ground-truthed against a real
// server rather than asserted — TestMacaddr8WidensEUI48OnInput in
// engines/postgres. Postgres is the only engine that produces [ir.Macaddr] at
// all, so the rule has exactly one claimant.
func checkMACLiteralWidth(col, lit string, colWidth int) error {
	// A literal that does not parse as a MAC at all is not this check's
	// business — it falls through to the generic invalid-literal refusal,
	// which already names the column and the kind.
	width, parsed := macLiteralWidth(lit)
	if !parsed {
		return nil
	}
	// An unspecified column width means a producer of ir.Macaddr appeared
	// that did not decide (both Postgres readers do). Refuse rather than
	// assume 6: assuming is how the macaddr8 row above stayed silent for the
	// whole life of this check, and refusing surfaces the drift loudly. Same
	// adjudication, and the same shape, as an unknown NetworkRendering.
	if colWidth == 0 {
		return fmt.Errorf(
			"column %q is a macaddr column whose WIDTH this source engine did not report — `macaddr` holds 6 "+
				"bytes and `macaddr8` holds 8, and Postgres widens a 6-byte value on input to the latter, so no "+
				"literal spelling can be known to compare correctly and the wrong one would score every row out "+
				"of scope and silently drop every change to it. Filter on a different column",
			col,
		)
	}
	if width == colWidth || (width == ir.MacaddrEUI48 && colWidth == ir.MacaddrEUI64) {
		return nil
	}
	return fmt.Errorf(
		"literal %q on macaddr column %q is a %d-byte address; that column holds %d bytes, so no stored value "+
			"can equal it and this filter would compile and then match nothing, silently dropping every change "+
			"to every row. Write the %d-byte form, or filter on a different column",
		lit, col, width, colWidth, colWidth,
	)
}

// macLiteralWidth reports the byte width [net.ParseMAC] reads out of lit, and
// whether it parsed at all. Split out so the not-a-MAC case returns a value
// rather than a nil error alongside a non-nil one.
func macLiteralWidth(lit string) (width int, parsed bool) {
	hw, err := net.ParseMAC(lit)
	if err != nil {
		return 0, false
	}
	return len(hw), true
}

// widenEUI48 renders a 6-byte MAC in the 8-byte form Postgres stores it as in
// a `macaddr8` column: the two halves separated by FF:FE. It is deliberately
// NOT a general EUI-48→EUI-64 conversion — the IEEE modified-EUI-64 form also
// flips the universal/local bit, and Postgres does not do that on input (that
// is `macaddr8_set7bit()`, which an operator calls explicitly).
func widenEUI48(hw net.HardwareAddr) net.HardwareAddr {
	out := make(net.HardwareAddr, 0, ir.MacaddrEUI64)
	out = append(out, hw[:3]...)
	out = append(out, 0xff, 0xfe)
	return append(out, hw[3:]...)
}

func identifierKindName(k identifierKind) string {
	switch k {
	case identifierUUID:
		return "UUID"
	case identifierNetwork:
		return "inet/cidr"
	case identifierMAC:
		return "macaddr"
	case identifierTime:
		return "time"
	}
	return "identifier"
}
