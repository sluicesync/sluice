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
// goes through netip, decodeMacaddr through net.HardwareAddr). So a
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

// canonicalIdentifierLiteral returns the canonical spelling of s for kind —
// the form the engine's decoder produces, and therefore the only form the
// client evaluator can compare byte-exactly. ok is false when s is not a valid
// value of that kind at all.
func canonicalIdentifierLiteral(kind identifierKind, s string, network ir.NetworkLiteralRendering) (canonical string, ok bool) {
	switch kind {
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
		// net.ParseMAC also accepts 8-byte EUI-64 and 20-byte InfiniBand
		// forms. Only the 6-byte form has a canonical spelling this package
		// can stand behind — see checkMACLiteralWidth for why, and for what
		// it deliberately does NOT close.
		hw, err := net.ParseMAC(s)
		if err != nil || len(hw) != 6 {
			return "", false
		}
		return hw.String(), true

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

	if info.Identifier == identifierNetwork {
		if err := checkNetworkLiteralZone(col, lit.str); err != nil {
			return err
		}
	}
	if info.Identifier == identifierMAC {
		if err := checkMACLiteralWidth(col, lit.str); err != nil {
			return err
		}
	}

	canonical, ok := canonicalIdentifierLiteral(info.Identifier, lit.str, info.NetworkRendering)
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

// checkMACLiteralWidth refuses a MAC literal that is not the 6-byte EUI-48
// form, because [net.ParseMAC] also accepts 8-byte EUI-64 and 20-byte
// InfiniBand spellings and both render back to themselves — so they compile
// clean and match nothing on a `macaddr` column.
//
// WHAT THIS DOES NOT CLOSE, stated because the narrow scope is deliberate and
// reads as broader than it is: [ir.Macaddr] is an EMPTY STRUCT, and the
// Postgres reader maps BOTH `macaddr` and `macaddr8` onto it
// (engines/postgres/types.go). The width is therefore indistinguishable here
// BY CONSTRUCTION, and this check picks the side that fails loudly on the
// common column type:
//
//   - 8-byte literal on `macaddr`  — impossible to store. REFUSED by this.
//   - 8-byte literal on `macaddr8` — correct today. Also refused by this, and
//     that is the cost: a working filter becomes a loud error naming why.
//   - 6-byte literal on `macaddr`  — correct. Accepted.
//   - 6-byte literal on `macaddr8` — STILL SILENTLY WRONG, and NOT closed
//     here. Postgres widens EUI-48 to EUI-64 on input (`08:00:2b:01:02:03` is
//     stored and delivered as `08:00:2b:ff:fe:01:02:03`), so the literal never
//     equals the delivered value.
//
// That last row needs a width on the IR type plus a fingerprint exclusion for
// the new field (the item-104 trap), which is the queued C1 residual and not
// this. Until it lands, a `--where` on a macaddr8 column is unsafe in one
// direction and this comment is the only thing that says so.
func checkMACLiteralWidth(col, lit string) error {
	// A literal that does not parse as a MAC at all is not this check's
	// business — it falls through to the generic invalid-literal refusal,
	// which already names the column and the kind.
	width, parsed := macLiteralWidth(lit)
	if !parsed || width == 6 {
		return nil
	}
	return fmt.Errorf(
		"literal %q on macaddr column %q is a %d-byte address; Postgres `macaddr` holds 6 bytes, so no stored "+
			"value can equal it and this filter would compile and then match nothing, silently dropping every "+
			"change to every row. sluice cannot tell `macaddr` from `macaddr8` (both read as ir.Macaddr), so an "+
			"8-byte literal is refused even on a macaddr8 column rather than guessed at. Filter on a different "+
			"column, or write the 6-byte form if the column is `macaddr`",
		lit, col, width,
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
