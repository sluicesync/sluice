package ir

import (
	"fmt"
	"net/netip"
	"strconv"
	"strings"
)

// Network-literal rendering — the inet/cidr axis of the "filtered replicas
// follow the source engine's comparison semantics" contract.
//
// A client-side `--where` filter compares an operator-written literal against
// the value the CHANGE STREAM delivers, so the literal must be spelled the way
// that stream renders it. There are THREE renderings in play across two
// engines and two IR types, and they were ground-truthed on a real server
// (PG 16.14, MariaDB 11.4) rather than reasoned about — see the cost note
// below for why that distinction earned its own paragraph.
//
//   - Postgres `inet` omits the prefix length at FULL WIDTH and shows it
//     otherwise: a stored 10.0.0.1 is delivered "10.0.0.1", a stored
//     10.0.0.0/24 is delivered "10.0.0.0/24". ([NetworkLiteralRenderingHostBare])
//   - Postgres `cidr` ALWAYS shows the prefix length, including at full width:
//     a stored 10.0.0.1/32 is delivered "10.0.0.1/32", never "10.0.0.1".
//     ([NetworkLiteralRenderingAlwaysMasked])
//   - MariaDB native inet4/inet6 are always BARE — a dotted quad, or the BSD
//     inet_ntop6 rendering — because those columns hold an address and cannot
//     hold a network at all. ([NetworkLiteralRenderingAddressOnly])
//
// So the rendering depends on the engine AND on which network type the column
// is, which is why [NetworkLiteralResolver] takes the shape flag rather than
// answering per engine — the same shape as ResolveStringEquality's fixedChar
// argument, and of [TemporalLiteralResolver] on the temporal axis.
//
// Getting it wrong is silent: the literal simply never equals the delivered
// value, so every row scores out-of-scope and every change to it is dropped
// with no error. That is why the zero value REFUSES rather than guessing —
// unlike the temporal axis, where "no normalization" is a safe pre-existing
// behavior, there is no neutral rendering to fall back to here.
//
// # What this cost, because the shape recurs
//
// The audit filed this as one Postgres bug. The first fix attempt inverted its
// direction after reading pgx v5.10.0's InetCodec.DecodeValue, which scans
// unconditionally into a netip.Prefix and therefore renders "10.0.0.1/32".
// That reading was of real code and was still wrong about THIS program: the
// row reader goes through database/sql, where the value arrives as Postgres's
// own text output, and decodeNetwork takes its string arm. Reading the right
// module says nothing about which path the caller takes. Only a live two-leg
// integration pin settled it, and it also surfaced the cidr/inet split that
// neither the audit nor the module source hinted at — so the ONE filed bug is
// actually two mirror-image bugs: a masked literal on an `inet` column, and a
// bare literal on a `cidr` column, each silently matching nothing.

// NetworkLiteralRendering names how the SOURCE engine's change stream renders
// an inet/cidr value, which fixes the spelling a `--where` literal must use to
// compare against it.
type NetworkLiteralRendering uint8

const (
	// NetworkLiteralRenderingUnknown is the zero value: the engine has not
	// named its rendering, so no spelling can be known correct. Value
	// comparisons against a network column are REFUSED under it, rather than
	// compared under a guessed rendering that would silently drop every
	// change to every matching row. It is what a resolver that does not
	// implement [NetworkLiteralResolver] gets — and what hand-built test
	// ColumnInfos get, deliberately.
	NetworkLiteralRenderingUnknown NetworkLiteralRendering = iota
	// NetworkLiteralRenderingHostBare is the Postgres `inet` rule: the prefix
	// length is omitted when it is the FULL width of the address family and
	// shown otherwise. "10.0.0.1" stays bare; "10.0.0.1/32" canonicalises DOWN
	// to "10.0.0.1"; "10.0.0.0/24" and even the host-bits-set "10.0.0.5/24"
	// keep their mask.
	NetworkLiteralRenderingHostBare
	// NetworkLiteralRenderingAlwaysMasked is the Postgres `cidr` rule: the
	// prefix length is ALWAYS present, including at full width. A bare
	// "10.0.0.1" canonicalises UP to "10.0.0.1/32". This is the mirror image
	// of HostBare, on the sibling type — which is why one rendering per
	// engine was not enough.
	NetworkLiteralRenderingAlwaysMasked
	// NetworkLiteralRenderingAddressOnly is the MariaDB rule for native
	// inet4/inet6: the delivered value is an address with no mask. A
	// full-width prefix literal canonicalises down to the bare address; a
	// NARROWER prefix names a network, which such a column cannot hold, so no
	// stored value could ever match it and it is refused outright.
	NetworkLiteralRenderingAddressOnly
)

// NetworkLiteralResolver is the optional [CollationResolver] companion surface
// a source engine implements to name how its change stream renders network
// values.
//
// isCidr distinguishes the two IR network types, because Postgres renders them
// DIFFERENTLY — `inet` drops a full-width mask, `cidr` keeps it — so an
// engine-only answer would be wrong for one of them whichever way it went.
// Mirrors the fixedChar argument on ResolveStringEquality.
//
// A resolver that does not implement this gets
// [NetworkLiteralRenderingUnknown], under which network columns are refused
// for value comparison — the fail-closed default, because every concrete
// rendering is silently wrong for at least one of the others.
type NetworkLiteralResolver interface {
	ResolveNetworkLiteralRendering(isCidr bool) NetworkLiteralRendering
}

// RenderNetworkAddr renders a in the textual form Postgres and MariaDB
// actually deliver, which is NOT always what Go's netip produces
// (audit 2026-08-04 C1).
//
// # THE TWO SERVERS DO NOT AGREE WITH EACH OTHER
//
// An earlier version of this doc said "both servers render IPv6 through the
// BSD inet_ntop6 convention … and they AGREE on everything else". The second
// half is false, and it was false when written (value-fidelity review,
// 2026-08-04). MariaDB compresses a zero run of length ONE, which RFC 5952
// §4.2.2 forbids and Postgres obeys. Ground-truthed on mariadb 11.4 and 10.11:
//
//	stored 2001:db8:0:1:2:3:4:5   MariaDB 2001:db8::1:2:3:4:5   PG (uncompressed)
//	stored 0:1:2:3:4:5:6:7        MariaDB ::1:2:3:4:5:6:7       PG (uncompressed)
//
// So the rendering is chosen per [NetworkLiteralRendering], not once for all
// engines: the two Postgres arms use the RFC's minimum run of 2, and the
// MariaDB arm uses 1. Sharing one rule is what made the claim above look
// plausible.
//
// # The dotted-quad divergence, which BOTH servers share
//
// Both render the trailing 32 bits as a dotted quad whenever the leading 96
// bits are zero and the address is not one of the short forms. Go's netip
// prints a dotted quad ONLY for IPv4-mapped addresses (`::ffff:a.b.c.d`). So:
//
//	::1.2.3.4          server ::1.2.3.4          netip ::102:304
//	::10.0.0.1         server ::10.0.0.1         netip ::a00:1
//	::255.255.255.255  server ::255.255.255.255  netip ::ffff:ffff
//	::1:0              server ::0.1.0.0          netip ::1:0
//
// and they AGREE on everything else, including `::`, `::1`, `::2`,
// `::ffff:10.0.0.1` and every ordinary global address.
//
// # Why this matters, and why it is not cosmetic
//
// The `--where` gate compares an operator's literal against the spelling the
// change stream delivers, byte-exactly. Rendering the literal with netip meant
// a predicate on a diverging address compiled clean and then matched NOTHING —
// every CDC change to a matching row silently dropped, at exit 0. The gate was
// asking "is this canonical as a Go value?" when the only question that matters
// is "is this what the server will send?".
//
// # The rule
//
// Derived from `inet_ntop6`'s longest-zero-run logic and then checked against
// the enumerated shapes above. The C code's dotted branch fires when the best
// zero run starts at word 0 and either has length 6, or has length 5 with
// word 5 == 0xffff (the IPv4-mapped case netip already handles). Its third
// documented clause — length 7 with a non-1 final word — is unreachable,
// because a run of length 7 covers word 6 and the loop skips it. That leaves
// exactly one shape netip gets differently: leading 96 bits zero, word 6
// non-zero.
// A zone-scoped address (`fe80::1%eth0`) is returned unchanged. Neither server
// accepts one — PG raises "invalid input syntax for type inet", MariaDB errno
// 1292 — so no stored value can carry a zone, and the CALLER refuses such a
// literal rather than silently dropping the zone here (which the dotted-quad
// branch would otherwise do).
func RenderNetworkAddr(a netip.Addr, rendering NetworkLiteralRendering) string {
	if a.Zone() != "" {
		return a.String()
	}
	if !a.Is6() || a.Is4In6() {
		// IPv4, and IPv4-mapped v6, already agree everywhere.
		return a.String()
	}
	b := a.As16()

	leading96Zero := true
	for i := range 12 {
		if b[i] != 0 {
			leading96Zero = false
			break
		}
	}
	// The dotted-quad form, shared by both servers: leading 96 bits zero and
	// word 6 non-zero (if word 6 were zero the run would swallow it and the
	// server prints `::`, `::1`, `::2` instead).
	if leading96Zero && (b[12] != 0 || b[13] != 0) {
		return fmt.Sprintf("::%d.%d.%d.%d", b[12], b[13], b[14], b[15])
	}

	if rendering == NetworkLiteralRenderingAddressOnly {
		// MariaDB: minimum zero-run of ONE. netip obeys RFC 5952's minimum of
		// two, so it leaves `2001:db8:0:1:2:3:4:5` uncompressed where MariaDB
		// sends `2001:db8::1:2:3:4:5`.
		return compressIPv6MinRunOne(b)
	}
	// Postgres: RFC-conformant, which is what netip already produces.
	return a.String()
}

// compressIPv6MinRunOne renders the 16 bytes with MariaDB's zero-run
// compression: longest run wins, leftmost on a tie, and a run of ONE is
// compressed.
func compressIPv6MinRunOne(b [16]byte) string {
	var w [8]uint16
	for i := range w {
		w[i] = uint16(b[2*i])<<8 | uint16(b[2*i+1])
	}
	bestBase, bestLen, curBase, curLen := -1, 0, -1, 0
	for i, v := range w {
		if v == 0 {
			if curBase == -1 {
				curBase, curLen = i, 1
			} else {
				curLen++
			}
			if curLen > bestLen {
				bestBase, bestLen = curBase, curLen
			}
			continue
		}
		curBase, curLen = -1, 0
	}

	var sb strings.Builder
	for i := range 8 {
		if bestBase != -1 && i >= bestBase && i < bestBase+bestLen {
			if i == bestBase {
				sb.WriteByte(':')
			}
			continue
		}
		if i != 0 {
			sb.WriteByte(':')
		}
		sb.WriteString(strconv.FormatUint(uint64(w[i]), 16))
	}
	if bestBase != -1 && bestBase+bestLen == 8 {
		sb.WriteByte(':')
	}
	return sb.String()
}

// RenderNetworkPrefix is [RenderNetworkAddr] for a masked value.
func RenderNetworkPrefix(p netip.Prefix, rendering NetworkLiteralRendering) string {
	return RenderNetworkAddr(p.Addr(), rendering) + "/" + strconv.Itoa(p.Bits())
}
