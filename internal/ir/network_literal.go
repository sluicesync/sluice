package ir

// Network-literal rendering — the inet/cidr axis of the "filtered replicas
// follow the source engine's comparison semantics" contract.
//
// A client-side `--where` filter compares an operator-written literal against
// the value the CHANGE STREAM delivers, so the literal must be spelled the way
// that stream renders it. For the network families the two engines eligible for
// continuous filtered sync disagree, and the disagreement is invisible:
//
//   - Postgres delivers inet/cidr through pgx's InetCodec, whose DecodeValue
//     always yields a [net/netip.Prefix] — so a host address stored in an
//     `inet` column arrives as "10.0.0.1/32", carrying a mask Postgres's own
//     text output omits. (pgx v5.10.0 pgtype/inet.go: DecodeValue scans into a
//     netip.Prefix unconditionally; the netip.Addr shape never comes back from
//     an untyped scan.)
//   - MariaDB delivers native inet4/inet6 as BARE text — a dotted quad, or the
//     BSD inet_ntop6 rendering for inet6 — with no mask, because those columns
//     hold an address rather than a network.
//
// Both collapse to [Inet] in the IR, so a canonicaliser keyed on the IR type
// alone cannot be right for both: normalizing to the prefix form fixes
// Postgres and breaks MariaDB, and normalizing to the bare form does the
// reverse. The engine has to name its own rendering, which is what
// [NetworkLiteralResolver] is for — the same shape as
// [TemporalLiteralResolver] on the temporal axis.
//
// Getting it wrong is silent: the literal simply never equals the delivered
// value, so every row scores out-of-scope and every change to it is dropped
// with no error. That is why the zero value REFUSES rather than guessing —
// unlike the temporal axis, where "no normalization" is a safe pre-existing
// behavior, there is no neutral rendering to fall back to here.

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
	// NetworkLiteralRenderingMasked is the Postgres rule: the delivered value
	// ALWAYS carries a prefix length, including the full-width mask on a host
	// address ("10.0.0.1/32", "2001:db8::/128"). A bare literal is refused
	// with the masked spelling to write instead.
	NetworkLiteralRenderingMasked
	// NetworkLiteralRenderingBare is the MariaDB rule for native inet4/inet6:
	// the delivered value is an address with no mask ("10.0.0.1",
	// "2001:db8::"). A literal carrying a prefix length is refused — those
	// columns cannot hold a network, so no stored value could match it.
	NetworkLiteralRenderingBare
)

// NetworkLiteralResolver is the optional [CollationResolver] companion surface
// a source engine implements to name how its change stream renders inet/cidr
// values. A resolver that does not implement it gets
// [NetworkLiteralRenderingUnknown], under which network columns are refused
// for value comparison — the fail-closed default, because both concrete
// renderings are silently wrong for the other engine.
type NetworkLiteralResolver interface {
	ResolveNetworkLiteralRendering() NetworkLiteralRendering
}
