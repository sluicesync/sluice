// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// AAAA-only resolve hint (roadmap item 69f, live-probed against
// Supabase 2026-07-15).
//
// Supabase free-tier direct endpoints carry ONLY an AAAA record (IPv4
// is a paid add-on), so from an IPv4-only host sluice fails in ~1s
// with the platform resolver's cryptic no-data error (Windows:
// `getaddrinfow: ... no data of the requested type`). The static hint
// registry can't express this — whether the host is IPv6-only is a
// fact about DNS, not about the error text — so [WrapWithHint] runs
// this dynamic classifier first: when the error chain carries a
// [net.DNSError] of the not-found/no-data class, probe the host for
// an AAAA record; if one exists, the host is resolvable-but-IPv6-only
// and the error gains the pooler-vs-add-on remedy.
//
// The probe fires only on that already-terminal resolve-failure path
// and is bounded (see dnsProbeTimeout), so it adds no cost to any
// healthy run. A host with no AAAA either (a genuinely unknown name)
// falls through to the static registry unchanged.

// dnsProbeTimeout bounds the AAAA probe so a slow resolver can't
// stall the error path it decorates.
const dnsProbeTimeout = 3 * time.Second

// lookupIPv6 is the AAAA probe, a seam so unit tests can pin the
// classifier without real DNS. Production uses the default resolver
// with the "ip6" network, which asks specifically for AAAA records.
var lookupIPv6 = func(ctx context.Context, host string) ([]net.IP, error) {
	return net.DefaultResolver.LookupIP(ctx, "ip6", host)
}

// dnsResolveHint classifies err's resolve-failure shape. Returns the
// matched hint entry and true when the chain carries a no-data /
// not-found [net.DNSError] for a host that DOES have an AAAA record;
// (zero, false) otherwise — including on probe failure, so the static
// registry keeps owning every other shape.
func dnsResolveHint(err error) (errorHint, bool) {
	var dnsErr *net.DNSError
	if !errors.As(err, &dnsErr) || dnsErr.Name == "" {
		return errorHint{}, false
	}
	// The not-found/no-data class. IsNotFound covers Go's own resolver
	// and most getaddrinfo mappings; the substring covers Windows'
	// WSANO_DATA ("no data of the requested type"), which reaches the
	// DNSError text without the flag on some Go/Windows combinations.
	if !dnsErr.IsNotFound && !strings.Contains(strings.ToLower(dnsErr.Err), "no data") {
		return errorHint{}, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), dnsProbeTimeout)
	defer cancel()
	ips, probeErr := lookupIPv6(ctx, dnsErr.Name)
	if probeErr != nil || len(ips) == 0 {
		return errorHint{}, false
	}
	return errorHint{
		hint: fmt.Sprintf(
			"host %q is IPv6-only (it has an AAAA record but did not resolve from this network, which appears "+
				"IPv4-only — e.g. a Supabase direct endpoint without the IPv4 add-on): for bulk migrate use the "+
				"provider's pooler endpoint; for CDC the direct endpoint is required — a pooler cannot proxy "+
				"replication — so enable the provider's IPv4 add-on or run sluice from an IPv6-capable network",
			dnsErr.Name,
		),
		code: sluicecode.CodeConnectIPv6Only,
	}, true
}
