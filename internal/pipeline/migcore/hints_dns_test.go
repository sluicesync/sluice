// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// withFakeAAAA installs a fake AAAA probe for the duration of a test.
func withFakeAAAA(t *testing.T, fn func(ctx context.Context, host string) ([]net.IP, error)) {
	t.Helper()
	prev := lookupIPv6
	lookupIPv6 = fn
	t.Cleanup(func() { lookupIPv6 = prev })
}

// supabaseDirectDNSError builds the live-probed Supabase shape: an
// IPv4-only network resolving an AAAA-only direct endpoint. Both
// flavors of the class are exercised by the callers — IsNotFound
// (Go's own resolver / most getaddrinfo mappings) and the Windows
// WSANO_DATA text without the flag.
func supabaseDirectDNSError(host, errText string, isNotFound bool) error {
	return fmt.Errorf("pipeline: open source schema reader: %w", &net.DNSError{
		Err:        errText,
		Name:       host,
		IsNotFound: isNotFound,
	})
}

// TestWrapWithHint_IPv6OnlyResolve pins the item-69f hint end to end
// through the real WrapWithHint boundary: a no-data resolve failure
// for a host that DOES carry an AAAA record gains the IPv6-only
// remedy and the SLUICE-E-CONNECT-IPV6-ONLY code — for BOTH shapes of
// the resolve-failure class (IsNotFound flag, and Windows' "no data
// of the requested type" text without the flag).
func TestWrapWithHint_IPv6OnlyResolve(t *testing.T) {
	const host = "db.abcdefghijkl.supabase.co"
	withFakeAAAA(t, func(_ context.Context, h string) ([]net.IP, error) {
		if h != host {
			t.Errorf("AAAA probe asked for %q; want %q", h, host)
		}
		return []net.IP{net.ParseIP("2600:1f18::1")}, nil
	})

	shapes := []struct {
		name string
		err  error
	}{
		{"IsNotFound flag", supabaseDirectDNSError(host, "no such host", true)},
		{"windows WSANO_DATA text", supabaseDirectDNSError(host,
			"getaddrinfow: The requested name is valid, but no data of the requested type was found.", false)},
	}
	for _, s := range shapes {
		t.Run(s.name, func(t *testing.T) {
			got := WrapWithHint(PhaseConnect, s.err)
			ce, ok := sluicecode.FromError(got)
			if !ok {
				t.Fatalf("WrapWithHint did not attach a code; got: %v", got)
			}
			if ce.Code != sluicecode.CodeConnectIPv6Only {
				t.Errorf("Code = %q; want %q", ce.Code, sluicecode.CodeConnectIPv6Only)
			}
			msg := got.Error()
			for _, want := range []string{host, "IPv6-only", "pooler endpoint", "IPv4 add-on", "CDC"} {
				if !strings.Contains(msg, want) {
					t.Errorf("message should mention %q; got: %v", want, msg)
				}
			}
			var dnsErr *net.DNSError
			if !errors.As(got, &dnsErr) {
				t.Error("original net.DNSError must stay traversable in the chain")
			}
		})
	}
}

// TestWrapWithHint_IPv6OnlyResolve_PhaseIndependent pins that the
// structural classifier fires in ANY phase — the same failure hits
// the CDC open (the Supabase direct endpoint is exactly where the
// replication connection must go).
func TestWrapWithHint_IPv6OnlyResolve_PhaseIndependent(t *testing.T) {
	withFakeAAAA(t, func(context.Context, string) ([]net.IP, error) {
		return []net.IP{net.ParseIP("2600:1f18::1")}, nil
	})
	err := supabaseDirectDNSError("db.x.supabase.co", "no such host", true)
	got := WrapWithHint(PhaseCDC, err)
	if ce, ok := sluicecode.FromError(got); !ok || ce.Code != sluicecode.CodeConnectIPv6Only {
		t.Errorf("PhaseCDC: want the IPv6-only code; got %v", got)
	}
}

// TestWrapWithHint_ResolveFailureWithoutAAAA_FallsThrough pins the
// negative arm: a genuinely-unknown hostname (no AAAA either) must
// NOT claim the IPv6-only hint — it falls through to the static
// registry (which has no entry for this text), returning the error
// unchanged.
func TestWrapWithHint_ResolveFailureWithoutAAAA_FallsThrough(t *testing.T) {
	withFakeAAAA(t, func(context.Context, string) ([]net.IP, error) {
		return nil, &net.DNSError{Err: "no such host", Name: "nope.example", IsNotFound: true}
	})
	err := supabaseDirectDNSError("nope.example", "no such host", true)
	got := WrapWithHint(PhaseConnect, err)
	if _, ok := sluicecode.FromError(got); ok {
		t.Errorf("no-AAAA host must not gain a code; got %v", got)
	}
	//nolint:errorlint // pinning identity passthrough
	if got != err {
		t.Errorf("want the error unchanged; got %v", got)
	}
}

// TestWrapWithHint_NonResolveDNSErrorFallsThrough pins that a DNS
// error OUTSIDE the not-found/no-data class (a timeout) never probes
// and never claims the hint.
func TestWrapWithHint_NonResolveDNSErrorFallsThrough(t *testing.T) {
	withFakeAAAA(t, func(context.Context, string) ([]net.IP, error) {
		t.Error("AAAA probe must not run for a non-resolve-class DNS error")
		return nil, nil
	})
	err := fmt.Errorf("connect: %w", &net.DNSError{
		Err: "i/o timeout", Name: "db.example", IsTimeout: true,
	})
	if got := WrapWithHint(PhaseConnect, err); got != err { //nolint:errorlint // identity passthrough pin
		t.Errorf("want the error unchanged; got %v", got)
	}
}

// TestWrapWithHint_StaticRegistryUnaffected pins that the dynamic
// classifier does not shadow the existing static hints: the classic
// "connection refused" connect hint still fires (no DNS error in the
// chain, no probe).
func TestWrapWithHint_StaticRegistryUnaffected(t *testing.T) {
	withFakeAAAA(t, func(context.Context, string) ([]net.IP, error) {
		t.Error("AAAA probe must not run without a net.DNSError in the chain")
		return nil, nil
	})
	err := errors.New("dial tcp 127.0.0.1:5432: connection refused")
	got := WrapWithHint(PhaseConnect, err)
	ce, ok := sluicecode.FromError(got)
	if !ok || ce.Code != sluicecode.CodeConnectRefused {
		t.Errorf("static connection-refused hint should still fire; got %v", got)
	}
}
