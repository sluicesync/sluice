// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/go-sql-driver/mysql"
)

// CA-pinned verify-ca TLS for MySQL connections (ADR-0158).
//
// MySQL's auto-generated server certificates carry NO SubjectAltName, and
// Go's TLS stack will not fall back to the CN — so a hostname-verifying
// ("verify-full") config can never validate them. The practical secure mode,
// and what Postgres calls sslmode=verify-ca, is: trust a specified CA, verify
// that the presented server certificate CHAINS to it, but skip the hostname
// check. The operator supplies the CA via --source-tls-ca / --target-tls-ca;
// this file builds the config, registers it with the driver, and splices the
// endpoint DSN's tls= to reference it.
//
// The security invariant is the whole point: a server certificate NOT signed
// by the provided CA MUST be rejected (the handshake fails). That check lives
// in the [tls.Config.VerifyPeerCertificate] callback built by
// [buildVerifyCATLSConfig].

// verifyCATLSCounter names each registered verify-ca config uniquely so a
// source and a target endpoint in the same process never collide on the
// go-sql-driver global TLS-config registry.
var verifyCATLSCounter atomic.Uint64

// buildVerifyCATLSConfig builds a CA-pinned "verify-ca" *tls.Config from the
// PEM CA file at caPath. It:
//
//   - loads the CA PEM into an x509.CertPool, failing LOUDLY if the file is
//     unreadable or contains no valid certificate;
//   - sets InsecureSkipVerify to bypass Go's built-in hostname verification
//     (MySQL server certs have no SAN, so it can never pass) — but installs a
//     VerifyPeerCertificate callback that MANUALLY verifies the presented
//     chain against the CA pool with NO DNSName, so the peer is still
//     AUTHENTICATED: a cert that does not chain to the provided CA is refused
//     and the handshake fails;
//   - floors the transport at TLS 1.2.
//
// This is the load-bearing security code; see ADR-0158 and the wrong-CA-
// rejected pin in tls_ca_test.go.
func buildVerifyCATLSConfig(caPath string) (*tls.Config, error) {
	pemBytes, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("read CA file %q: %w", caPath, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("no valid PEM certificate found in CA file %q", caPath)
	}
	return &tls.Config{
		// verify-ca: Go's default verification checks the hostname against the
		// certificate SANs, which MySQL server certs lack — so the built-in
		// verification is disabled here and replaced by the chain-only check in
		// VerifyPeerCertificate below. The peer is NOT unauthenticated: a cert
		// that does not chain to the pinned CA is rejected there.
		InsecureSkipVerify: true, //nolint:gosec // verify-ca: chain verified in VerifyPeerCertificate, hostname intentionally skipped (MySQL certs lack SANs). ADR-0158.
		MinVersion:         tls.VersionTLS12,
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			certs := make([]*x509.Certificate, 0, len(rawCerts))
			for _, raw := range rawCerts {
				c, err := x509.ParseCertificate(raw)
				if err != nil {
					return fmt.Errorf("verify-ca: parse server certificate: %w", err)
				}
				certs = append(certs, c)
			}
			if len(certs) == 0 {
				return errors.New("verify-ca: server presented no certificate")
			}
			// The leaf is certs[0]; any remaining certs are intermediates the
			// server sent. Verify the leaf chains to the pinned CA (Roots),
			// with NO DNSName so the hostname is not checked.
			opts := x509.VerifyOptions{
				Roots:         pool,
				Intermediates: x509.NewCertPool(),
			}
			for _, ic := range certs[1:] {
				opts.Intermediates.AddCert(ic)
			}
			if _, err := certs[0].Verify(opts); err != nil {
				return fmt.Errorf("verify-ca: server certificate does not chain to the provided CA: %w", err)
			}
			return nil
		},
	}, nil
}

// isVerifyCATLSConfig reports whether c is a verify-ca config built by
// [buildVerifyCATLSConfig]: InsecureSkipVerify set (hostname skipped) AND a
// VerifyPeerCertificate callback present (chain authenticated). A BLIND
// tls=skip-verify config sets InsecureSkipVerify with NO callback, so this
// cleanly distinguishes the authenticated verify-ca case from the
// unauthenticated skip-verify case — which is what the binlog transport WARN
// keys off (see warnBinlogTransport).
func isVerifyCATLSConfig(c *tls.Config) bool {
	return c != nil && c.InsecureSkipVerify && c.VerifyPeerCertificate != nil
}

// binlogTLSModeLabel returns the transport-mode label carried onto a CDC
// reader for its stream-open WARN. A verify-ca config is relabeled "verify-ca"
// (so the WARN path can recognise it as authenticated rather than a blind
// skip-verify); every other config keeps its raw DSN tls= value (rawMode).
func binlogTLSModeLabel(rawMode string, tc *tls.Config) string {
	if isVerifyCATLSConfig(tc) {
		return "verify-ca"
	}
	return rawMode
}

// DSNWithVerifyCATLS returns a copy of dsn rewritten to use CA-pinned
// verify-ca TLS (ADR-0158) built from the PEM CA at caPath. It builds the
// verify-ca *tls.Config (loud failure on an unreadable file or a PEM with no
// certificate), registers it under a unique name via mysql.RegisterTLSConfig,
// and re-emits dsn with its tls= parameter pointing at that name. Because
// every MySQL open path parses the DSN through the driver's ParseDSN — which
// resolves the registered name into cfg.TLS — the verify-ca posture reaches
// BOTH the data-plane database/sql connections AND the binlog replication
// stream (which clones cfg.TLS via binlogTLSFromConfig) with no per-open
// wiring.
//
// Composition with an explicit DSN tls=: this REFUSES loudly if dsn already
// carries a tls= parameter. The CA flag and a DSN tls= are two conflicting
// declarations of the same transport; rather than silently override one with
// the other, the operator must supply exactly one. See ADR-0158.
//
// The method has a value receiver and ignores the Engine's flavor: the
// verify-ca posture is a property of the DSN's transport, identical for every
// MySQL-compatible flavor. (VStream flavors dial their gRPC endpoint with a
// separate TLS config — see vstreamTLSConfigFromDSN — which this does not
// touch; the CA flag governs the MySQL data/binlog plane.)
func (Engine) DSNWithVerifyCATLS(dsn, caPath string) (string, error) {
	if v, ok := dsnParamValue(dsn, "tls"); ok {
		return "", fmt.Errorf(
			"--tls-ca conflicts with the explicit tls=%q parameter already in the DSN; "+
				"supply exactly one (the flag provides CA-pinned verify-ca TLS)", v,
		)
	}
	tc, err := buildVerifyCATLSConfig(caPath)
	if err != nil {
		return "", err
	}
	// Raw ParseDSN (not the sluice parseDSN): this is a faithful string
	// round-trip, not an open — finishParseDSN's keep-alive net swap /
	// time_zone / collation injection would otherwise bake into the re-emitted
	// DSN, and the engine re-applies them when it actually opens.
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("parse DSN: %w", err)
	}
	name := fmt.Sprintf("sluice-verifyca-%d", verifyCATLSCounter.Add(1))
	if err := mysql.RegisterTLSConfig(name, tc); err != nil {
		return "", fmt.Errorf("register verify-ca TLS config: %w", err)
	}
	cfg.TLSConfig = name
	// ADR-0153 explicit-DSN-wins preservation (mirrors [Engine.WithDatabase]):
	// FormatDSN omits any param whose value equals the driver default, so an
	// operator's explicit interpolateParams=false would silently vanish and a
	// downstream flavor-aware parse would then apply the PlanetScale/Vitess
	// interpolation default the operator opted out of. Re-materialize it.
	if dsnSetsInterpolateParams(dsn) && !cfg.InterpolateParams {
		if cfg.Params == nil {
			cfg.Params = map[string]string{}
		}
		cfg.Params["interpolateParams"] = "false"
	}
	return cfg.FormatDSN(), nil
}
