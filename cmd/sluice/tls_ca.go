// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// CA-pinned verify-ca TLS flags (ADR-0158).
//
// MySQL's auto-generated server certificates carry no SubjectAltName, so a
// hostname-verifying TLS config can never validate them. --source-tls-ca /
// --target-tls-ca point at a PEM CA and request "verify-ca" TLS: trust that
// CA, verify the server cert chains to it, skip the hostname check. The
// engine-side construction (the security-critical *tls.Config +
// VerifyPeerCertificate callback) lives in internal/engines/mysql/tls_ca.go;
// this seam threads the flag onto the endpoint DSN.
//
// The flags are per-ENDPOINT (source vs target may pin different CAs), so —
// unlike the process-wide value-fidelity flags on Globals — they live on the
// command and are applied by [applyEndpointTLSCA] alongside applyEngineOptions.

// sourceTLSCAFlag carries --source-tls-ca for a command with a MySQL source
// endpoint. Embedded so the flag parses identically across migrate / sync
// start / verify / backup.
type sourceTLSCAFlag struct {
	SourceTLSCA string `name:"source-tls-ca" help:"Path to a PEM CA certificate for CA-pinned verify-ca TLS to a MySQL SOURCE (ADR-0158). Trusts this CA, verifies the server certificate chains to it, and skips the hostname check — the strongest mode that works against MySQL's SAN-less auto-generated certs. Applies to both the data connection and the binlog/CDC stream. Refused if the source DSN already sets tls=. Postgres sources use sslrootcert=/path/ca.pem in the DSN instead." placeholder:"PATH"`
}

// targetTLSCAFlag carries --target-tls-ca for a command with a MySQL target
// endpoint.
type targetTLSCAFlag struct {
	TargetTLSCA string `name:"target-tls-ca" help:"Path to a PEM CA certificate for CA-pinned verify-ca TLS to a MySQL TARGET (ADR-0158). Trusts this CA, verifies the server certificate chains to it, and skips the hostname check (MySQL's auto-generated certs carry no SubjectAltName). Refused if the target DSN already sets tls=. Postgres targets use sslrootcert=/path/ca.pem in the DSN instead." placeholder:"PATH"`
}

// applyEndpointTLSCA rewrites endpointDSN to use CA-pinned verify-ca TLS
// (ADR-0158) when caPath is set and the endpoint is a MySQL-family engine. It
// mirrors applyEngineOptions/labelEngine: the engine-specific behaviour is
// reached through an inline structural interface so this stays out of the
// neutral ir package. role ("source"/"target") shapes the error messages.
//
// A non-MySQL engine (Postgres, SQLite) does not implement DSNWithVerifyCATLS
// and is REFUSED loudly, naming the DSN-native mechanism — Postgres already
// takes sslrootcert=/path/ca.pem in the DSN (pgx/libpq verify-ca), so silently
// ignoring the flag there would give a false sense of a secured connection.
// An empty caPath is a no-op: the endpoint DSN is returned unchanged.
func applyEndpointTLSCA(e ir.Engine, endpointDSN, caPath, role string) (string, error) {
	if caPath == "" {
		return endpointDSN, nil
	}
	if c, ok := e.(interface {
		DSNWithVerifyCATLS(dsn, caPath string) (string, error)
	}); ok {
		newDSN, err := c.DSNWithVerifyCATLS(endpointDSN, caPath)
		if err != nil {
			return "", fmt.Errorf("--%s-tls-ca: %w", role, err)
		}
		return newDSN, nil
	}
	return "", fmt.Errorf(
		"--%s-tls-ca is only supported for MySQL endpoints; the %s engine %q does not use it — "+
			"for a Postgres endpoint put sslrootcert=/path/to/ca.pem in the --%s DSN instead (pgx/libpq verify-ca)",
		role, role, e.Name(), role,
	)
}
