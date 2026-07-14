// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"crypto/tls"
	"log/slog"

	"github.com/go-sql-driver/mysql"
)

// binlogTLSFromConfig maps the source DSN's `tls=` parameter onto the
// TLS config the binlog syncer dials with, closing the silent-downgrade
// gap where a tls=true DSN encrypted every database/sql query
// connection while the replication stream — carrying every replicated
// row — rode plaintext with no warning (audit finding N-3).
//
// The mapping leans on go-sql-driver's own resolution: ParseDSN's
// normalize step has already turned the tls= value into cfg.TLS
// (tls=true → verifying config with ServerName defaulted from the DSN
// host; tls=skip-verify / tls=preferred → InsecureSkipVerify; a
// REGISTERED custom config name → a clone of the registered config).
// Cloning that config here means the binlog stream gets exactly the
// transport posture the query connections negotiate — including a
// custom config's CA pool / client certs — instead of a hand-rebuilt
// approximation. An UNREGISTERED custom name never reaches this
// helper: ParseDSN itself refuses it loudly ("invalid value / unknown
// config name: ..."), which [TestBinlogTLSFromDSN] pins so a driver
// behavior change can't silently reopen that hole.
//
// Two deliberate deltas from the driver's semantics, both toward the
// loud-failure tenet:
//
//   - tls=preferred: go-sql-driver falls back to plaintext when the
//     server lacks TLS (AllowFallbackToPlaintext). go-mysql's binlog
//     client has no try-TLS-then-plaintext mode, and silently riding
//     plaintext when the query connections may be encrypted is exactly
//     the downgrade class this fix closes — so the binlog stream keeps
//     the skip-verify TLS half of "preferred" and REFUSES the fallback
//     half: against a server with TLS disabled the stream open fails
//     loudly ("the MySQL Server does not support TLS required by the
//     client") and the operator sets tls=false explicitly if plaintext
//     is intended. [CDCReader.warnBinlogTransport] says so at every
//     stream open.
//   - MinVersion is raised to TLS 1.2 when the resolved config leaves
//     it unset (the driver-built true/skip-verify/preferred configs
//     all do), matching the VStream path's floor. A custom registered
//     config that EXPLICITLY sets an older MinVersion is respected —
//     that is the operator's named choice, and an incompatible server
//     still fails the handshake loudly rather than corrupting anything.
//
// nil in (no tls param / tls=false / a nil cfg) → nil out: the
// historical plaintext stream, so every non-DSN construction — unit
// readers built as struct literals, and the zero value generally —
// is byte-identical to pre-fix behavior (the zero-value-safe default).
//
// host is the DSN host (sans port) used to default ServerName on a
// verifying config; the driver's normalize has usually done this
// already, so it only matters for hand-built configs.
func binlogTLSFromConfig(cfg *mysql.Config, host string) *tls.Config {
	if cfg == nil || cfg.TLS == nil {
		return nil
	}
	tc := cfg.TLS.Clone()
	if tc.ServerName == "" && !tc.InsecureSkipVerify {
		tc.ServerName = host
	}
	if tc.MinVersion == 0 {
		tc.MinVersion = tls.VersionTLS12
	}
	return tc
}

// warnBinlogTransport surfaces the binlog stream's transport posture at
// stream open, mirroring the vstream_insecure_tls precedent (warn on
// every use, never silently downgrade). [StreamChanges] is single-use
// per reader, so each WARN fires once per stream open — low-noise for
// the common local-Docker plaintext case, loud enough that a dev DSN
// copied into production is told what its replicated rows ride on.
// Verifying modes (tls=true, a verifying custom config) say nothing.
func (r *CDCReader) warnBinlogTransport(ctx context.Context) {
	switch {
	case r.binlogTLS == nil:
		slog.WarnContext(
			ctx, "mysql: cdc: binlog replication stream is UNENCRYPTED — every replicated row rides plaintext "+
				"(source DSN has no tls parameter, or tls=false); add tls=true to the source DSN to encrypt "+
				"(or tls=skip-verify for a server with a self-signed certificate)",
		)
	case r.binlogTLSMode == "verify-ca":
		// CA-pinned verify-ca (ADR-0158, --source-tls-ca): the server cert is
		// authenticated against the operator's CA in VerifyPeerCertificate;
		// only the hostname check is skipped (MySQL certs carry no SAN). This
		// is NOT the unauthenticated skip-verify case below — say so at INFO,
		// and never emit the "verification DISABLED" WARN.
		slog.InfoContext(
			ctx, "mysql: cdc: binlog TLS: CA-chain verified, hostname check skipped (verify-ca)",
		)
	case r.binlogTLSMode == "preferred":
		slog.WarnContext(
			ctx, "mysql: cdc: tls=preferred — the binlog replication stream uses TLS WITHOUT certificate "+
				"verification and WITHOUT plaintext fallback (the binlog client has no try-TLS-then-plaintext "+
				"mode; against a server with TLS disabled the stream open fails loudly — set tls=false "+
				"explicitly if a plaintext stream is intended, or tls=true for verified TLS)",
		)
	case r.binlogTLS.InsecureSkipVerify:
		// tls=skip-verify, or a registered custom config that sets
		// InsecureSkipVerify — the attr names which.
		slog.WarnContext(
			ctx, "mysql: cdc: TLS certificate verification is DISABLED on the binlog replication stream "+
				"(encrypted, but the peer is unauthenticated; intended for self-signed dev/test servers — "+
				"never use against a production endpoint)",
			slog.String("tls", r.binlogTLSMode),
		)
	}
}
