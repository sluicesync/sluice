# ADR-0158: CA-pinned verify-ca TLS for MySQL connections

## Status

Accepted (implemented; shipping). Adds `--source-tls-ca` / `--target-tls-ca` for
CA-pinned "verify-ca" TLS to MySQL endpoints. Postgres is out of scope (it
already takes `sslrootcert=` in the DSN).

## Context

sluice already maps a source DSN's `tls=` onto both the data-plane
`database/sql` connections and the binlog replication stream (audit finding
N-3, `internal/engines/mysql/cdc_reader_tls.go`). The available `tls=` modes,
however, force an operator to choose between two unsatisfying options against a
typical self-managed MySQL server:

- `tls=true` — **verify-full**: encrypt AND verify the server certificate,
  including the hostname. MySQL's auto-generated server certificates (the ones
  `mysqld` mints on first start) carry **no SubjectAltName**, and Go's TLS
  stack does not fall back to the certificate CN. So verify-full simply cannot
  validate a stock MySQL server — the handshake fails with a SAN error.
- `tls=skip-verify` — encrypt but **do not authenticate** the peer at all. This
  is unauthenticated transport: it stops a passive eavesdropper but not an
  active man-in-the-middle. sluice warns loudly on it ("certificate
  verification is DISABLED") precisely because it is not a secure posture for a
  production endpoint.

The missing middle is exactly what Postgres calls **`sslmode=verify-ca`**:
trust a specified CA, verify that the presented server certificate **chains to
it**, but skip the hostname check (which the SAN-less cert cannot satisfy
anyway). That authenticates the server against the operator's own CA — the
practical secure mode for MySQL's certificate reality.

## Decision

Add two per-endpoint flags:

- `--source-tls-ca <path>` — verify-ca for a MySQL **source** (applies to the
  data connection AND the binlog/CDC stream).
- `--target-tls-ca <path>` — verify-ca for a MySQL **target** (data
  connection).

Each points at a PEM CA file. When set for a MySQL endpoint, sluice builds a
`*tls.Config` (`buildVerifyCATLSConfig`, `internal/engines/mysql/tls_ca.go`)
that:

- loads the CA PEM into an `x509.CertPool`, failing **loudly** if the file is
  unreadable or contains no valid certificate;
- sets `InsecureSkipVerify: true` to bypass Go's built-in hostname
  verification, **and** installs a `VerifyPeerCertificate` callback that
  manually verifies the presented chain against the CA pool via
  `x509.Certificate.Verify(x509.VerifyOptions{Roots: pool})` with **no
  `DNSName`** (hostname not checked). The peer is still **authenticated**: a
  certificate that does not chain to the provided CA is refused and the
  handshake fails. **This is the security invariant.**
- sets `MinVersion: tls.VersionTLS12`.

**The security invariant** — a server certificate not signed by the provided
CA MUST be rejected — is pinned by the wrong-CA-rejected test in
`internal/engines/mysql/tls_ca_test.go`
(`TestBuildVerifyCATLSConfig_AcceptsMatchingRejectsWrongCA`), which generates
two throwaway CAs and proves the config built from CA-A rejects a leaf signed
by CA-B.

### Threading (leak-proof via the DSN)

The engine dials MySQL by parsing a DSN string through the driver's
`ParseDSN`, which resolves a registered `tls=<name>` into `cfg.TLS`.
`DSNWithVerifyCATLS` (an `Engine` method) builds the config, registers it under
a unique name via `mysql.RegisterTLSConfig`, and re-emits the endpoint DSN with
its `tls=` pointing at that name. Because **every** MySQL open path parses
through `ParseDSN`, the verify-ca posture reaches both the data-plane
connections and the binlog stream (which clones `cfg.TLS` via
`binlogTLSFromConfig`) with no per-open wiring. The CLI applies it at the
engine-resolution seam (`applyEndpointTLSCA`, `cmd/sluice/tls_ca.go`) alongside
`applyEngineOptions`, on `migrate`, `sync start`, `verify`, `backup
full|incremental|stream`, and `restore`.

### Warn-logic change

A verify-ca config also sets `InsecureSkipVerify` (to skip the hostname), so
the pre-existing `warnBinlogTransport` would have emitted its
"verification DISABLED" WARN — wrong, because verify-ca IS authenticated. The
CDC reader now carries a `"verify-ca"` transport-mode label
(`binlogTLSModeLabel`, keyed off `InsecureSkipVerify && VerifyPeerCertificate
!= nil`), and `warnBinlogTransport` emits a mild INFO
("CA-chain verified, hostname check skipped (verify-ca)") for it instead of the
DISABLED warning. **Every other case is unchanged**: a blind `tls=skip-verify`
(no `--*-tls-ca`, `VerifyPeerCertificate == nil`) still warns exactly as
before; UNENCRYPTED and `tls=preferred` are untouched.

### Composition with an explicit DSN `tls=`

If the endpoint DSN already carries a `tls=` parameter, `--*-tls-ca` is
**refused loudly** — the flag and the DSN param are two conflicting
declarations of the same transport; the operator supplies exactly one. (Chosen
over silently overriding one with the other.)

### Source/target symmetry and Postgres

The flags are symmetric: source covers reads (data + CDC), target covers
writes. Postgres endpoints are **out of scope** and refused loudly, pointing
the operator at `sslrootcert=/path/ca.pem` in the DSN (pgx/libpq already
implement verify-ca). Silently ignoring the flag on a PG endpoint would give a
false sense of a secured connection, so it is a loud refusal, not a no-op.

The VStream flavors (planetscale / vitess) dial their vtgate gRPC endpoint with
a **separate** TLS config (`vstreamTLSConfigFromDSN`, system roots) that this
change does not touch; the CA flag governs the MySQL data/binlog plane, which
is what the vanilla binlog flavor uses.

## Alternatives considered

- **verify-full (`tls=true`)** — rejected: needs a SAN, which MySQL's
  auto-generated server certs lack, so it cannot validate a stock server. An
  operator who *has* a properly-SAN'd cert and wants hostname verification uses
  `tls=true` in the DSN directly; this feature is for the (common) SAN-less
  case.
- **blind skip-verify (`tls=skip-verify`)** — the status quo: encrypted but
  unauthenticated, warned loudly. verify-ca is strictly stronger (authenticates
  the server against the operator's CA) and is what this ADR adds.
- **A registered custom `tls=<name>` in the DSN** — already possible
  programmatically, but requires the operator to call `RegisterTLSConfig` in
  Go; there is no CLI surface. These flags are the ergonomic CLI form of the
  common verify-ca case.

## Consequences

- Operators can securely connect sluice to a self-managed MySQL server without
  re-issuing its certificate with a SAN.
- One new loud-failure surface: an unreadable / cert-less CA file, and an
  explicit-`tls=`-plus-flag conflict, both refuse before any connection.
- The binlog transport WARN gains a fourth, authenticated case; the three
  pre-existing cases are byte-identical.
- No change to Postgres, to the VStream gRPC dial, or to any non-CLI engine
  construction (the flags are opt-in; the empty path is a no-op).
