//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration ground truth for the binlog stream's DSN-derived TLS
// (audit finding N-3). MySQL 8 auto-generates a self-signed server
// certificate at initialization, which gives the shared container both
// sides of the pin for free:
//
//   - tls=skip-verify: the binlog stream ACTUALLY negotiates TLS (the
//     go-mysql client sends an SSLRequest and upgrades; a server
//     without TLS would refuse loudly) and rows replicate over it.
//   - tls=true: certificate verification is enforced — the self-signed
//     cert is refused loudly, on the query connections AND on the
//     binlog connection itself.
//   - default (no tls param): the historical plaintext stream still
//     works, now with the transport WARN at stream open.
//
// This file covers the NATIVE binlog path only; the VStream flavors'
// TLS (TLS-by-default, vstream_insecure_tls) is pinned by the vstream
// suites and untouched by this change.

package mysql

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// captureWarnLogs swaps the default slog handler for a WARN-level
// buffer for the duration of the test. Same pattern as
// rls_warn_integration_test.go; the engines-mysql integration tests
// run sequentially, so the process-global swap is safe here.
func captureWarnLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

// TestCDCReader_TLS_SkipVerifyStreamsOverTLS proves the fix end to end
// through the real DSN plumbing: with tls=skip-verify the binlog
// syncer dials TLS (rows arriving proves the encrypted stream works —
// go-mysql refuses loudly when a TLS config is set and the server
// lacks TLS, so plaintext cannot masquerade as success) and the
// every-stream-open downgrade WARN fires, mirroring the
// vstream_insecure_tls precedent.
func TestCDCReader_TLS_SkipVerifyStreamsOverTLS(t *testing.T) {
	dsn, cleanup := startMySQLForCDC(t)
	defer cleanup()

	applyMySQL(t, dsn, `
		CREATE TABLE notes (
			id   BIGINT       NOT NULL AUTO_INCREMENT,
			body VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)

	buf := captureWarnLogs(t)

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, dsn+"&tls=skip-verify")
	if err != nil {
		t.Fatalf("OpenCDCReader (tls=skip-verify): %v", err)
	}
	defer func() { _ = rdr.(*CDCReader).Close() }()

	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	applyMySQL(t, dsn, `INSERT INTO notes (body) VALUES ('over tls');`)

	got := drainChanges(t, ctx, changes, 1, 30*time.Second)
	if len(got) != 1 {
		if streamErr := rdr.(*CDCReader).Err(); streamErr != nil {
			t.Fatalf("got %d changes; want 1 (stream error: %v)", len(got), streamErr)
		}
		t.Fatalf("got %d changes; want 1", len(got))
	}
	ins, ok := got[0].(ir.Insert)
	if !ok {
		t.Fatalf("change[0] = %T; want ir.Insert", got[0])
	}
	if body, _ := ins.Row["body"].(string); body != "over tls" {
		t.Errorf("Row[body] = %#v; want \"over tls\"", ins.Row["body"])
	}

	warns := buf.String()
	if !strings.Contains(warns, "certificate verification is DISABLED") ||
		!strings.Contains(warns, "binlog replication stream") {
		t.Errorf("skip-verify downgrade WARN did not fire at stream open; warn output:\n%s", warns)
	}
}

// TestCDCReader_TLS_VerifyRefusesSelfSignedCert pins the loud-failure
// side: tls=true against the container's auto-generated self-signed
// certificate must refuse, never silently downgrade. Two layers:
//
//  1. end to end — OpenCDCReader's query connection fails certificate
//     verification first (the DSN governs both connection kinds);
//  2. the binlog connection ITSELF — a reader whose schema-cache DB
//     rides plaintext but whose binlogTLS is the verifying config
//     fails at StreamChanges, proving BinlogSyncerConfig.TLSConfig is
//     actually wired through and enforced on the replication stream
//     (the load-bearing claim of the N-3 fix), not silently ignored.
func TestCDCReader_TLS_VerifyRefusesSelfSignedCert(t *testing.T) {
	dsn, cleanup := startMySQLForCDC(t)
	defer cleanup()

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// (1) End to end: the tls=true DSN is refused at open (the query
	// connection's ping verifies first).
	if rdr, err := eng.OpenCDCReader(ctx, dsn+"&tls=true"); err == nil {
		_ = rdr.(*CDCReader).Close()
		t.Fatal("OpenCDCReader (tls=true) accepted a self-signed server certificate; want loud verification failure")
	} else if !strings.Contains(err.Error(), "certificate") {
		t.Errorf("OpenCDCReader (tls=true) error = %q; want a certificate-verification failure", err)
	}

	// (2) The binlog connection itself enforces verification: build the
	// reader over the plaintext DSN (so the schema-cache DB opens), then
	// swap in the verifying binlog TLS config exactly as a tls=true DSN
	// would have produced it.
	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader (plaintext): %v", err)
	}
	cdcRdr := rdr.(*CDCReader)
	defer func() { _ = cdcRdr.Close() }()

	cfg, err := parseDSN(dsn + "&tls=true")
	if err != nil {
		t.Fatalf("parseDSN (tls=true): %v", err)
	}
	cdcRdr.binlogTLS = binlogTLSFromConfig(cfg, cdcRdr.host)
	cdcRdr.binlogTLSMode = cfg.TLSConfig

	if _, err := cdcRdr.StreamChanges(ctx, ir.Position{}); err == nil {
		t.Fatal("StreamChanges accepted a self-signed server certificate on the binlog connection; want loud verification failure")
	} else if !strings.Contains(err.Error(), "certificate") {
		t.Errorf("StreamChanges (verifying binlog TLS) error = %q; want a certificate-verification failure", err)
	}
}

// TestCDCReader_TLS_DefaultPlaintextWorksAndWarns pins the
// zero-behavior-change guarantee: a DSN with no tls param streams
// exactly as before (plaintext), with the new one-per-stream-open
// transport WARN as the only delta.
func TestCDCReader_TLS_DefaultPlaintextWorksAndWarns(t *testing.T) {
	dsn, cleanup := startMySQLForCDC(t)
	defer cleanup()

	applyMySQL(t, dsn, `
		CREATE TABLE notes (
			id   BIGINT       NOT NULL AUTO_INCREMENT,
			body VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)

	buf := captureWarnLogs(t)

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() { _ = rdr.(*CDCReader).Close() }()

	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	applyMySQL(t, dsn, `INSERT INTO notes (body) VALUES ('plaintext');`)

	got := drainChanges(t, ctx, changes, 1, 30*time.Second)
	if len(got) != 1 {
		if streamErr := rdr.(*CDCReader).Err(); streamErr != nil {
			t.Fatalf("got %d changes; want 1 (stream error: %v)", len(got), streamErr)
		}
		t.Fatalf("got %d changes; want 1", len(got))
	}
	if _, ok := got[0].(ir.Insert); !ok {
		t.Fatalf("change[0] = %T; want ir.Insert", got[0])
	}

	if !strings.Contains(buf.String(), "UNENCRYPTED") {
		t.Errorf("plaintext transport WARN did not fire at stream open; warn output:\n%s", buf.String())
	}
}
