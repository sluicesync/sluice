// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// writeThrowawayCA mints a self-signed CA, writes it to a temp PEM file, and
// returns the path — enough for the CLI-layer pins, which only need a VALID CA
// file (the wrong-CA-rejected security assertion lives in the mysql package).
func writeThrowawayCA(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "cli-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write CA file: %v", err)
	}
	return path
}

// dsnIsVerifyCA reports whether the MySQL DSN resolves (through the driver's
// own ParseDSN) to a verify-ca TLS config: InsecureSkipVerify set AND a
// VerifyPeerCertificate callback present. This is the exact shape the engine
// dials with, so asserting it here proves the flag threaded end-to-end.
func dsnIsVerifyCA(t *testing.T, dsn string) bool {
	t.Helper()
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("ParseDSN(%q): %v", dsn, err)
	}
	return cfg.TLS != nil && cfg.TLS.InsecureSkipVerify && cfg.TLS.VerifyPeerCertificate != nil
}

// TestTLSCA_Migrate_Pin is the Bug-180 CLI-layer pin: parse a real `migrate`
// invocation with --source-tls-ca through kong, run the resolve seam, and
// assert the resolved MySQL source DSN carries verify-ca (not skip-verify, not
// plaintext). Absent the flag, the DSN is unchanged.
func TestTLSCA_Migrate_Pin(t *testing.T) {
	caFile := writeThrowawayCA(t)
	const srcDSN = "user:pw@tcp(db.example.com:3306)/app"

	t.Run("flag threads verify-ca onto the source", func(t *testing.T) {
		cli := parseInto(
			t,
			"migrate",
			"--source-driver=mysql", "--source="+srcDSN,
			"--target-driver=postgres", "--target=postgres://u@h/db",
			"--source-tls-ca="+caFile,
		)
		g := &cli.Globals
		if _, _, cleanup, err := cli.Migrate.resolveEngines(context.Background(), g); err != nil {
			cleanup()
			t.Fatalf("resolveEngines: %v", err)
		} else {
			cleanup()
		}
		if !dsnIsVerifyCA(t, cli.Migrate.Source) {
			t.Errorf("source DSN did not become verify-ca after resolve: %q", cli.Migrate.Source)
		}
	})

	t.Run("absent flag leaves the source DSN unchanged", func(t *testing.T) {
		cli := parseInto(
			t,
			"migrate",
			"--source-driver=mysql", "--source="+srcDSN,
			"--target-driver=postgres", "--target=postgres://u@h/db",
		)
		g := &cli.Globals
		_, _, cleanup, err := cli.Migrate.resolveEngines(context.Background(), g)
		if err != nil {
			cleanup()
			t.Fatalf("resolveEngines: %v", err)
		}
		cleanup()
		if cli.Migrate.Source != srcDSN {
			t.Errorf("source DSN mutated with no --source-tls-ca: got %q want %q", cli.Migrate.Source, srcDSN)
		}
	})
}

// TestTLSCA_SyncStart_Pin mirrors the migrate pin for `sync start`, and also
// pins the target side.
func TestTLSCA_SyncStart_Pin(t *testing.T) {
	caFile := writeThrowawayCA(t)
	const srcDSN = "user:pw@tcp(src.example.com:3306)/app"
	const tgtDSN = "user:pw@tcp(dst.example.com:3306)/app"

	cli := parseInto(
		t,
		"sync", "start",
		"--source-driver=mysql", "--source="+srcDSN,
		"--target-driver=mysql", "--target="+tgtDSN,
		"--source-tls-ca="+caFile, "--target-tls-ca="+caFile,
	)
	if _, _, err := cli.Sync.Start.resolveEngines(context.Background(), &cli.Globals); err != nil {
		t.Fatalf("resolveEngines: %v", err)
	}
	if !dsnIsVerifyCA(t, cli.Sync.Start.Source) {
		t.Errorf("source DSN not verify-ca: %q", cli.Sync.Start.Source)
	}
	if !dsnIsVerifyCA(t, cli.Sync.Start.Target) {
		t.Errorf("target DSN not verify-ca: %q", cli.Sync.Start.Target)
	}
}

// TestTLSCA_PostgresEndpointRefused pins that --source-tls-ca against a
// Postgres endpoint is refused LOUDLY (pointing at sslrootcert) rather than
// silently ignored — silent ignore would give a false sense of a secured
// connection.
func TestTLSCA_PostgresEndpointRefused(t *testing.T) {
	caFile := writeThrowawayCA(t)
	pg := mustEngine(t, "postgres")

	_, err := applyEndpointTLSCA(pg, "postgres://u@h/db", caFile, "source")
	if err == nil {
		t.Fatal("applyEndpointTLSCA on a Postgres endpoint: err = nil; want a loud refusal")
	}
	if !strings.Contains(err.Error(), "sslrootcert") {
		t.Errorf("Postgres refusal %q should point at sslrootcert", err)
	}
}

// TestTLSCA_EmptyPathIsNoop pins that no flag = byte-identical DSN (the
// zero-value-safe default) for a MySQL endpoint.
func TestTLSCA_EmptyPathIsNoop(t *testing.T) {
	my := mustEngine(t, "mysql")
	const dsn = "user:pw@tcp(h:3306)/app"
	got, err := applyEndpointTLSCA(my, dsn, "", "source")
	if err != nil {
		t.Fatalf("applyEndpointTLSCA(empty): %v", err)
	}
	if got != dsn {
		t.Errorf("empty --source-tls-ca mutated the DSN: got %q want %q", got, dsn)
	}
}

// TestTLSCA_FlagsParse pins that the flags bind onto their fields across the
// wired commands (the kong wiring itself).
func TestTLSCA_FlagsParse(t *testing.T) {
	caFile := writeThrowawayCA(t)
	cases := []struct {
		name string
		args []string
		get  func(*CLI) string
	}{
		{
			"migrate source",
			[]string{"migrate", "--source-driver=mysql", "--source=s", "--target-driver=postgres", "--target=t", "--source-tls-ca=" + caFile},
			func(c *CLI) string { return c.Migrate.SourceTLSCA },
		},
		{
			"migrate target",
			[]string{"migrate", "--source-driver=mysql", "--source=s", "--target-driver=postgres", "--target=t", "--target-tls-ca=" + caFile},
			func(c *CLI) string { return c.Migrate.TargetTLSCA },
		},
		{
			"verify source",
			[]string{"verify", "--source-driver=mysql", "--source=s", "--target-driver=postgres", "--target=t", "--source-tls-ca=" + caFile},
			func(c *CLI) string { return c.Verify.SourceTLSCA },
		},
		{
			"backup full source",
			[]string{"backup", "full", "--source-driver=mysql", "--source=s", "--output-dir=/tmp/x", "--source-tls-ca=" + caFile},
			func(c *CLI) string { return c.Backup.Full.SourceTLSCA },
		},
		{
			"restore target",
			[]string{"restore", "--target-driver=mysql", "--target=t", "--from-dir=/tmp/x", "--target-tls-ca=" + caFile},
			func(c *CLI) string { return c.Restore.TargetTLSCA },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cli := parseInto(t, tc.args...)
			if got := tc.get(cli); got != caFile {
				t.Errorf("flag bound %q; want %q", got, caFile)
			}
		})
	}
}
