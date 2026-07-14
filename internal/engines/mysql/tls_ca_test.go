// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// --- throwaway PKI helpers (no external files) ---

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte // the CA cert, PEM-encoded (what --tls-ca points at)
}

// newTestCA mints a self-signed CA with a fresh ECDSA key.
func newTestCA(t *testing.T, cn string) testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	return testCA{cert: cert, key: key, pem: pemCert(der)}
}

// signLeaf issues a server (leaf) certificate signed by the CA and returns its
// DER bytes — the shape a TLS peer presents in rawCerts[0].
func (ca testCA) signLeaf(t *testing.T, cn string) []byte {
	t.Helper()
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &leafKey.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("sign leaf: %v", err)
	}
	return der
}

// signIntermediate issues an intermediate CA signed by the root, plus a leaf
// signed by that intermediate. Returns the leaf and intermediate DER, in the
// order a server would present them ([leaf, intermediate]).
func (ca testCA) signIntermediateChain(t *testing.T) (leafDER, intermediateDER []byte) {
	t.Helper()
	interKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate intermediate key: %v", err)
	}
	interTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(3),
		Subject:               pkix.Name{CommonName: "test-intermediate"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	interDER, err := x509.CreateCertificate(rand.Reader, interTmpl, ca.cert, &interKey.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("sign intermediate: %v", err)
	}
	interCert, err := x509.ParseCertificate(interDER)
	if err != nil {
		t.Fatalf("parse intermediate: %v", err)
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(4),
		Subject:      pkix.Name{CommonName: "db.example.com"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	leafDER, err = x509.CreateCertificate(rand.Reader, leafTmpl, interCert, &leafKey.PublicKey, interKey)
	if err != nil {
		t.Fatalf("sign leaf under intermediate: %v", err)
	}
	return leafDER, interDER
}

func pemCert(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// writeCA writes the CA PEM to a temp file and returns its path.
func writeCA(t *testing.T, pemBytes []byte) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(path, pemBytes, 0o600); err != nil {
		t.Fatalf("write CA file: %v", err)
	}
	return path
}

// --- the load-bearing security pin ---

// TestBuildVerifyCATLSConfig_AcceptsMatchingRejectsWrongCA is the whole point
// of the feature: a server certificate that chains to the pinned CA is
// ACCEPTED, and one signed by a DIFFERENT CA is REJECTED (the handshake would
// fail). Both directions are asserted through the real VerifyPeerCertificate
// callback the config carries.
func TestBuildVerifyCATLSConfig_AcceptsMatchingRejectsWrongCA(t *testing.T) {
	pinnedCA := newTestCA(t, "pinned-ca")
	otherCA := newTestCA(t, "attacker-ca")

	caFile := writeCA(t, pinnedCA.pem)
	cfg, err := buildVerifyCATLSConfig(caFile)
	if err != nil {
		t.Fatalf("buildVerifyCATLSConfig: %v", err)
	}
	if cfg.VerifyPeerCertificate == nil {
		t.Fatal("VerifyPeerCertificate is nil; verify-ca must install a manual chain check")
	}

	// ACCEPT: a leaf signed by the pinned CA chains to it.
	goodLeaf := pinnedCA.signLeaf(t, "db.example.com")
	if err := cfg.VerifyPeerCertificate([][]byte{goodLeaf}, nil); err != nil {
		t.Errorf("verify-ca rejected a cert signed by the PINNED CA: %v; want accept", err)
	}

	// REJECT: a leaf signed by a different CA must NOT chain to the pinned CA.
	badLeaf := otherCA.signLeaf(t, "db.example.com")
	if err := cfg.VerifyPeerCertificate([][]byte{badLeaf}, nil); err == nil {
		t.Fatal("verify-ca ACCEPTED a cert signed by a DIFFERENT CA; the security invariant is broken")
	}
}

// TestBuildVerifyCATLSConfig_IntermediateChain pins the certs[1:] intermediate
// branch: a leaf signed by an intermediate that is signed by the pinned root
// is accepted when the server presents [leaf, intermediate].
func TestBuildVerifyCATLSConfig_IntermediateChain(t *testing.T) {
	root := newTestCA(t, "root-ca")
	leaf, inter := root.signIntermediateChain(t)

	cfg, err := buildVerifyCATLSConfig(writeCA(t, root.pem))
	if err != nil {
		t.Fatalf("buildVerifyCATLSConfig: %v", err)
	}
	if err := cfg.VerifyPeerCertificate([][]byte{leaf, inter}, nil); err != nil {
		t.Errorf("verify-ca rejected a valid leaf→intermediate→root chain: %v", err)
	}
	// Sanity: WITHOUT the intermediate, the leaf can't chain to the root and is
	// rejected (proves the intermediate is actually load-bearing).
	if err := cfg.VerifyPeerCertificate([][]byte{leaf}, nil); err == nil {
		t.Error("verify-ca accepted a leaf with no path to the root (missing intermediate); want reject")
	}
}

// TestBuildVerifyCATLSConfig_Invariants pins the non-negotiable config shape.
func TestBuildVerifyCATLSConfig_Invariants(t *testing.T) {
	cfg, err := buildVerifyCATLSConfig(writeCA(t, newTestCA(t, "ca").pem))
	if err != nil {
		t.Fatalf("buildVerifyCATLSConfig: %v", err)
	}
	if !cfg.InsecureSkipVerify {
		t.Error("InsecureSkipVerify = false; verify-ca must skip Go's built-in hostname check (MySQL certs lack SANs)")
	}
	if cfg.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion = %#x; want TLS 1.2 (%#x)", cfg.MinVersion, uint16(tls.VersionTLS12))
	}
	if !isVerifyCATLSConfig(cfg) {
		t.Error("isVerifyCATLSConfig(cfg) = false; want true (InsecureSkipVerify + VerifyPeerCertificate)")
	}
}

// TestBuildVerifyCATLSConfig_LoudFailures pins that an unreadable path and a
// PEM with no certificate both fail LOUDLY rather than yielding a config that
// trusts nothing (or everything).
func TestBuildVerifyCATLSConfig_LoudFailures(t *testing.T) {
	t.Run("unreadable path", func(t *testing.T) {
		if _, err := buildVerifyCATLSConfig(filepath.Join(t.TempDir(), "does-not-exist.pem")); err == nil {
			t.Fatal("buildVerifyCATLSConfig on a missing file: err = nil; want a loud error")
		}
	})
	t.Run("PEM with no certificate", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "junk.pem")
		if err := os.WriteFile(path, []byte("not a certificate\n"), 0o600); err != nil {
			t.Fatalf("write junk: %v", err)
		}
		if _, err := buildVerifyCATLSConfig(path); err == nil {
			t.Fatal("buildVerifyCATLSConfig on a cert-less PEM: err = nil; want a loud error")
		}
	})
}

// TestBuildVerifyCATLSConfig_EmptyAndMalformedPeerRefused pins the two
// defensive branches of the callback: no certificate presented, and an
// unparseable certificate — both refuse rather than pass.
func TestBuildVerifyCATLSConfig_EmptyAndMalformedPeerRefused(t *testing.T) {
	cfg, err := buildVerifyCATLSConfig(writeCA(t, newTestCA(t, "ca").pem))
	if err != nil {
		t.Fatalf("buildVerifyCATLSConfig: %v", err)
	}
	if err := cfg.VerifyPeerCertificate(nil, nil); err == nil {
		t.Error("verify-ca accepted an empty certificate list; want reject")
	}
	if err := cfg.VerifyPeerCertificate([][]byte{[]byte("garbage-der")}, nil); err == nil {
		t.Error("verify-ca accepted an unparseable certificate; want reject")
	}
}

// --- DSN rewrite + threading ---

// TestDSNWithVerifyCATLS_RewritesAndThreads pins that DSNWithVerifyCATLS
// produces a DSN whose driver-parsed cfg.TLS is the verify-ca config — so both
// the data-plane connection AND the binlog stream (via binlogTLSFromConfig)
// inherit it — and that the transport label resolves to "verify-ca".
func TestDSNWithVerifyCATLS_RewritesAndThreads(t *testing.T) {
	caFile := writeCA(t, newTestCA(t, "ca").pem)
	const dsn = "user:pw@tcp(db.example.com:3306)/app"

	got, err := Engine{}.DSNWithVerifyCATLS(dsn, caFile)
	if err != nil {
		t.Fatalf("DSNWithVerifyCATLS: %v", err)
	}

	// The driver's own ParseDSN must resolve the rewritten tls= into cfg.TLS —
	// this is the leak-proof threading: every Open* path parses through it.
	cfg, err := parseDSN(got)
	if err != nil {
		t.Fatalf("parseDSN(rewritten): %v", err)
	}
	if cfg.TLS == nil {
		t.Fatal("rewritten DSN did not resolve to a cfg.TLS; verify-ca did not thread")
	}
	if !isVerifyCATLSConfig(cfg.TLS) {
		t.Error("cfg.TLS is not a verify-ca config (InsecureSkipVerify + VerifyPeerCertificate)")
	}

	// The binlog stream clones cfg.TLS and must be labeled "verify-ca" so the
	// stream-open WARN treats it as authenticated, not blind skip-verify.
	btls := binlogTLSFromConfig(cfg, "db.example.com")
	if !isVerifyCATLSConfig(btls) {
		t.Error("binlogTLSFromConfig did not carry the verify-ca config onto the binlog stream")
	}
	if label := binlogTLSModeLabel(cfg.TLSConfig, btls); label != "verify-ca" {
		t.Errorf("binlogTLSModeLabel = %q; want \"verify-ca\"", label)
	}
}

// TestDSNWithVerifyCATLS_RefusesConflictingTLSParam pins the loud refusal when
// the DSN already declares tls= — the flag and the DSN param are conflicting
// transport declarations; exactly one must be supplied.
func TestDSNWithVerifyCATLS_RefusesConflictingTLSParam(t *testing.T) {
	caFile := writeCA(t, newTestCA(t, "ca").pem)
	for _, mode := range []string{"skip-verify", "true", "false", "preferred"} {
		t.Run("tls="+mode, func(t *testing.T) {
			dsn := "user:pw@tcp(db.example.com:3306)/app?tls=" + mode
			if _, err := (Engine{}).DSNWithVerifyCATLS(dsn, caFile); err == nil {
				t.Fatalf("DSNWithVerifyCATLS accepted a DSN with tls=%s; want a loud conflict refusal", mode)
			}
		})
	}
}

// TestDSNWithVerifyCATLS_BadCARefused pins that a bad CA path surfaces loudly
// through the rewrite (not just through the low-level builder).
func TestDSNWithVerifyCATLS_BadCARefused(t *testing.T) {
	if _, err := (Engine{}).DSNWithVerifyCATLS("user:pw@tcp(h:3306)/app", filepath.Join(t.TempDir(), "nope.pem")); err == nil {
		t.Fatal("DSNWithVerifyCATLS with a missing CA file: err = nil; want a loud error")
	}
}

// TestDSNWithVerifyCATLS_PreservesInterpolateParams pins the ADR-0153
// explicit-DSN-wins preservation: an operator's interpolateParams=false must
// survive the FormatDSN round-trip (mirrors [Engine.WithDatabase]).
func TestDSNWithVerifyCATLS_PreservesInterpolateParams(t *testing.T) {
	caFile := writeCA(t, newTestCA(t, "ca").pem)
	const dsn = "user:pw@tcp(db.example.com:3306)/app?interpolateParams=false"

	got, err := Engine{}.DSNWithVerifyCATLS(dsn, caFile)
	if err != nil {
		t.Fatalf("DSNWithVerifyCATLS: %v", err)
	}
	cfg, err := mysql.ParseDSN(got)
	if err != nil {
		t.Fatalf("ParseDSN(rewritten): %v", err)
	}
	if cfg.InterpolateParams {
		t.Error("interpolateParams=false was lost across the verify-ca rewrite")
	}
}
