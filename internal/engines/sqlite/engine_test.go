// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"errors"
	"testing"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
)

// TestEngineRegistered confirms the init() self-registration under "sqlite".
func TestEngineRegistered(t *testing.T) {
	e, ok := engines.Get("sqlite")
	if !ok {
		t.Fatal("sqlite engine not registered")
	}
	if e.Name() != "sqlite" {
		t.Errorf("Name() = %q; want sqlite", e.Name())
	}
}

// TestCapabilities pins the honestly-declared migrate-source capability
// shape: no CDC, flat namespace, no extension types, no bulk-load target.
func TestCapabilities(t *testing.T) {
	c := Engine{}.Capabilities()
	if c.CDC != ir.CDCNone {
		t.Errorf("CDC = %v; want CDCNone", c.CDC)
	}
	if c.SchemaScope != ir.SchemaScopeFlat {
		t.Errorf("SchemaScope = %v; want flat", c.SchemaScope)
	}
	if c.BulkLoad != ir.BulkLoadNone {
		t.Errorf("BulkLoad = %v; want none", c.BulkLoad)
	}
	if c.SupportedTypes != ir.NewTypeSet() {
		t.Errorf("SupportedTypes = %v; want empty", c.SupportedTypes)
	}
}

// TestWriteSideNotImplemented confirms every write/CDC/snapshot Open*
// returns ErrNotImplemented — SQLite is a migrate source only.
func TestWriteSideNotImplemented(t *testing.T) {
	e := Engine{}
	ctx := context.Background()
	const dsn = "ignored.db"

	if _, err := e.OpenSchemaWriter(ctx, dsn); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("OpenSchemaWriter err = %v; want ErrNotImplemented", err)
	}
	if _, err := e.OpenRowWriter(ctx, dsn); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("OpenRowWriter err = %v; want ErrNotImplemented", err)
	}
	if _, err := e.OpenCDCReader(ctx, dsn); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("OpenCDCReader err = %v; want ErrNotImplemented", err)
	}
	if _, err := e.OpenChangeApplier(ctx, dsn); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("OpenChangeApplier err = %v; want ErrNotImplemented", err)
	}
	if _, err := e.OpenSnapshotStream(ctx, dsn); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("OpenSnapshotStream err = %v; want ErrNotImplemented", err)
	}
}

// TestParseDSN pins DSN normalisation across the three accepted forms.
func TestParseDSN(t *testing.T) {
	cases := []struct {
		in       string
		wantPath string
	}{
		{"./app.db", "./app.db"},
		{"/data/app.db", "/data/app.db"},
		{`C:\data\app.db`, `C:\data\app.db`},
		{"file:app.db", "app.db"},
		{"file:/data/app.db?cache=shared", "/data/app.db"},
		{"sqlite://./app.db", "./app.db"},
		{"sqlite:///data/app.db", "/data/app.db"},
	}
	for _, c := range cases {
		driverDSN, path, _, err := parseDSN(c.in)
		if err != nil {
			t.Errorf("parseDSN(%q) error: %v", c.in, err)
			continue
		}
		if path != c.wantPath {
			t.Errorf("parseDSN(%q) path = %q; want %q", c.in, path, c.wantPath)
		}
		if !contains(driverDSN, queryOnlyPragma) {
			t.Errorf("parseDSN(%q) driverDSN = %q; want it to carry %q", c.in, driverDSN, queryOnlyPragma)
		}
	}

	if _, _, _, err := parseDSN(""); err == nil {
		t.Error("parseDSN(\"\") should error")
	}
}

// TestParseDSN_DateEncodingParam pins ADR-0129's per-source
// sqlite_date_encoding DSN param: it resolves to the right encoding, is
// STRIPPED from the driver DSN (so it never reaches modernc), other query
// params survive, absence yields the inherit sentinel, and an invalid value
// refuses loudly before any connection opens.
func TestParseDSN_DateEncodingParam(t *testing.T) {
	cases := []struct {
		in      string
		wantEnc dateEncoding
		wantErr bool
	}{
		{"./app.db", dateEncodingInherit, false},
		{"./app.db?sqlite_date_encoding=iso", dateEncodingISO, false},
		{"./app.db?sqlite_date_encoding=unixepoch", dateEncodingUnixEpoch, false},
		{"./app.db?sqlite_date_encoding=unixmillis", dateEncodingUnixMillis, false},
		{"./app.db?sqlite_date_encoding=julian", dateEncodingJulian, false},
		{"file:app.db?sqlite_date_encoding=julian&cache=shared", dateEncodingJulian, false},
		{"./app.db?sqlite_date_encoding=bogus", dateEncodingInherit, true},
	}
	for _, c := range cases {
		driverDSN, _, enc, err := parseDSN(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("parseDSN(%q) err = nil; want a loud refusal", c.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseDSN(%q) error: %v", c.in, err)
			continue
		}
		if enc != c.wantEnc {
			t.Errorf("parseDSN(%q) enc = %v; want %v", c.in, enc, c.wantEnc)
		}
		// The sluice-internal param must never reach the driver DSN.
		if contains(driverDSN, dsnDateEncodingParam) {
			t.Errorf("parseDSN(%q) driverDSN = %q still carries %q", c.in, driverDSN, dsnDateEncodingParam)
		}
		// A non-sluice query param must survive the strip.
		if c.in == "file:app.db?sqlite_date_encoding=julian&cache=shared" && !contains(driverDSN, "cache=shared") {
			t.Errorf("parseDSN(%q) dropped cache=shared: %q", c.in, driverDSN)
		}
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
