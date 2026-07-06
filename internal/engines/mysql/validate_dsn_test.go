// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestValidateDSN pins the [ir.DSNValidator] surface (the driver/host
// mismatch preflight): the vanilla flavor refuses a PlanetScale endpoint
// on BOTH documented suffixes and recommends the planetscale driver, the
// VStream flavors accept the same host, and no non-PSDB / unparsable DSN
// yields a false positive.
func TestValidateDSN(t *testing.T) {
	// Confirm the engine actually satisfies the optional surface — a
	// silent drop of the method would make the preflight a no-op.
	var _ ir.DSNValidator = Engine{Flavor: FlavorVanilla}

	const (
		publicPSDB  = "u:p@tcp(aws.connect.psdb.cloud:3306)/db?tls=true"
		privatePSDB = "u:p@tcp(aws.private-connect.psdb.cloud:3306)/db?tls=true"
		normalHost  = "u:p@tcp(db.internal.example.com:3306)/db"
		garbage     = "://not a dsn@@@"
		unixSocket  = "u:p@unix(/var/run/mysqld/mysqld.sock)/db"
	)

	cases := []struct {
		name      string
		flavor    Flavor
		dsn       string
		wantErr   bool
		wantInMsg string
	}{
		{"vanilla rejects public psdb host", FlavorVanilla, publicPSDB, true, "planetscale"},
		{"vanilla rejects private psdb host", FlavorVanilla, privatePSDB, true, "planetscale"},
		{"planetscale accepts public psdb host", FlavorPlanetScale, publicPSDB, false, ""},
		{"planetscale accepts private psdb host", FlavorPlanetScale, privatePSDB, false, ""},
		{"vitess accepts psdb host", FlavorVitess, publicPSDB, false, ""},
		{"vanilla accepts a normal tcp host", FlavorVanilla, normalHost, false, ""},
		{"vanilla no false positive on garbage dsn", FlavorVanilla, garbage, false, ""},
		{"vanilla no false positive on unix socket", FlavorVanilla, unixSocket, false, ""},
		{"vanilla no false positive on empty dsn", FlavorVanilla, "", false, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := (Engine{Flavor: c.flavor}).ValidateDSN(c.dsn)
			if c.wantErr {
				if err == nil {
					t.Fatalf("ValidateDSN(%q) = nil; want an error", c.dsn)
				}
				if c.wantInMsg != "" && !strings.Contains(strings.ToLower(err.Error()), c.wantInMsg) {
					t.Errorf("ValidateDSN(%q) message = %q; want it to mention %q", c.dsn, err.Error(), c.wantInMsg)
				}
				return
			}
			if err != nil {
				t.Errorf("ValidateDSN(%q) = %v; want nil", c.dsn, err)
			}
		})
	}
}
