//go:build psverify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Credentialed end-to-end test for `sluice deploy-ddl` against a REAL
// PlanetScale database (ADR-0165) — same posture and env surface as
// TestPSVerify_ExpandContract (live_psverify_test.go), net-zero on the
// table's schema: an ADD COLUMN shipped via one deploy request, then a
// DROP COLUMN via a second. Gated behind the psverify build tag so a
// CI run without credentials never runs (or builds) it.
//
// Usage from a shell with credentials available:
//
//	go test -tags=psverify -v -count=1 -timeout=40m \
//	  -run 'TestPSVerify_DeployDDL' ./internal/planetscale/expandcontract/...
package expandcontract

import (
	"context"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/planetscale/api"
)

func TestPSVerify_DeployDDL(t *testing.T) {
	tokenID := psverifyEnv("PLANETSCALE_SERVICE_TOKEN_ID")
	token := psverifyEnv("PLANETSCALE_SERVICE_TOKEN")
	org := psverifyEnv("PLANETSCALE_EC_ORG")
	database := psverifyEnv("PLANETSCALE_EC_DATABASE")
	branch := psverifyEnv("PLANETSCALE_EC_BRANCH")
	table := psverifyEnv("PLANETSCALE_EC_TABLE")

	if tokenID == "" || token == "" || org == "" || database == "" || table == "" {
		t.Skip("PLANETSCALE_SERVICE_TOKEN_* / PLANETSCALE_EC_* env absent — skipping credentialed e2e")
	}
	t.Logf("psverify deploy-ddl: org=%s database=%s branch=%s table=%s token_id=%s token=<%d-char redacted>",
		org, database, branchLabel(branch), table, tokenID, len(token))

	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Minute)
	defer cancel()

	client := api.New(api.Config{TokenID: tokenID, Token: token})
	deploy := func(ddl string) *DeployDDLResult {
		t.Helper()
		d := &DDLDeployer{
			API:           client,
			Org:           org,
			Database:      database,
			Branch:        branch,
			DDL:           ddl,
			PollInterval:  5 * time.Second,
			DeployTimeout: 15 * time.Minute,
			Out:           testWriter{t},
		}
		result, err := d.Run(ctx)
		if err != nil {
			t.Fatalf("deploy-ddl e2e (%s): %v", ddl, err)
		}
		if result.DeployRequest == 0 {
			t.Fatalf("deploy-ddl e2e (%s): no deploy-request number", ddl)
		}
		return result
	}

	add := deploy(fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN `sluice_dd_probe` VARCHAR(32) NULL", table))
	drop := deploy(fmt.Sprintf("ALTER TABLE `%s` DROP COLUMN `sluice_dd_probe`", table))
	t.Logf("e2e complete: add DR #%d, drop DR #%d (net-zero schema)", add.DeployRequest, drop.DeployRequest)
}
