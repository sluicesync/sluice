//go:build psverify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Credentialed end-to-end test for `sluice expand-contract` against a
// REAL PlanetScale database. Gated behind the psverify build tag —
// same posture as the telemetry smoke test — so a CI run WITHOUT
// credentials never runs (or builds) it. It drives the full pattern:
// expand (ADD COLUMN via dev branch + deploy request), migrate (the
// ADR-0159 backfill on the production branch), verify, contract (DROP
// COLUMN via a second deploy request) — net-zero on the table's
// schema when it completes.
//
// Prerequisites on the target database:
//   - safe migrations ENABLED on the production branch (the test
//     refuses otherwise — by design)
//   - a pre-existing table (env PLANETSCALE_EC_TABLE) with an
//     orderable primary key and at least one row
//   - a service token with branch + deploy-request + password scopes
//
// Usage from a shell with credentials available:
//
//	go test -tags=psverify -v -count=1 -timeout=30m \
//	  -run 'TestPSVerify_ExpandContract' ./internal/planetscale/expandcontract/...
//
// Credentials/identifiers are read from the environment, the token
// halves falling back to the machine-local file
// C:\code\PLANETSCALE_SLUICESYNC.env:
//
//	PLANETSCALE_SERVICE_TOKEN_ID
//	PLANETSCALE_SERVICE_TOKEN
//	PLANETSCALE_EC_ORG
//	PLANETSCALE_EC_DATABASE
//	PLANETSCALE_EC_BRANCH   (optional; defaults to "main")
//	PLANETSCALE_EC_DSN      (go-sql-driver DSN for the production branch)
//	PLANETSCALE_EC_TABLE    (pre-existing table with a PK and rows)
//
// The token is NEVER printed; only its presence is reported. The test
// adds, backfills, and then drops the column `sluice_ec_probe`.
package expandcontract

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	_ "sluicesync.dev/sluice/internal/engines/mysql" // register the planetscale flavor
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline"
	"sluicesync.dev/sluice/internal/planetscale/api"
)

const psverifyTokenEnvFile = `C:\code\PLANETSCALE_SLUICESYNC.env`

func psverifyEnv(key string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	f, err := os.Open(psverifyTokenEnvFile)
	if err != nil {
		return ""
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if ok && strings.TrimSpace(k) == key {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func TestPSVerify_ExpandContract(t *testing.T) {
	tokenID := psverifyEnv("PLANETSCALE_SERVICE_TOKEN_ID")
	token := psverifyEnv("PLANETSCALE_SERVICE_TOKEN")
	org := psverifyEnv("PLANETSCALE_EC_ORG")
	database := psverifyEnv("PLANETSCALE_EC_DATABASE")
	branch := psverifyEnv("PLANETSCALE_EC_BRANCH")
	dsn := psverifyEnv("PLANETSCALE_EC_DSN")
	table := psverifyEnv("PLANETSCALE_EC_TABLE")

	if tokenID == "" || token == "" || org == "" || database == "" || dsn == "" || table == "" {
		t.Skip("PLANETSCALE_SERVICE_TOKEN_* / PLANETSCALE_EC_* env absent — skipping credentialed e2e")
	}
	t.Logf("psverify expand-contract: org=%s database=%s branch=%s table=%s token_id=%s token=<%d-char redacted>",
		org, database, branchLabel(branch), table, tokenID, len(token))

	engine, ok := engines.Get("planetscale")
	if !ok {
		t.Fatal("planetscale engine not registered")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	o := &Orchestrator{
		API:      api.New(api.Config{TokenID: tokenID, Token: token}),
		Org:      org,
		Database: database,
		Branch:   branch,
		Engine:   engine,
		DSN:      dsn,
		Table:    table,
		Sets:     mustSets(t, "sluice_ec_probe = 'backfilled'"),
		Where:    "sluice_ec_probe IS NULL",
		ExpandDDL: fmt.Sprintf(
			"ALTER TABLE `%s` ADD COLUMN `sluice_ec_probe` VARCHAR(32) NULL", table,
		),
		ContractDDL: fmt.Sprintf(
			"ALTER TABLE `%s` DROP COLUMN `sluice_ec_probe`", table,
		),
		Yes:           true,
		PollInterval:  5 * time.Second,
		DeployTimeout: 15 * time.Minute,
		Out:           testWriter{t},
	}
	result, err := o.Run(ctx)
	if err != nil {
		t.Fatalf("expand-contract e2e: %v", err)
	}
	if !result.Verified || !result.ContractRun {
		t.Fatalf("result = %+v; want verified + contract run", result)
	}
	if result.Backfill == nil || result.Backfill.RowsUpdated == 0 {
		t.Fatalf("backfill result = %+v; want at least one row backfilled (seed the table)", result.Backfill)
	}
	t.Logf("e2e complete: expand DR #%d, %d row(s) backfilled, contract DR #%d",
		result.ExpandDeployRequest, result.Backfill.RowsUpdated, result.ContractDeployRequest)
}

func branchLabel(b string) string {
	if b == "" {
		return "main"
	}
	return b
}

func mustSets(t *testing.T, spec string) []ir.BackfillSet {
	t.Helper()
	sets, err := pipeline.ParseBackfillSets([]string{spec})
	if err != nil {
		t.Fatalf("ParseBackfillSets: %v", err)
	}
	return sets
}

// testWriter narrates orchestrator output into the test log.
type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}
