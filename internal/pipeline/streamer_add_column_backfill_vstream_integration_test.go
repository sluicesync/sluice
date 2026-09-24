//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"fmt"
	"testing"
	"time"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestStreamer_AddColumnBackfill_VStreamToPostgres is the VStream
// (planetscale flavor) lane of the default-on added-column backfill — see
// streamer_add_column_backfill_integration_test.go. The VStream FIELD event
// carries no DEFAULT at all, so this is the lane where the forward's carry
// rests entirely on a source catalog read taken when the boundary arrives;
// the backfill reads the rows themselves, through the planetscale engine's
// row reader against vtgate.
//
// Build tag / shard: `integration vstream` and the TestStreamer_…VStream
// name put it in extended-suites.yml's vstream-pipeline leg.
func TestStreamer_AddColumnBackfill_VStreamToPostgres(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanupSrc := startVTTestServerKeyspaces(t, []string{"commerce"}, 1)
	defer cleanupSrc()
	targetDSN, cleanupTgt := startPGTarget(t)
	defer cleanupTgt()
	abRun(t, fdLane{
		name: "backfill vstream->postgres", sourceEngine: "planetscale", targetEngine: "postgres",
		sourceDSN: mysqlDSN, targetDSN: targetDSN, src: fdMySQL, tgt: fdPG,
		streamParams: fmt.Sprintf("&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0", grpcEndpoint),
		settle:       time.Second,
	}, abMySQLCells())
}
