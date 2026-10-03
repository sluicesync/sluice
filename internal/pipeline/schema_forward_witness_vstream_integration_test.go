//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The GC-44 anti-phantom family matrix on the VStream → Postgres pair
// (schema_forward_witness_family_integration_test.go has the binlog and
// pgoutput directions; internal/engines/mysql's
// TestVStream_TWFB_StoppedWidenForwardsAndFamilyResumesClean has VStream →
// MySQL). VStream hands the pipeline a FIELD event for every table at its
// first row of every resumed stream, so this pair takes the witness on
// every restart, through the cross-engine retarget — the cell a phantom
// would hurt most.
//
// Named TestStreamer_… so the extended-suites vstream-pipeline leg's -run
// filter picks it up.

package pipeline

import (
	"fmt"
	"testing"
	"time"
)

func TestStreamer_TWFBFamilyMatrix_VStreamToPostgres(t *testing.T) {
	const keyspace = "commerce"
	mysqlDSN, grpcEndpoint, _, cleanupSrc := startShardedVTTestServer(t, keyspace, 1)
	defer cleanupSrc()
	tgtDSN, cleanupTgt := startPGTarget(t)
	defer cleanupTgt()

	src := twfbDB{"planetscale", fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
		mysqlDSN, grpcEndpoint,
	)}
	// The tables exist before the cold start enumerates them, with time
	// for vtgate's schema tracker to see them.
	src.exec(t, twfbMySQLFamilyDDL)
	src.exec(t, twfbMySQLOverrideDDL)
	time.Sleep(3 * time.Second)

	runTWFBFamilyMatrix(t, twfbCell{
		src: src, tgt: twfbDB{"postgres", tgtDSN}, streamID: "twfb-fam-vs2p",
		mappings: twfbOverrides(true),
	}, []twfbFamilyTable{
		{"fam", "", twfbMySQLFamilyProbe},
		{"fam_ovr", "", twfbMySQLOverrideProbe},
	})
}
