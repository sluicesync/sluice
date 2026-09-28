//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// ADR-0190 phase 4's crash suite (named TestStreamer_*VStream*, the
// pipeline package's vstream leg filter in extended-suites.yml — a
// TestVStream_ name here would match no CI leg and never run): the same
// source transaction, kill and restart as TestStreamer_CrashMidTxn_*, on a
// real vttestserver VStream source. The cold start runs through the
// snapshot stream's post-COPY pump and the restart through the tail reader,
// so every converging cell also proves the two VStream dispatchers name the
// same change the same way.
//
// The keyspace is UNSHARDED on purpose: the harness's secondary-unique table
// would be unique only per shard on a sharded one, and shard transactions
// arrive in no fixed order across shards, so the target's global unique
// index could collide with no crash at all. The multi-shard identity premise
// is TestVStream_ApplyIdentity_StableAcrossMidStreamResume's job.

func TestStreamer_CrashMidTxn_VStream_ToPostgres(t *testing.T) {
	src, cleanup := startVStreamCrashSource(t)
	defer cleanup()
	_, pgDSN, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()
	runCrashMidTxnSuite(t, src, pgResendTarget(pgDSN), crashPinsRefusals|crashPinsColdStart)
}

func TestStreamer_CrashMidTxn_VStream_ToMySQL(t *testing.T) {
	src, cleanup := startVStreamCrashSource(t)
	defer cleanup()
	_, tgt, tgtCleanup := startMySQL(t)
	defer tgtCleanup()
	runCrashMidTxnSuite(t, src, mysqlResendTarget(t, tgt), crashPinsRefusals)
}

// startVStreamCrashSource boots an unsharded vttestserver keyspace as a
// planetscale-flavor source. The harness's own SQL goes to vtgate's MySQL
// port; the stream opens the VStream gRPC endpoint.
func startVStreamCrashSource(t *testing.T) (crashSource, func()) {
	t.Helper()
	const keyspace, shard = "crash", "0"
	mysqlDSN, grpcEndpoint, _, cleanup := startShardedVTTestServer(t, keyspace, 1)
	src := mysqlCrashSource("planetscale", mysqlDSN)
	// No keyless table: the VStream cold-start COPY refuses one by design
	// (Bug 125 — it needs a unique key to absorb Vitess's catch-up
	// re-emissions idempotently).
	src.setup = strings.Replace(src.setup, "DROP TABLE IF EXISTS kl;", "", 1)
	src.setup = src.setup[:strings.Index(src.setup, "CREATE TABLE kl")]
	src.noKeyless = true
	src.streamDSN = fmt.Sprintf("%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=%s",
		mysqlDSN, grpcEndpoint, shard)
	// vtgate's schema tracker is asynchronous: let it see the recreated
	// tables before the COPY phase enumerates them.
	src.afterSetup = func(*testing.T) { time.Sleep(3 * time.Second) }
	// The identity the marks must carry is derived from the POSITION
	// persisted at the kill, not from the source's gtid_executed: Vitess
	// commits its own sidecar writes, so "the last GTID minus one" is not
	// the interrupted transaction's pre-transaction set on a vtgate source
	// (measured: an internal write took the GNO after the kill
	// transaction's).
	src.lastTxID = nil
	src.firstTxAfter = func(t *testing.T, token string) string { return vstreamFirstTxAfter(t, token, keyspace, shard) }
	return src, cleanup
}

// vstreamFirstTxAfter is the ADR-0190 identity of the first shard
// transaction a restart from the persisted VGTID token re-delivers: the
// keyspace/shard and that shard's component of the token, which is exactly
// the pre-transaction component the reader stamps at that transaction's
// BEGIN. It is derived from the applier's persisted position, a different
// computation from the reader's BEGIN-time stamp.
func vstreamFirstTxAfter(t *testing.T, token, keyspace, shard string) string {
	t.Helper()
	var shards []struct {
		Keyspace string `json:"keyspace"`
		Shard    string `json:"shard"`
		Gtid     string `json:"gtid"`
	}
	if err := json.Unmarshal([]byte(token), &shards); err != nil {
		t.Fatalf("decode the VStream position %q: %v", token, err)
	}
	for _, s := range shards {
		if s.Keyspace == keyspace && s.Shard == shard {
			return fmt.Sprintf("vstream:%s/%s:%s", keyspace, shard, s.Gtid)
		}
	}
	t.Fatalf("the VStream position %q holds no component for %s/%s", token, keyspace, shard)
	return ""
}
