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

// TestStreamer_AddColumnForward_PreexistingRowDefaults_VStreamToPostgres
// is the VStream (planetscale flavor) lane of the pre-existing-row
// DEFAULT gate — see streamer_schema_forward_preexisting_defaults_
// integration_test.go for the design, the canonical forms and the
// known-wrong discipline.
//
// VStream is the lane where the DEFAULT is structurally absent from the
// change stream: the FIELD event's projection carries none, so
// newSourceDefaultProber reads it back from the source SchemaReader —
// but only to classify its volatility (ADR-0058 §2a). The same MySQL 8
// shape matrix as the binlog lanes runs here, so a divergence between
// "the binlog lane's in-band default" and "the VStream lane's probed
// one" shows up cell by cell.
//
// Build tag / shard: `integration vstream` and the TestStreamer_…VStream
// name put it in extended-suites.yml's vstream-pipeline leg (the
// `TestStreamer_.*VStream` filter, guarded by
// scripts/check-run-filter-coverage.sh). A keyspace cannot be created
// over SQL, so the halt cells' fresh sources are keyspaces declared at
// boot (fdVStreamHaltKeyspaces of them).
func TestStreamer_AddColumnForward_PreexistingRowDefaults_VStreamToPostgres(t *testing.T) {
	keyspaces := []string{"commerce"}
	for i := 0; i < fdVStreamHaltKeyspaces; i++ {
		keyspaces = append(keyspaces, fmt.Sprintf("s_halt%d", i))
	}
	mysqlDSN, grpcEndpoint, _, cleanupSrc := startVTTestServerKeyspaces(t, keyspaces, 1)
	defer cleanupSrc()
	targetDSN, cleanupTgt := startPGTarget(t)
	defer cleanupTgt()

	all := fdMySQLShapes()
	runForwardedDefaultLane(t, fdLane{
		name: "vstream->postgres", sourceEngine: "planetscale", targetEngine: "postgres",
		sourceDSN: mysqlDSN, targetDSN: targetDSN, src: fdMySQL, tgt: fdPG,
		streamParams: fmt.Sprintf("&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0", grpcEndpoint),
		settle:       time.Second,
		shapes:       all,
		// No known-wrong cells since the carrySourceDefaults fix: the VStream
		// FIELD projection carries no DEFAULT, and the source SchemaReader's
		// default is now carried into the forwarded ADD COLUMN (before it,
		// every pre-existing target row held NULL).
		knownWrong: map[string]fdKnownWrong{},
		halts: []fdHalt{
			// KNOWN LOUD DEFECTS (the ALTER lands, the first carried row
			// cannot apply). The unsigned max is refused as out of range /
			// unencodable for int8 — the wording differs by stream history,
			// the value in it does not. A YEAR column added mid-stream
			// reaches PG as bytea hex text ("\x32303234"), but only when it
			// is not the stream's first forward, hence the prelude.
			// With the DEFAULT now carried, the unsigned max fails at the ALTER
			// itself, exactly as on the MySQL → Postgres lane.
			fdLoud(t, all, "i_ubigmax", "out of range for type bigint",
				"BIGINT UNSIGNED forwards as PG bigint; its max DEFAULT overflows the target ALTER"),
			fdLoud(t, all, "x_blobexpr", "is of type bytea but default expression is of type integer",
				"MySQL's (0x00FF) expression DEFAULT is re-emitted verbatim on PG, where 0x00FF lexes as an integer"),
			fdLoud(t, all, "x_bit", "does not match type bit(8)",
				"BIT(8) DEFAULT b'1010' is emitted as a 4-bit literal PG will not widen"),
			fdLoud(t, all, "t_year", `invalid input syntax for type smallint: "\x32303234"`,
				"a mid-stream-added YEAR column's VStream value reaches PG as bytea hex text").
				afterAlter().after(fdPrelude),
			fdRefused(t, all, "j_obj"),
			fdRefused(t, all, "j_kv"),
			fdDesignedRefusal(fdMySQLNow),
		},
		freshPair: func(t *testing.T, tag string) (string, string) {
			t.Helper()
			src, err := buildMySQLDSN(mysqlDSN, "s_"+tag)
			if err != nil {
				t.Fatalf("halt keyspace DSN: %v", err)
			}
			return src, fdFreshDB(t, fdPG, targetDSN, "t_"+tag)
		},
	})
}

// fdVStreamHaltKeyspaces is how many spare keyspaces the VStream lane
// boots for its halt cells; runForwardedDefaultLane names them
// s_halt0, s_halt1, … in halt order.
const fdVStreamHaltKeyspaces = 8
