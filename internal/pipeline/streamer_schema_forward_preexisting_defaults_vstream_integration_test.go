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
		knownWrong: fdDefaultDropped("VStream-source DEFAULT never forwarded",
			"s_plain", "s_empty", "s_quote", "s_bslash", "s_unicode", "s_nullword",
			"s_char", "s_nn", "s_textexpr", "i_int", "i_neg", "i_nn", "i_small",
			"i_bigmax", "i_bigmin", "i_ubigmax", "d_dec", "d_decneg", "f_float",
			"f_double", "b_bool", "b_boolf", "t_date", "t_dt", "t_dt6", "t_dtnn",
			"t_ts", "t_time", "t_year", "x_bin", "x_vbin", "x_vbinplain",
			"x_binstr", "x_vbinempty", "x_blobexpr", "x_bit", "e_enum", "e_enumq",
			"e_set", "g_expr"),
		halts: []fdHalt{
			// KNOWN LOUD DEFECTS (the ALTER lands, the first carried row
			// cannot apply). The unsigned max is refused as out of range /
			// unencodable for int8 — the wording differs by stream history,
			// the value in it does not. A YEAR column added mid-stream
			// reaches PG as bytea hex text ("\x32303234"), but only when it
			// is not the stream's first forward, hence the prelude.
			fdLoud(t, all, "i_ubigmax", "18446744073709551615",
				"BIGINT UNSIGNED forwards as PG bigint; the unsigned max row cannot apply").afterAlter(),
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
const fdVStreamHaltKeyspaces = 6
