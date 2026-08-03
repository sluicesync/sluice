//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// A row larger than gRPC's default receive ceiling copies (field report).
//
// A PlanetScale cold copy failed on a table holding wide rows with
//
//	rpc error: code = ResourceExhausted desc = grpc: received message after
//	decompression larger than max 4194304
//
// 4194304 is 4 MiB — gRPC's stock CLIENT default. sluice builds its VStream
// client by hand rather than through Vitess's grpcclient.DialContext, so it
// inherited that ceiling while every Vitess-native client gets 16 MiB.
// VStream ships whole ROWS and cannot split one across messages, so one wide
// row is enough: a `mediumtext` column is 16 MiB on its own.
//
// WHY THIS TEST EXISTS, specifically. The fix shipped with unit coverage of
// the DSN knob, the error classification and the string match — none of which
// exercise the actual claim, which is that a wide row now COPIES. Everything
// verifying that claim was a reading of gRPC's and Vitess's source. This test
// is the missing half: it pushes a genuinely oversized row through a real
// vtgate and requires the copy to succeed.
//
// SIZING. The row is ~8 MiB: comfortably above gRPC's 4 MiB client default
// (so it FAILS without the fix) and comfortably below vtgate's own 16 MiB
// --grpc_max_message_size (so the SERVER will send it, and a failure is
// unambiguously ours). Both bounds matter — a row above 16 MiB would fail
// server-side even with the fix and would make this test prove nothing.
//
// Falsified against the pre-fix client: reverting the MaxCallRecvMsgSize dial
// option reproduces the reported error here verbatim.

package mysql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestVStream_OversizedRow_CopiesPastTheGRPCDefaultCeiling(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()

	// mediumtext is the reported column type; it holds up to 16 MiB.
	applyVTTestSQL(t, mysqlDSN, `CREATE TABLE wide_rows (
		id      BIGINT     NOT NULL,
		content MEDIUMTEXT NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)

	// Exactly 8 MiB: over gRPC's 4 MiB client default (so this fails without
	// the fix) and under vtgate's own 16 MiB --grpc_max_message_size (so the
	// server WILL send it, and a failure is unambiguously ours). Both bounds
	// are load-bearing — a row over 16 MiB would fail server-side even with
	// the fix and would make this test prove nothing.
	//
	// Built server-side with REPEAT so the statement itself stays tiny. That
	// the payload is trivially compressible does not weaken the test: the
	// error the field reported is "received message AFTER DECOMPRESSION larger
	// than max", and the decompressed size is 8 MiB however well it compresses
	// on the wire. If anything it exercises the exact wording reported.
	const payloadBytes = 8 << 20
	applyVTTestSQL(t, mysqlDSN, fmt.Sprintf(
		"INSERT INTO wide_rows (id, content) VALUES (1, REPEAT('x', %d))", payloadBytes,
	))
	// Ordinary rows either side, so a failure that drops the whole table is
	// distinguishable from one that drops only the wide row.
	applyVTTestSQL(t, mysqlDSN, "INSERT INTO wide_rows (id, content) VALUES (2, 'small'), (3, 'also small')")

	// Let vttestserver's async schema tracker see the table before COPY
	// enumerates it.
	time.Sleep(3 * time.Second)

	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
		mysqlDSN, grpcEndpoint,
	)

	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	stream, err := eng.OpenSnapshotStreamForTables(ctx, sluiceDSN, []string{"wide_rows"})
	if err != nil {
		t.Fatalf("OpenSnapshotStreamForTables: %v", err)
	}
	defer func() { _ = stream.Close() }()

	table := &ir.Table{
		Schema: "",
		Name:   "wide_rows",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "content", Type: ir.Text{}},
		},
	}

	rows, err := stream.Rows.ReadRows(ctx, table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}

	got := map[int64]int{}
	for row := range rows {
		id, _ := row["id"].(int64)
		switch v := row["content"].(type) {
		case string:
			got[id] = len(v)
		case []byte:
			got[id] = len(v)
		default:
			t.Errorf("row %d: content decoded as %T, want string/[]byte", id, row["content"])
		}
	}

	// The reader's sticky error is where the ResourceExhausted surfaces —
	// checked BEFORE the row assertions so a failure reports the real cause
	// rather than "missing row".
	if rerr := stream.Rows.Err(); rerr != nil {
		if isVStreamMessageTooLargeError(rerr) {
			t.Fatalf("the oversized row was REFUSED BY OUR OWN CLIENT: %v\n\n"+
				"This is the reported failure. The VStream client's MaxCallRecvMsgSize is not taking "+
				"effect — vtgate was willing to send this row (it is under the server's own 16 MiB "+
				"ceiling), and sluice declined to receive it.", rerr)
		}
		t.Fatalf("copy failed: %v", rerr)
	}

	if len(got) != 3 {
		t.Fatalf("copied %d rows, want 3 (ids 1,2,3) — got %v", len(got), got)
	}
	if got[1] != payloadBytes {
		t.Errorf("the wide row's content is %d bytes, want %d — it copied but was TRUNCATED, which is "+
			"worse than the refusal this test was written for", got[1], payloadBytes)
	}
}
