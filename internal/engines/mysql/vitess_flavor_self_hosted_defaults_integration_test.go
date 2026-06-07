//go:build integration && vstream

package mysql

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestVitessFlavor_SelfHostedDefaults_StreamsWithoutTransportAuthParams is
// the self-hosted `vitess` flavor's reason to exist (ADR-0073(a)): it
// connects to a self-hosted vtgate WITHOUT hand-set vstream_transport /
// vstream_auth — the flavor defaults them to plaintext / none (the common
// self-hosted shape). The hosted `planetscale` flavor defaults to tls /
// basic and would fail this same DSN. Only vstream_endpoint (no universal
// self-hosted default) is supplied.
//
// Driving the VStream COPY (stream.Rows) forces a REAL gRPC stream, so a
// wrong transport/auth default surfaces here rather than being masked by
// gRPC's lazy dial. This exercises the snapshot dial path
// (openVStreamSnapshotStreamFrom), which is the entry point the backup
// path also funnels through — so it pins the defaults for every VStream
// dial path, not just the CDC reader.
func TestVitessFlavor_SelfHostedDefaults_StreamsWithoutTransportAuthParams(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()

	applyVTTestSQL(t, mysqlDSN, `
		CREATE TABLE widgets (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			label VARCHAR(64)  NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	applyVTTestSQL(t, mysqlDSN+"&multiStatements=true", `
		INSERT INTO widgets (label) VALUES ('a');
		INSERT INTO widgets (label) VALUES ('b');
		INSERT INTO widgets (label) VALUES ('c');
	`)
	// vttestserver's schema tracker is async; let vtgate pick up the table.
	time.Sleep(3 * time.Second)

	// The vitess flavor's whole point: NO vstream_transport / vstream_auth
	// in the DSN — the flavor supplies plaintext / none. vstream_endpoint
	// (no universal self-hosted default) and vstream_shards (the operator's
	// shard layout — vttestserver's single shard is "0") are still supplied;
	// the flavor only defaults transport + auth, not those.
	sluiceDSN := mysqlDSN + "&vstream_endpoint=" + grpcEndpoint + "&vstream_shards=0"

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	stream, err := (Engine{Flavor: FlavorVitess}).OpenSnapshotStream(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("vitess OpenSnapshotStream (default plaintext/none transport+auth): %v", err)
	}
	defer func() { _ = stream.Close() }()

	table := &ir.Table{
		Name: "widgets",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
			{Name: "label", Type: ir.Varchar{Length: 64}},
		},
		PrimaryKey: &ir.Index{Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
	}

	rowsCh, err := stream.Rows.ReadRows(ctx, table)
	if err != nil {
		t.Fatalf("vitess snapshot ReadRows: %v", err)
	}
	got := 0
	for range rowsCh {
		got++
	}
	// A wrong transport/auth default would have surfaced as a reader error
	// after the channel closed (an empty/early-closed COPY), not a panic.
	if e, ok := stream.Rows.(interface{ Err() error }); ok {
		if err := e.Err(); err != nil {
			t.Fatalf("vitess snapshot reader error after drain (defaults must connect): %v", err)
		}
	}
	if got != 3 {
		t.Fatalf("vitess snapshot COPY returned %d rows; want 3 seeded — the plaintext/none flavor defaults must have connected the VStream", got)
	}
}
