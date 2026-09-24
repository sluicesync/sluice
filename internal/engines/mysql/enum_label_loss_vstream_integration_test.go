//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestVStream_EnumLabelLoss_ThroughVTGate is the VStream sibling of the
// binlog pin in enum_label_loss_integration_test.go (GC-37 (i)). VStream
// does not hand sluice an index: vttablet's vstreamer renders an ENUM/SET
// cell as label TEXT — through its own catalog view, which writes a label
// character outside the BMP as '?' just as information_schema does. The
// independent expected value is the label the test inserted: the BMP
// control must stream exactly, and the lost label must refuse
// (ENUM-LABEL-NOT-RECOVERABLE) rather than arrive as the catalog's '?b'.
func TestVStream_EnumLabelLoss_ThroughVTGate(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()
	applyVTTestSQL(t, mysqlDSN, `
		CREATE TABLE lossy (
			id INT NOT NULL,
			e  ENUM('😀b','x','é') CHARACTER SET utf8mb4 NULL,
			s  SET('😀','y','é')   CHARACTER SET utf8mb4 NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB`)

	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
		mysqlDSN, grpcEndpoint,
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	rdr, err := Engine{Flavor: FlavorPlanetScale}.OpenCDCReader(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	time.Sleep(2 * time.Second)

	// Control: labels the catalog kept, non-ASCII included, stream exactly.
	applyVTTestSQL(t, mysqlDSN, `INSERT INTO lossy VALUES (1, 'é', 'y,é')`)
	got := drainVTTestChanges(t, ctx, changes, 1, 60*time.Second)
	if len(got) != 1 {
		t.Fatalf("control: got %d changes; want 1 (stream error: %v)", len(got), rdr.(*vstreamCDCReader).Err())
	}
	ins, ok := got[0].(ir.Insert)
	if !ok || ins.Row["e"] != "é" || !reflect.DeepEqual(ins.Row["s"], []string{"y", "é"}) {
		t.Fatalf("control row = %#v; want e=é s=[y é]", got[0])
	}

	// The lost label. Before the guard this arrived as e="?b" s=["?" "y"]
	// at exit 0 (MEASURED); now the stream must end naming it, and emit
	// nothing for the row.
	applyVTTestSQL(t, mysqlDSN, `INSERT INTO lossy VALUES (2, '😀b', '😀,y')`)
	if got := drainVTTestChanges(t, ctx, changes, 1, 45*time.Second); len(got) != 0 {
		t.Fatalf("a row holding a lost label was emitted as %#v; want a refusal", got)
	}
	streamErr := rdr.(*vstreamCDCReader).Err()
	if streamErr == nil || !strings.Contains(streamErr.Error(), enumLabelNotRecoverableMarker) {
		t.Fatalf("stream error = %v; want %s", streamErr, enumLabelNotRecoverableMarker)
	}
	// vtgate's field carries the label list, so the refusal is the precise
	// one (the cell equals a lost label), not the no-label-list fallback.
	if !strings.Contains(streamErr.Error(), "already rewritten") {
		t.Errorf("refusal took the fallback branch, not the label match: %v", streamErr)
	}
}
