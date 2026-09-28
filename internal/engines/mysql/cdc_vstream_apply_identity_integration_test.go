//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// vstreamDelivered is one row change as a VStream delivery produced it.
type vstreamDelivered struct {
	id      ir.ApplyID
	table   string
	content string // operation + images, rendered deterministically
	pos     ir.Position
}

// vstreamDelivery is a drained stream: its rows in delivery order and the
// position after each committed transaction.
type vstreamDelivery struct {
	rows    []vstreamDelivered
	commits []ir.Position
	// txOf maps a TxID to its rows' indexes in rows, in delivery order.
	txOf map[string][]int
}

// drainVStreamDelivery reads changes until wantRows rows arrived and the
// stream then went quiet (so the last transaction's COMMIT is in).
func drainVStreamDelivery(t *testing.T, ctx context.Context, changes <-chan ir.Change, wantRows int) vstreamDelivery {
	t.Helper()
	d := vstreamDelivery{txOf: map[string][]int{}}
	deadline := time.After(3 * time.Minute)
	for {
		quiet := time.After(3 * time.Second)
		if len(d.rows) < wantRows {
			quiet = nil
		}
		select {
		case c, ok := <-changes:
			if !ok {
				return d
			}
			switch v := c.(type) {
			case ir.TxCommit:
				d.commits = append(d.commits, v.Position)
			case ir.Insert, ir.Update, ir.Delete:
				id := ir.ApplyIDOf(c)
				_, table := schemaTableOf(c)
				d.txOf[id.TxID] = append(d.txOf[id.TxID], len(d.rows))
				d.rows = append(d.rows, vstreamDelivered{id: id, table: table, content: renderRowChange(c), pos: c.Pos()})
			}
		case <-quiet:
			return d
		case <-deadline:
			t.Fatalf("timed out with %d/%d rows", len(d.rows), wantRows)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}

func schemaTableOf(c ir.Change) (string, string) {
	switch v := c.(type) {
	case ir.Insert:
		return v.Schema, v.Table
	case ir.Update:
		return v.Schema, v.Table
	case ir.Delete:
		return v.Schema, v.Table
	}
	return "", ""
}

func renderRowChange(c ir.Change) string {
	switch v := c.(type) {
	case ir.Insert:
		return fmt.Sprint("I ", v.Table, " ", v.Row)
	case ir.Update:
		return fmt.Sprint("U ", v.Table, " ", v.Before, " -> ", v.After)
	case ir.Delete:
		return fmt.Sprint("D ", v.Table, " ", v.Before)
	}
	return ""
}

// TestVStream_ApplyIdentity_StableAcrossMidStreamResume verifies ADR-0190
// §1's UNVERIFIED PREMISE for the VStream reader against a real two-shard
// vttestserver: a resume from a stored VGTID re-delivers every shard
// transaction's ROW events in the same order, so each re-delivered change
// carries the SAME (TxID, Seq) — and the same content under it — as on the
// original delivery.
//
// The workload is cross-shard transactions (vtgate splits each into one
// transaction per shard) touching two tables with several rows per table per
// shard, so both the per-shard TxID and the per-table ordinal beyond 1 are
// exercised. Two resumes are graded: from a mid-stream COMMIT position (the
// stored VGTID a checkpoint would persist), and from a ROW's position —
// the merged PRE-transaction VGTID, which re-delivers that transaction whole
// (the mid-transaction crash's resume point).
//
// The independent expected value is the original delivery itself, compared
// change by change: identity AND content, per transaction, in order. The
// anti-vacuity floor requires both shards, ordinals above 1 on both tables,
// and every re-delivered row stamped.
func TestVStream_ApplyIdentity_StableAcrossMidStreamResume(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServerWithShards(t, 2)
	defer cleanup()
	for _, tbl := range []string{"pt", "qt"} {
		applyVTTestSQL(t, mysqlDSN, fmt.Sprintf(`CREATE TABLE %s (id BIGINT NOT NULL, v VARCHAR(32) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB`, tbl))
		applyVTTestSQL(t, mysqlDSN, fmt.Sprintf(`ALTER VSCHEMA ON test.%s ADD VINDEX hash(id) USING hash`, tbl))
	}
	time.Sleep(3 * time.Second)

	sluiceDSN := fmt.Sprintf("%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_auto_discover_shards=true",
		mysqlDSN, grpcEndpoint)
	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	open := func(from ir.Position) (<-chan ir.Change, func()) {
		rdr, err := eng.OpenCDCReader(ctx, sluiceDSN)
		if err != nil {
			t.Fatalf("OpenCDCReader: %v", err)
		}
		changes, err := rdr.StreamChanges(ctx, from)
		if err != nil {
			t.Fatalf("StreamChanges(%q): %v", from.Token, err)
		}
		return changes, func() {
			if c, ok := rdr.(interface{ Close() error }); ok {
				_ = c.Close()
			}
		}
	}

	changes, closeA := open(ir.Position{})
	time.Sleep(3 * time.Second)
	const txs, perTable = 8, 6
	for i := 1; i <= txs; i++ {
		var b strings.Builder
		b.WriteString("START TRANSACTION;")
		for _, tbl := range []string{"pt", "qt"} {
			for j := 1; j <= perTable; j++ {
				id := i*100 + j
				fmt.Fprintf(&b, "INSERT INTO %s (id, v) VALUES (%d, 'i%d');", tbl, id, i)
			}
			fmt.Fprintf(&b, "UPDATE %s SET v = 'u%d' WHERE id IN (%d, %d, %d);", tbl, i, i*100+1, i*100+2, i*100+3)
			fmt.Fprintf(&b, "DELETE FROM %s WHERE id IN (%d, %d);", tbl, i*100+4, i*100+5)
		}
		b.WriteString("COMMIT;")
		applyVTTestSQL(t, mysqlDSN+"&multiStatements=true", b.String())
	}
	// Per transaction and table: 6 inserts + 3 updates + 2 deletes.
	const rowsPerTx = 2 * (perTable + 3 + 2)
	original := drainVStreamDelivery(t, ctx, changes, txs*rowsPerTx)
	closeA()
	if len(original.rows) != txs*rowsPerTx {
		t.Fatalf("the original delivery holds %d rows; want %d", len(original.rows), txs*rowsPerTx)
	}

	// The original delivery, by identity.
	// Seq is a per-TABLE ordinal, so an identity is unique within a table.
	type tableID struct {
		table string
		id    ir.ApplyID
	}
	byID := map[tableID]vstreamDelivered{}
	shards := map[string]bool{}
	maxSeq := map[string]uint64{}
	for _, r := range original.rows {
		if r.id.IsZero() {
			t.Fatalf("an original row carries no identity: %s", r.content)
		}
		if prev, dup := byID[tableID{r.table, r.id}]; dup {
			t.Fatalf("two rows share identity %v: %s and %s", r.id, prev.content, r.content)
		}
		byID[tableID{r.table, r.id}] = r
		shards[strings.SplitN(r.id.TxID, ":", 3)[1]] = true // vstream:<ks/shard>:<gtid>
		if r.id.Seq > maxSeq[r.table] {
			maxSeq[r.table] = r.id.Seq
		}
	}
	if len(shards) < 2 || maxSeq["pt"] < 2 || maxSeq["qt"] < 2 {
		t.Fatalf("anti-vacuity: the workload reached shards %v with max ordinals %v; need two shards and ordinals above 1 on both tables",
			shards, maxSeq)
	}
	t.Logf("original delivery: %d rows, %d shard transactions, shards %v", len(original.rows), len(original.txOf), shards)

	grade := func(name string, from ir.Position, mustRedeliver string) {
		changes, closeB := open(from)
		defer closeB()
		re := drainVStreamDelivery(t, ctx, changes, 1)
		if len(re.rows) == 0 {
			t.Fatalf("%s: the resume re-delivered nothing", name)
		}
		for _, r := range re.rows {
			if r.id.IsZero() {
				t.Errorf("%s: a re-delivered row carries no identity: %s", name, r.content)
				continue
			}
			o, ok := byID[tableID{r.table, r.id}]
			if !ok {
				t.Errorf("%s: re-delivered identity %v was never delivered originally (%s) — the identity is not stable", name, r.id, r.content)
				continue
			}
			if o.content != r.content {
				t.Errorf("%s: identity %v names %q on the original delivery but %q on the resume — a skip on it would drop a change",
					name, r.id, o.content, r.content)
			}
		}
		// Every re-delivered transaction is re-delivered WHOLE and in order.
		for tx, idx := range re.txOf {
			orig := original.txOf[tx]
			if len(idx) != len(orig) {
				t.Errorf("%s: transaction %s re-delivered %d rows; originally %d", name, tx, len(idx), len(orig))
				continue
			}
			for k := range idx {
				if re.rows[idx[k]].content != original.rows[orig[k]].content {
					t.Errorf("%s: transaction %s row %d differs in order or content", name, tx, k)
				}
			}
		}
		if mustRedeliver != "" {
			if _, ok := re.txOf[mustRedeliver]; !ok {
				t.Errorf("%s: the resume did not re-deliver transaction %s, whose row position it resumed from", name, mustRedeliver)
			}
		}
		t.Logf("%s: re-delivered %d rows in %d transactions, all with the original identities", name, len(re.rows), len(re.txOf))
	}

	// A stored mid-stream COMMIT position.
	grade("from a mid-stream COMMIT", original.commits[len(original.commits)/2], "")
	// A row's position (the merged pre-transaction VGTID) in the middle of
	// the delivery: its transaction must come back whole.
	mid := original.rows[len(original.rows)/2]
	grade("from a mid-transaction row position", mid.pos, mid.id.TxID)
}
