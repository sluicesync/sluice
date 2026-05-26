//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pin for ADR-0055 (research finding F1).
//
// The audit's load-bearing empirical claim is that sluice's
// START_REPLICATION (proto_version=2, no streaming='on' plugin arg)
// causes PG to NEVER emit streaming chunk messages in production —
// even for transactions that exceed logical_decoding_work_mem at the
// source. This test confirms that empirically by running an
// oversized transaction through a stock PG container and observing
// the receiver-side event sequence.
//
// Shape:
//
//  1. Boot PG with logical_decoding_work_mem set to a small value
//     (64 kB) so even modest transactions exceed it.
//  2. Open a sluice CDC reader (proto_version=2, no streaming).
//  3. Run a single large transaction whose row count produces well
//     over 64 kB of decoded change records.
//  4. Drain the changes channel. Assert: the transaction arrives as
//     a SINGLE TxBegin / N Insert / TxCommit triplet — not as
//     multiple TxBegin/TxCommit pairs (which would indicate
//     streaming chunks).
//
// If streaming were enabled, the applier would see ≥2 TxBegin /
// TxCommit pairs (one per chunk plus the final commit). The single-
// triplet assertion is the strict negative pin.

package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/orware/sluice/internal/ir"
)

// startPostgresForCDCWithSmallDecodeMem boots a PG container with
// logical_decoding_work_mem aggressively low so the streaming-or-not
// observation has bite. 64 kB is far below the default 64 MB; any
// reasonably-sized transaction crosses it.
//
// Stays per-test (does NOT share the container booted by TestMain)
// because logical_decoding_work_mem is set at server boot and the
// 64 kB override would corrupt the spill semantics of every other CDC
// test if the container were shared. The 3-attempt retry shape is
// applied via runPGWithRetry — see ci-retry-asymmetry: per-test boots
// stay at 3 attempts, NOT the shared container's 5.
func startPostgresForCDCWithSmallDecodeMem(t *testing.T) (dsn string, cleanup func()) {
	t.Helper()

	container := runPGWithRetry(
		t, "postgres:16",
		pgtc.WithDatabase("source_db"),
		pgtc.WithUsername("test"),
		pgtc.WithPassword("test"),
		pgtc.BasicWaitStrategies(),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Cmd: []string{
					"-c", "wal_level=logical",
					"-c", "max_wal_senders=4",
					"-c", "max_replication_slots=4",
					// 64 kB — well below any non-trivial txn. Forces
					// the source to spill to disk on the txn we drive
					// below, which is exactly the scenario streaming
					// would chunk if it were enabled.
					"-c", "logical_decoding_work_mem=64kB",
				},
			},
		}),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}

	srcConn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		terminate()
		t.Fatalf("connection string: %v", err)
	}
	return srcConn, terminate
}

// drainAllChanges drains the changes channel for up to timeout,
// returning every event observed (Insert/Update/Delete plus
// TxBegin/TxCommit; SchemaSnapshot is forwarded too — F1's negative
// pin cares about the boundary events). Unlike the row-counting
// drainChanges helper, this one returns whatever it has when the
// timeout fires; callers assert on the full event shape.
func drainAllChanges(
	t *testing.T,
	ctx context.Context,
	changes <-chan ir.Change,
	timeout time.Duration,
) []ir.Change {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	idle := time.NewTimer(2 * time.Second)
	defer idle.Stop()

	var got []ir.Change
	for {
		select {
		case c, ok := <-changes:
			if !ok {
				return got
			}
			got = append(got, c)
			// Reset the idle timer: as long as events keep arriving
			// we keep waiting; only after a 2-second silence do we
			// declare the drain complete.
			if !idle.Stop() {
				select {
				case <-idle.C:
				default:
				}
			}
			idle.Reset(2 * time.Second)
		case <-idle.C:
			return got
		case <-deadline.C:
			return got
		case <-ctx.Done():
			return got
		}
	}
}

// TestCDCReader_F1_NoStreamingChunksFromLargeTxn is the receiver-side
// empirical confirmation that sluice's START_REPLICATION does NOT
// enable pgoutput streaming.
//
// Drives a single transaction whose decoded change records exceed
// logical_decoding_work_mem (set to 64 kB on the container). If
// streaming were enabled, the receiver would see >=2 TxBegin / TxCommit
// pairs (one per chunk plus the final). The single-triplet shape
// proves streaming is OFF.
func TestCDCReader_F1_NoStreamingChunksFromLargeTxn(t *testing.T) {
	dsn, cleanup := startPostgresForCDCWithSmallDecodeMem(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE f1_pin (
			id      BIGSERIAL PRIMARY KEY,
			payload TEXT NOT NULL
		);
		ALTER TABLE f1_pin REPLICA IDENTITY FULL;
	`
	applyPGSQL(t, dsn, seedDDL)

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, dsn)
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

	// Let START_REPLICATION settle.
	time.Sleep(300 * time.Millisecond)

	// Drive a single transaction that easily exceeds 64 kB of
	// decoded change records. 1000 rows × ~100 bytes payload = ~100
	// kB of row data + pgoutput framing, comfortably over the
	// 64 kB logical_decoding_work_mem cap. Wrapping it in an
	// explicit BEGIN/COMMIT ensures PG sees a single transaction
	// (not 1000 implicit transactions). The padding string keeps
	// each row body well above the per-row threshold so the cap is
	// hit during decode, not after.
	const largeDML = `
		BEGIN;
		INSERT INTO f1_pin (payload)
			SELECT repeat('x', 100) FROM generate_series(1, 1000);
		COMMIT;
	`
	applyPGSQL(t, dsn, largeDML)

	// Drain. With streaming OFF the source decodes the whole txn
	// (spilling to disk past 64 kB) then emits BEGIN + 1000 INSERTs
	// + COMMIT. With streaming ON we'd see multiple TxBegin/TxCommit
	// pairs as the source's decoder flushes each chunk.
	got := drainAllChanges(t, ctx, changes, 90*time.Second)
	if len(got) == 0 {
		t.Fatalf("no changes drained — START_REPLICATION not delivering events")
	}

	// Count TxBegin / TxCommit / Insert. The shape we expect (with
	// streaming OFF):
	//   - 1 SchemaSnapshot (ADR-0049, emitted on first-touch of the
	//     relation) — optional, count separately.
	//   - 1 TxBegin
	//   - 1000 Inserts
	//   - 1 TxCommit
	// With streaming ON we'd see >=2 TxBegin / >=2 TxCommit.
	var (
		txBeginCount  int
		txCommitCount int
		insertCount   int
		snapshotCount int
		otherSummary  []string
	)
	for _, c := range got {
		switch v := c.(type) {
		case ir.TxBegin:
			txBeginCount++
		case ir.TxCommit:
			txCommitCount++
		case ir.Insert:
			insertCount++
		case ir.SchemaSnapshot:
			snapshotCount++
		default:
			otherSummary = append(otherSummary, summariseChange(v))
		}
	}

	t.Logf("F1 receiver-side observation: TxBegin=%d, TxCommit=%d, Insert=%d, SchemaSnapshot=%d, other=%d",
		txBeginCount, txCommitCount, insertCount, snapshotCount, len(otherSummary))
	if len(otherSummary) > 0 {
		t.Logf("F1 unexpected event types: %s", strings.Join(otherSummary, ", "))
	}

	// Load-bearing assertions:
	//   - Exactly one TxBegin / TxCommit pair. Streaming would
	//     produce more.
	//   - All 1000 inserts arrived (sanity — the test isn't
	//     exercising the audit if the txn fragmented for some other
	//     reason).
	if txBeginCount != 1 {
		t.Errorf("got %d TxBegin events; want exactly 1 (streaming OFF should produce a single boundary pair)", txBeginCount)
	}
	if txCommitCount != 1 {
		t.Errorf("got %d TxCommit events; want exactly 1 (streaming OFF should produce a single boundary pair)", txCommitCount)
	}
	if insertCount != 1000 {
		t.Errorf("got %d Insert events; want 1000 (lost rows would mean the txn fragmented or the drain timed out early)", insertCount)
	}
}

// summariseChange returns a one-line describer for an unexpected
// event type so the F1 log line is informative when something
// unexpected slips through.
func summariseChange(c ir.Change) string {
	switch v := c.(type) {
	case ir.Insert:
		return "Insert"
	case ir.Update:
		return "Update"
	case ir.Delete:
		return "Delete"
	case ir.Truncate:
		return "Truncate"
	case ir.TxBegin:
		return "TxBegin"
	case ir.TxCommit:
		return "TxCommit"
	case ir.SchemaSnapshot:
		return "SchemaSnapshot"
	default:
		_ = v
		return "unknown"
	}
}
