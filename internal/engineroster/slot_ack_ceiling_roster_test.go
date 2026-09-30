// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Fail-by-default roster for the slot-ack ceiling's PREMISE (GC-41).
//
// A Postgres source's slot is released only as far as the position the
// TARGET reports through ChangeApplier.ReadPosition — the pipeline's slot-ack
// ceiling sidecar reads it back and releases it. That is safe for exactly one
// reason: ReadPosition returns the position the target has DURABLY committed,
// the same row a warm resume restarts from. An applier whose ReadPosition
// answered from an in-memory frontier, or from a row written before the data
// it describes committed, would let the slot release changes the target does
// not hold — the silent-loss shape GC-41 closed.
//
// The design needs no applier to REPORT anything (the defect it replaced was
// feedback that only one engine sent, and that engine's lane path forgot), so
// what a new engine owes is not an opt-in call but a VERDICT on that premise.
// Every registered engine must carry one here, and an engine nobody has
// classified fails the build.
//
// Reach, stated so the name cannot be read as broader than the truth: for
// the NO-applier class the verdict is CHECKED — OpenChangeApplier is called and
// must refuse with the engine's own not-implemented sentinel. For the applier
// class it is a CITATION: the named ReadPosition and the integration test that
// grades it end to end (a stopped stream resumes to the exact row count, and
// confirmed_flush_lsn never passes the persisted position). Checking that
// class mechanically needs a live target, which this package does not boot.
package engineroster_test

import (
	"context"
	"errors"
	"testing"

	"sluicesync.dev/sluice/internal/engines"
	d1trigger "sluicesync.dev/sluice/internal/engines/d1-trigger"
	"sluicesync.dev/sluice/internal/engines/flatfile"
	"sluicesync.dev/sluice/internal/engines/mydumper"
	"sluicesync.dev/sluice/internal/engines/sqlite"
	sqlitetrigger "sluicesync.dev/sluice/internal/engines/sqlite-trigger"
)

// The citations the applier-class entries carry: which ReadPosition, why it
// is durable, and the test that grades it end to end.
const (
	pgReadPosition = "postgres.ChangeApplier.ReadPosition reads sluice_cdc_state.source_position, written with or after the " +
		"data it covers (in the same transaction on the serial paths; in WriteCheckpoint's own transaction after the " +
		"lanes' data commits on the concurrent path); graded by " +
		"TestStreamer_PostgresToPostgres_SlotAckFollowsTheDurablePosition. UNVERIFIED PREMISE on a PlanetScale Neki " +
		"(sharded Postgres) target: the argument assumes a ReadPosition routed through the Neki router sees the " +
		"committed control row (read-your-writes for the row's placement); no test runs the sidecar against Neki"
	mysqlReadPosition = "mysql.ChangeApplier.ReadPosition reads sluice_cdc_state.source_position, written with or after the " +
		"data it covers (in the same transaction on the serial paths; in WriteCheckpoint's own transaction after the " +
		"lanes' data commits on the concurrent path); graded by " +
		"TestStreamer_PostgresToMySQL_SlotAckNeverPassesTheDurablePosition"
	// vitessControlKeyspace qualifies "same transaction" for the Vitess
	// flavors: with --control-keyspace the control row lives in another
	// keyspace, and a vtgate transaction_mode=MULTI commit spanning two
	// keyspaces is NOT atomic — shards commit in first-touch order and stop
	// at the first failure. The row is durable only AFTER its data because
	// the batch hooks touch the data keyspace first (GC-41 (c), data before
	// control); a control row that committed before its data would release
	// the slot past rows that never landed.
	vitessControlKeyspace = ". Under vtgate MULTI with --control-keyspace the position is not in the data's " +
		"transaction; it is durable after the data only because the apply hooks commit data before control " +
		"(GC-41 (c))"
)

// slotAckCeilingRoster classifies every registered engine as a sync TARGET.
var slotAckCeilingRoster = map[string]struct {
	applier bool
	reason  string
}{
	"postgres":         {true, pgReadPosition},
	"postgres-trigger": {true, "OpenChangeApplier delegates to the postgres engine — the same applier. " + pgReadPosition},

	"mysql":       {true, mysqlReadPosition},
	"mariadb":     {true, "MySQL flavor: the same ChangeApplier. " + mysqlReadPosition},
	"planetscale": {true, "MySQL flavor: the same ChangeApplier. " + mysqlReadPosition + vitessControlKeyspace},
	"vitess":      {true, "MySQL flavor: the same ChangeApplier. " + mysqlReadPosition + vitessControlKeyspace},

	"mydumper":       {false, "a MySQL dump-format reader; source-only"},
	"sqlite":         {false, "SQLite has no change-apply; migrate target only"},
	"sqlite-trigger": {false, "trigger-CDC source only"},
	"d1":             {false, "Cloudflare D1 is a migrate source only"},
	"d1-trigger":     {false, "trigger-CDC source only"},
	"csv":            {false, "flat-file reader"},
	"tsv":            {false, "flat-file reader"},
	"ndjson":         {false, "flat-file reader"},
}

// notImplemented is every no-applier engine's refusal sentinel.
var notImplemented = []error{
	flatfile.ErrNotImplemented,
	mydumper.ErrNotImplemented,
	sqlite.ErrNotImplemented,
	sqlite.ErrD1NotImplemented,
	sqlitetrigger.ErrNotImplemented,
	d1trigger.ErrNotImplemented,
}

func TestSlotAckCeilingRoster_EveryRegisteredEngine(t *testing.T) {
	names := engines.Names()
	if len(names) < 12 {
		t.Fatalf("registry reports %d engines; want >= 12 — the enumeration is broken and this roster "+
			"would pass while checking almost nothing", len(names))
	}

	appliers := 0
	for _, name := range names {
		entry, listed := slotAckCeilingRoster[name]
		if !listed {
			t.Errorf("engine %q is registered but absent from slotAckCeilingRoster.\n"+
				"  If it can be a sync TARGET, a Postgres source's slot will be released as far as its "+
				"ChangeApplier.ReadPosition says (GC-41). Confirm ReadPosition returns the DURABLY committed "+
				"position — never an in-memory frontier — and cite the integration test that grades it.\n"+
				"  If it cannot, classify it applier=false; the test then checks the refusal.", name)
			continue
		}
		if entry.reason == "" {
			t.Errorf("engine %q is classified with an EMPTY reason", name)
		}
		eng, _ := engines.Get(name)
		if entry.applier {
			appliers++
			continue
		}
		applier, err := eng.OpenChangeApplier(context.Background(), "")
		if applier != nil || !isNotImplemented(err) {
			t.Errorf("engine %q is classified applier=false, but OpenChangeApplier returned (%v, %v) rather "+
				"than its not-implemented refusal — it may now be a sync target, and its ReadPosition owes a "+
				"verdict (GC-41)", name, applier, err)
		}
	}
	if appliers < 2 {
		t.Errorf("only %d engine(s) classify as sync targets; want >= 2 (the Postgres and MySQL appliers)", appliers)
	}

	for name := range slotAckCeilingRoster {
		if _, ok := engines.Get(name); !ok {
			t.Errorf("slotAckCeilingRoster lists %q, which is not registered — drop the entry or restore the engine", name)
		}
	}
}

func isNotImplemented(err error) bool {
	for _, sentinel := range notImplemented {
		if errors.Is(err, sentinel) {
			return true
		}
	}
	return false
}
