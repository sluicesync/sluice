// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// lineageFakeDriver answers the lineage probes from its DSN:
// "executed=<set>|contained=<0|1>|uuid=<server_uuid>" for the MySQL
// probe — ONE statement carrying the GTID_SUBSET verdict, the executed
// set and the @@server_uuid witness, matched on its exact text so a
// change that split the witness back onto a second round trip (two pool
// connections; two backends behind a load balancer) is red here — and
// "state=<set>" for MariaDB's @@gtid_binlog_state. A DSN that omits
// uuid= makes the WHOLE MySQL statement fail, which is what a source
// that refuses @@global.server_uuid does to the folded probe: a cell
// that wants a verdict must state its uuid, and the one cell that wants
// the unreadable-witness shape omits it on purpose.
type lineageFakeDriver struct{}

type lineageFakeConn struct {
	executed, contained, state string
	uuid                       string
	hasUUID                    bool
}

func (lineageFakeDriver) Open(dsn string) (driver.Conn, error) {
	c := &lineageFakeConn{contained: "0"}
	for _, kv := range strings.Split(dsn, "|") {
		k, v, _ := strings.Cut(kv, "=")
		switch k {
		case "executed":
			c.executed = v
		case "contained":
			c.contained = v
		case "state":
			c.state = v
		case "uuid":
			c.uuid, c.hasUUID = v, true
		}
	}
	return c, nil
}

func (c *lineageFakeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected Prepare")
}
func (c *lineageFakeConn) Close() error              { return nil }
func (c *lineageFakeConn) Begin() (driver.Tx, error) { return nil, errors.New("unexpected Begin") }

func (c *lineageFakeConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	switch query {
	case "SELECT GTID_SUBSET(?, @@global.gtid_executed), @@global.gtid_executed, @@global.server_uuid":
		if !c.hasUUID {
			return nil, errors.New("lineage fake: @@global.server_uuid is not readable on this source (no uuid= in the cell's DSN)")
		}
		return &lineageFakeRows{cols: []string{"contained", "executed", "server_uuid"}, vals: []driver.Value{c.contained, c.executed, c.uuid}}, nil
	case "SELECT @@gtid_binlog_state":
		return &lineageFakeRows{cols: []string{"@@gtid_binlog_state"}, vals: []driver.Value{c.state}}, nil
	}
	return nil, errors.New("unexpected query: " + query)
}

type lineageFakeRows struct {
	cols []string
	vals []driver.Value
	sent bool
}

func (r *lineageFakeRows) Columns() []string { return r.cols }
func (r *lineageFakeRows) Close() error      { return nil }
func (r *lineageFakeRows) Next(dest []driver.Value) error {
	if r.sent {
		return io.EOF
	}
	r.sent = true
	copy(dest, r.vals)
	return nil
}

var registerLineageFakeOnce sync.Once

func newLineageFakeDB(t *testing.T, spec string) *sql.DB {
	t.Helper()
	registerLineageFakeOnce.Do(func() { sql.Register("sluice-lineage-test", lineageFakeDriver{}) })
	db, err := sql.Open("sluice-lineage-test", spec)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// wantVerdict grades an error against the ONE property this class turns
// on: whether it may route into the automatic cold-start re-snapshot
// (ir.ErrPositionInvalid) or must be terminal (ir.ErrPositionForeignLineage).
func wantVerdict(t *testing.T, name string, err error, foreign bool) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: verdict = nil; want a refusal", name)
	}
	if got := errors.Is(err, ir.ErrPositionForeignLineage); got != foreign {
		t.Errorf("%s: wraps ErrPositionForeignLineage = %v; want %v: %v", name, got, foreign, err)
	}
	if got := errors.Is(err, ir.ErrPositionInvalid); got != !foreign {
		t.Errorf("%s: wraps ErrPositionInvalid = %v; want %v (that sentinel routes the destructive automatic "+
			"re-copy): %v", name, got, !foreign, err)
	}
	if foreign {
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeCDCLineageMismatch {
			t.Errorf("%s: a foreign verdict must carry %s: %v", name, sluicecode.CodeCDCLineageMismatch, err)
		}
		if !strings.Contains(err.Error(), "--restart-from-scratch") {
			t.Errorf("%s: a foreign verdict must name the deliberate re-copy flag: %v", name, err)
		}
	}
}

// TestLineageVerdicts_ForeignIsTerminalResetIsNot pins the discrimination
// audit 2026-09-09 A0909-MYSQL-HIGH-1 asked for, per lane, as refined by
// audit 2026-09-15 A0915-MYSQL-HIGH-2 and the operator decisions of
// 2026-09-15 that closed its two open policy questions: a source whose
// executed set is NON-EMPTY and shares no UUID with the position is a
// different lineage (terminal); an EMPTY executed set keeps the automatic
// re-copy ONLY on an instance whose @@server_uuid the position names; a
// set merely BEHIND the position (shares UUIDs, lacks transactions) is
// terminal under EITHER uuid — under a foreign one it is the rebuilt-node
// shape, under the server's own it is a rollback or in-place restore
// with the target AHEAD, and the two diagnoses differ. On MariaDB an
// EMPTY binlog state is terminal: no instance identity can tell a reset
// from a rebuild. The MySQL cells are the full {empty, behind} × {uuid
// named, uuid foreign} matrix, because a green on one cell of a
// family-dispatched verdict says nothing about the others.
func TestLineageVerdicts_ForeignIsTerminalResetIsNot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const (
		uuidA  = "aaaaaaaa-0000-0000-0000-000000000001"
		uuidB  = "bbbbbbbb-0000-0000-0000-000000000002"
		resume = uuidA + ":1-15"
	)

	t.Run("mysql GTID continuity", func(t *testing.T) {
		t.Parallel()
		if err := verifyGTIDLineageContinuity(ctx, newLineageFakeDB(t, "contained=1|executed="+resume+"|uuid="+uuidA), resume); err != nil {
			t.Fatalf("contained: %v; want nil", err)
		}
		// The disjoint arm never consults the witness: the source's own
		// uuid being named is irrelevant once the executed set shares
		// nothing with the position.
		wantVerdict(t, "foreign (non-empty, disjoint)", verifyGTIDLineageContinuity(ctx,
			newLineageFakeDB(t, "contained=0|executed="+uuidB+":1-8|uuid="+uuidB), resume), true)
		wantVerdict(t, "foreign (non-empty, disjoint) even when the witness names the position's uuid", verifyGTIDLineageContinuity(ctx,
			newLineageFakeDB(t, "contained=0|executed="+uuidB+":1-8|uuid="+uuidA), resume), true)
		// The four-cell witness matrix.
		wantVerdict(t, "reset on the SAME instance (empty executed, uuid named by the position)", verifyGTIDLineageContinuity(ctx,
			newLineageFakeDB(t, "contained=0|executed=|uuid="+uuidA), resume), false)
		wantVerdict(t, "reset on a REBUILT instance (empty executed, uuid the position never saw)", verifyGTIDLineageContinuity(ctx,
			newLineageFakeDB(t, "contained=0|executed=|uuid="+uuidB), resume), true)
		behindOwn := verifyGTIDLineageContinuity(ctx, newLineageFakeDB(t, "contained=0|executed="+uuidA+":1-9|uuid="+uuidA), resume)
		wantVerdict(t, "behind on the SAME instance (shares the UUID, lacks transactions)", behindOwn, true)
		// Its own diagnosis, not the rebuilt-node one: the instance is the
		// right one and holds LESS than the target.
		for _, phrase := range []string{"under this server's own @@server_uuid " + uuidA, "rolled back", "AHEAD", "nothing on the target was touched"} {
			if behindOwn != nil && !strings.Contains(behindOwn.Error(), phrase) {
				t.Errorf("behind-own-uuid refusal missing %q: %v", phrase, behindOwn)
			}
		}
		if ce, ok := sluicecode.FromError(behindOwn); !ok || ce.Hint != rolledBackRemedy {
			t.Errorf("behind-own-uuid refusal must carry the rolled-back remedy, got %+v", ce)
		}
		behindForeign := verifyGTIDLineageContinuity(ctx, newLineageFakeDB(t, "contained=0|executed="+uuidA+":1-9|uuid="+uuidB), resume)
		wantVerdict(t, "behind on a REBUILT instance (seeded through gtid_purged under the old uuid)", behindForeign, true)
		if behindForeign != nil && strings.Contains(behindForeign.Error(), "AHEAD") {
			t.Errorf("the rebuilt-node BEHIND refusal carries the same-instance rollback diagnosis: %v", behindForeign)
		}
		wantVerdict(t, "behind on a promoted replica whose own uuid the multi-source position names", verifyGTIDLineageContinuity(ctx,
			newLineageFakeDB(t, "contained=0|executed="+uuidA+":1-9,"+uuidB+":1-3|uuid="+uuidB), resume+","+uuidB+":1-3"), true)
		// The uuid compare is case-insensitive like the set compare, and a
		// promoted replica's position names its old primary too.
		wantVerdict(t, "reset on the same instance, uuid spelled upper-case by the server", verifyGTIDLineageContinuity(ctx,
			newLineageFakeDB(t, "contained=0|executed=|uuid="+strings.ToUpper(uuidA)), resume), false)
		wantVerdict(t, "reset on a promoted replica whose uuid the multi-source position names", verifyGTIDLineageContinuity(ctx,
			newLineageFakeDB(t, "contained=0|executed=|uuid="+uuidB), resume+","+uuidB+":1-3"), false)
		// A witness that cannot be read is neither verdict: loud, and
		// routing nothing. Under the folded statement the unreadable
		// witness takes the whole probe with it, on every arm — the
		// contained cell here would have passed on the split read and
		// is now loud too (documented at verifyGTIDLineageContinuity).
		for _, cell := range []string{"contained=0|executed=", "contained=1|executed=" + resume} {
			if err := verifyGTIDLineageContinuity(ctx, newLineageFakeDB(t, cell), resume); err == nil ||
				errors.Is(err, ir.ErrPositionInvalid) || errors.Is(err, ir.ErrPositionForeignLineage) {
				t.Fatalf("unreadable @@server_uuid (%s): %v; want a plain error carrying neither sentinel", cell, err)
			}
		}
	})

	t.Run("mariadb domain door", func(t *testing.T) {
		t.Parallel()
		if err := verifyMariaDBDomainsPresent(ctx, newLineageFakeDB(t, "state=0-1-30"), "0-1-12"); err != nil {
			t.Fatalf("domain present: %v; want nil", err)
		}
		wantVerdict(t, "foreign (state non-empty, domain absent)",
			verifyMariaDBDomainsPresent(ctx, newLineageFakeDB(t, "state=7-4-100"), "0-1-12"), true)
		empty := verifyMariaDBDomainsPresent(ctx, newLineageFakeDB(t, "state="), "0-1-12")
		wantVerdict(t, "state empty (a same-server reset or a rebuilt node; MariaDB cannot say which)", empty, true)
		// Both possibilities named, because sluice cannot tell the operator
		// which one it is looking at.
		for _, phrase := range []string{"RESET MASTER on this same server", "REBUILT", "point the sync at the instance that holds the position"} {
			if empty != nil && !strings.Contains(empty.Error(), phrase) {
				t.Errorf("empty-state refusal missing %q: %v", phrase, empty)
			}
		}
		if ce, ok := sluicecode.FromError(empty); !ok || ce.Hint != mariadbEmptyStateRemedy {
			t.Errorf("empty-state refusal must carry the two-possibility remedy, got %+v", ce)
		}
		// The brand-new-source position (an empty resume set) still binds
		// nothing, even on an empty state.
		if err := verifyMariaDBDomainsPresent(ctx, newLineageFakeDB(t, "state="), ""); err != nil {
			t.Fatalf("empty resume set on an empty state: %v; want nil", err)
		}
	})

	t.Run("file/pos instance identity", func(t *testing.T) {
		t.Parallel()
		wantVerdict(t, "different server_uuid",
			verifySourceInstanceIdentity(ctx, "aaaaaaaa-0000-0000-0000-000000000001", "bbbbbbbb-0000-0000-0000-000000000002"), true)
		if err := verifySourceInstanceIdentity(ctx, "aaaaaaaa-0000-0000-0000-000000000001", "aaaaaaaa-0000-0000-0000-000000000001"); err != nil {
			t.Fatalf("same server_uuid: %v; want nil", err)
		}
	})

	t.Run("vstream foreign arm", func(t *testing.T) {
		t.Parallel()
		wantVerdict(t, "lineageForeign", lineageRefusal(lineageForeign, "0", "ks:0@replica", "a:1-2", "b:1-1"), true)
		wantVerdict(t, "lineageErrantGTID (policy: still routes)", lineageRefusal(lineageErrantGTID, "0", "ks:0@replica", "a:1-2", "b:1-1"), false)
	})
}

// TestVerifyLineage_AnswersOnlyTheForeignQuestion pins the reactive door's
// contract: the reader reports a foreign lineage and NOTHING else — a
// reset, a purge, an undecodable position all answer nil, because the door
// may only narrow what the automatic recovery destroys.
func TestVerifyLineage_AnswersOnlyTheForeignQuestion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	gtidPos := func(set string) ir.Position {
		p, err := encodeBinlogPos(binlogPos{Mode: positionModeGTID, GTIDSet: set})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		return p
	}
	const resume = "aaaaaaaa-0000-0000-0000-000000000001:1-15"

	foreign := &CDCReader{db: newLineageFakeDB(t, "contained=0|executed=bbbbbbbb-0000-0000-0000-000000000002:1-8|uuid=bbbbbbbb-0000-0000-0000-000000000002"), flavor: FlavorVanilla}
	if err := foreign.VerifyLineage(ctx, gtidPos(resume)); !errors.Is(err, ir.ErrPositionForeignLineage) {
		t.Fatalf("foreign source: VerifyLineage = %v; want ErrPositionForeignLineage", err)
	}
	// An UNREADABLE witness is "could not judge", and the door's
	// documented answer to that is nil — fail-OPEN: the reactive recovery
	// proceeds exactly as it would on a source this door cannot see. That
	// is a deliberate posture (the door may only narrow what the recovery
	// destroys, never widen what it refuses), and this pin exists so a
	// change to it is a decision rather than a drift. Under the folded
	// probe an unreadable @@server_uuid fails the whole statement, so the
	// shape is one source that answers nothing.
	unreadable := &CDCReader{db: newLineageFakeDB(t, "contained=0|executed="), flavor: FlavorVanilla}
	if err := unreadable.VerifyLineage(ctx, gtidPos(resume)); err != nil {
		t.Fatalf("unreadable witness: VerifyLineage = %v; want nil (documented fail-open — the door answers only the foreign question)", err)
	}
	reset := &CDCReader{db: newLineageFakeDB(t, "contained=0|executed=|uuid=aaaaaaaa-0000-0000-0000-000000000001"), flavor: FlavorVanilla}
	if err := reset.VerifyLineage(ctx, gtidPos(resume)); err != nil {
		t.Fatalf("reset source: VerifyLineage = %v; want nil (not this door's question)", err)
	}
	// The reactive door reaches the SAME function, so the rebuilt-instance
	// shape (empty set, foreign uuid) answers foreign here too — this is
	// what stops the mid-stream re-snapshot from dropping the target on a
	// replaced node (A0915-MYSQL-HIGH-2).
	rebuilt := &CDCReader{db: newLineageFakeDB(t, "contained=0|executed=|uuid=bbbbbbbb-0000-0000-0000-000000000002"), flavor: FlavorVanilla}
	if err := rebuilt.VerifyLineage(ctx, gtidPos(resume)); !errors.Is(err, ir.ErrPositionForeignLineage) {
		t.Fatalf("rebuilt source (empty set, foreign uuid): VerifyLineage = %v; want ErrPositionForeignLineage", err)
	}
	// And the BEHIND-own-uuid arm: the reactive door must refuse it too, or
	// a position that goes invalid mid-stream on a rolled-back source would
	// still drop the target it is behind (operator decision 2026-09-15).
	rolledBack := &CDCReader{db: newLineageFakeDB(t, "contained=0|executed=aaaaaaaa-0000-0000-0000-000000000001:1-9|uuid=aaaaaaaa-0000-0000-0000-000000000001"), flavor: FlavorVanilla}
	if err := rolledBack.VerifyLineage(ctx, gtidPos(resume)); !errors.Is(err, ir.ErrPositionForeignLineage) {
		t.Fatalf("rolled-back source (behind, own uuid): VerifyLineage = %v; want ErrPositionForeignLineage", err)
	}
	if err := foreign.VerifyLineage(ctx, ir.Position{}); err != nil {
		t.Fatalf("undecodable position: VerifyLineage = %v; want nil", err)
	}
}
