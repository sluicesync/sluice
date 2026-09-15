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
// "executed=<set>|contained=<0|1>" for the MySQL GTID_SUBSET probe,
// "uuid=<server_uuid>" for the @@server_uuid witness the BEHIND and EMPTY
// arms read (a DSN that omits it makes that read FAIL, so a cell that
// reaches the witness without stating it errors rather than silently
// measuring the empty-uuid case), and "state=<set>" for MariaDB's
// @@gtid_binlog_state.
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
	case "SELECT GTID_SUBSET(?, @@global.gtid_executed), @@global.gtid_executed":
		return &lineageFakeRows{cols: []string{"contained", "executed"}, vals: []driver.Value{c.contained, c.executed}}, nil
	case "SELECT @@gtid_binlog_state":
		return &lineageFakeRows{cols: []string{"@@gtid_binlog_state"}, vals: []driver.Value{c.state}}, nil
	case "SELECT @@global.server_uuid":
		if !c.hasUUID {
			return nil, errors.New("lineage fake: the cell reached the @@server_uuid witness without a uuid= in its DSN")
		}
		return &lineageFakeRows{cols: []string{"@@global.server_uuid"}, vals: []driver.Value{c.uuid}}, nil
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
// audit 2026-09-15 A0915-MYSQL-HIGH-2 on the MySQL arm: a source whose
// executed set is NON-EMPTY and shares no UUID with the position is a
// different lineage (terminal); an EMPTY executed set, and a set that is
// merely BEHIND (shares UUIDs, lacks transactions), keep the automatic
// re-copy ONLY on an instance whose @@server_uuid the position names —
// on any other instance they are the rebuilt/restored-node shape and are
// terminal too. The MySQL cells are the full {empty, behind} × {uuid
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
		if err := verifyGTIDLineageContinuity(ctx, newLineageFakeDB(t, "contained=1|executed="+resume), resume); err != nil {
			t.Fatalf("contained: %v; want nil", err)
		}
		wantVerdict(t, "foreign (non-empty, disjoint)", verifyGTIDLineageContinuity(ctx,
			newLineageFakeDB(t, "contained=0|executed="+uuidB+":1-8"), resume), true)
		// The four-cell witness matrix.
		wantVerdict(t, "reset on the SAME instance (empty executed, uuid named by the position)", verifyGTIDLineageContinuity(ctx,
			newLineageFakeDB(t, "contained=0|executed=|uuid="+uuidA), resume), false)
		wantVerdict(t, "reset on a REBUILT instance (empty executed, uuid the position never saw)", verifyGTIDLineageContinuity(ctx,
			newLineageFakeDB(t, "contained=0|executed=|uuid="+uuidB), resume), true)
		wantVerdict(t, "behind on the SAME instance (shares the UUID, lacks transactions)", verifyGTIDLineageContinuity(ctx,
			newLineageFakeDB(t, "contained=0|executed="+uuidA+":1-9|uuid="+uuidA), resume), false)
		wantVerdict(t, "behind on a REBUILT instance (seeded through gtid_purged under the old uuid)", verifyGTIDLineageContinuity(ctx,
			newLineageFakeDB(t, "contained=0|executed="+uuidA+":1-9|uuid="+uuidB), resume), true)
		// The uuid compare is case-insensitive like the set compare, and a
		// promoted replica's position names its old primary too.
		wantVerdict(t, "reset on the same instance, uuid spelled upper-case by the server", verifyGTIDLineageContinuity(ctx,
			newLineageFakeDB(t, "contained=0|executed=|uuid="+strings.ToUpper(uuidA)), resume), false)
		wantVerdict(t, "reset on a promoted replica whose uuid the multi-source position names", verifyGTIDLineageContinuity(ctx,
			newLineageFakeDB(t, "contained=0|executed=|uuid="+uuidB), resume+","+uuidB+":1-3"), false)
		// A witness that cannot be read is neither verdict: loud, and
		// routing nothing.
		if err := verifyGTIDLineageContinuity(ctx, newLineageFakeDB(t, "contained=0|executed="), resume); err == nil ||
			errors.Is(err, ir.ErrPositionInvalid) || errors.Is(err, ir.ErrPositionForeignLineage) {
			t.Fatalf("unreadable @@server_uuid on the empty arm: %v; want a plain error carrying neither sentinel", err)
		}
	})

	t.Run("mariadb domain door", func(t *testing.T) {
		t.Parallel()
		if err := verifyMariaDBDomainsPresent(ctx, newLineageFakeDB(t, "state=0-1-30"), "0-1-12"); err != nil {
			t.Fatalf("domain present: %v; want nil", err)
		}
		wantVerdict(t, "foreign (state non-empty, domain absent)",
			verifyMariaDBDomainsPresent(ctx, newLineageFakeDB(t, "state=7-4-100"), "0-1-12"), true)
		wantVerdict(t, "reset (state empty)",
			verifyMariaDBDomainsPresent(ctx, newLineageFakeDB(t, "state="), "0-1-12"), false)
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

	foreign := &CDCReader{db: newLineageFakeDB(t, "contained=0|executed=bbbbbbbb-0000-0000-0000-000000000002:1-8"), flavor: FlavorVanilla}
	if err := foreign.VerifyLineage(ctx, gtidPos(resume)); !errors.Is(err, ir.ErrPositionForeignLineage) {
		t.Fatalf("foreign source: VerifyLineage = %v; want ErrPositionForeignLineage", err)
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
	if err := foreign.VerifyLineage(ctx, ir.Position{}); err != nil {
		t.Fatalf("undecodable position: VerifyLineage = %v; want nil", err)
	}
}
