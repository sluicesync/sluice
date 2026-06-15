// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"sluicesync.dev/sluice/internal/ir"
)

// TestB1_AnchorCapturedAtClearTime is the locked-decision-#4c unit
// pin for the MySQL binlog path. clear(r.schemaCache) is eager but the
// *tableSchema (→ ir.Table) rebuilds LAZILY on the next row. The
// schema-history version MUST be anchored at the DDL event's OWN GTID
// captured at clear time — NOT a later position. The silent bug this
// kills: a replayed event between the DDL and the first post-DDL row
// would resolve against the pre-DDL schema if the anchor were the
// row's position.
//
// Sequence: GTIDEvent(g1) → DDL QueryEvent (captures anchor =
// {g1}) → GTIDEvent(g2) advances r.gtidSet. The captured
// pendingDDLAnchor must STILL be the g1-only set (the DDL's own
// position), unmoved by the later g2.
func TestB1_AnchorCapturedAtClearTime(t *testing.T) {
	g0, _ := gomysql.ParseGTIDSet(gomysql.MySQLFlavor, "")
	r := &CDCReader{
		schema:      "app",
		posMode:     positionModeGTID,
		gtidSet:     g0,
		tableMap:    map[uint64]string{},
		schemaCache: map[string]*tableSchema{},
		snapshotSig: map[string]ir.SchemaSignature{},
	}
	out := make(chan ir.Change, 8)
	ctx := context.Background()

	const uuid = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	sid := mustSID(t, uuid)

	// GTID g1 (the DDL's own transaction).
	g1 := &replication.BinlogEvent{
		Header: &replication.EventHeader{},
		Event:  &replication.GTIDEvent{SID: sid, GNO: 1},
	}
	if err := r.dispatch(ctx, g1, out); err != nil {
		t.Fatalf("GTID g1: %v", err)
	}

	// Generic DDL QueryEvent (not BEGIN/COMMIT/TRUNCATE) → clears the
	// cache AND captures the anchor at THIS event's position.
	ddl := &replication.BinlogEvent{
		Header: &replication.EventHeader{},
		Event:  &replication.QueryEvent{Schema: []byte("app"), Query: []byte("ALTER TABLE users ADD c INT")},
	}
	if err := r.dispatch(ctx, ddl, out); err != nil {
		t.Fatalf("DDL: %v", err)
	}
	if !r.pendingDDLActive {
		t.Fatal("pendingDDLActive not set after generic DDL")
	}
	capturedAnchor := r.pendingDDLAnchor

	// A LATER GTID (g2) advances r.gtidSet — simulating the first
	// post-DDL transaction. The captured anchor must NOT move.
	g2 := &replication.BinlogEvent{
		Header: &replication.EventHeader{},
		Event:  &replication.GTIDEvent{SID: sid, GNO: 2},
	}
	if err := r.dispatch(ctx, g2, out); err != nil {
		t.Fatalf("GTID g2: %v", err)
	}
	if r.pendingDDLAnchor != capturedAnchor {
		t.Fatalf("anchor moved after a later GTID: was %q now %q (#4c violated — replay would resolve to pre-DDL schema)",
			capturedAnchor.Token, r.pendingDDLAnchor.Token)
	}

	// Ground-truth the anchor: it must decode to the g1-only set
	// (the DDL's own position), NOT the g1+g2 set the reader is at
	// now.
	decoded, ok, err := decodeBinlogPos(capturedAnchor)
	if err != nil || !ok {
		t.Fatalf("decode captured anchor: ok=%v err=%v", ok, err)
	}
	curSet := r.gtidSet.String() // g1+g2
	if decoded.GTIDSet == curSet {
		t.Fatalf("anchor == current set %q; it must be frozen at the DDL's own (g1-only) position", curSet)
	}
}

// TestB1_MaybeSnapshot_TrueDeltaAndAnchor exercises the deferred
// emitter directly: with a DDL pending, the first post-DDL rebuild of
// a table emits ONE SchemaSnapshot anchored at the captured DDL
// position; an unchanged signature emits zero; a real column change
// emits exactly one.
func TestB1_MaybeSnapshot_TrueDeltaAndAnchor(t *testing.T) {
	anchor := ir.Position{Engine: engineNameMySQL, Token: "ddl-anchor-token"}
	r := &CDCReader{
		schema:           "app",
		snapshotSig:      map[string]ir.SchemaSignature{},
		pendingDDLActive: true,
		pendingDDLAnchor: anchor,
	}
	out := make(chan ir.Change, 8)
	ctx := context.Background()

	v1 := &tableSchema{Schema: "app", Name: "users", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "email", Type: ir.Varchar{Length: 255}},
	}}
	if err := r.maybeSnapshotSchemaB1(ctx, "app.users", v1, out); err != nil {
		t.Fatalf("first rebuild: %v", err)
	}
	// Same signature again (e.g. another row for the same table before
	// the next DDL) → no new version.
	if err := r.maybeSnapshotSchemaB1(ctx, "app.users", v1, out); err != nil {
		t.Fatalf("second rebuild (no delta): %v", err)
	}
	// Real ADD COLUMN → exactly one new version.
	v2 := &tableSchema{Schema: "app", Name: "users", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "email", Type: ir.Varchar{Length: 255}},
		{Name: "country", Type: ir.Varchar{Length: 2}},
	}}
	if err := r.maybeSnapshotSchemaB1(ctx, "app.users", v2, out); err != nil {
		t.Fatalf("post-ALTER rebuild: %v", err)
	}

	close(out)
	var snaps []ir.SchemaSnapshot
	for c := range out {
		if s, ok := c.(ir.SchemaSnapshot); ok {
			snaps = append(snaps, s)
		}
	}
	if len(snaps) != 2 {
		t.Fatalf("want 2 versions (initial + post-ALTER), got %d", len(snaps))
	}
	for i, s := range snaps {
		if s.Position != anchor {
			t.Errorf("version %d anchored at %q, want the captured DDL anchor %q (#4c)",
				i, s.Position.Token, anchor.Token)
		}
	}
	if len(snaps[1].IR.Columns) != 3 {
		t.Errorf("post-ALTER snapshot has %d columns, want 3", len(snaps[1].IR.Columns))
	}
}

// TestB1_MaybeSnapshot_NullabilityForward is the ADR-0091 F7a GAP #2 unit
// pin: a per-column NULLABILITY-only change (which does NOT move
// ir.SchemaSignatureOf — name + ordered type — the ADR-0049 decode
// contract) must produce a boundary when schemaForward=true, and must NOT
// produce an extra boundary when schemaForward=false (refuse mode keeps
// today's behavior). A genuine signature delta (ADD COLUMN) must still
// emit in BOTH modes. This locks the "separate forward signal, untouched
// decode contract" fix shape so the gate can't regress silently.
func TestB1_MaybeSnapshot_NullabilityForward(t *testing.T) {
	anchor := ir.Position{Engine: engineNameMySQL, Token: "ddl-anchor-token"}

	// notNull / dropNotNull differ ONLY in label's Nullable flag; same
	// names + ordered types, so ir.SchemaSignatureOf is identical.
	notNull := func() *tableSchema {
		return &tableSchema{Schema: "app", Name: "widgets", Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "label", Type: ir.Varchar{Length: 64}, Nullable: false},
		}}
	}
	dropNotNull := func() *tableSchema {
		return &tableSchema{Schema: "app", Name: "widgets", Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "label", Type: ir.Varchar{Length: 64}, Nullable: true},
		}}
	}
	// addCol is a real signature delta on top of dropNotNull.
	addCol := func() *tableSchema {
		return &tableSchema{Schema: "app", Name: "widgets", Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "label", Type: ir.Varchar{Length: 64}, Nullable: true},
			{Name: "extra", Type: ir.Integer{Width: 32}},
		}}
	}

	drain := func(out chan ir.Change) int {
		close(out)
		n := 0
		for c := range out {
			if _, ok := c.(ir.SchemaSnapshot); ok {
				n++
			}
		}
		return n
	}

	// Ground-truth that the two nullability shapes share a signature, so
	// the test is actually exercising the nullability-only path and not a
	// latent signature difference.
	sigNN := ir.SchemaSignatureOf(&ir.Table{Schema: "app", Name: "widgets", Columns: notNull().Columns})
	sigDN := ir.SchemaSignatureOf(&ir.Table{Schema: "app", Name: "widgets", Columns: dropNotNull().Columns})
	if !sigNN.Equal(sigDN) {
		t.Fatalf("test premise broken: notNull and dropNotNull have different signatures; the nullability-only path isn't being exercised")
	}

	newReader := func(forward bool) *CDCReader {
		return &CDCReader{
			schema:           "app",
			snapshotSig:      map[string]ir.SchemaSignature{},
			forwardNullSig:   map[string]string{},
			pendingDDLActive: true,
			pendingDDLAnchor: anchor,
			schemaForward:    forward,
		}
	}

	ctx := context.Background()

	t.Run("forward=true emits on nullability-only delta", func(t *testing.T) {
		r := newReader(true)
		out := make(chan ir.Change, 8)
		// Prime boundary (establishes both trackers).
		if err := r.maybeSnapshotSchemaB1(ctx, "app.widgets", notNull(), out); err != nil {
			t.Fatalf("prime: %v", err)
		}
		// Nullability-only flip: same signature, label NOT NULL → NULL.
		if err := r.maybeSnapshotSchemaB1(ctx, "app.widgets", dropNotNull(), out); err != nil {
			t.Fatalf("nullability flip: %v", err)
		}
		if got := drain(out); got != 2 {
			t.Fatalf("forward=true: want 2 boundaries (prime + nullability), got %d", got)
		}
	})

	t.Run("forward=false does NOT emit on nullability-only delta", func(t *testing.T) {
		r := newReader(false)
		out := make(chan ir.Change, 8)
		if err := r.maybeSnapshotSchemaB1(ctx, "app.widgets", notNull(), out); err != nil {
			t.Fatalf("prime: %v", err)
		}
		if err := r.maybeSnapshotSchemaB1(ctx, "app.widgets", dropNotNull(), out); err != nil {
			t.Fatalf("nullability flip: %v", err)
		}
		if got := drain(out); got != 1 {
			t.Fatalf("forward=false: want 1 boundary (prime only — nullability swallowed, pre-ADR-0091 behavior), got %d", got)
		}
	})

	t.Run("signature delta still emits in both modes", func(t *testing.T) {
		for _, forward := range []bool{true, false} {
			r := newReader(forward)
			out := make(chan ir.Change, 8)
			if err := r.maybeSnapshotSchemaB1(ctx, "app.widgets", dropNotNull(), out); err != nil {
				t.Fatalf("forward=%t prime: %v", forward, err)
			}
			// Real ADD COLUMN — signature changes.
			if err := r.maybeSnapshotSchemaB1(ctx, "app.widgets", addCol(), out); err != nil {
				t.Fatalf("forward=%t add column: %v", forward, err)
			}
			if got := drain(out); got != 2 {
				t.Fatalf("forward=%t: signature delta must emit; want 2 boundaries, got %d", forward, got)
			}
		}
	})
}

// TestB1_MaybeSnapshot_InactiveIsNoOp: with no DDL pending, the
// emitter is a pure no-op (steady-state rows must not write versions).
func TestB1_MaybeSnapshot_InactiveIsNoOp(t *testing.T) {
	r := &CDCReader{schema: "app", snapshotSig: map[string]ir.SchemaSignature{}}
	out := make(chan ir.Change, 2)
	tbl := &tableSchema{Schema: "app", Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}
	if err := r.maybeSnapshotSchemaB1(context.Background(), "app.users", tbl, out); err != nil {
		t.Fatalf("inactive: %v", err)
	}
	close(out)
	if len(out) != 0 {
		t.Errorf("steady-state (no DDL pending) emitted %d changes, want 0", len(out))
	}
}

func mustSID(t *testing.T, uuid string) []byte {
	t.Helper()
	u, err := parseUUIDBytes(uuid)
	if err != nil {
		t.Fatalf("parse uuid: %v", err)
	}
	return u
}

// parseUUIDBytes turns the canonical 8-4-4-4-12 hex form into the
// 16-byte SID the GTIDEvent carries (inverse of formatSIDAsUUID).
func parseUUIDBytes(s string) ([]byte, error) {
	out := make([]byte, 0, 16)
	for i := 0; i < len(s); i++ {
		if s[i] == '-' {
			continue
		}
		hi := hexVal(s[i])
		i++
		lo := hexVal(s[i])
		out = append(out, byte(hi<<4|lo))
	}
	return out, nil
}

func hexVal(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10
	}
	return 0
}
