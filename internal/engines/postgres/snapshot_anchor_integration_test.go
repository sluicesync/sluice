//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// A0909-STOP-1, source half: the premise the stopped-cold-start resume
// rests on, re-measured against a real server, and every verdict
// VerifySnapshotAnchor can reach.
//
// The premise is a fact about PostgreSQL, not about sluice — "an
// unconsumed logical slot's confirmed_flush_lsn IS the consistent point
// its snapshot was exported at, and consuming it delivers only what
// happened after that point". A future PostgreSQL release could change
// it, and if it did, the resume gate would keep passing while resuming
// from a position that no longer means what it means today. So it is
// asserted here rather than written down in a comment.

package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"sluicesync.dev/sluice/internal/ir"
)

// anchorLSN pulls the LSN out of a snapshot stream's position token.
// The token is this engine's own JSON shape; decoding it here rather
// than reaching into pgPos keeps the test reading the same surface the
// pipeline persists.
func anchorLSN(t *testing.T, pos ir.Position) string {
	t.Helper()
	var decoded struct {
		Slot string `json:"slot"`
		LSN  string `json:"lsn"`
	}
	if err := json.Unmarshal([]byte(pos.Token), &decoded); err != nil {
		t.Fatalf("decode position token %q: %v", pos.Token, err)
	}
	if decoded.LSN == "" {
		t.Fatalf("position token %q carries no LSN", pos.Token)
	}
	return decoded.LSN
}

func slotLSNs(t *testing.T, dsn, slot string) (restart, confirmed string, active bool) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var r, c sql.NullString
	err = db.QueryRow(
		`SELECT COALESCE(restart_lsn::text, ''), COALESCE(confirmed_flush_lsn::text, ''), active
		   FROM pg_replication_slots WHERE slot_name = $1`, slot,
	).Scan(&r, &c, &active)
	if err != nil {
		t.Fatalf("read slot %q: %v", slot, err)
	}
	return r.String, c.String, active
}

// TestSnapshotAnchor_UnconsumedSlotHoldsTheConsistentPoint is the
// PREMISE pin. It re-derives, on the running server, the three facts
// the resume gate treats as given:
//
//  1. immediately after CREATE_REPLICATION_SLOT … EXPORT_SNAPSHOT, the
//     slot's confirmed_flush_lsn EQUALS the consistent point the
//     snapshot was taken at (restart_lsn may be earlier — that is the
//     retention floor, not the read point);
//  2. committing more rows does NOT move either LSN, because nothing
//     has consumed the slot;
//  3. consuming the slot then delivers EXACTLY the post-snapshot rows
//     and none of the pre-snapshot ones — which is what makes
//     "skip the copy and start CDC here" both lossless and
//     duplicate-free.
//
// Measured by hand on postgres:16 (2026-09-09) as consistent_point
// 0/1946620 == confirmed_flush_lsn 0/1946620, restart_lsn 0/19465E8.
func TestSnapshotAnchor_UnconsumedSlotHoldsTheConsistentPoint(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	// The markers are deliberately unlike anything else in the stream.
	// A pgoutput message carries the RELATION and its column names as
	// well as the values, so a marker that is a substring of the table
	// or column name ("pre" in "anchor_premise") matches the relation
	// message and the assertion reads inverted — which is exactly what
	// the first cut of this test did.
	applyDDL(t, dsn, `CREATE TABLE anchor_premise (id BIGINT PRIMARY KEY, v TEXT);
		INSERT INTO anchor_premise (id, v) VALUES (1, 'zzbeforesnapzz'), (2, 'zzbeforesnapzz'), (3, 'zzbeforesnapzz');`)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	eng := Engine{}
	const slot = "sluice_premise_slot"
	stream, err := eng.OpenSnapshotStreamWithSlot(ctx, dsn, slot)
	if err != nil {
		t.Fatalf("open snapshot stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	consistentPoint := anchorLSN(t, stream.Position)
	restart, confirmed, active := slotLSNs(t, dsn, slot)
	if confirmed != consistentPoint {
		t.Fatalf("PREMISE BROKEN: a freshly created, unconsumed slot's confirmed_flush_lsn is %q but the "+
			"snapshot's consistent point is %q. The stopped-cold-start resume compares those two for equality "+
			"to prove nothing has consumed the slot; if they differ at creation, that gate can never pass — and "+
			"worse, if they differ in the other direction on some server, it would pass on a slot that HAS moved",
			confirmed, consistentPoint)
	}
	if active {
		t.Errorf("slot %q reads active immediately after creation; the resume gate treats active as "+
			"unprovable, so this would make every resume refuse", slot)
	}
	t.Logf("premise 1: consistent_point=%s confirmed_flush_lsn=%s restart_lsn=%s", consistentPoint, confirmed, restart)

	// Release the snapshot's read side so the slot is genuinely
	// unattached, then commit rows AFTER the snapshot.
	if err := stream.ReleaseRows(); err != nil {
		t.Fatalf("release snapshot rows: %v", err)
	}
	applyDDL(t, dsn, `INSERT INTO anchor_premise (id, v) VALUES (4, 'zzaftersnapzz'), (5, 'zzaftersnapzz');`)

	_, confirmedAfter, _ := slotLSNs(t, dsn, slot)
	if confirmedAfter != consistentPoint {
		t.Fatalf("PREMISE BROKEN: committing rows moved an UNCONSUMED slot's confirmed_flush_lsn from %q to "+
			"%q. The resume gate reads a moved LSN as 'something consumed this slot' and refuses, so this would "+
			"turn ordinary source traffic into a refusal", consistentPoint, confirmedAfter)
	}

	// Premise 3: consuming delivers only what came after the snapshot.
	// PEEK, not get, so the slot is left where the other assertions want
	// it — and the BINARY variant, because pgoutput is a binary output
	// plugin and the textual function refuses it outright (SQLSTATE
	// 0A000). pgoutput still renders column VALUES as text inside those
	// bytes, which is why the substring search below works.
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.QueryContext(ctx,
		`SELECT data FROM pg_logical_slot_peek_binary_changes($1, NULL, NULL, 'proto_version', '1', 'publication_names', $2)`,
		slot, eng.publicationName())
	if err != nil {
		t.Fatalf("peek slot: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var delivered []string
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			t.Fatalf("scan peeked change: %v", err)
		}
		delivered = append(delivered, string(data))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("peek slot: %v", err)
	}
	joined := strings.Join(delivered, "\n")
	if strings.Contains(joined, "zzbeforesnapzz") {
		t.Fatalf("PREMISE BROKEN: the slot re-delivered PRE-snapshot rows, so resuming CDC from the "+
			"snapshot's anchor would duplicate rows the copy already carried:\n%s", joined)
	}
	if !strings.Contains(joined, "zzaftersnapzz") {
		t.Fatalf("PREMISE BROKEN: the slot did NOT deliver the rows committed after the snapshot, so "+
			"resuming CDC from the anchor would LOSE them. Delivered %d messages:\n%s", len(delivered), joined)
	}
	t.Logf("premise 3: %d change messages delivered, post-snapshot only", len(delivered))
}

// TestVerifySnapshotAnchor_Verdicts pins every answer the source-side
// gate can give. The happy path is what licenses skipping a copy, so
// each refusal is pinned beside it — a gate that only ever says yes is
// the shape this project keeps paying for.
func TestVerifySnapshotAnchor_Verdicts(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()
	applyDDL(t, dsn, `CREATE TABLE anchor_verdicts (id BIGINT PRIMARY KEY, v TEXT);
		INSERT INTO anchor_verdicts (id, v) VALUES (1, 'a');`)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	eng := Engine{}
	const slot = "sluice_verdict_slot"

	// ABSENT, before anything exists: the caller must be able to tell
	// "no slot" from "moved", because they call for opposite behaviour.
	absentAnchor := `{"slot":"` + slot + `","lsn":"0/1000000"}`
	if _, err := eng.VerifySnapshotAnchor(ctx, dsn, slot, absentAnchor); !errors.Is(err, ir.ErrSnapshotAnchorAbsent) {
		t.Fatalf("verify against a nonexistent slot = %v; want an error satisfying ErrSnapshotAnchorAbsent "+
			"(the caller proceeds as if this surface did not exist; a plain error would make it REFUSE)", err)
	}

	stream, err := eng.OpenSnapshotStreamWithSlot(ctx, dsn, slot)
	if err != nil {
		t.Fatalf("open snapshot stream: %v", err)
	}
	anchor := stream.Position.Token
	if err := stream.ReleaseRows(); err != nil {
		t.Fatalf("release snapshot rows: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("close snapshot stream: %v", err)
	}
	waitForSlotInactive(t, dsn, slot, 30*time.Second)

	// ACTIVE, attached deliberately rather than observed opportunistically.
	// The first cut ran this arm only `if active` after the snapshot
	// open, which is a race: on a run where the slot read inactive the
	// arm silently did not execute and the test still passed. Here a
	// walsender is attached on purpose, so the arm either runs or the
	// attach fails loudly.
	requireActiveSlotRefused(ctx, t, eng, dsn, slot, anchor)
	waitForSlotInactive(t, dsn, slot, 30*time.Second)

	// HAPPY: unconsumed and inactive.
	pos, err := eng.VerifySnapshotAnchor(ctx, dsn, slot, anchor)
	if err != nil {
		t.Fatalf("verify against the untouched slot the anchor came from: %v", err)
	}
	if pos.Token != anchor {
		t.Errorf("verified position token = %q; want the recorded anchor verbatim %q — the token is what CDC "+
			"resumes from, so any rewriting here changes where the stream starts", pos.Token, anchor)
	}
	if pos.Engine != engineNamePostgres {
		t.Errorf("verified position engine = %q; want %q", pos.Engine, engineNamePostgres)
	}

	// WRONG SLOT: an anchor recorded for a different slot says nothing
	// about this one.
	otherAnchor := strings.Replace(anchor, `"slot":"`+slot+`"`, `"slot":"sluice_some_other_slot"`, 1)
	if otherAnchor == anchor {
		t.Fatal("test bug: the wrong-slot anchor was not rewritten")
	}
	if _, err := eng.VerifySnapshotAnchor(ctx, dsn, slot, otherAnchor); err == nil {
		t.Error("verify accepted an anchor recorded for a DIFFERENT slot")
	}

	// EMPTY: the no-evidence value must never verify.
	if _, err := eng.VerifySnapshotAnchor(ctx, dsn, slot, ""); err == nil {
		t.Error("verify accepted an EMPTY anchor; empty means no anchor was recorded, never 'the empty position'")
	}

	// MOVED: advance the slot and require the same anchor to be refused,
	// naming both positions.
	//
	// pg_replication_slot_advance, not "consume and hope": the first cut
	// of this pin consumed the slot and SKIPPED when
	// confirmed_flush_lsn had not moved — and a skip is green, so the
	// arm that grades the silent-loss case could quietly stop running.
	// Advance names the target LSN, so either the slot moves or the
	// call fails.
	recordedLSN := anchorLSN(t, ir.Position{Engine: engineNamePostgres, Token: anchor})
	applyDDL(t, dsn, `INSERT INTO anchor_verdicts (id, v) VALUES (2, 'b');`)
	advanceSlotToCurrentWAL(t, dsn, slot)
	_, movedTo, _ := slotLSNs(t, dsn, slot)
	if movedTo == recordedLSN {
		t.Fatalf("pg_replication_slot_advance left confirmed_flush_lsn at the recorded anchor (%s), so the "+
			"moved-anchor arm did not run; this pin must not pass without exercising it", movedTo)
	}
	_, err = eng.VerifySnapshotAnchor(ctx, dsn, slot, anchor)
	if err == nil {
		t.Fatal("verify returned OK for a slot that has been CONSUMED past the recorded anchor — resuming " +
			"there would silently skip every change in between")
	}
	if errors.Is(err, ir.ErrSnapshotAnchorAbsent) {
		t.Fatalf("a MOVED anchor was reported as ABSENT, which sends the caller down the proceed-as-usual "+
			"path instead of refusing: %v", err)
	}
	for _, want := range []string{recordedLSN, movedTo, "MOVED"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the moved-anchor refusal does not name %q — the operator needs both positions to know "+
				"what consumed their slot:\n%v", want, err)
		}
	}
}

// requireActiveSlotRefused attaches a real walsender to the slot with
// START_REPLICATION — the same command the CDC reader issues — and
// requires VerifySnapshotAnchor to refuse while it is attached, even
// though the slot's position is still exactly the recorded anchor.
//
// That is the point of the arm: "equal right now" proves nothing about
// a slot something else is consuming, so equality alone must not be
// enough to authorise skipping a copy.
func requireActiveSlotRefused(ctx context.Context, t *testing.T, eng Engine, dsn, slot, anchor string) {
	t.Helper()
	cfg, err := eng.parseDSN(dsn)
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	conn, err := openReplicationConn(ctx, cfg.dsn, cfg.appID)
	if err != nil {
		t.Fatalf("open replication conn: %v", err)
	}
	defer closeReplConnGraceful(conn)
	if err := pglogrepl.StartReplication(ctx, conn, slot, 0, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{"proto_version '1'", "publication_names '" + eng.publicationName() + "'"},
	}); err != nil {
		t.Fatalf("attach a walsender to %q: %v", slot, err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, _, active := slotLSNs(t, dsn, slot); active {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("slot %q never read active after START_REPLICATION; the active arm cannot be graded", slot)
		}
		time.Sleep(100 * time.Millisecond)
	}
	_, err = eng.VerifySnapshotAnchor(ctx, dsn, slot, anchor)
	if err == nil {
		t.Fatal("verify returned OK for an ACTIVE slot; a consumer attached to it may be advancing it, so " +
			"equality at this instant proves nothing")
	}
	if !strings.Contains(err.Error(), "ACTIVE") {
		t.Errorf("the active-slot refusal does not say the slot is active: %v", err)
	}
}

// advanceSlotToCurrentWAL moves the slot to the server's current WAL
// insert position — deterministically, unlike draining changes, which
// advances confirmed_flush_lsn only as far as the decoded stream
// happens to reach.
func advanceSlotToCurrentWAL(t *testing.T, dsn, slot string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(
		`SELECT pg_replication_slot_advance($1, pg_current_wal_insert_lsn())`, slot,
	); err != nil {
		t.Fatalf("advance slot %q: %v", slot, err)
	}
}

// consumeSlot advances the slot the way another consumer would.
func consumeSlot(t *testing.T, dsn, slot, publication string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(
		`SELECT 1 FROM pg_logical_slot_get_binary_changes($1, NULL, NULL, 'proto_version', '1', 'publication_names', $2)`,
		slot, publication,
	); err != nil {
		t.Fatalf("consume slot %q: %v", slot, err)
	}
}
