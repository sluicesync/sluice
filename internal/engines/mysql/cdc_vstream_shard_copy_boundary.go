// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"
)

// Where each shard's copy ended (VSTREAM-SHARD-SKEW-COPY-CHANGES-DROPPED,
// Bug 331).
//
// # The harm
//
// A copy VStream spans every shard of the keyspace. Each shard's vttablet
// copies the stream's tables under its own snapshot, sends a per-scope
// COPY_COMPLETED (keyspace and shard set), and then streams that shard's
// binlog from the snapshot GTID (uvstreamer.Stream: copy, allCopyComplete,
// then a replicating vstreamer from uvs.pos). vtgate forwards those binlog
// events without waiting for the stream-wide COPY_COMPLETED, which it sends
// only when every shard has finished (vstream_manager.isCopyFullyCompleted).
// Its skew alignment (MinimizeSkew) paces them against the slower shard's
// clock, so of the changes a finished shard commits while a slower shard is
// still copying, some arrive INSIDE the copy phase and the rest after it
// (measured on vttestserver: about one transaction per heartbeat).
//
// Through v0.163.7 the copy dispatchers sent those ROW events to the copy's
// row buffer, which decodes after images only: an INSERT or UPDATE landed as
// an upsert, a DELETE (no after image) was skipped as "malformed", and an
// UPDATE that changed the primary key left the old row behind. The table's
// resume position was the VGTID at the stream-wide COPY_COMPLETED, already
// past those changes, so the CDC tail never replayed them. Measured by the
// v0.163.7 regression cycle: 52 of 52 deletes on the early shard kept on the
// target, at exit 0, on every release back to v0.100.0; `backup full`
// archived the same rows twice and its restore failed on a duplicate key.
//
// # The rule
//
// A shard's copy of the stream's tables ends at its per-scope
// COPY_COMPLETED. From then on its ROW events are binlog changes, not copy
// rows, so the copy does not buffer them (it holds them back, counted), and
// the table's snapshot position for that shard is the GTID the shard had
// reached when its COPY_COMPLETED arrived, not the one at the stream-wide
// COPY_COMPLETED. The CDC tail (or a backup's incremental) resumes from that
// earlier GTID and replays every held change through the ordinary change
// decoder, with before and after images: deletes delete, a key change moves
// the row, and per-key order is the binlog's.
//
// Why the GTID at the shard's COPY_COMPLETED is exactly its snapshot: vtgate
// turns every GTID and LASTPK event into a VGTID carrying the whole
// per-shard position (vstream_manager.sendEventsLocked), and it delivers one
// shard's events in that shard's order. The shard's copy announces its
// snapshot GTID in the GTID event its FIELD batch carries
// (uvstreamer.sendFieldEvent → setPosition), every later VGTID of its copy
// repeats it with a moving lastpk, the closing LASTPK (Completed) removes the
// cursor, and only after the per-scope COPY_COMPLETED does its replicating
// vstreamer move the GTID on. [shardCopyBoundary.snapshotPosition] refuses if
// the recorded GTID is not contained in the final one, so a premise that
// stops holding fails loudly instead of resuming past a change.
//
// The held changes are not applied twice and none is lost: the copy writes
// none of them, the resume replays all of them (and every change that
// arrives after the stream-wide COPY_COMPLETED, as before).
// TestStreamer_VStreamShardSkewCopy (internal/pipeline) fails if either half
// breaks: it requires the target to match the source by key and value, and
// the held count to equal the changes each finished shard made. A row a later shard copies
// cannot be touched by an earlier shard's held change, because a row lives on
// one shard (vtgate refuses an UPDATE of a primary-vindex column, VT12001, so
// a row changes shard only by an application DELETE on one shard and INSERT
// on another). That holds per KEY only while the table's primary vindex is
// within its primary key: otherwise the DELETE and the INSERT carry the same
// key from two shards, nothing orders them, and the moved row can be lost
// (Bugs 337/338). The pipeline's CROSS-SHARD-VINDEX-NOT-IN-PK door refuses
// that shape under --allow-cross-shard-merge unless the operator asserts
// --assert-no-cross-shard-row-moves.
//
// A single-shard stream sees its per-scope and stream-wide COPY_COMPLETED in
// one batch, holds nothing, and keeps its position byte for byte.
//
// # What the copy still refuses
//
// On a shard that has NOT finished its copy, a copy row is an after image and
// nothing else (uvstreamer.sendEventsForRows: Before nil, After the row). A
// row change with a before image is a replayed change, from one of two
// causes: vttablet's catch-up before the next table of a multi-table stream
// (deterministic), or vttablet's catch-up after vtgate restarted the shard's
// tablet stream from its copy cursor on a retriable error, which reaches
// every copy path, the default per-table copy included (transient;
// VSTREAM-VTGATE-SHARD-RETRY-CATCHUP-DELETES, see [copyRowChangeRefusal] for
// how each is classified). An UPDATE
// that leaves every key cell as it was is still applied as an upsert of its
// after image, which is the row's value. A DELETE, a key-changing UPDATE or a
// change with no image cannot be applied in order against the rows the copy
// has buffered. It used to be skipped silently (a DELETE) or upserted from
// its after image (a key change, leaving the old row); it is now refused
// (VSTREAM-COPY-ROW-CHANGE-REFUSED) for the table it belongs to. The refusal
// surfaces through Err once that table's rows are
// read, so a keyspace-wide stream, which streams every table, is refused
// only for the tables its caller reads.
//
// # Deferring instead of refusing
//
// The drained `schema add-table` (the only add-table mode a VStream source
// allows since NO-DRAIN-VSTREAM-SOURCE) opens that keyspace-wide stream, so
// any delete or key change on the added table during its copy is replayed by
// the catch-up before the next table. Its stream was stopped before the add
// and resumes from its pre-add position, replaying every change made during
// the copy in order. So add-table calls DeferCopyReplaysToStream
// ([ir.CopyReplayDeferrer]) and the copy leaves those changes unapplied for
// that stream, counted and logged, instead of failing the add. Measured
// exact with deletes by TestStreamer_VStreamAddTableKeyspaceWideCopy's
// Drained cells (v0.163.8, which dropped the deletes the same way and was
// exact for the same reason) and with deletes and key changes by
// TestStreamer_VStreamShardSkewCopy/AddTable. The premise is the drain, and
// it inherits DRAINED-ADD-TABLE-RUNNING-STREAM-UNCHECKED: an add run against
// a stream that was never stopped loses those changes, exactly as it loses
// every other change made during the copy.

// copyRowChangeRefusedMarker names the refusal of a copy-phase row change the
// copy cannot apply.
const copyRowChangeRefusedMarker = "VSTREAM-COPY-ROW-CHANGE-REFUSED"

// shardCopyBoundary is one copy stream's record of where each shard's copy
// ended. The zero value is ready; reset it for every new copy stream.
// Keyed by shard name: a copy stream reads one keyspace.
type shardCopyBoundary struct {
	// finishedAt maps a shard to the GTID it had reached when its per-scope
	// COPY_COMPLETED arrived.
	finishedAt map[string]string
	// held counts, per shard, the row changes received after the shard
	// finished, which the copy left to the CDC tail.
	held map[string]int
}

// markFinished records shard's per-scope COPY_COMPLETED against the
// stream's current per-shard position.
func (b *shardCopyBoundary) markFinished(shard string, cur []shardGtid) {
	if b.finishedAt == nil {
		b.finishedAt = map[string]string{}
	}
	gtid := ""
	for _, sg := range cur {
		if sg.Shard == shard {
			gtid = sg.Gtid
			break
		}
	}
	b.finishedAt[shard] = gtid
}

// finished reports whether shard has finished its copy on this stream.
func (b *shardCopyBoundary) finished(shard string) bool {
	_, ok := b.finishedAt[shard]
	return ok
}

// hold counts n row changes of shard that the copy leaves to the CDC tail.
func (b *shardCopyBoundary) hold(shard string, n int) {
	if b.held == nil {
		b.held = map[string]int{}
	}
	b.held[shard] += n
}

// snapshotPosition is the stream's snapshot position: cur (the position at
// the stream-wide COPY_COMPLETED) with every finished shard's GTID replaced
// by the GTID it had reached when it finished, and its copy cursor dropped.
// It reports whether any shard moved, in which case the CDC tail must not
// continue the copy stream in place (it is past the held changes).
//
// It refuses, rather than resume past a held change, when a shard that
// holds changes recorded no GTID at its finish, or when the recorded GTID
// is not contained in the shard's final one.
func (b *shardCopyBoundary) snapshotPosition(cur []shardGtid) ([]shardGtid, bool, error) {
	out := make([]shardGtid, len(cur))
	copy(out, cur)
	moved := false
	for i, sg := range out {
		at, ok := b.finishedAt[sg.Shard]
		if !ok || at == sg.Gtid {
			continue
		}
		if at == "" {
			if b.held[sg.Shard] > 0 {
				return nil, false, fmt.Errorf("mysql/vstream: snapshot: shard %s finished its copy before any position was received for it, "+
					"and %d later change(s) on it were left to the CDC tail; refusing a resume position that would skip them "+
					"(VSTREAM-SHARD-SKEW-COPY-CHANGES-DROPPED). Re-run the cold start", sg.Shard, b.held[sg.Shard])
			}
			continue
		}
		contained, err := gtidAtOrAfter(stripGTIDFlavor(sg.Gtid), stripGTIDFlavor(at))
		if err != nil {
			return nil, false, fmt.Errorf("mysql/vstream: snapshot: shard %s: compare its copy-finish GTID %q with %q: %w", sg.Shard, at, sg.Gtid, err)
		}
		if !contained {
			return nil, false, fmt.Errorf("mysql/vstream: snapshot: shard %s finished its copy at GTID %q, which the stream's final position %q "+
				"does not contain; refusing to guess a resume position (VSTREAM-SHARD-SKEW-COPY-CHANGES-DROPPED). Re-run the cold start",
				sg.Shard, at, sg.Gtid)
		}
		out[i].Gtid = at
		out[i].TablePKs = nil
		moved = true
	}
	return out, moved, nil
}

// logHeld records, once per copy stream, the changes left to the CDC tail.
func (b *shardCopyBoundary) logHeld(keyspace string, tables []string) {
	if len(b.held) == 0 {
		return
	}
	shards := make([]string, 0, len(b.held))
	for s := range b.held {
		shards = append(shards, s)
	}
	sort.Strings(shards)
	for _, s := range shards {
		slog.Info("mysql/vstream: snapshot: shard finished its copy before the stream did; its later changes are replayed from its copy position",
			slog.String("keyspace", keyspace),
			slog.String("shard", s),
			slog.String("tables", strings.Join(tables, ",")),
			slog.Int("changes", b.held[s]),
			slog.String("resume_gtid", b.finishedAt[s]))
	}
}

// logShardCopyFinished records one shard's per-scope COPY_COMPLETED.
func logShardCopyFinished(ev *binlogdata.VEvent, tables []string) {
	slog.Debug("mysql/vstream: snapshot: shard finished its copy",
		slog.String("keyspace", ev.GetKeyspace()),
		slog.String("shard", ev.GetShard()),
		slog.String("tables", strings.Join(tables, ",")))
}

// errCopyRowChangeRefused is the sentinel every copy-row refusal wraps.
var errCopyRowChangeRefused = errors.New(copyRowChangeRefusedMarker)

// copyRowChangeRefusal judges one row change received on a shard still
// copying the table. It returns nil for what the copy can apply by upsert: a
// copy row (an after image and no before image), or an UPDATE that leaves
// every primary-key column byte for byte as it was AND carries a full after
// image (no column omitted under binlog_row_image=NOBLOB, no partial-JSON
// diff; [refuseVStreamPartialRowImage], the check the CDC dispatchers make),
// whose after image is then the row's value. Anything else is refused: a
// DELETE, an UPDATE with a partial image (the upsert would write NULL or a
// JSON diff over the real value), an UPDATE that changes the key (the upsert
// would leave the old row), a change with no image, or an UPDATE on a table
// whose key the fields do not name. The partial-image refusal is classified
// like the rest: retriable on a single-table stream, and on the drained
// add-table deferred to the stopped stream, whose CDC tail makes the same
// partial-image check and refuses loudly when it replays the change, so a
// deferral cannot apply the partial image either.
//
// singleTableStream says whether the stream copies exactly one table (the
// default per-table copy, the concurrent copy, a one-table scope). There the
// only replay inside the copy is vtgate's: it restarts a shard's tablet
// stream on a retriable error (vstream_manager.streamFromTablet, up to three
// times: a reparent, a vttablet restart, a health-check stream end) from the
// shard's last position and copy cursor, and vttablet catches up before it
// copies on (copy.go catchupAndCopy; shouldSendEventForTable sends every
// event of a table with a cursor). That is a transient fault, so the refusal
// is retriable and takes the same route as a mid-copy Recv fault
// (VSTREAM-COPY-RECONNECT-STALE-ROWS): the Streamer's retry restarts the cold
// start from a new snapshot (ADR-0109 §B). On a stream that copies several
// tables in one VStream (the keyspace-wide open `schema add-table` uses)
// vttablet also replays deterministically before every later table, so a
// retry would meet it again: terminal. Neither verdict applies when the
// caller deferred replays ([ir.CopyReplayDeferrer]; the drained `schema
// add-table`, whose stopped stream replays from before the copy): the
// change is then left unapplied for that stream ([refuseTableLocked]). The
// terminal verdict is therefore reached only through a table-less snapshot
// open whose caller does not defer: today the cold start's fallback for an
// empty table list (openSnapshotStreamScoped), where refusing is right
// because its change stream starts at the copy's own position.
// TestCopyReplayDeferralRoster (internal/pipeline) holds every production
// caller of the table-less opener to deferring or to a listed exemption.
func copyRowChangeRefusal(keyspace, shard, table string, rc *binlogdata.RowChange, fields []*query.Field, singleTableStream bool) error {
	var shape string
	switch {
	case rc.GetBefore() == nil && rc.GetAfter() != nil:
		return nil
	case rc.GetBefore() != nil && rc.GetAfter() != nil:
		// The after image is the row's value only when it is FULL: under
		// binlog_row_image=NOBLOB an unchanged BLOB/TEXT column arrives as a
		// NULL cell, and under PARTIAL_JSON a JSON column carries a diff
		// expression. Upserting either writes NULL or the diff text over the
		// real value (the item-74 class the CDC dispatchers refuse with the
		// same check), so a partial image is never "the row's value".
		if perr := refuseVStreamPartialRowImage(rc, fields, keyspace, table); perr != nil {
			shape = "an UPDATE with a partial row image (" + perr.Error() + ")"
			break
		}
		if copyRowKeyUnchanged(rc, fields) {
			return nil
		}
		shape = "an UPDATE that changes the primary key (or of a table whose key the stream does not name)"
	case rc.GetBefore() != nil:
		shape = "a DELETE (no after image)"
	default:
		shape = "a row change with neither image"
	}
	if singleTableStream {
		return &retriableMySQLError{err: fmt.Errorf("mysql/vstream: snapshot: %w: table %s.%s received %s on shard %s while that "+
			"shard was still copying it. A copy row carries only an after image, so this is a change Vitess replayed inside the "+
			"copy: vtgate restarted that shard's stream mid-copy (a reparent, a vttablet restart, maintenance) and vttablet "+
			"caught up before copying on. The copy cannot apply it in order against the rows it has buffered, so this copy "+
			"stops and is retried from a new snapshot: a sync cold start restarts it automatically (a first cold start clears "+
			"the in-scope target tables first), and any other command can simply be run again",
			errCopyRowChangeRefused, keyspace, table, shape, shard)}
	}
	return fmt.Errorf("mysql/vstream: snapshot: %w: table %s.%s received %s on shard %s while that shard was still copying it. "+
		"A copy row carries only an after image, so this is a change Vitess replayed inside the copy: this stream copies "+
		"several tables in one VStream, and before each later "+
		"table vttablet replays the changes made to the tables it already copied (or vtgate restarted the shard's stream). "+
		"The copy cannot apply it in order, and a retry would meet the same replay, so this copy stops. Bring the table in "+
		"with a re-copy instead, which copies each table in its own stream: with the stream stopped, run "+
		"`sluice sync start --source-driver=... --source=... --target-driver=... --target=... --stream-id=...` with "+
		"--include-table naming every table of the stream and this one, and --restart-from-scratch",
		errCopyRowChangeRefused, keyspace, table, shape, shard)
}

// copyRowKeyUnchanged reports whether an UPDATE leaves every primary-key
// column's raw cell unchanged. A table whose fields flag no key column is
// reported changed (refused): without a key nothing proves the upsert hits
// the same row.
func copyRowKeyUnchanged(rc *binlogdata.RowChange, fields []*query.Field) bool {
	before, okB := vstreamRawCells(rc.GetBefore(), len(fields))
	after, okA := vstreamRawCells(rc.GetAfter(), len(fields))
	if !okB || !okA {
		return false
	}
	keyed := false
	for i, f := range fields {
		if f.GetFlags()&uint32(query.MySqlFlag_PRI_KEY_FLAG) == 0 {
			continue
		}
		keyed = true
		if (before[i] == nil) != (after[i] == nil) || !bytes.Equal(before[i], after[i]) {
			return false
		}
	}
	return keyed
}

// vstreamRawCells splits a row into its n raw cells (nil for SQL NULL),
// reporting false for a row whose lengths do not describe n cells inside
// its values.
func vstreamRawCells(row *query.Row, n int) ([][]byte, bool) {
	lengths, values := row.GetLengths(), row.GetValues()
	if len(lengths) != n {
		return nil, false
	}
	out := make([][]byte, n)
	offset := 0
	for i, l := range lengths {
		if l < 0 {
			continue
		}
		if offset+int(l) > len(values) {
			return nil, false
		}
		out[i] = values[offset : offset+int(l) : offset+int(l)]
		if out[i] == nil {
			out[i] = []byte{}
		}
		offset += int(l)
	}
	return out, true
}
