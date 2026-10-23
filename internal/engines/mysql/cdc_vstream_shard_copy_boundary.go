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
// on another, two changes the tail replays like any cross-shard pair, which
// is what --allow-cross-shard-merge already accepts).
//
// A single-shard stream sees its per-scope and stream-wide COPY_COMPLETED in
// one batch, holds nothing, and keeps its position byte for byte.
//
// # What the copy still refuses
//
// On a shard that has NOT finished its copy, a copy row is an after image and
// nothing else (uvstreamer.sendEventsForRows: Before nil, After the row). A
// row change with a before image is a replayed change (vttablet's catch-up
// before the next table of a multi-table stream, or a resume's). An UPDATE
// that leaves every key cell as it was is still applied as an upsert of its
// after image, which is the row's value. A DELETE, a key-changing UPDATE or a
// change with no image cannot be applied in order against the rows the copy
// has buffered. It used to be skipped silently (a DELETE) or upserted from
// its after image (a key change, leaving the old row); it is now refused
// (VSTREAM-COPY-ROW-CHANGE-REFUSED) for the table it belongs to. The refusal
// surfaces through Err once that table's rows are
// read, so the keyspace-wide stream `schema add-table` opens, which streams
// every table but reads one, is refused for the table it adds and not for the
// tables it never reads.

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
// every primary-key column byte for byte as it was, whose after image is the
// row's value. Anything else is refused: a DELETE, an UPDATE that changes the
// key (the upsert would leave the old row), a change with no image, or an
// UPDATE on a table whose key the fields do not name.
func copyRowChangeRefusal(keyspace, shard, table string, rc *binlogdata.RowChange, fields []*query.Field) error {
	var shape string
	switch {
	case rc.GetBefore() == nil && rc.GetAfter() != nil:
		return nil
	case rc.GetBefore() != nil && rc.GetAfter() != nil:
		if copyRowKeyUnchanged(rc, fields) {
			return nil
		}
		shape = "an UPDATE that changes the primary key (or of a table whose key the stream does not name)"
	case rc.GetBefore() != nil:
		shape = "a DELETE (no after image)"
	default:
		shape = "a row change with neither image"
	}
	return fmt.Errorf("mysql/vstream: snapshot: %w: table %s.%s received %s on shard %s while that shard was still copying it. "+
		"A copy row carries only an after image, so this is a change vttablet replayed inside the copy (its catch-up before "+
		"the next table of a multi-table stream), which the copy cannot apply in order; copying on would leave the target "+
		"different from the source. A cold start copies each table in its own stream, which replays nothing: run "+
		"`sluice sync start --source-driver=... --source=... --target-driver=... --target=...` with the table in scope "+
		"(and `--restart-from-scratch` for an established stream)",
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
