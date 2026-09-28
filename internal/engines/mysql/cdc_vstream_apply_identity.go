// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"log/slog"

	"vitess.io/vitess/go/vt/proto/binlogdata"

	"sluicesync.dev/sluice/internal/applymarks"
	"sluicesync.dev/sluice/internal/ir"
)

// # ADR-0190 change identity on the VStream readers (phase 4)
//
// vtgate delivers each shard's transactions as contiguous BEGIN … ROW …
// VGTID … COMMIT groups, merged across shards. The rows carry the MERGED
// pre-transaction VGTID, which is not a transaction identity: which other
// shards' components it holds depends on how the shards interleaved on this
// particular delivery. A single shard's component is: vtgate resumes every
// shard from its own component, so a shard transaction's PRE-transaction
// component is the same on every delivery. So a transaction is named by its
// keyspace, its shard and that shard's component as it stood at BEGIN —
// read from the reader's current VGTID, which only a VGTID event (one per
// shard transaction, at its end) advances.
//
// The ordinal counts the transaction's ROWS per table, in the order the
// reader emits them, after its scope gate (which drops a table whole). That
// a resume re-delivers a shard transaction's ROW events in the same order —
// ADR-0190 §1's UNVERIFIED PREMISE — is pinned against a real multi-shard
// vttestserver by TestVStream_ApplyIdentity_StableAcrossMidStreamResume.
//
// What gets NO identity (so it is never skipped):
//
//   - rows while vtgate runs a COPY (a start position with an empty GTID or
//     a TablePKs cursor, until the stream-wide COPY_COMPLETED): a copy
//     group's shard component does not advance per group, so every group
//     would share one "transaction" — and the rows are a table scan, not a
//     source transaction;
//   - a shard component that is not a GTID set (empty, "current");
//   - rows of a transaction whose BEGIN arrived while another was open
//     (interleaved groups break the premise above; WARNed once).
//
// Both VStream dispatchers — the tail reader and the snapshot stream's
// post-COPY pump — go through vstreamTxState, so an original delivery on the
// cold-start stream and its re-delivery on the tail reader after a restart
// name every change the same way.

// vstreamTxIdentity names the shard transaction opening at BEGIN from the
// current merged VGTID, or "" when the shard's component is not a GTID set.
func vstreamTxIdentity(vgtid []shardGtid, keyspace, shard string) string {
	for _, sg := range vgtid {
		if sg.Shard != shard || (keyspace != "" && sg.Keyspace != keyspace) {
			continue
		}
		if sg.Gtid == "" || sg.Gtid == "current" || len(sg.TablePKs) > 0 {
			return ""
		}
		return "vstream:" + sg.Keyspace + "/" + sg.Shard + ":" + sg.Gtid
	}
	return ""
}

// vstreamTxState is one VStream dispatcher's transaction bookkeeping: the
// ADR-0190 stamper and whether a shard transaction is open. Owned by the
// single dispatch goroutine.
type vstreamTxState struct {
	seq     applymarks.Sequencer
	open    bool
	warned  bool
	copying bool
}

// reset closes any transaction — a stream (re)start re-delivers from a
// boundary, so nothing opened before it continues.
func (s *vstreamTxState) reset() {
	s.open = false
	s.seq.End()
}

// begin opens the shard transaction ev starts.
func (s *vstreamTxState) begin(ctx context.Context, ev *binlogdata.VEvent, vgtid []shardGtid) {
	if s.open {
		// A BEGIN inside an open transaction: vtgate interleaved two shard
		// transactions, and the ordinal of neither is then guaranteed to
		// repeat on a re-delivery. Neither gets an identity.
		if !s.warned {
			s.warned = true
			slog.WarnContext(ctx, "mysql/vstream: two shard transactions arrived interleaved; their changes carry no "+
				"ADR-0190 apply identity (they replay as before, never skipped)",
				slog.String("keyspace", ev.GetKeyspace()), slog.String("shard", ev.GetShard()))
		}
		s.seq.Begin("")
		return
	}
	s.open = true
	if s.copying {
		s.seq.Begin("")
		return
	}
	s.seq.Begin(vstreamTxIdentity(vgtid, ev.GetKeyspace(), ev.GetShard()))
}

// commit closes the open transaction.
func (s *vstreamTxState) commit() {
	s.open = false
	s.seq.End()
}

// next stamps the transaction's next row change to table.
func (s *vstreamTxState) next(table string) ir.ApplyID { return s.seq.Next(table) }

// copyingFrom reports whether a stream opened at start runs a COPY first.
func copyingFrom(start []shardGtid) bool {
	for _, sg := range start {
		if sg.Gtid == "" || len(sg.TablePKs) > 0 {
			return true
		}
	}
	return false
}

// StampsApplyIdentity implements [ir.ApplyIdentityProvider].
func (r *vstreamCDCReader) StampsApplyIdentity() bool { return true }

var _ ir.ApplyIdentityProvider = (*vstreamCDCReader)(nil)
