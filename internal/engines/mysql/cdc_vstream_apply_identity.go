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
//   - every row from a BEGIN that arrives while another transaction is open
//     until the last open one commits (interleaved groups break the premise
//     above — an UNVERIFIED PREMISE, see vstreamTxState.begin; WARNed once).
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
// ADR-0190 stamper and how many shard transactions are open (more than one
// only when vtgate interleaved them — see begin). Owned by the single
// dispatch goroutine.
type vstreamTxState struct {
	seq     applymarks.Sequencer
	depth   int
	warned  bool
	copying bool
}

// reset closes any transaction — a stream (re)start re-delivers from a
// boundary, so nothing opened before it continues.
func (s *vstreamTxState) reset() {
	s.depth = 0
	s.seq.End()
}

// begin opens the shard transaction ev starts, and reports whether the
// dispatcher emits its [ir.TxBegin].
//
// UNVERIFIED PREMISE: vtgate does not interleave shard transactions — each
// arrives as one contiguous BEGIN … COMMIT group. The multi-shard premise
// test (TestVStream_ApplyIdentity_StableAcrossMidStreamResume) saw no
// interleave across 15 transactions on two shards, which is evidence, not a
// guarantee across vtgate versions. So a BEGIN inside an open transaction is
// handled rather than assumed away:
//
//   - the rest of the stream until every open transaction commits carries NO
//     identity (the ordinal of neither is guaranteed to repeat on a
//     re-delivery), so nothing in it can ever be skipped — the pre-ADR
//     replay;
//   - the emission stays balanced: the nested BEGIN and all but the last
//     COMMIT are not emitted, so the applier sees ONE source transaction
//     spanning the interleaved groups and persists no position inside it —
//     the conservative bracket, where the alternative (a TxCommit while
//     another group is still open) would hand it a boundary that is not one.
//
// It WARNs rather than refuses, deliberately: the interleaved rows lose only
// their exactly-once optimisation and apply exactly as they did before
// ADR-0190, so no value is at risk, and a refusal would turn a shape the
// stream handled correctly into an outage.
func (s *vstreamTxState) begin(ctx context.Context, ev *binlogdata.VEvent, vgtid []shardGtid) bool {
	s.depth++
	if s.depth > 1 {
		if !s.warned {
			s.warned = true
			slog.WarnContext(ctx, "mysql/vstream: two shard transactions arrived interleaved; their changes carry no "+
				"ADR-0190 apply identity (they replay as before, never skipped), and they apply as one source "+
				"transaction",
				slog.String("keyspace", ev.GetKeyspace()), slog.String("shard", ev.GetShard()))
		}
		s.seq.Begin("")
		return false
	}
	if s.copying {
		s.seq.Begin("")
		return true
	}
	s.seq.Begin(vstreamTxIdentity(vgtid, ev.GetKeyspace(), ev.GetShard()))
	return true
}

// commit closes a transaction, and reports whether the dispatcher emits its
// [ir.TxCommit]: only the one that leaves no transaction open (see begin). A
// COMMIT with nothing open is emitted as it always was.
func (s *vstreamTxState) commit() bool {
	if s.depth > 1 {
		s.depth--
		return false
	}
	s.depth = 0
	s.seq.End()
	return true
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
