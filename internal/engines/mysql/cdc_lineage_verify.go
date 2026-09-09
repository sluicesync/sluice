// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"

	"sluicesync.dev/sluice/internal/ir"
)

// VerifyLineage implements [ir.LineageVerifier] for the binlog reader
// (audit 2026-09-09 A0909-MYSQL-HIGH-1).
//
// It runs exactly the position verification StreamChanges runs — the
// file/pos @@server_uuid identity check, the GTID continuity check, the
// MariaDB anchor and domain checks — without opening a stream, and
// answers ONE question: is the source answering this DSN a different
// lineage from the one the position was captured from? Every other
// outcome (same lineage, a purged position, a probe that could not run,
// a position this reader cannot decode) is nil: this door exists only to
// stop the pipeline's REACTIVE recovery from dropping the target on a
// foreign source, never to widen what it refuses. The reactive recovery
// itself was measured doing that on a real replaced instance — the
// target's four correct rows replaced by the wrong database's one row,
// at exit 0 — because it restarts from scratch with no pre-flight in
// between.
func (r *CDCReader) VerifyLineage(ctx context.Context, from ir.Position) error {
	decoded, ok, err := decodeBinlogPos(from)
	if err != nil || !ok {
		return nil //nolint:nilerr // an undecodable position is not this door's question; the stream open reports it
	}
	if err := r.verifyPositionResumable(ctx, decoded); errors.Is(err, ir.ErrPositionForeignLineage) {
		return err
	}
	return nil //nolint:nilerr // every non-foreign outcome is deliberately nil: this door only narrows what the recovery may destroy
}

// VerifyLineage implements [ir.LineageVerifier] for the VStream reader.
//
// The binlog method above was the whole of the first cut, and that was a
// gate defending the defect: [Engine.OpenCDCReader] routes every
// PlanetScale/Vitess flavor to [openVStreamReader], whose reader is a
// DIFFERENT type — so the pipeline's reactive door took its "no such
// capability" branch, proceeded, and dropped the target's tables to
// re-copy from a foreign keyspace, which is the very outcome the
// sentinel exists to prevent. Worse, the roster test exempted the
// reactive VStream classification with a reason that named the reactive
// door as its cover. Found by the pre-tag value-fidelity review
// (2026-09-09).
//
// It asks the question the shard-scoped pre-flight already asks — is the
// resume position's lineage foreign to this shard — and reports ONLY
// that verdict. A purged position, replica lag, an errant GTID, a probe
// that could not run: all nil, because this door may only narrow what
// the automatic recovery destroys.
func (r *vstreamCDCReader) VerifyLineage(ctx context.Context, from ir.Position) error {
	decoded, err := r.resolveStartPosition(from)
	if err != nil || len(decoded) == 0 {
		return nil //nolint:nilerr // an undecodable position is not this door's question; the stream open reports it
	}
	if err := r.verifyVStreamPositionReachable(ctx, decoded); errors.Is(err, ir.ErrPositionForeignLineage) {
		return err
	}
	return nil //nolint:nilerr // every non-foreign outcome is deliberately nil; see the doc comment
}

// Both reader types [Engine.OpenCDCReader] can return answer the reactive
// path's lineage question. The pins fail the BUILD if either method is
// dropped; TestEveryCDCReaderAnswersTheLineageQuestion fails the build if
// a THIRD reader type appears without one.
var (
	_ ir.LineageVerifier = (*CDCReader)(nil)
	_ ir.LineageVerifier = (*vstreamCDCReader)(nil)
)
