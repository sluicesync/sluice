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

// The binlog reader answers the reactive path's lineage question. The pin
// fails the build if the method is dropped rather than letting the
// reactive door silently revert to "not asked".
var _ ir.LineageVerifier = (*CDCReader)(nil)
