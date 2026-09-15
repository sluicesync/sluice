// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// nekiShardKeyMissingCode is the SQLSTATE a PlanetScale Neki router returns
// for an INSERT into a sharded table that does not carry the shard-key
// column:
//
//	ERROR: shard-key column "tenant_id" of primary index 0 is required
//	but missing from INSERT (SQLSTATE NK306)
//
// Until 2026-09-14 this code appeared in the repository only as prose — the
// engine graded NK013, NK205 and NK213 and nothing else in the NK3xx range —
// so a refusal the suite had measured twice still carried no verdict: the
// same class as the unclassified NK205 that killed a full-volume copy in
// v0.152.1. Terminal, because the statement's SHAPE is what is refused and
// the same shape is refused on retry.
//
// SIBLING SWEEP — the paths that can receive NK306, and which reach this
// annotation:
//
//   - CDC apply of a USER table's INSERT (change_applier*.go): REACHED via
//     [classifyApplierError].
//   - The CDC POSITION write, serial and pipelined lanes (control_table.go
//     WritePosition, change_applier_pipelined.go): REACHED — annotated at
//     the wrap, because neither routes through the classifier. This is the
//     write the NK306 bisect measured the serial lane dying on.
//   - The migrate-state breadcrumb write: NOT annotated here. It already
//     surfaces as SLUICE-E-MIGRATE-PROGRESS-UNRECORDABLE, whose doc row
//     names NK306 and the placement remedy; a second code on the same
//     failure would compete with a more specific one.
//   - The bulk-copy `COPY` writer: NOT reached. It does not route through
//     the classifier (the same exemption neki_blocked_table.go records for
//     NK213), and a COPY without the shard key is a schema-level mismatch
//     the preflight's topology check is the right home for.
//
// The control-table case is expected to be rare after the placement in
// neki_control_placement.go: sluice places its own tables in the
// authoritative shard group at creation, and refuses with
// SLUICE-E-TARGET-CONTROL-TABLE-PLACEMENT when it cannot. What remains for
// this code is the path where placement was skipped because the topology
// could not be READ (a warning, not a refusal, so an unsharded database
// keeps working) and the database turned out to be sharded after all.
const nekiShardKeyMissingCode = "NK306"

// isNekiShardKeyMissing reports whether err is Neki's missing-shard-key
// refusal.
func isNekiShardKeyMissing(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == nekiShardKeyMissingCode
}

// annotateNekiShardKeyMissing wraps Neki's missing-shard-key refusal in the
// coded refusal an operator can act on. Returns err unchanged for every other
// shape, so it is safe to call on any error.
func annotateNekiShardKeyMissing(err error) error {
	if err == nil || !isNekiShardKeyMissing(err) {
		return err
	}
	// Idempotent: the position-write sites annotate at the wrap and the
	// same error may then pass through the classifier.
	var coded *sluicecode.CodedError
	if errors.As(err, &coded) && coded.Code == sluicecode.CodeTargetShardKeyMissing {
		return err
	}
	return sluicecode.Wrap(
		sluicecode.CodeTargetShardKeyMissing,
		"if the table named is one of sluice's own control tables (sluice_cdc_state, sluice_migrate_state, …), "+
			"place it in the authoritative shard group — see SLUICE-E-TARGET-CONTROL-TABLE-PLACEMENT — or grant "+
			"sluice's role SELECT on __neki.get_data_topology() so it can plan the placement itself; for a user "+
			"table, give the target a shard-key column the source rows actually carry, or exclude the table "+
			"with --exclude-table",
		fmt.Errorf("a PlanetScale Neki target refused an INSERT that does not carry the table's shard-key column "+
			"— the router routes every row by that column, so the statement's shape is refused and would be "+
			"refused identically on retry: %w", err),
	)
}
