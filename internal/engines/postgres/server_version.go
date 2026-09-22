// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
)

// pgVersionFailoverSupport is the lowest server_version_num that
// accepts the FAILOVER option on the CREATE_REPLICATION_SLOT
// replication-protocol command. PG 17.0 = 170000. Anything below
// that number rejects FAILOVER as an unknown option, so sluice has
// to take the FAILOVER-less path on those servers and warn the
// operator that slot HA depends on Patroni / sync_replication_slots
// configuration outside the slot itself.
//
// server_version_num packs major/minor as MMmm00 (e.g. 17.2 →
// 170002), so a simple ">= 170000" check is the right gate even
// for future point releases.
const pgVersionFailoverSupport = 170000

// pgVersionUniqueNullsNotDistinct is the first server version whose
// pg_index carries `indnullsnotdistinct` (UNIQUE NULLS NOT DISTINCT —
// stored on the index side, not pg_constraint) — PG 15.
// pgVersionUniqueWithoutOverlaps is the first whose pg_constraint
// carries `conperiod` (temporal UNIQUE ... WITHOUT OVERLAPS) — PG 18.
// Below each, the catalog column does not exist and referencing it
// would 42703 the whole index read, so the schema reader substitutes a
// constant-false select expression there (the same version-gated
// catalog-read precedent as [pgVersionPublicationAttrs] /
// [pgVersionFailoverSupport]).
const (
	pgVersionUniqueNullsNotDistinct = 150000
	pgVersionUniqueWithoutOverlaps  = 180000
)

// pgVersionSchemaReaderFloor is the oldest server the schema reader can
// read at all — PG 12. [populateColumns] references
// `pg_collation.collisdeterministic` and `pg_attribute.attgenerated`
// unconditionally, and both arrived in 12; on an older server the
// column read 42703s before anything else runs. Recorded here because
// no earlier home named a floor: an earlier revision version-gated the
// attgenerated read to 12+ "for PG 10/11", which was dead code — the
// same query had already required 12 through collisdeterministic (the
// value-fidelity review's finding). The CDC lane's floor is separate
// (pgoutput, PG 10) and lower than this; `cutover` is documented at
// 12–18 in docs/production-readiness.md.
//
// pgVersionVirtualGeneratedColumns is the first server version that
// accepts `GENERATED ALWAYS AS (…) VIRTUAL` — PG 18, whose
// `attgenerated` then carries 'v' beside the 's' every earlier version
// wrote. Ground-truthed 2026-09-22 on 18.6 and 16.15: 16 rejects the
// VIRTUAL keyword with a syntax error, and 18 makes VIRTUAL the DEFAULT
// when neither keyword is written. Both directions are pinned on real
// servers by TestGeneratedColumns_StorageClass_PG18 and
// TestGeneratedColumns_StorageClass_DefaultImage.
const (
	pgVersionSchemaReaderFloor       = 120000
	pgVersionVirtualGeneratedColumns = 180000
)

// pgVersionConstraintParentID is the first server version whose
// pg_constraint carries `conparentid` — PG 11, which is also the first
// version that can create the internal per-partition FK clones the column
// identifies. sluice's floor is PG 10 (pgoutput CDC), and the foreign-key
// read runs against EVERY source — the Bug 100 partition refusal covers
// partitioned tables, not partition-free ones — so an unconditional
// reference would 42703 the whole FK read on a PG 10 server.
const pgVersionConstraintParentID = 110000

// serverVersionNum returns the Postgres server's numeric version
// (server_version_num), e.g. 170002 for PG 17.2 or 160006 for PG
// 16.6. Used by the slot-creation path to decide whether to opt in
// to the FAILOVER flag (PG 17+ only).
//
// The query goes through the regular *sql.DB pool, not the
// replication connection — replication-mode connections only
// accept replication-protocol commands, not normal SQL.
func serverVersionNum(ctx context.Context, db *sql.DB) (int, error) {
	var s string
	if err := db.QueryRowContext(ctx, "SHOW server_version_num").Scan(&s); err != nil {
		return 0, fmt.Errorf("postgres: read server_version_num: %w", err)
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("postgres: parse server_version_num %q: %w", s, err)
	}
	return n, nil
}
