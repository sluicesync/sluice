// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import "sluicesync.dev/sluice/internal/ir"

// PGEffectiveIntegerWidth returns the signed width a Postgres target
// needs in order to hold i's full range: identity for a signed column,
// one rank wider for an unsigned one (Postgres has no unsigned integer
// types).
//
// The 64-bit unsigned case widens to 64, NOT to a wider numeric — the
// (2^63, 2^64) range loss is Bug 11's deliberate, documented policy,
// surfaced loudly by [UnsignedBigintNoticeError] rather than avoided
// here. See internal/engines/postgres.emitIntegerType for the full
// FK-compatibility argument.
func PGEffectiveIntegerWidth(i ir.Integer) int8 {
	if !i.Unsigned {
		return i.Width
	}
	switch i.Width {
	case 8:
		return 16
	case 16, 24:
		return 32
	case 32, 64:
		return 64
	}
	return i.Width
}

// PGStorageIntegerWidth returns the width a Postgres CATALOG reads back
// for a column the Postgres writer emits from i — the unsigned widening
// of [PGEffectiveIntegerWidth] followed by the collapse onto the three
// widths Postgres actually has (SMALLINT / INTEGER / BIGINT). So a MySQL
// `TINYINT` lands as SMALLINT and reads back as `Int16`, and a
// `MEDIUMINT UNSIGNED` lands as INTEGER and reads back as `Int32`.
//
// # Why this lives in translate rather than in the engine
//
// Two callers need the SAME answer and they sit on opposite sides of the
// wire. internal/engines/postgres.emitIntegerType names the type it
// CREATES from it; [retargetMySQLtoPGShapeCompare] predicts the type a
// diff will FIND from it. Bug 234's index-NAME rule moved into this
// package for exactly this reason — the name sluice creates and the name
// it expects have to be one statement or they drift — and duplicated
// two-stage integer arithmetic is a far easier thing to drift than a
// string prefix. `internal/engines/postgres` imports this package (the
// reverse would cycle), so this is the only place both can reach.
//
// # The premise, and what binds it
//
// "A PG catalog reads SMALLINT back as Int16" is a claim about
// internal/engines/postgres.translateScalarType, which this package
// cannot see. It is not asserted here: TestSchemaDiffAfterMigrate_
// MySQLToPostgres_TypeFamilyMatrix (internal/pipeline) migrates every
// integer width × signedness onto a REAL Postgres and compares this
// function's prediction against that server's own read-back, so a
// change on either side fails rather than diverges quietly.
func PGStorageIntegerWidth(i ir.Integer) int8 {
	switch PGEffectiveIntegerWidth(i) {
	case 8, 16:
		return 16
	case 24, 32:
		return 32
	}
	// 64, and any width no reader produces — the pre-existing fallback
	// (widest wins) rather than a refusal, since a narrower guess is the
	// only direction that can truncate.
	return 64
}
