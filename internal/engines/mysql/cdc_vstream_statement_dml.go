// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"vitess.io/vitess/go/vt/proto/binlogdata"

	"sluicesync.dev/sluice/internal/ir"
)

// The VStream lane's half of the STATEMENT-DML belt (audit 2026-09-01
// SLM-3, sibling sweep).
//
// The capture-completeness matrix recorded this lane as "absorbed
// upstream: the vendored vstreamer kills the stream on statement-format
// DML". It does not. vstreamer.go's QueryEvent switch has explicit arms
// for the four row-DML categories — `case sqlparser.StmtInsert:` emits a
// VEvent of type INSERT with the SQL in `Dml` (likewise REPLACE, UPDATE,
// DELETE; vitess.io/vitess@v0.24.2 vstreamer.go:613-656, gated only on
// the statement's database matching the tablet's) — and its "unexpected
// statement type" default arm is reached by OTHER categories. Both of
// sluice's hand-mirrored dispatchers then dropped those four types at
// their default arm, with a comment saying they "don't get" them: the
// binlog lane's silent drop, reproduced on this one by a false premise.
//
// The population is narrower than the binlog lane's — a session
// override on a tablet's mysqld needs a direct connection past vtgate,
// and PlanetScale pins ROW platform-wide — but the shape is identical,
// so the refusal is identical: same code, same verb, same withheld
// values, with the VGTID as the coordinate.
//
// Statement-format LOAD DATA on a tablet is the one exempt shape here,
// and it is exempt UPSTREAM: vstreamer's `IsQuery()` is QUERY_EVENT-only,
// so an EXECUTE_LOAD_QUERY event produces no VEvent at all and sluice
// never sees it. Stated, not implied, in TestVStreamStatementDML.

// vstreamStatementDMLVerbs maps the VEvent types the vendored vstreamer
// emits for statement-format DML to the belt's verb.
var vstreamStatementDMLVerbs = map[binlogdata.VEventType]string{
	binlogdata.VEventType_INSERT:  "INSERT",
	binlogdata.VEventType_REPLACE: "REPLACE",
	binlogdata.VEventType_UPDATE:  "UPDATE",
	binlogdata.VEventType_DELETE:  "DELETE",
}

// vstreamStatementDMLError builds the belt's refusal for a statement-DML
// VEvent: the event's keyspace stands in for the session database, the
// last observed VGTID for the binlog coordinate, and the SQL in `Dml` is
// redacted through the same lead sanitizer as the binlog lane.
func vstreamStatementDMLError(ev *binlogdata.VEvent, pos ir.Position) error {
	locator := "no VGTID observed yet on this stream"
	if pos.Token != "" {
		locator = "vgtid " + pos.Token
	}
	return statementDMLError(vstreamStatementDMLVerbs[ev.GetType()], ev.GetKeyspace(), locator, ev.GetDml())
}

// The two dispatchers' arms — vstreamCDCReader.statementDMLRefusal and
// vstreamSnapshotStream.statementDMLRefusal — live in their own files,
// where the mirror-parity gate (vstream_mirror_parity_test.go) looks
// for them.
