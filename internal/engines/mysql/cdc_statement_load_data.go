// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"fmt"

	"github.com/go-mysql-org/go-mysql/replication"
)

// Audit 2026-09-01 SLM-3 shape 1 — statement-format LOAD DATA.
//
// Under a session binlog_format=STATEMENT override, LOAD DATA is not a
// QueryEvent: the server writes the file's bytes as one or more
// BEGIN_LOAD_QUERY events and then an EXECUTE_LOAD_QUERY event that
// carries the session database and the statement text (a Query event
// with a 13-byte post-header extension). Neither reached any arm of
// [CDCReader.dispatch] — the default arm returned nil — so the load's
// rows were dropped with the stream alive (observed on 8.0.46: two rows
// loaded, both absent from the target, no log line).
//
// go-mysql decodes EXECUTE_LOAD_QUERY's fixed post-header only
// ([replication.ExecuteLoadQueryEvent] has no Schema or Query field),
// so the variable tail is decoded here from the event's raw bytes,
// following the layout the server writes and the parser leaves intact:
//
//	19  event header
//	26  post-header: thread_id(4) exec_time(4) schema_len(1)
//	    error_code(2) status_vars_len(2) file_id(4) fn_pos_start(4)
//	    fn_pos_end(4) dup_handling(1)
//	 n  status vars (status_vars_len)
//	 m  schema (schema_len), then one NUL
//	 …  statement text, to the end of the event
//	 4  CRC32 trailer, present when the current FORMAT_DESCRIPTION
//	    declares binlog_checksum=CRC32 — absent on the inner events of a
//	    TRANSACTION_PAYLOAD (nested events carry no trailer)
//
// The trailer length is the one thing the raw bytes cannot state about
// themselves; the reader learns it from the FORMAT_DESCRIPTION arm and
// forgets it inside a compressed payload, which is exactly what the
// parser does for the fields it decodes.

// statementDMLLoadDataVerb is the verb the refusal names for this shape.
const statementDMLLoadDataVerb = "LOAD DATA"

// executeLoadQueryPostHeaderLen is the fixed post-header size of an
// EXECUTE_LOAD_QUERY event, after the 19-byte event header.
const executeLoadQueryPostHeaderLen = 26

// dispatchExecuteLoadQuery is the EXECUTE_LOAD_QUERY arm: decode the
// event's own schema and statement, then apply the STATEMENT-DML belt's
// scope rule and refusal exactly as the QueryEvent arm does. A raw
// payload the decoder cannot read is refused too — loudly, with what is
// known — because the alternative is the silent drop this arm exists to
// close.
func (r *CDCReader) dispatchExecuteLoadQuery(ev *replication.BinlogEvent, e *replication.ExecuteLoadQueryEvent) error {
	schema, query, err := decodeExecuteLoadQuery(ev.RawData, e, r.eventTrailerLen())
	if err != nil {
		return fmt.Errorf("mysql: cdc: a statement-format LOAD DATA arrived as an EXECUTE_LOAD_QUERY event "+
			"sluice could not decode (%s) — refusing rather than dropping the load: %w",
			r.statementDMLLocator(ev.Header), err)
	}
	if !r.statementInScope(schema, query) {
		return nil
	}
	return statementDMLError(statementDMLLoadDataVerb, schema, r.statementDMLLocator(ev.Header), query)
}

// eventTrailerLen is the byte length of the checksum trailer on the
// event currently being dispatched: the FORMAT_DESCRIPTION's algorithm
// for a top-level event, none for an event unpacked from a
// TRANSACTION_PAYLOAD.
func (r *CDCReader) eventTrailerLen() int {
	if r.payloadDepth > 0 || r.binlogChecksumAlg != replication.BINLOG_CHECKSUM_ALG_CRC32 {
		return 0
	}
	return replication.BinlogChecksumLength
}

// decodeExecuteLoadQuery extracts the session database and the
// statement text from an EXECUTE_LOAD_QUERY event's raw bytes; see the
// file comment for the layout. e supplies the two lengths the parser
// already decoded (schema, status vars); trailer is the checksum length
// to leave off the tail.
func decodeExecuteLoadQuery(raw []byte, e *replication.ExecuteLoadQueryEvent, trailer int) (schema, query string, err error) {
	const headerLen = replication.EventHeaderSize
	body := headerLen + executeLoadQueryPostHeaderLen + int(e.StatusVars)
	schemaEnd := body + int(e.SchemaLength)
	queryStart := schemaEnd + 1 // the NUL after the schema
	queryEnd := len(raw) - trailer
	if len(raw) < queryStart || queryEnd < queryStart {
		return "", "", fmt.Errorf("event is %d bytes; schema+status vars need %d and a %d-byte trailer",
			len(raw), queryStart, trailer)
	}
	if raw[schemaEnd] != 0 {
		return "", "", fmt.Errorf("byte %d after the %d-byte schema is 0x%02x, not the NUL the layout requires",
			schemaEnd, e.SchemaLength, raw[schemaEnd])
	}
	return string(raw[body:schemaEnd]), string(raw[queryStart:queryEnd]), nil
}

// legacyLoadDataEventTypes are the pre-5.0.3 LOAD DATA wire shapes
// (MySQL 3.23/4.x wrote LOAD_EVENT / NEW_LOAD_EVENT with the data in
// CREATE_FILE / APPEND_BLOCK / EXEC_LOAD / DELETE_FILE events). No server
// sluice can connect to writes them, and go-mysql surfaces each as an
// undecoded [replication.GenericEvent] — but they CAN carry a write, so
// the dispatch roster (TestDispatchEventRoster_EveryWriteBearingType)
// refuses to classify them as bookkeeping on a promise. They are refused
// loudly instead: unreachable, and stated so, rather than exempt.
var legacyLoadDataEventTypes = map[replication.EventType]bool{
	replication.LOAD_EVENT:         true,
	replication.NEW_LOAD_EVENT:     true,
	replication.CREATE_FILE_EVENT:  true,
	replication.APPEND_BLOCK_EVENT: true,
	replication.EXEC_LOAD_EVENT:    true,
	replication.DELETE_FILE_EVENT:  true,
}

// dispatchGenericEvent is the arm for event types go-mysql does not
// decode. Everything is bookkeeping except the legacy LOAD DATA family,
// which is refused with the same code as its modern sibling.
func (r *CDCReader) dispatchGenericEvent(ev *replication.BinlogEvent) error {
	if ev.Header == nil || !legacyLoadDataEventTypes[ev.Header.EventType] {
		return nil
	}
	return statementDMLError(statementDMLLoadDataVerb, "",
		r.statementDMLLocator(ev.Header)+fmt.Sprintf(", legacy %v", ev.Header.EventType), "")
}
