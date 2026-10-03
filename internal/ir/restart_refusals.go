// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import "errors"

// The sentinels below classify engine refusals that a fresh run of the same
// stream repeats exactly, because the condition they refuse lives in durable
// state the next start reads back: the source's replication slot, the change
// stream a restart re-delivers, or the DSN. The engine wraps its refusal with
// %w; like [ErrUnforwardedSchemaChange], each sentinel's text IS the refusal's
// grep-stable marker, so the message reads the same with or without it.
//
// The pipeline's fleet supervisor keys on them to stop restarting a leg that
// hit one. Under its restart-forever default (max-consecutive-failures 0) such
// a leg would otherwise refuse behind backoff indefinitely while the fleet
// looked healthy. All but [ErrKeyScopedWriteMatchedMultipleRows] are codeless
// refusals that exit 1 from a single command; that one is the coded
// SLUICE-E-CDC-KEY-MATCHED-MULTIPLE-ROWS refusal, listed because a restart
// repeats it, which a coded refusal does not by itself promise.

// ErrSlotAckedPastTargetPosition classifies the Postgres warm-resume refusal
// raised when the source replication slot's confirmed_flush_lsn is past the
// position the target persisted (GC-41 (h)). A restart reads the same slot
// and the same control row, so it refuses again until the operator re-copies
// or acknowledges with `--accept-slot-acked-past-position`.
var ErrSlotAckedPastTargetPosition = errors.New("SLOT-ACKED-PAST-TARGET-POSITION")

// ErrShardedTargetVindexUpdate classifies the MySQL-family apply refusal
// raised when vtgate refuses a change that assigns a vindex column on a
// sharded target (GC-41 (e)). The change sits after the persisted position,
// so a restart re-delivers it and vtgate refuses it again.
var ErrShardedTargetVindexUpdate = errors.New("SHARDED-TARGET-VINDEX-UPDATE")

// ErrCharsetNotDecodable classifies the MySQL-family refusal of a value whose
// declared character set has no faithful conversion to UTF-8. The value is in
// the source row or binlog event a restart reads again.
var ErrCharsetNotDecodable = errors.New("CHARSET-NOT-DECODABLE")

// ErrDSNTimeZoneNotUTC classifies the MySQL-family refusal of a DSN whose
// time_zone parameter is not UTC (GC-39). The DSN is configuration; a restart
// reads the same one.
var ErrDSNTimeZoneNotUTC = errors.New("DSN-TIME-ZONE-NOT-UTC")

// ErrKeyScopedWriteMatchedMultipleRows classifies the apply refusal of an
// UPDATE or DELETE that names its row by key and matched more than one target
// row (GC-42, SLUICE-E-CDC-KEY-MATCHED-MULTIPLE-ROWS). The apply transaction is
// rolled back, and a restart replays the same source changes in order onto the
// same committed rows, so the write matches them again.
var ErrKeyScopedWriteMatchedMultipleRows = errors.New("KEY-SCOPED-WRITE-MATCHED-MULTIPLE-ROWS")

// ErrHeartbeatTableNotSluices classifies the refusal raised when the source
// heartbeat (--source-heartbeat-interval) finds a table already present
// under its name whose shape is not the one sluice creates. Through
// v0.156.8 the writer used such a table as it found it: it INSERTed into it
// (succeeding when stream_id was its only required column) and, when it had
// a ts column, its prune DELETEd every row whose ts was older than the
// window — on the SOURCE. A --source-heartbeat-table-name naming a user
// table could therefore delete source rows. The table and the flag are
// durable state a restart reads back unchanged.
var ErrHeartbeatTableNotSluices = errors.New("HEARTBEAT-TABLE-NOT-SLUICES")

// ErrChangeLogWatermarkStalled classifies the trigger-CDC (postgres-trigger)
// refusal raised when a change-log poll read a gap-free window and the
// stream's watermark did not reach it (GC-43 (a)'s tripwire). Only a sluice
// bug produces that shape. A restart resumes at or below the same watermark
// and reads the same immutable change-log rows through the same code, so a
// cause that lives in how a window is consumed refuses again; a cause that
// lived only in the stopped process's memory would not, and since the
// cause is unknown by definition that half is UNVERIFIED. It is listed
// anyway because restarting forever behind backoff would turn the halt back
// into the silent stall it exists to end, while not restarting costs one
// `sync start` by hand, which the operator owes the bug report regardless.
var ErrChangeLogWatermarkStalled = errors.New("CHANGE-LOG-WATERMARK-STALLED")

// ErrResumeSchemaDivergence classifies the CDC first-boundary refusal (GC-44,
// pipeline/schema_forward_witness.go): a table's first schema boundary after
// the stream (re)started disagrees with the target table in a way sluice
// cannot forward without knowing what the source changed — a possible
// rename, more than one changed column, a type change across families that
// the target's type does not already hold. The check is a pure function of
// the source's current shape and the target's
// catalog, so a restart reads both back unchanged and refuses again until the
// operator reconciles the target (the drained model).
var ErrResumeSchemaDivergence = errors.New("RESUME-SCHEMA-DIVERGENCE")

// ErrSchemaChangeRefused classifies the refusal of a schema boundary on a
// stream that does not forward source DDL — `--schema-changes=refuse`, a
// multi-database stream, `--inject-shard-column` with
// `--no-coordinate-live-ddl` (GC-44 F5, pipeline/schema_change_refuse.go):
// the source table now holds something the target column cannot faithfully
// take, so applying the rows after it would change their values. Like
// [ErrResumeSchemaDivergence] the check persists nothing and compares the
// source's current shape with the target's catalog, so a restart refuses
// again until the operator applies the change on the target.
var ErrSchemaChangeRefused = errors.New("SCHEMA-CHANGE-REFUSED")
