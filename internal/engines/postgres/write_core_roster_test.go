// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/applyorder"
)

// controlTxWriters are the functions that put a control-table statement on a
// transaction that can also carry row data — executed on a *sql.Tx or queued
// onto the pipelined batch — the statements the DATA BEFORE CONTROL rule
// orders (GC-41 (c); see writePositionTx). Completeness is enforced below:
// every function taking a transaction is classified in txFuncClass, and each
// "control" one must be listed here.
var controlTxWriters = []string{
	"execApplyMarksTx", "queueApplyMarks", "writePositionTx", "writePositionPipelined",
	"writeSchemaVersion", "buildWriteSchemaVersionSQL", "buildWritePositionSQL", "applyMarkStatements",
	"queueFold", "execFold", "writeCheckpointTx", "writeOwnPositionTx",
}

// txFuncClass classifies every function in the package that takes a
// transaction (*sql.Tx, a pgx.Tx, the pipelined batch handle, or the
// schema-history execers): "control" writes a control table, "data" writes
// rows, "neutral" reads, commits or sets session state.
var txFuncClass = map[string]string{
	"ChangeApplier.execApplyMarksTx":            "control",
	"ChangeApplier.queueApplyMarks":             "control",
	"ChangeApplier.writePositionPipelined":      "control",
	"ChangeApplier.writeCheckpointTx":           "control", // ADR-0190 amendment E: marks, skip-ledger flush, position — after any data
	"ChangeApplier.writeOwnPositionTx":          "control", // applyOneImpl's marks, then (serial per-change) the change's own position
	"laneApplierAdapter.queueFold":              "control", // ADR-0190 amendment D: marks, then the fold's position
	"laneApplierAdapter.execFold":               "control", // the same on the serial fall-back's *sql.Tx
	"writePositionTx":                           "control",
	"writeSchemaVersion":                        "control",
	"compactSchemaHistoryBelow":                 "neutral", // runs on a.db, never inside an apply transaction
	"ChangeApplier.txExec":                      "data",
	"ChangeApplier.dispatch":                    "data", // and its SchemaSnapshot arm writes history (walked below)
	"ChangeApplier.dispatchPipelined":           "data", // likewise, queued
	"ChangeApplier.bypassForeignKeyEnforcement": "neutral",
	"ChangeApplier.forceSynchronousCommitOn":    "neutral",
	"ChangeApplier.commitWithTimeout":           "neutral",
	"ChangeApplier.flushAndCommit":              "neutral", // sends the queue in queue order, then commits
	"ChangeApplier.flushAndCommitStep":          "neutral", // the same, reporting whether the COMMIT raised the error
	"ChangeApplier.sendBatchUnderDeadline":      "neutral",
	"ChangeApplier.conflictKeyFor":              "neutral",
	"loadConflictKey":                           "neutral",
	"loadGeneratedColumns":                      "neutral",
	"loadPrimaryKey":                            "neutral",
	"pgxBatchTx.queue":                          "neutral", // the one enqueue every statement takes, in call order
	"pgxBatchTx.Rollback":                       "neutral",
	"pgxBatchTx.release":                        "neutral",
}

// writeCoreClass classifies every caller the walk up from controlTxWriters
// reaches: applyorder.Helper (not a core; its own callers are walked),
// applyorder.Unrelated (a bare-name collision), or the write core it is, as
// TestWriteCoreStatementOrder drives it.
var writeCoreClass = map[string]string{
	"writeSchemaVersion":                      applyorder.Helper,
	"writePositionTx":                         applyorder.Helper, // the executors, reached from the SQL builders
	"ChangeApplier.execApplyMarksTx":          applyorder.Helper,
	"ChangeApplier.queueApplyMarks":           applyorder.Helper,
	"ChangeApplier.writePositionPipelined":    applyorder.Helper,
	"laneApplierAdapter.queueFold":            applyorder.Helper, // reached by ApplyLaneBatch's fold batches
	"laneApplierAdapter.execFold":             applyorder.Helper, // reached by applyLaneBatchSerial's
	"ChangeApplier.dispatch":                  applyorder.Helper,
	"ChangeApplier.dispatchPipelined":         applyorder.Helper,
	"ChangeApplier.applyOneImpl":              applyorder.Helper,
	"ChangeApplier.applyOne":                  applyorder.Helper,
	"ChangeApplier.applySchemaEvent":          applyorder.Helper, // the batch loop's ApplyOne, unreachable while TransactionalDDL is true
	"ChangeApplier.applyBarrier":              applyorder.Helper, // the lane barrier, folding or not (ADR-0190 amendment E)
	"ChangeApplier.writeCheckpointTx":         applyorder.Helper,
	"ChangeApplier.writeOwnPositionTx":        applyorder.Helper,
	"ChangeApplier.commitCheckpoint":          applyorder.Helper, // WriteCheckpoint's transaction, and a skipped barrier's
	"laneApplierAdapter.ApplyBarrierChange":   "lane-barrier",
	"ChangeApplier.Apply":                     "serial",
	"ChangeApplier.persistSourceTxCommit":     "serial-tx-commit-position",
	"ChangeApplier.WritePosition":             "bare-write-position",
	"ChangeApplier.batchConfig":               "batch", // pipelined AND serial-fallback handles
	"laneApplierAdapter.ApplyLaneBatch":       "lane-batch",
	"laneApplierAdapter.applyLaneBatchSerial": "lane-batch-serial",
	"laneApplierAdapter.WriteCheckpoint":      "lane-checkpoint",
}

// TestWriteCoreRoster_EveryControlWriterCallerIsClassified is the MySQL
// gate's Postgres twin (GC-41 (c)): derived from the package's AST, so a new
// write core or control writer cannot escape TestWriteCoreStatementOrder by
// not being listed. Reach, stated: the walk starts at the executors and the
// SQL builders (buildWritePositionSQL, applyMarkStatements,
// buildWriteSchemaVersionSQL); a control statement spelled inline and sent
// through txExec or pgxBatchTx.queue is reached only by the integration
// test's by-text Classify, on the cores it drives.
func TestWriteCoreRoster_EveryControlWriterCallerIsClassified(t *testing.T) {
	funcs, err := applyorder.ParseFuncs(".")
	if err != nil {
		t.Fatalf("parse package: %v", err)
	}
	var problems []string
	txFuncs := applyorder.TakingAny(funcs, "*sql.Tx", "pgx.Tx", "*pgxBatchTx", "*pgx.Batch", "schemaHistoryExecer", "schemaHistoryExecQuerier")
	for _, key := range txFuncs {
		if _, ok := txFuncClass[key]; !ok {
			problems = append(problems, key+" takes a transaction and is not classified in txFuncClass")
		}
	}
	for key, class := range txFuncClass {
		found := false
		for _, k := range txFuncs {
			found = found || k == key
		}
		if !found {
			problems = append(problems, "txFuncClass entry "+key+" no longer takes a transaction — stale")
		}
		if class == "control" && !containsWriter(key) {
			problems = append(problems, key+" writes a control table but is missing from controlTxWriters")
		}
	}
	reached, rp := applyorder.WriterCallers(funcs, controlTxWriters, writeCoreClass)
	problems = append(problems, rp...)
	for _, p := range problems {
		t.Error(p)
	}
	// Anti-vacuity floor: today's walk reaches 15 callers across 8 cores.
	if len(reached) < 15 || len(applyorder.Cores(writeCoreClass)) < 8 {
		t.Fatalf("the walk reached %d callers and %d cores; want at least 15 and 8 — it is not finding the write paths",
			len(reached), len(applyorder.Cores(writeCoreClass)))
	}
}

func containsWriter(key string) bool {
	for _, w := range controlTxWriters {
		if key == w || strings.HasSuffix(key, "."+w) {
			return true
		}
	}
	return false
}
