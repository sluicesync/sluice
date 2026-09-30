// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/applyorder"
)

// controlTxWriters are the functions that send a control-table statement on a
// transaction that can also carry row data — the statements the DATA BEFORE
// CONTROL rule orders (GC-41 (c); see writePositionTx). Completeness is
// enforced below: every function taking a transaction is classified in
// txFuncClass, and each "control" one must be listed here.
var controlTxWriters = []string{"execApplyMarksTx", "writePositionTx", "writeSchemaVersion", "writePositionUpsertSQL", "applyMarkStatements", "schemaVersionUpsertSQL"}

// txFuncClass classifies every function in the package that takes a
// transaction (*sql.Tx or the schema-history execers): "control" writes a
// control table, "data" writes rows, "neutral" reads or commits. Reach,
// stated: the batch handle's own methods (receiver *mysqlBatchTx) reach a
// control table only through the listed writers, and are walked from them
// below rather than classified here.
var txFuncClass = map[string]string{
	"ChangeApplier.execApplyMarksTx":         "control",
	"writePositionTx":                        "control",
	"writeSchemaVersion":                     "control",
	"compactSchemaHistoryBelow":              "neutral", // runs on a.db in its own statement, never inside an apply transaction
	"ChangeApplier.txExec":                   "data",
	"ChangeApplier.dispatch":                 "data", // and its SchemaSnapshot arm calls writeSchemaVersion (walked below)
	"ChangeApplier.commitWithTimeout":        "neutral",
	"ChangeApplier.reportApplyClampWarnings": "neutral",
	"ChangeApplier.pkFor":                    "neutral",
	"ChangeApplier.colTypesFor":              "neutral",
	"loadPrimaryKey":                         "neutral",
}

// writeCoreClass classifies every caller the walk up from controlTxWriters
// reaches: applyorder.Helper (not a core; its own callers are walked) or the
// write core it is, as TestWriteCoreStatementOrder drives it.
var writeCoreClass = map[string]string{
	"ChangeApplier.dispatch":                applyorder.Helper,
	"ChangeApplier.execApplyMarksTx":        applyorder.Helper, // the executors, reached from the SQL builders
	"writePositionTx":                       applyorder.Helper,
	"writeSchemaVersion":                    applyorder.Helper,
	"ChangeApplier.applyOneImpl":            applyorder.Helper,
	"mysqlBatchTx.writeApplyMarks":          applyorder.Helper,
	"mysqlBatchTx.writePosition":            applyorder.Helper,
	"mysqlBatchTx.applySerial":              applyorder.Helper,
	"mysqlBatchTx.dispatch":                 applyorder.Helper,
	"mysqlBatchTx.dispatchInsert":           applyorder.Helper,
	"mysqlBatchTx.dispatchUpdate":           applyorder.Helper,
	"mysqlBatchTx.dispatchDelete":           applyorder.Helper,
	"ChangeApplier.applyOne":                applyorder.Helper,
	"ChangeApplier.applySchemaEvent":        applyorder.Helper,
	"ChangeApplier.applyBarrierNoPosition":  applyorder.Helper,
	"CDCReader.deliver":                     applyorder.Unrelated, // the binlog reader's own dispatch
	"CDCReader.dispatchTransactionPayload":  applyorder.Unrelated,
	"vstreamCDCReader.pump":                 applyorder.Unrelated,
	"ChangeApplier.Apply":                   "serial",
	"ChangeApplier.persistSourceTxCommit":   "serial-tx-commit-position",
	"laneApplierAdapter.ApplyBarrierChange": "lane-barrier",
	"ChangeApplier.WritePosition":           "bare-write-position",
	"ChangeApplier.batchConfig":             "batch",
	"laneApplierAdapter.ApplyLaneBatch":     "lane-batch",
	"laneApplierAdapter.WriteCheckpoint":    "lane-checkpoint",
}

// TestWriteCoreRoster_EveryControlWriterCallerIsClassified is the half of the
// GC-41 (c) gate that needs no database: it derives, from the package's own
// AST, every function that can put a control-table statement on a
// transaction and every path up to a write core, and fails on any it has not
// been told about — so a new write core (or a new control writer) cannot
// escape TestWriteCoreStatementOrder by not being listed.
//
// Reach, stated: the walk starts at the named executors AND at the SQL
// builders they render with (writePositionUpsertSQL, applyMarkStatements,
// schemaVersionUpsertSQL), so a new caller of a builder is caught too. What it
// cannot see is a control statement spelled inline and sent through a generic
// executor (txExec) — that is reached only by TestWriteCoreStatementOrder's
// by-text Classify, and only on the cores that test drives.

func TestWriteCoreRoster_EveryControlWriterCallerIsClassified(t *testing.T) {
	funcs, err := applyorder.ParseFuncs(".")
	if err != nil {
		t.Fatalf("parse package: %v", err)
	}
	var problems []string
	txFuncs := applyorder.TakingAny(funcs, "*sql.Tx", "schemaHistoryExecer", "schemaHistoryExecQuerier")
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
	// Anti-vacuity floor: today's walk reaches 22 callers across 7 cores.
	if len(reached) < 20 || len(applyorder.Cores(writeCoreClass)) < 7 {
		t.Fatalf("the walk reached %d callers and %d cores; want at least 20 and 7 — it is not finding the write paths",
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
