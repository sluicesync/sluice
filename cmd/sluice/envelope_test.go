// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/pipeline"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// captureEnvelope redirects the run's stdout writer into a buffer and
// returns it. Tests decode the buffer instead of touching os.Stdout.
func captureEnvelope(e *envelopeRun) *bytes.Buffer {
	var buf bytes.Buffer
	e.out = &buf
	return &buf
}

func decodeEnvelope(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal(buf.Bytes(), &doc); err != nil {
		t.Fatalf("envelope is not valid JSON: %v\noutput: %s", err, buf.String())
	}
	return doc
}

func TestEnvelope_CompletedShape(t *testing.T) {
	e := newEnvelopeRun("migrate", "json")
	buf := captureEnvelope(e)
	e.setEngines("mysql", "postgres")
	e.setResume(true, "--resume")
	e.setNextSteps("sluice verify --source-driver mysql --source <SOURCE_DSN> --target-driver postgres --target <TARGET_DSN>")
	e.summary.RecordTable("app", "users")
	e.summary.RecordTableRows("", "orders", 42)
	e.markEngaged()

	if err := e.finish(nil); err != nil {
		t.Fatalf("finish(nil) returned %v", err)
	}
	doc := decodeEnvelope(t, buf)

	if doc["command"] != "migrate" || doc["status"] != "completed" {
		t.Fatalf("command/status: got %v/%v", doc["command"], doc["status"])
	}
	if doc["source_engine"] != "mysql" || doc["target_engine"] != "postgres" {
		t.Fatalf("engines: got %v/%v", doc["source_engine"], doc["target_engine"])
	}
	if _, ok := doc["elapsed_seconds"].(float64); !ok {
		t.Fatalf("elapsed_seconds missing or not a number: %v", doc["elapsed_seconds"])
	}
	if _, ok := doc["error"]; ok {
		t.Fatalf("completed envelope must not carry an error: %v", doc["error"])
	}
	tables, ok := doc["tables"].([]any)
	if !ok || len(tables) != 2 {
		t.Fatalf("tables: got %v", doc["tables"])
	}
	first := tables[0].(map[string]any)
	if first["schema"] != "app" || first["name"] != "users" {
		t.Fatalf("tables[0]: got %v", first)
	}
	if _, ok := first["rows"]; ok {
		t.Fatalf("unknown row count must be omitted, not rendered: %v", first)
	}
	second := tables[1].(map[string]any)
	if second["name"] != "orders" || second["rows"] != float64(42) {
		t.Fatalf("tables[1]: got %v", second)
	}
	resume := doc["resume"].(map[string]any)
	if resume["supported"] != true || resume["hint"] != "--resume" {
		t.Fatalf("resume: got %v", resume)
	}
	steps, ok := doc["next_steps"].([]any)
	if !ok || len(steps) != 1 || !strings.Contains(steps[0].(string), "sluice verify") {
		t.Fatalf("next_steps: got %v", doc["next_steps"])
	}
}

// TestEnvelope_RefusedVsFailedClassification pins the classification
// boundary: errors before markEngaged are pre-work refusals; errors
// after it are runtime failures. Both carry the human error text and
// suppress next_steps.
func TestEnvelope_RefusedVsFailedClassification(t *testing.T) {
	for _, tc := range []struct {
		name       string
		engaged    bool
		wantStatus string
	}{
		{name: "refused before engage", engaged: false, wantStatus: "refused"},
		{name: "failed after engage", engaged: true, wantStatus: "failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnvelopeRun("backup full", "json")
			buf := captureEnvelope(e)
			e.setNextSteps("sluice backup verify --from-dir <BACKUP_DIR>")
			if tc.engaged {
				e.markEngaged()
			}

			runErr := errors.New("--include-table and --exclude-table are mutually exclusive")
			if got := e.finish(runErr); !errors.Is(got, runErr) {
				t.Fatalf("finish must return the run error unchanged; got %v", got)
			}
			doc := decodeEnvelope(t, buf)
			if doc["status"] != tc.wantStatus {
				t.Fatalf("status: got %v, want %s", doc["status"], tc.wantStatus)
			}
			errObj := doc["error"].(map[string]any)
			if errObj["message"] != runErr.Error() {
				t.Fatalf("error.message: got %v", errObj["message"])
			}
			if _, ok := doc["next_steps"]; ok {
				t.Fatalf("next_steps must be omitted on %s: %v", tc.wantStatus, doc["next_steps"])
			}
		})
	}
}

// TestEnvelope_CodedRefusalReclassifiesAfterEngage pins the
// sluicecode merge point: a ClassRefusal CodedError surfacing AFTER
// markEngaged still reports status "refused" (consistent with its
// exit-3 taxonomy class) and lifts code + hint into the error object;
// a ClassRuntime CodedError keeps the engagement classification
// ("failed") while still carrying its code.
func TestEnvelope_CodedRefusalReclassifiesAfterEngage(t *testing.T) {
	for _, tc := range []struct {
		name       string
		code       sluicecode.Code
		wantStatus string
	}{
		{name: "refusal class wins over engagement", code: sluicecode.CodeColdStartTargetNotEmpty, wantStatus: "refused"},
		{name: "runtime class keeps failed", code: sluicecode.CodeBulkCopyTableFailed, wantStatus: "failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnvelopeRun("migrate", "json")
			buf := captureEnvelope(e)
			e.markEngaged()

			runErr := sluicecode.Wrap(tc.code, "pass --reset-target-data for a clean re-copy",
				errors.New("cold-start refused: target table \"public.users\" already contains data"))
			if got := e.finish(runErr); !errors.Is(got, runErr) {
				t.Fatalf("finish must return the run error unchanged; got %v", got)
			}
			doc := decodeEnvelope(t, buf)
			if doc["status"] != tc.wantStatus {
				t.Fatalf("status: got %v, want %s", doc["status"], tc.wantStatus)
			}
			errObj := doc["error"].(map[string]any)
			if errObj["code"] != string(tc.code) {
				t.Fatalf("error.code: got %v, want %s", errObj["code"], tc.code)
			}
			if errObj["hint"] == "" || errObj["hint"] == nil {
				t.Fatalf("error.hint must carry the remedy; got %v", errObj["hint"])
			}
		})
	}
}

// TestEnvelope_NoDSNLeak pins the redaction contract: a registered DSN
// that leaks into the error message (config errors and driver errors
// sometimes echo the DSN they failed on) is scrubbed out of the
// rendered envelope, credentials first.
func TestEnvelope_NoDSNLeak(t *testing.T) {
	for _, dsn := range []string{
		"postgres://app:s3cretpw@db.example.com:5432/prod?sslmode=disable&connect_timeout=5",
		"app:s3cretpw@tcp(db.example.com:3306)/prod?parseTime=true",
	} {
		t.Run(dsn[:8], func(t *testing.T) {
			e := newEnvelopeRun("migrate", "json")
			buf := captureEnvelope(e)
			e.scrub(dsn, "")
			e.markEngaged()

			_ = e.finish(fmt.Errorf("pipeline: open source row reader: dial %s: connection refused", dsn))
			out := buf.String()
			if strings.Contains(out, "s3cretpw") {
				t.Fatalf("envelope leaked the DSN password:\n%s", out)
			}
			if strings.Contains(out, dsn) {
				t.Fatalf("envelope leaked the raw DSN:\n%s", out)
			}
			// The credential-free locator survives so the error stays
			// diagnosable.
			if !strings.Contains(out, "db.example.com") {
				t.Fatalf("scrub dropped the host locator entirely:\n%s", out)
			}
		})
	}
}

// TestEnvelope_DryRunMigratePlanGolden pins the `--dry-run --format
// json` object byte-for-byte (it is deterministic — no elapsed
// timer), including the multi-database sink merge.
func TestEnvelope_DryRunMigratePlanGolden(t *testing.T) {
	e := newEnvelopeRun("migrate", "json")
	buf := captureEnvelope(e)
	e.captureMigratePlan(&pipeline.MigrationPlan{
		SourceEngine: "mysql",
		TargetEngine: "postgres",
		Views:        1,
		Tables: []pipeline.PlanTable{
			{Name: "users", Columns: 3, PrimaryKey: true, SecondaryIndexes: 2, ForeignKeys: 0, RowCount: 7},
		},
	})
	// Second sink call (multi-database fan-out) merges.
	e.captureMigratePlan(&pipeline.MigrationPlan{
		SourceEngine: "mysql",
		TargetEngine: "postgres",
		Tables: []pipeline.PlanTable{
			{Name: "orders", Columns: 2, PrimaryKey: false, SecondaryIndexes: 0, ForeignKeys: 1, RowCount: -1},
		},
	})
	if err := e.finish(nil); err != nil {
		t.Fatalf("finish: %v", err)
	}

	golden := `{
  "command": "migrate",
  "dry_run": true,
  "plan": {
    "source_engine": "mysql",
    "target_engine": "postgres",
    "views": 1,
    "tables": [
      {
        "name": "users",
        "columns": 3,
        "primary_key": true,
        "secondary_indexes": 2,
        "foreign_keys": 0,
        "row_count": 7
      },
      {
        "name": "orders",
        "columns": 2,
        "primary_key": false,
        "secondary_indexes": 0,
        "foreign_keys": 1,
        "row_count": -1
      }
    ]
  }
}
`
	if got := buf.String(); got != golden {
		t.Fatalf("plan envelope drifted from golden.\ngot:\n%s\nwant:\n%s", got, golden)
	}
}

// TestEnvelope_DryRunStreamPlanGolden pins the sync-start stream-plan
// serialization for both branches (warm resume and cold start).
func TestEnvelope_DryRunStreamPlanGolden(t *testing.T) {
	e := newEnvelopeRun("sync start", "json")
	buf := captureEnvelope(e)
	e.captureStreamPlan(&pipeline.StreamPlan{
		SourceEngine:  "postgres",
		SourceHost:    "src.example.com:5432",
		TargetEngine:  "mysql",
		TargetHost:    "dst.example.com:3306",
		StreamID:      "stream-1",
		WarmResume:    true,
		PositionToken: `{"lsn":"0/15D6A88"}`,
	})
	if err := e.finish(nil); err != nil {
		t.Fatalf("finish: %v", err)
	}

	golden := `{
  "command": "sync start",
  "dry_run": true,
  "plan": {
    "source_engine": "postgres",
    "source_host": "src.example.com:5432",
    "target_engine": "mysql",
    "target_host": "dst.example.com:3306",
    "stream_id": "stream-1",
    "warm_resume": true,
    "position_token": "{\"lsn\":\"0/15D6A88\"}"
  }
}
`
	if got := buf.String(); got != golden {
		t.Fatalf("stream plan envelope drifted from golden.\ngot:\n%s\nwant:\n%s", got, golden)
	}
}

// TestEnvelope_DryRunFailureStillEmitsResultEnvelope: a dry run that
// errors before its plan is built must still emit exactly one JSON
// object — the failure envelope.
func TestEnvelope_DryRunFailureStillEmitsResultEnvelope(t *testing.T) {
	e := newEnvelopeRun("migrate", "json")
	buf := captureEnvelope(e)
	e.markEngaged()
	_ = e.finish(errors.New("pipeline: open source schema reader: connection refused"))
	doc := decodeEnvelope(t, buf)
	if doc["status"] != "failed" {
		t.Fatalf("status: got %v", doc["status"])
	}
	if _, ok := doc["dry_run"]; ok {
		t.Fatalf("failure envelope must not carry dry_run: %v", doc)
	}
}

// TestEnvelope_TextModeWritesNothing pins that text mode is
// byte-identical to before the envelope layer existed: nothing on
// stdout, error passed through untouched.
func TestEnvelope_TextModeWritesNothing(t *testing.T) {
	e := newEnvelopeRun("migrate", "text")
	buf := captureEnvelope(e)
	if e.summary != nil {
		t.Fatal("text mode must not allocate a summary collector")
	}
	runErr := errors.New("boom")
	if got := e.finish(runErr); !errors.Is(got, runErr) {
		t.Fatalf("finish must pass the error through; got %v", got)
	}
	if got := e.finish(nil); got != nil {
		t.Fatalf("finish(nil) must return nil; got %v", got)
	}
	if buf.Len() != 0 {
		t.Fatalf("text mode wrote to stdout: %q", buf.String())
	}
}

// TestEnvelope_FormatFlagShape pins the four primary verbs' Format
// flag to the shared house shape (default text, enum text,json) so a
// drive-by rename can't silently drop a verb out of the envelope
// contract.
func TestEnvelope_FormatFlagShape(t *testing.T) {
	for name, cmd := range map[string]any{
		"migrate":     MigrateCmd{},
		"sync start":  SyncStartCmd{},
		"backup full": BackupFullCmd{},
		"restore":     RestoreCmd{},
	} {
		f, ok := reflect.TypeOf(cmd).FieldByName("Format")
		if !ok {
			t.Errorf("%s: no Format field", name)
			continue
		}
		if got := f.Tag.Get("default"); got != "text" {
			t.Errorf("%s: Format default = %q, want text", name, got)
		}
		if got := f.Tag.Get("enum"); got != "text,json" {
			t.Errorf("%s: Format enum = %q, want text,json", name, got)
		}
	}
}
