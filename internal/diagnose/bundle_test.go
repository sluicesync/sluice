// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package diagnose

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// fakeEngine + fakeApplier + fakeSchemaReader are minimal in-memory
// implementations of the ir surfaces the bundle assembler uses. They
// let us exercise the privacy-level contracts without a live database.
type fakeEngine struct {
	name string
	caps ir.Capabilities

	applier  *fakeApplier
	schemaSR *fakeSchemaReader
}

func (f *fakeEngine) Name() string                  { return f.name }
func (f *fakeEngine) Capabilities() ir.Capabilities { return f.caps }
func (f *fakeEngine) OpenSchemaReader(_ context.Context, _ string) (ir.SchemaReader, error) {
	return f.schemaSR, nil
}

func (f *fakeEngine) OpenSchemaWriter(_ context.Context, _ string) (ir.SchemaWriter, error) {
	return nil, errors.New("fake: schema writer not implemented")
}

func (f *fakeEngine) OpenRowReader(_ context.Context, _ string) (ir.RowReader, error) {
	return nil, errors.New("fake: row reader not implemented")
}

func (f *fakeEngine) OpenRowWriter(_ context.Context, _ string) (ir.RowWriter, error) {
	return nil, errors.New("fake: row writer not implemented")
}

func (f *fakeEngine) OpenCDCReader(_ context.Context, _ string) (ir.CDCReader, error) {
	return nil, errors.New("fake: CDC reader not implemented")
}

func (f *fakeEngine) OpenChangeApplier(_ context.Context, _ string) (ir.ChangeApplier, error) {
	if f.applier == nil {
		return nil, errors.New("fake: no applier configured")
	}
	return f.applier, nil
}

func (f *fakeEngine) OpenSnapshotStream(_ context.Context, _ string) (*ir.SnapshotStream, error) {
	return nil, errors.New("fake: snapshot stream not implemented")
}

type fakeApplier struct {
	streams       []ir.StreamStatus
	historyRows   []ir.RetainedSchemaVersionRow
	leases        []ir.ShardConsolidationLeaseRow
	historyErr    error
	leasesErr     error
	listStreamErr error
}

func (a *fakeApplier) EnsureControlTable(_ context.Context) error { return nil }
func (a *fakeApplier) ReadPosition(_ context.Context, _ string) (ir.Position, bool, error) {
	return ir.Position{}, false, nil
}

func (a *fakeApplier) ListStreams(_ context.Context) ([]ir.StreamStatus, error) {
	return a.streams, a.listStreamErr
}

func (a *fakeApplier) Apply(_ context.Context, _ string, _ <-chan ir.Change) error {
	return nil
}
func (a *fakeApplier) RequestStop(_ context.Context, _ string) error { return nil }
func (a *fakeApplier) ClearStopRequested(_ context.Context, _ string) error {
	return nil
}

// ListSchemaHistory satisfies ir.SchemaHistoryReader. Only registered
// when implHistory is true (via fakeApplierWithHistory).
type fakeApplierWithHistory struct{ *fakeApplier }

func (a *fakeApplierWithHistory) ListSchemaHistory(_ context.Context, _ string, _ int) ([]ir.RetainedSchemaVersionRow, error) {
	if a.historyErr != nil {
		return nil, a.historyErr
	}
	return a.historyRows, nil
}

type fakeApplierWithLeases struct{ *fakeApplier }

func (a *fakeApplierWithLeases) ListLeases(_ context.Context) ([]ir.ShardConsolidationLeaseRow, error) {
	if a.leasesErr != nil {
		return nil, a.leasesErr
	}
	return a.leases, nil
}

type fakeApplierFull struct {
	*fakeApplier
	*fakeApplierWithHistory
	*fakeApplierWithLeases
}

func (a *fakeApplierFull) EnsureControlTable(_ context.Context) error { return nil }
func (a *fakeApplierFull) ReadPosition(ctx context.Context, streamID string) (ir.Position, bool, error) {
	return a.fakeApplier.ReadPosition(ctx, streamID)
}

func (a *fakeApplierFull) ListStreams(ctx context.Context) ([]ir.StreamStatus, error) {
	return a.fakeApplier.ListStreams(ctx)
}

func (a *fakeApplierFull) Apply(ctx context.Context, streamID string, changes <-chan ir.Change) error {
	return a.fakeApplier.Apply(ctx, streamID, changes)
}

func (a *fakeApplierFull) RequestStop(ctx context.Context, streamID string) error {
	return a.fakeApplier.RequestStop(ctx, streamID)
}

func (a *fakeApplierFull) ClearStopRequested(ctx context.Context, streamID string) error {
	return a.fakeApplier.ClearStopRequested(ctx, streamID)
}

func (a *fakeApplierFull) ListSchemaHistory(ctx context.Context, streamID string, limit int) ([]ir.RetainedSchemaVersionRow, error) {
	return a.fakeApplierWithHistory.ListSchemaHistory(ctx, streamID, limit)
}

func (a *fakeApplierFull) ListLeases(ctx context.Context) ([]ir.ShardConsolidationLeaseRow, error) {
	return a.fakeApplierWithLeases.ListLeases(ctx)
}

type fakeSchemaReader struct{}

func (f *fakeSchemaReader) ReadSchema(_ context.Context) (*ir.Schema, error) {
	return &ir.Schema{}, nil
}

// newFakeTarget constructs a fakeEngine wired with a full applier (all
// optional interfaces present) and standard stream / history / lease
// rows. Tests can override fields on the returned applier for
// per-test variations.
func newFakeTarget(streamID string) (*fakeEngine, *fakeApplierFull) {
	streams := []ir.StreamStatus{
		{
			StreamID:  streamID,
			Position:  ir.Position{Engine: "postgres", Token: "0/1A2B3C4D"},
			UpdatedAt: time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC),
			SlotName:  "sluice_slot",
		},
		// Unrelated stream — should NOT appear in the bundle.
		{
			StreamID:  "other-stream",
			Position:  ir.Position{Engine: "postgres", Token: "0/DEADBEEF"},
			UpdatedAt: time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC),
		},
	}
	historyRows := []ir.RetainedSchemaVersionRow{
		{
			VersionKey:     "vk-001",
			StreamID:       streamID,
			SchemaName:     "public",
			TableName:      "users",
			AnchorPosition: "0/1000",
			TableJSON:      []byte(`{"name":"users","columns":[]}`),
		},
	}
	leases := []ir.ShardConsolidationLeaseRow{
		{
			TargetTableFullName: "public.orders",
			LeaseHolderStreamID: streamID,
		},
	}
	base := &fakeApplier{streams: streams, historyRows: historyRows, leases: leases}
	full := &fakeApplierFull{
		fakeApplier:            base,
		fakeApplierWithHistory: &fakeApplierWithHistory{fakeApplier: base},
		fakeApplierWithLeases:  &fakeApplierWithLeases{fakeApplier: base},
	}
	e := &fakeEngine{
		name:     "postgres",
		caps:     ir.Capabilities{BulkLoad: ir.BulkLoadCopy, CDC: ir.CDCBinlog},
		applier:  base,
		schemaSR: &fakeSchemaReader{},
	}
	// The fakeEngine's OpenChangeApplier returns `f.applier` (the base
	// fakeApplier). Tests that need the full surface use
	// newFakeTargetFull, but most don't — they swap the returned
	// applier in via this helper.
	e.applier = base
	// Wrap so OpenChangeApplier hands out the FULL surface.
	e.applier = base
	_ = full
	return e, full
}

// fakeEngineFull always returns the full applier (with history + leases) from
// OpenChangeApplier. Separate type so the assembler exercises the
// SchemaHistoryReader + ShardConsolidationLeaseLister branches.
type fakeEngineFull struct {
	*fakeEngine
	full *fakeApplierFull
}

func (f *fakeEngineFull) OpenChangeApplier(_ context.Context, _ string) (ir.ChangeApplier, error) {
	return f.full, nil
}

// readBundle parses the in-memory ZIP and returns the map of
// filename → contents. Test helper.
func readBundle(t *testing.T, buf *bytes.Buffer) map[string][]byte {
	t.Helper()
	zr, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("zip.NewReader: %v", err)
	}
	out := map[string][]byte{}
	for _, f := range zr.File {
		rc, err := f.Open()
		if err != nil {
			t.Fatalf("open %s: %v", f.Name, err)
		}
		b, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			t.Fatalf("read %s: %v", f.Name, err)
		}
		out[f.Name] = b
	}
	return out
}

// TestBundle_BasicLevel pins the basic privacy contract: state-table
// dumps only. NO version, NO DSN, NO health, NO capabilities, NO row
// counts, NO logs.
func TestBundle_BasicLevel_IncludesStateExcludesEverythingElse(t *testing.T) {
	base, full := newFakeTarget("stream-1")
	target := &fakeEngineFull{fakeEngine: base, full: full}

	var buf bytes.Buffer
	err := Write(context.Background(), &buf, Request{
		StreamID:        "stream-1",
		PrivacyLevel:    PrivacyBasic,
		TargetEngine:    target,
		TargetDSN:       "postgres://user:secret@target.example.com:5432/appdb",
		SluiceVersion:   "v0.74.2",
		SluiceCommit:    "deadbeef",
		SluiceBuildDate: "2026-05-22",
		CLIArgs:         []string{"diagnose", "--target", "postgres://user:secret@target.example.com:5432/appdb"},
		Now:             time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	files := readBundle(t, &buf)

	// MUST be present: manifest + state dumps.
	mustHave := []string{
		"bundle.json",
		"state/cdc_state.json",
		"state/schema_history.json",
		"state/shard_consolidation_lease.json",
	}
	for _, name := range mustHave {
		if _, ok := files[name]; !ok {
			t.Errorf("basic level: missing %s", name)
		}
	}

	// MUST NOT be present: anything from standard or verbose.
	mustNotHave := []string{
		"config/cli_args.json",
		"engine/capabilities.json",
		"engine/source_diagnose.json",
		"engine/target_diagnose.json",
		"health/sync_health.json",
		"verbose/row_counts.json",
		"logs/log_tail.txt",
	}
	for _, name := range mustNotHave {
		if _, ok := files[name]; ok {
			t.Errorf("basic level: should not include %s", name)
		}
	}

	// Manifest MUST NOT carry version / DSN / engine name at basic
	// level (the privacy contract pinned per ADR-0056).
	var mf Manifest
	if err := json.Unmarshal(files["bundle.json"], &mf); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if mf.PrivacyLevel != "basic" {
		t.Errorf("manifest PrivacyLevel = %q, want basic", mf.PrivacyLevel)
	}
	if mf.SluiceVersion != "" || mf.SluiceCommit != "" || mf.SluiceBuildDate != "" {
		t.Errorf("basic level: manifest should not include sluice version/commit/build_date; got %+v", mf)
	}
	if mf.GoVersion != "" || mf.GOOS != "" || mf.GOARCH != "" {
		t.Errorf("basic level: manifest should not include Go runtime fields; got %+v", mf)
	}
	if mf.SourceDSNRedacted != "" || mf.TargetDSNRedacted != "" {
		t.Errorf("basic level: manifest should not include redacted DSN; got source=%q target=%q",
			mf.SourceDSNRedacted, mf.TargetDSNRedacted)
	}
	if mf.SourceEngine != "" || mf.TargetEngine != "" {
		t.Errorf("basic level: manifest should not include engine names; got source=%q target=%q",
			mf.SourceEngine, mf.TargetEngine)
	}
}

// TestBundle_StandardLevel pins the standard privacy contract: basic +
// version / DSN-redacted CLI / engine health / capabilities. Still NO
// row data, NO logs.
func TestBundle_StandardLevel_IncludesHealthAndConfigExcludesLogsAndRowCounts(t *testing.T) {
	base, full := newFakeTarget("stream-1")
	target := &fakeEngineFull{fakeEngine: base, full: full}

	var buf bytes.Buffer
	err := Write(context.Background(), &buf, Request{
		StreamID:        "stream-1",
		PrivacyLevel:    PrivacyStandard,
		TargetEngine:    target,
		TargetDSN:       "postgres://user:secret@target.example.com:5432/appdb",
		SluiceVersion:   "v0.74.2",
		SluiceCommit:    "deadbeef",
		SluiceBuildDate: "2026-05-22",
		CLIArgs:         []string{"diagnose", "--target", "postgres://user:secret@target.example.com:5432/appdb"},
		Now:             time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	files := readBundle(t, &buf)

	mustHave := []string{
		"bundle.json",
		"state/cdc_state.json",
		"config/cli_args.json",
		"engine/capabilities.json",
	}
	for _, name := range mustHave {
		if _, ok := files[name]; !ok {
			t.Errorf("standard level: missing %s", name)
		}
	}

	// Standard level must NOT include verbose-only sections.
	mustNotHave := []string{
		"verbose/row_counts.json",
		"logs/log_tail.txt",
	}
	for _, name := range mustNotHave {
		if _, ok := files[name]; ok {
			t.Errorf("standard level: should not include %s", name)
		}
	}

	var mf Manifest
	if err := json.Unmarshal(files["bundle.json"], &mf); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if mf.SluiceVersion != "v0.74.2" {
		t.Errorf("standard: SluiceVersion = %q, want v0.74.2", mf.SluiceVersion)
	}
	if mf.TargetDSNRedacted == "" {
		t.Errorf("standard: TargetDSNRedacted empty; expected redacted DSN")
	}
	if strings.Contains(mf.TargetDSNRedacted, "secret") {
		t.Errorf("standard: TargetDSNRedacted contains password material: %q", mf.TargetDSNRedacted)
	}
	if mf.TargetEngine != "postgres" {
		t.Errorf("standard: TargetEngine = %q, want postgres", mf.TargetEngine)
	}
}

// TestBundle_VerboseLevel pins the verbose privacy contract: standard
// + row counts + log tail (when LogFile is set).
func TestBundle_VerboseLevel_IncludesEverything(t *testing.T) {
	base, full := newFakeTarget("stream-1")
	target := &fakeEngineFull{fakeEngine: base, full: full}

	// Create a small log file for the tail-section to scrape.
	logDir := t.TempDir()
	logPath := logDir + "/sluice.log"
	if err := writeTestFile(logPath, "line1\nline2\nline3\n"); err != nil {
		t.Fatalf("seed log file: %v", err)
	}

	var buf bytes.Buffer
	err := Write(context.Background(), &buf, Request{
		StreamID:        "stream-1",
		PrivacyLevel:    PrivacyVerbose,
		TargetEngine:    target,
		TargetDSN:       "postgres://user:secret@target.example.com:5432/appdb",
		SluiceVersion:   "v0.74.2",
		SluiceCommit:    "deadbeef",
		SluiceBuildDate: "2026-05-22",
		CLIArgs:         []string{"diagnose", "--target", "postgres://user:secret@target.example.com:5432/appdb"},
		LogFile:         logPath,
		Now:             time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	files := readBundle(t, &buf)

	// Verbose: row counts (the row reader doesn't support RowCounter
	// in the fake — a skipped-reason file lands instead, which still
	// represents the verbose-section coverage).
	verboseExpect := []string{
		"logs/log_tail.txt",
		// row counts will surface as a reason file because the fake
		// engine doesn't implement RowCounter.
		"verbose/row_counts/__skipped.txt",
	}
	for _, name := range verboseExpect {
		if _, ok := files[name]; !ok {
			t.Errorf("verbose level: missing %s; got files: %v", name, fileNames(files))
		}
	}

	// Standard sections still present.
	if _, ok := files["config/cli_args.json"]; !ok {
		t.Errorf("verbose level: config/cli_args.json missing")
	}
	if _, ok := files["engine/capabilities.json"]; !ok {
		t.Errorf("verbose level: engine/capabilities.json missing")
	}
	// Basic still present.
	if _, ok := files["state/cdc_state.json"]; !ok {
		t.Errorf("verbose level: state/cdc_state.json missing")
	}

	logTail := string(files["logs/log_tail.txt"])
	if !strings.Contains(logTail, "line1") || !strings.Contains(logTail, "line3") {
		t.Errorf("verbose: log tail did not contain expected lines: %q", logTail)
	}
}

// TestBundle_RedactsDSNCredentials pins the DSN-redaction contract.
// A DSN with embedded credentials must NEVER land in the bundle in
// clear form. Exercises BOTH DSN shapes (URI form, go-sql-driver form)
// — the Bug-74 "pin the class, not the representative" discipline.
func TestBundle_RedactsDSNCredentials(t *testing.T) {
	cases := []struct {
		name       string
		dsn        string
		wantRedact string // substring that must appear post-redaction
		wantStrip  string // substring that must NOT appear post-redaction
	}{
		{
			name:       "uri-form-postgres",
			dsn:        "postgres://admin:supersecret@db.example.com:5432/appdb?sslmode=require",
			wantRedact: "db.example.com:5432/appdb",
			wantStrip:  "supersecret",
		},
		{
			name:       "uri-form-mysql",
			dsn:        "mysql://root:p%40ssw0rd@10.0.0.1:3306/inventory",
			wantRedact: "10.0.0.1:3306/inventory",
			wantStrip:  "p%40ssw0rd",
		},
		{
			name:       "go-sql-driver-form",
			dsn:        "user:topsecret@tcp(mysql.example.com:3306)/orders?parseTime=true",
			wantRedact: "tcp(mysql.example.com:3306)/orders",
			wantStrip:  "topsecret",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := RedactDSN(tc.dsn)
			if !strings.Contains(got, tc.wantRedact) {
				t.Errorf("%s: RedactDSN(%q) = %q; want substring %q", tc.name, tc.dsn, got, tc.wantRedact)
			}
			if strings.Contains(got, tc.wantStrip) {
				t.Errorf("%s: RedactDSN(%q) = %q; LEAKED credential %q", tc.name, tc.dsn, got, tc.wantStrip)
			}
		})
	}
}

// TestBundle_PreservesDatabaseName pins the contract that the
// database NAME (not credential material) survives redaction. The
// recipient of a bundle needs the DB name to correlate the issue
// with their environment.
func TestBundle_PreservesDatabaseName(t *testing.T) {
	got := RedactDSN("postgres://user:pw@host:5432/important_db?sslmode=disable")
	if !strings.Contains(got, "important_db") {
		t.Errorf("RedactDSN dropped the database name; got %q", got)
	}
}

// TestBundle_RedactCLIArgs pins the contract that DSN-bearing flag
// values in CLIArgs are redacted before bundle embedding.
func TestBundle_RedactCLIArgs(t *testing.T) {
	cases := []struct {
		name    string
		args    []string
		want    []string
		notWant string
	}{
		{
			name:    "two-token-source",
			args:    []string{"sync", "start", "--source", "postgres://u:p@h:5432/d"},
			want:    []string{"sync", "start", "--source", "h:5432/d"},
			notWant: "u:p",
		},
		{
			name:    "equals-form-target",
			args:    []string{"diagnose", "--target=postgres://u:p@h:5432/d"},
			want:    []string{"diagnose", "--target=h:5432/d"},
			notWant: "u:p",
		},
		{
			name:    "non-dsn-flag-passthrough",
			args:    []string{"--log-level=debug", "--pprof-listen=:6060"},
			want:    []string{"--log-level=debug", "--pprof-listen=:6060"},
			notWant: "",
		},
		{
			name:    "keyset-source-db-form",
			args:    []string{"--keyset-source", "db:postgres://u:p@h/keys"},
			want:    []string{"--keyset-source", "h/keys"},
			notWant: "u:p",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := RedactCLIArgs(tc.args)
			if len(got) != len(tc.want) {
				t.Fatalf("RedactCLIArgs length = %d, want %d (got=%v)", len(got), len(tc.want), got)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("RedactCLIArgs[%d] = %q, want %q", i, got[i], tc.want[i])
				}
			}
			if tc.notWant != "" {
				for _, s := range got {
					if strings.Contains(s, tc.notWant) {
						t.Errorf("RedactCLIArgs leaked %q in %q", tc.notWant, s)
					}
				}
			}
		})
	}
}

// TestBundle_RejectsUnsetPrivacyLevel pins the loud-failure contract
// for misconfigured requests — the assembler must refuse rather than
// silently default.
func TestBundle_RejectsUnsetPrivacyLevel(t *testing.T) {
	var buf bytes.Buffer
	err := Write(context.Background(), &buf, Request{
		StreamID: "stream-1",
	})
	if err == nil {
		t.Fatalf("Write with unset privacy level returned nil; expected refusal")
	}
}

// TestBundle_RejectsEmptyStreamID pins the loud-failure contract for
// missing stream-id.
func TestBundle_RejectsEmptyStreamID(t *testing.T) {
	var buf bytes.Buffer
	err := Write(context.Background(), &buf, Request{
		PrivacyLevel: PrivacyBasic,
	})
	if err == nil {
		t.Fatalf("Write with empty stream-id returned nil; expected refusal")
	}
}

// TestBundle_BasicScopesToRequestedStream pins that the cdc_state
// dump only contains the requested stream-id, not every stream on
// the target — privacy-by-scoping.
func TestBundle_BasicScopesToRequestedStream(t *testing.T) {
	base, full := newFakeTarget("stream-1")
	target := &fakeEngineFull{fakeEngine: base, full: full}

	var buf bytes.Buffer
	err := Write(context.Background(), &buf, Request{
		StreamID:     "stream-1",
		PrivacyLevel: PrivacyBasic,
		TargetEngine: target,
		TargetDSN:    "postgres://u:p@h:5432/d",
		Now:          time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	files := readBundle(t, &buf)
	got := string(files["state/cdc_state.json"])
	if !strings.Contains(got, "stream-1") {
		t.Errorf("cdc_state dump missing requested stream-1: %s", got)
	}
	if strings.Contains(got, "other-stream") {
		t.Errorf("cdc_state dump leaked unrelated stream-id: %s", got)
	}
}

// writeTestFile is a tiny helper for log-file seeding in tests. Lives
// here rather than as a dedicated helper file because it's used only
// by one verbose-level test.
func writeTestFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o600)
}

func fileNames(files map[string][]byte) []string {
	out := make([]string, 0, len(files))
	for k := range files {
		out = append(out, k)
	}
	return out
}
