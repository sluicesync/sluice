//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The sync cold start's persisted position on a GTID-mode MySQL source,
// through the real Streamer (audit 2026-09-01 SLM-4).
//
// The engine-level matrix (internal/engines/mysql/
// cdc_snapshot_gtid_handoff_integration_test.go) pins what each opener
// STAMPS. This file pins what the pipeline PERSISTS and RESUMES from,
// which is the surface the finding was observed on:
//
//   - the token in sluice_cdc_state after the cold copy and BEFORE any
//     CDC event is the opener's own anchor (streamer_coldstart.go writes
//     stream.Position at the handoff), and it must be a GTID-mode token
//     on a gtid_mode=ON source — for both openers, selected the way
//     production selects them (the default parallelism over the
//     enumerated table list → N-way; copy_table_parallelism=1 → serial);
//   - the failover cell: that anchor, resumed against a promoted replica
//     (a different instance whose gtid_executed contains the anchor's
//     set), is ACCEPTED and delivers the replica's next write — with a
//     row that exists only on the target as the witness that the
//     file/pos identity refusal's fall-through (drop the target tables,
//     re-copy) did not run;
//   - gtid_mode=OFF still persists file/pos bound to @@server_uuid and
//     still logs the POSITION-MODE advisory.
//
// Ground truth before the fix (2026-09-02, this file on the parent
// commit): both openers persisted
// {"mode":"file_pos","file":"mysql-bin.000001","pos":…,"server_uuid":<A>}
// on a gtid_mode=ON source, and the failover cell lost the witness row —
// the resume was refused on @@server_uuid and the target was re-copied.

package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
)

// persistedBinlogToken is the persisted MySQL position's contract as this
// package sees it — the JSON the engine writes, decoded without the
// engine's private type.
type persistedBinlogToken struct {
	Mode       string `json:"mode"`
	GTIDSet    string `json:"gtid_set"`
	File       string `json:"file"`
	Pos        uint32 `json:"pos"`
	ServerUUID string `json:"server_uuid"`
}

func decodePersistedMySQLToken(t *testing.T, token string) persistedBinlogToken {
	t.Helper()
	var p persistedBinlogToken
	if err := json.Unmarshal([]byte(token), &p); err != nil {
		t.Fatalf("persisted token %q is not a binlog position: %v", token, err)
	}
	return p
}

func serverUUIDMySQL(t *testing.T, dsn string) string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var uuid string
	if err := db.QueryRow("SELECT @@global.server_uuid").Scan(&uuid); err != nil {
		t.Fatalf("read @@server_uuid: %v", err)
	}
	return uuid
}

func globalVarMySQL(t *testing.T, dsn, name string) string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var v string
	if err := db.QueryRow("SELECT @@global." + name).Scan(&v); err != nil {
		t.Fatalf("read @@global.%s: %v", name, err)
	}
	return v
}

// gtidSubsetMySQL asks the server whether sub ⊆ super — the same primitive
// the resume check uses, evaluated by MySQL rather than by string logic.
func gtidSubsetMySQL(t *testing.T, dsn, sub, super string) bool {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var subset int
	if err := db.QueryRow("SELECT GTID_SUBSET(?, ?)", sub, super).Scan(&subset); err != nil {
		t.Fatalf("GTID_SUBSET: %v", err)
	}
	return subset == 1
}

// waitPersistedPositionMySQL polls sluice_cdc_state until the stream has
// written a position. Used right after the cold copy lands and before
// any CDC write, so the token read is the opener's handoff anchor rather
// than a per-event position.
func waitPersistedPositionMySQL(t *testing.T, dsn, streamID string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if tok := readPersistedPositionMySQL(t, dsn, streamID); tok != "" {
			return tok
		}
		time.Sleep(100 * time.Millisecond)
	}
	return ""
}

const slm4Seed = `
	CREATE TABLE gh_a (
		id      BIGINT       NOT NULL AUTO_INCREMENT,
		payload VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	CREATE TABLE gh_b (
		id      BIGINT       NOT NULL AUTO_INCREMENT,
		payload VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	INSERT INTO gh_a (payload) VALUES ('a-1'), ('a-2'), ('a-3');
	INSERT INTO gh_b (payload) VALUES ('b-1');
`

// slm4Opener names how the pipeline reaches each opener: the enumerated
// two-table scope under the default parallelism selects the N-way opener;
// copy_table_parallelism=1 on the source DSN selects the serial one. The
// log marker pins which one actually ran.
type slm4PipelineOpener struct {
	name      string
	dsnSuffix string
	logMarker string
}

var slm4PipelineOpeners = []slm4PipelineOpener{
	{name: "concurrent", dsnSuffix: "", logMarker: "opened consistent multi-table snapshot"},
	{name: "serial", dsnSuffix: "&copy_table_parallelism=1", logMarker: "captured consistent snapshot and CDC handoff position"},
}

// runColdStartAndReadAnchor runs one cold start to completion of the
// copy, reads the persisted handoff anchor before any CDC write, and
// returns it with the streamer's log output for the run. dsnSuffix is
// appended to the Streamer's source DSN only (the opener knob); the
// test's own writes use the plain DSN.
func runColdStartAndReadAnchor(t *testing.T, mysqlEng ir.Engine, sourceDSN, dsnSuffix, targetDSN, streamID string) (token string, logs string) {
	t.Helper()
	logBuf := &lockedBuffer{}
	prevDefault := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(prevDefault)

	streamer := &Streamer{
		Source:    mysqlEng,
		Target:    mysqlEng,
		SourceDSN: sourceDSN + dsnSuffix,
		TargetDSN: targetDSN,
		StreamID:  streamID,
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForRowCountMySQL(t, targetDSN, "gh_a", 3, 60*time.Second) || !waitForRowCountMySQL(t, targetDSN, "gh_b", 1, 30*time.Second) {
		streamCancel()
		<-runErr
		t.Fatalf("%s: bulk copy did not deliver the seed rows", streamID)
	}
	token = waitPersistedPositionMySQL(t, targetDSN, streamID, 30*time.Second)
	if token == "" {
		streamCancel()
		<-runErr
		t.Fatalf("%s: no position persisted after the cold copy", streamID)
	}
	// One live change through the tail before cancelling: it proves the
	// handoff to CDC actually completed on that anchor (a cancel while the
	// reader is still opening surfaces as a Run error), and it leaves the
	// anchor read above untouched — the token returned is the opener's,
	// not this event's. The anchor is persisted after the copy has
	// drained, so the count read here is the copy's final count; earlier
	// runs' live rows are part of it.
	copied := pollRowCountMySQL(targetDSN, "gh_b")
	applyDDLMySQL(t, sourceDSN, "INSERT INTO gh_b (payload) VALUES ('b-live')")
	if !waitForRowCountMySQL(t, targetDSN, "gh_b", copied+1, 60*time.Second) {
		streamCancel()
		<-runErr
		t.Fatalf("%s: the CDC tail did not deliver a change after the handoff", streamID)
	}
	streamCancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("%s: Streamer.Run returned err on cancel: %v", streamID, err)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("%s: Streamer.Run did not return after cancel", streamID)
	}
	return token, string(logBuf.Bytes())
}

func TestStreamer_MySQLGTIDMode_HandoffPersistsGTIDPositionOnBothOpeners(t *testing.T) {
	setPollIntervalForTest(t, 200*time.Millisecond)

	sourceDSN, targetDSN, cleanup := startMySQLGTID(t)
	defer cleanup()
	if got := globalVarMySQL(t, sourceDSN, "gtid_mode"); got != "ON" {
		t.Fatalf("premise: gtid_mode = %q, want ON", got)
	}
	uuid := serverUUIDMySQL(t, sourceDSN)
	applyDDLMySQL(t, sourceDSN, slm4Seed)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	for _, o := range slm4PipelineOpeners {
		streamID := "gtid-handoff-" + o.name
		// The independent expected value is a bracket rather than an
		// equality: source_db and target_db share this server, so the
		// streamer's own control-table writes advance @@gtid_executed
		// during the run. The anchor must contain everything executed
		// before the run and be contained in everything executed after it,
		// as the server itself computes it (GTID_SUBSET).
		preSet := globalVarMySQL(t, sourceDSN, "gtid_executed")
		token, logs := runColdStartAndReadAnchor(t, mysqlEng, sourceDSN, o.dsnSuffix, targetDSN, streamID)
		postSet := globalVarMySQL(t, sourceDSN, "gtid_executed")
		if !strings.Contains(logs, o.logMarker) {
			t.Fatalf("%s: the run's log does not carry %q — the cell ran on the wrong opener", o.name, o.logMarker)
		}
		t.Logf("%s opener persisted handoff anchor = %s", o.name, token)
		p := decodePersistedMySQLToken(t, token)
		if p.Mode != "gtid" {
			t.Fatalf("%s opener: the sync cold start on a gtid_mode=ON source persisted a %q position (%s); "+
				"want a GTID-mode one — the SLM-4 defect", o.name, p.Mode, token)
		}
		if !strings.Contains(p.GTIDSet, uuid) {
			t.Fatalf("%s opener: persisted set %q does not carry the source uuid %s", o.name, p.GTIDSet, uuid)
		}
		if !gtidSubsetMySQL(t, sourceDSN, preSet, p.GTIDSet) || !gtidSubsetMySQL(t, sourceDSN, p.GTIDSet, postSet) {
			t.Fatalf("%s opener: persisted set %q is not bracketed by the server's executed set before (%q) and after (%q) the run",
				o.name, p.GTIDSet, preSet, postSet)
		}
		// Fresh target for the next opener (the Bug 9 pre-flight refuses a
		// cold start onto populated tables).
		applyDDLMySQL(t, targetDSN, "DROP TABLE gh_a; DROP TABLE gh_b")
	}
}

// TestStreamer_MySQLGTIDMode_FailoverToPromotedReplicaResumes is the
// operator scenario the finding names: the primary the sync cold-started
// on is gone, the application now points at a promoted replica, and
// `sync start` resumes from the anchor that cold start persisted.
func TestStreamer_MySQLGTIDMode_FailoverToPromotedReplicaResumes(t *testing.T) {
	setPollIntervalForTest(t, 200*time.Millisecond)

	srcA, tgtA, cleanupA := startMySQLGTID(t)
	defer cleanupA()
	uuidA := serverUUIDMySQL(t, srcA)
	applyDDLMySQL(t, srcA, slm4Seed)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	const streamID = "gtid-failover"
	anchorFromA, _ := runColdStartAndReadAnchor(t, mysqlEng, srcA, "", tgtA, streamID)
	t.Logf("anchor persisted on A = %s", anchorFromA)
	// The anchor's MODE is not gated here — the handoff test pins it. This
	// cell grades what happens NEXT, and must reach the witness assertion
	// below on the defective (file/pos) anchor too, or it would be
	// measuring its own precondition rather than the outcome.
	anchor := decodePersistedMySQLToken(t, anchorFromA)

	// The promoted replica C: the same logical data (seeded BEFORE its
	// lineage is rewritten, so nothing about the seeding is in its binlog),
	// then A's lineage adopted through gtid_purged — the shape a replica
	// promoted from A, or a --set-gtid-purged=ON restore of A, presents —
	// under C's own @@server_uuid. The adopted set is the anchor's own
	// (the retention check requires gtid_purged ⊆ the resume set, as it
	// would on a real replica whose binlog carries what it replicated);
	// a file/pos anchor has no set, and A's executed set stands in.
	lineage := anchor.GTIDSet
	if lineage == "" {
		lineage = globalVarMySQL(t, srcA, "gtid_executed")
	}
	srcC, tgtC, cleanupC := startMySQLGTID(t)
	defer cleanupC()
	applyDDLMySQL(t, srcC, slm4Seed)
	applyDDLMySQL(t, srcC, "RESET MASTER")
	applyDDLMySQL(t, srcC, "SET @@GLOBAL.gtid_purged = '"+lineage+"'")
	uuidC := serverUUIDMySQL(t, srcC)
	if uuidC == uuidA {
		t.Fatal("the promoted replica must carry a different @@server_uuid")
	}
	if got := globalVarMySQL(t, srcC, "gtid_executed"); !strings.Contains(got, uuidA) {
		t.Fatalf("seeding C's lineage failed: gtid_executed %q does not carry A's uuid", got)
	}

	// C's target: the rows the cold start delivered, the stream's persisted
	// anchor, and a WITNESS row that exists only here. If the resume is
	// refused, the streamer's fall-through drops the target tables and
	// re-copies, and the witness is gone — that is the independent
	// evidence, not the absence of a log line.
	applyDDLMySQL(t, tgtC, slm4Seed)
	applyDDLMySQL(t, tgtC, "INSERT INTO gh_a (id, payload) VALUES (9000, 'target-only-witness')")
	seedCDCStateMySQL(t, tgtC, streamID, anchorFromA)

	logBuf := &lockedBuffer{}
	prevDefault := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(prevDefault)

	resume := &Streamer{
		Source:    mysqlEng,
		Target:    mysqlEng,
		SourceDSN: srcC,
		TargetDSN: tgtC,
		StreamID:  streamID,
	}
	resumeCtx, resumeCancel := context.WithCancel(context.Background())
	defer resumeCancel()
	resumeErr := make(chan error, 1)
	go func() { resumeErr <- resume.Run(resumeCtx) }()

	applyDDLMySQL(t, srcC, "INSERT INTO gh_a (payload) VALUES ('after-failover')")
	// 3 seed + witness + the post-failover row.
	if !waitForRowCountMySQL(t, tgtC, "gh_a", 5, 60*time.Second) {
		resumeCancel()
		<-resumeErr
		t.Fatalf("the promoted replica's write did not arrive (gh_a has %d rows; witness present: %v). Log tail:\n%s",
			pollRowCountMySQL(tgtC, "gh_a"), witnessPresentMySQL(t, tgtC), tailOf(string(logBuf.Bytes()), 4000))
	}
	if !witnessPresentMySQL(t, tgtC) {
		resumeCancel()
		<-resumeErr
		t.Fatal("the target-only witness row is GONE: the resume against the promoted replica was refused and the " +
			"fall-through dropped and re-copied the target — the SLM-4 outcome")
	}
	after := decodePersistedMySQLToken(t, readPersistedPositionMySQL(t, tgtC, streamID))
	if after.Mode != "gtid" || !strings.Contains(after.GTIDSet, uuidC) || !strings.Contains(after.GTIDSet, uuidA) {
		t.Fatalf("after the failover the persisted position is %+v; want a GTID set carrying both lineages (%s, %s)", after, uuidA, uuidC)
	}
	if strings.Contains(string(logBuf.Bytes()), "UNVERIFIED-INSTANCE-IDENTITY") {
		t.Fatal("the resume ran with the UNVERIFIED-INSTANCE-IDENTITY warning; a GTID resume checks lineage and must not degrade")
	}

	resumeCancel()
	select {
	case err := <-resumeErr:
		if err != nil {
			t.Errorf("resume Streamer.Run returned err: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Errorf("resume Streamer.Run did not return after ctx cancel")
	}
}

// TestStreamer_MySQLFilePosMode_HandoffStampsServerUUIDAndAdvises pins the
// other arm through the pipeline: gtid_mode=OFF (MySQL 8's default) still
// persists file/pos bound to @@server_uuid on both openers, and the
// POSITION-MODE advisory still fires for the cold start. A cold start
// crosses the CDC-open preflight chokepoint twice — once in the snapshot
// opener and once in the paired reader's StreamChanges — so the advisory
// is expected once per crossing; the count is pinned so a change to
// either chokepoint's roster shows up here.
func TestStreamer_MySQLFilePosMode_HandoffStampsServerUUIDAndAdvises(t *testing.T) {
	setPollIntervalForTest(t, 200*time.Millisecond)

	sourceDSN, targetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()
	if got := globalVarMySQL(t, sourceDSN, "gtid_mode"); got != "OFF" {
		t.Fatalf("premise: gtid_mode = %q, want OFF", got)
	}
	uuid := serverUUIDMySQL(t, sourceDSN)
	applyDDLMySQL(t, sourceDSN, slm4Seed)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	const advisoriesPerColdStart = 2
	for _, o := range slm4PipelineOpeners {
		streamID := "filepos-handoff-" + o.name
		token, logs := runColdStartAndReadAnchor(t, mysqlEng, sourceDSN, o.dsnSuffix, targetDSN, streamID)
		if !strings.Contains(logs, o.logMarker) {
			t.Fatalf("%s: the run's log does not carry %q — the cell ran on the wrong opener", o.name, o.logMarker)
		}
		t.Logf("%s opener persisted handoff anchor = %s", o.name, token)
		p := decodePersistedMySQLToken(t, token)
		if p.Mode != "file_pos" || p.File == "" || p.Pos == 0 {
			t.Fatalf("%s opener on a gtid_mode=OFF source persisted %+v; want a file/pos position", o.name, p)
		}
		if p.ServerUUID != uuid {
			t.Fatalf("%s opener: persisted server_uuid %q, want the source's %q", o.name, p.ServerUUID, uuid)
		}
		if p.GTIDSet != "" {
			t.Fatalf("%s opener: a file/pos position must not carry a GTID set: %+v", o.name, p)
		}
		if n := strings.Count(logs, "POSITION-MODE"); n != advisoriesPerColdStart {
			t.Fatalf("%s opener: POSITION-MODE advisory logged %d times, want %d (opener preflight + paired reader open)", o.name, n, advisoriesPerColdStart)
		}
		applyDDLMySQL(t, targetDSN, "DROP TABLE gh_a; DROP TABLE gh_b")
	}
}

func witnessPresentMySQL(t *testing.T, dsn string) bool {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM gh_a WHERE payload = 'target-only-witness'").Scan(&n); err != nil {
		// The table itself is gone mid-re-copy: the witness is gone too.
		return false
	}
	return n == 1
}

func tailOf(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}
