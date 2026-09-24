//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/logcapture"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// TestIncrementalBackup_ChainRestore_AddColumnDefaults is the chain-replay
// lane of the pre-existing-row DEFAULT gate (GC-36 (2); the live-forward
// lanes are TestStreamer_AddColumnForward_PreexistingRowDefaults_*).
//
// An `ALTER TABLE users ADD COLUMN c … DEFAULT d` taken on the source
// between the full and an incremental fills every row the source already
// held with d, and that fill writes no row event. Each cell is graded per
// row against the SOURCE's own value (read back by id), so the expected
// value is independent of the manifest the restore replays.
//
// Four capture lanes, because the delta is recorded by two orchestrators
// (IncrementalBackup.Run, BackupStream's rollover) and read from two source
// catalogs, plus the two capture lanes encrypted. The replay side
// (migcore.ApplyAlterDelta) is shared by chain restore and the broker, so
// it is one lane on the replay axis.
//
// The pre-existing rows are ids 1-3 (restored by the full) and id 4,
// inserted IN the window but BEFORE the ALTERs, so its own INSERT carries
// no value for the added columns: a fill replayed anywhere but after the
// window's events would miss it. Id 5 is inserted after every ALTER and
// carries its values in its row event.
//
// What the replayed ADD COLUMN cannot reproduce — a DEFAULT dropped or
// changed later in the window (Django's AddField), a non-constant DEFAULT —
// is carried by the capture-time fill (captureAddColumnFill), so every cell
// must read back the source. The `d_` twins re-run each constant family as
// a Django AddField, so every value family reaches the target through the
// fill rather than through the replayed DEFAULT (Bug 74: the class, not a
// representative).
func TestIncrementalBackup_ChainRestore_AddColumnDefaults(t *testing.T) {
	t.Run("pg-incremental", func(t *testing.T) { runACDLane(t, acdPGLane(false)) })
	t.Run("pg-stream", func(t *testing.T) { runACDLane(t, acdPGLane(true)) })
	t.Run("mysql-incremental", func(t *testing.T) { runACDLane(t, acdMySQLLane(false)) })
	t.Run("mysql-stream", func(t *testing.T) { runACDLane(t, acdMySQLLane(true)) })
	// The fill's chunks are sealed after the window's, at the next ordinals
	// of the same run namespace, through each lane's own CEK resolution —
	// so an encrypted chain must open them under the ADR-0152 binding on
	// both sealers, in both key modes.
	t.Run("pg-incremental-encrypted-per-chunk", func(t *testing.T) {
		lane := acdPGLane(false)
		lane.encrypt = crypto.EncryptModePerChunk
		runACDLane(t, lane)
	})
	t.Run("pg-stream-encrypted-per-chain", func(t *testing.T) {
		lane := acdPGLane(true)
		lane.encrypt = crypto.EncryptModePerChain
		runACDLane(t, lane)
	})
}

// acdCell is one ADD COLUMN shape.
type acdCell struct {
	col string
	def string // the column definition after the name
	// then runs in the same window, after every ADD COLUMN.
	then string
	// post is the column's explicit value on the post-ALTER row, for a
	// column that no longer has a default to omit it against.
	post string
}

// acdLane is one (source engine, capture orchestrator) combination.
type acdLane struct {
	engine string
	driver string
	start  func(t *testing.T) (src, tgt string, cleanup func())
	seed   string
	// startPos records the position the full's chain starts at (slot LSN
	// or binlog coordinate) and returns a teardown.
	startPos func(t *testing.T, src string) (ir.Position, func())
	exec     func(t *testing.T, dsn, sql string)
	// render is the per-cell grading expression (textual, both sides).
	render func(col string) string
	cells  []acdCell
	stream bool
	// preInWindow inserts id 4 in the window, before the ALTERs. Empty
	// when the seed holds id 4 instead: the MySQL binlog reader refuses,
	// loudly, to decode an event recorded under an older table shape once
	// information_schema has moved on — and these captures run after the
	// DDL. (A live `backup stream` decodes that INSERT before the ALTER.)
	preInWindow string
	// encrypt, when set, is the key mode every link is encrypted under.
	encrypt string
}

// acdPassphrase keys the encrypted lanes.
const acdPassphrase = "acd-fill-test-passphrase"

// encryption returns the lane's encryption for a backup write — first is
// the full, which mints the chain's salt; every later writer rebinds to it.
func (l acdLane) encryption(t *testing.T, first bool) *lineage.BackupEncryption {
	t.Helper()
	if l.encrypt == "" {
		return nil
	}
	enc := &lineage.BackupEncryption{Envelope: newTestPassphraseEnvelope(t, acdPassphrase), Mode: l.encrypt}
	if !first {
		enc.RebuildForChain = passphraseRebuildHook(acdPassphrase)
	}
	return enc
}

// acdDjangoTwins returns cells plus, for every constant nullable `c_` cell,
// a `d_` twin that drops the default after the ADD — Django's AddField. The
// twin's pre-existing rows can only get the source's value from the fill.
func acdDjangoTwins(cells []acdCell) []acdCell {
	out := append([]acdCell(nil), cells...)
	for _, c := range cells {
		if !strings.HasPrefix(c.col, "c_") || c.then != "" || strings.Contains(c.def, "NOT NULL") {
			continue
		}
		twin := "d_" + strings.TrimPrefix(c.col, "c_")
		out = append(out, acdCell{col: twin, def: c.def, then: fmt.Sprintf("ALTER TABLE users ALTER COLUMN %s DROP DEFAULT", twin), post: "NULL"})
	}
	return out
}

func acdPGLane(stream bool) acdLane {
	return acdLane{
		engine: "postgres",
		driver: "pgx",
		start:  startPostgresLogical,
		seed: `
			CREATE TABLE users (
				id    BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
				email VARCHAR(255) NOT NULL
			);
			ALTER TABLE users REPLICA IDENTITY FULL;
			INSERT INTO users (email) VALUES ('a@x'), ('b@x'), ('c@x');
			CREATE PUBLICATION sluice_pub FOR ALL TABLES;`,
		startPos: func(t *testing.T, src string) (ir.Position, func()) {
			lsn, err := createPGLogicalSlotReturningLSN(t, src, "sluice_slot")
			if err != nil {
				t.Fatalf("create slot: %v", err)
			}
			return ir.Position{Engine: "postgres", Token: fmt.Sprintf(`{"slot":"sluice_slot","lsn":%q}`, lsn)},
				func() { dropPGLogicalSlot(t, src, "sluice_slot") }
		},
		exec:        applyDDL,
		preInWindow: "INSERT INTO users (email) VALUES ('pre@x')",
		render:      func(col string) string { return fmt.Sprintf(`%q::text`, col) },
		stream:      stream,
		cells: acdDjangoTwins([]acdCell{
			{col: "c_text", def: `TEXT DEFAULT 'abc'`},
			{col: "c_varchar", def: `VARCHAR(10) DEFAULT 'vv'`},
			{col: "c_int", def: `INTEGER DEFAULT 42`},
			{col: "c_bigint", def: `BIGINT DEFAULT -9000000000`},
			{col: "c_numeric", def: `NUMERIC(10,2) DEFAULT 1.10`},
			{col: "c_float", def: `DOUBLE PRECISION DEFAULT 2.5`},
			{col: "c_bool", def: `BOOLEAN DEFAULT true`},
			{col: "c_ts", def: `TIMESTAMP DEFAULT '2020-01-02 03:04:05.123456'`},
			{col: "c_tstz", def: `TIMESTAMPTZ DEFAULT '2020-01-02 03:04:05+00'`},
			{col: "c_date", def: `DATE DEFAULT '2020-01-02'`},
			{col: "c_bytea", def: `BYTEA DEFAULT '\x00ff'::bytea`},
			{col: "c_jsonb", def: `JSONB DEFAULT '{"a": 1}'::jsonb`},
			{col: "c_uuid", def: `UUID DEFAULT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'`},
			{col: "c_nn_text", def: `TEXT NOT NULL DEFAULT 'nn'`},
			{col: "c_nn_int", def: `INTEGER NOT NULL DEFAULT 7`},
			// Django's AddField: the default exists only for the ALTER.
			{col: "x_django", def: `TEXT DEFAULT 'dj'`, then: `ALTER TABLE users ALTER COLUMN x_django DROP DEFAULT`, post: `'p'`},
			{col: "x_django_nn", def: `INTEGER NOT NULL DEFAULT 5`, then: `ALTER TABLE users ALTER COLUMN x_django_nn DROP DEFAULT`, post: `9`},
			{col: "x_reset", def: `TEXT DEFAULT 'first'`, then: `ALTER TABLE users ALTER COLUMN x_reset SET DEFAULT 'second'`},
			// The in-window race: a pre-existing row updated after the ADD
			// keeps its update, then the default goes away.
			{col: "x_race", def: `TEXT DEFAULT 'r0'`, then: `UPDATE users SET x_race = 'upd' WHERE id = 2; ALTER TABLE users ALTER COLUMN x_race DROP DEFAULT`, post: `NULL`},
			// PG stores now()'s ALTER-time value as the fill (a STABLE
			// default); clock_timestamp() rewrites the table per row.
			{col: "x_now", def: `TIMESTAMPTZ DEFAULT now()`},
			{col: "x_clock", def: `TIMESTAMPTZ DEFAULT clock_timestamp()`},
		}),
	}
}

func acdMySQLLane(stream bool) acdLane {
	return acdLane{
		engine: "mysql",
		driver: "mysql",
		start:  startMySQLBinlog,
		seed: `
			CREATE TABLE users (
				id    BIGINT       NOT NULL AUTO_INCREMENT,
				email VARCHAR(255) NOT NULL,
				PRIMARY KEY (id)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
			INSERT INTO users (email) VALUES ('a@x'), ('b@x'), ('c@x'), ('pre@x');`,
		startPos: func(t *testing.T, src string) (ir.Position, func()) {
			file, pos := readMySQLBinlogPos(t, src)
			return ir.Position{Engine: "mysql", Token: fmt.Sprintf(`{"mode":"file_pos","file":%q,"pos":%d}`, file, pos)}, func() {}
		},
		exec: applyDDLMySQL,
		render: func(col string) string {
			if strings.Contains(col, "_bin") {
				return fmt.Sprintf("HEX(`%s`)", col)
			}
			return fmt.Sprintf("CAST(`%s` AS CHAR)", col)
		},
		stream: stream,
		cells: acdDjangoTwins([]acdCell{
			{col: "c_varchar", def: `VARCHAR(10) DEFAULT 'vv'`},
			{col: "c_int", def: `INT DEFAULT 42`},
			{col: "c_bigint", def: `BIGINT DEFAULT -9000000000`},
			{col: "c_decimal", def: `DECIMAL(10,2) DEFAULT 1.10`},
			{col: "c_double", def: `DOUBLE DEFAULT 2.5`},
			{col: "c_bool", def: `TINYINT(1) DEFAULT 1`},
			{col: "c_datetime", def: `DATETIME(6) DEFAULT '2020-01-02 03:04:05.123456'`},
			{col: "c_date", def: `DATE DEFAULT '2020-01-02'`},
			{col: "c_enum", def: `ENUM('a','b') DEFAULT 'b'`},
			{col: "c_binary", def: `VARBINARY(4) DEFAULT 0x00FF`},
			// MySQL 8.0.13+ expression defaults on TEXT/BLOB/JSON.
			{col: "c_text", def: `TEXT DEFAULT ('abc')`},
			{col: "c_bin_blob", def: `BLOB DEFAULT (0x00FF)`},
			{col: "c_json", def: `JSON DEFAULT ('{"a": 1}')`},
			{col: "c_nn_varchar", def: `VARCHAR(10) NOT NULL DEFAULT 'nn'`},
			{col: "c_nn_int", def: `INT NOT NULL DEFAULT 7`},
			{col: "x_django", def: `VARCHAR(10) DEFAULT 'dj'`, then: "ALTER TABLE users ALTER COLUMN x_django DROP DEFAULT", post: `'p'`},
			{col: "x_django_nn", def: `INT NOT NULL DEFAULT 5`, then: "ALTER TABLE users ALTER COLUMN x_django_nn DROP DEFAULT", post: `9`},
			{col: "x_reset", def: `VARCHAR(10) DEFAULT 'first'`, then: "ALTER TABLE users ALTER COLUMN x_reset SET DEFAULT 'second'"},
			{col: "x_race", def: `VARCHAR(10) DEFAULT 'r0'`, then: "UPDATE users SET x_race = 'upd' WHERE id = 2; ALTER TABLE users ALTER COLUMN x_race DROP DEFAULT", post: `NULL`},
			{col: "x_now", def: `DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6)`},
		}),
	}
}

// acdPreRows are the ids that exist before the window's ALTERs; acdPostID
// is the row inserted after them.
const (
	acdPreRows = 4
	acdPostID  = 5
)

// runACDLane takes a full, inserts one row, applies every cell's ADD COLUMN
// (then its follow-up) plus one post-ALTER write, captures one incremental
// with the lane's orchestrator, chain-restores into the fresh target and
// grades.
func runACDLane(t *testing.T, lane acdLane) {
	src, tgt, cleanup := lane.start(t)
	defer cleanup()
	lane.exec(t, src, lane.seed)

	eng, ok := engines.Get(lane.engine)
	if !ok {
		t.Fatalf("%s engine not registered", lane.engine)
	}
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	pos, teardown := lane.startPos(t, src)
	defer teardown()

	ctx := context.Background()
	if err := (&backup.Backup{Source: eng, SourceDSN: src, Store: store, SluiceVersion: "test", Encryption: lane.encryption(t, true)}).Run(ctx); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	full, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	full.Kind = irbackup.BackupKindFull
	full.EndPosition = pos
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("rewrite full: %v", err)
	}

	if lane.preInWindow != "" {
		lane.exec(t, src, lane.preInWindow)
	}

	var window []string
	for _, c := range lane.cells {
		window = append(window, fmt.Sprintf("ALTER TABLE users ADD COLUMN %s %s", c.col, c.def))
	}
	for _, c := range lane.cells {
		if c.then != "" {
			window = append(window, c.then)
		}
	}
	cols, vals := []string{"email"}, []string{"'post@x'"}
	for _, c := range lane.cells {
		if c.post != "" {
			cols, vals = append(cols, c.col), append(vals, c.post)
		}
	}
	window = append(window, fmt.Sprintf("INSERT INTO users (%s) VALUES (%s)", strings.Join(cols, ", "), strings.Join(vals, ", ")))

	captureLogs := &logcapture.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(captureLogs, &slog.HandlerOptions{Level: slog.LevelInfo})))
	lane.exec(t, src, strings.Join(window, ";\n")+";")
	if lane.stream {
		acdRunStream(t, eng, src, store, full.BackupID, lane.encryption(t, false))
	} else {
		incrCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		err := (&IncrementalBackup{
			Source: eng, SourceDSN: src, Store: store, ParentRef: full.BackupID,
			Window: 8 * time.Second, ChunkChanges: 3, SluiceVersion: "test", Encryption: lane.encryption(t, false),
		}).Run(incrCtx)
		cancel()
		if err != nil {
			slog.SetDefault(prev)
			t.Fatalf("IncrementalBackup.Run: %v", err)
		}
	}
	slog.SetDefault(prev)
	if strings.Contains(captureLogs.String(), AddColumnFillNotCapturedMarker) {
		t.Errorf("capture named a fill it could not record on a keyed table: %s", captureLogs.String())
	}
	acdAssertFillRecorded(t, store, lane)

	logBuf := &logcapture.Buffer{}
	slog.SetDefault(slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	restore := &backup.Restore{Target: eng, TargetDSN: tgt, Store: store}
	if lane.encrypt != "" {
		restore.Envelope = envelopeFromManifest(t, store, acdPassphrase)
	}
	restoreErr := restore.Run(ctx)
	slog.SetDefault(prev)
	if restoreErr != nil {
		t.Fatalf("Restore.Run: %v", restoreErr)
	}
	logs := logBuf.String()
	if !strings.Contains(logs, "applied ADD COLUMN") {
		t.Fatalf("restore applied no ADD COLUMN — the gate graded nothing it claims to (logs: %s)", logs)
	}
	// Every added column's fill was captured, so the restore reproduces the
	// source and has nothing to name. (Chains without a fill record — those
	// an older build captured — are TestApplyAlterDelta_NamesAnUnreproducibleAddColumnFill.)
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, migcore.AddColumnFillNotReproducibleMarker) {
			t.Errorf("restore named a column whose fill the chain carries: %s", line)
		}
	}

	for _, c := range lane.cells {
		acdGradeCell(t, lane, src, tgt, c)
	}
}

// acdAssertFillRecorded checks the capture stamped a fill covering every
// added column on the incremental's delta. Structural only — the values are
// graded against the source by acdGradeCell.
func acdAssertFillRecorded(t *testing.T, store irbackup.Store, lane acdLane) {
	t.Helper()
	records, err := lineage.ListAllManifestsViaWalk(context.Background(), store)
	if err != nil {
		t.Fatalf("list manifests: %v", err)
	}
	var fill *irbackup.AddColumnFill
	for _, r := range records {
		for _, d := range r.Manifest.SchemaDelta {
			if d != nil && d.AddColumnFill != nil {
				fill = d.AddColumnFill
				acdAssertChunksSealed(t, r.Manifest, lane)
			}
		}
	}
	if fill == nil {
		// Error, not Fatal: the per-row grades below are the evidence that
		// matters, and they must still run.
		t.Error("no incremental recorded an ADD COLUMN fill")
		return
	}
	if fill.Skipped != "" || fill.Rows != acdPreRows+1 || len(fill.Columns) != len(lane.cells) {
		t.Errorf("fill = %+v; want every one of %d columns captured over %d rows", fill, len(lane.cells), acdPreRows+1)
	}
}

// acdAssertChunksSealed checks an encrypted lane's fill-bearing incremental
// really is encrypted, in its key mode — so the lane's green is a statement
// about sealed fill chunks and not a plaintext run.
func acdAssertChunksSealed(t *testing.T, m *irbackup.Manifest, lane acdLane) {
	t.Helper()
	if lane.encrypt == "" {
		return
	}
	for i, c := range m.ChangeChunks {
		switch {
		case c.Encryption == nil:
			t.Errorf("change chunk %d (%s) is plaintext on an encrypted lane", i, c.File)
		case lane.encrypt == crypto.EncryptModePerChunk && len(c.Encryption.WrappedCEK) == 0:
			t.Errorf("change chunk %d (%s) carries no per-chunk CEK", i, c.File)
		}
	}
}

// acdRunStream runs `backup stream` until at least one rollover has
// committed and the count has settled, then stops it.
func acdRunStream(t *testing.T, eng ir.Engine, src string, store irbackup.Store, parent string, enc *lineage.BackupEncryption) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	stream := &BackupStream{
		Source: eng, SourceDSN: src, Store: store, ParentRef: parent,
		RolloverWindow: 2 * time.Second, RolloverMaxChanges: 10, RolloverMaxBytes: 1 << 30,
		ChunkChanges: 3, SluiceVersion: "test", Encryption: enc,
	}
	done := make(chan error, 1)
	go func() { done <- stream.Run(ctx) }()

	var last, stable int
	for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline); time.Sleep(500 * time.Millisecond) {
		records, _ := lineage.ListAllManifestsViaWalk(context.Background(), store)
		n, withDelta := 0, false
		for _, r := range records {
			if r.Manifest.Kind == irbackup.BackupKindIncremental {
				n++
				withDelta = withDelta || len(r.Manifest.SchemaDelta) > 0
			}
		}
		if n == last {
			stable++
		} else {
			last, stable = n, 0
		}
		if withDelta && stable >= 6 {
			break
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("stream.Run after cancel = %v; want nil", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("stream.Run did not exit within 15s of cancel")
	}
}

// acdGradeCell grades one cell's rows against the source.
func acdGradeCell(t *testing.T, lane acdLane, src, tgt string, c acdCell) {
	t.Helper()
	want := acdColumnByID(t, lane, src, c.col)
	got := acdColumnByID(t, lane, tgt, c.col)
	if len(want) != acdPostID || len(got) != len(want) {
		t.Errorf("%s: row counts source=%d target=%d; want %d each", c.col, len(want), len(got), acdPostID)
		return
	}
	// The post-ALTER row carries an explicit value in its row event, so a
	// miss there is the replay, not the fill.
	if got[acdPostID] != want[acdPostID] {
		t.Errorf("%s (%s): the post-ALTER row id=%d reads %q, source %q — the row event's own value did not land",
			c.col, c.def, acdPostID, got[acdPostID], want[acdPostID])
	}
	var wrong []string
	for id := int64(1); id <= acdPreRows; id++ {
		if got[id] != want[id] {
			wrong = append(wrong, fmt.Sprintf("id=%d source %q target %q", id, want[id], got[id]))
		}
	}
	if len(wrong) > 0 {
		t.Errorf("%s (%s): WRONG — pre-existing rows restored without the source's fill: %s", c.col, c.def, strings.Join(wrong, "; "))
	}
}

// acdColumnByID returns the lane's textual rendering of users.<col> keyed
// by id, "<NULL>" for NULL.
func acdColumnByID(t *testing.T, lane acdLane, dsn, col string) map[int64]string {
	t.Helper()
	db, err := sql.Open(lane.driver, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`SELECT id, %s FROM users`, lane.render(col)))
	if err != nil {
		t.Fatalf("query %s: %v", col, err)
	}
	defer func() { _ = rows.Close() }()
	out := map[int64]string{}
	for rows.Next() {
		var id int64
		var v sql.NullString
		if err := rows.Scan(&id, &v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out[id] = "<NULL>"
		if v.Valid {
			out[id] = v.String
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}
